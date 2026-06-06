package planner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/google/uuid"
	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/catalog"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/db"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/perm"
	"github.com/vektah/gqlparser/v2/ast"
)

const ingestViewNamePrefix = "_hugr_ingest_"

type ingestColumn struct {
	ArrowField arrow.Field
	Field      *sdl.Field
	FieldDef   *ast.FieldDefinition
	InputDef   *ast.FieldDefinition
}

type ingestExecState struct {
	reader  array.RecordReader
	view    string
	arrow   *db.Arrow
	release func()
	once    sync.Once
	err     error
}

func ingestRootNode(ctx context.Context, provider catalog.Provider, planner Catalog, dataObject string, reader array.RecordReader) (*QueryPlanNode, func() error, error) {
	if dataObject == "" {
		return nil, nil, fmt.Errorf("missing data object")
	}
	if reader == nil {
		return nil, nil, fmt.Errorf("missing arrow reader")
	}

	info, mutationField, err := resolveIngestTarget(ctx, provider, dataObject)
	if err != nil {
		return nil, nil, err
	}
	engine, err := planner.Engine(info.Catalog)
	if err != nil {
		return nil, nil, err
	}
	if caps := engine.Capabilities(); caps == nil || !caps.Insert.Ingest {
		return nil, nil, fmt.Errorf("engine %q does not support IPC ingest", engine.Type())
	}
	mutation := sdl.MutationInfo(ctx, provider, mutationField)
	if mutation == nil || mutation.Type != sdl.MutationTypeInsert {
		return nil, nil, fmt.Errorf("data object %q has no insert mutation defined", dataObject)
	}

	columns, err := resolveIngestColumns(ctx, provider, info, mutation, reader.Schema())
	if err != nil {
		return nil, nil, err
	}
	if len(columns) == 0 {
		return nil, nil, fmt.Errorf("no insertable columns matched between arrow stream and data object")
	}
	permissionData, err := checkIngestPermissions(ctx, provider, info, mutationField, columns)
	if err != nil {
		return nil, nil, err
	}

	state := &ingestExecState{
		reader: reader,
		view:   ingestViewNamePrefix + strings.ReplaceAll(uuid.NewString(), "-", ""),
	}
	node := ingestNode(ctx, info, mutation, engine, columns, permissionData, state)
	return node, state.cancel, nil
}

func resolveIngestTarget(ctx context.Context, provider catalog.Provider, dataObject string) (*sdl.Object, *ast.FieldDefinition, error) {
	var def *ast.Definition
	if strings.Contains(dataObject, ".") {
		queryDef := provider.ForName(ctx, base.QueryBaseName)
		if queryDef == nil {
			return nil, nil, fmt.Errorf("query base type not found in schema")
		}
		cur := queryDef
		for _, part := range strings.Split(dataObject, ".") {
			f := cur.Fields.ForName(part)
			if f == nil {
				return nil, nil, fmt.Errorf("data object %q: segment %q not found", dataObject, part)
			}
			cur = provider.ForName(ctx, f.Type.Name())
			if cur == nil {
				return nil, nil, fmt.Errorf("data object %q: type %q not found", dataObject, f.Type.Name())
			}
		}
		def = cur
	} else {
		def = provider.ForName(ctx, dataObject)
	}
	if def == nil {
		return nil, nil, fmt.Errorf("data object %q not found in schema", dataObject)
	}
	if !sdl.IsDataObject(def) {
		return nil, nil, fmt.Errorf("%q is not a data object", dataObject)
	}
	info := sdl.DataObjectInfo(def)
	if info == nil {
		return nil, nil, fmt.Errorf("data object %q: no info", dataObject)
	}
	if info.Type != sdl.TableDataObject {
		return nil, nil, fmt.Errorf("data object %q is not a table (got %q): only tables are ingestable", dataObject, info.Type)
	}
	if info.Catalog == "" {
		return nil, nil, fmt.Errorf("data object %q has no catalog", dataObject)
	}
	_, mutationField := sdl.ObjectMutationDefinition(ctx, provider, def, sdl.MutationTypeInsert)
	if mutationField == nil {
		return nil, nil, fmt.Errorf("data object %q has no insert mutation defined", dataObject)
	}
	return info, mutationField, nil
}

func resolveIngestColumns(ctx context.Context, provider catalog.Provider, info *sdl.Object, mutation *sdl.Mutation, schema *arrow.Schema) ([]ingestColumn, error) {
	if schema == nil {
		return nil, fmt.Errorf("arrow stream has no schema")
	}
	inputName := info.InputInsertDataName()
	if inputName == "" {
		return nil, fmt.Errorf("data object %q has no insert input type", info.Name)
	}
	input := provider.ForName(ctx, inputName)
	if input == nil {
		return nil, fmt.Errorf("insert input type %q not found", inputName)
	}

	seen := map[string]struct{}{}
	byName := make(map[string]ingestColumn, schema.NumFields())
	for _, af := range schema.Fields() {
		if _, dup := seen[af.Name]; dup {
			return nil, fmt.Errorf("duplicate arrow column %q", af.Name)
		}
		seen[af.Name] = struct{}{}

		inputField := input.Fields.ForName(af.Name)
		if inputField == nil {
			return nil, fmt.Errorf("column %q is not defined in insert input %q", af.Name, inputName)
		}
		objectField := info.Definition().Fields.ForName(af.Name)
		if objectField == nil {
			return nil, fmt.Errorf("column %q is not defined in data object %q", af.Name, info.Definition().Name)
		}
		fieldInfo := info.FieldForName(af.Name)
		if fieldInfo == nil {
			return nil, fmt.Errorf("column %q is not defined in data object %q", af.Name, info.Definition().Name)
		}
		if fieldInfo.IsReferencesSubquery() {
			return nil, fmt.Errorf("column %q is a reference and cannot be ingested directly", af.Name)
		}
		if fieldInfo.IsNotDBField() {
			return nil, fmt.Errorf("column %q is a computed/virtual field and cannot be ingested", af.Name)
		}
		if fieldInfo.FieldSourceName("", false) == "-" {
			return nil, fmt.Errorf("column %q has no database mapping", af.Name)
		}
		byName[af.Name] = ingestColumn{
			ArrowField: af,
			Field:      fieldInfo,
			FieldDef:   objectField,
			InputDef:   inputField,
		}
	}

	for _, fieldInfo := range mutation.Fields() {
		if _, ok := byName[fieldInfo.Name]; ok {
			continue
		}
		if !fieldInfo.IsRequired() {
			continue
		}
		if fieldInfo.SequenceName() != "" || mutation.FieldHasDefaultInsertExpr(fieldInfo.Name) {
			continue
		}
		if fd := info.Definition().Fields.ForName(fieldInfo.Name); fd != nil &&
			fd.Directives.ForName(base.FieldDefaultDirectiveName) != nil {
			continue
		}
		return nil, fmt.Errorf("field %q is required for ingest into %q", fieldInfo.Name, info.Name)
	}

	columns := make([]ingestColumn, 0, len(byName))
	for _, af := range schema.Fields() {
		columns = append(columns, byName[af.Name])
	}
	return columns, nil
}

func checkIngestPermissions(ctx context.Context, provider catalog.Provider, info *sdl.Object, mutationField *ast.FieldDefinition, columns []ingestColumn) (map[string]any, error) {
	if auth.IsFullAccess(ctx) {
		return nil, nil
	}
	rp := perm.PermissionsFromCtx(ctx)
	if rp == nil {
		return nil, nil
	}
	if rp.Disabled {
		return nil, auth.ErrForbidden
	}

	parent := sdl.ModuleTypeName(sdl.ObjectModule(info.Definition()), sdl.ModuleMutation)
	if _, ok := rp.Enabled(parent, mutationField.Name); !ok {
		return nil, auth.ErrForbidden
	}

	data := make(map[string]any, len(columns))
	for _, c := range columns {
		data[c.InputDef.Name] = nil
	}
	var permissionData map[string]any
	if arg := rp.DataArgument(ctx, parent, mutationField.Name); arg != nil {
		values, err := sdl.ParseDataAsInputObject(ctx, provider, &ast.Type{
			NamedType: info.InputInsertDataName(),
			Position:  base.CompiledPos("ingest permission data"),
		}, arg, false)
		if err != nil {
			return nil, err
		}
		if values != nil {
			permissionData = values.(map[string]any)
			for k, v := range permissionData {
				data[k] = v
			}
		}
	}
	if err := rp.CheckMutationInput(ctx, provider, info.InputInsertDataName(), data); err != nil {
		return nil, err
	}
	return permissionData, nil
}

func ingestNode(ctx context.Context, info *sdl.Object, mutation *sdl.Mutation, engine engines.Engine, columns []ingestColumn, permissionData map[string]any, state *ingestExecState) *QueryPlanNode {
	needsSpatial := ingestNeedsSpatial(columns)
	return &QueryPlanNode{
		Name: "ingest_" + info.Name,
		Before: func(ctx context.Context, pool *db.Pool, node *QueryPlanNode) error {
			ar, err := pool.Arrow(ctx)
			if err != nil {
				return fmt.Errorf("acquire duckdb arrow conn: %w", err)
			}
			if needsSpatial {
				if _, err := ar.Exec(ctx, "LOAD spatial; CALL register_geoarrow_extensions()"); err != nil {
					_ = ar.Close()
					return fmt.Errorf("prepare spatial arrow ingest: %w", err)
				}
			}
			release, err := ar.RegisterView(state.reader, state.view)
			if err != nil {
				_ = ar.Close()
				return fmt.Errorf("register arrow view: %w", err)
			}
			state.arrow = ar
			state.release = release
			node.plan.exec = func(ctx context.Context, query string, args ...any) (sql.Result, error) {
				if len(args) != 0 {
					return nil, fmt.Errorf("ingest execution does not support SQL parameters")
				}
				res, err := ar.Exec(ctx, query)
				if err != nil {
					return nil, err
				}
				return res, nil
			}
			return nil
		},
		After: func(ctx context.Context, pool *db.Pool, node *QueryPlanNode) error {
			return state.cancel()
		},
		CollectFunc: func(node *QueryPlanNode, children Results, params []any) (string, []any, error) {
			fieldValues := make(map[string]string, len(columns))
			for _, c := range columns {
				value := engines.Ident(c.ArrowField.Name)
				field := &ast.Field{
					Name:             c.Field.Name,
					Alias:            c.Field.Name,
					Definition:       c.FieldDef,
					ObjectDefinition: info.Definition(),
				}
				var err error
				if caster, ok := engine.(engines.EngineTypeCaster); ok {
					value, err = caster.CastArrowIngestValue(field, c.ArrowField, value)
				} else {
					value, err = engines.CastArrowIngestValueToDuckDB(field, c.ArrowField, value)
				}
				if err != nil {
					return "", nil, err
				}
				fieldValues[c.Field.Name] = value
			}
			for name, value := range permissionData {
				fieldInfo := info.FieldForName(name)
				if fieldInfo == nil {
					return "", nil, fmt.Errorf("permission data field %q is not defined in data object %q", name, info.Name)
				}
				if fieldInfo.IsReferencesSubquery() || fieldInfo.IsNotDBField() {
					return "", nil, fmt.Errorf("permission data field %q cannot be ingested directly", name)
				}
				sqlValue, err := engine.SQLValue(value)
				if err != nil {
					return "", nil, err
				}
				fieldValues[name] = sqlValue
			}
			if err := mutation.AppendInsertSQLExpression(fieldValues, perm.AuthVars(ctx), engine); err != nil {
				return "", nil, err
			}

			var targetFields, selectExprs []string
			for _, c := range columns {
				targetFields = append(targetFields, c.Field.FieldSourceName("", true))
				selectExprs = append(selectExprs, fieldValues[c.Field.Name])
				delete(fieldValues, c.Field.Name)
			}
			for _, fieldInfo := range mutation.Fields() {
				expr, ok := fieldValues[fieldInfo.Name]
				if !ok {
					continue
				}
				if fieldInfo.FieldSourceName("", false) == "-" {
					continue
				}
				targetFields = append(targetFields, fieldInfo.FieldSourceName("", true))
				selectExprs = append(selectExprs, expr)
				delete(fieldValues, fieldInfo.Name)
			}
			if len(targetFields) == 0 {
				return "", nil, fmt.Errorf("no values provided for ingest")
			}

			target := info.SQL(ctx, engines.Ident(info.Catalog))
			return fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s",
				target,
				strings.Join(targetFields, ", "),
				strings.Join(selectExprs, ", "),
				engines.Ident(state.view),
			), params, nil
		},
	}
}

func ingestNeedsSpatial(columns []ingestColumn) bool {
	for _, c := range columns {
		if c.FieldDef != nil && c.FieldDef.Type.Name() == base.GeometryTypeName {
			return true
		}
	}
	return false
}

func (s *ingestExecState) cancel() error {
	s.once.Do(func() {
		if s.release != nil {
			s.release()
		}
		if s.arrow != nil {
			s.err = s.arrow.Close()
		}
	})
	if errors.Is(s.err, sql.ErrConnDone) {
		return nil
	}
	return s.err
}
