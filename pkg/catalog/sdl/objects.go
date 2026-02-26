package sdl

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	TableDataObject = base.TableDataObject
	ViewDataObject  = base.ViewDataObject
)

type Object struct {
	Name                string
	sql                 string
	Type                string
	Catalog             string
	SoftDelete          bool
	IsM2M               bool
	softDeleteCondition string
	softDeleteSet       string
	IsCube              bool
	IsHypertable        bool

	inputFilterName     string
	inputFilterListName string

	InputArgsName string
	RequiredArgs  bool
	HasVectors    bool
	functionCall  bool

	def *ast.Definition
}

func DataObjectInfo(def *ast.Definition) *Object {
	if !IsDataObject(def) {
		return nil
	}

	info := Object{
		Catalog: CatalogName(def),
		def:     def,
	}

	if def.Directives.ForName(base.ObjectTableDirectiveName) != nil {
		info.Name = base.DefinitionDirectiveArgString(def, base.ObjectTableDirectiveName, base.ArgName)
		info.SoftDelete = base.DefinitionDirectiveArgString(def, base.ObjectTableDirectiveName, base.ArgSoftDelete) == "true"
		info.softDeleteCondition = base.DefinitionDirectiveArgString(def, base.ObjectTableDirectiveName, base.ArgSoftDeleteCond)
		info.softDeleteSet = base.DefinitionDirectiveArgString(def, base.ObjectTableDirectiveName, base.ArgSoftDeleteSet)
		info.IsM2M = base.DefinitionDirectiveArgString(def, base.ObjectTableDirectiveName, base.ArgIsM2M) == "true"
		info.Type = TableDataObject
	}

	if def.Directives.ForName(base.ObjectViewDirectiveName) != nil {
		info.Name = base.DefinitionDirectiveArgString(def, base.ObjectViewDirectiveName, base.ArgName)
		info.sql = base.DefinitionDirectiveArgString(def, base.ObjectViewDirectiveName, base.ArgSQL)
		info.Type = ViewDataObject
	}

	info.inputFilterName = base.DefinitionDirectiveArgString(def, base.FilterInputDirectiveName, base.ArgName)
	info.inputFilterListName = base.DefinitionDirectiveArgString(def, base.FilterListInputDirectiveName, base.ArgName)
	info.IsCube = def.Directives.ForName(base.ObjectCubeDirectiveName) != nil
	info.IsHypertable = def.Directives.ForName(base.ObjectHyperTableDirectiveName) != nil
	info.InputArgsName = base.DefinitionDirectiveArgString(def, base.ViewArgsDirectiveName, base.ArgName)
	info.RequiredArgs = base.DefinitionDirectiveArgString(def, base.ViewArgsDirectiveName, base.ArgRequired) == "true"
	info.HasVectors = IsVectorSearchable(def)

	return &info
}

func CatalogName(def *ast.Definition) string {
	return base.DefinitionCatalog(def)
}

func ObjectSQL(def *ast.Definition) string {
	name := base.DefinitionDirectiveArgString(def, base.ObjectTableDirectiveName, base.ArgName)
	if name != "" {
		return name
	}
	name = base.DefinitionDirectiveArgString(def, base.ObjectViewDirectiveName, base.ArgName)
	sql := base.DefinitionDirectiveArgString(def, base.ObjectViewDirectiveName, base.ArgSQL)
	if sql != "" {
		return fmt.Sprintf("(%s) AS %s", sql, name)
	}
	return name
}

func (info *Object) SoftDeleteCondition(prefix string) string {
	if prefix != "" {
		prefix += "."
	}
	sql := info.softDeleteCondition
	for _, f := range ExtractFieldsFromSQL(info.softDeleteCondition) {
		sql = strings.ReplaceAll(sql, "["+f+"]", prefix+f)
	}
	return sql
}

func (info *Object) SoftDeleteSet(prefix string) string {
	if prefix != "" {
		prefix += "."
	}
	sql := info.softDeleteSet
	for _, f := range ExtractFieldsFromSQL(info.softDeleteSet) {
		sql = strings.ReplaceAll(sql, "["+f+"]", prefix+f)
	}
	return sql
}

func (info *Object) HasArguments() bool {
	return info.InputArgsName != ""
}

// Type aliases re-exported from base.
type ObjectQueryType = base.ObjectQueryType

const (
	QueryTypeSelect                      = base.QueryTypeSelect
	QueryTypeSelectOne                   = base.QueryTypeSelectOne
	QueryTypeAggregate                   = base.QueryTypeAggregate
	QueryTypeAggregateBucket             = base.QueryTypeAggregateBucket
	SubQueryTypeReferencesData           = base.SubQueryTypeReferencesData
	SubQueryTypeJoinData                 = base.SubQueryTypeJoinData
	SubQueryTypeFunctionCallData         = base.SubQueryTypeFunctionCallData
	SubQueryTypeFunctionCallTableJoinData = base.SubQueryTypeFunctionCallTableJoinData
)

type ObjectQuery struct {
	Name string
	Type ObjectQueryType
}

func (info *Object) Queries() (queries []ObjectQuery) {
	for _, d := range info.def.Directives.ForNames(base.QueryDirectiveName) {
		name := base.DirectiveArgString(d, base.ArgName)
		switch base.DirectiveArgString(d, base.ArgType) {
		case base.QueryTypeTextSelect:
			queries = append(queries, ObjectQuery{Name: name, Type: QueryTypeSelect})
		case base.QueryTypeTextSelectOne:
			queries = append(queries, ObjectQuery{Name: name, Type: QueryTypeSelectOne})
		case base.QueryTypeTextAggregate:
			queries = append(queries, ObjectQuery{Name: name, Type: QueryTypeAggregate})
		case base.QueryTypeTextAggregateBucket:
			queries = append(queries, ObjectQuery{Name: name, Type: QueryTypeAggregateBucket})
		}
	}
	return queries
}

type sqlBuilder interface {
	SQLValue(any) (string, error)
	FunctionCall(name string, positional []any, named map[string]any) (string, error)
}

func (info *Object) ApplyArguments(ctx context.Context, defs base.DefinitionsSource, args map[string]any, builder sqlBuilder) (err error) {
	if !info.HasArguments() || len(args) == 0 {
		return nil
	}
	it := defs.ForName(ctx, info.InputArgsName)
	if it == nil {
		return ErrorPosf(info.def.Position, "input object %s not found", info.InputArgsName)
	}

	var posArgs []any
	namedArgs := make(map[string]any)

	for _, field := range it.Fields {
		val := args[field.Name]
		if val == nil && field.Type.NonNull {
			return ErrorPosf(field.Position, "argument %s is required", field.Name)
		}
		if info.sql != "" {
			sv, err := builder.SQLValue(val)
			if err != nil {
				return ErrorPosf(field.Position, "wrong argument %s value: %s", field.Name, err.Error())
			}
			info.sql = strings.ReplaceAll(info.sql, "["+field.Name+"]", sv)
			continue
		}
		if d := field.Directives.ForName(base.InputFieldNamedArgDirectiveName); d != nil {
			name := field.Name
			if fn := base.DirectiveArgString(d, base.ArgName); fn != "" {
				name = fn
			}
			namedArgs[name] = val
			continue
		}
		posArgs = append(posArgs, val)
	}
	if info.sql != "" {
		return nil
	}
	info.sql, err = builder.FunctionCall(info.Name, posArgs, namedArgs)
	info.functionCall = true
	return err
}

func (info *Object) SQL(ctx context.Context, prefix string) string {
	if prefix != "" {
		prefix += "."
	}

	if info.sql != "" {
		if info.functionCall {
			return info.sql
		}
		sql := info.sql
		if !strings.HasPrefix(sql, "(") || !strings.HasSuffix(sql, ")") {
			sql = "(" + sql + ")"
		}
		for _, dbObject := range ExtractFieldsFromSQL(sql) {
			if dbObject == base.CatalogSystemVariableName {
				sql = strings.ReplaceAll(sql, "["+dbObject+"]", info.Catalog)
			}
			sql = strings.ReplaceAll(sql, "["+dbObject+"]", prefix+dbObject)
		}
		return sql
	}

	return prefix + info.Name
}

func (info *Object) AggregationTypeName(ctx context.Context, defs base.DefinitionsSource) string {
	name := buildObjectAggregationTypeName(info.def.Name, false, false)
	if defs.ForName(ctx, name) == nil {
		return ""
	}
	return name
}

func (info *Object) BucketAggregationTypeName(ctx context.Context, defs base.DefinitionsSource) string {
	name := buildObjectAggregationTypeName(info.def.Name, false, true)
	if defs.ForName(ctx, name) == nil {
		return ""
	}
	return name
}

func (info *Object) SubAggregationTypeName(ctx context.Context, defs base.DefinitionsSource) string {
	name := buildObjectAggregationTypeName(info.def.Name, true, false)
	if defs.ForName(ctx, name) == nil {
		return ""
	}
	return name
}

func (info *Object) InputFilterName() string {
	return base.DefinitionDirectiveArgString(info.def, base.FilterInputDirectiveName, base.ArgName)
}

type ObjectMutationType = base.ObjectMutationType

const (
	MutationTypeInsert = base.MutationTypeInsert
	MutationTypeUpdate = base.MutationTypeUpdate
	MutationTypeDelete = base.MutationTypeDelete
)

func (info *Object) InputInsertDataName() string {
	for _, d := range info.def.Directives.ForNames(base.MutationDirectiveName) {
		if base.DirectiveArgString(d, base.ArgType) == base.MutationTypeTextInsert {
			return base.DirectiveArgString(d, base.DataInputDirectiveName)
		}
	}
	return ""
}

func (info *Object) InsertMutationName() string {
	for _, d := range info.def.Directives.ForNames(base.MutationDirectiveName) {
		if base.DirectiveArgString(d, base.ArgType) == base.MutationTypeTextInsert {
			return base.DirectiveArgString(d, base.ArgName)
		}
	}
	return ""
}

func (info *Object) InputUpdateDataName() string {
	for _, d := range info.def.Directives.ForNames(base.MutationDirectiveName) {
		if base.DirectiveArgString(d, base.ArgType) == base.MutationTypeTextUpdate {
			return base.DirectiveArgString(d, base.DataInputDirectiveName)
		}
	}
	return ""
}

func (info *Object) UpdateMutationName() string {
	for _, d := range info.def.Directives.ForNames(base.MutationDirectiveName) {
		if base.DirectiveArgString(d, base.ArgType) == base.MutationTypeTextUpdate {
			return base.DirectiveArgString(d, base.ArgName)
		}
	}
	return ""
}

func (info *Object) DeleteMutationName() string {
	for _, d := range info.def.Directives.ForNames(base.MutationDirectiveName) {
		if base.DirectiveArgString(d, base.ArgType) == base.MutationTypeTextDelete {
			return base.DirectiveArgString(d, base.ArgName)
		}
	}
	return ""
}

func (info *Object) FieldForName(name string) *Field {
	for _, field := range info.def.Fields {
		if field.Name == name {
			return FieldDefinitionInfo(field, info.def)
		}
	}
	return nil
}

func (info *Object) Definition() *ast.Definition {
	return info.def
}

func (info *Object) ReferencesQueryInfo(ctx context.Context, defs base.DefinitionsSource, name string) *References {
	field := info.FieldForName(name)
	if field == nil {
		return nil
	}
	refName := base.FieldDefDirectiveArgString(field.def, base.FieldReferencesQueryDirectiveName, base.ArgName)
	def := info.def
	for _, ref := range def.Directives.ForNames(base.ReferencesDirectiveName) {
		if ri := ReferencesInfo(ref); ri.Name == refName {
			if ri.Query != name && ri.ReferencesName == info.def.Name {
				return referencesInfo(ref, def.Name, true)
			}
			return referencesInfo(ref, def.Name, false)
		}
	}
	refObject := base.FieldDefDirectiveArgString(field.def, base.FieldReferencesQueryDirectiveName, base.ArgReferencesName)
	def = defs.ForName(ctx, refObject)
	if def == nil {
		return nil
	}
	for _, ref := range def.Directives.ForNames(base.ReferencesDirectiveName) {
		if ReferencesInfo(ref).Name == refName {
			return referencesInfo(ref, def.Name, true)
		}
	}
	return nil
}

func (info *Object) ReferencesQueryInfoByName(ctx context.Context, defs base.DefinitionsSource, name string) *References {
	for _, ref := range info.def.Directives.ForNames(base.ReferencesDirectiveName) {
		if ReferencesInfo(ref).Name == name {
			return referencesInfo(ref, info.def.Name, false)
		}
	}
	return nil
}

func (info *Object) M2MReferencesQueryInfo(ctx context.Context, defs base.DefinitionsSource, name string) *References {
	for _, ref := range info.def.Directives.ForNames(base.ReferencesDirectiveName) {
		if ReferencesInfo(ref).Name != name {
			return referencesInfo(ref, info.def.Name, false)
		}
	}
	return nil
}

func ObjectPrimaryKeys(def *ast.Definition) []string {
	var fields []string
	for _, f := range def.Fields {
		if f.Directives.ForName(base.FieldPrimaryKeyDirectiveName) != nil {
			fields = append(fields, f.Name)
		}
	}
	return fields
}
