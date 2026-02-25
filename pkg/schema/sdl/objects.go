package sdl

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

const (
	TableDataObject = "table"
	ViewDataObject  = "view"
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
		info.Name = objectDirectiveArgValue(def, base.ObjectTableDirectiveName, "name")
		info.SoftDelete = objectDirectiveArgValue(def, base.ObjectTableDirectiveName, "soft_delete") == "true"
		info.softDeleteCondition = objectDirectiveArgValue(def, base.ObjectTableDirectiveName, "soft_delete_cond")
		info.softDeleteSet = objectDirectiveArgValue(def, base.ObjectTableDirectiveName, "soft_delete_set")
		info.IsM2M = objectDirectiveArgValue(def, base.ObjectTableDirectiveName, "is_m2m") == "true"
		info.Type = TableDataObject
	}

	if def.Directives.ForName(base.ObjectViewDirectiveName) != nil {
		info.Name = objectDirectiveArgValue(def, base.ObjectViewDirectiveName, "name")
		info.sql = objectDirectiveArgValue(def, base.ObjectViewDirectiveName, "sql")
		info.Type = ViewDataObject
	}

	info.inputFilterName = objectDirectiveArgValue(def, base.FilterInputDirectiveName, "name")
	info.inputFilterListName = objectDirectiveArgValue(def, base.FilterListInputDirectiveName, "name")
	info.IsCube = def.Directives.ForName(base.ObjectCubeDirectiveName) != nil
	info.IsHypertable = def.Directives.ForName(base.ObjectHyperTableDirectiveName) != nil
	info.InputArgsName = objectDirectiveArgValue(def, base.ViewArgsDirectiveName, "name")
	info.RequiredArgs = objectDirectiveArgValue(def, base.ViewArgsDirectiveName, "required") == "true"
	info.HasVectors = IsVectorSearchable(def)

	return &info
}

func CatalogName(def *ast.Definition) string {
	return base.DefinitionCatalog(def)
}

func ObjectSQL(def *ast.Definition) string {
	name := objectDirectiveArgValue(def, base.ObjectTableDirectiveName, "name")
	if name != "" {
		return name
	}
	name = objectDirectiveArgValue(def, base.ObjectViewDirectiveName, "name")
	sql := objectDirectiveArgValue(def, base.ObjectViewDirectiveName, "sql")
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

type ObjectQueryType int

const (
	QueryTypeSelect ObjectQueryType = iota
	QueryTypeSelectOne
	QueryTypeAggregate
	QueryTypeAggregateBucket

	SubQueryTypeReferencesData
	SubQueryTypeJoinData
	SubQueryTypeFunctionCallData
	SubQueryTypeFunctionCallTableJoinData
)

type ObjectQuery struct {
	Name string
	Type ObjectQueryType
}

func (info *Object) Queries() (queries []ObjectQuery) {
	for _, d := range info.def.Directives.ForNames(base.QueryDirectiveName) {
		name := directiveArgValue(d, "name")
		switch directiveArgValue(d, "type") {
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

// Definitions is a local alias for types that need to resolve definitions
// without requiring full context.Context (e.g. methods on Object, Mutation, etc.).
// Callers construct this via DefinitionsAdapter.
type Definitions interface {
	ForName(name string) *ast.Definition
}

func (info *Object) ApplyArguments(defs Definitions, args map[string]any, builder sqlBuilder) (err error) {
	if !info.HasArguments() || len(args) == 0 {
		return nil
	}
	it := defs.ForName(info.InputArgsName)
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
			if fn := directiveArgValue(d, "name"); fn != "" {
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

func (info *Object) AggregationTypeName(defs Definitions) string {
	name := buildObjectAggregationTypeName(info.def.Name, false, false)
	if defs.ForName(name) == nil {
		return ""
	}
	return name
}

func (info *Object) BucketAggregationTypeName(defs Definitions) string {
	name := buildObjectAggregationTypeName(info.def.Name, false, true)
	if defs.ForName(name) == nil {
		return ""
	}
	return name
}

func (info *Object) SubAggregationTypeName(defs Definitions) string {
	name := buildObjectAggregationTypeName(info.def.Name, true, false)
	if defs.ForName(name) == nil {
		return ""
	}
	return name
}

func (info *Object) InputFilterName() string {
	return objectDirectiveArgValue(info.def, base.FilterInputDirectiveName, "name")
}

type ObjectMutationType int

const (
	MutationTypeInsert ObjectMutationType = iota
	MutationTypeUpdate
	MutationTypeDelete
)

func (info *Object) InputInsertDataName() string {
	for _, d := range info.def.Directives.ForNames(base.MutationDirectiveName) {
		if directiveArgValue(d, "type") == base.MutationTypeTextInsert {
			return directiveArgValue(d, base.DataInputDirectiveName)
		}
	}
	return ""
}

func (info *Object) InsertMutationName() string {
	for _, d := range info.def.Directives.ForNames(base.MutationDirectiveName) {
		if directiveArgValue(d, "type") == base.MutationTypeTextInsert {
			return directiveArgValue(d, "name")
		}
	}
	return ""
}

func (info *Object) InputUpdateDataName() string {
	for _, d := range info.def.Directives.ForNames(base.MutationDirectiveName) {
		if directiveArgValue(d, "type") == base.MutationTypeTextUpdate {
			return directiveArgValue(d, base.DataInputDirectiveName)
		}
	}
	return ""
}

func (info *Object) UpdateMutationName() string {
	for _, d := range info.def.Directives.ForNames(base.MutationDirectiveName) {
		if directiveArgValue(d, "type") == base.MutationTypeTextUpdate {
			return directiveArgValue(d, "name")
		}
	}
	return ""
}

func (info *Object) DeleteMutationName() string {
	for _, d := range info.def.Directives.ForNames(base.MutationDirectiveName) {
		if directiveArgValue(d, "type") == base.MutationTypeTextDelete {
			return directiveArgValue(d, "name")
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

func (info *Object) ReferencesQueryInfo(defs Definitions, name string) *References {
	field := info.FieldForName(name)
	if field == nil {
		return nil
	}
	refName := fieldDirectiveArgValue(field.def, base.FieldReferencesQueryDirectiveName, "name")
	def := info.def
	for _, ref := range def.Directives.ForNames(base.ReferencesDirectiveName) {
		if ri := ReferencesInfo(ref); ri.Name == refName {
			if ri.Query != name && ri.ReferencesName == info.def.Name {
				return referencesInfo(ref, def.Name, true)
			}
			return referencesInfo(ref, def.Name, false)
		}
	}
	refObject := fieldDirectiveArgValue(field.def, base.FieldReferencesQueryDirectiveName, "references_name")
	def = defs.ForName(refObject)
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

func (info *Object) ReferencesQueryInfoByName(defs Definitions, name string) *References {
	for _, ref := range info.def.Directives.ForNames(base.ReferencesDirectiveName) {
		if ReferencesInfo(ref).Name == name {
			return referencesInfo(ref, info.def.Name, false)
		}
	}
	return nil
}

func (info *Object) M2MReferencesQueryInfo(defs Definitions, name string) *References {
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
