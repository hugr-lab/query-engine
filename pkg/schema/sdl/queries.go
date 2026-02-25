package sdl

import (
	"strings"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// Type aliases re-exported from base.
type QueryRequest = base.QueryRequest
type QueryType = base.QueryType

const (
	QueryTypeNone             = base.QueryTypeNone
	QueryTypeMeta             = base.QueryTypeMeta
	QueryTypeQuery            = base.QueryTypeQuery
	QueryTypeJQTransform      = base.QueryTypeJQTransform
	QueryTypeMutation         = base.QueryTypeMutation
	QueryTypeFunction         = base.QueryTypeFunction
	QueryTypeFunctionMutation = base.QueryTypeFunctionMutation
	QueryTypeH3Aggregation    = base.QueryTypeH3Aggregation
)

const (
	MetadataSchemaQuery   = base.MetadataSchemaQuery
	MetadataTypeQuery     = base.MetadataTypeQuery
	MetadataTypeNameQuery = base.MetadataTypeNameQuery
	JQTransformQueryName  = base.JQTransformQueryName
)

func QueryRequestInfo(ss ast.SelectionSet) ([]QueryRequest, QueryType) {
	resolvers := make([]QueryRequest, 0)
	qtt := QueryTypeNone
	for _, sel := range ss {
		if fragment, ok := sel.(*ast.FragmentSpread); ok {
			rr, qt := QueryRequestInfo(fragment.Definition.SelectionSet)
			resolvers = append(resolvers, rr...)
			qtt |= qt
			continue
		}
		if _, ok := sel.(*ast.InlineFragment); ok {
			continue
		}
		field, ok := sel.(*ast.Field)
		if !ok {
			continue
		}
		if field.Name == MetadataSchemaQuery ||
			field.Name == MetadataTypeQuery ||
			field.Name == MetadataTypeNameQuery {
			resolvers = append(resolvers, QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: QueryTypeMeta,
			})
			qtt |= QueryTypeMeta
			continue
		}
		if field.Directives.ForName("skip") != nil {
			continue
		}
		if field.ObjectDefinition == nil {
			continue
		}
		if field.ObjectDefinition.Kind != ast.Object {
			continue
		}
		fd := field.ObjectDefinition.Fields.ForName(field.Name)
		if fd == nil {
			continue
		}
		info := ModuleRootInfo(field.ObjectDefinition)
		if info == nil {
			continue
		}
		switch {
		case info.Type == ModuleQuery && fd.Directives.ForName(base.QueryDirectiveName) != nil ||
			fd.Directives.ForName(base.FieldAggregationQueryDirectiveName) != nil:
			resolvers = append(resolvers, QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: QueryTypeQuery,
			})
			qtt |= QueryTypeQuery
		case info.Type == ModuleMutation && fd.Directives.ForName(base.MutationDirectiveName) != nil:
			resolvers = append(resolvers, QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: QueryTypeMutation,
			})
			qtt |= QueryTypeMutation
		case info.Type == ModuleFunction && fd.Directives.ForName(base.FunctionDirectiveName) != nil:
			resolvers = append(resolvers, QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: QueryTypeFunction,
			})
			qtt |= QueryTypeFunction
		case info.Type == ModuleMutationFunction && fd.Directives.ForName(base.FunctionDirectiveName) != nil:
			resolvers = append(resolvers, QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: QueryTypeFunctionMutation,
			})
			qtt |= QueryTypeFunctionMutation
		case field.Name == JQTransformQueryName:
			resolvers = append(resolvers, QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: QueryTypeJQTransform,
			})
			qtt |= QueryTypeJQTransform
		case field.Name == base.H3QueryFieldName:
			resolvers = append(resolvers, QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: QueryTypeH3Aggregation,
			})
			qtt |= QueryTypeH3Aggregation
		default:
			rr, qt := QueryRequestInfo(field.SelectionSet)
			resolvers = append(resolvers, QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: QueryTypeNone,
				Subset:    rr,
			})
			qtt |= qt
		}
	}
	return resolvers, qtt
}

func FlatQuery(queries []QueryRequest) map[string]QueryRequest {
	if len(queries) == 0 {
		return nil
	}
	flat := make(map[string]QueryRequest, len(queries))
	for _, q := range queries {
		switch q.QueryType {
		case QueryTypeNone:
			for k, v := range FlatQuery(q.Subset) {
				flat[q.Field.Alias+"."+k] = v
			}
		case QueryTypeQuery,
			QueryTypeFunction,
			QueryTypeFunctionMutation,
			QueryTypeMutation,
			QueryTypeH3Aggregation,
			QueryTypeMeta,
			QueryTypeJQTransform:
			flat[q.Field.Alias] = q
		}
	}
	return flat
}

func ObjectQueryDefinition(defs Definitions, def *ast.Definition, queryType ObjectQueryType) (string, *ast.FieldDefinition) {
	if def == nil {
		return "", nil
	}
	qt := queryTypeToText(queryType)
	for _, d := range def.Directives.ForNames(base.QueryDirectiveName) {
		if directiveArgValue(d, "type") != qt {
			continue
		}
		qn := directiveArgValue(d, "name")
		if qn == "" {
			return "", nil
		}
		if queryType == QueryTypeSelectOne && !strings.HasSuffix(qn, base.ObjectQueryByPKSuffix) {
			continue
		}
		module := defs.ForName(ModuleTypeName(ObjectModule(def), ModuleQuery))
		return ObjectModule(def), module.Fields.ForName(qn)
	}
	return "", nil
}

func ObjectMutationDefinition(defs Definitions, def *ast.Definition, mutationType ObjectMutationType) (string, *ast.FieldDefinition) {
	if def == nil {
		return "", nil
	}
	mt := mutationTypeToText(mutationType)
	for _, d := range def.Directives.ForNames(base.MutationDirectiveName) {
		if directiveArgValue(d, "type") != mt {
			continue
		}
		mn := directiveArgValue(d, "name")
		if mn == "" {
			return "", nil
		}
		module := defs.ForName(ModuleTypeName(ObjectModule(def), ModuleMutation))
		return ObjectModule(def), module.Fields.ForName(mn)
	}
	return "", nil
}

func queryTypeToText(queryType ObjectQueryType) string {
	switch queryType {
	case QueryTypeSelect:
		return base.QueryTypeTextSelect
	case QueryTypeSelectOne:
		return base.QueryTypeTextSelectOne
	case QueryTypeAggregate:
		return base.QueryTypeTextAggregate
	case QueryTypeAggregateBucket:
		return base.QueryTypeTextAggregateBucket
	}
	return ""
}

func mutationTypeToText(mutationType ObjectMutationType) string {
	switch mutationType {
	case MutationTypeInsert:
		return base.MutationTypeTextInsert
	case MutationTypeUpdate:
		return base.MutationTypeTextUpdate
	case MutationTypeDelete:
		return base.MutationTypeTextDelete
	}
	return ""
}

type Mutation struct {
	ObjectName       string
	Catalog          string
	Type             ObjectMutationType
	ObjectDefinition *ast.Definition

	query *ast.FieldDefinition
	defs  Definitions
}

func MutationInfo(defs Definitions, query *ast.FieldDefinition) *Mutation {
	if query == nil {
		return nil
	}
	d := query.Directives.ForName(base.MutationDirectiveName)
	if d == nil {
		return nil
	}
	m := Mutation{
		ObjectName: directiveArgValue(d, "name"),
		Catalog:    fieldDirectiveArgValue(query, base.CatalogDirectiveName, "name"),
		query:      query,
		defs:       defs,
	}
	if m.ObjectName == "" {
		return nil
	}
	t := directiveArgValue(d, "type")
	if t == "" {
		return nil
	}
	switch t {
	case base.MutationTypeTextInsert:
		m.Type = MutationTypeInsert
	case base.MutationTypeTextUpdate:
		m.Type = MutationTypeUpdate
	case base.MutationTypeTextDelete:
		m.Type = MutationTypeDelete
	default:
		return nil
	}
	m.ObjectDefinition = defs.ForName(m.ObjectName)
	if m.ObjectDefinition == nil {
		return nil
	}
	return &m
}

func (m *Mutation) Fields() []*Field {
	var out []*Field
	if m.Type == MutationTypeDelete {
		return out
	}
	arg := m.query.Arguments.ForName("data")
	if arg == nil {
		return out
	}
	dt := m.defs.ForName(arg.Type.Name())
	if dt == nil || IsScalarType(dt.Name) {
		return out
	}
	for _, f := range dt.Fields {
		of := m.ObjectDefinition.Fields.ForName(f.Name)
		if of == nil {
			continue
		}
		fi := FieldDefinitionInfo(of, m.ObjectDefinition)
		if fi.IsNotDBField() {
			continue
		}
		out = append(out, fi)
	}
	return out
}

func (m *Mutation) FieldDefinition(name string) *ast.FieldDefinition {
	return m.ObjectDefinition.Fields.ForName(name)
}

func (m *Mutation) ReferencesFields() []string {
	var out []string
	if m.Type == MutationTypeDelete {
		return out
	}
	arg := m.query.Arguments.ForName("data")
	if arg == nil {
		return out
	}
	dt := m.defs.ForName(arg.Type.Name())
	if dt == nil || IsScalarType(dt.Name) {
		return out
	}
	for _, f := range dt.Fields {
		of := m.ObjectDefinition.Fields.ForName(f.Name)
		if of == nil {
			continue
		}
		fi := FieldDefinitionInfo(of, m.ObjectDefinition)
		if fi.IsReferencesSubquery() {
			out = append(out, fi.Name)
		}
	}
	return out
}

func (m *Mutation) M2MReferencesFields() []string {
	var out []string
	if m.Type == MutationTypeDelete {
		return out
	}
	arg := m.query.Arguments.ForName("data")
	if arg == nil {
		return out
	}
	dt := m.defs.ForName(arg.Type.Name())
	if dt == nil || IsScalarType(dt.Name) {
		return out
	}
	for _, f := range dt.Fields {
		of := m.ObjectDefinition.Fields.ForName(f.Name)
		if of == nil {
			continue
		}
		fi := FieldDefinitionInfo(of, m.ObjectDefinition)
		if !fi.IsReferencesSubquery() {
			continue
		}
		ref := FieldReferencesInfo(m.defs, m.ObjectDefinition, f)
		if ref == nil || !ref.IsM2M {
			continue
		}
		out = append(out, fi.Name)
	}
	return out
}

func (m Mutation) ReferencesFieldsSource(name string) []string {
	f := m.ObjectDefinition.Fields.ForName(name)
	if f == nil {
		return nil
	}
	ref := FieldReferencesInfo(m.defs, m.ObjectDefinition, f)
	if ref == nil {
		return nil
	}
	if ref.isBackRef {
		return ref.referencesFields
	}
	return ref.sourceFields
}

func (m Mutation) ReferencesFieldsReferences(name string) []string {
	f := m.ObjectDefinition.Fields.ForName(name)
	if f == nil {
		return nil
	}
	ref := FieldReferencesInfo(m.defs, m.ObjectDefinition, f)
	if ref == nil {
		return nil
	}
	if ref.isBackRef {
		return ref.sourceFields
	}
	return ref.referencesFields
}

func (m *Mutation) ReferencesMutation(name string) *Mutation {
	if m.Type != MutationTypeInsert {
		return nil
	}
	if m.ObjectName == name {
		return m
	}
	f := m.ObjectDefinition.Fields.ForName(name)
	if f == nil {
		return nil
	}
	ref := FieldReferencesInfo(m.defs, m.ObjectDefinition, f)
	rt := ref.ReferencesObjectDef(m.defs)
	if rt == nil {
		return nil
	}
	moduleObject := m.defs.ForName(ModuleTypeName(ObjectModule(rt), ModuleMutation))
	if moduleObject == nil {
		return nil
	}
	for _, d := range rt.Directives.ForNames(base.MutationDirectiveName) {
		t := directiveArgValue(d, "type")
		if t != base.MutationTypeTextInsert {
			continue
		}
		mn := objectDirectiveArgValue(rt, base.MutationDirectiveName, "name")
		if mn == "" {
			return nil
		}
		return MutationInfo(m.defs, moduleObject.Fields.ForName(mn))
	}
	return nil
}

func (m *Mutation) DefaultSequencesValues() map[string]string {
	if m.Type != MutationTypeInsert {
		return nil
	}
	sequencesValues := make(map[string]string)
	for _, field := range m.ObjectDefinition.Fields {
		if field.Directives.ForName(base.FieldDefaultDirectiveName) == nil {
			continue
		}
		sequence := fieldDirectiveArgValue(field, base.FieldDefaultDirectiveName, "sequence")
		if sequence == "" {
			continue
		}
		sequencesValues[field.Name] = sequence
	}
	return sequencesValues
}

func (m *Mutation) FieldHasDefaultInsertExpr(name string) bool {
	field := m.ObjectDefinition.Fields.ForName(name)
	if field == nil {
		return false
	}
	d := field.Directives.ForName(base.FieldDefaultDirectiveName)
	if d == nil {
		return false
	}
	return d.Arguments.ForName(base.FieldDefaultDirectiveInsertExprArgName) != nil
}

func (m *Mutation) FieldHasDefaultUpdateExpr(name string) bool {
	field := m.ObjectDefinition.Fields.ForName(name)
	if field == nil {
		return false
	}
	d := field.Directives.ForName(base.FieldDefaultDirectiveName)
	if d == nil {
		return false
	}
	return d.Arguments.ForName(base.FieldDefaultDirectiveUpdateExprArgName) != nil
}

func (m *Mutation) AppendInsertSQLExpression(data map[string]string, vars map[string]any, builder sqlBuilder) error {
	for _, field := range m.Fields() {
		if !m.FieldHasDefaultInsertExpr(field.Name) {
			continue
		}
		if field.def == nil {
			return ErrorPosf(m.query.Position, "field %s definition not found", field.Name)
		}
		sql := fieldDirectiveArgValue(field.def, base.FieldDefaultDirectiveName, base.FieldDefaultDirectiveInsertExprArgName)
		if sql == "" {
			continue
		}
		sql, err := applySQLVars(sql, data, vars, builder)
		if err != nil {
			return err
		}
		data[field.Name] = sql
	}
	return nil
}

func (m *Mutation) AppendUpdateSQLExpression(data map[string]string, vars map[string]any, builder sqlBuilder) error {
	for _, field := range m.Fields() {
		if !m.FieldHasDefaultUpdateExpr(field.Name) {
			continue
		}
		if field.def == nil {
			return ErrorPosf(m.query.Position, "field %s definition not found", field.Name)
		}
		sql := fieldDirectiveArgValue(field.def, base.FieldDefaultDirectiveName, base.FieldDefaultDirectiveUpdateExprArgName)
		if sql == "" {
			continue
		}
		sql, err := applySQLVars(sql, data, vars, builder)
		if err != nil {
			return err
		}
		data[field.Name] = sql
	}
	return nil
}

func applySQLVars(sql string, data map[string]string, vars map[string]any, builder sqlBuilder) (string, error) {
	for _, f := range ExtractFieldsFromSQL(sql) {
		if !strings.HasPrefix(f, "$") {
			continue
		}
		if v, ok := vars[f]; ok {
			sv, err := builder.SQLValue(v)
			if err != nil {
				return "", err
			}
			sql = strings.ReplaceAll(sql, "["+f+"]", sv)
			continue
		}
		if v, ok := data[strings.TrimPrefix(f, "$")]; ok {
			sql = strings.ReplaceAll(sql, "["+f+"]", v)
			continue
		}
		sql = strings.ReplaceAll(sql, "["+f+"]", "NULL")
	}
	return sql, nil
}

func (m *Mutation) DBFieldName(name string) string {
	field := m.ObjectDefinition.Fields.ForName(name)
	if field == nil {
		return ""
	}
	if d := field.Directives.ForName(base.FieldSourceDirectiveName); d != nil {
		return directiveArgValue(d, "field")
	}
	return field.Name
}

func (m *Mutation) SelectByPKQuery(query *ast.Field) *ast.Field {
	qm := m.defs.ForName(ModuleTypeName(ObjectModule(m.ObjectDefinition), ModuleQuery))
	if qm == nil {
		return nil
	}
	var qn string
	for _, d := range m.ObjectDefinition.Directives.ForNames(base.QueryDirectiveName) {
		if directiveArgValue(d, "type") != base.QueryTypeTextSelectOne {
			continue
		}
		name := directiveArgValue(d, "name")
		if name == "" || !strings.HasSuffix(name, base.ObjectQueryByPKSuffix) {
			continue
		}
		qn = name
		break
	}
	if qn == "" {
		return nil
	}
	qd := qm.Fields.ForName(qn)
	if qd == nil {
		return nil
	}
	return &ast.Field{
		Alias:            query.Alias,
		Name:             qd.Name,
		SelectionSet:     query.SelectionSet,
		Definition:       qd,
		ObjectDefinition: qm,
		Position:         query.Position,
	}
}

type FieldQueryArgument struct {
	Name  string
	Type  *ast.Type
	Value any
}

type FieldQueryArguments []FieldQueryArgument

func (a FieldQueryArguments) ForName(name string) *FieldQueryArgument {
	for _, arg := range a {
		if arg.Name == name {
			return &arg
		}
	}
	return nil
}

func ArgumentValues(defs Definitions, field *ast.Field, vars map[string]any, checkRequired bool) (FieldQueryArguments, error) {
	args := make([]FieldQueryArgument, 0, len(field.Arguments))
	for _, arg := range field.Arguments {
		def := field.Definition.Arguments.ForName(arg.Name)
		if def == nil {
			return nil, ErrorPosf(arg.Position, "unknown argument %s", arg.Name)
		}
		value := arg.Value
		if value == nil {
			value = def.DefaultValue
		}
		v, err := ParseArgumentValue(defs, def, value, vars, checkRequired)
		if err != nil {
			return nil, err
		}
		if v == nil {
			continue
		}
		args = append(args, FieldQueryArgument{
			Name:  arg.Name,
			Type:  def.Type,
			Value: v,
		})
	}
	return args, nil
}
