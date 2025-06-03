package compiler

import (
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

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

const (
	JoinDirectiveName      = "join"
	queryDirectiveName     = "query"
	queryTypeTextSelect    = "SELECT"
	queryTypeTextSelectOne = "SELECT_ONE"
	mutationDirectiveName  = "mutation"
	mutationTypeTextInsert = "INSERT"
	mutationTypeTextUpdate = "UPDATE"
	mutationTypeTextDelete = "DELETE"
)

func addJoinsFilter(schema *ast.SchemaDocument, def *ast.Definition) {
	for _, field := range def.Fields {
		if field.Directives.ForName(JoinDirectiveName) != nil {
			refObject := schema.Definitions.ForName(field.Type.Name())
			field.Arguments = inputObjectQueryArgs(schema, refObject, true)
		}
	}
}

func objectQueryDirective(name string, queryType ObjectQueryType) *ast.Directive {
	query := queryTypeTextSelect
	if queryType == QueryTypeSelectOne {
		query = queryTypeTextSelectOne
	}
	return &ast.Directive{Name: queryDirectiveName,
		Arguments: []*ast.Argument{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue}, Position: compiledPos()},
			{Name: "type", Value: &ast.Value{Raw: query, Kind: ast.EnumValue}, Position: compiledPos()},
		},
		Position: compiledPos(),
	}
}

type ObjectMutationType int

const (
	MutationTypeInsert ObjectMutationType = iota
	MutationTypeUpdate
	MutationTypeDelete
)

func objectMutationDirective(name string, mutationType ObjectMutationType) *ast.Directive {
	mutation := mutationTypeTextInsert
	if mutationType == MutationTypeUpdate {
		mutation = mutationTypeTextUpdate
	}
	if mutationType == MutationTypeDelete {
		mutation = mutationTypeTextDelete
	}
	return &ast.Directive{Name: mutationDirectiveName,
		Arguments: []*ast.Argument{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue}, Position: compiledPos()},
			{Name: "type", Value: &ast.Value{Raw: mutation, Kind: ast.EnumValue}, Position: compiledPos()},
		},
		Position: compiledPos(),
	}
}

func IsSubQuery(field *ast.FieldDefinition) bool {
	return field.Directives.ForName(JoinDirectiveName) != nil ||
		field.Directives.ForName(fieldReferencesQueryDirectiveName) != nil
}

func validateJoin(defs Definitions, def *ast.Definition, field *ast.FieldDefinition) error {
	j := joinInfoFromDirective(field.Directives.ForName(JoinDirectiveName))
	if j == nil {
		return nil
	}
	j.field = field

	return j.validate(defs, def)
}

type Join struct {
	ReferencesName   string
	sourceFields     []string
	referencesFields []string
	SQL              string
	IsQueryTime      bool

	field *ast.FieldDefinition
}

func JoinInfo(field *ast.Field) *Join {
	d := field.Directives.ForName(JoinDirectiveName)
	if d == nil {
		d = field.Definition.Directives.ForName(JoinDirectiveName)
	}
	if d == nil {
		return nil
	}
	ref := joinInfoFromDirective(d)
	if ref.ReferencesName == "" {
		ref.ReferencesName = field.Definition.Type.Name()
	}
	ref.field = field.Definition
	ref.IsQueryTime = field.Directives.ForName(JoinDirectiveName) != nil

	return ref
}

func (j *Join) Catalog(defs Definitions) string {
	cat := fieldDirectiveArgValue(j.field, base.CatalogDirectiveName, "name")
	if cat != "" {
		return cat
	}

	return objectDirectiveArgValue(
		defs.ForName(j.field.Type.Name()),
		base.CatalogDirectiveName, "name",
	)
}

func joinInfoFromDirective(def *ast.Directive) *Join {
	if def == nil {
		return nil
	}
	return &Join{
		ReferencesName:   directiveArgValue(def, "references_name"),
		sourceFields:     directiveArgChildValues(def, "source_fields"),
		referencesFields: directiveArgChildValues(def, "references_fields"),
		SQL:              directiveArgValue(def, "sql"),
	}
}

const (
	JoinSourceFieldPrefix = "source"
	JoinRefFieldPrefix    = "dest"
)

func (j *Join) SourceFields() ([]string, error) {
	fields := append([]string{}, j.sourceFields...)
	for _, field := range ExtractFieldsFromSQL(j.SQL) {
		parts := strings.SplitN(field, ".", 2)
		if len(parts) != 2 {
			return nil, ErrorPosf(j.field.Position, "invalid field %s in SQL", field)
		}
		if JoinSourceFieldPrefix == parts[0] {
			fields = append(fields, parts[1])
		}
	}
	return RemoveFieldsDuplicates(fields), nil
}

func (j *Join) ReferencesFields() ([]string, error) {
	fields := append([]string{}, j.referencesFields...)
	for _, field := range ExtractFieldsFromSQL(j.SQL) {
		parts := strings.SplitN(field, ".", 2)
		if len(parts) != 2 {
			return nil, ErrorPosf(j.field.Position, "invalid field %s in SQL", field)
		}
		if JoinRefFieldPrefix == parts[0] {
			fields = append(fields, parts[1])
		}
	}
	return RemoveFieldsDuplicates(fields), nil
}

func (j *Join) JoinConditionsTemplate() string {
	conditions := make([]string, 0, len(j.sourceFields))
	for i, sfn := range j.sourceFields {
		conditions = append(conditions, fmt.Sprintf(
			"[%s.%s] = [%s.%s]",
			JoinSourceFieldPrefix, sfn,
			JoinRefFieldPrefix, j.referencesFields[i],
		))
	}
	if j.SQL != "" {
		conditions = append(conditions, j.SQL)
	}
	return strings.Join(conditions, " AND ")
}

func (j *Join) validate(defs Definitions, def *ast.Definition) error {
	if len(j.sourceFields) != len(j.referencesFields) {
		return ErrorPosf(j.field.Position, "source_fields and references_fields must have the same number of fields")
	}
	ro := defs.ForName(j.ReferencesName)
	if ro == nil {
		return ErrorPosf(j.field.Position, "references object %s not found", j.ReferencesName)
	}
	refCatalog := ro.Directives.ForName(base.CatalogDirectiveName)
	if base.FieldCatalogName(j.field) == "" && refCatalog != nil { // add catalog directive to field
		j.field.Directives = append(j.field.Directives, refCatalog)
	}

	for i, sfn := range j.sourceFields {
		sf := objectFieldByPath(defs, def.Name, sfn, false, false)
		if sf == nil {
			return ErrorPosf(j.field.Position, "field %s not found", sfn)
		}
		if sf.Type.NamedType == "" {
			return ErrorPosf(j.field.Position, "field %s must be a scalar type", sfn)
		}
		if !IsScalarType(sf.Type.Name()) {
			return ErrorPosf(j.field.Position, "field %s must be a scalar type", sfn)
		}
		rf := objectFieldByPath(defs, ro.Name, j.referencesFields[i], false, false)
		if rf == nil {
			return ErrorPosf(j.field.Position, "references field %s not found", j.referencesFields[i])
		}

		if !IsEqualTypes(sf.Type, rf.Type) {
			return ErrorPosf(j.field.Position, "field %s and references field %s must have the same type", sfn, j.referencesFields[i])
		}
	}

	ff, err := j.SourceFields()
	if err != nil {
		return err
	}
	err = validateFieldsForSubQuery(defs, def, ff)
	if err != nil {
		return err
	}
	ff, err = j.ReferencesFields()
	if err != nil {
		return err
	}
	err = validateFieldsForSubQuery(defs, ro, ff)
	if err != nil {
		return err
	}

	return nil
}

func validateFieldsForSubQuery(defs Definitions, def *ast.Definition, fields []string) error {
	subFields := map[string][]string{}
	for _, fn := range fields {
		if fn == "" {
			return ErrorPosf(def.Position, "field name can't be empty")
		}
		parts := strings.SplitN(fn, ".", 2)
		field := def.Fields.ForName(parts[0])
		if field == nil {
			return ErrorPosf(def.Position, "field %s not found in object %s", parts[0], def.Name)
		}
		if len(parts) == 1 {
			if field.Type.NamedType == "" || !IsScalarType(field.Type.Name()) {
				return ErrorPosf(def.Position, "field %s must be a scalar type and not a list", parts[0])
			}
			continue
		}
		if field.Type.NamedType == "" || IsScalarType(field.Type.Name()) {
			return ErrorPosf(def.Position, "field %s be a object and not be a list", parts[0])
		}
		subFields[field.Type.Name()] = append(subFields[parts[0]], parts[1])
	}
	for on, fields := range subFields {
		sub := defs.ForName(on)
		if sub == nil {
			return ErrorPosf(def.Position, "object %s not found", on)
		}
		if err := validateFieldsForSubQuery(defs, sub, fields); err != nil {
			return err
		}
	}
	return nil
}

type QueryRequest struct {
	Name      string
	OrderNum  int
	QueryType QueryType
	Field     *ast.Field
	Subset    []QueryRequest
}

type QueryType int

const (
	QueryTypeNone QueryType = 0
	QueryTypeMeta QueryType = 1 << iota
	QueryTypeQuery
	QueryJQTransform
	QueryTypeMutation
	QueryTypeFunction
	QueryTypeFunctionMutation
)

const (
	MetadataSchemaQuery   = "__schema"
	MetadataTypeQuery     = "__type"
	MetadataTypeNameQuery = "__typename"

	JQTransformQueryName = "jq"
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
			// ignore inline fragment unions and interfaces should be handled in data query
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
		case info.Type == ModuleQuery && fd.Directives.ForName("query") != nil ||
			fd.Directives.ForName(fieldAggregationQueryDirectiveName) != nil:
			resolvers = append(resolvers, QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: QueryTypeQuery,
			})
			qtt |= QueryTypeQuery
		case info.Type == ModuleMutation && fd.Directives.ForName("mutation") != nil:
			resolvers = append(resolvers, QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: QueryTypeMutation,
			})
			qtt |= QueryTypeMutation
		case info.Type == ModuleFunction && fd.Directives.ForName("function") != nil:
			resolvers = append(resolvers, QueryRequest{
				Name:      field.Alias,
				Field:     field,
				QueryType: QueryTypeFunction,
			})
			qtt |= QueryTypeFunction
		case info.Type == ModuleMutationFunction && fd.Directives.ForName("function") != nil:
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
				QueryType: QueryJQTransform,
			})
			qtt |= QueryJQTransform
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
	d := query.Directives.ForName(mutationDirectiveName)
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
	case mutationTypeTextInsert:
		m.Type = MutationTypeInsert
	case mutationTypeTextUpdate:
		m.Type = MutationTypeUpdate
	case mutationTypeTextDelete:
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
		fi := fieldInfo(of, m.ObjectDefinition)
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
		fi := fieldInfo(of, m.ObjectDefinition)
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
		fi := fieldInfo(of, m.ObjectDefinition)
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

	moduleObject := objectModuleType(m.defs, rt, ModuleMutation)
	if moduleObject == nil {
		return nil
	}

	for _, d := range rt.Directives.ForNames(mutationDirectiveName) {
		t := directiveArgValue(d, "type")
		if t != mutationTypeTextInsert {
			continue
		}
		mn := objectDirectiveArgValue(rt, mutationDirectiveName, "name")
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
		if field.Directives.ForName(fieldDefaultDirectiveName) == nil {
			continue
		}
		sequence := fieldDirectiveArgValue(field, fieldDefaultDirectiveName, "sequence")
		if sequence == "" {
			continue
		}
		sequencesValues[field.Name] = sequence
	}
	return sequencesValues
}

func (m *Mutation) DBFieldName(name string) string {
	field := m.ObjectDefinition.Fields.ForName(name)
	if field == nil {
		return ""
	}
	if d := field.Directives.ForName(fieldFieldSourceDirectiveName); d != nil {
		return directiveArgValue(d, "field")
	}
	return field.Name
}

func (m *Mutation) SelectByPKQuery(query *ast.Field) *ast.Field {
	qm := objectModuleType(m.defs, m.ObjectDefinition, ModuleQuery)
	if qm == nil {
		return nil
	}
	var qn string
	for _, d := range m.ObjectDefinition.Directives.ForNames(queryDirectiveName) {
		if directiveArgValue(d, "type") != queryTypeTextSelectOne {
			continue
		}
		name := directiveArgValue(d, "name")
		if name == "" || !strings.HasSuffix(name, ObjectQueryByPKSuffix) {
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

func ArgumentValues(defs Definitions, field *ast.Field, vars map[string]any, checkRequired bool) (FieldQueryArguments, error) {
	args := make([]FieldQueryArgument, 0, len(field.Arguments))
	for _, arg := range field.Arguments {
		def := field.Definition.Arguments.ForName(arg.Name)
		if def == nil {
			return nil, ErrorPosf(arg.Position, "unknown argument %s", arg.Name)
		}
		value := arg.Value
		if value == nil { // get value from default value
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
