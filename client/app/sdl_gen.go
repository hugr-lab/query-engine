package app

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/hugr-lab/airport-go/catalog"
	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/formatter"
)

var graphqlNameRe = regexp.MustCompile(`^[_A-Za-z][_0-9A-Za-z]*$`)

// ValidGraphQLName returns true if name is a valid GraphQL identifier.
func ValidGraphQLName(name string) bool {
	return graphqlNameRe.MatchString(name)
}

// ToGraphQLName transforms a string into a valid GraphQL identifier.
// Replaces invalid characters with underscores, prepends _ if starts with digit.
func ToGraphQLName(name string) string {
	if name == "" {
		return "_"
	}
	var b strings.Builder
	for i, r := range name {
		if r == '_' || unicode.IsLetter(r) || (i > 0 && unicode.IsDigit(r)) {
			b.WriteRune(r)
		} else if unicode.IsDigit(r) && i == 0 {
			b.WriteRune('_')
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	return b.String()
}

// sqlSourceName returns the SQL-style source "schema"."NAME" for directives.
func sqlSourceName(schema, name string) string {
	return fmt.Sprintf(`"%s"."%s"`, schema, strings.ToUpper(name))
}

// graphQLTypeName returns the GraphQL type name for a table/view.
// For default schema, returns just the name (no prefix).
// For other schemas, returns "schema_name" as prefix.
func graphQLTypeName(schema, name string) string {
	if schema == DefaultSchema {
		return ToGraphQLName(name)
	}
	return ToGraphQLName(schema + "_" + name)
}

// strVal creates an ast.Value of kind StringValue.
func strVal(s string) *ast.Value {
	return &ast.Value{Kind: ast.StringValue, Raw: s}
}

// boolVal creates an ast.Value of kind BooleanValue.
func boolVal(v bool) *ast.Value {
	if v {
		return &ast.Value{Kind: ast.BooleanValue, Raw: "true"}
	}
	return &ast.Value{Kind: ast.BooleanValue, Raw: "false"}
}

// functionDirective creates @function(name: "...") or @function(name: "...", is_table: true).
func functionDirective(source string, isTable bool) *ast.Directive {
	args := ast.ArgumentList{
		{Name: "name", Value: strVal(source)},
	}
	if isTable {
		args = append(args, &ast.Argument{Name: "is_table", Value: boolVal(true)})
	}
	return &ast.Directive{Name: "function", Arguments: args}
}

// viewDirective creates @view(name: "...").
func viewDirective(source string) *ast.Directive {
	return &ast.Directive{
		Name:      "view",
		Arguments: ast.ArgumentList{{Name: "name", Value: strVal(source)}},
	}
}

// argsDirective creates @args(name: "InputTypeName").
func argsDirective(inputTypeName string) *ast.Directive {
	return &ast.Directive{
		Name:      "args",
		Arguments: ast.ArgumentList{{Name: "name", Value: strVal(inputTypeName)}},
	}
}

// tableDirective creates @table(name: "...").
func tableDirective(source string) *ast.Directive {
	return &ast.Directive{
		Name:      "table",
		Arguments: ast.ArgumentList{{Name: "name", Value: strVal(source)}},
	}
}

// pkDirective creates @pk.
func pkDirective() *ast.Directive {
	return &ast.Directive{Name: "pk"}
}

// gqlType creates a named GraphQL type (nullable or not).
func gqlType(name string, nonNull bool) *ast.Type {
	return &ast.Type{NamedType: name, NonNull: nonNull}
}

// formatDefinitions formats AST definitions and extensions into an SDL string.
func formatDefinitions(defs []*ast.Definition, extensions []*ast.Definition) string {
	doc := &ast.SchemaDocument{
		Definitions: defs,
		Extensions:  extensions,
	}
	var buf strings.Builder
	f := formatter.NewFormatter(&buf, formatter.WithIndent("    "))
	f.FormatSchemaDocument(doc)
	return buf.String()
}

// --- Generators for handler.go ---

// generateScalarFuncSDL generates SDL for a scalar function using gqlparser AST.
func generateScalarFuncSDL(schema, name string, def *funcDef) string {
	source := sqlSourceName(schema, name)

	field := &ast.FieldDefinition{
		Name:        name,
		Description: def.description,
		Type:        gqlType(def.retType.graphql, false),
		Directives:  ast.DirectiveList{functionDirective(source, false)},
	}
	for _, a := range def.args {
		field.Arguments = append(field.Arguments, &ast.ArgumentDefinition{
			Name:        a.name,
			Description: a.description,
			Type:        gqlType(a.typ.graphql, true),
		})
	}

	// extend type Function { ... }
	ext := &ast.Definition{
		Kind:   ast.Object,
		Name:   "Function",
		Fields: ast.FieldList{field},
	}

	return formatDefinitions(nil, []*ast.Definition{ext})
}

// generateTableFuncSDL generates SDL for a table function as a parameterized view:
//
//	input <name>_args { params... }
//	type <name> @view(name: "...") @args(name: "<name>_args") { columns... }
func generateTableFuncSDL(schema, name string, def *funcDef) string {
	source := sqlSourceName(schema, name)
	typeName := graphQLTypeName(schema, name)
	inputTypeName := typeName + "_args"

	var defs []*ast.Definition

	// input type for parameters (only if there are args)
	if len(def.args) > 0 {
		inputDef := &ast.Definition{
			Kind: ast.InputObject,
			Name: inputTypeName,
		}
		for _, a := range def.args {
			inputDef.Fields = append(inputDef.Fields, &ast.FieldDefinition{
				Name:        a.name,
				Description: a.description,
				Type:        gqlType(a.typ.graphql, true),
			})
		}
		defs = append(defs, inputDef)
	}

	// result type: type TypeName @view(name: "...") @args(name: "...") { columns... }
	resultDef := &ast.Definition{
		Kind:        ast.Object,
		Name:        typeName,
		Description: def.description,
		Directives:  ast.DirectiveList{viewDirective(source)},
	}
	if len(def.args) > 0 {
		resultDef.Directives = append(resultDef.Directives, argsDirective(inputTypeName))
	}
	for _, c := range def.cols {
		fd := &ast.FieldDefinition{
			Name:        c.name,
			Description: c.description,
			Type:        gqlType(c.typ.graphql, !c.nullable),
		}
		if c.pk {
			fd.Directives = append(fd.Directives, pkDirective())
		}
		resultDef.Fields = append(resultDef.Fields, fd)
	}
	defs = append(defs, resultDef)

	return formatDefinitions(defs, nil)
}

// --- Arrow-to-GraphQL type mapping ---

// arrowToGraphQL maps an arrow.DataType to a GraphQL type name.
func arrowToGraphQL(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.BOOL:
		return "Boolean"
	case arrow.INT8, arrow.INT16, arrow.INT32:
		return "Int"
	case arrow.INT64:
		return "BigInt"
	case arrow.UINT8, arrow.UINT16, arrow.UINT32:
		return "UInt"
	case arrow.UINT64:
		return "BigUInt"
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return "Float"
	case arrow.STRING, arrow.LARGE_STRING:
		return "String"
	case arrow.BINARY, arrow.LARGE_BINARY:
		return "Base64"
	case arrow.TIMESTAMP:
		return "DateTime"
	case arrow.DATE32, arrow.DATE64:
		return "Date"
	case arrow.EXTENSION:
		if _, ok := dt.(*catalog.GeometryExtensionType); ok {
			return "Geometry"
		}
		return "String"
	default:
		return "String"
	}
}

// arrowFieldToGQL converts an arrow.Field to an ast.FieldDefinition.
// If the GraphQL name differs from the original Arrow name, @field_source is added.
func arrowFieldToGQL(f arrow.Field) *ast.FieldDefinition {
	gqlName := ToGraphQLName(f.Name)
	fd := &ast.FieldDefinition{
		Name: gqlName,
		Type: gqlType(arrowToGraphQL(f.Type), !f.Nullable),
	}
	if gqlName != f.Name {
		fd.Directives = append(fd.Directives, fieldSourceDirective(f.Name))
	}
	return fd
}

// fieldSourceDirective creates @field_source(field: "original_name").
func fieldSourceDirective(originalName string) *ast.Directive {
	return &ast.Directive{
		Name:      "field_source",
		Arguments: ast.ArgumentList{{Name: "field", Value: strVal(originalName)}},
	}
}

// filterRequiredDirective creates @filter_required.
func filterRequiredDirective() *ast.Directive {
	return &ast.Directive{Name: "filter_required"}
}

// --- Schema options for tables, table functions, and table refs ---

// SchemaOption configures SDL generation for tables, table functions, and table refs.
type SchemaOption func(*schemaDef)

type referenceDef struct {
	referencesName string
	sourceFields   []string
	referencesFields []string
	query          string
	referencesQuery string
	isM2M          bool
	m2mName        string
}

type fieldReferenceDef struct {
	fieldName       string // which field this applies to
	referencesName  string
	field           string // DB field (optional)
	query           string
	referencesQuery string
}

type schemaDef struct {
	rawSDL           string           // user-provided SDL (skips auto-generation)
	description      string           // object description
	fieldDescriptions map[string]string // field name -> description
	pkFields         []string         // explicit PK field names
	m2m              bool             // @table(is_m2m: true)
	references       []referenceDef
	fieldReferences  []fieldReferenceDef
	filterRequired   []string         // field names that need @filter_required
}

// WithDescription sets the description for the table/view/function type.
func WithDescription(desc string) SchemaOption {
	return func(d *schemaDef) {
		d.description = desc
	}
}

// WithFieldDescription sets the description for a specific field.
func WithFieldDescription(field, desc string) SchemaOption {
	return func(d *schemaDef) {
		if d.fieldDescriptions == nil {
			d.fieldDescriptions = make(map[string]string)
		}
		d.fieldDescriptions[field] = desc
	}
}

// WithRawSDL provides user-written SDL directly, skipping auto-generation.
func WithRawSDL(sdl string) SchemaOption {
	return func(d *schemaDef) {
		d.rawSDL = sdl
	}
}

// WithPK marks fields as primary key. Multiple calls or multiple names for composite PK.
func WithPK(fields ...string) SchemaOption {
	return func(d *schemaDef) {
		d.pkFields = append(d.pkFields, fields...)
	}
}

// WithM2M marks the table as a many-to-many junction table.
func WithM2M() SchemaOption {
	return func(d *schemaDef) {
		d.m2m = true
	}
}

// WithFilterRequired marks fields as requiring a filter when queried.
func WithFilterRequired(fields ...string) SchemaOption {
	return func(d *schemaDef) {
		d.filterRequired = append(d.filterRequired, fields...)
	}
}

// WithReferences adds an object-level @references directive.
func WithReferences(referencesName string, sourceFields, referencesFields []string, query, referencesQuery string) SchemaOption {
	return func(d *schemaDef) {
		d.references = append(d.references, referenceDef{
			referencesName:   referencesName,
			sourceFields:     sourceFields,
			referencesFields: referencesFields,
			query:            query,
			referencesQuery:  referencesQuery,
		})
	}
}

// WithM2MReferences adds an object-level @references directive with is_m2m flag.
func WithM2MReferences(referencesName string, sourceFields, referencesFields []string, query, referencesQuery, m2mName string) SchemaOption {
	return func(d *schemaDef) {
		d.references = append(d.references, referenceDef{
			referencesName:   referencesName,
			sourceFields:     sourceFields,
			referencesFields: referencesFields,
			query:            query,
			referencesQuery:  referencesQuery,
			isM2M:            true,
			m2mName:          m2mName,
		})
	}
}

// WithFieldReferences adds a field-level @field_references directive.
func WithFieldReferences(fieldName, referencesName, query, referencesQuery string) SchemaOption {
	return func(d *schemaDef) {
		d.fieldReferences = append(d.fieldReferences, fieldReferenceDef{
			fieldName:       fieldName,
			referencesName:  referencesName,
			query:           query,
			referencesQuery: referencesQuery,
		})
	}
}

// --- Directive builders from schema options ---

func buildReferencesDirective(r referenceDef) *ast.Directive {
	args := ast.ArgumentList{
		{Name: "references_name", Value: strVal(r.referencesName)},
		{Name: "source_fields", Value: strListVal(r.sourceFields)},
		{Name: "references_fields", Value: strListVal(r.referencesFields)},
	}
	if r.query != "" {
		args = append(args, &ast.Argument{Name: "query", Value: strVal(r.query)})
	}
	if r.referencesQuery != "" {
		args = append(args, &ast.Argument{Name: "references_query", Value: strVal(r.referencesQuery)})
	}
	if r.isM2M {
		args = append(args, &ast.Argument{Name: "is_m2m", Value: boolVal(true)})
	}
	if r.m2mName != "" {
		args = append(args, &ast.Argument{Name: "m2m_name", Value: strVal(r.m2mName)})
	}
	return &ast.Directive{Name: "references", Arguments: args}
}

func buildFieldReferencesDirective(fr fieldReferenceDef) *ast.Directive {
	args := ast.ArgumentList{
		{Name: "references_name", Value: strVal(fr.referencesName)},
	}
	if fr.field != "" {
		args = append(args, &ast.Argument{Name: "field", Value: strVal(fr.field)})
	}
	if fr.query != "" {
		args = append(args, &ast.Argument{Name: "query", Value: strVal(fr.query)})
	}
	if fr.referencesQuery != "" {
		args = append(args, &ast.Argument{Name: "references_query", Value: strVal(fr.referencesQuery)})
	}
	return &ast.Directive{Name: "field_references", Arguments: args}
}

// strListVal creates an ast.Value of kind ListValue from a string slice.
func strListVal(ss []string) *ast.Value {
	children := make(ast.ChildValueList, len(ss))
	for i, s := range ss {
		children[i] = &ast.ChildValue{Value: strVal(s)}
	}
	return &ast.Value{Kind: ast.ListValue, Children: children}
}

// --- Table and TableRef SDL generators ---

// isMutableTable returns true if the table supports any write operations.
func isMutableTable(table catalog.Table) bool {
	switch table.(type) {
	case catalog.InsertableTable:
		return true
	case catalog.UpdatableTable:
		return true
	case catalog.UpdatableBatchTable:
		return true
	case catalog.DeletableTable:
		return true
	case catalog.DeletableBatchTable:
		return true
	}
	return false
}

// buildObjectDef builds an ast.Definition from an Arrow schema and schema options.
func buildObjectDef(typeName, source, comment string, arrowSchema *arrow.Schema, isTable bool, opts []SchemaOption) *ast.Definition {
	sd := &schemaDef{}
	for _, o := range opts {
		o(sd)
	}

	// Build PK set for quick lookup
	pkSet := make(map[string]bool, len(sd.pkFields))
	for _, pk := range sd.pkFields {
		pkSet[pk] = true
	}
	// If no explicit PK and no M2M, first field is PK
	autoPK := len(sd.pkFields) == 0 && !sd.m2m

	// Build filter_required set
	frSet := make(map[string]bool, len(sd.filterRequired))
	for _, fr := range sd.filterRequired {
		frSet[fr] = true
	}

	// Build field_references map: arrow field name -> directive
	frMap := make(map[string]fieldReferenceDef, len(sd.fieldReferences))
	for _, fr := range sd.fieldReferences {
		frMap[fr.fieldName] = fr
	}

	desc := comment
	if sd.description != "" {
		desc = sd.description
	}

	def := &ast.Definition{
		Kind:        ast.Object,
		Name:        typeName,
		Description: desc,
	}

	// Object directive: @table or @view. M2M implies @table.
	if isTable || sd.m2m {
		td := tableDirective(source)
		if sd.m2m {
			td.Arguments = append(td.Arguments, &ast.Argument{Name: "is_m2m", Value: boolVal(true)})
		}
		def.Directives = append(def.Directives, td)
	} else {
		def.Directives = append(def.Directives, viewDirective(source))
	}

	// Object-level @references
	for _, r := range sd.references {
		def.Directives = append(def.Directives, buildReferencesDirective(r))
	}

	// Fields
	for i := range arrowSchema.NumFields() {
		f := arrowSchema.Field(i)
		fd := arrowFieldToGQL(f)

		// description
		if d, ok := sd.fieldDescriptions[f.Name]; ok {
			fd.Description = d
		}

		// @pk
		if pkSet[f.Name] || (autoPK && i == 0) {
			fd.Directives = append(fd.Directives, pkDirective())
		}

		// @filter_required
		if frSet[f.Name] {
			fd.Directives = append(fd.Directives, filterRequiredDirective())
		}

		// @field_references
		if fr, ok := frMap[f.Name]; ok {
			fd.Directives = append(fd.Directives, buildFieldReferencesDirective(fr))
		}

		def.Fields = append(def.Fields, fd)
	}

	return def
}

// GenerateTableSDL generates SDL for a catalog.Table from its Arrow schema.
// Mutable tables get @table, read-only tables get @view.
// If WithRawSDL is provided, returns that SDL directly.
func GenerateTableSDL(schemaName string, table catalog.Table, opts ...SchemaOption) string {
	if raw := rawSDLFromOpts(opts); raw != "" {
		return raw
	}
	arrowSchema := table.ArrowSchema(nil)
	if arrowSchema == nil {
		return ""
	}
	source := sqlSourceName(schemaName, table.Name())
	typeName := graphQLTypeName(schemaName, table.Name())
	def := buildObjectDef(typeName, source, table.Comment(), arrowSchema, isMutableTable(table), opts)
	return formatDefinitions([]*ast.Definition{def}, nil)
}

// GenerateTableRefSDL generates SDL for a catalog.TableRef from its Arrow schema.
// Table refs are always @view (read-only).
// If WithRawSDL is provided, returns that SDL directly.
func GenerateTableRefSDL(schemaName string, ref catalog.TableRef, opts ...SchemaOption) string {
	if raw := rawSDLFromOpts(opts); raw != "" {
		return raw
	}
	arrowSchema := ref.ArrowSchema()
	if arrowSchema == nil {
		return ""
	}
	source := sqlSourceName(schemaName, ref.Name())
	typeName := graphQLTypeName(schemaName, ref.Name())
	def := buildObjectDef(typeName, source, ref.Comment(), arrowSchema, false, opts)
	return formatDefinitions([]*ast.Definition{def}, nil)
}

func rawSDLFromOpts(opts []SchemaOption) string {
	sd := &schemaDef{}
	for _, o := range opts {
		o(sd)
	}
	return sd.rawSDL
}
