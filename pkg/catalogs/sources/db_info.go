package sources

import (
	"context"
	"fmt"
	"strings"

	"github.com/hugr-lab/query-engine/pkg/compiler"
	"github.com/hugr-lab/query-engine/pkg/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/types"
	"github.com/vektah/gqlparser/v2/ast"
)

func DescribeDataSource(ctx context.Context, qe types.Querier, name string) (*DBInfo, error) {
	res, err := qe.Query(ctx, `query dbMeta($name: String!) {
		core {
			meta {
				databases_by_name(name: $name) {
					name
					description: comment
					type
					schemas(filter:{
						_or:[
							{internal: {eq: false}}
							{name: {eq: "public"}}
							{name: {eq: "main"}}
						]
						_not: {
            				_or:[
								{name: {like: "_timescaledb%"}}
								{name: {eq: "timescaledb_experimental"}}
								{name: {eq: "timescaledb_information"}}
							]
						}
					}){
						name
						description: comment
						tables(filter:{internal: {eq: false}}){
							name
							description: comment
							schema_name: schema_name
							columns(filter: {internal: {eq: false}}){
								name
								description: comment
								data_type
								default
								is_nullable
							}
							constraints(filter:{
								database_name: {eq: $name}
							}){
								name
								type
								references_schema_name: schema_name
								references_table_name
								columns
								references_columns
							}
						}
						views(filter:{internal: {eq: false}}){
							name
							description: comment
							schema_name: schema_name
							columns(filter: {internal: {eq: false}}){
								name
								description: comment
								data_type
								default
								is_nullable
							}
						}
					}
				}
			}
		}
	}`, map[string]any{
		"name": name,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to query data source %s: %w", name, err)
	}
	defer res.Close()
	var dbInfo DBInfo
	err = res.ScanData("core.meta.databases_by_name", &dbInfo)
	if err != nil {
		return nil, fmt.Errorf("failed to scan data source %s: %w", name, err)
	}
	if dbInfo.Name == "" {
		return nil, fmt.Errorf("data source %s not found", name)
	}

	return &dbInfo, nil
}

// create catalog source by provided database definition
type DBInfo struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Type        string         `json:"type"` // e.g., "mysql", "postgres", "duckdb", "memory", etc.
	SchemaInfo  []DBSchemaInfo `json:"schemas"`
}

type DBSchemaInfo struct {
	Name        string        `json:"name"`
	Description string        `json:"description"`
	Tables      []DBTableInfo `json:"tables"`
	Views       []DBViewInfo  `json:"views"`
}

type DBTableInfo struct {
	Name        string             `json:"name"`
	Description string             `json:"description"`
	SchemaName  string             `json:"schema_name"`
	Columns     []DBColumnInfo     `json:"columns"`
	Constraints []DBConstraintInfo `json:"constraints"`
}

type DBViewInfo struct {
	Name        string         `json:"name"`
	SchemaName  string         `json:"schema_name"`
	Description string         `json:"description"`
	Columns     []DBColumnInfo `json:"columns"`
}

type DBColumnInfo struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	DataType    string `json:"data_type"`
	IsNullable  bool   `json:"is_nullable"`
	Default     string `json:"default,omitempty"`
}

type DBConstraintInfo struct {
	Name              string   `json:"name"`
	Type              string   `json:"type"`                         // e.g., PRIMARY KEY, FOREIGN KEY, UNIQUE
	Columns           []string `json:"columns"`                      // list of column names involved in the constraint
	ReferencedSchema  string   `json:"referenced_schema,omitempty"`  // for foreign keys, the schema of the referenced table
	ReferencedTable   string   `json:"referenced_table,omitempty"`   // for foreign keys
	ReferencedColumns []string `json:"referenced_columns,omitempty"` // columns in the referenced table
}

func (s *DBInfo) SchemaDocument(ctx context.Context) (*ast.SchemaDocument, error) {
	doc := &ast.SchemaDocument{}

	for _, schema := range s.SchemaInfo {
		defs, err := schema.Definitions()
		if err != nil {
			return nil, err
		}
		for _, def := range defs {
			doc.Definitions = append(doc.Definitions, def)
		}
	}

	return doc, nil
}

func (s *DBSchemaInfo) Definitions() (ast.DefinitionList, error) {
	var defs ast.DefinitionList

	for _, table := range s.Tables {
		def, err := table.Definition()
		if err != nil {
			return nil, err
		}
		if def == nil {
			continue
		}
		defs = append(defs, def)
	}

	for _, view := range s.Views {
		def, err := view.Definition()
		if err != nil {
			return nil, err
		}
		if def == nil {
			continue
		}
		defs = append(defs, def)
	}

	return defs, nil
}

func (t *DBTableInfo) Definition() (*ast.Definition, error) {
	if len(t.Columns) == 0 {
		return nil, nil // No columns, no definition
	}
	name := dataObjectName(t.SchemaName, t.Name)

	def := &ast.Definition{
		Name:        identGraphQL(name),
		Kind:        ast.Object,
		Description: t.Description,
		Position:    compiler.CompiledPosName("self-described"),
	}

	// table directive
	def.Directives = append(def.Directives, &ast.Directive{
		Name: "table",
		Arguments: ast.ArgumentList{
			&ast.Argument{
				Name:     "name",
				Value:    &ast.Value{Raw: name, Kind: ast.StringValue, Position: compiler.CompiledPosName("self-described-table-source")},
				Position: compiler.CompiledPosName("self-described-table"),
			},
		},
		Position: compiler.CompiledPosName("self-described-table"),
	})

	if name != t.Name {
		// add module directive if the name is qualified
		def.Directives = append(def.Directives, &ast.Directive{
			Name: "module",
			Arguments: ast.ArgumentList{
				&ast.Argument{
					Name:     "name",
					Value:    &ast.Value{Raw: identGraphQL(t.SchemaName), Kind: ast.StringValue, Position: compiler.CompiledPosName("self-described-module")},
					Position: compiler.CompiledPosName("self-described-module"),
				},
			},
			Position: compiler.CompiledPosName("self-described-module"),
		})
	}

	for _, col := range t.Columns {
		colDef, err := col.Definition()
		if err != nil {
			return nil, err
		}
		if colDef != nil {
			def.Fields = append(def.Fields, colDef)
		}
	}

	if len(def.Fields) == 0 {
		return nil, nil // No fields, no definition
	}
	for _, constraint := range t.Constraints {
		switch strings.ToUpper(constraint.Type) {
		case "PRIMARY KEY":
			// add primary key directive to the field
			var pkf ast.FieldList
			for _, colName := range constraint.Columns {
				fieldDef := def.Fields.ForName(identGraphQL(colName))
				if fieldDef == nil {
					pkf = nil // Reset if any field is not found
					continue  // Skip if the field is not found
				}
				pkf = append(pkf, fieldDef)
			}
			for _, field := range pkf {
				field.Directives = append(field.Directives, &ast.Directive{
					Name:     "pk",
					Position: compiler.CompiledPosName("self-described-primary-key"),
				})
			}
		case "FOREIGN KEY":
			// add foreign key directive to the type
			if len(constraint.ReferencedColumns) == 0 ||
				len(constraint.Columns) != len(constraint.ReferencedColumns) {
				continue
			}
			// make columns list
			columns := make(ast.ChildValueList, len(constraint.Columns))
			for i, col := range constraint.Columns {
				columns[i] = &ast.ChildValue{
					Name:     identGraphQL(col),
					Value:    &ast.Value{Raw: identGraphQL(col), Kind: ast.StringValue, Position: compiler.CompiledPosName("self-described-foreign-key")},
					Position: compiler.CompiledPosName("self-described-foreign-key"),
				}
			}
			// make references columns list
			references := make(ast.ChildValueList, len(constraint.ReferencedColumns))
			for i, col := range constraint.ReferencedColumns {
				references[i] = &ast.ChildValue{
					Name:     identGraphQL(col),
					Value:    &ast.Value{Raw: identGraphQL(constraint.Columns[i]), Kind: ast.StringValue, Position: compiler.CompiledPosName("self-described-foreign-key")},
					Position: compiler.CompiledPosName("self-described-foreign-key"),
				}
			}
			ref := &ast.Directive{
				Name: "references",
				Arguments: ast.ArgumentList{
					&ast.Argument{
						Name: "name",
						Value: &ast.Value{
							Raw:      t.Name + "_" + constraint.Name + "_fk",
							Kind:     ast.StringValue,
							Position: compiler.CompiledPosName("self-described-foreign-key"),
						},
						Position: compiler.CompiledPosName("self-described-foreign-key"),
					},
					&ast.Argument{
						Name: "references_name",
						Value: &ast.Value{
							Raw:      dataObjectName(constraint.ReferencedSchema, constraint.ReferencedTable),
							Kind:     ast.StringValue,
							Position: compiler.CompiledPosName("self-described-foreign-key"),
						},
						Position: compiler.CompiledPosName("self-described-foreign-key"),
					},
					&ast.Argument{
						Name: "source_fields",
						Value: &ast.Value{
							Kind:     ast.ListValue,
							Children: columns,
							Position: compiler.CompiledPosName("self-described-foreign-key"),
						},
					},
					&ast.Argument{
						Name: "references_fields",
						Value: &ast.Value{
							Kind:     ast.ListValue,
							Children: references,
							Position: compiler.CompiledPosName("self-described-foreign-key"),
						},
					},
					&ast.Argument{
						Name: "query",
						Value: &ast.Value{
							Raw:      identGraphQL("ref_" + constraint.ReferencedTable),
							Kind:     ast.StringValue,
							Position: compiler.CompiledPosName("self-described-foreign-key"),
						},
					},
					&ast.Argument{
						Name: "references_query",
						Value: &ast.Value{
							Raw:      identGraphQL("ref_" + t.Name),
							Kind:     ast.StringValue,
							Position: compiler.CompiledPosName("self-described-foreign-key"),
						},
					},
				},
				Position: compiler.CompiledPosName("self-described-foreign-key"),
			}

			def.Directives = append(def.Directives, ref)
		}
	}

	return def, nil
}

func (v *DBViewInfo) Definition() (*ast.Definition, error) {
	if len(v.Columns) == 0 {
		return nil, nil // No columns, no definition
	}
	name := dataObjectName(v.SchemaName, v.Name)

	def := &ast.Definition{
		Name:        identGraphQL(name),
		Kind:        ast.Object,
		Description: v.Description,
		Position:    compiler.CompiledPosName("self-described"),
	}

	// view directive
	def.Directives = append(def.Directives, &ast.Directive{
		Name: "view",
		Arguments: ast.ArgumentList{
			&ast.Argument{
				Name:     "name",
				Value:    &ast.Value{Raw: name, Kind: ast.StringValue, Position: compiler.CompiledPosName("self-described-view")},
				Position: compiler.CompiledPosName("self-described-view"),
			},
		},
		Position: compiler.CompiledPosName("self-described-view"),
	})
	if name != v.Name {
		// add module directive if the name is qualified
		def.Directives = append(def.Directives, &ast.Directive{
			Name: "module",
			Arguments: ast.ArgumentList{
				&ast.Argument{
					Name:     "name",
					Value:    &ast.Value{Raw: identGraphQL(v.SchemaName), Kind: ast.StringValue, Position: compiler.CompiledPosName("self-described-module")},
					Position: compiler.CompiledPosName("self-described-module"),
				},
			},
			Position: compiler.CompiledPosName("self-described-module"),
		})
	}
	for _, col := range v.Columns {
		colDef, err := col.Definition()
		if err != nil {
			return nil, err
		}
		if colDef != nil {
			def.Fields = append(def.Fields, colDef)
		}
	}
	if len(def.Fields) == 0 {
		return nil, nil // No fields, no definition
	}

	return def, nil
}

func (c *DBColumnInfo) Definition() (*ast.FieldDefinition, error) {
	fieldDef := &ast.FieldDefinition{
		Name:        c.Name,
		Description: c.Description,
		Type:        graphQLType(c.DataType),
		Position:    compiler.CompiledPosName("self-described"),
	}
	if fieldDef.Type == nil {
		return nil, nil // If the type is not recognized, return nil
	}

	// check if the field name is qualified
	name := identGraphQL(c.Name)
	if name != c.Name {
		fieldDef.Name = name // Use the sanitized name
		fieldDef.Directives = append(fieldDef.Directives, base.FieldSourceDirective(c.Name))
	}

	if !c.IsNullable {
		fieldDef.Type.NonNull = true // Mark as non-nullable if applicable
	}

	// add default value if provided
	if strings.HasPrefix(c.Default, "nextval('") && strings.HasSuffix(c.Default, "')") {
		seqName := strings.TrimPrefix(strings.TrimSuffix(c.Default, "'"), "nextval(")
		fieldDef.Directives = append(fieldDef.Directives, &ast.Directive{
			Name: "default",
			Arguments: ast.ArgumentList{
				&ast.Argument{
					Name:     "sequence",
					Value:    &ast.Value{Raw: seqName, Kind: ast.StringValue, Position: compiler.CompiledPosName("default-sequence")},
					Position: compiler.CompiledPosName("default-sequence"),
				},
			},
			Position: compiler.CompiledPosName("default-sequence"),
		})
	}

	return fieldDef, nil
}

// graphQLType converts a string to a GraphQL type
// The struct, map ignored
func graphQLType(name string) *ast.Type {
	if strings.HasSuffix(name, "[]") {
		// Handle array types
		elemType := graphQLType(strings.TrimSuffix(name, "[]"))
		if elemType == nil {
			return nil
		}
		return ast.ListType(elemType, compiler.CompiledPosName("self-described-type"))
	}

	pos := compiler.CompiledPosName("self-described-type")
	if strings.HasPrefix(name, "DECIMAL") ||
		strings.HasPrefix(name, "NUMERIC") {
		return ast.NamedType("Float", pos)
	}
	switch strings.ToUpper(name) {
	case "INT", "INTEGER", "INT4", "SIGNED":
		return ast.NamedType("Int", pos)
	case "BIGINT", "INT8", "LONG", "UINTEGER":
		return ast.NamedType("BigInt", pos)
	case "SMALLINT", "INT2", "SHORT", "TINYINT":
		return ast.NamedType("Int", pos)
	case "FLOAT", "REAL", "FLOAT4", "DOUBLE", "FLOAT8":
		return ast.NamedType("Float", pos)
	case "BLOB", "BYTEA", "BINARY", "VARBINARY":
		return ast.NamedType("String", pos)
	case "BIT", "BITSTRING":
		return ast.NamedType("String", pos)
	case "BOOLEAN", "BOOL", "LOGICAL":
		return ast.NamedType("Boolean", pos)
	case "DATE":
		return ast.NamedType("Date", pos)
	case "TIME":
		return ast.NamedType("Time", pos)
	case "TIMESTAMP", "DATETIME", "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return ast.NamedType("Timestamp", pos)
	case "INTERVAL":
		return ast.NamedType("Interval", pos)
	case "JSON":
		return ast.NamedType("JSON", pos)
	case "TEXT", "VARCHAR", "CHAR", "BPCHAR", "STRING":
		return ast.NamedType("String", pos)
	case "UUID":
		return ast.NamedType("String", pos)
	case "GEOMETRY", "GEOGRAPHY", "WKB_BLOB":
		return ast.NamedType("Geometry", pos)
	default:
		return nil
	}
}

// Makes valid graphql identifier
func identGraphQL(name string) string {
	// Replace invalid characters with underscores
	name = strings.Map(func(r rune) rune {
		// allow alphanumeric english characters and underscores
		if r >= 'a' && r <= 'z' ||
			r >= 'A' && r <= 'Z' ||
			r >= '0' && r <= '9' ||
			r == '_' {
			return r
		}
		return '_'
	}, name)
	if strings.HasPrefix(name, "_") {
		// Ensure it doesn't start with an underscore
		name = "db_" + name[1:]
	}
	return name
}

var skipSchemaModules = map[string]bool{
	"public": true,
	"main":   true,
}

func dataObjectName(schema, name string) string {
	// if schema is empty, use name as is
	if schema == "" {
		return name
	}
	// if schema is in skipSchemaModules, return name only
	if _, ok := skipSchemaModules[schema]; ok {
		return name
	}
	// otherwise return schema.name
	return schema + "." + name
}
