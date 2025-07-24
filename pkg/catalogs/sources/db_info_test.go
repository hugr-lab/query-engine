package sources

import (
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

func Test_graphQLType(t *testing.T) {
	tests := []struct {
		name     string
		wantType *ast.Type
	}{
		{"INT", ast.NamedType("Int", nil)},
		{"INTEGER", ast.NamedType("Int", nil)},
		{"INT4", ast.NamedType("Int", nil)},
		{"SIGNED", ast.NamedType("Int", nil)},
		{"BIGINT", ast.NamedType("BigInt", nil)},
		{"INT8", ast.NamedType("BigInt", nil)},
		{"LONG", ast.NamedType("BigInt", nil)},
		{"UINTEGER", ast.NamedType("BigInt", nil)},
		{"SMALLINT", ast.NamedType("Int", nil)},
		{"INT2", ast.NamedType("Int", nil)},
		{"SHORT", ast.NamedType("Int", nil)},
		{"TINYINT", ast.NamedType("Int", nil)},
		{"FLOAT", ast.NamedType("Float", nil)},
		{"REAL", ast.NamedType("Float", nil)},
		{"FLOAT4", ast.NamedType("Float", nil)},
		{"DOUBLE", ast.NamedType("Float", nil)},
		{"FLOAT8", ast.NamedType("Float", nil)},
		{"DECIMAL", ast.NamedType("Float", nil)},
		{"DECIMAL(10,2)", ast.NamedType("Float", nil)},
		{"NUMERIC", ast.NamedType("Float", nil)},
		{"BLOB", ast.NamedType("String", nil)},
		{"BYTEA", ast.NamedType("String", nil)},
		{"BINARY", ast.NamedType("String", nil)},
		{"VARBINARY", ast.NamedType("String", nil)},
		{"BIT", ast.NamedType("String", nil)},
		{"BITSTRING", ast.NamedType("String", nil)},
		{"BOOLEAN", ast.NamedType("Boolean", nil)},
		{"BOOL", ast.NamedType("Boolean", nil)},
		{"LOGICAL", ast.NamedType("Boolean", nil)},
		{"DATE", ast.NamedType("Date", nil)},
		{"TIME", ast.NamedType("Time", nil)},
		{"TIMESTAMP", ast.NamedType("Timestamp", nil)},
		{"DATETIME", ast.NamedType("Timestamp", nil)},
		{"TIMESTAMPTZ", ast.NamedType("Timestamp", nil)},
		{"TIMESTAMP WITH TIME ZONE", ast.NamedType("Timestamp", nil)},
		{"INTERVAL", ast.NamedType("Interval", nil)},
		{"JSON", ast.NamedType("JSON", nil)},
		{"TEXT", ast.NamedType("String", nil)},
		{"VARCHAR", ast.NamedType("String", nil)},
		{"CHAR", ast.NamedType("String", nil)},
		{"BPCHAR", ast.NamedType("String", nil)},
		{"STRING", ast.NamedType("String", nil)},
		{"UUID", ast.NamedType("String", nil)},
		{"UNKNOWN", nil},
		{"INT[]", ast.ListType(ast.NamedType("Int", nil), nil)},
		{"VARCHAR[]", ast.ListType(ast.NamedType("String", nil), nil)},
		{"FLOAT[]", ast.ListType(ast.NamedType("Float", nil), nil)},
		{"BOOLEAN[]", ast.ListType(ast.NamedType("Boolean", nil), nil)},
		{"UNKNOWN[]", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := graphQLType(tt.name)
			if (got == nil) != (tt.wantType == nil) {
				t.Errorf("graphQLType(%q) = %v, want %v", tt.name, got, tt.wantType)
				return
			}
			if got != nil && tt.wantType != nil {
				if got.Name() != tt.wantType.Name() || (got.Elem != nil) != (tt.wantType.Elem != nil) {
					t.Errorf("graphQLType(%q) = %v, want %v", tt.name, got, tt.wantType)
				}
			}
		})
	}
}

func TestDBColumnInfo_Definition(t *testing.T) {
	tests := []struct {
		name           string
		column         DBColumnInfo
		wantNil        bool
		wantName       string
		wantType       string
		wantNonNull    bool
		wantDirectives []string
	}{
		{
			name: "simple int nullable",
			column: DBColumnInfo{
				Name:        "id",
				Description: "primary key",
				DataType:    "INT",
				IsNullable:  true,
			},
			wantNil:        false,
			wantName:       "id",
			wantType:       "Int",
			wantNonNull:    false,
			wantDirectives: nil,
		},
		{
			name: "simple int non-nullable",
			column: DBColumnInfo{
				Name:        "id",
				Description: "primary key",
				DataType:    "INT",
				IsNullable:  false,
			},
			wantNil:        false,
			wantName:       "id",
			wantType:       "Int",
			wantNonNull:    true,
			wantDirectives: nil,
		},
		{
			name: "unknown type returns nil",
			column: DBColumnInfo{
				Name:        "foo",
				Description: "unknown type",
				DataType:    "FOOBAR",
				IsNullable:  true,
			},
			wantNil: true,
		},
		{
			name: "qualified name triggers field_source directive",
			column: DBColumnInfo{
				Name:        "foo.bar",
				Description: "qualified",
				DataType:    "VARCHAR",
				IsNullable:  true,
			},
			wantNil:        false,
			wantName:       "foo_bar",
			wantType:       "String",
			wantNonNull:    false,
			wantDirectives: []string{"field_source"},
		},
		{
			name: "default nextval triggers default directive",
			column: DBColumnInfo{
				Name:        "id",
				Description: "with default",
				DataType:    "BIGINT",
				IsNullable:  false,
				Default:     "nextval('my_seq')",
			},
			wantNil:        false,
			wantName:       "id",
			wantType:       "BigInt",
			wantNonNull:    true,
			wantDirectives: []string{"default"},
		},
		{
			name: "default not nextval does not trigger default directive",
			column: DBColumnInfo{
				Name:        "created_at",
				Description: "with default",
				DataType:    "TIMESTAMP",
				IsNullable:  false,
				Default:     "CURRENT_TIMESTAMP",
			},
			wantNil:        false,
			wantName:       "created_at",
			wantType:       "Timestamp",
			wantNonNull:    true,
			wantDirectives: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def, err := tt.column.Definition()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantNil {
				if def != nil {
					t.Errorf("expected nil, got %+v", def)
				}
				return
			}
			if def == nil {
				t.Fatalf("expected non-nil definition")
			}
			if def.Name != tt.wantName {
				t.Errorf("got Name %q, want %q", def.Name, tt.wantName)
			}
			if def.Type == nil || def.Type.Name() != tt.wantType {
				t.Errorf("got Type %v, want %v", def.Type, tt.wantType)
			}
			if def.Type != nil && def.Type.NonNull != tt.wantNonNull {
				t.Errorf("got NonNull %v, want %v", def.Type.NonNull, tt.wantNonNull)
			}
			// Check directives
			gotDirectives := map[string]bool{}
			for _, d := range def.Directives {
				gotDirectives[d.Name] = true
			}
			for _, wantDir := range tt.wantDirectives {
				if !gotDirectives[wantDir] {
					t.Errorf("expected directive %q, not found in %+v", wantDir, def.Directives)
				}
			}
			if len(def.Directives) != len(tt.wantDirectives) {
				t.Errorf("got %d directives, want %d", len(def.Directives), len(tt.wantDirectives))
			}
		})
	}
}

func TestDBTableInfo_Definition(t *testing.T) {
	tests := []struct {
		name           string
		table          DBTableInfo
		wantNil        bool
		wantName       string
		wantKind       ast.DefinitionKind
		wantFields     []string
		wantDirectives []string
		pkFields       []string
		fkDirective    bool
	}{
		{
			name: "empty columns returns nil",
			table: DBTableInfo{
				Name:    "users",
				Columns: nil,
			},
			wantNil: true,
		},
		{
			name: "simple table with one column",
			table: DBTableInfo{
				Name:        "users",
				Description: "users table",
				SchemaName:  "public",
				Columns: []DBColumnInfo{
					{Name: "id", DataType: "INT", IsNullable: false},
				},
			},
			wantNil:        false,
			wantName:       "users",
			wantKind:       ast.Object,
			wantFields:     []string{"id"},
			wantDirectives: []string{"table"},
		},
		{
			name: "qualified schema adds module directive",
			table: DBTableInfo{
				Name:        "orders",
				Description: "orders table",
				SchemaName:  "sales",
				Columns: []DBColumnInfo{
					{Name: "order_id", DataType: "BIGINT", IsNullable: false},
				},
			},
			wantNil:        false,
			wantName:       "sales.orders",
			wantKind:       ast.Object,
			wantFields:     []string{"order_id"},
			wantDirectives: []string{"table", "module"},
		},
		{
			name: "table with multiple columns and primary key",
			table: DBTableInfo{
				Name:        "products",
				SchemaName:  "public",
				Description: "products table",
				Columns: []DBColumnInfo{
					{Name: "id", DataType: "INT", IsNullable: false},
					{Name: "name", DataType: "VARCHAR", IsNullable: false},
				},
				Constraints: []DBConstraintInfo{
					{
						Name:    "pk_products",
						Type:    "PRIMARY KEY",
						Columns: []string{"id"},
					},
				},
			},
			wantNil:        false,
			wantName:       "products",
			wantKind:       ast.Object,
			wantFields:     []string{"id", "name"},
			wantDirectives: []string{"table"},
			pkFields:       []string{"id"},
		},
		{
			name: "table with foreign key",
			table: DBTableInfo{
				Name:        "orders",
				SchemaName:  "public",
				Description: "orders table",
				Columns: []DBColumnInfo{
					{Name: "id", DataType: "INT", IsNullable: false},
					{Name: "user_id", DataType: "INT", IsNullable: false},
				},
				Constraints: []DBConstraintInfo{
					{
						Name:              "fk_user",
						Type:              "FOREIGN KEY",
						Columns:           []string{"user_id"},
						ReferencesSchema:  "public",
						ReferencesTable:   "users",
						ReferencesColumns: []string{"id"},
					},
				},
			},
			wantNil:        false,
			wantName:       "orders",
			wantKind:       ast.Object,
			wantFields:     []string{"id", "user_id"},
			wantDirectives: []string{"table", "references"},
			fkDirective:    true,
		},
		{
			name: "table with no valid columns after filtering",
			table: DBTableInfo{
				Name:       "empty",
				SchemaName: "public",
				Columns: []DBColumnInfo{
					{Name: "foo", DataType: "UNKNOWN", IsNullable: true},
				},
			},
			wantNil: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			def, err := tt.table.Definition()
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tt.wantNil {
				if def != nil {
					t.Errorf("expected nil, got %+v", def)
				}
				return
			}
			if def == nil {
				t.Fatalf("expected non-nil definition")
			}
			if def.Name != identGraphQL(tt.wantName) {
				t.Errorf("got Name %q, want %q", def.Name, identGraphQL(tt.wantName))
			}
			if def.Kind != tt.wantKind {
				t.Errorf("got Kind %v, want %v", def.Kind, tt.wantKind)
			}
			// Check fields
			gotFields := map[string]bool{}
			for _, f := range def.Fields {
				gotFields[f.Name] = true
			}
			for _, wantField := range tt.wantFields {
				if !gotFields[wantField] {
					t.Errorf("expected field %q, not found in %+v", wantField, def.Fields)
				}
			}
			if len(def.Fields) != len(tt.wantFields) {
				t.Errorf("got %d fields, want %d", len(def.Fields), len(tt.wantFields))
			}
			// Check directives
			gotDirectives := map[string]bool{}
			for _, d := range def.Directives {
				gotDirectives[d.Name] = true
			}
			for _, wantDir := range tt.wantDirectives {
				if !gotDirectives[wantDir] {
					t.Errorf("expected directive %q, not found in %+v", wantDir, def.Directives)
				}
			}
			// Check pk directive on fields if applicable
			if len(tt.pkFields) > 0 {
				for _, pkField := range tt.pkFields {
					field := def.Fields.ForName(identGraphQL(pkField))
					if field == nil {
						t.Errorf("expected pk field %q not found", pkField)
						continue
					}
					found := false
					for _, d := range field.Directives {
						if d.Name == "pk" {
							found = true
							break
						}
					}
					if !found {
						t.Errorf("expected pk directive on field %q", pkField)
					}
				}
			}
			// Check foreign key directive if applicable
			if tt.fkDirective {
				found := false
				for _, d := range def.Directives {
					if d.Name == "References" {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("expected References directive for foreign key")
				}
			}
		})
	}
}

func TestDBSchemaInfo_Definitions(t *testing.T) {
	tests := []struct {
		name      string
		schema    DBSchemaInfo
		wantNames []string
		wantKinds []ast.DefinitionKind
		wantErr   bool
	}{
		{
			name: "empty schema returns empty list",
			schema: DBSchemaInfo{
				Name:        "public",
				Description: "empty schema",
				Tables:      nil,
				Views:       nil,
			},
			wantNames: nil,
			wantKinds: nil,
			wantErr:   false,
		},
		{
			name: "schema with one valid table",
			schema: DBSchemaInfo{
				Name: "public",
				Tables: []DBTableInfo{
					{
						Name:       "users",
						SchemaName: "public",
						Columns: []DBColumnInfo{
							{Name: "id", DataType: "INT", IsNullable: false},
						},
					},
				},
			},
			wantNames: []string{"users"},
			wantKinds: []ast.DefinitionKind{ast.Object},
			wantErr:   false,
		},
		{
			name: "schema with one valid view",
			schema: DBSchemaInfo{
				Name: "public",
				Views: []DBViewInfo{
					{
						Name:       "active_users",
						SchemaName: "public",
						Columns: []DBColumnInfo{
							{Name: "id", DataType: "INT", IsNullable: false},
						},
					},
				},
			},
			wantNames: []string{"active_users"},
			wantKinds: []ast.DefinitionKind{ast.Object},
			wantErr:   false,
		},
		{
			name: "schema with table and view",
			schema: DBSchemaInfo{
				Name: "public",
				Tables: []DBTableInfo{
					{
						Name:       "users",
						SchemaName: "public",
						Columns: []DBColumnInfo{
							{Name: "id", DataType: "INT", IsNullable: false},
						},
					},
				},
				Views: []DBViewInfo{
					{
						Name:       "active_users",
						SchemaName: "public",
						Columns: []DBColumnInfo{
							{Name: "id", DataType: "INT", IsNullable: false},
						},
					},
				},
			},
			wantNames: []string{"users", "active_users"},
			wantKinds: []ast.DefinitionKind{ast.Object, ast.Object},
			wantErr:   false,
		},
		{
			name: "schema with invalid table and valid view",
			schema: DBSchemaInfo{
				Name: "public",
				Tables: []DBTableInfo{
					{
						Name:       "invalid",
						SchemaName: "public",
						Columns: []DBColumnInfo{
							{Name: "foo", DataType: "UNKNOWN", IsNullable: true},
						},
					},
				},
				Views: []DBViewInfo{
					{
						Name:       "v1",
						SchemaName: "public",
						Columns: []DBColumnInfo{
							{Name: "id", DataType: "INT", IsNullable: false},
						},
					},
				},
			},
			wantNames: []string{"v1"},
			wantKinds: []ast.DefinitionKind{ast.Object},
			wantErr:   false,
		},
		{
			name: "schema with table returning error",
			schema: DBSchemaInfo{
				Name: "public",
				Tables: []DBTableInfo{
					{
						Name:       "errtable",
						SchemaName: "public",
						Columns: []DBColumnInfo{
							{Name: "id", DataType: "INT", IsNullable: false},
						},
					},
				},
			},
			wantNames: []string{"errtable"},
			wantKinds: []ast.DefinitionKind{ast.Object},
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defs, err := tt.schema.Definitions()
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(defs) != len(tt.wantNames) {
				t.Errorf("got %d definitions, want %d", len(defs), len(tt.wantNames))
			}
			for i, wantName := range tt.wantNames {
				if i >= len(defs) {
					t.Errorf("missing definition for %q", wantName)
					continue
				}
				if defs[i].Name != identGraphQL(wantName) {
					t.Errorf("definition[%d] got Name %q, want %q", i, defs[i].Name, identGraphQL(wantName))
				}
				if defs[i].Kind != tt.wantKinds[i] {
					t.Errorf("definition[%d] got Kind %v, want %v", i, defs[i].Kind, tt.wantKinds[i])
				}
			}
		})
	}
}
