package ducklake

import (
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

func TestDuckDBTypeToGraphQL(t *testing.T) {
	tests := []struct {
		name     string
		duckType string
		wantName string // expected GraphQL type name, empty string means nil
		wantList bool   // expected to be a list type
	}{
		// Integer types → Int
		{"INT", "INT", "Int", false},
		{"INTEGER", "INTEGER", "Int", false},
		{"INT4", "INT4", "Int", false},
		{"SIGNED", "SIGNED", "Int", false},
		{"SMALLINT", "SMALLINT", "Int", false},
		{"INT2", "INT2", "Int", false},
		{"SHORT", "SHORT", "Int", false},
		{"TINYINT", "TINYINT", "Int", false},
		{"INT8", "INT8", "Int", false},
		{"UTINYINT", "UTINYINT", "Int", false},
		{"USMALLINT", "USMALLINT", "Int", false},

		// Big integer types → BigInt
		{"BIGINT", "BIGINT", "BigInt", false},
		{"LONG", "LONG", "BigInt", false},
		{"HUGEINT", "HUGEINT", "BigInt", false},
		{"UINTEGER", "UINTEGER", "BigInt", false},
		{"UBIGINT", "UBIGINT", "BigInt", false},
		{"UHUGEINT", "UHUGEINT", "BigInt", false},

		// Float types → Float
		{"FLOAT", "FLOAT", "Float", false},
		{"REAL", "REAL", "Float", false},
		{"FLOAT4", "FLOAT4", "Float", false},
		{"DOUBLE", "DOUBLE", "Float", false},
		{"FLOAT8", "FLOAT8", "Float", false},
		{"DECIMAL", "DECIMAL", "Float", false},
		{"NUMERIC", "NUMERIC", "Float", false},
		{"DECIMAL(10,2)", "DECIMAL(10,2)", "Float", false},

		// String types → String
		{"VARCHAR", "VARCHAR", "String", false},
		{"TEXT", "TEXT", "String", false},
		{"STRING", "STRING", "String", false},
		{"CHAR", "CHAR", "String", false},
		{"BPCHAR", "BPCHAR", "String", false},
		{"VARCHAR(255)", "VARCHAR(255)", "String", false},

		// UUID / BLOB → String
		{"UUID", "UUID", "String", false},
		{"BLOB", "BLOB", "String", false},
		{"BYTEA", "BYTEA", "String", false},
		{"BINARY", "BINARY", "String", false},
		{"VARBINARY", "VARBINARY", "String", false},

		// Bit types → String
		{"BIT", "BIT", "String", false},
		{"BITSTRING", "BITSTRING", "String", false},

		// Boolean → Boolean
		{"BOOLEAN", "BOOLEAN", "Boolean", false},
		{"BOOL", "BOOL", "Boolean", false},
		{"LOGICAL", "LOGICAL", "Boolean", false},

		// Timestamp types → Timestamp
		{"TIMESTAMP", "TIMESTAMP", "Timestamp", false},
		{"DATETIME", "DATETIME", "Timestamp", false},
		{"TIMESTAMPTZ", "TIMESTAMPTZ", "Timestamp", false},
		{"TIMESTAMP WITH TIME ZONE", "TIMESTAMP WITH TIME ZONE", "Timestamp", false},
		{"TIMESTAMP_S", "TIMESTAMP_S", "Timestamp", false},
		{"TIMESTAMP_MS", "TIMESTAMP_MS", "Timestamp", false},
		{"TIMESTAMP_NS", "TIMESTAMP_NS", "Timestamp", false},

		// Date → Date
		{"DATE", "DATE", "Date", false},

		// Time types → Time
		{"TIME", "TIME", "Time", false},
		{"TIMETZ", "TIMETZ", "Time", false},
		{"TIME WITH TIME ZONE", "TIME WITH TIME ZONE", "Time", false},

		// Interval → Interval
		{"INTERVAL", "INTERVAL", "Interval", false},

		// JSON → JSON
		{"JSON", "JSON", "JSON", false},

		// Geometry types → Geometry
		{"GEOMETRY", "GEOMETRY", "Geometry", false},
		{"GEOGRAPHY", "GEOGRAPHY", "Geometry", false},
		{"WKB_BLOB", "WKB_BLOB", "Geometry", false},

		// Enum → String
		{"ENUM", "ENUM", "String", false},

		// Complex types → JSON
		{"STRUCT", "STRUCT", "JSON", false},
		{"MAP", "MAP", "JSON", false},
		{"UNION", "UNION", "JSON", false},

		// Array types
		{"INTEGER[]", "INTEGER[]", "Int", true},
		{"VARCHAR[]", "VARCHAR[]", "String", true},
		{"BIGINT[]", "BIGINT[]", "BigInt", true},
		{"BOOLEAN[]", "BOOLEAN[]", "Boolean", true},

		// DuckDB internal physical type names (used by DuckLake metadata)
		{"int32", "int32", "Int", false},
		{"int64", "int64", "BigInt", false},
		{"int16", "int16", "Int", false},
		{"float32", "float32", "Float", false},
		{"float64", "float64", "Float", false},

		// Case insensitivity (function upper-cases internally)
		{"lowercase int", "int", "Int", false},
		{"lowercase varchar", "varchar", "String", false},

		// Unknown type → nil
		{"UNKNOWN_TYPE", "UNKNOWN_TYPE", "", false},
		{"UNKNOWN[]", "UNKNOWN[]", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := duckDBTypeToGraphQL(tt.duckType)

			if tt.wantName == "" && !tt.wantList {
				if got != nil {
					t.Errorf("duckDBTypeToGraphQL(%q) = %v, want nil", tt.duckType, got)
				}
				return
			}

			// For UNKNOWN[] the element type is nil, so the function returns nil
			if tt.wantName == "" && tt.wantList {
				if got != nil {
					t.Errorf("duckDBTypeToGraphQL(%q) = %v, want nil", tt.duckType, got)
				}
				return
			}

			if got == nil {
				t.Fatalf("duckDBTypeToGraphQL(%q) = nil, want %s", tt.duckType, tt.wantName)
			}

			if tt.wantList {
				// List type: got.Elem should be the inner type
				if got.Elem == nil {
					t.Fatalf("duckDBTypeToGraphQL(%q) is not a list type", tt.duckType)
				}
				if got.Elem.NamedType != tt.wantName {
					t.Errorf("duckDBTypeToGraphQL(%q).Elem.NamedType = %q, want %q", tt.duckType, got.Elem.NamedType, tt.wantName)
				}
			} else {
				if got.NamedType != tt.wantName {
					t.Errorf("duckDBTypeToGraphQL(%q).NamedType = %q, want %q", tt.duckType, got.NamedType, tt.wantName)
				}
			}
		})
	}
}

func TestDuckDBTypeToGraphQL_Nullability(t *testing.T) {
	// The function itself does not set NonNull — that is handled by columnToFieldDef.
	// Verify the returned type has NonNull = false by default.
	got := duckDBTypeToGraphQL("INTEGER")
	if got == nil {
		t.Fatal("expected non-nil type")
	}
	if got.NonNull {
		t.Error("expected NonNull to be false by default")
	}
}

func TestTableToDefinition(t *testing.T) {
	t.Run("basic table in main schema", func(t *testing.T) {
		tbl := DuckLakeTable{
			SchemaName: "main",
			TableName:  "users",
			Columns: []DuckLakeColumn{
				{Name: "id", Type: "INTEGER", IsNullable: false, IsPK: true},
				{Name: "name", Type: "VARCHAR", IsNullable: true, IsPK: false},
			},
		}

		def := tableToDefinition(tbl)
		if def == nil {
			t.Fatal("expected non-nil definition")
		}

		// Type name: main schema is skipped, so just "users"
		if def.Name != "users" {
			t.Errorf("Name = %q, want %q", def.Name, "users")
		}

		// @table directive
		tableDir := findDirective(def.Directives, "table")
		if tableDir == nil {
			t.Fatal("expected @table directive")
		}
		nameArg := tableDir.Arguments.ForName("name")
		if nameArg == nil || nameArg.Value.Raw != "users" {
			t.Errorf("@table name = %v, want %q", nameArg, "users")
		}

		// No @module for main schema
		moduleDir := findDirective(def.Directives, "module")
		if moduleDir != nil {
			t.Error("expected no @module directive for main schema")
		}

		// Fields
		if len(def.Fields) != 2 {
			t.Fatalf("expected 2 fields, got %d", len(def.Fields))
		}

		// Check PK on id field
		idField := def.Fields[0]
		pkDir := findDirective(idField.Directives, "pk")
		if pkDir == nil {
			t.Error("expected @pk directive on id field")
		}
		// id is not nullable → NonNull should be true
		if !idField.Type.NonNull {
			t.Error("expected id field to be NonNull")
		}

		// name is nullable → NonNull should be false
		nameField := def.Fields[1]
		if nameField.Type.NonNull {
			t.Error("expected name field to be nullable")
		}
	})

	t.Run("non-default schema adds module directive", func(t *testing.T) {
		tbl := DuckLakeTable{
			SchemaName: "analytics",
			TableName:  "events",
			Columns: []DuckLakeColumn{
				{Name: "id", Type: "BIGINT", IsNullable: false},
			},
		}

		def := tableToDefinition(tbl)
		if def == nil {
			t.Fatal("expected non-nil definition")
		}

		// Type name should be "analytics_events" (dot replaced by underscore)
		if def.Name != "analytics_events" {
			t.Errorf("Name = %q, want %q", def.Name, "analytics_events")
		}

		// @table directive should have "analytics.events"
		tableDir := findDirective(def.Directives, "table")
		if tableDir == nil {
			t.Fatal("expected @table directive")
		}
		nameArg := tableDir.Arguments.ForName("name")
		if nameArg == nil || nameArg.Value.Raw != "analytics.events" {
			t.Errorf("@table name = %v, want %q", nameArg, "analytics.events")
		}

		// @module directive should be present
		moduleDir := findDirective(def.Directives, "module")
		if moduleDir == nil {
			t.Fatal("expected @module directive")
		}
		modNameArg := moduleDir.Arguments.ForName("name")
		if modNameArg == nil || modNameArg.Value.Raw != "analytics" {
			t.Errorf("@module name = %v, want %q", modNameArg, "analytics")
		}
	})

	t.Run("sanitized column name gets field_source", func(t *testing.T) {
		tbl := DuckLakeTable{
			SchemaName: "main",
			TableName:  "data",
			Columns: []DuckLakeColumn{
				{Name: "my-col.name", Type: "VARCHAR", IsNullable: true},
			},
		}

		def := tableToDefinition(tbl)
		if def == nil {
			t.Fatal("expected non-nil definition")
		}

		field := def.Fields[0]
		if field.Name != "my_col_name" {
			t.Errorf("field Name = %q, want %q", field.Name, "my_col_name")
		}

		fsDir := findDirective(field.Directives, "field_source")
		if fsDir == nil {
			t.Error("expected @field_source directive for sanitized name")
		}
	})

	t.Run("empty columns returns nil", func(t *testing.T) {
		tbl := DuckLakeTable{
			SchemaName: "main",
			TableName:  "empty",
			Columns:    nil,
		}
		if def := tableToDefinition(tbl); def != nil {
			t.Error("expected nil for table with no columns")
		}
	})

	t.Run("all unknown column types returns nil", func(t *testing.T) {
		tbl := DuckLakeTable{
			SchemaName: "main",
			TableName:  "weird",
			Columns: []DuckLakeColumn{
				{Name: "col1", Type: "UNKNOWNTYPE", IsNullable: true},
			},
		}
		if def := tableToDefinition(tbl); def != nil {
			t.Error("expected nil when all columns have unknown types")
		}
	})
}

func TestIdentGraphQL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"normal name", "users", "users"},
		{"already valid", "my_table_1", "my_table_1"},
		{"dot becomes underscore", "schema.table", "schema_table"},
		{"hyphen becomes underscore", "my-table", "my_table"},
		{"leading underscore gets prefix", "_hidden", "dl_hidden"},
		{"multiple special chars", "a.b-c", "a_b_c"},
		{"all special", "...", "dl___"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := identGraphQL(tt.in)
			if got != tt.want {
				t.Errorf("identGraphQL(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestContentHash(t *testing.T) {
	t.Run("same tables produce same hash", func(t *testing.T) {
		tables := []DuckLakeTable{
			{
				SchemaName: "main",
				TableName:  "users",
				Columns: []DuckLakeColumn{
					{Name: "id", Type: "INTEGER", IsNullable: false, IsPK: true},
					{Name: "name", Type: "VARCHAR", IsNullable: true},
				},
			},
		}

		h1 := ContentHash(tables)
		h2 := ContentHash(tables)
		if h1 != h2 {
			t.Errorf("same input produced different hashes: %q vs %q", h1, h2)
		}
		if h1 == "" {
			t.Error("hash should not be empty")
		}
	})

	t.Run("different tables produce different hash", func(t *testing.T) {
		tables1 := []DuckLakeTable{
			{SchemaName: "main", TableName: "users", Columns: []DuckLakeColumn{
				{Name: "id", Type: "INTEGER"},
			}},
		}
		tables2 := []DuckLakeTable{
			{SchemaName: "main", TableName: "orders", Columns: []DuckLakeColumn{
				{Name: "id", Type: "INTEGER"},
			}},
		}

		h1 := ContentHash(tables1)
		h2 := ContentHash(tables2)
		if h1 == h2 {
			t.Error("different tables should produce different hashes")
		}
	})

	t.Run("order independent", func(t *testing.T) {
		tablesA := []DuckLakeTable{
			{SchemaName: "main", TableName: "a", Columns: []DuckLakeColumn{{Name: "id", Type: "INT"}}},
			{SchemaName: "main", TableName: "b", Columns: []DuckLakeColumn{{Name: "id", Type: "INT"}}},
		}
		tablesB := []DuckLakeTable{
			{SchemaName: "main", TableName: "b", Columns: []DuckLakeColumn{{Name: "id", Type: "INT"}}},
			{SchemaName: "main", TableName: "a", Columns: []DuckLakeColumn{{Name: "id", Type: "INT"}}},
		}

		h1 := ContentHash(tablesA)
		h2 := ContentHash(tablesB)
		if h1 != h2 {
			t.Errorf("order should not matter: %q vs %q", h1, h2)
		}
	})

	t.Run("column difference changes hash", func(t *testing.T) {
		tables1 := []DuckLakeTable{
			{SchemaName: "main", TableName: "t", Columns: []DuckLakeColumn{
				{Name: "id", Type: "INTEGER", IsNullable: false, IsPK: true},
			}},
		}
		tables2 := []DuckLakeTable{
			{SchemaName: "main", TableName: "t", Columns: []DuckLakeColumn{
				{Name: "id", Type: "INTEGER", IsNullable: true, IsPK: false},
			}},
		}

		h1 := ContentHash(tables1)
		h2 := ContentHash(tables2)
		if h1 == h2 {
			t.Error("different column properties should produce different hashes")
		}
	})
}

func TestDataObjectName(t *testing.T) {
	tests := []struct {
		schema, name, want string
	}{
		{"main", "users", "users"},
		{"", "users", "users"},
		{"analytics", "events", "analytics.events"},
	}
	for _, tt := range tests {
		got := dataObjectName(tt.schema, tt.name)
		if got != tt.want {
			t.Errorf("dataObjectName(%q, %q) = %q, want %q", tt.schema, tt.name, got, tt.want)
		}
	}
}

func TestGenerateSchemaDocument(t *testing.T) {
	tables := []DuckLakeTable{
		{
			SchemaName: "main",
			TableName:  "users",
			Columns: []DuckLakeColumn{
				{Name: "id", Type: "INTEGER", IsNullable: false, IsPK: true},
			},
		},
		{
			SchemaName: "main",
			TableName:  "empty_table",
			Columns:    nil, // should be skipped
		},
	}

	doc := GenerateSchemaDocument(tables)
	if len(doc.Definitions) != 1 {
		t.Fatalf("expected 1 definition, got %d", len(doc.Definitions))
	}
	if doc.Definitions[0].Name != "users" {
		t.Errorf("definition name = %q, want %q", doc.Definitions[0].Name, "users")
	}
}

func TestMetaCatalogIdent(t *testing.T) {
	tests := []struct {
		prefix string
		want   string
	}{
		{"my_lake", `"__ducklake_metadata_my_lake"`},
		{"my.lake", `"__ducklake_metadata_my.lake"`},
		{"data-lake", `"__ducklake_metadata_data-lake"`},
		{"simple", `"__ducklake_metadata_simple"`},
	}
	for _, tt := range tests {
		got := metaCatalogIdent(tt.prefix)
		if got != tt.want {
			t.Errorf("metaCatalogIdent(%q) = %q, want %q", tt.prefix, got, tt.want)
		}
	}
}

// findDirective is a test helper that finds a directive by name.
func findDirective(dirs []*ast.Directive, name string) *ast.Directive {
	for _, d := range dirs {
		if d.Name == name {
			return d
		}
	}
	return nil
}
