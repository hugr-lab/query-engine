package sources

import (
	"context"
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

func TestIsDefaultSchema(t *testing.T) {
	tests := []struct {
		schema        string
		defaultSchema string
		want          bool
	}{
		{"public", "", true},
		{"main", "", true},
		{"crm", "", false},
		{"crm", "crm", true},
		{"audit", "crm", false},
		{"", "crm", true},
	}
	for _, tt := range tests {
		if got := isDefaultSchema(tt.schema, tt.defaultSchema); got != tt.want {
			t.Errorf("isDefaultSchema(%q, %q) = %v, want %v", tt.schema, tt.defaultSchema, got, tt.want)
		}
	}
}

func TestRawObjectName(t *testing.T) {
	tests := []struct {
		schema        string
		name          string
		defaultSchema string
		want          string
	}{
		{"public", "users", "", "users"},
		{"crm", "clients", "", "crm.clients"},
		{"crm", "clients", "crm", "clients"},
		{"audit", "logs", "crm", "audit.logs"},
	}
	for _, tt := range tests {
		if got := rawObjectName(tt.schema, tt.name, tt.defaultSchema); got != tt.want {
			t.Errorf("rawObjectName(%q, %q, %q) = %q, want %q", tt.schema, tt.name, tt.defaultSchema, got, tt.want)
		}
	}
}

func TestSchemaDocumentDefaultSchema(t *testing.T) {
	cols := []DBColumnInfo{{Name: "id", DataType: "INTEGER"}}
	info := DBInfo{
		InfoName: "crm_db",
		Type:     "mysql",
		SchemaInfo: []DBSchemaInfo{
			{Name: "crm", Tables: []DBTableInfo{{
				Name: "clients", SchemaName: "crm", Columns: cols,
				Constraints: []DBConstraintInfo{{
					Name: "clients_log_fk", Type: "FOREIGN KEY",
					Columns:          []string{"id"},
					ReferencesSchema: "audit", ReferencesTable: "logs",
					ReferencesColumns: []string{"id"},
				}},
			}}},
			{Name: "audit", Tables: []DBTableInfo{{
				Name: "logs", SchemaName: "audit", Columns: cols,
				Constraints: []DBConstraintInfo{{
					Name: "logs_client_fk", Type: "FOREIGN KEY",
					Columns:          []string{"id"},
					ReferencesSchema: "crm", ReferencesTable: "clients",
					ReferencesColumns: []string{"id"},
				}},
			}}},
		},
		DefaultSchema: "crm",
	}

	doc, err := info.schemaDocument(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	byName := map[string]*ast.Definition{}
	for _, def := range doc.Definitions {
		byName[def.Name] = def
	}

	def, ok := byName["clients"]
	if !ok {
		t.Fatalf("expected type name %q for the default schema table, got types: %v", "clients", keysOf(byName))
	}
	if def.Directives.ForName("module") != nil {
		t.Errorf("default schema table must not get a @module directive")
	}
	if got := def.Directives.ForName("table").Arguments.ForName("name").Value.Raw; got != "crm.clients" {
		t.Errorf("@table(name:) must stay schema-qualified, got %q", got)
	}
	// FK from the default schema to a non-default schema: references_name must
	// be the generated GraphQL type name of the target.
	if ref := def.Directives.ForName("references"); ref == nil {
		t.Errorf("default schema table must keep its @references directive")
	} else if got := ref.Arguments.ForName("references_name").Value.Raw; got != "audit_logs" {
		t.Errorf("@references(references_name:) = %q, want %q", got, "audit_logs")
	}

	def, ok = byName["audit_logs"]
	if !ok {
		t.Fatalf("expected type name %q for the non-default schema table, got types: %v", "audit_logs", keysOf(byName))
	}
	if m := def.Directives.ForName("module"); m == nil || m.Arguments.ForName("name").Value.Raw != "audit" {
		t.Errorf("non-default schema table must keep its @module(name: \"audit\") directive")
	}
	// FK to a default schema table: references_name must be the unprefixed
	// GraphQL type name, not the schema-qualified data object name.
	if ref := def.Directives.ForName("references"); ref == nil {
		t.Errorf("non-default schema table must keep its @references directive")
	} else if got := ref.Arguments.ForName("references_name").Value.Raw; got != "clients" {
		t.Errorf("@references(references_name:) = %q, want %q", got, "clients")
	}
}

func TestContentHashDefaultSchema(t *testing.T) {
	info := DBInfo{Type: "mysql", SchemaInfo: []DBSchemaInfo{{Name: "crm"}}}
	base := info.contentHash()
	info.DefaultSchema = "crm"
	if info.contentHash() == base {
		t.Error("contentHash must change when DefaultSchema changes")
	}
}

func keysOf(m map[string]*ast.Definition) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
