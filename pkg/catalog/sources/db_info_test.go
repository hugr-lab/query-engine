package sources

import (
	"context"
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

func TestHasSchemaModule(t *testing.T) {
	tests := []struct {
		schema        string
		defaultSchema string
		want          bool
	}{
		{"public", "", false},
		{"main", "", false},
		{"crm", "", true},
		{"crm", "crm", false},
		{"audit", "crm", true},
		{"", "crm", false},
	}
	for _, tt := range tests {
		if got := hasSchemaModule(tt.schema, tt.defaultSchema); got != tt.want {
			t.Errorf("hasSchemaModule(%q, %q) = %v, want %v", tt.schema, tt.defaultSchema, got, tt.want)
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
			{Name: "crm", Tables: []DBTableInfo{{Name: "clients", SchemaName: "crm", Columns: cols}}},
			{Name: "audit", Tables: []DBTableInfo{{Name: "logs", SchemaName: "audit", Columns: cols}}},
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

	def, ok = byName["audit_logs"]
	if !ok {
		t.Fatalf("expected type name %q for the non-default schema table, got types: %v", "audit_logs", keysOf(byName))
	}
	if m := def.Directives.ForName("module"); m == nil || m.Arguments.ForName("name").Value.Raw != "audit" {
		t.Errorf("non-default schema table must keep its @module(name: \"audit\") directive")
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
