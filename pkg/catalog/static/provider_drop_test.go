//go:build duckdb_arrow

package static

import (
	"context"
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

// moduleCatalogDir creates a @module_catalog(name: ...) directive.
func moduleCatalogDir(name string) *ast.Directive {
	return &ast.Directive{
		Name: "module_catalog",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue}},
		},
	}
}

// catalogDir creates a @catalog(name: ...) directive.
func catalogDir(name string) *ast.Directive {
	return &ast.Directive{
		Name: "catalog",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue}},
		},
	}
}

// moduleRootDir creates a @module_root(name: ..., type: QUERY) directive.
func moduleRootDir(name string) *ast.Directive {
	return &ast.Directive{
		Name: "module_root",
		Arguments: ast.ArgumentList{
			{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue}},
			{Name: "type", Value: &ast.Value{Raw: "QUERY", Kind: ast.EnumValue}},
		},
	}
}

func TestDropCatalog_OrphanCleanup(t *testing.T) {
	provider, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	schema := provider.Schema()
	if schema.Query == nil {
		t.Fatal("expected Query type to exist after New()")
	}

	// --- Set up cat_a (module: cat_a) ---
	schema.Types["Table1"] = &ast.Definition{
		Kind: ast.Object, Name: "Table1",
		Directives: ast.DirectiveList{catalogDir("cat_a")},
		Fields: ast.FieldList{
			{Name: "id", Type: ast.NamedType("Int", nil)},
		},
	}
	schema.Types["_module_cat_a_query"] = &ast.Definition{
		Kind: ast.Object, Name: "_module_cat_a_query",
		Directives: ast.DirectiveList{moduleRootDir("cat_a"), moduleCatalogDir("cat_a")},
		Fields: ast.FieldList{
			{Name: "table1", Type: ast.NamedType("Table1", nil),
				Directives: ast.DirectiveList{catalogDir("cat_a")}},
		},
	}
	schema.Query.Fields = append(schema.Query.Fields, &ast.FieldDefinition{
		Name: "cat_a", Type: ast.NamedType("_module_cat_a_query", nil),
		Directives: ast.DirectiveList{moduleCatalogDir("cat_a")},
	})

	// --- Set up cat_b (module: cat_b) ---
	schema.Types["Table2"] = &ast.Definition{
		Kind: ast.Object, Name: "Table2",
		Directives: ast.DirectiveList{catalogDir("cat_b")},
		Fields: ast.FieldList{
			{Name: "id", Type: ast.NamedType("Int", nil)},
		},
	}
	schema.Types["_module_cat_b_query"] = &ast.Definition{
		Kind: ast.Object, Name: "_module_cat_b_query",
		Directives: ast.DirectiveList{moduleRootDir("cat_b"), moduleCatalogDir("cat_b")},
		Fields: ast.FieldList{
			{Name: "table2", Type: ast.NamedType("Table2", nil),
				Directives: ast.DirectiveList{catalogDir("cat_b")}},
		},
	}
	schema.Query.Fields = append(schema.Query.Fields, &ast.FieldDefinition{
		Name: "cat_b", Type: ast.NamedType("_module_cat_b_query", nil),
		Directives: ast.DirectiveList{moduleCatalogDir("cat_b")},
	})

	// --- Drop cat_a ---
	ctx := context.Background()
	if err := provider.DropCatalog(ctx, "cat_a", true); err != nil {
		t.Fatalf("DropCatalog(cat_a): %v", err)
	}

	errs := provider.ValidateSchema()
	for _, e := range errs {
		t.Errorf("validation error after drop: %v", e)
	}
	if len(errs) > 0 {
		t.Fatal("expected no validation errors after DropCatalog")
	}

	if _, ok := schema.Types["Table1"]; ok {
		t.Error("Table1 should have been dropped with cat_a")
	}
	if _, ok := schema.Types["_module_cat_a_query"]; ok {
		t.Error("_module_cat_a_query should have been dropped (no module_catalog left)")
	}
	if schema.Query.Fields.ForName("cat_a") != nil {
		t.Error("Query.cat_a should have been removed (no module_catalog left)")
	}

	if _, ok := schema.Types["Table2"]; !ok {
		t.Error("Table2 should still exist after dropping cat_a")
	}
	if _, ok := schema.Types["_module_cat_b_query"]; !ok {
		t.Error("_module_cat_b_query should still exist")
	}
	if schema.Query.Fields.ForName("cat_b") == nil {
		t.Error("Query.cat_b should still exist")
	}
}

func TestDropCatalog_SharedModule(t *testing.T) {
	provider, err := New()
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	schema := provider.Schema()
	if schema.Query == nil {
		t.Fatal("expected Query type to exist after New()")
	}

	// --- Set up shared module: cat_a has shared.a, cat_b has shared.b ---
	// Data types
	schema.Types["TableA"] = &ast.Definition{
		Kind: ast.Object, Name: "TableA",
		Directives: ast.DirectiveList{catalogDir("cat_a")},
		Fields:     ast.FieldList{{Name: "id", Type: ast.NamedType("Int", nil)}},
	}
	schema.Types["TableB"] = &ast.Definition{
		Kind: ast.Object, Name: "TableB",
		Directives: ast.DirectiveList{catalogDir("cat_b")},
		Fields:     ast.FieldList{{Name: "id", Type: ast.NamedType("Int", nil)}},
	}

	// Leaf module types
	schema.Types["_module_shared_a_query"] = &ast.Definition{
		Kind: ast.Object, Name: "_module_shared_a_query",
		Directives: ast.DirectiveList{moduleRootDir("shared.a"), moduleCatalogDir("cat_a")},
		Fields: ast.FieldList{
			{Name: "tableA", Type: ast.NamedType("TableA", nil),
				Directives: ast.DirectiveList{catalogDir("cat_a")}},
		},
	}
	schema.Types["_module_shared_b_query"] = &ast.Definition{
		Kind: ast.Object, Name: "_module_shared_b_query",
		Directives: ast.DirectiveList{moduleRootDir("shared.b"), moduleCatalogDir("cat_b")},
		Fields: ast.FieldList{
			{Name: "tableB", Type: ast.NamedType("TableB", nil),
				Directives: ast.DirectiveList{catalogDir("cat_b")}},
		},
	}

	// Shared parent module type — both catalogs contribute
	schema.Types["_module_shared_query"] = &ast.Definition{
		Kind: ast.Object, Name: "_module_shared_query",
		Directives: ast.DirectiveList{
			moduleRootDir("shared"),
			moduleCatalogDir("cat_a"),
			moduleCatalogDir("cat_b"),
		},
		Fields: ast.FieldList{
			{Name: "a", Type: ast.NamedType("_module_shared_a_query", nil),
				Directives: ast.DirectiveList{moduleCatalogDir("cat_a")}},
			{Name: "b", Type: ast.NamedType("_module_shared_b_query", nil),
				Directives: ast.DirectiveList{moduleCatalogDir("cat_b")}},
		},
	}

	// Root wiring field — both catalogs
	schema.Query.Fields = append(schema.Query.Fields, &ast.FieldDefinition{
		Name: "shared", Type: ast.NamedType("_module_shared_query", nil),
		Directives: ast.DirectiveList{
			moduleCatalogDir("cat_a"),
			moduleCatalogDir("cat_b"),
		},
	})

	ctx := context.Background()

	// --- Drop cat_a: shared module should survive for cat_b ---
	if err := provider.DropCatalog(ctx, "cat_a", true); err != nil {
		t.Fatalf("DropCatalog(cat_a): %v", err)
	}

	errs := provider.ValidateSchema()
	for _, e := range errs {
		t.Errorf("validation error after dropping cat_a: %v", e)
	}
	if len(errs) > 0 {
		t.Fatal("expected no validation errors")
	}

	// cat_a specific artifacts gone
	if _, ok := schema.Types["TableA"]; ok {
		t.Error("TableA should be gone")
	}
	if _, ok := schema.Types["_module_shared_a_query"]; ok {
		t.Error("_module_shared_a_query should be gone (only cat_a)")
	}

	// Shared parent should survive
	if _, ok := schema.Types["_module_shared_query"]; !ok {
		t.Error("_module_shared_query should survive (still has cat_b)")
	}
	sharedDef := schema.Types["_module_shared_query"]
	if sharedDef != nil {
		if sharedDef.Fields.ForName("a") != nil {
			t.Error("field 'a' should be removed from _module_shared_query (only cat_a)")
		}
		if sharedDef.Fields.ForName("b") == nil {
			t.Error("field 'b' should survive on _module_shared_query (cat_b)")
		}
	}

	// Root wiring should survive
	if schema.Query.Fields.ForName("shared") == nil {
		t.Error("Query.shared should survive (still has cat_b)")
	}

	// cat_b artifacts intact
	if _, ok := schema.Types["TableB"]; !ok {
		t.Error("TableB should still exist")
	}
	if _, ok := schema.Types["_module_shared_b_query"]; !ok {
		t.Error("_module_shared_b_query should still exist")
	}

	// --- Drop cat_b: everything should be cleaned up ---
	if err := provider.DropCatalog(ctx, "cat_b", true); err != nil {
		t.Fatalf("DropCatalog(cat_b): %v", err)
	}

	errs = provider.ValidateSchema()
	for _, e := range errs {
		t.Errorf("validation error after dropping cat_b: %v", e)
	}
	if len(errs) > 0 {
		t.Fatal("expected no validation errors after dropping both catalogs")
	}

	if _, ok := schema.Types["TableB"]; ok {
		t.Error("TableB should be gone after dropping cat_b")
	}
	if _, ok := schema.Types["_module_shared_b_query"]; ok {
		t.Error("_module_shared_b_query should be gone")
	}
	if _, ok := schema.Types["_module_shared_query"]; ok {
		t.Error("_module_shared_query should be gone (no catalogs left)")
	}
	if schema.Query.Fields.ForName("shared") != nil {
		t.Error("Query.shared should be gone (no catalogs left)")
	}
}
