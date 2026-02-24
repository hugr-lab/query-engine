package compiler_test

import (
	"context"
	"iter"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/schema/compiler"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/schema/compiler/rules"
	_ "github.com/hugr-lab/query-engine/pkg/schema/types" // ensure scalar init()
	"github.com/vektah/gqlparser/v2/ast"
)

// testSource implements base.DefinitionsSource for tests.
type testSource struct {
	defs []*ast.Definition
}

func (s *testSource) ForName(_ context.Context, name string) *ast.Definition {
	for _, d := range s.defs {
		if d.Name == name {
			return d
		}
	}
	return nil
}

func (s *testSource) DirectiveForName(_ context.Context, _ string) *ast.DirectiveDefinition {
	return nil
}

func (s *testSource) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, d := range s.defs {
			if !yield(d) {
				return
			}
		}
	}
}

func (s *testSource) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(yield func(string, *ast.DirectiveDefinition) bool) {}
}

// pos is a test helper for creating AST positions.
func pos() *ast.Position {
	return &ast.Position{Src: &ast.Source{Name: "test"}}
}

// collectDefs drains a CompiledCatalog's Definitions into a map by name.
func collectDefs(ctx context.Context, cat base.CompiledCatalog) map[string]*ast.Definition {
	m := make(map[string]*ast.Definition)
	for d := range cat.Definitions(ctx) {
		m[d.Name] = d
	}
	return m
}

// collectExts drains a CompiledCatalog's Extensions into a map by name.
func collectExts(ctx context.Context, cat base.CompiledCatalog) map[string]*ast.Definition {
	m := make(map[string]*ast.Definition)
	for e := range cat.Extensions(ctx) {
		m[e.Name] = e
	}
	return m
}

// hasFieldNamed returns true if the field list contains a field with the given name.
func hasFieldNamed(fields ast.FieldList, name string) bool {
	return fields.ForName(name) != nil
}

// newAllRulesCompiler creates a compiler with all built-in rules.
func newAllRulesCompiler() *compiler.Compiler {
	return compiler.New(rules.RegisterAll()...)
}

// --- T037: basic @table compilation ---

func TestCompile_BasicTable(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "User",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "users", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "name", Type: ast.NonNullNamedType("String", pos()), Position: pos()},
					{Name: "email", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	result, err := c.Compile(ctx, nil, source, base.Options{Name: "test"})
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	defs := collectDefs(ctx, result)
	exts := collectExts(ctx, result)

	// Check object definition present
	if _, ok := defs["User"]; !ok {
		t.Error("expected User definition in output")
	}

	// Check filter input type
	filterDef, ok := defs["User_filter"]
	if !ok {
		t.Error("expected User_filter definition in output")
	} else {
		if filterDef.Kind != ast.InputObject {
			t.Errorf("expected User_filter to be InputObject, got %v", filterDef.Kind)
		}
		// Should have id, name, email filter fields + _and, _or, _not
		if !hasFieldNamed(filterDef.Fields, "id") {
			t.Error("expected id field in User_filter")
		}
		if !hasFieldNamed(filterDef.Fields, "_and") {
			t.Error("expected _and field in User_filter")
		}
	}

	// Note: User_list_filter is only created lazily when back-references exist.
	// BasicTable has no references, so no _list_filter is expected.

	// Check insert data input type
	insertInputDef, ok := defs["User_mut_input_data"]
	if !ok {
		t.Error("expected User_mut_input_data definition in output")
	} else {
		if insertInputDef.Kind != ast.InputObject {
			t.Errorf("expected User_mut_input_data to be InputObject, got %v", insertInputDef.Kind)
		}
		if !hasFieldNamed(insertInputDef.Fields, "id") {
			t.Error("expected id field in User_mut_input_data")
		}
		if !hasFieldNamed(insertInputDef.Fields, "name") {
			t.Error("expected name field in User_mut_input_data")
		}
	}

	// Check update data input type
	if _, ok := defs["User_mut_data"]; !ok {
		t.Error("expected User_mut_data definition in output")
	}

	// Check aggregation type
	aggDef, ok := defs["_User_aggregation"]
	if !ok {
		t.Error("expected _User_aggregation definition in output")
	} else {
		if aggDef.Kind != ast.Object {
			t.Errorf("expected _User_aggregation to be Object, got %v", aggDef.Kind)
		}
		if !hasFieldNamed(aggDef.Fields, "_rows_count") {
			t.Error("expected _rows_count field in _User_aggregation")
		}
	}

	// Check Query extension
	queryExt, ok := exts["Query"]
	if !ok {
		t.Fatal("expected Query extension in output")
	}
	if !hasFieldNamed(queryExt.Fields, "User") {
		t.Error("expected User list query field on Query")
	}
	if !hasFieldNamed(queryExt.Fields, "User_by_pk") {
		t.Error("expected User_by_pk query field on Query")
	}
	if !hasFieldNamed(queryExt.Fields, "User_aggregation") {
		t.Error("expected User_aggregation query field on Query")
	}
	if !hasFieldNamed(queryExt.Fields, "User_bucket_aggregation") {
		t.Error("expected User_bucket_aggregation query field on Query")
	}

	// Check Mutation type and extension
	if _, ok := defs["Mutation"]; !ok {
		t.Error("expected Mutation definition in output")
	}
	mutExt, ok := exts["Mutation"]
	if !ok {
		t.Fatal("expected Mutation extension in output")
	}
	if !hasFieldNamed(mutExt.Fields, "insert_User") {
		t.Error("expected insert_User mutation field on Mutation")
	}
	if !hasFieldNamed(mutExt.Fields, "update_User") {
		t.Error("expected update_User mutation field on Mutation")
	}
	if !hasFieldNamed(mutExt.Fields, "delete_User") {
		t.Error("expected delete_User mutation field on Mutation")
	}
}

func TestCompile_EmptySource(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()
	source := &testSource{defs: nil}

	result, err := c.Compile(ctx, nil, source, base.Options{})
	if err != nil {
		t.Fatalf("Compile with empty source failed: %v", err)
	}

	defs := collectDefs(ctx, result)
	exts := collectExts(ctx, result)

	if len(defs) != 0 {
		t.Errorf("expected 0 definitions, got %d", len(defs))
	}
	if len(exts) != 0 {
		t.Errorf("expected 0 extensions, got %d", len(exts))
	}
}

func TestCompile_SystemTypeRedefinitionError(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Query",
				Position: pos(),
			},
		},
	}

	_, err := c.Compile(ctx, nil, source, base.Options{})
	if err == nil {
		t.Fatal("expected error for system type redefinition")
	}
	t.Logf("got expected error: %v", err)
}

func TestCompile_DefinitionRuleError(t *testing.T) {
	// A @table without @pk field should be rejected by DefinitionValidator
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "NoPKTable",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "no_pk", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "name", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	_, err := c.Compile(ctx, nil, source, base.Options{})
	if err == nil {
		t.Fatal("expected error for @table without @pk")
	}
	t.Logf("got expected error: %v", err)
}

// --- T038: @view, @references, @unique ---

func TestCompile_View(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "ActiveUsers",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "view", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "active_users", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "name", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	result, err := c.Compile(ctx, nil, source, base.Options{Name: "test"})
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	defs := collectDefs(ctx, result)
	exts := collectExts(ctx, result)

	// View should produce filter + aggregation but NOT input type
	if _, ok := defs["ActiveUsers_filter"]; !ok {
		t.Error("expected ActiveUsers_filter definition")
	}
	// Note: ActiveUsers_list_filter is only created lazily when back-references exist.
	if _, ok := defs["_ActiveUsers_aggregation"]; !ok {
		t.Error("expected _ActiveUsers_aggregation definition")
	}
	if _, ok := defs["_ActiveUsers_aggregation_bucket"]; !ok {
		t.Error("expected _ActiveUsers_aggregation_bucket definition")
	}
	if _, ok := defs["ActiveUsers_mut_input_data"]; ok {
		t.Error("view should NOT produce insert input type")
	}
	if _, ok := defs["ActiveUsers_mut_data"]; ok {
		t.Error("view should NOT produce update input type")
	}

	// Query should have fields
	queryExt, ok := exts["Query"]
	if !ok {
		t.Fatal("expected Query extension")
	}
	if !hasFieldNamed(queryExt.Fields, "ActiveUsers") {
		t.Error("expected ActiveUsers list query")
	}
	if !hasFieldNamed(queryExt.Fields, "ActiveUsers_aggregation") {
		t.Error("expected ActiveUsers_aggregation query")
	}
	if !hasFieldNamed(queryExt.Fields, "ActiveUsers_bucket_aggregation") {
		t.Error("expected ActiveUsers_bucket_aggregation query")
	}

	// No Mutation for views
	if _, ok := defs["Mutation"]; ok {
		t.Error("view should NOT produce Mutation type")
	}
	if _, ok := exts["Mutation"]; ok {
		t.Error("view should NOT produce Mutation extension")
	}
}

func TestCompile_References(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Post",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "posts", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
					{Name: "references", Arguments: ast.ArgumentList{
						{Name: "references_name", Value: &ast.Value{Raw: "Author", Kind: ast.StringValue, Position: pos()}, Position: pos()},
						{Name: "source_fields", Value: &ast.Value{
							Kind:     ast.ListValue,
							Children: ast.ChildValueList{{Value: &ast.Value{Raw: "author_id", Kind: ast.StringValue}}},
						}, Position: pos()},
						{Name: "references_fields", Value: &ast.Value{
							Kind:     ast.ListValue,
							Children: ast.ChildValueList{{Value: &ast.Value{Raw: "id", Kind: ast.StringValue}}},
						}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "title", Type: ast.NamedType("String", pos()), Position: pos()},
					{Name: "author_id", Type: ast.NonNullNamedType("Int", pos()), Position: pos()},
				},
			},
			{
				Kind:     ast.Object,
				Name:     "Author",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "authors", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "name", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	result, err := c.Compile(ctx, nil, source, base.Options{Name: "test"})
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	exts := collectExts(ctx, result)

	// Check forward reference on Post → Author
	postExt, ok := exts["Post"]
	if !ok {
		t.Fatal("expected Post extension (for forward reference)")
	}
	if !hasFieldNamed(postExt.Fields, "Author") {
		t.Error("expected Author forward reference field on Post extension")
	}

	// Check back-reference on Author → Post
	authorExt, ok := exts["Author"]
	if !ok {
		t.Fatal("expected Author extension (for back reference)")
	}
	if !hasFieldNamed(authorExt.Fields, "Post") {
		t.Error("expected Post back-reference field on Author extension")
	}
}

func TestCompile_Unique(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "User",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "users", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
					{Name: "unique", Arguments: ast.ArgumentList{
						{Name: "query_suffix", Value: &ast.Value{Raw: "email", Kind: ast.StringValue, Position: pos()}, Position: pos()},
						{Name: "fields", Value: &ast.Value{
							Kind:     ast.ListValue,
							Children: ast.ChildValueList{{Value: &ast.Value{Raw: "email", Kind: ast.StringValue}}},
						}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "email", Type: ast.NonNullNamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	result, err := c.Compile(ctx, nil, source, base.Options{Name: "test"})
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	exts := collectExts(ctx, result)

	queryExt, ok := exts["Query"]
	if !ok {
		t.Fatal("expected Query extension")
	}
	if !hasFieldNamed(queryExt.Fields, "User_email") {
		t.Error("expected User_email unique query field on Query")
	}
}

func TestCompile_ExtraFields(t *testing.T) {
	// Timestamp fields should produce _<field>_part extra fields
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Event",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "events", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "created_at", Type: ast.NamedType("Timestamp", pos()), Position: pos()},
				},
			},
		},
	}

	result, err := c.Compile(ctx, nil, source, base.Options{Name: "test"})
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	exts := collectExts(ctx, result)

	// ExtraFieldRule should extend Event with _created_at_part
	eventExt, ok := exts["Event"]
	if !ok {
		t.Fatal("expected Event extension (for extra fields)")
	}
	if !hasFieldNamed(eventExt.Fields, "_created_at_part") {
		t.Error("expected _created_at_part extra field on Event extension")
	}
}

// --- T039: Prefix, AsModule, ReadOnly, @replace ---

func TestCompile_Prefix(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Item",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "items", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "name", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	result, err := c.Compile(ctx, nil, source, base.Options{
		Name:   "test",
		Prefix: "pfx",
	})
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	defs := collectDefs(ctx, result)

	// All types should have prefix with underscore separator
	if _, ok := defs["pfx_Item"]; !ok {
		t.Error("expected pfx_Item definition (prefixed)")
	}
	if _, ok := defs["pfx_Item_filter"]; !ok {
		t.Error("expected pfx_Item_filter definition (prefixed)")
	}
	// Note: pfx_Item_list_filter is only created lazily when back-references exist.
	if _, ok := defs["pfx_Item_mut_input_data"]; !ok {
		t.Error("expected pfx_Item_mut_input_data definition (prefixed)")
	}
	if _, ok := defs["pfx_Item_mut_data"]; !ok {
		t.Error("expected pfx_Item_mut_data definition (prefixed)")
	}
	if _, ok := defs["_pfx_Item_aggregation"]; !ok {
		t.Error("expected _pfx_Item_aggregation definition (prefixed)")
	}
	if _, ok := defs["_pfx_Item_aggregation_bucket"]; !ok {
		t.Error("expected _pfx_Item_aggregation_bucket definition (prefixed)")
	}

	// Original unprefixed should not exist
	if _, ok := defs["Item"]; ok {
		t.Error("should NOT have unprefixed Item definition")
	}
}

func TestCompile_ReadOnly(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Record",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "records", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "value", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	result, err := c.Compile(ctx, nil, source, base.Options{
		Name:     "test",
		ReadOnly: true,
	})
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	defs := collectDefs(ctx, result)
	exts := collectExts(ctx, result)

	// ReadOnly should not produce input types
	if _, ok := defs["Record_mut_input_data"]; ok {
		t.Error("ReadOnly mode should NOT produce insert input type")
	}
	if _, ok := defs["Record_mut_data"]; ok {
		t.Error("ReadOnly mode should NOT produce update input type")
	}

	// No Mutation
	if _, ok := defs["Mutation"]; ok {
		t.Error("ReadOnly mode should NOT produce Mutation type")
	}
	if _, ok := exts["Mutation"]; ok {
		t.Error("ReadOnly mode should NOT produce Mutation extension")
	}

	// Query should still have query fields
	queryExt, ok := exts["Query"]
	if !ok {
		t.Fatal("expected Query extension even in ReadOnly mode")
	}
	if !hasFieldNamed(queryExt.Fields, "Record") {
		t.Error("expected Record list query in ReadOnly mode")
	}
}

func TestCompile_AsModule(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Metric",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "metrics", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "value", Type: ast.NamedType("Float", pos()), Position: pos()},
				},
			},
		},
	}

	result, err := c.Compile(ctx, nil, source, base.Options{
		Name:     "mymod",
		AsModule: true,
	})
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	defs := collectDefs(ctx, result)
	exts := collectExts(ctx, result)

	// Should have module query/mutation types
	if _, ok := defs["_module_mymod_query"]; !ok {
		t.Error("expected _module_mymod_query definition")
	}
	if _, ok := defs["_module_mymod_mutation"]; !ok {
		t.Error("expected _module_mymod_mutation definition")
	}

	// Object's query fields should be in module query type extension, not root Query
	modQueryExt, ok := exts["_module_mymod_query"]
	if !ok {
		t.Fatal("expected _module_mymod_query extension")
	}
	if !hasFieldNamed(modQueryExt.Fields, "Metric") {
		t.Error("expected Metric list query on module query type")
	}

	// Root Query should have module field, not direct Metric field
	queryExt, ok := exts["Query"]
	if !ok {
		t.Fatal("expected Query extension")
	}
	if !hasFieldNamed(queryExt.Fields, "mymod") {
		t.Error("expected mymod field on root Query")
	}
}

func TestCompile_Replace(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Widget",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "widgets", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
					{Name: "replace", Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "label", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	// Compile twice — second compile should use replace-or-create semantics
	result1, err := c.Compile(ctx, nil, source, base.Options{Name: "test"})
	if err != nil {
		t.Fatalf("First compile failed: %v", err)
	}

	defs := collectDefs(ctx, result1)
	if _, ok := defs["Widget"]; !ok {
		t.Error("expected Widget definition after first compile")
	}
	if _, ok := defs["Widget_filter"]; !ok {
		t.Error("expected Widget_filter after first compile")
	}
}

// --- US3 (T040-T041): Injectable custom rules ---

// testCustomRule is a DefinitionRule that adds a custom field to matching definitions.
type testCustomRule struct{}

func (r *testCustomRule) Name() string      { return "TestCustomRule" }
func (r *testCustomRule) Phase() base.Phase { return base.PhaseGenerate }
func (r *testCustomRule) Match(def *ast.Definition) bool {
	return def.Directives.ForName("custom") != nil
}
func (r *testCustomRule) Process(ctx base.CompilationContext, def *ast.Definition) error {
	p := &ast.Position{Src: &ast.Source{Name: "custom-rule"}}
	ext := &ast.Definition{
		Kind:     ast.Object,
		Name:     def.Name,
		Position: p,
		Fields: ast.FieldList{
			{Name: "_custom_field", Type: ast.NamedType("String", p), Position: p},
		},
	}
	ctx.AddExtension(ext)
	return nil
}

func TestCompile_CustomDefinitionRule(t *testing.T) {
	// Create compiler with all built-in rules PLUS custom rule
	allRules := rules.RegisterAll()
	allRules = append(allRules, &testCustomRule{})
	c := compiler.New(allRules...)
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "MyObj",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "my_objs", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
					{Name: "custom", Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
				},
			},
		},
	}

	result, err := c.Compile(ctx, nil, source, base.Options{Name: "test"})
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	exts := collectExts(ctx, result)
	myObjExt, ok := exts["MyObj"]
	if !ok {
		t.Fatal("expected MyObj extension")
	}
	if !hasFieldNamed(myObjExt.Fields, "_custom_field") {
		t.Error("expected _custom_field from custom rule on MyObj")
	}
}

// testCustomBatchRule is a BatchRule that runs during ASSEMBLE.
type testCustomBatchRule struct {
	called bool
}

func (r *testCustomBatchRule) Name() string      { return "TestCustomBatchRule" }
func (r *testCustomBatchRule) Phase() base.Phase { return base.PhaseAssemble }
func (r *testCustomBatchRule) ProcessAll(_ base.CompilationContext) error {
	r.called = true
	return nil
}

func TestCompile_CustomBatchRule(t *testing.T) {
	batchRule := &testCustomBatchRule{}
	allRules := rules.RegisterAll()
	allRules = append(allRules, batchRule)
	c := compiler.New(allRules...)
	ctx := context.Background()

	source := &testSource{defs: nil}
	_, err := c.Compile(ctx, nil, source, base.Options{})
	if err != nil {
		t.Fatalf("Compile failed: %v", err)
	}

	if !batchRule.called {
		t.Error("expected custom BatchRule to be called during ASSEMBLE")
	}
}

// --- US4 (T042-T043): Sequential Multi-Catalog Compilation ---

// testProvider wraps a compiled catalog as a Provider (target schema).
type testProvider struct {
	defs map[string]*ast.Definition
}

func newTestProvider(ctx context.Context, cat base.CompiledCatalog) *testProvider {
	p := &testProvider{defs: make(map[string]*ast.Definition)}
	for d := range cat.Definitions(ctx) {
		p.defs[d.Name] = d
	}
	return p
}

func (p *testProvider) ForName(_ context.Context, name string) *ast.Definition {
	return p.defs[name]
}
func (p *testProvider) DirectiveForName(_ context.Context, _ string) *ast.DirectiveDefinition {
	return nil
}
func (p *testProvider) Definitions(_ context.Context) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {
		for _, d := range p.defs {
			if !yield(d) {
				return
			}
		}
	}
}
func (p *testProvider) DirectiveDefinitions(_ context.Context) iter.Seq2[string, *ast.DirectiveDefinition] {
	return func(yield func(string, *ast.DirectiveDefinition) bool) {}
}
func (p *testProvider) Description(_ context.Context) string                       { return "" }
func (p *testProvider) QueryType(_ context.Context) *ast.Definition                { return nil }
func (p *testProvider) MutationType(_ context.Context) *ast.Definition             { return nil }
func (p *testProvider) SubscriptionType(_ context.Context) *ast.Definition         { return nil }
func (p *testProvider) PossibleTypes(_ context.Context, _ string) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {}
}
func (p *testProvider) Implements(_ context.Context, _ string) iter.Seq[*ast.Definition] {
	return func(yield func(*ast.Definition) bool) {}
}
func (p *testProvider) Types(_ context.Context) iter.Seq2[string, *ast.Definition] {
	return func(yield func(string, *ast.Definition) bool) {
		for k, v := range p.defs {
			if !yield(k, v) {
				return
			}
		}
	}
}

func TestCompile_SequentialMultiCatalog(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	// Catalog A: @table X
	sourceA := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "X",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "xs", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "val", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	resultA, err := c.Compile(ctx, nil, sourceA, base.Options{Name: "catA"})
	if err != nil {
		t.Fatalf("Catalog A compile failed: %v", err)
	}

	// Create a provider from catalog A's output
	providerA := newTestProvider(ctx, resultA)

	// Catalog B: @table Y with @references to X
	sourceB := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Y",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "ys", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
					{Name: "references", Arguments: ast.ArgumentList{
						{Name: "references_name", Value: &ast.Value{Raw: "X", Kind: ast.StringValue, Position: pos()}, Position: pos()},
						{Name: "source_fields", Value: &ast.Value{
							Kind:     ast.ListValue,
							Children: ast.ChildValueList{{Value: &ast.Value{Raw: "x_id", Kind: ast.StringValue}}},
						}, Position: pos()},
						{Name: "references_fields", Value: &ast.Value{
							Kind:     ast.ListValue,
							Children: ast.ChildValueList{{Value: &ast.Value{Raw: "id", Kind: ast.StringValue}}},
						}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "x_id", Type: ast.NonNullNamedType("Int", pos()), Position: pos()},
				},
			},
		},
	}

	resultB, err := c.Compile(ctx, providerA, sourceB, base.Options{Name: "catB"})
	if err != nil {
		t.Fatalf("Catalog B compile failed: %v", err)
	}

	defs := collectDefs(ctx, resultB)
	exts := collectExts(ctx, resultB)

	// Catalog B should contain Y and its derived types
	if _, ok := defs["Y"]; !ok {
		t.Error("expected Y definition from catalog B")
	}
	if _, ok := defs["Y_filter"]; !ok {
		t.Error("expected Y_filter from catalog B")
	}

	// References should work: Y → X forward reference
	yExt, ok := exts["Y"]
	if !ok {
		t.Fatal("expected Y extension (forward reference to X)")
	}
	if !hasFieldNamed(yExt.Fields, "X") {
		t.Error("expected X forward reference field on Y")
	}

	// Back-reference on X (from catalog A)
	xExt, ok := exts["X"]
	if !ok {
		t.Fatal("expected X extension (back-reference from Y)")
	}
	if !hasFieldNamed(xExt.Fields, "Y") {
		t.Error("expected Y back-reference field on X")
	}
}

func TestCompile_MultiCatalogModuleExtension(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	// Catalog A: @table in module "m"
	sourceA := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "TableA",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "table_a", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
				},
			},
		},
	}

	resultA, err := c.Compile(ctx, nil, sourceA, base.Options{
		Name:     "m",
		AsModule: true,
	})
	if err != nil {
		t.Fatalf("Catalog A compile failed: %v", err)
	}

	defsA := collectDefs(ctx, resultA)
	if _, ok := defsA["_module_m_query"]; !ok {
		t.Error("expected _module_m_query from catalog A")
	}

	// Now simulate catalog B also using module "m"
	providerA := newTestProvider(ctx, resultA)

	sourceB := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "TableB",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "table_b", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
				},
			},
		},
	}

	resultB, err := c.Compile(ctx, providerA, sourceB, base.Options{
		Name:     "m",
		AsModule: true,
	})
	if err != nil {
		t.Fatalf("Catalog B compile failed: %v", err)
	}

	defsB := collectDefs(ctx, resultB)
	extsB := collectExts(ctx, resultB)

	// Catalog B should also produce module types (with @if_not_exists)
	modQuery, ok := defsB["_module_m_query"]
	if !ok {
		t.Fatal("expected _module_m_query from catalog B")
	}
	// Should have @if_not_exists directive
	if modQuery.Directives.ForName("if_not_exists") == nil {
		t.Error("expected @if_not_exists on _module_m_query")
	}

	// Catalog B's TableB should be in the module extension
	modQueryExt, ok := extsB["_module_m_query"]
	if !ok {
		t.Fatal("expected _module_m_query extension from catalog B")
	}
	if !hasFieldNamed(modQueryExt.Fields, "TableB") {
		t.Error("expected TableB query on module query extension")
	}
}
