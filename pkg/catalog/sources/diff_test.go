package sources

import (
	"context"
	"iter"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/vektah/gqlparser/v2/ast"
)

// testSource is a minimal DefinitionsSource for testing.
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
	return func(_ func(string, *ast.DirectiveDefinition) bool) {}
}

func tableDef(name string, fields ...string) *ast.Definition {
	pos := &ast.Position{Src: &ast.Source{Name: "test"}}
	var ff ast.FieldList
	for _, f := range fields {
		ff = append(ff, &ast.FieldDefinition{
			Name:     f,
			Type:     ast.NamedType("String", pos),
			Position: pos,
		})
	}
	return &ast.Definition{
		Kind:     ast.Object,
		Name:     name,
		Fields:   ff,
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "table", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: name, Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
	}
}

func TestDiffSchemas_AddedTypes(t *testing.T) {
	ctx := context.Background()
	oldSchema := &testSource{defs: []*ast.Definition{
		tableDef("A", "id", "name"),
		tableDef("B", "id", "value"),
	}}
	newSchema := &testSource{defs: []*ast.Definition{
		tableDef("A", "id", "name"),
		tableDef("B", "id", "value"),
		tableDef("C", "id", "extra"),
	}}

	result := DiffSchemas(ctx, oldSchema, newSchema)

	changes := collectDefs(ctx, result)
	if len(changes) != 1 {
		t.Fatalf("expected 1 definition change, got %d", len(changes))
	}
	if changes[0].Name != "C" {
		t.Errorf("expected added type C, got %s", changes[0].Name)
	}
	if base.IsDropDefinition(changes[0]) {
		t.Errorf("added type should not have @drop")
	}
	exts := collectExts(ctx, result)
	if len(exts) != 0 {
		t.Errorf("expected no extensions for added types, got %d", len(exts))
	}
}

func TestDiffSchemas_DroppedTypes(t *testing.T) {
	ctx := context.Background()
	oldSchema := &testSource{defs: []*ast.Definition{
		tableDef("A", "id", "name"),
		tableDef("B", "id", "value"),
		tableDef("C", "id", "extra"),
	}}
	newSchema := &testSource{defs: []*ast.Definition{
		tableDef("A", "id", "name"),
		tableDef("B", "id", "value"),
	}}

	result := DiffSchemas(ctx, oldSchema, newSchema)

	changes := collectDefs(ctx, result)
	if len(changes) != 1 {
		t.Fatalf("expected 1 definition change, got %d", len(changes))
	}
	if changes[0].Name != "C" {
		t.Errorf("expected dropped type C, got %s", changes[0].Name)
	}
	if !base.IsDropDefinition(changes[0]) {
		t.Error("dropped type should have @drop directive")
	}
}

func TestDiffSchemas_FieldAdded(t *testing.T) {
	ctx := context.Background()
	oldSchema := &testSource{defs: []*ast.Definition{
		tableDef("A", "id", "name"),
	}}
	newSchema := &testSource{defs: []*ast.Definition{
		tableDef("A", "id", "name", "email"),
	}}

	result := DiffSchemas(ctx, oldSchema, newSchema)

	// No whole-type changes.
	changes := collectDefs(ctx, result)
	if len(changes) != 0 {
		t.Fatalf("expected 0 definition changes, got %d", len(changes))
	}

	// Should have a field-add extension.
	exts := collectExts(ctx, result)
	if len(exts) != 1 {
		t.Fatalf("expected 1 extension, got %d", len(exts))
	}
	if exts[0].Name != "A" {
		t.Errorf("expected extension on type A, got %s", exts[0].Name)
	}
	if len(exts[0].Fields) != 1 || exts[0].Fields[0].Name != "email" {
		t.Errorf("expected extension with field 'email', got %v", exts[0].Fields)
	}
	if base.IsDropField(exts[0].Fields[0]) {
		t.Error("added field should not have @drop")
	}
}

func TestDiffSchemas_FieldDropped(t *testing.T) {
	ctx := context.Background()
	oldSchema := &testSource{defs: []*ast.Definition{
		tableDef("A", "id", "name", "email"),
	}}
	newSchema := &testSource{defs: []*ast.Definition{
		tableDef("A", "id", "name"),
	}}

	result := DiffSchemas(ctx, oldSchema, newSchema)

	// No whole-type changes.
	changes := collectDefs(ctx, result)
	if len(changes) != 0 {
		t.Fatalf("expected 0 definition changes, got %d", len(changes))
	}

	// Should have a field-drop extension.
	exts := collectExts(ctx, result)
	if len(exts) != 1 {
		t.Fatalf("expected 1 extension, got %d", len(exts))
	}
	if len(exts[0].Fields) != 1 || exts[0].Fields[0].Name != "email" {
		t.Errorf("expected extension with field 'email', got %v", exts[0].Fields)
	}
	if !base.IsDropField(exts[0].Fields[0]) {
		t.Error("dropped field should have @drop")
	}
}

func TestDiffSchemas_FieldReplaced(t *testing.T) {
	ctx := context.Background()
	pos := &ast.Position{Src: &ast.Source{Name: "test"}}

	oldDef := tableDef("A", "id", "name")
	// Change "name" type from String to Int.
	newDef := &ast.Definition{
		Kind: ast.Object,
		Name: "A",
		Fields: ast.FieldList{
			{Name: "id", Type: ast.NamedType("String", pos), Position: pos},
			{Name: "name", Type: ast.NamedType("Int", pos), Position: pos},
		},
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "table", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: "A", Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
	}

	result := DiffSchemas(ctx,
		&testSource{defs: []*ast.Definition{oldDef}},
		&testSource{defs: []*ast.Definition{newDef}},
	)

	changes := collectDefs(ctx, result)
	if len(changes) != 0 {
		t.Fatalf("expected 0 definition changes, got %d", len(changes))
	}

	exts := collectExts(ctx, result)
	if len(exts) != 1 {
		t.Fatalf("expected 1 extension, got %d", len(exts))
	}
	if len(exts[0].Fields) != 1 || exts[0].Fields[0].Name != "name" {
		t.Errorf("expected extension with field 'name', got %v", exts[0].Fields)
	}
	if !base.IsReplaceField(exts[0].Fields[0]) {
		t.Error("changed field should have @replace")
	}
	if exts[0].Fields[0].Type.Name() != "Int" {
		t.Errorf("replaced field should have new type Int, got %s", exts[0].Fields[0].Type.Name())
	}
}

func TestDiffSchemas_IdenticalSchemas(t *testing.T) {
	ctx := context.Background()
	schema := &testSource{defs: []*ast.Definition{
		tableDef("A", "id", "name"),
		tableDef("B", "id", "value"),
	}}

	result := DiffSchemas(ctx, schema, schema)

	changes := collectDefs(ctx, result)
	if len(changes) != 0 {
		t.Fatalf("expected 0 changes for identical schemas, got %d", len(changes))
	}
	exts := collectExts(ctx, result)
	if len(exts) != 0 {
		t.Fatalf("expected 0 extensions for identical schemas, got %d", len(exts))
	}
}

func TestDiffSchemas_MixedChanges(t *testing.T) {
	ctx := context.Background()
	oldSchema := &testSource{defs: []*ast.Definition{
		tableDef("A", "id", "name"),
		tableDef("B", "id", "value"),
		tableDef("C", "id", "extra"),
	}}
	newSchema := &testSource{defs: []*ast.Definition{
		tableDef("A", "id", "name", "email"), // field added
		tableDef("B", "id", "value"),          // identical (no change)
		tableDef("D", "id", "new_field"),      // add
		// C is dropped
	}}

	result := DiffSchemas(ctx, oldSchema, newSchema)

	changes := collectDefs(ctx, result)
	if len(changes) != 2 {
		t.Fatalf("expected 2 definition changes (add+drop), got %d", len(changes))
	}

	byName := make(map[string]*ast.Definition)
	for _, c := range changes {
		byName[c.Name] = c
	}

	// C should be @drop
	if c, ok := byName["C"]; !ok {
		t.Error("expected change for C")
	} else if !base.IsDropDefinition(c) {
		t.Error("C should be @drop")
	}

	// D should be add (no DDL directives)
	if d, ok := byName["D"]; !ok {
		t.Error("expected change for D")
	} else if base.IsDropDefinition(d) {
		t.Error("D should be a plain add (no DDL directives)")
	}

	// B should not appear (identical)
	if _, ok := byName["B"]; ok {
		t.Error("B should not appear in changes (identical)")
	}

	// A should produce field-level extension (email added)
	exts := collectExts(ctx, result)
	if len(exts) != 1 {
		t.Fatalf("expected 1 extension, got %d", len(exts))
	}
	if exts[0].Name != "A" {
		t.Errorf("expected extension on type A, got %s", exts[0].Name)
	}
}

func TestDiffSchemas_DirectiveChange(t *testing.T) {
	ctx := context.Background()
	pos := &ast.Position{Src: &ast.Source{Name: "test"}}
	oldDef := tableDef("A", "id")
	newDef := &ast.Definition{
		Kind: ast.Object,
		Name: "A",
		Fields: ast.FieldList{
			{Name: "id", Type: ast.NamedType("String", pos), Position: pos},
		},
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "table", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: "A", Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
			{Name: "pk", Position: pos},
		},
	}

	result := DiffSchemas(ctx, &testSource{defs: []*ast.Definition{oldDef}}, &testSource{defs: []*ast.Definition{newDef}})

	changes := collectDefs(ctx, result)
	if len(changes) != 0 {
		t.Fatalf("expected 0 definition changes, got %d", len(changes))
	}

	// Should produce an extension with the new directive.
	exts := collectExts(ctx, result)
	if len(exts) != 1 {
		t.Fatalf("expected 1 extension, got %d", len(exts))
	}
	// Extension should have the new @pk directive.
	if exts[0].Directives.ForName("pk") == nil {
		t.Error("directive change should include the new @pk directive")
	}
}

func TestDiffSchemas_FieldWithDirectiveChange(t *testing.T) {
	ctx := context.Background()
	pos := &ast.Position{Src: &ast.Source{Name: "test"}}

	oldDef := &ast.Definition{
		Kind: ast.Object,
		Name: "A",
		Fields: ast.FieldList{
			{Name: "id", Type: ast.NamedType("String", pos), Position: pos},
			{Name: "name", Type: ast.NamedType("String", pos), Position: pos},
		},
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "table", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: "A", Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
	}
	newDef := &ast.Definition{
		Kind: ast.Object,
		Name: "A",
		Fields: ast.FieldList{
			{Name: "id", Type: ast.NamedType("String", pos), Position: pos},
			{Name: "name", Type: ast.NamedType("String", pos), Position: pos, Directives: ast.DirectiveList{
				{Name: "pk", Position: pos},
			}},
		},
		Position: pos,
		Directives: ast.DirectiveList{
			{Name: "table", Arguments: ast.ArgumentList{
				{Name: "name", Value: &ast.Value{Raw: "A", Kind: ast.StringValue, Position: pos}, Position: pos},
			}, Position: pos},
		},
	}

	result := DiffSchemas(ctx, &testSource{defs: []*ast.Definition{oldDef}}, &testSource{defs: []*ast.Definition{newDef}})

	changes := collectDefs(ctx, result)
	if len(changes) != 0 {
		t.Fatalf("expected 0 definition changes, got %d", len(changes))
	}

	exts := collectExts(ctx, result)
	if len(exts) != 1 {
		t.Fatalf("expected 1 extension, got %d", len(exts))
	}
	if len(exts[0].Fields) != 1 || exts[0].Fields[0].Name != "name" {
		t.Errorf("expected extension with field 'name', got %v", exts[0].Fields)
	}
	if !base.IsReplaceField(exts[0].Fields[0]) {
		t.Error("field with directive change should have @replace")
	}
}

func collectDefs(ctx context.Context, src base.DefinitionsSource) []*ast.Definition {
	var defs []*ast.Definition
	for def := range src.Definitions(ctx) {
		defs = append(defs, def)
	}
	return defs
}

func collectExts(ctx context.Context, src base.ExtensionsSource) []*ast.Definition {
	var exts []*ast.Definition
	for ext := range src.Extensions(ctx) {
		exts = append(exts, ext)
	}
	return exts
}
