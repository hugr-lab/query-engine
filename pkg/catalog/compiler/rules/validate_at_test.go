package rules_test

import (
	"context"
	"iter"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/rules"
	_ "github.com/hugr-lab/query-engine/pkg/catalog/types" // ensure scalar init()
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

func pos() *ast.Position {
	return &ast.Position{Src: &ast.Source{Name: "test"}}
}

func newAllRulesCompiler() *compiler.Compiler {
	return compiler.New(rules.RegisterAll()...)
}

func TestAtValidator_NonTimeTravelSource(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Snapshot",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "snapshots", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
					{Name: "at", Arguments: ast.ArgumentList{
						{Name: "version", Value: &ast.Value{Raw: "5", Kind: ast.IntValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "data", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	// No capabilities → SupportTimeTravel is false
	_, err := c.Compile(ctx, nil, source, base.Options{Name: "test"})
	if err == nil {
		t.Fatal("expected error for @at on source without time travel support, got nil")
	}
	t.Logf("got expected error: %v", err)
}

func TestAtValidator_ValidUsage(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Snapshot",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "snapshots", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
					{Name: "at", Arguments: ast.ArgumentList{
						{Name: "version", Value: &ast.Value{Raw: "5", Kind: ast.IntValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "data", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	_, err := c.Compile(ctx, nil, source, base.Options{
		Name: "test",
		Capabilities: &base.EngineCapabilities{
			General: base.EngineGeneralCapabilities{
				SupportTimeTravel: true,
			},
		},
	})
	if err != nil {
		t.Fatalf("expected no error for valid @at usage, got: %v", err)
	}
}

func TestAtValidator_BothArgs(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Snapshot",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "snapshots", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
					{Name: "at", Arguments: ast.ArgumentList{
						{Name: "version", Value: &ast.Value{Raw: "5", Kind: ast.IntValue, Position: pos()}, Position: pos()},
						{Name: "timestamp", Value: &ast.Value{Raw: "2024-01-01", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "data", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	_, err := c.Compile(ctx, nil, source, base.Options{
		Name: "test",
		Capabilities: &base.EngineCapabilities{
			General: base.EngineGeneralCapabilities{
				SupportTimeTravel: true,
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for @at with both version and timestamp, got nil")
	}
	if got := err.Error(); !contains(got, "only one of") {
		t.Fatalf("expected 'only one of' error, got: %v", err)
	}
	t.Logf("got expected error: %v", err)
}

func TestAtValidator_NoArgs(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Snapshot",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "table", Arguments: ast.ArgumentList{
						{Name: "name", Value: &ast.Value{Raw: "snapshots", Kind: ast.StringValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
					{Name: "at", Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "id", Type: ast.NonNullNamedType("Int", pos()), Position: pos(), Directives: ast.DirectiveList{{Name: "pk", Position: pos()}}},
					{Name: "data", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	_, err := c.Compile(ctx, nil, source, base.Options{
		Name: "test",
		Capabilities: &base.EngineCapabilities{
			General: base.EngineGeneralCapabilities{
				SupportTimeTravel: true,
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for @at with neither version nor timestamp, got nil")
	}
	if got := err.Error(); !contains(got, "exactly one of") {
		t.Fatalf("expected 'exactly one of' error, got: %v", err)
	}
	t.Logf("got expected error: %v", err)
}

func TestAtValidator_OnMutation(t *testing.T) {
	c := newAllRulesCompiler()
	ctx := context.Background()

	// @at on a Mutation type definition — this should fail because @at
	// is only meaningful for query-side data objects, and Mutation is a system type.
	source := &testSource{
		defs: []*ast.Definition{
			{
				Kind:     ast.Object,
				Name:     "Mutation",
				Position: pos(),
				Directives: ast.DirectiveList{
					{Name: "at", Arguments: ast.ArgumentList{
						{Name: "version", Value: &ast.Value{Raw: "1", Kind: ast.IntValue, Position: pos()}, Position: pos()},
					}, Position: pos()},
				},
				Fields: ast.FieldList{
					{Name: "doSomething", Type: ast.NamedType("String", pos()), Position: pos()},
				},
			},
		},
	}

	_, err := c.Compile(ctx, nil, source, base.Options{
		Name: "test",
		Capabilities: &base.EngineCapabilities{
			General: base.EngineGeneralCapabilities{
				SupportTimeTravel: true,
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for @at on Mutation type, got nil")
	}
	t.Logf("got expected error: %v", err)
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstring(s, substr)
}

func searchSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
