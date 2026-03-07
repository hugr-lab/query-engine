package compiler_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/hugr-lab/query-engine/pkg/catalog/compiler"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/base"
	"github.com/hugr-lab/query-engine/pkg/catalog/compiler/rules"
	_ "github.com/hugr-lab/query-engine/pkg/catalog/types"
	"github.com/vektah/gqlparser/v2/ast"
)

// generateTableDefs creates n table definitions with standard fields.
func generateTableDefs(n int) []*ast.Definition {
	p := &ast.Position{Src: &ast.Source{Name: "bench"}}
	defs := make([]*ast.Definition, n)
	for i := range n {
		name := fmt.Sprintf("Table%d", i)
		defs[i] = &ast.Definition{
			Kind:     ast.Object,
			Name:     name,
			Position: p,
			Directives: ast.DirectiveList{
				{Name: "table", Arguments: ast.ArgumentList{
					{Name: "name", Value: &ast.Value{Raw: fmt.Sprintf("table_%d", i), Kind: ast.StringValue, Position: p}, Position: p},
				}, Position: p},
			},
			Fields: ast.FieldList{
				{Name: "id", Type: ast.NonNullNamedType("Int", p), Position: p, Directives: ast.DirectiveList{{Name: "pk", Position: p}}},
				{Name: "name", Type: ast.NamedType("String", p), Position: p},
				{Name: "value", Type: ast.NamedType("Float", p), Position: p},
				{Name: "count", Type: ast.NamedType("Int", p), Position: p},
				{Name: "created_at", Type: ast.NamedType("Timestamp", p), Position: p},
			},
		}
	}
	return defs
}

func BenchmarkCompile_200Tables(b *testing.B) {
	c := compiler.New(rules.RegisterAll()...)
	ctx := context.Background()
	source := &testSource{defs: generateTableDefs(200)}
	opts := base.Options{Name: "bench"}

	b.ResetTimer()
	for range b.N {
		_, err := c.Compile(ctx, nil, source, opts)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestCompile_PerformanceNoQuadratic(t *testing.T) {
	c := compiler.New(rules.RegisterAll()...)
	ctx := context.Background()
	opts := base.Options{Name: "perf"}

	// Measure 100 types
	source100 := &testSource{defs: generateTableDefs(100)}
	start100 := time.Now()
	for range 3 {
		_, err := c.Compile(ctx, nil, source100, opts)
		if err != nil {
			t.Fatal(err)
		}
	}
	dur100 := time.Since(start100) / 3

	// Measure 200 types
	source200 := &testSource{defs: generateTableDefs(200)}
	start200 := time.Now()
	for range 3 {
		_, err := c.Compile(ctx, nil, source200, opts)
		if err != nil {
			t.Fatal(err)
		}
	}
	dur200 := time.Since(start200) / 3

	t.Logf("100 types: %v, 200 types: %v, ratio: %.2f", dur100, dur200, float64(dur200)/float64(dur100))

	// 2x input should not take more than 3x time (allowing headroom for setup costs)
	ratio := float64(dur200) / float64(dur100)
	if ratio > 3.0 {
		t.Errorf("performance regression: ratio %.2f exceeds 3.0 (quadratic behavior suspected)", ratio)
	}
}
