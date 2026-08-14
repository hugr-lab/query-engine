package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

// Hiding soft-deleted rows is a property of the generated SQL, in the same
// class as the role rules: no shape of caller input may switch it off. Only
// the @with_deleted directive does.
//
// It used to be switchable. The predicate was added when the filter argument
// was ABSENT, and otherwise left to whereNode — which returns nothing at all
// for an empty map, so `filter: {}` produced a SELECT with no WHERE and
// happily returned deleted rows. A UI that passes an empty object when the
// user has cleared every control hits that on the very first render.
func TestSoftDeleteSurvivesEveryFilterShape(t *testing.T) {
	ctx := context.Background()
	def := testSchemaService.ForName(ctx, "sd_object")
	if def == nil {
		t.Fatal("sd_object not found in the test schema")
	}
	info := sdl.DataObjectInfo(def)
	if !info.SoftDelete {
		t.Fatal("sd_object must be a soft-delete table for this test to mean anything")
	}
	want := info.SoftDeleteCondition("_objects")

	tests := []struct {
		name   string
		filter any // nil == the argument is not passed at all
	}{
		{"argument absent", nil},
		{"empty object", map[string]any{}},
		{"only null members", map[string]any{"field2": nil}},
		{"a real condition", map[string]any{"field2": map[string]any{"eq": 1}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var args sdl.FieldQueryArguments
			if tt.filter != nil {
				args = sdl.FieldQueryArguments{{Name: "filter", Value: tt.filter}}
			}
			query := &ast.Field{Name: "sd_object"}

			nodes, err := selectQueryParamsNodes(ctx, testSchemaService, &engines.DuckDB{}, info, "_objects", query, args, false)
			if err != nil {
				t.Fatalf("selectQueryParamsNodes: %v", err)
			}
			where := nodes.ForName("where")
			if where == nil {
				t.Fatalf("no where node at all — deleted rows would be returned")
			}
			sql := collectSQL(t, where)
			if !strings.Contains(sql, want) {
				t.Errorf("where clause %q does not carry the soft-delete predicate %q", sql, want)
			}
		})
	}
}

// TestWithDeletedStillOptsOut keeps the escape hatch honest: the directive is
// the one and only way to see deleted rows, so a fix that hard-wired the
// predicate everywhere would break it.
func TestWithDeletedStillOptsOut(t *testing.T) {
	ctx := context.Background()
	info := sdl.DataObjectInfo(testSchemaService.ForName(ctx, "sd_object"))
	query := &ast.Field{
		Name:       "sd_object",
		Directives: ast.DirectiveList{{Name: "with_deleted"}},
	}

	for _, filter := range []any{nil, map[string]any{}} {
		var args sdl.FieldQueryArguments
		if filter != nil {
			args = sdl.FieldQueryArguments{{Name: "filter", Value: filter}}
		}
		nodes, err := selectQueryParamsNodes(ctx, testSchemaService, &engines.DuckDB{}, info, "_objects", query, args, false)
		if err != nil {
			t.Fatalf("selectQueryParamsNodes: %v", err)
		}
		if where := nodes.ForName("where"); where != nil {
			if sql := collectSQL(t, where); strings.Contains(sql, "deleted_at IS NULL") {
				t.Errorf("@with_deleted must drop the predicate, got %q", sql)
			}
		}
	}
}

func collectSQL(t *testing.T, node *QueryPlanNode) string {
	t.Helper()
	node.provider = testSchemaService.Provider()
	node.engines = testSchemaService
	var children Results
	for _, child := range node.Nodes {
		child.provider = node.provider
		child.engines = node.engines
		sql, _, err := child.CollectFunc(child, nil, nil)
		if err != nil {
			t.Fatalf("child %s: %v", child.Name, err)
		}
		children = append(children, &Result{Name: child.Name, Result: sql})
	}
	sql, _, err := node.CollectFunc(node, children, nil)
	if err != nil {
		t.Fatalf("collect: %v", err)
	}
	return sql
}
