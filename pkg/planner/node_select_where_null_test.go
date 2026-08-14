package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/vektah/gqlparser/v2/ast"
)

// "A null value drops the filter condition" is an operator-agnostic engine
// contract, and since design/039 it is load-bearing: every optional report and
// viz filter compiles to `filter: {f: {op: $var}}` and counts on a null
// variable removing the predicate — no sentinel values, no conditional query
// text. The drop has to survive the boolean combinators too:
// `_and: [{a: {eq: null}}, {b: {eq: null}}]` used to compile every dropped
// member into an empty group — `WHERE (() AND ())` — which no SQL parser
// accepts.
func TestNullDroppedConditionsSurviveCombinators(t *testing.T) {
	ctx := context.Background()
	info := sdl.DataObjectInfo(testSchemaService.ForName(ctx, "table_object"))
	if info == nil {
		t.Fatal("table_object not found in the test schema")
	}

	null := map[string]any{"eq": nil}
	live := map[string]any{"eq": 1}

	tests := []struct {
		name   string
		filter map[string]any
		// wantEmpty: the whole WHERE must vanish; otherwise the live
		// condition must survive alone, with no empty groups anywhere.
		wantEmpty bool
	}{
		{"and of dropped members", map[string]any{
			"_and": []any{map[string]any{"field2": null}, map[string]any{"field1": map[string]any{"eq": nil}}},
		}, true},
		{"or of dropped members", map[string]any{
			"_or": []any{map[string]any{"field2": null}},
		}, true},
		{"not over a dropped condition", map[string]any{
			"_not": map[string]any{"field2": null},
		}, true},
		{"not over an empty object", map[string]any{
			"_not": map[string]any{},
		}, true},
		{"and of empty objects", map[string]any{
			"_and": []any{map[string]any{}, map[string]any{}},
		}, true},
		{"null member in the list", map[string]any{
			"_and": []any{nil, map[string]any{"field2": live}},
		}, false},
		{"live member survives its dropped sibling", map[string]any{
			"_and": []any{map[string]any{"field2": live}, map[string]any{"field1": map[string]any{"eq": nil}}},
		}, false},
		{"nested combinator drops as a whole", map[string]any{
			"_and": []any{
				map[string]any{"field2": live},
				map[string]any{"_or": []any{map[string]any{"field1": map[string]any{"eq": nil}}}},
			},
		}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := sdl.FieldQueryArguments{{Name: "filter", Value: tt.filter}}
			query := &ast.Field{Name: "table_object"}

			nodes, err := selectQueryParamsNodes(ctx, testSchemaService, &engines.DuckDB{}, info, "_objects", query, args, false)
			if err != nil {
				t.Fatalf("selectQueryParamsNodes: %v", err)
			}
			where := nodes.ForName("where")
			if where == nil {
				if !tt.wantEmpty {
					t.Fatal("the live condition lost its where node")
				}
				return
			}
			sql := collectSQLDeep(t, where)
			if strings.Contains(sql, "()") {
				t.Fatalf("empty group survived into SQL: %q", sql)
			}
			if tt.wantEmpty {
				if sql != "" {
					t.Fatalf("every condition was null-dropped, want an empty WHERE, got %q", sql)
				}
				return
			}
			if !strings.Contains(sql, "field2") {
				t.Fatalf("the live condition is gone: %q", sql)
			}
			if strings.Contains(sql, "field1") {
				t.Fatalf("a dropped condition leaked into SQL: %q", sql)
			}
		})
	}
}

// collectSQLDeep runs the collect pass over the whole subtree — combinator
// nodes hold nested where nodes, which the flat helper in the soft-delete
// test never needed.
func collectSQLDeep(t *testing.T, node *QueryPlanNode) string {
	t.Helper()
	node.provider = testSchemaService.Provider()
	node.engines = testSchemaService
	var children Results
	for _, child := range node.Nodes {
		children = append(children, &Result{Name: child.Name, Result: collectSQLDeep(t, child)})
	}
	sql, _, err := node.CollectFunc(node, children, nil)
	if err != nil {
		t.Fatalf("collect %s: %v", node.Name, err)
	}
	return sql
}
