package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/catalog/sdl"
	"github.com/hugr-lab/query-engine/pkg/engines"
	"github.com/hugr-lab/query-engine/pkg/perm"
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
		// The engine contract split: null WIDENS, an explicit empty array is
		// a deliberate match-nothing.
		{"in null drops", map[string]any{
			"field2": map[string]any{"in": nil},
		}, true},
		{"in empty array is a condition", map[string]any{
			"field2": map[string]any{"in": []any{}},
		}, false},
		// Struct (nested object) filters follow the same contract instead of
		// compiling `WHERE (())`.
		{"struct filter with dropped leaf", map[string]any{
			"nested_field": map[string]any{"field2": map[string]any{"eq": nil}},
		}, true},
		{"struct field null", map[string]any{
			"nested_field": nil,
		}, true},
		{"live struct leaf survives", map[string]any{
			"nested_field": map[string]any{"field2": map[string]any{"eq": 1}},
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

// The widening contract is a convenience for the CALLER's own filters — an
// RLS rule that cannot be evaluated (a null auth claim null-dropping its
// condition) must fail CLOSED, not disappear.
func TestPermissionFilterFailsClosedOnNullDrop(t *testing.T) {
	base := context.Background()
	info := sdl.DataObjectInfo(testSchemaService.ForName(base, "table_object"))
	if info == nil {
		t.Fatal("table_object not found in the test schema")
	}
	query := &ast.Field{
		Name:             "table_object",
		ObjectDefinition: &ast.Definition{Name: "Query"},
	}
	mk := func(filter map[string]any) *QueryPlanNode {
		t.Helper()
		ctx := perm.CtxWithPerm(base, &perm.RolePermissions{
			Name:        "restricted",
			Permissions: []perm.Permission{{Object: "Query", Field: "table_object", Filter: filter}},
		})
		node, err := permissionFilterNode(ctx, testSchemaService, info, query, "_objects", false, perm.OpQuery)
		if err != nil {
			t.Fatalf("permissionFilterNode: %v", err)
		}
		if node == nil {
			t.Fatal("a role with a filter rule must produce an RLS node")
		}
		return node
	}

	// A live rule compiles into its condition.
	sql := collectSQLDeep(t, mk(map[string]any{"field2": map[string]any{"eq": 1}}))
	if !strings.Contains(sql, "field2") {
		t.Fatalf("the rule's condition is missing: %q", sql)
	}

	// A rule whose only condition null-drops compiles into FALSE — never
	// into nothing (fail-open) and never into empty SQL (syntax error).
	sql = collectSQLDeep(t, mk(map[string]any{"field2": map[string]any{"in": nil}}))
	if sql != "FALSE" {
		t.Fatalf("a null-dropped RLS rule must fail closed, got %q", sql)
	}
}

// filterHasConditions backs the quantifier drop: values decide at build time
// whether any condition survives the null-drop contract.
func TestFilterHasConditions(t *testing.T) {
	for name, tc := range map[string]struct {
		v    any
		want bool
	}{
		"nil":                 {nil, false},
		"scalar":              {1, true},
		"empty array":         {[]any{}, true}, // in: [] deliberately matches nothing
		"all-nil map":         {map[string]any{"a": nil, "b": map[string]any{"eq": nil}}, false},
		"live leaf deep down": {map[string]any{"a": map[string]any{"b": map[string]any{"eq": 1}}}, true},
	} {
		if got := filterHasConditions(tc.v); got != tc.want {
			t.Errorf("%s: filterHasConditions = %v, want %v", name, got, tc.want)
		}
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
