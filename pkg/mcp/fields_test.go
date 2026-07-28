//go:build duckdb_arrow

package mcp

import "testing"

// classifyFieldKind is shared by catalog-object_fields and catalog-search on
// purpose: they read different sources (the compiled type vs a verification
// probe through the owner object), and before they shared it the same field
// came back as a column from one and an extra from the other — which also made
// the field_kinds filter drop the wrong hits.
//
// The case that broke it: a field that takes ARGUMENTS but returns a plain
// (non-list) type — an aggregation field, a @function_call — read as a stored
// column to anything that only looked at the type.
func TestClassifyFieldKind(t *testing.T) {
	for _, tc := range []struct {
		name       string
		hugrType   string
		args       int
		isRelation bool
		want       string
	}{
		{"plain column", "String!", 0, false, fieldKindColumn},
		{"declared relation", "[shop_orders]", 9, true, fieldKindRelation},
		{"relation with a scalar type", "shop_customer", 0, true, fieldKindRelation},
		{"aggregation field", "_shop_orders_aggregation", 9, false, fieldKindExtra},
		{"function call, scalar return", "Float", 2, false, fieldKindExtra},
		{"list-typed extra without args", "[String]", 0, false, fieldKindExtra},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyFieldKind(tc.hugrType, tc.args, tc.isRelation); got != tc.want {
				t.Errorf("classifyFieldKind(%q, %d, %v) = %q, want %q",
					tc.hugrType, tc.args, tc.isRelation, got, tc.want)
			}
		})
	}
}
