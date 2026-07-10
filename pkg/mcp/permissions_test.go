package mcp

import (
	"testing"

	"github.com/hugr-lab/query-engine/pkg/perm"
)

func dataObjectRole() *perm.RolePermissions {
	return &perm.RolePermissions{Permissions: []perm.Permission{
		{Object: "data-object:query", Field: "hidden_table", Hidden: true},
		{Object: "data-object:query", Field: "disabled_table", Disabled: true},
	}}
}

// A data object hidden or disabled by a data-object:query rule must not be
// exposed as an MCP tool, mirroring GraphQL introspection.
func TestVisibleType_DataObject(t *testing.T) {
	f := &mcpFilter{perm: dataObjectRole()}
	if f.visibleType("hidden_table") {
		t.Error("hidden data-object must not be visible in MCP")
	}
	if f.visibleType("disabled_table") {
		t.Error("disabled data-object must not be visible in MCP")
	}
	if !f.visibleType("normal_table") {
		t.Error("a table with no data-object rule must stay visible")
	}

	// nil filter (full access / no perms) → everything visible.
	var nilf *mcpFilter
	if !nilf.visibleType("hidden_table") {
		t.Error("nil filter must treat everything as visible")
	}
}

// A data-object-reference field (relation/join/aggregation — by hugr_type)
// whose return type is a hidden/disabled data object is hidden along with the
// table; a normal-table relation stays visible.
func TestVisibleFieldOfType_DataObjectRelation(t *testing.T) {
	f := &mcpFilter{perm: dataObjectRole()}
	if f.visibleFieldOfType("t", "rel", "hidden_table", "select") {
		t.Error("relation to a hidden data-object must be hidden")
	}
	if f.visibleFieldOfType("t", "j", "disabled_table", "join") {
		t.Error("join to a disabled data-object must be hidden")
	}
	if !f.visibleFieldOfType("t", "rel2", "normal_table", "select") {
		t.Error("relation to a normal table must be visible")
	}
}

// The key regression: a scalar or embedded-STRUCT field must NOT be hidden by a
// wildcard data-object:query rule — it is not a data-object reference (its
// hugr_type is not a relation type), so a table-level rule must not govern it.
func TestVisibleFieldOfType_WildcardSparesNonRelations(t *testing.T) {
	rp := &perm.RolePermissions{Permissions: []perm.Permission{
		{Object: "data-object:query", Field: "*", Hidden: true},
	}}
	f := &mcpFilter{perm: rp}

	// embedded struct field (e.g. specs: ProductSpecs) — hugr_type is not a
	// relation, so it stays visible even under a hide-all-tables wildcard.
	if !f.visibleFieldOfType("products", "specs", "ProductSpecs", "") {
		t.Error("struct field must not be hidden by a wildcard data-object rule")
	}
	if !f.visibleFieldOfType("products", "meta", "ProductMetadata", "extra_field") {
		t.Error("extra/struct field must not be hidden by a wildcard rule")
	}
	if !f.visibleFieldOfType("products", "name", "String", "") {
		t.Error("scalar field must not be hidden")
	}
	// a genuine relation IS hidden by the wildcard.
	if f.visibleFieldOfType("products", "tags", "tags_table", "join") {
		t.Error("relation must be hidden by the wildcard data-object rule")
	}
}

func TestIsDataObjectRefField(t *testing.T) {
	// Only fields whose RETURN type is the base data object itself.
	for _, ht := range []string{"select", "select_one", "join"} {
		if !isDataObjectRefField(ht) {
			t.Errorf("%q must be a data-object reference field", ht)
		}
	}
	// Aggregation fields return a synthetic _X_aggregation type; function-call
	// fields may return a scalar/struct — both are excluded (see doc comment).
	for _, ht := range []string{"", "extra_field", "function", "aggregate", "bucket_agg", "mutation_insert", "submodule"} {
		if isDataObjectRefField(ht) {
			t.Errorf("%q must NOT be a data-object reference field", ht)
		}
	}
}
