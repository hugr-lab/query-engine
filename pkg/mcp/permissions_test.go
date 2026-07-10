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

// A field whose return type is a hidden/disabled data object is hidden along
// with the table. baseTypeName is the catalog's already-unwrapped type name.
func TestVisibleFieldOfType_DataObject(t *testing.T) {
	f := &mcpFilter{perm: dataObjectRole()}
	if f.visibleFieldOfType("some_type", "rel", "hidden_table") {
		t.Error("relation to a hidden data-object must be hidden")
	}
	if f.visibleFieldOfType("some_type", "rel", "disabled_table") {
		t.Error("relation to a disabled data-object must be hidden")
	}
	// scalar and normal-table relations stay visible.
	if !f.visibleFieldOfType("some_type", "name", "String") {
		t.Error("scalar field must be visible")
	}
	if !f.visibleFieldOfType("some_type", "rel2", "normal_table") {
		t.Error("relation to a normal table must be visible")
	}
}

// Regression: a wildcard data-object:query field:"*" rule (allow-list pattern)
// must NOT hide scalar column types — dataObjectDenied guards scalars, matching
// the !sdl.IsScalarType guard used in the validator and introspection.
func TestDataObjectDenied_ScalarGuard(t *testing.T) {
	rp := &perm.RolePermissions{Permissions: []perm.Permission{
		{Object: "data-object:query", Field: "*", Hidden: true},
	}}
	f := &mcpFilter{perm: rp}

	for _, scalar := range []string{"String", "Int", "Boolean", "Float", "Timestamp"} {
		if !f.visibleFieldOfType("t", "col", scalar) {
			t.Errorf("scalar field of type %q must not be hidden by a wildcard data-object rule", scalar)
		}
	}
	// A real data-object relation IS hidden by the wildcard.
	if f.visibleFieldOfType("t", "rel", "other_table") {
		t.Error("relation to a data-object must be hidden by the wildcard rule")
	}
	// Empty type name is never a data object.
	if f.dataObjectDenied("") {
		t.Error("empty type name must not be denied")
	}
}
