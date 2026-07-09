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

// A relation field whose return type is a hidden/disabled data object must be
// hidden along with the table.
func TestVisibleFieldOfType_DataObject(t *testing.T) {
	f := &mcpFilter{perm: dataObjectRole()}
	for _, ft := range []string{"[hidden_table]", "hidden_table!", "[hidden_table!]!", "disabled_table"} {
		if f.visibleFieldOfType("some_type", "rel", ft) {
			t.Errorf("relation of type %q to a hidden/disabled data-object must be hidden", ft)
		}
	}
	// scalar and normal-table relations stay visible.
	if !f.visibleFieldOfType("some_type", "name", "String") {
		t.Error("scalar field must be visible")
	}
	if !f.visibleFieldOfType("some_type", "rel2", "[normal_table]") {
		t.Error("relation to a normal table must be visible")
	}
}

func TestBaseGraphQLTypeName(t *testing.T) {
	cases := map[string]string{
		"[Type!]!": "Type",
		"[Type]":   "Type",
		"Type!":    "Type",
		"Type":     "Type",
		" [X] ":    "X",
	}
	for in, want := range cases {
		if got := baseGraphQLTypeName(in); got != want {
			t.Errorf("baseGraphQLTypeName(%q) = %q, want %q", in, got, want)
		}
	}
}
