package perm

import (
	"reflect"
	"testing"
)

func TestDataObjectFilter(t *testing.T) {
	ctx := authCtx()
	own := map[string]any{"agent_id": map[string]any{"eq": "[$auth.user_id]"}}
	ownSub := map[string]any{"agent_id": map[string]any{"eq": "42"}}
	del := map[string]any{"pin": map[string]any{"eq": false}}

	r := RolePermissions{Permissions: []Permission{
		{Object: "data-object:query", Field: "skills", Filter: own},
		{Object: "data-object:delete", Field: "skills", Filter: del},
	}}

	if got := r.DataObjectFilter(ctx, "skills", OpQuery); !reflect.DeepEqual(got, ownSub) {
		t.Errorf("query filter = %#v, want %#v", got, ownSub)
	}
	if got := r.DataObjectFilter(ctx, "skills", OpDelete); !reflect.DeepEqual(got, del) {
		t.Errorf("delete filter must use the op-specific row, got %#v", got)
	}
	// no :update row — falls back to the :query filter (you cannot update rows
	// you cannot read unless an op row explicitly says otherwise)
	if got := r.DataObjectFilter(ctx, "skills", OpUpdate); !reflect.DeepEqual(got, ownSub) {
		t.Errorf("update filter must fall back to :query, got %#v", got)
	}
	if got := r.DataObjectFilter(ctx, "other_table", OpQuery); got != nil {
		t.Errorf("unmatched object must have no filter, got %#v", got)
	}
}

func TestDataObjectFilter_Wildcard(t *testing.T) {
	ctx := authCtx()
	exact := map[string]any{"agent_id": map[string]any{"eq": "a"}}
	all := map[string]any{"tenant": map[string]any{"eq": "t"}}
	r := RolePermissions{Permissions: []Permission{
		{Object: "data-object:query", Field: "*", Filter: all},
		{Object: "data-object:query", Field: "skills", Filter: exact},
	}}
	if got := r.DataObjectFilter(ctx, "skills", OpQuery); !reflect.DeepEqual(got, exact) {
		t.Errorf("exact object row must beat '*', got %#v", got)
	}
	if got := r.DataObjectFilter(ctx, "sessions", OpQuery); !reflect.DeepEqual(got, all) {
		t.Errorf("'*' row must apply to other data objects, got %#v", got)
	}
}

func TestDataObjectData_ForceStamp(t *testing.T) {
	ctx := authCtx()
	r := RolePermissions{Permissions: []Permission{
		{Object: "data-object:insert", Field: "skills", Data: map[string]any{"agent_id": "[$auth.user_id]"}},
	}}
	got := r.DataObjectData(ctx, "skills", OpInsert)
	want := map[string]any{"agent_id": "42"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("insert data = %#v, want %#v", got, want)
	}
	if got := r.DataObjectData(ctx, "skills", OpUpdate); got != nil {
		t.Errorf("no :update row must yield no data (no cross-op fallback for data), got %#v", got)
	}
}

func TestDataObjectDisabled(t *testing.T) {
	r := RolePermissions{Permissions: []Permission{
		{Object: "data-object:query", Field: "secrets", Disabled: true},
		{Object: "data-object:insert", Field: "audit_log", Disabled: true},
	}}

	// a disabled :query row denies the table on every path, mutations included
	for _, op := range []Op{OpQuery, OpInsert, OpUpdate, OpDelete} {
		if !r.DataObjectDisabled("secrets", op) {
			t.Errorf("disabled :query row must deny op %s", op)
		}
	}
	// an op-specific row denies only that op
	if !r.DataObjectDisabled("audit_log", OpInsert) {
		t.Error("disabled :insert row must deny insert")
	}
	if r.DataObjectDisabled("audit_log", OpQuery) {
		t.Error("disabled :insert row must not deny reads")
	}
	if r.DataObjectDisabled("other", OpQuery) {
		t.Error("unmatched object must stay open")
	}

	disabled := RolePermissions{Disabled: true}
	if !disabled.DataObjectDisabled("anything", OpQuery) {
		t.Error("disabled role denies everything")
	}
}

func TestDataObjectHidden(t *testing.T) {
	r := RolePermissions{Permissions: []Permission{
		{Object: "data-object:query", Field: "internal_notes", Hidden: true},
	}}
	if !r.DataObjectHidden("internal_notes") {
		t.Error("hidden :query row must hide the data object")
	}
	if r.DataObjectHidden("products") {
		t.Error("unmatched object must stay visible")
	}
}

// data-object rows live in a synthetic type_name namespace ("data-object:...")
// that cannot collide with GraphQL type names, so they must not leak into
// field-level lookups — and field-level rows must not answer data-object
// lookups.
func TestDataObjectRows_DoNotAffectFieldLevel(t *testing.T) {
	ctx := authCtx()
	r := RolePermissions{Permissions: []Permission{
		{Object: "data-object:query", Field: "skills", Disabled: true, Filter: map[string]any{"x": map[string]any{"eq": 1}}},
	}}
	if _, ok := r.Enabled("skills", "name"); !ok {
		t.Error("data-object row must not disable field-level access checks")
	}
	if got := r.FilterArgument(ctx, "skills", "name"); got != nil {
		t.Errorf("data-object row must not produce a field-level filter, got %#v", got)
	}

	fieldOnly := RolePermissions{Permissions: []Permission{
		{Object: "Query", Field: "skills", Filter: map[string]any{"x": map[string]any{"eq": 1}}},
	}}
	if got := fieldOnly.DataObjectFilter(ctx, "skills", OpQuery); got != nil {
		t.Errorf("field-level rows must not answer data-object lookups, got %#v", got)
	}
	if fieldOnly.DataObjectDisabled("skills", OpQuery) {
		t.Error("field-level rows must not disable data objects")
	}
}

func TestDataObjectMatch_EmptyObjType(t *testing.T) {
	r := RolePermissions{Permissions: []Permission{
		{Object: "data-object:query", Field: "*", Disabled: true},
	}}
	// an empty object type (non-data-object target) never matches, even "*"
	if r.DataObjectDisabled("", OpQuery) {
		t.Error("empty objType must not match data-object rows")
	}
}
