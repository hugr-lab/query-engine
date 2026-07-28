package perm

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/auth"
)

// Full access is an elevation, not a hint. An internal read that asks for it
// runs on a context the endpoint middleware has already stamped with the
// caller's role, so the flag has to win here — otherwise the elevation is
// silently a no-op everywhere enforcement reads the role from the context (the
// planner's row filters and table-level denials, the introspection resolvers)
// rather than from the flag (the validation rule).
func TestPermissionsFromCtxFullAccessOverridesRole(t *testing.T) {
	restricted := &RolePermissions{
		Name: "reader",
		Permissions: []Permission{
			{Object: "data-object:query", Field: "secrets", Disabled: true},
			{Object: "data-object:query", Field: "documents",
				Filter: map[string]any{"owner": map[string]any{"eq": "me"}}},
			{Object: "documents", Field: "body", Hidden: true},
		},
	}
	ctx := CtxWithPerm(context.Background(), restricted)

	if got := PermissionsFromCtx(ctx); got != restricted {
		t.Fatalf("without the flag the caller's own role is enforced, got %+v", got)
	}

	p := PermissionsFromCtx(auth.ContextWithFullAccess(ctx))
	if p == nil {
		t.Fatal("full access must resolve to a role, the same way Service.RolePermissions does")
	}
	if p.DataObjectDisabled("secrets", OpQuery) {
		t.Error("a table denied to the caller is still denied under full access")
	}
	if f := p.DataObjectFilter(context.Background(), "documents", OpQuery); f != nil {
		t.Errorf("the caller's row filter still narrows a full-access read: %v", f)
	}
	if _, ok := p.Visible("documents", "body"); !ok {
		t.Error("a field hidden from the caller is still hidden under full access")
	}
}

func TestPermissionsFromCtxWithoutPermissions(t *testing.T) {
	if got := PermissionsFromCtx(context.Background()); got != nil {
		t.Fatalf("nothing to enforce means nil, got %+v", got)
	}
}
