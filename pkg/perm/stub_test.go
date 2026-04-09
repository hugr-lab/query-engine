package perm

import (
	"context"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/auth"
)

func TestStubStore_RolePermissions(t *testing.T) {
	stub := &StubStore{}
	role, err := stub.RolePermissions(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !role.CanImpersonate {
		t.Error("StubStore should return CanImpersonate=true")
	}
	if role.Name != "admin" {
		t.Errorf("Name = %q, want %q", role.Name, "admin")
	}
}

func TestStubStore_ContextWithPermissions(t *testing.T) {
	stub := &StubStore{}
	ctx := context.Background()
	ctx = auth.ContextWithAuthInfo(ctx, &auth.AuthInfo{
		Role:     "viewer",
		UserId:   "user-123",
		AuthType: "impersonation",
		ImpersonatedBy: &auth.AuthInfo{
			Role: "admin", UserId: "api", AuthType: "apiKey",
		},
	})
	ctx, err := stub.ContextWithPermissions(ctx)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	perm := PermissionsFromCtx(ctx)
	if perm == nil {
		t.Fatal("expected permissions in context")
	}
	if !perm.CanImpersonate {
		t.Error("StubStore should allow impersonation")
	}
}
