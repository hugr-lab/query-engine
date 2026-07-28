package perm

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/auth"
)

type contextKey string

const rolePermissionsKey contextKey = "role_permissions"

// fullAccess is the role Service.RolePermissions resolves a full-access
// context to: no rows, and without a matching row access is open.
var fullAccess = RolePermissions{Name: "admin"}

func CtxWithPerm(ctx context.Context, perm *RolePermissions) context.Context {
	return context.WithValue(ctx, rolePermissionsKey, perm)
}

// PermissionsFromCtx returns the permissions to enforce for this context.
//
// Full access answers the same way Service.RolePermissions does, and it wins
// over whatever the context already carries: an internal read that elevates
// runs on a context the endpoint middleware has already stamped with the
// caller's role, and the caller's rules must not follow it there. The
// validation rule short-circuits on the flag itself; the planner and the
// introspection resolvers come through here, so this is where the two agree.
func PermissionsFromCtx(ctx context.Context) *RolePermissions {
	if auth.IsFullAccess(ctx) {
		return &fullAccess
	}
	perm, _ := ctx.Value(rolePermissionsKey).(*RolePermissions)
	return perm
}
