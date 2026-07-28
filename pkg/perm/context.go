package perm

import (
	"context"

	"github.com/hugr-lab/query-engine/pkg/auth"
)

type contextKey string

const rolePermissionsKey contextKey = "role_permissions"

// fullAccess is the role Service.RolePermissions resolves a full-access
// context to: no rows, and without a matching row access is open.
//
// READ-ONLY. PermissionsFromCtx hands out its address, so every elevated
// request in the process shares this one value; nothing may write through the
// returned pointer. (Nothing does — RolePermissions is only ever read after
// loading — and the empty Permissions slice means bestMatch returns nil, so
// there is no row to mutate either.)
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
//
// Two consequences, since this is the process-wide answer and not an MCP-local
// one: the auth UDFs (core_auth_my_permissions, core_auth_check_access) report
// admin when they run inside an elevated read rather than the role the context
// was stamped with, and an elevated read no longer carries the caller's row
// filters or data defaults. Both follow from "full access means admin", which
// is what the flag has always meant to Service.RolePermissions — only the
// planner disagreed. Every elevation site is internal (cluster, cache, catalog
// and data-source maintenance, role loading, MCP's ranking read); none of them
// runs a query the caller supplied.
func PermissionsFromCtx(ctx context.Context) *RolePermissions {
	if auth.IsFullAccess(ctx) {
		return &fullAccess
	}
	perm, _ := ctx.Value(rolePermissionsKey).(*RolePermissions)
	return perm
}
