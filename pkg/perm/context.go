package perm

import "context"

type contextKey string

const rolePermissionsKey contextKey = "role_permissions"

func CtxWithPerm(ctx context.Context, perm *RolePermissions) context.Context {
	return context.WithValue(ctx, rolePermissionsKey, perm)
}

func PermissionsFromCtx(ctx context.Context) *RolePermissions {
	perm, _ := ctx.Value(rolePermissionsKey).(*RolePermissions)
	return perm
}
