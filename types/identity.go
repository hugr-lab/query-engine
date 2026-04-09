package types

import "context"

// UserIdentity represents an impersonated user identity.
// When set in context via AsUser, queries and subscriptions execute
// as this user with this role's permissions.
type UserIdentity struct {
	UserId   string
	UserName string
	Role     string
}

type asUserKeyType struct{}

// AsUser returns a context that causes Query/Subscribe to execute
// as the specified user. Requires admin (secret key) authentication.
func AsUser(ctx context.Context, userId, userName, role string) context.Context {
	return context.WithValue(ctx, asUserKeyType{}, &UserIdentity{
		UserId:   userId,
		UserName: userName,
		Role:     role,
	})
}

// AsUserFromContext extracts impersonation identity from context.
func AsUserFromContext(ctx context.Context) *UserIdentity {
	id, _ := ctx.Value(asUserKeyType{}).(*UserIdentity)
	return id
}
