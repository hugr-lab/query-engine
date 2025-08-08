package auth

import "context"

type AuthInfo struct {
	Role     string
	UserId   string
	UserName string

	AuthType     string
	AuthProvider string
	Token        string
}

type authInfoKeyType string

const authInfoKey authInfoKeyType = "authInfo"
const fullAccessKey authInfoKeyType = "fullAccess"

func ContextWithAuthInfo(ctx context.Context, info *AuthInfo) context.Context {
	return context.WithValue(ctx, authInfoKey, info)
}

func AuthInfoFromContext(ctx context.Context) *AuthInfo {
	info, _ := ctx.Value(authInfoKey).(*AuthInfo)
	return info
}

func IsFullAccess(ctx context.Context) bool {
	ok, _ := ctx.Value(fullAccessKey).(bool)
	return ok
}

func ContextWithFullAccess(ctx context.Context) context.Context {
	return context.WithValue(ctx, fullAccessKey, true)
}
