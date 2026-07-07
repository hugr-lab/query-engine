package auth

import (
	"context"
	"encoding/json"
)

type AuthInfo struct {
	Role     string
	UserId   string
	UserName string

	AuthType     string
	AuthProvider string
	Token        string

	// Claims holds the scalar claims carried by the authentication token
	// (JWT/OIDC), exposed to permission filters and mutation data as
	// [$auth.<claim>] placeholders. Nil for token-less auth (API key,
	// anonymous) and for impersonated identities. Built-in placeholders
	// (user_id, role, …) always take precedence over a same-named claim.
	Claims map[string]any

	// ImpersonatedBy holds the original admin identity when this request
	// is running under impersonation. Nil means no impersonation.
	ImpersonatedBy *AuthInfo
}

// ScalarClaims returns the subset of claims whose values are scalars
// (string, bool, or number) — the only claims usable as [$auth.<claim>]
// substitution values in filters and mutation data. Nested objects and
// arrays are dropped. Returns nil when nothing scalar remains.
func ScalarClaims(claims map[string]any) map[string]any {
	if len(claims) == 0 {
		return nil
	}
	out := make(map[string]any, len(claims))
	for k, v := range claims {
		switch v.(type) {
		case string, bool, float64, float32, int, int64, int32, json.Number:
			out[k] = v
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
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
