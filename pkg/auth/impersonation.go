package auth

import (
	"context"
	"fmt"

	"github.com/hugr-lab/query-engine/types"
)

type impersonatedByKeyType struct{}

// ContextWithImpersonatedBy stores the original admin identity when impersonation is active.
func ContextWithImpersonatedBy(ctx context.Context, original *AuthInfo) context.Context {
	return context.WithValue(ctx, impersonatedByKeyType{}, original)
}

// ImpersonatedByFromContext returns the original admin identity if impersonation is active.
func ImpersonatedByFromContext(ctx context.Context) *AuthInfo {
	info, _ := ctx.Value(impersonatedByKeyType{}).(*AuthInfo)
	return info
}

// IsImpersonated returns true if the current request is running under impersonation.
func IsImpersonated(ctx context.Context) bool {
	return ImpersonatedByFromContext(ctx) != nil
}

// ApplyImpersonationCtx checks if the context contains an AsUser identity override.
// If so, verifies the current auth is admin (secret key), overrides AuthInfo,
// and sets ImpersonatedBy. Returns the context unchanged if no AsUser is present.
func ApplyImpersonationCtx(ctx context.Context) (context.Context, error) {
	id := types.AsUserFromContext(ctx)
	if id == nil {
		return ctx, nil
	}

	original := AuthInfoFromContext(ctx)
	if original == nil || original.AuthProvider != "x-hugr-secret" {
		return ctx, fmt.Errorf("identity override requires authentication via secret key")
	}

	ctx = ContextWithImpersonatedBy(ctx, original)
	ctx = ContextWithAuthInfo(ctx, &AuthInfo{
		Role:         id.Role,
		UserId:       id.UserId,
		UserName:     id.UserName,
		AuthType:     "impersonation",
		AuthProvider: original.AuthProvider,
	})
	return ctx, nil
}

// ApplyImpersonationFromMessage applies identity override from IPC message fields.
// Only allowed when the connection was authenticated via secret key.
func ApplyImpersonationFromMessage(ctx context.Context, userId, userName, role string) (context.Context, error) {
	if userId == "" {
		return ctx, nil
	}

	original := AuthInfoFromContext(ctx)
	if original == nil || original.AuthProvider != "x-hugr-secret" {
		return ctx, fmt.Errorf("identity override requires authentication via secret key")
	}

	ctx = ContextWithImpersonatedBy(ctx, original)
	ctx = ContextWithAuthInfo(ctx, &AuthInfo{
		Role:         role,
		UserId:       userId,
		UserName:     userName,
		AuthType:     "impersonation",
		AuthProvider: original.AuthProvider,
	})
	return ctx, nil
}

// DetectImpersonation checks if the current auth context represents an impersonation
// via override headers on a secret-key authenticated request. If so, sets the
// ImpersonatedBy context for audit purposes.
func DetectImpersonation(ctx context.Context) context.Context {
	if IsImpersonated(ctx) {
		return ctx // already detected
	}
	ai := AuthInfoFromContext(ctx)
	if ai == nil || ai.AuthProvider != "x-hugr-secret" {
		return ctx
	}
	// If role or userId differ from defaults, override headers were used
	if ai.Role == "admin" && ai.UserId == "api" {
		return ctx // no override, using defaults
	}
	originalAdmin := &AuthInfo{
		Role:         "admin",
		UserId:       "api",
		UserName:     "api",
		AuthType:     "apiKey",
		AuthProvider: "x-hugr-secret",
	}
	ctx = ContextWithImpersonatedBy(ctx, originalAdmin)
	// Create a copy to avoid mutating the shared AuthInfo pointer
	overridden := *ai
	overridden.AuthType = "impersonation"
	ctx = ContextWithAuthInfo(ctx, &overridden)
	return ctx
}
