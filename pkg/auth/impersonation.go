package auth

import "context"

// ImpersonatedByFromContext returns the original admin identity if impersonation is active.
func ImpersonatedByFromContext(ctx context.Context) *AuthInfo {
	info := AuthInfoFromContext(ctx)
	if info == nil {
		return nil
	}
	return info.ImpersonatedBy
}

// IsImpersonated returns true if the current request is running under impersonation.
func IsImpersonated(ctx context.Context) bool {
	return ImpersonatedByFromContext(ctx) != nil
}

// BuildImpersonatedAuthInfo constructs an AuthInfo representing an impersonated identity.
// The target identity (userId, userName, role) becomes the effective identity.
// The original AuthInfo is preserved in ImpersonatedBy for audit purposes.
func BuildImpersonatedAuthInfo(original *AuthInfo, targetUserId, targetUserName, targetRole string) *AuthInfo {
	return &AuthInfo{
		Role:           targetRole,
		UserId:         targetUserId,
		UserName:       targetUserName,
		AuthType:       "impersonation",
		AuthProvider:   original.AuthProvider,
		ImpersonatedBy: original,
	}
}
