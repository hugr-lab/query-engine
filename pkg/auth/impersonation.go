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
//
// An omitted user name falls back to the user id: an impersonated identity
// must always carry a name, because an empty one resolves [$auth.user_name]
// to SQL NULL downstream — which on Airport scalar functions silently
// short-circuits the whole call before the remote handler runs.
func BuildImpersonatedAuthInfo(original *AuthInfo, targetUserId, targetUserName, targetRole string) *AuthInfo {
	if targetUserName == "" {
		targetUserName = targetUserId
	}
	return &AuthInfo{
		Role:           targetRole,
		UserId:         targetUserId,
		UserName:       targetUserName,
		AuthType:       "impersonation",
		AuthProvider:   original.AuthProvider,
		ImpersonatedBy: original,
	}
}
