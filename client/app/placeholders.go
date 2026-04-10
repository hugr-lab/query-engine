package app

// ContextPlaceholder is a typed wrapper for server-side context placeholders
// used with app.ArgFromContext. Use the predefined constants below — passing
// a string that isn't in the recognized whitelist will fail at registration.
type ContextPlaceholder string

// Recognized context placeholders. The hugr planner resolves these via
// perm.AuthVars(ctx) at request time. When the underlying value is unavailable
// (e.g. unauthenticated request), the planner substitutes NULL.
//
// This list mirrors pkg/catalog/sdl/placeholders.go on the server side; keep
// them in sync.
const (
	// AuthUserID — current user's ID as a string.
	AuthUserID ContextPlaceholder = "[$auth.user_id]"
	// AuthUserIDInt — current user's ID parsed as integer.
	AuthUserIDInt ContextPlaceholder = "[$auth.user_id_int]"
	// AuthUserName — current user's display name.
	AuthUserName ContextPlaceholder = "[$auth.user_name]"
	// AuthRole — current authenticated role.
	AuthRole ContextPlaceholder = "[$auth.role]"
	// AuthType — auth method (e.g. "apiKey", "jwt", "oidc", "impersonation").
	AuthType ContextPlaceholder = "[$auth.auth_type]"
	// AuthProvider — auth provider name.
	AuthProvider ContextPlaceholder = "[$auth.provider]"
	// AuthImpersonatedByRole — original role when impersonating.
	AuthImpersonatedByRole ContextPlaceholder = "[$auth.impersonated_by_role]"
	// AuthImpersonatedByUserID — original user ID when impersonating.
	AuthImpersonatedByUserID ContextPlaceholder = "[$auth.impersonated_by_user_id]"
	// AuthImpersonatedByUserName — original user name when impersonating.
	AuthImpersonatedByUserName ContextPlaceholder = "[$auth.impersonated_by_user_name]"
	// Catalog — current catalog name.
	Catalog ContextPlaceholder = "[$catalog]"
)

// knownArgPlaceholders is the validation set used by ArgFromContext.
var knownArgPlaceholders = map[ContextPlaceholder]bool{
	AuthUserID:                 true,
	AuthUserIDInt:              true,
	AuthUserName:               true,
	AuthRole:                   true,
	AuthType:                   true,
	AuthProvider:               true,
	AuthImpersonatedByRole:     true,
	AuthImpersonatedByUserID:   true,
	AuthImpersonatedByUserName: true,
	Catalog:                    true,
}

// IsKnownArgPlaceholder reports whether the given context placeholder is in
// the recognized whitelist for app.ArgFromContext.
func IsKnownArgPlaceholder(p ContextPlaceholder) bool {
	return knownArgPlaceholders[p]
}
