package sdl

// KnownArgPlaceholders is the whitelist of context-variable placeholders that
// can be embedded in @function(sql: "...") templates and used as @arg_default
// values. At request time, the planner resolves these via perm.AuthVars(ctx).
// When the underlying value is unavailable (e.g. unauthenticated request, no
// impersonation), the planner substitutes NULL.
//
// Note: [$catalog] is intentionally NOT in this list. It is resolved upstream
// by Function.SQL() (in functions.go) before the planner's substitution loop
// runs, and perm.AuthVars does not populate it. Using it inside @arg_default
// would silently resolve to NULL, so it is excluded.
//
// Adding a new placeholder is a deliberate change: update this map and ensure
// perm.AuthVars populates the value.
var KnownArgPlaceholders = map[string]bool{
	"[$auth.user_name]":                 true,
	"[$auth.user_id]":                   true,
	"[$auth.user_id_int]":               true,
	"[$auth.role]":                      true,
	"[$auth.auth_type]":                 true,
	"[$auth.provider]":                  true,
	"[$auth.impersonated_by_role]":      true,
	"[$auth.impersonated_by_user_id]":   true,
	"[$auth.impersonated_by_user_name]": true,
}

// IsKnownPlaceholder reports whether name is a recognized context placeholder.
func IsKnownPlaceholder(name string) bool {
	return KnownArgPlaceholders[name]
}
