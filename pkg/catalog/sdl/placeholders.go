package sdl

// KnownArgPlaceholders is the whitelist of context-variable placeholders that
// are allowed in @function(sql: "...") templates and @arg_default directives.
//
// At request time, the planner resolves these via perm.AuthVars(ctx). When the
// underlying value is unavailable (e.g. unauthenticated request, no
// impersonation), the planner substitutes NULL.
//
// Adding a new placeholder is a deliberate change: update this map and ensure
// perm.AuthVars (or another resolver) populates the value.
var KnownArgPlaceholders = map[string]bool{
	"[$auth.user_name]":                  true,
	"[$auth.user_id]":                    true,
	"[$auth.user_id_int]":                true,
	"[$auth.role]":                       true,
	"[$auth.auth_type]":                  true,
	"[$auth.provider]":                   true,
	"[$auth.impersonated_by_role]":       true,
	"[$auth.impersonated_by_user_id]":    true,
	"[$auth.impersonated_by_user_name]":  true,
	"[$catalog]":                         true,
}

// IsKnownPlaceholder reports whether name is a recognized context placeholder.
func IsKnownPlaceholder(name string) bool {
	return KnownArgPlaceholders[name]
}
