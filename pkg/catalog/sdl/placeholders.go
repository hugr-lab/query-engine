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

// IsEmptyContextValue reports whether a context placeholder value is "empty"
// for the purpose of NULL substitution. Treats nil, empty string, and zero
// integer as empty — covers the [$auth.user_id_int] case where a non-numeric
// or absent user id resolves to 0. Used by both the function/@arg_default path
// and the @view(sql:) substitution so the same placeholder resolves to the
// same SQL regardless of where it is embedded.
func IsEmptyContextValue(v any) bool {
	switch val := v.(type) {
	case nil:
		return true
	case string:
		return val == ""
	case int:
		return val == 0
	case int64:
		return val == 0
	}
	return false
}
