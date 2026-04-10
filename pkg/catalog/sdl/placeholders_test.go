package sdl

import "testing"

func TestIsKnownPlaceholder(t *testing.T) {
	known := []string{
		"[$auth.user_name]",
		"[$auth.user_id]",
		"[$auth.user_id_int]",
		"[$auth.role]",
		"[$auth.auth_type]",
		"[$auth.provider]",
		"[$auth.impersonated_by_role]",
		"[$auth.impersonated_by_user_id]",
		"[$auth.impersonated_by_user_name]",
	}
	for _, p := range known {
		if !IsKnownPlaceholder(p) {
			t.Errorf("expected %q to be a known placeholder", p)
		}
	}

	unknown := []string{
		"",
		"[$auth.userid]",  // typo
		"[$auth]",
		"[$random]",
		"$auth.user_id",   // missing brackets
		"[$auth.user_id", // missing closing bracket
		"user_id",
		"[$catalog]", // intentionally NOT in @arg_default whitelist
	}
	for _, p := range unknown {
		if IsKnownPlaceholder(p) {
			t.Errorf("expected %q to NOT be a known placeholder", p)
		}
	}
}
