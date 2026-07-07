package sdl

import "testing"

func TestIsEmptyContextValue(t *testing.T) {
	cases := []struct {
		name  string
		value any
		empty bool
	}{
		{"nil", nil, true},
		{"empty string", "", true},
		{"int zero", 0, true},
		{"int64 zero", int64(0), true},
		{"non-empty string", "alice", false},
		{"int positive", 42, false},
		{"int64 positive", int64(42), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := IsEmptyContextValue(tc.value); got != tc.empty {
				t.Errorf("IsEmptyContextValue(%v) = %v, want %v", tc.value, got, tc.empty)
			}
		})
	}
}

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
		"[$auth.userid]", // typo
		"[$auth]",
		"[$random]",
		"$auth.user_id",  // missing brackets
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
