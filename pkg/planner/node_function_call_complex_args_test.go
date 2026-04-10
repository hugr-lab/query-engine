package planner

import "testing"

func TestIsComplexValue(t *testing.T) {
	cases := []struct {
		name    string
		value   any
		complex bool
	}{
		{"nil", nil, false},
		{"string", "alice", false},
		{"int", 42, false},
		{"int64", int64(42), false},
		{"float", 3.14, false},
		{"bool", true, false},
		{"map[string]any", map[string]any{"foo": "bar"}, true},
		{"[]map[string]any", []map[string]any{{"a": 1}, {"b": 2}}, true},
		{"[]any with map", []any{map[string]any{"a": 1}}, true},
		{"[]any of scalars", []any{int64(1), int64(2), int64(3)}, false},
		{"[]any of strings", []any{"a", "b"}, false},
		{"empty []any", []any{}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isComplexValue(tc.value); got != tc.complex {
				t.Errorf("isComplexValue(%v) = %v, want %v", tc.value, got, tc.complex)
			}
		})
	}
}
