package sdl

import (
	"testing"
)

func TestInterpolateVars(t *testing.T) {
	tests := []struct {
		name     string
		args     map[string]any
		vars     map[string]any
		expected map[string]any
	}{
		{
			name:     "nil args",
			args:     nil,
			vars:     map[string]any{"Year": "2026"},
			expected: nil,
		},
		{
			name:     "nil vars",
			args:     map[string]any{"path": "gkh_kapremont_$Year.fed_price"},
			vars:     nil,
			expected: map[string]any{"path": "gkh_kapremont_$Year.fed_price"},
		},
		{
			name:     "no variables in string",
			args:     map[string]any{"path": "field.subfield"},
			vars:     map[string]any{"Year": "2026"},
			expected: map[string]any{"path": "field.subfield"},
		},
		{
			name:     "single variable interpolation",
			args:     map[string]any{"path": "gkh_kapremont_$Year.fed_price"},
			vars:     map[string]any{"Year": "2026"},
			expected: map[string]any{"path": "gkh_kapremont_2026.fed_price"},
		},
		{
			name:     "integer variable",
			args:     map[string]any{"path": "catalog_$Year.field"},
			vars:     map[string]any{"Year": 2026},
			expected: map[string]any{"path": "catalog_2026.field"},
		},
		{
			name:     "multiple variables separated by dot",
			args:     map[string]any{"path": "$Prefix.$Year.field"},
			vars:     map[string]any{"Prefix": "gkh_kapremont", "Year": "2026"},
			expected: map[string]any{"path": "gkh_kapremont.2026.field"},
		},
		{
			name:     "unresolved variable left as-is",
			args:     map[string]any{"path": "prefix_$Unknown.field"},
			vars:     map[string]any{"Year": "2026"},
			expected: map[string]any{"path": "prefix_$Unknown.field"},
		},
		{
			name:     "non-string values unchanged",
			args:     map[string]any{"path": "catalog_$Year.field", "limit": 10},
			vars:     map[string]any{"Year": "2026"},
			expected: map[string]any{"path": "catalog_2026.field", "limit": 10},
		},
		{
			name:     "variable at start of string",
			args:     map[string]any{"path": "$Catalog.field"},
			vars:     map[string]any{"Catalog": "gkh_kapremont_2026"},
			expected: map[string]any{"path": "gkh_kapremont_2026.field"},
		},
		{
			name:     "variable at end of string",
			args:     map[string]any{"path": "catalog.$Field"},
			vars:     map[string]any{"Field": "fed_price"},
			expected: map[string]any{"path": "catalog.fed_price"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := InterpolateVars(tt.args, tt.vars)
			if tt.expected == nil {
				if result != nil {
					t.Errorf("expected nil, got %v", result)
				}
				return
			}
			if len(result) != len(tt.expected) {
				t.Errorf("expected %d keys, got %d", len(tt.expected), len(result))
				return
			}
			for k, ev := range tt.expected {
				rv, ok := result[k]
				if !ok {
					t.Errorf("missing key %q in result", k)
					continue
				}
				if rv != ev {
					t.Errorf("key %q: expected %v, got %v", k, ev, rv)
				}
			}
		})
	}
}

func Test_interpolateVarsInString(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		vars     map[string]any
		expected string
	}{
		{
			name:     "no vars",
			input:    "plain.path",
			vars:     map[string]any{},
			expected: "plain.path",
		},
		{
			name:     "single substitution",
			input:    "gkh_kapremont_$Year.field",
			vars:     map[string]any{"Year": "2026"},
			expected: "gkh_kapremont_2026.field",
		},
		{
			name:     "underscore in var name",
			input:    "prefix_$My_Var.field",
			vars:     map[string]any{"My_Var": "value"},
			expected: "prefix_value.field",
		},
		{
			name:     "adjacent to dot",
			input:    "$A.$B",
			vars:     map[string]any{"A": "foo", "B": "bar"},
			expected: "foo.bar",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := interpolateVarsInString(tt.input, tt.vars)
			if result != tt.expected {
				t.Errorf("expected %q, got %q", tt.expected, result)
			}
		})
	}
}
