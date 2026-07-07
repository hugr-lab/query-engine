package sdl

import (
	"fmt"
	"strings"
	"testing"

	"github.com/vektah/gqlparser/v2/ast"
)

// mockSQLBuilder renders values as SQL literals like the real engines
// (single-quote-escaped strings, NULL for nil).
type mockSQLBuilder struct{}

func (mockSQLBuilder) SQLValue(v any) (string, error) {
	if v == nil {
		return "NULL", nil
	}
	if s, ok := v.(string); ok {
		return "'" + strings.ReplaceAll(s, "'", "''") + "'", nil
	}
	return fmt.Sprintf("%v", v), nil
}

func (mockSQLBuilder) FunctionCall(string, []any, map[string]any) (string, error) {
	return "", nil
}

func newView(sql string) *Object {
	return &Object{
		Type: ViewDataObject,
		sql:  sql,
		def:  &ast.Definition{Name: "v", Position: &ast.Position{}},
	}
}

func TestSubstituteContextPlaceholders(t *testing.T) {
	vars := map[string]any{
		"[$auth.user_id]":   "u'1", // contains a quote to exercise escaping
		"[$auth.role]":      "agent",
		"[$auth.tenant_id]": "acme", // a custom claim — must NOT be substituted
	}

	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "whitelisted placeholder substituted as escaped literal",
			sql:  "SELECT * FROM t WHERE user_id = [$auth.user_id]",
			want: "SELECT * FROM t WHERE user_id = 'u''1'",
		},
		{
			name: "multiple whitelisted placeholders",
			sql:  "SELECT * FROM t WHERE user_id = [$auth.user_id] AND role = [$auth.role]",
			want: "SELECT * FROM t WHERE user_id = 'u''1' AND role = 'agent'",
		},
		{
			name: "custom claim placeholder is left untouched (not whitelisted)",
			sql:  "SELECT * FROM t WHERE tenant = [$auth.tenant_id]",
			want: "SELECT * FROM t WHERE tenant = [$auth.tenant_id]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newView(tt.sql)
			if err := v.substituteContextPlaceholders(mockSQLBuilder{}, vars); err != nil {
				t.Fatalf("substituteContextPlaceholders: %v", err)
			}
			if v.sql != tt.want {
				t.Errorf("sql = %q, want %q", v.sql, tt.want)
			}
		})
	}
}

// An empty context value (unauthenticated request → nil, empty string, or a
// zero user_id_int from a non-numeric id) must render as NULL, not as a literal
// placeholder string or a bare 0 — otherwise "col = [$auth.*]" would silently
// match col = 0. Mirrors the function/@arg_default path.
func TestSubstituteContextPlaceholders_EmptyValueIsNull(t *testing.T) {
	cases := []struct {
		name string
		vars map[string]any
	}{
		{"nil vars (unauthenticated)", nil},
		{"empty string user_id", map[string]any{"[$auth.user_id]": ""}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := newView("SELECT * FROM t WHERE user_id = [$auth.user_id]")
			if err := v.substituteContextPlaceholders(mockSQLBuilder{}, tc.vars); err != nil {
				t.Fatalf("substituteContextPlaceholders: %v", err)
			}
			if want := "SELECT * FROM t WHERE user_id = NULL"; v.sql != want {
				t.Errorf("sql = %q, want %q", v.sql, want)
			}
		})
	}

	// zero user_id_int (non-numeric/absent id) must also coalesce to NULL,
	// not render as literal 0.
	v := newView("SELECT * FROM t WHERE category_id = [$auth.user_id_int]")
	if err := v.substituteContextPlaceholders(mockSQLBuilder{}, map[string]any{"[$auth.user_id_int]": 0}); err != nil {
		t.Fatalf("substituteContextPlaceholders: %v", err)
	}
	if want := "SELECT * FROM t WHERE category_id = NULL"; v.sql != want {
		t.Errorf("sql = %q, want %q (zero int must be NULL, not 0)", v.sql, want)
	}
}

func TestSQLHasContextPlaceholder(t *testing.T) {
	cases := map[string]bool{
		"SELECT * FROM t WHERE user_id = [$auth.user_id]":  true,
		"SELECT * FROM t WHERE role = [$auth.role]":        true,
		"SELECT * FROM t WHERE tenant = [$auth.tenant_id]": false, // custom claim
		"SELECT * FROM t": false,
		"":                false,
	}
	for sql, want := range cases {
		if got := newView(sql).SQLHasContextPlaceholder(); got != want {
			t.Errorf("SQLHasContextPlaceholder(%q) = %v, want %v", sql, got, want)
		}
	}
}
