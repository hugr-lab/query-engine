package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/perm"
)

func TestSubstitutePlaceholders(t *testing.T) {
	authInfo := &auth.AuthInfo{
		Role:         "admin",
		UserId:       "alice",
		UserName:     "Alice",
		AuthType:     "apiKey",
		AuthProvider: "x-hugr-secret",
	}
	ctxAuth := auth.ContextWithAuthInfo(context.Background(), authInfo)
	ctxAnon := context.Background()

	tests := []struct {
		name     string
		ctx      context.Context
		sql      string
		params   []any
		wantSQL  string
		wantArgs []any
	}{
		{
			name:     "no placeholders — unchanged",
			ctx:      ctxAuth,
			sql:      "func($1, $2)",
			params:   []any{1, 2},
			wantSQL:  "func($1, $2)",
			wantArgs: []any{1, 2},
		},
		{
			name:     "single auth placeholder",
			ctx:      ctxAuth,
			sql:      "func([$auth.user_id])",
			params:   nil,
			wantSQL:  "func($1)",
			wantArgs: []any{"alice"},
		},
		{
			name:     "multiple auth placeholders",
			ctx:      ctxAuth,
			sql:      "func([$auth.user_id], [$auth.role])",
			params:   nil,
			wantArgs: []any{"alice", "admin"},
		},
		{
			name:     "placeholder appended to existing params",
			ctx:      ctxAuth,
			sql:      "func($1, [$auth.user_id])",
			params:   []any{42},
			wantSQL:  "func($1, $2)",
			wantArgs: []any{42, "alice"},
		},
		{
			name:     "anonymous request — substitute NULL",
			ctx:      ctxAnon,
			sql:      "func([$auth.user_id])",
			params:   nil,
			wantSQL:  "func(NULL)",
			wantArgs: nil,
		},
		{
			name:     "anonymous mixed with existing params",
			ctx:      ctxAnon,
			sql:      "func($1, [$auth.user_id])",
			params:   []any{99},
			wantSQL:  "func($1, NULL)",
			wantArgs: []any{99},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSQL, gotParams := substitutePlaceholders(perm.AuthVars(tt.ctx), tt.sql, tt.params, false)

			// For multi-placeholder cases the iteration order over the placeholder map
			// is non-deterministic, so verify the SQL doesn't contain any raw placeholder
			// rather than asserting an exact string.
			if strings.Contains(gotSQL, "[$auth.") || strings.Contains(gotSQL, "[$catalog]") {
				t.Errorf("unsubstituted placeholder remains in SQL: %s", gotSQL)
			}
			if tt.wantSQL != "" && gotSQL != tt.wantSQL {
				// only enforce when wantSQL is set (single-placeholder tests)
				t.Errorf("SQL = %q, want %q", gotSQL, tt.wantSQL)
			}
			if !equalAnySliceUnordered(gotParams, tt.wantArgs) {
				t.Errorf("params = %v, want %v", gotParams, tt.wantArgs)
			}
		})
	}
}

// equalAnySliceUnordered compares two slices ignoring order (used for multi-placeholder
// substitution where map iteration order is non-deterministic).
func equalAnySliceUnordered(a, b []any) bool {
	if len(a) != len(b) {
		return false
	}
	used := make([]bool, len(b))
outer:
	for _, av := range a {
		for j, bv := range b {
			if !used[j] && av == bv {
				used[j] = true
				continue outer
			}
		}
		return false
	}
	return true
}

func TestSubstitutePlaceholders_UserIDIntZero(t *testing.T) {
	// Regression: when user_id is non-numeric (e.g. "alice"), strconv.Atoi
	// returns 0 in perm.AuthVars, and isEmptyContextValue(int(0)) must return
	// true so the placeholder resolves to NULL instead of leaking 0 as a
	// valid parameter.
	authInfo := &auth.AuthInfo{
		Role:     "admin",
		UserId:   "alice", // non-numeric → user_id_int becomes 0
		UserName: "Alice",
	}
	ctx := auth.ContextWithAuthInfo(context.Background(), authInfo)

	sql := "lookup([$auth.user_id_int])"
	got, params := substitutePlaceholders(perm.AuthVars(ctx), sql, nil, false)
	if got != "lookup(NULL)" {
		t.Errorf("expected NULL substitution for zero user_id_int, got %q", got)
	}
	if len(params) != 0 {
		t.Errorf("expected empty params, got %v", params)
	}
}

func TestSubstitutePlaceholders_AirportScalarBindsEmpty(t *testing.T) {
	// On Airport scalar functions an empty [$auth.*] must bind a typed non-NULL
	// empty instead of the literal NULL: DuckDB registers remote scalars with
	// default NULL handling, so a NULL argument short-circuits the row to NULL
	// before the Flight call runs and the remote handler never executes.

	// Anonymous request → placeholder absent (nil): a typed empty is synthesized.
	// String placeholder → "" bound as a param, not NULL.
	got, params := substitutePlaceholders(perm.AuthVars(context.Background()), "func([$auth.user_name])", nil, true)
	if got != "func($1)" {
		t.Errorf("SQL = %q, want func($1)", got)
	}
	if len(params) != 1 || params[0] != "" {
		t.Errorf("params = %v, want [\"\"]", params)
	}

	// Int placeholder ([$auth.user_id_int]) → int 0, matching perm.AuthVars' type.
	got, params = substitutePlaceholders(perm.AuthVars(context.Background()), "func([$auth.user_id_int])", nil, true)
	if got != "func($1)" {
		t.Errorf("SQL = %q, want func($1)", got)
	}
	if len(params) != 1 || params[0] != 0 {
		t.Errorf("params = %v, want [0]", params)
	}

	// Authenticated but empty-named identity → placeholder resolves to "" (the
	// real AuthVars value is reused): still bound, still non-NULL.
	ctxEmptyName := auth.ContextWithAuthInfo(context.Background(),
		&auth.AuthInfo{Role: "admin", UserId: "alice", UserName: ""})
	got, params = substitutePlaceholders(perm.AuthVars(ctxEmptyName), "func([$auth.user_name])", nil, true)
	if got != "func($1)" || len(params) != 1 || params[0] != "" {
		t.Errorf("empty-name identity: SQL=%q params=%v, want func($1) [\"\"]", got, params)
	}
}

func TestEmptyBindValue(t *testing.T) {
	// A non-nil resolved value is reused verbatim — it already carries the
	// placeholder's real Go type, keeping empty and non-empty binds consistent.
	if got := emptyBindValue("[$auth.user_id_int]", 0); got != 0 {
		t.Errorf("reuse int 0 → %v (%T), want 0 (int)", got, got)
	}
	if got := emptyBindValue("[$auth.user_name]", ""); got != "" {
		t.Errorf("reuse \"\" → %v, want \"\"", got)
	}
	// A nil value (identity entirely absent) synthesizes a typed empty from the
	// placeholder: [$auth.user_id_int] is the only Int, everything else a string.
	if got := emptyBindValue("[$auth.user_id_int]", nil); got != 0 {
		t.Errorf("nil [$auth.user_id_int] → %v (%T), want 0 (int)", got, got)
	}
	for _, ph := range []string{"[$auth.user_name]", "[$auth.user_id]", "[$auth.role]"} {
		if got := emptyBindValue(ph, nil); got != "" {
			t.Errorf("nil %s → %v, want \"\"", ph, got)
		}
	}
}

func TestSubstitutePlaceholders_CatalogNotInWhitelist(t *testing.T) {
	// [$catalog] is intentionally NOT in KnownArgPlaceholders. It is resolved
	// upstream by Function.SQL() before substitutePlaceholders runs, so the
	// generic loop must leave it alone if it ever appears here.
	ctx := context.Background()
	sql := "lookup([$catalog].x)"
	got, params := substitutePlaceholders(perm.AuthVars(ctx), sql, nil, false)
	if got != sql {
		t.Errorf("[$catalog] should be left unchanged by substitutePlaceholders, got %q", got)
	}
	if len(params) != 0 {
		t.Errorf("expected empty params, got %v", params)
	}
}
