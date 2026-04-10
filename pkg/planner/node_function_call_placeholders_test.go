package planner

import (
	"context"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/auth"
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
			gotSQL, gotParams := substitutePlaceholders(tt.ctx, tt.sql, tt.params)

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

func TestSubstitutePlaceholders_CatalogPreserved(t *testing.T) {
	// [$catalog] is normally substituted upstream by Function.SQL() before reaching
	// substitutePlaceholders. Verify our generic loop also handles it correctly if
	// it does encounter the placeholder (e.g., via auth context catalog mechanism).
	ctx := context.Background()
	sql := "lookup([$catalog].x)"
	got, params := substitutePlaceholders(ctx, sql, nil)
	// No catalog in AuthVars by default → NULL substitution
	if !strings.Contains(got, "NULL") {
		t.Errorf("expected NULL substitution for unset catalog, got %q", got)
	}
	if len(params) != 0 {
		t.Errorf("expected empty params, got %v", params)
	}
}
