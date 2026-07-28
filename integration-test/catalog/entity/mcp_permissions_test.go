//go:build duckdb_arrow

package entity_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hugr-lab/query-engine/pkg/auth"
	"github.com/hugr-lab/query-engine/pkg/perm"
)

// Permissions on the SEARCH path, which only the entity storage can exercise:
// this is the one place where the ranking read runs with full access and a Go
// filter is the only thing between the index and the caller (design-035 D2).
// integration-test/mcp covers the structural half, where every read already
// happens in the caller's context and there is nothing to filter after the
// fact — but there the ranking always falls back to lexical, so the crossing
// tested here is never reached.

// roleHandler wraps the MCP handler with the identity and permissions the
// endpoint middleware installs in production.
func roleHandler(t testing.TB, h http.Handler, p *perm.RolePermissions) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := auth.ContextWithAuthInfo(r.Context(), &auth.AuthInfo{
			Role: p.Name, UserId: p.Name, UserName: p.Name,
			AuthType: "test", AuthProvider: "test",
		})
		h.ServeHTTP(w, r.WithContext(perm.CtxWithPerm(ctx, p)))
	})
}

func hitNames(payload map[string]any) []string {
	items, _ := payload["items"].([]any)
	out := make([]string, 0, len(items))
	for _, raw := range items {
		out = append(out, raw.(map[string]any)["name"].(string))
	}
	return out
}

func fieldHit(payload map[string]any, object, field string) bool {
	items, _ := payload["items"].([]any)
	for _, raw := range items {
		hit := raw.(map[string]any)
		if hit["object"] == object && hit["name"] == field {
			return true
		}
	}
	return false
}

// TestMCPSearchFiltersRankedHits — the index is read with full access, so a
// hidden object DOES rank; what must never happen is it reaching the caller.
// The admin half of every assertion is what makes it a filtering test rather
// than a "the embedder ranked something else today" test.
func TestMCPSearchFiltersRankedHits(t *testing.T) {
	url, size := liveEmbedder(t)
	_, admin := mcpService(t, size, url)
	restricted := roleHandler(t, admin, &perm.RolePermissions{
		Name: "entity_vis",
		Permissions: []perm.Permission{
			{Object: "data-object:query", Field: "core_api_keys", Hidden: true},
			{Object: "core_data_sources", Field: "path", Hidden: true},
		},
	})

	t.Run("object", func(t *testing.T) {
		args := map[string]any{"query": "keys used to authenticate a client", "limit": 200}

		full := mcpCall(t, admin, "catalog-search", args)
		require.Empty(t, full["lexical_reason"], "this test is about the vector path")
		require.Contains(t, hitNames(full), "core_api_keys", "the index ranks the object unrestricted")

		mine := mcpCall(t, restricted, "catalog-search", args)
		require.Empty(t, mine["lexical_reason"], "the role must still rank by vector")
		assert.NotContains(t, hitNames(mine), "core_api_keys", "a hidden object must not survive the filter")
		assert.Positive(t, mine["filtered_out"],
			"filtered_out is how an agent tells 'nothing matches' from 'nothing you may see matches'")
	})

	t.Run("field", func(t *testing.T) {
		args := map[string]any{
			"query": "connection string to reach the database",
			"kinds": []string{"field"}, "limit": 200,
		}

		full := mcpCall(t, admin, "catalog-search", args)
		require.Empty(t, full["lexical_reason"])
		require.True(t, fieldHit(full, "core_data_sources", "path"),
			"the field is in the index unrestricted: %v", hitNames(full))

		mine := mcpCall(t, restricted, "catalog-search", args)
		require.Empty(t, mine["lexical_reason"])
		assert.False(t, fieldHit(mine, "core_data_sources", "path"),
			"a hidden field must not survive the filter")
		assert.Positive(t, mine["filtered_out"])
	})
}

// TestMCPSearchWithoutCatalogViewAccess is the case the design was written
// for: the catalog views are ordinary data objects, so a role can be denied
// them outright — and MCP must keep working anyway. The ranking read is
// supposed to run with full access; if the caller's own rules still applied to
// it, this role would silently lose the vector path (and with it every field
// hit, which the lexical fallback cannot produce at all).
func TestMCPSearchWithoutCatalogViewAccess(t *testing.T) {
	url, size := liveEmbedder(t)
	_, base := mcpService(t, size, url)

	denied := []perm.Permission{
		// The other shape a rule takes: a row filter would not deny the read,
		// it would quietly shrink the index to nothing.
		{Object: "data-object:query", Field: "core_entity_fields",
			Filter: map[string]any{"name": map[string]any{"eq": "___no_such_field___"}}},
	}
	for _, view := range []string{
		"core_entity_modules", "core_entity_data_objects",
		"core_entity_functions", "core_entity_data_sources",
	} {
		denied = append(denied, perm.Permission{Object: "data-object:query", Field: view, Disabled: true})
	}
	h := roleHandler(t, base, &perm.RolePermissions{Name: "entity_no_views", Permissions: denied})

	payload := mcpCall(t, h, "catalog-search", map[string]any{
		"query": "where are the attached databases described", "limit": 20,
	})
	require.Empty(t, payload["lexical_reason"],
		"the ranking read must not be subject to the caller's rules on the catalog views")
	require.NotEqual(t, true, payload["lexical"])
	assert.NotEmpty(t, hitNames(payload), "and it must still answer")

	// Field hits are the proof the vector index was really read: the lexical
	// fallback drops the whole kind.
	fields := mcpCall(t, h, "catalog-search", map[string]any{
		"query": "connection string to reach the database",
		"kinds": []string{"field"}, "limit": 20,
	})
	assert.NotEmpty(t, hitNames(fields), "field hits require the index, not the fallback")
}

// TestMCPStructuralToolsUnderRole — the same permission story on the entity
// storage's structural half, so a regression in the on-the-fly generation
// cannot quietly widen what a role sees.
func TestMCPStructuralToolsUnderRole(t *testing.T) {
	_, base := mcpService(t, 0, "")
	h := roleHandler(t, base, &perm.RolePermissions{
		Name: "entity_struct",
		Permissions: []perm.Permission{
			{Object: "data-object:query", Field: "core_api_keys", Hidden: true},
			{Object: "core_data_sources", Field: "path", Hidden: true},
		},
	})

	full := mcpCall(t, base, "catalog-list", map[string]any{"kind": "data_object", "limit": 200})
	require.Contains(t, hitNames(full), "core_api_keys")

	mine := mcpCall(t, h, "catalog-list", map[string]any{"kind": "data_object", "limit": 200})
	assert.NotContains(t, hitNames(mine), "core_api_keys")

	fields := mcpCall(t, h, "catalog-object_fields", map[string]any{"object": "core_data_sources", "limit": 200})
	assert.NotContains(t, hitNames(fields), "path", "the hidden field is gone from the entity field surface too")
	assert.Contains(t, hitNames(fields), "name")
}
