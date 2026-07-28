//go:build duckdb_arrow

package entity_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"

	hugr "github.com/hugr-lab/query-engine"
	"github.com/hugr-lab/query-engine/pkg/auth"
	coredb "github.com/hugr-lab/query-engine/pkg/data-sources/sources/runtime/core-db"
	"github.com/hugr-lab/query-engine/pkg/db"
	mcpserver "github.com/hugr-lab/query-engine/pkg/mcp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MCP on the ENTITY storage, which is where the vector index actually lives.
// The point of these tests is the failure mode a silent fallback would hide:
// catalog-search swallows any error from the ranking query and answers
// lexically, so a ranking query that names a column the entity views do not
// have would look exactly like "this deployment has no embedder" — forever,
// and in every deployment. lexical_reason is what makes the two
// distinguishable, and this test reads it.

func mcpHandler(t testing.TB, vectorSize int) http.Handler {
	t.Helper()
	return mcpHandlerWithEmbedder(t, vectorSize, "")
}

func mcpHandlerWithEmbedder(t testing.TB, vectorSize int, embedderURL string) http.Handler {
	t.Helper()
	service, err := hugr.New(hugr.Config{
		DB:             db.Config{Path: ""},
		CoreDB:         coredb.New(coredb.Config{VectorSize: vectorSize}),
		CatalogStorage: hugr.CatalogStorageEntity,
		Auth: &auth.Config{
			Providers: []auth.AuthProvider{
				auth.NewAnonymous(auth.AnonymousConfig{Allowed: true, Role: "admin"}),
			},
		},
		Embedder:   hugr.EmbedderConfig{URL: embedderURL, VectorSize: vectorSize},
		MCPEnabled: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { service.Close() })
	require.NoError(t, service.Init(context.Background()))
	return mcpserver.New(service, nil, false).Handler()
}

// liveEmbedder returns the configured embedder, or skips: the vector path
// cannot be exercised without one, and pretending otherwise — asserting only
// that the query validates — is what let the fallback hide everything.
func liveEmbedder(t testing.TB) (string, int) {
	t.Helper()
	url := os.Getenv("EMBEDDER_URL")
	if url == "" {
		t.Skip("set EMBEDDER_URL (and EMBEDDER_VECTOR_SIZE) to exercise vector ranking")
	}
	size := 0
	if v := os.Getenv("EMBEDDER_VECTOR_SIZE"); v != "" {
		fmt.Sscanf(v, "%d", &size)
	}
	require.Positive(t, size, "EMBEDDER_VECTOR_SIZE must be set alongside EMBEDDER_URL")
	return url, size
}

func mcpCall(t testing.TB, h http.Handler, tool string, args map[string]any) map[string]any {
	t.Helper()
	call := func(method string, params any) map[string]any {
		body, err := json.Marshal(map[string]any{
			"jsonrpc": "2.0", "id": 1, "method": method, "params": params,
		})
		require.NoError(t, err)
		req := httptest.NewRequest("POST", "/mcp", bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, req)
		raw, err := io.ReadAll(w.Result().Body)
		require.NoError(t, err)
		var out map[string]any
		if err := json.Unmarshal(raw, &out); err == nil {
			return out
		}
		for _, line := range bytes.Split(raw, []byte("\n")) {
			line = bytes.TrimSpace(line)
			if after, ok := bytes.CutPrefix(line, []byte("data: ")); ok {
				if err := json.Unmarshal(after, &out); err == nil {
					return out
				}
			}
		}
		t.Fatalf("unparsable MCP response: %s", raw)
		return nil
	}
	call("initialize", map[string]any{
		"protocolVersion": "2024-11-05",
		"capabilities":    map[string]any{},
		"clientInfo":      map[string]any{"name": "entity-test", "version": "1"},
	})
	resp := call("tools/call", map[string]any{"name": tool, "arguments": args})
	require.Contains(t, resp, "result", "response: %v", resp)
	result := resp["result"].(map[string]any)
	require.NotEqual(t, true, result["isError"], "tool error: %v", result["content"])
	var payload map[string]any
	text := result["content"].([]any)[0].(map[string]any)["text"].(string)
	require.NoError(t, json.Unmarshal([]byte(text), &payload), "payload: %s", text)
	return payload
}

// TestMCPSearchRankingQueryMatchesEntityViews is the guard against a ranking
// query that silently never runs. With a vector size configured the entity
// views carry the index columns, so the query must at least VALIDATE: if it
// still falls back, the reason must be the missing embedder, never a schema
// mismatch.
func TestMCPSearchRankingQueryMatchesEntityViews(t *testing.T) {
	h := mcpHandler(t, 8)

	payload := mcpCall(t, h, "catalog-search", map[string]any{"query": "data sources"})
	reason, _ := payload["lexical_reason"].(string)

	assert.NotContains(t, reason, "Cannot query field",
		"the ranking query names something the entity views do not have: %s", reason)
	assert.NotContains(t, reason, "Unknown argument",
		"the ranking query passes an argument the entity views do not take: %s", reason)
	assert.NotContains(t, reason, "Unknown type",
		"the ranking query names a view that does not exist: %s", reason)
	if reason != "" {
		t.Logf("fell back to lexical, as expected without an embedder: %s", reason)
	}
}

// TestMCPSearchWorksOnEntityStorage — whichever path ranks, the tool answers
// on the entity storage, which is the mode MCP could not run in at all before.
func TestMCPSearchWorksOnEntityStorage(t *testing.T) {
	h := mcpHandler(t, 0)

	payload := mcpCall(t, h, "catalog-search", map[string]any{"query": "data sources"})
	items, _ := payload["items"].([]any)
	require.NotEmpty(t, items, "search answers on the entity storage")

	var found bool
	for _, raw := range items {
		hit := raw.(map[string]any)
		assert.NotEmpty(t, hit["next_call"], "every hit names the next rung")
		if name, _ := hit["name"].(string); strings.Contains(name, "data_sources") {
			found = true
		}
	}
	assert.True(t, found, "the obvious match is in there")
}

// TestMCPListAndDescribeOnEntityStorage — the structural half over the entity
// storage: the same tools, the same answers, no compiled-schema views.
func TestMCPListAndDescribeOnEntityStorage(t *testing.T) {
	h := mcpHandler(t, 0)

	list := mcpCall(t, h, "catalog-list", map[string]any{"kind": "data_object", "limit": 200})
	items, _ := list["items"].([]any)
	require.NotEmpty(t, items)

	name := items[0].(map[string]any)["name"].(string)
	desc := mcpCall(t, h, "catalog-describe", map[string]any{
		"kind": "data_object", "names": []string{name},
	})
	described, _ := desc["items"].([]any)
	require.Len(t, described, 1)
	queries, _ := described[0].(map[string]any)["queries"].([]any)
	assert.NotEmpty(t, queries, "the entity storage reconstructs the query field names too")
}

// TestMCPSearchVectorRanking is the one test that actually RANKS. Everything
// else about search survives without an embedder, which is exactly why the
// vector path could rot unnoticed: the fallback answers, the tool looks fine,
// and nothing ever ran the query that matters.
//
// It needs a live embedder (EMBEDDER_URL) and skips without one.
func TestMCPSearchVectorRanking(t *testing.T) {
	url, size := liveEmbedder(t)
	h := mcpHandlerWithEmbedder(t, size, url)

	payload := mcpCall(t, h, "catalog-search", map[string]any{
		"query": "where are the attached databases described",
		"limit": 10,
	})

	reason, _ := payload["lexical_reason"].(string)
	require.Empty(t, reason, "vector ranking must be the path taken, not the fallback")
	require.NotEqual(t, true, payload["lexical"], "the answer must not be marked lexical")

	items, _ := payload["items"].([]any)
	require.NotEmpty(t, items, "vector ranking returned nothing")

	// The query shares no substring with the answer — that is the point. A
	// lexical ranker scores 0 for "where are the attached databases described"
	// against "core_data_sources"; only an embedding connects them.
	var names []string
	var lastScore float64 = 2
	for _, raw := range items {
		hit := raw.(map[string]any)
		names = append(names, hit["name"].(string))
		score := hit["score"].(float64)
		assert.LessOrEqual(t, score, lastScore, "hits must come back ordered by score")
		lastScore = score
		assert.NotEmpty(t, hit["next_call"], "every hit names the next rung")
	}
	t.Logf("semantic hits: %v", names)

	var found bool
	for _, n := range names {
		if strings.Contains(n, "data_source") {
			found = true
			break
		}
	}
	assert.True(t, found, "semantic search should reach the data-source catalog for %q, got %v",
		"where are the attached databases described", names)
}

// TestMCPSearchVectorFieldHits — field hits are the reason to search rather
// than list, and they are also the only hits the lexical fallback cannot
// produce at all. This is the only place they are exercised.
func TestMCPSearchVectorFieldHits(t *testing.T) {
	url, size := liveEmbedder(t)
	h := mcpHandlerWithEmbedder(t, size, url)

	payload := mcpCall(t, h, "catalog-search", map[string]any{
		"query": "connection string to reach the database",
		"kinds": []string{"field"},
		"limit": 10,
	})
	require.Empty(t, payload["lexical_reason"])

	items, _ := payload["items"].([]any)
	require.NotEmpty(t, items, "no field hits — the entity_fields index is not being ranked")

	for _, raw := range items {
		hit := raw.(map[string]any)
		assert.Equal(t, "field", hit["kind"])
		assert.NotEmpty(t, hit["object"], "a field hit must name its owning object")
		assert.NotEmpty(t, hit["hugr_type"], "a field hit must carry its type")
		assert.Contains(t, []any{"column", "relation", "extra"}, hit["field_kind"])
		if hit["field_kind"] == "relation" {
			assert.NotEmpty(t, hit["ref_object"],
				"a relation hit is a PATH — it must name where it leads")
		}
	}
}
