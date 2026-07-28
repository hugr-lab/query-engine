//go:build duckdb_arrow

package entity_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
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

func mcpHandler(t *testing.T, vectorSize int) http.Handler {
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
		MCPEnabled: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { service.Close() })
	require.NoError(t, service.Init(t.Context()))
	return mcpserver.New(service, nil, true).Handler()
}

func mcpCall(t *testing.T, h http.Handler, tool string, args map[string]any) map[string]any {
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
