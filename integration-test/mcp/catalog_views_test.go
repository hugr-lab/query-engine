//go:build duckdb_arrow

package mcp_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MCP does not read the catalog views directly — it goes through _search, and
// the engine reads the index for it. That indirection is what keeps the skills
// and the tool contract free of view names, and it is also what makes a move
// of those views invisible from here until search quietly stops ranking.
//
// These two tests close that gap from the MCP side: one pins WHERE the views
// live, the other proves the ranking read actually reaches them.

// TestCatalogViewsLiveInTheCatalogModule pins the module and the names. It is
// the test a rename has to walk past: everything else about MCP keeps working
// when a view moves, because the failure degrades into a lexical fallback
// rather than an error.
func TestCatalogViewsLiveInTheCatalogModule(t *testing.T) {
	res, err := testService.Query(context.Background(),
		`{ _module(name: "core.catalog") { name dataObjects { name } } }`, nil)
	require.NoError(t, err)
	require.NoError(t, res.Err())
	defer res.Close()

	var mod struct {
		Name        string `json:"name"`
		DataObjects []struct {
			Name string `json:"name"`
		} `json:"dataObjects"`
	}
	require.NoError(t, res.ScanData("_module", &mod))
	require.Equal(t, "core.catalog", mod.Name)

	served := make(map[string]bool, len(mod.DataObjects))
	for _, o := range mod.DataObjects {
		served[o.Name] = true
	}
	// The type name carries the SOURCE prefix and not the module, which is why
	// active_sources and stored_catalogs are not called data_sources and
	// catalogs: those type names are already taken by the core-db registry and
	// its m2m junction.
	for _, want := range []string{
		"core_modules", "core_module_data_sources", "core_data_objects",
		"core_fields", "core_relations", "core_functions", "core_types",
		"core_active_sources", "core_stored_catalogs",
		"core_catalog_dependencies", "core_annotations",
	} {
		assert.Truef(t, served[want], "core.catalog must serve %s", want)
	}
}

// TestCatalogSearchRankingReachesTheViews is the guard against a ranking query
// that silently never runs.
//
// catalog-search swallows any error from the ranking read and answers
// lexically, so a query naming a view that has moved looks exactly like "this
// deployment has no embedder" — forever, and everywhere. lexical_reason is what
// tells the two apart, and this reads it.
func TestCatalogSearchRankingReachesTheViews(t *testing.T) {
	// Without a vector size the views carry no @embeddings, so the ranking
	// query legitimately fails on _distance_to_query and falls back — which is
	// indistinguishable from the failure this test looks for. The guard only
	// means something where the index columns exist.
	if os.Getenv("EMBEDDER_VECTOR_SIZE") == "" {
		t.Skip("set EMBEDDER_URL and EMBEDDER_VECTOR_SIZE: without them the fallback is expected, not a symptom")
	}
	h := handler(t)
	mcpInit(t, h)

	page := catalogSearch(t, h, map[string]any{"query": "data sources"})
	// Allowlist, not denylist: an empty reason means the vector path ranked,
	// and the ONLY acceptable fallback reason is the missing embedder model.
	// Anything else — a validation error, a Binder error from a view whose SQL
	// lost a column, a missing relation — is the ranking read failing against
	// the catalog views, whatever layer it failed at. (The same guard lives in
	// TestSearchRankingQueryMatchesCatalogViews on the engine side; keep them
	// in step.)
	if page.LexicalReason != "" {
		assert.Contains(t, page.LexicalReason, "_system_embedder",
			"the ranking read fails against the catalog views for a reason other than the missing embedder: %s",
			page.LexicalReason)
	}
	require.NotEmpty(t, page.Items, "search must answer whichever path ranked")
}
