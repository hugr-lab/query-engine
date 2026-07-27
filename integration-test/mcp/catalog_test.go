//go:build duckdb_arrow

package mcp_test

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// catalogPage is the uniform pagination envelope every list-returning tool of
// the catalog-* family answers with.
type catalogPage struct {
	Items []struct {
		Kind        string   `json:"kind"`
		Name        string   `json:"name"`
		Type        string   `json:"type"`
		Module      string   `json:"module"`
		DataSource  string   `json:"data_source"`
		Description string   `json:"description"`
		DataObjects *int     `json:"data_objects"`
		Functions   *int     `json:"functions"`
		Submodules  *int     `json:"submodules"`
		ReadOnly    *bool    `json:"read_only"`
		AsModule    *bool    `json:"as_module"`
		IsExtension *bool    `json:"is_extension"`
		Modules     []string `json:"modules"`
	} `json:"items"`
	Total     int  `json:"total"`
	Limit     int  `json:"limit"`
	Offset    int  `json:"offset"`
	HasMore   bool `json:"has_more"`
	Truncated bool `json:"truncated"`
}

func (p catalogPage) names() []string {
	out := make([]string, 0, len(p.Items))
	for _, it := range p.Items {
		out = append(out, it.Name)
	}
	return out
}

// catalogList calls the tool and decodes its structured result.
func catalogList(t *testing.T, h http.Handler, args map[string]any) catalogPage {
	t.Helper()
	resp := jsonRPC(t, h, "tools/call", map[string]any{
		"name":      "catalog-list",
		"arguments": args,
	})
	require.Contains(t, resp, "result", "response: %v", resp)
	result := resp["result"].(map[string]any)
	require.NotEqual(t, true, result["isError"], "tool error: %v", result["content"])
	content := result["content"].([]any)
	require.NotEmpty(t, content)

	var page catalogPage
	text := content[0].(map[string]any)["text"].(string)
	require.NoError(t, json.Unmarshal([]byte(text), &page), "payload: %s", text)
	return page
}

// TestCatalogList_Modules closes the enumeration gap: with no query at all an
// agent can see every namespace it may use, as a FLAT list of dotted names.
func TestCatalogList_Modules(t *testing.T) {
	h := handler(t)
	mcpInit(t, h)

	page := catalogList(t, h, map[string]any{"kind": "module"})
	require.NotEmpty(t, page.Items)
	assert.Equal(t, len(page.Items), page.Total, "the whole enumeration fits one page here")
	assert.False(t, page.HasMore)
	assert.False(t, page.Truncated, "the core module tree is well inside the walk depth")

	names := page.names()
	assert.Contains(t, names, "", "the root module is a namespace too")
	assert.Contains(t, names, "core")
	assert.True(t, slices.IsSorted(names), "deterministic order: %v", names)

	for _, it := range page.Items {
		require.Equal(t, "module", it.Kind)
		require.NotNil(t, it.DataObjects, "counts tell an agent whether the module is worth opening")
		require.NotNil(t, it.Functions)
		require.NotNil(t, it.Submodules)
		if it.Name == "core" {
			assert.Positive(t, *it.DataObjects, "core holds the catalog views")
		}
	}
}

// TestCatalogList_Pagination pins the envelope: total counts everything,
// has_more reflects the remainder, and paging never repeats or drops an item.
func TestCatalogList_Pagination(t *testing.T) {
	h := handler(t)
	mcpInit(t, h)

	all := catalogList(t, h, map[string]any{"kind": "data_object", "limit": 200})
	require.Greater(t, all.Total, 3, "need a few objects to page through")

	first := catalogList(t, h, map[string]any{"kind": "data_object", "limit": 2})
	assert.Len(t, first.Items, 2)
	assert.Equal(t, all.Total, first.Total, "total is the full count, not the page size")
	assert.True(t, first.HasMore)
	assert.Equal(t, 2, first.Limit)

	second := catalogList(t, h, map[string]any{"kind": "data_object", "limit": 2, "offset": 2})
	assert.Equal(t, 2, second.Offset)
	assert.Equal(t, all.names()[2:4], second.names(), "offset walks the same deterministic order")

	// Past the end is an empty page, not an error.
	beyond := catalogList(t, h, map[string]any{"kind": "data_object", "offset": 10000})
	assert.Empty(t, beyond.Items)
	assert.False(t, beyond.HasMore)
	assert.Equal(t, all.Total, beyond.Total)

	// The hard cap wins over the argument.
	capped := catalogList(t, h, map[string]any{"kind": "data_object", "limit": 5000})
	assert.Equal(t, 200, capped.Limit)
}

// TestCatalogList_DataObjects checks the fact an agent actually needs from a
// data object entry: the module to nest it in and the source it came from.
func TestCatalogList_DataObjects(t *testing.T) {
	h := handler(t)
	mcpInit(t, h)

	page := catalogList(t, h, map[string]any{"kind": "data_object", "module": "core", "limit": 200})
	require.NotEmpty(t, page.Items)
	for _, it := range page.Items {
		assert.Equal(t, "data_object", it.Kind)
		assert.Contains(t, []string{"TABLE", "VIEW"}, it.Type, "object %s", it.Name)
		assert.True(t, it.Module == "core" || strings.HasPrefix(it.Module, "core."),
			"module scope walks the subtree, got %q", it.Module)
		assert.NotEmpty(t, it.DataSource)
	}

	// prefix narrows on the name, case-insensitively.
	pref := catalogList(t, h, map[string]any{"kind": "data_object", "prefix": "CORE_DATA"})
	require.NotEmpty(t, pref.Items, "case-insensitive prefix must match core_data_*")
	for _, it := range pref.Items {
		assert.Contains(t, it.Name, "core_data")
	}
	assert.Less(t, pref.Total, page.Total, "total is counted after filtering")
}

// TestCatalogList_Functions and data sources: the two remaining kinds, each
// carrying the discriminator an agent routes on.
func TestCatalogList_FunctionsAndSources(t *testing.T) {
	h := handler(t)
	mcpInit(t, h)

	fns := catalogList(t, h, map[string]any{"kind": "function", "limit": 200})
	require.NotEmpty(t, fns.Items)
	kinds := map[string]bool{}
	for _, it := range fns.Items {
		assert.Equal(t, "function", it.Kind)
		kinds[it.Type] = true
	}
	for k := range kinds {
		assert.Contains(t, []string{"FUNCTION", "MUTATION", "SUBSCRIPTION"}, k)
	}
	assert.True(t, kinds["MUTATION"], "core exposes mutation functions (load/unload data source)")

	sources := catalogList(t, h, map[string]any{"kind": "data_source"})
	require.NotEmpty(t, sources.Items)
	for _, it := range sources.Items {
		assert.Equal(t, "data_source", it.Kind)
		assert.NotEmpty(t, it.Name)
	}
	assert.Contains(t, sources.names(), "core")
}

// TestCatalogList_BadKind — an unknown kind names the valid ones rather than
// failing silently or returning an empty page.
func TestCatalogList_BadKind(t *testing.T) {
	h := handler(t)
	mcpInit(t, h)

	resp := jsonRPC(t, h, "tools/call", map[string]any{
		"name":      "catalog-list",
		"arguments": map[string]any{"kind": "tables"},
	})
	result := resp["result"].(map[string]any)
	assert.Equal(t, true, result["isError"])
	text := result["content"].([]any)[0].(map[string]any)["text"].(string)
	assert.Contains(t, text, "data_object", "the error must list the valid kinds")
}
