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

// --- catalog-describe ---

type describeResult struct {
	Items []struct {
		Kind            string   `json:"kind"`
		Name            string   `json:"name"`
		Type            string   `json:"type"`
		Description     string   `json:"description"`
		LongDescription string   `json:"long_description"`
		Module          string   `json:"module"`
		DataSource      string   `json:"data_source"`
		DataSources     []string `json:"data_sources"`
		PrimaryKey      []string `json:"primary_key"`
		FieldsCount     int      `json:"fields_count"`
		RelationsTotal  int      `json:"relations_total"`
		Properties      *struct {
			IsM2M      bool `json:"is_m2m"`
			HasVectors bool `json:"has_vectors"`
		} `json:"properties"`
		Queries []struct {
			Name         string `json:"name"`
			Type         string `json:"type"`
			RootTypeName string `json:"root_type_name"`
			Args         []struct {
				Name string `json:"name"`
				Type string `json:"type"`
			} `json:"args"`
		} `json:"queries"`
		Args []struct {
			Name string `json:"name"`
			Type string `json:"type"`
		} `json:"args"`
		Relations []struct {
			Name       string `json:"name"`
			Direction  string `json:"direction"`
			Kind       string `json:"kind"`
			FieldName  string `json:"field_name"`
			DataObject string `json:"data_object"`
		} `json:"relations"`
		RelationsOffset  int    `json:"relations_offset"`
		RelationsMore    bool   `json:"relations_has_more"`
		Returns          string `json:"returns"`
		IsTable          *bool  `json:"is_table"`
		Engine           string `json:"engine"`
		DataObjectsCount *int   `json:"data_objects_count"`
		FunctionsCount   *int   `json:"functions_count"`
	} `json:"items"`
	NotFound []string `json:"not_found"`
}

func catalogDescribe(t *testing.T, h http.Handler, args map[string]any) describeResult {
	t.Helper()
	resp := jsonRPC(t, h, "tools/call", map[string]any{
		"name":      "catalog-describe",
		"arguments": args,
	})
	require.Contains(t, resp, "result", "response: %v", resp)
	result := resp["result"].(map[string]any)
	require.NotEqual(t, true, result["isError"], "tool error: %v", result["content"])
	var out describeResult
	text := result["content"].([]any)[0].(map[string]any)["text"].(string)
	require.NoError(t, json.Unmarshal([]byte(text), &out), "payload: %s", text)
	return out
}

// TestCatalogDescribe_DataObject — the rung where an agent learns the exact
// query field names to write, and does NOT get the field list.
func TestCatalogDescribe_DataObject(t *testing.T) {
	h := handler(t)
	mcpInit(t, h)

	res := catalogDescribe(t, h, map[string]any{
		"kind":  "data_object",
		"names": []string{"core_data_sources"},
	})
	require.Empty(t, res.NotFound)
	require.Len(t, res.Items, 1)
	it := res.Items[0]

	assert.Equal(t, "data_object", it.Kind)
	assert.Equal(t, "core", it.Module)
	assert.Contains(t, []string{"TABLE", "VIEW"}, it.Type)
	assert.NotEmpty(t, it.PrimaryKey, "an agent needs the pk to write _by_pk")
	assert.Positive(t, it.FieldsCount, "the COUNT is here; the fields themselves are one call away")
	require.NotNil(t, it.Properties)

	byName := map[string]string{}
	for _, q := range it.Queries {
		byName[q.Name] = q.Type
		assert.NotEmpty(t, q.RootTypeName)
	}
	// The TYPE is core_data_sources; the query field inside module "core" is
	// data_sources — the prefix lives in the type name, not in the call. This
	// gap is the whole reason the tool exists.
	assert.Equal(t, "SELECT", byName["data_sources"])
	assert.Equal(t, "SELECT_ONE", byName["data_sources_by_pk"])
	assert.Equal(t, "AGGREGATION", byName["data_sources_aggregation"])

	// The by-pk query names its key in its arguments — that is how an agent
	// tells the pk lookup from a unique-key one.
	for _, q := range it.Queries {
		if q.Name != "data_sources_by_pk" {
			continue
		}
		var argNames []string
		for _, a := range q.Args {
			argNames = append(argNames, a.Name)
		}
		assert.ElementsMatch(t, it.PrimaryKey, argNames, "by_pk args ARE the primary key")
		for _, a := range q.Args {
			assert.NotEmpty(t, a.Type, "argument types are rendered as SDL, not left blank")
		}
	}

	// Relations carry the field to select to traverse them.
	assert.Equal(t, len(it.Relations), it.RelationsTotal)
	for _, r := range it.Relations {
		assert.Contains(t, []string{"FORWARD", "BACK"}, r.Direction)
		assert.Contains(t, []string{"FK", "M2M", "JOIN"}, r.Kind)
	}
}

// TestCatalogDescribe_RelationsPaging — relations are the one list in a
// description that can be long, so the cap must be a WINDOW, not a dead end:
// whatever it truncates has to be reachable with an offset.
func TestCatalogDescribe_RelationsPaging(t *testing.T) {
	h := handler(t)
	mcpInit(t, h)

	// Find an object with at least two relations to page through.
	var target string
	var total int
	for _, name := range []string{"core_data_sources", "core_catalog_sources", "core_catalogs"} {
		res := catalogDescribe(t, h, map[string]any{"kind": "data_object", "names": []string{name}})
		if len(res.Items) == 1 && res.Items[0].RelationsTotal >= 2 {
			target, total = name, res.Items[0].RelationsTotal
			break
		}
	}
	require.NotEmpty(t, target, "fixture needs an object with relations")

	full := catalogDescribe(t, h, map[string]any{"kind": "data_object", "names": []string{target}})
	require.Len(t, full.Items, 1)
	assert.Len(t, full.Items[0].Relations, total, "the default window holds this object whole")
	assert.False(t, full.Items[0].RelationsMore)

	first := catalogDescribe(t, h, map[string]any{
		"kind": "data_object", "names": []string{target}, "relations_limit": 1,
	})
	require.Len(t, first.Items, 1)
	require.Len(t, first.Items[0].Relations, 1)
	assert.Equal(t, total, first.Items[0].RelationsTotal, "total is the full count, not the window")
	assert.True(t, first.Items[0].RelationsMore, "truncation is announced")

	rest := catalogDescribe(t, h, map[string]any{
		"kind": "data_object", "names": []string{target},
		"relations_limit": 1, "relations_offset": 1,
	})
	require.Len(t, rest.Items, 1)
	require.Len(t, rest.Items[0].Relations, 1)
	assert.Equal(t, 1, rest.Items[0].RelationsOffset)
	assert.NotEqual(t, first.Items[0].Relations[0].Name, rest.Items[0].Relations[0].Name,
		"the offset moves the window — the truncated entries are reachable")
	assert.Equal(t, full.Items[0].Relations[1].Name, rest.Items[0].Relations[0].Name,
		"and it walks the same order the unpaged answer used")

	// Past the end: empty, not an error, and no phantom "more".
	beyond := catalogDescribe(t, h, map[string]any{
		"kind": "data_object", "names": []string{target}, "relations_offset": 10000,
	})
	require.Len(t, beyond.Items, 1)
	assert.Empty(t, beyond.Items[0].Relations)
	assert.False(t, beyond.Items[0].RelationsMore)
	assert.Equal(t, total, beyond.Items[0].RelationsTotal)
}

// TestCatalogDescribe_Batch — one call, several names, answered in the order
// asked, with the unresolvable ones named rather than silently dropped.
func TestCatalogDescribe_Batch(t *testing.T) {
	h := handler(t)
	mcpInit(t, h)

	res := catalogDescribe(t, h, map[string]any{
		"kind":  "data_object",
		"names": []string{"core_data_sources", "no_such_object", "core_catalog_sources"},
	})
	require.Len(t, res.Items, 2)
	assert.Equal(t, "core_data_sources", res.Items[0].Name, "answers follow the requested order")
	assert.Equal(t, "core_catalog_sources", res.Items[1].Name)
	assert.Equal(t, []string{"no_such_object"}, res.NotFound)
}

// TestCatalogDescribe_Function — a callable's signature, including whether it
// returns a row set.
func TestCatalogDescribe_Function(t *testing.T) {
	h := handler(t)
	mcpInit(t, h)

	res := catalogDescribe(t, h, map[string]any{
		"kind":   "function",
		"module": "core",
		"names":  []string{"load_data_source"},
	})
	require.Empty(t, res.NotFound)
	require.Len(t, res.Items, 1)
	it := res.Items[0]

	assert.Equal(t, "function", it.Kind)
	assert.Equal(t, "MUTATION", it.Type)
	assert.Equal(t, "core", it.Module)
	require.NotNil(t, it.IsTable)
	assert.NotEmpty(t, it.Returns, "the return type is rendered as SDL")

	var argNames []string
	for _, a := range it.Args {
		argNames = append(argNames, a.Name)
		assert.NotEmpty(t, a.Type)
	}
	assert.Contains(t, argNames, "name")
}

// TestCatalogDescribe_ModuleAndSource — the two cheap kinds.
func TestCatalogDescribe_ModuleAndSource(t *testing.T) {
	h := handler(t)
	mcpInit(t, h)

	mods := catalogDescribe(t, h, map[string]any{"kind": "module", "names": []string{"core"}})
	require.Len(t, mods.Items, 1)
	require.NotNil(t, mods.Items[0].DataObjectsCount)
	require.NotNil(t, mods.Items[0].FunctionsCount)
	assert.Positive(t, *mods.Items[0].DataObjectsCount)

	srcs := catalogDescribe(t, h, map[string]any{"kind": "data_source", "names": []string{"core"}})
	require.Len(t, srcs.Items, 1)
	assert.Equal(t, "data_source", srcs.Items[0].Kind)
	assert.NotEmpty(t, srcs.Items[0].Engine)
}

// TestCatalogDescribe_NoNames — the argument is required, and saying so beats
// returning an empty result an agent would read as "nothing exists".
func TestCatalogDescribe_NoNames(t *testing.T) {
	h := handler(t)
	mcpInit(t, h)

	resp := jsonRPC(t, h, "tools/call", map[string]any{
		"name":      "catalog-describe",
		"arguments": map[string]any{"kind": "data_object"},
	})
	result := resp["result"].(map[string]any)
	assert.Equal(t, true, result["isError"])
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
