//go:build duckdb_arrow

package mcp_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMCP_ReportWithDependentFilters drives the whole dependent-filters loop
// against the REAL engine, exactly as the widget does — no UI, no live host:
//
//  1. seed two registry rows (never loaded) so the option queries aggregate
//     actual data;
//  2. viz-report resolves BOTH option lists — the parent from a key-only
//     bucket aggregation (the shape that used to compile `FROM ()`), the
//     dependent filtered by the parent variable, null-dropped on the first
//     render;
//  3. the spec ECHOED from the response — the widget resends it verbatim —
//     goes back through report-data with a new parent value and
//     options_only: the dependent list narrows, the sections never run.
func TestMCP_ReportWithDependentFilters(t *testing.T) {
	h := handler(t)
	mcpInit(t, h)

	mutate := func(q string) {
		t.Helper()
		resp := jsonRPC(t, h, "tools/call", map[string]any{
			"name":      "data-execute_mutation",
			"arguments": map[string]any{"query": q, "max_result_size": 2000},
		})
		result := resp["result"].(map[string]any)
		require.NotEqual(t, true, result["isError"], "mutation failed: %v", result["content"])
	}
	for _, ds := range [][2]string{{"rep_duck", "duckdb"}, {"rep_pg", "postgres"}} {
		mutate(fmt.Sprintf(`mutation { core { insert_data_sources(data: {name: %[1]q, prefix: %[1]q, type: %[2]q, path: "", as_module: false, self_defined: true}) { name } } }`, ds[0], ds[1]))
	}
	defer mutate(`mutation { core { delete_data_sources(filter: {name: {in: ["rep_duck", "rep_pg"]}}) { affected_rows } } }`)

	spec := map[string]any{
		"title": "Sources report",
		"variables": []any{
			map[string]any{"name": "tp", "type": "String"},
		},
		"controls": []any{
			map[string]any{"label": "Type", "control": "select", "bind": "tp",
				"options_query": map[string]any{
					"query": `query { core { t: data_sources_bucket_aggregation { key { type } } } }`,
					"jq":    ".data.core.t | map(.key.type)"}},
			map[string]any{"label": "Source", "control": "select", "bind": "src",
				"options_query": map[string]any{
					"query": `query($tp: String) { core { data_sources(filter: {type: {eq: $tp}}, limit: 50) { name } } }`,
					"jq":    ".data.core.data_sources | map(.name)"}},
		},
		"sections": []any{
			map[string]any{"kind": "table", "title": "Sources",
				"query": `query($tp: String) { core { data_sources(filter: {type: {eq: $tp}}, limit: 50) { name type } } }`},
		},
	}
	// src exists only for the dependent control to bind.
	spec["variables"] = append(spec["variables"].([]any), map[string]any{"name": "src", "type": "String"})

	optionValues := func(sc map[string]any, i int) []string {
		controls, _ := sc["controls"].([]any)
		require.Greater(t, len(controls), i)
		ctl := controls[i].(map[string]any)
		require.Empty(t, ctl["error"], "control %d options failed", i)
		var out []string
		opts, _ := ctl["options"].([]any)
		for _, o := range opts {
			out = append(out, fmt.Sprintf("%v", o.(map[string]any)["value"]))
		}
		return out
	}

	resp := jsonRPC(t, h, "tools/call", map[string]any{
		"name":      "viz-report",
		"arguments": map[string]any{"spec": spec},
	})
	result := resp["result"].(map[string]any)
	require.NotEqual(t, true, result["isError"], "viz-report failed: %v", result["content"])
	sc := result["structuredContent"].(map[string]any)
	data := sc["data"].(map[string]any)

	// The parent options came from a key-only bucket aggregation over real
	// rows; the dependent's eq: $tp was null-dropped, so it lists everything.
	assert.Subset(t, optionValues(data, 0), []string{"duckdb", "postgres"})
	assert.Subset(t, optionValues(data, 1), []string{"rep_duck", "rep_pg"})
	sections := data["sections"].([]any)
	require.Len(t, sections, 1)
	assert.GreaterOrEqual(t, sections[0].(map[string]any)["row_count"].(float64), 2.0)

	// The widget's Apply-side echo: the SPEC FROM THE RESPONSE, verbatim.
	resp = jsonRPC(t, h, "tools/call", map[string]any{
		"name": "report-data",
		"arguments": map[string]any{
			"spec":         sc["spec"],
			"variables":    map[string]any{"tp": "postgres"},
			"options_only": true,
		},
	})
	result = resp["result"].(map[string]any)
	require.NotEqual(t, true, result["isError"], "echoed spec must parse: %v", result["content"])
	d2 := result["structuredContent"].(map[string]any)
	assert.Empty(t, d2["sections"], "options_only must not run the sections")

	dependent := optionValues(d2, 1)
	assert.Contains(t, dependent, "rep_pg", "the dependent list follows the new parent value")
	assert.NotContains(t, dependent, "rep_duck")
	assert.Subset(t, optionValues(d2, 0), []string{"duckdb", "postgres"})
}
