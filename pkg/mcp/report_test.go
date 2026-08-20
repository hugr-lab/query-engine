package mcp

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/hugr-lab/query-engine/pkg/mcp/reports"
	"github.com/hugr-lab/query-engine/types"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReportToolsDeclareTheAppsMetadata(t *testing.T) {
	s := New(nil, server.NewMCPServer("t", "0", server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, true)), Config{})
	tools := s.mcp.ListTools()

	report, ok := tools["viz-report"]
	require.True(t, ok, "viz-report must be registered")
	ui := uiMeta(t, report.Tool.Meta)
	assert.Equal(t, reportResourceURI, ui["resourceUri"], "viz-report must open the report view")
	assert.Equal(t, []any{"model", "app"}, jsonRoundTrip(t, ui["visibility"]))

	// Apply's channel carries the spec verbatim — not a tool to hand the model.
	data, ok := tools["report-data"]
	require.True(t, ok, "report-data must be registered")
	ui = uiMeta(t, data.Tool.Meta)
	assert.Equal(t, []any{"app"}, jsonRoundTrip(t, ui["visibility"]))
	assert.NotContains(t, ui, "resourceUri", "a re-run must feed the open view, not open a new one")
}

func TestReportPageIsSelfContained(t *testing.T) {
	page := reportPage()

	assert.Contains(t, page, "echarts", "the chart runtime must be inlined — the CSP denies every CDN")
	assert.Contains(t, page, "hugrVizCore", "the shared core must be inlined")
	assert.NotContains(t, page, "<!--__ECHARTS__-->")
	assert.NotContains(t, page, "<!--__VIZCORE__-->")
	assert.NotContains(t, page, "https://cdn.")
	// The widget path gets its payload from the tool result; the island must
	// hold an EMPTY object, not the placeholder text.
	assert.NotContains(t, page, "__REPORT_PAYLOAD__", "the payload island must be substituted")
	assert.Contains(t, page, `<script type="application/json" id="report-payload">{}</script>`)
	assert.Equal(t, 4, strings.Count(page, "</script>"), "library, core, the JSON island and the app script")
	// PDF is the browser's print engine over this block; losing it silently
	// would make every "save as PDF" cut charts in half.
	assert.Contains(t, page, "@media print")
	assert.Contains(t, page, "break-inside: avoid")

	entry, ok := New(nil, server.NewMCPServer("t", "0", server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, true)), Config{}).mcp.ListResources()[reportResourceURI]
	require.True(t, ok, "the report view must be served under %s", reportResourceURI)
	assert.Equal(t, vizMIMEType, entry.Resource.MIMEType)
}

// Rows belong to the view: the wire copy carries a sample and the view pulls
// the full set through report-data. Cards are never sampled.
func TestReportResultSamplesLargeSections(t *testing.T) {
	data := &reports.ReportData{Sections: make([]reports.SectionData, 2)}
	data.Sections[0].Kind = "chart"
	for i := 0; i < 200; i++ {
		data.Sections[0].Rows = append(data.Sections[0].Rows, map[string]any{"i": i})
	}
	data.Sections[0].RowCount = 200
	data.Sections[1] = reports.SectionData{Kind: "table", Rows: []map[string]any{{"a": 1}}, RowCount: 1}

	out := sampleReportData(data)
	assert.Len(t, out.Sections[0].Rows, vizModelRows)
	assert.True(t, out.Sections[0].RowsSampled)
	assert.Equal(t, 200, out.Sections[0].RowCount, "the real length still travels")
	assert.False(t, out.Sections[1].RowsSampled)
	assert.Len(t, data.Sections[0].Rows, 200, "the caller's data is left intact")
}

func TestReportFallbackTextIsReadable(t *testing.T) {
	spec := &reports.Spec{
		Title:       "Quarterly review",
		Description: "one page over the numbers",
		Sections: []reports.Section{
			{Kind: "kpi", Title: "Headline"},
			{Kind: "chart", Title: "Trend", Chart: &reports.ChartSpec{Type: "line", X: "month", Y: []string{"total"}}},
			{Kind: "table", Title: "Top"},
			{Kind: "text", Markdown: "## Notes\nplain narrative"},
		},
	}
	pct := 3.25
	data := &reports.ReportData{Sections: []reports.SectionData{
		{Kind: "kpi", Kpis: []VizKPI{{Label: "Revenue", Value: 15635261.0, Unit: "$", DeltaPct: &pct}}},
		{Kind: "chart", RowCount: 24},
		{Kind: "table", Error: "query failed: boom"},
		{Kind: "text"},
	}}

	text := reportFallbackText(spec, data)
	assert.Contains(t, text, "# Quarterly review")
	assert.Contains(t, text, "| Revenue | 15635261 $ | +3.25% |", "cards in full, human numbers")
	assert.Contains(t, text, "line chart of total over month — 24 rows")
	assert.Contains(t, text, "⚠️ section failed: query failed: boom")
	assert.Contains(t, text, "plain narrative")
	assert.NotContains(t, text, "{", "a summary, not a JSON dump")
}

// The end-to-end wire: a spec through the real tool handler against the stub
// engine — parse, validate, run, sample, envelope.
func TestVizReportHandlerRunsTheSpec(t *testing.T) {
	s := New(reportStub{}, server.NewMCPServer("t", "0", server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, true)), Config{})

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{
		"spec": map[string]any{
			"title": "T",
			"sections": []any{
				map[string]any{"kind": "table", "query": "{ m { obj(limit: 5) { a } } }"},
			},
		},
	}
	res, err := s.vizReport(t.Context(), req)
	require.NoError(t, err)
	require.False(t, res.IsError, "%v", res.Content)
	env, ok := res.StructuredContent.(ReportEnvelope)
	require.True(t, ok)
	assert.Equal(t, "T", env.Spec.Title)
	require.Len(t, env.Data.Sections, 1)
	assert.Empty(t, env.Data.Sections[0].Error)
	assert.Equal(t, 2, env.Data.Sections[0].RowCount)

	// A broken spec fails loudly before anything runs.
	req.Params.Arguments = map[string]any{
		"spec": map[string]any{"title": "T", "sections": []any{map[string]any{"kind": "graph"}}},
	}
	res, err = s.vizReport(t.Context(), req)
	require.NoError(t, err)
	assert.True(t, res.IsError)
}

// The rake, stepped on twice now: a failure the MODEL has to fix must come
// back as isError — a "successful" result with error text inside makes the
// host render the broken document and the model retry the same spec forever.
// viz-report therefore refuses to render unless the run is clean; the
// per-section degradation lives in report-data, where a USER moving filters
// must not lose the whole document.
func TestVizReportRefusesToRenderADirtyRun(t *testing.T) {
	s := New(reportStub{}, server.NewMCPServer("t", "0", server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, true)), Config{})

	spec := map[string]any{
		"title": "T",
		"sections": []any{
			map[string]any{"kind": "table", "title": "Good", "query": "{ m { obj(limit: 5) { a } } }"},
			map[string]any{"kind": "table", "title": "Bad", "query": "{ m { obj_boom(limit: 5) { a } } }"},
		},
	}
	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{"spec": spec}

	res, err := s.vizReport(t.Context(), req)
	require.NoError(t, err)
	require.True(t, res.IsError, "a failed section must fail the AUTHORING call")
	msg := res.Content[0].(mcp.TextContent).Text
	assert.Contains(t, msg, `section 1 ("Bad")`, "the error names the section")
	assert.Contains(t, msg, "nothing was rendered")

	// The same spec through the app channel keeps the document alive: the
	// failure stays inside its section.
	res, err = s.reportData(t.Context(), req)
	require.NoError(t, err)
	require.False(t, res.IsError)
	data, ok := res.StructuredContent.(reports.ReportData)
	require.True(t, ok)
	assert.Empty(t, data.Sections[0].Error)
	assert.NotEmpty(t, data.Sections[1].Error)
}

// The widget's own refresh loop, server side: the spec the tool RETURNS is
// what the panel echoes back into report-data when the user changes a parent
// filter. The polymorphic bind (a name AND a {from, to} pair) must survive
// that round trip, and options_only must re-resolve the dependent list
// against the NEW value without running a single section.
func TestReportDataEchoesTheSpecAndReResolvesOptions(t *testing.T) {
	s := New(depStub{}, server.NewMCPServer("t", "0", server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, true)), Config{})

	req := mcp.CallToolRequest{}
	req.Params.Arguments = map[string]any{
		"spec": map[string]any{
			"title": "T",
			"variables": []any{
				map[string]any{"name": "state", "type": "String"},
				map[string]any{"name": "city", "type": "String"},
				map[string]any{"name": "date_from", "type": "Date", "default": "2023-01-01"},
				map[string]any{"name": "date_to", "type": "Date", "default": "2023-12-31"},
			},
			"controls": []any{
				map[string]any{"label": "State", "control": "select", "bind": "state",
					"options_query": map[string]any{
						"query": "query { m { states_src { s } } }",
						"jq":    ".data.m.states_src | map(.s)"}},
				map[string]any{"label": "City", "control": "select", "bind": "city",
					"options_query": map[string]any{
						"query": "query($state: String) { m { cities(filter: {st: {eq: $state}}) { name } } }",
						"jq":    ".data.m.cities | map(.name)"}},
				map[string]any{"label": "Period", "control": "daterange",
					"bind": map[string]any{"from": "date_from", "to": "date_to"}},
			},
			"sections": []any{
				map[string]any{"kind": "table", "title": "Rows", "query": "{ m { obj(limit: 5) { a } } }"},
			},
		},
		"variables": map[string]any{"state": "CA"},
	}
	res, err := s.vizReport(t.Context(), req)
	require.NoError(t, err)
	require.False(t, res.IsError, "%v", res.Content)
	env := res.StructuredContent.(ReportEnvelope)
	require.Len(t, env.Data.Controls, 3)
	assert.Equal(t, []reports.Option{{Value: "LA"}}, env.Data.Controls[1].Options,
		"the dependent list resolves against the initial value")

	// Echo EXACTLY what the widget holds: the returned spec, through JSON.
	raw, err := json.Marshal(env.Spec)
	require.NoError(t, err)
	var echoed map[string]any
	require.NoError(t, json.Unmarshal(raw, &echoed))

	req.Params.Arguments = map[string]any{
		"spec":         echoed,
		"variables":    map[string]any{"state": "NY"},
		"options_only": true,
	}
	res, err = s.reportData(t.Context(), req)
	require.NoError(t, err)
	require.False(t, res.IsError, "the echoed spec must parse — the widget sends it verbatim: %v", res.Content)
	data := res.StructuredContent.(reports.ReportData)
	assert.Empty(t, data.Sections, "options_only must not run the sections")
	assert.Equal(t, []reports.Option{{Value: "NYC"}}, data.Controls[1].Options,
		"the dependent list follows the new parent value")
	assert.Equal(t, []reports.Option{{Value: "CA"}, {Value: "NY"}}, data.Controls[0].Options)
}

// depStub serves an options hierarchy: states, and cities that depend on the
// submitted $state.
type depStub struct{ types.Querier }

func (depStub) Query(_ context.Context, query string, vars map[string]any) (*types.Response, error) {
	switch {
	case strings.Contains(query, "states_src"):
		return &types.Response{Data: map[string]any{"m": map[string]any{"states_src": []any{
			map[string]any{"s": "CA"}, map[string]any{"s": "NY"},
		}}}}, nil
	case strings.Contains(query, "cities"):
		byState := map[any]string{"CA": "LA", "NY": "NYC"}
		var rows []any
		if name, ok := byState[vars["state"]]; ok {
			rows = []any{map[string]any{"name": name}}
		} else {
			rows = []any{map[string]any{"name": "LA"}, map[string]any{"name": "NYC"}}
		}
		return &types.Response{Data: map[string]any{"m": map[string]any{"cities": rows}}}, nil
	}
	return &types.Response{Data: map[string]any{"m": map[string]any{"obj": []any{
		map[string]any{"a": 1.0},
	}}}}, nil
}

// reportStub answers every query with two rows (queries mentioning "boom"
// fail); the embedded interface covers the Querier methods the handler never
// touches.
type reportStub struct{ types.Querier }

func (reportStub) Query(_ context.Context, query string, _ map[string]any) (*types.Response, error) {
	if strings.Contains(query, "boom") {
		return &types.Response{Errors: types.WarpGraphQLError(assert.AnError)}, nil
	}
	return &types.Response{Data: map[string]any{"m": map[string]any{"obj": []any{
		map[string]any{"a": 1.0},
		map[string]any{"a": 2.0},
	}}}}, nil
}
