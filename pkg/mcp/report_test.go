package mcp

import (
	"context"
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
