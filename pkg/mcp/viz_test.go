package mcp

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The MCP Apps contract lives half in Go and half in an HTML file the Go tests
// never execute, so what is worth pinning here is the wiring the host reads
// before any of our code runs: the _meta.ui keys, the mime type, and the fact
// that the page is self-contained. A host silently declines to open a view
// when any of those is wrong — there is no error to see in Claude Desktop.

func TestVizToolsDeclareTheAppsMetadata(t *testing.T) {
	s := New(nil, server.NewMCPServer("t", "0", server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, true)), Config{})
	tools := s.mcp.ListTools()

	for name, wantURI := range map[string]string{
		"viz-chart": vizResourceURI,
		"viz-table": vizTableResourceURI,
		"viz-kpi":   vizKpiResourceURI,
	} {
		tool, ok := tools[name]
		require.Truef(t, ok, "%s must be registered", name)
		ui := uiMeta(t, tool.Tool.Meta)
		assert.Equal(t, wantURI, ui["resourceUri"], "%s must link its own view build", name)
		assert.Equal(t, []any{"model", "app"}, jsonRoundTrip(t, ui["visibility"]),
			"%s must stay callable by both the model and the view", name)
		require.NotNil(t, tool.Tool.Annotations.ReadOnlyHint)
		assert.True(t, *tool.Tool.Annotations.ReadOnlyHint, "%s only reads", name)
	}

	// The refresh channel must be invisible to the model: it carries the
	// view's own query verbatim, which is not something to hand the model as
	// a second, weaker data-query tool.
	dataTool, ok := tools["viz-data"]
	require.True(t, ok, "viz-data must be registered")
	ui := uiMeta(t, dataTool.Tool.Meta)
	assert.Equal(t, []any{"app"}, jsonRoundTrip(t, ui["visibility"]))
	assert.NotContains(t, ui, "resourceUri", "a refresh call must feed the open view, not open a new one")
}

func TestVizPageIsSelfContained(t *testing.T) {
	page := vizPage()

	assert.Contains(t, page, "echarts", "the chart library must be inlined — the default CSP blocks every CDN")
	assert.NotContains(t, page, "<!--__ECHARTS__-->", "the placeholder must be substituted")
	assert.NotContains(t, page, "https://cdn.", "no external fetches may survive in the page")
	// One stray "</script>" inside the inlined library would close our tag
	// early and truncate the page into a blank view.
	assert.Equal(t, 2, strings.Count(page, "</script>"), "exactly the library tag and the app tag")
	assert.Contains(t, page, `"2026-01-26"`, "the handshake must name the Apps protocol revision")
	assert.Greater(t, len(page), 500_000, "sanity: the library really is in there")
}

// The table build exists so a grid does not cost a megabyte per render. It is
// the same source with the library left out, which only works because nothing
// outside renderChart touches echarts.
func TestVizTablePageOmitsTheChartLibrary(t *testing.T) {
	table, chart := vizTablePage(), vizPage()

	assert.NotContains(t, table, "<!--__ECHARTS__-->", "the placeholder must be substituted")
	assert.Less(t, len(table), 60_000, "the table build must not carry the library")
	assert.Less(t, len(table), len(chart)/10)
	assert.Equal(t, 1, strings.Count(table, "</script>"), "only the app script")
	assert.Contains(t, table, "renderTable", "it is still the same view")
	assert.NotContains(t, table, "Apache Software Foundation", "the library itself must be gone")
	// The call site survives — it is simply never reached, because renderChart
	// only runs for kind=chart. Pin that, or a future edit could reintroduce a
	// top-level reference and blank the table view.
	assert.Contains(t, table, "echarts.init(")
}

func TestVizResourceServesTheAppProfile(t *testing.T) {
	s := New(nil, server.NewMCPServer("t", "0", server.WithToolCapabilities(true),
		server.WithResourceCapabilities(false, true)), Config{})

	entry, ok := s.mcp.ListResources()[vizResourceURI]
	require.True(t, ok, "the view resource must be registered under %s", vizResourceURI)
	assert.Equal(t, vizMIMEType, entry.Resource.MIMEType)
	require.NotNil(t, entry.Resource.Size)
	assert.EqualValues(t, len(vizPage()), *entry.Resource.Size)

	// Declared in both allowed places: the listing entry (reviewable at
	// connection time) and the read content item (takes precedence).
	listMeta := uiMeta(t, entry.Resource.Meta)
	assert.Equal(t, true, listMeta["prefersBorder"])
	assert.NotContains(t, listMeta, "csp", "a self-contained view declares no domains")
	// A sandboxed iframe cannot download anything by itself, so the clipboard
	// is the export route wherever the host has no ui/download-file. The
	// permission has to be declared here or the copy silently fails.
	perms, ok := listMeta["permissions"].(map[string]any)
	require.True(t, ok, "the view must declare its permissions")
	assert.Contains(t, perms, "clipboardWrite")

	contents, err := entry.Handler(t.Context(), mcp.ReadResourceRequest{
		Params: mcp.ReadResourceParams{URI: vizResourceURI},
	})
	require.NoError(t, err)
	require.Len(t, contents, 1)
	text, ok := contents[0].(mcp.TextResourceContents)
	require.True(t, ok)
	assert.Equal(t, vizMIMEType, text.MIMEType)
	assert.Equal(t, vizPage(), text.Text)
	require.Contains(t, text.Meta, "ui")
}

func TestVizCanonicalRowsUnwrapsAndExplains(t *testing.T) {
	// The usual GraphQL shape needs no jq at all.
	rows, err := vizCanonicalRows(mustJSON(t, `{"sales":{"orders":[{"month":"2026-01","total":10}]}}`))
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.Equal(t, "2026-01", rows[0]["month"])

	// An object with two keys is NOT a wrapper — unwrapping it would guess.
	_, err = vizCanonicalRows(mustJSON(t, `{"a":[],"b":[]}`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "got object")

	// Errors must name the offending field: the model's next call depends on
	// knowing which value to flatten.
	_, err = vizCanonicalRows(mustJSON(t, `[{"month":"2026-01","customer":{"name":"acme"}}]`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `field "customer"`)
	assert.Contains(t, err.Error(), "nested object")

	_, err = vizCanonicalRows(mustJSON(t, `[1,2,3]`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "row 0 is a number")
}

func TestVizFallbackTextIsReadableWithoutTheApp(t *testing.T) {
	env := VizEnvelope{
		Kind:  "chart",
		Title: "Revenue by month",
		Chart: &VizChartSpec{Type: "line", X: "month", Y: []string{"revenue"}},
		Rows: []map[string]any{
			{"revenue": 100.0, "month": "2026-01", "note": "a|b"},
			{"revenue": 120.0, "month": "2026-02", "note": "c"},
		},
		RowCount: 2,
	}
	text := vizFallbackText(&env)

	assert.Contains(t, text, "## Revenue by month")
	assert.Contains(t, text, "line chart of revenue over month")
	// Chart bindings lead the columns even though "note" sorts first.
	assert.Contains(t, text, "| month | revenue | note |")
	assert.Contains(t, text, `a\|b`, "a pipe in a value must not break the table")
	assert.NotContains(t, text, "{", "the fallback is a summary, not a JSON dump")
}

func TestVizFallbackTextCapsRows(t *testing.T) {
	env := VizEnvelope{Kind: "table", Title: "wide", RowCount: 50}
	for i := range 50 {
		env.Rows = append(env.Rows, map[string]any{"i": i})
	}
	text := vizFallbackText(&env)
	assert.Contains(t, text, "… 30 more rows in the structured result.")
	assert.NotContains(t, text, "| 21 |")
}

// Volume is stated by the caller, never papered over here: hugr tables run to
// hundreds of millions of rows, and a silently truncated chart is a wrong
// chart. The guard fires only for a query that set no bound of its own.
func TestVizQueryHasLimit(t *testing.T) {
	for query, want := range map[string]bool{
		`query { m { obj { a } } }`:                             false,
		`query($limit: Int) { m { obj(limit: $limit) { a } } }`: true,
		`query { m { obj(limit: 100) { a } } }`:                 true,
		`query { m { obj_bucket_aggregation { key { a } } } }`:  false,
	} {
		assert.Equalf(t, want, vizQueryHasLimit(query), "limit detection for %s", query)
	}
}

func TestVizQueryLimit(t *testing.T) {
	cases := []struct {
		query string
		vars  map[string]any
		want  int
		known bool
	}{
		{`{ m { obj(limit: 500) { a } } }`, nil, 500, true},
		{`{ m { obj(limit:2000, offset: 0) { a } } }`, nil, 2000, true},
		{`query($limit: Int) { m { obj(limit: $limit) { a } } }`, map[string]any{"limit": float64(300)}, 300, true},
		{`{ m { obj { a } } }`, nil, 0, false},
		// The innermost bound is the one that shapes the rows we receive.
		{`{ m { obj(limit: 100) { rel(limit: 5) { a } } } }`, nil, 5, true},
	}
	for _, c := range cases {
		got, known := vizQueryLimit(c.query, c.vars)
		assert.Equalf(t, c.known, known, "known for %s", c.query)
		if c.known {
			assert.Equalf(t, c.want, got, "limit for %s", c.query)
		}
	}
}

func TestVizFallbackTextFlagsAFullPage(t *testing.T) {
	env := VizEnvelope{Kind: "table", Title: "page", RowCount: 2, AtLimit: true,
		Rows: []map[string]any{{"a": 1}, {"a": 2}}}
	text := vizFallbackText(&env)
	assert.Contains(t, text, "one page, not the whole answer")
	assert.Contains(t, text, "offset", "the note must carry the way out, not just the diagnosis")
}

// The rows must reach the view without passing through the model's context:
// 2000 rows of structuredContent is tens of thousands of tokens the model
// would never read line by line.
func TestVizTableResultSamplesAndDelegatesTheFetch(t *testing.T) {
	env := VizEnvelope{Kind: "table", Title: "big", RowCount: 100,
		Source: &VizSource{Type: "query", Query: "{ x }"}}
	for i := range 100 {
		env.Rows = append(env.Rows, map[string]any{"i": i})
	}

	out := vizResult(&env).StructuredContent.(VizEnvelope)

	assert.Len(t, out.Rows, vizModelRows, "the conversation gets a sample, not the table")
	assert.True(t, out.RowsSampled, "and is told that it is one")
	assert.Equal(t, 100, out.RowCount, "while still learning the real length")
	assert.True(t, out.Fetch, "the view loads the rows itself")
	assert.Len(t, env.Rows, 100, "the caller's envelope is left intact")
	assert.Equal(t, vizFallbackText(&env), vizResult(&env).Content[0].(mcp.TextContent).Text,
		"the text fallback still summarises the whole result")
}

// An aggregation that fits in the sample travels inline: a round trip to move
// forty rows would buy nothing, and the numbers are worth the model seeing.
func TestVizSmallResultTravelsInline(t *testing.T) {
	env := VizEnvelope{Kind: "chart", Title: "agg", RowCount: 40,
		Chart:  &VizChartSpec{Type: "bar", X: "m", Y: []string{"n"}},
		Source: &VizSource{Type: "query", Query: "{ x }"}}
	for i := range 40 {
		env.Rows = append(env.Rows, map[string]any{"m": i, "n": i})
	}

	out := vizResult(&env).StructuredContent.(VizEnvelope)
	assert.Len(t, out.Rows, 40)
	assert.False(t, out.Fetch)
	assert.False(t, out.RowsSampled)
}

// A big chart is sampled like anything else — the difference is only WHEN the
// view fetches, which is the view's business, not the result's.
func TestVizLargeChartIsSampledToo(t *testing.T) {
	env := VizEnvelope{Kind: "chart", Title: "wide", RowCount: 300,
		Chart:  &VizChartSpec{Type: "line", X: "m", Y: []string{"n"}},
		Source: &VizSource{Type: "query", Query: "{ x }"}}
	for i := range 300 {
		env.Rows = append(env.Rows, map[string]any{"m": i, "n": i})
	}

	out := vizResult(&env).StructuredContent.(VizEnvelope)
	assert.Len(t, out.Rows, vizModelRows)
	assert.True(t, out.RowsSampled)
	assert.True(t, out.Fetch, "a chart cannot be drawn from a sample")
}

// Inline data has no query to re-run, so there is nothing to delegate.
func TestVizInlineTableKeepsItsRows(t *testing.T) {
	env := VizEnvelope{Kind: "table", Title: "inline", RowCount: 2,
		Source: &VizSource{Type: "inline"},
		Rows:   []map[string]any{{"i": 1}, {"i": 2}}}
	out := vizResult(&env).StructuredContent.(VizEnvelope)
	assert.Len(t, out.Rows, 2)
	assert.False(t, out.Fetch)
}

func uiMeta(t *testing.T, meta any) map[string]any {
	t.Helper()
	var raw map[string]any
	switch m := meta.(type) {
	case *mcp.Meta:
		require.NotNil(t, m, "tool must carry _meta")
		raw = m.AdditionalFields
	case map[string]any:
		raw = m
	default:
		t.Fatalf("unexpected meta type %T", meta)
	}
	ui, ok := raw["ui"].(map[string]any)
	require.True(t, ok, "_meta.ui must be an object, got %#v", raw["ui"])
	return ui
}

// jsonRoundTrip normalizes []string vs []any so the assertion reads the value
// the host will actually see on the wire.
func jsonRoundTrip(t *testing.T, v any) any {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	var out any
	require.NoError(t, json.Unmarshal(b, &out))
	return out
}

func mustJSON(t *testing.T, s string) any {
	t.Helper()
	var v any
	require.NoError(t, json.Unmarshal([]byte(s), &v))
	return v
}

// The KPI-card contract is validated with the same discipline as canonical
// rows: precise errors naming the card and the field, because a vague error
// costs the caller a whole round trip.
func TestVizCanonicalKpis(t *testing.T) {
	full := mustJSON(t, `[{"label":"Revenue","value":12.5,"unit":"$","format":"number",
		"delta":1.2,"delta_pct":3.4,"direction":"up_good","trend":[1,2,3],"subtitle":"vs July"}]`)
	kpis, err := vizCanonicalKpis(full)
	require.NoError(t, err)
	require.Len(t, kpis, 1)
	assert.Equal(t, "Revenue", kpis[0].Label)
	assert.Equal(t, 12.5, kpis[0].Value)
	assert.Equal(t, []float64{1, 2, 3}, kpis[0].Trend)
	assert.Equal(t, 3.4, *kpis[0].DeltaPct)

	// The usual single-key GraphQL wrapping unwraps, same as rows.
	wrapped := mustJSON(t, `{"data":{"m":[{"label":"a","value":1}]}}`)
	kpis, err = vizCanonicalKpis(wrapped)
	require.NoError(t, err)
	assert.Len(t, kpis, 1)

	for name, tc := range map[string]struct{ in, wantErr string }{
		"not an array":  {`{"a":1,"b":2}`, "expected an array of KPI cards"},
		"empty":         {`[]`, "at least one"},
		"no label":      {`[{"value":1}]`, "label is required"},
		"no value":      {`[{"label":"x"}]`, "value is required"},
		"nested value":  {`[{"label":"x","value":{"a":1}}]`, "nested object"},
		"typoed field":  {`[{"label":"x","value":1,"delta_percent":5}]`, `unknown field "delta_percent"`},
		"bad direction": {`[{"label":"x","value":1,"direction":"up"}]`, "up_good|down_good|neutral"},
		"bad trend":     {`[{"label":"x","value":1,"trend":[1,"a"]}]`, "trend[1]"},
		"bad delta":     {`[{"label":"x","value":1,"delta":"big"}]`, "delta must be a number"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := vizCanonicalKpis(mustJSON(t, tc.in))
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}

	// The cap: a panel is a glance surface.
	big := make([]any, vizKpiCap+1)
	for i := range big {
		big[i] = map[string]any{"label": "x", "value": float64(i)}
	}
	_, err = vizCanonicalKpis(any(big))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "at 32 at most")
}

// A KPI panel is always complete: no sampling, no fetch — even for a query
// source, because the cap keeps it small by construction.
func TestVizKpiResultTravelsInline(t *testing.T) {
	env := &VizEnvelope{
		Kind:  "kpi",
		Title: "Health",
		Rows:  []map[string]any{},
		Kpis: []VizKPI{
			{Label: "Revenue", Value: 100.0, Unit: "$", DeltaPct: ptr(3.25)},
			{Label: "Rows", Value: 15635261.0},
			{Label: "Status", Value: "OK", Subtitle: "all groups"},
		},
		Source: &VizSource{Type: "query", Query: "query{...}"},
	}
	res := vizResult(env)
	require.NotNil(t, res)
	out, ok := res.StructuredContent.(VizEnvelope)
	require.True(t, ok)
	assert.False(t, out.RowsSampled)
	assert.False(t, out.Fetch)
	assert.Len(t, out.Kpis, 3)

	text := vizFallbackText(env)
	assert.Contains(t, text, "## Health")
	assert.Contains(t, text, "| metric | value | change |")
	assert.Contains(t, text, "| Revenue | 100 $ | +3.25% |")
	assert.Contains(t, text, "| Rows | 15635261 |", "no scientific notation in a human-facing table")
	assert.Contains(t, text, "| Status | OK |  | all groups |")
	assert.NotContains(t, text, "0 rows", "a panel does not talk about rows")
}

func ptr[T any](v T) *T { return &v }
