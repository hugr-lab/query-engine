package mcp

// MCP Apps visualization tools (design/038).
//
// viz-chart, viz-table and viz-kpi are model-facing tools whose results a
// host with the MCP Apps extension renders in the ui://hugr/viz-* iframes;
// hosts without it fall back to the markdown/text content. viz-data is the
// app-only fetch channel: the iframe re-runs the view's own query, unchanged,
// to pull the rows that were sampled out of the conversation. All filtering
// in the view is client-side. The view HTML is self-contained (ECharts
// inlined at embed time) because the MCP Apps CSP denies all external fetches
// by default.

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/hugr-lab/query-engine/pkg/mcp/ui"
	"github.com/hugr-lab/query-engine/pkg/mcp/viz"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

const (
	// vizViewVersion is part of the resource URIs to bust host-side template
	// caches: ChatGPT caches the widget HTML by URI and keeps serving a stale
	// build after the server changes. Bump it whenever the shipped view must
	// replace one a host may have cached.
	vizViewVersion      = "v3"
	vizResourceURI      = "ui://hugr/viz-" + vizViewVersion
	vizTableResourceURI = "ui://hugr/viz-table-" + vizViewVersion
	vizKpiResourceURI   = "ui://hugr/viz-kpi-" + vizViewVersion

	vizMIMEType = "text/html;profile=mcp-app"

	vizFallbackRows = 20

	// A table's rows are for the view, not for the conversation: two thousand
	// of them is tens of thousands of tokens the model would never read line
	// by line. So the table result carries a sample and the view fetches the
	// real set itself through viz-data. A chart's rows are an aggregation —
	// small, and worth the model actually seeing — so they ride along inline.
	// Fifty covers the inline table (which shows about fifteen) with room for
	// sorting and a filter, and still costs the conversation almost nothing.
	vizModelRows = 50
)

var vizControls = []string{"select", "multiselect", "search", "number", "numrange", "date", "daterange", "toggle"}

// The view is served in two builds from one source. The chart build inlines
// the library — the app iframe cannot reach any CDN under the default MCP Apps
// CSP — and costs about a megabyte. The table build drops it: a table has
// nothing to plot, and shipping a charting library to draw a grid would be a
// megabyte per render for nothing. Both carry the shared core.
var vizPage = sync.OnceValue(func() string {
	return vizAssemble("<script>" + ui.EChartsJS + "</script>")
})

var vizTablePage = sync.OnceValue(func() string {
	return vizAssemble("")
})

func vizAssemble(echarts string) string {
	page := strings.Replace(ui.VizHTML, "<!--__ECHARTS__-->", echarts, 1)
	return strings.Replace(page, "<!--__VIZCORE__-->", "<script>"+ui.CoreJS+"</script>", 1)
}

// --- wire format ---

// The specs and the KPI card are shared with the report renderer and live in
// pkg/mcp/viz; the aliases keep this package's wire surface (and its output
// schemas) exactly where it was.
type (
	VizChartSpec  = viz.ChartSpec
	VizColumnSpec = viz.ColumnSpec
	VizKPI        = viz.KPI
)

type VizFilterSpec struct {
	Name    string `json:"name" jsonschema_description:"Identifier of the control, unique within the view"`
	Label   string `json:"label,omitempty"`
	Control string `json:"control,omitempty" jsonschema_description:"select | multiselect | search | number | numrange | date | daterange | toggle. Omit and it is inferred from the values actually present in the rows"`
	Field   string `json:"field,omitempty" jsonschema_description:"Row field the control matches against (default: name)"`
	Default any    `json:"default,omitempty" jsonschema_description:"Initial value when the user has not touched the control"`
	Value   any    `json:"value,omitempty" jsonschema_description:"Pre-applied value set from the conversation; applied to the first render too"`
}

type VizSource struct {
	Type        string         `json:"type" jsonschema_description:"query | inline"`
	Query       string         `json:"query,omitempty"`
	Variables   map[string]any `json:"variables,omitempty" jsonschema_description:"Original variables, passed back unchanged when the view fetches its rows"`
	JQTransform string         `json:"jq_transform,omitempty"`
}

// VizEnvelope is the structured result both the model and the app view read.
type VizEnvelope struct {
	Kind      string           `json:"kind" jsonschema_description:"chart | table | kpi"`
	Title     string           `json:"title"`
	Chart     *VizChartSpec    `json:"chart,omitempty"`
	Columns   []VizColumnSpec  `json:"columns,omitempty"`
	Kpis      []VizKPI         `json:"kpis,omitempty" jsonschema_description:"The KPI cards (kind=kpi); always complete — a panel is never sampled"`
	Filters   []VizFilterSpec  `json:"filters,omitempty" jsonschema_description:"Controls over the delivered rows; the view derives their options from those rows"`
	Source    *VizSource       `json:"source,omitempty"`
	Rows      []map[string]any `json:"rows" jsonschema_description:"Canonical flat rows the view renders"`
	RowCount  int              `json:"row_count"`
	Truncated bool             `json:"truncated" jsonschema_description:"true when the hard cap cut the result"`
	AtLimit   bool             `json:"at_limit,omitempty" jsonschema_description:"The result is exactly as long as the query's own limit, so rows beyond it were almost certainly left behind"`
	// Both only ever set on the copy the caller receives.
	RowsSampled bool `json:"rows_sampled,omitempty" jsonschema_description:"rows here is a sample of the first few; row_count is the real length"`
	Fetch       bool `json:"fetch,omitempty" jsonschema_description:"The rendered view loads the full set itself through viz-data — a chart at once, since it cannot be drawn from a sample; a table when the user opens it fullscreen"`
}

type VizDataResult struct {
	Rows      []map[string]any `json:"rows"`
	RowCount  int              `json:"row_count"`
	Truncated bool             `json:"truncated"`
}

type vizToolArgs struct {
	Title       string          `json:"title"`
	Chart       *VizChartSpec   `json:"chart"`
	Columns     []VizColumnSpec `json:"columns"`
	Query       string          `json:"query"`
	Variables   map[string]any  `json:"variables"`
	Data        []any           `json:"data"`
	JQTransform string          `json:"jq_transform"`
	Filters     []VizFilterSpec `json:"filters"`
}

// --- registration ---

func (s *Server) addViz(mcpServer *server.MCPServer) {
	// The view template. Hosts without MCP Apps never read it.
	//
	// _meta.ui is declared in BOTH places the spec allows: on the listing
	// entry, so a host can review the security posture at connection time
	// without fetching a megabyte of HTML, and on the read content item,
	// which takes precedence and is the location the stable revision defines.
	vizMeta := func() map[string]any {
		return map[string]any{"ui": map[string]any{
			"prefersBorder": true,
			// No csp domains: the view is self-contained and reaches hugr only
			// through the MCP bridge.
			//
			// clipboardWrite is the fallback export route. A sandboxed iframe
			// has no allow-downloads, so saving a real file is only possible
			// where the host implements ui/download-file; copying to the
			// clipboard is what remains everywhere else.
			"permissions": map[string]any{"clipboardWrite": map[string]any{}},
		}}
	}
	addView := func(uri, name, desc string, page func() string) {
		size := int64(len(page()))
		res := mcp.NewResource(uri, name,
			mcp.WithResourceDescription(desc),
			mcp.WithMIMEType(vizMIMEType),
		)
		res.Meta = mcp.NewMetaFromMap(vizMeta())
		res.Size = &size
		mcpServer.AddResource(res, func(_ context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
			slog.Info("MCP app view read", "uri", req.Params.URI, "bytes", size)
			return []mcp.ResourceContents{
				mcp.TextResourceContents{
					URI:      req.Params.URI,
					MIMEType: vizMIMEType,
					Text:     page(),
					Meta:     vizMeta(),
				},
			}, nil
		})
	}
	addView(vizResourceURI, "Hugr Viz", "Interactive chart view for viz-chart (MCP Apps)", vizPage)
	addView(vizTableResourceURI, "Hugr Viz Table", "Interactive table view for viz-table (MCP Apps)", vizTablePage)
	// The KPI panel needs no charting library — sparklines are hand-drawn
	// SVG — so it serves the same lean build as the table.
	addView(vizKpiResourceURI, "Hugr Viz KPI", "KPI panel view for viz-kpi (MCP Apps)", vizTablePage)

	chartTool := mcp.NewTool("viz-chart",
		mcp.WithDescription(`Render an interactive CHART over hugr data or caller-supplied rows. Hosts with MCP Apps open it as a live view (filters, inline/fullscreen, refresh); other hosts still get the rows as structured JSON plus a markdown preview, so calling it is always safe.
Data: pass 'query' (hugr GraphQL, read-only) OR 'data' (inline array of flat row objects) — exactly one. jq_transform shapes the query result into canonical ROWS: a flat array of objects with scalar values, e.g. [{"month":"2026-01","revenue":100}]. Its input is the FULL response envelope, so paths start with .data, same as data-inline_graphql_result: .data.sales.orders | map({month: .month, revenue: .total}). Without jq a single-path result is unwrapped for you, so a plain "one table, scalar columns" query needs no transform at all; nested values are rejected with a precise error naming the field.
Chart mapping: 'x' names the category field. WIDE rows: list value fields in 'y' — one series each. LONG rows: 'series' names the field whose distinct values become series, y[0] holds the value. stacked stacks series; pie uses x as label and y[0] as value; scatter plots x/y[0] as numbers.
Filters are controls over the rows ALREADY delivered — they never re-run the query. Narrowing what is fetched is YOUR job: put the condition in the query's own filter argument and call this tool again (a new view appears; the old one keeps its data). So filter server-side for what the user asked to see, and add controls only for slicing that result.
A control is just a row 'field' and an optional label: its option list, the counts under each option and even the control TYPE are derived from the rows themselves, so what the dropdown offers always matches what the table holds. Do not restate the query's conditions here — that filtering already happened.
CHANGING AN EXISTING VIEW IS NOT POSSIBLE — a view is bound to the call that produced it, and nothing can push into it afterwards. When the user asks for a different cut, call the tool AGAIN: a fresh view renders below and the previous one keeps its data. Set 'value' on a control to have it already applied on that first render, and change the query itself when the data needed is different. The result echoes every control back with its options resolved, so after one call you know the legal values; the open view also reports its current filter state back to you as the user changes it, so "show me that same view but only for X" needs no re-discovery.
DATA VOLUME COSTS YOU NOTHING — above 50 rows the result carries a SAMPLE of the first 50 (rows_sampled=true) with the real row_count, and the rendered chart pulls every point itself through a side channel: the full set never passes through the conversation, so there is no row limit to plan around and no reason to split one chart into several. A chart usually reads best built from an aggregation shaped to the question, with limit as a deliberate top-N for readability — that is chart craft, your call. Use viz-table when there is nothing to plot.`),
		mcp.WithString("title", mcp.Required(), mcp.Description("View title, shown in the header and the text fallback")),
		mcp.WithObject("chart", mcp.Required(), mcp.Description("Chart mapping: type + field bindings"),
			mcp.Properties(map[string]any{
				"type":    map[string]any{"type": "string", "enum": viz.ChartTypes, "description": "Chart type"},
				"x":       map[string]any{"type": "string", "description": "Row field for the category/x value"},
				"y":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}, "description": "Value fields (wide form: one series each; long form: y[0])"},
				"series":  map[string]any{"type": "string", "description": "Field whose distinct values become series (long form)"},
				"stacked": map[string]any{"type": "boolean", "description": "Stack the series"},
			})),
		mcp.WithString("query", mcp.Description("hugr GraphQL query (read-only). Mutually exclusive with data")),
		mcp.WithObject("variables", mcp.Description("Query variables")),
		mcp.WithArray("data", mcp.Description("Inline rows: flat objects with scalar values. Mutually exclusive with query"),
			mcp.Items(map[string]any{"type": "object"})),
		mcp.WithString("jq_transform", mcp.Description("jq shaping the response into canonical rows; input is the full envelope, so paths start with .data")),
		mcp.WithArray("filters", mcp.Description("Interactive filter specs"), mcp.Items(vizFilterSchema())),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithOutputSchema[VizEnvelope](),
	)
	chartTool.Meta = mcp.NewMetaFromMap(map[string]any{"ui": map[string]any{
		"resourceUri": vizResourceURI,
		"visibility":  []string{"model", "app"},
	}})
	mcpServer.AddTool(chartTool, s.vizChart)

	tableTool := mcp.NewTool("viz-table",
		mcp.WithDescription(`Render an interactive TABLE over hugr data or caller-supplied rows — same contract as viz-chart minus the chart mapping. Hosts with MCP Apps open a sortable table with filter controls; other hosts get the rows as structured JSON plus a markdown preview.
Data: 'query' XOR 'data'; jq_transform shapes the result into a flat array of scalar-valued row objects, and its input is the FULL response envelope, so paths start with .data (a plain single-table query needs no transform — it is unwrapped for you). columns picks and orders the visible columns (default: all).
Filters are controls over the rows ALREADY delivered — they never re-run the query. Narrowing what is fetched is YOUR job: put the condition in the query's own filter argument and call this tool again. A control is just a row 'field' and an optional label: its options, their counts and even the control TYPE come from the rows themselves, so the dropdown always matches what the table holds. Do not restate the query's conditions here.
CHANGING AN EXISTING VIEW IS NOT POSSIBLE — a view is bound to the call that produced it. For a different cut, call the tool AGAIN: a fresh view renders below and the previous one keeps its data. Set 'value' on a control to have it already applied on that first render; the result echoes every control back with its options resolved, and the open view reports its current filter state back to you as the user changes it.
VOLUME — hugr tables run to hundreds of millions of rows, so bound the query yourself. An unbounded query returning more than 2000 rows FAILS with instructions; up to 2000 render fine and fullscreen shows them all. Nothing is truncated behind your back. Any limit you set is honoured. When the page comes back exactly as long as its own limit, at_limit=true says there is probably a tail behind it — tell the user this is one page rather than the whole answer, and offer the next one by calling again with a larger offset.
WHAT YOU GET BACK — above 50 rows, rows here is a SAMPLE of the first 50 (rows_sampled=true) while row_count is the real length: the rendered table loads the rest itself when the user opens it fullscreen, so the whole set never passes through the conversation. Reason from row_count and the sample, and say "the view has all N" rather than claiming you only saw 50.`),
		mcp.WithString("title", mcp.Required(), mcp.Description("View title, shown in the header and the text fallback")),
		mcp.WithString("query", mcp.Description("hugr GraphQL query (read-only). Mutually exclusive with data")),
		mcp.WithObject("variables", mcp.Description("Query variables")),
		mcp.WithArray("data", mcp.Description("Inline rows: flat objects with scalar values. Mutually exclusive with query"),
			mcp.Items(map[string]any{"type": "object"})),
		mcp.WithString("jq_transform", mcp.Description("jq shaping the response into canonical rows; input is the full envelope, so paths start with .data")),
		mcp.WithArray("columns", mcp.Description("Visible columns, in order (default: all row fields)"),
			mcp.Items(map[string]any{"type": "object", "properties": map[string]any{
				"field":  map[string]any{"type": "string"},
				"label":  map[string]any{"type": "string"},
				"format": map[string]any{"type": "string", "enum": []string{"number"}},
				"align":  map[string]any{"type": "string", "enum": []string{"left", "right"}},
			}, "required": []string{"field"}})),
		mcp.WithArray("filters", mcp.Description("Interactive filter specs"), mcp.Items(vizFilterSchema())),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithOutputSchema[VizEnvelope](),
	)
	tableTool.Meta = mcp.NewMetaFromMap(map[string]any{"ui": map[string]any{
		"resourceUri": vizTableResourceURI,
		"visibility":  []string{"model", "app"},
	}})
	mcpServer.AddTool(tableTool, s.vizTable)

	kpiTool := mcp.NewTool("viz-kpi",
		mcp.WithDescription(`Render a KPI PANEL — a row of metric cards (headline value, unit, delta vs a comparison period, sparkline trend) over hugr data or numbers you computed yourself. Hosts with MCP Apps show the cards; other hosts get a markdown summary, so calling it is always safe.
ONE hugr request computes every card in parallel: alias several _aggregation / _bucket_aggregation queries inside a single 'query', then assemble the cards with jq_transform — its input is the FULL response envelope, so paths start with .data. jq is where the panel takes shape: compute deltas ((current / previous - 1) * 100), collect a trend array from bucket rows, and append literal cards for values of your own — each key in 'variables' is visible to jq as its own variable (variables: {"sla": 99.9} → $sla), so one panel can mix queried aggregates with numbers you computed yourself. Example:
  .data.shop as $d | [
    {label: "Revenue", value: $d.total.amount.sum, unit: "$", delta_pct: (($d.total.amount.sum / $d.prev.amount.sum - 1) * 100), direction: "up_good"},
    {label: "Orders", value: $d.total._rows_count, trend: [$d.by_month[].aggregations._rows_count]}
  ]
CANONICAL CARDS — the result of jq (or inline 'data') must be [{label, value, unit?, format?, delta?, delta_pct?, direction?, trend?, subtitle?}]: label+value required, trend the only array (numbers, drawn as a sparkline), direction up_good|down_good|neutral colours the delta. Anything else is rejected with a precise error.
Alternatively pass 'data' with ready-made cards and no query at all — when the numbers came from your own reasoning, not a hugr query.
The whole panel returns in structuredContent (at most 32 cards): nothing is sampled, the view never re-fetches, and you see every card you rendered. No filters — a KPI panel is a glance surface; use viz-chart to plot and viz-table to browse.`),
		mcp.WithString("title", mcp.Required(), mcp.Description("Panel title, shown in the header and the text fallback")),
		mcp.WithString("query", mcp.Description("hugr GraphQL query (read-only), aliasing every aggregation the cards need. Mutually exclusive with data")),
		mcp.WithObject("variables", mcp.Description("Query variables; each key is also a jq variable ({\"sla\": 99.9} → $sla) — the way to blend your own numbers into cards")),
		mcp.WithArray("data", mcp.Description("Ready-made KPI cards. Mutually exclusive with query"),
			mcp.Items(map[string]any{"type": "object", "required": []string{"label", "value"}, "properties": map[string]any{
				"label":     map[string]any{"type": "string"},
				"value":     map[string]any{"description": "Number or short string"},
				"unit":      map[string]any{"type": "string"},
				"format":    map[string]any{"type": "string", "enum": viz.KPIFormats},
				"delta":     map[string]any{"type": "number"},
				"delta_pct": map[string]any{"type": "number"},
				"direction": map[string]any{"type": "string", "enum": viz.KPIDirections},
				"trend":     map[string]any{"type": "array", "items": map[string]any{"type": "number"}},
				"subtitle":  map[string]any{"type": "string"},
			}})),
		mcp.WithString("jq_transform", mcp.Description("jq assembling the cards; input is the full envelope, so paths start with .data. Effectively required with query — an aggregation envelope does not unwrap into cards by itself")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithOutputSchema[VizEnvelope](),
	)
	kpiTool.Meta = mcp.NewMetaFromMap(map[string]any{"ui": map[string]any{
		"resourceUri": vizKpiResourceURI,
		"visibility":  []string{"model", "app"},
	}})
	mcpServer.AddTool(kpiTool, s.vizKpi)

	// The refresh channel: callable from the iframe only. No resourceUri — a
	// call must feed the already-rendered view, not open a new one.
	dataTool := mcp.NewTool("viz-data",
		mcp.WithDescription("App-only refresh channel for the viz views: re-runs the view's own query unchanged and returns fresh canonical rows. Not callable by the model."),
		mcp.WithString("query", mcp.Required(), mcp.Description("The view's source query")),
		mcp.WithObject("variables", mcp.Description("The view's variables, unchanged")),
		mcp.WithString("jq_transform", mcp.Description("The view's jq transform")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		mcp.WithOutputSchema[VizDataResult](),
	)
	dataTool.Meta = mcp.NewMetaFromMap(map[string]any{"ui": map[string]any{
		"visibility": []string{"app"},
	}})
	mcpServer.AddTool(dataTool, s.vizData)
}

func vizFilterSchema() map[string]any {
	return map[string]any{
		"type":     "object",
		"required": []string{"name"},
		"properties": map[string]any{
			"name":    map[string]any{"type": "string", "description": "Identifier of the control, unique within the view"},
			"label":   map[string]any{"type": "string"},
			"control": map[string]any{"type": "string", "enum": vizControls, "description": "Omit and it is inferred from the values present in the rows"},
			"field":   map[string]any{"type": "string", "description": "Row field the control matches (default: name)"},
			"default": map[string]any{"description": "Initial value"},
			"value":   map[string]any{"description": "Pre-applied value from the conversation; applied on the first render too"},
		},
	}
}

// --- handlers ---

func (s *Server) vizChart(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args vizToolArgs
	if err := req.BindArguments(&args); err != nil {
		return toolResultError(fmt.Sprintf("bad arguments: %v", err)), nil
	}
	if args.Chart == nil {
		return toolResultError("chart is required"), nil
	}
	if !slices.Contains(viz.ChartTypes, args.Chart.Type) {
		return toolResultError(fmt.Sprintf("chart.type %q is not one of %s", args.Chart.Type, strings.Join(viz.ChartTypes, "|"))), nil
	}
	if args.Chart.X == "" {
		return toolResultError("chart.x is required"), nil
	}
	if len(args.Chart.Y) == 0 {
		return toolResultError("chart.y needs at least one value field (long form uses y[0])"), nil
	}
	return s.vizEnvelope(ctx, "chart", &args)
}

func (s *Server) vizTable(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args vizToolArgs
	if err := req.BindArguments(&args); err != nil {
		return toolResultError(fmt.Sprintf("bad arguments: %v", err)), nil
	}
	args.Chart = nil
	return s.vizEnvelope(ctx, "table", &args)
}

// vizKpi skips the shared rows pipeline on purpose: a KPI panel has no rows,
// no filters, no caps race and no sampling — its whole contract is "one small
// complete panel, inline".
func (s *Server) vizKpi(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args vizToolArgs
	if err := req.BindArguments(&args); err != nil {
		return toolResultError(fmt.Sprintf("bad arguments: %v", err)), nil
	}
	if args.Title == "" {
		return toolResultError("title is required"), nil
	}
	if (args.Query == "") == (len(args.Data) == 0) {
		return toolResultError("pass exactly one of query or data"), nil
	}

	var (
		v   any
		err error
	)
	if args.Query != "" {
		v, err = viz.QueryValue(ctx, s.querier, args.Query, args.Variables, args.JQTransform, s.cfg.QueryTTL)
	} else {
		items := make([]any, len(args.Data))
		copy(items, args.Data)
		v = any(items)
	}
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	kpis, err := viz.CanonicalKPIs(v)
	if err != nil {
		if args.Query != "" && args.JQTransform == "" {
			err = fmt.Errorf("%w — with a query, jq_transform is required to assemble the cards (paths start with .data, e.g. .data.%s)", err, viz.JQPathHint(args.Query))
		}
		return toolResultError(err.Error()), nil
	}

	env := VizEnvelope{
		Kind:  "kpi",
		Title: args.Title,
		Kpis:  kpis,
		Rows:  []map[string]any{},
	}
	if args.Query != "" {
		env.Source = &VizSource{Type: "query", Query: args.Query, Variables: args.Variables, JQTransform: args.JQTransform}
	} else {
		env.Source = &VizSource{Type: "inline"}
	}
	return vizResult(&env), nil
}

func (s *Server) vizData(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	var args vizToolArgs
	if err := req.BindArguments(&args); err != nil {
		return toolResultError(fmt.Sprintf("bad arguments: %v", err)), nil
	}
	if args.Query == "" {
		return toolResultError("query is required"), nil
	}
	// Uncapped: this feeds an already-rendered view, and the query is either a
	// chart's (no caps by design) or a table's (bounded by its own limit — an
	// unbounded one never produced a view in the first place).
	rows, truncated, err := viz.QueryRows(ctx, s.querier, args.Query, args.Variables, args.JQTransform, 0, s.cfg.QueryTTL)
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	// The answer goes straight to the view that asked for it, so the rows are
	// simply the payload; nothing here is ever put in front of the model.
	res := VizDataResult{Rows: rows, RowCount: len(rows), Truncated: truncated}
	return mcp.NewToolResultStructured(res, fmt.Sprintf("%d rows", len(rows))), nil
}

// vizEnvelope runs the shared pipeline: validate the source, apply model-set
// filter values, fetch and canonicalize rows, resolve filter options, and wrap
// everything into the envelope the view (and the fallback text) renders.
func (s *Server) vizEnvelope(ctx context.Context, kind string, args *vizToolArgs) (*mcp.CallToolResult, error) {
	if args.Title == "" {
		return toolResultError("title is required"), nil
	}
	if (args.Query == "") == (len(args.Data) == 0) {
		return toolResultError("pass exactly one of query or data"), nil
	}
	// Normalize filters before running the query: values the model set must
	// reach the FIRST render too, not only refreshes from the view.
	for i := range args.Filters {
		if args.Filters[i].Name == "" {
			return toolResultError(fmt.Sprintf("filters[%d].name is required", i)), nil
		}
	}

	// Nothing to resolve: a control's options come from the rows it filters,
	// which is the only population where the values and their counts can
	// actually agree with what the user sees. The view derives them.
	for i := range args.Filters {
		f := &args.Filters[i]
		if f.Control != "" && !slices.Contains(vizControls, f.Control) {
			return toolResultError(fmt.Sprintf("filter %q: control %q is not one of %s", f.Name, f.Control, strings.Join(vizControls, "|"))), nil
		}
		if f.Field == "" {
			f.Field = f.Name
		}
	}

	// The caps are a TABLE story. A chart never moves rows through the
	// conversation — the view fetches every point itself — so a chart query
	// runs uncapped: how many points make sense is the chart author's call.
	cap := 0
	if kind == "table" {
		cap = viz.RowHardCap
	}
	var (
		rows      []map[string]any
		truncated bool
		err       error
	)
	if args.Query != "" {
		vars := args.Variables
		rows, truncated, err = viz.QueryRows(ctx, s.querier, args.Query, vars, args.JQTransform, cap, s.cfg.QueryTTL)
	} else {
		rows, err = viz.CanonicalRows(args.Data)
		if err == nil && cap > 0 && len(rows) > cap {
			rows, truncated = rows[:cap], true
		}
	}
	if err != nil {
		return toolResultError(err.Error()), nil
	}

	// Volume is the caller's problem to state, not ours to paper over: hugr
	// tables run to hundreds of millions of rows, and quietly truncating would
	// produce a table that is simply wrong. An unbounded query that turns out
	// to be large comes back as an error asking for the window, once.
	// One rule, and it cannot loop: an UNBOUNDED query that turns out to be
	// large is refused, with the row count in the message. A bounded one is
	// always honoured — the caller has by then been told how many rows there
	// are, so asking for the first 2000 of 5432 is an informed decision, not a
	// mistake to correct.
	if kind == "table" && args.Query != "" && len(rows) >= viz.RowSoftLimit && !viz.QueryHasLimit(args.Query) {
		return toolResultError(fmt.Sprintf(
			"the query returned %d rows and sets no bound of its own. Up to %d render fine; beyond that say what to show rather than shipping everything. %s",
			len(rows), viz.RowSoftLimit, viz.PagingRecipe)), nil
	}
	// Below the render limit a full page is a window the caller chose on
	// purpose ("show me 50 examples"), so it is flagged, not refused. A chart's
	// limit is a top-N and needs no flag at all.
	atLimit := false
	if kind == "table" && args.Query != "" && len(rows) > 0 {
		if limit, known := viz.QueryLimit(args.Query, args.Variables); known && len(rows) == limit {
			atLimit = true
		}
	}

	env := VizEnvelope{
		Kind:      kind,
		Title:     args.Title,
		Chart:     args.Chart,
		Columns:   args.Columns,
		Filters:   args.Filters,
		Rows:      rows,
		RowCount:  len(rows),
		Truncated: truncated,
		AtLimit:   atLimit,
	}
	if args.Query != "" {
		env.Source = &VizSource{
			Type:        "query",
			Query:       args.Query,
			Variables:   args.Variables,
			JQTransform: args.JQTransform,
		}
	} else {
		env.Source = &VizSource{Type: "inline"}
	}
	return vizResult(&env), nil
}

// vizResult decides what the caller receives. The text fallback is always the
// full summary — it is what a host without MCP Apps shows — but once a result
// outgrows the sample size the conversation gets the sample and the rendered
// view goes and fetches the rows for itself. Anything that fits in the sample
// travels inline: a round trip to move fifty rows would buy nothing.
func vizResult(env *VizEnvelope) *mcp.CallToolResult {
	text := vizFallbackText(env)

	out := *env
	if env.Source != nil && env.Source.Type == "query" && len(env.Rows) > vizModelRows {
		out.Rows = env.Rows[:vizModelRows]
		out.RowsSampled = true
		out.Fetch = true
	}
	return mcp.NewToolResultStructured(out, text)
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// --- text fallback ---

// vizFallbackText is what hosts WITHOUT MCP Apps show: not a JSON dump but a
// readable summary — title, shape, and the first rows as a markdown table.
func vizFallbackText(env *VizEnvelope) string {
	var b strings.Builder
	fmt.Fprintf(&b, "## %s\n\n", env.Title)
	if env.Kind == "kpi" {
		b.WriteString("| metric | value | change | |\n| --- | --- | --- | --- |\n")
		for _, c := range env.Kpis {
			change := ""
			switch {
			case c.DeltaPct != nil:
				change = fmt.Sprintf("%+.2f%%", *c.DeltaPct)
			case c.Delta != nil:
				change = strconv.FormatFloat(*c.Delta, 'f', -1, 64)
				if *c.Delta >= 0 {
					change = "+" + change
				}
			}
			val := mdCell(c.Value)
			if c.Unit != "" {
				val += " " + c.Unit
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", mdCell(c.Label), val, change, mdCell(c.Subtitle))
		}
		return b.String()
	}
	if env.Chart != nil {
		series := strings.Join(env.Chart.Y, ", ")
		if env.Chart.Series != "" {
			series = fmt.Sprintf("%s by %s", env.Chart.Y[0], env.Chart.Series)
		}
		fmt.Fprintf(&b, "%s chart of %s over %s. ", env.Chart.Type, series, env.Chart.X)
	}
	fmt.Fprintf(&b, "%d rows", env.RowCount)
	if env.Truncated {
		fmt.Fprintf(&b, " (cut at the hard cap)")
	}
	b.WriteString(".\n\n")
	if env.AtLimit {
		fmt.Fprintf(&b, "⚠️ Exactly the query's own limit came back, so there are almost certainly more rows behind it — this view is one page, not the whole answer. %s\n\n", viz.PagingRecipe)
	}

	cols := vizFallbackColumns(env)
	if len(cols) == 0 {
		return b.String()
	}
	shown := min(len(env.Rows), vizFallbackRows)
	b.WriteString("| " + strings.Join(cols, " | ") + " |\n")
	b.WriteString("|" + strings.Repeat(" --- |", len(cols)) + "\n")
	for _, r := range env.Rows[:shown] {
		cells := make([]string, len(cols))
		for i, c := range cols {
			cells[i] = mdCell(r[c])
		}
		b.WriteString("| " + strings.Join(cells, " | ") + " |\n")
	}
	if len(env.Rows) > shown {
		fmt.Fprintf(&b, "\n… %d more rows in the structured result.\n", len(env.Rows)-shown)
	}
	return b.String()
}

// vizFallbackColumns picks a deterministic column order: the explicit table
// columns, else the chart bindings first, else sorted row fields.
func vizFallbackColumns(env *VizEnvelope) []string {
	if len(env.Columns) > 0 {
		cols := make([]string, len(env.Columns))
		for i, c := range env.Columns {
			cols[i] = c.Field
		}
		return cols
	}
	if len(env.Rows) == 0 {
		return nil
	}
	rest := sortedKeys(env.Rows[0])
	if env.Chart == nil {
		return rest
	}
	first := make([]string, 0, 2+len(env.Chart.Y))
	if env.Chart.X != "" {
		first = append(first, env.Chart.X)
	}
	if env.Chart.Series != "" {
		first = append(first, env.Chart.Series)
	}
	first = append(first, env.Chart.Y...)
	cols := make([]string, 0, len(rest))
	seen := map[string]bool{}
	for _, c := range first {
		if !seen[c] {
			seen[c] = true
			cols = append(cols, c)
		}
	}
	for _, c := range rest {
		if !seen[c] {
			cols = append(cols, c)
		}
	}
	return cols
}

func mdCell(v any) string {
	if v == nil {
		return ""
	}
	var s string
	switch f := v.(type) {
	case float64:
		// %v prints large floats in scientific notation — 1.5635261e+07 in a
		// fallback table meant for humans.
		s = strconv.FormatFloat(f, 'f', -1, 64)
	default:
		s = fmt.Sprintf("%v", v)
	}
	s = strings.ReplaceAll(s, "|", "\\|")
	s = strings.ReplaceAll(s, "\n", " ")
	return s
}
