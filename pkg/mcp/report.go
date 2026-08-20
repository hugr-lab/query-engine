package mcp

// MCP surface of design/039 reports: one prepared multi-section document over
// hugr data. viz-report is the model-facing tool — it runs the report and the
// host renders the ui://hugr/viz-report-* view; report-data is the app-only
// re-run channel behind the panel's Apply (and its options_only light mode).
// Everything heavy lives in pkg/mcp/reports; this file is the wire.

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/hugr-lab/query-engine/pkg/mcp/reports"
	"github.com/hugr-lab/query-engine/pkg/mcp/ui"
	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"
)

const reportResourceURI = "ui://hugr/viz-report-" + vizViewVersion

// The report page always carries the chart runtime: whether the document
// holds charts is the spec's business, and the resource is one static build
// per URI. The payload island stays an empty object — a widget receives its
// report through the tool result, not the template.
var reportPage = sync.OnceValue(func() string {
	page := strings.Replace(ui.ReportHTML, "<!--__ECHARTS__-->", "<script>"+ui.EChartsJS+"</script>", 1)
	page = strings.Replace(page, "<!--__VIZCORE__-->", "<script>"+ui.CoreJS+"</script>", 1)
	return strings.Replace(page, "__REPORT_PAYLOAD__", "{}", 1)
})

// ReportEnvelope is what viz-report returns: the spec echoed verbatim (the
// view's Apply re-submits it) plus the run results, sampled for the wire the
// same way viz results are.
type ReportEnvelope struct {
	Spec *reports.Spec       `json:"spec"`
	Data *reports.ReportData `json:"data"`
}

type reportToolArgs struct {
	Spec        map[string]any `json:"spec"`
	Variables   map[string]any `json:"variables"`
	OptionsOnly bool           `json:"options_only"`
}

func (s *Server) addReports(mcpServer *server.MCPServer) {
	res := mcp.NewResource(reportResourceURI, "Hugr Report",
		mcp.WithResourceDescription("Multi-section report document for viz-report (MCP Apps)"),
		mcp.WithMIMEType(vizMIMEType),
	)
	meta := map[string]any{"ui": map[string]any{"prefersBorder": true}}
	res.Meta = mcp.NewMetaFromMap(meta)
	size := int64(len(reportPage()))
	res.Size = &size
	mcpServer.AddResource(res, func(_ context.Context, req mcp.ReadResourceRequest) ([]mcp.ResourceContents, error) {
		slog.Info("MCP app view read", "uri", req.Params.URI, "bytes", size)
		return []mcp.ResourceContents{
			mcp.TextResourceContents{
				URI:      req.Params.URI,
				MIMEType: vizMIMEType,
				Text:     reportPage(),
				Meta:     meta,
			},
		}, nil
	})

	reportTool := mcp.NewTool("viz-report",
		mcp.WithDescription(`Render a REPORT — one printable document assembling several sections at once: KPI panels, charts, tables and markdown narrative, laid out on a 12-column grid, with a server-side filter panel. Hosts with MCP Apps open it as a live view (the user changes filters and the queries re-run under their permissions; Download saves a self-contained HTML snapshot; Print makes the PDF); other hosts get a markdown summary, so calling it is always safe. Use it when the user wants "a report" / "one document" — for a single ad-hoc chart or table keep using viz-chart / viz-table.
THE SPEC — one JSON object:
- variables: [{name, type, default?, required?}] — typed query parameters shared by all sections. 'type' is a hugr GraphQL type reference: String, Int, Float, Boolean, Date, Timestamp, [String!] (what 'in:' takes), an enum or input type name. A variable is used by a section simply by being DECLARED in that section's query; jq also sees each value as $name.
- controls: the filter panel, a layer OVER variables: [{label, control?, bind, required?, template?, options?|options_query?}]. Unlike viz-chart filters, a control has NO name and NO field — 'bind' is its only target: a variable name (or {from,to} — two variables — for numrange/daterange; dotted paths reach into input-typed variables). control is select|multiselect|search|number|numrange|date|daterange|toggle (inferred when omitted). options_query {query, jq} resolves choices from data and may itself declare other variables — dependent lists re-resolve when their parents change. An empty control submits null, and hugr DROPS a filter condition whose value is null — so 'filter:{st:{in:$states}}' simply loses the predicate when nothing is selected; mark a control required when that widening must not happen.
- sections: [{kind: kpi|chart|table|text, title?, description?, width?, span?, page_break?, query?, jq?, chart?, columns?, markdown?}]. Each kpi/chart/table section runs its own hugr query (aliased aggregations run in parallel inside one query) + jq shaping the result into the SAME canonical forms as viz-kpi/viz-chart/viz-table: KPI cards [{label, value, ...}], flat rows for charts/tables. jq input is the full envelope — paths start with .data. 'chart' is the viz-chart mapping {type,x,y,series?,stacked?}. width is full|two_thirds|half|third|quarter (or span: 1..12) — a chart and its companion table are two sections side by side. text sections carry markdown. page_break:"before" forces a print page break. Table queries must set their own limit.
Errors in one section render inside that section — the document still comes back. The result samples large row sets (rows_sampled=true) — the rendered view fetches the full data itself, so volume costs the conversation nothing.`),
		mcp.WithObject("spec", mcp.Required(),
			mcp.Description("The report spec: {title, description?, variables?, controls?, sections}"),
			mcp.AdditionalProperties(true)),
		mcp.WithObject("variables", mcp.Description("Initial variable values, by name (defaults apply where absent)")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		// Deliberately NO output schema: a control's bind is polymorphic (a
		// variable name OR {from, to}), and the reflected schema declares the
		// Go struct instead — the HOST then validates structuredContent
		// against it and refuses to render every result that carries
		// controls. An undeclared schema is honest; the description teaches
		// the shape.
	)
	reportTool.Meta = mcp.NewMetaFromMap(map[string]any{"ui": map[string]any{
		"resourceUri": reportResourceURI,
		"visibility":  []string{"model", "app"},
	}})
	mcpServer.AddTool(reportTool, s.vizReport)

	// The panel's Apply: re-runs the report with new variable values (or only
	// the option lists, when a parent variable changed). App-only — the model
	// re-calls viz-report instead, so the conversation keeps the spec.
	dataTool := mcp.NewTool("report-data",
		mcp.WithDescription("App-only re-run channel for the report view: executes the report spec with the submitted variable values and returns fresh section data (options_only refreshes just the control option lists). Not callable by the model."),
		mcp.WithObject("spec", mcp.Required(), mcp.Description("The report spec, unchanged"),
			mcp.AdditionalProperties(true)),
		mcp.WithObject("variables", mcp.Description("Current variable values from the panel")),
		mcp.WithBoolean("options_only", mcp.Description("Resolve control options only; skip the sections")),
		mcp.WithReadOnlyHintAnnotation(true),
		mcp.WithDestructiveHintAnnotation(false),
		// No output schema — same polymorphic-bind reason as viz-report.
	)
	dataTool.Meta = mcp.NewMetaFromMap(map[string]any{"ui": map[string]any{
		"visibility": []string{"app"},
	}})
	mcpServer.AddTool(dataTool, s.reportData)
}

func (s *Server) parseReportArgs(req mcp.CallToolRequest) (*reports.Spec, *reportToolArgs, error) {
	var args reportToolArgs
	if err := req.BindArguments(&args); err != nil {
		return nil, nil, fmt.Errorf("bad arguments: %w", err)
	}
	if len(args.Spec) == 0 {
		return nil, nil, fmt.Errorf("spec is required")
	}
	raw, err := json.Marshal(args.Spec)
	if err != nil {
		return nil, nil, fmt.Errorf("bad spec: %w", err)
	}
	spec, err := reports.Parse(raw)
	if err != nil {
		return nil, nil, err
	}
	return spec, &args, nil
}

func (s *Server) vizReport(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	spec, args, err := s.parseReportArgs(req)
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	data, err := reports.Run(ctx, s.querier, spec, args.Variables, reports.RunOptions{QueryTTL: s.cfg.QueryTTL})
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	// At authoring time the document must run CLEAN. A partial render with
	// error boxes reads as success to the calling model — it retries the
	// same broken spec against a rendered widget forever. So every section
	// and options failure comes back as ONE tool error, everything at once:
	// fix the queries, call again. (Per-section degradation still exists —
	// through report-data, where a USER moving filters must not lose the
	// whole document.)
	if errs := reportRunErrors(spec, data); errs != "" {
		return toolResultError("the report did not run clean — nothing was rendered. Fix the queries and call viz-report again:\n" + errs), nil
	}
	env := ReportEnvelope{Spec: spec, Data: sampleReportData(data)}
	logReportRun("viz-report", spec, env.Data, env)
	return mcp.NewToolResultStructured(env, reportFallbackText(spec, data)), nil
}

// logReportRun leaves one line per run with what the hugr log cannot otherwise
// show: the shape and the SIZE of the structured payload the host receives.
// The middleware's response_len counts only the text preview — a host choking
// on a large structuredContent was invisible.
func logReportRun(tool string, spec *reports.Spec, data *reports.ReportData, wire any) {
	secs := make([]string, 0, len(data.Sections))
	for i, s := range data.Sections {
		part := fmt.Sprintf("%d:%s", i, s.Kind)
		switch {
		case s.Error != "":
			part += "=ERR"
		case s.Kind == "kpi":
			part += fmt.Sprintf("=%d cards", len(s.Kpis))
		case s.Kind == "text":
		default:
			part += fmt.Sprintf("=%d rows", s.RowCount)
			if s.RowsSampled {
				part += fmt.Sprintf(" (sampled to %d)", len(s.Rows))
			}
		}
		secs = append(secs, part)
	}
	opts := make([]string, 0, len(data.Controls))
	for _, c := range data.Controls {
		switch {
		case c.Error != "":
			opts = append(opts, c.Label+"=ERR")
		case len(c.Options) > 0:
			opts = append(opts, fmt.Sprintf("%s=%d", c.Label, len(c.Options)))
		}
	}
	size := 0
	if b, err := json.Marshal(wire); err == nil {
		size = len(b)
	}
	slog.Info("MCP report run", "tool", tool, "title", spec.Title,
		"structured_bytes", size,
		"sections", strings.Join(secs, "; "),
		"options", strings.Join(opts, "; "))
}

// reportRunErrors collects every failure of a run, named precisely: the
// model fixes them all in one round trip instead of discovering them one by
// one.
func reportRunErrors(spec *reports.Spec, data *reports.ReportData) string {
	var b strings.Builder
	for i, sec := range data.Sections {
		if sec.Error == "" {
			continue
		}
		name := fmt.Sprintf("section %d", i)
		if i < len(spec.Sections) && spec.Sections[i].Title != "" {
			name = fmt.Sprintf("section %d (%q)", i, spec.Sections[i].Title)
		}
		fmt.Fprintf(&b, "- %s: %s\n", name, sec.Error)
	}
	for _, c := range data.Controls {
		if c.Error != "" {
			fmt.Fprintf(&b, "- control %q options: %s\n", c.Label, c.Error)
		}
	}
	return b.String()
}

func (s *Server) reportData(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	spec, args, err := s.parseReportArgs(req)
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	data, err := reports.Run(ctx, s.querier, spec, args.Variables,
		reports.RunOptions{QueryTTL: s.cfg.QueryTTL, OptionsOnly: args.OptionsOnly})
	if err != nil {
		return toolResultError(err.Error()), nil
	}
	// Straight to the open view, full rows — nothing here reaches the model.
	logReportRun("report-data", spec, data, data)
	return mcp.NewToolResultStructured(*data, fmt.Sprintf("%d sections", len(data.Sections))), nil
}

// sampleReportData trims the wire copy the same way viz results are trimmed:
// rows belong to the view, not to the conversation. The view sees
// rows_sampled and pulls the full set through report-data (the section
// queries are cache reads by then).
func sampleReportData(data *reports.ReportData) *reports.ReportData {
	out := *data
	out.Sections = make([]reports.SectionData, len(data.Sections))
	for i, sec := range data.Sections {
		if len(sec.Rows) > vizModelRows {
			sec.Rows = sec.Rows[:vizModelRows]
			sec.RowsSampled = true
		}
		out.Sections[i] = sec
	}
	return &out
}

// reportFallbackText is the document for hosts without MCP Apps: a readable
// markdown summary — the KPI cards in full, the shape of everything else.
func reportFallbackText(spec *reports.Spec, data *reports.ReportData) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n", spec.Title)
	if spec.Description != "" {
		fmt.Fprintf(&b, "%s\n\n", spec.Description)
	}
	for i := range spec.Sections {
		sec := &spec.Sections[i]
		var out *reports.SectionData
		if data != nil && i < len(data.Sections) {
			out = &data.Sections[i]
		}
		if sec.Title != "" {
			fmt.Fprintf(&b, "## %s\n\n", sec.Title)
		}
		if out != nil && out.Error != "" {
			fmt.Fprintf(&b, "⚠️ section failed: %s\n\n", out.Error)
			continue
		}
		switch sec.Kind {
		case "text":
			fmt.Fprintf(&b, "%s\n\n", sec.Markdown)
		case "kpi":
			if out == nil {
				continue
			}
			b.WriteString("| metric | value | change | |\n| --- | --- | --- | --- |\n")
			for _, c := range out.Kpis {
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
			b.WriteString("\n")
		case "chart":
			series := strings.Join(sec.Chart.Y, ", ")
			if sec.Chart.Series != "" {
				series = fmt.Sprintf("%s by %s", sec.Chart.Y[0], sec.Chart.Series)
			}
			n := 0
			if out != nil {
				n = out.RowCount
			}
			fmt.Fprintf(&b, "%s chart of %s over %s — %d rows (rendered in the report view).\n\n", sec.Chart.Type, series, sec.Chart.X, n)
		case "table":
			n := 0
			if out != nil {
				n = out.RowCount
			}
			fmt.Fprintf(&b, "table — %d rows (rendered in the report view).\n\n", n)
		}
	}
	return b.String()
}
