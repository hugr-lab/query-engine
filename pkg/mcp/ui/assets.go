// Package ui holds the embedded browser assets of the visualization stack —
// the widget page, the shared JS core and the ECharts runtime — and exports
// them as bytes: go:embed does not cross package borders, so everything that
// inlines these assets into a page (pkg/mcp for the widget builds,
// pkg/mcp/reports for the report document) imports them from here.
package ui

import _ "embed"

// VizHTML is the widget page source. It carries two placeholders the page
// builders substitute: <!--__ECHARTS__--> (the chart runtime, or nothing for
// the lean table/KPI build) and <!--__VIZCORE__--> (always CoreJS).
//
//go:embed viz.html
var VizHTML string

// CoreJS is the shared visualization core (window.hugrVizCore): the pure
// builders — chart option, KPI card, table fill, formats — used by every
// page, so a fix lands once.
//
//go:embed viz-core.js
var CoreJS string

// EChartsJS is the committed ECharts runtime (full build; the slim recipe
// lives in README.md next to it).
//
//go:embed echarts.min.js
var EChartsJS string

// ReportHTML is the report document (design-039): one page for both lives —
// the MCP Apps widget (payload arrives via tool-result) and the standalone
// saved file (payload baked into the __REPORT_PAYLOAD__ island; the widget
// bakes it itself when the user downloads, a future HTTP renderer fills the
// same island server-side).
//
//go:embed report.html
var ReportHTML string
