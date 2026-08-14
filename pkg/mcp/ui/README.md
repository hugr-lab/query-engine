# pkg/mcp/ui — the MCP Apps views and their assets

The interactive chart/table/KPI view that `viz-chart`, `viz-table` and
`viz-kpi` render in (design-038). Served as an MCP resource with mime
`text/html;profile=mcp-app`; see `../viz.go`. The URI carries a version
segment (`vizViewVersion`) because ChatGPT caches the widget HTML by URI —
bump it whenever the shipped view must replace one a host may have cached.

| file | what it is |
|---|---|
| `viz.html` | the widget. Hand-written MCP Apps bridge (spec revision `2026-01-26`) — no npm build step, no SDK bundle. Widget-only concerns live here: bridge, filters, chart bar, export routes |
| `viz-core.js` | the shared visualization core (`window.hugrVizCore`): chart option builder, KPI card, table fill, sparkline, formats, CSV. Pure builders — no host bridge, no page state. Inlined into every page (the report document of design-039 included), so a fix lands once instead of drifting per template |
| `echarts.min.js` | Apache ECharts 5.6.0, `dist/echarts.min.js` verbatim. Apache-2.0, license header intact |
| `assets.go` | exports the files as bytes — go:embed does not cross package borders, and `pkg/mcp/reports` inlines the same assets into the report document |
| `viz_view_test.mjs` | drives `viz.html` headlessly in jsdom against a mock host |
| `dev-host.html` | a mock host you can open in a browser to see the view render outside Claude Desktop |

## Why the library is committed

The MCP Apps CSP denies every external origin unless the resource declares it,
so the page must be self-contained. Declaring a CDN domain would trade a 1 MB
resource for a runtime dependency on the network — bad for air-gapped
deployments and a version-skew risk. `viz.go` splices the library into the page
once, at first read.

The page is assembled by string substitution of the `ECHARTS` and `VIZCORE`
placeholder comments, so neither inlined script may contain a literal
`</script>` (or the placeholder text itself); a Go test asserts the assembled
chart page has exactly three closing script tags — library, core, app.

## Working on the view

```bash
npm i jsdom          # once
node viz_view_test.mjs
```

The jsdom run covers the handshake, tool-result handling, both filter kinds,
the refresh call, display modes, theming, the KPI panel and the table. It
stubs ECharts and asserts on the option object, so chart *mapping* is tested
but chart *painting* is not — for that, use `dev-host.html` against a running
engine:

```bash
BIND=:15005 MCP_ENABLED=true CORE_DB_PATH= ALLOWED_ANONYMOUS=true ANONYMOUS_ROLE=admin ./hugr
python3 -m http.server 15006 --directory pkg/mcp/ui
open http://localhost:15006/dev-host.html
```

Neither `dev-host.html` nor the test is embedded in the binary — only
`viz.html`, `viz-core.js` and `echarts.min.js` are, by exact name
(`assets.go`).

## Slim ECharts build (deferred on purpose)

The committed `echarts.min.js` is the FULL build (~1 MB inlined per chart
page). A tree-shaken build (~400 KB) was considered in design/039 and
deferred: it saves only bytes, while cutting a module a future feature needs
(dataZoom, markLine, more chart types) would force a rebuild round-trip.
When size starts to matter, the recipe — revisit the import list first:

```sh
npm i echarts esbuild
cat > slim.mjs <<'JS'
export * from "echarts/core";
import { use } from "echarts/core";
import { LineChart, BarChart, PieChart, ScatterChart } from "echarts/charts";
import { GridComponent, TooltipComponent, LegendComponent } from "echarts/components";
import { SVGRenderer } from "echarts/renderers";
use([LineChart, BarChart, PieChart, ScatterChart,
     GridComponent, TooltipComponent, LegendComponent, SVGRenderer]);
JS
npx esbuild slim.mjs --bundle --minify --format=iife \
    --global-name=echarts --outfile=echarts.min.js
```

The widget needs exactly: line/bar/pie/scatter (area = line + areaStyle),
grid, tooltip, legend, SVGRenderer. Check `viz-core.js:buildOption` before
trusting that list.
