# `ui://hugr/viz-<version>` — the MCP Apps view

The interactive chart/table view that `viz-chart` and `viz-table` render in
(design-038). Served as an MCP resource with mime
`text/html;profile=mcp-app`; see `../viz.go`. The URI carries a version
segment (`vizViewVersion`) because ChatGPT caches the widget HTML by URI —
bump it whenever the shipped view must replace one a host may have cached.

| file | what it is |
|---|---|
| `viz.html` | the view. Hand-written MCP Apps bridge (spec revision `2026-01-26`) — no npm build step, no SDK bundle |
| `echarts.min.js` | Apache ECharts 5.6.0, `dist/echarts.min.js` verbatim. Apache-2.0, license header intact |
| `viz_view_test.mjs` | drives `viz.html` headlessly in jsdom against a mock host |
| `dev-host.html` | a mock host you can open in a browser to see the view render outside Claude Desktop |

## Why the library is committed

The MCP Apps CSP denies every external origin unless the resource declares it,
so the page must be self-contained. Declaring a CDN domain would trade a 1 MB
resource for a runtime dependency on the network — bad for air-gapped
deployments and a version-skew risk. `viz.go` splices the library into the page
once, at first read.

The page is assembled by string substitution of the `<!--__ECHARTS__-->`
placeholder, so the library must not contain a literal `</script>`; a Go test
asserts the assembled page has exactly two closing script tags.

## Working on the view

```bash
npm i jsdom          # once
node viz_view_test.mjs
```

The jsdom run covers the handshake, tool-result handling, both filter kinds,
the refresh call, display modes, theming and the table. It stubs ECharts and
asserts on the option object, so chart *mapping* is tested but chart *painting*
is not — for that, use `dev-host.html` against a running engine:

```bash
BIND=:15005 MCP_ENABLED=true CORE_DB_PATH= ALLOWED_ANONYMOUS=true ANONYMOUS_ROLE=admin ./hugr
python3 -m http.server 15006 --directory pkg/mcp/ui
open http://localhost:15006/dev-host.html
```

Neither file is embedded in the binary — only `viz.html` and `echarts.min.js`
are, by exact name.
