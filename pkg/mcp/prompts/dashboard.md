You are creating a **data dashboard** — a visual overview of a dataset with KPIs, charts, breakdowns, and trends.

A dashboard here is a **sequence of independent views**, one `viz-chart` /
`viz-table` call per section, tied together by a short narrative. Every call
is stateless and complete in itself — there is no dashboard object to create
or update, and no view can be changed after it renders. A different cut means
calling the tool again.

## Approach

1. **Understand the domain** — `catalog-list(kind: module)` for the map, `catalog-search` for what is relevant to the dashboard.
2. **Identify key metrics** — find numeric fields for KPIs (totals, averages, counts) and categorical fields for breakdowns.
3. **Explore dimensions** — `data-field_values` to find useful grouping dimensions (categories, statuses, time periods, regions).
4. **Lead with a KPI panel** — ONE `viz-kpi` call whose query aliases the `_aggregation` totals (current + comparison period); `jq_transform` assembles the cards with deltas and trends.
5. **Render one view per section**, each its own tool call:

| Section | Tool | Query shape |
|---|---|---|
| Headline metrics | `viz-kpi` | aliased `_aggregation` queries + jq into cards |
| Time trend | `viz-chart` (line/area) | `_bucket_aggregation`, `key { date_field(bucket: month) }`, sorted by `key.date_field` |
| Breakdown by dimension | `viz-chart` (bar or pie) | `_bucket_aggregation` grouped by the dimension |
| Top-N ranking | `viz-chart` (bar) | `_bucket_aggregation` sorted by `aggregations.metric.sum`, `limit: N` |
| Detail / drill-down | `viz-table` | plain select with an explicit `limit` (and `offset` for paging) |

6. **Narrate between views** — one or two sentences per section: what the view shows and what stands out. Close with the main takeaways.

## Rules

- Charts are built from aggregations and have no row limits — the rendered
  view fetches its own data, nothing large passes through the conversation.
- Tables must be bounded: always an explicit `limit`.
- Filters on a view are client-side controls over the delivered rows — put
  real conditions in the query itself, and add controls only for slicing the
  result on screen.
- Do not restate the numbers a view already shows; the narrative adds
  interpretation, not duplication.
- On hosts without MCP Apps the same calls return the rows as structured JSON
  with a markdown preview — the dashboard degrades to a readable report by
  itself. Do not build artifact pages instead of calling the tools.
