You are creating a **data dashboard** — a visual overview of a dataset with KPIs, charts, breakdowns, and trends.

## Approach

1. **Understand the domain** — use `discovery-search_modules` and `discovery-search_module_data_objects` to map out available data.
2. **Identify key metrics** — find numeric fields for KPIs (totals, averages, counts) and categorical fields for breakdowns.
3. **Explore dimensions** — use `discovery-field_values` to find useful grouping dimensions (categories, statuses, time periods, regions).
4. **Build ONE comprehensive query** with aliases for all dashboard sections:
   - **Summary KPIs**: `_aggregation` for totals, counts, averages
   - **Breakdowns**: `_bucket_aggregation` grouped by key dimensions
   - **Time series**: `_bucket_aggregation` with `key { date_field(bucket: month) }` sorted by `key.date_field`
   - **Top-N rankings**: `_bucket_aggregation` sorted by `aggregations.metric.sum` with limit
5. **Execute with jq** — reshape data into a clean structure for the dashboard.
6. **Generate React artifact** — create a single React component with the Hugr brand style.

## Output Format

Choose the best format based on your capabilities:

**If React artifacts are supported** (Claude Desktop, chat with artifacts):
- Generate a **React component** (JSX) as an artifact
- Embed query result data as a const (no fetch calls)
- Self-contained with inline styles

**If only text/HTML is available** (API, terminal, MCP without artifacts):
- Generate a **self-contained HTML page** with inline CSS
- Embed data directly in the HTML
- Use `<style>` tag for all styling

Both formats should:
- Use the Hugr brand color palette and typography below
- Include KPI cards, tables with visual bars, and section headers
- Import Google Font Quicksand via `@import`

## Hugr Brand Style Guide

### Color Palette

```js
const colors = {
  primary:        '#1c7d78',  // Headers, KPI values, chart bars, active states
  primaryDark:    '#145854',  // Section titles, dark accents
  primaryLight:   '#20908a',  // Secondary elements, lighter accents
  navy:           '#0a3145',  // Dashboard title, main headings
  text:           '#333333',  // Body text
  textSecondary:  '#4f4f4f',  // Labels, descriptions
  background:     '#ffffff',  // Card backgrounds
  backgroundAlt:  'rgba(214, 229, 228, 0.39)', // Section bg, alternating rows
  border:         '#e1e1e1',  // Card borders, table borders
  accent:         '#3b82f6',  // Trend indicators, sparklines
  success:        '#10b981',  // Positive trends
  danger:         '#ef4444',  // Negative trends
};
```

### Typography

```js
const fonts = {
  family: "'Quicksand', sans-serif",
  // Import: @import url('https://fonts.googleapis.com/css2?family=Quicksand:wght@400;500;600;700&display=swap')
  dashboardTitle: { fontWeight: 700, fontSize: 24, color: colors.navy },
  sectionTitle:   { fontWeight: 600, fontSize: 18, color: colors.primaryDark },
  kpiValue:       { fontWeight: 700, fontSize: 32, color: colors.primary },
  kpiLabel:       { fontWeight: 500, fontSize: 12, color: colors.textSecondary, textTransform: 'uppercase', letterSpacing: 1 },
  tableHeader:    { fontWeight: 600, fontSize: 13, color: colors.textSecondary, textTransform: 'uppercase' },
  body:           { fontWeight: 400, fontSize: 14, color: colors.text },
};
```

### Component Patterns

**KPI Card:**
```jsx
<div style={{
  background: '#fff', border: '1px solid #e1e1e1', borderRadius: 12,
  padding: 20, boxShadow: '0 2px 8px rgba(0,0,0,0.08)', flex: 1, minWidth: 160
}}>
  <div style={fonts.kpiLabel}>Total Revenue</div>
  <div style={fonts.kpiValue}>$1.2M</div>
</div>
```

**Percentage Bar in Table:**
```jsx
<td>
  <div style={{ display: 'flex', alignItems: 'center', gap: 8 }}>
    <div style={{ background: '#1c7d78', height: 8, borderRadius: 4, width: `${pct}%`, minWidth: 4 }} />
    <span style={{ fontSize: 12, color: '#4f4f4f' }}>{pct}%</span>
  </div>
</td>
```

**Section Container:**
```jsx
<div style={{
  background: '#fff', border: '1px solid #e1e1e1', borderRadius: 12,
  padding: 24, boxShadow: '0 2px 8px rgba(0,0,0,0.08)', marginBottom: 16
}}>
  <h3 style={fonts.sectionTitle}>Breakdown by Category</h3>
  {/* table content */}
</div>
```

### Dashboard Layout

```jsx
<div style={{ fontFamily: "'Quicksand', sans-serif", maxWidth: 1000, margin: '0 auto', padding: 24 }}>
  <style>{'@import url("https://fonts.googleapis.com/css2?family=Quicksand:wght@400;500;600;700&display=swap")'}</style>
  <h1 style={fonts.dashboardTitle}>Dashboard Title</h1>
  {/* KPI row */}
  <div style={{ display: 'flex', gap: 16, marginBottom: 24, flexWrap: 'wrap' }}>
    {/* KPI cards */}
  </div>
  {/* Sections */}
</div>
```

## Dashboard Sections

1. **Title** — dashboard name + date range + record count
2. **KPI row** — 3-5 key metrics in cards (value + label + optional delta/trend)
3. **Breakdowns** — tables with category distribution, percentage bars, count
4. **Time trends** — table with time periods, use bar widths to show relative values
5. **Top-N** — ranked table sorted by key metric, with visual bars
6. **Insights** — 2-3 text callouts highlighting notable patterns (teal left border)

## Query Rules Reminder

- Bucket aggregation `order_by` uses dot-paths: `"key.fieldname"`, `"aggregations._rows_count"`, `"aggregations.amount.sum"`
- Direction is UPPERCASE: `ASC`, `DESC`
- String fields: count/any/first/last/list only — NO min/max/avg/sum
- If result is truncated, increase `max_result_size` (up to 5000) or use jq
- Combine ALL dashboard sections in ONE query using aliases

Example query:
```graphql
query {
  module {
    summary: table_aggregation {
      _rows_count
      revenue { sum avg }
    }
    by_category: table_bucket_aggregation(
      order_by: [{field: "aggregations.revenue.sum", direction: DESC}]
      limit: 10
    ) {
      key { category }
      aggregations { _rows_count revenue { sum avg } }
    }
    by_month: table_bucket_aggregation(
      order_by: [{field: "key.created_at", direction: ASC}]
    ) {
      key { created_at(bucket: month) }
      aggregations { _rows_count revenue { sum } }
    }
  }
}
```
