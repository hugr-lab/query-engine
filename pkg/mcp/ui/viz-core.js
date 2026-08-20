/* Shared visualization core — one source consumed by the MCP viz widget
   (viz.html) and the report document (report.html), inlined into each page at
   embed time through the VIZCORE placeholder. Pure builders only: data in,
   DOM/option out. No host bridge, no page state — each consumer keeps its
   own. Fixes here reach every page; a copy in a template would drift (the
   reason this file exists). */
"use strict";
(function () {
  const PALETTE = ["#1c7d78", "#0a3145", "#e8a838", "#7f9c9a", "#c94f4f", "#5b8dbf", "#8a6fae", "#4caf7d"];

  function fmtNum(v) {
    return Number.isInteger(v) ? v.toLocaleString("en-US") : v.toLocaleString("en-US", { maximumFractionDigits: 2 });
  }

  function distinct(rows, key) {
    const seen = new Set(), out = [];
    for (const r of rows) { const s = String(r[key]); if (!seen.has(s)) { seen.add(s); out.push(r[key]); } }
    return out;
  }

  /* chart mapping + canonical rows → ECharts option. Theme comes in through
     opts ({dark, textColor}) — the core reads no page globals. */
  function buildOption(chart, rows, opts) {
    opts = opts || {};
    const dark = !!opts.dark;
    const axis = dark ? "#8aa0ae" : "#64748b";
    const split = dark ? "#24343e" : "#e2e8f0";
    const base = {
      color: PALETTE,
      backgroundColor: "transparent",
      // still: render the series synchronously. A print pass snapshots the
      // page right after the beforeprint handler returns — animated series
      // are still on frame 0 there, and the PDF got axes with no data.
      animation: !opts.still,
      animationDuration: 300,
      textStyle: { color: opts.textColor || (dark ? "#e2e8f0" : "#1c2b33") },
      tooltip: { trigger: chart.type === "pie" || chart.type === "scatter" ? "item" : "axis" },
      legend: { top: 0, type: "scroll", textStyle: { color: axis } },
      grid: { left: 8, right: 12, top: 30, bottom: 4, containLabel: true },
    };
    const y0 = (chart.y || [])[0];

    if (chart.type === "pie") {
      return Object.assign(base, {
        series: [{ type: "pie", radius: ["38%", "70%"], label: { color: base.textStyle.color },
          data: rows.map((r) => ({ name: String(r[chart.x]), value: r[y0] })) }],
      });
    }
    if (chart.type === "scatter") {
      const series = chart.series
        ? distinct(rows, chart.series).map((g) => ({ name: String(g), type: "scatter",
            data: rows.filter((r) => r[chart.series] === g).map((r) => [r[chart.x], r[y0]]) }))
        : (chart.y || []).map((y) => ({ name: y, type: "scatter", data: rows.map((r) => [r[chart.x], r[y]]) }));
      return Object.assign(base, {
        xAxis: { type: "value", name: chart.x, axisLabel: { color: axis }, splitLine: { lineStyle: { color: split } } },
        yAxis: { type: "value", axisLabel: { color: axis }, splitLine: { lineStyle: { color: split } } },
        series,
      });
    }

    // line | area | bar over categories
    const et = chart.type === "area" ? "line" : chart.type;
    const cats = distinct(rows, chart.x).map(String);
    const mk = (name, byX) => ({ name, type: et, data: cats.map((c) => byX.get(c) ?? null),
      stack: chart.stacked ? "total" : undefined, areaStyle: chart.type === "area" ? {} : undefined,
      smooth: chart.type !== "bar" });
    const series = chart.series
      ? distinct(rows, chart.series).map((g) =>                       // long form
          mk(String(g), new Map(rows.filter((r) => r[chart.series] === g).map((r) => [String(r[chart.x]), r[y0]]))))
      : (chart.y || []).map((y) =>                                    // wide form
          mk(y, new Map(rows.map((r) => [String(r[chart.x]), r[y]]))));
    const category = { type: "category", data: cats, axisLabel: { color: axis, hideOverlap: true } };
    const value = { type: "value", axisLabel: { color: axis }, splitLine: { lineStyle: { color: split } } };
    // A horizontal bar is the same chart with the axes exchanged.
    return Object.assign(base, chart.horizontal && chart.type === "bar"
      ? { xAxis: value, yAxis: Object.assign({ inverse: true }, category), series }
      : { xAxis: category, yAxis: value, series });
  }

  // A hand-drawn polyline: a sparkline is not worth a charting library.
  function sparkline(vals) {
    const ns = "http://www.w3.org/2000/svg";
    const svg = document.createElementNS(ns, "svg");
    svg.setAttribute("class", "spark");
    svg.setAttribute("viewBox", "0 0 100 26");
    svg.setAttribute("preserveAspectRatio", "none");
    const min = Math.min(...vals), max = Math.max(...vals), span = max - min || 1;
    const pts = vals.map((v, i) =>
      `${(i / (vals.length - 1)) * 100},${25 - ((v - min) / span) * 23 - 1}`).join(" ");
    const line = document.createElementNS(ns, "polyline");
    line.setAttribute("points", pts);
    line.setAttribute("fill", "none");
    line.setAttribute("stroke", "var(--hugr-primary)");
    line.setAttribute("stroke-width", "1.5");
    line.setAttribute("vector-effect", "non-scaling-stroke");
    svg.appendChild(line);
    return svg;
  }

  /* one canonical KPI card → its DOM (.kpi) — styling comes from the page */
  function kpiCard(c) {
    const card = document.createElement("div");
    card.className = "kpi";
    const lab = document.createElement("div");
    lab.className = "lab";
    lab.textContent = c.label;
    const val = document.createElement("div");
    val.className = "val";
    val.textContent = typeof c.value === "number"
      ? fmtNum(c.value) + (c.format === "percent" ? "%" : "")
      : String(c.value);
    if (c.unit) {
      const u = document.createElement("span");
      u.className = "unit";
      u.textContent = c.unit;
      val.appendChild(u);
    }
    card.append(lab, val);
    const d = c.delta_pct ?? c.delta;
    if (d !== undefined && d !== null) {
      const dl = document.createElement("div");
      const sign = d > 0 ? 1 : d < 0 ? -1 : 0;
      const dir = c.direction || "up_good";
      dl.className = "dlt " + (sign === 0 || dir === "neutral" ? "flat"
        : (sign > 0) === (dir === "up_good") ? "good" : "bad");
      const arrow = sign > 0 ? "▲" : sign < 0 ? "▼" : "•";
      dl.textContent = arrow + " " + (c.delta_pct !== undefined && c.delta_pct !== null
        ? fmtNum(Math.abs(c.delta_pct)) + "%" : fmtNum(Math.abs(c.delta)));
      card.appendChild(dl);
    }
    if (Array.isArray(c.trend) && c.trend.length > 1) card.appendChild(sparkline(c.trend));
    if (c.subtitle) {
      const s = document.createElement("div");
      s.className = "sub";
      s.textContent = c.subtitle;
      card.appendChild(s);
    }
    return card;
  }

  function rowComparator(col, dir) {
    return (a, b) => {
      const x = a[col], y = b[col];
      if (typeof x === "number" && typeof y === "number") return (x - y) * dir;
      return String(x ?? "").localeCompare(String(y ?? "")) * dir;
    };
  }

  function noteRow(tbody, span, text) {
    const td = tbody.insertRow().insertCell();
    td.colSpan = span || 1;
    td.style.color = "var(--muted)";
    td.textContent = text;
  }

  /* rows + column specs → a filled <table>. o: {sortCol, sortDir, onSort,
     limit, emptyText, moreSuffix}. Sorting STATE belongs to the caller; the
     core only draws it and reports clicks. */
  function fillTable(tbl, rows, cols, o) {
    o = o || {};
    let view = rows;
    if (o.sortCol) view = [...rows].sort(rowComparator(o.sortCol, o.sortDir || 1));
    const limit = o.limit || view.length;
    const shown = view.slice(0, limit);

    tbl.textContent = "";
    const hr = tbl.createTHead().insertRow();
    for (const c of cols) {
      const th = document.createElement("th");
      th.textContent = (c.label || c.field) + (o.sortCol === c.field ? (o.sortDir > 0 ? " ↑" : " ↓") : "");
      if (c.align === "right" || c.format === "number") th.className = "num";
      if (o.onSort) th.onclick = () => o.onSort(c.field);
      hr.appendChild(th);
    }
    const tb = tbl.createTBody();
    for (const r of shown) {
      const tr = tb.insertRow();
      for (const c of cols) {
        const td = tr.insertCell();
        const v = r[c.field];
        if (typeof v === "number") { td.className = "num"; td.textContent = fmtNum(v); }
        else td.textContent = v === null || v === undefined ? "" : String(v);
      }
    }
    if (!view.length) noteRow(tb, cols.length, o.emptyText || "no rows");
    else if (view.length > limit) noteRow(tb, cols.length, `… ${view.length - limit} more rows` + (o.moreSuffix || ""));
  }

  function csvCell(v) {
    if (v === null || v === undefined) return "";
    const s = String(v);
    return /[",\n;]/.test(s) ? '"' + s.replace(/"/g, '""') + '"' : s;
  }
  function toCSV(rows, cols) {
    return [cols.join(","), ...rows.map((r) => cols.map((c) => csvCell(r[c])).join(","))].join("\n");
  }
  function exportName(title, ext) {
    const base = (title || "hugr-viz").replace(/[^\p{L}\p{N}]+/gu, "-").replace(/^-|-$/g, "").toLowerCase();
    return (base || "hugr-viz") + "." + ext;
  }

  window.hugrVizCore = { PALETTE, fmtNum, distinct, buildOption, sparkline, kpiCard,
    rowComparator, fillTable, csvCell, toCSV, exportName };
})();
