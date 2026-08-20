// Headless drive of pkg/mcp/ui/report.html in jsdom — both lives of the
// document: STANDALONE (payload baked into the island, no host, top-level
// window) and WIDGET (empty island, mock MCP Apps host, payload via
// tool-result, Apply through report-data). ECharts is stubbed; the shared
// core rides exactly as the server inlines it.
import { JSDOM } from "jsdom";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const SRC = fileURLToPath(new URL("./report.html", import.meta.url));
const CORE = fileURLToPath(new URL("./viz-core.js", import.meta.url));

const ECHARTS_STUB = `<script>
  window.__charts = [];
  window.echarts = { init: (el, theme, opts) => {
    const inst = { theme, el, opts: opts || {}, option: null,
      setOption(o) { this.option = o; window.__lastOption = o;
        el.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg"><rect/></svg>'; },
      resize() {}, dispose() { el.innerHTML = ""; } };
    window.__charts.push(inst);
    return inst;
  }};
</script>`;

function assemble(payload) {
  return readFileSync(SRC, "utf8")
    .replace("<!--__ECHARTS__-->", ECHARTS_STUB)
    .replace("<!--__VIZCORE__-->", "<script>" + readFileSync(CORE, "utf8") + "</script>")
    .replace("__REPORT_PAYLOAD__", payload ? JSON.stringify(payload).replace(/</g, "\\u003c") : "{}");
}

let failures = 0;
let snapshotHTML = "";
function check(name, cond, detail) {
  if (cond) console.log(`  ok   ${name}`);
  else { failures++; console.log(`  FAIL ${name}${detail ? " — " + detail : ""}`); }
}
const tick = (n = 3) => new Promise((r) => setTimeout(r, n));

// --- the report used throughout -------------------------------------------
const SPEC = {
  title: "Quarterly review",
  description: "one page over the numbers",
  variables: [
    { name: "states", type: "[String!]" },
    { name: "date_from", type: "Date", default: "2023-01-01" },
    { name: "date_to", type: "Date", default: "2023-12-31" },
    { name: "cutoff", type: "Date" },
    { name: "min_total", type: "Int" },
  ],
  controls: [
    // The options depend on $date_from — a Period change re-resolves them.
    { label: "States", control: "multiselect", bind: "states",
      options_query: { query: "query($date_from: Date) { op { st(filter: {d: {gte: $date_from}}) { key { st } } } }",
        jq: ".data.op.st | map(.key.st)" } },
    { label: "Period", control: "daterange", bind: { from: "date_from", to: "date_to" }, required: true },
    // No explicit kind — the type of the bound variable decides.
    { label: "Cutoff", bind: "cutoff" },
    { label: "Min total", bind: "min_total" },
  ],
  sections: [
    { kind: "kpi", title: "Headline" },
    { kind: "chart", title: "Trend", width: "two_thirds", chart: { type: "line", x: "month", y: ["total"] } },
    { kind: "table", title: "Top", width: "third", columns: [{ field: "name" }, { field: "total", format: "number" }] },
    { kind: "text", markdown: "## Notes\n- first **bold** point\n\nplain <script>alert(1)</script> stays text\n\n| Объект | Записей |\n|---|---|\n| general_payments | **14 607 336** |\n| providers | 628 012 |" },
  ],
};
const DATA = {
  variables: { states: ["CA"], date_from: "2023-01-01", date_to: "2023-12-31" },
  controls: [
    { label: "States", options: [{ value: "CA" }, { value: "NY", label: "New York" }] },
    { label: "Period" },
    { label: "Cutoff" },
    { label: "Min total" },
  ],
  sections: [
    { kind: "kpi", kpis: [
      { label: "Revenue", value: 12345.6, unit: "$", delta_pct: 3.2, direction: "up_good", trend: [1, 2, 3] },
      { label: "Orders", value: 42 },
    ] },
    { kind: "chart", rows: [{ month: "2023-01", total: 10 }, { month: "2023-02", total: 20 }], row_count: 2 },
    { kind: "table", rows: [{ name: "acme", total: 100 }], row_count: 1 },
    { kind: "text" },
  ],
};

/* ========================= standalone life ========================= */
{
  const dom = new JSDOM(assemble({ spec: SPEC, data: DATA }), {
    runScripts: "dangerously", pretendToBeVisual: true, url: "https://report.test/",
    beforeParse(window) {
      window.Element.prototype.getBoundingClientRect = () => ({ height: 800, width: 900, top: 0, left: 0, right: 900, bottom: 800 });
    },
  });
  const win = dom.window;
  const $ = (sel) => win.document.querySelector(sel);
  const $$ = (sel) => [...win.document.querySelectorAll(sel)];
  await tick(30);

  console.log("\n[standalone: baked payload renders without any host]");
  check("title from the spec", $("#title").textContent === "Quarterly review");
  check("description shown", $("#desc").textContent === "one page over the numbers");
  check("four sections", $$("#sections .sec").length === 4);
  check("two KPI cards", $$(".kpi").length === 2);
  check("delta coloured by direction", !!$(".kpi .dlt.good"));
  check("sparkline drawn", !!$(".kpi svg.spark polyline"));
  check("chart option built", win.__lastOption?.series?.[0]?.type === "line");
  check("chart categories from rows", JSON.stringify(win.__lastOption?.xAxis?.data) === '["2023-01","2023-02"]');
  check("table rows filled", $$(".sec.sec-table tbody tr").length === 1);
  check("numeric cell right-aligned", $(".sec.sec-table td.num")?.textContent === "100");
  check("grid width honoured", $$("#sections .sec")[1].style.gridColumn === "span 8");

  console.log("\n[standalone: markdown is DOM-built, never HTML]");
  check("heading rendered", $(".md h3")?.textContent === "Notes");
  check("bold inline", $(".md li strong")?.textContent === "bold");
  check("script text stays text", !win.document.querySelector(".md script")
    && $(".md").textContent.includes("<script>alert(1)</script>"));
  check("pipe table becomes a table", $$(".md table tbody tr").length === 2);
  check("with header cells", $$(".md table th").map((c) => c.textContent).join(",") === "Объект,Записей");
  check("inline formatting inside cells", $(".md table tbody strong")?.textContent === "14 607 336");
  check("no pipe paragraphs left", !$$(".md p").some((p) => p.textContent.includes("|")));

  console.log("\n[standalone: panel is an honest snapshot]");
  check("panel visible", !$("#panel").hidden);
  check("panel has its title", $(".panel-title")?.textContent === "Filters");
  check("no Apply without a host", !$("#btn-apply"));
  check("snapshot note shown", $("#panel-note")?.textContent.includes("snapshot"));
  check("download hidden (you already have the file)", $("#btn-dl").hidden === true);
  check("print offered instead", $("#btn-print").hidden === false);
  check("range prefilled from variables", $$(".ctl .range input")[0]?.value === "2023-01-01");

  console.log("\n[standalone: multiselect is a dropdown picker, inert here]");
  check("no inline checkbox column", !$(".ctl .checks"));
  const pickBtn = $(".pick > button");
  check("picker summarises the current value", pickBtn?.querySelector(".val")?.textContent === "CA");
  check("picker inert without a host", pickBtn?.disabled === true);
  check("inputs inert without a host", $$(".ctl .range input").every((i) => i.disabled));

  console.log("\n[standalone: omitted control kind follows the variable type]");
  const ctlInput = (label) => $$(".ctl").find((b) => b.querySelector("label")?.textContent.startsWith(label))?.querySelector("input, select, .checks");
  check("Date variable gets a date input", ctlInput("Cutoff")?.type === "date");
  check("Int variable gets a number input", ctlInput("Min total")?.type === "number");
}

/* =========================== widget life =========================== */
{
  const fromView = [];
  let lastToolCall = null;
  let lastDownload = null;
  const parentMock = { postMessage(msg) {
    fromView.push(msg);
    if (msg.method === "ui/initialize") {
      toView({ jsonrpc: "2.0", id: msg.id, result: {
        protocolVersion: "2026-01-26",
        hostCapabilities: { serverTools: {}, downloadFile: {} },
        hostInfo: { name: "mock", version: "1" },
        hostContext: { theme: "light", displayMode: "inline", availableDisplayModes: ["inline", "fullscreen"] },
      }});
    } else if (msg.method === "tools/call") {
      lastToolCall = msg.params;
      toView({ jsonrpc: "2.0", id: msg.id, result: { structuredContent: msg.params.arguments.options_only
        ? { controls: [{ label: "States", options: [{ value: "TX" }] }, { label: "Period" }, { label: "Cutoff" }, { label: "Min total" }] }
        : { ...DATA, variables: msg.params.arguments.variables } } });
    } else if (msg.method === "ui/download-file") {
      lastDownload = msg;
      toView({ jsonrpc: "2.0", id: msg.id, result: {} });
    } else if (msg.id !== undefined) {
      toView({ jsonrpc: "2.0", id: msg.id, result: {} });
    }
  }};
  const dom = new JSDOM(assemble(null), {
    runScripts: "dangerously", pretendToBeVisual: true, url: "https://report.test/",
    beforeParse(window) {
      Object.defineProperty(window, "parent", { value: parentMock, configurable: true });
      window.Element.prototype.getBoundingClientRect = () => ({ height: 800, width: 900, top: 0, left: 0, right: 900, bottom: 800 });
    },
  });
  const win = dom.window;
  const $ = (sel) => win.document.querySelector(sel);
  const $$ = (sel) => [...win.document.querySelectorAll(sel)];
  function toView(msg) {
    setTimeout(() => {
      const ev = new win.MessageEvent("message", { data: msg });
      Object.defineProperty(ev, "source", { value: parentMock });
      win.dispatchEvent(ev);
    }, 0);
  }
  const sent = (m) => fromView.filter((x) => x.method === m);
  await tick(30);

  console.log("\n[widget: handshake]");
  check("ui/initialize sent", sent("ui/initialize").length === 1);
  check("initialized notified", sent("ui/notifications/initialized").length === 1);
  check("download offered once a host can save", $("#btn-dl").hidden === false);
  check("print hidden in the widget", $("#btn-print").hidden === true);
  check("fullscreen toggle offered", $("#btn-mode").hidden === false);

  console.log("\n[widget: report arrives via tool-result, sampled rows are re-fetched]");
  const sampled = JSON.parse(JSON.stringify(DATA));
  sampled.sections[1].rows_sampled = true;   // the chart section came as a sample
  toView({ jsonrpc: "2.0", method: "ui/notifications/tool-result",
    params: { structuredContent: { spec: SPEC, data: sampled }, content: [] } });
  await tick(30);
  check("sections rendered", $$("#sections .sec").length === 4);
  check("size reported", sent("ui/notifications/size-changed").length > 0);
  check("full data pulled through report-data", lastToolCall?.name === "report-data");
  check("with the spec riding along", lastToolCall?.arguments?.spec?.title === "Quarterly review");
  check("not options_only", !lastToolCall?.arguments?.options_only);

  console.log("\n[widget: Apply re-runs with the panel's values]");
  lastToolCall = null;
  $(".pick > button").click();
  await tick();
  check("picker opens with the options", $$(".pick .opt input").length === 2);
  check("current value ticked", $$(".pick .opt input")[0]?.checked === true);
  const nyBox = $$(".pick .opt input")[1];
  nyBox.checked = true;
  nyBox.onchange();
  $("#btn-apply").click();
  await tick(30);
  check("report-data called", lastToolCall?.name === "report-data");
  check("the new value went out", JSON.stringify(lastToolCall?.arguments?.variables?.states) === '["CA","NY"]');
  check("sections re-rendered from the result", $$("#sections .sec").length === 4);

  console.log("\n[widget: download = this page, current data baked in]");
  $("#btn-dl").click();
  await tick(30);
  const res = lastDownload?.params?.contents?.[0]?.resource;
  check("a self-contained html goes out", (res?.text || "").startsWith("<!doctype html>"));
  check("with the payload baked into the island", (res?.text || "").includes('"Quarterly review"'));
  check("island in the live page stays empty", win.document.getElementById("report-payload").textContent === "{}");
  check("named after the report", res?.uri === "file:///quarterly-review.html");
  check("marked as a snapshot", (res?.text || "").includes("data-snapshot"));
  snapshotHTML = res?.text || "";

  console.log("\n[widget: a parent change re-resolves dependent options live]");
  lastToolCall = null;
  const sectionsBefore = $("#sections").innerHTML;
  const fromInput = $$(".ctl .range input")[0];
  fromInput.value = "2023-02-01";
  fromInput.onchange();
  await tick(600);   // past the 400ms debounce
  check("options_only went out, debounced", lastToolCall?.name === "report-data"
    && lastToolCall?.arguments?.options_only === true);
  check("with the new parent value", lastToolCall?.arguments?.variables?.date_from === "2023-02-01");
  check("sections untouched — options only", $("#sections").innerHTML === sectionsBefore);
  const statesBox = $('#panel .ctl[data-ci="0"]');
  check("dependent box rebuilt with fresh options", !!statesBox);
  statesBox.querySelector(".pick > button").click();
  await tick();
  check("the new option list is offered", [...statesBox.querySelectorAll(".pick .opt span:last-child")]
    .map((s) => s.textContent).join(",") === "TX");
  check("a selection outside the new list is pruned", statesBox.querySelector(".pick .val")?.textContent === "— all —");

  console.log("\n[widget: an unrelated change does not refetch options]");
  lastToolCall = null;
  const cutoffInput = $('#panel .ctl[data-ci="2"] input');
  cutoffInput.value = "2023-06-01";
  cutoffInput.onchange();
  await tick(600);
  check("no options call for a variable nothing depends on", lastToolCall === null);
}

/* ========================== snapshot life ==========================
   The downloaded file REOPENED: everything a static page cannot honour must
   be gone, and printing must be offered instead. */
{
  const dom = new JSDOM(snapshotHTML, {
    runScripts: "dangerously", pretendToBeVisual: true, url: "https://saved.test/",
    beforeParse(window) {
      window.Element.prototype.getBoundingClientRect = () => ({ height: 800, width: 900, top: 0, left: 0, right: 900, bottom: 800 });
    },
  });
  const win = dom.window;
  const $ = (sel) => win.document.querySelector(sel);
  const $$ = (sel) => [...win.document.querySelectorAll(sel)];
  await tick(30);

  console.log("\n[snapshot: the saved file is honest about what it can do]");
  check("sections render from the baked payload", $$("#sections .sec").length === 4);
  check("chart alive in the snapshot", win.__lastOption?.series?.[0]?.type === "line");
  check("no Apply", !$("#btn-apply"));
  check("snapshot note shown", $("#panel-note")?.textContent.includes("snapshot"));
  check("panel inputs inert", $$("#panel input").every((i) => i.disabled));
  check("Download gone", $("#btn-dl").hidden === true);
  check("bridge log gone", $("#btn-debug").hidden === true);
  check("Print offered", $("#btn-print").hidden === false);
  check("filters summary carries the values", $("#applied").textContent.includes("CA"));

  console.log("\n[snapshot: printing re-renders still, light and restores]");
  win.dispatchEvent(new win.Event("beforeprint"));
  await tick();
  check("print pass disables series animation", win.__lastOption?.animation === false);
  check("print pass forces the light theme", win.document.documentElement.dataset.theme === "light");
  check("print layout flag set", win.document.documentElement.dataset.printing === "1");
  win.dispatchEvent(new win.Event("afterprint"));
  await tick();
  check("theme restored after print", win.document.documentElement.dataset.theme === "light");
  check("animation back on screen", win.__lastOption?.animation === true);
  check("print layout flag cleared", !win.document.documentElement.dataset.printing);

  console.log("\n[snapshot: the honest state is baked into the MARKUP too]");
  // Sandboxed previews render the serialized DOM without running the script.
  const raw = new JSDOM(snapshotHTML).window.document;
  check("Download hidden in raw markup", raw.getElementById("btn-dl")?.hidden === true);
  check("bridge log hidden in raw markup", raw.getElementById("btn-debug")?.hidden === true);
  check("Print visible in raw markup", raw.getElementById("btn-print")?.hidden === false);
  check("no Apply in raw markup", !raw.getElementById("btn-apply"));
  check("inputs disabled in raw markup", [...raw.querySelectorAll("#panel input")].every((i) => i.disabled));
}

console.log(failures ? `\n${failures} checks FAILED` : "\nall checks passed");
process.exit(failures ? 1 : 0);
