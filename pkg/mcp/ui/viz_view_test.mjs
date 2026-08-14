// Headless drive of pkg/mcp/ui/viz.html against a mock host, in jsdom.
// ECharts is stubbed so we can assert on the option object the view builds.
import { JSDOM } from "jsdom";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const SRC = fileURLToPath(new URL("./viz.html", import.meta.url));
const html = readFileSync(SRC, "utf8").replace(
  "<!--__ECHARTS__-->",
  `<script>
     window.__charts = [];
     window.echarts = { init: (el, theme, opts) => {
       const inst = { theme, el, opts: opts || {}, option: null,
         setOption(o) { this.option = o; window.__lastOption = o;
           el.innerHTML = '<svg xmlns="http://www.w3.org/2000/svg"><rect/></svg>'; },
         resize() {}, dispose() { el.innerHTML = ""; } };
       window.__charts.push(inst);
       return inst;
     }};
   </script>`,
);

// --- mock host -------------------------------------------------------------
// The view starts its handshake as soon as its script parses, so the mock
// parent must be installed in beforeParse — installing it afterwards means the
// initialize request goes to jsdom's real window.parent and is never seen.
const fromView = [];
let lastToolCall = null;
let lastExport = null;
let win;
const hostCtx = {
  theme: "light", displayMode: "inline",
  availableDisplayModes: ["inline", "fullscreen"],
  styles: { variables: { "--color-text-primary": "#111827" } },
};

const parentMock = { postMessage(msg) {
    fromView.push(msg);
    if (msg.method === "ui/initialize") {
      toView({ jsonrpc: "2.0", id: msg.id, result: {
        protocolVersion: "2026-01-26",
        hostCapabilities: { serverTools: {}, openLinks: {}, downloadFile: {} },
        hostInfo: { name: "mock", version: "1" },
        hostContext: hostCtx,
      }});
    } else if (msg.method === "ui/request-display-mode") {
      hostCtx.displayMode = msg.params.mode;
      toView({ jsonrpc: "2.0", id: msg.id, result: { mode: msg.params.mode } });
    } else if (msg.method === "ui/download-file" || msg.method === "ui/message") {
      lastExport = msg;
      toView({ jsonrpc: "2.0", id: msg.id, result: {} });
    } else if (msg.method === "tools/call") {
      lastToolCall = msg.params;
      toView({ jsonrpc: "2.0", id: msg.id, result: {
        structuredContent: { rows: [{ module: "refreshed", objects: 42 }], row_count: 1, truncated: false },
      }});
    } else if (msg.id !== undefined) {
      toView({ jsonrpc: "2.0", id: msg.id, result: {} });
    }
  }};

const dom = new JSDOM(html, {
  runScripts: "dangerously", pretendToBeVisual: true, url: "https://view.test/",
  beforeParse(window) {
    Object.defineProperty(window, "parent", { value: parentMock, configurable: true });
    // jsdom lays nothing out, so the view would always measure 0 and skip the
    // size report; give it a height so the reporting path is exercised.
    window.Element.prototype.getBoundingClientRect = () => ({ height: 420, width: 600, top: 0, left: 0, right: 600, bottom: 420 });
  },
});
win = dom.window;

// Always asynchronous: a real host answers across a process boundary, and the
// view's very first request is issued while its own script is still parsing.
function toView(msg) {
  setTimeout(() => {
    const ev = new win.MessageEvent("message", { data: msg });
    Object.defineProperty(ev, "source", { value: parentMock });
    win.dispatchEvent(ev);
  }, 0);
}
const tick = (n = 3) => new Promise((r) => setTimeout(r, n));

// --- assertions ------------------------------------------------------------
let failures = 0;
function check(name, cond, detail) {
  if (cond) console.log(`  ok   ${name}`);
  else { failures++; console.log(`  FAIL ${name}${detail ? " — " + detail : ""}`); }
}
const sent = (m) => fromView.filter((x) => x.method === m);
const $ = (sel) => win.document.querySelector(sel);
const closeMenusForTest = () => win.document.querySelectorAll(".menu").forEach((m) => { m.hidden = true; });

await tick(30);

console.log("\n[handshake]");
check("view sent ui/initialize", sent("ui/initialize").length === 1);
// Pinned against McpUiInitializeRequest in @modelcontextprotocol/ext-apps:
// appInfo + appCapabilities + protocolVersion, and nothing else. The prose in
// the spec shows a stale shape (capabilities/clientInfo); sending that gets the
// handshake rejected, and a rejected handshake is silent — the host then never
// sends tool-result and the view never appears at all.
const initParams = sent("ui/initialize")[0]?.params ?? {};
check("params are exactly the schema's three", JSON.stringify(Object.keys(initParams).sort()) === '["appCapabilities","appInfo","protocolVersion"]', JSON.stringify(Object.keys(initParams)));
check("appInfo carries name and version", !!initParams.appInfo?.name && !!initParams.appInfo?.version);
check("protocolVersion is 2026-01-26", initParams.protocolVersion === "2026-01-26");
check("declares inline+fullscreen+pip", JSON.stringify(initParams.appCapabilities?.availableDisplayModes) === '["inline","fullscreen","pip"]');
check("sent initialized notification", sent("ui/notifications/initialized").length === 1);
check("initialized comes after the response", fromView.indexOf(sent("ui/notifications/initialized")[0]) > fromView.indexOf(sent("ui/initialize")[0]));
check("host theme applied", win.document.documentElement.dataset.theme === "light");
check("host style variable applied", win.document.documentElement.style.getPropertyValue("--color-text-primary") === "#111827");

console.log("\n[tool result → chart]");
const envelope = {
  kind: "chart", title: "Revenue",
  chart: { type: "bar", x: "month", y: ["revenue"] },
  filters: [
    { name: "src", label: "Source", control: "select", field: "src", value: "core" },
    { name: "month", label: "Month", control: "select", field: "month" },
  ],
  source: { type: "query", query: "query{x}", variables: {}, jq_transform: ".data.x" },
  rows: [
    { month: "2026-01", revenue: 100, src: "core" },
    { month: "2026-02", revenue: 120, src: "core" },
    { month: "2026-03", revenue: 50, src: "pg" },
  ],
  row_count: 3, truncated: false,
};
toView({ jsonrpc: "2.0", method: "ui/notifications/tool-result", params: { structuredContent: envelope, content: [{ type: "text", text: "fallback" }] } });
await tick(20);

check("title rendered", $("#title").textContent === "Revenue");
check("a chart keeps its rows inline", lastToolCall === null);
check("chart container visible", !$("#chart").hidden);
check("echarts initialized", win.__charts.length === 1);
check("bar series built", win.__lastOption?.series?.[0]?.type === "bar");
check("categories from x field", JSON.stringify(win.__lastOption?.xAxis?.data) === '["2026-01","2026-02"]');
check("values from y field", JSON.stringify(win.__lastOption?.series?.[0]?.data) === "[100,120]");
check("table hidden inline for a chart", $("#tblwrap").hidden);
check("both filters rendered", win.document.querySelectorAll("#filters .filter").length === 2);
// The options are built from the delivered rows, so they cannot offer a value
// the table does not hold — the bug that made counts disagree with the screen.
$("#filters .filter .pick button").dispatchEvent(new win.MouseEvent("click", { bubbles: true }));
check("options come from the rows", [...win.document.querySelectorAll("#filters .filter .pick .opt .lbl")]
  .map((e) => e.textContent).sort().join(",") === "core,pg");
check("with their real counts, most frequent first", $("#filters .filter .pick .opt .desc").textContent === "2 rows");
closeMenusForTest();
check("every control is client-side", !win.document.querySelector("#filters .filter label .srv"));
check("model-set value preselected", $("#filters .filter .pick .val").textContent === "core");
check("row badge", $("#b-rows").textContent === "2 of 3 rows");
check("pushed model context", sent("ui/update-model-context").length >= 1);
check("reported size", sent("ui/notifications/size-changed").length >= 1);

console.log("\n[chart toolbar]");
const bar = () => [...win.document.querySelectorAll("#cbar .grp")].map((g) => g.querySelector("label").textContent);
check("toolbar offers the four mappings", bar().join(",") === "Chart,Category,Value,Series");
const barSelect = (label) => [...win.document.querySelectorAll("#cbar .grp")]
  .find((g) => g.querySelector("label").textContent === label).querySelector("select");
check("category options come from the rows", [...barSelect("Category").options].map((o) => o.value).sort().join(",") === "month,revenue,src");
check("value options are the numeric fields only", [...barSelect("Value").options].map((o) => o.value).join(",") === "revenue");

// Switching to a horizontal bar exchanges the axes rather than rebuilding
// anything — the rows never move.
const flip = (name) => [...win.document.querySelectorAll("#cbar .toggles label")]
  .find((l) => l.textContent.includes(name)).querySelector("input");
flip("horizontal").checked = true;
flip("horizontal").dispatchEvent(new win.Event("change"));
await tick(20);
check("horizontal puts the categories on the y axis", win.__lastOption?.yAxis?.type === "category");
check("and the values on the x axis", win.__lastOption?.xAxis?.type === "value");
check("no round trip for a chart setting", lastToolCall === null);
flip("horizontal").checked = false;
flip("horizontal").dispatchEvent(new win.Event("change"));
await tick(20);

barSelect("Chart").value = "line";
barSelect("Chart").dispatchEvent(new win.Event("change"));
await tick(20);
check("switching the type redraws from the same rows", win.__lastOption?.series?.[0]?.type === "line");

barSelect("Series").value = "src";
barSelect("Series").dispatchEvent(new win.Event("change"));
await tick(20);
check("a series field turns the rows long-form", win.__lastOption?.series?.[0]?.name === "core");
const swap = [...win.document.querySelectorAll("#cbar .toggles button")].find((b) => b.textContent.includes("swap"));
swap.dispatchEvent(new win.Event("click"));
await tick(20);
check("swap reads category and series the other way round",
  JSON.stringify(win.__lastOption?.xAxis?.data) === '["core"]');
// back to the shape the rest of the checks expect
barSelect("Series").value = "";
barSelect("Series").dispatchEvent(new win.Event("change"));
await tick(20);
barSelect("Category").value = "month";
barSelect("Category").dispatchEvent(new win.Event("change"));
await tick(20);
barSelect("Chart").value = "bar";
barSelect("Chart").dispatchEvent(new win.Event("change"));
await tick(20);

console.log("\n[export]");
// A sandboxed iframe cannot download anything itself, so everything leaves
// through the host — and what leaves is the FILTERED rows, not the delivery.
$("#btn-dl").dispatchEvent(new win.MouseEvent("click", { bubbles: true }));
const exportItems = [...win.document.querySelectorAll("#dl-menu .opt .lbl")].map((e) => e.textContent);
check("images first, then the rows", exportItems.join(",") === "PNG,SVG,CSV,JSON");
check("the chart renders with the svg renderer", win.__charts.at(-1)?.opts?.renderer === "svg");
const clickExport = (label) => {
  const row = [...win.document.querySelectorAll("#dl-menu .opt")]
    .find((r) => r.querySelector(".lbl").textContent === label);
  row.dispatchEvent(new win.MouseEvent("click", { bubbles: true }));
};
clickExport("CSV");
await tick(30);
check("a host with downloadFile gets a real file", lastExport?.method === "ui/download-file");
const res = lastExport?.params.contents[0].resource;
check("named after the title", res?.uri === "file:///revenue.csv");
check("a comma in a value is quoted, not breaking the row", !res?.text.includes('x,y,'));
check("as text/csv", res?.mimeType === "text/csv");
check("header plus every visible row", res?.text.split("\n").length === 3);
check("and the values themselves", res?.text.split("\n")[1].startsWith("2026-01,100,core"));

// The SVG export is the rendered chart itself, serialized as text.
$("#btn-dl").dispatchEvent(new win.MouseEvent("click", { bubbles: true }));
clickExport("SVG");
await tick(30);
const svgRes = lastExport?.params.contents[0].resource;
check("svg export goes out as text", svgRes?.mimeType === "image/svg+xml");
check("named after the title too", svgRes?.uri === "file:///revenue.svg");
check("and is the rendered svg node", (svgRes?.text || "").startsWith("<svg"));

console.log("\n[local filter]");
// The controls are searchable checkbox pickers now, not native selects: open
// the popover and click an option, exactly as a user does.
function pick(filterIndex, optionLabel) {
  const box = win.document.querySelectorAll("#filters .filter .pick")[filterIndex];
  box.querySelector("button").dispatchEvent(new win.MouseEvent("click", { bubbles: true }));
  const row = [...box.querySelectorAll(".opt")].find((r) => r.querySelector(".lbl").textContent === optionLabel);
  if (!row) throw new Error(`option ${optionLabel} not offered`);
  const input = row.querySelector("input");
  input.checked = !input.checked;
  input.dispatchEvent(new win.Event("change"));
}
pick(1, "2026-01");
await tick(20);
check("rows filtered client-side", JSON.stringify(win.__lastOption?.xAxis?.data) === '["2026-01"]');
check("badge shows the subset", $("#b-rows").textContent === "1 of 3 rows");
check("no server round trip for a local filter", lastToolCall === null);

console.log("\n[controls never re-query]");
pick(0, "pg");
await tick(30);
check("no tool call for any control", lastToolCall === null);
check("it filtered the delivered rows instead", $("#b-rows").textContent.startsWith("0 of 3"));

console.log("\n[fullscreen]");
$("#btn-mode").dispatchEvent(new win.Event("click"));
await tick(30);
check("requested fullscreen", sent("ui/request-display-mode")[0]?.params.mode === "fullscreen");
check("body mode switched", win.document.body.dataset.mode === "fullscreen");
check("table appears in fullscreen", !$("#tblwrap").hidden);
check("headers survive an empty filtered set", [...win.document.querySelectorAll("#tbl th")].map((t) => t.textContent).join(",") === "month,revenue,src");
check("empty state is explained", win.document.querySelector("#tbl tbody td").textContent === "no rows match the current filters");

console.log("\n[theme change]");
toView({ jsonrpc: "2.0", method: "ui/notifications/host-context-changed", params: { theme: "dark" } });
await tick(20);
check("dark attribute set", win.document.documentElement.dataset.theme === "dark");
check("chart re-initialized with the dark theme", win.__charts.at(-1).theme === "dark");

console.log("\n[a table fetches its own rows, but only when there is room]");
// Back to inline first: the sample is enough for a view that shows ~15 rows.
hostCtx.displayMode = "inline";
toView({ jsonrpc: "2.0", method: "ui/notifications/host-context-changed", params: { displayMode: "inline" } });
await tick(20);
lastToolCall = null;
toView({ jsonrpc: "2.0", method: "ui/notifications/tool-result", params: { structuredContent: {
  kind: "table", title: "Big", rows: [{ i: 0 }], row_count: 1000, rows_sampled: true, fetch: true,
  source: { type: "query", query: "{ big }", variables: {}, jq_transform: "" },
}}});
await tick(40);
check("an inline TABLE renders the sample without a round trip", lastToolCall === null);
check("and says so", $("#status").textContent.includes("first 1 of 1000 rows"));

// A chart is the opposite: it cannot be drawn from a sample, so it loads at
// once even inline.
lastToolCall = null;
toView({ jsonrpc: "2.0", method: "ui/notifications/tool-result", params: { structuredContent: {
  kind: "chart", title: "Wide", chart: { type: "line", x: "m", y: ["n"] },
  rows: [{ m: "a", n: 1 }], row_count: 300, rows_sampled: true, fetch: true,
  source: { type: "query", query: "{ wide }", variables: {}, jq_transform: "" },
}}});
await tick(60);
check("an inline CHART fetches every point at once", lastToolCall?.arguments.query === "{ wide }");

// back to the table case for the fullscreen check below
lastToolCall = null;
toView({ jsonrpc: "2.0", method: "ui/notifications/tool-result", params: { structuredContent: {
  kind: "table", title: "Big", rows: [{ i: 0 }], row_count: 1000, rows_sampled: true, fetch: true,
  source: { type: "query", query: "{ big }", variables: {}, jq_transform: "" },
}}});
await tick(40);
check("the table still waits", lastToolCall === null);

$("#btn-mode").dispatchEvent(new win.Event("click"));
await tick(60);
check("going fullscreen fetches the real rows", lastToolCall?.name === "viz-data");
check("with the view's own query", lastToolCall?.arguments.query === "{ big }");
// Caching is the deployment's business: nothing about it is sent, and the
// server derives the key from the query it is handed.
check("only query, variables and transform are sent",
  Object.keys(lastToolCall.arguments).sort().join(",") === "jq_transform,query,variables");
check("and renders what came back", $("#b-rows").textContent === "1 rows");
lastToolCall = null;
$("#btn-mode").dispatchEvent(new win.Event("click"));
await tick(40);
$("#btn-mode").dispatchEvent(new win.Event("click"));
await tick(60);
check("a second fullscreen does not re-fetch", lastToolCall === null);

console.log("\n[table view + errors]");
toView({ jsonrpc: "2.0", method: "ui/notifications/tool-result", params: { structuredContent: {
  kind: "table", title: "Objects", columns: [{ field: "name" }, { field: "n", format: "number" }],
  rows: [{ name: "b", n: 2 }, { name: "a", n: 1 }], row_count: 2, source: { type: "inline" },
}}});
await tick(20);
check("table visible for kind=table", !$("#tblwrap").hidden);
check("chart hidden for kind=table", $("#chart").hidden);
check("columns respected", [...win.document.querySelectorAll("#tbl th")].map((t) => t.textContent).join(",") === "name,n");
check("first row as delivered", win.document.querySelector("#tbl tbody tr td").textContent === "b");
win.document.querySelectorAll("#tbl th")[0].dispatchEvent(new win.Event("click"));
await tick(20);
check("clicking a header sorts", win.document.querySelector("#tbl tbody tr td").textContent === "a");

toView({ jsonrpc: "2.0", method: "ui/notifications/tool-result", params: { content: [{ type: "text", text: "query failed: boom" }] } });
await tick(20);
check("error surfaced from a result without rows", $("#error").textContent.includes("query failed: boom"));

/* A host that cannot save files must still let the data out. jsdom has no
   navigator.clipboard, so this exercises the whole chain down to the last
   route: download → clipboard → post into the chat. */
console.log("\n[export without host download support]");
{
  const sent = [];
  const parent2 = { postMessage(msg) {
    sent.push(msg);
    if (msg.method === "ui/initialize") {
      setTimeout(() => to2({ jsonrpc: "2.0", id: msg.id, result: {
        protocolVersion: "2026-01-26", hostCapabilities: { serverTools: {} },
        hostInfo: { name: "mock", version: "1" },
        hostContext: { theme: "light", displayMode: "inline", availableDisplayModes: ["inline"] },
      }}), 0);
    } else if (msg.id !== undefined) setTimeout(() => to2({ jsonrpc: "2.0", id: msg.id, result: {} }), 0);
  }};
  const dom2 = new JSDOM(html, { runScripts: "dangerously", pretendToBeVisual: true, url: "https://v2.test/",
    beforeParse(w) {
      Object.defineProperty(w, "parent", { value: parent2, configurable: true });
      w.Element.prototype.getBoundingClientRect = () => ({ height: 300, width: 600, top: 0, left: 0, right: 600, bottom: 300 });
    }});
  const w2 = dom2.window;
  function to2(m) {
    const ev = new w2.MessageEvent("message", { data: m });
    Object.defineProperty(ev, "source", { value: parent2 });
    w2.dispatchEvent(ev);
  }
  await tick(40);
  to2({ jsonrpc: "2.0", method: "ui/notifications/tool-result", params: { structuredContent: {
    kind: "table", title: "Small", rows: [{ a: 1, b: "x,y" }], row_count: 1, source: { type: "inline" },
  }}});
  await tick(30);
  w2.document.getElementById("btn-dl").dispatchEvent(new w2.MouseEvent("click", { bubbles: true }));
  check("the menu says it will copy, not download",
    w2.document.querySelector("#dl-menu .opt .desc").textContent.includes("copy"));
  check("no chart, so no image entry",
    [...w2.document.querySelectorAll("#dl-menu .opt .lbl")].map((e) => e.textContent).join(",") === "CSV,JSON");
  [...w2.document.querySelectorAll("#dl-menu .opt")][0].dispatchEvent(new w2.MouseEvent("click", { bubbles: true }));
  await tick(40);
  // No clipboard in jsdom either, so this lands on the last rung: an honest
  // refusal rather than dumping rows into the conversation.
  check("nothing is posted into the chat", !sent.some((m) => m.method === "ui/message"));
  check("and the user is told why", w2.document.getElementById("error").textContent.includes("neither saves files nor grants clipboard"));
}

/* ChatGPT bridges no ui/download-file but injects window.openai into the
   iframe; the export must find it by feature-detection and land the file in
   the user's library, with a best-effort browser download on top. */
console.log("\n[export via window.openai (ChatGPT)]");
{
  const sent3 = [];
  const uploads = [], extOpens = [];
  let dlUrlAsked = null;
  const parent3 = { postMessage(msg) {
    sent3.push(msg);
    if (msg.method === "ui/initialize") {
      setTimeout(() => to3({ jsonrpc: "2.0", id: msg.id, result: {
        protocolVersion: "2026-01-26", hostCapabilities: { serverTools: {} },
        hostInfo: { name: "chatgpt-mock", version: "1" },
        hostContext: { theme: "light", displayMode: "inline", availableDisplayModes: ["inline"] },
      }}), 0);
    } else if (msg.id !== undefined) setTimeout(() => to3({ jsonrpc: "2.0", id: msg.id, result: {} }), 0);
  }};
  const dom3 = new JSDOM(html, { runScripts: "dangerously", pretendToBeVisual: true, url: "https://v3.test/",
    beforeParse(w) {
      Object.defineProperty(w, "parent", { value: parent3, configurable: true });
      w.Element.prototype.getBoundingClientRect = () => ({ height: 300, width: 600, top: 0, left: 0, right: 600, bottom: 300 });
      w.openai = {
        uploadFile: async (file, opts) => { uploads.push({ file, opts }); return { fileId: "f-1" }; },
        getFileDownloadUrl: async ({ fileId }) => { dlUrlAsked = fileId; return { downloadUrl: "https://files.test/f-1" }; },
        openExternal: (a) => { extOpens.push(a); },
      };
    }});
  const w3 = dom3.window;
  function to3(m) {
    const ev = new w3.MessageEvent("message", { data: m });
    Object.defineProperty(ev, "source", { value: parent3 });
    w3.dispatchEvent(ev);
  }
  await tick(40);
  to3({ jsonrpc: "2.0", method: "ui/notifications/tool-result", params: { structuredContent: {
    kind: "table", title: "Lib", rows: [{ a: 1, b: "x,y" }], row_count: 1, source: { type: "inline" },
  }}});
  await tick(30);
  w3.document.getElementById("btn-dl").dispatchEvent(new w3.MouseEvent("click", { bubbles: true }));
  check("the menu says it will save to the library",
    w3.document.querySelector("#dl-menu .opt .desc").textContent.includes("save to library"));
  [...w3.document.querySelectorAll("#dl-menu .opt")][0].dispatchEvent(new w3.MouseEvent("click", { bubbles: true }));
  await tick(40);
  check("the file went through uploadFile", uploads.length === 1);
  check("named after the view", uploads[0]?.file?.name === "lib.csv");
  check("as text/csv", uploads[0]?.file?.type === "text/csv");
  check("and into the user's library", uploads[0]?.opts?.library === true);
  check("csv content is the quoted rows", (await uploads[0]?.file?.text())?.includes('"x,y"'));
  check("then a download url is fetched for it", dlUrlAsked === "f-1");
  check("and opened externally", extOpens[0]?.href === "https://files.test/f-1");
  check("without the redirect wrapper", extOpens[0]?.redirectUrl === false);
  check("the standard route was not attempted", !sent3.some((m) => m.method === "ui/download-file"));
  check("the user is told where it went", w3.document.getElementById("status").textContent.includes("file library"));
}

/* Past CHART_TABLE_CAP filtered rows the chart's companion table stops
   listing and offers the CSV instead; narrowing with a filter brings the
   listing back. viz-table keeps listing regardless. */
console.log("\n[chart companion table cap]");
{
  const sent4 = [];
  const parent4 = { postMessage(msg) {
    sent4.push(msg);
    if (msg.method === "ui/initialize") {
      setTimeout(() => to4({ jsonrpc: "2.0", id: msg.id, result: {
        protocolVersion: "2026-01-26", hostCapabilities: { serverTools: {}, downloadFile: {} },
        hostInfo: { name: "mock", version: "1" },
        hostContext: { theme: "light", displayMode: "fullscreen", availableDisplayModes: ["inline", "fullscreen"] },
      }}), 0);
    } else if (msg.id !== undefined) setTimeout(() => to4({ jsonrpc: "2.0", id: msg.id, result: {} }), 0);
  }};
  const dom4 = new JSDOM(html, { runScripts: "dangerously", pretendToBeVisual: true, url: "https://v4.test/",
    beforeParse(w) {
      Object.defineProperty(w, "parent", { value: parent4, configurable: true });
      w.Element.prototype.getBoundingClientRect = () => ({ height: 300, width: 600, top: 0, left: 0, right: 600, bottom: 300 });
    }});
  const w4 = dom4.window;
  function to4(m) {
    const ev = new w4.MessageEvent("message", { data: m });
    Object.defineProperty(ev, "source", { value: parent4 });
    w4.dispatchEvent(ev);
  }
  await tick(40);
  const big = Array.from({ length: 2001 }, (_, i) => ({ cat: "c" + (i % 5), val: i }));
  to4({ jsonrpc: "2.0", method: "ui/notifications/tool-result", params: { structuredContent: {
    kind: "chart", title: "Big", chart: { type: "bar", x: "cat", y: ["val"] },
    filters: [{ name: "cat" }], rows: big, row_count: big.length, source: { type: "inline" },
  }}});
  await tick(30);
  check("the companion table lists nothing", w4.document.querySelectorAll("#tbl tbody tr").length === 1);
  check("and says why", w4.document.querySelector("#tbl tbody td").textContent.includes("too many to list"));
  const csvBtn = w4.document.getElementById("csv-instead");
  check("offering the csv right there", !!csvBtn);
  csvBtn.dispatchEvent(new w4.MouseEvent("click", { bubbles: true }));
  await tick(40);
  const dl = sent4.find((m) => m.method === "ui/download-file");
  check("which exports through the host", dl?.params?.contents?.[0]?.resource?.uri === "file:///big.csv");
  check("all the rows, not a page", (dl?.params?.contents?.[0]?.resource?.text || "").split("\n").length === 2002);
  // narrow below the cap: the listing comes back
  const box = w4.document.querySelector("#filters .filter .pick");
  box.querySelector("button").dispatchEvent(new w4.MouseEvent("click", { bubbles: true }));
  const row = [...box.querySelectorAll(".opt")].find((r) => r.querySelector(".lbl").textContent === "c1");
  const input = row.querySelector("input");
  input.checked = true;
  input.dispatchEvent(new w4.Event("change"));
  await tick(30);
  check("a narrowing filter brings the listing back", w4.document.querySelectorAll("#tbl tbody tr").length > 1);
  check("and the note is gone", !w4.document.querySelector("#tbl tbody td").textContent.includes("too many"));
}

/* The KPI panel: cards render straight from structuredContent, nothing is
   fetched, and the delta colouring follows direction semantics. */
console.log("\n[kpi panel]");
{
  const sent5 = [];
  let toolCall5 = null;
  const parent5 = { postMessage(msg) {
    sent5.push(msg);
    if (msg.method === "ui/initialize") {
      setTimeout(() => to5({ jsonrpc: "2.0", id: msg.id, result: {
        protocolVersion: "2026-01-26", hostCapabilities: { serverTools: {}, downloadFile: {} },
        hostInfo: { name: "mock", version: "1" },
        hostContext: { theme: "light", displayMode: "inline", availableDisplayModes: ["inline", "fullscreen"] },
      }}), 0);
    } else if (msg.method === "tools/call") {
      toolCall5 = msg.params;
      setTimeout(() => to5({ jsonrpc: "2.0", id: msg.id, result: {} }), 0);
    } else if (msg.id !== undefined) setTimeout(() => to5({ jsonrpc: "2.0", id: msg.id, result: {} }), 0);
  }};
  const dom5 = new JSDOM(html, { runScripts: "dangerously", pretendToBeVisual: true, url: "https://v5.test/",
    beforeParse(w) {
      Object.defineProperty(w, "parent", { value: parent5, configurable: true });
      w.Element.prototype.getBoundingClientRect = () => ({ height: 300, width: 600, top: 0, left: 0, right: 600, bottom: 300 });
    }});
  const w5 = dom5.window;
  function to5(m) {
    const ev = new w5.MessageEvent("message", { data: m });
    Object.defineProperty(ev, "source", { value: parent5 });
    w5.dispatchEvent(ev);
  }
  await tick(40);
  to5({ jsonrpc: "2.0", method: "ui/notifications/tool-result", params: { structuredContent: {
    kind: "kpi", title: "Shop health", rows: [], source: { type: "query", query: "query{...}" },
    kpis: [
      { label: "Revenue", value: 1234567.89, unit: "$", delta_pct: 3.2, direction: "up_good",
        trend: [1, 3, 2, 5, 4], subtitle: "vs July" },
      { label: "Refund rate", value: 1.4, format: "percent", delta_pct: -0.3, direction: "down_good" },
      { label: "Status", value: "OK" },
    ],
  }}});
  await tick(30);
  const cards = [...w5.document.querySelectorAll("#kpis .kpi")];
  check("three cards render", cards.length === 3);
  check("the panel is visible", !w5.document.getElementById("kpis").hidden);
  check("chart and table stay hidden",
    w5.document.getElementById("chart").hidden && w5.document.getElementById("tblwrap").hidden);
  check("value is formatted with its unit",
    cards[0].querySelector(".val").textContent === "1,234,567.89$" ||
    cards[0].querySelector(".val").textContent.startsWith("1,234,567.89"));
  check("percent format marks the value", cards[1].querySelector(".val").textContent.startsWith("1.4%"));
  check("growing up_good delta is good", cards[0].querySelector(".dlt").classList.contains("good"));
  check("falling down_good delta is good too", cards[1].querySelector(".dlt").classList.contains("good"));
  check("a sparkline is drawn from trend", !!cards[0].querySelector("svg.spark polyline"));
  check("no sparkline without trend", !cards[1].querySelector("svg.spark"));
  check("subtitle shows", cards[0].querySelector(".sub")?.textContent === "vs July");
  check("badge counts cards, not rows", w5.document.getElementById("b-rows").textContent === "3 KPIs");
  check("nothing was fetched", toolCall5 === null);
  w5.document.getElementById("btn-dl").dispatchEvent(new w5.MouseEvent("click", { bubbles: true }));
  check("export offers the card list",
    [...w5.document.querySelectorAll("#dl-menu .opt .lbl")].map((e) => e.textContent).join(",") === "CSV,JSON");
  [...w5.document.querySelectorAll("#dl-menu .opt")][0].dispatchEvent(new w5.MouseEvent("click", { bubbles: true }));
  await tick(30);
  const kpiCsv = sent5.find((m) => m.method === "ui/download-file")?.params.contents[0].resource;
  check("csv of the cards goes out", (kpiCsv?.text || "").startsWith("label,value,unit"));
  check("with the card values in it", (kpiCsv?.text || "").includes("Revenue"));
}

console.log(failures ? `\n${failures} FAILURE(S)` : "\nall checks passed");
process.exit(failures ? 1 : 0);
