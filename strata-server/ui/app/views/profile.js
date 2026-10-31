// The profiler: any query's EXPLAIN ANALYZE. Run one with its CPU time
// measured, once or many times (each run's time, and the profile of the
// median one); or open the profile of a query that already ran, among the
// slow ones (under a threshold set here while the server runs) or the
// latest. A profile is the flame chart of every step, the operator tree
// with total and self time, CPU time and rows, the segments by thread,
// where the time went, and on a coordinator each shard's call.

import { h, icon, clear, fmt, api, store, toast, card, empty, seg, every, failure, table, pill, toggle, debounce, sqlBlock, copy, bus, shareBar } from "../core.js";
import { timeChart, SERIES } from "../charts.js";
import { visible, last, domain, setZoom } from "../data.js";
import { editor } from "../sqledit.js";
import { tables, cached } from "../catalog.js";
import { profileView, profileFigures, kpi } from "../profile.js";
import { flameGraph, folded, shortName } from "../flame.js";

export default { id: "profile", title: "Profiler", icon: "flame", group: "Workload", keywords: "profile profiler explain analyze flame chart flame graph cpu sampling pprof perf folded speedscope hot functions memory heap allocations allocator leak rss threads context switches segments operators steps slow queries debug latency", render, onParams };

const TABS = [["query", "Profile a query"], ["cpu", "CPU"], ["memory", "Memory"], ["slow", "Slow queries"], ["recent", "Latest queries"]];
const OUTCOMES = { ok: "ok", partial: "warn", error: "bad", timeout: "bad", rejected: "warn", cancelled: "info" };

let route = null;
function onParams(r) { route?.(r.params); }

function render(page, r) {
  const tabs = h("div", { class: "tabs", role: "tablist" });
  const body = h("div");
  page.append(h("div", { class: "page-head" }, h("div", {}, h("h1", {}, "Profiler"), h("p", {}, "Where the time and memory go: every step of a query, on which segment, thread and shard; the server's CPU by function; its heap by where it was allocated; and its threads."))), tabs, body);
  let tab = null, node = null, stop = null, view = null;
  function go(params) {
    const t = params.get("tab") || (params.get("id") ? "recent" : "query");
    // On a coordinator, the CPU and Memory tabs profile any server of its
    // cluster: ?node= names it (a replica's URL), none the coordinator.
    const n = t === "cpu" || t === "memory" ? params.get("node") || "" : "";
    if (t !== tab || n !== node) {
      tab = t; node = n; stop?.();
      clear(tabs, TABS.map(([k, v]) => h("button", { role: "tab", "aria-selected": String(k === t), onclick: () => { location.hash = "#/profile?tab=" + k + (node && (k === "cpu" || k === "memory") ? "&node=" + encodeURIComponent(node) : ""); } }, v)));
      clear(body);
      if (t === "cpu" || t === "memory") body.append(nodePicker(t, n));
      view = t === "query" ? runner(body) : t === "cpu" ? cpu(body, n) : t === "memory" ? memory(body, n) : ended(body, t);
      stop = view.stop;
    }
    view.open?.(params);
  }
  route = go;
  go(r.params);
  return () => { stop?.(); route = null; };
}

// ---------------------------------------------------------------- run one

function runner(body) {
  let runs = store.get("profile.runs", 1);
  let sql = store.get("profile.sql", "");
  let paramsText = store.get("profile.params", "");
  const ed = editor({ value: sql, onChange: (v) => { sql = v; store.set("profile.sql", v); }, onRun: () => run(), catalog: cached, label: "SQL to profile" });
  const go = h("button", { class: "btn primary", title: "Run it with its CPU time measured (Ctrl+Enter)", onclick: () => run() }, icon("flame"), "Profile");
  const params = h("textarea", { class: "input mono", rows: 2, placeholder: '{"q": "laptop"}', style: { width: "100%" }, value: paramsText, oninput: debounce(() => { paramsText = params.value; store.set("profile.params", paramsText); }, 200) });
  const runsSeg = seg([[1, "once"], [5, "5 times"], [20, "20 times"]], runs, (v) => { runs = v; store.set("profile.runs", v); }, { title: "Run it more than once to see how its time varies; the profile shown is the median run's" });
  const strip = h("div");
  const out = h("div");
  body.append(
    card({ body: h("div", { class: "stack" },
      h("div", { class: "wb-ed", style: { height: "150px", border: "1px solid var(--line)", borderRadius: "var(--r-sm)", overflow: "hidden" } }, ed.el),
      h("details", { open: !!paramsText }, h("summary", { class: "small dim" }, "Parameters, as JSON (for :names in the SQL)"), params),
      h("div", { class: "row" }, go, runsSeg, h("span", { class: "sep" }), h("span", { class: "small muted" }, "Each run is a normal query: it takes a slot and shows in Queries.")),
    ), attrs: { style: { marginBottom: "14px" } } }),
    strip, out);
  if (!sql) tables().then(() => { if (!sql && cached()[0]) { const t = cached()[0]; const f = (t.short?.fields || []).find((x) => x.type === "text" && x.indexed !== false); ed.value = f ? `SELECT * FROM ${t.table} ORDER BY bm25(${f.name}, 'search') DESC LIMIT 10` : `SELECT * FROM ${t.table} LIMIT 10`; sql = ed.value; } }).catch(() => {});
  clear(out, empty("flame", "Profile a query", "Write a SELECT and press Profile: the server runs it with its CPU time measured and answers every step it took."));
  let busy = false;
  async function run() {
    if (busy) return;
    const text = (ed.value || "").trim().replace(/;\s*$/, "");
    if (!text) { toast("Write a query first"); return; }
    let p = {};
    if (paramsText.trim()) { try { p = JSON.parse(paramsText); } catch (e) { toast("The parameters are not JSON: " + e.message, "bad"); return; } }
    busy = true; go.disabled = true;
    const all = [];
    try {
      for (let i = 0; i < runs; i++) {
        clear(strip, h("div", { class: "small muted", style: { marginBottom: "10px" } }, `Running ${i + 1} of ${runs}…`));
        const t0 = performance.now();
        const j = await api("/v1/query", { body: { sql: text, params: p, profile: true } });
        j.roundtrip_ms = performance.now() - t0;
        all.push(j);
      }
      show(all, text);
    } catch (e) { clear(strip); clear(out, h("div", { class: "error" }, e.message)); }
    finally { busy = false; go.disabled = false; }
  }
  function show(all, text) {
    const order = all.map((j, i) => [j.took_ms, i]).sort((a, b) => a[0] - b[0]);
    let pick = order[Math.floor((order.length - 1) / 2)][1];
    function draw() {
      const j = all[pick];
      if (all.length > 1) {
        const t = order.map(([v]) => v), max = t[t.length - 1];
        const cpu = all.map((x) => profileFigures({ steps: x.stats?.steps, trace: x.trace }).cpu).filter((x) => x != null).sort((a, b) => a - b);
        clear(strip, card({ title: `${all.length} runs`, sub: "click a run for its profile", attrs: { style: { marginBottom: "14px" } }, body: h("div", { class: "row", style: { gap: "24px", alignItems: "flex-end" } },
          h("div", { class: "prof-runs", role: "list", style: { width: Math.min(560, all.length * 26) + "px" } }, all.map((x, i) => h("i", { role: "listitem", tabindex: 0, class: i === pick ? "sel" : "", title: `run ${i + 1}: ${fmt.ms(x.took_ms)}`, style: { height: Math.max(6, x.took_ms / max * 100) + "%", cursor: "pointer" }, onclick: () => { pick = i; draw(); }, onkeydown: (e) => { if (e.key === "Enter") { pick = i; draw(); } } }))),
          h("div", { class: "row small", style: { gap: "16px" } }, [["min", fmt.ms(t[0])], ["median", fmt.ms(t[Math.floor((t.length - 1) / 2)])], ["max", fmt.ms(max)], ["CPU, median", cpu.length ? fmt.ms(cpu[Math.floor((cpu.length - 1) / 2)] / 1000) : "–"]].map(([k, v]) => h("span", { class: "nowrap" }, h("span", { class: "muted" }, k + " "), h("b", {}, v))))) }));
      } else clear(strip);
      clear(out, h("div", { class: "stack" },
        h("div", { class: "row" }, h("span", { class: "small muted" }, all.length > 1 ? `run ${pick + 1} of ${all.length}` : "one run"), h("span", { class: "sep" }),
          h("a", { class: "btn sm", href: "#/sql?q=" + encodeURIComponent(text) }, icon("sql"), "Open in the workbench"),
          h("button", { class: "btn sm", onclick: () => copy(JSON.stringify({ sql: text, took_ms: j.took_ms, steps: j.stats?.steps, trace: j.trace, plan: j.stats?.plan }, null, 1), "Profile copied as JSON") }, icon("copy"), "Copy as JSON")),
        profileView({ steps: j.stats?.steps, steps_dropped: j.stats?.steps_dropped, trace: j.trace, took_ms: j.took_ms, roundtrip_ms: j.roundtrip_ms, plan: j.stats?.plan, rows: j.rows?.length, filter_docs: j.stats?.filter_docs, scored: j.stats?.scored, profiled: true })));
    }
    draw();
  }
  return {
    stop: () => {},
    open(params) { const q = params.get("sql"); if (q) { ed.value = q; sql = q; store.set("profile.sql", q); run(); } },
  };
}

// ---------------------------------------------------------------- CPU and memory

const SECONDS = [[1, "1 s"], [5, "5 s"], [10, "10 s"], [30, "30 s"]];
const HZ = [[49, "49 Hz"], [99, "99 Hz"], [499, "499 Hz"], [997, "997 Hz"]];
const EVERY = [[64 << 10, "64 KB"], [512 << 10, "512 KB"], [2 << 20, "2 MB"]];
/** An interval in bytes, as its button names it. */
const everyName = (n) => EVERY.find((e) => e[0] === n)?.[1] || fmt.bytes(n);
const CLIENTS = [[1, "1"], [4, "4"], [16, "16"]];

/** The query string that sends a profile to a node of the cluster. */
const onNode = (node) => node ? "?node=" + encodeURIComponent(node) : "";

/**
 * On a coordinator, which server of its cluster the tab profiles: the
 * coordinator itself, or any replica of any shard, which takes the
 * profile itself, asked through the coordinator. Hidden on any other server.
 */
function nodePicker(tab, node) {
  const el = h("div", { class: "row", style: { gap: "10px", flexWrap: "wrap", marginBottom: "14px", display: "none" } });
  api("/v1/cluster").then((c) => {
    if (c.role !== "coordinator") return;
    const opts = [h("option", { value: "", selected: !node }, "this coordinator")];
    let found = !node;
    for (const s of c.shards || []) for (const r of s.replicas || []) {
      const role = r.role && r.role !== "standalone" ? " · " + r.role : "";
      found ||= r.url === node;
      opts.push(h("option", { value: r.url, selected: r.url === node }, `shard ${s.shard} · ${r.url}${role}${r.status === "down" ? " (down)" : ""}`));
    }
    if (!found) opts.push(h("option", { value: node, selected: true }, node));
    const sel = h("select", { class: "select", "aria-label": "Server to profile", onchange: () => { location.hash = "#/profile?tab=" + tab + (sel.value ? "&node=" + encodeURIComponent(sel.value) : ""); } }, opts);
    clear(el, h("span", { class: "small dim" }, "Profile"), sel,
      h("span", { class: "small muted" }, node ? "This server takes the profile itself; the coordinator asks it over gRPC." : "The coordinator's own process: choose a shard's server to profile it."));
    el.style.display = "";
  }).catch(() => {});
  return el;
}

function save(name, text, type) {
  const a = h("a", { href: URL.createObjectURL(new Blob([text], { type })), download: name });
  document.body.append(a); a.click();
  setTimeout(() => { URL.revokeObjectURL(a.href); a.remove(); }, 1000);
}
const stampNow = () => new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");

/**
 * A profile taken over a while, of the whole server or of a query run in a
 * loop: its controls, a countdown while it is taken, and `cfg.show` of
 * what it answers. cfg: {key, button, rate: {label, options, param, def,
 * title}, endpoint, doing(what, rate), empty: [title, sub], busy, show(out,
 * p, req)}.
 */
function sampler(body, cfg) {
  const k = (x) => `profile.${cfg.key}.${x}`;
  let what = store.get(k("what"), "server");
  let seconds = store.get(k("seconds"), 5);
  let rate = store.get(k("rate"), cfg.rate.def);
  let clients = store.get(k("clients"), 1);
  let sql = store.get(k("sql"), store.get("profile.cpu.sql", store.get("profile.sql", "")));
  const ed = editor({ value: sql, onChange: (v) => { sql = v; store.set(k("sql"), v); }, onRun: () => take(), catalog: cached, label: "SQL to run while profiling" });
  const qbox = h("div", { class: "stack" },
    h("div", { class: "wb-ed", style: { height: "110px", border: "1px solid var(--line)", borderRadius: "var(--r-sm)", overflow: "hidden" } }, ed.el),
    h("div", { class: "row small dim" }, "Run by", seg(CLIENTS, clients, (v) => { clients = v; store.set(k("clients"), v); }, { title: "How many callers run it at once: 1 shows one query's work, more shows the server under load" }), "callers at once, again and again until the profile ends. Its runs are not kept among the latest queries."));
  const showQ = () => { qbox.style.display = what === "query" ? "" : "none"; };
  const go = h("button", { class: "btn primary", onclick: () => take() }, icon("flame"), cfg.button);
  const out = h("div");
  body.append(
    card({ body: h("div", { class: "stack" },
      h("div", { class: "row", style: { flexWrap: "wrap", gap: "14px" } },
        seg([["server", "the whole server"], ["query", "a query in a loop"]], what, (v) => { what = v; store.set(k("what"), v); showQ(); }, { title: "Profile whatever the server is doing, or one query run again and again" }),
        h("span", { class: "row nw small dim", style: { gap: "6px" } }, "for", seg(SECONDS, seconds, (v) => { seconds = v; store.set(k("seconds"), v); })),
        h("span", { class: "row nw small dim", style: { gap: "6px" }, title: cfg.rate.title }, cfg.rate.label, seg(cfg.rate.options, rate, (v) => { rate = v; store.set(k("rate"), v); }))),
      qbox,
      h("div", { class: "row" }, go, h("span", { class: "sep" }), h("span", { class: "small muted" }, cfg.note))),
    attrs: { style: { marginBottom: "14px" } } }),
    out);
  showQ();
  if (!sql) tables().then(() => { if (!sql && cached()[0]) { const t = cached()[0]; const f = (t.short?.fields || []).find((x) => x.type === "text" && x.indexed !== false); ed.value = f ? `SELECT * FROM ${t.table} ORDER BY bm25(${f.name}, 'search') DESC LIMIT 10` : `SELECT * FROM ${t.table} LIMIT 10`; sql = ed.value; } }).catch(() => {});
  clear(out, empty("flame", ...cfg.empty));
  let busy = false, tick = null, gone = false;
  async function take() {
    if (busy) return;
    const req = { seconds, [cfg.rate.param]: rate };
    if (what === "query") {
      const text = (ed.value || "").trim().replace(/;\s*$/, "");
      if (!text) { toast("Write a query first"); return; }
      req.sql = text; req.clients = clients;
    }
    busy = true; go.disabled = true;
    const t0 = performance.now();
    const bar = h("i", { style: { width: "0%" } });
    const status = h("div", { class: "small muted" }, `${seconds} s left`);
    clear(out, card({ body: h("div", { class: "stack", style: { alignItems: "center", padding: "26px 0" } },
      h("div", { class: "small dim" }, cfg.doing(what, rate)),
      h("div", { class: "meter", style: { width: "min(420px, 90%)" } }, bar), status) }));
    tick = setInterval(() => { const el = (performance.now() - t0) / 1000; bar.style.width = Math.min(100, el / seconds * 100) + "%"; status.textContent = el < seconds ? `${Math.ceil(seconds - el)} s left` : "Reading the frames' names…"; }, 200);
    try {
      const p = await api(cfg.endpoint + onNode(cfg.node), { body: req });
      if (gone) return;
      cfg.show(out, p, req);
    } catch (e) {
      if (gone) return;
      clear(out, e.status === 409 ? empty("clock", "A profile is being taken", cfg.busy) : e.status === 501 ? empty("info", "Not available here", e.message) : h("div", { class: "error" }, e.message));
    } finally { clearInterval(tick); busy = false; go.disabled = false; }
  }
  return { stop: () => { gone = true; clearInterval(tick); } };
}

/** The head of a profile's answer: what it was of, downloads, the query. */
function profileHead(p, req, what, downloads) {
  return [
    h("div", { class: "row" }, h("span", { class: "small muted" }, `${req.sql ? "a query in a loop" : "the whole server"}${p.node ? " on " + p.node : ""} · ${fmt.n(p.seconds)} s ${what} · ${new Date().toLocaleTimeString()}`), h("span", { class: "sep" }),
      downloads,
      req.sql ? h("a", { class: "btn sm", href: "#/profile?tab=query&sql=" + encodeURIComponent(req.sql), title: "Its steps, once" }, icon("flame"), "Profile its steps") : null),
    req.sql ? sqlBlock(req.sql) : null,
    p.runs?.error ? h("div", { class: "error" }, "A run failed, so the loop stopped: " + p.runs.error) : null,
  ];
}

const runsKpis = (r, p) => [
  kpi("Runs", fmt.n(r.count), r.count ? `${fmt.n(Math.round(r.count / p.seconds))} a second${r.clients > 1 ? `, ${r.clients} callers` : ""}` : null),
  kpi("Median run", r.median_ms != null ? fmt.ms(r.median_ms) : "–", r.p99_ms != null ? `p99 ${fmt.ms(r.p99_ms)} · max ${fmt.ms(r.max_ms)}` : null),
];

/** The frame most of a value was in, itself: [name, value]. */
function hottest(p, value = (s) => s.samples) {
  const self = new Map();
  for (const s of p.stacks) { const f = s.frames[s.frames.length - 1]; if (f != null) self.set(f, (self.get(f) || 0) + value(s)); }
  const best = [...self].sort((x, y) => y[1] - x[1])[0];
  return best && best[1] > 0 ? [p.names[best[0]], best[1]] : null;
}

/**
 * A CPU profile: every thread of the server sampled for a while, what it
 * was running, as a flame graph and by function. Of the whole server
 * (whatever its clients send), or of one query run again and again.
 */
function cpu(body, node) {
  return sampler(body, {
    key: "cpu", button: "Take a CPU profile", endpoint: "/v1/profile/cpu", node,
    rate: { label: "at", options: HZ, param: "hz", def: 99, title: "Samples a second of each thread's CPU time: more is finer, and slows the threads it interrupts more" },
    note: "One profile at a time. The stacks are sampled with SIGPROF; frames are named from the binary's symbols.",
    doing: (what, hz) => what === "query" ? `Running the query in a loop and sampling every thread at ${hz} Hz…` : `Sampling every thread at ${hz} Hz…`,
    empty: ["Sample the server's CPU", "Take a profile of the whole server while it serves its clients, or of a query run in a loop: every thread's stacks, as a flame graph and the functions that took the most time."],
    busy: "One at a time: the timer is the process's. Try again when it ends.",
    show: showCpu,
  });
}

function showCpu(out, p, req) {
  const total = p.stacks.reduce((a, s) => a + s.samples, 0);
  // A sample stands for its share of the process's measured CPU time:
  // the system's timer delivers fewer than asked when the process is busy.
  const per = p.samples && p.cpu_seconds ? p.cpu_seconds * 1000 / p.samples : 1000 / p.hz;
  const unit = (n) => fmt.ms(n * per);
  const stamp = stampNow();
  const fg = flameGraph(p, { unit });
  const r = p.runs;
  const top = p.threads[0];
  const few = p.rate && p.rate < p.hz * 0.6;
  const hot = hottest(p);
  clear(out, h("div", { class: "stack" },
    ...profileHead(p, req, `at ${p.hz} Hz`, [
      h("button", { class: "btn sm", title: "Folded stacks (thread;root;…;leaf samples) for flamegraph.pl, inferno or speedscope", onclick: () => save(`strata-cpu-${stamp}.folded`, folded(p), "text/plain") }, icon("download"), "Folded"),
      h("button", { class: "btn sm", onclick: () => save(`strata-cpu-${stamp}.json`, JSON.stringify(p), "application/json") }, icon("download"), "JSON")]),
    h("div", { class: "grid g6 prof-kpis" },
      kpi("CPU time", fmt.ms(p.cpu_seconds * 1000), `of every thread, in ${fmt.n(p.seconds)} s`, "The process's CPU time (user and system) while the profile was taken, from the system"),
      kpi("Cores busy", p.cores.toFixed(2), `of ${fmt.n(p.seconds)} s`, "CPU seconds over wall seconds: the cores the process kept busy on average"),
      kpi("Samples", fmt.n(p.samples), `${fmt.n(Math.round(p.rate || 0))} a CPU second`, `${p.hz} asked for a CPU second; each sample stands for ${fmt.ms(per)} of CPU time`),
      kpi("Threads", fmt.n(p.threads.reduce((a, t) => a + t.count, 0)), top ? `${top.name} ${fmt.pct(top.samples / Math.max(1, total), 0)}` : null, "Threads sampled at least once; the busiest pool"),
      ...(r ? runsKpis(r, p) : [
        kpi("Stacks", fmt.n(p.stacks.length), `${fmt.n(p.names.length)} functions`),
        hot ? kpi("Hottest function", h("span", { class: "mono", style: { fontSize: "15px" } }, shortName(hot[0])), `${fmt.pct(hot[1] / Math.max(1, total), 1)} of the samples in itself`, hot[0]) : kpi("Hottest function", "–")])),
    few ? h("div", { class: "small muted" }, `The system delivered ${fmt.n(Math.round(p.rate))} samples a CPU second of the ${p.hz} asked: its timer gives the whole process at most one a tick (250 a second on many Linux kernels). Each sample is weighed by the measured CPU time, so the times are right; fewer samples make small functions noisier, so sample for longer.`) : null,
    (p.blocked || []).includes("libc") ? h("div", { class: "small muted" }, "Samples inside the C library (malloc, memcpy, system calls) are not taken on this system: unwinding from there could deadlock before glibc 2.35. Their CPU time is spread over the rest.") : null,
    !total ? empty("info", "No samples", "No thread used the CPU while the profile was taken. Sample for longer, or while the server is busy.") : null,
    total ? card({ title: "Flame graph", sub: "each frame as wide as its CPU time, what it called under it; click one to zoom in", body: fg.graph }) : null,
    total ? card({ title: "Functions", sub: "self: CPU time in the function itself · total: with what it called · click one to find it in the graph", body: fg.functions, flush: true }) : null));
}

/**
 * Memory: the heap in use and its peak, how fast the server allocates,
 * its resident memory, over time; an allocation profile (where the bytes
 * are allocated, and which are still held); and the threads.
 */
function memory(body, node) {
  const figures = h("div", { class: "grid g6 prof-kpis", style: { marginBottom: "14px" } });
  const brush = (a, b) => setZoom(a == null ? null : [a, b]);
  // Another node's charts are drawn from its figures read every 2 s since
  // the tab opened; this server's from its series.
  const own = node ? { label: "memory" } : { label: "memory", domain, onBrush: brush };
  const read = [];
  const heapChart = timeChart({ height: 190, ...own, fmt: (v) => fmt.bytes(v), series: [
    { name: "heap in use", get: (p) => p.heap_bytes, area: true },
    { name: "resident", get: (p) => p.rss_bytes, color: SERIES[2] },
    { name: "heap peak", get: (p) => p.heap_peak_bytes, color: SERIES[6], dash: "4 3" },
  ] });
  const rateChart = timeChart({ height: 190, ...own, label: "allocations", fmt: (v) => fmt.bytes(v) + "/s", series: [
    { name: "allocated a second", get: (p) => p.alloc_bytes_s, area: true, color: SERIES[1] },
  ] });
  const charts = h("div", { class: "grid g2", style: { marginBottom: "14px" } },
    card({ title: "Memory", sub: node ? "the heap in use, its peak and the resident memory, since this page opened" : "the heap in use, its peak and the resident memory", body: heapChart }),
    card({ title: "Allocation rate", sub: node ? "bytes allocated a second, every thread, since this page opened" : "bytes allocated a second, every thread", body: rateChart }));
  const prof = h("div", { style: { marginBottom: "14px" } });
  const threadsBox = h("div");
  body.append(figures, charts, prof, threadsBox);
  const drawCharts = () => { const pts = node ? read : visible(); heapChart.update(pts); rateChart.update(pts); };
  drawCharts();
  const offPoints = node ? null : bus.on("points", drawCharts);
  const offRange = node ? null : bus.on("range", drawCharts);
  async function drawFigures() {
    try {
      const m = await api("/v1/profile/memory" + onNode(node));
      const hp = m.heap || {};
      let pt = last();
      if (node) {
        // The rates from two reads of the node's counts.
        const b = read[read.length - 1], secs = b && m.at_ms > b.t ? (m.at_ms - b.t) / 1000 : null;
        pt = { t: m.at_ms, heap_bytes: hp.in_use_bytes, heap_peak_bytes: hp.peak_bytes, rss_bytes: m.rss_bytes, allocated: hp.allocated_bytes, allocs: hp.allocs,
          alloc_bytes_s: secs ? Math.max(0, (hp.allocated_bytes - b.allocated) / secs) : null, allocs_s: secs ? Math.max(0, (hp.allocs - b.allocs) / secs) : null };
        read.push(pt);
        if (read.length > 900) read.shift();
        drawCharts();
      }
      clear(figures,
        kpi("Heap in use", hp.installed ? fmt.bytes(hp.in_use_bytes) : "–", hp.installed ? `peak ${fmt.bytes(hp.peak_bytes)}` : "not counted in this binary", "Bytes allocated and not yet freed, counted at every allocation and free"),
        kpi("Allocating", pt?.alloc_bytes_s != null ? fmt.bytes(pt.alloc_bytes_s) + "/s" : "–", pt?.allocs_s != null ? `${fmt.si(pt.allocs_s)} allocations a second` : null, "Over the latest 2 s"),
        kpi("Allocated", hp.installed ? fmt.bytes(hp.allocated_bytes) : "–", hp.installed ? `${fmt.si(hp.allocs)} allocations since it started` : null),
        kpi("Resident", fmt.bytes(m.rss_bytes), m.peak_rss_bytes ? `peak ${fmt.bytes(m.peak_rss_bytes)}` : null, "The process's memory in RAM: the heap, the mapped database files' pages read, the threads' stacks"),
        kpi("Query memory", m.query_memory_bytes != null ? fmt.bytes(m.query_memory_bytes) : "–", m.query_memory_bytes != null ? "held by running queries" : "no budget set"),
        kpi("Threads", fmt.n(m.threads), "in the process"));
    } catch (e) { clear(figures, failure(e, "The memory figures")); }
  }
  const stopFigures = every(2000, drawFigures);
  const heapSampler = sampler(prof, {
    key: "heap", button: "Take an allocation profile", endpoint: "/v1/profile/heap", node,
    rate: { label: "a sample every", options: EVERY, param: "every", def: 2 << 20, title: "About one allocation is sampled in every this many bytes a thread allocates: smaller is finer and slows the server more (each sample walks a stack)" },
    note: "One profile at a time. Each sampled allocation keeps its stack; one freed before the end is marked so.",
    doing: (what, every) => what === "query" ? `Running the query in a loop and sampling an allocation every ${everyName(every)}…` : `Sampling an allocation every ${everyName(every)} each thread allocates…`,
    empty: ["Where is memory allocated?", "Take an allocation profile of the whole server or of a query in a loop: the stacks that allocated the most bytes, and those still holding memory when it ends."],
    busy: "One allocation profile at a time. Try again when it ends.",
    show: showHeap,
  });
  // The threads, their CPU over the latest 2 s from two reads.
  let prev = null, byPool = store.get("profile.threads.pool", true);
  const tbl = table([
    { key: "name", label: "thread", render: (t) => h("span", { class: "row nw", style: { gap: "6px" } }, h("span", { class: "mono" }, t.name), t.n > 1 ? h("span", { class: "badge" }, "×" + t.n) : null) },
    { key: "tid", label: "id", num: true, render: (t) => t.n > 1 ? h("span", { class: "muted" }, "–") : String(t.tid) },
    { key: "state", label: "state", render: (t) => t.state },
    { key: "cpu_pct", label: "CPU now", num: true, sort: (t) => t.cpu_pct ?? -1, render: (t) => t.cpu_pct == null ? "–" : shareBar(Math.min(1, t.cpu_pct), t.cpu_pct > 0.8 ? "var(--crit)" : "var(--s1)") },
    { key: "cpu_s", label: "CPU time", num: true, render: (t) => fmt.ms(t.cpu_s * 1000) },
    { key: "switches", label: "waits a second", num: true, title: "Times a second it gave the CPU up: to wait on a lock, a read, a timer", sort: (t) => t.switches ?? -1, render: (t) => t.switches == null ? "–" : fmt.si(t.switches) },
    { key: "forced", label: "preempted a second", num: true, title: "Times a second the system took the CPU from it", sort: (t) => t.forced ?? -1, render: (t) => t.forced == null ? "–" : fmt.si(t.forced) },
  ], { sortKey: "cpu_pct", desc: true, compact: true, maxHeight: "460px", empty: empty("info", "No threads listed", "The system lists a process's threads on Linux.") });
  const poolSeg = seg([[true, "by pool"], [false, "each thread"]], byPool, (v) => { byPool = v; store.set("profile.threads.pool", v); drawThreads(); });
  const threadNote = h("span", { class: "small muted" });
  threadsBox.append(card({ title: "Threads", sub: "every thread of the process, from the system; CPU now over the latest 2 s", tools: h("div", { class: "row", style: { gap: "10px" } }, threadNote, poolSeg), body: tbl, flush: true }));
  let lastList = null;
  async function readThreads() {
    try {
      const j = await api("/v1/profile/threads" + onNode(node));
      const before = prev ? new Map(prev.threads.map((t) => [t.tid, t])) : null;
      const secs = prev ? Math.max(0.001, (j.at_ms - prev.at_ms) / 1000) : null;
      lastList = j.threads.map((t) => {
        const b = before?.get(t.tid);
        return { ...t, cpu_pct: b ? Math.max(0, (t.cpu_s - b.cpu_s) / secs) : null, switches: b ? Math.max(0, (t.voluntary - b.voluntary) / secs) : null, forced: b ? Math.max(0, (t.involuntary - b.involuntary) / secs) : null };
      });
      prev = j;
      drawThreads();
    } catch (e) { clear(threadNote, e.message); }
  }
  function drawThreads() {
    if (!lastList) return;
    const pool = (name) => name.replace(/[-_ #]?\d+$/, "") || name;
    let rows = lastList.map((t) => ({ ...t, n: 1 }));
    if (byPool) {
      const by = new Map();
      for (const t of lastList) {
        const k = pool(t.name);
        const g = by.get(k) || { name: k, tid: t.tid, n: 0, states: new Map(), cpu_s: 0, cpu_pct: null, switches: null, forced: null };
        g.n++; g.cpu_s += t.cpu_s;
        g.states.set(t.state, (g.states.get(t.state) || 0) + 1);
        for (const f of ["cpu_pct", "switches", "forced"]) if (t[f] != null) g[f] = (g[f] || 0) + t[f];
        by.set(k, g);
      }
      rows = [...by.values()].map((g) => ({ ...g, state: [...g.states].map(([s, n]) => (g.n > 1 ? `${n} ` : "") + s).join(", ") }));
    }
    clear(threadNote, `${fmt.plural(lastList.length, "thread")} · R running, S sleeping, D waiting on the disk`);
    tbl.update(rows);
  }
  const stopThreads = every(2000, readThreads);
  return { stop: () => { stopFigures(); stopThreads(); heapSampler.stop(); offPoints?.(); offRange?.(); } };
}

/** What an allocation profile shows, by stack: bytes allocated, bytes still in use at its end, or allocations. */
const HEAP_VALUES = [["bytes", "bytes allocated"], ["in_use", "still in use at the end"], ["allocs", "allocations"]];

function showHeap(out, p, req) {
  let value = store.get("profile.heap.value", "bytes");
  const stamp = stampNow();
  const sum = (f) => p.stacks.reduce((a, s) => a + s[f], 0);
  const est = { bytes: sum("bytes"), in_use: sum("in_use"), allocs: sum("allocs") };
  const r = p.runs;
  const graph = h("div");
  const grow = p.in_use_end - p.in_use_start;
  const valueSeg = seg(HEAP_VALUES, value, (v) => { value = v; store.set("profile.heap.value", v); draw(); });
  function draw() {
    const stacks = p.stacks.map((s) => ({ ...s, samples: s[value] })).filter((s) => s.samples > 0);
    const per = p.threads.map(() => 0);
    for (const s of stacks) per[s.thread] += s.samples;
    const view = { ...p, stacks, threads: p.threads.map((t, i) => ({ ...t, samples: per[i] })) };
    const unit = value === "allocs" ? (n) => fmt.si(n) + " allocations" : (n) => fmt.bytes(n);
    const hot = hottest(view);
    if (!view.stacks.length) { clear(graph, empty("info", value === "in_use" ? "Nothing sampled is still in use" : "No allocations sampled", value === "in_use" ? "Every sampled allocation was freed before the profile ended." : "Profile for longer, or with a smaller interval.")); return; }
    const fg = flameGraph(view, { unit });
    clear(graph, h("div", { class: "stack" },
      hot ? h("div", { class: "small muted" }, "Most, itself: ", h("span", { class: "mono" }, shortName(hot[0])), ` · ${unit(hot[1])}`) : null,
      card({ title: "Flame graph", sub: `each frame as wide as the ${HEAP_VALUES.find((x) => x[0] === value)[1]} under it; click one to zoom in`, tools: valueSeg, body: fg.graph }),
      card({ title: "Functions", sub: "self: allocated by the function itself · total: with what it called", body: fg.functions, flush: true })));
  }
  clear(out, h("div", { class: "stack" },
    ...profileHead(p, req, `a sample every ${everyName(p.every)}`, [
      h("button", { class: "btn sm", title: "Folded stacks of the bytes allocated, for flamegraph.pl, inferno or speedscope", onclick: () => save(`strata-heap-${stamp}.folded`, folded({ ...p, stacks: p.stacks.map((s) => ({ ...s, samples: s.bytes })) }), "text/plain") }, icon("download"), "Folded"),
      h("button", { class: "btn sm", onclick: () => save(`strata-heap-${stamp}.json`, JSON.stringify(p), "application/json") }, icon("download"), "JSON")]),
    h("div", { class: "grid g6 prof-kpis" },
      kpi("Allocated", fmt.bytes(p.allocated_bytes), `${fmt.bytes(p.allocated_bytes / Math.max(0.001, p.seconds))}/s, counted`, "Bytes every thread allocated while the profile was taken, counted at each allocation"),
      kpi("Allocations", fmt.si(p.allocs), `${fmt.si(p.allocs / Math.max(0.001, p.seconds))} a second`),
      kpi("Heap", fmt.bytes(p.in_use_end), `${grow >= 0 ? "+" : "−"}${fmt.bytes(Math.abs(grow))} while profiled`, "The heap in use at the end, and how it changed from the start"),
      kpi("Still held", fmt.bytes(est.in_use), est.bytes ? `${fmt.pct(est.in_use / est.bytes, 1)} of the sampled bytes` : null, "Of the bytes allocated while profiled, those still in use at the end (estimated from the samples)"),
      ...(r ? runsKpis(r, p) : [
        kpi("Samples", fmt.n(p.samples), p.untracked ? `${fmt.n(p.untracked)} not watched` : `${fmt.n(p.stacks.length)} stacks`, "Allocations sampled, each standing for its share of the bytes"),
        kpi("Per allocation", p.allocs ? fmt.bytes(p.allocated_bytes / p.allocs) : "–", "on average")])),
    graph));
  draw();
}

// ---------------------------------------------------------------- ended ones

function ended(body, which) {
  const settings = h("div", { class: "prof-set" });
  const note = h("div", { class: "small muted" });
  const picked = h("div", { style: { marginTop: "14px" } });
  let sel = null;
  const tbl = table([
    { key: "ts_ms", label: "time", num: true, render: (q) => h("span", { class: "nowrap", title: fmt.datetime(q.ts_ms) }, fmt.time(q.ts_ms)) },
    { key: "outcome", label: "outcome", render: (q) => pill(OUTCOMES[q.outcome] || "info", q.outcome) },
    { key: "ms", label: "took", num: true, render: (q) => h("b", {}, fmt.ms(q.ms)) },
    { key: "cpu_ms", label: "CPU", num: true, sort: (q) => q.cpu_ms ?? -1, render: (q) => q.cpu_ms != null ? fmt.ms(q.cpu_ms) : h("span", { class: "muted", title: "Not measured: run it with profile on, or turn on CPU time for every query" }, "–") },
    { key: "steps_n", label: "steps", num: true, render: (q) => fmt.n(q.steps_n || 0) },
    { key: "rows", label: "rows", num: true, render: (q) => fmt.n(q.rows) },
    { key: "kind", label: "kind", render: (q) => h("span", { class: "badge mono" }, q.kind) },
    { key: "sql", label: "SQL", cls: "sql", render: (q) => h("span", { title: q.sql }, q.sql) },
  ], { sortKey: which === "slow" ? "ms" : "ts_ms", desc: true, max: 400, maxHeight: "340px", onRow: (q) => { location.hash = `#/profile?tab=${which}&id=${q.id}`; }, rowClass: (q) => q.id === sel ? "sel" : "",
    empty: empty("flame", which === "slow" ? "No slow queries" : "No queries yet", which === "slow" ? "Set a threshold above: every query that takes longer keeps its profile here." : "Run a query and its profile shows here.") });
  body.append(
    which === "slow" ? card({ title: "What is kept", sub: "set while the server runs; a restart goes back to its flags", body: settings, attrs: { style: { marginBottom: "14px" } } }) : null,
    h("div", { class: "row", style: { marginBottom: "10px" } }, note),
    card({ body: tbl, flush: true }),
    picked);
  if (which === "slow") drawSettings();
  async function drawSettings() {
    try {
      const s = await api("/v1/profile/settings");
      const inp = h("input", { class: "input sm num", type: "number", min: 0, step: "any", value: s.slow_ms ?? "", placeholder: "off" });
      const save = async (body) => {
        try { const r = await api("/v1/profile/settings", { method: "PUT", body }); toast(r.slow_ms == null ? "No query is kept as slow" : `Queries over ${fmt.ms(r.slow_ms)} are kept as slow`, "ok"); drawSettings(); load(); }
        catch (e) { toast(e.message, "bad"); }
      };
      clear(settings,
        h("label", { class: "row nw small dim", style: { gap: "8px" } }, "Keep queries slower than", inp, "ms"),
        h("button", { class: "btn sm primary", onclick: () => save({ slow_ms: inp.value === "" ? null : Number(inp.value) }) }, "Set"),
        h("button", { class: "btn sm", title: "Keep none as slow", onclick: () => save({ slow_ms: null }) }, "Off"),
        s.started_slow_ms !== s.slow_ms ? h("span", { class: "small muted" }, `started with ${s.started_slow_ms == null ? "none" : fmt.ms(s.started_slow_ms)}`) : null,
        h("span", { class: "sep" }),
        toggle(!!s.cpu_all, (v) => save({ cpu_all: v }), "Measure CPU time for every query (about 0.3 µs a step)"));
    } catch (e) { clear(settings, failure(e, "The profiling settings")); }
  }
  let all = [];
  async function load() {
    try {
      const j = await api(`/v1/queries/${which}?limit=500`);
      all = j.queries;
      tbl.update(all);
      const ms = all.map((q) => q.ms).sort((a, b) => a - b);
      clear(note, all.length ? `${fmt.plural(all.length, "query", "queries")} · median ${fmt.ms(ms[Math.floor(ms.length / 2)])} · slowest ${fmt.ms(ms[ms.length - 1])} · ${all.filter((q) => q.cpu_ms != null).length} with CPU time · click one for its profile` : "");
    } catch (e) { clear(body, failure(e, which === "slow" ? "The slow queries" : "The latest queries")); stopper(); }
  }
  async function open(id) {
    sel = id; tbl.update(all);
    clear(picked, h("div", { class: "small muted" }, "Reading its profile…"));
    try {
      const q = await api("/v1/queries/finished/" + id);
      clear(picked, h("div", { class: "stack" },
        h("div", { class: "row" }, pill(OUTCOMES[q.outcome] || "info", q.outcome), h("b", {}, `${fmt.ms(q.ms)} · ${q.kind}`), h("span", { class: "small muted" }, `${fmt.datetime(q.ts_ms)}${q.shard ? " · for a coordinator" : ""}`), h("span", { class: "sep" }),
          h("a", { class: "btn sm", href: "#/profile?tab=query&sql=" + encodeURIComponent(q.sql), title: "Run it again with its CPU time measured" }, icon("flame"), "Profile it again"),
          h("a", { class: "btn sm", href: "#/sql?q=" + encodeURIComponent(q.sql) }, icon("sql"), "Open in the workbench")),
        sqlBlock(q.sql),
        q.error ? h("div", { class: "error" }, q.error) : null,
        profileView({ ...q, took_ms: q.ms })));
    } catch (e) { clear(picked, e.status === 404 ? empty("clock", "That query's profile is gone", "The server keeps the latest 500 queries and the slowest; it ran too long ago.") : h("div", { class: "error" }, e.message)); }
  }
  const stopper = every(5000, load);
  return { stop: stopper, open(params) { const id = Number(params.get("id")); if (id && id !== sel) open(id); } };
}
