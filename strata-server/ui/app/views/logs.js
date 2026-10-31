// Logs: what the servers logged and the queries they answered, kept in
// Strata (each server's log store, `<db>.logs`) and searched with SQL. On a
// coordinator every search reads every server's store and merges their
// answers. Lines by time, level, text, request, server, module and route,
// with their rate; queries, the failed and slow ones and their statements,
// each with its trace; any SQL over both tables; and the latest lines
// followed live from the server's memory, which needs no store.

import { h, s, icon, clear, fmt, card, api, every, toast, seg, debounce, failure, copy, showTip, hideTip, store, drawer, table, pill, empty, sqlBlock, ApiError } from "../core.js";
import { state } from "../data.js";
import { lit } from "../catalog.js";
import { editor } from "../sqledit.js";
import { grid, exportMenu, resultChart, answerStats } from "../results.js";
import { queryDrawer } from "./queries.js";

export default { id: "logs", title: "Logs", icon: "logs", group: "Workload", keywords: "log lines tail errors warnings request id filter level debug search sql store history queries slow failed node server", render, onParams };

const LV = ["error", "warn", "info", "debug", "trace"];
const LCOLOR = { error: "var(--crit)", warn: "var(--s4)", info: "var(--s1)", debug: "var(--muted)", trace: "var(--line-2)" };
const OUTCOMES = { ok: "ok", partial: "warn", error: "bad", timeout: "bad", rejected: "warn", cancelled: "info" };
const OCOLOR = { ok: "var(--s1)", partial: "var(--s4)", error: "var(--crit)", timeout: "var(--s8)", rejected: "var(--s2)", cancelled: "var(--s7)" };
const PRESETS = ["error", "warn", "info", "debug", "info,strata_server=debug", "info,strata=debug", "trace"];
const RANGES = [[15, "15m"], [60, "1h"], [360, "6h"], [1440, "24h"], [10080, "7d"]];
const TABS = [["lines", "Lines"], ["queries", "Queries"], ["sql", "SQL"], ["live", "Live tail"]];
const PAGE = 200;
const LINE_COLS = "ts, node, level, target, method, route, message, fields, request_id, status, ms, id";
const QUERY_COLS = "ts, node, request_id, kind, outcome, shard, sql, fingerprint, ms, n_rows, error, detail, id";

const iso = (ms) => new Date(ms).toISOString();
const msOf = (ts) => typeof ts === "string" ? Date.parse(ts) : (ts || 0) / 1000;
const clock = (ms) => fmt.time(ms) + "." + String(Math.floor(ms) % 1000).padStart(3, "0");
const day = (ms) => new Date(ms).toLocaleDateString(undefined, { weekday: "short", month: "short", day: "numeric" });
const period = (sec) => sec % 86400 === 0 ? fmt.plural(sec / 86400, "day") : sec % 3600 === 0 ? fmt.plural(sec / 3600, "hour") : fmt.dur(sec);
const objects = (j) => (j.rows || []).map((r) => Object.fromEntries(j.columns.map((c, i) => [c, r[i]])));
function parseJson(t) { if (!t) return null; try { return JSON.parse(t); } catch { return t; } }

/** A query over the log store: every server's on a coordinator (scope "cluster"), or this one's. */
const ask = (sql, scope) => api("/v1/logs/query", { body: scope ? { sql, scope } : { sql } });

let reroute = null;
function onParams(r) { reroute?.(r.params); }

function render(page, r) {
  const filt = h("input", { class: "input sm mono", value: state.system?.logs?.filter || "", style: { width: "230px" }, list: "log-presets", title: "What this server logs from now on: a level, or target=level pairs, as --log" });
  const apply = h("button", { class: "btn sm", onclick: async () => {
    try { const j = await api("/v1/logs/filter", { method: "PUT", body: { filter: filt.value.trim() } }); filt.value = j.filter; toast(`The server now logs: ${j.filter}`, "ok"); }
    catch (e) { toast(e instanceof ApiError && e.status === 403 ? "Changing the log level needs an admin key" : e.message, "bad"); }
  } }, icon("check"), "Apply");
  const strip = h("div", { class: "lg-store" });
  const banner = h("div");
  const tabs = h("div", { class: "tabs", role: "tablist" });
  const body = h("div");
  page.append(
    h("div", { class: "page-head" },
      h("div", {}, h("h1", {}, "Logs"), h("p", {}, "Every line the servers logged and every query they answered, kept in Strata and searched with SQL. Click a line for its fields, a request id for the whole request.")),
      h("div", { class: "tools" }, h("span", { class: "small muted" }, "The server logs"), filt, h("datalist", { id: "log-presets" }, PRESETS.map((p) => h("option", { value: p }))), apply)),
    strip, banner, tabs, body);

  let info = null, tab = null, stop = null;
  const ctx = {
    coord: false,
    schemas: [],
    /** Names the servers that did not answer a search, under the strip. */
    note(nodes) {
      const bad = (nodes || []).filter((n) => n.error);
      clear(banner, bad.length ? h("div", { class: "banner warn" }, icon("warn"), h("div", { class: "grow" },
        h("b", {}, `${fmt.plural(bad.length, "server")} did not answer, so ${bad.length === 1 ? "its" : "their"} lines are missing here`),
        bad.map((n) => h("div", { class: "small mono" }, `${n.node || n.url}${n.shard != null ? ` (shard ${n.shard})` : ""}: ${n.error}`)))) : null);
    },
    go(t, params = {}) { location.hash = "#/logs?" + new URLSearchParams({ tab: t, ...params }); },
  };

  function show(t, params) {
    stop?.();
    tab = t;
    clear(banner);
    clear(tabs, TABS.filter(([k]) => k === "live" || ctx.has).map(([k, v]) => h("button", { role: "tab", "aria-selected": String(k === t), onclick: () => ctx.go(k) }, v)));
    clear(body);
    const init = Object.fromEntries(params || []);
    stop = t === "live" || !ctx.has ? live(body, init, filt) : t === "queries" ? queries(body, ctx, init) : t === "sql" ? sqlTab(body, ctx, init) : lines(body, ctx, init);
  }
  reroute = (params) => show(params.get("tab") || (ctx.has ? "lines" : "live"), params);

  api("/v1/logs?limit=1&level=error").then((j) => { if (j.filter && document.activeElement !== filt) filt.value = j.filter; }).catch(() => {});
  api("/v1/logs/store").then((j) => {
    info = j;
    ctx.coord = !!j.nodes;
    ctx.schemas = (j.schemas || []).map((x) => ({ table: x.table, short: x.short }));
    const kept = [j.store, ...(j.nodes || []).map((n) => n.store)].filter(Boolean);
    ctx.has = kept.length > 0;
    drawStrip(strip, j, kept);
    reroute(r.params);
  }).catch((e) => {
    if (e instanceof ApiError && (e.status === 401 || e.status === 403)) { clear(body, failure(e, "The logs")); return; }
    ctx.has = false;
    reroute(r.params);
  });
  return () => { stop?.(); reroute = null; };
}

/** What the stores hold, how big they are and how long they keep it; on a coordinator, each server's. */
function drawStrip(strip, j, kept) {
  if (!kept.length) {
    clear(strip, h("div", { class: "notice grow" }, j.nodes
      ? "No server of this cluster keeps a log store (each was started with --log-store off): only the live tail of this coordinator's latest lines is here."
      : "This server keeps no log store (it was started with --log-store off): only its latest 5,000 lines, in memory, are here."));
    return;
  }
  const sum = (f) => kept.reduce((a, x) => a + (f(x) || 0), 0);
  const lines = sum((x) => x.tables?.logs?.rows), queries = sum((x) => x.tables?.queries?.rows), bytes = sum((x) => x.bytes);
  const dropped = sum((x) => (x.dropped?.lines || 0) + (x.dropped?.failed || 0)), sampled = sum((x) => x.sampled_out);
  const errors = kept.filter((x) => x.last_error);
  const servers = j.nodes ? j.nodes.length + 1 : 1;
  const one = kept[0];
  clear(strip,
    h("span", { class: "row nw", style: { gap: "6px" } }, icon("db"), h("b", {}, "Kept in Strata")),
    j.nodes ? h("span", {}, h("b", {}, `${kept.length} of ${servers}`), " servers keep their logs") : h("span", { class: "mono", title: "the log store's file" }, one.path),
    h("span", {}, h("b", {}, fmt.n(lines)), " lines · ", h("b", {}, fmt.n(queries)), " queries"),
    h("span", { title: "what the stores take, and the most each keeps" }, h("b", {}, fmt.bytes(bytes)), ` of ${fmt.bytes(one.max_bytes)}${j.nodes ? " each" : ""}`),
    h("span", {}, "kept for ", h("b", {}, period(one.retain_s))),
    h("span", { title: "queries that ended well and were not slow, kept at most this many a second; every slow or failed one is kept" }, "queries sampled at ", h("b", {}, `${fmt.n(one.queries_per_sec)}/s`)),
    sampled ? h("span", { class: "muted", title: "fast queries that went over the rate and were not kept" }, `${fmt.n(sampled)} not kept`) : null,
    dropped ? pill("warn", `${fmt.n(dropped)} lines dropped`) : null,
    errors.length ? h("span", { class: "pill bad", title: errors.map((x) => `${x.node}: ${x.last_error}`).join("\n") }, icon("bad"), `${errors.length} cannot write`) : null,
    h("span", { class: "sep" }),
    h("button", { class: "btn sm ghost", onclick: () => storesDrawer(j) }, icon("layers"), j.nodes ? "Each server's store" : "The store"));
}

function storesDrawer(j) {
  const rows = [j.store ? { url: "this server", shard: null, status: "ok", store: j.store } : null, ...(j.nodes || [])].filter(Boolean);
  drawer({
    title: "Log stores", sub: "a Strata database beside each server's, of tables logs and queries", wide: true,
    body: [
      table([
        { key: "url", label: "server", render: (x) => h("span", { class: "mono small" }, x.store?.node ? `${x.store.node} · ${x.url}` : x.url) },
        { key: "shard", label: "shard", num: true, render: (x) => x.shard ?? "–" },
        { key: "lines", label: "lines", num: true, sort: (x) => x.store?.tables?.logs?.rows, render: (x) => x.store ? fmt.n(x.store.tables?.logs?.rows) : "–" },
        { key: "queries", label: "queries", num: true, sort: (x) => x.store?.tables?.queries?.rows, render: (x) => x.store ? fmt.n(x.store.tables?.queries?.rows) : "–" },
        { key: "bytes", label: "size", num: true, sort: (x) => x.store?.bytes, render: (x) => x.store ? fmt.bytes(x.store.bytes) : "–" },
        { key: "written", label: "last write", render: (x) => x.store ? fmt.ago(x.store.last_write_ms) : "–" },
        { key: "state", label: "", render: (x) => x.error ? pill("bad", "down") : !x.store ? pill("disabled", "no store") : x.store.last_error ? h("span", { class: "pill bad", title: x.store.last_error }, icon("bad"), "cannot write") : pill("ok", "ok") },
      ], { rows, compact: true }),
      h("p", { class: "small muted" }, "Each server writes its lines and queries to its own store every second and deletes what is older than its retention or past its size; a coordinator's search asks every server and merges the answers. Flags: --log-store, --log-retain, --log-store-mb, --log-queries, --node-name."),
      rows.filter((x) => x.store).map((x) => h("details", {}, h("summary", { class: "small" }, x.store.node), h("pre", { class: "code", style: { fontSize: "11.5px" } }, JSON.stringify(x.store, null, 2)))),
    ],
  });
}

// ---------------------------------------------------------------- shared

/** The time a search covers: the latest `range` minutes, or [from, to) when zoomed. */
function window0(range) { return { range, from: null, to: null }; }
const spanOf = (w) => w.from != null ? { from: w.from, to: w.to } : { from: Date.now() - w.range * 60e3, to: null };
const timeWhere = (t) => [`ts >= ${lit(iso(t.from))}`, t.to != null ? `ts < ${lit(iso(t.to))}` : null];

/** The time controls: the range, and a chip of the zoomed window that unzooms. */
function timeControls(w, key, onChange) {
  const zoom = h("span");
  const rng = seg(RANGES.map(([m, l]) => [m, l]), w.from == null ? w.range : null, (v) => { w.range = v; w.from = w.to = null; store.set(key, v); drawZoom(); onChange(); }, { title: "How far back" });
  function drawZoom() {
    rng.set(w.from == null ? w.range : null);
    clear(zoom, w.from == null ? null : h("button", { class: "chip", "aria-pressed": "true", title: "Back to the latest " + RANGES.find(([m]) => m === w.range)?.[1], onclick: () => { w.from = w.to = null; drawZoom(); onChange(); } },
      icon("clock"), `${fmt.datetime(w.from)} – ${w.to ? fmt.time(w.to) : "now"}`, h("span", { class: "x" }, "×")));
  }
  drawZoom();
  return { el: h("div", { class: "row nw", style: { gap: "6px" } }, rng, zoom), drawZoom };
}

/**
 * Counts over time as stacked bars: rows of [bucket, series, n], buckets in
 * minutes (or hours) since 1970. Drag across it, or click a bar, to zoom.
 */
function histogram(box, rows, { unit, t, order, colors, onZoom, what, whats }) {
  const to = t.to ?? Date.now(), spanMin = Math.max(1, (to - t.from) / 60e3);
  const binMin = Math.max(unit, Math.ceil(spanMin / 150 / unit) * unit);
  const t0 = Math.floor(t.from / 60e3 / binMin) * binMin, n = Math.max(1, Math.ceil((to / 60e3 - t0) / binMin));
  const bins = Array.from({ length: n }, () => ({}));
  const totals = {};
  for (const [b, k, c] of rows) {
    if (b == null) continue;
    const i = Math.floor((b * unit - t0) / binMin);
    if (i < 0 || i >= n) continue;
    const key = k ?? "–";
    bins[i][key] = (bins[i][key] || 0) + c;
    totals[key] = (totals[key] || 0) + c;
  }
  const keys = [...order.filter((k) => totals[k]), ...Object.keys(totals).filter((k) => !order.includes(k))];
  const max = Math.max(1, ...bins.map((b) => Object.values(b).reduce((a, x) => a + x, 0)));
  const W = 1000, H = 72, bw = W / n;
  const svgEl = s("svg", { viewBox: `0 0 ${W} ${H + 16}`, style: { width: "100%", height: H + 16 + "px", cursor: "crosshair" }, preserveAspectRatio: "none", role: "img", "aria-label": `${what} over time` });
  const binMs = binMin * 60e3, tAt = (i) => (t0 + i * binMin) * 60e3;
  bins.forEach((b, i) => {
    let y = H;
    for (const k of keys) {
      const v = b[k] || 0; if (!v) continue;
      const hh = Math.max(1, v / max * (H - 4));
      y -= hh;
      const rc = s("rect", { x: i * bw + 0.5, y, width: Math.max(1, bw - 1), height: hh, style: { fill: colors(k) } });
      rc.addEventListener("pointermove", (e) => showTip(e.clientX, e.clientY, [h("div", { class: "t" }, `${fmt.datetime(tAt(i))} – ${fmt.time(tAt(i + 1), binMin < 1440)}`), ...keys.filter((x) => b[x]).map((x) => h("div", { class: "r" }, h("span", { class: "k box", style: { background: colors(x) } }), h("b", {}, fmt.n(b[x])), h("span", { class: "n" }, x)))]));
      rc.addEventListener("pointerleave", hideTip);
      svgEl.append(rc);
    }
  });
  for (let k = 0; k <= 4; k++) {
    const x = k * W / 4, at = tAt(0) + k / 4 * n * binMs;
    svgEl.append(s("text", { x: Math.min(W - 2, Math.max(2, x)), y: H + 13, "text-anchor": k === 0 ? "start" : k === 4 ? "end" : "middle", style: { fill: "var(--muted)", fontSize: "10.5px" } }, spanMin > 1440 ? fmt.datetime(at).replace(/:\d\d$/, "") : fmt.time(at, false)));
  }
  const sel = s("rect", { class: "selr", x: 0, y: 0, width: 0, height: H, style: { display: "none" } });
  svgEl.append(sel);
  const xOf = (e) => { const r = svgEl.getBoundingClientRect(); return Math.max(0, Math.min(1, (e.clientX - r.left) / r.width)); };
  svgEl.addEventListener("pointerdown", (e) => {
    const a = xOf(e);
    svgEl.setPointerCapture(e.pointerId);
    const mv = (ev) => { const b = xOf(ev); sel.style.display = ""; sel.setAttribute("x", Math.min(a, b) * W); sel.setAttribute("width", Math.abs(b - a) * W); };
    const up = (ev) => {
      svgEl.removeEventListener("pointermove", mv); svgEl.removeEventListener("pointerup", up);
      sel.style.display = "none"; hideTip();
      const b = xOf(ev);
      if (Math.abs(b - a) * svgEl.getBoundingClientRect().width < 5) { const i = Math.min(n - 1, Math.floor(a * n)); if (bins[i] && Object.keys(bins[i]).length) onZoom(tAt(i), tAt(i + 1)); }
      else onZoom(tAt(0) + Math.min(a, b) * n * binMs, tAt(0) + Math.max(a, b) * n * binMs);
    };
    svgEl.addEventListener("pointermove", mv); svgEl.addEventListener("pointerup", up);
  });
  const total = Object.values(totals).reduce((a, x) => a + x, 0);
  clear(box, svgEl, h("div", { class: "legend", style: { marginTop: "4px" } },
    keys.map((k) => h("span", { class: "row nw", style: { gap: "5px" } }, h("span", { class: "k box", style: { background: colors(k) } }), k, h("b", { class: "num" }, fmt.n(totals[k])))),
    h("span", { class: "sep" }), h("span", { class: "muted" }, `${fmt.plural(total, what, whats)} · a bar is ${binMin >= 60 ? fmt.plural(binMin / 60, "hour") : fmt.plural(binMin, "minute")} · drag to zoom`)));
  return total;
}

/** A column of the most common values of a field; a click keeps only that one, another click lets them all back. */
function facet(title, rows, current, onPick) {
  const items = rows.filter((r) => r[0] != null);
  const max = Math.max(1, ...items.map((r) => r[1]));
  return card({ title, cls: "lg-facet", body: items.length ? h("div", { class: "facet" }, items.map(([v, n]) => h("button", { "aria-pressed": String(current === v), title: current === v ? "Show them all again" : "Only " + v, onclick: () => onPick(current === v ? "" : v) },
    h("span", { class: "ellipsis mono" }, v), h("b", { class: "num" }, fmt.si(n)),
    h("div", { class: "meter" }, h("i", { style: { width: n / max * 100 + "%" } }))))) : h("div", { class: "small muted" }, "None") });
}

/** A line of the store or of the server's memory, as a row of the log box. */
function lineRow(l, { coord, onRequest, actions }) {
  const where = l.route ? `${l.method || ""} ${l.route}` : l.method && /::grpc$/.test(l.target || "") ? `gRPC ${l.method}` : l.target;
  const fields = l.fields && typeof l.fields === "object" ? l.fields : {};
  const ln = h("div", { class: "ln", dataset: { key: l.key || "" } },
    h("span", { class: "muted", title: fmt.datetime(l.ms) }, clock(l.ms)),
    h("span", { class: "lv " + String(l.level).toUpperCase() }, l.level),
    coord ? h("span", { class: "nd", title: l.node }, l.node) : null,
    h("span", { class: "tg", title: l.target }, where),
    h("span", { class: "msg" }, l.message,
      Object.entries(fields).slice(0, 6).map(([k, v]) => h("span", { class: "fld" }, "  ", h("b", {}, k + "="), typeof v === "string" ? v : JSON.stringify(v))),
      l.request_id ? h("span", {}, "  ", h("span", { class: "rid", title: "Only this request", onclick: (e) => { e.stopPropagation(); onRequest(l.request_id); } }, l.request_id)) : null));
  ln.onclick = (e) => {
    if (e.target.closest("button,a,.rid")) return;
    if (ln.classList.toggle("open")) ln.append(h("div", { class: "more" },
      h("pre", { class: "code", style: { fontSize: "11.5px" } }, JSON.stringify(l.raw, null, 2)),
      h("div", { class: "row", style: { marginTop: "6px" } }, h("button", { class: "btn sm", onclick: () => copy(JSON.stringify(l.raw)) }, icon("copy"), "Copy"), actions?.(l))));
    else ln.querySelector(".more")?.remove();
  };
  return ln;
}

const stored = (r) => {
  const fields = parseJson(r.fields);
  return { key: `${r.node}/${r.id}`, ms: msOf(r.ts), level: r.level, node: r.node, target: r.target, method: r.method, route: r.route, message: r.message, fields: fields && typeof fields === "object" ? fields : {}, request_id: r.request_id, raw: { ...r, fields } };
};

// ---------------------------------------------------------------- lines

function lines(body, ctx, init) {
  const w = window0(store.get("logs.range", 60));
  const f = { level: store.get("logs.storedLevel", "debug"), q: init.q || "", request: init.request || "", node: init.node || "", target: "", route: "", follow: true };
  if (f.request) { w.range = 10080; }
  if (init.from) { w.from = +init.from; w.to = init.to ? +init.to : null; f.level = "trace"; }
  const at = init.at || "";
  const coord = ctx.coord;

  const time = timeControls(w, "logs.range", () => refresh());
  const lvl = seg(LV.map((x) => [x, x]), f.level, (v) => { f.level = v; store.set("logs.storedLevel", v); refresh(); }, { title: "The least severe level shown" });
  const q = h("input", { class: "input sm", value: f.q, placeholder: "Search: words, \"a phrase\", a OR b, -word", style: { width: "260px" }, title: "MATCH over the message and the fields, in Strata's query syntax", oninput: debounce(() => { f.q = q.value.trim(); refresh(); }, 300) });
  const req = h("input", { class: "input sm mono", placeholder: "request id", value: f.request, style: { width: "170px" }, oninput: debounce(() => { f.request = req.value.trim(); refresh(); }, 300) });
  const follow = h("button", { class: "chip", "aria-pressed": "true", title: "Add each new line at the top as it is written" }, icon("refresh"), "follow");
  follow.onclick = () => { f.follow = !f.follow; follow.setAttribute("aria-pressed", String(f.follow)); };
  const chips = h("div", { class: "row", style: { gap: "6px" } });
  const count = h("span", { class: "small muted" });
  const hist = h("div", { class: "hist" });
  const box = h("div", { class: "logs" + (coord ? " nodes" : ""), role: "log", "aria-live": "off", style: { height: "calc(100vh - 420px)", minHeight: "360px" } });
  const side = h("div", { class: "lg-side" });
  const sqlBtn = h("button", { class: "btn sm ghost", title: "Open this search as SQL, to change it" }, icon("sql"), "As SQL");
  body.append(
    h("div", { class: "row", style: { gap: "10px", marginBottom: "8px" } }, time.el, lvl, q, req, follow, sqlBtn, h("span", { class: "sep" }), count),
    chips,
    h("div", { class: "lg-main" },
      h("div", { class: "stack", style: { gap: "10px", minWidth: 0 } }, card({ title: "Lines over time", sub: "by level", body: hist }), box),
      side));

  const where = (skip) => [
    ...timeWhere(spanOf(w)),
    f.level !== "trace" ? `level IN (${LV.slice(0, LV.indexOf(f.level) + 1).map(lit).join(", ")})` : null,
    f.q ? `(MATCH(message, ${lit(f.q)}) OR MATCH(fields, ${lit(f.q)}))` : null,
    f.request ? `request_id = ${lit(f.request)}` : null,
    ...["node", "target", "route"].filter((k) => f[k] && k !== skip).map((k) => `${k} = ${lit(f[k])}`),
  ].filter(Boolean).join(" AND ");
  const rowsSql = (extra = "") => `SELECT ${LINE_COLS} FROM logs WHERE ${where()}${extra} ORDER BY ts DESC, id DESC LIMIT ${PAGE}`;
  sqlBtn.onclick = () => ctx.go("sql", { sql: rowsSql() });

  let shown = new Set(), newest = null, oldest = null, gen = 0, ticks = 0, matched = 0, lastDay = null;
  const pick = (k) => (v) => { f[k] = v; refresh(); };
  function drawChips() {
    clear(chips, ["node", "target", "route"].filter((k) => f[k]).map((k) => h("button", { class: "chip", "aria-pressed": "true", title: "Remove", onclick: () => pick(k)("") }, `${k} = ${f[k]}`, h("span", { class: "x" }, "×"))));
    chips.style.marginBottom = chips.childElementCount ? "8px" : "0";
  }
  const actions = (l) => [
    h("button", { class: "btn sm", title: "Every line of this server in the minute around this one", onclick: () => ctx.go("lines", { from: String(Math.floor(l.ms - 30e3)), to: String(Math.ceil(l.ms + 30e3)), node: l.node || "", at: l.key }) }, icon("clock"), "Lines around it"),
    l.request_id ? h("button", { class: "btn sm", onclick: () => ctx.go("lines", { request: l.request_id }) }, "This request's lines") : null,
    l.request_id ? h("button", { class: "btn sm", onclick: () => ctx.go("queries", { request: l.request_id }) }, "Its queries", icon("right")) : null,
  ];
  const onRequest = (id) => { req.value = id; f.request = id; if (w.range < 10080 && w.from == null) { w.range = 10080; time.drawZoom(); } refresh(); };
  const row = (r) => lineRow(stored(r), { coord, onRequest, actions });

  function place(rs, at) {
    const els = [];
    for (const r of rs) {
      const key = `${r.node}/${r.id}`;
      if (shown.has(key)) continue;
      shown.add(key);
      const ms = msOf(r.ts), d = day(ms);
      if (at !== "top" && d !== lastDay) { els.push(h("div", { class: "day" }, d)); lastDay = d; }
      els.push(row(r));
    }
    return els;
  }
  function tail() {
    box.querySelector(":scope > .tail")?.remove();
    if (!shown.size) return;
    box.append(h("div", { class: "tail" }, h("span", { class: "small muted" }, `${fmt.n(shown.size)} of ${fmt.plural(matched, "line")} shown`),
      shown.size < matched ? h("button", { class: "btn sm", onclick: older }, icon("down"), "Older lines") : null));
  }
  const edge = (rs) => rs.length ? [msOf(rs[0].ts), msOf(rs[rs.length - 1].ts)] : null;

  async function refresh() {
    const my = ++gen;
    drawChips();
    const t = spanOf(w), unit = (t.to ?? Date.now()) - t.from > 1440 * 60e3 ? 60 : 1, col = unit === 60 ? "hour" : "minute";
    clear(box, h("div", { class: "empty" }, h("div", { class: "spin" }), "Searching…"));
    try {
      const [rs, hs, ...fs] = await Promise.all([
        ask(rowsSql()),
        ask(`SELECT ${col}, level, COUNT(*) FROM logs WHERE ${where()} GROUP BY ${col}, level LIMIT 20000`),
        ...(coord ? ["node"] : []).concat(["target", "route"]).map((k) => ask(`SELECT ${k}, COUNT(*) FROM logs WHERE ${where(k)} GROUP BY ${k} ORDER BY 2 DESC LIMIT 8`)),
      ]);
      if (my !== gen) return;
      ctx.note(rs.nodes);
      matched = histogram(hist, hs.rows, { unit, t, order: LV, colors: (k) => LCOLOR[k] || "var(--s6)", what: "line", onZoom: (a, b) => { w.from = a; w.to = b; time.drawZoom(); refresh(); } });
      const keys = (coord ? ["node"] : []).concat(["target", "route"]);
      clear(side, keys.map((k, i) => facet({ node: "Server", target: "Module", route: "Route" }[k], fs[i].rows, f[k], pick(k))));
      const r = objects(rs);
      shown = new Set(); lastDay = null;
      [newest, oldest] = edge(r) || [null, null];
      clear(box, ...place(r));
      if (!r.length) clear(box, empty("logs", "No lines match", f.level === "error" || f.level === "warn" || f.level === "info" ? "Requests log at debug: choose debug above, and set the server to debug to keep each one." : "Try a longer time, another level, or fewer filters."));
      tail();
      count.textContent = `${fmt.plural(matched, "line")} · ${fmt.ms(rs.took_ms)}`;
      if (at) { const el = box.querySelector(`.ln[data-key="${CSS.escape(at)}"]`); if (el) { el.classList.add("hl"); el.scrollIntoView({ block: "center" }); } }
    } catch (e) {
      if (my !== gen) return;
      clear(box, h("div", { class: "error", style: { margin: "12px" } }, e.message));
      clear(hist); clear(side); count.textContent = "";
    }
  }
  async function older() {
    if (oldest == null) return;
    const my = gen;
    try {
      const j = await ask(rowsSql(` AND ts <= ${lit(iso(oldest))}`));
      if (my !== gen) return;
      const r = objects(j);
      const els = place(r);
      if (r.length) oldest = msOf(r[r.length - 1].ts);
      box.querySelector(":scope > .tail")?.remove();
      box.append(...els);
      if (!els.length) matched = shown.size;
      tail();
    } catch (e) { toast(e.message, "bad"); }
  }
  async function tick() {
    if (!f.follow || w.from != null) return;
    ticks++;
    if (newest == null) { if (ticks % 3 === 0 && await refreshCounts()) refresh(); return; }
    if (ticks % 4 === 0) refreshCounts();
    const my = gen;
    try {
      const j = await ask(rowsSql(` AND ts >= ${lit(iso(newest))}`));
      if (my !== gen) return;
      const r = objects(j);
      if (r.length >= PAGE) { refresh(); return; }
      const els = place(r, "top");
      if (!els.length) return;
      newest = msOf(r[0].ts);
      matched += els.length;
      const keep = box.scrollTop, top = box.firstElementChild;
      if (top?.classList.contains("day") && top.textContent === day(newest)) top.after(...els); else box.prepend(h("div", { class: "day" }, day(newest)), ...els);
      if (keep > 0) box.scrollTop = keep + els.reduce((a, el) => a + el.offsetHeight, 0);
      tail();
      count.textContent = `${fmt.plural(matched, "line")}`;
    } catch { /* the next refresh says what failed */ }
  }
  async function refreshCounts() {
    const my = gen, t = spanOf(w), unit = (t.to ?? Date.now()) - t.from > 1440 * 60e3 ? 60 : 1, col = unit === 60 ? "hour" : "minute";
    try {
      const hs = await ask(`SELECT ${col}, level, COUNT(*) FROM logs WHERE ${where()} GROUP BY ${col}, level LIMIT 20000`);
      if (my !== gen) return;
      matched = histogram(hist, hs.rows, { unit, t, order: LV, colors: (k) => LCOLOR[k] || "var(--s6)", what: "line", onZoom: (a, b) => { w.from = a; w.to = b; time.drawZoom(); refresh(); } });
      tail();
      return matched;
    } catch { return 0; /* kept as it was */ }
  }
  refresh();
  return every(3000, tick, false);
}

// ---------------------------------------------------------------- queries

const finished = (r) => ({ ...(parseJson(r.detail) || {}), ts_ms: msOf(r.ts), request_id: r.request_id, kind: r.kind, outcome: r.outcome, shard: r.shard, sql: r.sql, fingerprint: r.fingerprint, ms: r.ms, rows: r.n_rows, error: r.error, node: r.node });

/** A query's drawer, with what every server's store holds of its request: its parts on the shards and its lines. */
function openQuery(x, ctx) {
  const d = queryDrawer(finished(x));
  if (!x.request_id) return;
  const id = lit(x.request_id);
  const parts = h("div", { class: "small muted" }, "Searching…"), box = h("div", { class: "logs" + (ctx.coord ? " nodes" : ""), style: { height: "auto", maxHeight: "300px", minHeight: 0 } });
  d.body.append(card({ title: "This request in the log store", sub: ctx.coord ? "on every server" : null, body: [parts, box],
    tools: h("a", { class: "btn sm ghost", href: "#/logs?tab=lines&request=" + encodeURIComponent(x.request_id), onclick: () => d.close() }, "All its lines", icon("right")) }));
  ask(`SELECT ${QUERY_COLS} FROM queries WHERE request_id = ${id} ORDER BY ts, id LIMIT 100`).then((j) => {
    const qs = objects(j);
    clear(parts, qs.length > 1 ? h("div", { style: { marginBottom: "8px" } }, qs.map((q) => h("div", { class: "row nw small", style: { padding: "3px 0", gap: "8px" } },
      pill(OUTCOMES[q.outcome] || "info", q.outcome), h("span", { class: "mono", style: { width: "120px" } }, q.node), q.shard ? h("span", { class: "badge" }, "shard part") : h("span", { class: "badge accent" }, "the query"),
      h("span", { class: "mono ellipsis grow", title: q.sql }, q.sql), h("b", { class: "num", style: { whiteSpace: "nowrap" } }, fmt.ms(q.ms))))) : null);
  }).catch((e) => clear(parts, e.message));
  ask(`SELECT ${LINE_COLS} FROM logs WHERE request_id = ${id} ORDER BY ts, id LIMIT 200`).then((j) => {
    const ls = objects(j);
    clear(box, ls.length ? ls.map((r) => lineRow(stored(r), { coord: ctx.coord, onRequest: (rid) => { d.close(); ctx.go("lines", { request: rid }); } })) : h("div", { class: "small muted", style: { padding: "8px 12px" } }, "No lines were kept for it: requests log at debug."));
  }).catch((e) => clear(box, h("div", { class: "error" }, e.message)));
}

function queries(body, ctx, init) {
  const w = window0(store.get("logs.qrange", 60));
  const f = { outcome: "all", q: "", request: init.request || "", fingerprint: init.fingerprint || "", node: "", min: "", parts: false, follow: true };
  if (f.request || f.fingerprint) w.range = 10080;
  const coord = ctx.coord;
  const time = timeControls(w, "logs.qrange", () => refresh());
  const out = seg([["all", "all"], ["ok", "ok"], ["failed", "failed"]], f.outcome, (v) => { f.outcome = v; refresh(); }, { title: "How they ended" });
  const q = h("input", { class: "input sm", placeholder: "Search the SQL", style: { width: "200px" }, title: "MATCH over the statement, in Strata's query syntax", oninput: debounce(() => { f.q = q.value.trim(); refresh(); }, 300) });
  const req = h("input", { class: "input sm mono", placeholder: "request id", value: f.request, style: { width: "170px" }, oninput: debounce(() => { f.request = req.value.trim(); refresh(); }, 300) });
  const min = h("input", { class: "input sm num", type: "number", min: 0, placeholder: "ms", style: { width: "80px" }, title: "Only the queries that took at least this many milliseconds", oninput: debounce(() => { f.min = min.value; refresh(); }, 300) });
  const parts = h("label", { class: "row nw small dim", style: { gap: "6px", cursor: "pointer" }, title: "A coordinator's query runs a part on each shard, which each shard keeps too" },
    h("input", { type: "checkbox", onchange: (e) => { f.parts = e.target.checked; refresh(); } }), "with shards' parts");
  const follow = h("button", { class: "chip", "aria-pressed": "true", title: "Search again every few seconds" }, icon("refresh"), "follow");
  follow.onclick = () => { f.follow = !f.follow; follow.setAttribute("aria-pressed", String(f.follow)); };
  const chips = h("div", { class: "row", style: { gap: "6px" } });
  const kpis = h("div", { class: "grid g4", style: { marginBottom: "10px" } });
  const hist = h("div", { class: "hist" });
  const stmts = h("div", { class: "lg-stmts" });
  const list = h("div");
  const more = h("div", { class: "row", style: { padding: "8px 2px" } });
  const count = h("span", { class: "small muted" });
  const sqlBtn = h("button", { class: "btn sm ghost", title: "Open this search as SQL, to change it" }, icon("sql"), "As SQL");
  body.append(
    h("div", { class: "row", style: { gap: "10px", marginBottom: "8px" } }, time.el, out, q, req, h("span", { class: "row nw small muted", style: { gap: "6px" } }, "at least", min), parts, follow, sqlBtn, h("span", { class: "sep" }), count),
    chips, kpis,
    h("div", { class: "grid", style: { gridTemplateColumns: "minmax(0, 1fr) minmax(0, 1fr)", marginBottom: "10px", alignItems: "start" } },
      card({ title: "Queries over time", sub: "by how they ended", body: hist }),
      card({ title: "Statements", sub: "by the time they took in all", body: stmts, flush: true })),
    card({ title: "Queries", sub: "newest first: click one for its trace", body: [list, more], flush: true }));

  const where = (skip) => [
    ...timeWhere(spanOf(w)),
    f.outcome === "ok" ? "outcome = 'ok'" : f.outcome === "failed" ? "outcome != 'ok'" : null,
    f.q ? `MATCH(sql, ${lit(f.q)})` : null,
    f.request ? `request_id = ${lit(f.request)}` : null,
    f.fingerprint && skip !== "fingerprint" ? `fingerprint = ${lit(f.fingerprint)}` : null,
    f.node ? `node = ${lit(f.node)}` : null,
    f.min !== "" && Number.isFinite(+f.min) ? `ms >= ${+f.min}` : null,
    f.parts ? null : "shard = false",
  ].filter(Boolean).join(" AND ");
  const rowsSql = (extra = "") => `SELECT ${QUERY_COLS} FROM queries WHERE ${where()}${extra} ORDER BY ts DESC, id DESC LIMIT ${PAGE}`;
  sqlBtn.onclick = () => ctx.go("sql", { sql: rowsSql() });

  const pick = (k) => (v) => { f[k] = v; refresh(); };
  const cols = [
    { key: "ts", label: "time", render: (x) => h("span", { class: "mono small", title: fmt.datetime(msOf(x.ts)) }, clock(msOf(x.ts))) },
    coord ? { key: "node", label: "server", render: (x) => h("button", { class: "btn ghost sm mono", title: "Only this server's", onclick: () => pick("node")(x.node) }, x.node) } : null,
    { key: "outcome", label: "", render: (x) => pill(OUTCOMES[x.outcome] || "info", x.outcome) },
    { key: "ms", label: "time", num: true, render: (x) => fmt.ms(x.ms) },
    { key: "n_rows", label: "rows", num: true, render: (x) => fmt.n(x.n_rows) },
    { key: "kind", label: "kind", render: (x) => h("span", { class: "badge" }, x.kind) },
    { key: "sql", label: "SQL", render: (x) => h("div", { class: "mono small ellipsis", style: { maxWidth: "640px" }, title: x.error ? `${x.sql}\n\n${x.error}` : x.sql }, x.shard ? h("span", { class: "badge", style: { marginRight: "6px" } }, "shard part") : null, x.sql) },
  ].filter(Boolean);
  let rows = [], gen = 0, total = 0;
  const tbl = table(cols, { rows, compact: true, onRow: (x) => openQuery(x, ctx), empty: empty("queries", "No queries match", "Try a longer time or fewer filters. Fast queries that ended well are kept at most --log-queries a second; every slow or failed one is kept.") });
  list.append(tbl);

  function drawChips() {
    clear(chips, [["fingerprint", "statement"], ["node", "server"]].filter(([k]) => f[k]).map(([k, l]) => h("button", { class: "chip", "aria-pressed": "true", title: "Remove", onclick: () => pick(k)("") }, h("span", { class: "ellipsis", style: { maxWidth: "520px" } }, `${l} = ${f[k]}`), h("span", { class: "x" }, "×"))));
    chips.style.marginBottom = chips.childElementCount ? "8px" : "0";
  }
  function kpi(label, val, sub, color) { return h("div", { class: "card kpi" }, h("div", { class: "lbl" }, label), h("div", { class: "val", style: { fontSize: "22px", color } }, val), sub ? h("div", { class: "delta" }, sub) : null); }

  async function refresh(quiet) {
    const my = ++gen;
    drawChips();
    const t = spanOf(w), unit = (t.to ?? Date.now()) - t.from > 1440 * 60e3 ? 60 : 1, col = unit === 60 ? "hour" : "minute";
    try {
      const [rs, hs, by, st] = await Promise.all([
        ask(rowsSql()),
        ask(`SELECT ${col}, outcome, COUNT(*) FROM queries WHERE ${where()} GROUP BY ${col}, outcome LIMIT 20000`),
        ask(`SELECT outcome, COUNT(*), SUM(ms), MAX(ms) FROM queries WHERE ${where()} GROUP BY outcome`),
        ask(`SELECT fingerprint, COUNT(*), SUM(ms), AVG(ms), MAX(ms) FROM queries WHERE ${where("fingerprint")} GROUP BY fingerprint ORDER BY 3 DESC LIMIT 12`),
      ]);
      if (my !== gen) return;
      ctx.note(rs.nodes);
      const n = by.rows.reduce((a, r) => a + r[1], 0), bad = by.rows.filter((r) => r[0] !== "ok").reduce((a, r) => a + r[1], 0);
      const spent = by.rows.reduce((a, r) => a + (r[2] || 0), 0), worst = Math.max(0, ...by.rows.map((r) => r[3] || 0));
      clear(kpis, kpi("Queries", fmt.n(n), `${fmt.ms(rs.took_ms)} to search`), kpi("Failed", fmt.n(bad), n ? fmt.pct(bad / n) + " of them" : null, bad ? "var(--crit-ink)" : null), kpi("Mean time", fmt.ms(n ? spent / n : null), `${fmt.ms(spent)} in all`), kpi("Slowest", fmt.ms(worst)));
      histogram(hist, hs.rows, { unit, t, order: Object.keys(OCOLOR), colors: (k) => OCOLOR[k] || "var(--s6)", what: "query", whats: "queries", onZoom: (a, b) => { w.from = a; w.to = b; time.drawZoom(); refresh(); } });
      const sMax = Math.max(1e-9, ...st.rows.map((r) => r[2] || 0));
      clear(stmts, st.rows.length ? table([
        { key: "0", label: "statement", render: (x) => h("div", { class: "mono small ellipsis", style: { maxWidth: "240px" }, title: x[0] }, x[0]) },
        { key: "1", label: "runs", num: true, render: (x) => fmt.si(x[1]) },
        { key: "2", label: "in all", num: true, render: (x) => h("div", { class: "row nw", style: { gap: "6px", justifyContent: "flex-end" } }, h("div", { class: "barbox", style: { width: "48px" } }, h("div", { class: "bar", style: { width: (x[2] || 0) / sMax * 100 + "%" } })), fmt.ms(x[2])) },
        { key: "3", label: "mean", num: true, render: (x) => fmt.ms(x[3]) },
        { key: "4", label: "max", num: true, render: (x) => fmt.ms(x[4]) },
      ], { rows: st.rows, compact: true, onRow: (x) => pick("fingerprint")(f.fingerprint === x[0] ? "" : x[0]), rowClass: (x) => x[0] === f.fingerprint ? "sel" : "" }) : h("div", { class: "small muted", style: { padding: "12px" } }, "None in this time."));
      rows = objects(rs);
      tbl.update(rows);
      total = n;
      drawMore();
      count.textContent = `${fmt.plural(n, "query", "queries")}`;
    } catch (e) {
      if (my !== gen) return;
      if (!quiet) { clear(kpis); clear(hist); clear(stmts); tbl.update([]); clear(more, h("div", { class: "error" }, e.message)); }
    }
  }
  function drawMore() {
    clear(more, rows.length ? [h("span", { class: "small muted" }, `${fmt.n(rows.length)} of ${fmt.plural(total, "query", "queries")} shown`), rows.length < total ? h("button", { class: "btn sm", onclick: older }, icon("down"), "Older queries") : null] : null);
  }
  async function older() {
    if (!rows.length) return;
    f.follow = false; follow.setAttribute("aria-pressed", "false");
    const my = gen, last = rows[rows.length - 1];
    try {
      const j = await ask(rowsSql(` AND (ts < ${lit(last.ts)} OR (ts = ${lit(last.ts)} AND id < ${last.id}))`));
      if (my !== gen) return;
      const seen = new Set(rows.map((x) => `${x.node}/${x.id}`));
      const add = objects(j).filter((x) => !seen.has(`${x.node}/${x.id}`));
      rows = rows.concat(add);
      tbl.update(rows);
      if (!add.length) total = rows.length;
      drawMore();
    } catch (e) { toast(e.message, "bad"); }
  }
  refresh();
  return every(5000, () => f.follow && w.from == null ? refresh(true) : null, false);
}

// ---------------------------------------------------------------- SQL

const examples = () => {
  const hour = lit(iso(Date.now() - 3600e3)), day0 = lit(iso(Date.now() - 86400e3));
  return [
    ["Errors and warnings", `SELECT ts, node, level, target, message, fields\nFROM logs\nWHERE level IN ('error', 'warn') AND ts > ${day0}\nORDER BY ts DESC\nLIMIT 100`],
    ["Lines a minute", `SELECT minute, level, COUNT(*)\nFROM logs\nWHERE ts > ${hour}\nGROUP BY minute, level\nORDER BY minute\nLIMIT 1000`],
    ["Best matches", `SELECT ts, node, level, message\nFROM logs\nWHERE MATCH(message, 'replica OR shard OR timeout')\nORDER BY bm25(message, 'replica shard timeout') DESC\nLIMIT 20`],
    ["Slowest routes", `SELECT route, COUNT(*) AS n, AVG(ms), MAX(ms)\nFROM logs\nWHERE route IS NOT NULL AND ms IS NOT NULL AND ts > ${day0}\nGROUP BY route\nORDER BY 4 DESC\nLIMIT 20`],
    ["Statements by time", `SELECT fingerprint, COUNT(*) AS n, SUM(ms), AVG(ms), MAX(ms)\nFROM queries\nWHERE ts > ${day0} AND shard = false\nGROUP BY fingerprint\nORDER BY 3 DESC\nLIMIT 20`],
    ["Failed queries", `SELECT ts, node, kind, outcome, ms, error, sql\nFROM queries\nWHERE outcome != 'ok'\nORDER BY ts DESC\nLIMIT 100`],
    ["Errors by server", `SELECT node, level, COUNT(*)\nFROM logs\nWHERE level IN ('error', 'warn') AND ts > ${day0}\nGROUP BY node, level\nORDER BY 3 DESC`],
    ["5xx answers", `SELECT ts, node, method, route, status, ms, error, request_id\nFROM logs\nWHERE status >= 500\nORDER BY ts DESC\nLIMIT 100`],
  ];
};

function sqlTab(body, ctx, init) {
  let scope = "cluster", view = store.get("logs.sqlView", "grid"), last = null;
  const ed = editor({
    value: init.sql || store.get("logs.sql", examples()[0][1]),
    onChange: (v) => store.set("logs.sql", v),
    onRun: () => run(),
    catalog: () => ctx.schemas,
    label: "SQL over the log store",
  });
  const runBtn = h("button", { class: "btn primary", title: "Run (Ctrl+Enter)", onclick: () => run() }, icon("play"), "Run");
  const scopeSeg = ctx.coord ? seg([["cluster", "every server"], ["local", "this one"]], scope, (v) => { scope = v; run(); }, { title: "On a coordinator: every server's store, merged, or its own alone (which also runs joins and subqueries)" }) : null;
  const status = h("div", { class: "row", style: { gap: "12px", minHeight: "28px" } });
  const outTabs = h("div", { class: "tabs", role: "tablist", style: { marginBottom: "0" } });
  const out = h("div", { style: { minHeight: "200px" } });
  const tools = h("div", { class: "row nw" });
  body.append(
    h("div", { class: "row", style: { gap: "6px", marginBottom: "8px" } }, h("span", { class: "small muted" }, "Examples"), examples().map(([name, sql]) => h("button", { class: "chip", onclick: () => { ed.value = sql; store.set("logs.sql", sql); run(); } }, name))),
    h("div", { style: { height: "190px", marginBottom: "8px" } }, ed.el),
    h("div", { class: "row", style: { gap: "10px", marginBottom: "10px" } }, runBtn, scopeSeg, h("span", { class: "small muted" }, "Tables ", h("b", { class: "mono" }, "logs"), " and ", h("b", { class: "mono" }, "queries"), ": every column is in the editor's completions."),
      h("span", { class: "sep" }), h("button", { class: "btn sm ghost", onclick: () => schemaDrawer(ctx) }, icon("schema"), "Their schemas")),
    status,
    card({ body: [h("div", { class: "row between", style: { padding: "0 10px" } }, outTabs, tools), out], flush: true }));

  function drawOut() {
    clear(outTabs, [["grid", "Rows"], ["chart", "Chart"], ["plan", "Plan"]].map(([k, l]) => h("button", { role: "tab", "aria-selected": String(k === view), onclick: () => { view = k; store.set("logs.sqlView", k); drawOut(); } }, l)));
    if (!last) { clear(out, empty("sql", "Run a query", "Pick an example, or write SQL over logs and queries.")); clear(tools); return; }
    clear(tools, exportMenu(last, "strata-logs"));
    clear(out, view === "chart" ? h("div", { style: { padding: "12px" } }, resultChart(last)) : view === "plan" ? h("pre", { class: "code", style: { margin: "12px" } }, (last.stats?.plan || []).join("\n") || "No plan was kept.") : grid(last));
  }
  async function run() {
    const sql = ed.value.trim().replace(/;\s*$/, "");
    if (!sql) return;
    ed.clearMarks();
    clear(status, h("div", { class: "spin" }), h("span", { class: "small muted" }, "Running…"));
    try {
      last = await ask(sql, ctx.coord ? scope : undefined);
      ctx.note(last.nodes);
      clear(status, answerStats(last), ctx.coord && last.nodes ? h("span", { class: "small muted", title: last.nodes.map((n) => `${n.node || n.url}: ${n.error || `${fmt.plural(n.rows, "row")} in ${fmt.ms(n.ms)}`}`).join("\n") }, `from ${fmt.plural(last.nodes.filter((n) => !n.error).length, "server")}`) : null);
    } catch (e) {
      last = null;
      ed.markFrom(e.message);
      clear(status, h("div", { class: "error grow" }, e.message));
    }
    drawOut();
  }
  drawOut();
  if (init.sql || init.run) run();
  return () => {};
}

function schemaDrawer(ctx) {
  api("/v1/logs/store").then((j) => drawer({
    title: "The log store's tables", sub: "one Strata database beside each server's",
    body: (j.schemas || []).map((x) => card({ title: x.table, body: sqlBlock(x.sql) })),
  })).catch((e) => toast(e.message, "bad"));
}

// ---------------------------------------------------------------- live

/** The latest 5,000 lines in this server's memory, followed as they come: works with no store. */
function live(body, init, filt) {
  const f = { level: store.get("logs.level", "info"), q: "", request: init.request || "", follow: true };
  let lines = [], lastSeq = 0;
  const box = h("div", { class: "logs", role: "log", "aria-live": "off" });
  const rate = h("div");
  const count = h("span", { class: "small muted" });
  const lvl = seg(LV.map((x) => [x, x]), f.level, (v) => { f.level = v; store.set("logs.level", v); reload(); }, { title: "The least severe level shown" });
  const q = h("input", { class: "input sm", placeholder: "Search the text…", style: { width: "220px" }, oninput: debounce(() => { f.q = q.value; reload(); }, 250) });
  const req = h("input", { class: "input sm mono", placeholder: "request id", value: f.request, style: { width: "190px" }, oninput: debounce(() => { f.request = req.value.trim(); reload(); }, 250) });
  const follow = h("button", { class: "chip", "aria-pressed": "true", title: "Scroll to each new line", onclick: () => { f.follow = !f.follow; follow.setAttribute("aria-pressed", String(f.follow)); if (f.follow) box.scrollTop = box.scrollHeight; } }, icon("down"), "follow");
  body.append(
    h("div", { class: "row", style: { gap: "10px", marginBottom: "10px" } }, lvl, q, req, follow,
      h("button", { class: "btn sm ghost", title: "Download the lines shown, as JSON Lines", onclick: () => download() }, icon("download"), "Download"),
      h("span", { class: "sep" }), count),
    card({ title: "Lines per 10 seconds", sub: "of those in memory, by level", body: rate, attrs: { style: { marginBottom: "10px" } } }),
    box);

  const onRequest = (id) => { req.value = id; f.request = id; reload(); };
  const row = (l) => lineRow({ ms: l.ts_ms, level: l.level, target: l.target, method: l.method, route: l.route, message: l.message, fields: l.fields, request_id: l.request_id, raw: l }, { onRequest });

  function drawRate() {
    if (!lines.length) { clear(rate, h("div", { class: "small muted" }, "No lines at this level yet.")); return; }
    const t1 = Date.now(), t0 = Math.min(lines[0].ts_ms, t1 - 600e3), B = 10e3;
    const n = Math.ceil((t1 - t0) / B);
    const bins = Array.from({ length: n }, () => ({}));
    for (const l of lines) { const i = Math.min(n - 1, Math.floor((l.ts_ms - t0) / B)); bins[i][l.level] = (bins[i][l.level] || 0) + 1; }
    const max = Math.max(1, ...bins.map((b) => Object.values(b).reduce((a, x) => a + x, 0)));
    const W = 1000, H = 64, bw = W / n;
    const svgEl = s("svg", { viewBox: `0 0 ${W} ${H + 14}`, style: { width: "100%", height: H + 14 + "px" }, preserveAspectRatio: "none" });
    bins.forEach((b, i) => {
      let y = H;
      for (const L of LV) {
        const v = b[L] || 0; if (!v) continue;
        const hh = v / max * (H - 4);
        y -= hh;
        const rc = s("rect", { x: i * bw + 0.5, y, width: Math.max(1, bw - 1), height: hh, style: { fill: LCOLOR[L] } });
        rc.addEventListener("pointermove", (e) => showTip(e.clientX, e.clientY, [h("div", { class: "t" }, `${fmt.time(t0 + i * B)} – ${fmt.time(t0 + (i + 1) * B)}`), ...LV.filter((x) => b[x]).map((x) => h("div", { class: "r" }, h("span", { class: "k box", style: { background: LCOLOR[x] } }), h("b", {}, fmt.n(b[x])), h("span", { class: "n" }, x)))]));
        rc.addEventListener("pointerleave", hideTip);
        svgEl.append(rc);
      }
    });
    clear(rate, svgEl, h("div", { class: "legend", style: { marginTop: "4px" } }, LV.map((L) => h("span", { class: "row nw", style: { gap: "5px" } }, h("span", { class: "k box", style: { background: LCOLOR[L] } }), L))));
  }

  function params(after) {
    const p = new URLSearchParams({ after: String(after), limit: "1000", level: f.level });
    if (f.q) p.set("q", f.q);
    if (f.request) p.set("request", f.request);
    return p;
  }
  async function poll() {
    try {
      const j = await api("/v1/logs?" + params(lastSeq));
      if (j.filter && document.activeElement !== filt) filt.value = j.filter;
      for (const l of j.lines) l.level = String(l.level).toLowerCase();
      if (j.lines.length) {
        const atEnd = box.scrollHeight - box.scrollTop - box.clientHeight < 40;
        lines.push(...j.lines);
        lastSeq = j.lines[j.lines.length - 1].seq;
        box.querySelector(":scope > .empty")?.remove();
        box.append(...j.lines.map(row));
        if (lines.length > 3000) { const cut = lines.length - 3000; lines.splice(0, cut); for (let i = 0; i < cut; i++) box.firstChild?.remove(); }
        if (f.follow && (atEnd || j.lines.length === lines.length)) box.scrollTop = box.scrollHeight;
        drawRate();
      } else if (!lines.length) { clear(box, empty("logs", "No lines match", f.level === "info" || f.level === "warn" || f.level === "error" ? "Requests log at debug: set the server to debug above to see each one." : "Try a lower level or clear the filters.")); drawRate(); }
      count.textContent = `${fmt.plural(lines.length, "line")} · newest #${j.last}`;
    } catch (e) { clear(box, failure(e, "The logs")); stop(); }
  }
  function reload() { lines = []; lastSeq = 0; clear(box); poll(); }
  function download() {
    const blob = new Blob([lines.map((l) => JSON.stringify(l)).join("\n") + "\n"], { type: "application/x-ndjson" });
    const a = h("a", { href: URL.createObjectURL(blob), download: `strata-logs-${new Date().toISOString().slice(0, 19)}.jsonl` });
    document.body.append(a); a.click(); a.remove();
  }
  const stop = every(1500, poll);
  return stop;
}
