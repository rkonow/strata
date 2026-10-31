//! What the command center (the UI) shows of a running server, beside
//! `/metrics`: the queries running now, which an operator may cancel
//! ([`LiveQueries`]); the latest queries and the slow ones, each with its
//! trace ([`QueryLog`]); statistics per statement shape, as
//! `pg_stat_statements` keeps them ([`Statements`]); and a time series of
//! the server's rates and gauges, sampled every few seconds and kept for
//! an hour ([`Series`]), so that a page opened now has a history to draw.
//!
//! A trace ([`Trace`]) is the timed steps of one request: waiting for a
//! query slot, executing, and on a coordinator every call to a shard's
//! replica (hedges and retries included), so that a slow query shows
//! where its time went.

use std::borrow::Cow;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{json, Value as Json};
use tokio::sync::Notify;

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

// ---------------------------------------------------------------- traces

tokio::task_local! {
    static TRACE: Arc<Trace>;
}

/// The timed steps of one request, from its start.
pub struct Trace {
    t0: Instant,
    spans: Mutex<Vec<Span>>,
}

/// One step: what it was, on which shard and replica, when it started and
/// how long it took (microseconds from the request's start), and how it
/// ended ("ok", "error", "outrun" for a hedge that lost).
#[derive(Clone, Debug, serde::Serialize)]
pub struct Span {
    pub name: Cow<'static, str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replica: Option<String>,
    pub start_us: u64,
    pub dur_us: u64,
    pub outcome: &'static str,
    /// The call was a hedge or a retry: not the first asked of its shard.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub again: bool,
}

impl Trace {
    pub fn new() -> Arc<Trace> {
        Arc::new(Trace { t0: Instant::now(), spans: Mutex::new(Vec::new()) })
    }

    /// The steps so far, at most [`MAX_SPANS`].
    pub fn spans(&self) -> Vec<Span> {
        lock(&self.spans).clone()
    }
}

/// The most steps a trace keeps: a compound query over many shards could
/// make thousands.
pub const MAX_SPANS: usize = 512;

/// Runs `f` with `trace` as its request's trace.
pub async fn traced<F: Future>(trace: Arc<Trace>, f: F) -> F::Output {
    TRACE.scope(trace, f).await
}

/// The trace of the request this task serves, if any.
pub fn trace() -> Option<Arc<Trace>> {
    TRACE.try_with(|t| t.clone()).ok()
}

/// Runs `f` in `trace` if there is one (a task that a request's work
/// moved to: a blocking thread's `block_on`).
pub async fn within<F: Future>(trace: Option<Arc<Trace>>, f: F) -> F::Output {
    match trace {
        Some(t) => TRACE.scope(t, f).await,
        None => f.await,
    }
}

/// A step that started at `t0` and ends now, in the current trace.
pub fn record(name: impl Into<Cow<'static, str>>, t0: Instant, outcome: &'static str) {
    record_at(name.into(), None, None, t0, outcome, false);
}

/// A call to replica `replica` of shard `shard`.
pub fn record_call(name: Cow<'static, str>, shard: usize, replica: &str, t0: Instant, outcome: &'static str, again: bool) {
    record_at(name, Some(shard), Some(replica.to_string()), t0, outcome, again);
}

fn record_at(name: Cow<'static, str>, shard: Option<usize>, replica: Option<String>, t0: Instant, outcome: &'static str, again: bool) {
    let _ = TRACE.try_with(|t| {
        let mut spans = lock(&t.spans);
        if spans.len() < MAX_SPANS {
            let start_us = t0.saturating_duration_since(t.t0).as_micros() as u64;
            spans.push(Span { name, shard, replica, start_us, dur_us: t0.elapsed().as_micros() as u64, outcome, again });
        }
    });
}

/// Records a shard call when dropped: as `outcome` once set, else as
/// "outrun" (another replica answered first, or the request ended).
pub struct CallSpan {
    pub name: Cow<'static, str>,
    pub shard: usize,
    pub replica: String,
    pub t0: Instant,
    pub again: bool,
    pub outcome: Option<&'static str>,
}

impl Drop for CallSpan {
    fn drop(&mut self) {
        let name = std::mem::take(&mut self.name);
        record_call(name, self.shard, &self.replica, self.t0, self.outcome.unwrap_or("outrun"), self.again);
    }
}

/// The RPC a response type answers: `pb::QueryResponse` is "Query".
pub fn rpc_name<T>() -> Cow<'static, str> {
    let full = std::any::type_name::<T>();
    let last = full.rsplit("::").next().unwrap_or(full);
    let last = last.split('<').next().unwrap_or(last);
    Cow::Owned(last.strip_suffix("Response").unwrap_or(last).to_string())
}

// ---------------------------------------------------------- live queries

/// A query running now.
pub struct LiveQuery {
    pub id: u64,
    pub request: Option<Arc<str>>,
    pub sql: String,
    pub kind: &'static str,
    /// Asked by a coordinator, as a shard.
    pub shard: bool,
    pub started_ms: u64,
    t0: Instant,
    stop: AtomicBool,
    wake: Notify,
    trace: Option<Arc<Trace>>,
}

impl LiveQuery {
    /// Ends when an operator cancels the query.
    pub async fn cancelled(&self) {
        loop {
            let n = self.wake.notified();
            if self.stop.load(Ordering::Relaxed) {
                return;
            }
            n.await;
        }
    }

    fn json(&self) -> Json {
        let mut j = json!({
            "id": self.id, "sql": self.sql, "kind": self.kind, "shard": self.shard,
            "started_ms": self.started_ms, "elapsed_ms": self.t0.elapsed().as_secs_f64() * 1e3,
            "cancelled": self.stop.load(Ordering::Relaxed),
        });
        if let Some(r) = &self.request {
            j["request_id"] = json!(r);
        }
        if let Some(t) = &self.trace {
            j["trace"] = json!(t.spans());
        }
        j
    }
}

/// The queries running now, by id.
#[derive(Default)]
pub struct LiveQueries {
    next: AtomicU64,
    running: Mutex<BTreeMap<u64, Arc<LiveQuery>>>,
}

/// A query's place in [`LiveQueries`], given up when dropped.
pub struct Live {
    reg: Arc<LiveQueries>,
    pub query: Arc<LiveQuery>,
}

impl Drop for Live {
    fn drop(&mut self) {
        lock(&self.reg.running).remove(&self.query.id);
    }
}

/// The answer a query cancelled by an operator gets (status 499).
pub fn cancelled_error(id: u64) -> crate::ApiError {
    crate::ApiError { status: 499, message: format!("query {id} was cancelled by an operator (DELETE /v1/queries/{id})") }
}

impl LiveQueries {
    /// Lists `sql` as running until the guard is dropped.
    pub fn start(self: &Arc<Self>, sql: &str, kind: &'static str, shard: bool) -> Live {
        let id = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        let query = Arc::new(LiveQuery {
            id,
            request: crate::obs::current(),
            sql: crate::obs::truncate(sql, 4000).into_owned(),
            kind,
            shard,
            started_ms: now_ms(),
            t0: Instant::now(),
            stop: AtomicBool::new(false),
            wake: Notify::new(),
            trace: trace(),
        });
        lock(&self.running).insert(id, query.clone());
        Live { reg: self.clone(), query }
    }

    /// Runs `work` as query `live`, which ends with [`cancelled_error`] if
    /// an operator cancels it first (dropping `work`, which stops the
    /// query's execution at its next check).
    pub async fn run<T, F>(live: &Live, work: F) -> Result<T, crate::ApiError>
    where
        F: Future<Output = Result<T, crate::ApiError>>,
    {
        tokio::select! {
            r = work => r,
            _ = live.query.cancelled() => Err(cancelled_error(live.query.id)),
        }
    }

    /// The running queries, the longest running first.
    pub fn list(&self) -> Vec<Json> {
        let mut qs: Vec<Arc<LiveQuery>> = lock(&self.running).values().cloned().collect();
        qs.sort_by_key(|q| q.t0);
        qs.iter().map(|q| q.json()).collect()
    }

    pub fn len(&self) -> usize {
        lock(&self.running).len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Cancels query `id`; false if it is not running.
    pub fn cancel(&self, id: u64) -> bool {
        let Some(q) = lock(&self.running).get(&id).cloned() else { return false };
        q.stop.store(true, Ordering::Relaxed);
        q.wake.notify_waiters();
        true
    }
}

// ------------------------------------------------- latest and slow queries

/// A query that ended.
#[derive(Clone, Debug, serde::Serialize)]
pub struct Finished {
    pub ts_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<Arc<str>>,
    pub sql: String,
    pub fingerprint: String,
    pub kind: &'static str,
    pub outcome: &'static str,
    pub ms: f64,
    pub rows: usize,
    pub shard: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub trace: Vec<Span>,
}

/// The latest queries, the slow ones, and statistics per statement.
pub struct QueryLog {
    recent: Mutex<VecDeque<Arc<Finished>>>,
    slow: Mutex<VecDeque<Arc<Finished>>>,
    pub statements: Statements,
    /// Also hands every query that ended, and whether it was slow, here:
    /// the server's log store ([`crate::logstore::LogStore::keep_query`]).
    pub keep: std::sync::OnceLock<Keep>,
}

/// Where a [`QueryLog`] hands the queries that end.
pub type Keep = Box<dyn Fn(&Arc<Finished>, bool) + Send + Sync>;

/// Queries [`QueryLog`] keeps of the latest, and of the slow.
pub const RECENT: usize = 500;
pub const SLOW: usize = 200;

impl Default for QueryLog {
    fn default() -> QueryLog {
        QueryLog { recent: Mutex::new(VecDeque::with_capacity(RECENT)), slow: Mutex::new(VecDeque::new()), statements: Statements::default(), keep: std::sync::OnceLock::new() }
    }
}

impl QueryLog {
    /// Keeps a query that ended (in the slow ones too if `slow`).
    #[allow(clippy::too_many_arguments)]
    pub fn add(&self, sql: &str, kind: &'static str, outcome: &'static str, secs: f64, rows: usize, shard: bool, error: Option<&str>, slow: bool) {
        let fingerprint = fingerprint(sql);
        self.statements.add(&fingerprint, sql, kind, outcome, secs, rows, shard);
        let f = Arc::new(Finished {
            ts_ms: now_ms(),
            request_id: crate::obs::current(),
            sql: crate::obs::truncate(sql, 2000).into_owned(),
            fingerprint,
            kind,
            outcome,
            ms: secs * 1e3,
            rows,
            shard,
            error: error.map(|e| crate::obs::truncate(e, 500).into_owned()),
            trace: trace().map(|t| t.spans()).unwrap_or_default(),
        });
        if let Some(keep) = self.keep.get() {
            keep(&f, slow);
        }
        push(&self.recent, f.clone(), RECENT);
        if slow {
            push(&self.slow, f, SLOW);
        }
    }

    /// The latest queries, the newest first: those after `since_ms`, at
    /// most `limit`.
    pub fn recent(&self, since_ms: u64, limit: usize) -> Vec<Arc<Finished>> {
        lock(&self.recent).iter().rev().filter(|f| f.ts_ms > since_ms).take(limit).cloned().collect()
    }

    pub fn slow(&self, limit: usize) -> Vec<Arc<Finished>> {
        lock(&self.slow).iter().rev().take(limit).cloned().collect()
    }

    /// The query with request id `id`, as a coordinator asked or a client.
    pub fn by_request(&self, id: &str) -> Vec<Arc<Finished>> {
        lock(&self.recent).iter().rev().filter(|f| f.request_id.as_deref() == Some(id)).cloned().collect()
    }
}

fn push<T>(q: &Mutex<VecDeque<T>>, x: T, cap: usize) {
    let mut q = lock(q);
    if q.len() == cap {
        q.pop_front();
    }
    q.push_back(x);
}

/// A statement's shape: its text with every literal (string, number, a
/// list of them) replaced by `?`, and its spaces collapsed, so that
/// queries differing only in their values count as one statement.
pub fn fingerprint(sql: &str) -> String {
    let mut out = String::with_capacity(sql.len().min(512));
    let b = sql.as_bytes();
    let mut i = 0;
    let mut space = false;
    let ident = |c: u8| c.is_ascii_alphanumeric() || c == b'_';
    while i < b.len() && out.len() < 1000 {
        let c = b[i];
        if c.is_ascii_whitespace() {
            space = !out.is_empty();
            i += 1;
            continue;
        }
        if space {
            out.push(' ');
            space = false;
        }
        if c == b'\'' {
            // A string, '' inside it an escaped quote.
            i += 1;
            while i < b.len() {
                if b[i] == b'\'' {
                    if b.get(i + 1) == Some(&b'\'') {
                        i += 2;
                        continue;
                    }
                    break;
                }
                i += 1;
            }
            i += 1;
            out.push('?');
        } else if c == b'"' {
            // A quoted identifier, kept.
            let start = i;
            i += 1;
            while i < b.len() && b[i] != b'"' {
                i += 1;
            }
            i = (i + 1).min(b.len());
            out.push_str(&sql[start..i]);
        } else if (c.is_ascii_digit() || (c == b'-' && b.get(i + 1).is_some_and(|d| d.is_ascii_digit()) && !out.ends_with(|p: char| p.is_ascii_alphanumeric() || p == ')'))) && !out.ends_with(|p: char| p.is_ascii_alphanumeric() || p == '_') {
            i += 1;
            while i < b.len() && (b[i].is_ascii_alphanumeric() || b[i] == b'.' || ((b[i] == b'-' || b[i] == b'+') && matches!(b[i - 1], b'e' | b'E'))) {
                i += 1;
            }
            out.push('?');
        } else if ident(c) {
            let start = i;
            while i < b.len() && ident(b[i]) {
                i += 1;
            }
            out.push_str(&sql[start..i]);
        } else {
            // Any other character, whole.
            let ch = sql[i..].chars().next().unwrap_or(' ');
            out.push(ch);
            i += ch.len_utf8();
        }
    }
    collapse_lists(&out)
}

/// `(?, ?, ?)` as `(?...)`, `[?, ?]` as `[?...]`: a list of values is one
/// statement whatever its length.
fn collapse_lists(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(i) = rest.find(['(', '[']) {
        out.push_str(&rest[..=i]);
        let close = if rest.as_bytes()[i] == b'(' { ')' } else { ']' };
        let after = &rest[i + 1..];
        let end = after.find(close);
        match end {
            Some(e) if e > 1 && after[..e].split(',').all(|p| p.trim() == "?") && after[..e].contains(',') => {
                out.push_str("?...");
                rest = &after[e..];
            }
            _ => rest = after,
        }
    }
    out.push_str(rest);
    out
}

/// Statistics per statement ([`fingerprint`]).
#[derive(Default)]
pub struct Statements {
    by: Mutex<HashMap<String, Stmt>>,
    since_ms: AtomicU64,
}

/// Statements kept at most; past that, a new one replaces the least
/// called.
pub const STATEMENTS: usize = 2000;

/// Latency bounds of [`Stmt::hist`], in milliseconds.
const STMT_BUCKETS: [f64; 12] = [0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0, 1000.0, 5000.0];

#[derive(Clone)]
struct Stmt {
    example: String,
    kind: &'static str,
    calls: u64,
    errors: u64,
    shard_calls: u64,
    total_ms: f64,
    max_ms: f64,
    min_ms: f64,
    rows: u64,
    last_ms: u64,
    hist: [u64; STMT_BUCKETS.len() + 1],
}

impl Statements {
    #[allow(clippy::too_many_arguments)]
    fn add(&self, fp: &str, sql: &str, kind: &'static str, outcome: &'static str, secs: f64, rows: usize, shard: bool) {
        let ms = secs * 1e3;
        let mut by = lock(&self.by);
        if self.since_ms.load(Ordering::Relaxed) == 0 {
            self.since_ms.store(now_ms(), Ordering::Relaxed);
        }
        if !by.contains_key(fp) && by.len() >= STATEMENTS {
            if let Some(k) = by.iter().min_by_key(|(_, s)| (s.calls, s.last_ms)).map(|(k, _)| k.clone()) {
                by.remove(&k);
            }
        }
        let s = by.entry(fp.to_string()).or_insert_with(|| Stmt {
            example: crate::obs::truncate(sql, 2000).into_owned(),
            kind,
            calls: 0,
            errors: 0,
            shard_calls: 0,
            total_ms: 0.0,
            max_ms: 0.0,
            min_ms: f64::INFINITY,
            rows: 0,
            last_ms: 0,
            hist: [0; STMT_BUCKETS.len() + 1],
        });
        s.calls += 1;
        s.errors += (!matches!(outcome, "ok" | "partial")) as u64;
        s.shard_calls += shard as u64;
        s.total_ms += ms;
        s.max_ms = s.max_ms.max(ms);
        s.min_ms = s.min_ms.min(ms);
        s.rows += rows as u64;
        s.last_ms = now_ms();
        let b = STMT_BUCKETS.iter().position(|&b| ms <= b).unwrap_or(STMT_BUCKETS.len());
        s.hist[b] += 1;
    }

    /// Every statement, by total time spent, the most first.
    pub fn list(&self, limit: usize) -> Json {
        let by = lock(&self.by);
        let mut all: Vec<(&String, &Stmt)> = by.iter().collect();
        all.sort_by(|a, b| b.1.total_ms.total_cmp(&a.1.total_ms));
        let total: f64 = all.iter().map(|(_, s)| s.total_ms).sum();
        let rows: Vec<Json> = all
            .iter()
            .take(limit)
            .map(|(fp, s)| {
                json!({
                    "fingerprint": fp, "example": s.example, "kind": s.kind, "calls": s.calls, "errors": s.errors, "shard_calls": s.shard_calls,
                    "total_ms": s.total_ms, "mean_ms": s.total_ms / s.calls.max(1) as f64, "min_ms": s.min_ms, "max_ms": s.max_ms,
                    "p50_ms": quantile_ms(&s.hist, 0.5), "p95_ms": quantile_ms(&s.hist, 0.95), "p99_ms": quantile_ms(&s.hist, 0.99),
                    "rows": s.rows, "rows_per_call": s.rows as f64 / s.calls.max(1) as f64, "last_ms": s.last_ms,
                    "share": if total > 0.0 { s.total_ms / total } else { 0.0 },
                })
            })
            .collect();
        json!({"statements": rows, "count": by.len(), "since_ms": self.since_ms.load(Ordering::Relaxed), "buckets_ms": STMT_BUCKETS})
    }

    pub fn reset(&self) {
        lock(&self.by).clear();
        self.since_ms.store(now_ms(), Ordering::Relaxed);
    }
}

/// The `q` quantile of a histogram over [`STMT_BUCKETS`], interpolated
/// within its bucket.
fn quantile_ms(hist: &[u64], q: f64) -> f64 {
    let bounds: Vec<f64> = STMT_BUCKETS.to_vec();
    quantile(&bounds, hist, q).unwrap_or(0.0)
}

/// The `q` quantile of counts per bucket (`counts[i]` at most `bounds[i]`,
/// the last count past every bound), interpolated linearly within its
/// bucket; `None` without counts. Past the last bound, the last bound.
pub fn quantile(bounds: &[f64], counts: &[u64], q: f64) -> Option<f64> {
    let n: u64 = counts.iter().sum();
    if n == 0 {
        return None;
    }
    let want = (q * n as f64).max(1.0);
    let mut cum = 0.0;
    for (i, &c) in counts.iter().enumerate() {
        let next = cum + c as f64;
        if next >= want && c > 0 {
            let lo = if i == 0 { 0.0 } else { bounds[i - 1] };
            let Some(&hi) = bounds.get(i) else { return bounds.last().copied() };
            return Some(lo + (hi - lo) * ((want - cum) / c as f64));
        }
        cum = next;
    }
    bounds.last().copied()
}

// ------------------------------------------------------------ the series

/// Cumulative counters and gauges at one moment ([`Service::sample`]):
/// the sampler turns two into a point of rates.
#[derive(Clone, Default, Debug)]
pub struct Sample {
    pub at: Option<Instant>,
    pub ts_ms: u64,
    /// Queries ended, and those that failed (error or timeout), were
    /// refused (429) or answered partially.
    pub queries: u64,
    pub query_errors: u64,
    pub rejected: u64,
    pub partial: u64,
    /// Queries ended by kind.
    pub by_kind: BTreeMap<&'static str, u64>,
    /// Query latencies, every kind together: counts per
    /// [`crate::metrics::BUCKETS`] bucket, the last past every bound.
    pub query_hist: Vec<u64>,
    pub http: u64,
    pub http_4xx: u64,
    pub http_5xx: u64,
    pub writes: u64,
    pub written_docs: u64,
    pub write_errors: u64,
    pub write_hist: Vec<u64>,
    pub slow: u64,
    /// The process's CPU time, in seconds.
    pub cpu_s: f64,
    /// Gauges, by name.
    pub gauges: BTreeMap<&'static str, f64>,
}

/// Points kept: an hour at the default interval.
pub const POINTS: usize = 1800;

/// How often the sampler samples by default.
pub const INTERVAL: Duration = Duration::from_secs(2);

/// The latest points.
pub struct Series {
    pub interval: Duration,
    points: Mutex<VecDeque<Arc<Json>>>,
    last: Mutex<Option<Sample>>,
}

impl Default for Series {
    fn default() -> Series {
        Series { interval: INTERVAL, points: Mutex::new(VecDeque::with_capacity(POINTS)), last: Mutex::new(None) }
    }
}

impl Series {
    /// Adds the point between the last sample and `s`, and keeps `s` for
    /// the next. The first sample makes no point.
    pub fn add(&self, s: Sample) -> Option<Arc<Json>> {
        let mut last = lock(&self.last);
        let point = last.as_ref().map(|prev| Arc::new(point(prev, &s)));
        *last = Some(s);
        drop(last);
        if let Some(p) = &point {
            push(&self.points, p.clone(), POINTS);
        }
        point
    }

    /// The points after `since_ms`, the oldest first.
    pub fn since(&self, since_ms: u64) -> Vec<Arc<Json>> {
        lock(&self.points).iter().filter(|p| p["t"].as_u64().unwrap_or(0) > since_ms).cloned().collect()
    }

    /// The latest point.
    pub fn last(&self) -> Option<Arc<Json>> {
        lock(&self.points).back().cloned()
    }
}

/// The rates between samples `a` and `b`, and `b`'s gauges.
fn point(a: &Sample, b: &Sample) -> Json {
    let secs = match (a.at, b.at) {
        (Some(x), Some(y)) => y.saturating_duration_since(x).as_secs_f64(),
        _ => (b.ts_ms.saturating_sub(a.ts_ms)) as f64 / 1e3,
    }
    .max(1e-3);
    let rate = |x: u64, y: u64| y.saturating_sub(x) as f64 / secs;
    let delta = |x: &[u64], y: &[u64]| y.iter().enumerate().map(|(i, &v)| v.saturating_sub(x.get(i).copied().unwrap_or(0))).collect::<Vec<u64>>();
    let qh = delta(&a.query_hist, &b.query_hist);
    let wh = delta(&a.write_hist, &b.write_hist);
    let ms = |h: &[u64], q: f64| quantile(&crate::metrics::BUCKETS, h, q).map(|s| s * 1e3);
    let by_kind: serde_json::Map<String, Json> =
        b.by_kind.iter().map(|(k, &n)| (k.to_string(), json!(rate(a.by_kind.get(k).copied().unwrap_or(0), n)))).filter(|(_, v)| v.as_f64().unwrap_or(0.0) > 0.0).collect();
    let ended = b.queries.saturating_sub(a.queries);
    let failed = b.query_errors.saturating_sub(a.query_errors);
    let mut p = json!({
        "t": b.ts_ms,
        "secs": secs,
        "qps": rate(a.queries, b.queries),
        "errors_s": rate(a.query_errors, b.query_errors),
        "error_ratio": if ended > 0 { failed as f64 / ended as f64 } else { 0.0 },
        "rejected_s": rate(a.rejected, b.rejected),
        "partial_s": rate(a.partial, b.partial),
        "p50_ms": ms(&qh, 0.5), "p95_ms": ms(&qh, 0.95), "p99_ms": ms(&qh, 0.99),
        "http_s": rate(a.http, b.http), "http_4xx_s": rate(a.http_4xx, b.http_4xx), "http_5xx_s": rate(a.http_5xx, b.http_5xx),
        "writes_s": rate(a.writes, b.writes), "rows_s": rate(a.written_docs, b.written_docs), "write_errors_s": rate(a.write_errors, b.write_errors),
        "write_p99_ms": ms(&wh, 0.99),
        "slow_s": rate(a.slow, b.slow),
        "cpu": ((b.cpu_s - a.cpu_s) / secs).max(0.0),
        "by_kind": by_kind,
        "latency_hist": qh,
    });
    for (k, v) in &b.gauges {
        p[*k] = json!(v);
    }
    p
}

// --------------------------------------------------------- the process

/// The process's resident memory in bytes, and its CPU time (user and
/// system) in seconds.
pub fn process_usage() -> (Option<u64>, f64) {
    // SAFETY: getrusage writes the struct it is given.
    let cpu = unsafe {
        let mut u: libc::rusage = std::mem::zeroed();
        if libc::getrusage(libc::RUSAGE_SELF, &mut u) == 0 {
            let t = |tv: libc::timeval| tv.tv_sec as f64 + tv.tv_usec as f64 / 1e6;
            t(u.ru_utime) + t(u.ru_stime)
        } else {
            0.0
        }
    };
    (rss(), cpu)
}

#[cfg(target_os = "linux")]
fn rss() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/statm").ok()?;
    let pages: u64 = s.split_whitespace().nth(1)?.parse().ok()?;
    // SAFETY: sysconf has no preconditions.
    let page = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    Some(pages * page.max(4096) as u64)
}

#[cfg(target_os = "macos")]
fn rss() -> Option<u64> {
    // SAFETY: getrusage writes the struct it is given. On macOS
    // ru_maxrss is the peak, in bytes.
    unsafe {
        let mut u: libc::rusage = std::mem::zeroed();
        (libc::getrusage(libc::RUSAGE_SELF, &mut u) == 0).then_some(u.ru_maxrss as u64)
    }
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn rss() -> Option<u64> {
    None
}

/// The space of the file system holding `path`: total, free, and
/// available to this user, in bytes.
pub fn disk(path: &std::path::Path) -> Option<(u64, u64, u64)> {
    use std::os::unix::ffi::OsStrExt;
    let dir = if path.is_dir() { path.to_path_buf() } else { path.parent().filter(|p| !p.as_os_str().is_empty()).map_or_else(|| std::path::PathBuf::from("."), |p| p.to_path_buf()) };
    let c = std::ffi::CString::new(dir.as_os_str().as_bytes()).ok()?;
    // SAFETY: statvfs reads a NUL-terminated path and writes the struct.
    unsafe {
        let mut s: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(c.as_ptr(), &mut s) != 0 {
            return None;
        }
        let f = s.f_frsize as u64;
        Some((s.f_blocks as u64 * f, s.f_bfree as u64 * f, s.f_bavail as u64 * f))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_differing_in_values_share_a_fingerprint() {
        let a = fingerprint("SELECT id FROM t WHERE MATCH(body, 'laptop') AND price < 100 LIMIT 10");
        let b = fingerprint("SELECT  id FROM t\nWHERE MATCH(body, 'red ''phone''') AND price < 2.5e3 LIMIT 5");
        assert_eq!(a, b);
        assert_eq!(a, "SELECT id FROM t WHERE MATCH(body, ?) AND price < ? LIMIT ?");
        assert_eq!(fingerprint("SELECT x1 FROM t2 WHERE id IN (1, 2, 3)"), "SELECT x1 FROM t2 WHERE id IN (?...)");
        assert_eq!(fingerprint("SELECT a FROM t WHERE id IN (1, 2)"), fingerprint("SELECT a FROM t WHERE id IN (7, 8, 9, 10)"));
        assert_eq!(fingerprint("SELECT \"a b\" FROM t WHERE x = -3"), "SELECT \"a b\" FROM t WHERE x = ?");
        assert_eq!(fingerprint("SELECT a-1 FROM t"), "SELECT a-? FROM t");
        assert_eq!(fingerprint("SELECT 'é' AS ü FROM t"), "SELECT ? AS ü FROM t");
    }

    #[test]
    fn quantiles_interpolate_within_a_bucket() {
        let bounds = [1.0, 2.0, 4.0];
        assert_eq!(quantile(&bounds, &[0, 0, 0, 0], 0.5), None);
        assert_eq!(quantile(&bounds, &[10, 0, 0, 0], 0.5), Some(0.5));
        assert_eq!(quantile(&bounds, &[0, 10, 0, 0], 1.0), Some(2.0));
        assert_eq!(quantile(&bounds, &[5, 0, 5, 0], 0.75), Some(3.0));
        assert_eq!(quantile(&bounds, &[0, 0, 0, 3], 0.99), Some(4.0));
    }

    #[test]
    fn statements_are_ranked_by_time_spent() {
        let s = Statements::default();
        for _ in 0..3 {
            s.add("A", "a", "bm25", "ok", 0.001, 2, false);
        }
        s.add("B", "b", "vector", "error", 0.5, 0, false);
        let j = s.list(10);
        assert_eq!(j["statements"][0]["fingerprint"], "B");
        assert_eq!(j["statements"][0]["errors"], 1);
        assert_eq!(j["statements"][1]["calls"], 3);
        assert_eq!(j["statements"][1]["rows_per_call"], 2.0);
        s.reset();
        assert_eq!(s.list(10)["count"], 0);
    }

    #[test]
    fn a_point_is_the_rates_between_samples() {
        let t = Instant::now();
        let mut a = Sample { at: Some(t), queries: 10, query_hist: vec![0; 15], ..Default::default() };
        a.by_kind.insert("bm25", 10);
        let mut b = Sample { at: Some(t + Duration::from_secs(2)), ts_ms: 5, queries: 30, query_errors: 2, query_hist: vec![0; 15], ..Default::default() };
        b.by_kind.insert("bm25", 30);
        b.query_hist[3] = 20; // all at most 10 ms
        b.gauges.insert("docs", 7.0);
        let series = Series::default();
        assert!(series.add(a).is_none());
        let p = series.add(b).unwrap();
        assert_eq!(p["qps"], 10.0);
        assert_eq!(p["error_ratio"], 0.1);
        assert_eq!(p["by_kind"]["bm25"], 10.0);
        assert_eq!(p["docs"], 7.0);
        let p50 = p["p50_ms"].as_f64().unwrap();
        assert!(p50 > 5.0 && p50 <= 10.0, "{p50}");
        assert_eq!(series.since(0).len(), 1);
        assert!(series.since(5).is_empty());
    }

    #[tokio::test]
    async fn a_cancelled_query_ends_with_its_error() {
        let reg = Arc::new(LiveQueries::default());
        let live = reg.start("SELECT 1", "filter", false);
        assert_eq!(reg.len(), 1);
        let id = live.query.id;
        let r2 = reg.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            assert!(r2.cancel(id));
        });
        let r: Result<(), _> = LiveQueries::run(&live, std::future::pending()).await;
        assert_eq!(r.unwrap_err().status, 499);
        drop(live);
        assert!(reg.is_empty());
        assert!(!reg.cancel(id));
    }

    #[tokio::test]
    async fn spans_land_in_the_request_s_trace() {
        let t = Trace::new();
        traced(t.clone(), async {
            let t0 = Instant::now();
            record("execute", t0, "ok");
            let _c = CallSpan { name: rpc_name::<crate::pb::QueryResponse>(), shard: 1, replica: "http://r".into(), t0, again: true, outcome: None };
        })
        .await;
        record("outside", Instant::now(), "ok");
        let spans = t.spans();
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].name, "execute");
        assert_eq!(spans[1].name, "Query");
        assert_eq!(spans[1].outcome, "outrun");
        assert_eq!(spans[1].shard, Some(1));
    }

    #[test]
    fn the_process_and_its_disk_are_measured() {
        let (rss, cpu) = process_usage();
        assert!(cpu >= 0.0);
        if cfg!(any(target_os = "linux", target_os = "macos")) {
            assert!(rss.unwrap() > 0);
        }
        let (total, free, avail) = disk(std::path::Path::new(".")).unwrap();
        assert!(total >= free && free >= avail.min(free));
    }
}
