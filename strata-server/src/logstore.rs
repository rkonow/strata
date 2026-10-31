//! The server's own logs, kept in Strata.
//!
//! Every line the server logs and every query it answers go to a Strata
//! database of their own, its log store (`<db>.logs` beside the database
//! it serves; `--log-store`), in two tables:
//!
//! - `logs`, a row per line: its time (`ts`, and `minute` and `hour`,
//!   whole minutes and hours since 1970, for `GROUP BY`), the server it
//!   was logged on (`node`),
//!   `level`, `target` (the module), `message`, the event's other `fields`
//!   as JSON text (searchable with `MATCH`), and the request it was logged
//!   for (`request_id`, `method`, `route`). An event's `status`, `error`
//!   and `secs` are also columns of their own (`status`, `error`, `ms`).
//! - `queries`, a row per query answered: its time, `node`, `request_id`,
//!   `kind`, `outcome`, whether it was a shard's part of a coordinator's
//!   query (`shard`), its `sql`, `fingerprint` (its statement's shape,
//!   [`crate::telemetry::fingerprint`]), `ms`, `n_rows`, `error`, and the
//!   rest of what the query log keeps of it, its trace among them, as a
//!   JSON object (`detail`).
//!
//! They are queried with SQL like any other table (`POST /v1/logs/query`):
//!
//! ```sql
//! SELECT ts, node, level, message FROM logs
//! WHERE level IN ('warn', 'error') AND MATCH(message, 'replica OR shard') AND ts > '2026-10-31T06:00:00Z'
//! ORDER BY ts DESC LIMIT 100;
//! SELECT minute, level, COUNT(*) FROM logs WHERE ts > '2026-10-31' GROUP BY minute, level;
//! SELECT fingerprint, COUNT(*) AS n, AVG(ms), MAX(ms) FROM queries GROUP BY fingerprint ORDER BY 3 DESC LIMIT 20;
//! ```
//!
//! A coordinator answers such a query for the whole cluster
//! ([`cluster_query`]): it asks every replica of every shard (`Logs` over
//! gRPC, in shard mode) and its own store, and merges their answers as it
//! merges its shards' ([`strata::cluster::merge`]), so that `COUNT(*)` and
//! `GROUP BY` count every server and `ORDER BY ts` interleaves their lines.
//! Such a query reads one table (a join or a subquery runs on one server's
//! store), each server scores BM25 by its own rows' statistics, and a
//! server that does not answer is named in the answer, which goes on
//! without it.
//!
//! Lines come from the subscriber ([`obs::sink`]), and queries from the
//! query log ([`crate::telemetry::QueryLog::keep`]): every slow or failed
//! one, and at most [`LogOptions::queries_per_sec`] a second of the others.
//! A writer thread writes what waits every second, or as soon as
//! [`obs::SINK_BATCH`] lines wait, in one transaction, without syncing it:
//! a crash loses nothing written, a power cut the latest lines. What the
//! store's own threads log stays out of it ([`obs::quiet_thread`]): its
//! writes, flushes and merges log as any database's do, and storing those
//! lines would write again. Neither do its queries count as the server's
//! (they are not in the query log), and the log viewer's requests are not
//! logged as answered ([`crate::http`]), so reading the logs does not write
//! them.
//!
//! Every 30 seconds a maintenance thread deletes the rows older than
//! [`LogOptions::retain`] and, while the store takes more than
//! [`LogOptions::max_bytes`] (its live pages and write-ahead log), the
//! oldest quarter of what is left, then merges, which drops the deleted
//! rows and frees their pages for the next.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::{Duration, Instant};

use futures::future::join_all;
use serde_json::{json, Map, Value as Json};
use strata::db::Db;
use strata::query::exec::{ExecOptions, ResultSet};
use strata::query::{bind_command, compound, Command, Params, Select};
use strata::schema::Schema;
use strata::storage::{DbOptions, MergePolicy};
use strata::value::{Document, Value};
use tokio::sync::Semaphore;

use crate::coord::Coordinator;
use crate::grpc::{params_to_pb, result_from_pb};
use crate::metrics::Family;
use crate::obs::{self, LogLine};
use crate::telemetry::Finished;
use crate::{json as js, pb, ApiError};

/// The table of log lines.
pub const LOGS: &str = "logs";
/// The table of queries answered.
pub const QUERIES: &str = "queries";

const LOGS_SQL: &str = "CREATE TABLE logs (id INT PRIMARY KEY, ts TIMESTAMP NOT NULL, minute INT, hour INT, node KEYWORD, level KEYWORD, target KEYWORD, \
     message TEXT, fields TEXT, request_id KEYWORD, method KEYWORD, route KEYWORD, status INT, error TEXT, ms FLOAT)";
const QUERIES_SQL: &str = "CREATE TABLE queries (id INT PRIMARY KEY, ts TIMESTAMP NOT NULL, minute INT, hour INT, node KEYWORD, request_id KEYWORD, kind KEYWORD, \
     outcome KEYWORD, shard BOOL, sql TEXT, fingerprint KEYWORD, ms FLOAT, n_rows INT, error TEXT, detail KEYWORD OPTIONS(indexed = false))";

/// How often the writer writes what waits.
pub const WRITE_EVERY: Duration = Duration::from_secs(1);
/// How often retention runs.
pub const TRIM_EVERY: Duration = Duration::from_secs(30);
/// Lines written in one transaction at most.
const BATCH: usize = 8192;
/// Queries that may wait for the writer; more are dropped and counted.
const QUEUED_QUERIES: usize = 50_000;
/// The most rows a query of the store answers, and the groups it makes.
pub const MAX_ROWS: usize = 10_000;
const MAX_GROUPS: usize = 100_000;
/// Queries of the store running at once; more wait.
const RUNNING: usize = 4;

/// The tables' schemas, as a store this version makes holds them.
pub fn schemas() -> &'static [Arc<Schema>] {
    static S: OnceLock<Vec<Arc<Schema>>> = OnceLock::new();
    S.get_or_init(|| {
        [LOGS_SQL, QUERIES_SQL]
            .iter()
            .map(|sql| match bind_command(sql, &[], &Params::new()) {
                Ok(Command::CreateTable { schema, .. }) => Arc::new(schema),
                other => panic!("the log store's schema does not bind: {other:?}"),
            })
            .collect()
    })
}

fn schema_of(table: &str) -> &'static Arc<Schema> {
    schemas().iter().find(|s| s.table == table).expect("a table of the log store")
}

/// The error a server without a log store answers a query of it with.
pub fn none_here() -> ApiError {
    ApiError { status: 409, message: "this server keeps no log store (it was started with --log-store off, or is a coordinator without --log-store)".into() }
}

/// Where the log store of the database at `db` is by default.
pub fn file_of(db: &Path) -> PathBuf {
    let mut p = db.as_os_str().to_owned();
    p.push(".logs");
    p.into()
}

/// This machine's name, as `gethostname` gives it ("localhost" without
/// one).
pub fn hostname() -> String {
    let mut buf = [0u8; 256];
    // SAFETY: the buffer is writable for its length, and gethostname
    // writes at most that many bytes.
    let r = unsafe { libc::gethostname(buf.as_mut_ptr().cast(), buf.len()) };
    let n = buf.iter().position(|&b| b == 0).unwrap_or(buf.len());
    match std::str::from_utf8(&buf[..n]) {
        Ok(s) if r == 0 && !s.is_empty() => s.to_string(),
        _ => "localhost".into(),
    }
}

/// What a log store keeps, and where.
#[derive(Clone, Debug)]
pub struct LogOptions {
    /// Its database, created if missing.
    pub path: PathBuf,
    /// The name of this server in its rows (`node`).
    pub node: String,
    /// Rows older than this are deleted.
    pub retain: Duration,
    /// The most bytes it takes, its live pages and its write-ahead log;
    /// past them the oldest rows are deleted (0: no limit).
    pub max_bytes: u64,
    /// The queries a second it keeps of those that were neither slow nor
    /// failed, which it always keeps (0: none of those).
    pub queries_per_sec: u64,
    /// Take the server's log lines ([`obs::sink`]). Only one store of a
    /// process takes them: the first opened.
    pub capture: bool,
}

impl LogOptions {
    pub fn new(path: PathBuf, node: String) -> LogOptions {
        LogOptions { path, node, retain: Duration::from_secs(7 * 86_400), max_bytes: 1 << 30, queries_per_sec: 200, capture: true }
    }
}

/// Whether a store takes the process's lines ([`LogOptions::capture`]).
static CAPTURING: AtomicBool = AtomicBool::new(false);

/// A server's log store.
pub struct LogStore {
    db: Arc<Db>,
    opts: LogOptions,
    capture: bool,
    /// Queries waiting for the writer.
    queued: Mutex<Vec<Arc<Finished>>>,
    query_rate: obs::RateLimit,
    gate: Arc<Semaphore>,
    closed: AtomicBool,
    stats: Stats,
    writer: Mutex<Option<std::thread::JoinHandle<()>>>,
}

#[derive(Default)]
struct Stats {
    lines: AtomicU64,
    queries: AtomicU64,
    /// Queries past `queries_per_sec`, and past [`QUEUED_QUERIES`].
    sampled_out: AtomicU64,
    dropped_queries: AtomicU64,
    /// Rows a failed write lost.
    failed: AtomicU64,
    writes: AtomicU64,
    /// Rows deleted for their age, and for the store's size.
    expired: AtomicU64,
    trimmed: AtomicU64,
    last_write_ms: AtomicU64,
    last_error: Mutex<Option<String>>,
}

fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|e| e.into_inner())
}

/// How a log store's database is opened: published snapshots, so that the
/// latest lines are read; no sync at commits.
fn db_options() -> DbOptions {
    DbOptions { sync: false, ..crate::server::db_options(false) }
}

/// The store's database at `path`, made if missing, with both tables.
fn open_db(path: &Path) -> Result<Db, String> {
    let at = |e: strata::Error| format!("log store {}: {e}", path.display());
    let db = match path.exists() {
        true => Db::open(path, db_options()).map_err(at)?,
        false => Db::create(path, (**schema_of(LOGS)).clone(), db_options()).map_err(at)?,
    };
    for want in schemas() {
        match db.catalog().into_iter().find(|t| t.table == want.table) {
            None => db.create_table((**want).clone()).map_err(at)?,
            Some(have) => {
                let same = |f: &strata::schema::Field| have.fields.iter().any(|h| h.name == f.name && std::mem::discriminant(&h.ty) == std::mem::discriminant(&f.ty) && h.logical == f.logical);
                if have.primary_key.map(|k| &have.fields[k].name) != want.primary_key.map(|k| &want.fields[k].name) || !want.fields.iter().all(same) {
                    return Err(format!(
                        "log store {}: its table {} is not the one this server writes (another version made it): move it aside, or pass --log-store another path",
                        path.display(),
                        want.table
                    ));
                }
            }
        }
    }
    Ok(db)
}

/// The largest id in `table` (0: none).
fn max_id(db: &Db, table: &str) -> Result<i64, String> {
    let rs = db.query(&format!("SELECT MAX(id) FROM {table}"), &Params::new()).map_err(|e| format!("log store: {e}"))?;
    Ok(rs.rows.first().and_then(|r| r.first()).and_then(Value::as_i64).unwrap_or(0))
}

/// A write transaction, waiting while another (a delete, a merge's
/// install) holds the writer.
fn begin(db: &Db) -> strata::Result<strata::storage::WriteTxn<'_>> {
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        match db.storage().begin() {
            Err(strata::Error::Busy(_)) if Instant::now() < until => std::thread::sleep(Duration::from_millis(2)),
            r => return r,
        }
    }
}

/// A row of `schema` from its named values (the others NULL).
fn row(schema: &Schema, values: Vec<(&str, Value)>) -> Document {
    let mut d = vec![Value::Null; schema.fields.len()];
    for (name, v) in values {
        if let Some(i) = schema.field_index(name) {
            d[i] = v;
        }
    }
    d
}

fn text(s: Option<&str>) -> Value {
    s.map_or(Value::Null, |s| Value::Str(s.to_string()))
}

fn float(x: Option<f64>) -> Value {
    x.filter(|x| x.is_finite()).map_or(Value::Null, Value::Float)
}

/// The next id after `last` for a row of time `ts` (µs): the time, unless
/// a row of the same microsecond or a later one came before.
fn next_id(last: &mut i64, ts: i64) -> i64 {
    *last = ts.max(*last + 1);
    *last
}

fn line_row(schema: &Schema, node: &str, l: &LogLine, last: &mut i64) -> Document {
    let ts = l.ts_ms as i64 * 1000;
    let f = &l.fields;
    let str_of = |k: &str| f.get(k).map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string));
    let ms = f.get("secs").and_then(Json::as_f64).map(|s| s * 1e3).or_else(|| f.get("ms").and_then(Json::as_f64));
    row(
        schema,
        vec![
            ("id", Value::Int(next_id(last, ts))),
            ("ts", Value::Timestamp(ts)),
            ("minute", Value::Int(ts.div_euclid(60_000_000))),
            ("hour", Value::Int(ts.div_euclid(3_600_000_000))),
            ("node", Value::Str(node.to_string())),
            ("level", Value::Str(l.level.to_string())),
            ("target", Value::Str(l.target.clone())),
            ("message", Value::Str(l.message.clone())),
            ("fields", if f.is_empty() { Value::Null } else { Value::Str(Json::Object(f.clone()).to_string()) }),
            ("request_id", text(l.request_id.as_deref())),
            ("method", text(l.method.as_deref())),
            ("route", text(l.route.as_deref())),
            ("status", f.get("status").and_then(Json::as_i64).map_or(Value::Null, Value::Int)),
            ("error", str_of("error").map_or(Value::Null, Value::Str)),
            ("ms", float(ms)),
        ],
    )
}

/// The query log's fields that are columns of `queries`; the rest go to
/// `detail`.
const QUERY_COLUMNS: [&str; 10] = ["ts_ms", "request_id", "kind", "outcome", "shard", "sql", "fingerprint", "ms", "rows", "error"];

fn query_row(schema: &Schema, node: &str, q: &Finished, last: &mut i64) -> Document {
    let ts = q.ts_ms as i64 * 1000;
    let detail = match serde_json::to_value(q) {
        Ok(Json::Object(mut m)) => {
            m.retain(|k, v| !QUERY_COLUMNS.contains(&k.as_str()) && !v.is_null());
            (!m.is_empty()).then(|| Json::Object(m).to_string())
        }
        _ => None,
    };
    row(
        schema,
        vec![
            ("id", Value::Int(next_id(last, ts))),
            ("ts", Value::Timestamp(ts)),
            ("minute", Value::Int(ts.div_euclid(60_000_000))),
            ("hour", Value::Int(ts.div_euclid(3_600_000_000))),
            ("node", Value::Str(node.to_string())),
            ("request_id", text(q.request_id.as_deref())),
            ("kind", Value::Str(q.kind.to_string())),
            ("outcome", Value::Str(q.outcome.to_string())),
            ("shard", Value::Bool(q.shard)),
            ("sql", Value::Str(q.sql.clone())),
            ("fingerprint", Value::Str(q.fingerprint.clone())),
            ("ms", float(Some(q.ms))),
            ("n_rows", Value::Int(q.rows as i64)),
            ("error", text(q.error.as_deref())),
            ("detail", detail.map_or(Value::Null, Value::Str)),
        ],
    )
}

impl LogStore {
    /// Opens the store at `opts.path` (made if missing) and starts its
    /// writer and maintenance threads.
    pub fn open(opts: LogOptions) -> Result<Arc<LogStore>, String> {
        if opts.node.is_empty() {
            return Err("a log store needs the name of its server (--node-name)".into());
        }
        let db = Arc::new(open_db(&opts.path)?);
        let last = [max_id(&db, LOGS)?, max_id(&db, QUERIES)?];
        let capture = opts.capture && !CAPTURING.swap(true, Ordering::SeqCst);
        let store = Arc::new(LogStore {
            db,
            capture,
            queued: Mutex::new(Vec::new()),
            query_rate: obs::RateLimit::new(opts.queries_per_sec),
            gate: Arc::new(Semaphore::new(RUNNING)),
            closed: AtomicBool::new(false),
            stats: Stats::default(),
            writer: Mutex::new(None),
            opts,
        });
        let w = Arc::downgrade(&store);
        let writer = std::thread::Builder::new().name("strata-log-writer".into()).spawn(move || write_loop(w, last)).map_err(|e| format!("log store: {e}"))?;
        *lock(&store.writer) = Some(writer);
        let m = Arc::downgrade(&store);
        std::thread::Builder::new().name("strata-log-maintenance".into()).spawn(move || maintain_loop(m)).map_err(|e| format!("log store: {e}"))?;
        Ok(store)
    }

    /// The store's database (to read it as any other: tests, tools).
    pub fn db(&self) -> &Arc<Db> {
        &self.db
    }

    pub fn options(&self) -> &LogOptions {
        &self.opts
    }

    /// Whether it takes the process's lines ([`LogOptions::capture`]).
    pub fn captures(&self) -> bool {
        self.capture
    }

    /// Keeps a query that ended ([`crate::telemetry::QueryLog::keep`]):
    /// a slow or failed one always, the others up to
    /// [`LogOptions::queries_per_sec`] a second.
    pub fn keep_query(&self, q: &Arc<Finished>, slow: bool) {
        if !slow && q.outcome == "ok" && self.query_rate.admit().is_none() {
            self.stats.sampled_out.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let mut queued = lock(&self.queued);
        if queued.len() >= QUEUED_QUERIES {
            self.stats.dropped_queries.fetch_add(1, Ordering::Relaxed);
            return;
        }
        queued.push(q.clone());
    }

    /// Writes what waits now, rather than at the writer's next second.
    pub fn flush(&self) {
        let mut last = [0; 2];
        // The writer's ids are past every row written; reading them again
        // keeps this write's after them.
        if let (Ok(a), Ok(b)) = (max_id(&self.db, LOGS), max_id(&self.db, QUERIES)) {
            last = [a, b];
        }
        let _one = lock(&self.writer);
        self.write_waiting(&mut last);
    }

    /// Writes what waits and stops the writer (the server is stopping).
    pub fn close(&self) {
        self.closed.store(true, Ordering::Release);
        obs::sink().wake();
        let writer = lock(&self.writer).take();
        if let Some(w) = writer {
            let _ = w.join();
        }
    }

    /// Writes the lines and queries waiting, a batch per transaction.
    fn write_waiting(&self, last: &mut [i64; 2]) {
        loop {
            let lines = if self.capture { obs::sink().take(BATCH) } else { Vec::new() };
            let queries = std::mem::take(&mut *lock(&self.queued));
            if lines.is_empty() && queries.is_empty() {
                return;
            }
            let more = lines.len() == BATCH;
            match self.write(&lines, &queries, last) {
                Ok(()) => {
                    self.stats.lines.fetch_add(lines.len() as u64, Ordering::Relaxed);
                    self.stats.queries.fetch_add(queries.len() as u64, Ordering::Relaxed);
                    self.stats.writes.fetch_add(1, Ordering::Relaxed);
                    self.stats.last_write_ms.store(crate::telemetry::now_ms(), Ordering::Relaxed);
                }
                Err(e) => {
                    static LIMIT: obs::RateLimit = obs::RateLimit::new(1);
                    self.stats.failed.fetch_add((lines.len() + queries.len()) as u64, Ordering::Relaxed);
                    if LIMIT.admit().is_some() {
                        tracing::warn!(error = %e, rows = lines.len() + queries.len(), "the log store could not write; those rows are lost");
                    }
                    *lock(&self.stats.last_error) = Some(e.to_string());
                }
            }
            if !more {
                return;
            }
        }
    }

    /// Lines and queries for this store, which are written by
    /// [`Self::write_waiting`]; tests give lines this way too.
    pub fn write_lines(&self, lines: &[Arc<LogLine>]) -> strata::Result<()> {
        let mut last = [max_id(&self.db, LOGS).map_err(strata::Error::Query)?, 0];
        let _one = lock(&self.writer);
        self.write(lines, &[], &mut last)
    }

    fn write(&self, lines: &[Arc<LogLine>], queries: &[Arc<Finished>], last: &mut [i64; 2]) -> strata::Result<()> {
        let catalog = self.db.catalog();
        let table = |name: &str| catalog.iter().find(|t| t.table == name).cloned().ok_or_else(|| strata::Error::Query(format!("the log store has no table {name}")));
        let (logs, queried) = (table(LOGS)?, table(QUERIES)?);
        let node = &self.opts.node;
        let line_rows: Vec<Document> = lines.iter().map(|l| line_row(&logs, node, l, &mut last[0])).collect();
        let query_rows: Vec<Document> = queries.iter().map(|q| query_row(&queried, node, q, &mut last[1])).collect();
        let mut t = begin(&self.db)?;
        if !line_rows.is_empty() {
            let mut tt = t.table(LOGS)?;
            for d in line_rows {
                tt.insert(d)?;
            }
        }
        if !query_rows.is_empty() {
            let mut tt = t.table(QUERIES)?;
            for d in query_rows {
                tt.insert(d)?;
            }
        }
        t.commit()
    }

    /// The bytes the store takes: its live pages and its write-ahead log.
    pub fn bytes(&self) -> u64 {
        let f = self.db.storage().file_stats();
        let free = (f.free_pages + f.pending_pages) * strata::storage::pager::PAGE as u64;
        f.file_bytes.saturating_sub(free) + f.wal_bytes
    }

    /// Deletes the rows past retention, and the oldest quarter while the
    /// store is past its size; then merges. Returns the rows deleted.
    pub fn trim(&self) -> strata::Result<u64> {
        let now = strata::timestamp::now();
        let mut n = 0;
        let cut = now - self.opts.retain.as_micros().min(i64::MAX as u128) as i64;
        for t in [LOGS, QUERIES] {
            let d = self.delete_before(t, cut)?;
            self.stats.expired.fetch_add(d, Ordering::Relaxed);
            n += d;
        }
        self.merge()?;
        // Each round deletes a quarter of the time each table spans; the
        // merge after it frees the pages.
        let mut rounds = 0;
        while self.opts.max_bytes > 0 && self.bytes() > self.opts.max_bytes && rounds < 8 {
            rounds += 1;
            let mut deleted = 0;
            for t in [LOGS, QUERIES] {
                let rs = self.db.query(&format!("SELECT MIN(ts), MAX(ts) FROM {t}"), &Params::new())?;
                let time = |i: usize| match rs.rows.first().map(|r| &r[i]) {
                    Some(Value::Timestamp(t) | Value::Int(t)) => Some(*t),
                    _ => None,
                };
                let (Some(lo), Some(hi)) = (time(0), time(1)) else { continue };
                let d = self.delete_before(t, lo + (hi - lo) / 4 + 1)?;
                self.stats.trimmed.fetch_add(d, Ordering::Relaxed);
                deleted += d;
            }
            n += deleted;
            self.merge()?;
            if deleted == 0 {
                break;
            }
        }
        Ok(n)
    }

    fn delete_before(&self, table: &str, cut: i64) -> strata::Result<u64> {
        let sql = format!("DELETE FROM {table} WHERE ts < :cut");
        let params = Params::new().set("cut", Value::Timestamp(cut));
        let until = Instant::now() + Duration::from_secs(10);
        loop {
            match self.db.sql(&sql, &params) {
                Err(strata::Error::Busy(_)) if Instant::now() < until => std::thread::sleep(Duration::from_millis(5)),
                r => {
                    return r.map(|r| match r {
                        strata::db::SqlResult::Written(n) => n,
                        strata::db::SqlResult::Rows(_) => 0,
                    })
                }
            }
        }
    }

    fn merge(&self) -> strata::Result<()> {
        // Segments mostly deleted are rewritten (max_deleted_ratio);
        // wholly deleted ones dropped.
        self.db.storage().merge(&MergePolicy::default()).map(drop)
    }

    /// Runs a query of the store: SQL over its tables, any query one
    /// database runs; in shard mode ([`ExecOptions::shard`]), a query of
    /// one table, for a coordinator to merge ([`cluster_query`]).
    pub async fn query(&self, sql: &str, params: &Params, shard: bool, timeout: Duration) -> Result<(ResultSet, f64), ApiError> {
        let t0 = Instant::now();
        let deadline = t0 + timeout;
        let permit = tokio::time::timeout(timeout, self.gate.clone().acquire_owned())
            .await
            .map_err(|_| ApiError { status: 429, message: format!("{RUNNING} queries of the log store are running: try again") })?
            .map_err(ApiError::internal)?;
        let (db, sql, params) = (self.db.clone(), sql.to_string(), params.clone());
        let r = tokio::task::spawn_blocking(move || -> Result<ResultSet, ApiError> {
            let _held = permit;
            let opts = ExecOptions {
                shard,
                deadline: Some(deadline),
                threads: 2,
                max_rows: Some(if shard { MAX_ROWS.max(compound::MAX_ROWS + 1) } else { MAX_ROWS }),
                max_groups: Some(MAX_GROUPS),
                ..db.exec.clone()
            };
            let snap = db.snapshot();
            if !shard {
                return Ok(db.query_at(&snap, &sql, &params, &opts)?);
            }
            let (schema, select) = bind_select(&db.catalog(), &sql, &params)?;
            let view = snap.table(&schema.table).ok_or_else(|| ApiError::bad(format!("query error: no such table: {}", schema.table)))?;
            Ok(db.execute(&view, &select, &opts)?)
        })
        .await
        .map_err(|e| ApiError::internal(format!("a query of the log store: {e}")))??;
        Ok((r, t0.elapsed().as_secs_f64() * 1e3))
    }

    /// What `GET /v1/logs/store` answers, and `/v1/cluster` shows of each
    /// server.
    pub fn status(&self) -> Json {
        let s = &self.stats;
        let snap = self.db.snapshot();
        let rows = |t: &str| snap.table(t).map_or(0, |v| v.n_live_docs());
        let (waiting, dropped) = if self.capture { obs::sink().counts() } else { (0, 0) };
        let last = s.last_write_ms.load(Ordering::Relaxed);
        json!({
            "node": self.opts.node,
            "path": self.opts.path.display().to_string(),
            "captures": self.capture,
            "retain_s": self.opts.retain.as_secs(),
            "max_bytes": self.opts.max_bytes,
            "queries_per_sec": self.opts.queries_per_sec,
            "bytes": self.bytes(),
            "tables": {LOGS: {"rows": rows(LOGS)}, QUERIES: {"rows": rows(QUERIES)}},
            "written": {"lines": s.lines.load(Ordering::Relaxed), "queries": s.queries.load(Ordering::Relaxed), "writes": s.writes.load(Ordering::Relaxed)},
            "waiting": {"lines": waiting, "queries": lock(&self.queued).len()},
            "dropped": {"lines": dropped, "queries": s.dropped_queries.load(Ordering::Relaxed), "failed": s.failed.load(Ordering::Relaxed)},
            "sampled_out": s.sampled_out.load(Ordering::Relaxed),
            "deleted": {"expired": s.expired.load(Ordering::Relaxed), "for_size": s.trimmed.load(Ordering::Relaxed)},
            "last_write_ms": (last > 0).then_some(last),
            "last_error": *lock(&s.last_error),
        })
    }

    /// Its families for `/metrics`.
    pub fn metrics(&self) -> Vec<Family> {
        let s = &self.stats;
        let snap = self.db.snapshot();
        let rows = |t: &str| snap.table(t).map_or(0, |v| v.n_live_docs()) as f64;
        let (waiting, dropped) = if self.capture { obs::sink().counts() } else { (0, 0) };
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed) as f64;
        let by = |pairs: &[(&str, f64)], label: &str| pairs.iter().map(|(k, v)| (format!("{label}=\"{k}\""), *v)).collect();
        vec![
            Family { name: "strata_log_store_rows", ty: "gauge", help: "Rows in the log store, by table.", values: by(&[(LOGS, rows(LOGS)), (QUERIES, rows(QUERIES))], "table") },
            Family { name: "strata_log_store_written_total", ty: "counter", help: "Rows the log store wrote, by table.", values: by(&[(LOGS, get(&s.lines)), (QUERIES, get(&s.queries))], "table") },
            Family::one("strata_log_store_bytes", "gauge", "Bytes the log store takes: its live pages and write-ahead log.", self.bytes() as f64),
            Family::one("strata_log_store_waiting_lines", "gauge", "Log lines waiting for the log store's writer.", waiting as f64),
            Family {
                name: "strata_log_store_dropped_total",
                ty: "counter",
                help: "Rows the log store did not keep: lines and queries past what may wait, rows of failed writes, and queries past --log-queries a second.",
                values: by(&[("lines", dropped as f64), ("queries", get(&s.dropped_queries)), ("failed", get(&s.failed)), ("sampled", get(&s.sampled_out))], "reason"),
            },
            Family { name: "strata_log_store_deleted_total", ty: "counter", help: "Rows the log store deleted, past --log-retain or over --log-store-mb.", values: by(&[("expired", get(&s.expired)), ("size", get(&s.trimmed))], "reason") },
        ]
    }
}

impl Drop for LogStore {
    fn drop(&mut self) {
        self.closed.store(true, Ordering::Release);
        if self.capture {
            CAPTURING.store(false, Ordering::SeqCst);
        }
    }
}

/// The writer thread: what waits, every [`WRITE_EVERY`] or as soon as a
/// batch of lines waits, until the store closes or is dropped.
fn write_loop(store: Weak<LogStore>, mut last: [i64; 2]) {
    obs::quiet_thread();
    loop {
        obs::sink().wait(WRITE_EVERY);
        let Some(s) = store.upgrade() else { return };
        s.write_waiting(&mut last);
        if s.closed.load(Ordering::Acquire) {
            return;
        }
    }
}

/// The maintenance thread: retention every [`TRIM_EVERY`].
fn maintain_loop(store: Weak<LogStore>) {
    obs::quiet_thread();
    let mut next = Instant::now() + Duration::from_secs(5);
    loop {
        std::thread::sleep(Duration::from_millis(500));
        let Some(s) = store.upgrade() else { return };
        if s.closed.load(Ordering::Acquire) {
            return;
        }
        if Instant::now() < next {
            continue;
        }
        next = Instant::now() + TRIM_EVERY;
        if let Err(e) = s.trim() {
            static LIMIT: obs::RateLimit = obs::RateLimit::new(1);
            if LIMIT.admit().is_some() {
                tracing::warn!(error = %e, "the log store could not delete its old rows");
            }
            *lock(&s.stats.last_error) = Some(format!("retention: {e}"));
        }
    }
}

/// `sql` bound against `catalog` as a query of one table: what a store
/// answers in shard mode, and a coordinator merges.
pub fn bind_select(catalog: &[Arc<Schema>], sql: &str, params: &Params) -> Result<(Arc<Schema>, Select), ApiError> {
    let tables: Vec<&Schema> = catalog.iter().map(|t| &**t).collect();
    let plan = compound::plan_read(sql, &tables, params, &|_, _| None)?;
    if plan.root.uses_operators() {
        return Err(ApiError::bad(
            "query error: a query of every server's logs reads one table, without joins, subqueries, WITH or DISTINCT; ask one server's store for those (\"scope\": \"local\")",
        ));
    }
    let schema = catalog.iter().find(|t| t.table.eq_ignore_ascii_case(plan.table())).cloned().ok_or_else(|| ApiError::bad(format!("query error: no such table: {}", plan.table())))?;
    let select = plan.to_select(&schema)?;
    Ok((schema, select))
}

/// What one server answered of a cluster's query of the logs.
struct NodeAnswer {
    url: Option<String>,
    shard: Option<usize>,
    ms: f64,
    answer: Result<ResultSet, ApiError>,
}

/// A query of every server's log store, on a coordinator: its own store
/// (`local`, if it keeps one) and every replica of every shard, each in
/// shard mode, merged. Answers as `POST /v1/query` does, with `nodes`:
/// each server asked, its rows, time and error; a server that does not
/// answer is left out of the rows.
pub async fn cluster_query(local: Option<&Arc<LogStore>>, coord: &Coordinator, sql: &str, params: &Params, timeout: Duration) -> Result<Json, ApiError> {
    let t0 = Instant::now();
    let deadline = t0 + timeout;
    let (_, select) = bind_select(schemas(), sql, params)?;
    let mine = async {
        match local {
            Some(s) => {
                let t = Instant::now();
                let answer = s.query(sql, params, true, timeout).await.map(|(rs, _)| rs);
                vec![NodeAnswer { url: None, shard: None, ms: t.elapsed().as_secs_f64() * 1e3, answer }]
            }
            None => Vec::new(),
        }
    };
    let theirs = join_all(coord.replica_clients().into_iter().map(|(shard, url, mut c)| {
        let req = pb::QueryRequest { sql: sql.to_string(), params: params_to_pb(params), shard: true, timeout_ms: deadline.saturating_duration_since(Instant::now()).as_millis() as u32, ..Default::default() };
        async move {
            let t = Instant::now();
            let mut r = tonic::Request::new(req);
            if let Some(id) = obs::current().and_then(|id| id.parse().ok()) {
                r.metadata_mut().insert(obs::HEADER, id);
            }
            let answer = match tokio::time::timeout_at(deadline.into(), c.logs(r)).await {
                Err(_) => Err(ApiError { status: 504, message: "no answer before the deadline".into() }),
                Ok(Err(s)) if s.code() == tonic::Code::Unimplemented => Err(ApiError { status: 501, message: "an older server, without a log store".into() }),
                Ok(Err(s)) => Err(ApiError::from(s)),
                Ok(Ok(a)) => result_from_pb(a.into_inner()),
            };
            NodeAnswer { url: Some(url), shard: Some(shard), ms: t.elapsed().as_secs_f64() * 1e3, answer }
        }
    }));
    let (mut answers, theirs) = tokio::join!(mine, theirs);
    answers.extend(theirs);
    let mut nodes = Vec::new();
    let mut sets = Vec::new();
    let mut labels = Vec::new();
    let mut first_error = None;
    for a in answers {
        let mut n = json!({"url": a.url, "shard": a.shard, "ms": a.ms});
        let label = a.url.clone().unwrap_or_else(|| local.map_or_else(|| "the coordinator".to_string(), |s| s.options().node.clone()));
        if a.url.is_none() {
            n["coordinator"] = json!(true);
            n["node"] = json!(label);
        }
        match a.answer {
            Ok(rs) => {
                n["rows"] = json!(rs.rows.len());
                sets.push(rs);
                labels.push(label);
            }
            Err(e) => {
                n["error"] = json!(e.message);
                n["status"] = json!(e.status);
                // A query that fails everywhere fails: its own error.
                if e.status == 400 {
                    first_error.get_or_insert(e);
                }
            }
        }
        nodes.push(n);
    }
    if sets.is_empty() {
        return Err(first_error.unwrap_or_else(|| ApiError { status: 503, message: "no server's log store answered".into() }));
    }
    let rs = strata::cluster::merge(&select, sets)?;
    let mut j = js::result(&rs, t0.elapsed().as_secs_f64() * 1e3);
    // The merged plan names each answer "shard i": name its server instead.
    if let Some(plan) = j["stats"]["plan"].as_array_mut() {
        for line in plan.iter_mut() {
            let Some(text) = line.as_str() else { continue };
            let named = text.strip_prefix("shard ").and_then(|rest| rest.split_once(": ")).and_then(|(i, rest)| Some(format!("{}: {rest}", labels.get(i.parse::<usize>().ok()?)?)));
            if let Some(named) = named {
                *line = json!(named);
            }
        }
    }
    j["nodes"] = json!(nodes);
    Ok(j)
}

/// A line as the subscriber keeps it, for tests and tools that write
/// lines themselves ([`LogStore::write_lines`]).
pub fn line(ts_ms: u64, level: &'static str, target: &str, message: &str, fields: Map<String, Json>, request_id: Option<&str>) -> Arc<LogLine> {
    Arc::new(LogLine { seq: 0, ts_ms, level, target: target.into(), message: message.into(), fields, request_id: request_id.map(String::from), method: None, route: None })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn store(dir: &Path, name: &str, node: &str) -> Arc<LogStore> {
        LogStore::open(LogOptions { capture: false, ..LogOptions::new(dir.join(name), node.into()) }).unwrap()
    }

    fn fields(j: Json) -> Map<String, Json> {
        j.as_object().cloned().unwrap_or_default()
    }

    #[test]
    fn the_schemas_bind() {
        assert_eq!(schemas().len(), 2);
        assert_eq!(schema_of(LOGS).primary_key.map(|k| schema_of(LOGS).fields[k].name.as_str()), Some("id"));
    }

    #[test]
    fn ids_follow_time_and_never_repeat() {
        let mut last = 0;
        assert_eq!(next_id(&mut last, 100), 100);
        assert_eq!(next_id(&mut last, 100), 101);
        assert_eq!(next_id(&mut last, 50), 102);
        assert_eq!(next_id(&mut last, 500), 500);
    }

    #[tokio::test]
    async fn lines_are_rows_queried_with_sql() {
        let dir = tempfile::tempdir().unwrap();
        let s = store(dir.path(), "a.logs", "node-a");
        let t = 1_793_400_000_000u64;
        s.write_lines(&[
            line(t, "info", "strata_server::server", "serving", fields(json!({"rows": 3})), None),
            line(t + 1, "error", "strata_server::http", "request failed", fields(json!({"status": 500, "error": "disk full"})), Some("r1")),
            line(t + 61_000, "warn", "strata_server::coord", "waiting for the shards", fields(json!({"secs": 0.25})), Some("r1")),
        ])
        .unwrap();
        let q = |sql: &str| {
            let s = s.clone();
            let sql = sql.to_string();
            async move { s.query(&sql, &Params::new(), false, Duration::from_secs(5)).await.unwrap().0 }
        };
        let rs = q("SELECT level, status, error, node FROM logs WHERE MATCH(message, 'failed') ORDER BY ts DESC LIMIT 5").await;
        assert_eq!(rs.rows, vec![vec![Value::Str("error".into()), Value::Int(500), Value::Str("disk full".into()), Value::Str("node-a".into())]]);
        let rs = q("SELECT COUNT(*) FROM logs WHERE request_id = 'r1'").await;
        assert_eq!(rs.rows[0][0], Value::Int(2));
        let rs = q("SELECT minute, COUNT(*) FROM logs GROUP BY minute ORDER BY minute LIMIT 10").await;
        assert_eq!(rs.rows.len(), 2, "{:?}", rs.rows);
        let rs = q("SELECT ms FROM logs WHERE level = 'warn' LIMIT 1").await;
        assert_eq!(rs.rows[0][0], Value::Float(250.0));
        let rs = q("SELECT message FROM logs WHERE MATCH(fields, 'disk') LIMIT 5").await;
        assert_eq!(rs.rows.len(), 1);
    }

    #[tokio::test]
    async fn queries_are_kept_slow_and_failed_always() {
        let dir = tempfile::tempdir().unwrap();
        let s = LogStore::open(LogOptions { capture: false, queries_per_sec: 1, ..LogOptions::new(dir.path().join("q.logs"), "n".into()) }).unwrap();
        let q = |outcome: &'static str, ms: f64| {
            Arc::new(Finished {
                ts_ms: 1_793_400_000_000,
                request_id: Some("rq".into()),
                sql: "SELECT * FROM t WHERE x = 1".into(),
                fingerprint: "SELECT * FROM t WHERE x = ?".into(),
                kind: "filter",
                outcome,
                ms,
                rows: 3,
                shard: false,
                error: None,
                trace: vec![crate::telemetry::Span { name: "execute".into(), shard: None, replica: None, start_us: 0, dur_us: 9, outcome: "ok", again: false }],
            })
        };
        for _ in 0..5 {
            s.keep_query(&q("ok", 1.0), false);
        }
        s.keep_query(&q("ok", 2000.0), true);
        s.keep_query(&q("error", 3.0), false);
        s.flush();
        let rs = s.query("SELECT outcome, ms, fingerprint, detail, shard FROM queries ORDER BY ms DESC LIMIT 10", &Params::new(), false, Duration::from_secs(5)).await.unwrap().0;
        assert_eq!(rs.rows.len(), 3, "one ok a second, the slow one and the failed one: {:?}", rs.rows);
        assert_eq!(rs.rows[0][1], Value::Float(2000.0));
        assert_eq!(rs.rows[0][2], Value::Str("SELECT * FROM t WHERE x = ?".into()));
        let detail: Json = serde_json::from_str(rs.rows[0][3].as_str().unwrap()).unwrap();
        assert_eq!(detail["trace"][0]["name"], "execute");
        assert_eq!(rs.rows[0][4], Value::Bool(false));
        assert_eq!(s.status()["sampled_out"], 4);
    }

    #[tokio::test]
    async fn ids_go_on_after_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let t = 1_793_400_000_000u64;
        {
            let s = store(dir.path(), "r.logs", "n");
            s.write_lines(&[line(t, "info", "x", "one", Map::new(), None), line(t, "info", "x", "two", Map::new(), None)]).unwrap();
            s.close();
        }
        // The same millisecond again: the ids still differ.
        let s = store(dir.path(), "r.logs", "n");
        s.write_lines(&[line(t, "info", "x", "three", Map::new(), None)]).unwrap();
        let rs = s.query("SELECT id FROM logs ORDER BY id LIMIT 10", &Params::new(), false, Duration::from_secs(5)).await.unwrap().0;
        let ids: Vec<i64> = rs.rows.iter().map(|r| r[0].as_i64().unwrap()).collect();
        assert_eq!(ids, vec![t as i64 * 1000, t as i64 * 1000 + 1, t as i64 * 1000 + 2]);
    }

    #[tokio::test]
    async fn retention_deletes_old_rows() {
        let dir = tempfile::tempdir().unwrap();
        let s = LogStore::open(LogOptions { capture: false, retain: Duration::from_secs(3600), ..LogOptions::new(dir.path().join("t.logs"), "n".into()) }).unwrap();
        let now = crate::telemetry::now_ms();
        s.write_lines(&[line(now - 7_200_000, "info", "x", "old", Map::new(), None), line(now, "info", "x", "new", Map::new(), None)]).unwrap();
        assert_eq!(s.trim().unwrap(), 1);
        let rs = s.query("SELECT message FROM logs LIMIT 10", &Params::new(), false, Duration::from_secs(5)).await.unwrap().0;
        assert_eq!(rs.rows, vec![vec![Value::Str("new".into())]]);
        assert_eq!(s.status()["deleted"]["expired"], 1);
    }

    #[tokio::test]
    async fn past_its_size_the_oldest_go() {
        let dir = tempfile::tempdir().unwrap();
        let s = LogStore::open(LogOptions { capture: false, max_bytes: 1, ..LogOptions::new(dir.path().join("s.logs"), "n".into()) }).unwrap();
        let now = crate::telemetry::now_ms();
        let lines: Vec<_> = (0..400u64).map(|i| line(now - 400_000 + i * 1000, "info", "x", &format!("line {i}"), Map::new(), None)).collect();
        s.write_lines(&lines).unwrap();
        s.trim().unwrap();
        let rs = s.query("SELECT COUNT(*), MIN(ts) FROM logs", &Params::new(), false, Duration::from_secs(5)).await.unwrap().0;
        let left = rs.rows[0][0].as_i64().unwrap();
        assert!(left < 400, "{left}");
        // What is left is the newest.
        let rs = s.query("SELECT message FROM logs ORDER BY ts DESC LIMIT 1", &Params::new(), false, Duration::from_secs(5)).await.unwrap().0;
        assert_eq!(rs.rows[0][0], Value::Str("line 399".into()));
        assert!(s.status()["deleted"]["for_size"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn shard_answers_of_two_stores_merge() {
        let dir = tempfile::tempdir().unwrap();
        let (a, b) = (store(dir.path(), "a.logs", "a"), store(dir.path(), "b.logs", "b"));
        let t = 1_793_400_000_000u64;
        a.write_lines(&[line(t, "info", "x", "a first", Map::new(), None), line(t + 20, "error", "x", "a third", Map::new(), None)]).unwrap();
        b.write_lines(&[line(t + 10, "warn", "x", "b second", Map::new(), None), line(t + 30, "error", "x", "b fourth", Map::new(), None)]).unwrap();
        for (sql, want) in [
            ("SELECT node, message FROM logs ORDER BY ts DESC LIMIT 3", vec![vec!["b", "b fourth"], vec!["a", "a third"], vec!["b", "b second"]]),
            ("SELECT level, COUNT(*) AS n FROM logs GROUP BY level ORDER BY n DESC LIMIT 1", vec![vec!["error", "2"]]),
        ] {
            let (_, select) = bind_select(schemas(), sql, &Params::new()).unwrap();
            let mut sets = Vec::new();
            for s in [&a, &b] {
                sets.push(s.query(sql, &Params::new(), true, Duration::from_secs(5)).await.unwrap().0);
            }
            let rs = strata::cluster::merge(&select, sets).unwrap();
            let got: Vec<Vec<String>> = rs.rows.iter().map(|r| r.iter().map(|v| v.as_str().map_or_else(|| v.as_i64().unwrap().to_string(), String::from)).collect()).collect();
            assert_eq!(got, want, "{sql}");
        }
        assert!(bind_select(schemas(), "SELECT l.message FROM logs l JOIN queries q ON l.request_id = q.request_id LIMIT 3", &Params::new()).is_err());
    }

    #[test]
    fn a_store_of_another_shape_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("x.logs");
        let other = match bind_command("CREATE TABLE logs (id INT PRIMARY KEY, ts KEYWORD)", &[], &Params::new()).unwrap() {
            Command::CreateTable { schema, .. } => schema,
            _ => unreachable!(),
        };
        drop(Db::create(&path, other, DbOptions::default()).unwrap());
        let e = LogStore::open(LogOptions { capture: false, ..LogOptions::new(path, "n".into()) }).err().unwrap();
        assert!(e.contains("move it aside"), "{e}");
    }
}
