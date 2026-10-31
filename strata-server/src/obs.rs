//! Logs and request ids. Every HTTP and gRPC request gets an id (the
//! client's `x-request-id` if it is a plain one, a new one otherwise),
//! which its answer carries back, each log line written while serving it
//! names, and a coordinator passes on to its shards.
//!
//! Logs go to stderr through `tracing`, as text or as JSON lines
//! ([`init_logs`]), to a ring of the latest ([`ring`]), and to the server's
//! log store ([`sink`], [`crate::logstore`]), a Strata database of its own.

use std::collections::hash_map::RandomState;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

tokio::task_local! {
    static REQUEST_ID: Arc<str>;
}

/// The header (and gRPC metadata key) that carries a request's id.
pub const HEADER: &str = "x-request-id";

/// `given` if it is 1 to 64 letters, digits, '-', '_', '.' or ':', or a
/// new id.
pub fn request_id(given: Option<&str>) -> Arc<str> {
    match given {
        Some(s) if (1..=64).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_alphanumeric() || b"-_.:".contains(&b)) => s.into(),
        _ => new_id().into(),
    }
}

/// 16 hex digits: a counter hashed with keys drawn at random once per
/// process, so ids neither repeat nor can be guessed.
fn new_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    static KEYS: OnceLock<RandomState> = OnceLock::new();
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let mut h = KEYS.get_or_init(RandomState::new).build_hasher();
    h.write_u64(NEXT.fetch_add(1, Ordering::Relaxed));
    format!("{:016x}", h.finish())
}

/// The id of the request this task serves, if it serves one.
pub fn current() -> Option<Arc<str>> {
    REQUEST_ID.try_with(|id| id.clone()).ok()
}

/// Runs `f` as the request `id`'s (none: as it is).
pub async fn scope<F: Future>(id: Option<Arc<str>>, f: F) -> F::Output {
    match id {
        Some(id) => REQUEST_ID.scope(id, f).await,
        None => f.await,
    }
}

/// How log lines are written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogFormat {
    /// One line of text per event, with the request's span.
    Text,
    /// One JSON object per line: `timestamp`, `level`, `target`, the
    /// event's fields (`message` among them) and the request's (`span`).
    Json,
}

impl LogFormat {
    pub fn parse(s: &str) -> Option<LogFormat> {
        match s {
            "text" => Some(LogFormat::Text),
            "json" => Some(LogFormat::Json),
            _ => None,
        }
    }
}

/// The filter `filter` names (`info`, or per module as
/// `info,strata=debug`), with the libraries the server is built on at
/// warnings unless it names them.
fn env_filter(filter: &str) -> Result<tracing_subscriber::EnvFilter, String> {
    let quiet = ["h2", "hyper", "hyper_util", "tower", "tonic", "rustls", "reqwest"];
    let mut directives = filter.to_string();
    for m in quiet.iter().filter(|m| !filter.contains(*m)) {
        directives.push_str(&format!(",{m}=warn"));
    }
    tracing_subscriber::EnvFilter::try_new(&directives).map_err(|e| format!("bad log filter {filter:?}: {e}"))
}

type Reload = tracing_subscriber::reload::Handle<tracing_subscriber::EnvFilter, tracing_subscriber::Registry>;

/// The filter [`init_logs`] set, which [`set_log_filter`] changes.
static FILTER: OnceLock<(Reload, Mutex<String>)> = OnceLock::new();

/// Sends log events at `filter`'s levels (`info`, or per module as
/// `info,strata=debug`) to stderr in `format`, and keeps the latest in
/// [`ring`]. The libraries the server is built on log only warnings
/// unless `filter` names them.
pub fn init_logs(filter: &str, format: LogFormat) -> Result<(), String> {
    use tracing_subscriber::prelude::*;
    let (env, handle) = tracing_subscriber::reload::Layer::new(env_filter(filter)?);
    let fmt = tracing_subscriber::fmt::layer().with_writer(std::io::stderr);
    let r = match format {
        LogFormat::Text => tracing_subscriber::registry().with(env).with(fmt.with_ansi(false)).with(RingLayer).try_init(),
        LogFormat::Json => tracing_subscriber::registry().with(env).with(fmt.json().flatten_event(true).with_current_span(true).with_span_list(false)).with(RingLayer).try_init(),
    };
    r.map_err(|e| e.to_string())?;
    let _ = FILTER.set((handle, Mutex::new(filter.to_string())));
    Ok(())
}

/// The log filter now (`None` when logs were not set up by
/// [`init_logs`]: a test's own subscriber).
pub fn log_filter() -> Option<String> {
    FILTER.get().map(|(_, f)| f.lock().unwrap_or_else(|e| e.into_inner()).clone())
}

/// Changes what is logged while the server runs: `debug` to see every
/// request, `info` to go back.
pub fn set_log_filter(filter: &str) -> Result<(), String> {
    let (handle, now) = FILTER.get().ok_or("logs are not set up by the server here")?;
    handle.reload(env_filter(filter)?).map_err(|e| e.to_string())?;
    *now.lock().unwrap_or_else(|e| e.into_inner()) = filter.to_string();
    Ok(())
}

/// A log line kept in the [`ring`]: its number, time, level, module, text,
/// fields, and the request it was written for.
#[derive(Clone, Debug, serde::Serialize)]
pub struct LogLine {
    pub seq: u64,
    pub ts_ms: u64,
    pub level: &'static str,
    pub target: String,
    pub message: String,
    #[serde(skip_serializing_if = "serde_json::Map::is_empty")]
    pub fields: serde_json::Map<String, serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub request_id: Option<String>,
    /// The request's method or gRPC call, and its route.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub method: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
}

/// The latest log lines, for `GET /v1/logs`.
pub struct LogRing {
    lines: Mutex<std::collections::VecDeque<Arc<LogLine>>>,
    next: AtomicU64,
}

/// Lines the ring keeps.
pub const RING_LINES: usize = 5000;

/// The ring every server's log lines go to (as well as to stderr).
pub fn ring() -> &'static LogRing {
    static RING: OnceLock<LogRing> = OnceLock::new();
    RING.get_or_init(|| LogRing { lines: Mutex::new(std::collections::VecDeque::with_capacity(RING_LINES)), next: AtomicU64::new(0) })
}

impl LogRing {
    fn push(&self, mut line: LogLine) -> Arc<LogLine> {
        let mut lines = self.lines.lock().unwrap_or_else(|e| e.into_inner());
        line.seq = self.next.fetch_add(1, Ordering::Relaxed) + 1;
        if lines.len() == RING_LINES {
            lines.pop_front();
        }
        let line = Arc::new(line);
        lines.push_back(line.clone());
        line
    }

    /// The lines after number `after` at `min` level or more severe (as
    /// "warn"), of request `request` and holding `text` if given, the
    /// oldest first, at most `limit` (the latest of them).
    pub fn lines(&self, after: u64, min: Option<&str>, request: Option<&str>, text: Option<&str>, limit: usize) -> Vec<Arc<LogLine>> {
        let rank = |l: &str| ["trace", "debug", "info", "warn", "error"].iter().position(|x| x.eq_ignore_ascii_case(l)).unwrap_or(0);
        let min = min.map_or(0, rank);
        let text = text.map(|t| t.to_lowercase());
        let lines = self.lines.lock().unwrap_or_else(|e| e.into_inner());
        let mut out: Vec<Arc<LogLine>> = lines
            .iter()
            .rev()
            .filter(|l| l.seq > after && rank(l.level) >= min)
            .filter(|l| request.is_none_or(|r| l.request_id.as_deref() == Some(r)))
            .filter(|l| text.as_ref().is_none_or(|t| l.message.to_lowercase().contains(t) || l.target.contains(t.as_str()) || serde_json::Value::Object(l.fields.clone()).to_string().to_lowercase().contains(t)))
            .take(limit)
            .cloned()
            .collect();
        out.reverse();
        out
    }

    /// The number of the latest line (0: none yet).
    pub fn last(&self) -> u64 {
        self.next.load(Ordering::Relaxed)
    }
}

/// Keeps every event in the [`ring`], with its request's id.
struct RingLayer;

/// A span's fields, kept in its extensions for [`RingLayer`].
struct SpanFields(serde_json::Map<String, serde_json::Value>);

#[derive(Default)]
struct Fields(serde_json::Map<String, serde_json::Value>);

impl tracing::field::Visit for Fields {
    fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
        self.0.insert(f.name().to_string(), serde_json::json!(format!("{v:?}")));
    }
    fn record_str(&mut self, f: &tracing::field::Field, v: &str) {
        self.0.insert(f.name().to_string(), serde_json::json!(v));
    }
    fn record_i64(&mut self, f: &tracing::field::Field, v: i64) {
        self.0.insert(f.name().to_string(), serde_json::json!(v));
    }
    fn record_u64(&mut self, f: &tracing::field::Field, v: u64) {
        self.0.insert(f.name().to_string(), serde_json::json!(v));
    }
    fn record_f64(&mut self, f: &tracing::field::Field, v: f64) {
        self.0.insert(f.name().to_string(), serde_json::json!(v));
    }
    fn record_bool(&mut self, f: &tracing::field::Field, v: bool) {
        self.0.insert(f.name().to_string(), serde_json::json!(v));
    }
}

impl<S> tracing_subscriber::Layer<S> for RingLayer
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    fn on_new_span(&self, attrs: &tracing::span::Attributes<'_>, id: &tracing::span::Id, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut f = Fields::default();
        attrs.record(&mut f);
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(SpanFields(f.0));
        }
    }

    fn on_event(&self, event: &tracing::Event<'_>, ctx: tracing_subscriber::layer::Context<'_, S>) {
        let mut f = Fields::default();
        event.record(&mut f);
        let message = f.0.remove("message").map(|m| m.as_str().map_or_else(|| m.to_string(), str::to_string)).unwrap_or_default();
        let (mut request_id, mut method, mut route) = (None, None, None);
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                if let Some(SpanFields(sf)) = span.extensions().get::<SpanFields>() {
                    let get = |k: &str| sf.get(k).map(|v| v.as_str().map_or_else(|| v.to_string(), str::to_string));
                    request_id = request_id.or_else(|| get("id"));
                    method = method.or_else(|| get("method"));
                    route = route.or_else(|| get("route"));
                }
            }
        }
        let m = event.metadata();
        let line = ring().push(LogLine {
            seq: 0,
            ts_ms: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64),
            level: match *m.level() {
                tracing::Level::ERROR => "error",
                tracing::Level::WARN => "warn",
                tracing::Level::INFO => "info",
                tracing::Level::DEBUG => "debug",
                tracing::Level::TRACE => "trace",
            },
            target: m.target().to_string(),
            message,
            fields: f.0,
            request_id,
            method,
            route,
        });
        if !QUIET.get() {
            sink().push(line);
        }
    }
}

// ------------------------------------------------------ the log store's feed

thread_local! {
    /// Set on the log store's own threads: what they log goes to stderr and
    /// the ring, not to the store, which storing it would write to again.
    static QUIET: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

/// Keeps what this thread logs from now on out of the log store
/// ([`sink`]): for the store's own threads, whose writes and merges log as
/// any database's do.
pub fn quiet_thread() {
    QUIET.set(true);
}

/// The lines waiting for the log store ([`crate::logstore`]), the oldest
/// first: every line the server logs once [`Sink::start`] is called (by the
/// server, before it opens anything), but those of the store's own threads
/// ([`quiet_thread`]). At most [`SINK_LINES`] wait; more are counted and
/// dropped, as when the store cannot keep up.
pub struct Sink {
    state: Mutex<SinkState>,
    ready: std::sync::Condvar,
}

#[derive(Default)]
struct SinkState {
    on: bool,
    lines: std::collections::VecDeque<Arc<LogLine>>,
    dropped: u64,
}

/// Lines the [`sink`] holds for the store.
pub const SINK_LINES: usize = 100_000;

/// Lines that wake the store's writer before its next second.
pub const SINK_BATCH: usize = 4096;

/// The lines waiting for the log store.
pub fn sink() -> &'static Sink {
    static SINK: OnceLock<Sink> = OnceLock::new();
    SINK.get_or_init(|| Sink { state: Mutex::new(SinkState::default()), ready: std::sync::Condvar::new() })
}

impl Sink {
    fn lock(&self) -> std::sync::MutexGuard<'_, SinkState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Keeps lines for the store from now on.
    pub fn start(&self) {
        self.lock().on = true;
    }

    /// Keeps none, and lets go of those held: the server keeps no store.
    pub fn stop(&self) {
        let mut s = self.lock();
        s.on = false;
        s.lines = Default::default();
    }

    fn push(&self, line: Arc<LogLine>) {
        let mut s = self.lock();
        if !s.on {
            return;
        }
        if s.lines.len() >= SINK_LINES {
            s.dropped += 1;
            return;
        }
        s.lines.push_back(line);
        if s.lines.len() == SINK_BATCH {
            self.ready.notify_all();
        }
    }

    /// Waits until [`SINK_BATCH`] lines wait or `timeout` passes.
    pub fn wait(&self, timeout: Duration) {
        let s = self.lock();
        if s.lines.len() < SINK_BATCH {
            drop(self.ready.wait_timeout(s, timeout).unwrap_or_else(|e| e.into_inner()));
        }
    }

    /// Wakes a writer in [`Sink::wait`] (the store is closing).
    pub fn wake(&self) {
        self.ready.notify_all();
    }

    /// The oldest `max` lines waiting, taken.
    pub fn take(&self, max: usize) -> Vec<Arc<LogLine>> {
        let mut s = self.lock();
        let n = s.lines.len().min(max);
        s.lines.drain(..n).collect()
    }

    /// Lines waiting, and lines dropped because too many waited.
    pub fn counts(&self) -> (usize, u64) {
        let s = self.lock();
        (s.lines.len(), s.dropped)
    }
}

/// Lets through at most `per_second` lines a second and counts the rest,
/// so that a failure every request meets (a full disk) cannot flood the
/// log.
pub struct RateLimit {
    per_second: u64,
    /// The second counted, and the lines passed and held back in it.
    window: Mutex<(Option<Instant>, u64, u64)>,
}

impl RateLimit {
    pub const fn new(per_second: u64) -> RateLimit {
        RateLimit { per_second, window: Mutex::new((None, 0, 0)) }
    }

    /// `Some(held back since the last line passed)` if a line may pass now.
    pub fn admit(&self) -> Option<u64> {
        let mut w = self.window.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        if w.0.is_none_or(|t| now.duration_since(t) >= Duration::from_secs(1)) {
            w.0 = Some(now);
            w.1 = 0;
        }
        if w.1 >= self.per_second {
            w.2 += 1;
            return None;
        }
        w.1 += 1;
        Some(std::mem::take(&mut w.2))
    }
}

/// Logs a request that failed on the server's side (status 500 and up),
/// at most ten a second.
pub fn server_error(status: u16, message: &str) {
    static LIMIT: RateLimit = RateLimit::new(10);
    if let Some(held) = LIMIT.admit() {
        tracing::error!(status, error = message, held_back = held, "request failed");
    }
}

/// `s` cut to at most `max` bytes (at a character boundary), marked when
/// cut.
pub fn truncate(s: &str, max: usize) -> std::borrow::Cow<'_, str> {
    if s.len() <= max {
        return s.into();
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}... ({} bytes)", &s[..end], s.len()).into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_ids_are_kept_and_others_replaced() {
        assert_eq!(&*request_id(Some("abc-123_x.y:z")), "abc-123_x.y:z");
        for bad in ["", "has space", "new\nline", "é", &"x".repeat(65)] {
            let id = request_id(Some(bad));
            assert_ne!(&*id, bad);
            assert_eq!(id.len(), 16);
        }
        let ids: std::collections::HashSet<Arc<str>> = (0..1000).map(|_| request_id(None)).collect();
        assert_eq!(ids.len(), 1000);
    }

    #[test]
    fn the_rate_limit_counts_what_it_holds_back() {
        let l = RateLimit::new(2);
        assert_eq!((l.admit(), l.admit(), l.admit(), l.admit()), (Some(0), Some(0), None, None));
        l.window.lock().unwrap().0 = Some(Instant::now() - Duration::from_secs(2));
        assert_eq!(l.admit(), Some(2));
    }

    #[test]
    fn long_text_is_cut_at_a_character() {
        assert_eq!(truncate("short", 10), "short");
        assert_eq!(truncate("ééé", 3), "é... (6 bytes)");
    }

    #[test]
    fn the_ring_keeps_lines_with_their_request() {
        use tracing_subscriber::prelude::*;
        let sub = tracing_subscriber::registry().with(RingLayer);
        let before = ring().last();
        tracing::subscriber::with_default(sub, || {
            let span = tracing::info_span!("request", id = "ring-1", method = "POST", route = "/v1/query");
            let _in = span.enter();
            tracing::warn!(rows = 3, "slow ring query");
            tracing::debug!("detail of ring-1");
        });
        let all = ring().lines(before, None, Some("ring-1"), None, 10);
        assert_eq!(all.len(), 2, "{all:?}");
        assert_eq!(all[0].message, "slow ring query");
        assert_eq!(all[0].fields["rows"], 3);
        assert_eq!(all[0].route.as_deref(), Some("/v1/query"));
        assert_eq!(ring().lines(before, Some("warn"), Some("ring-1"), None, 10).len(), 1);
        assert_eq!(ring().lines(before, None, None, Some("DETAIL OF"), 10).len(), 1);
        assert_eq!(ring().lines(all[1].seq, None, Some("ring-1"), None, 10).len(), 0);
    }

    #[tokio::test]
    async fn the_id_is_the_scope_s() {
        assert!(current().is_none());
        let got = scope(Some("r1".into()), async { current() }).await;
        assert_eq!(got.as_deref(), Some("r1"));
    }
}
