//! A coordinator over shard servers: one collection split by primary key.
//!
//! A query runs in two rounds when it scores with BM25. The coordinator
//! asks every shard for the statistics of the query's terms, each shard
//! answering from a snapshot it holds for the next round (a lease), sums
//! them, and runs the query on every shard in shard mode with the sum and
//! the shard's lease. Each shard then scores as the unsplit collection
//! would, from the version it counted, and [`strata::cluster::merge`]
//! combines the answers. Other queries take one round.
//!
//! A query for a page of rows with stored columns is answered by query
//! then fetch ([`strata::cluster::fetches`], from protocol 3), when the
//! shards would send many more rows than the page in one round (a deep
//! page, many shards or a fused score: [`FETCH_FROM`]): every shard ranks
//! its rows and answers their keys, scores and sort values only, holding
//! the snapshot it read; the coordinator merges them, and the shards
//! holding the page's rows fetch their columns from that snapshot and end
//! its lease ([`CoordOptions::fetch`]). The other shards' leases are ended
//! alongside. Columns are read and sent for the page, not for each shard's
//! top `LIMIT + OFFSET` rows.
//!
//! A shard may be served by several replicas, servers holding the same
//! documents (`--shards a0|a1,b0|b1`). A read goes to one replica of each
//! shard, in turn; when a replica fails, the next one is asked, and when
//! one has not answered after [`CoordOptions::hedge`], the next one is
//! asked too and the first answer wins, while hedges stay within
//! [`HEDGE_SHARE`] of calls. A replica that refuses a query because too
//! many are running is not relieved by asking its sibling, which would
//! only be loaded more: the query fails (or leaves the shard out of a
//! partial answer). A replica that failed, or was outrun, is asked last
//! for [`CoordOptions::cool_off`]. With
//! [`CoordOptions::partial`], a query whose shard has no replica answering
//! is answered from the other shards and says which are missing, and the
//! queries of the next second leave that shard out without asking it.
//!
//! Writes go to the shard of their key: the shard holding its slot
//! ([`crate::slots`]) in the coordinator's slot map, which it gives every
//! shard when it connects and whose epoch it sends with every request, so
//! that a shard refuses a coordinator routing by an older map, and keys
//! another shard holds. UPDATE and DELETE statements go to every shard. A
//! shard whose replicas replicate (`Hello` names their roles) takes its
//! writes at its primary, which ships them to the others
//! ([`crate::replication`]); otherwise every replica takes every write,
//! and one that misses a write (it was down) is not caught up: rebuild it.
//! A write that spans shards is one transaction per shard, not one across
//! them.
//!
//! A coordinator serves every table its shards serve alike: it asks each
//! replica for its tables when it connects (`Schema`), again when a
//! request names a table it does not know, and after a `CREATE TABLE` or
//! `DROP TABLE` it ran on every shard ([`Coordinator::ddl`]). It serves
//! the rank profiles every replica holds alike, learned with the tables; a
//! query by a profile plans here, written out, and a shard whose profile
//! differs refuses the plan, after which the coordinator asks again and
//! plans once more ([`crate::service::Service::query`]). A row of a
//! table placed by a column (`shard_by`, [`Schema::with_shard_by`]) goes
//! to the shard of that column's value instead of its key's, so that rows
//! sharing a value (a user's events) live on one shard.
//!
//! The coordinator asks each replica the protocol versions it speaks
//! (`Hello`, [`crate::grpc::PROTOCOL`]) when it connects, again every
//! minute and after the replica fails, as it may have restarted on another
//! version, and speaks the oldest any replica speaks. From protocol 2 a
//! read reaches the shards as a typed plan ([`crate::plan_pb`]); a server
//! from before `Hello` takes SQL, or a JSON plan, as it did.

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicI32, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use futures::future::join_all;
use futures::stream::{FuturesUnordered, StreamExt};
use serde_json::{json, Value as Json};
use strata::db::Source;
use strata::query::agg::Part;
use strata::query::exec::ResultSet;
use strata::query::ir::Plan;
use strata::query::stats::TermStats;
use strata::query::steps::{self, Step};
use strata::query::{bind_command, Command, Conflict, Params, Projection, Select, Write};
use strata::schema::Schema;
use strata::storage::RankProfile;
use strata::value::Value;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;
use tonic::Code;

use crate::auth::Secret;
use crate::grpc::{params_to_pb, result_from_pb, CLUSTER_PROTOCOL, GUARD_PROTOCOL, MIN_PROTOCOL, PROTOCOL, SIDNEXT_PROTOCOL, SOURCES_PROTOCOL};
use crate::metrics::Family;
use crate::pb::{self, strata_client::StrataClient};
use crate::service::{is_mark, op_place, op_schema, op_source, After, QueryReq, Written};
use crate::replication::Acks;
use crate::slots::{hash_shard_of, SlotMap};
use crate::{json as js, plan_pb, ApiError};

/// How the coordinator uses replicas and handles shards that do not answer.
#[derive(Clone, Debug)]
pub struct CoordOptions {
    /// Ask another replica of a shard when one has not answered after this.
    pub hedge: Duration,
    /// Ask a replica that failed or was outrun last, for this long.
    pub cool_off: Duration,
    /// Give up on a shard after this long (`None`: at the query's deadline).
    pub shard_timeout: Option<Duration>,
    /// Answer without shards that have no replica answering, and say so,
    /// instead of failing the query.
    pub partial: bool,
    /// The shards' token, sent with every call.
    pub token: Option<Secret>,
    /// A CA certificate (PEM) that `https://` shards' certificates are
    /// checked against, besides the public roots.
    pub ca: Option<Vec<u8>>,
    /// Answer pages of rows by query then fetch when the shards speak
    /// protocol 3; otherwise in one round, every shard sending the columns
    /// of its top rows.
    pub fetch: bool,
    /// The slot map to route writes by and give the shards (`None`: the
    /// shards holding equal runs of slots, [`SlotMap::even`], epoch 1).
    pub slots: Option<SlotMap>,
}

impl Default for CoordOptions {
    fn default() -> CoordOptions {
        CoordOptions { hedge: Duration::from_millis(100), cool_off: Duration::from_secs(5), shard_timeout: None, partial: false, token: None, ca: None, fetch: true, slots: None }
    }
}

/// Adds the shards' token to every call, and the request's id.
#[derive(Clone)]
pub struct Credentials(Option<tonic::metadata::AsciiMetadataValue>);

impl tonic::service::Interceptor for Credentials {
    /// Also passes on the id of the request the call serves
    /// ([`crate::obs`]).
    fn call(&mut self, mut req: tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status> {
        if let Some(v) = &self.0 {
            req.metadata_mut().insert("authorization", v.clone());
        }
        if let Some(id) = crate::obs::current().and_then(|id| id.parse().ok()) {
            req.metadata_mut().insert(crate::obs::HEADER, id);
        }
        Ok(req)
    }
}

pub(crate) type Client = StrataClient<InterceptedService<Channel, Credentials>>;

/// A client of the server at gRPC URL `url`, sending `token` with every
/// call; an `https://` server's certificate is checked against the public
/// roots and `ca`. It connects on its first call.
pub(crate) fn client(url: &str, token: Option<&Secret>, ca: Option<&[u8]>) -> Result<Client, ApiError> {
    let credentials = match token {
        Some(t) => Some(format!("Bearer {}", t.0).parse().map_err(|_| ApiError::bad("the shards' token must be printable ASCII"))?),
        None => None,
    };
    let mut ep = Channel::from_shared(url.to_string()).map_err(|e| ApiError::bad(format!("bad shard URL {url}: {e}")))?;
    if url.starts_with("https://") {
        let mut tls = tonic::transport::ClientTlsConfig::new().with_webpki_roots();
        if let Some(ca) = ca {
            tls = tls.ca_certificate(tonic::transport::Certificate::from_pem(ca));
        }
        ep = ep.tls_config(tls).map_err(|e| ApiError::bad(format!("TLS to {url}: {e}")))?;
    }
    let ch = ep.connect_timeout(Duration::from_secs(5)).tcp_nodelay(true).connect_lazy();
    Ok(StrataClient::with_interceptor(ch, Credentials(credentials)).max_decoding_message_size(256 << 20))
}

/// One server holding a copy of a shard.
struct Replica {
    url: String,
    client: Client,
    /// Asked last until this time (ms after [`Coordinator::epoch`]).
    avoid_until: AtomicU64,
    /// The protocol it speaks with the coordinator (0: not known), and
    /// when it said so (ms after [`Coordinator::epoch`]).
    protocol: AtomicU32,
    hello_at: AtomicU64,
    /// Its part in replication ([`pb::Role`]), its term, and whether a
    /// primary of a newer term replaced it, as it last said.
    role: AtomicI32,
    term: AtomicU64,
    replaced: AtomicBool,
}

impl Replica {
    fn new(url: &str, client: Client) -> Replica {
        Replica {
            url: url.to_string(),
            client,
            avoid_until: AtomicU64::new(0),
            protocol: AtomicU32::new(0),
            hello_at: AtomicU64::new(0),
            role: AtomicI32::new(0),
            term: AtomicU64::new(0),
            replaced: AtomicBool::new(false),
        }
    }

    fn role(&self) -> pb::Role {
        pb::Role::try_from(self.role.load(Ordering::Relaxed)).unwrap_or_default()
    }

    /// Keeps what it said to `Hello`.
    fn heard(&self, said: &Said) {
        self.protocol.store(said.protocol, Ordering::Relaxed);
        self.role.store(said.role as i32, Ordering::Relaxed);
        self.term.store(said.term, Ordering::Relaxed);
        self.replaced.store(said.replaced, Ordering::Relaxed);
    }

    /// Whether it takes its shard's writes: a primary no other replaced.
    fn takes_writes(&self) -> bool {
        self.role() == pb::Role::Primary && !self.replaced.load(Ordering::Relaxed)
    }
}

/// Query then fetch costs a second round, so it is taken when one round
/// would send at least this many times the page's rows
/// ([`Coordinator::fetch_pays`]). Over localhost, with 3 shards of 20,000
/// demo products and a 10-row page on a 4-core cloud container, one round
/// was faster by up to 0.5 ms up to 6 times the page's rows, the two were
/// even at 9, and query then fetch was faster from 15 (2026-10-29).
pub const FETCH_FROM: usize = 8;

/// How long a replica's answer to `Hello` is trusted before it is asked
/// again, so that the coordinator follows a rolling upgrade or a rollback.
const HELLO_EVERY_MS: u64 = 60_000;

/// Why `seq_no()` and `if_seq_no` need a shard of several replicas to
/// replicate: otherwise each numbers the writes it takes in its own order.
const ONE_REPLICA: &str = "seq_no() and if_seq_no need one replica per shard, or a shard whose replicas replicate from a primary (--replicas): otherwise each replica numbers its own writes";

/// The tables every replica that answered `Schema` names alike, in the
/// first one's order: each names them all (`tables_json`), or, from a
/// server before tables, its first table alone. One that is not on every
/// replica is left out, and logged.
fn common_tables(answers: &[pb::SchemaResponse]) -> Result<Vec<Arc<Schema>>, ApiError> {
    let lists: Vec<Vec<&str>> = answers
        .iter()
        .map(|a| match a.tables_json.is_empty() {
            true => vec![a.schema_json.as_str()],
            false => a.tables_json.iter().map(String::as_str).collect(),
        })
        .collect();
    let Some(first) = lists.first() else { return Ok(Vec::new()) };
    let mut out = Vec::new();
    for t in first {
        let schema: Schema = serde_json::from_str(t).map_err(ApiError::internal)?;
        match lists.iter().all(|l| l.contains(t)) {
            true => out.push(Arc::new(schema)),
            false => tracing::warn!(table = %schema.table, "a table some shard replicas lack, or hold with another schema, is not served: create it on every shard (CREATE TABLE IF NOT EXISTS through the coordinator)"),
        }
    }
    Ok(out)
}

/// The rank profiles every replica that answered `Schema` holds alike
/// (`profiles_json`), by table: one that a replica lacks, or holds with
/// another definition, is left out, and logged, until they agree again.
fn common_profiles(answers: &[pb::SchemaResponse]) -> Vec<(String, RankProfile)> {
    let Some(first) = answers.first() else { return Vec::new() };
    let mut out = Vec::new();
    for p in &first.profiles_json {
        let Ok((table, profile)) = profile_of_json(p) else {
            tracing::warn!(profile = %p, "a shard sent a rank profile this coordinator cannot read");
            continue;
        };
        match answers.iter().all(|a| a.profiles_json.contains(p)) {
            true => out.push((table, profile)),
            false => tracing::warn!(%table, profile = %profile.name, "a rank profile some shard replicas lack, or hold with another definition, is not served: create or replace it through the coordinator"),
        }
    }
    out
}

/// A table's rank profile as `Schema` sends it: `{"table", "profile"}`.
pub fn profile_json(table: &str, profile: &RankProfile) -> String {
    json!({"table": table, "profile": profile}).to_string()
}

fn profile_of_json(text: &str) -> Result<(String, RankProfile), String> {
    #[derive(serde::Deserialize)]
    struct Of {
        table: String,
        profile: RankProfile,
    }
    let o: Of = serde_json::from_str(text).map_err(|e| e.to_string())?;
    Ok((o.table, o.profile))
}

/// The protocol to speak with a server that answered `Hello`: the newest
/// both speak.
fn agree(h: &pb::HelloResponse) -> Result<u32, String> {
    let p = h.max_protocol.min(PROTOCOL);
    if p < h.min_protocol.max(MIN_PROTOCOL) {
        return Err(format!("{} speaks protocols {} to {}, and this coordinator {MIN_PROTOCOL} to {PROTOCOL}", h.server, h.min_protocol, h.max_protocol));
    }
    Ok(p)
}

/// What a server said to `Hello`: the protocol to speak with it
/// ([`agree`]), the epoch of the slot map it holds, its role and term,
/// and whether a primary of a newer term replaced it.
struct Said {
    protocol: u32,
    epoch: u64,
    role: pb::Role,
    term: u64,
    replaced: bool,
}

/// Asks a server what [`Said`] holds. One from before `Hello` (it answers
/// Unimplemented) speaks protocol 1.
async fn hello(c: &mut Client) -> Result<Said, tonic::Status> {
    match c.hello(pb::HelloRequest { min_protocol: MIN_PROTOCOL, max_protocol: PROTOCOL }).await {
        Ok(h) => {
            let h = h.into_inner();
            Ok(Said { protocol: agree(&h).map_err(tonic::Status::failed_precondition)?, epoch: h.slot_epoch, role: h.role(), term: h.term, replaced: h.replaced_by_term > 0 })
        }
        Err(s) if s.code() == Code::Unimplemented => Ok(Said { protocol: 1, epoch: 0, role: pb::Role::Standalone, term: 0, replaced: false }),
        Err(s) => Err(s),
    }
}

/// What the servers of a topology said when a coordinator connected:
/// the first table's schema and every table of each that answered, and
/// the cluster maps they hold (from protocol 9).
struct Survey {
    schemas: Vec<String>,
    tables: Vec<pb::SchemaResponse>,
    /// Each server's URL and the map it holds.
    maps: Vec<(String, SlotMap)>,
}

/// Asks every server of `t` its tables, protocol and role (which each
/// replica keeps), and from protocol 9 the cluster map it holds. Every
/// shard needs a server answering within 5 s.
async fn survey(t: &Topology) -> Result<Survey, ApiError> {
    let mut found = Survey { schemas: Vec::new(), tables: Vec::new(), maps: Vec::new() };
    for (i, rs) in t.replicas.iter().enumerate() {
        let answers = join_all(rs.iter().map(|r| {
            let mut c = r.client.clone();
            let ask = async move {
                let s = c.schema(pb::Empty {}).await?.into_inner();
                let said = hello(&mut c).await?;
                let map = match said.protocol >= CLUSTER_PROTOCOL {
                    true => held_map(&mut c).await?.map(|(m, _)| m),
                    false => None,
                };
                Ok::<_, tonic::Status>((s, said, map))
            };
            tokio::time::timeout(Duration::from_secs(5), ask)
        }))
        .await;
        let mut errors = Vec::new();
        for (r, a) in rs.iter().zip(answers) {
            match a {
                Ok(Ok((s, said, map))) => {
                    found.schemas.push(s.schema_json.clone());
                    found.tables.push(s);
                    r.heard(&said);
                    found.maps.extend(map.map(|m| (r.url.clone(), m)));
                }
                Ok(Err(e)) => errors.push(format!("{}: {}", r.url, e.message())),
                Err(_) => errors.push(format!("{}: no answer within 5 s", r.url)),
            }
        }
        if errors.len() == rs.len() {
            return Err(ApiError { status: 502, message: format!("shard {i} has no replica answering: {}", errors.join("; ")) });
        }
    }
    Ok(found)
}

/// The cluster map a server of protocol 9 holds, and its shard in it.
async fn held_map(c: &mut Client) -> Result<Option<(SlotMap, usize)>, tonic::Status> {
    let a = c.slots(pb::SlotsRequest { map_json: Vec::new(), shard: 0 }).await?.into_inner();
    if a.map_json.is_empty() {
        return Ok(None);
    }
    let j: Json = serde_json::from_slice(&a.map_json).map_err(|e| tonic::Status::internal(format!("its cluster map: {e}")))?;
    let m = SlotMap::from_json(&j).map_err(|e| tonic::Status::internal(format!("its cluster map: {e}")))?;
    Ok(Some((m, a.shard as usize)))
}

/// Gives a server `map`, with it as shard `shard` ([`crate::service::Service::set_slots`]).
async fn give_slots(c: &mut Client, map: &SlotMap, shard: usize) -> Result<u64, tonic::Status> {
    let map_json = serde_json::to_vec(&map.to_json()).unwrap_or_default();
    Ok(c.slots(pb::SlotsRequest { map_json, shard: shard as u32 }).await?.into_inner().epoch)
}

/// A read in the forms the shards take: SQL and its parameters, or a JSON
/// plan, in protocol 1; a typed plan from protocol 2, when the read is to
/// speak it.
struct Read {
    sql: String,
    params: std::collections::HashMap<String, pb::Value>,
    plan_json: Vec<u8>,
    plan: Option<pb::Plan>,
    /// The slot map's epoch, sent from protocol 4.
    epoch: u64,
    /// The write each shard's answer must see ([`Coordinator::after`]).
    after: Vec<u64>,
}

impl Read {
    /// `sql` with `params`, or `given` (a compound query's leaf, which
    /// `sql` then only names), and `typed()`, the plan protocol 2 sends,
    /// made only when `protocol` is 2 or newer.
    fn new(sql: &str, params: &Params, given: Option<&Plan>, protocol: u32, epoch: u64, typed: impl FnOnce() -> strata::Result<Plan>) -> Result<Read, ApiError> {
        let plan = match (protocol >= 2, given) {
            (false, _) => None,
            (true, Some(p)) => Some(plan_pb::to_pb_plan(p)),
            (true, None) => Some(plan_pb::to_pb_plan(&typed()?)),
        };
        Ok(Read { sql: sql.to_string(), params: params_to_pb(params), plan_json: plan_json(given)?, plan, epoch, after: Vec::new() })
    }

    /// The read, seeing the writes `after` names, one per shard.
    fn after(self, after: Vec<u64>) -> Read {
        Read { after, ..self }
    }

    /// The number of the write shard `i`'s answer must see (0: none).
    fn min(&self, i: usize) -> u64 {
        self.after.get(i).copied().unwrap_or(0)
    }

    /// The request to shard `i` in `protocol`. A server before protocol 5
    /// passes over `min_seq_no` ([`Coordinator::reads_after`]).
    fn request(&self, protocol: u32, i: usize) -> pb::QueryRequest {
        let min_seq_no = self.min(i);
        match (&self.plan, protocol >= 2) {
            (Some(plan), true) => {
                let epoch = if protocol >= 4 { self.epoch } else { 0 };
                pb::QueryRequest { sql: self.sql.clone(), protocol, plan: Some(plan.clone()), epoch, min_seq_no, ..Default::default() }
            }
            _ => pb::QueryRequest { sql: self.sql.clone(), params: self.params.clone(), plan_json: self.plan_json.clone(), protocol: 1, min_seq_no, ..Default::default() },
        }
    }
}

/// The cluster a coordinator routes by: each shard's servers and the
/// slot map, under the map's epoch. A request routes by the topology it
/// started with, and a change installs a new one ([`Coordinator::install`]).
pub(crate) struct Topology {
    /// Each shard's servers, as named: URLs joined by `|`.
    shards: Vec<String>,
    replicas: Vec<Vec<Arc<Replica>>>,
    /// The slot map writes are routed by, which every shard holds; `None`
    /// when a shard spoke a protocol before 4 at connect, and writes go by
    /// the hash before slots ([`hash_shard_of`]).
    slots: Option<SlotMap>,
    /// Per shard: until when partial answers leave it out (ms after
    /// [`Coordinator::epoch`]), after no replica of it answered.
    skip_until: Vec<AtomicU64>,
    /// Per shard: calls answered, failed and timed out ([`Coordinator::call`]).
    outcomes: Vec<Arc<[AtomicU64; 3]>>,
}

impl Topology {
    /// `groups` (each shard's servers) under `slots`, keeping the replicas
    /// and counts of `before` that serve the same.
    fn new(groups: Vec<Vec<String>>, slots: Option<SlotMap>, before: Option<&Topology>, opts: &CoordOptions) -> Result<Topology, ApiError> {
        let mut replicas = Vec::new();
        let mut outcomes = Vec::new();
        for group in &groups {
            let mut rs = Vec::new();
            for url in group {
                let kept = before.and_then(|b| b.replicas.iter().flatten().find(|r| r.url == *url));
                rs.push(match kept {
                    Some(r) => r.clone(),
                    None => Arc::new(Replica::new(url, client(url, opts.token.as_ref(), opts.ca.as_deref())?)),
                });
            }
            if rs.is_empty() {
                return Err(ApiError::bad("a shard has no server"));
            }
            let same = before.and_then(|b| b.replicas.iter().position(|o| o.first().map(|r| &r.url) == group.first()));
            outcomes.push(same.and_then(|i| before.map(|b| b.outcomes[i].clone())).unwrap_or_default());
            replicas.push(rs);
        }
        Ok(Topology { shards: groups.iter().map(|g| g.join("|")).collect(), skip_until: groups.iter().map(|_| AtomicU64::new(0)).collect(), replicas, slots, outcomes })
    }

    /// Each shard's servers' URLs.
    fn groups(&self) -> Vec<Vec<String>> {
        self.replicas.iter().map(|rs| rs.iter().map(|r| r.url.clone()).collect()).collect()
    }

    /// The protocol reads speak to the shards: the oldest a replica
    /// speaks, of those that said (the newest while none has).
    fn protocol(&self) -> u32 {
        self.replicas.iter().flatten().map(|r| r.protocol.load(Ordering::Relaxed)).filter(|&p| p != 0).min().unwrap_or(PROTOCOL)
    }

    /// The epoch of the slot map writes are routed by; 0 without one.
    fn epoch(&self) -> u64 {
        self.slots.as_ref().map_or(0, |m| m.epoch)
    }
}

pub struct Coordinator {
    /// The shards and their servers, which changes replace.
    topo: RwLock<Arc<Topology>>,
    /// Held while this coordinator changes the cluster map.
    changing: tokio::sync::Mutex<()>,
    /// The newest map epoch a server said it holds, and what wakes the
    /// watcher to take it up ([`Coordinator::watch`]).
    newer: AtomicU64,
    wake: Arc<tokio::sync::Notify>,
    /// The first table's schema.
    pub schema: Schema,
    /// The tables every replica serves alike, the first first
    /// ([`Self::refresh`]).
    catalog: RwLock<Arc<Vec<Arc<Schema>>>>,
    /// The rank profiles every replica holds alike, by table
    /// ([`Self::refresh`]).
    profiles: RwLock<Arc<Vec<(String, RankProfile)>>>,
    /// When the replicas were last asked for their tables.
    asked: Mutex<Option<Instant>>,
    pub opts: CoordOptions,
    epoch: Instant,
    /// Where each read starts among a shard's replicas, to spread the load.
    turn: AtomicUsize,
    /// Replicas asked again after another failed, and hedged requests.
    retries: AtomicU64,
    hedges: AtomicU64,
    hedge_budget: HedgeBudget,
    /// Queries answered by query then fetch, and rows of their pages that
    /// no fetch found.
    fetches: AtomicU64,
    fetch_lost: AtomicU64,
}

/// A query's merged answer, and the shards it lacks (with
/// [`CoordOptions::partial`]).
pub struct Merged {
    pub rs: ResultSet,
    pub missing: Vec<String>,
}

/// The share of calls that may be hedged: each call earns this much of a
/// hedge, up to [`HEDGE_BURST`] saved, so that a slow cluster is not sent
/// twice its load.
pub const HEDGE_SHARE: f64 = 0.05;
/// Hedges that may be saved up for a burst.
pub const HEDGE_BURST: f64 = 10.0;

/// The hedges a coordinator may send now, in thousandths of one.
struct HedgeBudget(AtomicU64);

impl HedgeBudget {
    const ONE: u64 = 1000;
    const SHARE: u64 = (HEDGE_SHARE * 1000.0) as u64;
    const CAP: u64 = (HEDGE_BURST * 1000.0) as u64;

    fn new() -> HedgeBudget {
        HedgeBudget(AtomicU64::new(Self::CAP))
    }

    /// Earns a call's share of a hedge.
    fn earn(&self) {
        self.update(|b| (b < Self::CAP).then(|| (b + Self::SHARE).min(Self::CAP)));
    }

    /// Spends a hedge, if one is saved.
    fn spend(&self) -> bool {
        self.update(|b| b.checked_sub(Self::ONE))
    }

    /// Replaces the budget with `f` of it, unless `f` declines; whether it
    /// was replaced. (`fetch_update` is deprecated on newer toolchains and
    /// its replacement missing on older ones.)
    fn update(&self, f: impl Fn(u64) -> Option<u64>) -> bool {
        let mut b = self.0.load(Ordering::Relaxed);
        while let Some(next) = f(b) {
            match self.0.compare_exchange_weak(b, next, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return true,
                Err(now) => b = now,
            }
        }
        false
    }
}

/// When each call to a shard's replica was sent and its answer came, by
/// shard and replica ([`Coordinator::shard_answer`]).
type Sent = Mutex<HashMap<(usize, usize), (Instant, Instant)>>;

/// `call` to replica `r` of shard `i`, its times kept in `sent` when it
/// answers.
async fn timed<T>(sent: &Sent, i: usize, r: usize, call: impl Future<Output = Result<T, tonic::Status>>) -> Result<T, tonic::Status> {
    let t0 = Instant::now();
    let a = call.await;
    if a.is_ok() {
        sent.lock().unwrap_or_else(|e| e.into_inner()).insert((i, r), (t0, Instant::now()));
    }
    a
}

/// A step of the coordinator's own that began at `t0` and ends now.
fn leaf_step(op: &str, detail: String, t0: Instant, rows: Option<u64>) -> Step {
    Step { op: op.into(), detail, start_us: steps::us_at(t0), us: steps::tenth(t0.elapsed().as_nanos() as f64 / 1e3), rows, ..Step::default() }
}

fn ms_left(deadline: Instant) -> u32 {
    deadline.saturating_duration_since(Instant::now()).as_millis().clamp(1, u32::MAX as u128) as u32
}

/// A plan as a request carries it (`plan_json`; empty without one).
fn plan_json(plan: Option<&Plan>) -> Result<Vec<u8>, ApiError> {
    match plan {
        Some(p) => Ok(serde_json::to_vec(&p.to_json()?).map_err(|e| ApiError::internal(format!("plan: {e}")))?),
        None => Ok(Vec::new()),
    }
}

/// Marks a read the coordinator did not send to a replica, which then
/// goes to another without the replica counting as failed.
const PASSED_OVER: &str = "strata-passed-over";

/// Whether another replica may answer where this one failed: not when the
/// request itself is wrong, nor when the replica is overloaded
/// (`ResourceExhausted`), which would only load the next one more.
fn try_elsewhere(s: &tonic::Status) -> bool {
    !matches!(
        s.code(),
        Code::InvalidArgument
            | Code::NotFound
            | Code::AlreadyExists
            | Code::PermissionDenied
            | Code::Unauthenticated
            | Code::FailedPrecondition
            | Code::OutOfRange
            | Code::Unimplemented
            | Code::ResourceExhausted
    )
}

/// What one shard answered a write ([`Coordinator::write`]): the rows it
/// wrote and skipped, and its transaction's number (0 for none).
#[derive(Clone, Copy, Default)]
struct Wrote {
    rows: u64,
    seq_no: u64,
    skipped: u64,
}

impl Coordinator {
    /// Connects to `shards` (gRPC URLs such as `http://10.0.0.5:7001`,
    /// replicas of one shard joined by `|`) and checks that they serve the
    /// same table. Every shard needs a replica answering.
    pub async fn connect(shards: Vec<String>) -> Result<Coordinator, ApiError> {
        Coordinator::connect_with(shards, CoordOptions::default()).await
    }

    pub async fn connect_with(shards: Vec<String>, opts: CoordOptions) -> Result<Coordinator, ApiError> {
        if shards.is_empty() {
            return Err(ApiError::bad("a coordinator needs at least one shard"));
        }
        let mut groups = Vec::new();
        for group in &shards {
            let urls: Vec<String> = group.split('|').map(str::trim).filter(|u| !u.is_empty()).map(String::from).collect();
            if urls.is_empty() {
                return Err(ApiError::bad(format!("shard {group:?} has no URL")));
            }
            groups.push(urls);
        }
        let mut topo = Topology::new(groups, None, None, &opts)?;
        let mut found = survey(&topo).await?;
        // The cluster map the servers hold, unless a newer one is given,
        // names the cluster's servers now: when every server listed is one
        // of them, the list is where to find it (a coordinator started
        // again on the servers it was first given serves those added
        // since). A list that names servers new to clusters lays the
        // cluster out anew, under the next epoch; one that names a server
        // of another map (it left this cluster, or serves another) is
        // refused, so that an old list does not undo the admin calls.
        let given = opts.slots.as_ref().map_or(0, |m| m.epoch);
        if let Some(held) = found.maps.iter().map(|(_, m)| m).filter(|m| m.epoch >= given && !m.replicas().is_empty()).max_by_key(|m| m.epoch).cloned() {
            let known: std::collections::HashSet<&String> = held.replicas().iter().flatten().collect();
            let listed = topo.groups();
            let strangers: Vec<&String> = listed.iter().flatten().filter(|u| !known.contains(u)).collect();
            let member = |u: &String| found.maps.iter().find(|(v, m)| v == u && m.replicas().iter().flatten().any(|w| w == u)).map(|(_, m)| m.epoch);
            if let Some((u, e)) = strangers.iter().find_map(|u| member(u).map(|e| (u, e))) {
                return Err(ApiError::bad(format!(
                    "{u} served a cluster map (epoch {e}) that the newest the servers listed hold leaves it out of (epoch {}, {} shards: {}): list servers of the cluster as it is, add it again through a coordinator (POST /v1/admin/shards/:i/replicas), or give a newer map (--slot-map)",
                    held.epoch,
                    held.shards(),
                    held.replicas().iter().map(|g| g.join("|")).collect::<Vec<_>>().join(",")
                )));
            }
            if strangers.is_empty() && held.replicas() != listed.as_slice() {
                tracing::info!(epoch = held.epoch, shards = held.shards(), "the servers listed hold the cluster map: serving all of its servers");
                topo = Topology::new(held.replicas().to_vec(), None, Some(&topo), &opts)?;
                found = survey(&topo).await?;
            }
        }
        let Survey { schemas, tables, maps } = found;
        if schemas.iter().any(|s| *s != schemas[0]) {
            return Err(ApiError::bad("the shards serve different schemas"));
        }
        let schema: Schema = serde_json::from_str(&schemas[0]).map_err(ApiError::internal)?;
        if schema.primary_key.is_none() {
            return Err(ApiError::bad("a sharded table needs a primary key to route writes"));
        }
        let catalog = common_tables(&tables)?;
        let profiles = common_profiles(&tables);
        let n = topo.replicas.len();
        // The map names each shard's servers, once every server speaks
        // protocol 9; before, it would differ from the map they hold.
        let named = topo.replicas.iter().flatten().all(|r| r.protocol.load(Ordering::Relaxed) >= CLUSTER_PROTOCOL);
        // Slots once every replica answering speaks protocol 4.
        let old = topo.replicas.iter().flatten().find(|r| (1..4).contains(&r.protocol.load(Ordering::Relaxed)));
        let held = maps.iter().map(|(_, m)| m).max_by_key(|m| m.epoch);
        let slots = match (old, &opts.slots) {
            (None, Some(given)) if held.is_none_or(|h| given.epoch > h.epoch) => Some(given.clone()),
            (None, _) => Some(match held {
                Some(h) if h.shards() == n && h.replicas() == topo.groups().as_slice() => h.clone(),
                // The same slots on the servers listed, named under the
                // next epoch: held without servers (a coordinator before
                // protocol 9 gave it), or with others (the list lays the
                // cluster out anew, above).
                Some(h) if h.shards() == n && named => h.at_epoch(h.epoch + 1).with_replicas(topo.groups()).map_err(ApiError::bad)?,
                Some(h) if h.shards() == n => h.clone(),
                Some(h) => return Err(ApiError::bad(format!("the shards hold a cluster map of {} shards (epoch {}), and --shards names {n}", h.shards(), h.epoch))),
                None => SlotMap::even(n, 1),
            }),
            (Some(r), Some(_)) => return Err(ApiError::bad(format!("a slot map needs every shard to speak protocol 4; {} speaks {}", r.url, r.protocol.load(Ordering::Relaxed)))),
            (Some(r), None) => {
                tracing::warn!(shard = %r.url, protocol = r.protocol.load(Ordering::Relaxed), "a shard is from before slots (protocol 4): writes go by the old hash until every shard is upgraded and the coordinator restarts");
                None
            }
        };
        if let Some(m) = &slots {
            if m.shards() != n {
                return Err(ApiError::bad(format!("the slot map has {} shards, and --shards {n}", m.shards())));
            }
        }
        let slots = match slots {
            Some(m) if named && m.replicas().is_empty() => Some(m.with_replicas(topo.groups()).map_err(ApiError::bad)?),
            m => m,
        };
        topo.slots = slots;
        if let Some(m) = &topo.slots {
            let gives = topo.replicas.iter().enumerate().flat_map(|(i, rs)| rs.iter().filter(|r| r.protocol.load(Ordering::Relaxed) != 0).map(move |r| (i, r)));
            let answers = join_all(gives.map(|(i, r)| {
                let mut c = r.client.clone();
                async move { (r, tokio::time::timeout(Duration::from_secs(60), give_slots(&mut c, m, i)).await) }
            }))
            .await;
            for (r, a) in answers {
                match a {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) => return Err(ApiError { status: ApiError::from(e.clone()).status, message: format!("{} refused slot map epoch {}: {}", r.url, m.epoch, e.message()) }),
                    Err(_) => return Err(ApiError { status: 504, message: format!("{} took the slot map no answer within 60 s", r.url) }),
                }
            }
        }
        Ok(Coordinator {
            topo: RwLock::new(Arc::new(topo)),
            changing: tokio::sync::Mutex::new(()),
            newer: AtomicU64::new(0),
            wake: Arc::new(tokio::sync::Notify::new()),
            schema,
            catalog: RwLock::new(Arc::new(catalog)),
            profiles: RwLock::new(Arc::new(profiles)),
            asked: Mutex::new(Some(Instant::now())),
            opts,
            epoch: Instant::now(),
            turn: AtomicUsize::new(0),
            retries: AtomicU64::new(0),
            hedges: AtomicU64::new(0),
            hedge_budget: HedgeBudget::new(),
            fetches: AtomicU64::new(0),
            fetch_lost: AtomicU64::new(0),
        })
    }

    /// The shards and their servers now.
    pub(crate) fn topo(&self) -> Arc<Topology> {
        self.topo.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Every server of every shard: its shard, URL and client.
    pub fn replica_clients(&self) -> Vec<(usize, String, Client)> {
        let t = self.topo();
        t.replicas.iter().enumerate().flat_map(|(i, rs)| rs.iter().map(move |r| (i, r.url.clone(), r.client.clone()))).collect()
    }

    /// The cluster map it routes by (`None` while writes go by the hash
    /// before slots).
    pub fn map(&self) -> Option<SlotMap> {
        self.topo().slots.clone()
    }

    /// The tables it serves, the first first: those every replica named
    /// alike when last asked ([`Self::refresh`]).
    pub fn catalog(&self) -> Vec<Arc<Schema>> {
        self.catalog.read().unwrap_or_else(|e| e.into_inner()).to_vec()
    }

    /// The rank profiles it serves, by table: those every replica held
    /// alike when last asked ([`Self::refresh`]).
    pub fn rank_profiles(&self) -> Arc<Vec<(String, RankProfile)>> {
        self.profiles.read().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Asks every replica for its tables and rank profiles again, at most
    /// once a second unless `now`, and serves those every replica that
    /// answered (within 5 s) holds alike. Returns whether they changed.
    pub async fn refresh(&self, now: bool) -> bool {
        let t = &self.topo();
        {
            let mut asked = self.asked.lock().unwrap_or_else(|e| e.into_inner());
            if !now && asked.is_some_and(|t| t.elapsed() < Duration::from_secs(1)) {
                return false;
            }
            *asked = Some(Instant::now());
        }
        let calls = t.replicas.iter().flatten().map(|r| {
            let mut c = r.client.clone();
            async move { tokio::time::timeout(Duration::from_secs(5), c.schema(pb::Empty {})).await }
        });
        let answers: Vec<pb::SchemaResponse> = join_all(calls).await.into_iter().filter_map(|a| a.ok()?.ok()).map(|a| a.into_inner()).collect();
        let tables = match common_tables(&answers) {
            Ok(t) if t.first().is_some_and(|f| **f == self.schema) => t,
            _ => return false,
        };
        let profiles = common_profiles(&answers);
        let mut changed = false;
        {
            let mut held = self.catalog.write().unwrap_or_else(|e| e.into_inner());
            if **held != tables {
                *held = Arc::new(tables);
                changed = true;
            }
        }
        let mut held = self.profiles.write().unwrap_or_else(|e| e.into_inner());
        if **held != profiles {
            *held = Arc::new(profiles);
            changed = true;
        }
        changed
    }

    /// What `/metrics` shows of the shards: calls by outcome, retries and
    /// hedges, and replicas avoided now after failing or being outrun.
    pub fn metrics(&self) -> Vec<Family> {
        let t = &self.topo();
        let outcomes = ["ok", "error", "timeout"];
        let calls = t.outcomes.iter().enumerate().flat_map(|(i, o)| outcomes.iter().zip(o.iter()).map(move |(name, n)| (format!("shard=\"{i}\",outcome=\"{name}\""), n.load(Ordering::Relaxed) as f64))).collect();
        let now = self.now_ms();
        let avoided = t.replicas.iter().flatten().filter(|r| r.avoid_until.load(Ordering::Relaxed) > now).count();
        vec![
            Family { name: "strata_shard_calls_total", ty: "counter", help: "Calls to a shard (its replicas, hedges and retries together), by outcome: ok, error, timeout.", values: calls },
            Family::one("strata_shard_retries_total", "counter", "Calls asked of another replica after one failed.", self.retries.load(Ordering::Relaxed) as f64),
            Family::one("strata_shard_hedges_total", "counter", "Calls asked of another replica too after the first had not answered within --hedge-ms.", self.hedges.load(Ordering::Relaxed) as f64),
            Family::one("strata_replicas_avoided", "gauge", "Replicas asked last for now, after they failed or were outrun.", avoided as f64),
            Family::one("strata_shard_protocol", "gauge", "The protocol reads speak to the shards: the oldest a replica speaks.", t.protocol() as f64),
            Family::one("strata_slot_epoch", "gauge", "The epoch of the slot map writes are routed by; 0 while they go by the hash before slots.", t.epoch() as f64),
            Family::one("strata_fetch_queries_total", "counter", "Queries answered by query then fetch.", self.fetches.load(Ordering::Relaxed) as f64),
            Family::one("strata_fetch_lost_rows_total", "counter", "Rows of query-then-fetch pages that no fetch found: their shard did not answer the fetch (partial answers), or another replica did, without them.", self.fetch_lost.load(Ordering::Relaxed) as f64),
        ]
    }

    fn now_ms(&self) -> u64 {
        self.epoch.elapsed().as_millis() as u64
    }

    /// The protocol reads speak to the shards: the oldest a replica
    /// speaks, of those that said (the newest while none has).
    pub fn protocol(&self) -> u32 {
        self.topo().protocol()
    }

    /// The epoch of the slot map writes are routed by; 0 without one.
    pub fn epoch(&self) -> u64 {
        self.topo().epoch()
    }

    /// The shard holding `key`.
    fn shard_of(&self, t: &Topology, key: &strata::value::Value) -> usize {
        match &t.slots {
            Some(m) => m.shard_of(key),
            None => hash_shard_of(key, t.replicas.len()),
        }
    }

    /// The protocol to speak with replica `r` of shard `i`, `max` at most:
    /// asked of it first when not known, or not asked for a minute, and
    /// given the slot map when it holds another.
    async fn speak(&self, t: &Topology, i: usize, r: usize, c: &mut Client, max: u32) -> Result<u32, tonic::Status> {
        let rep = &t.replicas[i][r];
        let p = rep.protocol.load(Ordering::Relaxed);
        if p != 0 && self.now_ms() < rep.hello_at.load(Ordering::Relaxed) + HELLO_EVERY_MS {
            return Ok(p.min(max));
        }
        let said = hello(c).await?;
        match t.slots.as_ref() {
            // It holds a newer cluster map: the watcher takes it up.
            Some(m) if said.epoch > m.epoch => {
                self.newer.fetch_max(said.epoch, Ordering::Relaxed);
                self.wake.notify_one();
            }
            Some(m) if said.protocol >= 4 && said.epoch != m.epoch => {
                give_slots(c, m, i).await?;
            }
            _ => {}
        }
        rep.heard(&said);
        rep.hello_at.store(self.now_ms(), Ordering::Relaxed);
        Ok(said.protocol.min(max))
    }

    fn avoid(&self, t: &Topology, i: usize, r: usize) {
        t.replicas[i][r].avoid_until.store(self.now_ms() + self.opts.cool_off.as_millis() as u64, Ordering::Relaxed);
    }

    /// The order to ask shard `i`'s replicas in: `first` if given, then in
    /// turn from `start`, those being avoided last.
    fn order(&self, t: &Topology, i: usize, start: usize, first: Option<usize>) -> Vec<usize> {
        let n = t.replicas[i].len();
        let now = self.now_ms();
        let mut order: Vec<usize> = (0..n).map(|k| (start + k) % n).collect();
        order.sort_by_key(|&r| (t.replicas[i][r].avoid_until.load(Ordering::Relaxed) > now, Some(r) != first));
        order
    }

    /// Calls shard `i` with `f(replica, client)`, asking its replicas in
    /// [`Self::order`]: the next one when one fails with an error another
    /// may not have, and also when one has not answered after the hedge
    /// delay and the hedge budget allows. Returns the replica that
    /// answered and its answer.
    async fn call<T, F, Fut>(&self, t: &Topology, i: usize, start: usize, first: Option<usize>, deadline: Instant, f: F) -> Result<(usize, T), ApiError>
    where
        F: Fn(usize, Client) -> Fut,
        Fut: Future<Output = Result<tonic::Response<T>, tonic::Status>>,
    {
        let order = self.order(t, i, start, first);
        let limit = self.opts.shard_timeout.map_or(deadline, |t| deadline.min(Instant::now() + t));
        let mut pending = FuturesUnordered::new();
        let mut asked = Vec::new();
        // Asked, but passed over without being sent the read.
        let mut passed = Vec::new();
        let name = crate::telemetry::rpc_name::<T>();
        let launch = |r: usize, again: bool| {
            let fut = f(r, t.replicas[i][r].client.clone());
            // Timed in the request's trace, a hedge that lost as "outrun".
            let span = crate::telemetry::CallSpan { name: name.clone(), shard: i, replica: t.replicas[i][r].url.clone(), t0: Instant::now(), again, outcome: None };
            async move {
                // The whole span, not only the field set below, moves in.
                let mut span = span;
                let res = fut.await;
                span.outcome = Some(if res.is_ok() { "ok" } else { "error" });
                (r, res)
            }
        };
        pending.push(launch(order[0], false));
        asked.push(order[0]);
        self.hedge_budget.earn();
        let mut hedge_at = Instant::now() + self.opts.hedge;
        // Off once a hedge was due and the budget had none.
        let mut hedging = true;
        loop {
            let more = asked.len() < order.len();
            tokio::select! {
                Some((r, res)) = pending.next() => match res {
                    Ok(answer) => {
                        // Replicas still working were outrun (and those that
                        // failed are avoided already).
                        for &other in asked.iter().filter(|&&o| o != r && !passed.contains(&o)) {
                            self.avoid(t, i, other);
                        }
                        t.outcomes[i][0].fetch_add(1, Ordering::Relaxed);
                        return Ok((r, answer.into_inner()));
                    }
                    Err(s) if try_elsewhere(&s) => {
                        if s.metadata().get(PASSED_OVER).is_some() {
                            passed.push(r);
                        } else {
                            self.avoid(t, i, r);
                            // It may come back on another version: asked again.
                            t.replicas[i][r].protocol.store(0, Ordering::Relaxed);
                        }
                        // The primary holds every write: asked next.
                        if let Some(next) = self.next(t, i, &order, &asked, true) {
                            self.retries.fetch_add(1, Ordering::Relaxed);
                            pending.push(launch(next, true));
                            asked.push(next);
                            hedge_at = Instant::now() + self.opts.hedge;
                        } else if pending.is_empty() {
                            let mut e = self.named(&t.replicas[i][r].url, s);
                            if order.len() > 1 {
                                e.message = format!("every replica of shard {i} failed; the last: {}", e.message);
                            }
                            t.outcomes[i][1].fetch_add(1, Ordering::Relaxed);
                            return Err(e);
                        }
                    }
                    Err(s) => {
                        t.outcomes[i][1].fetch_add(1, Ordering::Relaxed);
                        return Err(self.named(&t.replicas[i][r].url, s));
                    }
                },
                _ = tokio::time::sleep_until(hedge_at.into()), if more && hedging => {
                    if !self.hedge_budget.spend() {
                        hedging = false;
                        continue;
                    }
                    self.hedges.fetch_add(1, Ordering::Relaxed);
                    if let Some(next) = self.next(t, i, &order, &asked, false) {
                        pending.push(launch(next, true));
                        asked.push(next);
                    }
                    hedge_at = Instant::now() + self.opts.hedge;
                }
                _ = tokio::time::sleep_until(limit.into()) => {
                    for &r in &asked {
                        self.avoid(t, i, r);
                    }
                    let urls: Vec<&str> = asked.iter().map(|&r| t.replicas[i][r].url.as_str()).collect();
                    t.outcomes[i][2].fetch_add(1, Ordering::Relaxed);
                    return Err(ApiError { status: 504, message: format!("shard {i} ({}): no answer in time", urls.join(", ")) });
                }
            }
        }
    }

    /// The next replica of shard `i` to ask: the first of `order` not
    /// `asked` yet, or with `primary_first` the shard's primary, if it was
    /// not asked.
    fn next(&self, t: &Topology, i: usize, order: &[usize], asked: &[usize], primary_first: bool) -> Option<usize> {
        let mut left = order.iter().copied().filter(|r| !asked.contains(r));
        let primary = left.clone().find(|&r| primary_first && t.replicas[i][r].takes_writes());
        primary.or_else(|| left.next())
    }

    /// Refuses (Unavailable, so that another copy is asked) to send a read
    /// that must see transaction `min` to replica `r` of shard `i`, a
    /// replica that speaks a protocol before 5 and would not wait for it.
    /// A primary, or a server without one, holds every write it answered.
    /// The replica did not fail, so it is not avoided ([`PASSED_OVER`]).
    fn reads_after(&self, t: &Topology, i: usize, r: usize, min: u64) -> Result<(), tonic::Status> {
        let rep = &t.replicas[i][r];
        let p = rep.protocol.load(Ordering::Relaxed);
        if min == 0 || rep.role() != pb::Role::Replica || p >= 5 {
            return Ok(());
        }
        let mut s = tonic::Status::unavailable(format!("{} speaks protocol {p}, before 5: it would not wait for a write a read must see", rep.url));
        s.metadata_mut().insert(PASSED_OVER, tonic::metadata::MetadataValue::from_static("1"));
        Err(s)
    }

    /// The number of the write each shard's answer must see: `min_seq_nos`
    /// as the coordinator's writes answer them (`seq_nos`), one per shard
    /// (fewer: 0 for the rest).
    fn after(&self, t: &Topology, after: &After) -> Result<Vec<u64>, ApiError> {
        let mins = match after {
            After::Any => return Ok(Vec::new()),
            After::Seq(_) => return Err(ApiError::bad("a coordinator takes min_seq_nos, one sequence number per shard, as its writes answer seq_nos; min_seq_no is a server's")),
            After::Shards(v) => v,
        };
        if mins.len() > t.replicas.len() {
            return Err(ApiError::bad(format!("min_seq_nos has {} numbers, and the coordinator {} shards", mins.len(), t.replicas.len())));
        }
        if let Some(i) = (0..mins.len()).find(|&i| mins[i] > 0 && !self.numbered_alike(t, i)) {
            return Err(ApiError::bad(format!("min_seq_nos names a write on shard {i}, whose replicas each number their own writes: its write answers 0 there")));
        }
        Ok(mins.clone())
    }

    fn named(&self, url: &str, s: tonic::Status) -> ApiError {
        let mut e = ApiError::from(s);
        e.message = format!("shard {url}: {}", e.message);
        e
    }

    /// The statistics of shards `which`, and the replica and lease each
    /// came from. A lease lasts until `deadline` and a second.
    async fn shard_stats(&self, t: &Topology, read: &Read, protocol: u32, which: Vec<usize>, start: usize, deadline: Instant) -> Vec<(usize, Result<(TermStats, usize, u64), ApiError>)> {
        let calls = which.into_iter().map(|i| {
            async move {
                let a = self
                    .call(t, i, start, None, deadline, |r, mut c| async move {
                        let p = self.speak(t, i, r, &mut c, protocol).await?;
                        self.reads_after(t, i, r, read.min(i))?;
                        c.term_stats(pb::QueryRequest { timeout_ms: ms_left(deadline), ..read.request(p, i) }).await
                    })
                    .await;
                let a = a.and_then(|(r, a)| {
                    let s: TermStats = serde_json::from_slice(&a.stats_json).map_err(ApiError::internal)?;
                    Ok((s, r, a.snapshot))
                });
                (i, a)
            }
        });
        join_all(calls).await
    }

    /// Summed statistics, from every shard, of `sql` or of `plan`, seeing
    /// the writes `after` names.
    pub async fn term_stats(&self, sql: &str, schema: &Schema, plan: Option<&Plan>, params: &Params, after: &After, deadline: Instant) -> Result<(TermStats, Vec<u64>), ApiError> {
        let t = &self.topo();
        let start = self.turn.fetch_add(1, Ordering::Relaxed);
        let mut sum = TermStats::default();
        let mut leases = Vec::new();
        let protocol = t.protocol();
        let read = Read::new(sql, params, plan, protocol, t.epoch(), || Plan::bind(sql, schema, params))?.after(self.after(t, after)?);
        for (_, a) in self.shard_stats(t, &read, protocol, (0..t.replicas.len()).collect(), start, deadline).await {
            let (s, _, lease) = a?;
            sum.add(&s);
            leases.push(lease);
        }
        Ok((sum, leases))
    }

    /// The next codes of `prefixes` in semantic-ID field `field` among the
    /// rows `sql` or `plan` filters, on every shard ([`SIDNEXT_PROTOCOL`]),
    /// their rows summed: per prefix, in the order of the codes' last
    /// numbers.
    #[allow(clippy::too_many_arguments)]
    pub async fn sid_next(&self, sql: &str, schema: &Schema, plan: Option<&Plan>, params: &Params, field: &str, prefixes: &[String], after: &After, deadline: Instant) -> Result<Vec<Vec<(String, u64)>>, ApiError> {
        let t = &self.topo();
        let start = self.turn.fetch_add(1, Ordering::Relaxed);
        let protocol = t.protocol();
        let read = &Read::new(sql, params, plan, protocol, t.epoch(), || Plan::bind(sql, schema, params))?.after(self.after(t, after)?);
        let calls = (0..t.replicas.len()).map(|i| async move {
            self.call(t, i, start, None, deadline, |r, mut c| async move {
                let p = self.speak(t, i, r, &mut c, PROTOCOL).await?;
                if p < SIDNEXT_PROTOCOL {
                    return Err(tonic::Status::unimplemented(format!("it speaks protocol {p}, and /v1/sid/next needs {SIDNEXT_PROTOCOL}: upgrade it")));
                }
                self.reads_after(t, i, r, read.min(i))?;
                c.sid_next(pb::SidNextRequest { query: Some(pb::QueryRequest { timeout_ms: ms_left(deadline), ..read.request(p, i) }), field: field.to_string(), prefixes: prefixes.to_vec() }).await
            })
            .await
        });
        let mut sums: Vec<BTreeMap<u32, (String, u64)>> = vec![BTreeMap::new(); prefixes.len()];
        for a in join_all(calls).await {
            let (_, a) = a?;
            if a.next.len() != prefixes.len() {
                return Err(ApiError::internal(format!("a shard answered {} prefixes of {}", a.next.len(), prefixes.len())));
            }
            for (sum, next) in sums.iter_mut().zip(a.next) {
                for (code, count) in next.codes.into_iter().zip(next.counts) {
                    let last = code.rsplit('.').next().and_then(|x| x.parse::<u32>().ok()).ok_or_else(|| ApiError::internal(format!("a shard answered code {code:?}")))?;
                    sum.entry(last).or_insert_with(|| (code, 0)).1 += count;
                }
            }
        }
        Ok(sums.into_iter().map(|m| m.into_values().collect()).collect())
    }

    /// Runs `select`, a query of table `schema`, on every shard and merges
    /// their answers. A guarded query (`min_rows`, `max_rows`) runs at the
    /// level the sums of its shards' counts choose ([`Self::level_counts`]).
    pub async fn query(&self, req: &QueryReq, schema: &Schema, select: &Select, deadline: Instant) -> Result<Merged, ApiError> {
        let t = &self.topo();
        let Some(g) = &select.guard else { return self.query_at(t, req, schema, select, deadline).await };
        let partial = req.partial.unwrap_or(self.opts.partial);
        let read = Read::new(&req.sql, &req.params, req.plan.as_deref(), t.protocol(), t.epoch(), || Ok(Plan::of_select(schema, select)))?.after(self.after(t, &req.after)?);
        let mut failed = self.skipped(t, partial);
        let (n, counts) = self.level_counts(t, &read, partial, &mut failed, deadline).await?;
        let (m, line) = match counts.is_empty() {
            true => (n, "guard: no items to count".to_string()),
            false => {
                let m = g.level(n, &counts);
                (m, g.line(n, &counts, m))
            }
        };
        // The shards run the query at that level, a plan without a guard.
        let settled = select.at_level(m);
        let req = QueryReq { plan: Some(Arc::new(Plan::of_select(schema, &settled))), params: Params::new(), ..req.clone() };
        let mut merged = self.query_at(t, &req, schema, &settled, deadline).await?;
        merged.rs.stats.plan.insert(0, line);
        Ok(merged)
    }

    /// The loosest level of `read`'s guard and how many rows each level
    /// from it to the strictest matches, summed over the shards that
    /// answer: every shard ([`GUARD_PROTOCOL`]), but those `partial` leaves
    /// out, which `failed` then names.
    async fn level_counts(&self, t: &Topology, read: &Read, partial: bool, failed: &mut [Option<ApiError>], deadline: Instant) -> Result<(usize, Vec<u64>), ApiError> {
        let start = self.turn.fetch_add(1, Ordering::Relaxed);
        let calls = (0..t.replicas.len()).filter(|&i| failed[i].is_none()).map(|i| async move {
            let a = self
                .call(t, i, start, None, deadline, |r, mut c| async move {
                    let p = self.speak(t, i, r, &mut c, PROTOCOL).await?;
                    if p < GUARD_PROTOCOL {
                        return Err(tonic::Status::invalid_argument(format!("it speaks protocol {p}, and a guarded query (min_rows, max_rows) needs {GUARD_PROTOCOL}: upgrade it")));
                    }
                    self.reads_after(t, i, r, read.min(i))?;
                    c.guard_counts(pb::QueryRequest { timeout_ms: ms_left(deadline), ..read.request(p, i) }).await
                })
                .await;
            (i, a)
        });
        let mut sum: Option<(usize, Vec<u64>)> = None;
        for (i, a) in join_all(calls).await {
            let a = match a {
                Ok((_, a)) => a,
                Err(e) => {
                    self.leave_out(t, partial, i, e, failed)?;
                    continue;
                }
            };
            match &mut sum {
                None => sum = Some((a.need as usize, a.counts)),
                Some((n, counts)) if *n == a.need as usize && counts.len() == a.counts.len() => counts.iter_mut().zip(a.counts).for_each(|(c, x)| *c += x),
                Some(_) => return Err(ApiError::internal(format!("shard {} counted another guard's levels: from {} to {}", t.shards[i], a.need, a.need as usize + a.counts.len()))),
            }
        }
        sum.ok_or_else(|| failed.iter_mut().find_map(|f| f.take()).unwrap_or_else(|| ApiError::internal("no shard answered")))
    }

    /// A guard's loosest level and the rows of each level, over every
    /// shard, of `sql` or `plan` seeing the writes `after` names.
    #[allow(clippy::too_many_arguments)]
    pub async fn guard_counts(&self, sql: &str, schema: &Schema, plan: Option<&Plan>, params: &Params, select: &Select, after: &After, deadline: Instant) -> Result<(usize, Vec<u64>), ApiError> {
        let t = &self.topo();
        let read = Read::new(sql, params, plan, t.protocol(), t.epoch(), || Ok(Plan::of_select(schema, select)))?.after(self.after(t, after)?);
        let mut failed = self.skipped(t, false);
        self.level_counts(t, &read, false, &mut failed, deadline).await
    }

    /// Per shard, the failure that leaves it out of a `partial` read: that
    /// no replica answered in the last second.
    fn skipped(&self, t: &Topology, partial: bool) -> Vec<Option<ApiError>> {
        let mut failed: Vec<Option<ApiError>> = (0..t.replicas.len()).map(|_| None).collect();
        if partial {
            let now = self.now_ms();
            for (i, f) in failed.iter_mut().enumerate() {
                if t.skip_until[i].load(Ordering::Relaxed) > now {
                    *f = Some(ApiError { status: 503, message: format!("shard {}: no replica answered in the last second", t.shards[i]) });
                }
            }
        }
        failed
    }

    /// [`Self::query`] of a query without a guard.
    async fn query_at(&self, t: &Topology, req: &QueryReq, schema: &Schema, select: &Select, deadline: Instant) -> Result<Merged, ApiError> {
        if select.projection.iter().any(|(_, p)| *p == Projection::SeqNo) && !(0..t.replicas.len()).all(|i| self.numbered_alike(t, i)) {
            return Err(ApiError::bad(ONE_REPLICA));
        }
        let partial = req.partial.unwrap_or(self.opts.partial);
        let protocol = t.protocol();
        let n = t.replicas.len();
        // A cross-encoder scores here, over the text of its phase's rows,
        // which the fetch round reads.
        let model = select.model_phase().is_some();
        if model && protocol < 3 {
            return Err(ApiError::bad("a cross-encoder over shards needs shards that fetch rows (protocol 3)"));
        }
        // Query then fetch: the query round answers the keys alone.
        let ids = match model || (protocol >= 3 && self.opts.fetch && n > 1 && strata::cluster::fetches(select, schema) && self.fetch_pays(t, select)) {
            true => Some(strata::cluster::ids(select, schema)?),
            false => None,
        };
        let ids_plan = ids.as_ref().map(|q| Plan::of_select(schema, q));
        let none = Params::new();
        let (params, given) = match &ids_plan {
            Some(p) => (&none, Some(p)),
            None => (&req.params, req.plan.as_deref()),
        };
        let read = &Read::new(&req.sql, params, given, protocol, t.epoch(), || Ok(Plan::of_select(schema, select)))?.after(self.after(t, &req.after)?);
        let start = self.turn.fetch_add(1, Ordering::Relaxed);
        let ranked_text = strata::cluster::scores_bm25(select);
        // Per shard: the replica holding a lease for this query, and the lease.
        let mut leased: Vec<Option<(usize, u64)>> = vec![None; n];
        let mut failed = self.skipped(t, partial);
        // What the coordinator does is in the query's steps
        // ([`strata::query::steps`]), each shard's call with the shard's own
        // steps inside it.
        let mut own: Vec<Step> = Vec::new();
        let stats = match &req.stats {
            Some(s) => Some(s.clone()),
            None if ranked_text && n > 1 => {
                let t0 = Instant::now();
                let mut sum = TermStats::default();
                let which = (0..n).filter(|&i| failed[i].is_none()).collect();
                for (i, a) in self.shard_stats(t, read, protocol, which, start, deadline).await {
                    match a {
                        Ok((s, r, lease)) => {
                            sum.add(&s);
                            leased[i] = Some((r, lease));
                        }
                        Err(e) => self.leave_out(t, partial, i, e, &mut failed)?,
                    }
                }
                own.push(leaf_step("term statistics", format!("BM25 statistics summed over {n} shards"), t0, None));
                Some(sum)
            }
            None => None,
        };
        let stats_json = stats.map(|s| serde_json::to_vec(&s).unwrap_or_default()).unwrap_or_default();
        let leased = &leased;
        let hold = ids.is_some();
        let sent = &Sent::default();
        let calls = (0..n).filter(|&i| failed[i].is_none()).map(|i| {
            let stats_json = stats_json.clone();
            let first = leased[i].map(|(r, _)| r);
            async move {
                let rs = self.call(t, i, start, first, deadline, |r, mut c| {
                    let stats_json = stats_json.clone();
                    // The lease holds on the replica that granted it; another
                    // replica scores from its latest version.
                    let snapshot = leased[i].filter(|(lr, _)| *lr == r).map_or(0, |(_, l)| l);
                    async move {
                        let p = self.speak(t, i, r, &mut c, protocol).await?;
                        self.reads_after(t, i, r, read.min(i))?;
                        // From protocol 3, the lease is held for the fetch
                        // round, or ended once answered.
                        let (hold, release) = (hold && p >= 3, !hold && snapshot != 0 && p >= 3);
                        timed(sent, i, r, c.query(pb::QueryRequest { timeout_ms: ms_left(deadline), shard: true, stats_json, snapshot, hold, release, ..read.request(p, i) })).await
                    }
                });
                (i, rs.await)
            }
        });
        let mut shards = Vec::new();
        // Per shard answering: the replica that did, and the lease it holds.
        let mut held = Vec::new();
        for (i, a) in join_all(calls).await {
            match a {
                Ok((r, a)) => {
                    held.push((i, r, a.snapshot));
                    shards.push(self.shard_answer(t, i, r, a, sent, "query")?);
                }
                Err(e) => self.leave_out(t, partial, i, e, &mut failed)?,
            }
        }

        if shards.is_empty() {
            let e = failed.into_iter().flatten().next();
            return Err(e.unwrap_or_else(|| ApiError::internal("no shard answered")));
        }
        let mut merge_step = None;
        let mut merge = |q: &Select, shards: Vec<ResultSet>| {
            let t0 = Instant::now();
            let rs = strata::cluster::merge(q, shards)?;
            merge_step = Some(leaf_step("merge", format!("{} shard answers", held.len()), t0, Some(rs.rows.len() as u64)));
            Ok::<_, ApiError>(rs)
        };
        let rs = match &ids {
            None => merge(select, shards)?,
            Some(ids) => {
                // Each key's shard, for the fetch round.
                let mut owner = HashMap::new();
                for (&(i, _, _), a) in held.iter().zip(&shards) {
                    owner.extend(a.keys.iter().map(|k| (Part::of(k), i)));
                }
                let ranked = merge(ids, shards)?;
                let mut keys = vec![Vec::new(); n];
                for k in &ranked.keys {
                    let i = owner.get(&Part::of(k)).ok_or_else(|| ApiError::internal("a merged key no shard answered"))?;
                    keys[*i].push(k.clone());
                }
                match model {
                    false => self.fetch(t, &req.sql, schema, select, ranked, &held, keys, (start, deadline, protocol, partial), &read.after, &stats_json, &mut failed).await?,
                    // The model phase's rows with their text, then the
                    // model, off the async threads.
                    true => {
                        let with = strata::cluster::with_texts(select);
                        let rows = self.fetch(t, &req.sql, schema, &with, ranked, &held, keys, (start, deadline, protocol, partial), &read.after, &stats_json, &mut failed).await?;
                        let select = select.clone();
                        let check = move || match Instant::now() < deadline {
                            true => Ok(()),
                            false => Err(strata::Error::Timeout("the query ran past its deadline".into())),
                        };
                        tokio::task::spawn_blocking(move || strata::cluster::rerank_models(&select, rows, &mut { check }))
                            .await
                            .map_err(|e| ApiError::internal(format!("cross-encoder: {e}")))??
                    }
                }
            }
        };
        let missing = failed.iter().enumerate().filter(|(_, f)| f.is_some()).map(|(i, _)| t.shards[i].clone()).collect();
        let mut rs = rs;
        // The term statistics came first; the merge (and a fetch round's
        // calls, inside it) last.
        rs.stats.steps.extend(merge_step);
        rs.stats.steps.splice(0..0, own);
        Ok(Merged { rs, missing })
    }

    /// Shard `i`'s answer `a` from replica `r`, its steps moved onto this
    /// server's clock, inside a `shard` step for the call (from sent to
    /// received, as `sent` holds it) and an `on the shard` step for the
    /// shard's own time (`took_ms`), centred in the call: what the network
    /// took is the difference, half each way.
    fn shard_answer(&self, t: &Topology, i: usize, r: usize, a: pb::QueryResponse, sent: &Sent, what: &str) -> Result<ResultSet, ApiError> {
        let took_us = a.took_ms * 1e3;
        let mut rs = result_from_pb(a)?;
        let Some((t0, t1)) = sent.lock().unwrap_or_else(|e| e.into_inner()).get(&(i, r)).copied() else { return Ok(rs) };
        let call_us = t1.saturating_duration_since(t0).as_nanos() as f64 / 1e3;
        let start = steps::us_at(t0);
        let on_shard = start + ((call_us - took_us) / 2.0).max(0.0);
        steps::shift(&mut rs.stats.steps, on_shard);
        let rows = Some(rs.rows.len() as u64);
        let own = Step { op: "on the shard".into(), detail: format!("network {:.2} ms", ((call_us - took_us) / 1e3).max(0.0)), start_us: steps::tenth(on_shard), us: steps::tenth(took_us.min(call_us)), rows, ..Step::default() };
        steps::wrap(&mut rs.stats.steps, 0, own);
        let replica = &t.replicas[i][r].url;
        let call = Step { op: "shard".into(), detail: format!("shard {i} {what}, {replica}"), start_us: start, us: steps::tenth(call_us), rows, shard: Some(i as u32), ..Step::default() };
        steps::wrap(&mut rs.stats.steps, 0, call);
        Ok(rs)
    }

    /// Whether query then fetch pays for its second round: when one round
    /// would send at least [`FETCH_FROM`] times the page's rows, every shard
    /// its top `LIMIT + OFFSET`, as it does for a fused score or a ranking
    /// expression, whose shards send each input's candidates to its depth,
    /// and for rerank phases, whose shards send the first phase's top count.
    fn fetch_pays(&self, t: &Topology, select: &Select) -> bool {
        let page = select.limit.unwrap_or(0);
        matches!(crate::metrics::kind(select), "hybrid" | "boosted" | "expression" | "reranked") || t.replicas.len() * (page + select.offset) >= FETCH_FROM * page
    }

    /// Leaves shard `i` out of a partial answer (and the next second's)
    /// after it failed with `e`; a request it rejected fails the query, as
    /// does any failure without `partial`.
    fn leave_out(&self, t: &Topology, partial: bool, i: usize, e: ApiError, failed: &mut [Option<ApiError>]) -> Result<(), ApiError> {
        if !partial || e.status == 400 {
            return Err(e);
        }
        t.skip_until[i].store(self.now_ms() + 1000, Ordering::Relaxed);
        failed[i] = Some(e);
        Ok(())
    }

    /// The fetch round of query then fetch: the columns of the page
    /// `ranked`, `keys[i]` from shard `i`, read from the snapshot its
    /// query round held (`held`: shard, replica, lease), whose lease the
    /// fetch ends, a ranking expression among them scored with the query
    /// round's BM25 statistics (`stats_json`). The leases of shards holding
    /// none of the page's rows are ended alongside, without waiting.
    async fn fetch(
        &self,
        t: &Topology,
        sql: &str,
        schema: &Schema,
        select: &Select,
        ranked: ResultSet,
        held: &[(usize, usize, u64)],
        mut keys: Vec<Vec<Value>>,
        (start, deadline, protocol, partial): (usize, Instant, u32, bool),
        after: &[u64],
        stats_json: &[u8],
        failed: &mut [Option<ApiError>],
    ) -> Result<ResultSet, ApiError> {
        self.fetches.fetch_add(1, Ordering::Relaxed);
        let mut reads = Vec::new();
        for &(i, r, lease) in held {
            if !keys[i].is_empty() {
                let q = strata::cluster::fetch(select, schema, std::mem::take(&mut keys[i]))?;
                let plan = Plan::of_select(schema, &q);
                reads.push((i, r, lease, Read::new(sql, &Params::new(), Some(&plan), protocol, t.epoch(), || Ok(plan.clone()))?.after(after.to_vec())));
            } else if lease != 0 {
                let mut c = t.replicas[i][r].client.clone();
                tokio::spawn(async move { tokio::time::timeout(Duration::from_secs(5), c.release(pb::ReleaseRequest { snapshots: vec![lease] })).await });
            }
        }
        let sent = &Sent::default();
        let calls = reads.iter().map(|(i, held_r, lease, read)| {
            let (i, held_r, lease) = (*i, *held_r, *lease);
            async move {
                let a = self.call(t, i, start, Some(held_r), deadline, |r, mut c| {
                    // Another replica reads its latest version.
                    let snapshot = if r == held_r { lease } else { 0 };
                    async move {
                        let p = self.speak(t, i, r, &mut c, protocol).await?;
                        self.reads_after(t, i, r, read.min(i))?;
                        let stats_json = stats_json.to_vec();
                        timed(sent, i, r, c.query(pb::QueryRequest { timeout_ms: ms_left(deadline), shard: true, stats_json, snapshot, release: snapshot != 0, ..read.request(p, i) })).await
                    }
                });
                (i, a.await)
            }
        });
        let mut fetched = Vec::new();
        for (i, a) in join_all(calls).await {
            match a {
                Ok((r, a)) => fetched.push(self.shard_answer(t, i, r, a, sent, "fetch")?),
                Err(e) => self.leave_out(t, partial, i, e, failed)?,
            }
        }
        let (mut rs, lost) = strata::cluster::fill(select, ranked, fetched)?;
        if lost > 0 {
            self.fetch_lost.fetch_add(lost as u64, Ordering::Relaxed);
            rs.stats.plan.push(format!("fetch: {lost} rows of the page left out: their shard did not answer the fetch, or another replica did, without them"));
        }
        Ok(rs)
    }

    /// Whether every replica of shard `i` holds the same sequence numbers:
    /// it has one, or a primary that ships its writes to the others.
    fn numbered_alike(&self, t: &Topology, i: usize) -> bool {
        t.replicas[i].len() == 1 || t.replicas[i].iter().any(|r| r.role() == pb::Role::Primary)
    }

    /// The replica of shard `i` that takes its writes: its primary (of the
    /// newest term, and not replaced), asking every replica again when
    /// none is (a promotion may have moved it). `None` when no replica has
    /// a role, and every one takes every write.
    async fn primary(&self, t: &Topology, i: usize) -> Result<Option<usize>, ApiError> {
        let find = || self.newest_primary(t, i);
        if let Some(k) = find() {
            return Ok(Some(k));
        }
        if t.replicas[i].iter().all(|r| r.role() == pb::Role::Standalone) {
            return Ok(None);
        }
        let asks = t.replicas[i].iter().enumerate().map(|(k, rep)| {
            let mut c = rep.client.clone();
            rep.protocol.store(0, Ordering::Relaxed);
            async move { self.speak(t, i, k, &mut c, PROTOCOL).await }
        });
        join_all(asks).await;
        find().map(Some).ok_or_else(|| ApiError { status: 503, message: format!("shard {i} has no primary answering: its replicas take writes from their primary alone") })
    }

    /// Of the replicas of shard `i`, the primary of the newest term that no
    /// other replaced, as each last said.
    fn newest_primary(&self, t: &Topology, i: usize) -> Option<usize> {
        let rs = &t.replicas[i];
        (0..rs.len()).filter(|&r| rs[r].takes_writes()).max_by_key(|&r| rs[r].term.load(Ordering::Relaxed))
    }

    /// After copy `k`, shard `i`'s primary, refused a write with `code` as
    /// one that is no longer its primary would (replaced, or down): the
    /// copy that takes the shard's writes now, if another does. A refusal
    /// (409) asks `k` again first, and the other copies only when it no
    /// longer takes writes, so that a conditional write's refusal costs
    /// one `Hello`.
    async fn moved(&self, t: &Topology, i: usize, k: usize, code: Code) -> Option<usize> {
        let ask = |r: usize| {
            let rep = &t.replicas[i][r];
            let mut c = rep.client.clone();
            rep.protocol.store(0, Ordering::Relaxed);
            async move { self.speak(t, i, r, &mut c, PROTOCOL).await.is_ok() }
        };
        if code == Code::FailedPrecondition && ask(k).await && t.replicas[i][k].takes_writes() {
            return None;
        }
        join_all((0..t.replicas[i].len()).map(ask)).await;
        self.newest_primary(t, i).filter(|&r| r != k)
    }

    /// Writes to shard `i`: to its primary, or to every replica while none
    /// has a role. Returns the first's count, and the transaction's
    /// sequence number on the shard: 0 where it numbers no writes, or
    /// where several replicas took the write, each numbering it its way.
    /// A primary that refuses the write as a replaced one does, or does not
    /// answer, is passed over for the copy that took its place, if any
    /// (once).
    async fn write(&self, t: &Topology, i: usize, r: pb::WriteRequest, need: u32) -> Result<Wrote, ApiError> {
        let to: Vec<usize> = match self.primary(t, i).await? {
            Some(k) => vec![k],
            None => (0..t.replicas[i].len()).collect(),
        };
        let named = |(k, s): (usize, tonic::Status)| self.named(&t.replicas[i][k].url, s);
        if let (&[k], true) = (&to[..], t.replicas[i].len() > 1) {
            return match self.write_to(t, i, vec![k], r.clone(), need).await {
                Err((_, s)) if matches!(s.code(), Code::FailedPrecondition | Code::Unavailable) => match self.moved(t, i, k, s.code()).await {
                    Some(m) => self.write_to(t, i, vec![m], r, need).await.map_err(named),
                    None => Err(named((k, s))),
                },
                a => a.map_err(named),
            };
        }
        self.write_to(t, i, to, r, need).await.map_err(named)
    }

    /// [`Self::write`] to replicas `to` of shard `i`, each of which must
    /// speak protocol `need` or newer; an error names the replica that
    /// answered it.
    async fn write_to(&self, t: &Topology, i: usize, to: Vec<usize>, r: pb::WriteRequest, need: u32) -> Result<Wrote, (usize, tonic::Status)> {
        let one = to.len() == 1;
        let calls = to.into_iter().map(|k| {
            let rep = &t.replicas[i][k];
            let (mut c, r) = (rep.client.clone(), r.clone());
            async move {
                // A replica not asked yet (it was down at connect), or one
                // that failed, is asked again and given the slot map first.
                let p = self.speak(t, i, k, &mut c, PROTOCOL).await.map_err(|s| (k, s))?;
                if p < need {
                    return Err((k, tonic::Status::unimplemented(format!("it speaks protocol {p}, and source offsets need {need}: upgrade it"))));
                }
                let a = c.write(r).await;
                if a.as_ref().is_err_and(try_elsewhere) {
                    rep.protocol.store(0, Ordering::Relaxed);
                }
                a.map_err(|s| (k, s))
            }
        });
        let mut written = None;
        for a in join_all(calls).await {
            let a = a?.into_inner();
            written.get_or_insert(Wrote { rows: a.written, seq_no: a.seq_no.filter(|_| one).unwrap_or(0), skipped: a.skipped });
        }
        Ok(written.unwrap_or_default())
    }

    /// One answer of the writes to shards `shards` ([`Self::write`]): the
    /// rows written and skipped, and each shard's number (none while every
    /// one is 0).
    fn written(&self, t: &Topology, shards: Vec<(usize, Wrote)>) -> Written {
        let mut seq_nos = vec![0; t.replicas.len()];
        let (mut rows, mut skipped) = (0, 0);
        for (i, w) in shards {
            rows += w.rows;
            skipped += w.skipped;
            seq_nos[i] = w.seq_no;
        }
        if seq_nos.iter().all(|&s| s == 0) {
            seq_nos.clear();
        }
        Written { rows, seq_nos, skipped, ..Default::default() }
    }

    /// An INSERT goes to the shards of its documents' keys (or their
    /// shard_by values); UPDATE and DELETE go to every shard, so they take
    /// no LIMIT here.
    pub async fn sql(&self, sql: &str, params: &Params) -> Result<Written, ApiError> {
        let t = &self.topo();
        let catalog = self.catalog();
        let tables: Vec<&Schema> = catalog.iter().map(|t| &**t).collect();
        let (schema, write) = match bind_command(sql, &tables, params)? {
            Command::Write { table, write } => (catalog.iter().find(|t| t.table == table).ok_or_else(|| ApiError::internal("a statement bound to a table the coordinator does not serve"))?, write),
            _ => return Err(ApiError::bad("not a write statement")),
        };
        match write {
            Write::Insert { docs, conflict } => {
                let op = match conflict {
                    Conflict::Fail => "insert",
                    Conflict::Replace => "upsert",
                    // The shards' write operations have no insert-if-absent
                    // or update-on-conflict.
                    Conflict::Skip | Conflict::Update(_) => {
                        return Err(ApiError::bad("INSERT ... ON CONFLICT DO NOTHING and DO UPDATE are not supported through a coordinator yet (INSERT OR REPLACE is)"))
                    }
                };
                // Rows of the first table name none, as before there were
                // tables.
                let named = (schema.table != self.schema.table).then(|| json!(schema.table));
                let ops = docs
                    .iter()
                    .map(|d| {
                        let mut op = json!({"op": op, "doc": js::document_json(schema, d)});
                        if let Some(t) = &named {
                            op["table"] = t.clone();
                        }
                        op
                    })
                    .collect();
                self.write_ops(ops, None).await
            }
            // Each shard would take its own LIMIT rows.
            Write::Update { select, .. } | Write::Delete { select } if select.limit.is_some() => {
                Err(ApiError::bad("UPDATE ... LIMIT and DELETE ... LIMIT are not supported through a coordinator yet"))
            }
            _ => {
                let r = pb::WriteRequest { sql: sql.into(), params: params_to_pb(params), epoch: t.epoch(), ..Default::default() };
                let calls = (0..t.replicas.len()).map(|i| {
                    let r = r.clone();
                    async move { self.write(t, i, r, MIN_PROTOCOL).await.map(|w| (i, w)) }
                });
                Ok(self.written(t, join_all(calls).await.into_iter().collect::<Result<Vec<_>, _>>()?))
            }
        }
    }

    /// Runs `CREATE TABLE`, `DROP TABLE`, `CREATE RANK PROFILE` or `DROP
    /// RANK PROFILE` on every shard (at its primary, or every replica while
    /// none has a role), then serves the tables and rank profiles they all
    /// have. It is one change per shard, not one across them: a shard that
    /// fails leaves the others changed, and the statement runs again with
    /// `IF NOT EXISTS`, `IF EXISTS` or `OR REPLACE`.
    pub async fn ddl(&self, sql: &str, params: &Params) -> Result<Written, ApiError> {
        let t = &self.topo();
        let r = pb::WriteRequest { sql: sql.into(), params: params_to_pb(params), epoch: t.epoch(), ..Default::default() };
        let calls = (0..t.replicas.len()).map(|i| {
            let r = r.clone();
            async move { self.write(t, i, r, MIN_PROTOCOL).await }
        });
        let answers = join_all(calls).await;
        self.refresh(true).await;
        for a in answers {
            a?;
        }
        Ok(Written::default())
    }

    /// Write operations, grouped by the shard of their key. An idempotency
    /// key goes to each shard as `key/shard`, so a retry after a replica
    /// failed is not applied twice by the replicas that took it.
    pub async fn write_ops(&self, ops: Vec<Json>, key: Option<String>) -> Result<Written, ApiError> {
        let t = &self.topo();
        let mut catalog = self.catalog();
        // A table created since the replicas were last asked.
        if ops.iter().any(|op| op_schema(&catalog, op).is_err()) && self.refresh(false).await {
            catalog = self.catalog();
        }
        let mut by_shard: BTreeMap<usize, Vec<Json>> = BTreeMap::new();
        // Each source's highest offset, which every shard keeps.
        let mut marks: BTreeMap<String, i64> = BTreeMap::new();
        for op in ops {
            if let Some(m) = op_source(&op)? {
                let top = marks.entry(m.name).or_insert(m.offset);
                *top = (*top).max(m.offset);
            }
            if is_mark(&op) {
                continue;
            }
            let s = self.shard_of(t, &op_place(op_schema(&catalog, &op)?, &op)?);
            if op.get("if_seq_no").is_some() && !self.numbered_alike(t, s) {
                return Err(ApiError::bad(ONE_REPLICA));
            }
            by_shard.entry(s).or_default().push(op);
        }
        // A shard the request gives no rows still keeps the offsets, so
        // that the least over the shards is where a stream resumes
        // ([`Self::sources`]): each shard skips what it applied already.
        let need = if marks.is_empty() { MIN_PROTOCOL } else { SOURCES_PROTOCOL };
        if !marks.is_empty() {
            for i in 0..t.replicas.len() {
                let ops = by_shard.entry(i).or_default();
                ops.extend(marks.iter().map(|(name, offset)| json!({"op": "source", "source": {"name": name, "offset": offset}})));
            }
        }
        let calls = by_shard.into_iter().map(|(i, ops)| {
            let idempotency_key = key.as_ref().map(|k| format!("{k}/{i}")).unwrap_or_default();
            let r = pb::WriteRequest { ops_json: serde_json::to_vec(&ops).unwrap_or_default(), idempotency_key, epoch: t.epoch(), ..Default::default() };
            async move { self.write(t, i, r, need).await.map(|w| (i, w)) }
        });
        Ok(self.written(t, join_all(calls).await.into_iter().collect::<Result<Vec<_>, _>>()?))
    }

    /// The stream sources written from through the shards, each at the
    /// least offset over them, which is where a stream resumes: a shard
    /// skips the events it applied already. A source some shard does not
    /// have is left out (its first batch did not reach that shard), so a
    /// stream of it starts over. Any replica may answer: one behind its
    /// primary has an older offset, which only means more to skip.
    pub async fn sources(&self) -> Result<Vec<Source>, ApiError> {
        let t = &self.topo();
        let deadline = Instant::now() + Duration::from_secs(30);
        let calls = (0..t.replicas.len()).map(|i| {
            self.call(t, i, 0, None, deadline, move |r, mut c| async move {
                let p = self.speak(t, i, r, &mut c, PROTOCOL).await?;
                if p < SOURCES_PROTOCOL {
                    return Err(tonic::Status::unimplemented(format!("it speaks protocol {p}, and source offsets need {SOURCES_PROTOCOL}: upgrade it")));
                }
                c.sources(pb::SourcesRequest {}).await
            })
        });
        let mut least: BTreeMap<String, (i64, i64, usize)> = BTreeMap::new();
        let n = t.replicas.len();
        for a in join_all(calls).await {
            let (_, a) = a?;
            for s in a.sources {
                let e = least.entry(s.name).or_insert((s.offset, s.updated_micros, 0));
                if s.offset < e.0 {
                    (e.0, e.1) = (s.offset, s.updated_micros);
                }
                e.2 += 1;
            }
        }
        Ok(least.into_iter().filter(|(_, (.., k))| *k == n).map(|(name, (offset, updated, _))| Source { name, offset, updated: (updated != 0).then_some(updated) }).collect())
    }

    /// "ok" when every replica answers, "degraded" when every shard still
    /// has one, "down" when a shard has none. Documents and segments are
    /// counted once per shard.
    /// Every replica of every shard: its role, health, latest point of
    /// rates and gauges (each server's own, from `Health`'s detail), and
    /// how long it took to answer; and the slots each shard holds.
    pub async fn cluster(&self) -> Json {
        let t = &self.topo();
        let calls = t.replicas.iter().map(|rs| {
            join_all(rs.iter().map(|r| {
                let mut c = r.client.clone();
                async move {
                    let t0 = Instant::now();
                    let a = tokio::time::timeout(Duration::from_secs(2), c.health(pb::Empty {})).await;
                    (a, t0.elapsed().as_secs_f64() * 1e3)
                }
            }))
        });
        let now = self.now_ms();
        let mut shards = Vec::new();
        for (i, answers) in join_all(calls).await.into_iter().enumerate() {
            let replicas: Vec<Json> = t.replicas[i]
                .iter()
                .zip(answers)
                .map(|(r, (a, ms))| {
                    let mut j = match a {
                        Ok(Ok(h)) => {
                            let h = h.into_inner();
                            let detail: Json = serde_json::from_str(&h.detail).unwrap_or(Json::Null);
                            json!({"url": r.url, "status": h.status, "docs": h.docs, "segments": h.segments, "txn": h.txn, "rtt_ms": ms, "detail": detail})
                        }
                        Ok(Err(e)) => json!({"url": r.url, "status": "down", "error": e.message(), "rtt_ms": ms}),
                        Err(_) => json!({"url": r.url, "status": "down", "error": "no answer within 2 s", "rtt_ms": ms}),
                    };
                    j["role"] = json!(r.role().as_str_name().to_lowercase());
                    j["takes_writes"] = json!(r.takes_writes());
                    j["protocol"] = json!(r.protocol.load(Ordering::Relaxed));
                    if r.avoid_until.load(Ordering::Relaxed) > now {
                        j["avoided"] = json!(true);
                    }
                    j
                })
                .collect();
            let calls: Vec<u64> = t.outcomes[i].iter().map(|n| n.load(Ordering::Relaxed)).collect();
            let slots = t.slots.as_ref().map(|m| m.ranges(i));
            let slots_held = t.slots.as_ref().map_or(crate::slots::SLOTS / t.replicas.len().max(1), |m| m.count(i));
            let up = replicas.iter().filter(|r| r["status"] != "down").count();
            let status = if up == replicas.len() { "ok" } else if up > 0 { "degraded" } else { "down" };
            shards.push(json!({"shard": i, "url": t.shards[i], "status": status, "replicas": replicas, "slots": slots, "slots_held": slots_held, "calls": {"ok": calls[0], "error": calls[1], "timeout": calls[2]}}));
        }
        let up = shards.iter().filter(|s| s["status"] == "ok").count();
        let status = if up == shards.len() { "ok" } else if shards.iter().all(|s| s["status"] != "down") { "degraded" } else { "down" };
        json!({
            "role": "coordinator", "status": status, "shards": shards, "slot_epoch": t.epoch(), "slot_count": crate::slots::SLOTS,
            "protocol": t.protocol(), "retries": self.retries.load(Ordering::Relaxed), "hedges": self.hedges.load(Ordering::Relaxed),
            "hedge_ms": self.opts.hedge.as_millis() as u64,
            "changes": self.can_change(t),
        })
    }

    /// Whether the cluster map may change now, and why not.
    fn can_change(&self, t: &Topology) -> Json {
        let old: Vec<&str> = t.replicas.iter().flatten().filter(|r| (1..CLUSTER_PROTOCOL).contains(&r.protocol.load(Ordering::Relaxed))).map(|r| r.url.as_str()).collect();
        match (t.slots.is_some(), old.is_empty()) {
            (false, _) => json!({"ok": false, "why": "the shards route by the hash before slots (protocol 3 or older)"}),
            (true, false) => json!({"ok": false, "why": format!("servers speaking a protocol before {CLUSTER_PROTOCOL}: {}", old.join(", "))}),
            (true, true) => json!({"ok": true}),
        }
    }

    pub async fn health(&self) -> Json {
        let t = &self.topo();
        let calls = t.replicas.iter().map(|rs| {
            join_all(rs.iter().map(|r| {
                let mut c = r.client.clone();
                async move { tokio::time::timeout(Duration::from_secs(2), c.health(pb::Empty {})).await }
            }))
        });
        let now = self.now_ms();
        let mut shards = Vec::new();
        let (mut docs, mut segments, mut all_up, mut every_shard) = (0, 0, true, true);
        for (i, answers) in join_all(calls).await.into_iter().enumerate() {
            let mut replicas = Vec::new();
            // Counted from a replica that is "ok", else from any answering:
            // one that cannot write ("failing") still answers queries.
            let (mut counted, mut answering) = (None, None);
            for (r, a) in t.replicas[i].iter().zip(answers) {
                let mut j = match a {
                    Ok(Ok(h)) => {
                        let h = h.into_inner();
                        if h.status == "ok" {
                            counted.get_or_insert((h.docs, h.segments));
                        }
                        answering.get_or_insert((h.docs, h.segments));
                        json!({"url": r.url, "status": h.status, "docs": h.docs, "segments": h.segments, "txn": h.txn})
                    }
                    Ok(Err(e)) => json!({"url": r.url, "status": "down", "error": e.message()}),
                    Err(_) => json!({"url": r.url, "status": "down", "error": "no answer within 2 s"}),
                };
                all_up &= j["status"] == "ok";
                if r.avoid_until.load(Ordering::Relaxed) > now {
                    j["avoided"] = json!(true);
                }
                if r.role() != pb::Role::Standalone {
                    j["role"] = json!(r.role().as_str_name().to_lowercase());
                }
                replicas.push(j);
            }
            let counted = counted.or(answering);
            every_shard &= counted.is_some();
            let (d, s) = counted.unwrap_or((0, 0));
            docs += d;
            segments += s;
            let status = if counted.is_none() { "down" } else { "ok" };
            let mut shard = json!({"url": t.shards[i], "status": status, "docs": d, "segments": s});
            if t.replicas[i].len() > 1 {
                shard["replicas"] = json!(replicas);
            } else if let Some(err) = replicas[0].get("error") {
                shard["error"] = err.clone();
            }
            if let Some(txn) = replicas[0].get("txn") {
                shard["txn"] = txn.clone();
            }
            shards.push(shard);
        }
        let status = if all_up { "ok" } else if every_shard { "degraded" } else { "down" };
        json!({
            "status": status, "role": "coordinator", "docs": docs, "segments": segments, "txn": 0, "shards": shards,
            "retries": self.retries.load(Ordering::Relaxed), "hedges": self.hedges.load(Ordering::Relaxed), "protocol": t.protocol(), "slot_epoch": t.epoch(),
        })
    }
}

/// How often the watcher asks the servers which cluster map they hold.
const WATCH_EVERY: Duration = Duration::from_secs(2);

/// A rank profile as the statement that makes it again.
fn profile_sql(table: &str, p: &RankProfile) -> String {
    use strata::query::sql::ident;
    let mut s = format!("CREATE OR REPLACE RANK PROFILE {} ON {} AS {}", ident(&p.name), ident(table), p.ranking);
    if !p.defaults.is_empty() {
        let args: Vec<String> = p
            .defaults
            .iter()
            .map(|(k, v)| {
                let v = match v {
                    Json::String(t) => format!("'{}'", t.replace('\'', "''")),
                    Json::Bool(b) => if *b { "TRUE".into() } else { "FALSE".into() },
                    v => v.to_string(),
                };
                format!("{} => {v}", ident(k))
            })
            .collect();
        s.push_str(&format!(" WITH ({})", args.join(", ")));
    }
    s
}

/// The cluster map: following other coordinators' changes, and changing it.
impl Coordinator {
    /// Routes by `map` from now on: its servers, keeping what is known of
    /// those it served already.
    pub(crate) fn install(&self, map: SlotMap) -> Result<(), ApiError> {
        let now = self.topo();
        if now.slots.as_ref().is_some_and(|m| m.epoch >= map.epoch) {
            return Ok(());
        }
        let groups = match map.replicas().is_empty() {
            true if map.shards() == now.replicas.len() => now.groups(),
            true => return Err(ApiError::internal(format!("cluster map epoch {} names no servers, and has {} shards, not {}", map.epoch, map.shards(), now.replicas.len()))),
            false => map.replicas().to_vec(),
        };
        let next = Topology::new(groups, Some(map), Some(&now), &self.opts)?;
        let mut held = self.topo.write().unwrap_or_else(|e| e.into_inner());
        if held.epoch() < next.epoch() {
            tracing::info!(epoch = next.epoch(), shards = next.replicas.len(), servers = next.replicas.iter().map(Vec::len).sum::<usize>(), "routing by a new cluster map");
            *held = Arc::new(next);
        }
        Ok(())
    }

    /// Asks every server which cluster map it holds (`Hello`), and takes
    /// up the newest when it is newer than the one routed by. Returns
    /// whether it did.
    pub async fn sync(&self) -> bool {
        let t = self.topo();
        let asks = t.replicas.iter().flatten().map(|r| {
            let mut c = r.client.clone();
            async move {
                let said = tokio::time::timeout(Duration::from_secs(2), hello(&mut c)).await.ok()?.ok()?;
                r.heard(&said);
                r.hello_at.store(self.now_ms(), Ordering::Relaxed);
                Some((said.epoch, said.protocol, c))
            }
        });
        let newest = join_all(asks).await.into_iter().flatten().filter(|(e, p, _)| *e > t.epoch() && *p >= CLUSTER_PROTOCOL).max_by_key(|(e, ..)| *e);
        let Some((_, _, mut c)) = newest else { return false };
        match tokio::time::timeout(Duration::from_secs(5), held_map(&mut c)).await {
            Ok(Ok(Some((m, _)))) => match self.install(m) {
                Ok(()) => self.topo().epoch() > t.epoch(),
                Err(e) => {
                    tracing::warn!(error = %e.message, "could not take up a newer cluster map");
                    false
                }
            },
            _ => false,
        }
    }

    /// Follows the cluster map the servers hold, every [`WATCH_EVERY`] and
    /// whenever a server says it holds a newer one, until the coordinator
    /// is dropped.
    pub fn watch(self: &Arc<Self>) {
        let me = Arc::downgrade(self);
        let wake = self.wake.clone();
        tokio::spawn(async move {
            loop {
                let Some(c) = me.upgrade() else { return };
                c.sync().await;
                drop(c);
                let _ = tokio::time::timeout(WATCH_EVERY, wake.notified()).await;
            }
        });
    }

    /// Changes the cluster map: `f` makes the next map from the newest,
    /// which every server of the next map is given, and the coordinator
    /// routes by it. One change at a time here; a change another
    /// coordinator made meanwhile (a server holds another map of that
    /// epoch) is taken up and `f` runs again on it, three times at most.
    /// A server that does not answer is given the map when it answers
    /// again. Returns the new map and the servers that did not take it.
    pub async fn change(&self, f: impl Fn(&SlotMap) -> Result<SlotMap, String>) -> Result<(SlotMap, Vec<String>), ApiError> {
        let _one = self.changing.lock().await;
        for _ in 0..3 {
            self.sync().await;
            let t = self.topo();
            let now = t.slots.clone().ok_or_else(|| ApiError::bad("a coordinator routing by the hash before slots (a shard from before protocol 4) cannot change the cluster"))?;
            if t.replicas.iter().flatten().any(|r| (1..CLUSTER_PROTOCOL).contains(&r.protocol.load(Ordering::Relaxed))) {
                return Err(ApiError::bad(format!("changing the cluster needs every server to speak protocol {CLUSTER_PROTOCOL}: upgrade them first")));
            }
            let now = match now.replicas().is_empty() {
                true => now.with_replicas(t.groups()).map_err(ApiError::internal)?,
                false => now,
            };
            let next = f(&now).map_err(ApiError::bad)?;
            if next.epoch <= now.epoch {
                return Err(ApiError::internal("a change must raise the map's epoch"));
            }
            let staged = Topology::new(next.replicas().to_vec(), Some(next.clone()), Some(&t), &self.opts)?;
            let gives = staged.replicas.iter().enumerate().flat_map(|(i, rs)| rs.iter().map(move |r| (i, r)));
            let answers = join_all(gives.map(|(i, r)| {
                let mut c = r.client.clone();
                let next = &next;
                async move { (r, tokio::time::timeout(Duration::from_secs(60), give_slots(&mut c, next, i)).await) }
            }))
            .await;
            let (mut missed, mut raced) = (Vec::new(), None);
            for (r, a) in answers {
                match a {
                    Ok(Ok(_)) => {}
                    Ok(Err(e)) if e.code() == Code::FailedPrecondition => {
                        // Another coordinator gave a map of this epoch or
                        // newer, or the server refuses this one.
                        let mut c = r.client.clone();
                        match held_map(&mut c).await {
                            Ok(Some((m, _))) if m.epoch >= next.epoch && m != next => raced = Some(format!("{}: {}", r.url, e.message())),
                            _ => missed.push(format!("{}: {}", r.url, e.message())),
                        }
                    }
                    Ok(Err(e)) => missed.push(format!("{}: {}", r.url, e.message())),
                    Err(_) => missed.push(format!("{}: no answer within 60 s", r.url)),
                }
            }
            if let Some(why) = raced {
                tracing::warn!(epoch = next.epoch, refused = %why, "another change of the cluster map came first: trying again on it");
                continue;
            }
            self.install(next.clone())?;
            if !missed.is_empty() {
                tracing::warn!(epoch = next.epoch, missed = %missed.join("; "), "servers that did not take the new cluster map get it when they answer again");
            }
            return Ok((next, missed));
        }
        Err(ApiError { status: 409, message: "other changes of the cluster map kept coming first: try again".into() })
    }

    /// What a server new to the cluster says: its protocol, part in
    /// replication, rows, and first table. It must speak protocol 9, hold
    /// no rows (unless `force`) and no slot map, and serve the cluster's
    /// first table.
    async fn admit(&self, url: &str, force: bool) -> Result<(Client, Said), ApiError> {
        let t = self.topo();
        if let Some(i) = t.replicas.iter().position(|rs| rs.iter().any(|r| r.url == url)) {
            return Err(ApiError::bad(format!("{url} serves shard {i} already")));
        }
        let mut c = client(url, self.opts.token.as_ref(), self.opts.ca.as_deref())?;
        let named = |s: tonic::Status| ApiError { status: ApiError::from(s.clone()).status, message: format!("{url}: {}", s.message()) };
        let ask = async {
            let said = hello(&mut c).await.map_err(named)?;
            let schema = c.schema(pb::Empty {}).await.map_err(named)?.into_inner();
            let health = c.health(pb::Empty {}).await.map_err(named)?.into_inner();
            let map = match said.protocol >= CLUSTER_PROTOCOL {
                true => held_map(&mut c).await.map_err(named)?,
                false => None,
            };
            Ok::<_, ApiError>((said, schema, health, map))
        };
        let (said, schema, health, map) = tokio::time::timeout(Duration::from_secs(10), ask).await.map_err(|_| ApiError { status: 504, message: format!("{url}: no answer within 10 s") })??;
        if said.protocol < CLUSTER_PROTOCOL {
            return Err(ApiError::bad(format!("{url} speaks protocol {}, and joining a cluster needs {CLUSTER_PROTOCOL}: upgrade it", said.protocol)));
        }
        if let Some((m, i)) = map {
            return Err(ApiError::bad(format!("{url} holds a cluster map already (epoch {}, as shard {i}): it belongs to a cluster; start it on a new database", m.epoch)));
        }
        let first: Schema = serde_json::from_str(&schema.schema_json).map_err(ApiError::internal)?;
        if first != self.schema {
            return Err(ApiError::bad(format!("{url} serves table {} with another schema than the cluster's first table, {}: create its database with the same --schema", first.table, self.schema.table)));
        }
        if health.docs > 0 && !force {
            return Err(ApiError::bad(format!("{url} holds {} rows: a new shard or replica starts empty (pass force to drop them)", health.docs)));
        }
        Ok((c, said))
    }

    /// Creates every table the cluster serves on a new server, `c`, which
    /// serves the first table already, and with `profiles` its rank
    /// profiles. A server that is to follow a primary is given no
    /// profile: a profile is a numbered write, which would make its
    /// history differ from its primary's, which ships its profiles.
    async fn copy_catalog(&self, url: &str, c: &mut Client, profiles: bool) -> Result<(), ApiError> {
        let mut statements: Vec<String> = self.catalog().iter().skip(1).map(|t| strata::query::ddl::create_sql(t).replacen("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)).collect();
        if profiles {
            statements.extend(self.rank_profiles().iter().map(|(t, p)| profile_sql(t, p)));
        }
        for sql in statements {
            let r = pb::WriteRequest { sql: sql.clone(), ..Default::default() };
            c.write(r).await.map_err(|s| ApiError { status: ApiError::from(s.clone()).status, message: format!("{url}: {sql}: {}", s.message()) })?;
        }
        Ok(())
    }

    /// Adds a shard served by `servers` (gRPC URLs; the first is its
    /// primary and ships to the others), holding no slot until slots move
    /// to it. Each must be new: protocol 9, no rows, no cluster map, the
    /// cluster's first table. It is given the cluster's other tables and
    /// rank profiles first.
    pub async fn add_shard(&self, servers: Vec<String>, acks: Acks) -> Result<Json, ApiError> {
        if servers.is_empty() {
            return Err(ApiError::bad("a shard needs at least one server"));
        }
        let mut clients = Vec::new();
        for url in &servers {
            clients.push(self.admit(url, false).await?.0);
        }
        // Tables before replication: a primary ships rows and rank
        // profiles, not tables.
        for (k, (url, c)) in servers.iter().zip(clients.iter_mut()).enumerate() {
            self.copy_catalog(url, c, k == 0).await?;
        }
        if servers.len() > 1 {
            for (url, c) in servers.iter().zip(clients.iter_mut()).skip(1) {
                configure(c, url, pb::Role::Replica, Vec::new(), None).await?;
            }
            configure(&mut clients[0], &servers[0], pb::Role::Primary, servers[1..].to_vec(), Some(acks)).await?;
        }
        let (map, missed) = self.change(|m| m.add_shard(servers.clone())).await?;
        self.refresh(true).await;
        Ok(json!({"epoch": map.epoch, "shard": map.shards() - 1, "servers": servers, "missed": missed}))
    }

    /// Removes shard `i`, which must hold no slot (drain it first): the
    /// shards after it move down one place. Its servers keep their rows.
    pub async fn remove_shard(&self, i: usize) -> Result<Json, ApiError> {
        let t = self.topo();
        let servers = t.replicas.get(i).map(|rs| rs.iter().map(|r| r.url.clone()).collect::<Vec<_>>()).ok_or_else(|| ApiError::bad(format!("there is no shard {i} of {}", t.replicas.len())))?;
        if t.replicas.len() == 1 {
            return Err(ApiError::bad("a cluster keeps one shard at least"));
        }
        let (map, missed) = self.change(|m| m.remove_shard(i)).await?;
        Ok(json!({"epoch": map.epoch, "removed": servers, "missed": missed}))
    }

    /// Adds `url` to shard `i`'s servers, as a replica its primary ships
    /// to (a shard served by one standalone server makes it its primary
    /// first). The new server is sent a copy of the primary, and serves
    /// reads once it has caught up.
    pub async fn add_replica(&self, i: usize, url: &str, force: bool) -> Result<Json, ApiError> {
        let t = self.topo();
        let rs = t.replicas.get(i).ok_or_else(|| ApiError::bad(format!("there is no shard {i} of {}", t.replicas.len())))?.clone();
        let (mut c, _) = self.admit(url, force).await?;
        // The shard's primary: asked again, as a promotion may have moved it.
        let k = self.primary(&t, i).await?;
        let k = match (k, rs.len()) {
            (Some(k), _) => k,
            (None, 1) => 0,
            (None, _) => return Err(ApiError::bad(format!("shard {i}'s servers do not replicate (each takes every write): a replica needs a primary to follow; serve the shard from one server first"))),
        };
        let primary = rs[k].clone();
        self.copy_catalog(url, &mut c, false).await?;
        configure(&mut c, url, pb::Role::Replica, Vec::new(), None).await?;
        let mut pc = primary.client.clone();
        let mut replicas: Vec<String> = rs.iter().filter(|r| r.url != primary.url).map(|r| r.url.clone()).collect();
        replicas.push(url.to_string());
        let said = configure(&mut pc, &primary.url, pb::Role::Primary, replicas, None).await?;
        primary.protocol.store(0, Ordering::Relaxed);
        let (map, missed) = self.change(|m| {
            let mut servers = m.replicas()[i].clone();
            servers.push(url.to_string());
            m.set_servers(i, servers)
        })
        .await?;
        Ok(json!({"epoch": map.epoch, "shard": i, "url": url, "primary": primary.url, "replicas": said.replicas, "write_acks": said.write_acks, "missed": missed}))
    }

    /// Removes `url` from shard `i`'s servers: the coordinators stop
    /// reading from it, then its primary stops shipping to it. A shard's
    /// primary is not removed while other servers serve the shard
    /// (promote another first), nor a shard's last server.
    pub async fn remove_replica(&self, i: usize, url: &str) -> Result<Json, ApiError> {
        let t = self.topo();
        let rs = t.replicas.get(i).ok_or_else(|| ApiError::bad(format!("there is no shard {i} of {}", t.replicas.len())))?.clone();
        let Some(gone) = rs.iter().position(|r| r.url == url) else {
            return Err(ApiError::bad(format!("{url} does not serve shard {i}")));
        };
        if rs.len() == 1 {
            return Err(ApiError::bad(format!("{url} is shard {i}'s last server: remove the shard instead")));
        }
        let k = self.primary(&t, i).await?;
        if k == Some(gone) {
            return Err(ApiError::bad(format!("{url} is shard {i}'s primary: promote another of its servers first")));
        }
        let (map, missed) = self.change(|m| m.set_servers(i, m.replicas()[i].iter().filter(|u| *u != url).cloned().collect())).await?;
        let mut shipping = Json::Null;
        if let Some(k) = k {
            let p = &rs[k];
            let mut pc = p.client.clone();
            let replicas = rs.iter().filter(|r| r.url != url && r.url != p.url).map(|r| r.url.clone()).collect();
            let said = configure(&mut pc, &p.url, pb::Role::Primary, replicas, None).await?;
            shipping = json!({"primary": p.url, "replicas": said.replicas});
        }
        Ok(json!({"epoch": map.epoch, "shard": i, "removed": url, "shipping": shipping, "missed": missed}))
    }
}

/// Sets the part in replication of the server at `url` (`Configure`).
async fn configure(c: &mut Client, url: &str, role: pb::Role, replicas: Vec<String>, acks: Option<Acks>) -> Result<pb::ConfigureResponse, ApiError> {
    let r = pb::ConfigureRequest { role: role as i32, replicas, write_acks: acks.map(|a| a.name().to_string()).unwrap_or_default(), force: false };
    match tokio::time::timeout(Duration::from_secs(30), c.configure(r)).await {
        Ok(Ok(a)) => Ok(a.into_inner()),
        Ok(Err(s)) => Err(ApiError { status: ApiError::from(s.clone()).status, message: format!("{url}: {}", s.message()) }),
        Err(_) => Err(ApiError { status: 504, message: format!("{url}: no answer within 30 s") }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hedges_stay_within_their_share() {
        let b = HedgeBudget::new();
        let mut hedged = 0;
        for _ in 0..1000 {
            b.earn();
            hedged += b.spend() as usize;
        }
        // The saved burst, then one call in twenty.
        assert!((1000 / 20..=HEDGE_BURST as usize + 1000 / 20).contains(&hedged), "{hedged}");
        assert!(!b.spend());
    }

    #[test]
    fn an_overloaded_replica_is_not_retried_elsewhere() {
        assert!(!try_elsewhere(&tonic::Status::resource_exhausted("too many queries running")));
        assert!(try_elsewhere(&tonic::Status::unavailable("connection refused")));
        assert!(!try_elsewhere(&tonic::Status::invalid_argument("bad SQL")));
        // The server answers too many queries with 429, which crosses gRPC
        // as ResourceExhausted and back.
        let s = tonic::Status::from(ApiError { status: 429, message: "too many queries running".into() });
        assert_eq!(s.code(), Code::ResourceExhausted);
        assert_eq!(ApiError::from(s).status, 429);
    }
}
