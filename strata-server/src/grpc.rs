//! The service over gRPC (`proto/strata.proto`).

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use strata::query::exec::ResultSet;
use strata::query::ir::{self, Plan};
use strata::query::Params;
use strata::value::Value;
use futures::StreamExt;
use tonic::{Request, Response, Status};
use tracing::Instrument;

use crate::pb::{self, value::Kind};
use crate::service::{After, QueryReq, Service, Stream};
use crate::keys::{Principal, Scope};
use crate::{obs, plan_pb, ApiError};

pub fn to_pb(v: &Value) -> pb::Value {
    let kind = match v {
        Value::Null => Kind::Null(true),
        Value::Int(x) => Kind::Int(*x),
        Value::Float(x) => Kind::Float(*x),
        Value::Str(s) => Kind::Str(s.clone()),
        Value::Vector(v) => Kind::Vector(pb::Floats { v: v.to_vec() }),
        Value::List(xs) => Kind::List(pb::Values { v: xs.iter().map(to_pb).collect() }),
        Value::Bool(b) => Kind::Boolean(*b),
        Value::Timestamp(t) => Kind::Timestamp(*t),
    };
    pb::Value { kind: Some(kind) }
}

pub fn from_pb(v: pb::Value) -> Value {
    match v.kind {
        None | Some(Kind::Null(_)) => Value::Null,
        Some(Kind::Int(x)) => Value::Int(x),
        Some(Kind::Float(x)) => Value::Float(x),
        Some(Kind::Str(s)) => Value::Str(s),
        Some(Kind::Vector(f)) => Value::Vector(f.v.into()),
        Some(Kind::List(xs)) => Value::List(xs.v.into_iter().map(from_pb).collect()),
        Some(Kind::Boolean(b)) => Value::Bool(b),
        Some(Kind::Timestamp(t)) => Value::Timestamp(t),
    }
}

pub fn params_to_pb(p: &Params) -> HashMap<String, pb::Value> {
    p.0.iter().map(|(k, v)| (k.clone(), to_pb(v))).collect()
}

pub fn params_from_pb(p: HashMap<String, pb::Value>) -> Params {
    Params(p.into_iter().map(|(k, v)| (k, from_pb(v))).collect())
}

fn json_bytes<T: serde::Serialize>(x: &T) -> Vec<u8> {
    serde_json::to_vec(x).unwrap_or_default()
}

fn from_json<T: serde::de::DeserializeOwned + Default>(b: &[u8], what: &str) -> Result<T, ApiError> {
    if b.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(b).map_err(|e| ApiError::bad(format!("bad {what}: {e}")))
}

pub fn result_to_pb(rs: &ResultSet, took_ms: f64) -> pb::QueryResponse {
    pb::QueryResponse {
        columns: rs.columns.clone(),
        rows: rs.rows.iter().map(|r| pb::Row { v: r.iter().map(to_pb).collect() }).collect(),
        keys: rs.keys.iter().map(to_pb).collect(),
        scores: rs.scores.clone(),
        parts_json: rs.parts.as_ref().map(json_bytes).unwrap_or_default(),
        stats_json: json_bytes(&rs.stats),
        took_ms,
        snapshot: 0,
    }
}

pub fn result_from_pb(r: pb::QueryResponse) -> Result<ResultSet, ApiError> {
    Ok(ResultSet {
        columns: r.columns,
        rows: r.rows.into_iter().map(|r| r.v.into_iter().map(from_pb).collect()).collect(),
        keys: r.keys.into_iter().map(from_pb).collect(),
        scores: r.scores,
        parts: if r.parts_json.is_empty() { None } else { Some(from_json(&r.parts_json, "fusion parts")?) },
        stats: from_json(&r.stats_json, "statistics")?,
    })
}

/// The protocol versions this server speaks (`proto/strata.proto`): 1
/// takes SQL or a JSON plan, 2 a typed plan, 3 holds a query's snapshot
/// for the fetch that follows, 4 places keys in slots, 5 reads the writes
/// a read must see (`min_seq_no`), which a replica waits for, 6 keeps
/// stream sources' offsets ([`SOURCES_PROTOCOL`]), 7 answers the next
/// codes of semantic-ID prefixes ([`SIDNEXT_PROTOCOL`]), 8 counts the
/// rows of a guard's levels ([`GUARD_PROTOCOL`]), and 9 keeps the cluster
/// map, every shard's servers, and takes `Configure` ([`CLUSTER_PROTOCOL`]).
pub const MIN_PROTOCOL: u32 = 1;
pub const PROTOCOL: u32 = 9;

/// The protocol whose writes carry stream sources' offsets
/// ([`crate::service::op_source`]), and that answers `Sources`.
pub const SOURCES_PROTOCOL: u32 = 6;

/// The protocol of `SidNext`, the next codes of semantic-ID prefixes.
pub const SIDNEXT_PROTOCOL: u32 = 7;

/// The protocol of `GuardCounts`, the rows of a guard's levels.
pub const GUARD_PROTOCOL: u32 = 8;

/// The protocol of the cluster map ([`crate::slots::SlotMap::replicas`]):
/// `Slots` asked without a map answers the shard's, and `Configure` sets a
/// server's part in replication.
pub const CLUSTER_PROTOCOL: u32 = 9;

/// A write's answer ([`crate::service::Written`]).
pub fn write_response(w: crate::service::Written) -> pb::WriteResponse {
    pb::WriteResponse { written: w.rows, seq_no: w.seq_no, seq_nos: w.seq_nos, skipped: w.skipped, ..Default::default() }
}

/// What [`pb::strata_server::Strata::hello`] answers, but what is the
/// server's own: its slot map's epoch, role, term and last number.
pub fn hello() -> pb::HelloResponse {
    pb::HelloResponse {
        min_protocol: MIN_PROTOCOL,
        max_protocol: PROTOCOL,
        plan_version: ir::VERSION,
        plan_features: ir::FEATURES.iter().map(|f| f.to_string()).collect(),
        server: format!("strata-server {}", env!("CARGO_PKG_VERSION")),
        ..Default::default()
    }
}

pub fn query_req(r: pb::QueryRequest) -> Result<QueryReq, ApiError> {
    let plan = request_plan(r.protocol, r.plan, &r.plan_json)?;
    Ok(QueryReq {
        sql: r.sql,
        params: params_from_pb(r.params),
        timeout_ms: r.timeout_ms as u64,
        shard: r.shard,
        stats: if r.stats_json.is_empty() { None } else { Some(from_json(&r.stats_json, "statistics")?) },
        snapshot: r.snapshot,
        partial: None,
        plan: plan.map(Arc::new),
        hold: r.hold,
        release: r.release,
        after: After::of(r.min_seq_no, r.min_seq_nos)?,
        // A shard measures CPU time when set to for every query.
        profile: false,
        quiet: false,
    })
}

/// A request's plan, if it has one: typed (`plan`, protocol 2) or JSON
/// (`plan_json`, protocol 1). A protocol newer than this server's is
/// refused, as the caller should have asked [`hello`] first.
pub fn request_plan(protocol: u32, plan: Option<pb::Plan>, json: &[u8]) -> Result<Option<Plan>, ApiError> {
    if protocol > PROTOCOL {
        return Err(ApiError::bad(format!("protocol {protocol} is newer than this server's ({MIN_PROTOCOL} to {PROTOCOL})")));
    }
    match plan {
        Some(p) => Ok(Some(plan_pb::from_pb_plan(p)?)),
        None => plan_of(json),
    }
}

/// A request's plan (`plan_json`), if it has one.
pub fn plan_of(json: &[u8]) -> Result<Option<Plan>, ApiError> {
    if json.is_empty() {
        return Ok(None);
    }
    let j: serde_json::Value = from_json(json, "plan")?;
    Ok(Some(Plan::from_json(&j)?))
}

pub struct Grpc(pub Arc<Service>);

/// A status code's name, as the metrics label it.
pub fn code_name(c: tonic::Code) -> &'static str {
    use tonic::Code::*;
    match c {
        Ok => "Ok",
        Cancelled => "Cancelled",
        Unknown => "Unknown",
        InvalidArgument => "InvalidArgument",
        DeadlineExceeded => "DeadlineExceeded",
        NotFound => "NotFound",
        AlreadyExists => "AlreadyExists",
        PermissionDenied => "PermissionDenied",
        ResourceExhausted => "ResourceExhausted",
        FailedPrecondition => "FailedPrecondition",
        Aborted => "Aborted",
        OutOfRange => "OutOfRange",
        Unimplemented => "Unimplemented",
        Internal => "Internal",
        Unavailable => "Unavailable",
        DataLoss => "DataLoss",
        Unauthenticated => "Unauthenticated",
    }
}

/// Refuses a call its credential may not make ([`crate::keys`]): the
/// internal methods only the root may (the token the servers of a
/// cluster share), writes a write or admin key, the rest any key.
fn may_call<T>(req: &Request<T>, method: &str) -> Result<(), Status> {
    let who = req.extensions().get::<Principal>().cloned().unwrap_or_else(Principal::open);
    let internal = matches!(method, "Replicate" | "Restore" | "Verify" | "Slots" | "Configure");
    if internal && !who.root {
        return Err(Status::permission_denied(format!("{method} is a call between the servers of a cluster: it needs the token they share, not a key ({:?})", who.name)));
    }
    let need = if matches!(method, "Write" | "WriteStream") { Scope::Write } else { Scope::Read };
    who.may(need, method).map_err(|e| Status::permission_denied(e.message))
}

impl Grpc {
    /// Serves one call: gives it its id (the caller's `x-request-id`, or a
    /// new one, sent back in the answer's metadata), runs it in a span that
    /// names it, counts it and logs it ([`crate::obs`]).
    async fn serve<T, R, Fut>(&self, method: &'static str, req: Request<T>, f: impl FnOnce(Arc<Service>, T) -> Fut) -> Result<Response<R>, Status>
    where
        Fut: Future<Output = Result<R, ApiError>>,
    {
        let id = obs::request_id(req.metadata().get(obs::HEADER).and_then(|v| v.to_str().ok()));
        let span = tracing::info_span!("request", id = %id, method);
        if let Err(s) = may_call(&req, method) {
            self.0.metrics.grpc(method, "PermissionDenied", 0.0);
            return Err(s);
        }
        let t0 = Instant::now();
        let r = crate::telemetry::traced(crate::telemetry::Trace::new(), obs::scope(Some(id.clone()), f(self.0.clone(), req.into_inner()))).instrument(span.clone()).await;
        let secs = t0.elapsed().as_secs_f64();
        let _in = span.enter();
        let (code, r) = match r {
            Ok(x) => {
                tracing::debug!(secs, "answered");
                ("Ok", Ok(Response::new(x)))
            }
            Err(e) => {
                if e.status >= 500 {
                    obs::server_error(e.status, &e.message);
                }
                let s = Status::from(e);
                (code_name(s.code()), Err(s))
            }
        };
        self.0.metrics.grpc(method, code, secs);
        let v = id.parse::<tonic::metadata::AsciiMetadataValue>().ok();
        match (r, v) {
            (Ok(mut a), Some(v)) => {
                a.metadata_mut().insert(obs::HEADER, v);
                Ok(a)
            }
            (Err(mut s), Some(v)) => {
                s.metadata_mut().insert(obs::HEADER, v);
                Err(s)
            }
            (r, None) => r,
        }
    }
}

#[tonic::async_trait]
impl pb::strata_server::Strata for Grpc {
    async fn query(&self, req: Request<pb::QueryRequest>) -> Result<Response<pb::QueryResponse>, Status> {
        self.serve("Query", req, |svc, r| async move {
            svc.check_epoch(r.epoch)?;
            let a = svc.query(query_req(r)?).await?;
            Ok(pb::QueryResponse { snapshot: a.lease, ..result_to_pb(&a.rs, a.took_ms) })
        })
        .await
    }

    async fn term_stats(&self, req: Request<pb::QueryRequest>) -> Result<Response<pb::TermStatsResponse>, Status> {
        self.serve("TermStats", req, |svc, r| async move {
            svc.check_epoch(r.epoch)?;
            let plan = request_plan(r.protocol, r.plan, &r.plan_json)?;
            let after = After::of(r.min_seq_no, r.min_seq_nos)?;
            let (stats, snapshot) = svc.term_stats(&r.sql, plan.as_ref(), &params_from_pb(r.params), &after, r.timeout_ms as u64).await?;
            Ok(pb::TermStatsResponse { stats_json: json_bytes(&stats), snapshot })
        })
        .await
    }

    async fn guard_counts(&self, req: Request<pb::QueryRequest>) -> Result<Response<pb::GuardCountsResponse>, Status> {
        self.serve("GuardCounts", req, |svc, r| async move {
            svc.check_epoch(r.epoch)?;
            let plan = request_plan(r.protocol, r.plan, &r.plan_json)?;
            let after = After::of(r.min_seq_no, r.min_seq_nos)?;
            let (need, counts) = svc.guard_counts(&r.sql, plan.as_ref(), &params_from_pb(r.params), &after, r.timeout_ms as u64).await?;
            Ok(pb::GuardCountsResponse { need: need as u32, counts })
        })
        .await
    }

    async fn sid_next(&self, req: Request<pb::SidNextRequest>) -> Result<Response<pb::SidNextResponse>, Status> {
        self.serve("SidNext", req, |svc, r| async move {
            let q = r.query.unwrap_or_default();
            svc.check_epoch(q.epoch)?;
            let plan = request_plan(q.protocol, q.plan, &q.plan_json)?;
            let after = After::of(q.min_seq_no, q.min_seq_nos)?;
            let next = svc.sid_next(&q.sql, plan.as_ref(), &params_from_pb(q.params), &r.field, &r.prefixes, &after, q.timeout_ms as u64).await?;
            let next = next.into_iter().map(|cs| {
                let (codes, counts) = cs.into_iter().unzip();
                pb::SidChildren { codes, counts }
            });
            Ok(pb::SidNextResponse { next: next.collect() })
        })
        .await
    }

    async fn write(&self, req: Request<pb::WriteRequest>) -> Result<Response<pb::WriteResponse>, Status> {
        self.serve("Write", req, |svc, r| async move {
            svc.check_epoch(r.epoch)?;
            let w = if !r.ops_json.is_empty() {
                let ops: Vec<serde_json::Value> = from_json(&r.ops_json, "write operations")?;
                let key = Some(r.idempotency_key).filter(|k| !k.is_empty());
                svc.write_ops(ops, key).await?
            } else {
                match svc.sql(&r.sql, &params_from_pb(r.params), After::Any, 0).await? {
                    crate::service::SqlAnswer::Written { w, .. } => w,
                    crate::service::SqlAnswer::Rows(_) => return Err(ApiError::bad("Write takes INSERT, UPDATE or DELETE; use Query")),
                }
            };
            Ok(write_response(w))
        })
        .await
    }

    type WriteStreamStream = std::pin::Pin<Box<dyn futures::Stream<Item = Result<pb::WriteResponse, Status>> + Send>>;

    /// Applies each batch once the one before has committed, and answers
    /// it; the first that fails ends the stream with its error, so that no
    /// batch after it is applied ([`Service::stream_write`]).
    async fn write_stream(&self, req: Request<tonic::Streaming<pb::WriteRequest>>) -> Result<Response<Self::WriteStreamStream>, Status> {
        may_call(&req, "WriteStream")?;
        let id = obs::request_id(req.metadata().get(obs::HEADER).and_then(|v| v.to_str().ok()));
        let mut input = req.into_inner();
        let svc = self.0.clone();
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let span = tracing::info_span!("request", id = %id, method = "WriteStream");
        let run = async move {
            let t0 = Instant::now();
            let mut s = Stream::default();
            let mut code = "Ok";
            while let Some(m) = input.next().await {
                let r = async {
                    let m = m.map_err(|e| ApiError::bad(format!("reading the stream: {}", e.message())))?;
                    svc.check_epoch(m.epoch)?;
                    if m.ops_json.is_empty() || !m.sql.is_empty() {
                        return Err(ApiError::bad("a WriteStream message carries write operations (ops_json), not SQL"));
                    }
                    if !m.idempotency_key.is_empty() {
                        return Err(ApiError::bad("WriteStream takes no idempotency keys: give the events sources, whose offsets make a replay skip what was written"));
                    }
                    let ops: Vec<serde_json::Value> = from_json(&m.ops_json, "write operations")?;
                    s.admit(&ops)?;
                    svc.stream_write(&mut s, ops).await
                }
                .await;
                match r {
                    Ok(w) => {
                        let r = pb::WriteResponse { offsets: s.offsets().into_iter().collect(), ..write_response(w) };
                        if tx.send(Ok(r)).await.is_err() {
                            break;
                        }
                    }
                    Err(e) => {
                        if e.status >= 500 {
                            obs::server_error(e.status, &e.message);
                        }
                        let st = Status::from(e);
                        code = code_name(st.code());
                        let _ = tx.send(Err(st)).await;
                        break;
                    }
                }
            }
            tracing::debug!(batches = s.batches, written = s.written, skipped = s.skipped, "stream ended");
            svc.metrics.grpc("WriteStream", code, t0.elapsed().as_secs_f64());
        };
        tokio::spawn(run.instrument(span));
        Ok(Response::new(Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))))
    }

    async fn sources(&self, req: Request<pb::SourcesRequest>) -> Result<Response<pb::SourcesResponse>, Status> {
        self.serve("Sources", req, |svc, _| async move {
            let sources = svc.sources().await?;
            Ok(pb::SourcesResponse { sources: sources.into_iter().map(|s| pb::Source { name: s.name, offset: s.offset, updated_micros: s.updated.unwrap_or(0) }).collect() })
        })
        .await
    }

    async fn schema(&self, req: Request<pb::Empty>) -> Result<Response<pb::SchemaResponse>, Status> {
        self.serve("Schema", req, |svc, _| async move {
            let tables = svc.catalog().iter().map(|t| serde_json::to_string(&**t).unwrap_or_default()).collect();
            let profiles = svc.rank_profiles().iter().map(|(t, p)| crate::coord::profile_json(t, p)).collect();
            Ok(pb::SchemaResponse { schema_json: serde_json::to_string(&svc.schema).unwrap_or_default(), tables_json: tables, profiles_json: profiles })
        })
        .await
    }

    async fn hello(&self, req: Request<pb::HelloRequest>) -> Result<Response<pb::HelloResponse>, Status> {
        self.serve("Hello", req, |svc, _| async move {
            let n = svc.local_db().and_then(|db| db.storage().numbering()).unwrap_or_default();
            Ok(pb::HelloResponse { slot_epoch: svc.slot_epoch(), role: svc.role() as i32, term: n.term, seq_no: n.seq, replaced_by_term: svc.replaced_by(), ..hello() })
        })
        .await
    }

    async fn replicate(&self, req: Request<pb::ReplicateRequest>) -> Result<Response<pb::ReplicateResponse>, Status> {
        self.serve("Replicate", req, |svc, r| async move { svc.replicate(r).await }).await
    }

    async fn restore(&self, req: Request<tonic::Streaming<pb::RestoreChunk>>) -> Result<Response<pb::ReplicateResponse>, Status> {
        self.serve("Restore", req, |svc, chunks| async move { svc.restore(chunks).await }).await
    }

    async fn verify(&self, req: Request<pb::VerifyRequest>) -> Result<Response<pb::VerifyResponse>, Status> {
        self.serve("Verify", req, |svc, r| async move { svc.verify_for(r).await }).await
    }

    async fn slots(&self, req: Request<pb::SlotsRequest>) -> Result<Response<pb::SlotsResponse>, Status> {
        self.serve("Slots", req, |svc, r| async move {
            if !r.map_json.is_empty() {
                let j: serde_json::Value = from_json(&r.map_json, "slot map")?;
                let map = crate::slots::SlotMap::from_json(&j).map_err(ApiError::bad)?;
                svc.set_slots(map, r.shard as usize).await?;
            }
            Ok(match svc.slots() {
                Some((m, shard)) => pb::SlotsResponse { epoch: m.epoch, map_json: serde_json::to_vec(&m.to_json()).unwrap_or_default(), shard: shard as u32 },
                None => pb::SlotsResponse::default(),
            })
        })
        .await
    }

    async fn configure(&self, req: Request<pb::ConfigureRequest>) -> Result<Response<pb::ConfigureResponse>, Status> {
        self.serve("Configure", req, |svc, r| async move {
            let acks = match r.write_acks.as_str() {
                "" => None,
                a => Some(crate::replication::Acks::parse(a).ok_or_else(|| ApiError::bad(format!("write_acks is all, majority or primary, not {a:?}")))?),
            };
            svc.configure(r.role(), r.replicas, acks, r.force).await
        })
        .await
    }

    async fn release(&self, req: Request<pb::ReleaseRequest>) -> Result<Response<pb::Empty>, Status> {
        self.serve("Release", req, |svc, r| async move {
            svc.release(&r.snapshots);
            Ok(pb::Empty {})
        })
        .await
    }

    async fn health(&self, req: Request<pb::Empty>) -> Result<Response<pb::HealthResponse>, Status> {
        self.serve("Health", req, |svc, _| async move {
            let d = svc.detail().await;
            let h = &d["health"];
            let n = |k: &str| h[k].as_u64().unwrap_or(0);
            Ok(pb::HealthResponse {
                status: h["status"].as_str().unwrap_or("unknown").to_string(),
                docs: n("docs"),
                segments: n("segments"),
                txn: n("txn"),
                detail: d.to_string(),
            })
        })
        .await
    }
}
