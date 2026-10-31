//! The log store: a server keeps the lines it logs and the queries it
//! answers in a Strata database of its own, searched with SQL through
//! `POST /v1/logs/query`; a coordinator answers such a query for every
//! server of the cluster, merged, and names the servers that did not
//! answer.

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use serde_json::{json, Map, Value as Json};
use strata::db::Db;
use strata::storage::DbOptions;
use strata_server::coord::Coordinator;
use strata_server::logstore::{self, LogOptions, LogStore};
use strata_server::service::{shard_of, AgentConfig, Backend, Service};
use strata_server::{demo, obs, server};
use tokio::net::TcpListener;

const N: usize = 200;

/// The process's logs go through the server's subscriber, as in
/// `strata-server`, with the lines kept for a store.
fn logs() {
    static ONCE: OnceLock<()> = OnceLock::new();
    ONCE.get_or_init(|| {
        obs::init_logs("debug", obs::LogFormat::Json).unwrap();
        obs::sink().start();
    });
}

async fn listen() -> (TcpListener, String) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = l.local_addr().unwrap().port();
    (l, format!("http://127.0.0.1:{port}"))
}

async fn http(svc: Arc<Service>) -> String {
    let (l, url) = listen().await;
    tokio::spawn(server::serve_http(svc, l, Default::default()));
    url
}

async fn grpc(svc: Arc<Service>) -> String {
    let (l, url) = listen().await;
    tokio::spawn(server::serve_grpc(svc, l, Default::default()));
    url
}

async fn post(base: &str, path: &str, body: Json) -> (u16, Json) {
    let r = reqwest::Client::new().post(format!("{base}{path}")).json(&body).send().await.unwrap();
    (r.status().as_u16(), r.json().await.unwrap_or(Json::Null))
}

async fn logs_query(base: &str, sql: &str) -> Json {
    let (status, j) = post(base, "/v1/logs/query", json!({"sql": sql})).await;
    assert_eq!(status, 200, "{sql}: {j}");
    j
}

/// A shard server of the demo catalogue with a log store of its own,
/// named `node`.
fn shard(dir: &std::path::Path, name: &str, node: &str, i: usize) -> Arc<Service> {
    let db = Db::create(&dir.join(name), demo::schema(N), DbOptions { publish_snapshots: true, ..DbOptions::default() }).unwrap();
    db.set_embedder("emb", Arc::new(strata::embed::HashEmbedder { dim: demo::DIM })).unwrap();
    db.load((0..N).filter(|&k| shard_of(&strata::Value::Str(format!("p{k:07}")), 2) == i).map(|k| demo::product(k, 7))).unwrap();
    let schema = db.schema().clone();
    let agent = AgentConfig { api_key: None, model: String::new(), base_url: String::new() };
    let mut svc = Service::new(Backend::Local(Arc::new(db)), schema, agent, 16, Duration::from_secs(10));
    svc.log_store = Some(LogStore::open(LogOptions { capture: false, ..LogOptions::new(dir.join(format!("{name}.logs")), node.into()) }).unwrap());
    Arc::new(svc)
}

fn line(ts: u64, level: &'static str, message: &str, request: &str) -> Arc<obs::LogLine> {
    logstore::line(ts, level, "strata_server::test", message, Map::new(), Some(request))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_server_keeps_its_lines_and_queries_but_not_its_reading_of_them() {
    logs();
    let dir = tempfile::tempdir().unwrap();
    let o = server::Options { db: Some(dir.path().join("a.strata")), demo: Some(N), node_name: "a:7700".into(), log_queries_per_sec: 1000, timeout_ms: 10_000, ..Default::default() };
    let svc = server::service(&o).await.unwrap();
    let store = svc.log_store.clone().expect("a store beside the database");
    assert!(store.captures());
    assert_eq!(store.options().path, dir.path().join("a.strata.logs"));
    let base = http(svc).await;
    for i in 0..5 {
        let (s, _) = post(&base, "/v1/query", json!({"sql": format!("SELECT id FROM products WHERE price > {i} LIMIT 3")})).await;
        assert_eq!(s, 200);
    }
    let (s, _) = post(&base, "/v1/query", json!({"sql": "SELECT nope FROM products"})).await;
    assert_eq!(s, 400);
    // Reading the logs is not logged as a request answered.
    logs_query(&base, "SELECT COUNT(*) FROM logs").await;
    store.flush();

    let j = logs_query(&base, "SELECT route, COUNT(*) AS n FROM logs WHERE node = 'a:7700' AND MATCH(message, 'answered') GROUP BY route ORDER BY n DESC LIMIT 10").await;
    let by_route: Vec<(String, i64)> = j["rows"].as_array().unwrap().iter().map(|r| (r[0].as_str().unwrap_or("").to_string(), r[1].as_i64().unwrap())).collect();
    assert!(by_route.iter().any(|(r, n)| r == "/v1/query" && *n >= 5), "{by_route:?}");
    assert!(by_route.iter().all(|(r, _)| !r.starts_with("/v1/logs")), "{by_route:?}");
    // The line the database logged as it loaded, before any request.
    let j = logs_query(&base, "SELECT level, target FROM logs WHERE MATCH(message, 'loaded new database') LIMIT 5").await;
    assert_eq!(j["rows"][0][0], "info", "{j}");

    // Each query: its shape, outcome and time, the failed one's error.
    let j = logs_query(&base, "SELECT fingerprint, outcome, error, n_rows FROM queries ORDER BY ts DESC LIMIT 20").await;
    let rows = j["rows"].as_array().unwrap();
    assert_eq!(rows.iter().filter(|r| r[0] == "SELECT id FROM products WHERE price > ? LIMIT ?").count(), 5, "{j}");
    assert!(rows.iter().any(|r| r[1] == "error" && r[2].as_str().unwrap_or("").contains("nope")), "{j}");
    let j = logs_query(&base, "SELECT fingerprint, COUNT(*) AS n, AVG(ms) FROM queries GROUP BY fingerprint ORDER BY n DESC LIMIT 1").await;
    assert_eq!(j["rows"][0][1], 5, "{j}");
    // A join of the two tables, on one server.
    let j = logs_query(&base, "SELECT q.sql, l.route FROM queries q JOIN logs l ON q.request_id = l.request_id WHERE q.outcome = 'error' LIMIT 5").await;
    assert_eq!(j["rows"][0][1], "/v1/query", "{j}");

    let r = reqwest::get(format!("{base}/v1/logs/store")).await.unwrap();
    let j: Json = r.json().await.unwrap();
    assert_eq!(j["schemas"][0]["table"], "logs", "{j}");
    let st = &j["store"];
    assert_eq!(st["node"], "a:7700");
    assert!(st["tables"]["logs"]["rows"].as_u64().unwrap() > 5, "{st}");
    assert!(st["written"]["queries"].as_u64().unwrap() >= 6, "{st}");
    let m = reqwest::get(format!("{base}/metrics")).await.unwrap().text().await.unwrap();
    assert!(m.contains("strata_log_store_rows{table=\"logs\"}"), "{m}");
    let cluster: Json = reqwest::get(format!("{base}/v1/cluster")).await.unwrap().json().await.unwrap();
    assert_eq!(cluster["node"]["log_store"]["node"], "a:7700", "{cluster}");

    let (s, j) = post(&base, "/v1/logs/query", json!({"sql": "SELECT * FROM products LIMIT 1"})).await;
    assert_eq!(s, 400, "the store's tables only: {j}");
    store.close();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_coordinator_searches_every_servers_logs() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (shard(dir.path(), "s0.strata", "s0", 0), shard(dir.path(), "s1.strata", "s1", 1));
    let t = 1_793_400_000_000u64;
    a.log_store.as_ref().unwrap().write_lines(&[line(t, "info", "shard zero starts", "r0"), line(t + 30, "error", "shard zero fails a write", "r9")]).unwrap();
    b.log_store.as_ref().unwrap().write_lines(&[line(t + 10, "warn", "shard one is slow", "r9"), line(t + 40, "error", "shard one fails too", "r9")]).unwrap();
    let urls = vec![grpc(a.clone()).await, grpc(b.clone()).await];
    // The coordinator's own store, not taking the process's lines (the
    // other test's server takes them).
    let c = Coordinator::connect(urls.clone()).await.unwrap();
    let schema = c.schema.clone();
    let agent = AgentConfig { api_key: None, model: String::new(), base_url: String::new() };
    let mut coord = Service::new(Backend::Cluster(Arc::new(c)), schema, agent, 16, Duration::from_secs(10));
    let store = LogStore::open(LogOptions { capture: false, ..LogOptions::new(dir.path().join("coord.logs"), "coord".into()) }).unwrap();
    store.write_lines(&[line(t + 20, "warn", "the coordinator hedges", "r9")]).unwrap();
    coord.log_store = Some(store);
    let base = http(Arc::new(coord)).await;

    // Every server's lines, interleaved by time.
    let j = logs_query(&base, "SELECT node, message FROM logs WHERE request_id = 'r9' ORDER BY ts DESC LIMIT 10").await;
    let got: Vec<String> = j["rows"].as_array().unwrap().iter().map(|r| format!("{}: {}", r[0].as_str().unwrap(), r[1].as_str().unwrap())).collect();
    assert_eq!(got, ["s1: shard one fails too", "s0: shard zero fails a write", "coord: the coordinator hedges", "s1: shard one is slow"], "{j}");
    assert_eq!(j["nodes"].as_array().unwrap().len(), 3, "{j}");
    assert_eq!(j["nodes"][0]["node"], "coord", "{j}");
    // The merged plan names each server's part by the server.
    let plan: Vec<&str> = j["stats"]["plan"].as_array().unwrap().iter().filter_map(|l| l.as_str()).collect();
    assert!(plan.iter().any(|l| l.starts_with("coord: ")) && plan.iter().any(|l| l.starts_with("http://")), "{plan:?}");
    assert!(!plan.iter().any(|l| l.starts_with("shard ")), "{plan:?}");
    // Counted over the cluster.
    let j = logs_query(&base, "SELECT level, COUNT(*) AS n FROM logs WHERE ts >= '2026-10-01' AND MATCH(message, 'shard OR coordinator') GROUP BY level ORDER BY n DESC LIMIT 5").await;
    assert_eq!(j["rows"], json!([["error", 2], ["warn", 2], ["info", 1]]), "{j}");
    let j = logs_query(&base, "SELECT COUNT(*) FROM logs WHERE level = 'error'").await;
    assert_eq!(j["rows"][0][0], 2);
    // One server's own: the coordinator's.
    let (s, j) = post(&base, "/v1/logs/query", json!({"sql": "SELECT node FROM logs WHERE request_id = 'r9' LIMIT 10", "scope": "local"})).await;
    assert_eq!((s, j["rows"].clone()), (200, json!([["coord"]])), "{j}");
    // A join reads one server's store.
    let (s, j) = post(&base, "/v1/logs/query", json!({"sql": "SELECT l.node FROM logs l JOIN queries q ON l.request_id = q.request_id LIMIT 3"})).await;
    assert_eq!(s, 400, "{j}");
    assert!(j["error"].as_str().unwrap().contains("\"scope\": \"local\""), "{j}");

    // A server without a store is named, and the others answer.
    let mut bare = Arc::try_unwrap(shard(dir.path(), "s2.strata", "s2", 0)).ok().unwrap();
    bare.log_store = None;
    let bare_url = grpc(Arc::new(bare)).await;
    let o = server::Options { shards: vec![format!("{}|{bare_url}", urls[0]), urls[1].clone()], log_store: Some(None), timeout_ms: 10_000, ..Default::default() };
    let coord2 = server::service(&o).await.unwrap();
    assert!(coord2.log_store.is_none());
    let base2 = http(coord2).await;
    let j: Json = reqwest::get(format!("{base2}/v1/logs/store")).await.unwrap().json().await.unwrap();
    assert!(j["store"].is_null(), "{j}");
    let stores: Vec<&Json> = j["nodes"].as_array().unwrap().iter().map(|n| &n["store"]).collect();
    assert_eq!(stores.iter().filter(|s| s["node"].is_string()).count(), 2, "{j}");
    let j = logs_query(&base2, "SELECT COUNT(*) FROM logs WHERE request_id = 'r9'").await;
    assert_eq!(j["rows"][0][0], 3, "{j}");
    let nodes = j["nodes"].as_array().unwrap();
    assert_eq!(nodes.len(), 3, "{j}");
    let missing: Vec<&Json> = nodes.iter().filter(|n| n["error"].is_string()).collect();
    assert_eq!(missing.len(), 1, "{j}");
    assert_eq!(missing[0]["url"], json!(bare_url));
    assert!(missing[0]["error"].as_str().unwrap().contains("no log store"), "{j}");
}
