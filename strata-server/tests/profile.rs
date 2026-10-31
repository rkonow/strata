//! Query profiles: every query's timed steps (`stats.steps`), on the
//! request's clock beside its trace; CPU time with `"profile": true` or
//! for every query; the ended queries keeping them, the slow ones under a
//! threshold set while the server runs; and a coordinator's profile with
//! each shard's call, the shard's own steps inside it.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value as Json};
use strata::db::Db;
use strata::storage::DbOptions;
use strata_server::coord::Coordinator;
use strata_server::service::{shard_of, AgentConfig, Backend, Service};
use strata_server::{demo, server};
use tokio::net::TcpListener;

const N: usize = 3000;
const BM25: &str = "SELECT id FROM products WHERE price > 5 ORDER BY bm25(body, 'laptop phone') DESC LIMIT 5";

fn open(dir: &std::path::Path, name: &str, keep: impl Fn(usize) -> bool) -> Arc<Db> {
    let opts = DbOptions { publish_snapshots: true, segment_max_docs: 400, ..DbOptions::default() };
    let db = Db::create(&dir.join(name), demo::schema(N), opts).unwrap();
    db.set_embedder("emb", Arc::new(strata::embed::HashEmbedder { dim: demo::DIM })).unwrap();
    db.load((0..N).filter(|&i| keep(i)).map(|i| demo::product(i, 7))).unwrap();
    Arc::new(db)
}

fn service(backend: Backend, schema: strata::Schema) -> Arc<Service> {
    let agent = AgentConfig { api_key: None, model: String::new(), base_url: String::new() };
    Arc::new(Service::new(backend, schema, agent, 16, Duration::from_secs(60)))
}

async fn listen() -> (TcpListener, String) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://127.0.0.1:{}", l.local_addr().unwrap().port());
    (l, url)
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

async fn get(base: &str, path: &str) -> (u16, Json) {
    let r = reqwest::get(format!("{base}{path}")).await.unwrap();
    (r.status().as_u16(), r.json().await.unwrap_or(Json::Null))
}

async fn send(method: reqwest::Method, base: &str, path: &str, body: Json) -> (u16, Json) {
    let r = reqwest::Client::new().request(method, format!("{base}{path}")).json(&body).send().await.unwrap();
    (r.status().as_u16(), r.json().await.unwrap_or(Json::Null))
}

async fn query(base: &str, body: Json) -> Json {
    let (status, j) = send(reqwest::Method::POST, base, "/v1/query", body).await;
    assert_eq!(status, 200, "{j}");
    j
}

fn steps(j: &Json) -> Vec<Json> {
    j.as_array().cloned().unwrap_or_default()
}

fn ops(steps: &[Json]) -> Vec<(String, u64)> {
    steps.iter().map(|s| (s["op"].as_str().unwrap().to_string(), s["depth"].as_u64().unwrap_or(0))).collect()
}

/// The steps directly inside the step at `at`.
fn children(steps: &[Json], at: usize) -> Vec<usize> {
    let d = steps[at]["depth"].as_u64().unwrap_or(0);
    let depth = |s: &Json| s["depth"].as_u64().unwrap_or(0);
    steps[at + 1..].iter().enumerate().take_while(|(_, s)| depth(s) > d).filter(|(_, s)| depth(s) == d + 1).map(|(i, _)| at + 1 + i).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_query_s_profile_is_on_its_request_s_clock() {
    let dir = tempfile::tempdir().unwrap();
    let db = open(dir.path(), "one.strata", |_| true);
    let schema = db.schema().clone();
    let svc = service(Backend::Local(db), schema);
    let base = http(svc.clone()).await;

    // Every query answers its steps; "profile" adds their CPU time and the
    // request's trace.
    let plain = query(&base, json!({"sql": BM25})).await;
    let st = steps(&plain["stats"]["steps"]);
    assert!(ops(&st).iter().any(|(op, d)| op == "rank" && *d == 0), "{plain}");
    assert!(st.iter().all(|s| s.get("cpu_us").is_none()), "{plain}");
    assert!(plain.get("trace").is_none());

    let j = query(&base, json!({"sql": BM25, "profile": true})).await;
    let st = steps(&j["stats"]["steps"]);
    let rank = st.iter().position(|s| s["op"] == "rank").unwrap();
    assert!(st[rank]["cpu_us"].as_f64().unwrap() > 0.0, "{j}");
    let bm25 = children(&st, rank).into_iter().find(|&i| st[i]["op"] == "bm25").expect("bm25 under rank");
    let segs: Vec<&Json> = children(&st, bm25).iter().map(|&i| &st[i]).filter(|s| s["op"] == "segment").collect();
    assert!(segs.len() >= 5, "a step per segment: {j}");
    assert!(segs.iter().all(|s| s["cpu_us"].as_f64().is_some() && s["seg"].as_u64().is_some()));
    // The steps lie inside the trace's execute span, on the same clock:
    // the span's start and length are whole microseconds, each rounded
    // down, so its end reads up to 2 µs early.
    let trace = j["trace"].as_array().unwrap();
    let exec = trace.iter().find(|s| s["name"] == "execute").unwrap();
    let (e0, e1) = (exec["start_us"].as_f64().unwrap(), exec["start_us"].as_f64().unwrap() + exec["dur_us"].as_f64().unwrap());
    for s in &st {
        let (a, b) = (s["start_us"].as_f64().unwrap(), s["start_us"].as_f64().unwrap() + s["us"].as_f64().unwrap());
        assert!(a + 1.0 >= e0 && b <= e1 + 2.0, "{s} outside execute {exec}");
    }

    // The latest queries list each without its steps, which its own route
    // has, with the plan lines.
    let (_, recent) = get(&base, "/v1/queries/recent").await;
    let q = &recent["queries"][0];
    assert_eq!(q["sql"], BM25);
    assert!(q.get("steps").is_none() && q["steps_n"].as_u64().unwrap() as usize == st.len(), "{q}");
    assert!(q["cpu_ms"].as_f64().unwrap() > 0.0 && q["profiled"] == true, "{q}");
    let (status, full) = get(&base, &format!("/v1/queries/finished/{}", q["id"])).await;
    assert_eq!(status, 200);
    assert_eq!(steps(&full["steps"]).len(), st.len());
    assert!(full["plan"].as_array().unwrap().iter().any(|l| l.as_str().unwrap().contains("bm25")), "{full}");
    assert_eq!(get(&base, "/v1/queries/finished/999999").await.0, 404);
    let (_, all) = get(&base, "/v1/queries/recent?full=true").await;
    assert!(all["queries"][0]["steps"].is_array());

    // No slow queries until a threshold is set, then every query's
    // profile is kept among them; with cpu_all, every query's CPU time.
    let (_, s) = get(&base, "/v1/profile/settings").await;
    assert_eq!(s["slow_ms"], Json::Null);
    assert_eq!(s["cpu_all"], false);
    assert!(get(&base, "/v1/queries/slow").await.1["queries"].as_array().unwrap().is_empty());
    let (status, s) = send(reqwest::Method::PUT, &base, "/v1/profile/settings", json!({"slow_ms": 0, "cpu_all": true})).await;
    assert_eq!(status, 200, "{s}");
    assert_eq!(s["slow_ms"], 0.0);
    assert_eq!(s["cpu_all"], true);
    let j = query(&base, json!({"sql": "SELECT COUNT(*) FROM products WHERE price > 10"})).await;
    assert!(steps(&j["stats"]["steps"]).iter().any(|s| s["op"] == "count" && s["cpu_us"].as_f64().is_some()), "{j}");
    let (_, slow) = get(&base, "/v1/queries/slow").await;
    assert_eq!(slow["queries"].as_array().unwrap().len(), 1, "{slow}");
    assert_eq!(slow["slow_ms"], 0.0);
    let (status, _) = send(reqwest::Method::PUT, &base, "/v1/profile/settings", json!({"slow_ms": -1})).await;
    assert_eq!(status, 400);
    let (_, s) = send(reqwest::Method::PUT, &base, "/v1/profile/settings", json!({"slow_ms": null, "cpu_all": false})).await;
    assert_eq!(s["slow_ms"], Json::Null);
    query(&base, json!({"sql": BM25})).await;
    assert_eq!(get(&base, "/v1/queries/slow").await.1["queries"].as_array().unwrap().len(), 1);
    assert_eq!(get(&base, "/v1/system").await.1["limits"]["slow_ms"], Json::Null);

    // A compound query's operators, each leaf's executor steps inside.
    let j = query(&base, json!({"sql": "SELECT DISTINCT category FROM (SELECT category FROM products ORDER BY bm25(body, 'laptop') DESC LIMIT 40) AS t"})).await;
    let st = steps(&j["stats"]["steps"]);
    let leaf = st.iter().position(|s| s["op"] == "leaf").unwrap_or_else(|| panic!("{j}"));
    assert!(children(&st, leaf).iter().any(|&i| st[i]["op"] == "rank"), "{j}");

    // Explain answers the steps of its run.
    let (_, e) = send(reqwest::Method::POST, &base, "/v1/explain", json!({"sql": BM25, "profile": true})).await;
    assert!(steps(&e["steps"]).iter().any(|s| s["op"] == "segment" && s["cpu_us"].as_f64().is_some()), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_coordinator_s_profile_holds_each_shard_s_steps() {
    let dir = tempfile::tempdir().unwrap();
    let shard = |s: usize| move |i: usize| shard_of(&strata::Value::Str(format!("p{i:07}")), 2) == s;
    let mut shards = Vec::new();
    for s in 0..2 {
        let db = open(dir.path(), &format!("s{s}.strata"), shard(s));
        let schema = db.schema().clone();
        // The shards measure their steps' CPU time when the coordinator's
        // query asks (QueryRequest.profile), not for every query.
        shards.push(grpc(service(Backend::Local(db), schema)).await);
    }
    let c = Coordinator::connect(shards.clone()).await.unwrap();
    let schema = c.schema.clone();
    let base = http(service(Backend::Cluster(Arc::new(c)), schema)).await;

    let j = query(&base, json!({"sql": BM25, "profile": true})).await;
    let st = steps(&j["stats"]["steps"]);
    let top: Vec<String> = ops(&st).into_iter().filter(|(_, d)| *d == 0).map(|(o, _)| o).collect();
    assert_eq!(top.first().map(String::as_str), Some("term statistics"), "{j}");
    assert_eq!(top.iter().filter(|o| *o == "shard").count(), 2, "{j}");
    assert_eq!(top.last().map(String::as_str), Some("merge"), "{j}");
    let mut seen = Vec::new();
    for (i, s) in st.iter().enumerate().filter(|(_, s)| s["op"] == "shard") {
        seen.push(s["shard"].as_u64().unwrap());
        assert!(shards.iter().any(|u| s["detail"].as_str().unwrap().ends_with(u.as_str())), "{s}");
        let inner = children(&st, i);
        assert_eq!(inner.len(), 1, "{j}");
        let own = &st[inner[0]];
        assert_eq!(own["op"], "on the shard");
        // The shard's time lies inside its call, and its steps inside it,
        // their CPU time measured as the query asked.
        let (c0, c1) = (s["start_us"].as_f64().unwrap(), s["start_us"].as_f64().unwrap() + s["us"].as_f64().unwrap());
        let (o0, o1) = (own["start_us"].as_f64().unwrap(), own["start_us"].as_f64().unwrap() + own["us"].as_f64().unwrap());
        assert!(o0 + 1.0 >= c0 && o1 <= c1 + 1.0, "{own} outside {s}");
        let on_shard: Vec<&Json> = children(&st, inner[0]).iter().map(|&k| &st[k]).collect();
        let rank = on_shard.iter().find(|x| x["op"] == "rank").unwrap_or_else(|| panic!("{j}"));
        assert!(rank["cpu_us"].as_f64().is_some(), "{rank}");
    }
    seen.sort();
    assert_eq!(seen, [0, 1]);
    // Its CPU time is its shards'.
    let (_, recent) = get(&base, "/v1/queries/recent").await;
    assert!(recent["queries"][0]["cpu_ms"].as_f64().unwrap() > 0.0, "{recent}");
    // Without "profile", the shards measure no CPU time.
    let j = query(&base, json!({"sql": BM25})).await;
    let st = steps(&j["stats"]["steps"]);
    assert!(st.iter().any(|s| s["op"] == "rank"), "{j}");
    assert!(st.iter().all(|s| s.get("cpu_us").is_none_or(|c| c.is_null())), "{j}");
}
