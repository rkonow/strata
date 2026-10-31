//! A coordinator profiles the servers of its cluster: `/v1/profile/...`
//! with `?node=` (a replica's URL) is that server's profile, asked over
//! gRPC (`Profile`): its memory, its threads, a CPU or an allocation
//! profile of it (a query run there in a loop), JSON or folded.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value as Json};
use strata::db::Db;
use strata::storage::DbOptions;
use strata_server::coord::Coordinator;
use strata_server::service::{shard_of, AgentConfig, Backend, Service};
use strata_server::{demo, server};
use tokio::net::TcpListener;

// Allocation profiles need the server's allocator.
#[global_allocator]
static ALLOC: strata_server::heap::Counting = strata_server::heap::Counting;

const N: usize = 2000;
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

async fn send(method: reqwest::Method, url: String, body: Option<Json>) -> (u16, String) {
    let mut r = reqwest::Client::new().request(method, url);
    if let Some(b) = body {
        r = r.json(&b);
    }
    let r = r.send().await.unwrap();
    (r.status().as_u16(), r.text().await.unwrap())
}

async fn json_of(method: reqwest::Method, url: String, body: Option<Json>) -> (u16, Json) {
    let (status, text) = send(method, url, body).await;
    (status, serde_json::from_str(&text).unwrap_or(Json::Null))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_coordinator_profiles_each_server_of_its_cluster() {
    let dir = tempfile::tempdir().unwrap();
    let mut shards = Vec::new();
    let mut alone = None;
    for s in 0..2 {
        let db = open(dir.path(), &format!("s{s}.strata"), |i| shard_of(&strata::Value::Str(format!("p{i:07}")), 2) == s);
        let schema = db.schema().clone();
        let svc = service(Backend::Local(db), schema);
        let (l, url) = listen().await;
        tokio::spawn(server::serve_grpc(svc.clone(), l, Default::default()));
        shards.push(url);
        if s == 0 {
            let (l, url) = listen().await;
            tokio::spawn(server::serve_http(svc, l, Default::default()));
            alone = Some(url);
        }
    }
    let c = Coordinator::connect(shards.clone()).await.unwrap();
    let schema = c.schema.clone();
    let (l, base) = listen().await;
    tokio::spawn(server::serve_http(service(Backend::Cluster(Arc::new(c)), schema), l, Default::default()));
    let node = |path: &str, s: usize| format!("{base}{path}{}node={}", if path.contains('?') { "&" } else { "?" }, urlencoding(&shards[s]));
    use reqwest::Method;

    // Its memory and threads, said to be that node's.
    let (status, m) = json_of(Method::GET, node("/v1/profile/memory", 1), None).await;
    assert_eq!(status, 200, "{m}");
    assert_eq!(m["node"], shards[1].as_str());
    assert!(m["heap"]["installed"].as_bool().unwrap(), "{m}");
    assert!(m["rss_bytes"].as_u64().unwrap() > 0 && m["at_ms"].as_u64().is_some(), "{m}");
    let (status, t) = json_of(Method::GET, node("/v1/profile/threads", 0), None).await;
    assert_eq!(status, 200, "{t}");
    assert_eq!(t["node"], shards[0].as_str());
    assert!(!t["threads"].as_array().unwrap().is_empty() || !cfg!(target_os = "linux"), "{t}");

    // A CPU profile of a query run in a loop on shard 0's server: its runs
    // read that shard's rows alone.
    let (status, p) = json_of(Method::POST, node("/v1/profile/cpu", 0), Some(json!({"seconds": 1, "hz": 99, "sql": BM25}))).await;
    if status == 501 {
        // No sampler on this system: the coordinator says so as the node did.
        assert!(p.to_string().contains("no profile"), "{p}");
    } else {
        assert_eq!(status, 200, "{p}");
        assert_eq!(p["node"], shards[0].as_str());
        assert!(p["runs"]["count"].as_u64().unwrap() > 0, "{p}");
        assert!(p["samples"].as_u64().is_some() && p["names"].is_array(), "{p}");
    }

    // An allocation profile of shard 1's server, folded.
    let (status, text) = send(Method::POST, node("/v1/profile/heap?format=folded", 1), Some(json!({"seconds": 0.5, "every": 8192, "sql": BM25}))).await;
    assert_eq!(status, 200, "{text}");
    let line = text.lines().next().unwrap_or_else(|| panic!("no folded lines"));
    let (stack, n) = line.rsplit_once(' ').unwrap();
    assert!(stack.contains(';') && n.parse::<u64>().is_ok(), "{line}");
    // The node's refusals come back as they were: a bad body.
    let (status, e) = json_of(Method::POST, node("/v1/profile/heap", 1), Some(json!({"seconds": 0}))).await;
    assert_eq!(status, 400, "{e}");
    let (status, e) = json_of(Method::POST, node("/v1/profile/cpu", 1), Some(json!({"sql": "SELECT nope FROM products"}))).await;
    assert_eq!(status, 400, "{e}");

    // A node not in the cluster, and a node on a server that is no coordinator.
    let (status, e) = json_of(Method::GET, format!("{base}/v1/profile/memory?node=http://127.0.0.1:1"), None).await;
    assert_eq!(status, 404, "{e}");
    let (status, e) = json_of(Method::GET, format!("{}/v1/profile/memory?node={}", alone.unwrap(), urlencoding(&shards[1])), None).await;
    assert_eq!(status, 400, "{e}");
    // Without node, the coordinator's own.
    let (status, m) = json_of(Method::GET, format!("{base}/v1/profile/memory"), None).await;
    assert_eq!(status, 200, "{m}");
    assert!(m.get("node").is_none(), "{m}");
}

fn urlencoding(s: &str) -> String {
    s.bytes().map(|b| if b.is_ascii_alphanumeric() || b"-._~".contains(&b) { (b as char).to_string() } else { format!("%{b:02X}") }).collect()
}
