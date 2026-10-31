//! CPU profiles: the whole server's threads sampled for a while, or a
//! query run again and again; one at a time; folded lines to download.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value as Json};
use strata::db::Db;
use strata::storage::DbOptions;
use strata_server::service::{AgentConfig, Backend, Service};
use strata_server::{demo, server};
use tokio::net::TcpListener;

const N: usize = 4000;
const BM25: &str = "SELECT id FROM products WHERE price > 5 ORDER BY bm25(body, 'laptop phone') DESC LIMIT 5";

async fn serve() -> String {
    let dir = Box::leak(Box::new(tempfile::tempdir().unwrap()));
    let opts = DbOptions { publish_snapshots: true, segment_max_docs: 500, ..DbOptions::default() };
    let db = Db::create(&dir.path().join("p.strata"), demo::schema(N), opts).unwrap();
    db.set_embedder("emb", Arc::new(strata::embed::HashEmbedder { dim: demo::DIM })).unwrap();
    db.load((0..N).map(|i| demo::product(i, 7))).unwrap();
    let schema = db.schema().clone();
    let agent = AgentConfig { api_key: None, model: String::new(), base_url: String::new() };
    let svc = Arc::new(Service::new(Backend::Local(Arc::new(db)), schema, agent, 16, Duration::from_secs(60)));
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://127.0.0.1:{}", l.local_addr().unwrap().port());
    tokio::spawn(server::serve_http(svc, l, Default::default()));
    url
}

async fn post(base: &str, path: &str, body: Json) -> (u16, String) {
    let r = reqwest::Client::new().post(format!("{base}{path}")).json(&body).send().await.unwrap();
    (r.status().as_u16(), r.text().await.unwrap())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cpu_profile_samples_the_server_s_threads() {
    let base = serve().await;
    if !cfg!(unix) {
        assert_eq!(post(&base, "/v1/profile/cpu", json!({"seconds": 0.2})).await.0, 501);
        return;
    }

    // A query run for a second: most samples are its work, on the
    // query's threads, and its runs are not kept among the latest.
    let (status, body) = post(&base, "/v1/profile/cpu", json!({"seconds": 1, "hz": 499, "sql": BM25})).await;
    assert_eq!(status, 200, "{body}");
    let p: Json = serde_json::from_str(&body).unwrap();
    let samples = p["samples"].as_u64().unwrap();
    assert!(samples >= 50, "{samples} samples");
    assert!(p["runs"]["count"].as_u64().unwrap() >= 10, "{}", p["runs"]);
    // The process's own CPU time: a query run without a pause keeps about
    // a core busy, whatever samples the system's timer delivered.
    assert!(p["cores"].as_f64().unwrap() > 0.5, "{}", p["cores"]);
    let rate = p["rate"].as_f64().unwrap();
    assert!(rate > 20.0 && rate <= 499.0 * 1.2, "{rate} samples a CPU second");
    let names: Vec<&str> = p["names"].as_array().unwrap().iter().map(|n| n.as_str().unwrap()).collect();
    let threads = p["threads"].as_array().unwrap();
    let stacks = p["stacks"].as_array().unwrap();
    assert_eq!(stacks.iter().map(|s| s["samples"].as_u64().unwrap()).sum::<u64>() + p["dropped"].as_u64().unwrap(), samples);
    for s in stacks {
        assert!((s["thread"].as_u64().unwrap() as usize) < threads.len());
        assert!(s["frames"].as_array().unwrap().iter().all(|f| (f.as_u64().unwrap() as usize) < names.len()));
    }
    // The executor's frames are named (the binary keeps its symbols).
    let strata: u64 = stacks.iter().filter(|s| s["frames"].as_array().unwrap().iter().any(|f| { let n = names[f.as_u64().unwrap() as usize]; n.starts_with("strata::") || n.starts_with("<strata::") })).map(|s| s["samples"].as_u64().unwrap()).sum();
    assert!(strata * 4 >= samples, "{strata} of {samples} samples in strata: {:?}", &names[..names.len().min(40)]);
    // A hash is "::h" and 16 hex digits at the end, as `profiler::clean`
    // takes it off; "::h1::role::Server>" (hyper) is the same length.
    assert!(!names.iter().any(|n| n.rfind("::h").is_some_and(|i| n.len() - i == 19 && n[i + 3..].bytes().all(|b| b.is_ascii_hexdigit()))), "hashes left: {names:?}");
    // Where the C library is sampled (glibc 2.35 and up), its time shows:
    // a query allocates and copies.
    let blocked: Vec<&str> = p["blocked"].as_array().unwrap().iter().map(|b| b.as_str().unwrap()).collect();
    if !blocked.contains(&"libc") {
        let c: u64 = stacks.iter().filter(|s| s["frames"].as_array().unwrap().last().is_some_and(|f| !names[f.as_u64().unwrap() as usize].contains("::"))).map(|s| s["samples"].as_u64().unwrap()).sum();
        assert!(c > 0, "no samples in the C library: {:?}", &names[..names.len().min(40)]);
    }
    let recent: Json = reqwest::get(format!("{base}/v1/queries/recent")).await.unwrap().json().await.unwrap();
    assert!(recent["queries"].as_array().unwrap().is_empty(), "the runs are not kept: {recent}");

    // One at a time.
    let first = tokio::spawn({
        let base = base.clone();
        async move { post(&base, "/v1/profile/cpu", json!({"seconds": 1.5})).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    let (status, body) = post(&base, "/v1/profile/cpu", json!({"seconds": 0.2})).await;
    assert_eq!(status, 409, "{body}");
    assert_eq!(first.await.unwrap().0, 200);

    // Folded lines: the thread, the frames from the root, the samples.
    let (status, folded) = post(&base, "/v1/profile/cpu?format=folded", json!({"seconds": 0.5, "hz": 499, "sql": BM25})).await;
    assert_eq!(status, 200);
    assert!(folded.lines().count() > 3, "{folded}");
    assert!(folded.lines().all(|l| l.rsplit_once(' ').is_some_and(|(stack, n)| !stack.is_empty() && n.parse::<u64>().is_ok())), "{folded}");

    // What it refuses.
    assert_eq!(post(&base, "/v1/profile/cpu", json!({"seconds": 0})).await.0, 400);
    assert_eq!(post(&base, "/v1/profile/cpu", json!({"seconds": 61})).await.0, 400);
    assert_eq!(post(&base, "/v1/profile/cpu", json!({"hz": 5000})).await.0, 400);
    assert_eq!(post(&base, "/v1/profile/cpu", json!({"sql": "SELECT nope FROM products"})).await.0, 400);
    assert_eq!(post(&base, "/v1/profile/cpu", json!({"sql": BM25, "clients": 0})).await.0, 400);

    // Several callers at once.
    let (status, body) = post(&base, "/v1/profile/cpu", json!({"seconds": 0.5, "sql": BM25, "clients": 4})).await;
    assert_eq!(status, 200, "{body}");
    let p: Json = serde_json::from_str(&body).unwrap();
    assert_eq!(p["runs"]["clients"], 4);
    assert!(p["runs"]["count"].as_u64().unwrap() >= 4, "{}", p["runs"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn without_the_counting_allocator_memory_says_so() {
    // This test binary keeps the system's allocator.
    let base = serve().await;
    let m: Json = reqwest::get(format!("{base}/v1/profile/memory")).await.unwrap().json().await.unwrap();
    assert_eq!(m["heap"]["installed"], false, "{m}");
    assert_eq!(m["heap"]["in_use_bytes"], 0, "{m}");
    let (status, body) = post(&base, "/v1/profile/heap", json!({"seconds": 0.1})).await;
    assert_eq!(status, 501, "{body}");
}
