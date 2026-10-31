//! Ranking models on servers: a coordinator opens the model from its own
//! models directory (it scores the shards' rows with it too) and sends the
//! shards a plan naming the model's hash, so a query by a model answers as
//! on one database, and a shard whose file of the model is another refuses
//! the plan rather than rank otherwise.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use strata::db::Db;
use strata::query::{compound, Params};
use strata::storage::DbOptions;
use strata::value::Value;
use strata_server::auth::Access;
use strata_server::coord::Coordinator;
use strata_server::service::{shard_of, After, AgentConfig, Backend, QueryReq, Service};
use strata_server::{demo, server};
use tokio::net::TcpListener;

const N: usize = 400;

fn agent() -> AgentConfig {
    AgentConfig { api_key: None, model: strata_server::agent::DEFAULT_MODEL.into(), base_url: "http://unused".into() }
}

/// A models directory holding the model `from` of strata/tests/models as
/// `m`.
fn models(dir: &Path, name: &str, from: &str) -> PathBuf {
    let d = dir.join(name);
    std::fs::create_dir_all(d.join("m")).unwrap();
    std::fs::copy(Path::new(env!("CARGO_MANIFEST_DIR")).join("../strata/tests/models").join(from).join("gbdt.json"), d.join("m/gbdt.json")).unwrap();
    d
}

/// The demo products `keep` keeps, of the first `N`, served with the
/// models of `models`.
fn service(dir: &Path, name: &str, models: &Path, keep: impl Fn(usize) -> bool) -> Arc<Service> {
    let db = Db::create(&dir.join(name), demo::schema(N), DbOptions { publish_snapshots: true, segment_max_docs: 150, ..DbOptions::default() }).unwrap();
    db.set_embedder("emb", Arc::new(strata::embed::HashEmbedder { dim: demo::DIM })).unwrap();
    db.load((0..N).filter(|&i| keep(i)).map(|i| demo::product(i, 7))).unwrap();
    let schema = db.schema().clone();
    let mut svc = Service::new(Backend::Local(Arc::new(db)), schema, agent(), 16, Duration::from_secs(10));
    svc.open_models(models);
    Arc::new(svc)
}

async fn grpc(svc: Arc<Service>) -> String {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(server::serve_grpc(svc, l, Access::default()));
    url
}

async fn coordinator(urls: &[String], models: &Path) -> Arc<Service> {
    let c = Coordinator::connect(urls.to_vec()).await.unwrap();
    let schema = c.schema.clone();
    let mut svc = Service::new(Backend::Cluster(Arc::new(c)), schema, agent(), 16, Duration::from_secs(10));
    svc.open_models(models);
    Arc::new(svc)
}

/// Ids and scores.
async fn query(svc: &Arc<Service>, s: &str) -> Result<Vec<(String, f64)>, String> {
    let a = svc.query(QueryReq { sql: s.into(), ..Default::default() }).await.map_err(|e| e.message)?;
    Ok(a.rs.rows.iter().zip(&a.rs.scores).map(|(r, s)| (r[0].as_str().unwrap().to_string(), *s)).collect())
}

const BY: &str = "gbdt('m', bm25(body, 'kettle lamp'), rating, reviews % 6)";

#[tokio::test(flavor = "multi_thread")]
async fn shards_rank_by_a_model_like_one_database() {
    let dir = tempfile::tempdir().unwrap();
    let lgb = models(dir.path(), "lgb", "lgb_rank");
    let whole = service(dir.path(), "whole.strata", &lgb, |_| true);
    let key = |i: usize| Value::Str(format!("p{i:07}"));
    let shards: Vec<Arc<Service>> = (0..3).map(|s| service(dir.path(), &format!("s{s}.strata"), &lgb, |i| shard_of(&key(i), 3) == s)).collect();
    let mut urls = Vec::new();
    for s in &shards {
        urls.push(grpc(s.clone()).await);
    }
    let coord = coordinator(&urls, &lgb).await;
    let queries = [
        format!("SELECT id, score() FROM products ORDER BY {BY} DESC, id LIMIT 10"),
        format!("SELECT id, score() FROM products WHERE price < 200 ORDER BY sigmoid({BY}) DESC, id LIMIT 8 OFFSET 2"),
        format!("WITH top AS (SELECT * FROM products ORDER BY bm25(body, 'kettle lamp') DESC LIMIT 40) SELECT id, score() FROM top ORDER BY {BY} DESC LIMIT 10"),
        format!("SELECT id, score() FROM products WHERE id IN (SELECT id FROM products ORDER BY {BY} DESC LIMIT 5) ORDER BY price DESC LIMIT 5"),
    ];
    for q in &queries {
        let want = query(&whole, q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        assert!(!want.is_empty(), "{q}");
        let got = query(&coord, q).await.unwrap_or_else(|e| panic!("{q}: {e}"));
        assert_eq!(want.iter().map(|w| &w.0).collect::<Vec<_>>(), got.iter().map(|g| &g.0).collect::<Vec<_>>(), "{q}");
        for (w, g) in want.iter().zip(&got) {
            assert_eq!(w.1.to_bits(), g.1.to_bits(), "{q}: {w:?} against {g:?}");
        }
    }
    // The plan names the model's file; a shard with another file of it
    // refuses the plan, through the coordinator too.
    let catalog = whole.catalog();
    let tables: Vec<&strata::Schema> = catalog.iter().map(|t| &**t).collect();
    let plan = compound::plan_read(&queries[0], &tables, &Params::new(), &|_, _| None).unwrap();
    let opened = whole.open_ranking_models(Arc::new(plan.clone()), false).unwrap();
    let hash = strata::query::model::Gbdt::parse("m", &std::fs::read(lgb.join("m/gbdt.json")).unwrap()).unwrap().hash;
    assert!(opened.to_json_text().unwrap().contains(&format!(r#""hash":"{hash}""#)));
    assert!(opened.explain(&tables).contains(&format!("ranking model: m (hash {hash})")));
    let xgb = models(dir.path(), "xgb", "xgb_rank");
    let other = service(dir.path(), "other.strata", &xgb, |i| shard_of(&key(i), 3) == 0);
    let e = other.run_plan((*opened).clone(), After::Any, 0).await.err().unwrap().message;
    assert!(e.contains("ranking model m here is not the one the plan was made with"), "{e}");
    // Over servers of their own: the shards above hold a cluster map that
    // names their servers.
    let mut mixed_urls = vec![grpc(other.clone()).await];
    for s in 1..3 {
        mixed_urls.push(grpc(service(dir.path(), &format!("mixed{s}.strata"), &lgb, |i| shard_of(&key(i), 3) == s)).await);
    }
    let mixed = coordinator(&mixed_urls, &lgb).await;
    let e = query(&mixed, &queries[0]).await.err().unwrap();
    assert!(e.contains("ranking model m here is not the one the plan was made with"), "{e}");
    // A coordinator without the model cannot score the shards' rows.
    let none = coordinator(&urls, &dir.path().join("none")).await;
    let e = query(&none, &queries[0]).await.err().unwrap();
    assert!(e.contains("no ranking model named m"), "{e}");
    // A plan without a hash runs with each server's model.
    let a = whole.run_plan(plan.clone(), After::Any, 0).await.unwrap();
    let b = coord.run_plan(plan, After::Any, 0).await.unwrap();
    let rows = |a: strata_server::service::SqlAnswer| match a {
        strata_server::service::SqlAnswer::Rows(r) => r.rs.rows,
        _ => panic!("rows"),
    };
    assert_eq!(rows(a), rows(b));
}
