//! Changing a cluster while it serves (shard protocol 9): a coordinator's
//! admin calls add and remove shards and replicas, each change a new
//! cluster map (slots and the servers of each shard) that every server
//! keeps beside its database; other coordinators take it up, and one
//! started on an older list of shards routes by the map the servers hold.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use strata::db::Db;
use strata::query::Params;
use strata::storage::DbOptions;
use strata::value::Value;
use strata_server::auth::Access;
use strata_server::coord::Coordinator;
use strata_server::replication::{self, Acks, KeptRole, PrimaryOptions};
use strata_server::service::{shard_of, After, AgentConfig, Backend, QueryReq, Service};
use strata_server::{demo, pb, server, slots};
use tokio::net::TcpListener;

const N: usize = 300;

fn agent() -> AgentConfig {
    AgentConfig { api_key: None, model: strata_server::agent::DEFAULT_MODEL.into(), base_url: "http://unused".into() }
}

fn key(i: usize) -> Value {
    Value::Str(format!("p{i:07}"))
}

/// A numbered database at `dir/name` of the demo products `keep` keeps.
fn create(dir: &Path, name: &str, keep: impl Fn(usize) -> bool) -> Arc<Db> {
    let opts = DbOptions { publish_snapshots: true, ship_log_bytes: 16 << 20, ..DbOptions::default() };
    let db = Db::create(&dir.join(name), demo::schema(N), opts).unwrap();
    db.set_embedder("emb", Arc::new(strata::embed::HashEmbedder { dim: demo::DIM })).unwrap();
    let docs: Vec<_> = (0..N).filter(|&i| keep(i)).map(|i| demo::product(i, 7)).collect();
    if !docs.is_empty() {
        db.load(docs).unwrap();
    }
    db.storage().start_seq_no().unwrap();
    Arc::new(db)
}

/// A server of `db` (at `dir/name`) as `strata-server --db` makes one: it
/// keeps the cluster map and its part in replication beside the database,
/// and ships quickly when it becomes a primary.
async fn serve(db: Arc<Db>, dir: &Path, name: &str) -> (Arc<Service>, String) {
    let path = dir.join(name);
    let schema = db.schema().clone();
    let mut svc = Service::new(Backend::Local(db), schema, agent(), 16, Duration::from_secs(10));
    svc.keep_slots_in(slots::file_of(&path)).unwrap();
    svc.keep_replication_in(replication::file_of(&path));
    svc.promote_options = Some(PrimaryOptions { heartbeat: Duration::from_millis(100), retry_max: Duration::from_millis(200), ..Default::default() });
    let svc = Arc::new(svc);
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(server::serve_grpc(svc.clone(), l, Access::default()));
    (svc, url)
}

async fn coordinator(urls: &[String]) -> (Arc<Coordinator>, Arc<Service>) {
    let c = Arc::new(Coordinator::connect(urls.to_vec()).await.unwrap());
    let schema = c.schema.clone();
    (c.clone(), Arc::new(Service::new(Backend::Cluster(c), schema, agent(), 16, Duration::from_secs(10))))
}

/// The ids of every product a server or coordinator serves, sorted.
async fn ids(svc: &Arc<Service>) -> Vec<String> {
    let a = svc.query(QueryReq { sql: "SELECT id FROM products LIMIT 10000".into(), ..Default::default() }).await.unwrap();
    let mut v: Vec<String> = a.rs.rows.iter().map(|r| r[0].as_str().unwrap().to_string()).collect();
    v.sort();
    v
}

async fn sql(svc: &Arc<Service>, s: &str) -> Result<(), String> {
    svc.sql(s, &Params::new(), After::Any, 0).await.map(|_| ()).map_err(|e| e.message)
}

fn servers(c: &Coordinator) -> Vec<Vec<String>> {
    c.map().unwrap().replicas().to_vec()
}

async fn until<F: std::future::Future<Output = bool>>(what: &str, f: impl Fn() -> F) {
    let t0 = Instant::now();
    while !f().await {
        assert!(t0.elapsed() < Duration::from_secs(30), "waited 30 s for: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

const CHEAP: &str = "CREATE RANK PROFILE cheap ON products AS
    WITH top AS (SELECT * FROM products ORDER BY bm25(body, :t) DESC LIMIT 40)
    SELECT * FROM top ORDER BY bm25(body, :t) / (1 + :w * price) DESC
    WITH (t => 'kettle', w => 0.01)";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shards_join_and_leave_a_serving_cluster() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let mut shards = Vec::new();
    let mut urls = Vec::new();
    for s in 0..2 {
        let name = format!("s{s}.strata");
        let (svc, url) = serve(create(d, &name, |i| shard_of(&key(i), 2) == s), d, &name).await;
        shards.push(svc);
        urls.push(url);
    }
    let (a, svc_a) = coordinator(&urls).await;
    // Another coordinator, which follows what `a` changes.
    let (b, svc_b) = coordinator(&urls).await;
    let first = a.map().unwrap();
    assert_eq!(servers(&a), vec![vec![urls[0].clone()], vec![urls[1].clone()]], "the map names each shard's servers");
    assert_eq!(b.map().unwrap(), first);
    sql(&svc_a, "CREATE TABLE notes (id INT PRIMARY KEY, body TEXT)").await.unwrap();
    sql(&svc_a, CHEAP).await.unwrap();
    let all = ids(&svc_a).await;
    assert_eq!(all.len(), N);

    // What a new shard must be: new, empty, of the cluster's first table.
    let db2 = create(d, "s2.strata", |_| false);
    let (_, u2) = serve(db2.clone(), d, "s2.strata").await;
    let (_, full) = serve(create(d, "full.strata", |i| i < 5), d, "full.strata").await;
    let other = Db::create(&d.join("other.strata"), strata::Schema::new("other", vec![strata::Field::new("id", strata::FieldType::Keyword)], Some("id")).unwrap(), DbOptions::default()).unwrap();
    let (_, u_other) = serve(Arc::new(other), d, "other.strata").await;
    let e = a.add_shard(vec![urls[0].clone()], Acks::All).await.unwrap_err();
    assert!(e.message.contains("serves shard 0 already"), "{}", e.message);
    let e = a.add_shard(vec![full.clone()], Acks::All).await.unwrap_err();
    assert!(e.message.contains("holds 5 rows"), "{}", e.message);
    let e = a.add_shard(vec![u_other.clone()], Acks::All).await.unwrap_err();
    assert!(e.message.contains("another schema"), "{}", e.message);
    assert_eq!(a.map().unwrap(), first, "a refused change changes nothing");

    let j = a.add_shard(vec![u2.clone()], Acks::All).await.unwrap();
    assert_eq!(j["shard"], 2);
    assert_eq!(j["missed"], serde_json::json!([]));
    let added = a.map().unwrap();
    assert_eq!(added.epoch, first.epoch + 1);
    assert_eq!((added.shards(), added.count(2)), (3, 0), "the new shard holds no slot yet");
    assert_eq!(servers(&a)[2], vec![u2.clone()]);
    for (s, svc) in shards.iter().enumerate() {
        let (m, i) = svc.slots().unwrap();
        assert_eq!((m, i), (added.clone(), s), "shard {s} holds the new map");
    }
    // The new shard was given the cluster's other tables and profiles.
    assert!(db2.catalog().iter().any(|t| t.table == "notes"), "the new shard has the table made before it joined");
    assert!(db2.storage().rank_profiles("products").unwrap().iter().any(|p| p.name == "cheap"), "and the rank profile");
    assert_eq!(ids(&svc_a).await, all, "the coordinator reads every shard of the new map");

    // `b` routes by the map before until it takes up the new one: the
    // shards, whose slots did not change, still serve it.
    assert_eq!(b.map().unwrap(), first);
    assert_eq!(ids(&svc_b).await, all);
    sql(&svc_b, "DELETE FROM products WHERE id = 'p0000003'").await.unwrap();
    assert!(b.sync().await, "b takes up the newer map");
    assert_eq!(b.map().unwrap(), added);
    assert_eq!(ids(&svc_b).await.len(), N - 1);
    // A coordinator started on the old list of shards routes by the map
    // they hold.
    let (c, svc_c) = coordinator(&urls).await;
    assert_eq!(c.map().unwrap(), added);
    assert_eq!(ids(&svc_c).await.len(), N - 1);
    sql(&svc_a, "DELETE FROM products WHERE id = 'p0000004'").await.unwrap();

    let e = a.remove_shard(0).await.unwrap_err();
    assert!(e.message.contains("drain"), "a shard holding slots stays: {}", e.message);
    let j = a.remove_shard(2).await.unwrap();
    assert_eq!(j["removed"], serde_json::json!([u2.clone()]));
    let removed = a.map().unwrap();
    assert_eq!((removed.epoch, removed.shards()), (added.epoch + 1, 2));
    assert!(removed.same_slots_as(0, &first, 0) && removed.same_slots_as(1, &first, 1));
    assert_eq!(ids(&svc_a).await.len(), N - 2);
    assert!(c.sync().await);
    assert_eq!(c.map().unwrap(), removed);
    assert_eq!(ids(&svc_c).await.len(), N - 2);
    // The server that left holds the map it served under, and so does not
    // join another cluster by mistake.
    let e = a.add_shard(vec![u2.clone()], Acks::All).await.unwrap_err();
    assert!(e.message.contains("holds a cluster map already"), "{}", e.message);

    let cluster = svc_a.cluster().await;
    assert_eq!(cluster["slot_epoch"], removed.epoch);
    assert_eq!(cluster["changes"]["ok"], true);
    assert_eq!(cluster["shards"][0]["slots_held"].as_u64().unwrap() + cluster["shards"][1]["slots_held"].as_u64().unwrap(), slots::SLOTS as u64);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replicas_join_and_leave_a_shard() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let mut shards = Vec::new();
    let mut urls = Vec::new();
    for s in 0..2 {
        let name = format!("s{s}.strata");
        let (svc, url) = serve(create(d, &name, |i| shard_of(&key(i), 2) == s), d, &name).await;
        shards.push(svc);
        urls.push(url);
    }
    let (a, svc_a) = coordinator(&urls).await;
    let all = ids(&svc_a).await;
    let mine = ids(&shards[0]).await;

    // A standalone shard becomes the primary of the new replica, which is
    // sent a copy of it (its rows were bulk loaded, and took no number).
    let r0db = create(d, "r0.strata", |_| false);
    let (r0, u_r0) = serve(r0db.clone(), d, "r0.strata").await;
    let j = a.add_replica(0, &u_r0, false).await.unwrap();
    assert_eq!(j["primary"], urls[0]);
    assert_eq!(j["replicas"], serde_json::json!([u_r0.clone()]));
    assert_eq!(servers(&a)[0], vec![urls[0].clone(), u_r0.clone()]);
    assert_eq!(shards[0].role(), pb::Role::Primary);
    assert_eq!(r0.role(), pb::Role::Replica);
    until("the new replica holds the shard's rows", || async { r0db.snapshot().n_live_docs() == mine.len() as u64 }).await;
    until("the new replica serves reads", || async {
        r0.query(QueryReq { sql: "SELECT id FROM products LIMIT 10000".into(), ..Default::default() }).await.is_ok()
    })
    .await;
    assert_eq!(ids(&r0).await, mine);
    // Their parts are kept with their databases, for when they start again.
    let (kept, _) = replication::kept_role(&d.join("s0.strata")).unwrap();
    assert_eq!(kept, Some(KeptRole::Primary { replicas: vec![u_r0.clone()], acks: Acks::All }));
    assert_eq!(replication::kept_role(&d.join("r0.strata")).unwrap().0, Some(KeptRole::Replica));

    // Writes reach the replica, and the coordinator reads from either copy.
    let gone = all.iter().find(|id| mine.contains(id)).unwrap().clone();
    sql(&svc_a, &format!("DELETE FROM products WHERE id = '{gone}'")).await.unwrap();
    until("the replica applies the write", || async { r0db.snapshot().n_live_docs() == mine.len() as u64 - 1 }).await;
    assert_eq!(ids(&svc_a).await.len(), N - 1);

    // A second replica, added to a shard that replicates already.
    let r1db = create(d, "r1.strata", |_| false);
    let (_, u_r1) = serve(r1db.clone(), d, "r1.strata").await;
    let j = a.add_replica(0, &u_r1, false).await.unwrap();
    assert_eq!(j["replicas"], serde_json::json!([u_r0.clone(), u_r1.clone()]));
    until("the second replica catches up", || async { r1db.snapshot().n_live_docs() == mine.len() as u64 - 1 }).await;

    let e = a.remove_replica(0, &urls[0]).await.unwrap_err();
    assert!(e.message.contains("primary"), "the primary stays: {}", e.message);
    let e = a.remove_replica(1, &urls[1]).await.unwrap_err();
    assert!(e.message.contains("last server"), "{}", e.message);
    let j = a.remove_replica(0, &u_r0).await.unwrap();
    assert_eq!(j["shipping"]["replicas"], serde_json::json!([u_r1.clone()]));
    assert_eq!(servers(&a)[0], vec![urls[0].clone(), u_r1.clone()]);
    let (kept, _) = replication::kept_role(&d.join("s0.strata")).unwrap();
    assert_eq!(kept, Some(KeptRole::Primary { replicas: vec![u_r1.clone()], acks: Acks::All }));
    assert_eq!(ids(&svc_a).await.len(), N - 1);
    let health = shards[0].health().await;
    assert_eq!(health["replication"]["replicas"].as_array().unwrap().len(), 1, "{health}");
}

/// `Configure` refuses the changes of part that would lose writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn configure_refuses_what_would_lose_writes() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    let (_, u) = serve(create(d, "a.strata", |i| i < 10), d, "a.strata").await;
    let mut c = pb::strata_client::StrataClient::connect(u.clone()).await.unwrap();
    let ask = |role: pb::Role, replicas: Vec<String>, force: bool| pb::ConfigureRequest { role: role as i32, replicas, write_acks: String::new(), force };
    let e = c.configure(ask(pb::Role::Replica, vec![], false)).await.unwrap_err();
    assert!(e.message().contains("holds 10 rows"), "{}", e.message());
    let (_, r) = serve(create(d, "b.strata", |_| false), d, "b.strata").await;
    let a = c.configure(ask(pb::Role::Primary, vec![r.clone()], false)).await.unwrap().into_inner();
    assert_eq!((a.role(), a.replicas.clone()), (pb::Role::Primary, vec![r.clone()]));
    let e = c.configure(ask(pb::Role::Replica, vec![], false)).await.unwrap_err();
    assert!(e.message().contains("promote another copy first"), "{}", e.message());
    let e = c.configure(ask(pb::Role::Primary, vec![r.clone(), r.clone()], false)).await.unwrap_err();
    assert!(e.message().contains("named twice"), "{}", e.message());
    // Standalone again: it forgets its part.
    let a = c.configure(ask(pb::Role::Standalone, vec![], false)).await.unwrap().into_inner();
    assert_eq!(a.role(), pb::Role::Standalone);
    assert_eq!(replication::kept_role(&d.join("a.strata")).unwrap().0, None);
}
