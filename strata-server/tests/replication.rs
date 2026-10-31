//! Replication between servers: a primary ships its numbered transactions
//! to its replicas over gRPC, and answers a write once the copies
//! `--write-acks` names hold it. Replicas take no writes from clients,
//! follow one primary per term and fence an older one, and a coordinator
//! sends a shard's writes to its primary.

use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{json, Value as Json};
use strata::db::Db;
use strata::query::Params;
use strata::storage::{CheckpointOptions, Database, DbOptions, Layout, LogRead};
use strata::{Field, FieldType, Schema};
use strata::value::Value;
use strata_server::auth::Access;
use strata_server::coord::Coordinator;
use strata_server::replication::{self, Acks, Digests, Follow, Primary, PrimaryOptions};
use strata_server::service::{shard_of, After, AgentConfig, Backend, QueryReq, Replication, Service, SqlAnswer};
use strata_server::{demo, pb, server, slots};
use tokio::net::TcpListener;

const N: usize = 300;

fn agent() -> AgentConfig {
    AgentConfig { api_key: None, model: strata_server::agent::DEFAULT_MODEL.into(), base_url: "http://unused".into() }
}

fn options(ship_log_bytes: usize) -> DbOptions {
    DbOptions { publish_snapshots: true, ship_log_bytes, ..DbOptions::default() }
}

/// A numbered database of the demo products `keep` keeps, of the first
/// `N`, with a ship log of `log` bytes.
fn create_with(dir: &Path, name: &str, log: usize, keep: impl Fn(usize) -> bool) -> Arc<Db> {
    let db = Db::create(&dir.join(name), demo::schema(N), options(log)).unwrap();
    db.set_embedder("emb", Arc::new(strata::embed::HashEmbedder { dim: demo::DIM })).unwrap();
    db.load((0..N).filter(|&i| keep(i)).map(|i| demo::product(i, 7))).unwrap();
    db.storage().start_seq_no().unwrap();
    Arc::new(db)
}

fn create(dir: &Path, name: &str) -> Arc<Db> {
    create_with(dir, name, 64 << 20, |_| true)
}

fn service(db: Arc<Db>, timeout: Duration) -> Service {
    let schema = db.schema().clone();
    Service::new(Backend::Local(db), schema, agent(), 16, timeout)
}

async fn grpc_on(svc: Arc<Service>, l: TcpListener) -> String {
    let url = format!("http://{}", l.local_addr().unwrap());
    tokio::spawn(server::serve_grpc(svc, l, Access::default()));
    url
}

async fn grpc(svc: Arc<Service>) -> String {
    grpc_on(svc, TcpListener::bind("127.0.0.1:0").await.unwrap()).await
}

/// A replica of `db` served over gRPC (on `l`, or a new port).
async fn replica_on(db: Arc<Db>, file: Option<&Path>, l: Option<TcpListener>) -> (Arc<Service>, String) {
    let mut svc = service(db.clone(), Duration::from_secs(10));
    svc.set_replication(Replication::Replica(Arc::new(Follow::open(db, file).unwrap())));
    let svc = Arc::new(svc);
    let l = match l {
        Some(l) => l,
        None => TcpListener::bind("127.0.0.1:0").await.unwrap(),
    };
    (svc.clone(), grpc_on(svc, l).await)
}

async fn replica(db: Arc<Db>, file: Option<&Path>) -> (Arc<Service>, String) {
    replica_on(db, file, None).await
}

/// Quick heartbeats and retries, for tests.
fn ships_to(replicas: &[&str], acks: Acks) -> PrimaryOptions {
    PrimaryOptions {
        replicas: replicas.iter().map(|u| u.to_string()).collect(),
        acks,
        heartbeat: Duration::from_millis(100),
        retry_max: Duration::from_millis(200),
        ..Default::default()
    }
}

/// A primary of `db`, whose writes wait up to `timeout`.
fn primary(db: Arc<Db>, file: Option<&Path>, opts: PrimaryOptions, timeout: Duration) -> Arc<Service> {
    let mut svc = service(db.clone(), timeout);
    svc.set_replication(Replication::Primary(Primary::start(db, file, opts).unwrap()));
    Arc::new(svc)
}

/// A gRPC URL whose port nothing listens on, and the address to listen
/// on later. The port is below the ephemeral ranges (32768 and up on
/// Linux, 49152 and up on macOS): a port taken with port 0 and let go
/// can be handed to another test's listener or to a connection's own
/// end before the replica binds it, which fails with "address in use".
async fn nobody() -> (String, std::net::SocketAddr) {
    static NEXT: AtomicU32 = AtomicU32::new(0);
    loop {
        let port = 20_000 + (std::process::id() + NEXT.fetch_add(1, Ordering::Relaxed)) % 12_000;
        if let Ok(l) = TcpListener::bind(("127.0.0.1", port as u16)).await {
            let addr = l.local_addr().unwrap();
            return (format!("http://{addr}"), addr);
        }
    }
}

/// Every row's stored fields and sequence number, in key order, and how
/// many rows the text index finds for a word.
fn rows(db: &Db) -> (Vec<Vec<Value>>, Json) {
    let r = db.query("SELECT id, title, brand, tags, price, reviews, sold, seq_no() FROM products LIMIT 100000", &Params::new()).unwrap();
    let mut rows = r.rows;
    rows.sort_by_key(|r| r[0].as_str().unwrap().to_string());
    let found = db.query("SELECT COUNT(*) FROM products WHERE MATCH(body, 'kettle')", &Params::new()).unwrap().rows[0][0].clone();
    (rows, strata_server::json::to_json(&found))
}

fn last(db: &Db) -> u64 {
    db.storage().seq_no().unwrap().seq
}

async fn until<F: std::future::Future<Output = bool>>(what: &str, f: impl Fn() -> F) {
    let t0 = Instant::now();
    while !f().await {
        assert!(t0.elapsed() < Duration::from_secs(30), "waited 30 s for: {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The primary's view of replica `k`.
async fn follower(p: &Arc<Service>, k: usize) -> Json {
    p.health().await["replication"]["replicas"][k].clone()
}

/// Holds `db`'s writer, as a long write would, until the sender is dropped.
fn stall(db: &Arc<Db>) -> mpsc::Sender<()> {
    let (release, wait) = mpsc::channel::<()>();
    let (held, ready) = mpsc::channel();
    let db = db.clone();
    std::thread::spawn(move || {
        // A heartbeat may be applying, and holds the writer for a moment.
        let t0 = Instant::now();
        let _t = loop {
            match db.storage().begin() {
                Ok(t) => break t,
                Err(e) => assert!(t0.elapsed() < Duration::from_secs(10), "{e}"),
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        held.send(()).unwrap();
        let _ = wait.recv();
    });
    ready.recv().unwrap();
    release
}

fn set(key: &str, field: &str, value: Json) -> Json {
    json!({"op": "set", "key": key, "field": field, "value": value})
}

fn written(a: SqlAnswer) -> (u64, Option<u64>) {
    match a {
        SqlAnswer::Written { w, .. } => (w.rows, w.seq_no),
        SqlAnswer::Rows(_) => panic!("not a write"),
    }
}

async fn hello(url: &str) -> pb::HelloResponse {
    let mut c = pb::strata_client::StrataClient::connect(url.to_string()).await.unwrap();
    c.hello(pb::HelloRequest { min_protocol: 1, max_protocol: 4 }).await.unwrap().into_inner()
}

#[tokio::test(flavor = "multi_thread")]
async fn replicas_hold_what_their_primary_wrote() {
    let dir = tempfile::tempdir().unwrap();
    let (r1db, r2db) = (create(dir.path(), "r1.strata"), create(dir.path(), "r2.strata"));
    let (r1, u1) = replica(r1db.clone(), Some(&replication::file_of(&dir.path().join("r1.strata")))).await;
    let (_r2, u2) = replica(r2db.clone(), None).await;
    let pdb = create(dir.path(), "p.strata");
    let p = primary(pdb.clone(), Some(&replication::file_of(&dir.path().join("p.strata"))), ships_to(&[&u1, &u2], Acks::All), Duration::from_secs(10));
    let pu = grpc(p.clone()).await;
    let holds = |seq: u64| {
        for db in [&r1db, &r2db] {
            assert!(last(db) >= seq, "a replica holds {} when the write of {seq} is answered", last(db));
        }
    };

    // Write operations of every kind, answered once both replicas hold them.
    let doc = json!({"id": "n1", "title": "Test kettle", "description": "A quiet kettle", "brand": "Hearthly", "category": "kitchen", "tags": ["quiet", "new"], "price": 20.5, "rating": 4.0, "reviews": 3, "sold": 0, "year": 2026});
    let ops = vec![
        json!({"op": "insert", "doc": doc}),
        json!({"op": "add", "key": "p0000003", "field": "sold", "value": 5}),
        set("p0000003", "price", json!(9.99)),
        json!({"op": "upsert_if_newer", "doc": {"id": "n1", "reviews": 2, "price": 1.0}, "version": "reviews"}),
        json!({"op": "delete", "key": "p0000004"}),
    ];
    let w = p.write_ops(ops, None).await.unwrap();
    assert_eq!((w.rows, w.seq_no), (4, Some(1)));
    holds(1);
    // SQL, a conditional write, and an import.
    let (n, seq) = written(p.sql("UPDATE products SET sold = sold + 1 WHERE category = 'kitchen'", &Params::new(), After::Any, 0).await.unwrap());
    assert!(n > 0);
    holds(seq.unwrap());
    written(p.sql("INSERT OR REPLACE INTO products (id, title, price, tags, reviews) VALUES ('n2', 'Steel kettle', 25.0, ARRAY['steel'], 9)", &Params::new(), After::Any, 0).await.unwrap());
    written(p.sql("DELETE FROM products WHERE price > 300", &Params::new(), After::Any, 0).await.unwrap());
    let seq3 = pdb.storage().seq_of(None, &Value::Str("p0000003".into())).unwrap().unwrap();
    let w = p.write_ops(vec![json!({"op": "add", "key": "p0000003", "field": "sold", "value": 1, "if_seq_no": seq3})], None).await.unwrap();
    holds(w.seq_no.unwrap());
    let (n, _) = p.import(None, axum::body::Bytes::from("id,title,price\nc1,Copper kettle,12.5\nc2,Iron pan,30\n"), strata::ingest::Format::Csv).await.unwrap();
    assert_eq!(n, 2);
    holds(last(&pdb));
    // Writers at once, which the writer groups into shared transactions. Each
    // adds to a row the primary holds, so that each write numbers one.
    let keys: Arc<Vec<String>> = Arc::new(rows(&pdb).0.iter().map(|r| r[0].as_str().unwrap().to_string()).collect());
    let tasks: Vec<_> = (0..24)
        .map(|t| {
            let (p, keys) = (p.clone(), keys.clone());
            tokio::spawn(async move {
                let mut seqs = Vec::new();
                for i in 0..5 {
                    let key = &keys[(t * 7 + i * 3) % keys.len()];
                    let w = p.write_ops(vec![json!({"op": "add", "key": key, "field": "reviews", "value": 1})], Some(format!("w{t}-{i}"))).await.unwrap();
                    seqs.push(w.seq_no.unwrap());
                }
                seqs
            })
        })
        .collect();
    for t in tasks {
        holds(*t.await.unwrap().iter().max().unwrap());
    }
    let end = last(&pdb);
    assert!(end > 10, "{end}");
    assert_eq!(rows(&r1db), rows(&pdb), "a replica holds the primary's rows, each with the primary's number");
    assert_eq!(rows(&r2db), rows(&pdb));
    assert!(rows(&pdb).1.as_u64().unwrap() >= 3, "the text index finds the written kettles, on every copy");

    // What the copies say of themselves.
    let h = p.health().await["replication"].clone();
    assert_eq!((h["role"].clone(), h["seq_no"].clone(), h["write_acks"].clone()), (json!("primary"), json!(end), json!("all")));
    for k in 0..2 {
        let f = &h["replicas"][k];
        assert_eq!((f["in_sync"].clone(), f["seq_no"].clone(), f["behind"].clone()), (json!(true), json!(end), json!(0)), "{h}");
    }
    assert_eq!(h["global_checkpoint"], end);
    let id = h["id"].as_str().unwrap().to_string();
    assert_eq!(id.len(), 32);
    until("the replica hears the global checkpoint", || async { r1.health().await["replication"]["global_checkpoint"] == end }).await;
    let rh = r1.health().await["replication"].clone();
    assert_eq!((rh["role"].clone(), rh["seq_no"].clone(), rh["follows"]["primary"].clone(), rh["follows"]["term"].clone()), (json!("replica"), json!(end), json!(id), json!(1)));
    let kept: Json = serde_json::from_slice(&std::fs::read(replication::file_of(&dir.path().join("r1.strata"))).unwrap()).unwrap();
    assert_eq!(kept["follows"]["primary"], json!(id), "the replica keeps the primary it follows");
    let kept: Json = serde_json::from_slice(&std::fs::read(replication::file_of(&dir.path().join("p.strata"))).unwrap()).unwrap();
    assert_eq!(kept["id"], json!(id), "the primary keeps its id");
    let (hp, hr) = (hello(&pu).await, hello(&u1).await);
    assert_eq!((hp.role(), hp.term, hp.seq_no), (pb::Role::Primary, 1, end));
    assert_eq!((hr.role(), hr.term, hr.seq_no), (pb::Role::Replica, 1, end));
    let m = p.prometheus().await;
    assert!(m.contains(&format!("strata_replica_in_sync{{replica=\"{u1}\"}} 1")), "{m}");
    assert!(m.contains(&format!("strata_replica_shipped_total{{replica=\"{u2}\"}} {}", end)), "{m}");
    assert!(r1.prometheus().await.contains(&format!("strata_replica_applied_total {end}")));

    // A replica takes no writes from clients, nor a server that is not
    // one a primary's.
    let e = r1.write_ops(vec![set("p0000003", "price", json!(1.0))], None).await.unwrap_err();
    assert!(e.status == 409 && e.message.contains("this server is a replica"), "{}", e.message);
    let e = r1.sql("UPDATE products SET sold = 0", &Params::new(), After::Any, 0).await.err().unwrap();
    assert_eq!(e.status, 409, "{}", e.message);
    let e = r1.import(None, axum::body::Bytes::from("id,title\nc3,x\n"), strata::ingest::Format::Csv).await.unwrap_err();
    assert_eq!(e.status, 409, "{}", e.message);
    assert_eq!(last(&r1db), end);
    let e = p.replicate(pb::ReplicateRequest { term: 1, primary: "another".into(), ..Default::default() }).await.unwrap_err();
    assert!(e.status == 409 && e.message.contains("not a replica"), "{}", e.message);
}

#[tokio::test(flavor = "multi_thread")]
async fn writes_wait_for_the_copies_their_acks_name() {
    let dir = tempfile::tempdir().unwrap();
    let ms = |d: Duration| d.as_millis();

    // all: a replica that is down drops out of sync, and the write is
    // answered; back, it catches up and is in sync again.
    let adb = create(dir.path(), "a.strata");
    let (_a, ua) = replica(adb.clone(), None).await;
    let (down, addr) = nobody().await;
    let pdb = create(dir.path(), "p.strata");
    let p = primary(pdb.clone(), None, ships_to(&[&ua, &down], Acks::All), Duration::from_secs(2));
    let w = p.write_ops(vec![set("p0000001", "price", json!(1.0))], None).await.unwrap();
    assert!(last(&adb) >= w.seq_no.unwrap());
    let f = follower(&p, 1).await;
    assert_eq!(f["in_sync"], false, "{f}");
    assert!(f["error"].as_str().unwrap().contains("Unavailable"), "{f}");
    let back = create(dir.path(), "back.strata");
    replica_on(back.clone(), None, Some(TcpListener::bind(addr).await.unwrap())).await;
    until("the replica back is in sync", || async { follower(&p, 1).await["in_sync"] == true }).await;
    assert_eq!(rows(&back), rows(&pdb));
    // A replica that does not acknowledge within the write's deadline
    // drops out of sync, and the write is answered.
    let held = stall(&adb);
    let t0 = Instant::now();
    let w = p.write_ops(vec![set("p0000001", "price", json!(2.0))], None).await.unwrap();
    assert!(t0.elapsed() >= Duration::from_secs(2), "it waited for the replica: {} ms", ms(t0.elapsed()));
    assert!(last(&back) >= w.seq_no.unwrap());
    let f = follower(&p, 0).await;
    assert!(f["in_sync"] == false && f["error"].as_str().unwrap().contains(&format!("did not acknowledge transaction {} within the write's deadline", w.seq_no.unwrap())), "{f}");
    // Out of sync, it holds up no write.
    let t0 = Instant::now();
    p.write_ops(vec![set("p0000001", "price", json!(3.0))], None).await.unwrap();
    assert!(t0.elapsed() < Duration::from_secs(1), "{} ms", ms(t0.elapsed()));
    drop(held);
    until("the stalled replica is in sync", || async { follower(&p, 0).await["in_sync"] == true }).await;
    assert_eq!(rows(&adb), rows(&pdb));

    // majority of three copies: one replica stalled is no matter.
    let (bdb, cdb) = (create(dir.path(), "b.strata"), create(dir.path(), "c.strata"));
    let ((_b, ub), (_c, uc)) = (replica(bdb.clone(), None).await, replica(cdb.clone(), None).await);
    let qdb = create(dir.path(), "q.strata");
    let q = primary(qdb.clone(), None, ships_to(&[&ub, &uc], Acks::Majority), Duration::from_secs(1));
    q.write_ops(vec![set("p0000002", "sold", json!(1))], None).await.unwrap();
    let held_b = stall(&bdb);
    let w = q.write_ops(vec![set("p0000002", "sold", json!(2))], None).await.unwrap();
    assert!(last(&cdb) >= w.seq_no.unwrap());
    // Both stalled: the write is durable on the primary, but answered 504.
    let held_c = stall(&cdb);
    let op = vec![set("p0000002", "sold", json!(3))];
    let e = q.write_ops(op.clone(), Some("k3".into())).await.unwrap_err();
    let seq = last(&qdb);
    assert!(e.status == 504 && e.message.contains(&format!("durable on the primary as sequence number {seq}, but 1 of 3 copies held it")), "{}", e.message);
    // Once their calls fail, too few copies are in sync, and a write is
    // refused before it is applied.
    until("both replicas are out of sync", || async { follower(&q, 0).await["in_sync"] == false && follower(&q, 1).await["in_sync"] == false }).await;
    let e = q.write_ops(vec![set("p0000002", "sold", json!(4))], None).await.unwrap_err();
    assert!(e.status == 503 && e.message.contains("1 of 3 copies are in sync, and --write-acks majority needs 2: the write was not applied"), "{}", e.message);
    assert_eq!(last(&qdb), seq, "the refused write took no number");
    // A retry of the write answered 504 is not a new write: it waits for
    // the copies again.
    let e = q.write_ops(op.clone(), Some("k3".into())).await.unwrap_err();
    assert!(e.status == 504 && e.message.contains(&format!("sequence number {seq},")), "{}", e.message);
    // Back: a retry of the write answered 504 answers its first answer,
    // once a majority holds it.
    drop((held_b, held_c));
    until("the replicas are in sync", || async { follower(&q, 0).await["in_sync"] == true && follower(&q, 1).await["in_sync"] == true }).await;
    let w = q.write_ops(op, Some("k3".into())).await.unwrap();
    assert_eq!(w.seq_no, Some(seq));
    assert_eq!(last(&qdb), seq, "applied once");
    assert_eq!(rows(&bdb), rows(&qdb));
    assert_eq!(rows(&cdb), rows(&qdb));

    // primary: answered at once, the replica getting it after.
    let ddb = create(dir.path(), "d.strata");
    let (_d, ud) = replica(ddb.clone(), None).await;
    let sdb = create(dir.path(), "s.strata");
    let s = primary(sdb.clone(), None, ships_to(&[&ud], Acks::Primary), Duration::from_secs(5));
    s.write_ops(vec![set("p0000005", "sold", json!(1))], None).await.unwrap();
    let held = stall(&ddb);
    let t0 = Instant::now();
    let w = s.write_ops(vec![set("p0000005", "sold", json!(2))], None).await.unwrap();
    assert!(t0.elapsed() < Duration::from_secs(1), "{} ms", ms(t0.elapsed()));
    assert!(last(&ddb) < w.seq_no.unwrap());
    drop(held);
    until("the replica gets the write", || async { last(&ddb) == w.seq_no.unwrap() }).await;
    assert_eq!(rows(&ddb), rows(&sdb));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_follows_one_primary_a_term_and_fences_an_older_one() {
    let dir = tempfile::tempdir().unwrap();
    let rdb = create(dir.path(), "r.strata");
    let file = replication::file_of(&dir.path().join("r.strata"));
    let (_r, ur) = replica(rdb.clone(), Some(&file)).await;
    let p1 = primary(create(dir.path(), "p1.strata"), None, ships_to(&[&ur], Acks::All), Duration::from_secs(5));
    let w = p1.write_ops(vec![set("p0000001", "sold", json!(1))], None).await.unwrap();
    assert_eq!(last(&rdb), w.seq_no.unwrap());
    let id1 = p1.health().await["replication"]["id"].as_str().unwrap().to_string();

    // A second primary in the same term is refused: the two would ship
    // different histories.
    let p2 = primary(create(dir.path(), "p2.strata"), None, ships_to(&[&ur], Acks::All), Duration::from_secs(5));
    p2.write_ops(vec![set("p0000002", "sold", json!(2))], None).await.unwrap();
    let f = follower(&p2, 0).await;
    assert!(f["in_sync"] == false && f["error"].as_str().unwrap().contains(&format!("follows primary {id1} in term 1")), "{f}");
    assert_eq!(last(&rdb), 1);
    // The replica keeps whom it follows: opened again, it still refuses.
    let again = Follow::open(rdb.clone(), Some(&file)).unwrap();
    assert_eq!(again.follows().await, Some(id1.clone()));
    let e = again.replicate(pb::ReplicateRequest { term: 1, prev_term: 1, prev_seq: 1, primary: "another".into(), ..Default::default() }).await.unwrap_err();
    assert_eq!(e.status, 409, "{}", e.message);

    // A replica of a newer term (as one is when promoted) fences the
    // primary of the older one, which then takes no writes.
    rdb.storage().begin_term(2).unwrap();
    until("the first primary is fenced", || async { p1.health().await["replication"]["replaced_by_term"] == 2 }).await;
    let e = p1.write_ops(vec![set("p0000001", "sold", json!(3))], None).await.unwrap_err();
    assert!(e.status == 409 && e.message.contains("a copy holds term 2, newer than this primary's (1): another primary replaced this one"), "{}", e.message);
    let f = follower(&p1, 0).await;
    assert!(f["error"].as_str().unwrap().contains("holds term 2"), "{f}");
    assert_eq!(last(&rdb), 1, "the replica applied nothing from an older term");
    // The primary of a newer term is followed, and kept.
    let a = again.replicate(pb::ReplicateRequest { term: 3, prev_term: 1, prev_seq: 1, primary: "p3".into(), ..Default::default() }).await.unwrap();
    assert!(a.matched && a.replica_term == 3, "{a:?}");
    assert_eq!(Follow::open(rdb.clone(), Some(&file)).unwrap().follows().await, Some("p3".into()));
    // An older term's primary is answered with the newer term, and
    // changes nothing.
    let a = again.replicate(pb::ReplicateRequest { term: 1, prev_term: 1, prev_seq: 1, primary: id1.clone(), ..Default::default() }).await.unwrap();
    assert!(!a.matched && a.replica_term == 3, "{a:?}");
    assert_eq!(Follow::open(rdb.clone(), Some(&file)).unwrap().follows().await, Some("p3".into()));
}

/// Copies of a database that `dir` holds beside it, which a copy leaves
/// only while it runs.
fn copies_in(dir: &Path) -> Vec<String> {
    std::fs::read_dir(dir).unwrap().map(|e| e.unwrap().file_name().to_string_lossy().into_owned()).filter(|n| n.contains(".copy-") || n.contains(".restore-")).collect()
}

/// Sends `data`, said to be `size` bytes, to `url`'s `Restore` in chunks of
/// 1,000 bytes, as primary `primary` of `term`.
async fn restore(url: &str, term: u64, primary: &str, size: u64, data: &[u8]) -> Result<pb::ReplicateResponse, tonic::Status> {
    let mut c = pb::strata_client::StrataClient::connect(url.to_string()).await.unwrap();
    let mut chunks: Vec<pb::RestoreChunk> = data.chunks(1000).map(|d| pb::RestoreChunk { data: d.to_vec(), ..Default::default() }).collect();
    if chunks.is_empty() {
        chunks.push(pb::RestoreChunk::default());
    }
    chunks[0] = pb::RestoreChunk { term, primary: primary.into(), size, ..chunks[0].clone() };
    c.restore(futures::stream::iter(chunks)).await.map(|r| r.into_inner())
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_the_log_does_not_reach_is_sent_a_copy() {
    let dir = tempfile::tempdir().unwrap();
    let [(fresh, fresh_at), (dir_layout, dir_at), (other, other_at), (restored, restored_at), (ahead, ahead_at)] =
        [nobody().await, nobody().await, nobody().await, nobody().await, nobody().await];
    // A ship log of a few transactions, and copies sent in chunks of 64 KiB.
    let pdb = create_with(dir.path(), "p.strata", 2000, |_| true);
    let opts = PrimaryOptions { copy_chunk_bytes: 64 << 10, copy_retry: Duration::from_millis(300), ..ships_to(&[&fresh, &dir_layout, &other, &restored, &ahead], Acks::All) };
    let p = primary(pdb.clone(), None, opts, Duration::from_secs(5));
    let pu = grpc(p.clone()).await;
    for i in 0..100 {
        p.write_ops(vec![set(&format!("p{:07}", i), "sold", json!(i))], None).await.unwrap();
    }
    let id = p.health().await["replication"]["id"].as_str().unwrap().to_string();

    // Replicas from before the first transaction the log holds are sent a
    // copy of the primary while it takes writes, one in each layout, and
    // then follow it.
    let fdb = create(dir.path(), "fresh.strata");
    let ddb = {
        let o = DbOptions { layout: Layout::Directory, ..options(64 << 20) };
        let db = Db::create(&dir.path().join("dir.strata"), demo::schema(N), o).unwrap();
        db.set_embedder("emb", Arc::new(strata::embed::HashEmbedder { dim: demo::DIM })).unwrap();
        db.storage().start_seq_no().unwrap();
        Arc::new(db)
    };
    let writing = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let writer = {
        let (p, writing) = (p.clone(), writing.clone());
        tokio::spawn(async move {
            let mut i = 0;
            while writing.load(std::sync::atomic::Ordering::Relaxed) {
                p.write_ops(vec![set(&format!("p{:07}", i % N), "reviews", json!(i))], None).await.unwrap();
                i += 1;
            }
            i
        })
    };
    let (fr, _) = replica_on(fdb.clone(), None, Some(TcpListener::bind(fresh_at).await.unwrap())).await;
    let (dr, _) = replica_on(ddb.clone(), None, Some(TcpListener::bind(dir_at).await.unwrap())).await;
    until("both are sent a copy", || async { follower(&p, 0).await["copies"] == 1 && follower(&p, 1).await["copies"] == 1 }).await;
    writing.store(false, std::sync::atomic::Ordering::Relaxed);
    assert!(writer.await.unwrap() > 0);
    let end = last(&pdb);
    until("both hold the primary's last", || async { follower(&p, 0).await["seq_no"] == end && follower(&p, 1).await["seq_no"] == end }).await;
    for (k, db, r) in [(0, &fdb, &fr), (1, &ddb, &dr)] {
        let f = follower(&p, k).await;
        assert!(f["in_sync"] == true && f["copying"] == false && f.get("error").is_none(), "{f}");
        assert_eq!(rows(db), rows(&pdb), "the copy holds the primary's rows, each with the primary's number");
        assert_eq!(r.health().await["replication"]["restores"], 1);
        assert_eq!(r.health().await["replication"]["serves_reads"], true, "back in sync, it serves reads");
        assert!(r.prometheus().await.contains("strata_replica_restores_total 1"));
        db.storage().check().unwrap();
    }
    assert!(p.prometheus().await.contains(&format!("strata_replica_copies_total{{replica=\"{fresh}\"}} 1")));
    // Then shipped to as any replica.
    p.write_ops(vec![set("p0000001", "sold", json!(-1))], None).await.unwrap();
    assert_eq!(last(&fdb), last(&pdb));
    assert_eq!(rows(&ddb), rows(&pdb));

    // One whose tables are not the primary's is not changed by the copy.
    let odb = {
        let schema = strata::Schema::new("products", vec![strata::Field::new("id", strata::FieldType::Keyword)], Some("id")).unwrap();
        let db = Db::create(&dir.path().join("other.strata"), schema, options(64 << 20)).unwrap();
        db.storage().start_seq_no().unwrap();
        Arc::new(db)
    };
    replica_on(odb.clone(), None, Some(TcpListener::bind(other_at).await.unwrap())).await;
    until("the copy is refused", || async { follower(&p, 2).await["error"].as_str().is_some_and(|e| e.contains("sending it a copy of the primary failed")) }).await;
    let f = follower(&p, 2).await;
    assert!(f["in_sync"] == false && f["copies"] == 0 && f["error"].as_str().unwrap().contains("the copy's tables (products) are not this database's (products), or their schemas differ"), "{f}");
    assert_eq!(odb.snapshot().n_live_docs(), 0);

    // One started from a checkpoint of the primary follows from there,
    // with no copy.
    let copy = dir.path().join("restored.strata");
    pdb.storage().checkpoint(&copy, &CheckpointOptions::default()).unwrap();
    for i in 100..103 {
        p.write_ops(vec![set(&format!("p{:07}", i), "sold", json!(i))], None).await.unwrap();
    }
    let rdb = Arc::new(Db::open(&copy, options(1 << 20)).unwrap());
    rdb.set_embedder("emb", Arc::new(strata::embed::HashEmbedder { dim: demo::DIM })).unwrap();
    replica_on(rdb.clone(), None, Some(TcpListener::bind(restored_at).await.unwrap())).await;
    until("the restored replica is in sync", || async { follower(&p, 3).await["in_sync"] == true }).await;
    assert_eq!(rows(&rdb), rows(&pdb));
    assert_eq!(follower(&p, 3).await["copies"], 0);
    // One that took writes of its own past the primary's is not a copy,
    // and is not sent one.
    let adb = create(dir.path(), "ahead.strata");
    let far = last(&pdb) + 20;
    for i in 0..far {
        let mut t = adb.storage().begin().unwrap();
        t.set(&Value::Str(format!("p{:07}", i as usize % N)), "sold", Value::Int(1)).unwrap();
        t.commit().unwrap();
    }
    replica_on(adb.clone(), None, Some(TcpListener::bind(ahead_at).await.unwrap())).await;
    until("the replica ahead is found to differ", || async { follower(&p, 4).await["error"].as_str().is_some_and(|e| e.contains("its history differs")) }).await;
    assert_eq!((last(&adb), follower(&p, 4).await["copies"].clone()), (far, json!(0)), "it applied nothing");

    // Copies refused, changing nothing: to a server that is not a replica,
    // from a primary of an older term or a second one of the term, cut
    // short, damaged, or not a database.
    let before = rows(&fdb);
    let image = {
        let to = dir.path().join("image.strata");
        pdb.storage().checkpoint(&to, &CheckpointOptions::default()).unwrap();
        std::fs::read(&to).unwrap()
    };
    let size = image.len() as u64;
    let e = restore(&pu, 1, &id, size, &image).await.unwrap_err();
    assert!(e.message().contains("not a replica"), "{e}");
    let fu = &fresh;
    let r = restore(fu, 0, "old", size, &image).await.unwrap();
    assert_eq!((r.matched, r.seq, r.replica_term), (false, last(&fdb), 1), "an older term's primary is told the replica's");
    let e = restore(fu, 1, "another", size, &image).await.unwrap_err();
    assert!(e.message().contains("a shard has one primary in a term"), "{e}");
    let e = restore(fu, 1, &id, size + 5, &image).await.unwrap_err();
    assert!(e.message().contains(&format!("the copy ended at {size} of its {} bytes", size + 5)), "{e}");
    let e = restore(fu, 1, &id, size - 5, &image).await.unwrap_err();
    assert!(e.message().contains("the copy is longer than"), "{e}");
    // A byte that only the segments' checksums find wrong, and one that
    // opening finds.
    let damaged = (1..image.len() / 4096)
        .find_map(|page| {
            let mut d = image.clone();
            d[page * 4096 + 2000] ^= 0x5a;
            let at = dir.path().join(format!("damaged-{page}.strata"));
            std::fs::write(&at, &d).unwrap();
            let clean = |verify_checksums| Database::open(&at, DbOptions { verify_checksums, ..DbOptions::default() }).is_ok_and(|db| db.damaged().is_none());
            let found = clean(false) && !clean(true);
            for p in [at.clone(), dir.path().join(format!("damaged-{page}.strata-wal"))] {
                let _ = std::fs::remove_file(p);
            }
            found.then_some(d)
        })
        .expect("a byte only the checksums find");
    let e = restore(fu, 1, &id, size, &damaged).await.unwrap_err();
    assert!(e.message().starts_with("the copy"), "{e}");
    let mut damaged = image.clone();
    for b in damaged.iter_mut().skip(100).step_by(4096) {
        *b ^= 0x5a;
    }
    let e = restore(fu, 1, &id, size, &damaged).await.unwrap_err();
    assert!(e.message().starts_with("the copy: "), "{e}");
    let e = restore(fu, 1, &id, 3, b"abc").await.unwrap_err();
    assert!(e.message().starts_with("the copy: "), "{e}");
    assert_eq!(rows(&fdb), before);
    assert_eq!(fr.health().await["replication"]["restores"], 1);
    // The copy of the primary itself is taken as the copy the primary sends.
    let r = restore(fu, 1, &id, size, &image).await.unwrap();
    assert!(r.matched && r.replica_term == 1, "{r:?}");
    assert_eq!(rows(&fdb), rows(&pdb));
    // The primary still sends `other` a copy every 300 ms, which `other`
    // refuses, so one may be on its way; a copy left behind stays.
    until("no copy is left beside the databases", || async { copies_in(dir.path()).is_empty() }).await;
}

/// Waits until a replica in `dir` holds all of the copy its primary sends
/// beside it, and is installing it.
async fn copy_arrived(dir: &Path) {
    until("the replica holds the whole copy", || async {
        // The primary writes its checkpoint under a temporary name and
        // renames it, so a file listed may be gone when it is read.
        let sizes = |what: &str| std::fs::read_dir(dir).unwrap().filter_map(Result::ok).filter(|e| e.file_name().to_string_lossy().contains(what)).filter_map(|e| e.metadata().ok()).map(|m| m.len()).collect::<Vec<_>>();
        // The copy, and the log that opening it made.
        let (sent, got) = (sizes(".copy-"), sizes(".restore-"));
        sent.len() == 1 && got.contains(&sent[0])
    })
    .await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_copy_is_followed_by_what_was_written_meanwhile() {
    let dir = tempfile::tempdir().unwrap();
    let ((near, near_at), (far, far_at)) = (nobody().await, nobody().await);
    let pdb = create_with(dir.path(), "p.strata", 2000, |_| true);
    let p = primary(pdb.clone(), None, ships_to(&[&near, &far], Acks::All), Duration::from_secs(5));
    let write = |from: usize, n: usize| {
        let p = p.clone();
        async move {
            for i in from..from + n {
                p.write_ops(vec![set(&format!("p{:07}", i % N), "sold", json!(i))], None).await.unwrap();
            }
        }
    };
    write(0, 100).await;
    // The replica's writer is held while it installs the copy, and the
    // primary takes more than its 2,000-byte log meanwhile: it holds them,
    // and the replica follows from the copy.
    let ndb = create(dir.path(), "near.strata");
    let held = stall(&ndb);
    let (nr, _) = replica_on(ndb.clone(), None, Some(TcpListener::bind(near_at).await.unwrap())).await;
    copy_arrived(dir.path()).await;
    let from = last(&pdb) + 1;
    write(100, 40).await;
    let LogRead::Txns(meanwhile) = pdb.storage().log_from(from, usize::MAX) else { panic!("the log holds what follows the copy") };
    let bytes: usize = meanwhile.iter().map(|t| t.bytes.len()).sum();
    assert!(meanwhile.len() == 40 && bytes > 2000 && bytes < 4 * 2000, "{} transactions, {bytes} bytes", meanwhile.len());
    drop(held);
    until("the replica is in sync", || async { follower(&p, 0).await["in_sync"] == true }).await;
    assert_eq!((follower(&p, 0).await["copies"].clone(), nr.health().await["replication"]["restores"].clone()), (json!(1), json!(1)), "one copy");
    assert_eq!(rows(&ndb), rows(&pdb));
    // Released once it caught up: the log keeps its 2,000 bytes again.
    write(200, 30).await;
    assert!(matches!(pdb.storage().log_from(from, usize::MAX), LogRead::Gone { .. }));

    // Past four times the log, what came meanwhile is gone, and the
    // replica is sent another copy.
    let fdb = create(dir.path(), "far.strata");
    let held = stall(&fdb);
    let (fr, _) = replica_on(fdb.clone(), None, Some(TcpListener::bind(far_at).await.unwrap())).await;
    copy_arrived(dir.path()).await;
    write(300, 200).await;
    drop(held);
    until("the replica is in sync", || async { follower(&p, 1).await["in_sync"] == true }).await;
    assert_eq!((follower(&p, 1).await["copies"].clone(), fr.health().await["replication"]["restores"].clone()), (json!(2), json!(2)), "two copies");
    assert_eq!(rows(&fdb), rows(&pdb));
    assert!(copies_in(dir.path()).is_empty());
}

/// A replica whose last transaction is not the primary's. One of an older
/// term than the primary's is of a primary this one replaced (a replaced
/// primary started again as a replica): the replica is sent a copy, which
/// drops it. One of the primary's own term is not copied: no promotion
/// explains it, and a copy would drop transactions of the primary's term.
#[tokio::test(flavor = "multi_thread")]
async fn a_replica_whose_last_transaction_is_not_the_primarys() {
    let dir = tempfile::tempdir().unwrap();
    let pdb = create(dir.path(), "p.strata");
    let (old, old_at) = nobody().await;
    let (same, same_at) = nobody().await;
    let p = primary(pdb.clone(), None, ships_to(&[&old, &same], Acks::All), Duration::from_secs(5));
    // Two transactions of term 1, and three of term 2.
    for i in 0..5 {
        if i == 2 {
            pdb.storage().begin_term(2).unwrap();
        }
        p.write_ops(vec![set(&format!("p{i:07}"), "sold", json!(i))], None).await.unwrap();
    }
    // Its third transaction is of term 1; the primary's third, of term 2.
    let odb = create(dir.path(), "o.strata");
    for i in 0..3 {
        set_on(&odb, &format!("p{i:07}"), "sold", 1);
    }
    let (o, _) = replica_on(odb.clone(), None, Some(TcpListener::bind(old_at).await.unwrap())).await;
    until("the replica of an older term is sent a copy", || async { follower(&p, 0).await["copies"] == 1 && follower(&p, 0).await["in_sync"] == true }).await;
    assert_eq!(o.health().await["replication"]["restores"], 1);
    assert_eq!(rows(&odb), rows(&pdb));
    // Its second transaction is of term 2; the primary's second, of term 1.
    let sdb = create(dir.path(), "s.strata");
    sdb.storage().begin_term(2).unwrap();
    for i in 0..2 {
        set_on(&sdb, &format!("p{i:07}"), "sold", 1);
    }
    replica_on(sdb.clone(), None, Some(TcpListener::bind(same_at).await.unwrap())).await;
    until("the replica is found to differ", || async { follower(&p, 1).await["error"].as_str().is_some_and(|e| e.contains("its last transaction, 2 of term 2, is not this primary's")) }).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(last(&sdb), 2, "it applied nothing");
    assert_eq!(follower(&p, 1).await["copies"], 0, "and was sent no copy");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_coordinator_writes_to_each_shards_primary() {
    let dir = tempfile::tempdir().unwrap();
    let key = |i: usize| Value::Str(format!("p{i:07}"));
    let mut shards = Vec::new();
    let mut copies = Vec::new();
    for s in 0..2 {
        let keep = |i: usize| shard_of(&key(i), 2) == s;
        let rdb = create_with(dir.path(), &format!("r{s}.strata"), 1 << 20, keep);
        let (_r, ur) = replica(rdb.clone(), None).await;
        let pdb = create_with(dir.path(), &format!("p{s}.strata"), 1 << 20, keep);
        let pu = grpc(primary(pdb.clone(), None, ships_to(&[&ur], Acks::All), Duration::from_secs(5))).await;
        // The replica listed first in one shard, second in the other.
        shards.push(if s == 0 { format!("{pu}|{ur}") } else { format!("{ur}|{pu}") });
        copies.push((pdb, rdb));
    }
    let c = Coordinator::connect(shards.clone()).await.unwrap();
    let schema = c.schema.clone();
    let coord = Arc::new(Service::new(Backend::Cluster(Arc::new(c)), schema, agent(), 16, Duration::from_secs(10)));
    let ops: Vec<Json> = (0..20).map(|i| set(&format!("p{i:07}"), "sold", json!(100 + i))).collect();
    assert_eq!(coord.write_ops(ops, None).await.unwrap().rows, 20);
    written(coord.sql("UPDATE products SET reviews = reviews + 1 WHERE category = 'kitchen'", &Params::new(), After::Any, 0).await.unwrap());
    for (pdb, rdb) in &copies {
        assert!((1..=2).contains(&last(pdb)), "each primary took the writes once: {}", last(pdb));
        assert_eq!(rows(rdb), rows(pdb), "each replica holds its primary's rows");
    }
    // seq_no() and if_seq_no pass through: the copies of a shard number
    // alike.
    let seq_of = |id: &'static str| {
        let coord = coord.clone();
        async move { coord.query(QueryReq { sql: format!("SELECT seq_no() FROM products WHERE id = '{id}'"), ..Default::default() }).await.unwrap().rs.rows[0][0].as_i64().unwrap() as u64 }
    };
    let n = seq_of("p0000003").await;
    assert!(n > 0);
    let cas = |n: u64| vec![json!({"op": "add", "key": "p0000003", "field": "sold", "value": 1, "if_seq_no": n})];
    coord.write_ops(cas(n), None).await.unwrap();
    let e = coord.write_ops(cas(n), None).await.unwrap_err();
    assert_eq!(e.status, 409, "{}", e.message);
    let h = coord.health().await;
    let roles: Vec<Json> = (0..2).map(|i| h["shards"][i]["replicas"].as_array().unwrap().iter().map(|r| r["role"].clone()).collect()).collect();
    assert_eq!(roles, [json!(["primary", "replica"]), json!(["replica", "primary"])], "{h}");

    // A shard with replicas and no primary answering takes no writes. (A
    // coordinator given only the replicas above would route by the cluster
    // map they hold, which names the primaries.)
    let mut lone = Vec::new();
    for s in 0..2 {
        let db = create_with(dir.path(), &format!("lone{s}.strata"), 1 << 20, |i| shard_of(&key(i), 2) == s);
        lone.push(replica(db, None).await.1);
    }
    let c = Coordinator::connect(lone).await.unwrap();
    let e = c.write_ops(vec![set("p0000003", "sold", json!(1))], None).await.unwrap_err();
    assert!(e.status == 503 && e.message.contains("has no primary answering"), "{}", e.message);
}

/// Product 3's `sold` as `svc` reads it, seeing the writes `after` names.
async fn sold_3(svc: &Arc<Service>, after: After, timeout_ms: u64) -> Result<i64, strata_server::ApiError> {
    let a = svc.query(QueryReq { sql: "SELECT sold FROM products WHERE id = 'p0000003'".into(), after, timeout_ms, ..Default::default() }).await?;
    Ok(a.rs.rows[0][0].as_i64().unwrap())
}

/// A replica serves reads once its primary's call reached the primary's
/// last transaction, and refuses them while it catches up. A read that
/// names a write's sequence number sees it: a replica waits until it has
/// applied it, a second at most, and a primary, or a lease from before,
/// refuses a number it does not hold.
#[tokio::test(flavor = "multi_thread")]
async fn a_replica_serves_reads_once_caught_up_and_waits_for_a_write() {
    let dir = tempfile::tempdir().unwrap();
    let (pdb, rdb) = (create(dir.path(), "p.strata"), create(dir.path(), "r.strata"));
    // One query slot, which a read waiting for a write does not hold.
    let mut r = Service::new(Backend::Local(rdb.clone()), rdb.schema().clone(), agent(), 1, Duration::from_secs(10));
    r.set_replication(Replication::Replica(Arc::new(Follow::open(rdb.clone(), None).unwrap())));
    let r = Arc::new(r);
    let ur = grpc(r.clone()).await;
    // Before its primary calls, it serves no reads.
    let e = sold_3(&r, After::Any, 0).await.unwrap_err();
    assert!(e.status == 503 && e.message.contains("its primary has not called since it started"), "{}", e.message);
    assert_eq!(r.lease().unwrap_err().status, 503);
    let stats = |svc: &Arc<Service>, after: After| {
        let svc = svc.clone();
        async move { svc.term_stats("SELECT id FROM products ORDER BY bm25(body, 'kettle') DESC LIMIT 3", None, &Params::new(), &after, 0).await }
    };
    assert_eq!(stats(&r, After::Any).await.unwrap_err().status, 503);
    let p = primary(pdb.clone(), None, ships_to(&[&ur], Acks::Primary), Duration::from_secs(5));
    until("the replica serves reads", || async { r.health().await["replication"]["serves_reads"] == true }).await;
    let sold = sold_3(&p, After::Any, 0).await.unwrap();
    assert_eq!(sold_3(&r, After::Any, 0).await.unwrap(), sold);

    // A write the replica has not applied: a read that names it waits.
    let hold = stall(&rdb);
    let n = p.write_ops(vec![set("p0000003", "sold", json!(1001))], None).await.unwrap().seq_no.unwrap();
    assert_eq!(sold_3(&r, After::Any, 0).await.unwrap(), sold, "a read that names no write reads what the replica holds");
    let waiting = tokio::spawn({
        let r = r.clone();
        async move { sold_3(&r, After::Seq(n), 0).await }
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(sold_3(&r, After::Any, 0).await.unwrap(), sold, "the replica's one query slot serves a read meanwhile");
    assert!(!waiting.is_finished(), "it waits for transaction {n}");
    drop(hold);
    assert_eq!(waiting.await.unwrap().unwrap(), 1001);

    // One it does not apply in time is refused: the primary holds it.
    let hold = stall(&rdb);
    let m = p.write_ops(vec![set("p0000003", "sold", json!(1002))], None).await.unwrap().seq_no.unwrap();
    let t0 = Instant::now();
    let e = sold_3(&r, After::Seq(m), 0).await.unwrap_err();
    assert!(e.status == 503 && e.message.contains(&format!("must see transaction {m}, which it has not applied in time: read from the primary")), "{}", e.message);
    assert!((replication::READ_WAIT..Duration::from_secs(3)).contains(&t0.elapsed()), "{:?}", t0.elapsed());
    // Within the read's deadline when that comes first.
    let t0 = Instant::now();
    assert_eq!(sold_3(&r, After::Seq(m), 200).await.unwrap_err().status, 503);
    assert!(t0.elapsed() < Duration::from_millis(800), "{:?}", t0.elapsed());
    assert_eq!(stats(&r, After::Seq(m)).await.unwrap_err().status, 503, "statistics wait for it too");
    // Over gRPC as well, as a coordinator asks.
    let mut g = pb::strata_client::StrataClient::connect(ur.clone()).await.unwrap();
    let ask = pb::QueryRequest { sql: "SELECT id FROM products ORDER BY bm25(body, 'kettle') DESC LIMIT 3".into(), protocol: 5, min_seq_no: m, timeout_ms: 200, ..Default::default() };
    assert_eq!(g.term_stats(ask.clone()).await.unwrap_err().code(), tonic::Code::Unavailable);
    assert_eq!(g.query(ask).await.unwrap_err().code(), tonic::Code::Unavailable);
    assert_eq!(sold_3(&p, After::Seq(m), 0).await.unwrap(), 1002);
    drop(hold);
    assert_eq!(sold_3(&r, After::Seq(m), 0).await.unwrap(), 1002);
    assert!(stats(&r, After::Seq(m)).await.is_ok());

    // A primary holds every write it answered, so a number past its last
    // was not committed there: refused, by every kind of read.
    let past = After::Seq(m + 10);
    let e = sold_3(&p, past.clone(), 0).await.unwrap_err();
    assert!(e.status == 409 && e.message.contains(&format!("transaction {} was not committed here", m + 10)), "{}", e.message);
    assert_eq!(stats(&p, past.clone()).await.unwrap_err().status, 409);
    let compound = "SELECT id FROM products WHERE id IN (SELECT id FROM products WHERE sold > 1000)";
    assert_eq!(p.query(QueryReq { sql: compound.into(), after: past.clone(), ..Default::default() }).await.err().unwrap().status, 409);
    let plan = strata::query::ir::Plan::bind("SELECT sold FROM products WHERE id = 'p0000003'", &p.schema, &Params::new()).unwrap();
    assert!(matches!(p.run_plan(plan.clone(), past.clone(), 0).await, Err(e) if e.status == 409));
    assert!(matches!(p.sql("SELECT sold FROM products WHERE id = 'p0000003'", &Params::new(), past.clone(), 0).await, Err(e) if e.status == 409));
    match r.run_plan(plan, After::Seq(m), 0).await.unwrap() {
        SqlAnswer::Rows(a) => assert_eq!(a.rs.rows[0][0], Value::Int(1002)),
        SqlAnswer::Written { .. } => panic!("a read"),
    }
    // A write sees every write before it, as SQL or as a plan.
    let update = "UPDATE products SET sold = 0 WHERE id = 'p0000003'";
    let e = p.sql(update, &Params::new(), After::Seq(m), 0).await.err().unwrap();
    assert!(e.status == 400 && e.message.contains("are for reads"), "{}", e.message);
    let strata::query::Command::Write { write, .. } = strata::query::bind_command(update, &[&p.schema], &Params::new()).unwrap() else { panic!() };
    let e = p.run_plan(strata::query::ir::Plan::of_write(&p.schema, &write), After::Seq(m), 0).await.err().unwrap();
    assert!(e.status == 400 && e.message.contains("are for reads"), "{}", e.message);
    // A lease keeps the version it pinned.
    let (lease, _) = r.lease().unwrap();
    let k = p.write_ops(vec![set("p0000003", "sold", json!(1003))], None).await.unwrap().seq_no.unwrap();
    until("the replica applied it", || async { last(&rdb) >= k }).await;
    let e = r.query(QueryReq { sql: "SELECT sold FROM products".into(), snapshot: lease, after: After::Seq(k), ..Default::default() }).await.err().unwrap();
    assert!(e.status == 409 && e.message.contains(&format!("keeps a version holding transactions up to {m}, before {k}")), "{}", e.message);
    // A server's read takes its own number, not a coordinator's.
    let e = sold_3(&r, After::Shards(vec![k]), 0).await.unwrap_err();
    assert!(e.status == 400 && e.message.contains("min_seq_nos is a coordinator's"), "{}", e.message);
    assert!(After::of(k, vec![k]).unwrap_err().message.contains("not both"));
    let plain = Db::create(&dir.path().join("plain.strata"), demo::schema(N), options(0)).unwrap();
    plain.load((0..N).map(|i| demo::product(i, 7))).unwrap();
    let e = sold_3(&Arc::new(service(Arc::new(plain), Duration::from_secs(5))), After::Seq(1), 0).await.unwrap_err();
    assert!(e.status == 400 && e.message.contains("start the server with --seq-no"), "{}", e.message);

    // A call that finds it behind stops its reads until one reaches the
    // primary's last: as when it catches up after a restart.
    let id = p.health().await["replication"]["id"].as_str().unwrap().to_string();
    drop(p);
    // The primary's shipping stops at its next turn.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut c = pb::strata_client::StrataClient::connect(ur.clone()).await.unwrap();
    let at = last(&rdb);
    let call = |prev_seq: u64, behind: bool| pb::ReplicateRequest { term: 1, prev_term: 1, prev_seq, primary: id.clone(), behind, ..Default::default() };
    assert!(c.replicate(call(at, true)).await.unwrap().into_inner().matched);
    let e = sold_3(&r, After::Any, 0).await.unwrap_err();
    assert!(e.status == 503 && e.message.contains("it is catching up with its primary"), "{}", e.message);
    assert_eq!(r.health().await["replication"]["serves_reads"], false);
    assert!(c.replicate(call(at, false)).await.unwrap().into_inner().matched);
    assert_eq!(sold_3(&r, After::Any, 0).await.unwrap(), 1003);
    // So does a copy: the primary wrote on while it sent it.
    let image = {
        let to = dir.path().join("image.strata");
        pdb.storage().checkpoint(&to, &CheckpointOptions::default()).unwrap();
        std::fs::read(&to).unwrap()
    };
    assert!(restore(&ur, 1, &id, image.len() as u64, &image).await.unwrap().matched);
    assert_eq!(sold_3(&r, After::Any, 0).await.unwrap_err().status, 503);
    assert!(c.replicate(call(at, false)).await.unwrap().into_inner().matched);
    assert_eq!(sold_3(&r, After::Any, 0).await.unwrap(), 1003);
    // A call that does not follow its last finds it behind as well.
    assert!(!c.replicate(call(at - 1, false)).await.unwrap().into_inner().matched);
    assert_eq!(sold_3(&r, After::Any, 0).await.unwrap_err().status, 503);
    let h = r.health().await["replication"].clone();
    assert!(h["read_waits"].as_u64().unwrap() >= 4 && h["reads_refused"].as_u64().unwrap() >= 8, "{h}");
    let m = r.prometheus().await;
    assert!(m.contains("strata_replica_serves_reads 0") && m.contains("strata_replica_read_waits_total") && m.contains("strata_replica_reads_refused_total"), "{m}");
}

/// A coordinator's write answers each shard's sequence number, and a read
/// that passes them on sees the write on whichever copy answers: a replica
/// waits for it, or refuses it, and the shard's primary is asked next.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_through_a_coordinator_sees_the_write_it_names() {
    let dir = tempfile::tempdir().unwrap();
    let key = |i: usize| Value::Str(format!("p{i:07}"));
    let mut shards = Vec::new();
    let mut replicas = Vec::new();
    for s in 0..2 {
        let keep = |i: usize| shard_of(&key(i), 2) == s;
        let mut urls = Vec::new();
        for k in 0..2 {
            let rdb = create_with(dir.path(), &format!("r{s}{k}.strata"), 1 << 20, keep);
            let (r, ur) = replica(rdb.clone(), None).await;
            urls.push(ur);
            replicas.push((r, rdb));
        }
        let pdb = create_with(dir.path(), &format!("p{s}.strata"), 1 << 20, keep);
        let pu = grpc(primary(pdb, None, ships_to(&[&urls[0], &urls[1]], Acks::Primary), Duration::from_secs(5))).await;
        for (r, _) in &replicas[2 * s..] {
            until("the replica serves reads", || async { r.health().await["replication"]["serves_reads"] == true }).await;
        }
        // Two replicas, then the primary: reads start at a replica twice
        // as often as at the primary.
        shards.push(format!("{}|{}|{pu}", urls[0], urls[1]));
    }
    // No hedges, so a read waits for the replica it asked, and a replica
    // that refused one is not asked last for a while: each read starts at
    // the copy its turn names.
    let opts = strata_server::coord::CoordOptions { hedge: Duration::from_secs(60), cool_off: Duration::ZERO, ..Default::default() };
    let c = Coordinator::connect_with(shards, opts).await.unwrap();
    let schema = c.schema.clone();
    let coord = Arc::new(Service::new(Backend::Cluster(Arc::new(c)), schema, agent(), 16, Duration::from_secs(10)));
    let both: Vec<String> = (0..20).map(|i| format!("p{i:07}")).collect();
    // The rows whose `sold` is `v`, as a read that names `after` sees them.
    let read = |after: After, v: i64| {
        let coord = coord.clone();
        async move {
            let sql = format!("SELECT id FROM products WHERE sold = {v} LIMIT 100");
            let a = coord.query(QueryReq { sql, after, ..Default::default() }).await?;
            let mut ids: Vec<String> = a.rs.rows.iter().map(|r| r[0].as_str().unwrap().to_string()).collect();
            ids.sort();
            Ok::<Vec<String>, strata_server::ApiError>(ids)
        }
    };
    let w = coord.write_ops(both.iter().map(|k| set(k, "sold", json!(900_001))).collect(), None).await.unwrap();
    assert!(w.seq_no.is_none() && w.seq_nos.len() == 2 && w.seq_nos.iter().all(|&s| s > 0), "{w:?}");
    assert_eq!(read(After::Shards(w.seq_nos.clone()), 900_001).await.unwrap(), both);

    // No replica can apply the next write: every read that names it sees
    // it, from the primary when a replica refuses it after a second. The
    // primary is asked next, not the other replica, which would refuse it
    // too, a second later. (A replica whose writer stays held for 5 s
    // fails the batch and stops serving reads.)
    let holds: Vec<_> = replicas.iter().map(|(_, rdb)| stall(rdb)).collect();
    let w = coord.write_ops(both.iter().map(|k| set(k, "sold", json!(900_002))).collect(), None).await.unwrap();
    // A ranked read asks the shards' statistics first, on the snapshot its
    // query then reads: both see the write.
    for _ in 0..3 {
        let q = QueryReq { sql: "SELECT id, sold FROM products ORDER BY bm25(body, 'built') DESC LIMIT 300".into(), after: After::Shards(w.seq_nos.clone()), ..Default::default() };
        let t0 = Instant::now();
        let a = coord.query(q).await.unwrap();
        assert!(t0.elapsed() < Duration::from_millis(1800), "{:?}", t0.elapsed());
        let ours: Vec<_> = a.rs.rows.iter().filter(|r| both.iter().any(|k| r[0].as_str() == Some(k.as_str()))).collect();
        assert!(ours.len() == both.len() && ours.iter().all(|r| r[1] == Value::Int(900_002)), "{ours:?}");
    }
    for _ in 0..3 {
        let t0 = Instant::now();
        assert_eq!(read(After::Shards(w.seq_nos.clone()), 900_002).await.unwrap(), both);
        assert!(t0.elapsed() < Duration::from_millis(1800), "{:?}", t0.elapsed());
    }
    for (r, _) in &replicas {
        let h = r.health().await["replication"].clone();
        assert!(h["reads_refused"].as_u64().unwrap() >= 1, "each replica was asked first once, and refused: {h}");
    }
    drop(holds);
    // An UPDATE goes to every shard, and answers each shard's number.
    let w = written_all(coord.sql("UPDATE products SET sold = 900003 WHERE id = 'p0000003' OR id = 'p0000004'", &Params::new(), After::Any, 0).await.unwrap());
    assert!(w.seq_nos.len() == 2 && w.rows == 2, "{w:?}");
    assert_eq!(read(After::Shards(w.seq_nos), 900_003).await.unwrap(), ["p0000003", "p0000004"]);

    // A coordinator takes one number per shard.
    let e = read(After::Seq(1), 0).await.unwrap_err();
    assert!(e.status == 400 && e.message.contains("a coordinator takes min_seq_nos"), "{}", e.message);
    let e = read(After::Shards(vec![1, 1, 1]), 0).await.unwrap_err();
    assert!(e.status == 400 && e.message.contains("min_seq_nos has 3 numbers, and the coordinator 2 shards"), "{}", e.message);
    // Past a primary's last: never committed there.
    let e = read(After::Shards(vec![0, 1_000_000]), 0).await.unwrap_err();
    assert!(e.status == 409 && e.message.contains("was not committed here"), "{}", e.message);
}

fn written_all(a: SqlAnswer) -> strata_server::service::Written {
    match a {
        SqlAnswer::Written { w, .. } => w,
        SqlAnswer::Rows(_) => panic!("not a write"),
    }
}

/// Sets INT `field` of the row with key `key` in a transaction of its own.
fn set_on(db: &Db, key: &str, field: &str, v: i64) {
    let mut tx = db.storage().begin().unwrap();
    assert!(tx.set(&Value::Str(key.into()), field, Value::Int(v)).unwrap());
    tx.commit_seq().unwrap();
}

fn slot_of(key: &str) -> usize {
    slots::slot(&Value::Str(key.into()))
}

/// The digests of `db`'s latest version.
fn digests(db: &Db) -> Digests {
    Digests::of(&db.snapshot()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn digests_cover_each_rows_table_key_and_number() {
    let dir = tempfile::tempdir().unwrap();
    let keyed = |name: &str| Schema::new(name, vec![Field::new("id", FieldType::Keyword)], Some("id")).unwrap();
    let keyless = Schema::new("log", vec![Field::new("line", FieldType::Keyword)], None).unwrap();
    let dbs: Vec<Arc<Db>> = ["a", "b"]
        .iter()
        .map(|name| {
            let db = create(dir.path(), &format!("{name}.strata"));
            for s in [keyed("x"), keyed("y"), keyless.clone()] {
                db.create_table(s).unwrap();
            }
            db
        })
        .collect();
    let (a, b) = (&dbs[0], &dbs[1]);
    let d = digests(a);
    assert_eq!(d, digests(b), "the same rows");
    assert_eq!((d.rows.len(), d.sums.len(), d.rows.iter().sum::<u64>()), (slots::SLOTS, slots::SLOTS, N as u64));
    let mut slots_of: Vec<usize> = (0..N).map(|i| slot_of(&format!("p{i:07}"))).collect();
    slots_of.sort();
    slots_of.dedup();
    assert_eq!(d.rows.iter().enumerate().filter(|(_, &n)| n > 0).map(|(i, _)| i).collect::<Vec<_>>(), slots_of, "each row in its key's slot");

    // The same writes in another order: the rows differ in their numbers.
    set_on(a, "p0000001", "sold", 5);
    set_on(a, "p0000002", "sold", 6);
    set_on(b, "p0000002", "sold", 6);
    set_on(b, "p0000001", "sold", 5);
    let mut want = vec![slot_of("p0000001"), slot_of("p0000002")];
    want.sort();
    want.dedup();
    assert_eq!(digests(a).differ(&digests(b)), want);
    // The same numbers with other values: the digests do not see values.
    set_on(a, "p0000001", "sold", 7);
    set_on(b, "p0000001", "sold", 8);
    set_on(a, "p0000002", "sold", 7);
    set_on(b, "p0000002", "sold", 8);
    assert_eq!(digests(a), digests(b), "the last write of each row has the same number");
    // A row missing, a row of another table under the same key and
    // number, and a row of a table without a primary key.
    let mut tx = a.storage().begin().unwrap();
    assert!(tx.delete(&Value::Str("p0000003".into())).unwrap());
    tx.commit_seq().unwrap();
    set_on(b, "p0000004", "sold", 1);
    for (db, table) in [(a, "x"), (b, "y")] {
        let mut tx = db.storage().begin().unwrap();
        tx.table(table).unwrap().insert(vec![Value::Str("k".into())]).unwrap();
        tx.commit_seq().unwrap();
    }
    let mut tx = a.storage().begin().unwrap();
    tx.table("log").unwrap().insert(vec![Value::Str("a line".into())]).unwrap();
    tx.commit_seq().unwrap();
    let (da, db) = (digests(a), digests(b));
    let differ = da.differ(&db);
    for (what, slot) in [("a missing row", slot_of("p0000003")), ("a row's number", slot_of("p0000004")), ("the table a row is in", slot_of("k"))] {
        assert!(differ.contains(&slot), "{what}: {differ:?}");
    }
    assert_eq!(da.rows.iter().sum::<u64>(), N as u64 - 1 + 2, "the keyless table's row counts");
    assert_eq!(differ.len(), 4, "{differ:?}");
    // The answer of a replica must hold every slot.
    assert!(Digests::from_pb(pb::VerifyResponse { rows: vec![0; 3], sums: vec![0; 3] }).unwrap_err().contains("not 4096"));
    assert_eq!(Digests::from_pb(da.to_pb()).unwrap(), da);
}

/// The primary's view of replica `k` as `/metrics` counts it.
async fn counted(p: &Arc<Service>, name: &str, url: &str) -> String {
    let m = p.prometheus().await;
    let line = format!("{name}{{replica=\"{url}\"}} ");
    m.lines().find_map(|l| l.strip_prefix(&line)).unwrap_or_else(|| panic!("no {line} in {m}")).to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_replica_whose_rows_differ_is_sent_a_copy() {
    let dir = tempfile::tempdir().unwrap();
    let pdb = create(dir.path(), "p.strata");
    let same_db = create(dir.path(), "same.strata");
    // Loaded without p0000007, as a replica started from other rows is.
    let short_db = create_with(dir.path(), "short.strata", 64 << 20, |i| i != 7);
    let (same, su) = replica(same_db.clone(), None).await;
    let (short, hu) = replica(short_db.clone(), None).await;
    let opts = PrimaryOptions { verify_every: Duration::ZERO, ..ships_to(&[&su, &hu], Acks::All) };
    let p = primary(pdb.clone(), None, opts, Duration::from_secs(5));
    for i in 10..30 {
        p.write_ops(vec![set(&format!("p{i:07}"), "sold", json!(i))], None).await.unwrap();
    }
    // Shipping alone does not find it: every write fits its rows.
    let h = p.health().await["replication"].clone();
    assert_eq!((&h["replicas"][0]["in_sync"], &h["replicas"][1]["in_sync"], &h["replicas"][1]["seq_no"]), (&json!(true), &json!(true), &json!(20)), "{h}");
    assert!(h["replicas"][0].get("verified").is_none(), "not compared yet: {h}");

    let v = p.verify_replicas(Duration::from_secs(10)).await.unwrap();
    let (s, d) = (&v["replicas"][0], &v["replicas"][1]);
    assert_eq!((&s["compared"], &s["seq_no"], &s["differing_slots"], &s["in_sync"]), (&json!(true), &json!(20), &json!(0), &json!(true)), "{v}");
    assert_eq!((&d["compared"], &d["seq_no"], &d["differing_slots"], &d["first_differing_slot"]), (&json!(true), &json!(20), &json!(1), &json!(slot_of("p0000007"))), "{v}");
    // It drops out, and is sent a copy.
    until("the short replica installs a copy and catches up", || async { follower(&p, 1).await["copies"] == 1 && follower(&p, 1).await["in_sync"] == true }).await;
    assert_eq!(rows(&short_db), rows(&pdb));
    assert_eq!(rows(&same_db), rows(&pdb));
    let f = follower(&p, 1).await;
    assert_eq!((&f["verifications"], &f["divergences"], &f["verified"]["differing_slots"]), (&json!(1), &json!(1), &json!(1)), "{f}");
    assert_eq!(short.health().await["replication"]["restores"], 1);

    // Compared again, after more writes: both hold the primary's rows.
    for i in 30..40 {
        p.write_ops(vec![set(&format!("p{i:07}"), "sold", json!(i))], None).await.unwrap();
    }
    let v = p.verify_replicas(Duration::from_secs(10)).await.unwrap();
    for k in 0..2 {
        let r = &v["replicas"][k];
        assert_eq!((&r["compared"], &r["seq_no"], &r["differing_slots"]), (&json!(true), &json!(last(&pdb)), &json!(0)), "{v}");
    }
    assert_eq!(follower(&p, 0).await["copies"], 0, "the same rows: no copy");
    for (url, verifications, divergences, copies) in [(&su, "2", "0", "0"), (&hu, "2", "1", "1")] {
        assert_eq!(counted(&p, "strata_replica_verifications_total", url).await, verifications);
        assert_eq!(counted(&p, "strata_replica_divergences_total", url).await, divergences);
        assert_eq!(counted(&p, "strata_replica_copies_total", url).await, copies);
    }
    for r in [&same, &short] {
        assert_eq!(r.health().await["replication"]["digests"], 2);
        assert!(r.prometheus().await.contains("strata_replica_digests_total 2"));
    }

    // A replica answers the primary it follows, at the version pinned.
    let id = p.health().await["replication"]["id"].as_str().unwrap().to_string();
    let mut c = pb::strata_client::StrataClient::connect(su.clone()).await.unwrap();
    let ask = |primary: &str, term: u64, seq_no: u64| pb::VerifyRequest { term, primary: primary.into(), seq_no };
    for (req, says) in [
        (ask("someone", 1, last(&pdb)), "this replica follows primary"),
        (ask(&id, 2, last(&pdb)), "not primary"),
        (ask(&id, 1, last(&pdb)), "holds no version pinned at transaction"),
    ] {
        let e = c.verify(req).await.unwrap_err();
        assert_eq!(e.code(), tonic::Code::FailedPrecondition, "{e:?}");
        assert!(e.message().contains(says), "{says}: {}", e.message());
    }
    let mut pc = pb::strata_client::StrataClient::connect(grpc(p.clone()).await).await.unwrap();
    assert!(pc.verify(ask(&id, 1, 1)).await.unwrap_err().message().contains("not a replica"));
    assert_eq!(same.verify_replicas(Duration::from_secs(1)).await.unwrap_err().status, 409);
    // A replica being sent a copy serves no reads until it is caught up.
    assert_eq!(same.health().await["replication"]["serves_reads"], true);
    assert!(restore(&su, 1, &id, 10, &[1, 2, 3]).await.unwrap_err().message().contains("ended at 3 of its 10 bytes"));
    assert_eq!(same.health().await["replication"]["serves_reads"], false);
    until("the next call finds it caught up", || async { same.health().await["replication"]["serves_reads"] == true }).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn replicas_are_compared_while_writes_go_on() {
    let dir = tempfile::tempdir().unwrap();
    // The primary lacks p0000009, and so do its replicas, but for one.
    let pdb = create_with(dir.path(), "p.strata", 64 << 20, |i| i != 9);
    let r0db = create_with(dir.path(), "r0.strata", 64 << 20, |i| i != 9);
    let r1db = create(dir.path(), "r1.strata");
    let (_r0, u0) = replica(r0db.clone(), None).await;
    let (_r1, u1) = replica(r1db.clone(), None).await;
    // Compared every 100 ms, with shipping in batches of a few
    // transactions, so that a round's version falls inside a batch.
    let opts = PrimaryOptions { verify_every: Duration::from_millis(100), batch_bytes: 500, ..ships_to(&[&u0, &u1], Acks::Primary) };
    let p = primary(pdb.clone(), None, opts, Duration::from_secs(5));
    // Found without asking, and sent a copy.
    until("the replica with a row too many is sent a copy", || async { follower(&p, 1).await["copies"] == 1 && follower(&p, 1).await["in_sync"] == true }).await;
    assert_eq!(rows(&r1db), rows(&pdb));
    assert_eq!(follower(&p, 1).await["divergences"], 1);

    // Writers at once, while rounds compare every 100 ms and when asked:
    // each round compares one version on both sides.
    let writing = Arc::new(std::sync::atomic::AtomicBool::new(true));
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let (p, writing) = (p.clone(), writing.clone());
            tokio::spawn(async move {
                let mut i = 0;
                while writing.load(std::sync::atomic::Ordering::Relaxed) {
                    let key = format!("p{:07}", (w * 50 + i) % N);
                    if key != "p0000009" {
                        p.write_ops(vec![set(&key, "reviews", json!(i))], None).await.unwrap();
                    }
                    i += 1;
                }
            })
        })
        .collect();
    let before = [follower(&p, 0).await["verifications"].as_u64().unwrap(), follower(&p, 1).await["verifications"].as_u64().unwrap()];
    let mut at = 0;
    for _ in 0..8 {
        tokio::time::sleep(Duration::from_millis(50)).await;
        // A round compares the primary's last write when it starts, so one
        // committed since the last round makes its version newer, however
        // long commits take meanwhile (a slow fsync holds them all).
        until("a write after the last round's version", || async { last(&pdb) > at }).await;
        let v = p.verify_replicas(Duration::from_secs(10)).await.unwrap();
        for k in 0..2 {
            let r = &v["replicas"][k];
            assert_eq!((&r["compared"], &r["differing_slots"]), (&json!(true), &json!(0)), "{v}");
            assert!(r["seq_no"].as_u64().unwrap() > at, "a newer version each round: {v}");
        }
        at = v["replicas"][0]["seq_no"].as_u64().unwrap().min(v["replicas"][1]["seq_no"].as_u64().unwrap());
    }
    // And every 100 ms without asking.
    let asked = [follower(&p, 0).await["verifications"].as_u64().unwrap(), follower(&p, 1).await["verifications"].as_u64().unwrap()];
    tokio::time::sleep(Duration::from_millis(600)).await;
    writing.store(false, std::sync::atomic::Ordering::Relaxed);
    for w in writers {
        w.await.unwrap();
    }
    for k in 0..2 {
        let f = follower(&p, k).await;
        assert!(asked[k] >= before[k] + 8 && f["verifications"].as_u64().unwrap() >= asked[k] + 3, "{before:?} {asked:?} {f}");
        assert_eq!((&f["divergences"], &f["copies"]), (&json!(k), &json!(k)), "no other: {f}");
    }
    let end = last(&pdb);
    until("both hold the primary's last", || async { follower(&p, 0).await["seq_no"] == end && follower(&p, 1).await["seq_no"] == end }).await;
    assert_eq!(rows(&r0db), rows(&pdb));
    assert_eq!(rows(&r1db), rows(&pdb));
}

#[tokio::test(flavor = "multi_thread")]
async fn the_server_starts_as_a_primary_or_a_replica() {
    let dir = tempfile::tempdir().unwrap();
    let opts = |name: &str| server::Options { db: Some(dir.path().join(name)), demo: Some(20), seq_no: true, timeout_ms: 5000, ..Default::default() };
    let r = server::service(&server::Options { replica: true, verify_every_s: 1800, ..opts("r.strata") }).await.unwrap();
    assert_eq!(r.role(), pb::Role::Replica);
    assert!(replication::file_of(&dir.path().join("r.strata")).exists(), "a replica keeps its id");
    let ur = grpc(r.clone()).await;
    let p = server::service(&server::Options { replicas: vec![ur], write_acks: Acks::Majority, verify_every_s: 3600, ..opts("p.strata") }).await.unwrap();
    assert_eq!(p.role(), pb::Role::Primary);
    assert!(p.local_db().unwrap().storage().keeps_ship_log());
    let w = p.write_ops(vec![set("p0000001", "sold", json!(7))], None).await.unwrap();
    assert_eq!(w.seq_no, Some(1));
    // With a majority of two copies, the replica holds it.
    let (pdb, rdb) = (p.local_db().unwrap(), r.local_db().unwrap());
    assert_eq!(last(rdb), 1);
    assert_eq!(rows(rdb), rows(pdb), "both loaded the same demo rows, and the write shipped");
    assert_eq!(p.health().await["replication"]["write_acks"], "majority");
    assert_eq!(p.health().await["replication"]["verify_every_s"], 3600);
    // Compared when asked over HTTP: both loaded the same rows.
    let http = |svc: Arc<strata_server::service::Service>| async move {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(server::serve_http(svc, l, Access::default()));
        url
    };
    let c = reqwest::Client::new();
    for body in [None, Some(json!({"timeout_ms": 5000}))] {
        let mut req = c.post(format!("{}/v1/admin/verify", http(p.clone()).await));
        if let Some(b) = &body {
            req = req.json(b);
        }
        let r = req.send().await.unwrap();
        assert_eq!(r.status(), 200);
        let v: Json = r.json().await.unwrap();
        assert_eq!((&v["replicas"][0]["compared"], &v["replicas"][0]["seq_no"], &v["replicas"][0]["differing_slots"]), (&json!(true), &json!(1), &json!(0)), "{v}");
    }
    let a = c.post(format!("{}/v1/admin/verify", http(r.clone()).await)).json(&json!({})).send().await.unwrap();
    assert_eq!(a.status(), 409);

    // Promoted over HTTP, as `strata-server promote` asks: the old primary,
    // still up, ships to it and is fenced.
    let ru = http(r.clone()).await;
    for (body, says) in [
        (json!({"replicas": "x"}), "replicas is a list"),
        (json!({"replicas": [1]}), "replicas is a list"),
        (json!({"write_acks": "most"}), "write_acks is all, majority or primary"),
        (json!({"force": "yes"}), "force is true or false"),
    ] {
        let a = c.post(format!("{ru}/v1/admin/promote")).json(&body).send().await.unwrap();
        assert_eq!(a.status(), 400, "{body}");
        assert!(a.json::<Json>().await.unwrap()["error"].as_str().unwrap().contains(says), "{body}");
    }
    let e = server::promote(&http(p.clone()).await, &[], "all", false, None).await.unwrap_err();
    assert!(e.contains("409 Conflict: this server is not a replica"), "{e}");
    // A copy holds two transactions another replica does not: refused,
    // unless forced.
    let x = server::service(&opts("x.strata")).await.unwrap();
    for v in [1, 2] {
        x.write_ops(vec![set("p0000001", "sold", json!(v))], None).await.unwrap();
    }
    let ux = grpc(x).await;
    let r2 = http(server::service(&server::Options { replica: true, ..opts("r2.strata") }).await.unwrap()).await;
    let e = server::promote(&r2, &[ux.clone()], "primary", false, None).await.unwrap_err();
    assert!(e.contains("409 Conflict") && e.contains("holds transactions up to 2 (term 1)") && e.contains("pass force"), "{e}");
    let a = server::promote(&r2, &[ux.clone()], "primary", true, None).await.unwrap();
    assert_eq!((&a["term"], &a["replicas"][0]["seq_no"]), (&json!(2), &json!(2)), "{a}");
    let a = server::promote(&format!("{ru}/"), &[], "majority", false, None).await.unwrap();
    assert_eq!((&a["term"], &a["seq_no"], &a["replicas"]), (&json!(2), &json!(1), &json!([])), "{a}");
    assert_eq!(r.role(), pb::Role::Primary);
    assert_eq!(r.health().await["replication"]["write_acks"], "majority");
    assert_eq!(r.health().await["replication"]["verify_every_s"], 1800, "it ships as its flags say");
    until("the old primary is fenced", || async { p.replaced_by() == 2 }).await;
    let w = r.write_ops(vec![set("p0000001", "sold", json!(8))], None).await.unwrap();
    assert_eq!(w.seq_no, Some(2));
    // Started as the primary of a new term, with no replica.
    let q = server::service(&server::Options { promote: true, ..opts("q.strata") }).await.unwrap();
    assert_eq!(q.role(), pb::Role::Primary);
    assert_eq!(q.local_db().unwrap().storage().numbering().unwrap().term, 2);
    let e = server::service(&server::Options { replica: true, promote: true, ..opts("rp.strata") }).await.err().unwrap();
    assert!(e.contains("not both"), "{e}");

    let e = server::service(&server::Options { replica: true, seq_no: false, ..opts("plain.strata") }).await.err().unwrap();
    assert!(e.contains("does not number its writes, which replication ships by: pass --seq-no"), "{e}");
    let e = server::service(&server::Options { replica: true, replicas: vec!["http://127.0.0.1:1".into()], ..opts("both.strata") }).await.err().unwrap();
    assert!(e.contains("not both"), "{e}");
    let e = server::service(&server::Options { replica: true, shards: vec!["http://127.0.0.1:1".into()], db: None, ..opts("x.strata") }).await.err().unwrap();
    assert!(e.contains("not the coordinator"), "{e}");
}

/// What `<db>.replication` beside database `name` in `dir` holds.
fn kept(dir: &Path, name: &str) -> Json {
    serde_json::from_slice(&std::fs::read(replication::file_of(&dir.join(name))).unwrap()).unwrap()
}

/// A replica is made the primary of a new term by hand. It refuses while a
/// copy it is to ship to holds a transaction it does not, or a newer term.
/// Once promoted, the other replica follows it, a call of the old primary's
/// term is answered with the new one, and the old primary, started again
/// as a replica with a transaction no copy got, is sent a copy of the new
/// primary, which drops that transaction.
#[tokio::test(flavor = "multi_thread")]
async fn a_replica_is_promoted_and_the_old_primary_follows_it() {
    let dir = tempfile::tempdir().unwrap();
    let file = |name: &str| replication::file_of(&dir.path().join(name));
    let (pdb, r1db, r2db) = (create(dir.path(), "p.strata"), create(dir.path(), "r1.strata"), create(dir.path(), "r2.strata"));
    let (r1, u1) = replica(r1db.clone(), Some(&file("r1.strata"))).await;
    let (r2, u2) = replica(r2db.clone(), Some(&file("r2.strata"))).await;
    let p = primary(pdb.clone(), Some(&file("p.strata")), ships_to(&[&u1, &u2], Acks::All), Duration::from_secs(5));
    for v in 1..=5 {
        p.write_ops(vec![set("p0000001", "sold", json!(v))], None).await.unwrap();
    }
    until("both replicas hold the writes", || async { last(&r1db) == 5 && last(&r2db) == 5 }).await;
    let old_id = p.health().await["replication"]["id"].as_str().unwrap().to_string();
    // The primary stops, its shippers with it, with a write that no copy
    // got.
    drop(p);
    tokio::time::sleep(Duration::from_millis(300)).await;
    set_on(&pdb, "p0000002", "sold", 99);
    assert_eq!((last(&pdb), last(&r1db), last(&r2db)), (6, 5, 5));

    // Only a replica is promoted.
    let standalone = Arc::new(service(create(dir.path(), "s.strata"), Duration::from_secs(5)));
    let e = standalone.promote(Vec::new(), Acks::All, false).await.unwrap_err();
    assert!(e.status == 409 && e.message.contains("only a replica is promoted"), "{}", e.message);
    // A copy it would ship to holds a later transaction, or a newer term:
    // refused, and nothing changes.
    let adb = create(dir.path(), "a.strata");
    for v in 0..6 {
        set_on(&adb, "p0000001", "sold", v);
    }
    let ua = grpc(Arc::new(service(adb, Duration::from_secs(5)))).await;
    let ndb = create(dir.path(), "n.strata");
    ndb.storage().begin_term(3).unwrap();
    let un = grpc(Arc::new(service(ndb, Duration::from_secs(5)))).await;
    for (u, holds) in [(&ua, "up to 6 (term 1)"), (&un, "up to 0 (term 3)")] {
        let e = r1.promote(vec![u2.clone(), u.clone()], Acks::All, false).await.unwrap_err();
        assert!(e.status == 409 && e.message.contains(&format!("{u} holds transactions {holds}, past this replica's last (5 of term 1)")), "{}", e.message);
    }
    assert_eq!(r1.role(), pb::Role::Replica);
    assert_eq!(r1db.storage().numbering().unwrap().term, 1, "a refused promotion begins no term");

    // Promoted, naming the other replica and the old primary, which is
    // down: it is said so, and not checked.
    // Asked twice at once, it is promoted once.
    let (old_url, old_addr) = nobody().await;
    let (a, b) = tokio::join!(r1.promote(vec![u2.clone(), old_url.clone()], Acks::All, false), r1.promote(vec![u2.clone()], Acks::All, false));
    let (a, e) = match (a, b) {
        (Ok(a), Err(e)) => (a, e),
        (a, b) => panic!("one promotion: {a:?} {b:?}"),
    };
    assert!(e.status == 409 && e.message.contains("only a replica is promoted"), "{}", e.message);
    assert_eq!((&a["term"], &a["seq_no"]), (&json!(2), &json!(5)), "{a}");
    assert_eq!(a["replicas"][0], json!({"url": u2, "role": "replica", "term": 1, "seq_no": 5}));
    assert!(a["replicas"][1]["error"].is_string(), "{a}");
    assert_eq!(r1.role(), pb::Role::Primary);
    let new_id = kept(dir.path(), "r1.strata")["id"].as_str().unwrap().to_string();
    assert!(kept(dir.path(), "r1.strata").get("follows").is_none(), "a primary follows no one");
    assert_eq!(r1.promote(Vec::new(), Acks::All, false).await.unwrap_err().status, 409, "promoted once");
    // It takes writes, numbered in term 2, which the other replica follows.
    let w = r1.write_ops(vec![set("p0000003", "sold", json!(7))], None).await.unwrap();
    assert_eq!(w.seq_no, Some(6));
    assert_eq!(r1db.storage().numbering().unwrap().term_at(6), 2);
    until("the other replica follows the new primary", || async { last(&r2db) == 6 }).await;
    assert_eq!(r2.health().await["replication"]["follows"], json!({"primary": new_id, "term": 2}));
    assert_eq!(rows(&r2db), rows(&r1db));
    // A call of the old primary's term is answered with the new term,
    // which fences it, and changes nothing.
    let a = r1.replicate(pb::ReplicateRequest { term: 1, prev_term: 1, prev_seq: 5, primary: old_id.clone(), ..Default::default() }).await.unwrap();
    assert_eq!((a.matched, a.seq, a.replica_term), (false, 6, 2));
    let image = {
        let to = dir.path().join("image.strata");
        r2db.storage().checkpoint(&to, &CheckpointOptions::default()).unwrap();
        std::fs::read(&to).unwrap()
    };
    let a = restore(&u1, 1, &old_id, image.len() as u64, &image).await.unwrap();
    assert_eq!((a.matched, a.replica_term), (false, 2));
    let mut c = pb::strata_client::StrataClient::connect(u1.clone()).await.unwrap();
    let e = c.verify(pb::VerifyRequest { term: 1, primary: old_id.clone(), seq_no: 5 }).await.unwrap_err();
    assert!(e.message().contains("this server is a primary"), "{e}");
    assert_eq!(r1.replaced_by(), 0, "an older term replaces nothing");
    assert_eq!(last(&r1db), 6);

    // The old primary starts again as a replica: it holds a transaction of
    // term 1 the new primary does not, and is sent a copy of it.
    let (old, _) = replica_on(pdb.clone(), Some(&file("p.strata")), Some(TcpListener::bind(old_addr).await.unwrap())).await;
    until("the old primary is sent a copy of the new one", || async { follower(&r1, 1).await["copies"] == 1 && follower(&r1, 1).await["in_sync"] == true }).await;
    assert_eq!(old.health().await["replication"]["restores"], 1);
    assert_eq!(rows(&pdb), rows(&r1db), "the transaction no copy got is dropped");
    let n = pdb.storage().numbering().unwrap();
    assert_eq!((n.term, n.seq, n.term_at(6)), (2, 6, 2));
    assert_eq!(kept(dir.path(), "p.strata"), json!({"id": old_id, "follows": {"primary": new_id, "term": 2}, "role": "replica"}));
    // And follows it from there.
    let w = r1.write_ops(vec![set("p0000004", "sold", json!(8))], None).await.unwrap();
    until("the old primary applies the new one's writes", || async { last(&pdb) == w.seq_no.unwrap() }).await;
    assert_eq!(rows(&pdb), rows(&r1db));
}

/// A primary that a primary of a newer term replaced is fenced: it takes no
/// writes and serves no reads, says so to `Hello`, and keeps it beside its
/// database, so that it starts again as a primary only when promoted. It
/// learns it from a replica that follows the new primary, or from the new
/// primary shipping to it, and a write that waits for its replicas when it
/// learns it is refused rather than answered. A database that was a
/// replica starts as a primary only when promoted too.
#[tokio::test(flavor = "multi_thread")]
async fn a_replaced_primary_is_fenced_and_starts_again_only_when_promoted() {
    let dir = tempfile::tempdir().unwrap();
    let file = |name: &str| replication::file_of(&dir.path().join(name));
    let (pdb, adb, bdb) = (create(dir.path(), "p.strata"), create(dir.path(), "a.strata"), create(dir.path(), "b.strata"));
    let (_a, ua) = replica(adb.clone(), None).await;
    let (_b, ub) = replica(bdb.clone(), None).await;
    let p = primary(pdb.clone(), Some(&file("p.strata")), ships_to(&[&ua, &ub], Acks::All), Duration::from_secs(5));
    p.write_ops(vec![set("p0000001", "sold", json!(1))], None).await.unwrap();
    // A write waits for replica a, whose writer is held, when replica b,
    // which took it, follows a newer term.
    let held = stall(&adb);
    let w = {
        let p = p.clone();
        tokio::spawn(async move { p.write_ops(vec![set("p0000001", "sold", json!(2))], None).await })
    };
    until("b holds the write", || async { last(&bdb) == 2 }).await;
    let t0 = Instant::now();
    bdb.storage().begin_term(2).unwrap();
    let e = w.await.unwrap().unwrap_err();
    assert!(t0.elapsed() < Duration::from_secs(3), "refused when fenced, not at the deadline: {} ms", t0.elapsed().as_millis());
    assert!(e.status == 409 && e.message.contains("the write is durable on this primary as sequence number 2, but a primary of term 2 replaced this one"), "{}", e.message);
    drop(held);

    // Fenced: no writes, no reads, and Hello says so.
    assert_eq!(p.replaced_by(), 2);
    assert_eq!(p.health().await["replication"]["replaced_by_term"], 2);
    let e = p.write_ops(vec![set("p0000001", "sold", json!(3))], None).await.unwrap_err();
    assert!(e.status == 409 && e.message.contains("a copy holds term 2, newer than this primary's (1)"), "{}", e.message);
    let e = p.query(QueryReq { sql: "SELECT sold FROM products WHERE id = 'p0000001'".into(), ..Default::default() }).await.err().unwrap();
    assert!(e.status == 503 && e.message.contains("a primary of term 2 replaced this one (term 1), which serves no reads"), "{}", e.message);
    let mut c = pb::strata_client::StrataClient::connect(grpc(p.clone()).await).await.unwrap();
    let h = c.hello(pb::HelloRequest { min_protocol: 1, max_protocol: 5 }).await.unwrap().into_inner();
    assert_eq!((h.role(), h.term, h.replaced_by_term), (pb::Role::Primary, 1, 2));
    assert_eq!(kept(dir.path(), "p.strata")["replaced_by"], 2);
    // A primary of a newer term still that ships to it fences it again.
    let e = p.replicate(pb::ReplicateRequest { term: 3, primary: "q".into(), ..Default::default() }).await.unwrap_err();
    assert!(e.status == 409 && e.message.contains("primary q of term 3 ships to it: that one replaced this one"), "{}", e.message);
    assert_eq!((p.replaced_by(), &kept(dir.path(), "p.strata")["replaced_by"]), (3, &json!(3)));

    // It starts again as a primary only when promoted, in a term above
    // the one that replaced it.
    let id = kept(dir.path(), "p.strata")["id"].clone();
    let e = Primary::start(pdb.clone(), Some(&file("p.strata")), PrimaryOptions::default()).err().unwrap();
    assert!(e.contains("a primary of term 3 replaced this one: start it with --replica"), "{e}");
    let p = Primary::start(pdb.clone(), Some(&file("p.strata")), PrimaryOptions { promote: true, ..Default::default() }).unwrap();
    assert_eq!(pdb.storage().numbering().unwrap().term, 4);
    assert_eq!(kept(dir.path(), "p.strata"), json!({"id": id, "role": "primary", "replicas": [], "write_acks": "all"}));
    // Replaced again, and started as a replica: it no longer counts as
    // replaced once it follows.
    assert_eq!(p.shipped_to("q", 5).unwrap_err().status, 409);
    // It keeps the part it has to take up: a replica of the new primary.
    assert_eq!(kept(dir.path(), "p.strata"), json!({"id": id, "replaced_by": 5, "role": "replica"}));
    drop(p);
    let f = Follow::open(pdb.clone(), Some(&file("p.strata"))).unwrap();
    assert!(f.replicate(pb::ReplicateRequest { term: 5, prev_term: 1, prev_seq: 2, primary: "q".into(), ..Default::default() }).await.unwrap().matched);
    assert_eq!(kept(dir.path(), "p.strata"), json!({"id": id, "follows": {"primary": "q", "term": 5}, "role": "replica"}));

    // So does a database that followed a primary as a replica.
    // Its primary's term is 3, though no transaction of that term reached
    // it: the new term is above it.
    let cdb = create(dir.path(), "c.strata");
    let f = Follow::open(cdb.clone(), Some(&file("c.strata"))).unwrap();
    assert!(f.replicate(pb::ReplicateRequest { term: 3, primary: "x".into(), ..Default::default() }).await.unwrap().matched);
    let e = Primary::start(cdb.clone(), Some(&file("c.strata")), PrimaryOptions::default()).err().unwrap();
    assert!(e.contains("this database is a replica of primary x in term 3: start it with --replica, or make it the primary of a new term with --promote"), "{e}");
    Primary::start(cdb.clone(), Some(&file("c.strata")), PrimaryOptions { promote: true, ..Default::default() }).unwrap();
    assert_eq!(cdb.storage().numbering().unwrap().term, 4);
    assert!(kept(dir.path(), "c.strata").get("follows").is_none());

    // Forced, a replica is promoted though a copy holds a later
    // transaction.
    let edb = create(dir.path(), "e.strata");
    let (e, _) = replica(edb.clone(), None).await;
    let adb = create(dir.path(), "ahead.strata");
    set_on(&adb, "p0000001", "sold", 1);
    let ua = grpc(Arc::new(service(adb, Duration::from_secs(5)))).await;
    assert_eq!(e.promote(vec![ua.clone()], Acks::All, false).await.unwrap_err().status, 409);
    let a = e.promote(vec![ua.clone()], Acks::All, true).await.unwrap();
    assert_eq!((&a["term"], &a["replicas"][0]["seq_no"]), (&json!(2), &json!(1)), "{a}");
    assert_eq!(e.role(), pb::Role::Primary);
}
