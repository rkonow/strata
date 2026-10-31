//! `strata-server`: Strata over HTTP JSON and gRPC, with a query UI.
//!
//! ```text
//! strata-server --demo 20000 --db products.strata      # a demo catalogue, UI at http://127.0.0.1:7700
//! strata-server --db my.strata                         # an existing database
//! STRATA_TOKEN=... strata-server --db my.strata --listen 0.0.0.0:7700   # beyond loopback: a token
//! strata-server --shards http://a:7701,http://b:7701   # a coordinator over shard servers
//! strata-server --shards 'http://a0:7701|http://a1:7701,http://b0:7701|http://b1:7701'   # two replicas each
//! strata-server --db a0.strata --seq-no --replicas http://a1:7701   # a primary shipping its writes to a replica
//! strata-server --db a1.strata --seq-no --replica                    # and that replica
//! strata-server --db new.strata --schema new.json --copy-from my.strata   # re-embed into a new schema
//! strata-server fetch-model potion-retrieval-32M       # download an embedding model
//! strata-server import --db my.strata --schema my.json rows.jsonl more.csv   # load files
//! strata-server import --db shop.strata shop.sqlite    # a SQLite database's tables
//! strata-server export --db shop.strata shop-copy.sqlite   # and back to SQLite
//! ```

use std::path::PathBuf;

/// Every allocation counted: the heap's size, its rate and where it is
/// allocated (`strata_server::heap`).
#[global_allocator]
static ALLOC: strata_server::heap::Counting = strata_server::heap::Counting;

use clap::{Parser, Subcommand};
use strata_server::auth::{self, Access, Secret, Tls};
use strata_server::obs;
use strata_server::server::{self, Options};

#[derive(Parser)]
#[command(about = "Strata over HTTP JSON and gRPC, with a query UI and an agent", args_conflicts_with_subcommands = true)]
struct Args {
    #[command(subcommand)]
    command: Option<Command>,
    /// The database to serve (created if missing, with --schema or --demo).
    #[arg(long)]
    db: Option<PathBuf>,
    /// Schema of a new database, as JSON.
    #[arg(long)]
    schema: Option<PathBuf>,
    /// Create the demo product catalogue with this many products.
    #[arg(long)]
    demo: Option<usize>,
    /// With --demo: keep only the products of shard I of N ("I/N"), those
    /// in its slots when N shards hold equal runs of slots.
    #[arg(long)]
    demo_shard: Option<String>,
    /// Fill the new database (--db with --schema) with this database's rows,
    /// field by field by name; vector fields whose embedding changed are
    /// embedded anew.
    #[arg(long)]
    copy_from: Option<PathBuf>,
    /// Where embedded fields' models are (see `fetch-model`).
    #[arg(long, env = "STRATA_MODELS")]
    models: Option<PathBuf>,
    /// Create a new database as a directory of segment files.
    #[arg(long)]
    directory: bool,
    /// Open a database whose write-ahead log was damaged after it was
    /// durable, dropping the transactions from the damage on (the log is
    /// copied to `<log>.damaged` first). Without it the server refuses to
    /// start on such a database.
    #[arg(long)]
    wal_drop_damaged: bool,
    /// Check every segment of the database against its checksum before
    /// serving (reads the whole database once). A damaged newest version
    /// is then skipped for the one before it, as when its manifest is
    /// damaged.
    #[arg(long)]
    verify_checksums: bool,
    /// Number the database's writes, for good: each row then has the
    /// sequence number of its last write (`seq_no()` in a SELECT), and a
    /// write operation can apply only while a row's number is the one it
    /// read (`if_seq_no`). The database then needs this version of
    /// Strata or a later one.
    #[arg(long)]
    seq_no: bool,
    /// Ship this database's writes to these replicas, as their primary
    /// (comma-separated gRPC URLs of servers started with --replica), so
    /// that each holds the same rows under the same sequence numbers.
    /// Needs --seq-no.
    #[arg(long, value_delimiter = ',')]
    replicas: Vec<String>,
    /// Serve as a replica: apply the writes a primary ships here (its
    /// --replicas names this server's gRPC URL), and refuse writes from
    /// clients. Needs --seq-no, and the rows the primary had when it
    /// started numbering (the same --demo, or a checkpoint of it).
    #[arg(long)]
    replica: bool,
    /// Start as the primary of a new term, above every term the database
    /// knows, shipping to --replicas (if any): needed when the database
    /// was a replica, or another primary replaced it. A running replica is
    /// promoted with `strata-server promote` instead.
    #[arg(long, conflicts_with = "replica")]
    promote: bool,
    /// With --replicas: the copies that hold a write before it is
    /// answered: all (the primary and every replica in sync), majority (of
    /// all copies), or primary.
    #[arg(long, default_value = "all")]
    write_acks: String,
    /// With --replicas or --replica: the latest transactions kept in
    /// memory to ship, in MiB. A replica further behind is sent a copy of
    /// the primary.
    #[arg(long, default_value_t = 64)]
    ship_log_mb: usize,
    /// With --replicas: compare each replica's rows with the primary's
    /// every this many seconds, and send a replica whose rows differ a
    /// copy of the primary (0: only when asked, POST /v1/admin/verify).
    #[arg(long, default_value_t = 600)]
    verify_every_s: u64,
    /// Coordinate these shard servers (comma-separated gRPC URLs; replicas
    /// of one shard joined by '|'). Servers of protocol 9 hold the cluster
    /// map, which names every shard's servers: the coordinator then routes
    /// by the newest map they hold, whatever servers this lists.
    #[arg(long, value_delimiter = ',')]
    shards: Vec<String>,
    /// With --shards: ask another replica of a shard when one has not
    /// answered after this many milliseconds.
    #[arg(long, default_value_t = 100)]
    hedge_ms: u64,
    /// With --shards: give up on a shard after this many milliseconds (0:
    /// at the query's deadline).
    #[arg(long, default_value_t = 0)]
    shard_timeout_ms: u64,
    /// With --shards: answer without shards that have no replica answering,
    /// flagged "partial" (a request's "partial" overrides it).
    #[arg(long)]
    partial: bool,
    /// HTTP address (API, metrics, UI).
    #[arg(long, default_value = "127.0.0.1:7700")]
    listen: String,
    /// gRPC address ("off" to disable).
    #[arg(long, default_value = "127.0.0.1:7701")]
    grpc: String,
    /// Require "Authorization: Bearer TOKEN" on the API and on gRPC (the
    /// UI asks for it). Needed to listen beyond loopback.
    #[arg(long, env = "STRATA_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// Listen beyond loopback without --token: anyone who can reach the
    /// ports can read and write.
    #[arg(long)]
    no_token: bool,
    /// Also answer HTTP requests for this host name (repeatable). On
    /// loopback the server answers only to localhost, 127.0.0.1 and ::1, so
    /// that a web page cannot reach it by DNS rebinding.
    #[arg(long)]
    allow_host: Vec<String>,
    /// Serve HTTP and gRPC over TLS with this certificate chain (PEM).
    #[arg(long, requires = "tls_key")]
    tls_cert: Option<PathBuf>,
    /// The private key of --tls-cert (PEM).
    #[arg(long, requires = "tls_cert")]
    tls_key: Option<PathBuf>,
    /// With --shards or --replicas: the token the shards or replicas
    /// require (default: --token).
    #[arg(long, env = "STRATA_SHARD_TOKEN", hide_env_values = true)]
    shard_token: Option<String>,
    /// With --shards or --replicas: a CA certificate (PEM) for https://
    /// shards or replicas, besides the public roots.
    #[arg(long)]
    shard_ca: Option<PathBuf>,
    /// With --shards: the slot map to route writes by, which the shards are
    /// given (JSON, each shard's slots as [start, end) ranges of the 4,096:
    /// {"epoch": 2, "shards": [[[0, 2048]], [[2048, 4096]]]}). Default:
    /// equal runs of slots, epoch 1.
    #[arg(long)]
    slot_map: Option<PathBuf>,
    /// Default query deadline.
    #[arg(long, default_value_t = 5000)]
    timeout_ms: u64,
    /// The longest deadline a request's `timeout_ms` may ask for.
    #[arg(long, default_value_t = 60000)]
    max_timeout_ms: u64,
    /// Queries running at once; more wait for a slot until their deadline.
    #[arg(long, default_value_t = 64)]
    max_concurrent: usize,
    /// The most rows a query may reach, OFFSET included: a larger LIMIT
    /// plus OFFSET, or a query without a LIMIT that matches more, fails
    /// (0: no limit).
    #[arg(long, default_value_t = 10_000)]
    max_result_window: usize,
    /// The most groups a GROUP BY may make before the query fails (0: no
    /// limit).
    #[arg(long, default_value_t = 65_536)]
    max_groups: usize,
    /// The most rows a cross-encoder's rerank phase may score, on a
    /// database or a coordinator: a query whose phase asks for more fails
    /// (0: no limit). Twenty abstracts take ms-marco-MiniLM-L-6-v2 about
    /// 1 s at the median and at most 1.6 s at p99 on four cores, a third of
    /// the default deadline; a hundred take all of it.
    #[arg(long, default_value_t = 20)]
    max_rerank: usize,
    /// Memory the running queries may hold together, in MiB: their groups,
    /// candidate rows, answers and joins; a query that needs more fails
    /// with 429 (default: a third of the cgroup memory limit, or of RAM;
    /// 0: no limit).
    #[arg(long)]
    query_memory_mb: Option<u64>,
    /// Write requests the server holds at once (embedding, waiting for the
    /// writer, or being applied); more are refused with 429 (0: no limit).
    #[arg(long, default_value_t = 1024)]
    max_queued_writes: usize,
    /// The largest body a write or an import may send, in MiB (other
    /// requests take up to 16 MiB).
    #[arg(long, default_value_t = 256)]
    max_body_mb: usize,
    /// The agent's default model.
    #[arg(long, default_value = strata_server::agent::DEFAULT_MODEL)]
    model: String,
    /// The Claude API's base URL.
    #[arg(long, env = "ANTHROPIC_BASE_URL", default_value = "https://api.anthropic.com")]
    api_base: String,
    /// Synonyms for `synonyms => 1` searches, one rule per line in Solr's
    /// format (`laptop, notebook` or `tv => television`); the UI edits it.
    #[arg(long)]
    synonyms: Option<PathBuf>,
    /// A directory for checkpoints (consistent copies of the database,
    /// taken while it serves) written by `POST /v1/admin/checkpoint`.
    #[arg(long)]
    backup_dir: Option<PathBuf>,
    /// What writes may fetch for MEDIA fields, comma-separated: directories
    /// (`file:/data/media`) and hosts fetched over HTTPS
    /// (`cdn.example.com`, `*.example.com`; `http://host:port` only spelled
    /// out). Nothing by default: writes then give the bytes or the whole
    /// value.
    #[arg(long, value_delimiter = ',')]
    media_fetch: Vec<String>,
    /// The largest media file a write fetches or gives, in bytes.
    #[arg(long, default_value_t = strata::media::DEFAULT_MAX_BYTES)]
    media_max_bytes: u64,
    /// Where the bytes a write gives a MEDIA field without a URI are kept,
    /// as DIR/<sha256> (the value's uri is then media:<sha256>).
    #[arg(long)]
    media_store: Option<PathBuf>,
    /// How long a media fetch may take, in seconds.
    #[arg(long, default_value_t = 30)]
    media_timeout_s: u64,
    /// Merges that may run at once (default: half the cores, 1 to 4).
    #[arg(long)]
    merge_threads: Option<usize>,
    /// Memory the running merges may use together, in MiB (default: a
    /// quarter of the cgroup memory limit, or of RAM).
    #[arg(long)]
    merge_memory_mb: Option<u64>,
    /// How fast merges may write, in MiB/s (default: no limit).
    #[arg(long)]
    merge_write_mb_per_sec: Option<u64>,
    /// Log queries and writes that take this many milliseconds or more,
    /// with their SQL (0: none).
    #[arg(long, default_value_t = 1000)]
    slow_ms: u64,
    /// The API keys' file (default: DB.keys.json beside --db; a
    /// coordinator keeps its keys in memory without it). Keys have scopes
    /// (read, write, admin) and are made in the UI or with POST
    /// /v1/admin/keys; --token stays the root credential.
    #[arg(long)]
    keys: Option<PathBuf>,
    /// Append the requests that need the admin scope, and refused ones, to
    /// this file as JSON lines (the latest are kept in memory either way:
    /// GET /v1/admin/audit).
    #[arg(long)]
    audit_log: Option<PathBuf>,
    /// The alert rules' file, JSON (created when the rules are first
    /// edited; default rules in memory without it).
    #[arg(long)]
    alerts: Option<PathBuf>,
    /// Post each alert that fires or resolves to this URL, as
    /// {"text": ..., "alert": {...}} (a Slack incoming webhook takes it).
    #[arg(long)]
    alert_webhook: Option<String>,
    /// What to log: a level (error, warn, info, debug, trace), or levels
    /// per module ("info,strata=debug"). debug logs every request.
    #[arg(long, env = "STRATA_LOG", default_value = "info")]
    log: String,
    /// How to write log lines: text, or json (one object per line).
    #[arg(long, env = "STRATA_LOG_FORMAT", default_value = "text")]
    log_format: String,
}

#[derive(Subcommand)]
enum Command {
    /// Download an embedding model from Hugging Face into the models directory.
    FetchModel {
        /// A known name (potion-retrieval-32M, potion-base-8M, bge-small-en-v1.5,
        /// snowflake-arctic-embed-m-v1.5, siglip-base-patch16-224,
        /// blip-image-captioning-base, whisper-small, clap-htsat-unfused) or a
        /// repository, owner/name.
        name: String,
        /// Where to put it (default: --models, $STRATA_MODELS, or
        /// ~/.local/share/strata/models).
        #[arg(long, env = "STRATA_MODELS")]
        models: Option<PathBuf>,
    },
    /// Make a running replica the primary of a new term (POST
    /// /v1/admin/promote): it ships to --replicas from then on. It refuses
    /// while one of them holds transactions it does not, unless --force.
    Promote {
        /// The replica's HTTP URL, as http://host:7700.
        url: String,
        /// The copies it is to ship to (comma-separated gRPC URLs): the
        /// shard's other replicas, and the old primary, which is sent a
        /// copy once it starts again with --replica.
        #[arg(long, value_delimiter = ',')]
        replicas: Vec<String>,
        /// The copies that hold a write before it is answered, as
        /// --write-acks: all, majority or primary.
        #[arg(long, default_value = "all")]
        write_acks: String,
        /// Promote it though a copy holds transactions it does not, which
        /// a copy of the new primary then drops.
        #[arg(long)]
        force: bool,
        /// The replica's --token.
        #[arg(long, env = "STRATA_TOKEN", hide_env_values = true)]
        token: Option<String>,
    },
    /// Load rows from JSON Lines, JSON, CSV or TSV files, and the tables of
    /// SQLite databases, into a database. A database that does not exist
    /// is created with --schema, or from the first SQLite file's tables. A
    /// SQLite table goes into the table of its name, created from SQLite's
    /// declarations and values when missing. A server must not have the
    /// database open (import through `POST /v1/import` instead).
    Import {
        /// The database.
        #[arg(long)]
        db: PathBuf,
        /// Schema of a new database, as JSON. With SQLite files, schemas
        /// for the tables it creates, in place of the ones made from
        /// SQLite's: one, an array, or the output of --plan, edited.
        #[arg(long)]
        schema: Option<PathBuf>,
        /// The table the rows go to (default: the first). A SQLite file's
        /// tables go to the tables of their names.
        #[arg(long)]
        table: Option<String>,
        /// The files' format: jsonl, json, csv, tsv or sqlite (default:
        /// SQLite for a SQLite database, else from each file's extension).
        #[arg(long)]
        format: Option<String>,
        /// SQLite: import only these tables (comma-separated).
        #[arg(long, value_delimiter = ',')]
        tables: Vec<String>,
        /// SQLite: keep BLOB columns as base64 text, rather than leave
        /// them out.
        #[arg(long)]
        blobs_base64: bool,
        /// SQLite: print what the import would do as JSON (each table's
        /// schema, its columns and what is left out), and write nothing.
        /// Edit it and give it back with --schema.
        #[arg(long)]
        plan: bool,
        /// Where embedded fields' models are (see `fetch-model`).
        #[arg(long, env = "STRATA_MODELS")]
        models: Option<PathBuf>,
        /// Create a new database as a directory of segment files.
        #[arg(long)]
        directory: bool,
        /// The files, loaded in order.
        #[arg(required = true)]
        files: Vec<PathBuf>,
    },
    /// Write a database's tables to a new SQLite file, from one snapshot:
    /// the rows, keys, references, NOT NULL and DEFAULT, a B-tree index on
    /// each indexed KEYWORD, INT, FLOAT, BOOL and TIMESTAMP field, an FTS5
    /// index over each table's TEXT fields, and the Strata schemas, which
    /// `import` restores. A server must not have the database open.
    Export {
        /// The database.
        #[arg(long)]
        db: PathBuf,
        /// Only these tables (comma-separated).
        #[arg(long, value_delimiter = ',')]
        tables: Vec<String>,
        /// No FTS5 indexes.
        #[arg(long)]
        no_fts: bool,
        /// No B-tree indexes.
        #[arg(long)]
        no_indexes: bool,
        /// Leave out the table that keeps the Strata schemas
        /// (`_strata_tables`).
        #[arg(long)]
        no_schemas: bool,
        /// The SQLite file to write; it must not exist.
        out: PathBuf,
    },
}

#[tokio::main]
async fn main() {
    let a = Args::parse();
    let format = obs::LogFormat::parse(&a.log_format).unwrap_or_else(|| {
        eprintln!("strata-server: --log-format is text or json, not {:?}", a.log_format);
        std::process::exit(1)
    });
    if let Err(e) = obs::init_logs(&a.log, format) {
        eprintln!("strata-server: {e}");
        std::process::exit(1)
    }
    match &a.command {
        Some(Command::FetchModel { name, models }) => {
            let dir = models.clone().unwrap_or_else(strata::embed::models_dir);
            match server::fetch_model(name, &dir).await {
                Ok(path) => tracing::info!(model = name, dir = %path.display(), "fetched the model"),
                Err(e) => fail(&e),
            }
            return;
        }
        Some(Command::Promote { url, replicas, write_acks, force, token }) => {
            match server::promote(url, replicas, write_acks, *force, token.as_deref()).await {
                Ok(j) => println!("{}", serde_json::to_string_pretty(&j).unwrap_or_default()),
                Err(e) => fail(&e),
            }
            return;
        }
        Some(Command::Import { db, schema, table, format, models, directory, files, tables, blobs_base64, plan }) => {
            let sqlite = server::SqliteImport { format: format.as_deref().is_some_and(|f| f.eq_ignore_ascii_case("sqlite")), tables: tables.clone(), blobs_base64: *blobs_base64, plan: *plan };
            let format = format.as_deref().filter(|_| !sqlite.format).map(|f| strata::ingest::Format::parse(f).unwrap_or_else(|| fail(&format!("unknown --format {f}: jsonl, json, csv, tsv or sqlite"))));
            let models = models.clone().unwrap_or_else(strata::embed::models_dir);
            if let Err(e) = server::import_files(db, schema.as_deref(), table.as_deref(), &models, format, *directory, files, sqlite) {
                fail(&e);
            }
            return;
        }
        Some(Command::Export { db, tables, no_fts, no_indexes, no_schemas, out }) => {
            let opts = strata::sqlite::ExportOptions { tables: tables.clone(), fts: !no_fts, indexes: !no_indexes, meta: !no_schemas };
            if let Err(e) = server::export_sqlite(db, out, &opts) {
                fail(&e);
            }
            return;
        }
        None => {}
    }
    let demo_shard = a.demo_shard.as_deref().map(|s| {
        let (i, n) = s.split_once('/').unwrap_or_else(|| fail("--demo-shard is I/N"));
        (i.parse().unwrap_or_else(|_| fail("--demo-shard is I/N")), n.parse().unwrap_or_else(|_| fail("--demo-shard is I/N")))
    });
    let read = |p: &PathBuf| std::fs::read(p).unwrap_or_else(|e| fail(&format!("read {}: {e}", p.display())));
    let tls = match (&a.tls_cert, &a.tls_key) {
        (Some(c), Some(k)) => Some(Tls { cert: read(c), key: read(k) }),
        _ => None,
    };
    if let Some(t) = &tls {
        auth::server_config(t).unwrap_or_else(|e| fail(&e));
    }
    let token = a.token.clone().filter(|t| !t.is_empty()).map(Secret::new);
    let write_acks = strata_server::replication::Acks::parse(&a.write_acks).unwrap_or_else(|| fail(&format!("--write-acks is all, majority or primary, not {:?}", a.write_acks)));
    if a.replica && a.grpc == "off" {
        fail("a replica takes its primary's writes over gRPC: --replica needs --grpc");
    }
    let o = Options {
        db: a.db,
        schema: a.schema,
        copy_from: a.copy_from,
        demo: a.demo,
        demo_shard,
        directory: a.directory,
        wal_drop_damaged: a.wal_drop_damaged,
        verify_checksums: a.verify_checksums,
        seq_no: a.seq_no,
        replicas: a.replicas,
        replica: a.replica,
        promote: a.promote,
        write_acks,
        ship_log_mb: a.ship_log_mb,
        verify_every_s: a.verify_every_s,
        shards: a.shards,
        coord: strata_server::coord::CoordOptions {
            hedge: std::time::Duration::from_millis(a.hedge_ms),
            shard_timeout: (a.shard_timeout_ms > 0).then(|| std::time::Duration::from_millis(a.shard_timeout_ms)),
            partial: a.partial,
            token: a.shard_token.clone().filter(|t| !t.is_empty()).map(Secret::new).or_else(|| token.clone()),
            ca: a.shard_ca.as_ref().map(read),
            slots: a.slot_map.as_ref().map(|p| {
                let j: serde_json::Value = serde_json::from_slice(&read(p)).unwrap_or_else(|e| fail(&format!("{}: {e}", p.display())));
                strata_server::slots::SlotMap::from_json(&j).unwrap_or_else(|e| fail(&format!("{}: {e}", p.display())))
            }),
            ..Default::default()
        },
        timeout_ms: a.timeout_ms,
        max_timeout_ms: a.max_timeout_ms,
        max_concurrent: a.max_concurrent,
        max_result_window: a.max_result_window,
        max_groups: a.max_groups,
        max_rerank: a.max_rerank,
        query_memory_mb: a.query_memory_mb,
        max_queued_writes: a.max_queued_writes,
        max_body_mb: a.max_body_mb,
        model: a.model,
        api_key: std::env::var("ANTHROPIC_API_KEY").ok().filter(|k| !k.is_empty()),
        api_base: a.api_base,
        models: a.models.unwrap_or_else(strata::embed::models_dir),
        synonyms: a.synonyms,
        backup_dir: a.backup_dir,
        media_fetch: a.media_fetch,
        // The default needs no resolver of its own: a database's has it.
        media_max_bytes: if a.media_max_bytes == strata::media::DEFAULT_MAX_BYTES { 0 } else { a.media_max_bytes },
        media_store: a.media_store,
        media_timeout_s: a.media_timeout_s,
        slow_ms: a.slow_ms,
        keys: a.keys,
        audit_log: a.audit_log,
        alerts: a.alerts,
        alert_webhook: a.alert_webhook.filter(|u| !u.is_empty()),
        merger: {
            let d = strata::storage::MergerOptions::default();
            strata::storage::MergerOptions {
                threads: a.merge_threads.unwrap_or(d.threads),
                memory_budget: a.merge_memory_mb.map(|m| m << 20),
                write_bytes_per_sec: a.merge_write_mb_per_sec.map(|m| m << 20),
                ..d
            }
        },
    };
    let coordinator = !o.shards.is_empty();
    let svc = server::service(&o).await.unwrap_or_else(|e| fail(&e));
    let bind = |addr: &str| {
        let addr = addr.to_string();
        // API keys close a server as a token does.
        let open = token.is_none() && svc.keys.is_empty() && !a.no_token;
        async move {
            let l = tokio::net::TcpListener::bind(&addr).await.unwrap_or_else(|e| fail(&format!("listen on {addr}: {e}")));
            let ip = l.local_addr().unwrap_or_else(|e| fail(&format!("listen on {addr}: {e}"))).ip();
            if open && !ip.is_loopback() {
                fail(&format!("{addr} is reachable beyond this machine: set --token (or $STRATA_TOKEN), make an API key, or pass --no-token to serve it open"));
            }
            (l, ip)
        }
    };
    let (http, ip) = bind(&a.listen).await;
    let access = Access { token: token.clone(), hosts: auth::allowed_hosts(ip, &a.allow_host), tls };
    let h = svc.health().await;
    tracing::info!(table = %svc.schema.table, rows = %h["docs"], segments = %h["segments"], version = env!("CARGO_PKG_VERSION"), simd = %strata::simd::describe(), "serving");
    let scheme = if access.tls.is_some() { "https" } else { "http" };
    let asks = if access.token.is_some() { " (it asks for the token)" } else { "" };
    tracing::info!("HTTP and UI on {scheme}://{}/{asks}", a.listen);
    if svc.agent.api_key.is_none() {
        tracing::info!("no ANTHROPIC_API_KEY; the agent asks for a key in the UI");
    }
    let mut tasks = Vec::new();
    // A coordinator answers queries but is not itself a shard.
    if a.grpc != "off" && !coordinator {
        let (l, _) = bind(&a.grpc).await;
        tracing::info!("gRPC on {}{}", a.grpc, if access.tls.is_some() { " (TLS)" } else { "" });
        let (svc, access) = (svc.clone(), access.clone());
        tasks.push(tokio::spawn(async move {
            if let Err(e) = server::serve_grpc(svc, l, access).await {
                tracing::error!(error = %e, "gRPC stopped");
            }
        }));
    }
    if let Err(e) = server::serve_http(svc, http, access).await {
        fail(&format!("HTTP server: {e}"));
    }
    for t in tasks {
        let _ = t.await;
    }
}

fn fail(msg: &str) -> ! {
    tracing::error!("{msg}");
    std::process::exit(1)
}
