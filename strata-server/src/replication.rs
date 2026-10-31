//! Replication: a primary ships its numbered transactions to its replicas.
//!
//! A server started with `--replicas` is the primary of its database. It
//! numbers its writes ([`strata::storage::Database::start_seq_no`]) and
//! keeps the latest of them as it logged them (its ship log,
//! [`strata::storage::DbOptions::ship_log_bytes`]); a task per replica
//! ships that log to the replica's `Replicate` call in order, in batches,
//! under the primary's term, and hears how far the replica got. A server
//! started with `--replica` applies what it is shipped
//! ([`strata::storage::Database::apply_shipped`], which checks that each
//! batch follows its last transaction) and takes no writes from clients,
//! so that every copy holds the same rows under the same sequence numbers.
//!
//! A write is answered once the copies `--write-acks` asks for hold it
//! ([`Acks`]), waiting up to the write's deadline: the primary alone, a
//! majority of the copies, or every replica in sync. A replica is in sync
//! while it holds what the primary committed, as far as it was shipped; it
//! drops out when a call to it fails, when its history differs from the
//! primary's, when the primary's log no longer reaches it, or when it does
//! not acknowledge a write within the write's deadline, and it is back in
//! once it holds the primary's last transaction again. The global
//! checkpoint is the highest number every in-sync replica holds, shipped
//! with each call.
//!
//! Each server keeps an id beside its database (`<db>.replication`), and a
//! replica keeps there the id and term of the primary it follows: it
//! refuses a second primary of the same term, whose history would differ,
//! and follows the primary of a newer term. A primary of an older term
//! than a replica's is answered with the replica's term, and stops taking
//! writes: another primary replaced it.
//!
//! A replica the primary's log no longer reaches is sent a copy of the
//! primary (`Restore`): a checkpoint, written beside the primary's
//! database and streamed in chunks, which the replica writes beside its
//! own, opens, and puts in place of its rows, numbers and terms in one
//! commit ([`strata::storage::Database::install_copy`]). It then follows
//! from the copy's last transaction, which the primary's log holds until
//! the replica has caught up ([`strata::storage::Database::hold_log`]). A
//! replica whose history differs from the primary's is not copied, and
//! the error says so, unless its last transaction is of an older term
//! than the primary's: it then holds transactions of a primary this one
//! replaced (a replaced primary started again as a replica), and the copy
//! drops them.
//!
//! A replica serves reads once a call from its primary since it started
//! reached the primary's last transaction, until a call finds it behind,
//! it fails to apply one or it installs a copy: while it catches up,
//! reads are refused (503), and a coordinator asks another copy. A read
//! that must see a write passes the write's sequence number
//! ([`crate::service::After`]): a replica waits until it has applied it,
//! up to [`READ_WAIT`], and refuses the read otherwise, so that a
//! coordinator asks the primary, which holds every write it answered. A
//! replica its primary cannot reach does not know it fell behind, and
//! goes on serving what it holds.
//!
//! A primary compares each replica's rows with its own every
//! [`PrimaryOptions::verify_every`] (`--verify-every-s`), and when asked
//! ([`Primary::verify`], `POST /v1/admin/verify`). It takes a version of
//! its database and ships the replica up to that version's last
//! transaction, in a call that has the replica keep the version it leaves
//! (`pin`); then it compares the [`Digests`] of the two versions
//! (`Verify`): for each slot of the keys, how many rows it holds and the
//! sum of a hash of each one's table, primary key and sequence number. A
//! replica whose digests differ drops out of sync and is sent a copy of
//! the primary. The digests cover which rows a replica holds and the
//! number of the write that last changed each, not their values: copies
//! that applied the same writes hold the same values, but a replica that
//! held other values under the same keys before numbering began (loaded
//! from other rows) is not found by them.
//!
//! A replica is made the primary of a new term by hand
//! ([`Follow::promote`]: `POST /v1/admin/promote`, `strata-server
//! promote`), or a stopped one with `--promote` ([`PrimaryOptions::promote`]).
//! It first asks the copies it is to ship to for their last transaction
//! (`Hello`) and refuses while one holds a later one, which the new
//! primary would not hold, unless forced. It then begins a term above
//! every term it knows ([`strata::storage::Database::begin_term`]), so
//! that the replicas follow it, and an old primary that ships to a copy
//! of the new term is answered with it and stops taking writes and serving
//! reads (it is fenced). It keeps that beside its database, and starts
//! again as a primary only with `--promote`. A primary that a primary of
//! a newer term ships to is fenced the same way, and one of an older term
//! is answered with this one's. There is no automatic failover: who is
//! primary is not agreed by the copies themselves.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, Instant};

use serde_json::{json, Value as Json};
use strata::db::Db;
use strata::storage::{Applied, CheckpointOptions, Database, DbOptions, Layout, LogRead, SeqNo, Snapshot};
use tokio::sync::watch;

use crate::auth::Secret;
use crate::coord::{self, Client};
use crate::metrics::Family;
use crate::slots::{self, SLOTS};
use crate::{pb, ApiError};

/// Which copies hold a write before it is answered (`--write-acks`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Acks {
    /// The primary: replicas get it after the answer.
    Primary,
    /// More than half the copies, the primary among them. A write is
    /// refused (503) before it is applied while fewer copies are in sync,
    /// and answered 504 when too few acknowledge it within its deadline,
    /// though the primary holds it.
    Majority,
    /// The primary and every replica in sync. A replica that does not
    /// acknowledge the write within its deadline drops out of sync, and
    /// the write is answered.
    #[default]
    All,
}

impl Acks {
    pub fn parse(s: &str) -> Option<Acks> {
        match s {
            "primary" => Some(Acks::Primary),
            "majority" => Some(Acks::Majority),
            "all" => Some(Acks::All),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Acks::Primary => "primary",
            Acks::Majority => "majority",
            Acks::All => "all",
        }
    }
}

/// Digests of a version's rows, slot by slot: what a primary and its
/// replicas compare.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Digests {
    /// For each of the [`SLOTS`] slots: the rows in it, and the wrapping
    /// sum of each one's hash.
    pub rows: Vec<u64>,
    pub sums: Vec<u64>,
}

impl Digests {
    /// Of every live row of every table `s` holds
    /// ([`Snapshot::row_versions`]): a row hashes its table, its primary
    /// key (as [`slots::slot`] encodes it) and the number of the write that
    /// last changed it (xxh3, 64 bits) into its key's slot. A row of a
    /// table without a primary key goes by its hash.
    pub fn of(s: &Snapshot) -> strata::Result<Digests> {
        let mut d = Digests { rows: vec![0; SLOTS], sums: vec![0; SLOTS] };
        let mut buf = Vec::new();
        for schema in s.schemas() {
            let Some(t) = s.table(&schema.table) else { continue };
            let name = schema.table.as_bytes();
            t.row_versions(|key, seq| {
                buf.clear();
                buf.extend_from_slice(&(name.len() as u32).to_le_bytes());
                buf.extend_from_slice(name);
                let at = buf.len();
                let slot = key.map(|k| {
                    slots::encode(k, |b| buf.extend_from_slice(b));
                    (twox_hash::XxHash3_64::oneshot(&buf[at..]) % SLOTS as u64) as usize
                });
                buf.extend_from_slice(&seq.to_le_bytes());
                let h = twox_hash::XxHash3_64::oneshot(&buf);
                let slot = slot.unwrap_or((h % SLOTS as u64) as usize);
                d.rows[slot] += 1;
                d.sums[slot] = d.sums[slot].wrapping_add(h);
            })?;
        }
        Ok(d)
    }

    /// The slots where `self` and `other` differ.
    pub fn differ(&self, other: &Digests) -> Vec<usize> {
        (0..SLOTS).filter(|&i| self.rows.get(i) != other.rows.get(i) || self.sums.get(i) != other.sums.get(i)).collect()
    }

    pub fn to_pb(&self) -> pb::VerifyResponse {
        pb::VerifyResponse { rows: self.rows.clone(), sums: self.sums.clone() }
    }

    /// A replica's answer, which must hold every slot.
    pub fn from_pb(r: pb::VerifyResponse) -> Result<Digests, String> {
        if r.rows.len() != SLOTS || r.sums.len() != SLOTS {
            return Err(format!("its digests hold {} and {} slots, not {SLOTS}", r.rows.len(), r.sums.len()));
        }
        Ok(Digests { rows: r.rows, sums: r.sums })
    }
}

/// How a primary ships to its replicas.
#[derive(Clone, Debug)]
pub struct PrimaryOptions {
    /// The replicas' gRPC URLs.
    pub replicas: Vec<String>,
    pub acks: Acks,
    /// The token the replicas require, and a CA for `https://` ones.
    pub token: Option<Secret>,
    pub ca: Option<Vec<u8>>,
    /// The most bytes of transactions a call ships (one at least).
    pub batch_bytes: usize,
    /// How often a replica with nothing to apply is called, which tells
    /// it the global checkpoint and the primary whether it is there.
    pub heartbeat: Duration,
    /// The longest wait before calling a replica that failed again.
    pub retry_max: Duration,
    /// How long a call may take. A copy may take a second more for each
    /// MiB it holds.
    pub call_timeout: Duration,
    /// The bytes of a copy each `Restore` chunk carries.
    pub copy_chunk_bytes: usize,
    /// How long to wait before sending a replica a copy again after one
    /// failed: each copy writes a checkpoint of the database.
    pub copy_retry: Duration,
    /// How often each replica's rows are compared with the primary's
    /// ([`Digests`]); zero: only when asked ([`Primary::verify`]).
    pub verify_every: Duration,
    /// Start as the primary of a new term, above every term the database
    /// knows (`--promote`): needed when it was a replica, or another
    /// primary replaced it.
    pub promote: bool,
    /// Its replicas join it now (`Configure`): each starts out of sync
    /// and is sent a copy of the primary before what follows, as it may
    /// lack rows that took no number (a bulk load). A replica added later
    /// ([`Primary::reconfigure`]) joins so too.
    pub joining: bool,
}

impl Default for PrimaryOptions {
    fn default() -> PrimaryOptions {
        PrimaryOptions {
            replicas: Vec::new(),
            acks: Acks::All,
            token: None,
            ca: None,
            batch_bytes: 4 << 20,
            heartbeat: Duration::from_secs(1),
            retry_max: Duration::from_secs(2),
            call_timeout: Duration::from_secs(10),
            copy_chunk_bytes: 1 << 20,
            copy_retry: Duration::from_secs(30),
            verify_every: Duration::from_secs(600),
            promote: false,
            joining: false,
        }
    }
}

/// Where a server keeps its id, and a replica the primary it follows.
pub fn file_of(db: &Path) -> PathBuf {
    let mut p = db.as_os_str().to_owned();
    p.push(".replication");
    p.into()
}

/// A server's part in replication, as kept beside its database, so that
/// it starts again in it without flags saying so ([`kept_role`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum KeptRole {
    /// The primary of its shard, shipping to these replicas.
    Primary { replicas: Vec<String>, acks: Acks },
    /// A replica, applying what its primary ships.
    Replica,
}

/// What `<db>.replication` holds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Kept {
    id: String,
    /// The primary followed, and its term.
    follows: Option<(String, u64)>,
    /// The newer term of a primary that replaced this one, as a primary.
    replaced_by: Option<u64>,
    /// Its part, as last set (`None`: standalone, or kept by a version
    /// from before roles were kept, whose flags say).
    role: Option<KeptRole>,
}

/// The part in replication kept beside the database at `db`
/// ([`KeptRole`]), and whether a primary of a newer term replaced it as a
/// primary: a server started without `--replicas`, `--replica` or
/// `--promote` takes it up again.
pub fn kept_role(db: &Path) -> Result<(Option<KeptRole>, bool), String> {
    let path = file_of(db);
    if !path.exists() {
        return Ok((None, false));
    }
    let k = Kept::open(Some(&path))?;
    Ok((k.role, k.replaced_by.is_some()))
}

impl Kept {
    /// Reads `path`, or makes a new id and keeps it there.
    fn open(path: Option<&Path>) -> Result<Kept, String> {
        let Some(path) = path else { return Ok(Kept { id: new_id(), ..Kept::default() }) };
        if path.exists() {
            let j: Json = std::fs::read(path).map_err(|e| e.to_string()).and_then(|b| serde_json::from_slice(&b).map_err(|e| e.to_string())).map_err(|e| format!("{}: {e}", path.display()))?;
            let id = j["id"].as_str().filter(|s| !s.is_empty()).ok_or_else(|| format!("{}: no id", path.display()))?.to_string();
            let follows = match &j["follows"] {
                Json::Null => None,
                f => Some((f["primary"].as_str().unwrap_or_default().to_string(), f["term"].as_u64().ok_or_else(|| format!("{}: no term followed", path.display()))?)),
            };
            let replaced_by = match &j["replaced_by"] {
                Json::Null => None,
                t => Some(t.as_u64().ok_or_else(|| format!("{}: replaced_by is not a term", path.display()))?),
            };
            let role = match j["role"].as_str() {
                None => None,
                Some("replica") => Some(KeptRole::Replica),
                Some("primary") => {
                    let replicas = j["replicas"].as_array().map(|a| a.iter().filter_map(|u| u.as_str().map(String::from)).collect()).unwrap_or_default();
                    let acks = j["write_acks"].as_str().and_then(Acks::parse).unwrap_or_default();
                    Some(KeptRole::Primary { replicas, acks })
                }
                Some(r) => return Err(format!("{}: role {r:?} is primary or replica", path.display())),
            };
            return Ok(Kept { id, follows, replaced_by, role });
        }
        let k = Kept { id: new_id(), ..Kept::default() };
        k.save(Some(path)).map_err(|e| format!("keep the replication id in {}: {e}", path.display()))?;
        Ok(k)
    }

    /// The newest term it knows of, besides the database's `term`.
    fn newest(&self, term: u64) -> u64 {
        term.max(self.follows.as_ref().map_or(0, |f| f.1)).max(self.replaced_by.unwrap_or(0))
    }

    fn save(&self, path: Option<&Path>) -> std::io::Result<()> {
        let Some(path) = path else { return Ok(()) };
        let mut j = json!({"id": self.id});
        if let Some((primary, term)) = &self.follows {
            j["follows"] = json!({"primary": primary, "term": term});
        }
        if let Some(t) = self.replaced_by {
            j["replaced_by"] = json!(t);
        }
        match &self.role {
            None => {}
            Some(KeptRole::Replica) => j["role"] = json!("replica"),
            Some(KeptRole::Primary { replicas, acks }) => {
                j["role"] = json!("primary");
                j["replicas"] = json!(replicas);
                j["write_acks"] = json!(acks.name());
            }
        }
        crate::service::save_json(path, &j)
    }
}

/// Forgets the part in replication kept at `file`: the server starts
/// again standalone.
pub fn forget_role(file: Option<&Path>) -> Result<(), String> {
    let mut k = Kept::open(file)?;
    k.role = None;
    k.save(file).map_err(|e| e.to_string())
}

/// 32 hex digits, drawn at random.
fn new_id() -> String {
    use std::hash::{BuildHasher, Hasher};
    let half = || {
        let mut h = std::collections::hash_map::RandomState::new().build_hasher();
        h.write_u128(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |d| d.as_nanos()));
        h.finish()
    };
    format!("{:016x}{:016x}", half(), half())
}

fn conflict(message: String) -> ApiError {
    ApiError { status: 409, message }
}

/// A path beside the database at `db` for a copy of it, which no other
/// copy has.
fn beside(db: &Path, what: &str) -> PathBuf {
    let name = db.file_name().map(|n| n.to_string_lossy().into_owned()).unwrap_or_default();
    db.with_file_name(format!(".{name}.{what}-{}", new_id()))
}

/// Removes the copy at its path, and its log, when dropped.
struct Removed(PathBuf);

impl Drop for Removed {
    fn drop(&mut self) {
        let mut wal = self.0.as_os_str().to_owned();
        wal.push("-wal");
        for p in [self.0.as_path(), Path::new(&wal)] {
            match std::fs::remove_file(p) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => tracing::warn!(path = %p.display(), error = %e, "could not remove a copy of the database"),
                _ => {}
            }
        }
    }
}

/// Keeps a primary's transactions from a number on in its ship log until
/// dropped ([`strata::storage::Database::hold_log`]).
struct LogHold {
    db: Arc<Db>,
    id: u64,
}

impl LogHold {
    fn new(db: Arc<Db>, from: u64) -> LogHold {
        let id = db.storage().hold_log(from);
        LogHold { db, id }
    }
}

impl Drop for LogHold {
    fn drop(&mut self) {
        self.db.storage().release_log(self.id);
    }
}

/// A replica, as its primary sees it.
struct Follower {
    url: String,
    client: Client,
    /// The last number it holds, as it last said.
    acked: AtomicU64,
    in_sync: AtomicBool,
    /// Why it is out of sync, or why its last call failed.
    error: Mutex<Option<String>>,
    /// Transactions it applied from this primary, and calls that failed.
    shipped: AtomicU64,
    failures: AtomicU64,
    /// Whether it is being sent a copy of the primary, and the copies it
    /// installed.
    copying: AtomicBool,
    copies: AtomicU64,
    /// What the last round comparing its rows with the primary's found
    /// ([`verify`]), whether a round is comparing, and whether one found
    /// its rows differ, until its shipper takes that up.
    verified: Mutex<Verified>,
    verifying: AtomicBool,
    diverged: AtomicBool,
    /// Rounds that compared its rows, those that found them differ, and
    /// those that could not compare.
    verifications: AtomicU64,
    divergences: AtomicU64,
    verify_failures: AtomicU64,
    /// Wakes its shipper: a round ended, or one was asked for.
    wake: Arc<tokio::sync::Notify>,
    /// The [`Primary::gen`] whose shipper ships to it: a primary given
    /// other replicas ([`Primary::reconfigure`]) takes over the replicas
    /// it keeps, and the shipper before stops.
    owner: AtomicU64,
}

/// Numbers each [`Primary`] made by this process.
static GENERATION: AtomicU64 = AtomicU64::new(1);

impl Follower {
    fn new(url: &str, opts: &PrimaryOptions) -> Result<Follower, String> {
        Ok(Follower {
            url: url.to_string(),
            client: coord::client(url, opts.token.as_ref(), opts.ca.as_deref()).map_err(|e| e.message)?,
            acked: AtomicU64::new(0),
            in_sync: AtomicBool::new(true),
            error: Mutex::new(None),
            shipped: AtomicU64::new(0),
            failures: AtomicU64::new(0),
            copying: AtomicBool::new(false),
            copies: AtomicU64::new(0),
            verified: Mutex::new(Verified::default()),
            verifying: AtomicBool::new(false),
            diverged: AtomicBool::new(false),
            verifications: AtomicU64::new(0),
            divergences: AtomicU64::new(0),
            verify_failures: AtomicU64::new(0),
            wake: Arc::new(tokio::sync::Notify::new()),
            owner: AtomicU64::new(0),
        })
    }

    /// A replica that joins a serving primary: out of sync, so that writes
    /// do not wait for it, until it has installed a copy of the primary
    /// and caught up from there.
    fn joining(self) -> Follower {
        *self.error.lock().unwrap_or_else(|e| e.into_inner()) = Some("it joined: it is sent a copy of the primary first".into());
        self.in_sync.store(false, Ordering::Relaxed);
        self.diverged.store(true, Ordering::Relaxed);
        self
    }
}

/// What the last round comparing a replica's rows with its primary's found.
#[derive(Clone, Debug, Default)]
struct Verified {
    /// The transaction it compared at, when, and the slots that differed.
    at: Option<(u64, Instant, Vec<usize>)>,
    /// Why the last round could not compare.
    error: Option<String>,
    /// The latest request ([`Primary::verify`]) a round ended for.
    answered: u64,
}

impl Follower {
    fn acked(&self) -> u64 {
        self.acked.load(Ordering::Relaxed)
    }

    fn in_sync(&self) -> bool {
        self.in_sync.load(Ordering::Relaxed)
    }

    fn error(&self) -> Option<String> {
        self.error.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Takes it out of sync for `why`, logging the change.
    fn drop_out(&self, why: String) {
        let was = self.in_sync.swap(false, Ordering::Relaxed);
        let mut e = self.error.lock().unwrap_or_else(|e| e.into_inner());
        if was || e.as_deref() != Some(&why) {
            tracing::warn!(replica = %self.url, error = %why, "a replica is out of sync");
        }
        *e = Some(why);
    }

    fn verified(&self) -> std::sync::MutexGuard<'_, Verified> {
        self.verified.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// What its health shows of the last round comparing its rows.
    fn verification(&self) -> Json {
        let v = self.verified().clone();
        let mut j = json!({});
        if let Some((seq, when, differ)) = &v.at {
            j = json!({"seq_no": seq, "ms_ago": when.elapsed().as_millis() as u64, "differing_slots": differ.len()});
            if let Some(first) = differ.first() {
                j["first_differing_slot"] = json!(first);
            }
        }
        if let Some(e) = v.error {
            j["error"] = json!(e);
        }
        j
    }

    /// Puts it in sync: it holds the primary's last transaction.
    fn caught_up(&self) {
        if !self.in_sync.swap(true, Ordering::Relaxed) {
            tracing::info!(replica = %self.url, seq_no = self.acked(), "a replica is in sync");
        }
        *self.error.lock().unwrap_or_else(|e| e.into_inner()) = None;
    }
}

/// A primary: ships its database's numbered transactions to its replicas,
/// and tells a write when enough copies hold it.
pub struct Primary {
    db: Arc<Db>,
    /// Where its id is kept, and the term that replaced it.
    file: Option<PathBuf>,
    id: String,
    opts: PrimaryOptions,
    followers: Vec<Arc<Follower>>,
    /// This primary among those the process made ([`GENERATION`]).
    gen: u64,
    /// The last number committed, as the database's commit hook says.
    committed: Arc<watch::Sender<u64>>,
    /// Changed whenever a replica acknowledges or drops out.
    progress: watch::Sender<u64>,
    /// A newer term a replica holds (0: none): another primary replaced
    /// this one.
    fenced: AtomicU64,
    /// Requests to compare the replicas now ([`Primary::verify`]).
    verify_asked: AtomicU64,
}

impl Primary {
    /// Starts shipping `db`'s transactions to `opts.replicas`, keeping
    /// the primary's id in `file` (none: a new one each start). Every
    /// replica counts as in sync until it fails, so that the first writes
    /// wait for replicas that are there.
    ///
    /// A database that followed a primary as a replica, or whose primary
    /// another replaced, starts only with [`PrimaryOptions::promote`],
    /// which begins a term above every term it knows.
    pub fn start(db: Arc<Db>, file: Option<&Path>, opts: PrimaryOptions) -> Result<Arc<Primary>, String> {
        let numbering = db.storage().numbering().ok_or("a primary numbers its writes: start it with --seq-no")?;
        let mut kept = Kept::open(file)?;
        if opts.promote {
            let term = kept.newest(numbering.term) + 1;
            db.storage().begin_term(term).map_err(|e| format!("begin term {term}: {e}"))?;
            kept = Kept { id: kept.id, ..Kept::default() };
            tracing::info!(term, "promoted: the primary of a new term");
        } else if let Some((primary, term)) = &kept.follows {
            return Err(format!("this database is a replica of primary {primary} in term {term}: start it with --replica, or make it the primary of a new term with --promote"));
        } else if let Some(term) = kept.replaced_by {
            return Err(format!("a primary of term {term} replaced this one: start it with --replica, as a replica of that primary, or make it the primary of a newer term with --promote"));
        }
        Primary::run(db, file, kept, opts)
    }

    /// Starts shipping as [`Primary::start`] does, under id `kept.id`.
    fn run(db: Arc<Db>, file: Option<&Path>, kept: Kept, opts: PrimaryOptions) -> Result<Arc<Primary>, String> {
        let joining = opts.joining;
        let followers = opts.replicas.iter().map(|url| Follower::new(url, &opts).map(|f| Arc::new(if joining { f.joining() } else { f }))).collect::<Result<Vec<_>, String>>()?;
        Primary::run_with(db, file, kept, opts, followers)
    }

    /// [`Primary::run`] shipping to `followers` (those of
    /// `opts.replicas`), keeping its part beside the database.
    fn run_with(db: Arc<Db>, file: Option<&Path>, mut kept: Kept, opts: PrimaryOptions, followers: Vec<Arc<Follower>>) -> Result<Arc<Primary>, String> {
        let numbering = db.storage().numbering().ok_or("a primary numbers its writes: start it with --seq-no")?;
        if !db.storage().keeps_ship_log() {
            return Err("a primary keeps a log of its transactions to ship (DbOptions::ship_log_bytes)".into());
        }
        let role = Some(KeptRole::Primary { replicas: opts.replicas.clone(), acks: opts.acks });
        if kept.role != role || file.is_some_and(|f| !f.exists()) {
            kept.role = role;
            kept.save(file).map_err(|e| format!("keep the replication id and role: {e}"))?;
        }
        let gen = GENERATION.fetch_add(1, Ordering::Relaxed);
        for f in &followers {
            f.owner.store(gen, Ordering::Relaxed);
        }
        let committed = Arc::new(watch::channel(numbering.seq).0);
        let hook = committed.clone();
        db.storage().on_commit(move |n| {
            hook.send_replace(n.seq);
        });
        let p = Arc::new(Primary {
            db,
            file: file.map(Path::to_path_buf),
            id: kept.id,
            opts,
            followers,
            gen,
            committed,
            progress: watch::channel(0).0,
            fenced: AtomicU64::new(0),
            verify_asked: AtomicU64::new(0),
        });
        for k in 0..p.followers.len() {
            tokio::spawn(ship(Arc::downgrade(&p), k));
        }
        Ok(p)
    }

    pub fn acks(&self) -> Acks {
        self.opts.acks
    }

    /// The replicas it ships to.
    pub fn replicas(&self) -> &[String] {
        &self.opts.replicas
    }

    /// A primary of the same database, id and term shipping to `replicas`
    /// under `acks` from now on, in place of this one, which stops
    /// shipping: replicas it ships to already keep what this one knows of
    /// them, and new ones are asked their last transaction (an empty one
    /// is sent a copy). Refused once another primary replaced this one.
    pub fn reconfigure(&self, replicas: Vec<String>, acks: Acks) -> Result<Arc<Primary>, ApiError> {
        if let t @ 1.. = self.replaced_by() {
            return Err(conflict(format!("a primary of term {t} replaced this one, which ships to no replica: make it a replica of the new primary")));
        }
        let mut seen = std::collections::HashSet::new();
        if let Some(dup) = replicas.iter().find(|u| !seen.insert(u.as_str())) {
            return Err(ApiError::bad(format!("{dup} is named twice")));
        }
        let opts = PrimaryOptions { replicas, acks, promote: false, ..self.opts.clone() };
        let followers = opts
            .replicas
            .iter()
            .map(|url| match self.followers.iter().find(|f| f.url == *url) {
                Some(f) => Ok(f.clone()),
                None => Follower::new(url, &opts).map(|f| Arc::new(f.joining())),
            })
            .collect::<Result<Vec<_>, String>>()
            .map_err(ApiError::bad)?;
        let kept = Kept { id: self.id.clone(), ..Kept::default() };
        let p = Primary::run_with(self.db.clone(), self.file.as_deref(), kept, opts, followers).map_err(ApiError::internal)?;
        tracing::info!(replicas = %p.opts.replicas.join(", "), write_acks = p.opts.acks.name(), "primary: ships to these replicas from now on");
        Ok(p)
    }

    fn term(&self) -> u64 {
        self.db.storage().numbering().map_or(0, |n| n.term)
    }

    /// Copies in all, the primary among them, and how many make a majority.
    fn majority(&self) -> usize {
        (self.followers.len() + 1) / 2 + 1
    }

    /// The highest number every in-sync replica holds (the primary's last
    /// while none is in sync).
    pub fn global_checkpoint(&self) -> u64 {
        let last = *self.committed.borrow();
        self.followers.iter().filter(|f| f.in_sync()).map(|f| f.acked()).min().unwrap_or(last).min(last)
    }

    fn changed(&self) {
        self.progress.send_modify(|n| *n += 1);
    }

    /// The newer term of a primary that replaced this one (0: none).
    pub fn replaced_by(&self) -> u64 {
        self.fenced.load(Ordering::Relaxed)
    }

    /// Takes no more writes and serves no reads: a primary of `term`, newer
    /// than this one's, replaced it. Kept beside the database, so that it
    /// does not start again as a primary without `--promote`.
    fn fence(&self, term: u64) {
        if self.fenced.fetch_max(term, Ordering::Relaxed) >= term {
            return;
        }
        tracing::error!(term = self.term(), replaced_by = term, "a primary of a newer term replaced this one, which takes no more writes and serves no reads");
        // It starts again as a replica of the primary that replaced it.
        let kept = Kept { id: self.id.clone(), follows: None, replaced_by: Some(term), role: Some(KeptRole::Replica) };
        if let Err(e) = kept.save(self.file.as_deref()) {
            tracing::error!(error = %e, "could not keep that another primary replaced this one");
        }
        self.changed();
    }

    /// Refuses a read once another primary replaced this one (503): it
    /// may hold writes the new primary does not, and not the new
    /// primary's.
    pub fn readable(&self) -> Result<(), ApiError> {
        match self.replaced_by() {
            0 => Ok(()),
            t => Err(ApiError { status: 503, message: format!("a primary of term {t} replaced this one (term {}), which serves no reads: read from another copy", self.term()) }),
        }
    }

    /// Answers a primary's call to ship to this one (`Replicate`,
    /// `Restore`, `Verify`), as a primary of term `term` makes when it
    /// does not know this one replaced it: answered with this primary's
    /// term, which fences it, when `term` is older. A newer term fences
    /// this primary, and the call is refused (409).
    pub fn shipped_to(&self, primary: &str, term: u64) -> Result<pb::ReplicateResponse, ApiError> {
        let (mine, last) = (self.term(), self.db.storage().numbering().unwrap_or_default().last());
        if term < mine {
            return Ok(pb::ReplicateResponse { matched: false, seq: last.seq, term: last.term, replica_term: mine });
        }
        if term > mine {
            self.fence(term);
            return Err(conflict(format!(
                "this server is the primary of term {mine}, and primary {primary} of term {term} ships to it: that one replaced this one, which takes no more writes; start this one with --replica"
            )));
        }
        if primary == self.id {
            return Err(conflict(format!("this server is primary {primary} of term {term}, not a replica: is its own URL among its replicas?")));
        }
        Err(conflict(format!("this server is the primary of term {term}, not a replica, and primary {primary} ships to it in the same term: a shard has one primary in a term")))
    }

    /// Answers a primary's copy of itself (`Restore`), as
    /// [`Primary::shipped_to`] answers a call.
    pub async fn copied_to(&self, mut chunks: tonic::Streaming<pb::RestoreChunk>) -> Result<pb::ReplicateResponse, ApiError> {
        let first = chunks.message().await.map_err(|s| ApiError::bad(format!("the copy was cut off: {}", s.message())))?.ok_or_else(|| ApiError::bad("the copy is empty"))?;
        self.shipped_to(&first.primary, first.term)
    }

    /// Whether a write may be applied: not on a primary another replaced
    /// (409), and with `majority`, while a majority of copies is in sync
    /// (503), so that a write the copies cannot acknowledge is not applied.
    pub fn admit(&self) -> Result<(), ApiError> {
        let fenced = self.fenced.load(Ordering::Relaxed);
        if fenced > 0 {
            return Err(conflict(format!(
                "a copy holds term {fenced}, newer than this primary's ({}): another primary replaced this one, which takes no more writes",
                self.term()
            )));
        }
        if self.opts.acks == Acks::Majority {
            let up = 1 + self.followers.iter().filter(|f| f.in_sync()).count();
            if up < self.majority() {
                return Err(ApiError {
                    status: 503,
                    message: format!("{up} of {} copies are in sync, and --write-acks majority needs {}: the write was not applied", self.followers.len() + 1, self.majority()),
                });
            }
        }
        Ok(())
    }

    /// Waits until the copies `--write-acks` asks for hold transaction
    /// `seq`, or `deadline`. Then, with `all`, the replicas in sync that
    /// do not hold it drop out of sync; with `majority`, the write is
    /// answered 504.
    pub async fn acknowledged(&self, seq: u64, deadline: Instant) -> Result<(), ApiError> {
        let mut progress = self.progress.subscribe();
        loop {
            // Its replicas follow the new primary, which may not hold it.
            if let t @ 1.. = self.replaced_by() {
                return Err(conflict(format!(
                    "the write is durable on this primary as sequence number {seq}, but a primary of term {t} replaced this one before it was acknowledged, and the new primary may not hold it: write to the new primary"
                )));
            }
            let holding = 1 + self.followers.iter().filter(|f| f.acked() >= seq).count();
            match self.opts.acks {
                Acks::Primary => return Ok(()),
                Acks::All if self.followers.iter().all(|f| !f.in_sync() || f.acked() >= seq) => return Ok(()),
                Acks::Majority if holding >= self.majority() => return Ok(()),
                _ => {}
            }
            if tokio::time::timeout_at(deadline.into(), progress.changed()).await.is_ok() {
                continue;
            }
            if self.opts.acks == Acks::Majority {
                return Err(ApiError {
                    status: 504,
                    message: format!(
                        "the write is durable on the primary as sequence number {seq}, but {holding} of {} copies held it by its deadline, and --write-acks majority needs {}",
                        self.followers.len() + 1,
                        self.majority()
                    ),
                });
            }
            for f in self.followers.iter().filter(|f| f.in_sync() && f.acked() < seq) {
                f.drop_out(format!("it did not acknowledge transaction {seq} within the write's deadline"));
            }
            self.changed();
            return Ok(());
        }
    }

    /// Compares each replica in sync with this primary now, as every
    /// `verify_every` does, and answers what each comparison found,
    /// waiting for them until `deadline`: `compared` is false for a
    /// replica out of sync, or whose round did not end in time.
    pub async fn verify(&self, deadline: Instant) -> Json {
        let n = self.verify_asked.fetch_add(1, Ordering::Relaxed) + 1;
        let mut progress = self.progress.subscribe();
        for f in &self.followers {
            f.wake.notify_one();
        }
        let done = |f: &Follower| f.verified().answered >= n;
        while !self.followers.iter().all(|f| done(f) || !f.in_sync()) {
            if tokio::time::timeout_at(deadline.into(), progress.changed()).await.is_err() {
                break;
            }
        }
        let replicas: Vec<Json> = self
            .followers
            .iter()
            .map(|f| {
                let mut j = f.verification();
                j["url"] = json!(f.url);
                j["in_sync"] = json!(f.in_sync());
                j["compared"] = json!(done(f));
                j
            })
            .collect();
        json!({"replicas": replicas})
    }

    /// What `/v1/health` shows of replication.
    pub fn health(&self) -> Json {
        let last = *self.committed.borrow();
        let replicas: Vec<Json> = self
            .followers
            .iter()
            .map(|f| {
                let mut j = json!({
                    "url": f.url, "seq_no": f.acked(), "behind": last.saturating_sub(f.acked()), "in_sync": f.in_sync(),
                    "copying": f.copying.load(Ordering::Relaxed), "copies": f.copies.load(Ordering::Relaxed),
                    "verifications": f.verifications.load(Ordering::Relaxed), "divergences": f.divergences.load(Ordering::Relaxed),
                    "verify_failures": f.verify_failures.load(Ordering::Relaxed),
                });
                if let Some(e) = f.error() {
                    j["error"] = json!(e);
                }
                let v = f.verification();
                if v.as_object().is_some_and(|o| !o.is_empty()) {
                    j["verified"] = v;
                }
                j
            })
            .collect();
        let mut h = json!({
            "role": "primary", "id": self.id, "term": self.term(), "seq_no": last, "write_acks": self.opts.acks.name(),
            "global_checkpoint": self.global_checkpoint(), "verify_every_s": self.opts.verify_every.as_secs(), "replicas": replicas,
        });
        let fenced = self.fenced.load(Ordering::Relaxed);
        if fenced > 0 {
            h["replaced_by_term"] = json!(fenced);
        }
        h
    }

    pub fn metrics(&self) -> Vec<Family> {
        let last = *self.committed.borrow();
        let by = |f: &dyn Fn(&Follower) -> f64| self.followers.iter().map(|r| (format!("replica=\"{}\"", r.url), f(r))).collect();
        vec![
            Family::one("strata_replication_role", "gauge", "1 on a primary (--replicas), 2 on a replica (--replica).", 1.0),
            Family::one("strata_global_checkpoint", "gauge", "The highest sequence number every in-sync replica holds.", self.global_checkpoint() as f64),
            Family { name: "strata_replica_in_sync", ty: "gauge", help: "1 while a replica holds what the primary committed, as far as it was shipped.", values: by(&|f| f.in_sync() as u8 as f64) },
            Family { name: "strata_replica_behind", ty: "gauge", help: "Transactions the primary committed that a replica does not hold yet.", values: by(&|f| last.saturating_sub(f.acked()) as f64) },
            Family { name: "strata_replica_shipped_total", ty: "counter", help: "Transactions a replica applied from this primary.", values: by(&|f| f.shipped.load(Ordering::Relaxed) as f64) },
            Family { name: "strata_replica_failures_total", ty: "counter", help: "Calls to a replica that failed or that it refused.", values: by(&|f| f.failures.load(Ordering::Relaxed) as f64) },
            Family { name: "strata_replica_copies_total", ty: "counter", help: "Copies of the primary a replica installed: the log no longer reached it, or its rows differed.", values: by(&|f| f.copies.load(Ordering::Relaxed) as f64) },
            Family { name: "strata_replica_verifications_total", ty: "counter", help: "Times a replica's rows were compared with the primary's.", values: by(&|f| f.verifications.load(Ordering::Relaxed) as f64) },
            Family { name: "strata_replica_divergences_total", ty: "counter", help: "Times a replica's rows differed from the primary's.", values: by(&|f| f.divergences.load(Ordering::Relaxed) as f64) },
            Family { name: "strata_replica_verify_failures_total", ty: "counter", help: "Rounds that could not compare a replica's rows with the primary's.", values: by(&|f| f.verify_failures.load(Ordering::Relaxed) as f64) },
        ]
    }
}

/// A round comparing a replica's rows with its primary's: the primary's
/// version, its last transaction, and the latest request
/// ([`Primary::verify`]) the round answers.
struct Round {
    snap: Snapshot,
    seq: u64,
    asked: u64,
}

/// Ships `p`'s transactions to its replica `k` until `p` is dropped:
/// asks the replica its last transaction, sends what follows in batches,
/// sends it a copy of the primary when the log no longer reaches it or
/// its rows differ, calls it every heartbeat while there is nothing to
/// send, and starts the rounds that compare its rows with the primary's.
async fn ship(p: Weak<Primary>, k: usize) {
    let Some(mut committed) = p.upgrade().map(|p| p.committed.subscribe()) else { return };
    // The next number to send; `None` until the replica said its last.
    let mut next: Option<u64> = None;
    let mut retry = Duration::from_millis(50);
    // What follows a copy, kept until the replica has it.
    let mut _hold: Option<LogHold> = None;
    // A round under way, until the call that pins the replica's version
    // at the round's transaction; when the next round is due; and whether
    // a round found the replica's rows differ, until it installs a copy.
    let mut round: Option<Round> = None;
    let mut due = Instant::now() + p.upgrade().map_or(Duration::ZERO, |p| p.opts.verify_every);
    let mut must_copy = false;
    loop {
        let Some(p) = p.upgrade() else { return };
        let f = &p.followers[k];
        // Another primary of this database ships to it now.
        if f.owner.load(Ordering::Relaxed) != p.gen {
            return;
        }
        // A shipper of the primary this one replaced is sending it a copy.
        if f.copying.load(Ordering::Relaxed) && next.is_none() && _hold.is_none() {
            drop(p);
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        }
        if f.diverged.swap(false, Ordering::Relaxed) {
            must_copy = true;
            round = None;
        }
        // A round starts when asked or due, once the replica holds a run
        // of this primary's history and is in sync.
        let every = p.opts.verify_every;
        let asked = p.verify_asked.load(Ordering::Relaxed);
        let wanted = asked > f.verified().answered || (!every.is_zero() && Instant::now() >= due);
        if wanted && round.is_none() && !must_copy && next.is_some() && f.in_sync() && !f.verifying.load(Ordering::Relaxed) {
            let snap = p.db.snapshot();
            let seq = snap.seq_no().unwrap_or(0);
            // The replica is shipped up to the version, and not past it.
            if next.is_some_and(|n| n <= seq + 1) {
                round = Some(Round { snap, seq, asked });
                due = Instant::now() + every;
            }
        }
        let Some(numbering) = p.db.storage().numbering() else { return };
        let last = numbering.seq;
        let from = next.unwrap_or(last + 1);
        // A replica that said a last past the round's version holds rows
        // the version does not.
        if round.as_ref().is_some_and(|r| from > r.seq + 1) {
            round = None;
        }
        let txns = match (must_copy, from > last) {
            (true, _) => None,
            (false, true) => Some(Vec::new()),
            (false, false) => match p.db.storage().log_from(from, p.opts.batch_bytes) {
                LogRead::Txns(t) => Some(t),
                LogRead::Gone { .. } => None,
            },
        };
        // What it answered, the transactions sent and the last of them
        // (none for a copy), and whether the call pinned the round's
        // version.
        let (answer, n, end, pin) = match txns {
            Some(mut txns) => {
                if let Some(r) = &round {
                    txns.retain(|t| t.seq.seq <= r.seq);
                }
                let prev = SeqNo { term: numbering.term_at(from - 1), seq: from - 1 };
                let end = txns.last().map_or(prev.seq, |t| t.seq.seq);
                let pin = round.as_ref().is_some_and(|r| r.seq == end);
                let n = txns.len() as u64;
                let req = pb::ReplicateRequest {
                    term: numbering.term,
                    prev_term: prev.term,
                    prev_seq: prev.seq,
                    txns: txns.iter().map(|t| t.bytes.to_vec()).collect(),
                    global_checkpoint: p.global_checkpoint(),
                    primary: p.id.clone(),
                    behind: end < last,
                    pin,
                };
                drop(txns);
                let mut c = f.client.clone();
                let answer = match tokio::time::timeout(p.opts.call_timeout, c.replicate(req)).await {
                    Ok(Ok(r)) => Ok(r.into_inner()),
                    Ok(Err(s)) => Err(format!("{}: {}", crate::grpc::code_name(s.code()), s.message())),
                    Err(_) => Err(format!("no answer within {} s", p.opts.call_timeout.as_secs())),
                };
                (answer, n, Some(end), pin)
            }
            None => {
                // A round that found its rows differ, or a replica holding
                // a replaced primary's transactions, said why already.
                if !must_copy {
                    f.drop_out(format!("it holds transactions up to {}, and this primary's log no longer reaches them: it is sent a copy of the primary", from - 1));
                }
                p.changed();
                f.copying.store(true, Ordering::Relaxed);
                // The copy holds at least what was committed by now.
                _hold = Some(LogHold::new(p.db.clone(), last + 1));
                let answer = send_copy(&p, f, numbering.term).await;
                f.copying.store(false, Ordering::Relaxed);
                (answer.map_err(|e| format!("sending it a copy of the primary failed: {e}")), 0, None, false)
            }
        };
        let failed = match answer {
            Ok(r) => {
                if r.replica_term > numbering.term {
                    tracing::error!(replica = %f.url, term = numbering.term, replica_term = r.replica_term, "a copy holds a newer term: another primary replaced this one");
                    p.fence(r.replica_term);
                    Some(format!("it holds term {}, newer than this primary's ({}): another primary replaced this one", r.replica_term, numbering.term))
                } else if r.matched {
                    debug_assert!(end.is_none_or(|e| e == r.seq));
                    f.acked.store(r.seq, Ordering::Relaxed);
                    f.shipped.fetch_add(n, Ordering::Relaxed);
                    if end.is_none() {
                        f.copies.fetch_add(1, Ordering::Relaxed);
                        must_copy = false;
                        tracing::info!(replica = %f.url, seq_no = r.seq, "a replica installed a copy of the primary");
                    }
                    next = Some(r.seq + 1);
                    // It holds what was committed when the batch was read.
                    // A copy is taken after that, and writes go on while
                    // it is sent and installed: it is in sync if it holds
                    // what is committed now.
                    let last = match end {
                        Some(_) => last,
                        None => p.db.storage().numbering().map_or(last, |n| n.seq),
                    };
                    if r.seq >= last {
                        f.caught_up();
                        // Shipped up to the last, and not just copied: what
                        // followed the copy is no longer needed.
                        if end.is_some() {
                            _hold = None;
                        }
                    }
                    // The replica keeps its version at the round's
                    // transaction: compare them, while shipping goes on.
                    if pin {
                        if let Some(rd) = round.take() {
                            f.verifying.store(true, Ordering::Relaxed);
                            tokio::spawn(verify(Arc::downgrade(&p), k, rd));
                        }
                    }
                    p.changed();
                    retry = Duration::from_millis(50);
                    None
                } else if end.is_some() && r.seq <= last && numbering.term_at(r.seq) == r.term {
                    // It holds a run of this primary's history: send what
                    // follows its last.
                    f.acked.store(r.seq, Ordering::Relaxed);
                    next = Some(r.seq + 1);
                    continue;
                } else if end.is_some() && r.term < numbering.term {
                    // Its last transactions are of a primary this one
                    // replaced, and not this one's: a copy drops them.
                    f.drop_out(format!(
                        "its last transaction, {} of term {}, is not this primary's (its last is {last}): it holds transactions of a replaced primary of an older term, and is sent a copy of this one, which drops them",
                        r.seq, r.term
                    ));
                    p.changed();
                    must_copy = true;
                    round = None;
                    continue;
                } else {
                    Some(format!(
                        "its last transaction, {} of term {}, is not this primary's (its last is {last}): its history differs from the primary's; start it again from a checkpoint of the primary",
                        r.seq, r.term
                    ))
                }
            }
            Err(why) => Some(why),
        };
        if let Some(why) = failed {
            f.failures.fetch_add(1, Ordering::Relaxed);
            f.drop_out(why);
            p.changed();
            next = None;
            _hold = None;
            round = None;
            // A copy is a checkpoint of the database: not again at once.
            let wait = if end.is_none() { p.opts.copy_retry } else { retry };
            retry = (retry * 2).min(p.opts.retry_max);
            drop(p);
            tokio::time::sleep(wait).await;
            continue;
        }
        if round.is_none() && !must_copy && next.is_some_and(|n| n > p.db.storage().numbering().map_or(0, |n| n.seq)) {
            let (beat, wake) = (p.opts.heartbeat, f.wake.clone());
            drop(p);
            let _ = tokio::time::timeout(beat, async {
                tokio::select! {
                    _ = committed.changed() => {}
                    _ = wake.notified() => {}
                }
            })
            .await;
        }
    }
}

/// Compares replica `k`'s digests at the version its last call pinned
/// with `p`'s at the same transaction, the round's: a replica whose
/// digests differ drops out of sync, and its shipper sends it a copy of
/// the primary. Shipping goes on meanwhile.
async fn verify(p: Weak<Primary>, k: usize, r: Round) {
    let Some(pr) = p.upgrade() else { return };
    let f = &pr.followers[k];
    let req = pb::VerifyRequest { term: pr.term(), primary: pr.id.clone(), seq_no: r.seq };
    // A second more for each 100,000 rows to hash.
    let rows: u64 = r.snap.schemas().filter_map(|s| r.snap.table(&s.table)).map(|t| t.n_live_docs()).sum();
    let timeout = pr.opts.call_timeout + Duration::from_secs(rows / 100_000);
    let mut c = f.client.clone();
    drop(pr);
    let (seq, asked, snap) = (r.seq, r.asked, r.snap);
    let (theirs, ours) = tokio::join!(tokio::time::timeout(timeout, c.verify(req)), tokio::task::spawn_blocking(move || Digests::of(&snap)));
    let found = match (theirs, ours) {
        (_, Err(e)) => Err(format!("the primary's digests: {e}")),
        (_, Ok(Err(e))) => Err(format!("the primary's digests: {e}")),
        (Err(_), _) => Err(format!("no digests within {} s", timeout.as_secs())),
        (Ok(Err(s)), _) => Err(format!("{}: {}", crate::grpc::code_name(s.code()), s.message())),
        (Ok(Ok(theirs)), Ok(Ok(ours))) => Digests::from_pb(theirs.into_inner()).map(|theirs| ours.differ(&theirs)),
    };
    let Some(pr) = p.upgrade() else { return };
    let f = &pr.followers[k];
    {
        let mut v = f.verified();
        match found {
            Ok(differ) => {
                f.verifications.fetch_add(1, Ordering::Relaxed);
                if let Some(first) = differ.first() {
                    f.divergences.fetch_add(1, Ordering::Relaxed);
                    f.drop_out(format!(
                        "its rows differ from the primary's at transaction {seq}, in {} of the {SLOTS} slots (the first: {first}): it is sent a copy of the primary",
                        differ.len()
                    ));
                    f.diverged.store(true, Ordering::Relaxed);
                }
                v.at = Some((seq, Instant::now(), differ));
                v.error = None;
            }
            Err(e) => {
                f.verify_failures.fetch_add(1, Ordering::Relaxed);
                tracing::warn!(replica = %f.url, seq_no = seq, error = %e, "could not compare a replica's rows with the primary's");
                v.error = Some(e);
            }
        }
        v.answered = v.answered.max(asked);
    }
    f.verifying.store(false, Ordering::Relaxed);
    f.wake.notify_one();
    pr.changed();
}

/// Sends replica `f` a copy of `p`'s database (`Restore`) under `term`: a
/// checkpoint written beside the database and removed once sent, in
/// chunks. Returns the replica's answer.
async fn send_copy(p: &Primary, f: &Follower, term: u64) -> Result<pb::ReplicateResponse, String> {
    let path = beside(p.db.storage().path(), "copy");
    let _removed = Removed(path.clone());
    let (db, to) = (p.db.clone(), path.clone());
    tokio::task::spawn_blocking(move || db.storage().checkpoint(&to, &CheckpointOptions::default()))
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| format!("checkpoint: {e}"))?;
    let mut file = std::fs::File::open(&path).map_err(|e| format!("open the checkpoint: {e}"))?;
    let size = file.metadata().map_err(|e| e.to_string())?.len();
    // A thread reads the file a chunk ahead of the call.
    let (tx, rx) = tokio::sync::mpsc::channel(2);
    let (chunk, primary) = (p.opts.copy_chunk_bytes.max(1), p.id.clone());
    let at = path.clone();
    tokio::task::spawn_blocking(move || {
        use std::io::Read;
        let mut first = Some((term, primary, size));
        let mut sent = 0;
        loop {
            let mut data = Vec::with_capacity(chunk);
            if let Err(e) = (&mut file).take(chunk as u64).read_to_end(&mut data) {
                // The replica finds the copy short.
                tracing::warn!(path = %at.display(), error = %e, "could not read the checkpoint to copy");
                return;
            }
            if data.is_empty() && first.is_none() {
                return;
            }
            sent += data.len() as u64;
            let (term, primary, said) = first.take().unwrap_or_default();
            if tx.blocking_send(pb::RestoreChunk { term, primary, size: said, data }).is_err() || sent >= size {
                return;
            }
        }
    });
    let timeout = p.opts.call_timeout + Duration::from_secs(size >> 20);
    let mut c = f.client.clone();
    match tokio::time::timeout(timeout, c.restore(tokio_stream::wrappers::ReceiverStream::new(rx))).await {
        Ok(Ok(r)) => Ok(r.into_inner()),
        Ok(Err(s)) => Err(format!("{}: {}", crate::grpc::code_name(s.code()), s.message())),
        Err(_) => Err(format!("no answer within {} s", timeout.as_secs())),
    }
}

/// How long a replica waits for its writer, which a flush, a merge or a
/// checkpoint may hold, before refusing a batch (503).
const WRITER_WAIT: Duration = Duration::from_secs(5);

/// How long a read on a replica waits for a write it must see
/// ([`crate::service::After`]) before it is refused (503), within its
/// deadline. An in-sync replica applies a write within milliseconds of
/// its primary; one a second behind is better skipped for the primary.
pub const READ_WAIT: Duration = Duration::from_secs(1);

/// A replica: applies what its primary ships.
pub struct Follow {
    db: Arc<Db>,
    file: Option<PathBuf>,
    kept: tokio::sync::Mutex<Kept>,
    /// The global checkpoint its primary last sent.
    global_checkpoint: AtomicU64,
    /// Transactions applied, copies of the primary installed, and when its
    /// primary last called.
    applied: AtomicU64,
    restores: AtomicU64,
    called_at: Mutex<Option<Instant>>,
    /// Whether it serves reads ([`Follow::readable`]).
    serves_reads: AtomicBool,
    /// The last number committed, as the database's commit hook says, for
    /// reads waiting for a write ([`Follow::holding`]).
    committed: Arc<watch::Sender<u64>>,
    /// Reads that waited for a write, and reads refused.
    read_waits: AtomicU64,
    reads_refused: AtomicU64,
    /// The version a call from its primary pinned for the `Verify` that
    /// follows, and the digests it computed for its primary.
    pinned: Mutex<Option<Snapshot>>,
    digests: AtomicU64,
}

impl Follow {
    /// A replica of `db`, keeping its id and the primary it follows in
    /// `file` (none: in memory).
    pub fn open(db: Arc<Db>, file: Option<&Path>) -> Result<Follow, String> {
        if db.storage().numbering().is_none() {
            return Err("a replica numbers its writes as its primary does: start it with --seq-no".into());
        }
        let mut kept = Kept::open(file)?;
        if kept.role != Some(KeptRole::Replica) {
            kept.role = Some(KeptRole::Replica);
            kept.save(file).map_err(|e| format!("keep the replication role: {e}"))?;
        }
        let committed = Arc::new(watch::channel(db.storage().numbering().map_or(0, |n| n.seq)).0);
        let hook = committed.clone();
        db.storage().on_commit(move |n| {
            hook.send_replace(n.seq);
        });
        Ok(Follow {
            db,
            file: file.map(Path::to_path_buf),
            kept: tokio::sync::Mutex::new(kept),
            global_checkpoint: AtomicU64::new(0),
            applied: AtomicU64::new(0),
            restores: AtomicU64::new(0),
            called_at: Mutex::new(None),
            serves_reads: AtomicBool::new(false),
            committed,
            read_waits: AtomicU64::new(0),
            reads_refused: AtomicU64::new(0),
            pinned: Mutex::new(None),
            digests: AtomicU64::new(0),
        })
    }

    /// Whether a read may take a new snapshot here: once a call from its
    /// primary since this server started reached the primary's last
    /// transaction, until a call finds it behind, it fails to apply one or
    /// it installs a copy. Refused otherwise (503), as it is catching up.
    pub fn readable(&self) -> Result<(), ApiError> {
        if self.serves_reads.load(Ordering::Relaxed) {
            return Ok(());
        }
        self.reads_refused.fetch_add(1, Ordering::Relaxed);
        let why = match *self.called_at.lock().unwrap_or_else(|e| e.into_inner()) {
            None => "its primary has not called since it started",
            Some(_) => "it is catching up with its primary",
        };
        Err(ApiError { status: 503, message: format!("this replica serves no reads while {why}: read from another copy or the primary") })
    }

    /// The latest version, once it holds transaction `seq`, waiting for it
    /// until `until`: refused then (503), as the primary holds it.
    pub async fn holding(&self, seq: u64, until: Instant) -> Result<Snapshot, ApiError> {
        let mut committed = self.committed.subscribe();
        let mut waited = false;
        loop {
            let snap = self.db.snapshot();
            let at = snap.seq_no().unwrap_or(0);
            if at >= seq {
                return Ok(snap);
            }
            if !waited {
                waited = true;
                self.read_waits.fetch_add(1, Ordering::Relaxed);
            }
            let now = Instant::now();
            if now >= until {
                self.reads_refused.fetch_add(1, Ordering::Relaxed);
                return Err(ApiError {
                    status: 503,
                    message: format!("this replica holds transactions up to {at}, and the read must see transaction {seq}, which it has not applied in time: read from the primary"),
                });
            }
            // A copy installed tells no hook: look again every few
            // milliseconds too.
            let _ = tokio::time::timeout((until - now).min(Duration::from_millis(10)), committed.changed()).await;
        }
    }

    /// The primary it follows, if one has called.
    pub async fn follows(&self) -> Option<String> {
        self.kept.lock().await.follows.as_ref().map(|(p, _)| p.clone())
    }

    /// Makes this replica the primary of a new term, shipping as `opts`
    /// says: once the copies in `opts.replicas` that answer hold no
    /// transaction past this replica's last, nor a newer term (409
    /// otherwise, unless `force`: the new primary would not hold those
    /// transactions, and a copy of it drops them), it begins a term above
    /// every term it knows, under the lock calls from its primary take,
    /// so that a call of the old primary that follows is answered with
    /// the new term. Answers the new term, its last transaction and what
    /// each copy said.
    pub async fn promote(&self, opts: PrimaryOptions, force: bool) -> Result<(Arc<Primary>, Json), ApiError> {
        let mine = self.db.storage().numbering().ok_or_else(|| ApiError::internal("the replica does not number its writes"))?;
        let asks = opts.replicas.iter().map(|url| async {
            let mut c = coord::client(url, opts.token.as_ref(), opts.ca.as_deref())?;
            let hello = c.hello(pb::HelloRequest { min_protocol: crate::grpc::MIN_PROTOCOL, max_protocol: crate::grpc::PROTOCOL });
            match tokio::time::timeout(opts.call_timeout, hello).await {
                Ok(Ok(h)) => Ok(h.into_inner()),
                Ok(Err(s)) => Err(ApiError::bad(format!("{}: {}", crate::grpc::code_name(s.code()), s.message()))),
                Err(_) => Err(ApiError::bad(format!("no answer within {} s", opts.call_timeout.as_secs()))),
            }
        });
        let mut copies = Vec::new();
        let mut ahead = Vec::new();
        for (url, a) in opts.replicas.iter().zip(futures::future::join_all(asks).await) {
            copies.push(match a {
                Ok(h) => {
                    if h.seq_no > mine.seq || h.term > mine.term {
                        ahead.push(format!("{url} holds transactions up to {} (term {})", h.seq_no, h.term));
                    }
                    json!({"url": url, "role": h.role().as_str_name().to_lowercase(), "term": h.term, "seq_no": h.seq_no})
                }
                Err(e) => json!({"url": url, "error": e.message}),
            });
        }
        if !ahead.is_empty() && !force {
            return Err(conflict(format!(
                "{}, past this replica's last ({} of term {}): the new primary would not hold those transactions, and a copy of it would drop them. Promote the copy with the latest transaction, or pass force",
                ahead.join("; "),
                mine.seq,
                mine.last().term
            )));
        }
        let mut kept = self.kept.lock().await;
        let term = kept.newest(self.db.storage().numbering().map_or(0, |n| n.term)) + 1;
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || crate::service::wait_for_writer(Instant::now() + WRITER_WAIT, || db.storage().begin_term(term).map_err(ApiError::from)))
            .await
            .map_err(ApiError::internal)??;
        let next = Kept { id: kept.id.clone(), role: Some(KeptRole::Primary { replicas: opts.replicas.clone(), acks: opts.acks }), ..Kept::default() };
        next.save(self.file.as_deref()).map_err(|e| ApiError::internal(format!("keep the replication id: {e}")))?;
        *kept = next.clone();
        let at = self.db.storage().numbering().unwrap_or_default();
        tracing::info!(term, seq_no = at.seq, replicas = %opts.replicas.join(", "), write_acks = opts.acks.name(), "promoted: the primary of a new term");
        let p = Primary::run(self.db.clone(), self.file.as_deref(), next, opts).map_err(ApiError::internal)?;
        Ok((p, json!({"term": at.term, "seq_no": at.seq, "replicas": copies})))
    }

    /// Applies a primary's batch (`Replicate`), one batch at a time.
    pub async fn replicate(&self, r: pb::ReplicateRequest) -> Result<pb::ReplicateResponse, ApiError> {
        let mut kept = self.kept.lock().await;
        let numbering = self.db.storage().numbering().ok_or_else(|| ApiError::internal("the replica does not number its writes"))?;
        let term = numbering.term.max(kept.follows.as_ref().map_or(0, |f| f.1));
        let answer = |matched: bool, at: SeqNo, replica_term: u64| pb::ReplicateResponse { matched, seq: at.seq, term: at.term, replica_term };
        if r.term < term {
            return Ok(answer(false, numbering.last(), term));
        }
        self.follow(&mut kept, &r.primary, r.term)?;
        let (db, prev, txns) = (self.db.clone(), SeqNo { term: r.prev_term, seq: r.prev_seq }, r.txns);
        let n = txns.len() as u64;
        let applied = tokio::task::spawn_blocking(move || {
            let txns: Vec<&[u8]> = txns.iter().map(|t| &t[..]).collect();
            // A flush or a merge may hold the writer for a moment.
            crate::service::wait_for_writer(Instant::now() + WRITER_WAIT, || db.storage().apply_shipped(prev, &txns).map_err(ApiError::from))
        })
        .await
        .map_err(ApiError::internal)
        .and_then(|r| r);
        let replica_term = self.db.storage().numbering().map_or(0, |n| n.term).max(r.term);
        // It serves reads while what it holds reaches the primary's last.
        self.serves_reads.store(matches!(applied, Ok(Applied::At(_))) && !r.behind, Ordering::Relaxed);
        // Nothing else writes here: the latest version holds the batch's
        // last transaction, and no later one.
        if let (true, Ok(Applied::At(at))) = (r.pin, &applied) {
            let snap = self.db.snapshot();
            if snap.seq_no() == Some(at.seq) {
                *self.pinned.lock().unwrap_or_else(|e| e.into_inner()) = Some(snap);
            }
        }
        Ok(match applied? {
            Applied::At(at) => {
                self.applied.fetch_add(n, Ordering::Relaxed);
                self.global_checkpoint.store(r.global_checkpoint.min(at.seq), Ordering::Relaxed);
                answer(true, at, replica_term)
            }
            Applied::Mismatch { seq, term } => answer(false, SeqNo { term, seq }, replica_term),
        })
    }

    /// Its digests at the version a call from its primary pinned
    /// (`Verify`): refused (409) for a primary or term it does not follow,
    /// and when it holds no version pinned at that transaction. The
    /// version is let go once answered.
    pub async fn verify(&self, r: pb::VerifyRequest) -> Result<pb::VerifyResponse, ApiError> {
        match &self.kept.lock().await.follows {
            Some((id, t)) if *id == r.primary && *t == r.term => {}
            f => {
                let follows = f.as_ref().map_or("no primary".to_string(), |(id, t)| format!("primary {id} in term {t}"));
                return Err(conflict(format!("this replica follows {follows}, not primary {} in term {}", r.primary, r.term)));
            }
        }
        let snap = {
            let mut pinned = self.pinned.lock().unwrap_or_else(|e| e.into_inner());
            match pinned.as_ref().map(|s| s.seq_no().unwrap_or(0)) {
                Some(at) if at == r.seq_no => pinned.take(),
                at => {
                    let holds = at.map_or("none".to_string(), |at| format!("one at {at}"));
                    return Err(conflict(format!(
                        "this replica holds no version pinned at transaction {} (it holds {holds}): its primary pins one with the call that ships that transaction",
                        r.seq_no
                    )));
                }
            }
        };
        let d = tokio::task::spawn_blocking(move || snap.map(|s| Digests::of(&s))).await.map_err(ApiError::internal)?;
        let d = d.ok_or_else(|| ApiError::internal("the pinned version went"))??;
        self.digests.fetch_add(1, Ordering::Relaxed);
        Ok(d.to_pb())
    }

    /// Follows `primary` in `term`, which is not older than this
    /// replica's, keeping it on disk: refused when another primary is
    /// followed in the same term.
    fn follow(&self, kept: &mut Kept, primary: &str, term: u64) -> Result<(), ApiError> {
        match &kept.follows {
            Some((id, t)) if *t == term && id != primary => {
                return Err(conflict(format!("this replica follows primary {id} in term {t}, and primary {primary} ships in the same term: a shard has one primary in a term")));
            }
            Some((_, t)) if *t == term => {}
            _ => {
                let next = Kept { id: kept.id.clone(), follows: Some((primary.to_string(), term)), replaced_by: None, role: Some(KeptRole::Replica) };
                next.save(self.file.as_deref()).map_err(|e| ApiError::internal(format!("keep the primary followed: {e}")))?;
                *kept = next;
            }
        }
        *self.called_at.lock().unwrap_or_else(|e| e.into_inner()) = Some(Instant::now());
        Ok(())
    }

    /// Puts the copy of its primary that `chunks` carry (`Restore`) in
    /// place of its database: writes it beside the database, opens it,
    /// checking every segment's checksum, and installs it in one commit
    /// ([`strata::storage::Database::install_copy`]). The primary's term
    /// is checked as [`Follow::replicate`] checks it. Answers the copy's
    /// last transaction.
    pub async fn restore(&self, mut chunks: tonic::Streaming<pb::RestoreChunk>) -> Result<pb::ReplicateResponse, ApiError> {
        let cut = |s: tonic::Status| ApiError::bad(format!("the copy was cut off: {}", s.message()));
        let first = chunks.message().await.map_err(cut)?.ok_or_else(|| ApiError::bad("the copy is empty"))?;
        let mut kept = self.kept.lock().await;
        let numbering = self.db.storage().numbering().ok_or_else(|| ApiError::internal("the replica does not number its writes"))?;
        let term = numbering.term.max(kept.follows.as_ref().map_or(0, |f| f.1));
        if first.term < term {
            return Ok(pb::ReplicateResponse { matched: false, seq: numbering.seq, term: numbering.last().term, replica_term: term });
        }
        self.follow(&mut kept, &first.primary, first.term)?;
        // Its rows are behind its primary's log, or differ: no reads
        // until the copy is in and a call finds it caught up.
        self.serves_reads.store(false, Ordering::Relaxed);
        *self.pinned.lock().unwrap_or_else(|e| e.into_inner()) = None;
        let path = beside(self.db.storage().path(), "restore");
        let _removed = Removed(path.clone());
        let size = first.size;
        let mut file = std::fs::File::create_new(&path).map_err(|e| ApiError::internal(format!("write the copy beside the database: {e}")))?;
        let (mut got, mut data) = (0u64, Some(first.data));
        loop {
            let d = match data.take() {
                Some(d) => d,
                None => match chunks.message().await.map_err(cut)? {
                    Some(c) => c.data,
                    None => break,
                },
            };
            got += d.len() as u64;
            if got > size {
                return Err(ApiError::bad(format!("the copy is longer than the {size} bytes it said")));
            }
            file = tokio::task::spawn_blocking(move || {
                use std::io::Write;
                file.write_all(&d).map(|_| file)
            })
            .await
            .map_err(ApiError::internal)?
            .map_err(|e| ApiError::internal(format!("write the copy beside the database: {e}")))?;
        }
        if got != size {
            return Err(ApiError::bad(format!("the copy ended at {got} of its {size} bytes")));
        }
        let db = self.db.clone();
        let at = tokio::task::spawn_blocking(move || {
            file.sync_all().map_err(|e| ApiError::internal(format!("write the copy beside the database: {e}")))?;
            drop(file);
            let copy = Database::open(&path, DbOptions { layout: Layout::SingleFile, verify_checksums: true, ..DbOptions::default() }).map_err(|e| ApiError::bad(format!("the copy: {e}")))?;
            // Opening falls back past a damaged version; a copy must not.
            if let Some(what) = copy.damaged() {
                return Err(ApiError::bad(format!("the copy is damaged: {what}")));
            }
            // A flush or a merge may hold the writer for a moment.
            crate::service::wait_for_writer(Instant::now() + WRITER_WAIT, || db.storage().install_copy(&copy).map_err(ApiError::from))
        })
        .await
        .map_err(ApiError::internal)??;
        self.restores.fetch_add(1, Ordering::Relaxed);
        // The primary wrote on while it sent the copy: its next call says
        // whether this replica reaches its last.
        self.serves_reads.store(false, Ordering::Relaxed);
        tracing::info!(primary = %first.primary, seq_no = at.seq, "installed a copy of the primary");
        let replica_term = self.db.storage().numbering().map_or(0, |n| n.term).max(first.term);
        Ok(pb::ReplicateResponse { matched: true, seq: at.seq, term: at.term, replica_term })
    }

    /// Whether it serves reads now: its primary's last call reached the
    /// primary's last transaction.
    pub fn serves_reads(&self) -> bool {
        self.serves_reads.load(Ordering::Relaxed)
    }

    /// What `/v1/health` shows of replication.
    pub async fn health(&self) -> Json {
        let kept = self.kept.lock().await.clone();
        let n = self.db.storage().numbering().unwrap_or_default();
        let mut h = json!({
            "role": "replica", "id": kept.id, "term": n.term, "seq_no": n.seq, "global_checkpoint": self.global_checkpoint.load(Ordering::Relaxed),
            "restores": self.restores.load(Ordering::Relaxed), "serves_reads": self.serves_reads.load(Ordering::Relaxed),
            "read_waits": self.read_waits.load(Ordering::Relaxed), "reads_refused": self.reads_refused.load(Ordering::Relaxed),
            "digests": self.digests.load(Ordering::Relaxed),
        });
        if let Some((primary, term)) = kept.follows {
            h["follows"] = json!({"primary": primary, "term": term});
        }
        if let Some(at) = *self.called_at.lock().unwrap_or_else(|e| e.into_inner()) {
            h["called_ms_ago"] = json!(at.elapsed().as_millis() as u64);
        }
        h
    }

    pub fn metrics(&self) -> Vec<Family> {
        vec![
            Family::one("strata_replication_role", "gauge", "1 on a primary (--replicas), 2 on a replica (--replica).", 2.0),
            Family::one("strata_global_checkpoint", "gauge", "The highest sequence number every in-sync replica holds, as the primary last said.", self.global_checkpoint.load(Ordering::Relaxed) as f64),
            Family::one("strata_replica_applied_total", "counter", "Transactions this replica applied from its primary.", self.applied.load(Ordering::Relaxed) as f64),
            Family::one("strata_replica_restores_total", "counter", "Copies of its primary this replica installed.", self.restores.load(Ordering::Relaxed) as f64),
            Family::one("strata_replica_serves_reads", "gauge", "1 while this replica serves reads: its primary's last call reached the primary's last transaction.", self.serves_reads.load(Ordering::Relaxed) as u8 as f64),
            Family::one("strata_replica_read_waits_total", "counter", "Reads that waited for this replica to apply a write they must see (min_seq_no).", self.read_waits.load(Ordering::Relaxed) as f64),
            Family::one("strata_replica_reads_refused_total", "counter", "Reads refused while this replica caught up, or because it had not applied a write they must see in time.", self.reads_refused.load(Ordering::Relaxed) as f64),
            Family::one("strata_replica_digests_total", "counter", "Digests of its rows this replica computed for its primary to compare (Verify).", self.digests.load(Ordering::Relaxed) as f64),
        ]
    }
}
