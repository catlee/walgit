//! GC support (`docs/GC.md`, D42): the live-pack-set existence probe the
//! connectivity-lite audit walks against.
//!
//! Existence is membership in some live pack's `.idx`. The idx comes from
//! wherever it already is: the installed copy in `objects/pack/` (Local/Link
//! plans, history packs), else the remote reader's `remote-idx/` copy,
//! downloaded now if absent (`remote::ensure_indexes` — the exact machinery
//! the web API uses on too-large repos). No pack data is touched.

use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::time::SystemTime;

use futures::StreamExt;
use walgit_proto::v1::{FsckReport, PackRef};
use walgit_store::{ObjectMeta, ObjectStore, ObjectStoreExt};

use crate::error::WalError;
use crate::handle::RepoHandle;
use crate::progress::Reporter;

/// Open `.idx` files covering the live pack set: probe with [`LiveIndexes::contains`].
pub struct LiveIndexes {
    files: Vec<gix_pack::index::File>,
}

impl LiveIndexes {
    /// Whether any live pack claims `oid`.
    pub fn contains(&self, oid: &gix_hash::oid) -> bool {
        self.files.iter().any(|f| f.lookup(oid).is_some())
    }
}

/// Open the live pack set's indexes for `handle` (manifest as of the caller's
/// sync). Prefers installed idx files; downloads the rest into `remote-idx/`.
pub async fn live_indexes(
    handle: &RepoHandle,
    manifest: &walgit_proto::v1::Manifest,
    reporter: &Reporter,
) -> Result<LiveIndexes, WalError> {
    let repo_dir = handle.local().path().to_path_buf();
    let hash = handle.local().object_format().kind();

    // Installed idx (objects/pack) when present; everything else through the
    // remote reader's directory.
    let mut paths: Vec<PathBuf> = Vec::with_capacity(manifest.packs.len());
    let mut need_remote: Vec<PackRef> = Vec::new();
    for p in &manifest.packs {
        let installed = repo_dir
            .join("objects")
            .join("pack")
            .join(format!("pack-{}.idx", p.checksum));
        if installed.is_file() {
            paths.push(installed);
        } else {
            need_remote.push(p.clone());
        }
    }
    if !need_remote.is_empty() {
        let keep: std::collections::HashSet<String> =
            manifest.packs.iter().map(|p| p.checksum.clone()).collect();
        let dir =
            crate::remote::ensure_indexes(handle.store(), &need_remote, &keep, &repo_dir, reporter)
                .await?;
        for p in &need_remote {
            paths.push(dir.join(format!("{}.idx", p.checksum)));
        }
    }

    let files = tokio::task::spawn_blocking(move || {
        paths
            .into_iter()
            .map(|path| {
                gix_pack::index::File::at(&path, hash).map_err(|e| {
                    WalError::Corrupt(format!("open pack index {}: {e}", path.display()))
                })
            })
            .collect::<Result<Vec<_>, _>>()
    })
    .await
    .map_err(|e| WalError::Corrupt(e.to_string()))??;
    Ok(LiveIndexes { files })
}

// ---------------------------------------------------------------------------
// gc-sweep (docs/GC.md §4)
// ---------------------------------------------------------------------------

/// One sweep pass over the bucket, bounded and replayable (D22). Stateless:
/// everything is recomputed from (manifest, one horizon checkpoint, LIST).
#[derive(Debug, Default, serde::Serialize)]
pub struct SweepOutcome {
    pub listed: u64,
    pub referenced: u64,
    /// Seq of the horizon checkpoint the pass judged against (0 = none exists
    /// yet: nothing WAL-side is deletable, fail closed).
    pub horizon_seq: u64,
    /// Objects provably dead since the horizon (deletable modulo the gate).
    pub candidates: u64,
    /// Candidates blocked on a missing/stale clean audit (WAL objects only).
    pub gated: u64,
    pub deleted: u64,
    pub deleted_bytes: u64,
    pub deleted_by_kind: BTreeMap<&'static str, u64>,
    /// Deletable candidates left for the next pass (`max_deletes_per_pass`).
    pub remaining: u64,
}

/// The prefixes the sweep lists. Everything else in the repo prefix
/// (`policy.json`, `fsck.pb`, `gc/*`, `events/`, `leases/`, `maintain/`,
/// `bundles/`, `lfs/`, the manifest) is never touched.
const SWEEP_PREFIXES: [&str; 4] = [
    walgit_proto::keys::WAL_DIR,
    walgit_proto::keys::CHECKPOINTS_DIR,
    walgit_proto::keys::LOG_DIR,
    walgit_proto::keys::RENDER_CACHE_DIR,
];

fn kind_of(key: &str) -> &'static str {
    if key.starts_with(walgit_proto::keys::CACHE_DIR) {
        "render"
    } else if key.starts_with(walgit_proto::keys::CHECKPOINTS_DIR) {
        "checkpoint"
    } else if key.starts_with(walgit_proto::keys::LOG_DIR) {
        "log"
    } else if std::path::Path::new(key)
        .extension()
        .is_some_and(|e| e == "pack")
    {
        "pack"
    } else {
        "side-file"
    }
}

/// Store keys the manifest references: live packs (all side-files), the
/// current checkpoint's directory, live log segments.
fn referenced_keys(manifest: &walgit_proto::v1::Manifest) -> HashSet<String> {
    use walgit_proto::keys;
    let mut set = HashSet::new();
    for p in &manifest.packs {
        set.insert(keys::pack_key(&p.checksum));
        set.insert(keys::idx_key(&p.checksum));
        set.insert(keys::rev_key(&p.checksum));
        set.insert(keys::bitmap_key(&p.checksum));
        set.insert(keys::commit_graph_key(&p.checksum));
    }
    for s in &manifest.log_segments {
        set.insert(keys::log_segment_key(s.first_seq));
    }
    set
}

fn coord_err(e: &walgit_store::CoordError) -> WalError {
    WalError::Corrupt(e.to_string())
}

/// One bounded gc-sweep pass (docs/GC.md §4). Stateless — the WAL already
/// wrote the bookkeeping down:
///
/// 1. refs sync → manifest (live now); read the audit verdicts; LIST wal/,
///    checkpoints/, log/, cache/api/v1/ (with store mtimes).
/// 2. Find the **horizon checkpoint** `S_h`: the newest checkpoint uploaded at
///    or before `now − gc.retention` (from the LIST), and GET its pack set
///    (live at `S_h`). No such checkpoint → nothing WAL-side is deletable
///    (fail closed; young repos have nothing to collect).
/// 3. A WAL object is a candidate iff its pack is **out of `live(now)` and
///    `live(S_h)` and uploaded before `S_h`** — provably dead for the whole
///    provenance window; anything `materialize --at-seq` could need since the
///    horizon is in one of those sets or younger than `S_h`. Old checkpoints
///    (seq below `S_h`) and log segments wholly below `S_h`'s replay range follow the
///    same horizon; render-cache entries age by their own mtime against
///    `gc.render_cache_ttl`.
/// 4. The gate, WAL objects only: the **newest audit verdict is clean and
///    audited seq at/past `S_h.seq`** — one comparison per pass; any standing finding
///    freezes deletion (superseded packs are the repair source).
/// 5. Re-read the manifest and drop candidates it now references (race-1
///    guard), then delete with the LIST's version, at most
///    `max_deletes_per_pass`; leftovers are next pass's work.
pub async fn sweep(
    handle: &RepoHandle,
    now: SystemTime,
    log: &(dyn Fn(String) + Send + Sync),
) -> Result<SweepOutcome, WalError> {
    let cfg = handle.effective_config();
    let gc = &cfg.gc;
    drop(handle.sync_refs().await?);
    let manifest = handle.manifest();
    let store = handle.store();
    // Independent control-plane reads share one round: the lite verdict plus
    // all four listings. Full fsck is deliberately excluded from the gate —
    // it sees loose cached objects, not just the manifest's live pack set.
    let verdict_read =
        walgit_store::coord::get_message::<FsckReport>(store, walgit_proto::keys::GC_AUDIT);
    let listings = futures::future::join_all(SWEEP_PREFIXES.map(|prefix| async move {
        let mut out = Vec::new();
        let mut stream = store.list(prefix, None);
        while let Some(meta) = stream.next().await {
            out.push(meta?);
        }
        Ok::<Vec<ObjectMeta>, WalError>(out)
    }));
    let (verdict, listings) = tokio::join!(verdict_read, listings);
    let verdict = verdict
        .map_err(|e| coord_err(&e))?
        .map(|(_, report)| report);
    let mut listed = Vec::new();
    for result in listings {
        listed.extend(result?);
    }

    let mut referenced = referenced_keys(&manifest);
    if let Some(cp) = &manifest.checkpoint {
        referenced.insert(cp.key.clone());
    }
    // Protect the whole current checkpoint directory (checkpoint.pb, refs.pb,
    // anything import left beside them).
    let current_cp_dir = manifest
        .checkpoint
        .as_ref()
        .map(|cp| walgit_proto::keys::checkpoint_dir(cp.seq));
    let live_now: HashSet<&str> = manifest.packs.iter().map(|p| p.checksum.as_str()).collect();

    let mut out = SweepOutcome {
        listed: listed.len() as u64,
        ..SweepOutcome::default()
    };

    // Committed checkpoint chain: follow `previous` from the manifest's
    // current checkpoint until the first checkpoint at/before the retention
    // cutoff. Abandoned import/checkpoint objects from LIST are never in this
    // chain and can never become the horizon.
    // Local `now` vs backend mtimes: support up to five minutes of host clock
    // skew without shortening retention (scaled down for rig windows).
    let clock_skew = (gc.retention / 2).min(std::time::Duration::from_mins(5));
    let cutoff = now.checked_sub(gc.retention + clock_skew);
    let mut next = manifest.checkpoint.clone();
    let mut seen_checkpoint_seqs = HashSet::new();
    let mut horizon_seq = 0u64;
    let mut horizon_checkpoint: Option<walgit_proto::v1::Checkpoint> = None;
    while let Some(cp_ref) = next.take() {
        if !seen_checkpoint_seqs.insert(cp_ref.seq) {
            return Err(WalError::Corrupt(format!(
                "checkpoint chain cycle at seq {}",
                cp_ref.seq
            )));
        }
        let Some((_, cp)) = walgit_store::coord::get_message::<walgit_proto::v1::Checkpoint>(
            store,
            &cp_ref.key,
        )
        .await
        .map_err(|e| coord_err(&e))?
        else {
            // A committed checkpoint vanished: fail closed. No WAL deletion
            // this pass; render-cache entries remain independently sweepable.
            log(format!(
                "committed checkpoint {} vanished; no WAL deletion this pass",
                cp_ref.key
            ));
            break;
        };
        let created = cp_ref
            .created_at
            .as_ref()
            .or(cp.created_at.as_ref())
            .map(walgit_proto::time::to_system);
        next.clone_from(&cp.previous);
        if cutoff.zip(created).is_some_and(|(h, at)| at <= h) {
            horizon_seq = cp_ref.seq;
            horizon_checkpoint = Some(cp);
            break;
        }
    }
    out.horizon_seq = horizon_seq;
    if horizon_seq == 0 {
        log(format!(
            "no committed checkpoint older than retention ({} listed objects): only render cache sweepable",
            out.listed
        ));
    }

    // Replay every retained log segment from S_h to the current manifest.
    // This is the exact provenance set: packs live at S_h plus every pack
    // introduced by an entry since it. It covers packs born and superseded
    // between checkpoints and arbitrary compact publication pauses — no mtime
    // or checkpoint-union inference.
    let mut log_metas: Vec<(u64, &ObjectMeta)> = listed
        .iter()
        .filter_map(|m| log_segment_seq_of(&m.key).map(|seq| (seq, m)))
        .collect();
    log_metas.sort_unstable_by_key(|(seq, _)| *seq);
    let mut replay_start = if horizon_seq > 0 {
        log_metas
            .iter()
            .filter(|(seq, _)| *seq <= horizon_seq)
            .map(|(seq, _)| *seq)
            .max()
    } else {
        None
    };
    let replay_segments: Vec<&ObjectMeta> = replay_start
        .map(|start| {
            log_metas
                .iter()
                .filter(|(seq, _)| *seq >= start)
                .map(|(_, meta)| *meta)
                .collect()
        })
        .unwrap_or_default();
    let log_gets = replay_segments.iter().map(|meta| store.get_bytes(&meta.key));
    let mut entries = Vec::new();
    let mut replay_complete = horizon_seq > 0;
    for r in futures::future::join_all(log_gets).await {
        match r? {
            Some((_, bytes)) => {
                let (mut decoded, _) = walgit_proto::frame::decode_entries(&bytes)
                    .map_err(|e| WalError::Corrupt(format!("decoding retained log: {e}")))?;
                entries.append(&mut decoded);
            }
            None => replay_complete = false,
        }
    }
    entries.sort_unstable_by_key(|e| e.seq);

    let mut protected_window: HashSet<String> = horizon_checkpoint
        .iter()
        .flat_map(|cp| cp.packs.iter().map(|p| p.checksum.clone()))
        .collect();
    let mut replay_live = protected_window.clone();
    for entry in &entries {
        // A claimed log slot above manifest head is a publisher paused after
        // writing its intent but before the manifest CAS. Protect its pack:
        // if the publisher resumes, it can commit without any post-CAS work.
        // A later writer either commits it or burns past it, at which point
        // seq <= head and an unreferenced segment/pack becomes collectable.
        if entry.seq > manifest.head_seq {
            if let Some(pack) = &entry.pack {
                protected_window.insert(pack.checksum.clone());
            }
            continue;
        }
        if entry.seq <= horizon_seq {
            continue;
        }
        if let Some(pack) = &entry.pack {
            protected_window.insert(pack.checksum.clone());
            replay_live.insert(pack.checksum.clone());
        }
        for checksum in &entry.supersedes {
            replay_live.remove(checksum);
        }
    }
    let current_live: HashSet<String> =
        manifest.packs.iter().map(|p| p.checksum.clone()).collect();
    if horizon_seq > 0 && (!replay_complete || replay_live != current_live) {
        // Missing retained log, abandoned/corrupt checkpoint chain, or replay
        // mismatch: the proof is incomplete. Fail closed for the whole
        // retention window. Dropping the horizon freezes WAL packs (no
        // provenance set) and checkpoints below it; dropping `replay_start`
        // also freezes log-segment deletion, so a horizon we no longer trust
        // never authorizes deleting the log that would rebuild it.
        log(format!(
            "retained WAL replay does not reach manifest head {}; no WAL, checkpoint, or log deletion this pass",
            manifest.head_seq
        ));
        horizon_seq = 0;
        out.horizon_seq = 0;
        protected_window.clear();
        replay_start = None;
    }

    // Causal deletion gate: the lite audit alone (full fsck sees loose cache)
    // must be clean at THIS sweep's coherent manifest head. No wall-clock
    // freshness heuristic and no pre-race verdict can authorize deletion.
    let gate_open = horizon_seq > 0
        && verdict.as_ref().is_some_and(|v| {
            v.missing_total == 0 && v.problems == 0 && v.seq >= manifest.head_seq
        });

    // Segments before the one containing S_h are outside every retained replay.
    let seg_dead = |first_seq: u64| -> bool {
        replay_start.is_some_and(|start| first_seq < start)
    };

    let mut deletable: Vec<&ObjectMeta> = Vec::new();
    for m in &listed {
        let is_ref = referenced.contains(&m.key)
            || current_cp_dir.as_deref().is_some_and(|d| m.key.starts_with(d));
        if is_ref {
            out.referenced += 1;
            continue;
        }
        if m.key.starts_with(walgit_proto::keys::CACHE_DIR) {
            // Render cache: derived, aged by its own write time, no gate.
            let old = m
                .updated
                .is_some_and(|u| now.duration_since(u).unwrap_or_default() >= gc.render_cache_ttl);
            if old {
                deletable.push(m);
            }
            continue;
        }
        if m.key.starts_with(walgit_proto::keys::CHECKPOINTS_DIR) {
            // Checkpoints strictly below the horizon checkpoint are no longer
            // any replay's starting point within the window.
            // Only committed-chain checkpoints below S_h are obsolete;
            // abandoned checkpoints are also below no committed replay and
            // become collectable once older than retention.
            let dead = checkpoint_seq_of(&m.key).is_some_and(|seq| {
                horizon_seq > 0 && seq < horizon_seq
            });
            if dead {
                out.candidates += 1;
                deletable.push(m);
            }
            continue;
        }
        if m.key.starts_with(walgit_proto::keys::LOG_DIR) {
            if log_segment_seq_of(&m.key).is_some_and(seg_dead) {
                out.candidates += 1;
                deletable.push(m);
            }
            continue;
        }
        // wal/: dead iff absent from the exact retained provenance set
        // (live at S_h + every pack-introducing log entry since), absent now,
        // and old enough. The mtime check ages true uncommitted orphans; log
        // provenance, not mtime, protects committed historical packs.
        let Some(sha) = wal_checksum_of(&m.key) else {
            continue;
        };
        let old = cutoff
            .zip(m.updated)
            .is_some_and(|(h, updated)| updated <= h);
        let dead = old && !live_now.contains(sha) && !protected_window.contains(sha);
        if !dead {
            continue;
        }
        out.candidates += 1;
        if gate_open {
            deletable.push(m);
        } else {
            out.gated += 1;
        }
    }

    // Race-1 guard: a publisher may have referenced a dead-looking object
    // between our LIST and now. Re-read the manifest fresh and drop those.
    if !deletable.is_empty() {
        let fresh = walgit_store::coord::get_message::<walgit_proto::v1::Manifest>(
            store,
            walgit_proto::keys::MANIFEST,
        )
        .await
        .map_err(|e| coord_err(&e))?
        .map(|(_, m)| m)
        .ok_or_else(|| WalError::Corrupt("manifest vanished mid-sweep".into()))?;
        let mut fresh_ref = referenced_keys(&fresh);
        if let Some(cp) = &fresh.checkpoint {
            fresh_ref.insert(cp.key.clone());
        }
        deletable.retain(|m| {
            let re_referenced = fresh_ref.contains(&m.key);
            if re_referenced {
                metrics::counter!("walgit_gc_reference_races_total").increment(1);
            }
            !re_referenced
        });
    }

    let cap = gc.max_deletes_per_pass;
    if deletable.len() > cap {
        out.remaining = (deletable.len() - cap) as u64;
        deletable.truncate(cap);
    }
    for batch in deletable.chunks(16) {
        enum Outcome {
            Deleted,
            /// 412: the object was rewritten since our LIST — the race-1 fence
            /// worked. It survives and is judged afresh next pass.
            Fenced,
            /// Already gone (another sweeper won).
            Gone,
        }
        let deletes = batch.iter().map(|m| async move {
            match store.delete(&m.key, Some(m.version.clone())).await {
                Ok(()) => Ok(Outcome::Deleted),
                Err(walgit_store::StoreError::NotFound { .. }) => Ok(Outcome::Gone),
                Err(walgit_store::StoreError::PreconditionFailed { .. }) => Ok(Outcome::Fenced),
                Err(e) => Err(WalError::Store(e)),
            }
        });
        let results = futures::future::join_all(deletes).await;
        for (m, r) in batch.iter().zip(results) {
            match r? {
                Outcome::Deleted => {
                    out.deleted += 1;
                    out.deleted_bytes += m.size;
                    *out.deleted_by_kind.entry(kind_of(&m.key)).or_default() += 1;
                    log(format!("deleted {} ({} bytes)", m.key, m.size));
                }
                Outcome::Fenced => {
                    metrics::counter!("walgit_gc_reference_races_total").increment(1);
                    log(format!(
                        "{} was rewritten mid-sweep (race-1 fence); it survives",
                        m.key
                    ));
                }
                Outcome::Gone => {}
            }
        }
    }
    Ok(out)
}

/// `checkpoints/<seq:016x>/…` → seq.
fn checkpoint_seq_of(key: &str) -> Option<u64> {
    let rest = key.strip_prefix(walgit_proto::keys::CHECKPOINTS_DIR)?;
    let (hex, _) = rest.split_once('/')?;
    u64::from_str_radix(hex, 16).ok()
}

/// `log/<first_seq:016x>.pb` → `first_seq`.
fn log_segment_seq_of(key: &str) -> Option<u64> {
    let rest = key.strip_prefix(walgit_proto::keys::LOG_DIR)?;
    u64::from_str_radix(rest.strip_suffix(".pb")?, 16).ok()
}

/// `wal/<checksum>.<ext>` → checksum.
fn wal_checksum_of(key: &str) -> Option<&str> {
    let rest = key.strip_prefix(walgit_proto::keys::WAL_DIR)?;
    Some(rest.split_once('.')?.0)
}

// ---------------------------------------------------------------------------
// Repair from superseded packs (docs/GC.md §6)
// ---------------------------------------------------------------------------

/// What [`recover_from_superseded`] brought back.
pub struct Recovery {
    /// Hex oids now sitting as loose objects in the scratch at `git_dir`.
    pub found: Vec<String>,
    /// Hex oids no unreferenced pack claims (need `upstream.git` or a human).
    pub remainder: Vec<String>,
    /// Unreferenced packs whose indexes were probed.
    pub packs_probed: usize,
}

/// Source `missing` objects from **unreferenced packs still in the bucket** —
/// the sweep gate froze them the moment the audit finding stood, which is
/// exactly why they are still there (D42: `upstream.git` is a fallback, never
/// a dependency). LISTs `wal/`, downloads candidate `.idx` files into the
/// scratch (~6 % of pack size), reads only the needed objects by range through
/// the remote reader (delta chains resolved), and writes them loose into the
/// bare scratch at `git_dir` for `repair::pack_oids` to pack.
pub async fn recover_from_superseded(
    handle: std::sync::Arc<RepoHandle>,
    missing: Vec<String>,
    git_dir: std::path::PathBuf,
) -> Result<Recovery, WalError> {
    // Index opens, range decode/inflate/delta application, and loose writes
    // never share the serving runtime (D19/VI).
    crate::sync::on_bulk_runtime(async move {
        let log = |message: String| tracing::info!(repo = %handle.id, %message, "repair recovery");
        recover_from_superseded_inner(&handle, &missing, &git_dir, &log).await
    })
    .await
}

async fn recover_from_superseded_inner(
    handle: &RepoHandle,
    missing: &[String],
    git_dir: &std::path::Path,
    log: &(dyn Fn(String) + Send + Sync),
) -> Result<Recovery, WalError> {
    let manifest = handle.manifest();
    let store = handle.store();
    let live: HashSet<&str> = manifest.packs.iter().map(|p| p.checksum.as_str()).collect();

    // Unreferenced packs with both .pack and .idx present, newest first (the
    // objects a repack just dropped live in the packs it just superseded).
    let mut listed: std::collections::HashMap<String, (u64, u64, Option<SystemTime>)> =
        std::collections::HashMap::new();
    let mut stream = store.list(walgit_proto::keys::WAL_DIR, None);
    while let Some(m) = stream.next().await {
        let m = m?;
        let name = m.key.trim_start_matches(walgit_proto::keys::WAL_DIR);
        if let Some(sha) = name.strip_suffix(".pack") {
            let e = listed.entry(sha.to_string()).or_default();
            e.0 = m.size;
            e.2 = m.updated;
        } else if let Some(sha) = name.strip_suffix(".idx") {
            listed.entry(sha.to_string()).or_default().1 = m.size;
        }
    }
    let mut candidates: Vec<(PackRef, Option<SystemTime>)> = listed
        .into_iter()
        .filter(|(sha, (pack, idx, _))| !live.contains(sha.as_str()) && *pack > 0 && *idx > 0)
        .map(|(sha, (pack_size, idx_size, updated))| {
            (
                PackRef {
                    checksum: sha,
                    pack_size,
                    idx_size,
                    ..PackRef::default()
                },
                updated,
            )
        })
        .collect();
    candidates.sort_by_key(|a| std::cmp::Reverse(a.1));
    log(format!(
        "{} unreferenced pack(s) to probe for {} missing object(s)",
        candidates.len(),
        missing.len()
    ));

    // One candidate pack at a time: its index is downloaded (~6 % of the
    // pack) only while earlier packs have not already satisfied every missing
    // object — the scratch stays bounded by need, not by history.
    let mut found = Vec::new();
    let mut remaining: Vec<(String, gix_hash::ObjectId)> = Vec::new();
    let mut remainder: Vec<String> = Vec::new();
    for hex in missing {
        match gix_hash::ObjectId::from_hex(hex.as_bytes()) {
            Ok(oid) => remaining.push((hex.clone(), oid)),
            Err(_) => remainder.push(hex.clone()),
        }
    }
    let mut packs_probed = 0usize;
    // Budget-mode tmpfs: at most one candidate idx at once, itself capped at
    // 1/4 of the whole cache budget (disk mode = unlimited). An oversized idx
    // is left for upstream/manual repair, never allowed to consume the host.
    let budget = handle.effective_config().cache_budget_bytes();
    let index_limit = if budget == 0 { u64::MAX } else { budget / 4 };
    for (candidate, _) in candidates {
        if remaining.is_empty() {
            break;
        }
        if candidate.idx_size > index_limit {
            log(format!(
                "skipping superseded pack {}: idx {} bytes exceeds repair limit {}",
                candidate.checksum, candidate.idx_size, index_limit
            ));
            continue;
        }
        packs_probed += 1;
        let checksum = candidate.checksum.clone();
        let keep = std::collections::HashSet::from([checksum.clone()]);
        let synth = walgit_proto::v1::Manifest {
            object_format: manifest.object_format.clone(),
            packs: vec![candidate],
            ..walgit_proto::v1::Manifest::default()
        };
        let packs = crate::remote::RemotePacks::open(
            store.clone(),
            &synth,
            Some(&keep),
            git_dir,
            handle.local().object_format().kind(),
            crate::remote::BlockCache::new(64 * 1024 * 1024),
            8 * 1024 * 1024,
            &Reporter::none(),
        )
        .await?;
        let mut still: Vec<(String, gix_hash::ObjectId)> = Vec::with_capacity(remaining.len());
        for (hex, oid) in remaining {
            match packs.find(&oid).await? {
                Some(obj) => {
                    // Loose write is std::fs: off the async runtime (VI).
                    let dir = git_dir.to_path_buf();
                    let data = obj.data.clone();
                    let kind = obj.kind;
                    tokio::task::spawn_blocking(move || {
                        walgit_git::repair::write_loose(&dir, kind, &oid, &data)
                    })
                    .await
                    .map_err(|e| WalError::Corrupt(e.to_string()))?
                    .map_err(|e| WalError::Corrupt(format!("writing loose {hex}: {e}")))?;
                    found.push(hex);
                }
                None => still.push((hex, oid)),
            }
        }
        remaining = still;
        // Recovered objects are loose now; the candidate idx is no longer
        // needed. Drop mmap first, then remove it so the next candidate is the
        // only historical index in scratch.
        drop(packs);
        let _ = tokio::fs::remove_file(
            crate::remote::idx_dir(git_dir).join(format!("{checksum}.idx")),
        )
        .await;
    }
    remainder.extend(remaining.into_iter().map(|(hex, _)| hex));
    log(format!(
        "recovered {} of {} from {packs_probed} superseded pack(s) ({} unrecoverable here)",
        found.len(),
        missing.len(),
        remainder.len()
    ));
    Ok(Recovery {
        found,
        remainder,
        packs_probed,
    })
}
