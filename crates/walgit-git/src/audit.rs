//! Connectivity-lite audit walk (`docs/GC.md` §2, D42): does every object
//! reachable from the refs exist in the **live pack set**?
//!
//! The walk reads commit/tree/tag *contents* from the local odb — always
//! locally readable at Serve level: old commits and trees come from the D18
//! history pack, everything newer from tier < 2 packs — and never opens a
//! blob; blobs are leaves and only their *existence* matters. Existence is
//! answered by a caller-supplied `probe` over the live packs' `.idx` files
//! (local, or the remote reader's `remote-idx/` copies), so the audit runs on
//! a tmpfs host whose 32 GB base pack data is remote.
//!
//! Two outcomes are distinguished with care, everywhere — tips and tag
//! targets included:
//! - `probe(oid)` false → the object is **missing from the live set**: a
//!   finding (`missing`), always recorded — whether or not its content is
//!   still readable locally (a missing commit/tree usually is not, once the
//!   superseded packs left this copy; the walk stops there like git fsck at a
//!   missing object). The verdict is written, the sweep gate stays closed,
//!   repair becomes due.
//! - `probe(oid)` true but the content is not locally readable where the walk
//!   needs it → **this host cannot audit**: an error, no verdict is written
//!   (fail closed — a partial walk must never produce a "clean" report that
//!   opens the deletion gate over an unaudited closure).
//!
//! Memory: exact seen-sets for commits and trees (the walk must terminate);
//! blobs are probed per occurrence instead of deduplicated — a binary search
//! per tree entry beats a multi-GB blob seen-set on a tmpfs host.

use std::collections::{HashSet, VecDeque};

use gix_hash::ObjectId;
use gix_object::{Find, FindHeader, Kind as ObjKind};

use crate::{GitError, LocalRepo};

/// Result of one connectivity walk.
#[derive(Debug, Default)]
pub struct AuditOutcome {
    /// Distinct commits walked.
    pub commits: u64,
    /// Distinct trees walked.
    pub trees: u64,
    /// Existence probes issued for blobs (per occurrence, not distinct).
    pub blob_probes: u64,
    /// Distinct objects reachable from refs but absent from the live pack
    /// set, capped at the caller's limit. `missing_total` is the full count.
    pub missing: Vec<ObjectId>,
    pub missing_total: u64,
}

/// Distinct missing objects: capped list + full count.
#[derive(Default)]
struct Missing {
    set: HashSet<ObjectId>,
    list: Vec<ObjectId>,
    cap: usize,
}

impl Missing {
    fn record(&mut self, oid: ObjectId) {
        if self.set.insert(oid) && self.list.len() < self.cap {
            self.list.push(oid);
        }
    }
}

/// Walk state shared by the helpers below.
struct Walk<'a> {
    hash: gix_hash::Kind,
    probe: &'a (dyn Fn(&gix_hash::oid) -> bool + Sync),
    missing: Missing,
    seen_commits: HashSet<ObjectId>,
    seen_trees: HashSet<ObjectId>,
    commit_queue: VecDeque<ObjectId>,
    tree_stack: Vec<ObjectId>,
    out: AuditOutcome,
    since_progress: u64,
}

/// How often the progress callback fires (objects visited between calls).
const PROGRESS_EVERY: u64 = 1_000_000;

/// Walk every ref tip and record reachable objects missing from `probe`'s
/// pack set. Blocking (CPU + mmap'd idx reads): callers run it on
/// `spawn_blocking` / the bulk runtime, never on the serving runtime (D19).
pub fn connectivity_walk(
    repo: &LocalRepo,
    probe: &(dyn Fn(&gix_hash::oid) -> bool + Sync),
    missing_cap: usize,
    log: &(dyn Fn(String) + Send + Sync),
) -> Result<AuditOutcome, GitError> {
    connectivity_walk_with_refs(repo, &repo.refs()?, probe, missing_cap, log)
}

/// As [`connectivity_walk`], with roots captured coherently with the manifest
/// being audited (D42; `ReadGuard::audit_snapshot`).
pub fn connectivity_walk_with_refs(
    repo: &LocalRepo,
    refs: &crate::RefSnapshotData,
    probe: &(dyn Fn(&gix_hash::oid) -> bool + Sync),
    missing_cap: usize,
    log: &(dyn Fn(String) + Send + Sync),
) -> Result<AuditOutcome, GitError> {
    let gix = repo.gix();
    let odb = &gix.objects;

    let mut w = Walk {
        hash: gix.object_hash(),
        probe,
        missing: Missing {
            cap: missing_cap,
            ..Missing::default()
        },
        seen_commits: HashSet::new(),
        seen_trees: HashSet::new(),
        commit_queue: VecDeque::new(),
        tree_stack: Vec::new(),
        out: AuditOutcome::default(),
        since_progress: 0,
    };
    let mut buf = Vec::new();

    // Seed from every ref tip. Annotated tags are chased link by link through
    // their own contents — never the snapshot's peeled shortcut, which would
    // skip probing intermediate tags of a nested chain.
    let tips: Vec<ObjectId> = refs
        .refs
        .iter()
        .filter_map(|r| ObjectId::from_hex(r.oid.as_bytes()).ok())
        .filter(|o| !o.is_null())
        .collect();
    log(format!("walking {} ref tips", tips.len()));
    for oid in tips {
        seed(&mut w, odb, oid, &mut buf)?;
    }

    // Commits: BFS over parents; contents from the history/fresh packs. Trees
    // drain eagerly so the stack stays small and odb access stays local.
    while let Some(oid) = w.commit_queue.pop_front() {
        w.out.commits += 1;
        w.since_progress += 1;
        if !(w.probe)(&oid) {
            // Missing from the live set: the finding. Its content is usually
            // gone here too (superseded packs are removed locally) — record
            // and stop descending, exactly like git fsck at a missing object.
            w.missing.record(oid);
            if odb.try_find(&oid, &mut buf).map_err(GitError::Gix)?.is_none() {
                continue;
            }
        }
        let data = odb
            .try_find(&oid, &mut buf)
            .map_err(GitError::Gix)?
            .ok_or_else(|| unreadable("commit", &oid))?;
        if data.kind != ObjKind::Commit {
            continue;
        }
        for token in gix_object::CommitRefIter::from_bytes(data.data, w.hash) {
            use gix_object::commit::ref_iter::Token;
            match token.map_err(crate::ge)? {
                Token::Tree { id } => {
                    if w.seen_trees.insert(id) {
                        w.tree_stack.push(id);
                    }
                }
                Token::Parent { id } => {
                    if w.seen_commits.insert(id) {
                        w.commit_queue.push_back(id);
                    }
                }
                _ => break, // past the parents: no structure tokens left
            }
        }
        drain_trees(&mut w, odb)?;
        if w.since_progress >= PROGRESS_EVERY {
            w.since_progress = 0;
            log(format!(
                "walked {} commits / {} trees, {} blob probes, {} missing",
                w.out.commits,
                w.out.trees,
                w.out.blob_probes,
                w.missing.set.len()
            ));
        }
    }
    drain_trees(&mut w, odb)?;

    let mut out = w.out;
    out.missing_total = w.missing.set.len() as u64;
    out.missing = w.missing.list;
    out.missing.sort_unstable();
    log(format!(
        "audit walk done: {} commits, {} trees, {} blob probes, {} missing",
        out.commits, out.trees, out.blob_probes, out.missing_total
    ));
    Ok(out)
}

/// Classify and enqueue one tip. Tags chase their target through the tag
/// object's own `object` header, one link at a time (nested tags included),
/// probing every link.
fn seed(
    w: &mut Walk<'_>,
    odb: &(impl Find + FindHeader),
    oid: ObjectId,
    buf: &mut Vec<u8>,
) -> Result<(), GitError> {
    let mut current = oid;
    loop {
        let kind = odb.try_header(&current).map_err(GitError::Gix)?.map(|h| h.kind);
        match kind {
            Some(ObjKind::Commit) => {
                if w.seen_commits.insert(current) {
                    w.commit_queue.push_back(current);
                }
                return Ok(());
            }
            Some(ObjKind::Tree) => {
                if w.seen_trees.insert(current) {
                    w.tree_stack.push(current);
                }
                return Ok(());
            }
            Some(ObjKind::Blob) => {
                w.out.blob_probes += 1;
                if !(w.probe)(&current) {
                    w.missing.record(current);
                }
                return Ok(());
            }
            Some(ObjKind::Tag) => {
                // The tag object itself must exist in the live set.
                let present = (w.probe)(&current);
                if !present {
                    w.missing.record(current);
                }
                let Some(data) = odb.try_find(&current, buf).map_err(GitError::Gix)? else {
                    if present {
                        return Err(unreadable("tag", &current));
                    }
                    // Missing AND unreadable: recorded, nothing to chase.
                    return Ok(());
                };
                let (target, kind) =
                    tag_target(data.data, w.hash).ok_or_else(|| unreadable("tag", &current))?;
                // The tag declares its target's kind: a blob target is a leaf
                // and needs only the existence probe — its content may live in
                // a remote base (e.g. a tag of a public key blob) and must not
                // fail the audit on a host that cannot read it.
                match kind {
                    ObjKind::Blob => {
                        w.out.blob_probes += 1;
                        if !(w.probe)(&target) {
                            w.missing.record(target);
                        }
                        return Ok(());
                    }
                    ObjKind::Commit => {
                        if w.seen_commits.insert(target) {
                            w.commit_queue.push_back(target);
                        }
                        return Ok(());
                    }
                    ObjKind::Tree => {
                        if w.seen_trees.insert(target) {
                            w.tree_stack.push(target);
                        }
                        return Ok(());
                    }
                    ObjKind::Tag => current = target,
                }
            }
            None => {
                // Not locally readable. Not in the live set → a finding. In
                // the live set → this host cannot classify it, and its whole
                // closure would go unaudited under a "clean" verdict: fail,
                // symmetric with the walk (fail closed, no verdict written).
                if (w.probe)(&current) {
                    return Err(unreadable("tip", &current));
                }
                w.missing.record(current);
                return Ok(());
            }
        }
    }
}

/// The `object` and `type` lines of a tag (its first two tokens).
fn tag_target(data: &[u8], hash: gix_hash::Kind) -> Option<(ObjectId, ObjKind)> {
    use gix_object::tag::ref_iter::Token;
    let mut iter = gix_object::TagRefIter::from_bytes(data, hash);
    let Some(Ok(Token::Target { id })) = iter.next() else {
        return None;
    };
    let Some(Ok(Token::TargetKind(kind))) = iter.next() else {
        return None;
    };
    Some((id, kind))
}

/// Depth-first tree walk: probe every tree and blob, skip gitlinks.
fn drain_trees(w: &mut Walk<'_>, odb: &impl Find) -> Result<(), GitError> {
    let mut buf = Vec::new();
    while let Some(oid) = w.tree_stack.pop() {
        w.out.trees += 1;
        w.since_progress += 1;
        if !(w.probe)(&oid) {
            // Missing from the live set: the finding. Descend only if the
            // stale content happens to still be readable (more findings),
            // else stop here — never error on an object the live set does
            // not claim.
            w.missing.record(oid);
            match odb.try_find(&oid, &mut buf).map_err(GitError::Gix)? {
                Some(_) => {}
                None => continue,
            }
        }
        let data = odb
            .try_find(&oid, &mut buf)
            .map_err(GitError::Gix)?
            .ok_or_else(|| unreadable("tree", &oid))?;
        if data.kind != ObjKind::Tree {
            continue;
        }
        for entry in gix_object::TreeRefIter::from_bytes(data.data, w.hash) {
            let entry = entry.map_err(crate::ge)?;
            if entry.mode.is_commit() {
                continue; // gitlink: points into another repository
            }
            if entry.mode.is_tree() {
                let id = entry.oid.to_owned();
                if w.seen_trees.insert(id) {
                    w.tree_stack.push(id);
                }
            } else {
                w.out.blob_probes += 1;
                if !(w.probe)(entry.oid) {
                    w.missing.record(entry.oid.to_owned());
                }
            }
        }
    }
    Ok(())
}

/// The live set claims the object but this host cannot read its content where
/// the walk needs it: the audit must fail rather than write a partial verdict.
fn unreadable(kind: &str, oid: &gix_hash::oid) -> GitError {
    GitError::Fsck(format!(
        "audit: {kind} {oid} is in the live pack set but not locally readable \
         (history pack missing or stale?); this host cannot audit — no verdict written"
    ))
}
