# Garbage collection

Context: design of record for D42. Read this before changing object deletion, checkpoints, WAL retention,
`gc-audit`, `repair`, or a publisher that adopts an existing bucket object.

Implementation:
- reachability walk: `crates/walgit-git/src/audit.rs`
- sweep and repair source: `crates/walgit-wal/src/gc.rs`
- operations and lease: `crates/walgit-server/src/ops.rs`
- maintainer scheduling: `crates/walgit-server/src/maintain.rs`
- tests: `crates/walgit-git/tests/audit.rs`, `crates/walgit-server/tests/gc.rs`,
  `crates/walgit-server/tests/sim.rs`

## 1. What GC must preserve

The bucket grows from:
- objects made unreachable by ref deletion or force-push
- packs superseded by compaction or base rebuilds
- abandoned pack and log uploads from crashed publishers
- old checkpoints and folded log segments
- stale shared render-cache entries

Two rules matter:

1. A sweep must fail closed. If it cannot prove an object is outside every retained repository state, it
   keeps the object.
2. Repair must not depend on `upstream.git`. Objects dropped from the live set remain available in
   superseded packs until an audit confirms the new live set is complete. Upstream is only a fallback for
   objects that never reached the bucket, such as an old import bug.

The object store remains the only durable state. GC scheduling hints are in memory. Losing them causes an
extra idempotent pass, not data loss.

## 2. Committed checkpoint chain

`Manifest.checkpoint` names the newest committed checkpoint. Every immutable `Checkpoint` records its
`previous` committed `CheckpointRef` (append-only proto field 9). Following that chain proves provenance:
an import or checkpoint writer that uploads an object but loses the manifest CAS never joins the chain.

GC follows the chain backwards to find `S_h`, the newest committed checkpoint older than
`now - gc.retention - clock_skew`. `clock_skew = min(5m, retention/2)`. If no such checkpoint exists, no WAL
object is deleted.

`walgit wal materialize --at-seq` follows the same chain and starts from the newest committed checkpoint at
or below its target. This is required because GC may remove log segments older than `S_h`; looking only at
the manifest's current checkpoint would make retained intermediate states unreadable.

Deletion latency is approximately `gc.retention + checkpoint cadence`. That delay is intentional.

## 3. Connectivity-lite audit

The audit asks: does every object reachable from the captured refs exist in the captured live pack set?
It runs on a tmpfs host without reading base-pack data:

- refs come from a snapshot captured with the manifest under `sync_mutex`
- commits and trees come from the local D18 history pack and newer local packs
- blob existence comes from live `.idx` files; a remote base uses the remote reader's local index copy
- blobs are leaves and are never opened
- gitlinks are skipped because they point into another repository
- annotated tags are followed link by link; each tag's declared target type lets a tag of a blob use an
  existence probe without reading the blob

The manifest, roots, and report sequence come from one coherent snapshot. Pack removal stays pinned for the
walk. A missing commit or tree is a finding even when its content is no longer readable locally; the walk
records it and stops descending at that object. If the live indexes claim an object whose required content
cannot be read, the audit fails and writes no verdict.

The result is CAS-written to `gc/audit.pb` using `FsckReport`. CAS prevents an audit that lost its lease from
overwriting a newer finding. Full `fsck.pb` never authorizes deletion: full fsck reads the serving object
database, which may contain disposable loose cached objects and therefore does not prove live-pack
membership. It still detects corruption and feeds repair.

## 4. Reachability repack

Repos without a weekly base rebuild run a full reachability repack every `gc.repack_interval` when the pack
set fits. It uses the existing resumable `compact --base` path:

- scratch copy
- `git repack -adb` from WAL refs
- history pack, bitmap, and commit graph
- one COMPACT entry superseding the packs that existed when the rebuild started

A weekly base rebuild counts as this unit. The tier-2 base upload time is the durable schedule clock. A
successful no-op rebuild keeps the same checksum and mtime, so the maintainer also records a memory-only
completion time to avoid selecting the same work every pass. A restart may repeat one harmless no-op.

## 5. Sweep proof

One sweep does this:

1. Sync refs and capture `live(now)` from the manifest.
2. In one parallel control-plane round, read `gc/audit.pb` and LIST `wal/`, `checkpoints/`, `log/`, and
   `cache/api/v1/`.
3. Follow the committed checkpoint chain to `S_h`.
4. Read retained log segments from `S_h` through the current manifest head. Start with `live(S_h)`, apply
   every pack introduction and `supersedes[]`, and require the replayed live set to equal `live(now)`.
   Missing log data, a missing checkpoint, a cycle, or a replay mismatch fails closed for the whole
   retention window: no WAL, checkpoint, or log deletion this pass. A horizon we can no longer trust never
   authorizes deleting the log segments that would rebuild it.
5. Build the retained provenance set from packs live at `S_h` plus every pack introduced by a retained log
   entry. This protects packs born and superseded between checkpoints. A claimed log entry above manifest
   head is an in-flight publisher intent; its pack remains protected until a later writer commits it or
   burns past it.
6. A WAL object is a candidate only when it is older than the retention cutoff, absent from `live(now)`,
   and absent from the retained provenance set.
7. WAL deletion requires a clean connectivity-lite verdict at a sequence at or beyond this sweep's
   manifest head. A pre-race audit cannot authorize deletion. Any standing finding freezes WAL deletion,
   exactly when superseded packs are needed for repair.
8. Re-read the manifest before deleting. Delete at most `gc.max_deletes_per_pass`, with the version from
   LIST. A concurrent rewrite makes the conditional delete fail and the object survives.

Checkpoint files below `S_h` and log segments before the segment containing `S_h` are outside every
retained replay and need no object-connectivity gate. Render-cache entries use `gc.render_cache_ttl` and
need no checkpoint or audit.

A `412` from conditional delete means the publisher fence worked. It increments
`walgit_gc_reference_races_total`; it is not counted as deleted bytes.

## 6. Publisher fence

The dangerous case is adopting a content-addressed object that already exists. A sweep may have listed it
while it was unreferenced and still hold its old version.

The publisher fences the object before its manifest CAS:

1. create-if-absent returns `412`
2. `ObjectStore::bump_version` changes the current version without changing bytes
3. only then may the manifest CAS reference it

A crash after the CAS is safe because the fence already happened. If the sweep deleted the object between
the create result and the bump, the publisher uploads its local bytes before committing.

Backend implementations:
- GCS composes the object onto itself, creating a new generation without moving data.
- Memory advances a monotonic counter.
- S3 uses native `DeleteObject If-Match` and flips the object between simple and multipart
  representations so each bump differs from its immediate predecessor. Small objects have a documented
  period-two ETag cycle because S3 ETags are content and layout derived. Two publishers would need to bump
  the same dead checksum during one sweep's delete window to re-arm the stale token. The contract suite
  pins adjacent-version difference, byte identity, and stale conditional-delete rejection.

`import --direct` applies the same fence to marker-trusted and HEAD-skipped objects before its manifest CAS.
New uploads are protected by their claimed log/checkpoint intent and retention age.

## 7. Repair without upstream

When the audit reports missing objects, the sweep gate freezes superseded packs. Repair:

1. Revalidates the finding against the current live indexes. An operator or another unit may already have
   restored it; in that case repair marks the report for re-audit and stops.
2. Lists unreferenced packs newest first.
3. Downloads and probes one historical index at a time. In budget mode an index may use at most one quarter
   of the host cache budget. The index is removed after probing; recovered objects remain as loose objects
   in the scratch repo.
4. Reads only matching objects by range through the remote reader, resolving delta chains on the bulk
   runtime.
5. Fetches only the remainder from `upstream.git`, when configured.
6. Packs the complete recovered set once and publishes one tier-0 COMPACT entry superseding nothing.

An unrecoverable finding with no upstream fails loudly while the gate keeps every remaining byte.

## 8. Coordination and scheduling

`gc-audit` and `gc-sweep` share `leases/gc.pb`, held for one unit and released synchronously on success,
exactly like the compaction lease and with no heartbeat. TTL is the backstop on a crash or a defer path
(the guard's Drop does a best-effort release). The lease is a dedup optimization, not a correctness
mechanism: a unit that outruns `lease_ttl` is still safe because audit verdict writes are CAS-protected and
every sweep delete is conditional and re-checked against a fresh manifest. Worst case on a lost lease is
another instance repeating idempotent work. Set `lease_ttl` above the longest expected audit.

The maintainer keeps three memory-only hints per repo:
- last successful full repack
- last completed sweep
- whether the last sweep left candidates gated on an audit

Lease contention, a capped backlog, and gated work remain pending for the next pass. Only a complete real
sweep advances the cadence. The priority order is geometric compaction, reverse-index work, full repack,
demanded GC audit, GC sweep, full fsck.

## 9. Configuration

```toml
[gc]
enabled = true
sweep_interval = "24h"
repack_interval = "30d"
retention = "7d"
render_cache_ttl = "7d"
max_deletes_per_pass = 1000
lease_ttl = "10m"
```

The section is per-repo overridable through D24 settings. A per-repo setting may not reduce `retention` or
`sweep_interval` below one hour; host configuration may use smaller values for a test rig.

## 10. Verification

- audit walk tests: clean, missing blobs, missing readable and unreadable commits/trees, gitlinks, nested
  tags
- GC integration tests: clean and broken verdicts, gated deletion, no-upstream repair, stale findings,
  full-repack cadence including a no-op
- simulation: seeded push/compact/checkpoint/audit/sweep chaos with a truth-store byte oracle
- request budget: steady-state sweep pinned in `docs/ROUNDTRIPS.md`
- store contract: repeated `bump_version`, byte identity, stale conditional-delete rejection

Out of scope: LFS reachability GC and pack-byte bit rot. Full fsck handles corruption; LFS needs its own
pointer-reachability design.
