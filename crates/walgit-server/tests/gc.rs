//! Garbage collection (`docs/GC.md`, D42): the connectivity-lite audit op.
//! A clean repository writes a clean verdict at `gc/audit.pb`; a hole in the
//! live pack set (an object reachable from refs that no live pack claims) is
//! a finding, not a unit failure.

mod harness;

use harness::{Server, git, git_in};
use prost::Message;
use std::collections::HashMap;
use walgit_store::ObjectStoreExt;

/// Every await is bounded so a hang names the step instead of stalling CI.
macro_rules! step {
    ($name:literal, $e:expr) => {
        tokio::time::timeout(std::time::Duration::from_secs(30), $e)
            .await
            .unwrap_or_else(|_| panic!("step timed out: {}", $name))
    };
}

async fn read_audit(h: &walgit_wal::RepoHandle) -> Option<walgit_proto::v1::FsckReport> {
    let (_, bytes) = h
        .store()
        .get_bytes(walgit_proto::keys::GC_AUDIT)
        .await
        .ok()??;
    walgit_proto::v1::FsckReport::decode(bytes.as_ref()).ok()
}

async fn run_op(
    server: &Server,
    id: &walgit_git::RepoId,
    op: &str,
    params: &[(&str, &str)],
) -> Result<serde_json::Value, String> {
    let task = walgit_server::ops::start(
        server.state.clone(),
        id.clone(),
        op,
        params
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect(),
    )
    .await
    .map_err(|_| "start failed".to_string())?;
    assert!(
        task.wait_done(std::time::Duration::from_secs(30)).await,
        "{op} op did not finish"
    );
    match task.outcome() {
        Some(Ok(o)) => Ok(o.value.unwrap_or(serde_json::Value::Null)),
        Some(Err((_, msg))) => Err(msg),
        None => Err("no outcome".into()),
    }
}

async fn run_audit(
    server: &Server,
    id: &walgit_git::RepoId,
) -> Result<serde_json::Value, String> {
    run_op(server, id, "gc-audit", &[]).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn audit_clean_then_finds_a_hole() -> anyhow::Result<()> {
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.maintenance.checkpoints = false;
            c.compaction.enabled = false;
            c.bundles.enabled = false;
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("a.txt"), "one\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one"])?;
    let c1 = git_in(src.path(), &["rev-parse", "HEAD"])?.trim().to_string();
    // An annotated tag rides along: the walk must chase it to its target.
    git_in(src.path(), &["tag", "-a", "v1", "-m", "v1"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main", "v1"],
        src.path(),
    )?;

    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;

    // Clean: every pushed object is in the live pack set.
    let v = run_audit(&server, &id).await.expect("clean audit");
    assert_eq!(v["missing"], 0, "{v}");
    assert!(v["commits"].as_u64().unwrap() >= 1, "{v}");
    let report = read_audit(&h).await.expect("gc/audit.pb written");
    assert_eq!(report.missing_total, 0);
    assert!(report.seq > 0);

    // The hole: publish commit 2 + its tree WITHOUT the new blob (a pack that
    // is not the closure of the ref), then move main onto it.
    std::fs::write(src.path().join("b.txt"), "the missing blob\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "two"])?;
    let c2 = git_in(src.path(), &["rev-parse", "HEAD"])?.trim().to_string();
    let tree2 = git_in(src.path(), &["rev-parse", "HEAD^{tree}"])?
        .trim()
        .to_string();
    let blob2 = git_in(src.path(), &["rev-parse", "HEAD:b.txt"])?
        .trim()
        .to_string();
    let holes = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .current_dir(src.path())
        .args(["pack-objects", &format!("{}/pack", holes.path().display())])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{c2}\n{tree2}\n").as_bytes())?;
            c.wait_with_output()
        })?;
    let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
    step!("sync", h.sync())?;
    step!(
        "add hole pack",
        h.add_pack(
            &holes.path().join(format!("pack-{sha}.pack")),
            &holes.path().join(format!("pack-{sha}.idx")),
            0,
            None
        )
    )?;
    step!("sync2", h.sync())?;
    let txn = walgit_proto::v1::RefTransaction {
        updates: vec![walgit_proto::v1::RefUpdate {
            name: "refs/heads/main".into(),
            old_oid: c1.clone(),
            new_oid: c2.clone(),
            ..Default::default()
        }],
        ..Default::default()
    };
    step!(
        "move main",
        h.publish_push_synced(None, txn, HashMap::default())
    )?;

    // The audit finds exactly the dropped blob; the unit reports it as a
    // finding (Ok), and the verdict at gc/audit.pb carries it.
    let v = run_audit(&server, &id).await.expect("audit with finding");
    assert_eq!(v["missing"], 1, "{v}");
    let report = read_audit(&h).await.expect("gc/audit.pb rewritten");
    assert_eq!(report.missing, vec![blob2], "{report:?}");
    assert_eq!(report.missing_total, 1);
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_deletes_superseded_packs_behind_a_clean_audit() -> anyhow::Result<()> {
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.maintenance.checkpoints = false;
            c.compaction.enabled = false; // manual compaction below
            c.bundles.enabled = false;
            // Everything unreferenced matures immediately; the audit gate is
            // then the only thing between a superseded pack and deletion.
            c.gc.retention = std::time::Duration::ZERO;
            c.gc.render_cache_ttl = std::time::Duration::ZERO;
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    for i in 0..3 {
        std::fs::write(src.path().join(format!("f{i}.txt")), format!("{i}\n"))?;
        git_in(src.path(), &["add", "."])?;
        git_in(src.path(), &["commit", "-q", "-m", &format!("c{i}")])?;
        git(
            &["push", "-q", &server.repo_url("o", "r"), "main"],
            src.path(),
        )?;
    }
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    let before: Vec<String> = h.manifest().packs.iter().map(|p| p.checksum.clone()).collect();
    assert_eq!(before.len(), 3, "three tier-0 push packs");

    // A stray render-cache entry and an orphan pack (a crashed publish).
    h.store()
        .put_bytes("cache/api/v1/stale.json", b"{}".to_vec(), walgit_store::PutMode::Overwrite)
        .await?;
    h.store()
        .put_bytes(
            "wal/00000000000000000000000000000000deadbeef.pack",
            b"orphan".to_vec(),
            walgit_store::PutMode::Overwrite,
        )
        .await?;

    // Fold the three fresh packs: one tier-1 pack supersedes them; the old
    // packs stay in the bucket, unreferenced. The checkpoint AFTER the fold is
    // the horizon reference the sweep judges against (retention 0 → any
    // checkpoint is old enough).
    let v = run_op(&server, &id, "compact", &[("force", "1")]).await.expect("compact");
    assert_eq!(v["superseded"], 3, "{v}");
    run_op(&server, &id, "checkpoint", &[]).await.expect("checkpoint");
    step!("sync after compact", h.sync())?;
    let committed_horizon = h.manifest().checkpoint.as_ref().unwrap().seq;
    // Abandoned import/checkpoint object: high seq, never won the manifest
    // CAS, intentionally omits old packs. A LIST-by-filename horizon would
    // select it and over-delete; the committed `previous` chain must ignore it.
    let abandoned_seq = committed_horizon + 1000;
    h.store()
        .put_bytes(
            &walgit_proto::keys::checkpoint_key(abandoned_seq),
            walgit_proto::v1::Checkpoint {
                seq: abandoned_seq,
                created_at: Some(walgit_proto::time::now()),
                ..Default::default()
            }
            .encode_to_vec(),
            walgit_store::PutMode::Overwrite,
        )
        .await?;
    let live: Vec<String> = h.manifest().packs.iter().map(|p| p.checksum.clone()).collect();
    assert_eq!(live.len(), 1);

    // Sweep 1: the superseded packs + orphan are provably dead against the
    // horizon checkpoint, but there is no clean audit → gated. The
    // render-cache entry needs no gate and goes at once.
    let v = run_op(&server, &id, "gc-sweep", &[]).await.expect("sweep 1");
    assert_eq!(v["horizon_seq"], committed_horizon, "abandoned checkpoint became horizon: {v}");
    assert!(v["gated"].as_u64().unwrap() >= 7, "3 packs + idx + orphan: {v}");
    assert_eq!(v["deleted_by_kind"]["render"], 1, "{v}");
    assert!(
        h.store().get_bytes("cache/api/v1/stale.json").await?.is_none(),
        "render-cache entry swept without a gate"
    );
    for sha in &before {
        assert!(
            h.store()
                .get_bytes(&walgit_proto::keys::pack_key(sha))
                .await?
                .is_some(),
            "superseded pack {sha} must survive an audit-less sweep"
        );
    }

    // A clean audit taken after the candidates matured opens the gate.
    let v = run_audit(&server, &id).await.expect("clean audit");
    assert_eq!(v["missing"], 0, "{v}");

    // Sweep 2: superseded packs + orphan deleted; the live pack survives.
    let v = run_op(&server, &id, "gc-sweep", &[]).await.expect("sweep 2");
    assert!(v["deleted"].as_u64().unwrap() >= 7, "{v}");
    assert_eq!(v["gated"], 0, "{v}");
    for sha in &before {
        assert!(
            h.store()
                .get_bytes(&walgit_proto::keys::pack_key(sha))
                .await?
                .is_none(),
            "superseded pack {sha} should be gone"
        );
    }
    assert!(
        h.store()
            .get_bytes("wal/00000000000000000000000000000000deadbeef.pack")
            .await?
            .is_none(),
        "orphan pack swept"
    );
    assert!(
        h.store()
            .get_bytes(&walgit_proto::keys::pack_key(&live[0]))
            .await?
            .is_some(),
        "live pack untouched"
    );

    // And the repository still serves: a fresh clone sees all three commits.
    let clone = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "-q",
            &server.repo_url("o", "r"),
            clone.path().to_str().unwrap(),
        ],
        clone.path(),
    )?;
    let n = git_in(clone.path(), &["rev-list", "--count", "HEAD"])?;
    assert_eq!(n.trim(), "3");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sweep_stays_frozen_while_the_audit_reports_missing_objects() -> anyhow::Result<()> {
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.maintenance.checkpoints = false;
            c.compaction.enabled = false;
            c.bundles.enabled = false;
            c.gc.retention = std::time::Duration::ZERO;
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    for i in 0..2 {
        std::fs::write(src.path().join(format!("f{i}.txt")), format!("{i}\n"))?;
        git_in(src.path(), &["add", "."])?;
        git_in(src.path(), &["commit", "-q", "-m", &format!("c{i}")])?;
        git(
            &["push", "-q", &server.repo_url("o", "r"), "main"],
            src.path(),
        )?;
    }
    let c2 = git_in(src.path(), &["rev-parse", "HEAD"])?.trim().to_string();
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    let before: Vec<String> = h.manifest().packs.iter().map(|p| p.checksum.clone()).collect();

    // The hole: a "repack" that drops every blob — publish a commits+trees
    // pack superseding both push packs, exactly what a buggy repack would do.
    let tree2 = git_in(src.path(), &["rev-parse", "HEAD^{tree}"])?.trim().to_string();
    let c1 = git_in(src.path(), &["rev-parse", "HEAD~1"])?.trim().to_string();
    let tree1 = git_in(src.path(), &["rev-parse", "HEAD~1^{tree}"])?.trim().to_string();
    let holes = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .current_dir(src.path())
        .args(["pack-objects", &format!("{}/pack", holes.path().display())])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{c1}\n{c2}\n{tree1}\n{tree2}\n").as_bytes())?;
            c.wait_with_output()
        })?;
    let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
    step!("sync", h.sync())?;
    let checksum = walgit_git::gix_hash::ObjectId::from_hex(sha.as_bytes())?;
    let dest = h.local().pack_path(&checksum);
    std::fs::copy(holes.path().join(format!("pack-{sha}.pack")), &dest)?;
    std::fs::copy(
        holes.path().join(format!("pack-{sha}.idx")),
        dest.with_extension("idx"),
    )?;
    step!("refresh", h.local().refresh_async())?;
    let info = h
        .local()
        .packs()?
        .into_iter()
        .find(|p| p.checksum == checksum)
        .expect("hole pack installed");
    let supersedes: Vec<walgit_git::gix_hash::ObjectId> = before
        .iter()
        .map(|s| walgit_git::gix_hash::ObjectId::from_hex(s.as_bytes()).unwrap())
        .collect();
    step!(
        "publish hole compact",
        h.publish_compact(info, supersedes, 1)
    )?;
    run_op(&server, &id, "checkpoint", &[]).await.expect("checkpoint");
    step!("sync2", h.sync())?;

    // The lite audit finds the dropped blobs.
    let v = run_audit(&server, &id).await.expect("audit");
    assert!(v["missing"].as_u64().unwrap() >= 2, "{v}");
    // A newer CLEAN full fsck verdict must not override it for GC: full fsck
    // reads the serving ODB, including disposable loose cache, not exclusively
    // the manifest's durable live packs. The deletion gate trusts gc/audit.pb
    // only.
    let clean_fsck = walgit_proto::v1::FsckReport {
        seq: h.manifest().head_seq,
        at: Some(walgit_proto::time::now()),
        ..Default::default()
    };
    h.store()
        .put_bytes(
            walgit_proto::keys::FSCK,
            clean_fsck.encode_to_vec(),
            walgit_store::PutMode::Overwrite,
        )
        .await?;

    // Sweeps must never delete WAL objects while the lite verdict is not clean —
    // the superseded packs are the repair source. (Folded log segments and
    // old checkpoints are gateless: their replay value ended at the horizon.)
    for _ in 0..2 {
        let v = run_op(&server, &id, "gc-sweep", &[]).await.expect("sweep");
        assert_eq!(v["deleted_by_kind"]["pack"], serde_json::Value::Null, "{v}");
        assert_eq!(v["deleted_by_kind"]["side-file"], serde_json::Value::Null, "{v}");
        assert!(v["gated"].as_u64().unwrap() >= 2, "{v}");
    }
    for sha in &before {
        assert!(
            h.store()
                .get_bytes(&walgit_proto::keys::pack_key(sha))
                .await?
                .is_some(),
            "superseded pack {sha} frozen while objects are missing"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn maintain_loop_drives_sweep_then_audit_then_deletion() -> anyhow::Result<()> {
    use walgit_server::maintain::{Unit, next_unit, run_pass};
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.maintenance.checkpoints = false;
            c.maintenance.fsck_interval = std::time::Duration::ZERO; // full fsck off
            c.compaction.enabled = false;
            c.bundles.enabled = false;
            c.gc.retention = std::time::Duration::ZERO;
            c.gc.sweep_interval = std::time::Duration::ZERO; // due every pass
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    for i in 0..2 {
        std::fs::write(src.path().join(format!("f{i}.txt")), format!("{i}\n"))?;
        git_in(src.path(), &["add", "."])?;
        git_in(src.path(), &["commit", "-q", "-m", &format!("c{i}")])?;
        git(
            &["push", "-q", &server.repo_url("o", "r"), "main"],
            src.path(),
        )?;
    }
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    let before: Vec<String> = h.manifest().packs.iter().map(|p| p.checksum.clone()).collect();
    let v = run_op(&server, &id, "compact", &[("force", "1")]).await.expect("compact");
    assert_eq!(v["superseded"], 2, "{v}");
    run_op(&server, &id, "checkpoint", &[]).await.expect("checkpoint");

    // Pass 1: never swept (this process) → sweep; the superseded packs are
    // dead against the horizon checkpoint but gated (no audit yet).
    let u = step!("plan 1", next_unit(&server.state, &id))?;
    assert!(matches!(u, Unit::GcSweep(_)), "{u:?}");
    step!("pass 1", run_pass(&server.state))?;

    // Pass 2: gated candidates and no audit since that sweep → audit.
    let u = step!("plan 2", next_unit(&server.state, &id))?;
    assert!(matches!(u, Unit::GcAudit(_)), "{u:?}");
    step!("pass 2", run_pass(&server.state))?;

    // Pass 3: the gate is open — deletion happens.
    let u = step!("plan 3", next_unit(&server.state, &id))?;
    assert!(matches!(u, Unit::GcSweep(_)), "{u:?}");
    step!("pass 3", run_pass(&server.state))?;
    for sha in &before {
        assert!(
            h.store()
                .get_bytes(&walgit_proto::keys::pack_key(sha))
                .await?
                .is_none(),
            "superseded pack {sha} deleted by the loop"
        );
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_repack_unit_fires_on_interval_and_rearms() -> anyhow::Result<()> {
    use walgit_server::maintain::{Unit, next_unit, run_pass};
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.maintenance.checkpoints = false;
            c.maintenance.fsck_interval = std::time::Duration::ZERO;
            c.compaction.enabled = false;
            c.bundles.enabled = false; // no weekly BaseRebuild → gc owns the repack
            c.gc.repack_interval = std::time::Duration::from_secs(1);
            c.gc.sweep_interval = std::time::Duration::from_hours(24);
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("a.txt"), "one\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one"])?;
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;

    // Never repacked and no checkpoint → no baseline → not due.
    // (A brand-new repository is not immediately rewritten.)
    // The first sweep runs instead (never swept).
    let u = step!("plan 0", next_unit(&server.state, &id))?;
    assert!(matches!(u, Unit::GcSweep(_)), "{u:?}");
    step!("pass 0", run_pass(&server.state))?;

    // A checkpoint gives the repository a first_state_at baseline.
    run_op(&server, &id, "checkpoint", &[]).await.expect("checkpoint");
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let u = step!("plan 1", next_unit(&server.state, &id))?;
    assert!(matches!(u, Unit::FullRepack(_)), "{u:?}");
    step!("pass 1", run_pass(&server.state))?;
    step!("sync", h.sync())?;
    assert!(
        h.manifest().packs.iter().any(|p| p.tier == 2),
        "full repack published a base: {:?}",
        h.manifest().packs
    );

    // The clock re-armed: not due again right away.
    let u = step!("plan 2", next_unit(&server.state, &id))?;
    assert!(!matches!(u, Unit::FullRepack(_)), "{u:?}");

    // At the next interval, the unchanged repository rebuilds to identical
    // checksums (no-op: base mtime does not move). The in-memory completion
    // stamp must still re-arm it; otherwise FullRepack wins every subsequent
    // pass and starves audit/sweep/fsck.
    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let u = step!("plan no-op", next_unit(&server.state, &id))?;
    assert!(matches!(u, Unit::FullRepack(_)), "{u:?}");
    step!("pass no-op", run_pass(&server.state))?;
    let u = step!("plan after no-op", next_unit(&server.state, &id))?;
    assert!(!matches!(u, Unit::FullRepack(_)), "{u:?}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn gc_audit_findings_drive_the_repair_unit() -> anyhow::Result<()> {
    use walgit_server::maintain::{Unit, next_unit, run_pass};
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.git.allow_any_sha1_in_want = true;
            c.maintenance.checkpoints = false;
            c.maintenance.fsck_interval = std::time::Duration::ZERO; // gc-audit is the only detector
            c.compaction.enabled = false;
            c.bundles.enabled = false;
            c.gc.sweep_interval = std::time::Duration::from_hours(24);
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    step!("put upstream", server.put_repo("o", "up"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("a.txt"), "one\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one"])?;
    let c1 = git_in(src.path(), &["rev-parse", "HEAD"])?.trim().to_string();
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    std::fs::write(src.path().join("b.txt"), "the dropped blob\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "two"])?;
    let c2 = git_in(src.path(), &["rev-parse", "HEAD"])?.trim().to_string();
    let tree2 = git_in(src.path(), &["rev-parse", "HEAD^{tree}"])?.trim().to_string();
    // The upstream has everything.
    git(
        &["push", "-q", &server.repo_url("o", "up"), "main"],
        src.path(),
    )?;

    // Publish commit 2 + tree without the blob, move main onto it.
    let holes = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .current_dir(src.path())
        .args(["pack-objects", &format!("{}/pack", holes.path().display())])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{c2}\n{tree2}\n").as_bytes())?;
            c.wait_with_output()
        })?;
    let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    step!("sync", h.sync())?;
    step!(
        "add hole pack",
        h.add_pack(
            &holes.path().join(format!("pack-{sha}.pack")),
            &holes.path().join(format!("pack-{sha}.idx")),
            0,
            None
        )
    )?;
    step!("sync2", h.sync())?;
    let txn = walgit_proto::v1::RefTransaction {
        updates: vec![walgit_proto::v1::RefUpdate {
            name: "refs/heads/main".into(),
            old_oid: c1,
            new_oid: c2,
            ..Default::default()
        }],
        ..Default::default()
    };
    step!("move main", h.publish_push_synced(None, txn, HashMap::default()))?;

    // The gc audit records the finding.
    let v = run_audit(&server, &id).await.expect("audit");
    assert_eq!(v["missing"], 1, "{v}");

    // Without upstream.git nothing can repair; with it (D24 setting) the
    // repair unit is due — driven by gc/audit.pb, no fsck.pb exists at all.
    let client = reqwest::Client::new();
    let r = client
        .put(format!("{}/o/r/api/settings", server.base_url))
        .header("Content-Type", "application/toml")
        .body(format!("[upstream]\ngit = \"{}\"\n", server.repo_url("o", "up")))
        .send()
        .await?;
    assert!(r.status().is_success(), "{}", r.text().await?);
    let u = step!("plan", next_unit(&server.state, &id))?;
    assert!(matches!(u, Unit::Repair(1)), "{u:?}");
    step!("repair pass", run_pass(&server.state))?;
    let report = read_audit(&h).await.expect("gc/audit.pb");
    assert!(report.repaired_seq > 0, "{report:?}");

    // Re-audit: clean.
    let v = run_audit(&server, &id).await.expect("re-audit");
    assert_eq!(v["missing"], 0, "{v}");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_recovers_from_superseded_packs_without_upstream() -> anyhow::Result<()> {
    use walgit_server::maintain::{Unit, next_unit, run_pass};
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.maintenance.checkpoints = false;
            c.maintenance.fsck_interval = std::time::Duration::ZERO;
            c.compaction.enabled = false;
            c.bundles.enabled = false;
            c.gc.retention = std::time::Duration::ZERO;
            c.gc.sweep_interval = std::time::Duration::ZERO;
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    for i in 0..2 {
        std::fs::write(src.path().join(format!("f{i}.txt")), format!("{i}\n"))?;
        git_in(src.path(), &["add", "."])?;
        git_in(src.path(), &["commit", "-q", "-m", &format!("c{i}")])?;
        git(
            &["push", "-q", &server.repo_url("o", "r"), "main"],
            src.path(),
        )?;
    }
    let c2 = git_in(src.path(), &["rev-parse", "HEAD"])?.trim().to_string();
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    let before: Vec<String> = h.manifest().packs.iter().map(|p| p.checksum.clone()).collect();

    // A "repack" that drops every blob (as in the freeze test): commits+trees
    // pack superseding both push packs.
    let tree2 = git_in(src.path(), &["rev-parse", "HEAD^{tree}"])?.trim().to_string();
    let c1 = git_in(src.path(), &["rev-parse", "HEAD~1"])?.trim().to_string();
    let tree1 = git_in(src.path(), &["rev-parse", "HEAD~1^{tree}"])?.trim().to_string();
    let holes = tempfile::tempdir()?;
    let out = std::process::Command::new("git")
        .current_dir(src.path())
        .args(["pack-objects", &format!("{}/pack", holes.path().display())])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut c| {
            use std::io::Write;
            c.stdin
                .take()
                .unwrap()
                .write_all(format!("{c1}\n{c2}\n{tree1}\n{tree2}\n").as_bytes())?;
            c.wait_with_output()
        })?;
    let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
    step!("sync", h.sync())?;
    let checksum = walgit_git::gix_hash::ObjectId::from_hex(sha.as_bytes())?;
    let dest = h.local().pack_path(&checksum);
    std::fs::copy(holes.path().join(format!("pack-{sha}.pack")), &dest)?;
    std::fs::copy(
        holes.path().join(format!("pack-{sha}.idx")),
        dest.with_extension("idx"),
    )?;
    step!("refresh", h.local().refresh_async())?;
    let info = h
        .local()
        .packs()?
        .into_iter()
        .find(|p| p.checksum == checksum)
        .expect("hole pack installed");
    let supersedes: Vec<walgit_git::gix_hash::ObjectId> = before
        .iter()
        .map(|s| walgit_git::gix_hash::ObjectId::from_hex(s.as_bytes()).unwrap())
        .collect();
    step!("publish hole compact", h.publish_compact(info, supersedes, 1))?;
    run_op(&server, &id, "checkpoint", &[]).await.expect("checkpoint");
    step!("sync2", h.sync())?;

    // Audit records the missing blobs; repair is due with NO upstream.git —
    // the superseded packs in the bucket are the source (D42).
    let v = run_audit(&server, &id).await.expect("audit");
    let missing = v["missing"].as_u64().unwrap();
    assert!(missing >= 2, "{v}");
    let u = step!("plan repair", next_unit(&server.state, &id))?;
    assert!(matches!(u, Unit::Repair(_)), "{u:?}");
    step!("repair pass", run_pass(&server.state))?;
    let report = read_audit(&h).await.expect("gc/audit.pb");
    assert!(report.repaired_seq > 0, "{report:?}");

    // Re-audit clean (at a seq past the horizon checkpoint): the gate opens
    // and one sweep deletes the superseded packs.
    let v = run_audit(&server, &id).await.expect("re-audit");
    assert_eq!(v["missing"], 0, "{v}");
    let v = run_op(&server, &id, "gc-sweep", &[]).await.expect("sweep");
    assert!(v["deleted"].as_u64().unwrap() >= 1, "{v}");

    // The repository serves everything again.
    let clone = tempfile::tempdir()?;
    git(
        &[
            "clone",
            "-q",
            &server.repo_url("o", "r"),
            clone.path().to_str().unwrap(),
        ],
        clone.path(),
    )?;
    assert_eq!(
        std::fs::read_to_string(clone.path().join("f1.txt"))?,
        "1\n",
        "recovered blob served"
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_repair_finding_already_in_live_pack_completes() -> anyhow::Result<()> {
    let server = step!(
        "start",
        Server::start_with_tweak(|c| {
            c.maintenance.checkpoints = false;
            c.compaction.enabled = false;
            c.bundles.enabled = false;
        })
    )?;
    step!("put repo", server.put_repo("o", "r"))?;
    let src = tempfile::tempdir()?;
    git_in(src.path(), &["init", "-q", "-b", "main"])?;
    git_in(src.path(), &["config", "user.email", "t@t"])?;
    git_in(src.path(), &["config", "user.name", "Tester"])?;
    std::fs::write(src.path().join("a.txt"), "already restored\n")?;
    git_in(src.path(), &["add", "."])?;
    git_in(src.path(), &["commit", "-q", "-m", "one"])?;
    let blob = git_in(src.path(), &["rev-parse", "HEAD:a.txt"])?
        .trim()
        .to_string();
    git(
        &["push", "-q", &server.repo_url("o", "r"), "main"],
        src.path(),
    )?;
    let id = walgit_git::RepoId::new("o", "r")?;
    let h = step!("open", server.state.registry.open(&id))?;
    let seq = h.manifest().head_seq;
    // Stale verdict: claims the blob is missing although a manual add-pack (or
    // another repair) already put it in the current live set.
    let report = walgit_proto::v1::FsckReport {
        seq,
        at: Some(walgit_proto::time::now()),
        missing: vec![blob],
        missing_total: 1,
        ..Default::default()
    };
    h.store()
        .put_bytes(
            walgit_proto::keys::GC_AUDIT,
            report.encode_to_vec(),
            walgit_store::PutMode::Overwrite,
        )
        .await?;

    let v = run_op(&server, &id, "repair", &[])
        .await
        .expect("stale finding should complete without upstream");
    assert_eq!(v["already_satisfied"], true, "{v}");
    let report = read_audit(&h).await.expect("gc/audit.pb");
    assert_eq!(report.repaired_seq, seq, "{report:?}");
    Ok(())
}
