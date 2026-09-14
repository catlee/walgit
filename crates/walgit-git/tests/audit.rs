// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::unwrap_used)]

//! The connectivity-lite audit walk (`docs/GC.md` §2, D42): a clean repo
//! reports nothing; an object absent from the probed pack set is a finding;
//! gitlinks are never probed; annotated tags chase their target.

mod common;

use walgit_git::{IngestOptions, LocalRepo, ObjectFormat, RepoId, audit, gix_hash};

mod cm {
    pub use super::common::*;
}

/// Ingest the closure of `refs` into a fresh `LocalRepo` and point refs at it.
async fn setup(src: &cm::SourceRepo, refs: &[(&str, &str)]) -> (tempfile::TempDir, LocalRepo) {
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "audit").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let revs: Vec<&str> = refs.iter().map(|(_, oid)| *oid).collect();
    let pack = src.pack(&revs, &[], false);
    repo.ingest_pack(
        cm::cursor(pack),
        IngestOptions {
            fsck: false,
            max_bytes: None,
            thin: false,
        },
    )
    .await
    .unwrap()
    .unwrap();
    let txn = walgit_proto::v1::RefTransaction {
        updates: refs
            .iter()
            .map(|(name, oid)| walgit_proto::v1::RefUpdate {
                name: (*name).to_string(),
                new_oid: (*oid).to_string(),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    repo.apply_ref_txn(&txn, false).unwrap();
    (root, repo)
}

/// Probe = "some pack in the odb has it", minus an optional simulated hole:
/// the full object list is materialized up front (test-sized repos) so the
/// probe is `Sync` like the real idx-file probe.
fn odb_probe(
    repo: &LocalRepo,
    hole: Option<gix_hash::ObjectId>,
) -> impl Fn(&gix_hash::oid) -> bool + Sync + use<> {
    let all: std::collections::HashSet<gix_hash::ObjectId> = cm::run_git(
        repo.path(),
        &["cat-file", "--batch-all-objects", "--batch-check=%(objectname)"],
    )
    .lines()
    .map(|l| gix_hash::ObjectId::from_hex(l.trim().as_bytes()).unwrap())
    .collect();
    move |oid: &gix_hash::oid| hole.as_deref() != Some(oid) && all.contains(oid)
}

fn quiet(_: String) {}

#[tokio::test]
async fn clean_repo_reports_nothing() {
    let src = cm::SourceRepo::new();
    src.commit_file("dir/file2.txt", "world\n", "second");
    let tag = src.annotated_tag("v1", "HEAD");
    let head = src.head();
    let (_r, repo) = setup(
        &src,
        &[("refs/heads/main", head.as_str()), ("refs/tags/v1", tag.as_str())],
    )
    .await;

    let probe = odb_probe(&repo, None);
    let out =
        tokio::task::spawn_blocking(move || audit::connectivity_walk(&repo, &probe, 100, &quiet))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(out.missing_total, 0, "{:?}", out.missing);
    assert_eq!(out.commits, 2);
    assert!(out.trees >= 3, "root trees + dir/: {}", out.trees);
    assert!(out.blob_probes >= 2);
}

#[tokio::test]
async fn a_hole_in_the_pack_set_is_a_finding() {
    let src = cm::SourceRepo::new();
    src.commit_file("file2.txt", "world\n", "second");
    let blob = src.rev("HEAD:file2.txt");
    let head = src.head();
    let (_r, repo) = setup(&src, &[("refs/heads/main", head.as_str())]).await;

    let hole = gix_hash::ObjectId::from_hex(blob.as_bytes()).unwrap();
    let probe = odb_probe(&repo, Some(hole));
    let out =
        tokio::task::spawn_blocking(move || audit::connectivity_walk(&repo, &probe, 100, &quiet))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(out.missing_total, 1);
    assert_eq!(out.missing, vec![hole]);
}

#[tokio::test]
async fn gitlinks_are_never_probed() {
    let src = cm::SourceRepo::new();
    // A gitlink entry pointing at a commit that exists nowhere: reachable
    // trees carry it, but it belongs to another repository.
    let fake = "deadbeefdeadbeefdeadbeefdeadbeefdeadbeef";
    cm::run_git(
        &src.dir,
        &[
            "update-index",
            "--add",
            "--cacheinfo",
            &format!("160000,{fake},submodule"),
        ],
    );
    cm::run_git(&src.dir, &["commit", "-q", "-m", "add submodule"]);
    let head = src.head();
    let (_r, repo) = setup(&src, &[("refs/heads/main", head.as_str())]).await;

    let hole = gix_hash::ObjectId::from_hex(fake.as_bytes()).unwrap();
    let probe = odb_probe(&repo, Some(hole));
    let out =
        tokio::task::spawn_blocking(move || audit::connectivity_walk(&repo, &probe, 100, &quiet))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(out.missing_total, 0, "gitlink probed: {:?}", out.missing);
}

#[tokio::test]
async fn a_missing_commit_and_tree_are_findings_too() {
    let src = cm::SourceRepo::new();
    let first = src.head();
    src.commit_file("file2.txt", "world\n", "second");
    let head = src.head();
    let root_tree = src.rev("HEAD^{tree}");
    let (root, repo) = setup(&src, &[("refs/heads/main", head.as_str())]).await;

    for hex in [first.as_str(), root_tree.as_str()] {
        let hole = gix_hash::ObjectId::from_hex(hex.as_bytes()).unwrap();
        let probe = odb_probe(&repo, Some(hole));
        let repo = LocalRepo::open(root.path(), &RepoId::new("acme", "audit").unwrap())
            .unwrap()
            .unwrap();
        let out = tokio::task::spawn_blocking(move || {
            audit::connectivity_walk(&repo, &probe, 100, &quiet)
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(out.missing, vec![hole]);
    }
}

#[tokio::test]
async fn nested_tags_are_probed_link_by_link() {
    let src = cm::SourceRepo::new();
    let inner = src.annotated_tag("v1", "HEAD");
    // A tag of a tag: refs advertise only the outer tag (peeled to the commit —
    // the snapshot's shortcut must not skip the inner link).
    cm::run_git(&src.dir, &["tag", "-a", "wrapped", "-m", "wrap", &inner]);
    let outer = src.rev("refs/tags/wrapped");
    let (_r, repo) = setup(&src, &[("refs/tags/wrapped", outer.as_str())]).await;

    // Clean: both tag objects and the commit closure are probed.
    let probe = odb_probe(&repo, None);
    let repo2 = repo.clone();
    let out =
        tokio::task::spawn_blocking(move || audit::connectivity_walk(&repo2, &probe, 100, &quiet))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(out.missing_total, 0, "{:?}", out.missing);

    // A hole at the INNER tag is a finding, not a clean verdict.
    let hole = gix_hash::ObjectId::from_hex(inner.as_bytes()).unwrap();
    let probe = odb_probe(&repo, Some(hole));
    let out =
        tokio::task::spawn_blocking(move || audit::connectivity_walk(&repo, &probe, 100, &quiet))
            .await
            .unwrap()
            .unwrap();
    assert_eq!(out.missing, vec![hole]);
}

#[tokio::test]
async fn a_missing_commit_with_unreadable_content_is_a_finding_not_an_error() {
    // The realistic race-2 shape: the superseded packs left this copy, so a
    // missing commit's CONTENT is unreadable too. The walk must record the
    // finding and stop there (like git fsck), never error — an erroring audit
    // writes no verdict and would let a stale clean one stand.
    let src = cm::SourceRepo::new();
    let parent = src.head();
    src.commit_file("f2.txt", "two\n", "second");
    let tip = src.head();

    // A pack holding only the tip's delta over the parent — the parent commit
    // and its closure are genuinely absent from the local odb.
    let root = tempfile::TempDir::new().unwrap();
    let id = RepoId::new("acme", "audit-hole").unwrap();
    let repo = LocalRepo::init(root.path(), &id, ObjectFormat::Sha1).unwrap();
    let pack = src.pack(&[tip.as_str()], &[parent.as_str()], false);
    repo.ingest_pack(
        cm::cursor(pack),
        IngestOptions {
            fsck: false,
            max_bytes: None,
            thin: false,
        },
    )
    .await
    .unwrap()
    .unwrap();
    let txn = walgit_proto::v1::RefTransaction {
        updates: vec![walgit_proto::v1::RefUpdate {
            name: "refs/heads/main".into(),
            new_oid: tip.clone(),
            ..Default::default()
        }],
        ..Default::default()
    };
    repo.apply_ref_txn(&txn, false).unwrap();

    let hole = gix_hash::ObjectId::from_hex(parent.as_bytes()).unwrap();
    let probe = odb_probe(&repo, None); // odb genuinely lacks the parent
    let out =
        tokio::task::spawn_blocking(move || audit::connectivity_walk(&repo, &probe, 100, &quiet))
            .await
            .unwrap()
            .expect("a missing+unreadable commit must not error the audit");
    assert!(out.missing.contains(&hole), "{:?}", out.missing);
}
