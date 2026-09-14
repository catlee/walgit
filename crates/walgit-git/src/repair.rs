//! Turn a missing-objects list into one publishable pack.
//!
//! The maintainer's `repair` unit (desired state: every object reachable from
//! refs is in a live pack) recovers objects into a scratch git dir — from
//! superseded packs still in the bucket (`walgit_wal::gc::recover_from_superseded`,
//! written as loose objects) and/or fetched from an upstream remote
//! ([`fetch_oids`]; the remote must serve wants by SHA — GitHub does, for
//! commits, trees and blobs reachable from any ref, verified 2026-08-21) —
//! then packs exactly the requested oids ([`pack_oids`]) for the WAL to
//! publish as a COMPACT entry. Never the serving copy.

use std::path::{Path, PathBuf};
use std::process::Stdio;

use crate::GitError;

pub struct RepairPack {
    pub pack: PathBuf,
    pub idx: PathBuf,
    pub objects: u64,
    pub bytes: u64,
}

fn git_cmd(git_dir: &Path, args: &[&str]) -> tokio::process::Command {
    let mut c = tokio::process::Command::new("git");
    c.arg("--git-dir")
        .arg(git_dir)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    c
}

fn ok(out: std::process::Output, what: &str) -> Result<std::process::Output, GitError> {
    if out.status.success() {
        Ok(out)
    } else {
        Err(GitError::Subprocess {
            cmd: what.to_string(),
            status: out.status.code(),
            stderr: String::from_utf8_lossy(&out.stderr).trim().to_string(),
        })
    }
}

/// A bare scratch repository at `dir` (idempotent: re-init of an existing
/// scratch keeps its objects — a resumed repair reuses what it recovered).
pub async fn init_scratch(dir: &Path) -> Result<(), GitError> {
    tokio::fs::create_dir_all(dir).await.map_err(GitError::Io)?;
    ok(
        git_cmd(dir, &["init", "-q", "--bare"])
            .output()
            .await
            .map_err(GitError::Io)?,
        "git init",
    )?;
    Ok(())
}

const FETCH_BATCH: usize = 500;

/// Fetch every oid in `oids` from `upstream` into the scratch at `git_dir`.
/// Wants are sent in batches (argv length, server limits). A refused want
/// surfaces later: [`pack_oids`] verifies every requested object.
pub async fn fetch_oids(
    git_dir: &Path,
    upstream: &str,
    token: Option<&str>,
    oids: &[String],
) -> Result<(), GitError> {
    let helper = token
        .map(|t| {
            format!(
                "!f(){{ echo username=x-access-token; echo 'password={}'; }}; f",
                t.replace('\'', "")
            )
        })
        .unwrap_or_default();
    for chunk in oids.chunks(FETCH_BATCH) {
        let mut args: Vec<&str> = vec![
            "-c",
            "fetch.negotiationAlgorithm=noop",
            "-c",
            "protocol.version=2",
        ];
        let helper_arg = format!("credential.helper={helper}");
        if token.is_some() {
            args.extend(["-c", helper_arg.as_str()]);
        }
        args.extend([
            "fetch",
            "--no-tags",
            "--no-write-fetch-head",
            "--quiet",
            "--depth=1",
            upstream,
        ]);
        args.extend(chunk.iter().map(String::as_str));
        ok(
            git_cmd(git_dir, &args).output().await.map_err(GitError::Io)?,
            "git fetch <upstream> <oids>",
        )?;
    }
    Ok(())
}

/// `pack-objects` exactly `oids` (no closure) from the scratch at `git_dir`
/// into `pack-<sha>.pack` + `.idx` beside it. **Every requested object must be
/// in the resulting pack** — a hole silently left open is worse than a failed
/// unit.
pub async fn pack_oids(git_dir: &Path, oids: &[String]) -> Result<RepairPack, GitError> {
    let pack_base = git_dir.join("pack");
    let mut child = git_cmd(
        git_dir,
        &[
            "pack-objects",
            "--no-reuse-delta",
            "--compression=6",
            pack_base.to_str().unwrap_or("pack"),
        ],
    )
    .stdin(Stdio::piped())
    .spawn()
    .map_err(GitError::Io)?;
    {
        use tokio::io::AsyncWriteExt;
        let mut stdin = child.stdin.take().ok_or_else(|| {
            GitError::InvalidInput("git pack-objects stdin unavailable".to_owned())
        })?;
        let mut input = oids.join("\n");
        input.push('\n');
        stdin
            .write_all(input.as_bytes())
            .await
            .map_err(GitError::Io)?;
    }
    let out = ok(
        child.wait_with_output().await.map_err(GitError::Io)?,
        "git pack-objects",
    )?;
    let sha = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if sha.len() < 40 {
        return Err(GitError::Protocol(format!(
            "pack-objects printed no checksum: {sha:?}"
        )));
    }
    let pack = git_dir.join(format!("pack-{sha}.pack"));
    let idx = git_dir.join(format!("pack-{sha}.idx"));
    let bytes = tokio::fs::metadata(&pack)
        .await
        .map_err(GitError::Io)?
        .len();
    let index = gix_pack::index::File::at(&idx, gix_hash::Kind::Sha1)
        .map_err(|e| GitError::Gix(Box::new(e)))?;
    let mut objects = 0u64;
    let mut first_missing = None;
    for o in oids {
        match gix_hash::ObjectId::from_hex(o.as_bytes()) {
            Ok(id) if index.lookup(id).is_some() => objects += 1,
            _ => {
                first_missing.get_or_insert(o.as_str());
            }
        }
    }
    if let Some(m) = first_missing {
        return Err(GitError::Protocol(format!(
            "recovered {objects} of {} requested objects (first missing: {m})",
            oids.len()
        )));
    }
    Ok(RepairPack {
        pack,
        idx,
        objects,
        bytes,
    })
}

/// Write one loose object into the scratch (skip if present). Standalone
/// (no `LocalRepo`): the scratch is a bare `git init` dir, not a serving copy.
pub fn write_loose(
    git_dir: &Path,
    kind: gix_object::Kind,
    oid: &gix_hash::oid,
    data: &[u8],
) -> Result<(), GitError> {
    use gix_object::Write as _;
    let hex = oid.to_hex().to_string();
    let path = git_dir
        .join("objects")
        .join(hex.get(..2).ok_or_else(|| GitError::InvalidInput("short object ID".into()))?)
        .join(hex.get(2..).ok_or_else(|| GitError::InvalidInput("short object ID".into()))?);
    if path.exists() {
        return Ok(());
    }
    let store = gix_odb::loose::Store::at(
        git_dir.join("objects"),
        gix_odb::loose::Options {
            object_hash: oid.kind(),
            ..Default::default()
        },
    );
    store
        .write_buf_with_known_id(kind, data, oid.to_owned())
        .map_err(GitError::Gix)?;
    Ok(())
}
