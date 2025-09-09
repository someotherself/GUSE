use std::{path::PathBuf, sync::atomic::{AtomicBool, AtomicUsize}};

use anyhow::anyhow;
use git2::Oid;
use tempfile::TempDir;

use crate::{fs::{ops::readdir::{classify_inode, DirCase}, GitFs}, inodes::NormalIno};

pub struct BuildSession {
    pub folder: TempDir,
    pub open_count: AtomicUsize,
    pub pinned: AtomicBool,
}

enum TargetCommit {
    BuildHead(Oid),
    Commit(Oid)
}

pub struct BuildOperationCtx {
    ino: NormalIno,
    target: TargetCommit,
    build_root: PathBuf,
    temp_dir: PathBuf,
}

impl BuildOperationCtx {
    fn new(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Option<Self>> {
        let case = classify_inode(fs, ino.to_norm_u64())?;

        let  DirCase::Commit { oid }  = case else {
            return Ok(None)
        };

        let target = if oid == Oid::zero() {
            let parent_oid = fs.parent_commit_build_session(ino)?;
            TargetCommit::BuildHead(parent_oid)
        } else {
            let exists = {
                let repo = fs.get_repo(ino.to_norm_u64())?;
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.inner.find_commit(oid).is_ok()
            };
            if !exists {
                return Ok(None)
            }
            TargetCommit::Commit(oid)
        };

        let build_root = fs.get_path_to_build_folder(ino)?;

        let temp_dir = {
            let oid = match target {
                TargetCommit::BuildHead(o) | TargetCommit::Commit(o) => o
            };
            let repo = fs.get_repo(ino.to_norm_u64())?;
            let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.get_build_state(oid, &build_root)?
        };

        Ok(Some(Self {
            ino,
            target,
            build_root,
            temp_dir
        }))
    }
}