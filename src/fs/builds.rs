use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize},
};

use anyhow::anyhow;
use git2::Oid;
use tempfile::TempDir;
use tracing::{info, instrument};

use crate::{
    fs::{
        GitFs,
        ops::readdir::{DirCase, classify_inode},
    },
    inodes::NormalIno,
};

#[derive(Debug)]
pub struct BuildSession {
    /// Relative path to tempdir
    pub folder: TempDir,
    pub open_count: AtomicUsize,
    pub pinned: AtomicBool,
}

impl BuildSession {
    pub fn finish_path(&self, fs: &GitFs, ino: NormalIno) -> anyhow::Result<PathBuf> {
        let temp_dir_path = self.folder.path().to_path_buf();

        let mut components = vec![];

        let mut cur_ino = ino.to_norm_u64();
        let mut cur_oid = fs.get_oid_from_db(cur_ino)?;

        let max_loops = 1000;

        for _ in 0..max_loops {
            if cur_oid != Oid::zero() {
                break;
            }
            components.push(fs.get_name_from_db(cur_ino)?);
            cur_ino = fs.get_single_parent(cur_ino)?;
            cur_oid = fs.get_oid_from_db(cur_ino)?;
        }

        components.reverse();
        let full_path = temp_dir_path.join(components.iter().collect::<PathBuf>());
        info!("{}", full_path.display());

        Ok(full_path)
    }

    pub fn temp_dir(&self) -> PathBuf {
        self.folder.path().to_path_buf()
    }
}

enum TargetCommit {
    BuildHead(Oid),
    Commit(Oid),
}

/// Used by readdir, create and mkdir
pub struct BuildOperationCtx {
    ino: NormalIno,
    target: TargetCommit,
    temp_dir: PathBuf,
    full_path: PathBuf,
}

impl BuildOperationCtx {
    #[instrument(level = "error", skip(fs), err(Display))]
    pub fn new(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Option<Self>> {
        let case = classify_inode(fs, ino.to_norm_u64())?;

        let DirCase::Commit { oid } = case else {
            return Ok(None);
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
                return Ok(None);
            }
            TargetCommit::Commit(oid)
        };

        let build_root = fs.get_path_to_build_folder(ino)?;

        let build_session = {
            let oid = match target {
                TargetCommit::BuildHead(o) | TargetCommit::Commit(o) => o,
            };
            let repo = fs.get_repo(ino.to_norm_u64())?;
            let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.get_or_init_build_session(oid, &build_root)?
        };
        let temp_dir = build_session.folder.path().to_path_buf();
        let full_path = build_session.finish_path(fs, ino)?;

        Ok(Some(Self {
            ino,
            target,
            temp_dir,
            full_path,
        }))
    }

    pub fn path(&self) -> PathBuf {
        self.full_path.clone()
    }
}
