use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, AtomicUsize},
};

use anyhow::anyhow;
use git2::Oid;
use tempfile::TempDir;

use crate::{
    fs::{
        GitFs,
        ops::readdir::{DirCase, classify_inode},
    },
    inodes::NormalIno,
};

#[derive(Debug)]
pub struct BuildSession {
    pub folder: TempDir,
    pub open_count: AtomicUsize,
    pub pinned: AtomicBool,
}

impl BuildSession {
    pub fn finish_path(&self, fs: & GitFs, ino: NormalIno) -> anyhow::Result<PathBuf> {
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
            cur_ino = fs.get_parent_ino(cur_ino)?;
            cur_oid = fs.get_oid_from_db(cur_ino)?;
        }

        components.reverse();
        let full_path = temp_dir_path.join(components.iter().collect::<PathBuf>());
        tracing::info!("{}", full_path.display());

        Ok(full_path)
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
    build_root: PathBuf,
    temp_dir: PathBuf,
}

impl BuildOperationCtx {
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

        Ok(Some(Self {
            ino,
            target,
            build_root,
            temp_dir,
        }))
    }

    pub fn temp_dir_path(&self) -> PathBuf {
        self.build_root.join(&self.temp_dir)
    }

    pub fn child_in_temp<P: AsRef<Path>>(&self, p: P) -> PathBuf {
        self.temp_dir_path().join(p)
    }
}

