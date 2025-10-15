use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize},
};

use git2::Oid;
use tempfile::TempDir;

use crate::{
    fs::{
        GitFs,
        fileattr::InoFlag,
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
        let mut ino_flag = fs.get_ino_flag_from_db(cur_ino.into())?;

        let max_loops = 1000;

        for _ in 0..max_loops {
            if ino_flag == InoFlag::SnapFolder {
                break;
            }
            components.push(fs.get_name_from_db(cur_ino)?);
            cur_ino = fs.get_single_parent(cur_ino)?;
            ino_flag = fs.get_ino_flag_from_db(cur_ino.into())?;
        }

        components.reverse();
        let full_path = temp_dir_path.join(components.iter().collect::<PathBuf>());

        Ok(full_path)
    }

    pub fn temp_dir(&self) -> PathBuf {
        self.folder.path().to_path_buf()
    }
}

/// Used by readdir, create and mkdir
pub struct BuildOperationCtx {
    ino: NormalIno,
    target: Oid,
    temp_dir: PathBuf,
    full_path: PathBuf,
}

impl BuildOperationCtx {
    pub fn new(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Option<Self>> {
        let case = classify_inode(fs, ino.to_norm_u64())?;

        let DirCase::Commit { oid } = case else {
            return Ok(None);
        };

        let ino_flag = fs.get_ino_flag_from_db(ino)?;
        let target = if ino_flag == InoFlag::SnapFolder || ino_flag == InoFlag::InsideBuild {
            oid
        } else {
            return Ok(None);
        };

        let build_root = fs.get_path_to_build_folder(ino)?;

        let build_session = {
            let repo = fs.get_repo(ino.to_norm_u64())?;
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

    pub fn commit_oid(&self) -> Oid {
        self.target
    }
}
