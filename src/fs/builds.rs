use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize},
};

use anyhow::bail;
use git2::Oid;
use tempfile::TempDir;

pub mod chase;
pub mod chase_resolver;
pub mod chase_runner;
pub mod inject;
pub mod job;
pub mod logger;
pub mod reporter;
pub mod runtime;

use crate::{
    fs::{GitFs, fileattr::InoFlag},
    inodes::NormalIno,
};

/// Allows creating files/folders in a Snap folder (which is normally read only)
///
/// When a user uses touch/mkdir/ln a build folder is creates in repo_dir/build
/// Any files or folders created in the Snap folder, are actually created in repo_dir/build/build_<rand>
///
/// The file system then redirects them to show to be inside the Snap folder
#[derive(Debug)]
pub struct BuildSession {
    /// Relative path to tempdir
    pub folder: TempDir,
    pub open_count: AtomicUsize,
    pub pinned: AtomicBool,
}

impl BuildSession {
    /// For a given inode, it will create that real path in the build folder
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

    #[inline]
    pub fn temp_dir(&self) -> PathBuf {
        self.folder.path().to_path_buf()
    }
}

/// Used by readdir, create, mkdir and link
pub struct BuildOperationCtx {
    target: Oid,
    full_path: PathBuf,
}

impl BuildOperationCtx {
    /// Creates a new sessions, and inserts into build_sessions map
    pub fn new(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Self> {
        let metadata = fs.get_builctx_metadata(ino)?;

        let target = if metadata.ino_flag == InoFlag::SnapFolder
            || metadata.ino_flag == InoFlag::InsideBuild
        {
            metadata.oid
        } else {
            bail!(std::io::Error::from_raw_os_error(libc::EPERM))
        };

        let repo = fs.get_repo(ino.to_norm_u64())?;
        let build_root = &repo.build_dir;
        let build_session = repo.get_or_init_build_session(metadata.oid, build_root)?;
        let full_path = build_session.finish_path(fs, ino)?;

        Ok(Self { target, full_path })
    }

    /// Creates a new session, without saving in in teh Repo State
    pub fn init(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Self> {
        let metadata = fs.get_builctx_metadata(ino)?;

        let target = if metadata.ino_flag == InoFlag::SnapFolder
            || metadata.ino_flag == InoFlag::InsideBuild
        {
            metadata.oid
        } else {
            bail!(std::io::Error::from_raw_os_error(libc::EPERM))
        };

        let repo = fs.get_repo(ino.to_norm_u64())?;
        let build_root = &repo.build_dir;
        let build_session = repo.new_build_session(metadata.oid, build_root)?;
        let full_path = build_session.finish_path(fs, ino)?;

        Ok(Self { target, full_path })
    }

    #[inline]
    pub fn path(&self) -> PathBuf {
        self.full_path.clone()
    }

    #[inline]
    pub fn commit_oid(&self) -> Oid {
        self.target
    }
}
