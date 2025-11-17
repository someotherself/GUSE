use std::{
    path::PathBuf,
    sync::atomic::{AtomicBool, AtomicUsize},
};

use git2::Oid;
use tempfile::TempDir;

pub mod chase;
pub mod chase_resolver;
pub mod inject;
pub mod reporter;
pub mod runtime;

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

/// Used by readdir, create, mkdir and link
pub struct BuildOperationCtx {
    target: Oid,
    full_path: PathBuf,
}

impl BuildOperationCtx {
    // /// Creates a new sessions, and inserts into build_sessions map
    // pub fn new(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Self> {
    //     let metadata = fs.get_builctx_metadata(ino)?;

    //     let target = if metadata.ino_flag == InoFlag::SnapFolder
    //         || metadata.ino_flag == InoFlag::InsideBuild
    //     {
    //         metadata.oid
    //     } else {
    //         tracing::error!("Wrong ino_flag: {}", metadata.ino_flag);
    //         bail!(std::io::Error::from_raw_os_error(libc::EPERM))
    //     };

    //     let repo = fs.get_repo(ino.to_norm_u64())?;
    //     let build_root = &repo.build_dir;
    //     let build_session = repo.get_or_init_build_session(metadata.oid, build_root)?;
    //     let full_path = build_session.finish_path(fs, ino)?;

    //     Ok(Self { target, full_path })
    // }

    // /// Creates a new session, without saving in in teh Repo State
    // pub fn init(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Self> {
    //     let metadata = fs.get_builctx_metadata(ino)?;

    //     let target = if metadata.ino_flag == InoFlag::SnapFolder
    //         || metadata.ino_flag == InoFlag::InsideBuild
    //     {
    //         metadata.oid
    //     } else {
    //         bail!(std::io::Error::from_raw_os_error(libc::EPERM))
    //     };

    //     let repo = fs.get_repo(ino.to_norm_u64())?;
    //     let build_root = &repo.build_dir;
    //     let build_session = repo.new_build_session(metadata.oid, build_root)?;
    //     let full_path = build_session.finish_path(fs, ino)?;

    //     Ok(Self { target, full_path })
    // }

    #[inline]
    pub fn path(&self) -> PathBuf {
        self.full_path.clone()
    }

    #[inline]
    pub fn commit_oid(&self) -> Oid {
        self.target
    }
}
