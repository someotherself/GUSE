use std::path::PathBuf;

use anyhow::{anyhow, bail};
use git2::Oid;

use crate::{
    fs::{
        FileAttr, GitFs,
        fileattr::{dir_attr, file_attr},
        ops::readdir::{DirCase, classify_inode},
    },
    inodes::NormalIno,
};

pub enum TargetGetAttr {
    /// Either a MONTH folder, or the build folder
    InsideRepo(GetAttrOperationCtx),
    /// One of the Snap folders
    InsideMonth(GetAttrOperationCtx),
    /// Git object inside the Snap folder. Only includes git objects.
    ///
    /// Share the same parent as real files inside Build, but need different handling
    InsideSnap(GetAttrOperationCtx),
    /// Real file inside the build folder
    ///
    /// Share the same parent as objects inside Snap, but need different handling
    InsideBuild(GetAttrOperationCtx),
}

pub struct GetAttrOperationCtx {
    ino: NormalIno,
    parent_tree: Oid,
    parent_commit: Oid,
    build_root: PathBuf,
    temp_folder: PathBuf,
    path: PathBuf,
}

impl GetAttrOperationCtx {
    pub fn get_target(fs: &GitFs, ino: NormalIno) -> anyhow::Result<TargetGetAttr> {
        let parent_ino = fs.get_parent_ino(ino.to_norm_u64())?;
        let repo_ino = fs.get_repo_ino(ino.to_norm_u64())?;
        if parent_ino == repo_ino {
            if let Ok(DirCase::Month { year: _, month: _ }) = classify_inode(fs, ino.to_norm_u64())
            {
                // Target is one of the MONTH folders
                return Ok(TargetGetAttr::InsideRepo(Self {
                    ino,
                    parent_tree: Oid::zero(),
                    parent_commit: Oid::zero(),
                    build_root: PathBuf::new(),
                    temp_folder: PathBuf::new(),
                    path: PathBuf::new(),
                }));
            } else if fs.get_name_from_db(ino.to_norm_u64())? == "build" {
                // Target is the build folder
                return Ok(TargetGetAttr::InsideRepo(Self {
                    ino,
                    parent_tree: Oid::zero(),
                    parent_commit: Oid::zero(),
                    build_root: PathBuf::new(),
                    temp_folder: PathBuf::new(),
                    path: PathBuf::new(),
                }));
            }
            bail!("Ino {ino} does not exist")
        }

        if let Ok(DirCase::Month { year: _, month: _ }) = classify_inode(fs, parent_ino) {
            return Ok(TargetGetAttr::InsideMonth(Self {
                ino,
                parent_tree: Oid::zero(),
                parent_commit: Oid::zero(),
                build_root: PathBuf::new(),
                temp_folder: PathBuf::new(),
                path: PathBuf::new(),
            }))
        }

        let (parent_commit, _) = fs.get_parent_commit(ino.to_norm_u64())?;
        let oid = fs.get_oid_from_db(ino.to_norm_u64())?;
        // let commit_oid = commit.id();
        let build_root = fs.get_path_to_build_folder(ino)?;
        let build_session = {
            let repo = fs.get_repo(ino.to_norm_u64())?;
            let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.get_or_init_build_session(parent_commit, &build_root)?
        };
        if oid != Oid::zero() {
            let parent_tree = fs.get_oid_from_db(parent_ino)?;
            Ok(TargetGetAttr::InsideSnap(Self {
                ino,
                parent_tree,
                parent_commit,
                build_root,
                temp_folder: build_session.temp_dir(),
                path: PathBuf::new(),
            }))
        } else {
            // Target is a real file/folder
            let path = build_session.finish_path(fs, ino)?;
            Ok(TargetGetAttr::InsideBuild(Self {
                ino,
                parent_tree: Oid::zero(),
                parent_commit,
                build_root,
                temp_folder: build_session.temp_dir(),
                path,
            }))
        }
    }

    pub fn is_in_build(&self) -> bool {
        self.build_root != PathBuf::new() && self.path != PathBuf::new()
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn parent_tree(&self) -> Oid {
        self.parent_tree
    }

    pub fn parent_commit(&self) -> Oid {
        self.parent_commit
    }

    pub fn temp_folder(&self) -> PathBuf {
        self.temp_folder.clone()
    }
}

pub fn getattr_live_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<FileAttr> {
    let ino = u64::from(ino);
    let filemode = fs.get_mode_from_db(ino)?;
    let mut attr: FileAttr = match filemode {
        git2::FileMode::Tree => dir_attr().into(),
        git2::FileMode::Commit => dir_attr().into(),
        _ => file_attr().into(),
    };
    attr.ino = ino;
    let attr = fs.refresh_attr(&mut attr)?;
    Ok(attr)
}

pub fn getattr_git_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<FileAttr> {
    let ctx = GetAttrOperationCtx::get_target(fs, ino)?;
    match ctx {
        TargetGetAttr::InsideRepo(_) => {
            let mut attr: FileAttr = dir_attr().into();
            attr.ino = ino.to_norm_u64();
            Ok(attr)
        }
        TargetGetAttr::InsideMonth(_) => {
            let mut attr: FileAttr = dir_attr().into();
            attr.ino = ino.to_norm_u64();
            Ok(attr)
        }
        TargetGetAttr::InsideSnap(ctx) => {
            let oid = fs.get_oid_from_db(ino.to_norm_u64())?;
            let object_attr = {
                let repo = fs.get_repo(ino.to_norm_u64())?;
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.find_in_commit(ctx.parent_commit(), oid)?
            };
            let mut attr = fs.object_to_file_attr(ino.to_norm_u64(), &object_attr)?;
            attr.ino = ino.to_norm_u64();
            Ok(attr)
        }
        TargetGetAttr::InsideBuild(ctx) => {
            let mut attr = fs.attr_from_path(ctx.temp_folder())?;
            attr.ino = ino.to_norm_u64();
            Ok(attr)
        }
    }
}
