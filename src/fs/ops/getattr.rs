use std::path::PathBuf;

use anyhow::{anyhow, bail};
use git2::Oid;
use tracing::{info, instrument};

use crate::{
    fs::{
        FileAttr, GitFs,
        fileattr::{InoFlag, dir_attr},
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
    attr: FileAttr,
    parent_commit: Oid,
    build_root: PathBuf,
    temp_folder: PathBuf,
    path: PathBuf,
    ino_flag: InoFlag,
}

impl GetAttrOperationCtx {
    #[instrument(level = "debug", skip(fs), fields(ino = %ino), err(Display))]
    pub fn get_target(fs: &GitFs, ino: NormalIno) -> anyhow::Result<TargetGetAttr> {
        info!("1");
        let attr = fs.get_metadata(ino.into())?;
        info!("ino {ino} is {} - 1", attr.ino_flag);
        info!("2");
        match attr.ino_flag {
            InoFlag::MonthFolder => Ok(TargetGetAttr::InsideRepo(Self {
                attr,
                parent_commit: Oid::zero(),
                build_root: PathBuf::new(),
                temp_folder: PathBuf::new(),
                path: PathBuf::new(),
                ino_flag: InoFlag::MonthFolder,
            })),
            InoFlag::BuildRoot => Ok(TargetGetAttr::InsideRepo(Self {
                attr,
                parent_commit: Oid::zero(),
                build_root: PathBuf::new(),
                temp_folder: PathBuf::new(),
                path: PathBuf::new(),
                ino_flag: InoFlag::BuildRoot,
            })),
            InoFlag::SnapFolder => Ok(TargetGetAttr::InsideMonth(Self {
                attr,
                parent_commit: Oid::zero(),
                build_root: PathBuf::new(),
                temp_folder: PathBuf::new(),
                path: PathBuf::new(),
                ino_flag: InoFlag::SnapFolder,
            })),
            InoFlag::InsideSnap => {
                info!("ino {ino} is {} - 2", attr.ino_flag);
                info!("3");
                let parent_commit = fs.get_parent_commit(ino.to_norm_u64())?;
                info!("4");
                let build_root = fs.get_path_to_build_folder(ino)?; // TODO
                info!("5");
                let build_session = {
                    let repo = fs.get_repo(ino.to_norm_u64())?;
                    let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                    repo.get_or_init_build_session(parent_commit, &build_root)?
                };
                info!("6");
                Ok(TargetGetAttr::InsideSnap(Self {
                    attr,
                    parent_commit,
                    build_root,
                    temp_folder: build_session.temp_dir(),
                    path: PathBuf::new(),
                    ino_flag: InoFlag::InsideSnap,
                }))
            }
            InoFlag::InsideBuild => {
                let parent_commit = fs.get_parent_commit(ino.to_norm_u64())?;
                let build_root = fs.get_path_to_build_folder(ino)?; // TODO
                let build_session = {
                    let repo = fs.get_repo(ino.to_norm_u64())?;
                    let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                    repo.get_or_init_build_session(parent_commit, &build_root)?
                };
                let path = build_session.finish_path(fs, ino)?;
                Ok(TargetGetAttr::InsideBuild(Self {
                    attr,
                    parent_commit,
                    build_root,
                    temp_folder: build_session.temp_dir(),
                    path,
                    ino_flag: InoFlag::InsideBuild,
                }))
            }
            _ => {
                bail!(
                    "Wrong location for ino_flag {} ino {}",
                    attr.ino_flag,
                    attr.ino
                )
            }
        }
    }

    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }

    pub fn parent_commit(&self) -> Oid {
        self.parent_commit
    }

    pub fn temp_folder(&self) -> PathBuf {
        self.temp_folder.clone()
    }
}

pub fn getattr_live_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<FileAttr> {
    fs.refresh_metadata_from_disk(ino) // TODO: Update DB?
}

pub fn getattr_git_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<FileAttr> {
    let ctx = GetAttrOperationCtx::get_target(fs, ino)?;
    match ctx {
        TargetGetAttr::InsideRepo(ctx) => {
            let mut attr: FileAttr = dir_attr(ctx.ino_flag).into();
            attr.ino = ino.to_norm_u64();
            Ok(attr)
        }
        TargetGetAttr::InsideMonth(ctx) => {
            let mut attr: FileAttr = dir_attr(ctx.ino_flag).into();
            attr.ino = ino.to_norm_u64();
            Ok(attr)
        }
        TargetGetAttr::InsideSnap(ctx) => {
            dbg!(ctx.parent_commit());
            dbg!(ctx.attr.oid);
            let object_attr = {
                let repo = fs.get_repo(ino.to_norm_u64())?;
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.find_in_commit(ctx.parent_commit(), ctx.attr.oid)?
            };
            let mut attr =
                fs.object_to_file_attr(ino.to_norm_u64(), &object_attr, InoFlag::InsideSnap)?;
            attr.ino = ino.to_norm_u64();
            Ok(attr)
        }
        TargetGetAttr::InsideBuild(ctx) => Ok(ctx.attr),
    }
}
