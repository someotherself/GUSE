use std::path::PathBuf;

use git2::Oid;

use anyhow::{anyhow, bail};
use tracing::instrument;

use crate::{
    fs::{
        FileAttr, GitFs, REPO_SHIFT, build_attr_dir,
        fileattr::{InoFlag, dir_attr},
    },
    inodes::{NormalIno, VirtualIno},
};

pub enum TargetAttr {
    /// Target is a MONTH folder, or the build folder
    InsideRepo(LookOperationCtx),
    /// One of the Snap folders
    InsideMonth(LookOperationCtx),
    /// Git object inside the Snap folder. Only includes git objects.
    ///
    /// Share the same parent as real files inside Build, but need different handling
    InsideSnap(LookOperationCtx),
    /// Real file inside the build folder
    ///
    /// Share the same parent as objects inside Snap, but need different handling
    InsideBuild(LookOperationCtx),
}

pub struct LookOperationCtx {
    attr: FileAttr,
    parent_commit: Oid,
    build_root: PathBuf,
    temp_folder: PathBuf,
    path: PathBuf,
    ino_flag: InoFlag,
}

impl LookOperationCtx {
    pub fn get_target(fs: &GitFs, parent: NormalIno, name: &str) -> anyhow::Result<TargetAttr> {
        let attr = fs.get_metadata(parent.into())?;

        match attr.ino_flag {
            InoFlag::RepoRoot => Ok(TargetAttr::InsideRepo(Self {
                attr,
                parent_commit: Oid::zero(),
                build_root: PathBuf::new(),
                temp_folder: PathBuf::new(),
                path: PathBuf::new(),
                ino_flag: InoFlag::MonthFolder,
            })),
            InoFlag::MonthFolder if name == "live" => Ok(TargetAttr::InsideRepo(Self {
                attr,
                parent_commit: Oid::zero(),
                build_root: PathBuf::new(),
                temp_folder: PathBuf::new(),
                path: PathBuf::new(),
                ino_flag: InoFlag::LiveRoot,
            })),
            InoFlag::MonthFolder if name == "build" => Ok(TargetAttr::InsideRepo(Self {
                attr,
                parent_commit: Oid::zero(),
                build_root: PathBuf::new(),
                temp_folder: PathBuf::new(),
                path: PathBuf::new(),
                ino_flag: InoFlag::BuildRoot,
            })),
            InoFlag::MonthFolder => Ok(TargetAttr::InsideRepo(Self {
                attr,
                parent_commit: Oid::zero(),
                build_root: PathBuf::new(),
                temp_folder: PathBuf::new(),
                path: PathBuf::new(),
                ino_flag: InoFlag::SnapFolder,
            })),
            InoFlag::BuildRoot | InoFlag::InsideBuild => {
                // Inside Build
                todo!()
            }
            InoFlag::SnapFolder | InoFlag::InsideSnap => {
                let parent_commit = fs.get_parent_commit(parent.to_norm_u64())?;
                let build_root = fs.get_path_to_build_folder(parent)?;
                let build_session = {
                    let repo = fs.get_repo(parent.to_norm_u64())?;
                    let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                    repo.get_or_init_build_session(parent_commit, &build_root)?
                };
                Ok(TargetAttr::InsideSnap(Self {
                    attr,
                    parent_commit,
                    build_root,
                    temp_folder: build_session.temp_dir(),
                    path: PathBuf::new(),
                    ino_flag: InoFlag::InsideSnap,
                }))
            }
            _ => bail!(
                "Wrong location for ino_flag {} ino {}",
                attr.ino_flag,
                attr.ino
            ),
        }

        // let repo_ino = fs.get_repo_ino(parent.to_norm_u64())?;
        // let target_ino = fs.get_ino_from_db(parent.into(), name)?;
        // let target_ino: Inodes = target_ino.into();
        // if repo_ino == parent.to_norm_u64() {
        //     if let Some((y, m)) = name.split_once('-')
        //         && let (Ok(_), Ok(_)) = (y.parse::<i32>(), m.parse::<u32>())
        //     {
        //         return Ok(TargetAttr::InsideRepo(Self {
        //             ino: target_ino.to_norm(),
        //             parent_commit: Oid::zero(),
        //             build_root: PathBuf::new(),
        //             temp_folder: PathBuf::new(),
        //             path: PathBuf::new(),
        //             ino_flag: InoFlag::MonthFolder,
        //         }));
        //     }
        //     if name == "build" {
        //         return Ok(TargetAttr::InsideRepo(Self {
        //             ino: target_ino.to_norm(),
        //             parent_commit: Oid::zero(),
        //             build_root: PathBuf::new(),
        //             temp_folder: PathBuf::new(),
        //             path: PathBuf::new(),
        //             ino_flag: InoFlag::BuildRoot,
        //         }));
        //     }
        //     bail!("Target {name} does not exist")
        // }

        // if let Ok(DirCase::Month { year: _, month: _ }) = classify_inode(fs, parent.to_norm_u64()) {
        //     if !name.starts_with("Snap") {
        //         bail!("Target {name} does not exist")
        //     }
        //     return Ok(TargetAttr::InsideMonth(Self {
        //         ino: target_ino.to_norm(),
        //         parent_commit: Oid::zero(),
        //         build_root: PathBuf::new(),
        //         temp_folder: PathBuf::new(),
        //         path: PathBuf::new(),
        //         ino_flag: InoFlag::SnapFolder,
        //     }));
        // }

        // let parent_commit = fs.get_parent_commit(parent.to_norm_u64())?;
        // let oid = fs.get_oid_from_db(target_ino.to_u64_n())?;
        // let build_root = fs.get_path_to_build_folder(parent)?;
        // let build_session = {
        //     let repo = fs.get_repo(parent.to_norm_u64())?;
        //     let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        //     repo.get_or_init_build_session(parent_commit, &build_root)?
        // };
        // if oid != Oid::zero() {
        //     Ok(TargetAttr::InsideSnap(Self {
        //         ino: target_ino.to_norm(),
        //         parent_commit,
        //         build_root,
        //         temp_folder: build_session.temp_dir(),
        //         path: PathBuf::new(),
        //         ino_flag: InoFlag::InsideSnap,
        //     }))
        // } else {
        //     let path = build_session.finish_path(fs, target_ino.to_norm())?;
        //     Ok(TargetAttr::InsideBuild(Self {
        //         ino: target_ino.to_norm(),
        //         parent_commit,
        //         build_root,
        //         temp_folder: build_session.temp_dir(),
        //         path,
        //         ino_flag: InoFlag::InsideBuild,
        //     }))
        // }
    }

    pub fn is_in_build(&self) -> bool {
        self.build_root != PathBuf::new() && self.path != PathBuf::new()
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

pub fn lookup_root(fs: &GitFs, name: &str) -> anyhow::Result<Option<FileAttr>> {
    // Handle a look-up for url -> github.tokio-rs.tokio.git
    let attr = fs.repos_list.values().find_map(|repo| {
        let (repo_name, repo_id) = {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned")).ok()?;
            (repo.repo_dir.clone(), repo.repo_id)
        };
        if repo_name == name {
            let perms = 0o775;
            let st_mode = libc::S_IFDIR | perms;
            let repo_ino = (repo_id as u64) << REPO_SHIFT;
            Some(build_attr_dir(repo_ino, InoFlag::Root, st_mode))
        } else {
            None
        }
    });
    Ok(attr)
}

#[instrument(level = "debug", skip(fs), fields(parent = %parent), err(Display))]
pub fn lookup_repo(fs: &GitFs, parent: NormalIno, name: &str) -> anyhow::Result<Option<FileAttr>> {
    let repo_id = GitFs::ino_to_repo_id(parent.into());
    let repo = match fs.repos_list.get(&repo_id) {
        Some(repo) => repo,
        None => return Ok(None),
    };
    let attr = if name == "live" {
        let live_ino = fs.get_ino_from_db(parent.into(), "live")?;
        let path = {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            fs.repos_dir.join(&repo.repo_dir)
        };
        let mut attr = fs.attr_from_path(InoFlag::LiveRoot, path)?;
        attr.ino = live_ino;
        attr
    } else {
        // It will always be a yyyy-mm folder
        // Build blank attr for it
        let res = {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.connection
                .lock()
                .map_err(|_| anyhow!("Lock poisoned"))?
                .get_ino_from_db(parent.into(), name)
        };
        let child_ino = match res {
            Ok(i) => i,
            Err(_) => return Ok(None),
        };
        let mut attr: FileAttr = dir_attr(InoFlag::MonthFolder).into();
        attr.ino = child_ino;
        attr
    };
    Ok(Some(attr))
}

#[instrument(level = "debug", skip(fs), fields(parent = %parent), err(Display))]
pub fn lookup_live(fs: &GitFs, parent: NormalIno, name: &str) -> anyhow::Result<Option<FileAttr>> {
    let parent = u64::from(parent);
    let repo_id = GitFs::ino_to_repo_id(parent);
    match fs.repos_list.get(&repo_id) {
        Some(_) => {}
        None => return Ok(None),
    };

    let attr = match fs.get_metadata_by_name(parent, name) {
        Ok(a) => a,
        Err(_) => return Ok(None),
    };

    Ok(Some(attr))
}

#[instrument(level = "debug", skip(fs), fields(parent = %parent), err(Display))]
pub fn lookup_git(fs: &GitFs, parent: NormalIno, name: &str) -> anyhow::Result<Option<FileAttr>> {
    let Ok(ctx) = LookOperationCtx::get_target(fs, parent, name) else {
        return Ok(None);
    };

    match ctx {
        TargetAttr::InsideRepo(ctx) => {
            let mut attr: FileAttr = dir_attr(ctx.ino_flag).into();
            attr.ino = ctx.attr.ino;
            Ok(Some(attr))
        }
        TargetAttr::InsideMonth(ctx) => {
            let mut attr: FileAttr = dir_attr(ctx.ino_flag).into();
            attr.ino = ctx.attr.ino;
            Ok(Some(attr))
        }
        TargetAttr::InsideSnap(ctx) => {
            let oid = fs.get_oid_from_db(ctx.attr.ino)?;
            let obj_attr = {
                let repo = fs.get_repo(parent.to_norm_u64())?;
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.find_in_commit(ctx.parent_commit(), oid)?
            };
            let attr = fs.object_to_file_attr(ctx.attr.ino, &obj_attr, ctx.ino_flag)?;
            Ok(Some(attr))
        }
        TargetAttr::InsideBuild(ctx) => {
            let mut attr = fs.attr_from_path(ctx.ino_flag, ctx.path())?;
            attr.ino = ctx.attr.ino;
            Ok(Some(attr))
        }
    }
}

#[instrument(level = "debug", skip(fs), fields(parent = %parent), err(Display))]
pub fn lookup_vdir(fs: &GitFs, parent: VirtualIno, name: &str) -> anyhow::Result<Option<FileAttr>> {
    let repo = fs.get_repo(u64::from(parent))?;
    let Ok(repo) = repo.lock() else {
        return Ok(None);
    };
    let Some(v_node) = repo.vdir_cache.get(&parent) else {
        return Ok(None);
    };
    let Some((entry_ino, object)) = v_node.log.get(name) else {
        return Ok(None);
    };
    let attr = fs.object_to_file_attr(*entry_ino, object, InoFlag::InsideSnap)?;
    Ok(Some(attr))
}
