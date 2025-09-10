use std::{os::unix::fs::MetadataExt, path::PathBuf};

use git2::Oid;

use anyhow::anyhow;
use tracing::instrument;

use crate::{
    fs::{
        FileAttr, GitFs, REPO_SHIFT, build_attr_dir,
        fileattr::{dir_attr, file_attr},
        ops::readdir::{DirCase, classify_inode},
    },
    inodes::{NormalIno, VirtualIno},
};

struct LookupOperationCtx {
    ino: NormalIno,
    parent_tree: Oid,
    parent_commit: Oid,
    snap_name: String,
    build_root: PathBuf,
    path: PathBuf,
}

impl LookupOperationCtx {
    fn new(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Self> {
        let res = classify_inode(fs, ino.to_norm_u64())?;
        match res {
            DirCase::Month { year: _, month: _ } => Ok(Self {
                ino,
                parent_tree: Oid::zero(),
                parent_commit: Oid::zero(),
                snap_name: String::new(),
                build_root: PathBuf::new(),
                path: PathBuf::new(),
            }),
            DirCase::Commit { oid } => {
                if oid == Oid::zero() {
                    // We are in the build folder
                    let parent_oid = fs.parent_commit_build_session(ino)?;
                    let build_root = fs.get_path_to_build_folder(ino)?;

                    let build_session = {
                        let repo = fs.get_repo(ino.to_norm_u64())?;
                        let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                        repo.get_or_init_build_session(parent_oid, &build_root)?
                    };
                    // TODO: Does it need path file?
                    let path = build_session.finish_path(fs, ino)?;
                    return Ok(Self {
                        ino,
                        parent_tree: Oid::zero(),
                        parent_commit: Oid::zero(),
                        snap_name: String::new(),
                        build_root,
                        path,
                    });
                }

                let (parent_commit, _) = fs.get_parent_commit(ino.to_norm_u64())?;
                let parent_tree = if oid == parent_commit {
                    let repo = fs.get_repo(ino.to_norm_u64())?;
                    let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                    let commit = repo.inner.find_commit(parent_commit)?;
                    commit.tree_id()
                } else {
                    // else, get parent oid from db
                    fs.get_oid_from_db(ino.to_norm_u64())?
                };

                Ok(Self {
                    ino,
                    parent_tree,
                    parent_commit,
                    snap_name: String::new(),
                    build_root: PathBuf::new(),
                    path: PathBuf::new(),
                })
            }
        }
    }

    fn is_month(&self) -> bool {
        self.parent_commit == Oid::zero() && self.parent_tree == Oid::zero()
    }

    fn is_in_build(&self) -> bool {
        self.build_root != PathBuf::new() && self.path != PathBuf::new()
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
            Some(build_attr_dir(repo_ino, st_mode))
        } else {
            None
        }
    });
    Ok(attr)
}

#[instrument(level = "debug", skip(fs), fields(parent = %parent), err(Display))]
pub fn lookup_repo(fs: &GitFs, parent: u64, name: &str) -> anyhow::Result<Option<FileAttr>> {
    let repo_id = GitFs::ino_to_repo_id(parent);
    let repo = match fs.repos_list.get(&repo_id) {
        Some(repo) => repo,
        None => return Ok(None),
    };
    let attr = if name == "live" {
        let live_ino = fs.get_ino_from_db(parent, "live")?;
        let path = {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            fs.repos_dir.join(&repo.repo_dir)
        };
        let mut attr = fs.attr_from_path(path)?;
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
                .get_ino_from_db(parent, name)
        };
        let child_ino = match res {
            Ok(i) => i,
            Err(_) => return Ok(None),
        };
        let mut attr: FileAttr = dir_attr().into();
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
    let res = fs.get_ino_from_db(parent, name);
    let child_ino = match res {
        Ok(i) => i,
        Err(_) => return Ok(None),
    };
    let filemode = fs.get_mode_from_db(child_ino)?;
    let mut attr: FileAttr = match filemode {
        git2::FileMode::Tree => dir_attr().into(),
        git2::FileMode::Commit => dir_attr().into(),
        _ => file_attr().into(),
    };

    let path = fs.build_full_path(parent)?.join(name);
    let size = path.metadata()?.size();
    if !path.exists() {
        return Ok(None);
    }

    attr.ino = child_ino;
    attr.perm = 0o775;
    attr.size = size;

    Ok(Some(attr))
}

#[instrument(level = "debug", skip(fs), fields(parent = %parent), err(Display))]
pub fn lookup_git(fs: &GitFs, parent: NormalIno, name: &str) -> anyhow::Result<Option<FileAttr>> {
    let ctx = LookupOperationCtx::new(fs, parent)?;
    let child_ino = {
        let repo = fs.get_repo(parent.to_norm_u64())?;
        let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let Ok(child_ino) = repo
            .connection
            .lock()
            .map_err(|_| anyhow!("Lock poisoned"))?
            .get_ino_from_db(parent.to_norm_u64(), name)
        else {
            return Ok(None);
        };
        child_ino
    };
    if ctx.is_month() {
        let mut attr: FileAttr = dir_attr().into();
        attr.ino = child_ino;
        return Ok(Some(attr));
    }

    if ctx.is_in_build() {
        let path = ctx.path.join(name);
        let mut attr = fs.attr_from_path(path)?;
        attr.ino = child_ino;
        return Ok(Some(attr));
    }

    let object_attr_res = {
        let repo = fs.get_repo(parent.to_norm_u64())?;
        let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        repo.find_by_name(ctx.parent_tree, name)
    };
    let mut attr = match object_attr_res {
        // Target is in the git tree
        Ok(obj_attr) => fs.object_to_file_attr(child_ino, &obj_attr)?,
        // Target may be in the build folder
        Err(_) => {
            let filemode = fs.get_mode_from_db(child_ino)?;
            match filemode {
                git2::FileMode::Tree => dir_attr().into(),
                git2::FileMode::Commit => dir_attr().into(),
                _ => file_attr().into(),
            }
        }
    };
    attr.ino = child_ino;
    Ok(Some(attr))
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
    let attr = fs.object_to_file_attr(*entry_ino, object)?;
    Ok(Some(attr))
}
