use std::os::unix::fs::MetadataExt;

use git2::Oid;

use anyhow::anyhow;

use crate::{
    fs::{FileAttr, GitFs, REPO_SHIFT, build_attr_dir},
    mount::{dir_attr, file_attr},
};

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
        let mut attr = fs.attr_from_dir(path)?;
        attr.inode = live_ino;
        attr
    } else {
        // It will always be a yyyy-mm folder
        // Build blank attr for it
        let res = {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.connection
                .lock()
                .unwrap()
                .get_ino_from_db(parent, name)
        };
        let child_ino = match res {
            Ok(i) => i,
            Err(_) => return Ok(None),
        };
        let mut attr: FileAttr = dir_attr().into();
        attr.inode = child_ino;
        attr.perm = 0o555;
        attr
    };
    Ok(Some(attr))
}

pub fn lookup_live(fs: &GitFs, parent: u64, name: &str) -> anyhow::Result<Option<FileAttr>> {
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

    attr.inode = child_ino;
    attr.perm = 0o775;
    attr.size = size;

    Ok(Some(attr))
}

pub fn lookup_git(fs: &GitFs, parent: u64, name: &str) -> anyhow::Result<Option<FileAttr>> {
    // If oid == zero, folder is yyyy-mm-dd. Build black
    // else oid is commit_id or tree_id
    let repo = fs.get_repo(parent)?;
    let child_ino = {
        let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        repo.connection
            .lock()
            .unwrap()
            .get_ino_from_db(parent, name)?
    };

    let oid = fs.get_oid_from_db(parent)?;
    let mut attr = if oid == Oid::zero() {
        let attr: FileAttr = dir_attr().into();
        attr
    } else {
        let (commit_oid, _) = fs.get_parent_commit(parent)?;
        let oid = fs.get_oid_from_db(parent)?;
        let parent_tree_oid = if oid == commit_oid {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            let commit = repo.inner.find_commit(commit_oid)?;
            commit.tree_id()
        } else {
            // else, get parent oid from db
            fs.get_oid_from_db(parent)?
        };
        let object_attr = {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.find_by_name(parent_tree_oid, name)?
        };
        fs.object_to_file_attr(child_ino, &object_attr)?
    };
    attr.inode = child_ino;
    attr.perm = 0o555;
    Ok(Some(attr))
}
