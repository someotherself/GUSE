use anyhow::anyhow;
use tracing::instrument;

use crate::{
    fs::{
        FileAttr, GitFs, REPO_SHIFT, build_attr_dir,
        fileattr::{InoFlag, dir_attr},
        meta_db::MetaDb,
    },
    inodes::{NormalIno, VirtualIno},
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
            Some(build_attr_dir(repo_ino, InoFlag::Root, st_mode))
        } else {
            None
        }
    });
    Ok(attr)
}

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
            let repo_id = GitFs::ino_to_repo_id(parent.into());
            let repo_db = fs
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("no db"))?;
            let conn = repo_db.ro_pool.get()?;
            MetaDb::get_ino_from_db(&conn, parent.into(), name)
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

pub fn lookup_live(fs: &GitFs, parent: NormalIno, name: &str) -> anyhow::Result<Option<FileAttr>> {
    let repo_id = GitFs::ino_to_repo_id(parent.to_norm_u64());
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
    let Ok(attr) = fs.get_metadata_by_name(parent, name) else {
        return Ok(None);
    };
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
    let attr = fs.object_to_file_attr(*entry_ino, object, InoFlag::InsideSnap)?;
    Ok(Some(attr))
}
