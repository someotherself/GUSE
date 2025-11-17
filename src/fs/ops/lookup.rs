use std::ffi::{OsStr, OsString};

use crate::{
    fs::{
        self, CHASE_FOLDER, FileAttr, GitFs, LIVE_FOLDER,
        fileattr::{InoFlag, dir_attr},
    },
    inodes::{NormalIno, VirtualIno},
};

pub fn lookup_root(fs: &GitFs, name: &OsStr) -> anyhow::Result<Option<FileAttr>> {
    // Handle a look-up for url -> github.tokio-rs.tokio.git
    let attr = fs.repos_list.iter().find_map(|repo| {
        let (repo_name, repo_id) = (OsString::from(repo.repo_dir.clone()), repo.repo_id);
        if repo_name == name {
            let perms = 0o775;
            let st_mode = libc::S_IFDIR | perms;
            let repo_ino = GitFs::repo_id_to_ino(repo_id);
            let mut attr: FileAttr = dir_attr(InoFlag::RepoRoot).into();
            attr.ino = repo_ino;
            attr.git_mode = st_mode;
            Some(attr)
        } else {
            None
        }
    });
    Ok(attr)
}

pub fn lookup_repo(
    fs: &GitFs,
    parent: NormalIno,
    name: &OsStr,
) -> anyhow::Result<Option<FileAttr>> {
    let repo_id = GitFs::ino_to_repo_id(parent.into());
    let repo = match fs.repos_list.get(&repo_id) {
        Some(repo) => repo,
        None => return Ok(None),
    };
    let attr = if name == LIVE_FOLDER {
        let Ok(live_ino) = fs.get_ino_from_db(parent.into(), OsStr::new(LIVE_FOLDER)) else {
            return Ok(None);
        };
        let path = fs.repos_dir.join(&repo.repo_dir).join(LIVE_FOLDER);
        let mut attr = GitFs::attr_from_path(InoFlag::LiveRoot, &path)?;
        attr.ino = live_ino;
        attr
    } else if name == CHASE_FOLDER {
        let Ok(chase_ino) = fs.get_ino_from_db(parent.into(), OsStr::new(CHASE_FOLDER)) else {
            return Ok(None);
        };
        let path = fs.repos_dir.join(&repo.repo_dir).join(CHASE_FOLDER);
        let mut attr = GitFs::attr_from_path(InoFlag::ChaseRoot, &path)?;
        attr.ino = chase_ino;
        attr
    } else {
        // It will always be a yyyy-mm folder
        // Build blank attr for it
        let Ok(child_ino) = fs.get_ino_from_db(parent.into(), name) else {
            return Ok(None);
        };
        let mut attr: FileAttr = dir_attr(InoFlag::MonthFolder).into();
        attr.ino = child_ino;
        attr
    };
    Ok(Some(attr))
}

pub fn lookup_live(
    fs: &GitFs,
    parent: NormalIno,
    name: &OsStr,
) -> anyhow::Result<Option<FileAttr>> {
    match fs.get_metadata_by_name(parent, name) {
        Ok(value) => Ok(Some(value)),
        Err(_) => {
            fs::ops::readdir::readdir_live_dir(fs, parent)?;
            match fs.get_metadata_by_name(parent, name) {
                Ok(attr) => Ok(Some(attr)),
                Err(_) => Ok(None),
            }
        }
    }
}

pub fn lookup_git(fs: &GitFs, parent: NormalIno, name: &OsStr) -> anyhow::Result<Option<FileAttr>> {
    match fs.get_metadata_by_name(parent, name) {
        Ok(value) => Ok(Some(value)),
        Err(_) => {
            let p_flag = fs.get_ino_flag_from_db(parent)?;
            if p_flag == InoFlag::InsideSnap
                || p_flag == InoFlag::InsideDotGit
                || p_flag == InoFlag::HeadFile
            {
                fs::ops::readdir::readdir_git_dir(fs, parent)?;
                match fs.get_metadata_by_name(parent, name) {
                    Ok(value) => Ok(Some(value)),
                    Err(_) => Ok(None),
                }
            } else {
                Ok(None)
            }
        }
    }
}

pub fn lookup_vdir(
    fs: &GitFs,
    parent: VirtualIno,
    name: &OsStr,
) -> anyhow::Result<Option<FileAttr>> {
    let repo = fs.get_repo(u64::from(parent))?;
    let v_node_opt = repo.with_state(|s| s.vdir_cache.get(&parent).cloned());
    let Some(v_node) = v_node_opt else {
        return Ok(None);
    };
    let Some((entry_ino, object)) = v_node.log.get(name) else {
        return Ok(None);
    };
    let mut attr = fs.object_to_file_attr(*entry_ino, object, InoFlag::InsideSnap)?;
    attr.parent_ino = parent.to_norm_u64();
    Ok(Some(attr))
}
