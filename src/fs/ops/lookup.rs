use crate::fs::{FileAttr, GitFs, REPO_SHIFT, build_attr_dir};

pub fn lookup_root(fs: &GitFs, name: &str) -> anyhow::Result<Option<FileAttr>> {
    // Handle a look-up for url -> github.tokio-rs.tokio.git
    let attr = fs.repos_list.values().find_map(|repo| {
        if repo.repo_dir == name {
            let perms = 0o775;
            let st_mode = libc::S_IFDIR | perms;
            let repo_ino = (repo.repo_id as u64) << REPO_SHIFT;
            Some(build_attr_dir(repo_ino, st_mode))
        } else {
            None
        }
    });
    Ok(attr)
}

pub fn lookup_repo(fs: &GitFs, parent: u64, name: &str) -> anyhow::Result<Option<FileAttr>> {
    // DOUBLE CHECK. Move code from GitDir.
    let repo_id = GitFs::ino_to_repo_id(parent);
    let repo = match fs.repos_list.get(&repo_id) {
        Some(repo) => repo,
        None => return Ok(None),
    };
    let attr = if name == "live" {
        let live_ino = fs.get_ino_from_db(parent, "live")?;
        let path = fs.repos_dir.join(&repo.repo_dir);
        let mut attr = fs.attr_from_dir(path)?;
        attr.inode = live_ino;
        attr
    } else {
        let child_ino = repo
            .connection
            .read()
            .unwrap()
            .get_ino_from_db(parent, name)?;
        let oid = fs.get_oid_from_db(child_ino)?;
        match repo.attr_from_snap(oid, name) {
            Ok(git_attr) => {
                let mut attr = fs.object_to_file_attr(child_ino, &git_attr)?;
                attr.inode = child_ino;
                attr
            }
            Err(_) => return Ok(None),
        }
    };
    Ok(Some(attr))
}

pub fn lookup_live(fs: &GitFs, parent: u64, name: &str) -> anyhow::Result<Option<FileAttr>> {
    // TODO Handle case if target it live itself
    let repo_id = GitFs::ino_to_repo_id(parent);
    match fs.repos_list.get(&repo_id) {
        Some(_) => {}
        None => return Ok(None),
    };
    let path = fs.build_full_path(parent)?.join(name);
    let mut attr = fs.attr_from_dir(path)?;
    let child_ino = fs.get_ino_from_db(parent, name)?;
    attr.inode = child_ino;

    Ok(Some(attr))
}

pub fn lookup_git(fs: &GitFs, parent: u64, name: &str) -> anyhow::Result<Option<FileAttr>> {
    let repo = fs.get_repo(parent)?;
    let (commit_oid, _) = fs.find_commit_in_gitdir(parent)?;
    let oid = fs.get_oid_from_db(parent)?;
    let parent_tree_oid = if oid == commit_oid {
        let commit = repo.inner.find_commit(commit_oid)?;
        commit.tree_id()
    } else {
        // else, get parent oid from db
        fs.get_oid_from_db(parent)?
    };
    let object_attr = repo.find_by_name(parent_tree_oid, name)?;
    let child_ino = fs.get_ino_from_db(parent, name)?;
    let mut attr = fs.object_to_file_attr(child_ino, &object_attr)?;
    attr.inode = child_ino;
    Ok(Some(attr))
}
