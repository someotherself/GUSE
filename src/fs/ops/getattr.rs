use crate::fs::{FileAttr, GitFs};

pub fn getattr_live_dir(fs: &GitFs, ino: u64) -> anyhow::Result<FileAttr> {
    let path = fs.build_full_path(ino)?;
    let mut attr: FileAttr = fs.attr_from_dir(path)?;
    attr.inode = ino;
    Ok(attr)
}

pub fn getattr_git_dir(fs: &GitFs, ino: u64) -> anyhow::Result<FileAttr> {
    let repo = fs.get_repo(ino)?;
    let (commit_id, snap_name) = fs.find_commit_in_gitdir(ino)?;
    let oid = fs.get_oid_from_db(ino)?;
    let gitdir_commit = repo.inner.find_commit(commit_id)?;
    if oid == commit_id {
        // We are looking at a commit
        let commit_attr = repo.attr_from_snap(gitdir_commit.id(), &snap_name)?;
        let attr = fs.object_to_file_attr(ino, &commit_attr)?;
        Ok(attr)
    } else {
        let git_attr = repo.find_in_commit(gitdir_commit.id(), oid)?;
        let mut attr = fs.object_to_file_attr(ino, &git_attr)?;
        attr.inode = ino;

        Ok(attr)
    }
}
