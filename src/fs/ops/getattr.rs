use git2::Oid;

use crate::{
    fs::{FileAttr, FsError, FsResult, GitFs},
    mount::dir_attr,
};

pub fn getattr_live_dir(fs: &GitFs, ino: u64) -> FsResult<FileAttr> {
    let path = fs.build_full_path(ino)?;
    let mut attr: FileAttr = fs.attr_from_dir(path)?;
    attr.inode = ino;
    Ok(attr)
}

pub fn getattr_git_dir(fs: &GitFs, ino: u64) -> FsResult<FileAttr> {
    let target_oid = fs.get_oid_from_db(ino)?;
    if target_oid == Oid::zero() {
        let mut attr: FileAttr = dir_attr().into();
        attr.inode = ino;
        attr.perm = 0o555;
        Ok(attr)
    } else {
        let repo = fs.get_repo(ino)?;
        let (commit_id, snap_name) = fs.get_parent_commit(ino)?;
        let oid = fs.get_oid_from_db(ino)?;
        let gitdir_commit_id = {
            let repo = repo.lock().map_err(|_| FsError::LockPoisoned)?;
            repo.inner.find_commit(commit_id)?.id()
        };
        if oid == commit_id {
            // We are looking at a commit
            let commit_attr = {
                let repo = repo.lock().map_err(|_| FsError::LockPoisoned)?;
                repo.attr_from_snap(gitdir_commit_id, &snap_name)?
            };
            let attr = fs.object_to_file_attr(ino, &commit_attr)?;
            Ok(attr)
        } else {
            let git_attr = {
                let repo = repo.lock().map_err(|_| FsError::LockPoisoned)?;
                repo.find_in_commit(gitdir_commit_id, oid)?
            };
            let mut attr = fs.object_to_file_attr(ino, &git_attr)?;
            attr.inode = ino;

            Ok(attr)
        }
    }
}
