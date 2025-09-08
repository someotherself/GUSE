use anyhow::anyhow;
use git2::Oid;

use crate::{
    fs::{
        FileAttr, GitFs,
        fileattr::{dir_attr, file_attr},
    },
    inodes::NormalIno,
};

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
    let ino = u64::from(ino);
    let target_oid = fs.get_oid_from_db(ino)?;
    if target_oid == Oid::zero() {
        let mut attr: FileAttr = dir_attr().into();
        attr.ino = ino;
        // attr.perm = 0o555;
        Ok(attr)
    } else {
        let repo = fs.get_repo(ino)?;
        let (commit_id, snap_name) = fs.get_parent_commit(ino)?;
        let oid = fs.get_oid_from_db(ino)?;
        let gitdir_commit_id = {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.inner.find_commit(commit_id)?.id()
        };
        if oid == commit_id {
            // We are looking at a commit
            let commit_attr = {
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.attr_from_snap(gitdir_commit_id, &snap_name)?
            };
            let attr = fs.object_to_file_attr(ino, &commit_attr)?;
            Ok(attr)
        } else {
            let git_attr = {
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.find_in_commit(gitdir_commit_id, oid)?
            };
            let attr = fs.object_to_file_attr(ino, &git_attr)?;

            Ok(attr)
        }
    }
}
