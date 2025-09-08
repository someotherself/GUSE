use std::{fs::File, os::unix::fs::PermissionsExt};

use anyhow::{anyhow, bail};
use git2::Oid;
use libc::EPERM;

use crate::{
    fs::{
        GitFs,
        fileattr::{FileAttr, file_attr},
        ops::readdir::{DirCase, classify_inode},
    },
    inodes::NormalIno,
};

pub fn create_live(
    fs: &GitFs,
    parent: u64,
    name: &str,
    read: bool,
    write: bool,
) -> anyhow::Result<(FileAttr, u64)> {
    if !read && !write {
        bail!("read and write cannot be false at the same time")
    };
    let ino = fs.next_inode_checked(parent)?;
    let mut attr: FileAttr = file_attr().into();
    attr.ino = ino;
    let file_path = fs.build_path(parent, name)?;

    let file = std::fs::File::create_new(&file_path)?;
    std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o775))?;
    file.sync_all()?;
    File::open(file_path.parent().ok_or_else(|| anyhow!("No parent"))?)?.sync_all()?;

    let nodes = vec![(parent, name.into(), attr)];
    fs.write_inodes_to_db(nodes)?;

    let fh = fs.open(ino, read, write, false)?;
    Ok((attr, fh))
}

pub fn create_git(
    fs: &GitFs,
    parent: NormalIno,
    name: &str,
    read: bool,
    write: bool,
) -> anyhow::Result<(FileAttr, u64)> {
    if !read && !write {
        bail!("read and write cannot be false at the same time")
    };
    match classify_inode(fs, parent.to_norm_u64())? {
        DirCase::Month { year: _, month: _ } => {
            bail!(std::io::Error::from_raw_os_error(EPERM))
        }
        DirCase::Commit { oid } => {
            if oid == Oid::zero() {
                let ino = fs.next_inode_checked(parent.to_norm_u64())?;
                let mut attr: FileAttr = file_attr().into();
                attr.ino = ino;

                let temp_dir = {
                    let repo = fs.get_repo(parent.to_norm_u64())?;
                    let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                    let oid = fs.parent_commit_build_session(parent)?;
                    repo.get_build_state(oid)?
                };

                let file_path = fs.full_path_build_folder(parent, &temp_dir)?.join(name);

                let file = std::fs::File::create_new(&file_path)?;
                std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o775))?;
                file.sync_all()?;
                File::open(file_path.parent().ok_or_else(|| anyhow!("No parent"))?)?.sync_all()?;

                let nodes = vec![(parent.to_norm_u64(), name.into(), attr)];
                fs.write_inodes_to_db(nodes)?;

                let fh = fs.open(ino, read, write, false)?;
                return Ok((attr, fh));
            }
            let res = {
                let repo = fs.get_repo(parent.to_norm_u64())?;
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.inner.find_commit(oid).is_ok()
            };
            if res {
                let ino = fs.next_inode_checked(parent.to_norm_u64())?;
                let mut attr: FileAttr = file_attr().into();
                attr.ino = ino;
                let temp_dir = {
                    let repo = fs.get_repo(parent.to_norm_u64())?;
                    let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                    repo.get_build_state(oid)?
                };

                let file_path = fs.path_to_build_folder(parent, &temp_dir)?.join(name);

                let file = std::fs::File::create_new(&file_path)?;
                std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o775))?;
                file.sync_all()?;
                File::open(file_path.parent().ok_or_else(|| anyhow!("No parent"))?)?.sync_all()?;

                let nodes = vec![(parent.to_norm_u64(), name.into(), attr)];
                fs.write_inodes_to_db(nodes)?;

                let fh = fs.open(ino, read, write, false)?;
                return Ok((attr, fh));
            } else {
                bail!(std::io::Error::from_raw_os_error(EPERM))
            }
        }
    };
}
