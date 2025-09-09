use std::os::unix::fs::PermissionsExt;

use anyhow::{anyhow, bail};
use git2::Oid;
use libc::EPERM;

use crate::fs::fileattr::FileAttr;
use crate::fs::ops::readdir::{DirCase, classify_inode};
use crate::fs::{CreateFileAttr, GitFs, REPO_SHIFT, repo};
use crate::inodes::NormalIno;

pub fn mkdir_root(
    fs: &mut GitFs,
    _parent: u64,
    name: &str,
    _create_attr: CreateFileAttr,
) -> anyhow::Result<FileAttr> {
    match repo::parse_mkdir_url(name)? {
        Some((url, repo_name)) => {
            println!("fetching repo {}", &repo_name);
            let repo = fs.new_repo(&repo_name)?;
            {
                let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.fetch_anon(&url)?;
                repo.refresh_snapshots()?;
            }
            let repo_id = {
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.repo_id
            };
            let attr = fs.getattr((repo_id as u64) << REPO_SHIFT)?;
            Ok(attr)
        }
        None => {
            println!("Creating repo {name}");
            let repo_id = {
                let repo = fs.new_repo(name)?;
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.repo_id
            };
            let attr = fs.getattr((repo_id as u64) << REPO_SHIFT)?;

            Ok(attr)
        }
    }
}

pub fn mkdir_repo(
    _fs: &GitFs,
    _parent: u64,
    _name: &str,
    _create_attr: CreateFileAttr,
) -> anyhow::Result<FileAttr> {
    bail!("This directory is read only")
}

pub fn mkdir_live(
    fs: &GitFs,
    parent: u64,
    name: &str,
    create_attr: CreateFileAttr,
) -> anyhow::Result<FileAttr> {
    let dir_path = fs.get_path_by_name_in_live(parent, name)?;
    std::fs::create_dir(&dir_path)?;
    std::fs::set_permissions(dir_path, std::fs::Permissions::from_mode(0o775))?;
    let new_ino = fs.next_inode_checked(parent)?;

    let mut attr: FileAttr = create_attr.into();

    attr.ino = new_ino;

    let nodes = vec![(parent, name.into(), attr)];
    fs.write_inodes_to_db(nodes)?;

    Ok(attr)
}

pub fn mkdir_git(
    fs: &GitFs,
    parent: NormalIno,
    name: &str,
    create_attr: CreateFileAttr,
) -> anyhow::Result<FileAttr> {
    match classify_inode(fs, parent.to_norm_u64())? {
        DirCase::Month { year: _, month: _ } => {
            bail!(std::io::Error::from_raw_os_error(EPERM))
        }
        DirCase::Commit { oid } => {
            if oid == Oid::zero() {
                let temp_dir = {
                    let build_folder = fs.get_path_to_build_folder(parent)?;
                    let repo = fs.get_repo(parent.to_norm_u64())?;
                    let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                    let oid = fs.parent_commit_build_session(parent)?;
                    repo.get_build_state(oid, &build_folder)?
                };
                let dir_path = fs.full_path_build_folder(parent, &temp_dir)?;

                std::fs::create_dir(&dir_path)?;
                std::fs::set_permissions(dir_path, std::fs::Permissions::from_mode(0o775))?;
                let new_ino = fs.next_inode_checked(parent.to_norm_u64())?;

                let mut attr: FileAttr = create_attr.into();

                attr.ino = new_ino;

                let nodes = vec![(parent.to_norm_u64(), name.into(), attr)];
                fs.write_inodes_to_db(nodes)?;

                return Ok(attr);
            }
            let res = {
                let repo = fs.get_repo(parent.to_norm_u64())?;
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.inner.find_commit(oid).is_ok()
            };
            if res {
                let build_folder = fs.get_path_to_build_folder(parent)?;
                let temp_dir = {
                    let repo = fs.get_repo(parent.to_norm_u64())?;
                    let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                    repo.get_build_state(oid, &build_folder)?
                };
                let dir_path = build_folder.join(temp_dir).join(name);

                std::fs::create_dir(&dir_path)?;
                std::fs::set_permissions(dir_path, std::fs::Permissions::from_mode(0o775))?;
                let new_ino = fs.next_inode_checked(parent.to_norm_u64())?;

                let mut attr: FileAttr = create_attr.into();

                attr.ino = new_ino;

                let nodes = vec![(parent.to_norm_u64(), name.into(), attr)];
                fs.write_inodes_to_db(nodes)?;

                Ok(attr)
            } else {
                bail!(std::io::Error::from_raw_os_error(EPERM))
            }
        }
    }
}
