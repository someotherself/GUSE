use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;

use anyhow::{anyhow, bail};
use libc::EPERM;

use crate::fs::builds::BuildOperationCtx;
use crate::fs::fileattr::{FileAttr, StorageNode};
use crate::fs::{CreateFileAttr, GitFs, REPO_SHIFT, repo};
use crate::inodes::NormalIno;
use crate::mount::InvalMsg;

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
    let dir_path = fs.get_live_path(parent.into())?.join(name);
    std::fs::create_dir(&dir_path)?;
    std::fs::set_permissions(dir_path, std::fs::Permissions::from_mode(0o775))?;
    let new_ino = fs.next_inode_checked(parent)?;

    let mut attr: FileAttr = create_attr.into();

    attr.ino = new_ino;

    let nodes = vec![StorageNode {
        parent_ino: parent,
        name: name.into(),
        attr: attr.into(),
    }];
    fs.write_inodes_to_db(nodes)?;

    let _ = fs.notifier.try_send(InvalMsg::Entry {
        parent,
        name: OsString::from(name),
    });
    let _ = fs.notifier.try_send(InvalMsg::Inode {
        ino: parent,
        off: 0,
        len: 0,
    });

    Ok(attr)
}

pub fn mkdir_git(
    fs: &GitFs,
    parent: NormalIno,
    name: &str,
    create_attr: CreateFileAttr,
) -> anyhow::Result<FileAttr> {
    let Some(ctx) = BuildOperationCtx::new(fs, parent)? else {
        bail!(std::io::Error::from_raw_os_error(EPERM))
    };
    let dir_path = ctx.path().join(name);

    std::fs::create_dir(&dir_path)?;
    std::fs::set_permissions(dir_path, std::fs::Permissions::from_mode(0o775))?;
    let new_ino = fs.next_inode_checked(parent.to_norm_u64())?;

    let mut attr: FileAttr = create_attr.into();

    attr.ino = new_ino;
    attr.oid = ctx.commit_oid();

    let nodes = vec![StorageNode {
        parent_ino: parent.to_norm_u64(),
        name: name.into(),
        attr: attr.into(),
    }];
    fs.write_inodes_to_db(nodes)?;

    let _ = fs.notifier.try_send(InvalMsg::Entry {
        parent: parent.to_norm_u64(),
        name: OsString::from(name),
    });
    let _ = fs.notifier.try_send(InvalMsg::Inode {
        ino: parent.to_norm_u64(),
        off: 0,
        len: 0,
    });

    Ok(attr)
}
