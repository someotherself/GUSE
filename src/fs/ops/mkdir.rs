use anyhow::bail;

use crate::fs::builds::BuildOperationCtx;
use crate::fs::fileattr::{FileAttr, StorageNode};
use crate::fs::{CreateFileAttr, GitFs, REPO_SHIFT, repo};
use crate::inodes::NormalIno;
use crate::mount::InvalMsg;

pub fn mkdir_root(
    fs: &GitFs,
    _parent: u64,
    name: &str,
    _create_attr: CreateFileAttr,
) -> anyhow::Result<FileAttr> {
    match repo::parse_mkdir_url(name)? {
        Some((url, repo_name)) => {
            println!("fetching repo {}", &repo_name);
            let repo_id = fs.new_repo(&repo_name, Some(&url))?.repo_id;
            let attr = fs.getattr((repo_id as u64) << REPO_SHIFT)?;
            Ok(attr)
        }
        None => {
            println!("Creating repo {name}");
            let repo_id = fs.new_repo(name, None)?.repo_id;
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
    let new_ino = fs.next_inode_checked(parent)?;

    let mut attr: FileAttr = create_attr.into();

    attr.ino = new_ino;

    let nodes = vec![StorageNode {
        parent_ino: parent,
        name: name.into(),
        attr,
    }];
    fs.write_inodes_to_db(nodes)?;
    let _ = fs.notifier.try_send(InvalMsg::Store {
        ino: new_ino,
        off: 0,
        data: Vec::new(),
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
    let ctx = BuildOperationCtx::new(fs, parent)?;
    let dir_path = ctx.path().join(name);

    std::fs::create_dir(&dir_path)?;
    let new_ino = fs.next_inode_checked(parent.to_norm_u64())?;

    let mut attr: FileAttr = create_attr.into();

    attr.ino = new_ino;
    let commit_oid = fs.get_oid_from_db(parent.into())?;
    attr.oid = commit_oid;

    let nodes = vec![StorageNode {
        parent_ino: parent.to_norm_u64(),
        name: name.into(),
        attr,
    }];
    fs.write_inodes_to_db(nodes)?;

    let _ = fs.notifier.try_send(InvalMsg::Store {
        ino: new_ino,
        off: 0,
        data: Vec::new(),
    });
    let _ = fs.notifier.try_send(InvalMsg::Inode {
        ino: parent.to_norm_u64(),
        off: 0,
        len: 0,
    });

    Ok(attr)
}
