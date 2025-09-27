use std::{ffi::OsString, os::unix::fs::PermissionsExt};

use anyhow::bail;
use libc::EPERM;

use crate::{
    fs::{
        GitFs,
        builds::BuildOperationCtx,
        fileattr::{FileAttr, InoFlag, StorageNode, file_attr},
    },
    inodes::NormalIno,
    mount::InvalMsg,
};

pub fn create_live(
    fs: &GitFs,
    parent: u64,
    name: &str,
    write: bool,
) -> anyhow::Result<(FileAttr, u64)> {
    let ino = fs.next_inode_checked(parent)?;
    let mut attr: FileAttr = file_attr(InoFlag::InsideLive).into();
    attr.ino = ino;
    let file_path = fs.get_live_path(parent.into())?.join(name);

    let file = std::fs::File::create_new(&file_path)?;
    std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o775))?;
    drop(file);

    let nodes = vec![StorageNode {
        parent_ino: parent,
        name: name.into(),
        attr: attr.into(),
    }];
    fs.write_inodes_to_db(nodes)?;
    {
        let _ = fs.notifier.try_send(InvalMsg::Entry {
            parent,
            name: OsString::from(name),
        });
        let _ = fs.notifier.try_send(InvalMsg::Inode {
            ino: parent,
            off: 0,
            len: 0,
        });
    }
    let fh = fs.open(ino, true, write, false)?;
    Ok((attr, fh))
}

pub fn create_git(
    fs: &GitFs,
    parent: NormalIno,
    name: &str,
    write: bool,
) -> anyhow::Result<(FileAttr, u64)> {
    let Some(ctx) = BuildOperationCtx::new(fs, parent)? else {
        bail!(std::io::Error::from_raw_os_error(EPERM))
    };

    let file_path = ctx.path().join(name);
    let ino = fs.next_inode_checked(parent.to_norm_u64())?;
    let mut attr: FileAttr = file_attr(InoFlag::InsideBuild).into();
    attr.ino = ino;

    let file = std::fs::File::create_new(&file_path)?;
    std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o775))?;
    drop(file);

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

    let fh = fs.open(ino, true, write, false)?;
    Ok((attr, fh))
}
