use std::{
    ffi::{OsStr, OsString},
    sync::Arc,
};

use anyhow::bail;
use libc::EPERM;

use crate::{
    fs::{
        GitFs, SourceTypes,
        builds::BuildOperationCtx,
        fileattr::{FileAttr, InoFlag, StorageNode, file_attr},
    },
    inodes::NormalIno,
    mount::InvalMsg,
};

pub fn create_live(
    fs: &GitFs,
    parent: u64,
    name: &OsStr,
    write: bool,
) -> anyhow::Result<(FileAttr, u64)> {
    let ino = fs.next_inode_checked(parent)?;
    let mut attr: FileAttr = file_attr(InoFlag::InsideLive).into();
    attr.ino = ino;
    let file_path = fs.get_live_path(parent.into())?.join(name);
    std::fs::File::create_new(&file_path)?;

    let nodes = vec![StorageNode {
        parent_ino: parent,
        name: name.into(),
        attr,
    }];
    fs.write_inodes_to_db(nodes)?;
    {
        let _ = fs.notifier.try_send(InvalMsg::Store {
            ino,
            off: 0,
            data: Vec::new(),
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
    name: &OsStr,
    write: bool,
) -> anyhow::Result<(FileAttr, u64)> {
    let Some(ctx) = BuildOperationCtx::new(fs, parent)? else {
        bail!(std::io::Error::from_raw_os_error(EPERM))
    };

    let file_path = ctx.path().join(name);
    let ino = fs.next_inode_checked(parent.to_norm_u64())?;
    let mut attr: FileAttr = file_attr(InoFlag::InsideBuild).into();
    attr.ino = ino;
    // Add the commit_oid to the attr
    let parent_oid = fs.get_oid_from_db(parent.into())?;
    attr.oid = parent_oid;

    let file = std::fs::File::create_new(&file_path)?;
    {
        let repo = fs.get_repo(parent.into())?;
        let real_file = SourceTypes::RealFile(Arc::new(file));
        repo.file_cache.insert(ino, real_file);
    }

    let nodes = vec![StorageNode {
        parent_ino: parent.to_norm_u64(),
        name: name.into(),
        attr,
    }];
    fs.write_inodes_to_db(nodes)?;
    let _ = fs.notifier.try_send(InvalMsg::Store {
        ino,
        off: 0,
        data: Vec::new(),
    });
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
