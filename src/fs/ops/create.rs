use std::{ffi::OsStr, sync::Arc};

use git2::Oid;
use uuid::Uuid;

use crate::{
    fs::{
        GitFs, SourceTypes,
        fileattr::{FileAttr, InoFlag, file_attr},
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
    let attr = FileAttr::new(
        file_attr(InoFlag::InsideLive),
        ino,
        name,
        parent,
        Oid::zero(),
        None,
    );
    let file_path = fs.get_live_path(parent.into())?.join(name);
    std::fs::File::create_new(&file_path)?;

    let nodes = vec![attr.clone()];
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
    let oid = fs.get_oid_from_db(parent.into())?;

    let repo = fs.get_repo(parent.into())?;
    let ctx = repo.get_or_init_build_session(oid, &repo.build_dir)?;

    let uuid = Uuid::new_v4().to_string();
    let file_path = ctx.folder.path().join(&uuid);

    let ino = fs.next_inode_checked(parent.to_norm_u64())?;
    let parent_oid = fs.get_oid_from_db(parent.into())?;
    let attr = FileAttr::new(
        file_attr(InoFlag::InsideBuild),
        ino,
        name,
        parent.to_norm_u64(),
        parent_oid,
        Some(uuid),
    );

    let file = std::fs::File::create_new(&file_path)?;
    {
        let repo = fs.get_repo(parent.into())?;
        let real_file = SourceTypes::RealFile(Arc::new(file));
        repo.file_cache.insert(ino, real_file);
    }

    let nodes = vec![attr.clone()];
    fs.write_inodes_to_db(nodes)?;
    let _ = fs.notifier.try_send(InvalMsg::Store {
        ino,
        off: 0,
        data: Vec::new(),
    });
    let _ = fs.notifier.try_send(InvalMsg::Inode {
        ino: parent.to_norm_u64(),
        off: 0,
        len: 0,
    });

    let fh = fs.open(ino, true, write, false)?;
    Ok((attr, fh))
}
