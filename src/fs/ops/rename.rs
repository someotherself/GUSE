use std::ffi::OsString;

use anyhow::{anyhow, bail};

use crate::{
    fs::{
        FileAttr, GitFs,
        fileattr::{FileType, InoFlag, StorageNode, dir_attr, file_attr},
    },
    inodes::NormalIno,
    mount::InvalMsg,
};

pub fn rename_live(
    fs: &GitFs,
    parent: NormalIno,
    name: &str,
    new_parent: NormalIno,
    new_name: &str,
) -> anyhow::Result<()> {
    let dest_in_live = fs.is_in_live(new_parent)?;
    let dest_in_build = fs.is_in_build(new_parent)?;
    if !dest_in_live && !dest_in_build {
        bail!(format!("New parent {} not allowed", new_parent));
    }

    let src_attr = fs
        .lookup(parent.to_norm_u64(), name)?
        .ok_or_else(|| anyhow!("Source does not exist"))?;

    let mut dest_exists = false;

    if let Some(dest_attr) = fs.lookup(new_parent.to_norm_u64(), new_name)? {
        dest_exists = true;

        if dest_attr.kind == FileType::Directory && fs.readdir(dest_attr.ino)?.is_empty() {
            bail!("Directory is not empty")
        }
        if dest_attr.kind != src_attr.kind {
            bail!("Source and destination are not the same type")
        }
    }

    let src = fs.build_full_path(parent)?.join(name);
    let dest = fs.build_full_path(new_parent)?.join(new_name);

    std::fs::rename(src, &dest)?;

    fs.remove_db_record(parent, name)?;
    if dest_exists {
        fs.remove_db_record(new_parent, new_name)?;
    }

    {
        let _ = fs.notifier.try_send(InvalMsg::Entry {
            parent: parent.to_norm_u64(),
            name: OsString::from(name),
        });
        let _ = fs.notifier.try_send(InvalMsg::Entry {
            parent: new_parent.to_norm_u64(),
            name: OsString::from(new_name),
        });

        let _ = fs.notifier.try_send(InvalMsg::Inode {
            ino: parent.to_norm_u64(),
            off: 0,
            len: 0,
        });
        if new_parent.to_norm_u64() != parent.to_norm_u64() {
            let _ = fs.notifier.try_send(InvalMsg::Inode {
                ino: new_parent.to_norm_u64(),
                off: 0,
                len: 0,
            });
        }
    }

    let ino_flag = if dest_in_live {
        InoFlag::InsideLive
    } else if dest_in_build {
        InoFlag::InsideBuild
    } else {
        bail!("Invalid location")
    };

    let mut new_attr: FileAttr = match src_attr.kind {
        FileType::Directory => dir_attr(ino_flag).into(),
        _ => file_attr(ino_flag).into(),
    };
    new_attr.ino = src_attr.ino;

    let nodes = vec![StorageNode {
        parent_ino: new_parent.to_norm_u64(),
        name: new_name.into(),
        attr: new_attr.into(),
    }];
    fs.write_inodes_to_db(nodes)?;
    Ok(())
}

pub fn rename_git_build(
    fs: &GitFs,
    parent: NormalIno,
    name: &str,
    new_parent: NormalIno,
    new_name: &str,
) -> anyhow::Result<()> {
    let dest_in_build = fs.is_in_build(new_parent)?;
    let oid = fs.get_oid_from_db(new_parent.into())?;
    let is_commit_folder = fs.is_commit(new_parent, oid)?;
    if !dest_in_build && !is_commit_folder {
        bail!(format!("New parent {} not allowed", new_parent));
    }
    let src_attr = fs.get_metadata_by_name(parent, name)?;

    let mut dest_exists = false;

    if let Ok(dest_attr) = fs.get_metadata_by_name(new_parent, new_name) {
        dest_exists = true;

        if dest_attr.kind == FileType::Directory {
            let children = fs.count_children(dest_attr.ino.into())?;
            if children > 0 {
                bail!(std::io::Error::from_raw_os_error(libc::ENOTEMPTY));
            }
        }
        if dest_attr.kind != src_attr.kind {
            bail!("Source and destination are not the same type")
        }
    }

    let src = {
        let ino = parent;
        let parent_oid = fs.parent_commit_build_session(ino)?;
        let build_root = fs.get_path_to_build_folder(ino)?;
        let repo = fs.get_repo(ino.to_norm_u64())?;
        let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let session = repo.get_or_init_build_session(parent_oid, &build_root)?;
        drop(repo);
        session.finish_path(fs, ino)?.join(name)
    };

    let dest = {
        let ino = new_parent;
        let parent_oid = fs.parent_commit_build_session(ino)?;
        let build_root = fs.get_path_to_build_folder(ino)?;
        let repo = fs.get_repo(ino.to_norm_u64())?;
        let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let session = repo.get_or_init_build_session(parent_oid, &build_root)?;
        drop(repo);
        session.finish_path(fs, ino)?.join(new_name)
    };

    std::fs::rename(src, &dest)?;

    if dest_exists {
        fs.remove_db_record(new_parent, new_name)?;
    }

    let ino_flag = if dest_in_build {
        InoFlag::InsideBuild
    } else {
        bail!("Invalid location")
    };

    let mut new_attr = fs.attr_from_path(ino_flag, dest)?;
    new_attr.ino = src_attr.ino;

    let node = StorageNode {
        parent_ino: new_parent.to_norm_u64(),
        name: new_name.into(),
        attr: new_attr.into(),
    };
    fs.update_db_record(node)?;

    {
        let _ = fs.notifier.try_send(InvalMsg::Entry {
            parent: parent.to_norm_u64(),
            name: OsString::from(name),
        });
        let _ = fs.notifier.try_send(InvalMsg::Entry {
            parent: new_parent.to_norm_u64(),
            name: OsString::from(new_name),
        });

        let _ = fs.notifier.try_send(InvalMsg::Inode {
            ino: parent.to_norm_u64(),
            off: 0,
            len: 0,
        });
        if new_parent.to_norm_u64() != parent.to_norm_u64() {
            let _ = fs.notifier.try_send(InvalMsg::Inode {
                ino: new_parent.to_norm_u64(),
                off: 0,
                len: 0,
            });
        }
    }

    Ok(())
}
