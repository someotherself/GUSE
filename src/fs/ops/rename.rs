use std::{
    ffi::{OsStr, OsString},
    time::SystemTime,
};

use anyhow::bail;

use crate::{
    fs::{
        GitFs,
        fileattr::{InoFlag, StorageNode},
        meta_db::DbReturn,
    },
    inodes::NormalIno,
    mount::InvalMsg,
};

pub fn rename_live(
    fs: &GitFs,
    old_parent: NormalIno,
    old_name: &OsStr,
    new_parent: NormalIno,
    new_name: &OsStr,
) -> anyhow::Result<()> {
    let dest_in_live = fs.is_in_live(new_parent)?;
    let dest_in_build = fs.is_in_build(new_parent)?;
    if !dest_in_live && !dest_in_build {
        bail!(format!("New parent {} not allowed", new_parent));
    }

    let src_attr = match fs.get_metadata_by_name(old_parent, old_name)? {
        DbReturn::Found { value } => value,
        _ => bail!(std::io::Error::from_raw_os_error(libc::ENOENT)),
    };

    let mut dest_exists = false;

    if let Ok(res) = fs.get_metadata_by_name(new_parent, new_name)
        && let DbReturn::Found { value } = res
    {
        dest_exists = true;

        if value.kind != src_attr.kind {
            bail!("Source and destination are not the same type")
        }
    };

    let src = fs.build_full_path(old_parent)?.join(old_name);
    let dest = fs.build_full_path(new_parent)?.join(new_name);

    std::fs::rename(src, &dest)?;

    if dest_exists {
        fs.remove_db_dentry(new_parent, new_name)?;
    }

    let ino_flag = if dest_in_live {
        InoFlag::InsideLive
    } else if dest_in_build {
        InoFlag::InsideBuild
    } else {
        bail!("Invalid location")
    };

    let mut new_attr = GitFs::attr_from_path(ino_flag, &dest.clone())?;
    new_attr.ino = src_attr.ino;

    let node = StorageNode {
        parent_ino: new_parent.to_norm_u64(),
        name: new_name.into(),
        attr: new_attr,
    };
    fs.update_db_record(old_parent, old_name, node)?;

    {
        let _ = fs.notifier.try_send(InvalMsg::Entry {
            parent: old_parent.to_norm_u64(),
            name: OsString::from(old_name),
        });
        if old_parent != new_parent {
            let _ = fs.notifier.try_send(InvalMsg::Entry {
                parent: new_parent.to_norm_u64(),
                name: OsString::from(new_name),
            });
        };
        let _ = fs.notifier.try_send(InvalMsg::Inode {
            ino: old_parent.to_norm_u64(),
            off: 0,
            len: 0,
        });
        if new_parent.to_norm_u64() != old_parent.to_norm_u64() {
            let _ = fs.notifier.try_send(InvalMsg::Inode {
                ino: new_parent.to_norm_u64(),
                off: 0,
                len: 0,
            });
        }
    }

    Ok(())
}

pub fn rename_git_build(
    fs: &GitFs,
    old_parent: NormalIno,
    old_name: &OsStr,
    new_parent: NormalIno,
    new_name: &OsStr,
) -> anyhow::Result<()> {
    let src_attr = match fs.get_metadata_by_name(old_parent, old_name)? {
        DbReturn::Found { value } => value,
        _ => bail!(std::io::Error::from_raw_os_error(libc::ENOENT)),
    };

    let mut dest_exists = false;

    if let Ok(res) = fs.get_metadata_by_name(new_parent, new_name)
        && let DbReturn::Found { value: _ } = res
    {
        dest_exists = true;
    };

    let repo = fs.get_repo(old_parent.to_norm_u64())?;
    let build_root = &repo.build_dir;
    let commit_oid = fs.get_oid_from_db(old_parent.into())?;
    let src = {
        let ino = old_parent;
        let session = repo.get_or_init_build_session(commit_oid, build_root)?;
        session.finish_path(fs, ino)?.join(old_name)
    };

    let dest = {
        let ino = new_parent;
        let session = repo.get_or_init_build_session(commit_oid, build_root)?;
        session.finish_path(fs, ino)?.join(new_name)
    };

    std::fs::rename(&src, &dest)?;

    if dest_exists {
        let _ = fs.remove_db_dentry(new_parent, new_name);
    }

    let mut new_attr = src_attr;
    new_attr.atime = SystemTime::now();

    let node = StorageNode {
        parent_ino: new_parent.to_norm_u64(),
        name: new_name.into(),
        attr: new_attr,
    };
    fs.update_db_record(old_parent, old_name, node)?;

    {
        let _ = fs.notifier.try_send(InvalMsg::Entry {
            parent: old_parent.to_norm_u64(),
            name: OsString::from(old_name),
        });
        if old_parent != new_parent {
            let _ = fs.notifier.try_send(InvalMsg::Entry {
                parent: new_parent.to_norm_u64(),
                name: OsString::from(new_name),
            });
        };
        let _ = fs.notifier.try_send(InvalMsg::Inode {
            ino: old_parent.to_norm_u64(),
            off: 0,
            len: 0,
        });
        if new_parent.to_norm_u64() != old_parent.to_norm_u64() {
            let _ = fs.notifier.try_send(InvalMsg::Inode {
                ino: new_parent.to_norm_u64(),
                off: 0,
                len: 0,
            });
        }
    }

    Ok(())
}
