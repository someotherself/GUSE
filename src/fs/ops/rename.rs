use std::{
    ffi::{OsStr, OsString},
    time::SystemTime,
};

use anyhow::bail;
use git2::Oid;

use crate::{
    fs::{
        GitFs,
        fileattr::{FileType, InoFlag, StorageNode},
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
        && let DbReturn::Found { value: _ } = res
    {
        dest_exists = true;
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
        bail!(std::io::Error::from_raw_os_error(libc::EPERM))
    };

    // let mut new_attr = GitFs::attr_from_path(ino_flag, &dest.clone())?;
    // new_attr.ino = src_attr.ino;
    let mut new_attr = src_attr;
    new_attr.atime = SystemTime::now();
    new_attr.ino_flag = ino_flag;

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
    let src_commit_oid = fs.get_oid_from_db(old_parent.into())?;
    let src = {
        let ino = old_parent;
        let session = repo.get_or_init_build_session(src_commit_oid, build_root)?;
        session.finish_path(fs, ino)?.join(old_name)
    };

    let dst_commit_oid = fs.get_oid_from_db(new_parent.into())?;
    let dest = {
        let ino = new_parent;
        let session = repo.get_or_init_build_session(dst_commit_oid, build_root)?;
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

    if src_commit_oid != dst_commit_oid {
        // Change the attr.oid of all the entries moved
        update_all_oids(fs, src_attr.ino, dst_commit_oid)?;
    };

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

fn update_all_oids(fs: &GitFs, target_ino: u64, oid: Oid) -> anyhow::Result<()> {
    let mut targets = Vec::with_capacity(256);
    read_oid_targets(fs, &mut targets, target_ino)?;

    let repo = fs.get_repo(target_ino)?;
    let store = &repo.ino_table;
    store.update_oid_targets(oid, &targets);

    Ok(())
}

fn read_oid_targets(fs: &GitFs, targets: &mut Vec<u64>, target_ino: u64) -> anyhow::Result<()> {
    targets.clear();
    let mut stack: Vec<u64> = Vec::with_capacity(16);
    if fs.is_file(target_ino.into())? {
        targets.push(target_ino);
        return Ok(());
    }
    targets.push(target_ino);
    stack.push(target_ino);

    while let Some(cur_ino) = stack.pop() {
        let entries = fs.readdir(cur_ino)?;
        for e in entries {
            if e.kind == FileType::Directory {
                stack.push(e.ino);
            };
            targets.push(e.ino);
        }
    }
    Ok(())
}
