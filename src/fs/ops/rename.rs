use std::ffi::{OsStr, OsString};

use anyhow::bail;

use crate::{
    fs::{GitFs, fileattr::InoFlag},
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

    let src_attr = fs.get_metadata_by_name(old_parent, old_name)?;

    let mut dest_exists = false;

    if let Ok(value) = fs.get_metadata_by_name(new_parent, new_name) {
        dest_exists = true;

        if value.kind != src_attr.kind {
            bail!("Source and destination are not the same type")
        }
    };

    let src = fs.build_full_path(old_parent)?.join(old_name);
    let dest = fs.build_full_path(new_parent)?.join(new_name);

    std::fs::rename(src, &dest)?;

    if dest_exists {
        fs.remove_db_entry(new_parent, new_name)?;
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
    new_attr.parent_ino = new_parent.to_norm_u64();
    new_attr.name = new_name.into();

    fs.update_db_record(old_parent, old_name, new_attr)?;

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
    let dest_in_build = fs.is_in_build(new_parent)?;
    let oid = fs.get_oid_from_db(new_parent.into())?;
    let is_commit_folder = fs.is_commit(new_parent, oid)?;
    if !dest_in_build && !is_commit_folder {
        bail!(format!("New parent {} not allowed", new_parent));
    }
    let src_attr = fs.get_metadata_by_name(old_parent, old_name)?;

    let mut dest_exists = false;

    if let Ok(value) = fs.get_metadata_by_name(new_parent, new_name) {
        dest_exists = true;

        if value.kind != src_attr.kind {
            bail!("Source and destination are not the same type")
        }
    };

    if dest_exists {
        fs.remove_db_entry(new_parent, new_name)?;
    }

    let new_attr = src_attr.clone();

    fs.update_db_record(old_parent, old_name, new_attr)?;

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
