use std::ffi::{OsStr, OsString};

use crate::{
    fs::{GitFs, janitor::rename_to_trash},
    inodes::NormalIno,
    mount::InvalMsg,
};

pub fn unlink_live(fs: &GitFs, parent: u64, name: &OsStr) -> anyhow::Result<()> {
    let from = fs.get_live_path(parent.into())?.join(name);
    rename_to_trash(fs, &from, name)?;
    fs.set_entry_negative(parent.into(), name)?;
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

    Ok(())
}

pub fn unlink_build_dir(fs: &GitFs, parent: NormalIno, name: &OsStr) -> anyhow::Result<()> {
    let from = {
        let commit_oid = fs.get_oid_from_db(parent.into())?;
        let repo = fs.get_repo(parent.into())?;
        let build_root = &repo.build_dir;
        let session = repo.get_or_init_build_session(commit_oid, build_root)?;
        session.finish_path(fs, parent)?.join(name)
    };
    rename_to_trash(fs, &from, name)?;
    fs.set_entry_negative(parent, name)?;
    {
        let _ = fs.notifier.try_send(InvalMsg::Entry {
            parent: parent.to_norm_u64(),
            name: OsString::from(name),
        });
        let _ = fs.notifier.try_send(InvalMsg::Inode {
            ino: parent.to_norm_u64(),
            off: 0,
            len: 0,
        });
    }

    Ok(())
}
