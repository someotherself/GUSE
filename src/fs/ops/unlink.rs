use std::ffi::{OsStr, OsString};

use anyhow::bail;

use crate::{fs::GitFs, inodes::NormalIno, mount::InvalMsg};

pub fn unlink_live(fs: &GitFs, parent: u64, name: &OsStr) -> anyhow::Result<()> {
    let Ok(target_ino) = fs.get_ino_from_db(parent, name) else {
        tracing::error!("Target does not exist");
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    let path = fs.build_full_path(target_ino.into())?;
    std::fs::remove_file(path)?;
    fs.remove_db_dentry(parent.into(), name)?;

    let _ = fs.notifier.try_send(InvalMsg::Entry {
        parent,
        name: OsString::from(name),
    });
    let _ = fs.notifier.try_send(InvalMsg::Inode {
        ino: parent,
        off: 0,
        len: 0,
    });

    Ok(())
}

pub fn unlink_build_dir(fs: &GitFs, parent: NormalIno, name: &OsStr) -> anyhow::Result<()> {
    let path = {
        let commit_oid = fs.get_oid_from_db(parent.into())?;
        let build_root = fs.get_path_to_build_folder(parent)?;
        let repo = fs.get_repo(parent.to_norm_u64())?;
        let session = repo.get_or_init_build_session(commit_oid, &build_root)?;
        session.finish_path(fs, parent)?.join(name)
    };

    std::fs::remove_file(path)?;
    fs.remove_db_dentry(parent, name)?;

    // TODO: Replace with notifier.delete
    let _ = fs.notifier.try_send(InvalMsg::Entry {
        parent: parent.to_norm_u64(),
        name: OsString::from(name),
    });
    let _ = fs.notifier.try_send(InvalMsg::Inode {
        ino: parent.to_norm_u64(),
        off: 0,
        len: 0,
    });

    Ok(())
}
