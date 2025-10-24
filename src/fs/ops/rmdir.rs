use std::ffi::{OsStr, OsString};

use anyhow::bail;

use crate::{
    fs::{GitFs, meta_db::DbReturn},
    inodes::NormalIno,
    mount::InvalMsg,
};

pub fn rmdir_live(fs: &GitFs, parent: NormalIno, name: &OsStr) -> anyhow::Result<()> {
    let target_ino = match fs.get_ino_from_db(parent.into(), name) {
        Ok(DbReturn::Found { value: ino }) => ino,
        _ => {
            tracing::error!("Target does not exist");
            bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
        }
    };
    let Ok(path) = fs.get_live_path(target_ino.into()) else {
        tracing::error!("Target does not exist");
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    std::fs::remove_dir(path)?;
    fs.remove_db_dentry(parent, name)?;
    {
        let _ = fs.notifier.try_send(InvalMsg::Entry {
            parent: parent.into(),
            name: OsString::from(name),
        });
        let _ = fs.notifier.try_send(InvalMsg::Inode {
            ino: parent.into(),
            off: 0,
            len: 0,
        });
    }

    Ok(())
}

pub fn rmdir_git(fs: &GitFs, parent: NormalIno, name: &OsStr) -> anyhow::Result<()> {
    let path = {
        let commit_oid = fs.get_oid_from_db(parent.into())?;
        let repo = fs.get_repo(parent.to_norm_u64())?;
        let build_root = &repo.build_dir;
        let session = repo.get_or_init_build_session(commit_oid, build_root)?;
        drop(repo);
        session.finish_path(fs, parent)?.join(name)
    };

    std::fs::remove_dir(path)?;
    fs.remove_db_dentry(parent, name)?;
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
