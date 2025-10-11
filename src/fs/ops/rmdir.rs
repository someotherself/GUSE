use std::ffi::{OsStr, OsString};

use anyhow::bail;

use crate::{fs::GitFs, inodes::NormalIno, mount::InvalMsg};

pub fn rmdir_live(fs: &GitFs, parent: NormalIno, name: &OsStr) -> anyhow::Result<()> {
    let Ok(target_ino) = fs.get_ino_from_db(parent.into(), name) else {
        tracing::error!("Target does not exist");
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
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
        let parent_oid = fs.parent_commit_build_session(parent)?;
        let build_root = fs.get_path_to_build_folder(parent)?;
        let repo = fs.get_repo(parent.to_norm_u64())?;
        let session = repo.get_or_init_build_session(parent_oid, &build_root)?;
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
