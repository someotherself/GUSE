use std::ffi::{OsStr, OsString};

use anyhow::bail;

use crate::{fs::GitFs, inodes::NormalIno, mount::InvalMsg};

pub fn rmdir_live(fs: &GitFs, parent: NormalIno, name: &OsStr) -> anyhow::Result<()> {
    // 1 - Set inactive in cache
    // 2 - Remove from storage
    let target_ino = fs.get_ino_from_db(parent.into(), name)?;
    let Ok(path) = fs.get_live_path(target_ino.into()) else {
        tracing::error!("Target does not exist");
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    std::fs::remove_dir(path)?;
    fs.set_inactive(parent.into(), name)?;
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
    fs.set_inactive(parent.into(), name)?;
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
