use std::ffi::OsString;

use anyhow::{anyhow, bail};

use crate::{fs::GitFs, inodes::NormalIno, mount::InvalMsg};

pub fn unlink_live(fs: &GitFs, parent: u64, name: &str) -> anyhow::Result<()> {
    let Ok(target_ino) = fs.get_ino_from_db(parent, name) else {
        tracing::error!("Target does not exist");
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    let path = fs.build_full_path(target_ino.into())?;
    std::fs::remove_file(path)?;
    fs.remove_db_record(parent.into(), name)?;

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

pub fn unlink_build_dir(fs: &GitFs, parent: NormalIno, name: &str) -> anyhow::Result<()> {
    let path = {
        let parent_oid = fs.parent_commit_build_session(parent)?;
        let build_root = fs.get_path_to_build_folder(parent)?;
        let repo = fs.get_repo(parent.to_norm_u64())?;
        let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let session = repo.get_or_init_build_session(parent_oid, &build_root)?;
        drop(repo);
        session.finish_path(fs, parent)?.join(name)
    };

    std::fs::remove_file(path)?;
    fs.remove_db_record(parent, name)?;

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
