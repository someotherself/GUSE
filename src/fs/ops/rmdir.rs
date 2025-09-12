use std::ffi::OsString;

use anyhow::{anyhow, bail};

use crate::{
    fs::GitFs,
    inodes::{Inodes, NormalIno}, mount::InvalMsg,
};

pub fn rmdir_live(fs: &GitFs, parent: NormalIno, name: &str) -> anyhow::Result<()> {
    let parent = parent.to_norm_u64();
    let attr = fs
        .lookup(parent, name)?
        .ok_or_else(|| anyhow!(format!("{name} not found in parent {parent}")))?;
    if !fs.is_dir(attr.ino.into())? {
        bail!("Not a directory")
    }
    let entries = fs.readdir(attr.ino)?;
    if !entries.is_empty() {
        bail!("Parent is not empty")
    }
    let path = fs.build_full_path(attr.ino)?;
    std::fs::remove_dir(path)?;

    fs.remove_db_record(attr.ino)?;

    let _ = fs.notifier.send(InvalMsg::Entry { parent: parent, name: OsString::from(name) });
    let _ = fs.notifier.send(InvalMsg::Inode { ino: parent, off: 0, len: 0 });
    let _ = fs.notifier.send(InvalMsg::Inode { ino: attr.ino,  off: 0, len: 0 });

    Ok(())
}

pub fn rmdir_git(fs: &GitFs, parent: NormalIno, name: &str) -> anyhow::Result<()> {
    let attr = fs
        .lookup(parent.to_norm_u64(), name)?
        .ok_or_else(|| anyhow!(format!("{name} not found in parent {parent}")))?;
    let target_ino: Inodes = attr.ino.into();
    if !fs.is_dir(target_ino)? {
        bail!("Not a dir")
    }

    let path = {
        let parent_oid = fs.parent_commit_build_session(parent)?;
        let build_root = fs.get_path_to_build_folder(parent)?;
        let repo = fs.get_repo(parent.to_norm_u64())?;
        let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let session = repo.get_or_init_build_session(parent_oid, &build_root)?;
        drop(repo);
        session.finish_path(fs, parent)?.join(name)
    };

    std::fs::remove_dir(path)?;

    fs.remove_db_record(attr.ino)?;

    let _ = fs.notifier.send(InvalMsg::Entry { parent: parent.to_norm_u64(), name: OsString::from(name) });
    let _ = fs.notifier.send(InvalMsg::Inode { ino: parent.to_norm_u64(), off: 0, len: 0 });
    let _ = fs.notifier.send(InvalMsg::Inode { ino: attr.ino,  off: 0, len: 0 });

    Ok(())
}
