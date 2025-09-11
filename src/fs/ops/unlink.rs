use std::ffi::OsStr;

use anyhow::{anyhow, bail};

use crate::{fs::GitFs, inodes::NormalIno};

pub fn unlink_live(fs: &GitFs, parent: u64, name: &str) -> anyhow::Result<()> {
    let attr = fs
        .lookup(parent, name)?
        .ok_or_else(|| anyhow!(format!("{name} not found in parent {parent}")))?;
    if !fs.is_file(attr.ino)? && !fs.is_link(attr.ino)? {
        bail!("Not a file")
    }
    let path = fs.build_full_path(attr.ino)?;
    std::fs::remove_file(path)?;

    fs.remove_db_record(attr.ino)?;

    if let Some(notifier) = fs.notifier.get() {
        let _ = notifier.inval_entry(parent, OsStr::new(name));
        let _ = notifier.inval_inode(parent, 0, 0);
        let _ = notifier.inval_inode(attr.ino, 0, 0);
    }

    Ok(())
}

pub fn unlink_build_dir(fs: &GitFs, parent: NormalIno, name: &str) -> anyhow::Result<()> {
    let attr = fs
        .lookup(parent.to_norm_u64(), name)?
        .ok_or_else(|| anyhow!(format!("{name} not found in parent {parent}")))?;
    if !fs.is_file(attr.ino)? && !fs.is_link(attr.ino)? {
        bail!("Not a file")
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

    std::fs::remove_file(path)?;

    if let Some(notifier) = fs.notifier.get() {
        let _ = notifier.inval_entry(parent.to_norm_u64(), OsStr::new(name));
        let _ = notifier.inval_inode(parent.to_norm_u64(), 0, 0);
        let _ = notifier.inval_inode(attr.ino, 0, 0);
    }

    fs.remove_db_record(attr.ino)?;
    Ok(())
}
