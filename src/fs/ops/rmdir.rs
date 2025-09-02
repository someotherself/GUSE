use anyhow::{anyhow, bail};

use crate::fs::GitFs;

pub fn rmdir_live(fs: &GitFs, parent: u64, name: &str) -> anyhow::Result<()> {
    let attr = fs
        .lookup(parent, name)?
        .ok_or_else(|| anyhow!(format!("{name} not found in parent {parent}")))?;
    if !fs.is_dir(attr.ino)? {
        bail!("Not a directory")
    }
    let entries = fs.readdir(attr.ino)?;
    if !entries.is_empty() {
        bail!("Parent is not empty")
    }
    let path = fs.build_full_path(attr.ino)?;
    std::fs::remove_dir(path)?;

    fs.remove_db_record(attr.ino)?;
    Ok(())
}

pub fn rmdir_repo(fs: &GitFs, parent: u64, name: &str) -> anyhow::Result<()> {
    let attr = fs
        .lookup(parent, name)?
        .ok_or_else(|| anyhow!(format!("{name} not found in parent {parent}")))?;
    if !fs.is_dir(attr.ino)? {
        bail!("Not a directory")
    }
    let live_ino = fs.get_live_ino(parent);
    if attr.ino == live_ino {
        {
            let repo = fs.get_repo(parent)?;
            let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.live_exists = false;
        }
        return Ok(());
    }

    let entries = fs.readdir(attr.ino)?;
    let mut entries_len = entries.len();
    for entry in &entries {
        if entry.ino == live_ino || entries_len > 0 {
            entries_len -= 1;
        }
    }

    if !entries.is_empty() {
        bail!("Parent is not empty")
    }
    let path = fs.build_full_path(attr.ino)?;
    std::fs::remove_dir(path)?;

    Ok(())
}
