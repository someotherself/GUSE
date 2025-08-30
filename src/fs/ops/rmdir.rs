use anyhow::{anyhow, bail};

use crate::fs::GitFs;

pub fn rmdir_live(fs: &GitFs, parent: u64, name: &str) -> anyhow::Result<()> {
    let attr = fs
        .lookup(parent, name)?
        .ok_or_else(|| anyhow!(format!("{name} not found in parent {parent}")))?;
    if !fs.is_dir(attr.ino)? {
        bail!("Not a directory")
    }
    let entries = fs.readdir(parent)?;
    if !entries.is_empty() {
        bail!("Parent is not empty")
    }
    let path = fs.build_full_path(attr.ino)?;
    std::fs::remove_dir(path)?;

    fs.remove_db_record(attr.ino)?;
    Ok(())
}
