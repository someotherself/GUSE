use anyhow::{anyhow, bail};

use crate::fs::GitFs;

pub fn unlink_live(fs: &GitFs, parent: u64, name: &str) -> anyhow::Result<()> {
    let attr = fs
        .lookup(parent, name)?
        .ok_or_else(|| anyhow!(format!("{name} not found in parent {parent}")))?;
    if !fs.is_file(attr.inode)? && !fs.is_link(attr.inode)? {
        bail!("Not a file")
    }
    let path = fs.build_full_path(attr.inode)?;
    std::fs::remove_file(path)?;

    fs.remove_db_record(attr.inode)?;
    Ok(())
}
