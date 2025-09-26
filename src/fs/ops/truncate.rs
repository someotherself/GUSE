use anyhow::{anyhow, bail};

use crate::{fs::{ops, GitFs}, inodes::NormalIno};

pub fn truncate_live(fs: &GitFs, ino: NormalIno, size: u64, fh: Option<u64>) -> anyhow::Result<()> {
    let fh = match fh {
        Some(fh) => fh,
        None => ops::open::open_live(fs, ino, true, true, false)?,
    };

    let guard = fs.handles.read().map_err(|_| anyhow!("Lock poisoned."))?;
    let ctx = guard
        .get(&fh)
        .ok_or_else(|| anyhow!(format!("Handle {} for ino {} does not exist", fh, ino)))?;
    if ctx.ino != ino.to_norm_u64() {
        bail!("Invalid filehandle")
    }
    if !ctx.source.is_file() {
        bail!("Invalid handle.")
    }
    ctx.source.trucate(size)
}

pub fn truncate_git(fs: &GitFs, ino: NormalIno, size: u64, fh: Option<u64>) -> anyhow::Result<()> {
    let fh = match fh {
        Some(fh) => fh,
        None => ops::open::open_git(fs, ino, true, true, false)?,
    };

    let guard = fs.handles.read().map_err(|_| anyhow!("Lock poisoned."))?;
    let ctx = guard
        .get(&fh)
        .ok_or_else(|| anyhow!(format!("Handle {} for ino {} does not exist", fh, ino)))?;
    if ctx.ino != ino.to_norm_u64() {
        bail!("Invalid filehandle")
    }
    if !ctx.source.is_file() {
        bail!("Invalid handle.")
    }
    ctx.source.trucate(size)
}
