use std::os::unix::fs::FileExt;

use anyhow::{anyhow, bail};

use crate::{fs::GitFs, inodes::NormalIno};

pub fn write_live(fs: &GitFs, ino: u64, offset: u64, buf: &[u8], fh: u64) -> anyhow::Result<usize> {
    let guard = fs.handles.read().map_err(|_| anyhow!("Lock poisoned."))?;
    let ctx = guard
        .get(&fh)
        .ok_or_else(|| anyhow!(format!("Handle {} for ino {} does not exist", fh, ino)))?;
    if ctx.ino != ino {
        bail!("Invalid filehandle")
    }
    if !ctx.write {
        bail!("Write not permitted")
    };
    if !ctx.file.is_file() {
        bail!("Invalid handle.")
    }
    let bytes_written = ctx.file.write_at(buf, offset)?;
    // Look into syncing
    Ok(bytes_written)
}

pub fn write_git(
    fs: &GitFs,
    ino: NormalIno,
    offset: u64,
    buf: &[u8],
    fh: u64,
) -> anyhow::Result<usize> {
    let guard = fs.handles.read().map_err(|_| anyhow!("Lock poisoned."))?;
    let ctx = guard
        .get(&fh)
        .ok_or_else(|| anyhow!(format!("Handle {} for ino {} does not exist", fh, ino)))?;
    if ctx.ino != ino.to_norm_u64() {
        bail!("Invalid filehandle")
    }
    if ctx.file.is_blob() {
        bail!("Cannot write to blobs")
    }
    if !ctx.write {
        bail!("Write not permitted")
    };
    let bytes_written = ctx.file.write_at(buf, offset)?;
    Ok(bytes_written)
}
