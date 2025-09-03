use std::os::unix::fs::FileExt;

use anyhow::{anyhow, bail};
use tracing::instrument;

use crate::{fs::GitFs, inodes::Inodes};

#[instrument(level = "debug", skip(fs), fields(ino), err(Display))]
pub fn read_live(
    fs: &GitFs,
    ino: Inodes,
    offset: u64,
    buf: &mut [u8],
    fh: u64,
) -> anyhow::Result<usize> {
    let guard = fs.handles.read().map_err(|_| anyhow!("Lock poisoned."))?;
    let ctx = guard
        .get(&fh)
        .ok_or_else(|| anyhow!(format!("Handle {} for ino {} does not exist", fh, ino)))?;
    if !ctx.file.is_file() {
        bail!("Invalid handle.")
    }
    if ctx.ino != *ino {
        bail!("Invalid filehandle")
    }

    let len = ctx.file.size()?;
    if offset >= len {
        return Ok(0);
    }

    let avail = (len - offset) as usize;
    let want = buf.len().min(avail);
    let n = ctx.file.read_at(&mut buf[..want], offset)?;
    Ok(n)
}

#[instrument(level = "debug", skip(fs), fields(ino), err(Display))]
pub fn read_git(
    fs: &GitFs,
    ino: Inodes,
    offset: u64,
    buf: &mut [u8],
    fh: u64,
) -> anyhow::Result<usize> {
    let guard = fs.handles.read().map_err(|_| anyhow!("Lock poisoned."))?;
    let ctx = guard
        .get(&fh)
        .ok_or_else(|| anyhow!("Handle does not exist"))?;
    if !ctx.file.is_blob() {
        bail!("Invalid handle.")
    }
    if ctx.ino != *ino {
        bail!("Invalid filehandle")
    }
    Ok(ctx.file.read_at(buf, offset)?)
}
