use std::os::unix::fs::FileExt;

use anyhow::{anyhow, bail};

use crate::fs::GitFs;

pub fn read_live(
    fs: &GitFs,
    _ino: u64,
    offset: u64,
    buf: &mut [u8],
    fh: u64,
) -> anyhow::Result<usize> {
    let guard = fs.handles.read().map_err(|_| anyhow!("Lock poisoned."))?;
    let ctx = guard
        .get(&fh)
        .ok_or_else(|| anyhow!("Handle does not exist"))?;
    if !ctx.write {
        bail!("Write not permitted")
    };
    if !ctx.file.is_file() {
        bail!("Invalid handle.")
    }
    Ok(ctx.file.read_at(buf, offset)?)
}

pub fn read_git(
    fs: &GitFs,
    _ino: u64,
    offset: u64,
    buf: &mut [u8],
    fh: u64,
) -> anyhow::Result<usize> {
    let guard = fs.handles.read().map_err(|_| anyhow!("Lock poisoned."))?;
    let ctx = guard
        .get(&fh)
        .ok_or_else(|| anyhow!("Handle does not exist"))?;
    if !ctx.write {
        bail!("Write not permitted")
    };
    if !ctx.file.is_blob() {
        bail!("Invalid handle.")
    }
    Ok(ctx.file.read_at(buf, offset)?)
}
