use std::os::unix::fs::FileExt;

use anyhow::{anyhow, bail};

use crate::{fs::GitFs, inodes::NormalIno, mount::InvalMsg};

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
    let new_size = ctx.file.size()?;
    fs.update_size_in_db(ino.into(), new_size)?;

    let _ = fs.notifier.send(InvalMsg::Inode {
        ino,
        off: 0,
        len: 0,
    });

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
    let new_size = ctx.file.size()?;
    fs.update_size_in_db(ino.into(), new_size)?;

    let _ = fs.notifier.send(InvalMsg::Inode {
        ino: ino.to_norm_u64(),
        off: 0,
        len: 0,
    });

    Ok(bytes_written)
}
