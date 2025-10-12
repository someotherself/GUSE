use std::os::unix::fs::FileExt;

use anyhow::bail;

use crate::{fs::GitFs, inodes::Inodes};

pub fn read_live(
    fs: &GitFs,
    ino: Inodes,
    offset: u64,
    buf: &mut [u8],
    fh: u64,
) -> anyhow::Result<usize> {
    let Some(ctx) = fs.handles.get_context(fh) else {
        bail!(format!("Handle {} for ino {} does not exist", fh, ino))
    };
    if !ctx.source.is_file() {
        bail!("Invalid handle - wrong file type")
    }
    if ctx.ino != *ino {
        bail!("Invalid handle - wrong inode")
    }

    let len = ctx.source.size()?;
    if offset >= len {
        return Ok(0);
    }

    let avail = (len - offset) as usize;
    let want = buf.len().min(avail);
    let n = ctx.source.read_at(&mut buf[..want], offset)?;
    Ok(n)
}

pub fn read_git(
    fs: &GitFs,
    ino: Inodes,
    offset: u64,
    buf: &mut [u8],
    fh: u64,
) -> anyhow::Result<usize> {
    let Some(ctx) = fs.handles.get_context(fh) else {
        bail!(format!("Handle {} for ino {} does not exist", fh, ino))
    };
    if ctx.ino != *ino {
        bail!("Invalid handle - wrong inode")
    }
    Ok(ctx.source.read_at(buf, offset)?)
}
