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

    let len = fs.get_file_size_from_db(ino.to_norm())?;
    if offset >= len {
        return Ok(0);
    }
    let mut filled = 0usize;
    while filled < buf.len() {
        let n = ctx
            .source
            .read_at(&mut buf[filled..], offset + filled as u64)?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
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
    // handle blobs and files separately
    Ok(ctx.source.read_at(buf, offset)?)
}
