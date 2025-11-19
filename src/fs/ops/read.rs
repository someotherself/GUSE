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
        tracing::error!("Handle {} for ino {} does not exist", fh, ino);
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
    };
    if !ctx.source.is_file() {
        tracing::error!("Handle {} for ino {} is not a file", fh, ino);
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
    }
    if ctx.ino != *ino {
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
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
        tracing::error!("Handle {} for ino {} does not exist", fh, ino);
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
    };
    if ctx.ino != *ino {
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
    }
    // handle blobs and files separately
    Ok(ctx.source.read_at(buf, offset)?)
}
