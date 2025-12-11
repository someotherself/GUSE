use std::os::unix::fs::FileExt;

use anyhow::bail;

use crate::{fs::GitFs, inodes::NormalIno};

pub fn write_live(fs: &GitFs, ino: u64, offset: u64, buf: &[u8], fh: u64) -> anyhow::Result<usize> {
    let Some(ctx) = fs.handles.get_context(fh) else {
        bail!(format!("Handle {} for ino {} does not exist", fh, ino))
    };
    if ctx.ino != ino {
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
    }
    if !ctx.write {
        bail!(std::io::Error::from_raw_os_error(libc::EPERM))
    };
    if !ctx.source.is_file() {
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
    }
    let old_size = fs.get_file_size_from_db(ino.into())?;
    let bytes_written = ctx.source.write_at(buf, offset)?;

    if bytes_written > 0 {
        let new_size = std::cmp::max(old_size, offset + bytes_written as u64);
        if new_size != old_size {
            fs.update_size_in_storage(ino.into(), new_size)?;
        }
    }
    Ok(bytes_written)
}

pub fn write_git(
    fs: &GitFs,
    ino: NormalIno,
    offset: u64,
    buf: &[u8],
    fh: u64,
) -> anyhow::Result<usize> {
    let Some(ctx) = fs.handles.get_context(fh) else {
        bail!(format!("Handle {} for ino {} does not exist", fh, ino))
    };
    if ctx.ino != ino.to_norm_u64() {
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
    }
    if ctx.source.is_blob() {
        bail!(std::io::Error::from_raw_os_error(libc::EPERM))
    }
    if !ctx.write {
        bail!(std::io::Error::from_raw_os_error(libc::EPERM))
    };
    let old_size = fs.get_file_size_from_db(ino)?;
    let bytes_written = ctx.source.write_at(buf, offset)?;

    if bytes_written > 0 {
        let new_size = std::cmp::max(old_size, offset + bytes_written as u64);
        if new_size != old_size {
            fs.update_size_in_storage(ino, new_size)?;
        }
    }

    Ok(bytes_written)
}
