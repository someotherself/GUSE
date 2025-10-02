use std::os::unix::fs::FileExt;

use anyhow::bail;

use crate::{fs::GitFs, inodes::NormalIno, mount::InvalMsg};

pub fn write_live(fs: &GitFs, ino: u64, offset: u64, buf: &[u8], fh: u64) -> anyhow::Result<usize> {
    let Some(ctx) = fs.handles.get_context(fh) else {
        bail!(format!("Handle {} for ino {} does not exist", fh, ino))
    };
    if ctx.ino != ino {
        bail!("Invalid filehandle")
    }
    if !ctx.write {
        bail!("Write not permitted")
    };
    if !ctx.source.is_file() {
        bail!("Invalid handle.")
    }
    let old_size = ctx.source.size()?;
    let bytes_written = ctx.source.write_at(buf, offset)?;

    if bytes_written > 0 {
        let new_size = std::cmp::max(old_size, offset + bytes_written as u64);
        if new_size != old_size {
            fs.update_size_in_db(ino.into(), new_size)?;
        }
        let _ = fs.notifier.try_send(InvalMsg::Inode {
            ino,
            off: 0,
            len: 0,
        });
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
        bail!("Invalid filehandle")
    }
    if ctx.source.is_blob() {
        bail!("Cannot write to blobs")
    }
    if !ctx.write {
        bail!("Write not permitted")
    };
    let old_size = ctx.source.size()?;
    let bytes_written = ctx.source.write_at(buf, offset)?;

    if bytes_written > 0 {
        let new_size = std::cmp::max(old_size, offset + bytes_written as u64);
        if new_size != old_size {
            fs.update_size_in_db(ino, new_size)?;
        }
        let _ = fs.notifier.try_send(InvalMsg::Inode {
            ino: ino.to_norm_u64(),
            off: 0,
            len: 0,
        });
    }

    Ok(bytes_written)
}
