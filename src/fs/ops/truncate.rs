use anyhow::bail;

use crate::{fs::GitFs, inodes::NormalIno, mount::InvalMsg};

pub fn truncate_live(fs: &GitFs, ino: NormalIno, size: u64, fh: Option<u64>) -> anyhow::Result<()> {
    let fh = match fh {
        Some(fh) => fh,
        None => fs.open(ino.to_norm_u64(), true, true, false)?,
    };
    let Some(ctx) = fs.handles.get_context(fh) else {
        tracing::error!("Handle {} for ino {} does not exist", fh, ino);
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
    };
    if ctx.ino != ino.to_norm_u64() {
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
    }
    if !ctx.source.is_file() {
        bail!(std::io::Error::from_raw_os_error(libc::EPERM))
    }
    ctx.source.trucate(size)?;
    fs.update_size_in_db(ino, size)?;
    let _ = fs.notifier.try_send(InvalMsg::Inode {
        ino: ino.to_norm_u64(),
        off: 0,
        len: 0,
    });

    Ok(())
}

pub fn truncate_git(fs: &GitFs, ino: NormalIno, size: u64, fh: Option<u64>) -> anyhow::Result<()> {
    let fh = match fh {
        Some(fh) => fh,
        None => fs.open(ino.to_norm_u64(), true, true, false)?,
    };
    let Some(ctx) = fs.handles.get_context(fh) else {
        tracing::error!("Handle {} for ino {} does not exist", fh, ino);
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
    };
    if ctx.ino != ino.to_norm_u64() {
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
    }
    if !ctx.source.is_file() {
        bail!(std::io::Error::from_raw_os_error(libc::EBADF))
    }
    ctx.source.trucate(size)?;
    fs.update_size_in_db(ino, size)?;
    let _ = fs.notifier.try_send(InvalMsg::Inode {
        ino: ino.to_norm_u64(),
        off: 0,
        len: 0,
    });

    Ok(())
}
