use std::os::unix::fs::FileExt;

use anyhow::{anyhow, bail};
use git2::Oid;

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
    _offset: u64,
    _buf: &[u8],
    _fh: u64,
) -> anyhow::Result<usize> {
    let res = classify_inode(fs, ino.to_norm_u64())?;
    match res {
        DirCase::Month { year: _, month: _ } => {
            bail!("This folder is read only!")
        }
        DirCase::Commit { oid: _ } => {
            todo!()
        }
    }
}
