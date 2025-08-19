#![allow(unused_variables)]
use crate::fs::GitFs;

pub fn write_live(
    fs: &GitFs,
    ino: u64,
    offset: u64,
    buf: &[u8],
    handle: u64,
) -> anyhow::Result<usize> {
    todo!()
}

pub fn write_git(fs: &GitFs, ino: u64, offset: u64, buf: &[u8], fh: u64) -> anyhow::Result<usize> {
    todo!()
}
