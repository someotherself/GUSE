#![allow(unused_variables)]
use crate::fs::GitFs;

pub fn read_live(
    fs: &GitFs,
    ino: u64,
    offset: u64,
    buf: &mut [u8],
    fh: u64,
) -> anyhow::Result<usize> {
    todo!()
}

pub fn read_git(
    fs: &GitFs,
    ino: u64,
    offset: u64,
    buf: &mut [u8],
    fh: u64,
) -> anyhow::Result<usize> {
    todo!()
}
