#![allow(unused_variables)]
use crate::fs::GitFs;

pub fn open_live(fs: &GitFs, ino: u64, read: bool, write: bool) -> anyhow::Result<u64> {
    todo!()
}

pub fn open_git(fs: &GitFs, ino: u64, read: bool, write: bool) -> anyhow::Result<u64> {
    todo!()
}
