use std::sync::{Arc, Mutex};

use anyhow::bail;

use crate::{
    fs::{GitFs, Handle, SourceTypes, ops::readdir::DirectoryStreamCookie},
    inodes::NormalIno,
};

pub fn opendir_root(fs: &GitFs, ino: NormalIno) -> anyhow::Result<u64> {
    let iter = DirectoryStreamCookie {
        next_name: None,
        last_stream: Vec::new(),
        dir_stream: None,
    };
    let dir = SourceTypes::DirSnapshot {
        entries: Arc::new(Mutex::new(iter)),
    };
    let handle = Handle {
        ino: ino.into(),
        source: dir,
        read: false,
        write: false,
    };
    let fh = fs.handles.open(handle)?;
    Ok(fh)
}

pub fn opendir_repo(fs: &GitFs, ino: NormalIno) -> anyhow::Result<u64> {
    if !fs.inode_exists(ino.into())? {
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    let iter = DirectoryStreamCookie {
        next_name: None,
        last_stream: Vec::new(),
        dir_stream: None,
    };
    let dir = SourceTypes::DirSnapshot {
        entries: Arc::new(Mutex::new(iter)),
    };
    let handle = Handle {
        ino: ino.into(),
        source: dir,
        read: false,
        write: false,
    };
    let fh = fs.handles.open(handle)?;
    Ok(fh)
}

pub fn opendir_live(fs: &GitFs, ino: NormalIno) -> anyhow::Result<u64> {
    if !fs.inode_exists(ino.into())? {
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    let iter = DirectoryStreamCookie {
        next_name: None,
        last_stream: Vec::new(),
        dir_stream: None,
    };
    let dir = SourceTypes::DirSnapshot {
        entries: Arc::new(Mutex::new(iter)),
    };
    let handle = Handle {
        ino: ino.into(),
        source: dir,
        read: false,
        write: false,
    };
    let fh = fs.handles.open(handle)?;
    Ok(fh)
}

pub fn opendir_git(fs: &GitFs, ino: NormalIno) -> anyhow::Result<u64> {
    if !fs.inode_exists(ino.into())? {
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    let iter = DirectoryStreamCookie {
        next_name: None,
        last_stream: Vec::new(),
        dir_stream: None,
    };
    let dir = SourceTypes::DirSnapshot {
        entries: Arc::new(Mutex::new(iter)),
    };
    let handle: Handle = Handle {
        ino: ino.into(),
        source: dir,
        read: false,
        write: false,
    };
    let fh = fs.handles.open(handle)?;
    Ok(fh)
}
