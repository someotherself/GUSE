use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail};

use crate::{
    fs::{ops::readdir::DirectoryIter, GitFs, Handle, SourceTypes},
    inodes::NormalIno,
};

pub fn opendir_root(fs: &GitFs, ino: NormalIno) -> anyhow::Result<u64> {
    let fh = fs.next_file_handle();
    let iter = DirectoryIter {
        last_offset: 2,
        last_name: None,
        next_offset: 3
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
    {
        let mut guard = fs.handles.write().map_err(|_| anyhow!("Lock poisoned"))?;
        guard.insert(fh, handle);
    }
    Ok(fh)
}

pub fn opendir_repo(fs: &GitFs, ino: NormalIno) -> anyhow::Result<u64> {
    if let Err(_) = fs.getattr(ino.into()) {
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    let fh = fs.next_file_handle();
    let iter = DirectoryIter {
        last_offset: 2,
        last_name: None,
        next_offset: 3
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
    {
        let mut guard = fs.handles.write().map_err(|_| anyhow!("Lock poisoned"))?;
        guard.insert(fh, handle);
    }
    Ok(fh)
}

pub fn opendir_live(fs: &GitFs, ino: NormalIno) -> anyhow::Result<u64> {
    if let Err(_) = fs.getattr(ino.into()) {
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    let fh = fs.next_file_handle();
    let iter = DirectoryIter {
        last_offset: 2,
        last_name: None,
        next_offset: 3
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
    {
        let mut guard = fs.handles.write().map_err(|_| anyhow!("Lock poisoned"))?;
        guard.insert(fh, handle);
    }
    Ok(fh)
}

pub fn opendir_git(fs: &GitFs, ino: NormalIno) -> anyhow::Result<u64> {
    if let Err(_) = fs.getattr(ino.into()) {
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    let iter = DirectoryIter {
        last_offset: 2,
        last_name: None,
        next_offset: 3
    };
    let fh = fs.next_file_handle();
    let dir = SourceTypes::DirSnapshot {
        entries: Arc::new(Mutex::new(iter)),
    };
    let handle: Handle = Handle {
        ino: ino.into(),
        source: dir,
        read: false,
        write: false,
    };
    {
        let mut guard = fs.handles.write().map_err(|_| anyhow!("Lock poisoned"))?;
        guard.insert(fh, handle);
    }
    Ok(fh)
}