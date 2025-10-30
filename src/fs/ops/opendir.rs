use std::{
    collections::BTreeMap,
    ffi::OsString,
    sync::{Arc, Mutex},
};

use anyhow::bail;
use git2::Oid;

use crate::{
    fs::{GitFs, Handle, SourceTypes, fileattr::ObjectAttr, ops::readdir::DirectoryStreamCookie},
    inodes::{NormalIno, VirtualIno},
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
        write: false,
    };
    fs.handles.open(handle)
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
        write: false,
    };
    fs.handles.open(handle)
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
        write: false,
    };
    fs.handles.open(handle)
}

pub fn opendir_vdir_file_commits(fs: &GitFs, ino: VirtualIno) -> anyhow::Result<u64> {
    if !fs.inode_exists(ino.into())? {
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };

    let oid = fs.get_oid_from_db(ino.to_norm_u64())?;
    let log_entries = log_entries(fs, ino.to_norm_u64(), oid)?;

    let entries = SourceTypes::FileCommits {
        entries: log_entries,
    };

    let handle: Handle = Handle {
        ino: ino.to_virt_u64(),
        source: entries,
        write: false,
    };
    fs.handles.open(handle)
}

fn log_entries(
    fs: &GitFs,
    ino: u64,
    origin_oid: Oid,
) -> anyhow::Result<BTreeMap<OsString, (u64, ObjectAttr)>> {
    let repo = fs.get_repo(ino)?;
    let entries = repo.blob_history_objects(origin_oid)?;
    let path = fs.build_full_path(ino.into())?;
    let file_ext = match path.extension().unwrap_or_default().to_str() {
        Some(e) => format!(".{e}"),
        None => String::new(),
    };

    let mut log_entries: BTreeMap<OsString, (u64, ObjectAttr)> = BTreeMap::new();
    for e in entries {
        let new_ino = fs.next_inode_checked(ino)?;
        // Format the name as 0001.<7char oid>.<orig file ext> (0001_e1ca722.txt)
        let name = OsString::from(format!("{}{file_ext}", e.name.display()));
        log_entries.insert(name, (new_ino, e));
    }
    Ok(log_entries)
}
