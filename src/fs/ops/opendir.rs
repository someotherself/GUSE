use std::{collections::BTreeMap, ffi::OsString, sync::Arc};

use anyhow::bail;
use git2::Oid;
use parking_lot::Mutex;
use tracing::instrument;

use crate::{
    fs::{
        GitFs, Handle, SourceTypes,
        fileattr::{FileAttr, InoFlag, ObjectAttr, StorageNode, file_attr},
        meta_db::DbReturn,
        ops::readdir::DirectoryStreamCookie,
        repo::VirtualNode,
    },
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
    if !fs.inode_exists(ino)? {
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
    if !fs.inode_exists(ino)? {
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
    if !fs.inode_exists(ino)? {
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

/// Saves the files in the GitRepo state, then prepares the handle so readdir can populate it
pub fn opendir_vdir_file_commits(fs: &GitFs, ino: VirtualIno) -> anyhow::Result<u64> {
    if !fs.inode_exists(ino.to_norm())? {
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };

    let oid = fs.get_oid_from_db(ino.to_norm_u64())?;
    let snap_commit = fs.get_parent_commit(ino.to_norm_u64())?;
    let log_entries = log_entries(fs, snap_commit, ino.to_norm_u64(), oid)?;

    let node = VirtualNode {
        real: ino.to_norm_u64(),
        ino: ino.to_virt_u64(),
        oid,
        log: log_entries,
    };
    let repo = fs.get_repo(ino.to_norm_u64())?;
    repo.with_ino_state_mut(|s| s.vdir_cache.insert(ino, node));

    let iter = DirectoryStreamCookie {
        next_name: None,
        last_stream: Vec::new(),
        dir_stream: None,
    };
    let entries = SourceTypes::DirSnapshot {
        entries: Arc::new(Mutex::new(iter)),
    };

    let handle = Handle {
        ino: ino.to_virt_u64(),
        source: entries,
        write: false,
    };

    fs.handles.open(handle)
}

#[instrument(level = "debug", skip(fs), fields(ino = %parent), err(Display))]
fn log_entries(
    fs: &GitFs,
    snap_commit: Oid,
    parent: u64,
    origin_oid: Oid,
) -> anyhow::Result<BTreeMap<OsString, (u64, ObjectAttr)>> {
    let repo = fs.get_repo(parent)?;
    let entries = repo.blob_history_objects(snap_commit, origin_oid)?;
    let path = fs.build_full_path(parent.into())?;
    let file_ext = match path.extension().unwrap_or_default().to_str() {
        Some(e) => format!(".{e}"),
        None => String::new(),
    };

    let mut log_entries: BTreeMap<OsString, (u64, ObjectAttr)> = BTreeMap::new();
    let mut nodes: Vec<StorageNode> = Vec::with_capacity(entries.len());
    let mut count = 0_usize;
    for e in entries {
        count += 1;
        let name = OsString::from(format!("{count:04}_{}{file_ext}", e.name.display()));
        let new_ino = match fs.exists_by_name(parent, &e.name)? {
            DbReturn::Found { value: ino } => ino,
            DbReturn::Missing => {
                let new_ino = fs.next_inode_checked(parent)?;
                let mut attr: FileAttr = file_attr(InoFlag::InsideSnap).into();
                attr.oid = e.oid;
                attr.ino = new_ino;
                attr.size = e.size;
                nodes.push(StorageNode {
                    parent_ino: parent,
                    name: name.clone(),
                    attr,
                });
                new_ino
            }
            DbReturn::Negative => continue,
        };
        // Format the name as 0001.<7char oid>.<orig file ext> (0001_e1ca722.txt)
        log_entries.insert(name, (new_ino, e));
    }
    fs.write_inodes_to_db(nodes)?;
    Ok(log_entries)
}
