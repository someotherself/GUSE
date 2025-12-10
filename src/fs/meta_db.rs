use std::{
    ffi::{OsStr, OsString},
    fmt::Debug,
    sync::atomic::{
        AtomicI32, AtomicI64, AtomicU32, AtomicU64,
        Ordering::{Relaxed, SeqCst},
    },
    time::SystemTime,
};

use anyhow::bail;
use dashmap::DashMap;
use git2::Oid;
use parking_lot::RwLock;

use crate::fs::{
    fileattr::{
        Dentry, FileAttr, FileType, InoFlag, SetFileAttr, StorageNode, dir_attr,
        pair_to_system_time, system_time_to_pair,
    },
    ops::readdir::{BuildCtxMetadata, DirectoryEntry},
};

const TABLE_SIZE: usize = 250_000;

#[derive(Debug)]
pub enum DbReturn<U> {
    Found { value: U },
    Negative,
    Missing,
}

impl<T> DbReturn<T> {
    pub fn map<U, F>(self, f: F) -> DbReturn<U>
    where
        F: FnOnce(T) -> U,
    {
        match self {
            DbReturn::Found { value } => DbReturn::Found { value: f(value) },
            DbReturn::Missing => DbReturn::Missing,
            DbReturn::Negative => DbReturn::Negative,
        }
    }

    /// ONLY USE FOR TESTS
    pub fn try_unwrap(self) -> T {
        match self {
            DbReturn::Found { value } => value,
            DbReturn::Missing => panic!("Called try_unwwap on a missing entry"),
            DbReturn::Negative => panic!("Called try_unwwap on a negative entry"),
        }
    }

    pub fn is_found(&self) -> bool {
        matches!(self, DbReturn::Found { value: _ })
    }

    pub fn is_miss(&self) -> bool {
        matches!(self, DbReturn::Missing)
    }

    pub fn is_neg(&self) -> bool {
        matches!(self, DbReturn::Negative)
    }
}

impl<U> From<Option<U>> for DbReturn<U> {
    fn from(value: Option<U>) -> DbReturn<U> {
        match value {
            Some(v) => DbReturn::Found { value: v },
            None => DbReturn::Missing,
        }
    }
}

impl<U: Debug> From<DbReturn<U>> for anyhow::Result<U> {
    fn from(value: DbReturn<U>) -> Self {
        match value {
            DbReturn::Found { value } => Ok(value),
            DbReturn::Missing | DbReturn::Negative => {
                bail!("Item not found in DB")
            }
        }
    }
}

// Target Inode
type InoId = u64;
// A parent Inode
type ParId = u64;
// The index of a Target Inode (InoId) in table
type InoIdx = usize;

impl FileAttr {
    pub fn into_storage(self, name: &OsStr, parent_ino: u64) -> InodeData {
        let (atime_secs, atime_nanos) = system_time_to_pair(self.atime);
        let (mtime_secs, mtime_nanos) = system_time_to_pair(self.mtime);

        let metadata: StoreAttr = StoreAttr {
            ino: self.ino as InoId,
            oid: self.oid,
            ino_flag: self.ino_flag,
            size: AtomicU64::new(self.size),
            atime_secs: AtomicI64::new(atime_secs),
            atime_nanos: AtomicI32::new(atime_nanos),
            mtime_secs: AtomicI64::new(mtime_secs),
            mtime_nanos: AtomicI32::new(mtime_nanos),
            kind: self.kind,
            perm: self.perm,
            uid: self.uid,
            gid: self.gid,
        };

        let entry: Entry = Entry {
            name: name.to_os_string(),
            parent_ino,
        };

        InodeData {
            metadata,
            dentry: vec![entry],
        }
    }
}

pub struct StoreAttr {
    pub ino: InoId,
    pub ino_flag: InoFlag,
    pub oid: Oid,
    pub size: AtomicU64,
    pub atime_secs: AtomicI64,
    pub atime_nanos: AtomicI32,
    pub mtime_secs: AtomicI64,
    pub mtime_nanos: AtomicI32,
    pub kind: FileType,
    pub perm: u16,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone)]
pub struct Entry {
    pub name: OsString,
    pub parent_ino: InoId,
}

pub struct InodeData {
    pub metadata: StoreAttr,
    pub dentry: Vec<Entry>,
}

pub struct InodeTable {
    inodes_map: DashMap<InoId, InoIdx>,
    dentry_map: DashMap<(ParId, OsString), InoId>,
    table: Vec<RwLock<Option<InodeData>>>,
    next_idx: AtomicU32,
}

impl InodeTable {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        let mut table = Vec::with_capacity(TABLE_SIZE);
        table.resize_with(TABLE_SIZE, || RwLock::new(None));
        Self {
            inodes_map: DashMap::new(),
            dentry_map: DashMap::new(),
            table,
            next_idx: AtomicU32::new(0),
        }
    }

    // TODO: Make this an option
    fn aloc_idx(&self) -> usize {
        self.next_idx.fetch_add(1, SeqCst) as usize
    }

    pub fn remove_inode(&self, target_ino: u64) {
        if let Some(idx_entry) = self.inodes_map.get(&target_ino) {
            let idx = idx_entry.value();
            *self.table[*idx].write() = None
        }
    }

    /// Returns true if there are any active dentries for this inode
    pub fn is_active(&self, target_ino: u64) -> bool {
        if let Some(idx_entry) = self.inodes_map.get(&target_ino) {
            let idx = idx_entry.value();
            if let Some(ino_entry) = self.table[*idx].read().as_ref() {
                !ino_entry.dentry.is_empty()
            } else {
                false
            }
        } else {
            false
        }
    }

    pub fn insert(&self, node: StorageNode) {
        let ino = node.attr.ino;
        let inodedata = node.attr.into_storage(&node.name, node.parent_ino);

        self.dentry_map.insert((node.parent_ino, node.name), ino);
        let idx = self.aloc_idx();
        *self.table[idx].write() = Some(inodedata);
        self.inodes_map.insert(ino, idx);
    }

    pub fn insert_dentry(&self, dentry: Dentry) {
        let ino = dentry.target_ino;
        if self
            .dentry_map
            .contains_key(&(dentry.parent_ino, dentry.target_name.clone()))
        {
            return;
        };
        if let Some(entry) = self.inodes_map.get(&ino) {
            let index = entry.value();
            if let Some(ino_entry) = self.table[*index].write().as_mut() {
                let entry = Entry {
                    name: dentry.target_name.clone(),
                    parent_ino: dentry.parent_ino,
                };
                ino_entry.dentry.push(entry);
                self.dentry_map
                    .insert((dentry.parent_ino, dentry.target_name), ino);
            }
        }
    }

    pub fn remove_dentry(&self, parent_ino: ParId, target_name: &OsStr, open_handles: bool) {
        if let Some((key, target_ino)) = self
            .dentry_map
            .remove(&(parent_ino, target_name.to_os_string()))
            && let Some(active_dentries) = self.set_inactive(key.0, &key.1)
            && active_dentries == 0
            && !open_handles
        {
            self.remove_inode(target_ino);
            self.inodes_map.remove(&key.0);
        }
    }

    pub fn set_inactive(&self, parent_ino: ParId, target_name: &OsStr) -> Option<usize> {
        let target_ino = self
            .dentry_map
            .get(&(parent_ino, target_name.to_os_string()))
            .map(|e| *e.value())?;
        if let Some(entry) = self.inodes_map.get(&target_ino) {
            let index = entry.value();
            if let Some(map) = self.table[*index].write().as_mut() {
                let len = map.dentry.len();
                if len == 0 {
                    return Some(0);
                } else if len == 1 {
                    map.dentry.pop();
                    self.dentry_map
                        .remove(&(parent_ino, target_name.to_os_string()));
                    return Some(0);
                } else {
                    if let Some(pos) = map
                        .dentry
                        .iter()
                        .position(|e| e.name == target_name && e.parent_ino == parent_ino)
                    {
                        self.dentry_map
                            .remove(&(parent_ino, target_name.to_os_string()));
                        map.dentry.remove(pos);
                    }
                    return Some(map.dentry.len());
                }
            }
            return None;
        }
        None
    }

    pub fn update_size(&self, target_ino: InoId, size: u64) {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let idx = id_entry.value();
            if let Some(entry) = self.table[*idx].read().as_ref() {
                entry.metadata.size.store(size, SeqCst);
            }
        }
    }

    pub fn get_size(&self, target_ino: InoId) -> DbReturn<u64> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let idx = id_entry.value();
            if let Some(entry) = self.table[*idx].read().as_ref() {
                let size = entry.metadata.size.load(Relaxed);
                return DbReturn::Found { value: size };
            }
        }
        DbReturn::Missing
    }

    pub fn set_size(&self, target_ino: InoId, size: u64) {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let idx = id_entry.value();
            if let Some(entry) = self.table[*idx].read().as_ref() {
                entry.metadata.size.store(size, SeqCst);
            }
        }
    }

    pub fn get_all_parents(&self, target_ino: u64) -> DbReturn<Vec<u64>> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let index = id_entry.value();
            if let Some(map) = self.table[*index].read().as_ref() {
                let len = map.dentry.len();
                if len == 0 {
                    return DbReturn::Negative;
                } else if len == 1 {
                    let parent = map.dentry[0].parent_ino;
                    return DbReturn::Found {
                        value: vec![parent],
                    };
                } else {
                    let mut parents = vec![];
                    for e in &map.dentry {
                        parents.push(e.parent_ino);
                    }
                    return DbReturn::Found { value: parents };
                }
            }
        }
        DbReturn::Missing
    }

    pub fn exists_by_name(&self, parent_ino: u64, target_name: &OsStr) -> DbReturn<u64> {
        let Some(target_ino_entry) = self
            .dentry_map
            .get(&(parent_ino, target_name.to_os_string()))
        else {
            return DbReturn::Missing;
        };
        DbReturn::Found {
            value: *target_ino_entry.value(),
        }
    }

    pub fn get_ino_flag(&self, target_ino: u64) -> DbReturn<InoFlag> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let idx = id_entry.value();
            if let Some(entry) = self.table[*idx].read().as_ref() {
                let flag = entry.metadata.ino_flag;
                return DbReturn::Found { value: flag };
            }
        }
        DbReturn::Missing
    }

    pub fn get_oid(&self, target_ino: u64) -> DbReturn<Oid> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let idx = id_entry.value();
            if let Some(entry) = self.table[*idx].read().as_ref() {
                let oid = entry.metadata.oid;
                return DbReturn::Found { value: oid };
            }
        }
        DbReturn::Missing
    }

    pub fn get_kind(&self, target_ino: u64) -> DbReturn<FileType> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let idx = id_entry.value();
            if let Some(entry) = self.table[*idx].read().as_ref() {
                let kind = entry.metadata.kind;
                return DbReturn::Found { value: kind };
            }
        }
        DbReturn::Missing
    }

    pub fn get_name(&self, target_ino: u64) -> DbReturn<OsString> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let index = id_entry.value();
            if let Some(map) = self.table[*index].read().as_ref() {
                if map.dentry.is_empty() {
                    return DbReturn::Negative;
                } else {
                    let name = map.dentry[0].name.clone();
                    return DbReturn::Found { value: name };
                }
            }
        }
        DbReturn::Missing
    }

    pub fn get_dentry(&self, target_ino: u64) -> DbReturn<Dentry> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let index = id_entry.value();
            if let Some(map) = self.table[*index].read().as_ref() {
                if map.dentry.is_empty() {
                    return DbReturn::Negative;
                } else {
                    let entry = &map.dentry[0];
                    let dentry = Dentry {
                        parent_ino: entry.parent_ino,
                        target_ino,
                        target_name: entry.name.clone(),
                    };
                    return DbReturn::Found { value: dentry };
                }
            }
        }
        DbReturn::Missing
    }

    pub fn get_metadata(&self, target_ino: u64) -> DbReturn<FileAttr> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let index = id_entry.value();
            if let Some(map) = self.table[*index].read().as_ref() {
                let store_attr = &map.metadata;
                let atime = pair_to_system_time(
                    store_attr.atime_secs.load(Relaxed),
                    store_attr.atime_nanos.load(Relaxed),
                );
                let mtime = pair_to_system_time(
                    store_attr.mtime_secs.load(Relaxed),
                    store_attr.mtime_nanos.load(Relaxed),
                );

                let attr: FileAttr = FileAttr {
                    ino: target_ino,
                    ino_flag: store_attr.ino_flag,
                    oid: store_attr.oid,
                    size: store_attr.size.load(Relaxed),
                    blocks: 0,
                    atime,
                    mtime,
                    ctime: SystemTime::now(),
                    crtime: SystemTime::now(),
                    kind: store_attr.kind,
                    perm: store_attr.perm,
                    nlink: 1,
                    uid: store_attr.uid,
                    gid: store_attr.gid,
                    rdev: 0,
                    blksize: 0,
                    flags: 0,
                };
                return DbReturn::Found { value: attr };
            }
        }

        DbReturn::Missing
    }

    pub fn update_metadata(&self, attr: &SetFileAttr) -> DbReturn<FileAttr> {
        if let Some(id_entry) = self.inodes_map.get(&attr.ino) {
            let index = id_entry.value();
            if let Some(map) = self.table[*index].read().as_ref() {
                if let Some(size) = attr.size {
                    map.metadata.size.store(size, SeqCst);
                };
                if let Some(atime) = attr.atime {
                    let (atime_secs, atime_nanos) = system_time_to_pair(atime);
                    map.metadata.atime_secs.store(atime_secs, SeqCst);
                    map.metadata.atime_nanos.store(atime_nanos, SeqCst);
                };
                if let Some(mtime) = attr.mtime {
                    let (mtime_secs, mtime_nanos) = system_time_to_pair(mtime);
                    map.metadata.mtime_secs.store(mtime_secs, SeqCst);
                    map.metadata.mtime_nanos.store(mtime_nanos, SeqCst);
                };

                let store_attr = &map.metadata;
                let atime = pair_to_system_time(
                    store_attr.atime_secs.load(Relaxed),
                    store_attr.atime_nanos.load(Relaxed),
                );
                let mtime = pair_to_system_time(
                    store_attr.mtime_secs.load(Relaxed),
                    store_attr.mtime_nanos.load(Relaxed),
                );

                let attr: FileAttr = FileAttr {
                    ino: attr.ino,
                    ino_flag: store_attr.ino_flag,
                    oid: store_attr.oid,
                    size: store_attr.size.load(Relaxed),
                    blocks: 0,
                    atime,
                    mtime,
                    ctime: SystemTime::now(),
                    crtime: SystemTime::now(),
                    kind: store_attr.kind,
                    perm: store_attr.perm,
                    nlink: 1,
                    uid: store_attr.uid,
                    gid: store_attr.gid,
                    rdev: 0,
                    blksize: 0,
                    flags: 0,
                };
                return DbReturn::Found { value: attr };
            }
        }
        DbReturn::Missing
    }

    pub fn update_record(
        &self,
        old_parent: u64,
        old_name: &OsStr,
        node: StorageNode,
    ) -> DbReturn<()> {
        if self.set_inactive(old_parent, old_name).is_none() {
            return DbReturn::Missing;
        };

        {
            self.insert_dentry(Dentry {
                target_ino: node.attr.ino,
                parent_ino: node.parent_ino,
                target_name: node.name.clone(),
            });
        }

        if let Some(id_entry) = self.inodes_map.get(&node.attr.ino) {
            let index = id_entry.value();
            if let Some(map) = self.table[*index].write().as_mut() {
                let inodedata = node.attr.into_storage(&node.name, node.parent_ino);
                map.metadata = inodedata.metadata;
                return DbReturn::Found { value: () };
            }
        }
        DbReturn::Missing
    }

    pub fn build_ctx_metadata(&self, target_ino: u64) -> DbReturn<BuildCtxMetadata> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let index = id_entry.value();
            if let Some(map) = self.table[*index].read().as_ref() {
                let name = if !map.dentry.is_empty() {
                    map.dentry[0].name.clone()
                } else {
                    return DbReturn::Negative;
                };

                let ctx: BuildCtxMetadata = BuildCtxMetadata {
                    kind: map.metadata.kind,
                    oid: map.metadata.oid,
                    name,
                    ino_flag: map.metadata.ino_flag,
                };
                return DbReturn::Found { value: ctx };
            }
        }
        DbReturn::Missing
    }

    pub fn ensure_root(&self, root_name: &OsStr) {
        let mut root_attr: FileAttr = dir_attr(InoFlag::Root).into();
        root_attr.ino = 1;
        let inodedata = root_attr.into_storage(root_name, 1);
        let idx = self.aloc_idx();
        *self.table[idx].write() = Some(inodedata);
        self.inodes_map.insert(1, idx);
    }

    pub fn read_children(&self, parent_ino: u64, build_dir: bool) -> DbReturn<Vec<DirectoryEntry>> {
        if self.inodes_map.get(&parent_ino).is_none() {
            return DbReturn::Missing;
        }

        let mut out: Vec<DirectoryEntry> = vec![];

        for entry in self.dentry_map.iter() {
            let target = if entry.key().0 == parent_ino {
                entry.value()
            } else {
                continue;
            };
            let Some(idx) = self.inodes_map.get(target).map(|e| *e.value()) else {
                continue;
            };

            if let Some(inodedata) = self.table[idx].read().as_ref() {
                if build_dir && inodedata.metadata.ino_flag != InoFlag::InsideBuild {
                    continue;
                };
                let name = if !inodedata.dentry.is_empty() {
                    inodedata.dentry[0].name.clone()
                } else {
                    continue;
                };
                let entry = DirectoryEntry {
                    ino: *target,
                    oid: inodedata.metadata.oid,
                    name,
                    kind: inodedata.metadata.kind,
                };
                out.push(entry);
            };
        }
        DbReturn::Found { value: out }
    }

    pub fn count_children(&self, parent_ino: u64) -> usize {
        if self.inodes_map.get(&parent_ino).is_none() {
            return 0;
        }
        let mut count = 0;
        for entry in self.dentry_map.iter() {
            let target = if entry.key().0 == parent_ino {
                entry.value()
            } else {
                continue;
            };
            let Some(idx) = self.inodes_map.get(target).map(|e| *e.value()) else {
                continue;
            };
            if self.table[idx].read().is_some() {
                count += 1;
            };
        }
        count
    }

    pub fn update_oid_targets(&self, oid: Oid, targets: &[u64]) {
        for ino in targets {
            let Some(idx) = self.inodes_map.get(ino).map(|v| *v) else {
                continue;
            };
            if let Some(inodedata) = self.table[idx].write().as_mut() {
                inodedata.metadata.oid = oid;
            };
        }
    }
}
