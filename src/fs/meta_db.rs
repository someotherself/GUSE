use std::{
    ffi::{OsStr, OsString},
    fmt::Debug,
    sync::atomic::{AtomicU32, AtomicU64},
};

use anyhow::bail;
use dashmap::DashMap;
use git2::Oid;
use parking_lot::RwLock;

use crate::fs::fileattr::{Dentry, FileAttr, FileType, InoFlag, SetFileAttr, StorageNode, system_time_to_pair};

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
    pub fn into_metadata(self, name: &OsStr, parent_ino: u64) -> InodeData {
        let (atime_secs, atime_nanos) = system_time_to_pair(self.atime);
        let (mtime_secs, mtime_nanos) = system_time_to_pair(self.mtime);

        let metadata: StoreAttr = StoreAttr {
            ino: self.ino as InoId,
            oid: self.oid,
            ino_flag: self.ino_flag,
            size: AtomicU64::new(self.size),
            atime_secs,
            atime_nanos,
            mtime_secs,
            mtime_nanos,
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

struct StoreAttr {
    pub ino: InoId,
    pub ino_flag: InoFlag,
    pub oid: Oid,
    pub size: AtomicU64,
    pub atime_secs: i64,
    pub atime_nanos: i32,
    pub mtime_secs: i64,
    pub mtime_nanos: i32,
    pub kind: FileType,
    pub perm: u16,
    pub uid: u32,
    pub gid: u32,
}

#[derive(Clone)]
struct Entry {
    pub name: OsString,
    pub parent_ino: InoId,
}

struct InodeData {
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

    fn aloc_idx(&self) -> usize {
        self.next_idx
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst) as usize
    }

    pub fn insert(&self, node: StorageNode) {
        let ino = node.attr.ino;
        let idx = self.aloc_idx();
        let inodedata = node.attr.into_metadata(&node.name, node.parent_ino);

        self.inodes_map.insert(ino, idx);
        self.dentry_map.insert((node.parent_ino, node.name), ino);
        *self.table[idx].write() = Some(inodedata)
    }

    pub fn remove_dentry(&self, parent_ino: ParId, target_name: &OsStr) {
        if let Some((key, _)) = self
            .dentry_map
            .remove(&(parent_ino, target_name.to_os_string()))
        {
            // TODO: Check if no open handles
            self.set_inactive(key.0, &key.1);
        }
    }

    pub fn set_inactive(&self, parent_ino: ParId, target_name: &OsStr) {
        let Some(target_ino_entry) = self
            .dentry_map
            .get(&(parent_ino, target_name.to_os_string()))
        else {
            return;
        };
        if let Some(entry) = self.inodes_map.get(&target_ino_entry.value()) {
            let index = entry.value();
            if let Some(map) = self.table[*index].write().as_mut() {
                let len = map.dentry.len();
                if len == 0 {
                    return;
                } else if len == 1 {
                    map.dentry.pop();
                } else {
                    if let Some(pos) = map
                        .dentry
                        .iter()
                        .position(|e| e.name == target_name && e.parent_ino == parent_ino)
                    {
                        map.dentry.remove(pos);
                    }
                }
            }
        }
    }

    pub fn update_size(&self, target_ino: InoId, size: u64) {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let idx = id_entry.value();
            if let Some(entry) = self.table[*idx].read().as_ref() {
                entry
                    .metadata
                    .size
                    .store(size, std::sync::atomic::Ordering::SeqCst);
            }
        }
    }

    pub fn get_size(&self, target_ino: InoId) -> DbReturn<u64> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let idx = id_entry.value();
            if let Some(entry) = self.table[*idx].read().as_ref() {
                let size = entry
                    .metadata
                    .size
                    .load(std::sync::atomic::Ordering::Relaxed);
                return DbReturn::Found { value: size };
            }
        }
        return DbReturn::Missing;
    }

    pub fn get_all_parents(&self, target_ino: u64) -> DbReturn<Vec<u64>> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let index = id_entry.value();
            if let Some(map) = self.table[*index].write().as_ref() {
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
        return DbReturn::Missing;
    }

    pub fn get_oid(&self, target_ino: u64) -> DbReturn<Oid> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let idx = id_entry.value();
            if let Some(entry) = self.table[*idx].read().as_ref() {
                let oid = entry.metadata.oid;
                return DbReturn::Found { value: oid };
            }
        }
        return DbReturn::Missing;
    }

    pub fn get_kind(&self, target_ino: u64) -> DbReturn<FileType> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let idx = id_entry.value();
            if let Some(entry) = self.table[*idx].read().as_ref() {
                let kind = entry.metadata.kind;
                return DbReturn::Found { value: kind };
            }
        }
        return DbReturn::Missing;
    }

    pub fn get_name(&self, target_ino: u64) -> DbReturn<OsString> {
        if let Some(id_entry) = self.inodes_map.get(&target_ino) {
            let index = id_entry.value();
            if let Some(map) = self.table[*index].write().as_ref() {
                let len = map.dentry.len();
                if len == 0 {
                    return DbReturn::Negative;
                } else if len > 0 {
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
            if let Some(map) = self.table[*index].write().as_ref() {
                let len = map.dentry.len();
                if len == 0 {
                    return DbReturn::Negative;
                } else if len > 0 {
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

    pub fn update_metadata(&self, attr: &SetFileAttr) {
        
    }


}
