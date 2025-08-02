use std::hash::Hash;
use std::rc::Rc;
use std::sync::Mutex;
use std::time::{Duration, UNIX_EPOCH};
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

use anyhow::{Context, Ok, anyhow, bail};
use git2::{ObjectType, Oid};
use tracing::instrument;

use crate::repo::GitRepo;

// Storage for the inode mapping, metadata etc. Sits inside repos/repo_/
pub(crate) const META_STORE: &str = "fs_meta";
pub(crate) const ROOT_INODE: u64 = 1;

const REPO_SHIFT: u8 = 48;

// Disk structure
// MOUNT_POINT/
// repos_dir/repo1
//        ├── repository_name1/
//        └── fs_meta/fs_meta.db
// repos_dir/repo2
//        ├── repository_name2/
//        └── fs_meta/fs_meta.db

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileAttr {
    // Inode in the fuse fs
    pub inode: u64,
    // SHA-1 in git
    pub oid: Oid,
    // Blob size
    pub size: u64,
    pub blocks: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
    pub kind: FileType,
    // mode bits
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct ObjectAttr {
    pub oid: Oid,
    pub kind: git2::ObjectType,
    pub filemode: i32,
    pub size: u64,
    pub commit_time: git2::Time,
}

#[derive(Clone, Copy, Debug)]
pub struct SetFileAttr {
    pub size: Option<u64>,
    pub atime: Option<SystemTime>,
    pub mtime: Option<SystemTime>,
    pub ctime: Option<SystemTime>,
    pub crtime: Option<SystemTime>,
    pub perm: Option<u16>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub rdev: Option<u32>,
    pub flags: Option<u32>,
}

impl SetFileAttr {
    #[must_use]
    pub const fn with_size(mut self, size: u64) -> Self {
        self.size = Some(size);
        self
    }

    #[must_use]
    pub const fn with_atime(mut self, atime: SystemTime) -> Self {
        self.atime = Some(atime);
        self
    }

    #[must_use]
    pub const fn with_mtime(mut self, mtime: SystemTime) -> Self {
        self.mtime = Some(mtime);
        self
    }

    #[must_use]
    pub const fn with_ctime(mut self, ctime: SystemTime) -> Self {
        self.ctime = Some(ctime);
        self
    }

    #[must_use]
    pub const fn with_crtime(mut self, crtime: SystemTime) -> Self {
        self.crtime = Some(crtime);
        self
    }

    #[must_use]
    pub const fn with_perm(mut self, perm: u16) -> Self {
        self.perm = Some(perm);
        self
    }

    #[must_use]
    pub const fn with_uid(mut self, uid: u32) -> Self {
        self.uid = Some(uid);
        self
    }

    #[must_use]
    pub const fn with_gid(mut self, gid: u32) -> Self {
        self.gid = Some(gid);
        self
    }

    #[must_use]
    pub const fn with_rdev(mut self, rdev: u32) -> Self {
        self.rdev = Some(rdev);
        self
    }

    #[must_use]
    pub const fn with_flags(mut self, flags: u32) -> Self {
        self.rdev = Some(flags);
        self
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FileType {
    File,
    Directory,
    Symlink,
}

impl FileType {
    pub fn from_filemode(mode: ObjectType) -> anyhow::Result<FileType> {
        match mode {
            ObjectType::Blob => Ok(FileType::File),
            ObjectType::Tree => Ok(FileType::Directory),
            ObjectType::Tag => Ok(FileType::Symlink),
            _ => bail!("Invalid file type {:?}", mode),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateFileAttr {
    pub kind: FileType,
    pub perm: u16,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub flags: u32,
}

impl From<CreateFileAttr> for FileAttr {
    fn from(value: CreateFileAttr) -> Self {
        let now = SystemTime::now();
        Self {
            inode: 0,
            oid: Oid::zero(),
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: value.kind,
            perm: value.perm,
            nlink: if value.kind == FileType::Directory {
                2
            } else {
                1
            },
            uid: value.uid,
            gid: value.gid,
            rdev: value.rdev,
            blksize: 0,
            flags: value.flags,
        }
    }
}

pub struct DirectoryEntry {
    pub inode: u64,
    // The git Oid (SHA-1)
    pub oid: Oid,
    // The real filename
    pub name: String,
    // File (Blob), Dir (Tree), or Symlink
    pub kind: FileType,
    // Mode (permissions)
    pub filemode: i32,
}

pub struct DirectoryEntryIterator(VecDeque<DirectoryEntry>);

impl Iterator for DirectoryEntryIterator {
    type Item = DirectoryEntry;

    #[instrument(name = "DirectoryEntryIterator::next", skip(self))]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front()
    }
}

pub struct DirectoryEntryPlus {
    // The the attributes in the normal struct
    pub entry: DirectoryEntry,
    // Plus the file attributes
    pub attr: FileAttr,
}

pub struct DirectoryEntryPlusIterator(VecDeque<DirectoryEntryPlus>);

impl Iterator for DirectoryEntryPlusIterator {
    type Item = DirectoryEntryPlus;

    #[instrument(name = "DirectoryEntryPlusIterator::next", skip(self))]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front()
    }
}

struct InodeAllocator {
    next: AtomicU64,
    map: Mutex<HashMap<Oid, u64>>,
}

impl InodeAllocator {
    fn get_or_alloc(&self, oid: &Oid) -> u64 {
        let mut map = self.map.lock().unwrap();
        *map.entry(*oid)
            .or_insert_with(|| self.next.fetch_add(1, Ordering::SeqCst))
    }
}

pub struct GitFs {
    repos_dir: PathBuf,
    repos_list: HashMap<u16, GitRepo>,
    next_inode: HashMap<u16, AtomicU64>,
    read_only: bool,
    // inode_map: RwLock<HashMap<u64, Node>>
}

impl GitFs {
    pub fn new(repos_dir: PathBuf, read_only: bool) -> anyhow::Result<Rc<Self>> {
        let fs = Self {
            repos_dir,
            repos_list: HashMap::new(),
            read_only,
            next_inode: HashMap::new(),
        };
        fs.ensure_base_dirs_exist()
            .context("Initializing base directories")?;
        Ok(Rc::new(fs).clone())
    }

    fn next_inode(&self, parent: u64) -> anyhow::Result<u64> {
        let repo_id = (parent >> REPO_SHIFT) as u16;
        let inode = self
            .next_inode
            .get(&repo_id)
            .ok_or_else(|| anyhow!("No repo found for this ID"))?
            .fetch_add(1, Ordering::SeqCst);
        Ok(inode)
    }

    fn get_repo(&self, inode: u64) -> anyhow::Result<&GitRepo> {
        let repo_id = (inode >> REPO_SHIFT) as u16;
        let repo = self
            .repos_list
            .get(&repo_id)
            .ok_or_else(|| anyhow!("No repo found for this ID"))?;
        Ok(repo)
    }

    fn pack_inode(repo_id: u16, sub_ino: u64) -> u64 {
        ((repo_id as u64) << REPO_SHIFT) | (sub_ino & ((1 << REPO_SHIFT) - 1))
    }

    fn ensure_base_dirs_exist(&self) -> anyhow::Result<()> {
        if !self.repos_dir.exists() {
            let mut attr: FileAttr = CreateFileAttr {
                kind: FileType::Directory,
                perm: 0o755,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
            }
            .into();
            unsafe {
                attr.uid = libc::getuid();
                attr.gid = libc::getgid();
            }

            // self.write_inode_to_db(&attr)?; // TODO
            let repos_dir = &self.repos_dir; // TODO change to attr.ino
            std::fs::create_dir_all(repos_dir)
                .with_context(|| format!("Failed to create repos dir {repos_dir:?}"))?;

            // TODO Insert directory entry
        }
        Ok(())
    }

    fn exists(&self, _inode: u64) -> bool {
        todo!()
    }

    pub fn get_ino_from_db(&self, _parent: u64, _name: &str) -> anyhow::Result<u64> {
        todo!()
    }

    pub fn get_path_from_db(&self, _inode: u64) -> anyhow::Result<PathBuf> {
        todo!()
    }

    pub fn write_inode_to_db(&self, _attr: &FileAttr) -> anyhow::Result<()> {
        todo!()
    }

    pub fn getattr(&self, inode: u64) -> anyhow::Result<FileAttr> {
        // Check inode exists
        if !self.exists(inode) {
            bail!("Inode not found!")
        }

        // Get ObjectAttr from git2
        let repo = self.get_repo(inode)?;
        let path = self.get_path_from_db(inode)?;

        let git_attr = repo.getattr(path)?;

        // Compute the rest of the attributes
        let blocks = git_attr.size.div_ceil(512);

        // Compute atime and mtime from commit_time
        let commit_secs = git_attr.commit_time.seconds() as u64;
        let time = UNIX_EPOCH + Duration::from_secs(commit_secs);

        let kind = match git_attr.filemode & 0o170000 {
            0o040000 => FileType::Directory,
            0o120000 => FileType::Symlink,
            _ => FileType::File,
        };
        let perm = (git_attr.filemode & 0o777) as u16;

        let nlink = if kind == FileType::Directory { 2 } else { 1 };

        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let rdev = 0;
        let blksize = 4096;
        let flags = 0;

        Ok(FileAttr {
            inode,
            oid: git_attr.oid,
            size: git_attr.size,
            blocks,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind,
            perm,
            nlink,
            uid,
            gid,
            rdev,
            blksize,
            flags,
        })
    }

    pub fn setattr(&self, _inode: u64, _attr: SetFileAttr) -> std::io::Result<FileAttr> {
        todo!()
    }

    pub fn find_by_name(&self, _parent: u64, _name: &str) -> anyhow::Result<Option<FileAttr>> {
        // Check parent exists

        // Check parent is a tree

        // getattr()
        todo!()
    }
}

// lookup               -> git ls-tree
// getattr              -> git cat-file -p <object>
// readdir              -> git ls-tree <tree>
// readdirplus          -> git ls-tree + git catfile -p <object>
// open                 -> no-op
// read                 -> git cat-file --batch / git cat-file -p <blob>
// create               -> git hash-object --stdin -w + git update-index --add <path>
// write                -> buffer in memory then on flush: git hash-object --stdin -w
// flush / release      -> git update-index --add <path>
// unlink               -> git update-index --remove <path>
// mkdir                -> update in mem tree, commit w/: git write-tree
// rmdir                -> update in mem tree, commit w/: git write-tree
// rename               -> git mv <old> <new> or idx update + working tree rename
// statfs               -> fuse3::statfs::Statfs or derive from git repo
