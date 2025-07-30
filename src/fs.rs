use std::{collections::HashMap, path::PathBuf, sync::atomic::AtomicU64, time::SystemTime};

use crate::repo::GitRepo;

// Does not get stored on file. Is computed when needed
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileAttr {
    pub inode: u64,
    pub size: u64,
    pub blocks: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
    pub kind: FileType,
    pub perm: u16,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FileType {
    File,
    Directory,
    Symlink,
}

pub struct GitFs {
    data_dir: PathBuf,
    repo: HashMap<String, GitRepo>,
    next_inode: AtomicU64,
    // inode_map: RwLock<HashMap<u64, Node>>
}

impl GitFs {
    pub fn new(data_dir: PathBuf) -> Self {
        Self {
            data_dir,
            repo: HashMap::new(),
            next_inode: AtomicU64::new(1),
        }
    }

    pub fn getattr(&self, _inode: u64) -> std::io::Result<FileAttr> {
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
