use std::time::SystemTime;

use git2::{ObjectType, Oid};

use crate::fs::FsResult;

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
    pub perm: u16,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}

#[derive(Clone, Debug)]
pub struct ObjectAttr {
    pub name: String,
    pub oid: Oid,
    pub kind: git2::ObjectType,
    pub filemode: u32,
    pub size: u64,
    pub commit_time: git2::Time,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
}

impl FileType {
    pub fn from_filemode(mode: ObjectType) -> FsResult<FileType> {
        match mode {
            ObjectType::Blob => Ok(FileType::RegularFile),
            ObjectType::Tree => Ok(FileType::Directory),
            ObjectType::Commit => Ok(FileType::Directory),
            _ => Ok(FileType::RegularFile),
        }
    }
}

impl From<FileAttr> for TimesFileAttr {
    fn from(value: FileAttr) -> Self {
        Self {
            atime: value.atime,
            mtime: value.mtime,
            ctime: value.ctime,
            crtime: value.crtime,
            size: value.size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateFileAttr {
    pub kind: FileType,
    pub perm: u16,
    pub mode: u32,
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
            mode: value.mode,
            nlink: if value.kind == FileType::Directory {
                2
            } else {
                1
            },
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: value.rdev,
            blksize: 0,
            flags: value.flags,
        }
    }
}

#[derive(Clone)]
pub struct TimesFileAttr {
    pub size: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
}

fn build_attr_file(inode: u64, st_mode: u32) -> FileAttr {
    let now = SystemTime::now();
    FileAttr {
        inode,
        oid: Oid::zero(),
        size: 0,
        blocks: 0,
        atime: now,
        mtime: now,
        ctime: now,
        crtime: now,
        kind: FileType::RegularFile,
        perm: 0o655,
        mode: st_mode,
        nlink: 2,
        uid: unsafe { libc::getuid() } as u32,
        gid: unsafe { libc::getgid() } as u32,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

pub fn build_attr_dir(inode: u64, st_mode: u32) -> FileAttr {
    let now = SystemTime::now();
    FileAttr {
        inode,
        oid: Oid::zero(),
        size: 0,
        blocks: 0,
        atime: now,
        mtime: now,
        ctime: now,
        crtime: now,
        kind: FileType::Directory,
        perm: 0o775,
        mode: st_mode,
        nlink: 2,
        uid: unsafe { libc::getuid() } as u32,
        gid: unsafe { libc::getgid() } as u32,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}
