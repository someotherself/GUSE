use std::time::SystemTime;

use anyhow::bail;
use git2::{ObjectType, Oid};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileAttr {
    // Inode in the fuse fs
    pub ino: u64,
    pub ino_mask: InoMask,
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
    pub git_mode: u32,
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
    pub git_mode: u32,
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
    pub fn from_filemode(mode: ObjectType) -> anyhow::Result<FileType> {
        match mode {
            ObjectType::Blob => Ok(FileType::RegularFile),
            ObjectType::Tree => Ok(FileType::Directory),
            ObjectType::Commit => Ok(FileType::Directory),
            _ => Ok(FileType::RegularFile),
        }
    }
}

pub const fn dir_attr(ino_mask: InoMask) -> CreateFileAttr {
    CreateFileAttr {
        kind: FileType::Directory,
        ino_mask,
        perm: 0o775,
        uid: 0,
        mode: libc::S_IFDIR,
        gid: 0,
        rdev: 0,
        flags: 0,
    }
}

pub const fn file_attr(ino_mask: InoMask) -> CreateFileAttr {
    CreateFileAttr {
        kind: FileType::RegularFile,
        ino_mask,
        perm: 0o655,
        uid: 0,
        mode: libc::S_IFREG,
        gid: 0,
        rdev: 0,
        flags: 0,
    }
}

#[derive(Debug, Clone)]
pub struct CreateFileAttr {
    pub kind: FileType,
    pub ino_mask: InoMask,
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
            ino: 0,
            ino_mask: value.ino_mask,
            oid: Oid::zero(),
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: value.kind,
            perm: value.perm,
            git_mode: value.mode,
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

fn build_attr_file(ino: u64, ino_mask: InoMask, st_mode: u32) -> FileAttr {
    let now = SystemTime::now();
    FileAttr {
        ino,
        ino_mask,
        oid: Oid::zero(),
        size: 0,
        blocks: 0,
        atime: now,
        mtime: now,
        ctime: now,
        crtime: now,
        kind: FileType::RegularFile,
        perm: 0o655,
        git_mode: st_mode,
        nlink: 1,
        uid: unsafe { libc::getuid() } as u32,
        gid: unsafe { libc::getgid() } as u32,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

pub fn build_attr_dir(ino: u64, ino_mask: InoMask, st_mode: u32) -> FileAttr {
    let now = SystemTime::now();
    FileAttr {
        ino,
        ino_mask,
        oid: Oid::zero(),
        size: 0,
        blocks: 0,
        atime: now,
        mtime: now,
        ctime: now,
        crtime: now,
        kind: FileType::Directory,
        perm: 0o775,
        git_mode: st_mode,
        nlink: 2,
        uid: unsafe { libc::getuid() } as u32,
        gid: unsafe { libc::getgid() } as u32,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

/// Used for inodes table in meta_db
pub struct StoredAttr {
    pub ino: u64,
    pub ino_mask: InoMask,
    pub oid: Oid,
    pub size: u64,
    pub git_mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub flags: u32,
}

/// Used for dentries table in meta_db
struct DirEntries {
    pub target_ino: u64, // ino from StoredAttr
    pub parent_ino: u64,
    pub name: String,
}

/// Used for passing to Gitfs::write_inodes_to_db()
pub struct StorageNode {
    pub parent_ino: u64,
    pub name: String,
    pub attr: StoredAttr,
}

impl From<FileAttr> for StoredAttr {
    fn from(value: FileAttr) -> Self {
        Self {
            ino: value.ino,
            ino_mask: value.ino_mask,
            oid: value.oid,
            size: value.size,
            git_mode: value.git_mode,
            uid: value.uid,
            gid: value.gid,
            rdev: value.rdev,
            flags: value.flags,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u64)]
pub enum InoMask {
    Root = 1 << 0,
    RepoRoot = 1 << 1,
    LiveRoot = 1 << 2,
    BuildRoot = 1 << 3,
    MonthFolder = 1 << 4,
    SnapFolder = 1 << 5,
    InsideSnap = 1 << 6,
    InsideBuild = 1 << 7,
    InsideLive = 1 << 8,
    VirtualFile = 1 << 9,
}

impl TryFrom<u64> for InoMask {
    type Error = anyhow::Error;
    fn try_from(v: u64) -> Result<Self, Self::Error> {
        match v {
            x if x == InoMask::RepoRoot as u64 => Ok(InoMask::RepoRoot),
            x if x == InoMask::LiveRoot as u64 => Ok(InoMask::RepoRoot),
            x if x == InoMask::BuildRoot as u64 => Ok(InoMask::RepoRoot),
            x if x == InoMask::MonthFolder as u64 => Ok(InoMask::MonthFolder),
            x if x == InoMask::SnapFolder as u64 => Ok(InoMask::SnapFolder),
            x if x == InoMask::InsideSnap as u64 => Ok(InoMask::InsideSnap),
            x if x == InoMask::InsideBuild as u64 => Ok(InoMask::InsideBuild),
            x if x == InoMask::InsideLive as u64 => Ok(InoMask::InsideLive),
            _ => bail!("Unknown InoMask valueL {v:#x}"),
        }
    }
}

impl From<InoMask> for u64 {
    fn from(v: InoMask) -> u64 {
        v as u64
    }
}

pub fn try_into_filetype(mode: u64) -> Option<FileType> {
    let m = u32::try_from(mode).ok()?;
    match m {
        0o040000 => Some(FileType::Directory),
        0o100644 | 0o100755 | 0o120000 | 0o160000 => Some(FileType::RegularFile),
        _ => {
            let typ = m & 0o170000;
            match typ {
                0o040000 => Some(FileType::Directory),
                0o120000 | 0o160000 | 0o100000 => Some(FileType::RegularFile),
                _ => None,
            }
        }
    }
}
