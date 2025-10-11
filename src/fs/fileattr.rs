use std::{
    ffi::OsString,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::bail;
use git2::{ObjectType, Oid};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileAttr {
    pub ino: u64,
    pub ino_flag: InoFlag,
    pub oid: Oid,
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Dentry {}

#[derive(Clone, Debug)]
pub struct ObjectAttr {
    pub name: OsString,
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

pub const fn dir_attr(ino_flag: InoFlag) -> CreateFileAttr {
    CreateFileAttr {
        kind: FileType::Directory,
        ino_flag,
        perm: 0o775,
        uid: 0,
        mode: libc::S_IFDIR,
        gid: 0,
        rdev: 0,
        flags: 0,
    }
}

pub const fn file_attr(ino_flag: InoFlag) -> CreateFileAttr {
    CreateFileAttr {
        kind: FileType::RegularFile,
        ino_flag,
        perm: 0o775,
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
    pub ino_flag: InoFlag,
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
            ino_flag: value.ino_flag,
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

/// Used for inodes table in meta_db
pub struct StoredAttr {
    pub ino: u64,
    pub ino_flag: InoFlag,
    pub oid: Oid,
    pub size: u64,
    pub git_mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime_secs: i64,
    pub atime_nsecs: i32,
    pub mtime_secs: i64,
    pub mtime_nsecs: i32,
    pub ctime_secs: i64,
    pub ctime_nsecs: i32,
    pub rdev: u32,
    pub flags: u32,
}

pub fn system_time_to_pair(t: SystemTime) -> (i64, i32) {
    match t.duration_since(UNIX_EPOCH) {
        Ok(dur) => (dur.as_secs() as i64, dur.subsec_nanos() as i32),
        Err(e) => {
            let d = e.duration();
            (-(d.as_secs() as i64), -(d.subsec_nanos() as i32))
        }
    }
}

pub fn pair_to_system_time(secs: i64, nsecs: i32) -> SystemTime {
    use std::time::{Duration, UNIX_EPOCH};
    if secs >= 0 {
        UNIX_EPOCH + Duration::new(secs as u64, nsecs as u32)
    } else {
        UNIX_EPOCH - Duration::new((-secs) as u64, nsecs as u32)
    }
}

pub struct SetStoredAttr {
    pub ino: u64,
    pub size: Option<u64>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub atime_secs: Option<i64>,
    pub atime_nsecs: Option<i32>,
    pub mtime_secs: Option<i64>,
    pub mtime_nsecs: Option<i32>,
    pub flags: Option<u32>,
}

/// Used for passing to Gitfs::write_inodes_to_db()
pub struct StorageNode {
    pub parent_ino: u64,
    pub name: OsString,
    pub attr: StoredAttr,
}

impl From<FileAttr> for StoredAttr {
    fn from(value: FileAttr) -> Self {
        let (atime_secs, atime_nsecs) = system_time_to_pair(value.atime);
        let (mtime_secs, mtime_nsecs) = system_time_to_pair(value.mtime);
        let (ctime_secs, ctime_nsecs) = system_time_to_pair(value.ctime);

        Self {
            ino: value.ino,
            ino_flag: value.ino_flag,
            oid: value.oid,
            size: value.size,
            git_mode: value.git_mode,
            uid: value.uid,
            gid: value.gid,
            atime_secs,
            atime_nsecs,
            mtime_secs,
            mtime_nsecs,
            ctime_secs,
            ctime_nsecs,
            rdev: value.rdev,
            flags: value.flags,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u64)]
pub enum InoFlag {
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
    DotGitRoot = 1 << 10,
    InsideDotGit = 1 << 11,
    HeadFile = 1 << 12,
}
impl InoFlag {
    pub const fn as_str(&self) -> &'static str {
        match *self {
            InoFlag::Root => "Root",
            InoFlag::RepoRoot => "RepoRoot",
            InoFlag::LiveRoot => "LiveRoot",
            InoFlag::BuildRoot => "BuildRoot",
            InoFlag::MonthFolder => "MonthFolder",
            InoFlag::SnapFolder => "SnapFolder",
            InoFlag::InsideSnap => "InsideSnap",
            InoFlag::InsideBuild => "InsideBuild",
            InoFlag::InsideLive => "InsideLive",
            InoFlag::VirtualFile => "VirtualFile",
            InoFlag::DotGitRoot => "DotGitRoot",
            InoFlag::InsideDotGit => "InsideDotGit",
            InoFlag::HeadFile => "HeadFile",
        }
    }
}

impl std::fmt::Display for InoFlag {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<u64> for InoFlag {
    type Error = anyhow::Error;
    fn try_from(v: u64) -> Result<Self, Self::Error> {
        match v {
            x if x == InoFlag::RepoRoot as u64 => Ok(InoFlag::RepoRoot),
            x if x == InoFlag::LiveRoot as u64 => Ok(InoFlag::LiveRoot),
            x if x == InoFlag::BuildRoot as u64 => Ok(InoFlag::BuildRoot),
            x if x == InoFlag::MonthFolder as u64 => Ok(InoFlag::MonthFolder),
            x if x == InoFlag::SnapFolder as u64 => Ok(InoFlag::SnapFolder),
            x if x == InoFlag::InsideSnap as u64 => Ok(InoFlag::InsideSnap),
            x if x == InoFlag::InsideBuild as u64 => Ok(InoFlag::InsideBuild),
            x if x == InoFlag::InsideLive as u64 => Ok(InoFlag::InsideLive),
            x if x == InoFlag::DotGitRoot as u64 => Ok(InoFlag::DotGitRoot),
            x if x == InoFlag::InsideDotGit as u64 => Ok(InoFlag::InsideDotGit),
            x if x == InoFlag::HeadFile as u64 => Ok(InoFlag::HeadFile),
            _ => bail!("Unknown InoFlag valueL {v:#x}"),
        }
    }
}

impl From<InoFlag> for u64 {
    fn from(v: InoFlag) -> u64 {
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
