#![allow(unused_imports, unused_variables)]

use anyhow::{Context, anyhow};
use fuser::{
    BackgroundSession, MountOption, ReplyAttr, ReplyData, ReplyEntry, ReplyOpen, ReplyWrite,
    TimeOrNow, consts,
};
use git2::Oid;
use libc::{EACCES, EIO, EISDIR, ENOENT, ENOTDIR, ENOTTY, O_DIRECTORY};
use ratatui::crossterm::style::Stylize;
use tracing::{Level, Span, info, instrument};
use tracing::{debug, error, trace, warn};

use std::ffi::{OsStr, OsString};
use std::io::{BufRead, BufReader, ErrorKind, Read, Write};
use std::iter::Skip;
use std::os::linux::fs;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread;
use std::time::{Duration, SystemTime};
use std::{num::NonZeroU32, path::PathBuf};

use crate::fs::fileattr::{CreateFileAttr, FileAttr, FileType, InoFlag, SetStoredAttr, dir_attr};
use crate::fs::ops::readdir::{DirectoryEntry, DirectoryEntryPlus};
use crate::fs::{GitFs, REPO_SHIFT, ROOT_INO, repo};
use crate::internals::sock::{socket_path, start_control_server};

const TTL: Duration = Duration::from_secs(1);

pub struct MountPoint {
    mountpoint: PathBuf,
    repos_dir: PathBuf,
    read_only: bool,
    allow_root: bool,
    allow_other: bool,
}

impl MountPoint {
    pub fn new(
        mountpoint: PathBuf,
        repos_dir: PathBuf,
        read_only: bool,
        allow_root: bool,
        allow_other: bool,
    ) -> Self {
        Self {
            mountpoint,
            repos_dir,
            read_only,
            allow_root,
            allow_other,
        }
    }
}

pub fn mount_fuse(opts: MountPoint) -> anyhow::Result<()> {
    let MountPoint {
        mountpoint,
        repos_dir,
        read_only,
        allow_root,
        allow_other,
    } = opts;

    if !mountpoint.exists() {
        std::fs::create_dir(&mountpoint)?;
    }

    let mut options = vec![
        MountOption::FSName("GUSE".to_string()),
        MountOption::AutoUnmount,
        MountOption::DefaultPermissions,
    ];
    if read_only {
        options.push(MountOption::RO);
    }
    if allow_other {
        fuse_allow_other_enabled()?;
        options.push(MountOption::AllowOther);
    }
    if allow_root {
        options.push(MountOption::AllowRoot);
    }

    let notif = Arc::new(OnceLock::new());
    let fs = GitFsAdapter::new(repos_dir.clone(), opts.read_only, notif.clone())?;

    let fs_arc = Arc::new(fs.clone());

    let socket_path = socket_path()?;

    start_control_server(
        fs_arc.clone(),
        socket_path,
        mountpoint.to_string_lossy().into(),
    )?;

    let mut session = fuser::Session::new(fs, mountpoint, &options)?;
    let notifier = session.notifier();
    let _ = notif.set(notifier);

    session.run()?;
    Ok(())
}

fn fuse_allow_other_enabled() -> std::io::Result<bool> {
    let file: std::fs::File = std::fs::File::open("/etc/fuse.conf")?;
    for line in BufReader::new(file).lines() {
        if line?.trim_start().starts_with("user_allow_other") {
            return Ok(true);
        }
    }
    Ok(false)
}

pub enum InvalMsg {
    Entry { parent: u64, name: OsString },
    Inode { ino: u64, off: i64, len: i64 },
}

#[derive(Clone)]
pub struct GitFsAdapter {
    inner: Arc<Mutex<GitFs>>,
}

impl GitFsAdapter {
    fn new(
        repos_dir: PathBuf,
        read_only: bool,
        notifier: Arc<OnceLock<fuser::Notifier>>,
    ) -> anyhow::Result<Self> {
        let fs = GitFs::new(repos_dir, read_only, notifier)?;
        Ok(GitFsAdapter { inner: fs })
    }

    pub fn getfs(&self) -> Arc<Mutex<GitFs>> {
        self.inner.clone()
    }
}

impl fuser::Filesystem for GitFsAdapter {
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        config
            .add_capabilities(consts::FUSE_WRITEBACK_CACHE)
            .unwrap();
        config.set_max_readahead(128 * 1024).unwrap();
        Ok(())
    }

    fn destroy(&mut self) {}

    fn lookup(&mut self, req: &fuser::Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        let attr_result = fs.getattr(parent);
        match attr_result {
            Ok(parent_attrs) => {
                if name == OsStr::new(".") {
                    reply.entry(&TTL, &parent_attrs.into(), 0);
                    return;
                }

                if name == OsStr::new("..") {
                    let parent_ino = if parent == ROOT_INO {
                        ROOT_INO
                    } else {
                        fs.get_dir_parent(parent).unwrap_or(ROOT_INO)
                    };
                    let Ok(parent_attr) = fs.getattr(parent_ino) else {
                        return reply.error(libc::ENOENT);
                    };
                    return reply.entry(&TTL, &parent_attr.into(), 0);
                }
            }
            Err(e) => {
                error!(e = %e, "Lookup parent inode");
                reply.error(ENOENT);
                return;
            }
        };

        match fs.lookup(parent, name.to_str().unwrap()) {
            Ok(Some(attr)) => {
                let ino = attr.ino;
                reply.entry(&TTL, &attr.into(), 0)
            }
            Ok(None) => {
                // The name is not found under this parent
                reply.error(ENOENT);
            }
            Err(e) => {
                // Other internal error
                error!(e = %e, "Finding lookup attribute {}{}:", parent, name.display());
                reply.error(EIO);
            }
        };
    }

    fn ioctl(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        flags: u32,
        cmd: u32,
        in_data: &[u8],
        out_size: u32,
        reply: fuser::ReplyIoctl,
    ) {
        reply.error(ENOTTY);
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e, "Getting attribute for {}", ino);
                return reply.error(EIO);
            }
        };

        match fs.getattr(ino) {
            Err(err) => {
                error!("getattr({}) failed: {:?}", ino, err);
                reply.error(ENOENT);
            }
            Ok(attr) => reply.attr(&TTL, &attr.into()),
        }
    }

    fn mknod(
        &mut self,
        _req: &fuser::Request<'_>,
        _parent: u64,
        _name: &OsStr,
        mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        let ftype = mode & libc::S_IFMT;
        match ftype {
            libc::S_IFREG => reply.error(libc::EOPNOTSUPP),
            libc::S_IFIFO | libc::S_IFCHR | libc::S_IFBLK | libc::S_IFSOCK => {
                reply.error(libc::EPERM)
            }
            _ => reply.error(libc::EINVAL),
        }
    }

    fn mkdir(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        let fs_arc = self.getfs();
        let mut fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        match fs.mkdir(parent, name) {
            Ok(attr) => reply.entry(&TTL, &attr.into(), 0),
            Err(e) => {
                error!(?e);
                reply.error(errno_from_anyhow(&e))
            }
        }
    }

    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        let res = fs.unlink(parent, name);
        match res {
            Ok(_) => reply.ok(),
            Err(e) => {
                error!(e = %e);
                reply.error(errno_from_anyhow(&e))
            }
        }
    }

    fn rmdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        let res = fs.rmdir(parent, name);
        match res {
            Ok(_) => reply.ok(),
            Err(e) => {
                error!(e = %e);
                reply.error(errno_from_anyhow(&e))
            }
        }
    }

    fn rename(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
        reply: fuser::ReplyEmpty,
    ) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        let res = fs.rename(parent, name, newparent, newname);
        match res {
            Ok(_) => reply.ok(),
            Err(e) => {
                error!(e = %e);
                reply.error(ENOENT)
            }
        }
    }

    fn open(&mut self, req: &fuser::Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        if ino == ROOT_INO {
            reply.error(EISDIR);
            return;
        };
        let (read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => {
                if flags & libc::O_TRUNC != 0 {
                    return reply.error(libc::EACCES);
                } else {
                    (true, false)
                }
            }
            libc::O_WRONLY => (false, true),
            libc::O_RDWR => (true, true),
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };

        let truncate = flags as u32 & libc::O_TRUNC as u32 != 0;

        match fs.open(ino, read, write, truncate) {
            Ok(fh) => reply.opened(fh, 0),
            Err(e) => reply.error(libc::EIO),
        }
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let fs_arc = self.getfs();
        let fs: std::sync::MutexGuard<'_, GitFs> = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        let mask: u64 = (1u64 << 48) - 1;
        let parent_ino = if ino == ROOT_INO {
            ROOT_INO
        } else {
            fs.get_dir_parent(ino).unwrap_or(ROOT_INO)
        };
        let parent_entries: Vec<DirectoryEntry> = vec![
            DirectoryEntry {
                ino,
                oid: Oid::zero(),
                kind: FileType::Directory,
                name: ".".to_string(),
                git_mode: libc::S_IFDIR,
            },
            DirectoryEntry {
                ino: parent_ino,
                oid: Oid::zero(),
                kind: FileType::Directory,
                name: "..".to_string(),
                git_mode: libc::S_IFDIR,
            },
        ];
        let mut entries: Vec<DirectoryEntry> = vec![];
        for entry in parent_entries {
            entries.push(entry);
        }
        let res_entries = fs.readdir(ino);
        let gitfs_entries = match res_entries {
            Ok(ent) => ent,
            Err(e) => {
                error!(e = %e, "Fetching dir entries");
                return reply.error(ENOENT);
            }
        };
        for entry in gitfs_entries {
            entries.push(entry);
        }

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry.ino, (i + 1) as i64, entry.kind.into(), entry.name) {
                break;
            }
        }
        reply.ok();
    }

    fn readdirplus(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectoryPlus,
    ) {
        let fs_arc = self.getfs();
        let fs: std::sync::MutexGuard<'_, GitFs> = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        let parent_entries: Vec<DirectoryEntry> = vec![
            DirectoryEntry {
                ino: ROOT_INO,
                oid: Oid::zero(),
                kind: FileType::Directory,
                name: ".".to_string(),
                git_mode: libc::S_IFDIR,
            },
            DirectoryEntry {
                ino: ROOT_INO,
                oid: Oid::zero(),
                kind: FileType::Directory,
                name: "..".to_string(),
                git_mode: libc::S_IFDIR,
            },
        ];
        let mut entries: Vec<DirectoryEntryPlus> = vec![];
        for entry in parent_entries {
            let attr = {
                let Ok(ino_flag) = fs.get_ino_flag_from_db(entry.ino.into()) else {
                    return reply.error(EIO);
                };
                dir_attr(ino_flag)
            };
            let entry_plus = DirectoryEntryPlus {
                entry,
                attr: attr.into(),
            };
            entries.push(entry_plus);
        }
        let res_entries = fs.readdirplus(ino);
        let gitfs_entries = match res_entries {
            Ok(ent) => ent,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        for entry in gitfs_entries {
            entries.push(entry);
        }

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(
                entry.entry.ino,
                (i + 1) as i64,
                entry.entry.name,
                &TTL,
                &entry.attr.into(),
                0,
            ) {
                break;
            }
        }
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        // TODO
        reply.ok();
    }

    fn fsync(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        // TODO
        reply.ok();
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        let mut buf = vec![0u8; size as usize];
        let res = fs.read(ino, offset as u64, &mut buf, fh);
        drop(fs);
        match res {
            Ok(n) => reply.data(&buf[..n]),
            Err(e) => {
                error!(e = %e);
                reply.error(errno_from_anyhow(&e))
            }
        }
    }

    fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        data: &[u8],
        write_flags: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        let res = fs.write(ino, offset as u64, data, fh);
        match res {
            Ok(size) => reply.written(size as u32),
            Err(e) => reply.error(errno_from_anyhow(&e)),
        }
    }

    fn statfs(&mut self, _req: &fuser::Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        let blocks = 1;
        let bfree = 0;
        let bavail = 0;
        let files = 1;
        let ffree = 0;
        let bsize = 4096;
        let namelen = u32::MAX;
        let frsize = 0;
        reply.statfs(blocks, bfree, bavail, files, ffree, bsize, namelen, frsize);
    }

    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        let atime_opt = match atime {
            Some(TimeOrNow::Now) => {
                let a = libc::timespec {
                    tv_sec: 0,
                    tv_nsec: libc::UTIME_NOW,
                };
                Some(SystemTime::now())
            }
            Some(TimeOrNow::SpecificTime(t)) => Some(t),
            _ => None,
        };
        let mtime_opt = match mtime {
            Some(TimeOrNow::Now) => Some(SystemTime::now()),
            Some(TimeOrNow::SpecificTime(t)) => Some(t),
            _ => None,
        };

        let set_stored_attr: SetStoredAttr = SetStoredAttr {
            ino,
            size,
            uid,
            gid,
            flags,
            atime: atime_opt,
            mtime: mtime_opt,
            ctime,
        };
        let mut attr = match fs.update_db_metadata(set_stored_attr) {
            Ok(a) => a,
            Err(e) => return reply.error(errno_from_anyhow(&e)),
        };

        if let Some(atime) = atime_opt {
            attr.atime = atime;
        };
        if let Some(mtime) = mtime_opt {
            attr.mtime = mtime;
        };

        reply.attr(&TTL, &attr.into());
    }

    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        let fs_arc = self.getfs();

        let res = match fs_arc.lock() {
            Ok(fs) => fs.release(fh),
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        match res {
            Ok(true) => reply.ok(),
            Ok(false) => reply.error(libc::EBADF),
            Err(e) => reply.error(errno_from_anyhow(&e)),
        }
    }

    fn flush(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn opendir(&mut self, _req: &fuser::Request<'_>, ino: u64, _flags: i32, reply: ReplyOpen) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(g) => g,
            Err(e) => {
                error!(e = %e);
                return reply.error(libc::EIO);
            }
        };

        let attr = match fs.getattr(ino) {
            Ok(a) => a,
            Err(e) => {
                error!("getattr({}) failed: {:?}", ino, e);
                return reply.error(libc::ENOENT);
            }
        };

        if fuser::FileType::Directory != attr.kind.into() {
            return reply.error(libc::ENOTDIR);
        }

        reply.opened(0, 0);
    }

    // fn link(
    //     &mut self,
    //     _req: &fuser::Request<'_>,
    //     ino: u64,
    //     newparent: u64,
    //     newname: &OsStr,
    //     reply: ReplyEntry,
    // ) {
    //     let fs_arc = self.getfs();
    //     let fs = match fs_arc.lock() {
    //         Ok(fs) => fs,
    //         Err(e) => {
    //             error!(e = %e);
    //             return reply.error(EIO);
    //         }
    //     };
    //     let res = fs.link(ino, newparent, newname);
    //     match res {
    //         Ok(attr) => reply.entry(&TTL, &attr.into(), 0),
    //         Err(e) => reply.error(errno_from_anyhow(&e)),
    //     }
    // }

    fn create(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                error!(e = %e);
                return reply.error(EIO);
            }
        };
        let (read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => (true, false),
            libc::O_WRONLY => (false, true),
            libc::O_RDWR => (true, true),
            _ => return reply.error(libc::EINVAL),
        };

        let (attr, fh) = match fs.create(parent, name, read, write) {
            Ok((a, h)) => (a, h),
            Err(e) => {
                return reply.error(errno_from_anyhow(&e));
            }
        };

        reply.created(&TTL, &attr.into(), 0, fh, flags as u32);
    }
}

impl From<FileAttr> for fuser::FileAttr {
    fn from(from: FileAttr) -> Self {
        Self {
            ino: from.ino,
            size: from.size,
            blocks: from.blocks,
            atime: from.atime,
            mtime: from.mtime,
            ctime: from.ctime,
            crtime: from.crtime,
            kind: if from.kind == FileType::Directory {
                fuser::FileType::Directory
            } else {
                fuser::FileType::RegularFile
            },
            perm: from.perm,
            nlink: from.nlink,
            uid: from.uid,
            gid: from.gid,
            rdev: from.rdev,
            flags: from.flags,
            blksize: from.blksize,
        }
    }
}

impl From<FileType> for fuser::FileType {
    fn from(kind: FileType) -> Self {
        match kind {
            FileType::Directory => fuser::FileType::Directory,
            FileType::RegularFile => fuser::FileType::RegularFile,
            FileType::Symlink => fuser::FileType::Symlink,
        }
    }
}

fn errno_from_anyhow(err: &anyhow::Error) -> i32 {
    if let Some(ioe) = err.downcast_ref::<std::io::Error>() {
        if let Some(code) = ioe.raw_os_error() {
            return code;
        }
        return match ioe.kind() {
            std::io::ErrorKind::NotFound => libc::ENOENT,
            std::io::ErrorKind::PermissionDenied => libc::EACCES,
            std::io::ErrorKind::AlreadyExists => libc::EEXIST,
            std::io::ErrorKind::InvalidInput => libc::EINVAL,
            std::io::ErrorKind::TimedOut => libc::ETIMEDOUT,
            std::io::ErrorKind::WouldBlock => libc::EAGAIN,
            std::io::ErrorKind::DirectoryNotEmpty => libc::ENOTEMPTY,
            std::io::ErrorKind::IsADirectory => libc::EISDIR,
            std::io::ErrorKind::NotADirectory => libc::ENOTDIR,
            _ => EIO,
        };
    }

    EIO
}
