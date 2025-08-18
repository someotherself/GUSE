#![allow(unused_imports, unused_variables)]

use fuser::{
    BackgroundSession, MountOption, ReplyAttr, ReplyData, ReplyEntry, ReplyOpen, ReplyWrite,
};
use git2::Oid;
use libc::{EACCES, EIO, EISDIR, ENOENT, ENOTDIR, O_DIRECTORY};
use tracing::{Level, Span, info};
use tracing::{debug, error, instrument, trace, warn};

use std::ffi::OsStr;
use std::io::{BufRead, BufReader, ErrorKind};
use std::iter::Skip;
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};
use std::{num::NonZeroU32, path::PathBuf};

use crate::fs::fileattr::{CreateFileAttr, FileAttr, FileType};
use crate::fs::ops::readdir::{DirectoryEntry, DirectoryEntryPlus};
use crate::fs::{FsError, FsResult, GitFs, MyBacktrace, REPO_SHIFT, ROOT_INO, repo};

const TTL: Duration = Duration::from_secs(60);
const FMODE_EXEC: i32 = 0x20;

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

pub fn mount_fuse(opts: MountPoint) -> FsResult<()> {
    let MountPoint {
        mountpoint,
        repos_dir,
        read_only,
        allow_root,
        allow_other,
    } = opts;

    if !mountpoint.exists() {
        std::fs::create_dir(&mountpoint).map_err(|s| FsError::Io {
            source: s,
            my_backtrace: MyBacktrace::capture(),
        })?;
    }

    let mut options = vec![
        MountOption::FSName("GitFs".to_string()),
        MountOption::AutoUnmount,
    ];
    if read_only {
        options.push(MountOption::RO);
    }
    if allow_other {
        fuse_allow_other_enabled().map_err(|s| FsError::Io {
            source: s,
            my_backtrace: MyBacktrace::capture(),
        })?;
        options.push(MountOption::AllowOther);
    }
    if allow_root {
        options.push(MountOption::AllowRoot);
    }

    let fs = GitFsAdapter::new(repos_dir, opts.read_only)?;

    // match fuser::spawn_mount2(fs, mountpoint, &options) {
    //     Ok(session) => {
    //         info!("Filesystem unmounted cleanly");
    //         Ok(session)
    //     }
    //     Err(e) if e.kind() == ErrorKind::PermissionDenied => {
    //         error!("Permission denied: {}", e);
    //         std::process::exit(2);
    //     }
    //     Err(e) => Err(e.into()),
    // }
    match fuser::mount2(fs, mountpoint, &options) {
        Ok(_) => {
            info!("Filesystem unmounted cleanly");
            Ok(())
        }
        Err(e) if e.kind() == ErrorKind::PermissionDenied => {
            error!("Permission denied: {}", e);
            std::process::exit(2);
        }
        Err(e) => Err(e.into()),
    }
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

struct GitFsAdapter {
    inner: Arc<Mutex<GitFs>>,
}

impl GitFsAdapter {
    fn new(repos_dir: PathBuf, read_only: bool) -> FsResult<Self> {
        let fs = GitFs::new(repos_dir, read_only)?;
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
        config.set_max_readahead(128 * 1024).unwrap();
        Ok(())
    }

    fn destroy(&mut self) {} // parent_attrs.mode

    fn lookup(&mut self, req: &fuser::Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                eprintln!("fs mutex poisoned: {e}");
                return reply.error(EIO);
            }
        };
        let attr_result = fs.getattr(parent);
        if let Ok(parent_attrs) = attr_result {
            if !check_access(
                parent_attrs.uid,
                parent_attrs.gid,
                parent_attrs.perm,
                req.uid(),
                req.gid(),
                libc::X_OK,
            ) {
                reply.error(libc::EACCES);
                return;
            }

            if name == OsStr::new(".") {
                reply.entry(&TTL, &parent_attrs.into(), 0);
                return;
            }

            if name == OsStr::new("..") {
                let parent_ino = if parent == ROOT_INO {
                    ROOT_INO
                } else {
                    fs.get_parent_ino(parent).unwrap_or(ROOT_INO)
                };
                let parent_attr = fs.getattr(parent_ino).unwrap();
                return reply.entry(&TTL, &parent_attr.into(), 0);
            }
        } else {
            reply.error(ENOENT);
            return;
        }
        match fs.find_by_name(parent, name.to_str().unwrap()) {
            Ok(Some(attr)) => {
                let ino = attr.inode;
                reply.entry(&TTL, &attr.into(), 0)
            }
            Ok(None) => {
                // The name is not found under this parent
                reply.error(ENOENT);
            }
            Err(e) => {
                // Other internal error
                reply.error(EIO);
            }
        };
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                eprintln!("fs mutex poisoned: {e}");
                return reply.error(EIO);
            }
        };

        match fs.exists(ino) {
            Err(e) => {
                tracing::error!("exists({}) failed: {}", ino, e);
                reply.error(EIO);
                return;
            }
            Ok(false) => {
                reply.error(ENOENT);
                return;
            }
            Ok(true) => {}
        }

        match fs.getattr(ino) {
            Err(err) => {
                error!("getattr({}) failed: {:?}", ino, err);
                reply.error(ENOENT);
            }
            Ok(attr) => reply.attr(&TTL, &attr.into()),
        }
    }

    // TODO
    fn mknod(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: ReplyEntry,
    ) {
        todo!()
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
                eprintln!("fs mutex poisoned: {e}");
                return reply.error(EIO);
            }
        };
        match fs.getattr(parent) {
            Ok(attr) => {
                if !check_access(
                    attr.uid,
                    attr.gid,
                    attr.mode as u16,
                    req.uid(),
                    req.gid(),
                    libc::W_OK,
                ) {
                    reply.error(libc::EACCES);
                    return;
                };
            }
            Err(e) => {
                reply.error(libc::ENOENT);
                return;
            }
        }

        let create_attr = dir_attr();
        match fs.mkdir(parent, name, create_attr) {
            Ok(attr) => reply.entry(&TTL, &attr.into(), 0),
            Err(e) => {
                error!(?e);
                reply.error(ENOENT)
            }
        }
    }

    // TODO
    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::EROFS);
    }

    // TODO
    fn rmdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        reply.error(libc::EROFS);
    }

    // TODO
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
        reply.error(libc::EROFS);
    }

    fn open(&mut self, req: &fuser::Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                eprintln!("fs mutex poisoned: {e}");
                return reply.error(EIO);
            }
        };
        if ino == ROOT_INO {
            reply.error(EISDIR);
            return;
        };
        let (access_mask, read, write) = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => {
                if flags & libc::O_TRUNC != 0 {
                    reply.error(libc::EACCES);
                    return;
                }
                if flags & FMODE_EXEC != 0 {
                    // Open is from internal exec syscall
                    (libc::X_OK, true, false)
                } else {
                    (libc::R_OK, true, false)
                }
            }
            libc::O_WRONLY => (libc::W_OK, false, true),
            libc::O_RDWR => (libc::R_OK | libc::W_OK, true, true),
            _ => {
                reply.error(libc::EINVAL);
                return;
            }
        };
        match fs.getattr(ino) {
            Ok(attr) => {
                if !check_access(
                    attr.uid,
                    attr.gid,
                    attr.perm,
                    req.uid(),
                    req.gid(),
                    access_mask,
                ) {
                    reply.error(libc::EACCES);
                    return;
                }

                match fs.open(ino, read, write) {
                    Ok(fh) => reply.opened(fh, 0),
                    Err(e) => reply.error(libc::EIO),
                }
            }
            Err(e) => reply.error(libc::ENOENT),
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
                eprintln!("fs mutex poisoned: {e}");
                return reply.error(EIO);
            }
        };
        let mask: u64 = (1u64 << 48) - 1;
        let parent_entries: Vec<DirectoryEntry> = vec![
            DirectoryEntry {
                inode: ino,
                oid: Oid::zero(),
                kind: FileType::Directory,
                name: ".".to_string(),
                filemode: libc::S_IFDIR,
            },
            DirectoryEntry {
                inode: fs.get_parent_ino(ino).unwrap_or(ROOT_INO),
                oid: Oid::zero(),
                kind: FileType::Directory,
                name: "..".to_string(),
                filemode: libc::S_IFDIR,
            },
        ];
        let mut entries: Vec<DirectoryEntry> = vec![];
        for entry in parent_entries {
            entries.push(entry);
        }
        let repos_as_entries = fs.readdir(ino).unwrap();
        for entry in repos_as_entries {
            entries.push(entry);
        }

        for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(entry.inode, (i + 1) as i64, entry.kind.into(), entry.name) {
                break;
            }
        }
        reply.ok();
    }

    // // TODO
    // fn readdirplus(
    //     &mut self,
    //     _req: &fuser::Request<'_>,
    //     ino: u64,
    //     fh: u64,
    //     offset: i64,
    //     mut reply: fuser::ReplyDirectoryPlus,
    // ) {
    //     let fs_arc = self.getfs();
    //     let fs: std::sync::MutexGuard<'_, GitFs> = match fs_arc.lock() {
    //     Ok(fs) => fs,
    //     Err(e) => {
    //         eprintln!("fs mutex poisoned: {e}");
    //         return reply.error(EIO)
    //     },
    // };
    //     let mask: u64 = (1u64 << 48) - 1;
    //     let parent_entries: Vec<DirectoryEntry> = vec![
    //         DirectoryEntry {
    //             inode: ROOT_INO,
    //             oid: Oid::zero(),
    //             kind: FileType::Directory,
    //             name: ".".to_string(),
    //             filemode: libc::S_IFDIR,
    //         },
    //         DirectoryEntry {
    //             inode: ROOT_INO,
    //             oid: Oid::zero(),
    //             kind: FileType::Directory,
    //             name: "..".to_string(),
    //             filemode: libc::S_IFDIR,
    //         },
    //     ];
    //     let mut entries: Vec<DirectoryEntryPlus> = vec![];
    //     for entry in parent_entries {
    //         let entry_plus = DirectoryEntryPlus {
    //             entry,
    //             attr: dir_attr().into(),
    //         };
    //         entries.push(entry_plus);
    //     }
    //     let repos_as_entries = fs.readdirplus(ino).unwrap();
    //     for entry in repos_as_entries {
    //         entries.push(entry);
    //     }

    //     for (i, entry) in entries.into_iter().enumerate().skip(offset as usize) {
    //         if reply.add(
    //             entry.entry.inode,
    //             (i + 1) as i64,
    //             entry.entry.name,
    //             &TTL,
    //             &entry.attr.into(),
    //             0,
    //         ) {
    //             break;
    //         }
    //     }
    //     reply.ok();
    // }

    // TODO
    fn fsyncdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        todo!()
    }

    // TODO
    fn fsync(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        todo!()
    }

    // TODO
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
        todo!()
    }

    // TODO
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
        todo!()
    }

    // TODO
    fn statfs(&mut self, _req: &fuser::Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        todo!()
    }

    // TODO
    fn release(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        todo!()
    }

    // TODO
    fn flush(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        lock_owner: u64,
        reply: fuser::ReplyEmpty,
    ) {
        todo!()
    }

    fn opendir(&mut self, req: &fuser::Request<'_>, ino: u64, flags: i32, reply: ReplyOpen) {
        let fs_arc = self.getfs();
        let fs = match fs_arc.lock() {
            Ok(fs) => fs,
            Err(e) => {
                eprintln!("fs mutex poisoned: {e}");
                return reply.error(EIO);
            }
        };

        let attr = match fs.getattr(ino) {
            Ok(attr) => attr,
            Err(e) => {
                error!("getattr({}) failed: {:?}", ino, e);
                return reply.error(ENOENT);
            }
        };

        if fuser::FileType::Directory != attr.kind.into() {
            return reply.error(ENOTDIR);
        }

        let mut access_mask = match flags & libc::O_ACCMODE {
            libc::O_RDONLY => libc::R_OK,
            libc::O_WRONLY => libc::W_OK,
            libc::O_RDWR => libc::R_OK | libc::W_OK,
            _ => return reply.error(libc::EINVAL),
        };

        access_mask |= libc::X_OK;

        if flags & O_DIRECTORY == 0 {
            return reply.error(ENOTDIR);
        }

        info!(
            "opendir ino={} kind={:?} uid:{} gid:{} perm={:#o}",
            ino, attr.kind, attr.uid, attr.gid, attr.perm
        );

        if check_access(
            attr.uid,
            attr.gid,
            attr.perm,
            req.uid(),
            req.gid(),
            access_mask,
        ) {
            reply.opened(0, 0)
        } else {
            reply.error(EACCES)
        }
    }

    // TODO
    fn create(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        reply.error(libc::EROFS);
    }
}

fn check_access(
    file_uid: u32,
    file_gid: u32,
    file_mode: u16,
    uid: u32,
    gid: u32,
    mut access_mask: i32,
) -> bool {
    // F_OK tests for existence of file
    if access_mask == libc::F_OK {
        return true;
    }
    let file_mode = i32::from(file_mode);

    // root is allowed to read & write anything
    if uid == 0 {
        // root only allowed to exec if one of the X bits is set
        access_mask &= libc::X_OK;
        access_mask -= access_mask & (file_mode >> 6);
        access_mask -= access_mask & (file_mode >> 3);
        access_mask -= access_mask & file_mode;
        return access_mask == 0;
    }

    if uid == file_uid {
        access_mask -= access_mask & (file_mode >> 6);
    } else if gid == file_gid {
        access_mask -= access_mask & (file_mode >> 3);
    } else {
        access_mask -= access_mask & file_mode;
    }

    access_mask == 0
}

impl From<FileAttr> for fuser::FileAttr {
    fn from(from: FileAttr) -> Self {
        Self {
            ino: from.inode,
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

pub const fn dir_attr() -> CreateFileAttr {
    CreateFileAttr {
        kind: FileType::Directory,
        perm: 0o775,
        uid: 0,
        mode: libc::S_IFDIR,
        gid: 0,
        rdev: 0,
        flags: 0,
    }
}

pub const fn file_attr() -> CreateFileAttr {
    CreateFileAttr {
        kind: FileType::RegularFile,
        perm: 0o655,
        uid: 0,
        mode: libc::S_IFREG,
        gid: 0,
        rdev: 0,
        flags: 0,
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
