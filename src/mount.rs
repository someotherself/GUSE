#![allow(unused_imports, unused_variables)]

use fuser::{MountOption, ReplyAttr, ReplyData, ReplyEntry, ReplyOpen, ReplyWrite};
use libc::ENOENT;
use tracing::{Level, info};
use tracing::{debug, error, instrument, trace, warn};

use std::ffi::OsStr;
use std::io::{BufRead, BufReader, ErrorKind};
use std::iter::Skip;
use std::time::{Duration, SystemTime};
use std::{num::NonZeroU32, path::PathBuf, sync::Arc};

use crate::fs::{FileAttr, FileType, GitFs};

const TTL: Duration = Duration::from_secs(1);

pub struct MountPoint {
    mountpoint: PathBuf,
    data_dir: PathBuf,
    read_only: bool,
    allow_root: bool,
    allow_other: bool,
}

impl MountPoint {
    fn new(
        mountpoint: PathBuf,
        data_dir: PathBuf,
        read_only: bool,
        allow_root: bool,
        allow_other: bool,
    ) -> Self {
        Self {
            mountpoint,
            data_dir,
            read_only,
            allow_root,
            allow_other,
        }
    }
}

fn mount_fuse(opts: MountPoint) -> anyhow::Result<()> {
    let MountPoint {
        mountpoint,
        data_dir,
        read_only,
        allow_root,
        allow_other,
    } = opts;

    if !mountpoint.exists() {
        std::fs::create_dir_all(&mountpoint)?;
    }

    let mut options = vec![
        MountOption::FSName("GitFs".to_string()),
        MountOption::AutoUnmount,
        MountOption::RO,
    ];
    if read_only {
        options.push(MountOption::RO);
    }
    if allow_other {
        options.push(MountOption::AllowOther);
    }
    if allow_root {
        options.push(MountOption::AllowRoot);
    }

    let fs = GitFsAdapter::new(data_dir, opts.read_only)?;

    match fuser::mount2(fs, mountpoint, &options) {
        Ok(()) => {
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
    let file = std::fs::File::open("/etc/fuse.conf")?;
    for line in BufReader::new(file).lines() {
        if line?.trim_start().starts_with("user_allow_other") {
            return Ok(true);
        }
    }
    Ok(false)
}

struct GitFsAdapter {
    inner: Arc<GitFs>,
}

impl GitFsAdapter {
    fn new(data_dir: PathBuf, read_only: bool) -> anyhow::Result<Self> {
        let fs = GitFs::new(data_dir, read_only)?;
        Ok(GitFsAdapter { inner: fs })
    }

    pub fn getfs(&self) -> Arc<GitFs> {
        self.inner.clone()
    }
}

// pub struct DirectoryEntryIterator;

// impl Iterator for DirectoryEntryIterator {
//     type Item = Result<f>;

//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

// pub struct DirectoryEntryIteratorPlus;

// impl Iterator for DirectoryEntryIteratorPlus {
//     type Item = Result<DirectoryEntryPlus>;

//     fn next(&mut self) -> Option<Self::Item> {
//         todo!()
//     }
// }

impl fuser::Filesystem for GitFsAdapter {
    // impl fuse::raw::Filesystem for GitFsAdapter {
    // type DirEntryPlusStream<'a>
    //     = Iter<Skip<DirectoryEntryIteratorPlus>>
    // where
    //     Self: 'a;

    // type DirEntryStream<'a>
    //     = Iter<Skip<DirectoryEntryIterator>>
    // where
    //     Self: 'a;

    #[instrument(skip(self), err(level = Level::WARN), ret(level = Level::INFO))]
    fn init(
        &mut self,
        _req: &fuser::Request<'_>,
        _config: &mut fuser::KernelConfig,
    ) -> Result<(), libc::c_int> {
        // Ok(ReplyInit {
        //         max_write: NonZeroU32::new(16 * 1024).unwrap(),
        //     })
        todo!()
    }

    fn destroy(&mut self) {}

    fn lookup(&mut self, _req: &fuser::Request<'_>, parent: u64, name: &OsStr, reply: ReplyEntry) {
        // Lookup a dir (tree) by name (hash) and get attr
        // TODO: Should check the access?

        // let attr = match self.getfs().find_by_name() {
        //     Ok(Some(attr)) => attr,
        //     Err(err) => {
        //         return Err(ENOENT.into());
        //     }
        //     _ => {
        //         return Err(ENOENT.into());
        //     }
        // };

        // Ok(ReplyEntry {
        //     ttl: TTL,
        //     attr: attr.into(),
        //     generation: 0,
        // })
        todo!()
    }

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, fh: Option<u64>, reply: ReplyAttr) {
        // match self.getfs().getattr(ino) {
        //     Err(err) => {
        //         error!(err = %err);
        //         return Err(ENOENT.into());
        //     }
        //     // Ok(attr) => Ok(ReplyAttr {
        //     //     attr: attr.into(),
        //     // }),
        //     Ok(attr) => reply.attr(TTL, &attr),
        // }
        todo!()
    }

    // TODO
    fn setattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<SystemTime>,
        fh: Option<u64>,
        _crtime: Option<SystemTime>,
        _chgtime: Option<SystemTime>,
        _bkuptime: Option<SystemTime>,
        flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        let attr = self.getfs().getattr(ino).map_err(|err| todo!());

        todo!()
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

    // TODO
    fn mkdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        reply: ReplyEntry,
    ) {
        todo!()
    }

    // TODO
    fn unlink(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        todo!()
    }

    // TODO
    fn rmdir(
        &mut self,
        _req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        todo!()
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
        todo!()
    }

    // TODO
    fn open(&mut self, _req: &fuser::Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
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

    // TODO
    fn opendir(&mut self, _req: &fuser::Request<'_>, _ino: u64, _flags: i32, reply: ReplyOpen) {
        todo!()
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
        todo!()
    }
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
