#![allow(unused_imports, unused_variables)]

use fuse3::MountOptions;
use fuse3::raw::reply::{
    ReplyAttr, ReplyCreated, ReplyData, ReplyEntry, ReplyInit, ReplyOpen, ReplyStatFs, ReplyWrite,
};
use fuse3::raw::{MountHandle, Session};
use fuse3::{Errno, Inode, Result, SetAttr, raw::Request};
use libc::ENOENT;
use tracing::{Level, info};
use tracing::{debug, error, instrument, trace, warn};

use std::ffi::OsStr;
use std::time::Duration;
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

// async fn mount_fuse(opts: MountPoint) -> anyhow::Result<MountHandle> {
//     let MountPoint {
//         mountpoint,
//         data_dir,
//         read_only,
//         allow_root,
//         allow_other,
//     } = opts;

//     let mount_options = &mut MountOptions::default();

//     if !mountpoint.exists() {
//         std::fs::create_dir_all(&mountpoint)?;
//     }

//     let fs = GitFsAdapter::new(data_dir);

//     let mount_options = mount_options
//                                 .read_only(read_only)
//                                 .allow_other(allow_other)
//                                 .allow_root(allow_root)
//                                 .fs_name("GitFs")
//                                 .clone();

//     let fs = GitFs::new(data_dir);
//     let session = Session::new(mount_options)
//     .mount_with_unprivileged(fs, &mountpoint).await?;

//     Ok(session)
// }

struct GitFsAdapter {
    inner: Arc<GitFs>,
}

impl GitFsAdapter {
    fn new(data_dir: PathBuf) -> Self {
        let fs = GitFs::new(data_dir);
        Self {
            inner: Arc::new(fs),
        }
    }

    pub fn getfs(&self) -> Arc<GitFs> {
        self.inner.clone()
    }
}

// impl fuse3::raw::Filesystem for GitFsAdapter {
//     #[instrument(skip(self), err(level = Level::WARN), ret(level = Level::INFO))]
//     async fn init(&self, _req: Request) -> Result<ReplyInit> {
//         Ok(ReplyInit {
//             max_write: NonZeroU32::new(16 * 1024).unwrap(),
//         })
//     }

//     async fn destroy(&self, _req: Request) {}

//     #[instrument(skip(self), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> Result<ReplyEntry> {
//         // Lookup a dir (tree) by name (hash) and get attr
//         todo!()
//     }

//     #[instrument(skip(self), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn getattr(
//         &self,
//         _req: Request,
//         inode: u64,
//         _fh: Option<u64>,
//         _flags: u32,
//     ) -> Result<ReplyAttr> {
//         match self.getfs().getattr(inode) {
//             Err(err) => {
//                 error!(err = %err);
//                 return Err(ENOENT.into());
//             }
//             Ok(attr) => Ok(ReplyAttr {
//                 ttl: TTL,
//                 attr: attr.into(),
//             }),
//         }
//     }

//     #[instrument(skip(self), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn setattr(
//         &self,
//         req: Request,
//         inode: Inode,
//         fh: Option<u64>,
//         set_attr: SetAttr,
//     ) -> Result<ReplyAttr> {
//         let attr = self.getfs().getattr(inode).map_err(|err| {
//             error!(err = %err);
//             Errno::from(ENOENT)
//         })?;

//         if let Some(mode) = set_attr.mode {
//             todo!()
//         }

//         if set_attr.uid.is_some() || set_attr.gid.is_some() {
//             todo!()
//         }

//         if let Some(size) = set_attr.size {
//             todo!()
//         }

//         if let Some(lock_owner) = set_attr.lock_owner {
//             // needed?
//             todo!()
//         }

//         if let Some(atime) = set_attr.atime {
//             todo!()
//         }

//         if let Some(mtime) = set_attr.mtime {
//             todo!()
//         }

//         if let Some(ctime) = set_attr.ctime {
//             todo!()
//         }
//         todo!()
//     }

//     #[instrument(skip(self), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn mknod(
//         &self,
//         req: Request,
//         parent: Inode,
//         name: &OsStr,
//         mode: u32,
//         rdev: u32,
//     ) -> Result<ReplyEntry> {
//         todo!()
//     }

//     #[instrument(skip(self, name), fields(name = name.to_str().unwrap()), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn mkdir(
//         &self,
//         req: Request,
//         parent: Inode,
//         name: &OsStr,
//         mode: u32,
//         umask: u32,
//     ) -> Result<ReplyEntry> {
//         todo!()
//     }

//     #[instrument(skip(self, name), fields(name = name.to_str().unwrap()), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn unlink(&self, req: Request, parent: Inode, name: &OsStr) -> Result<()> {
//         todo!()
//     }
//     #[instrument(skip(self, name), fields(name = name.to_str().unwrap()), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn rmdir(&self, req: Request, parent: Inode, name: &OsStr) -> Result<()> {
//         todo!()
//     }
//     #[instrument(skip(self, name, new_name), fields(name = name.to_str().unwrap(), new_name = new_name.to_str().unwrap()), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn rename(
//         &self,
//         req: Request,
//         parent: Inode,
//         name: &OsStr,
//         new_parent: Inode,
//         new_name: &OsStr,
//     ) -> Result<()> {
//         todo!()
//     }

//     #[instrument(skip(self), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn open(&self, req: Request, inode: Inode, flags: u32) -> Result<ReplyOpen> {
//         todo!()
//     }

//     #[instrument(skip(self), err(level = Level::WARN))]
//     async fn read(
//         &self,
//         req: Request,
//         inode: u64,
//         fh: u64,
//         offset: u64,
//         size: u32,
//     ) -> Result<ReplyData> {
//         todo!()
//     }

//     #[instrument(skip(self, data), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn write(
//         &self,
//         req: Request,
//         inode: Inode,
//         fh: u64,
//         offset: u64,
//         data: &[u8],
//         write_flags: u32,
//         flags: u32,
//     ) -> Result<ReplyWrite> {
//         todo!()
//     }

//     #[instrument(skip(self), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn statfs(&self, req: Request, inode: u64) -> Result<ReplyStatFs> {
//         todo!()
//     }

//     #[instrument(skip(self), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn release(
//         &self,
//         req: Request,
//         inode: Inode,
//         fh: u64,
//         flags: u32,
//         lock_owner: u64,
//         flush: bool,
//     ) -> Result<()> {
//         todo!()
//     }

//     #[instrument(skip(self), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn flush(&self, req: Request, inode: Inode, fh: u64, lock_owner: u64) -> Result<()> {
//         todo!()
//     }

//     #[instrument(skip(self), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn opendir(&self, req: Request, inode: Inode, flags: u32) -> Result<ReplyOpen> {
//         todo!()
//     }

//     #[instrument(skip(self, name), fields(name = name.to_str().unwrap()), err(level = Level::WARN), ret(level = Level::DEBUG))]
//     async fn create(
//         &self,
//         req: Request,
//         parent: Inode,
//         name: &OsStr,
//         mode: u32,
//         flags: u32,
//     ) -> Result<ReplyCreated> {
//         todo!()
//     }
// }

impl From<FileAttr> for fuse3::raw::prelude::FileAttr {
    fn from(from: FileAttr) -> Self {
        Self {
            ino: from.inode,
            size: from.size,
            blocks: from.blocks,
            atime: from.atime.into(),
            mtime: from.mtime.into(),
            ctime: from.ctime.into(),
            kind: if from.kind == FileType::Directory {
                fuse3::raw::prelude::FileType::Directory
            } else {
                fuse3::raw::prelude::FileType::RegularFile
            },
            perm: from.perm,
            nlink: from.nlink,
            uid: from.uid,
            gid: from.gid,
            rdev: from.rdev,
            blksize: from.blksize,
        }
    }
}
