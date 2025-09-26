use std::ffi::OsString;

use anyhow::{anyhow, bail};

use crate::{
    fs::{GitFs, fileattr::FileAttr},
    inodes::NormalIno,
    mount::InvalMsg,
};

pub fn link_git(
    fs: &GitFs,
    source_ino: NormalIno,
    newparent: NormalIno,
    newname: &str,
) -> anyhow::Result<FileAttr> {
    if !fs.is_in_build(source_ino)? {
        tracing::error!("This directory is read only");
        bail!(std::io::Error::from_raw_os_error(libc::EACCES))
    }
    if !fs.is_in_build(newparent)? {
        tracing::error!("This directory is read only");
        bail!(std::io::Error::from_raw_os_error(libc::EACCES))
    }

    let original = {
        let parent_oid = fs.parent_commit_build_session(source_ino)?;
        let build_root = fs.get_path_to_build_folder(source_ino)?;
        let repo = fs.get_repo(source_ino.to_norm_u64())?;
        let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let session = repo.get_or_init_build_session(parent_oid, &build_root)?;
        drop(repo);
        session.finish_path(fs, source_ino)?
    };

    let link = {
        let ino = newparent;
        let parent_oid = fs.parent_commit_build_session(ino)?;
        let build_root = fs.get_path_to_build_folder(ino)?;
        let repo = fs.get_repo(ino.to_norm_u64())?;
        let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let session = repo.get_or_init_build_session(parent_oid, &build_root)?;
        drop(repo);
        session.finish_path(fs, ino)?.join(newname)
    };
    std::fs::hard_link(&original, &link)?;
    fs.write_dentry(newparent, source_ino, newname)?;
    {
        let _ = fs.notifier.try_send(InvalMsg::Entry {
            parent: newparent.to_norm_u64(),
            name: OsString::from(newname),
        });
        let _ = fs.notifier.try_send(InvalMsg::Inode {
            ino: newparent.to_norm_u64(),
            off: 0,
            len: 0,
        });
    }
    fs.get_metadata(source_ino.to_norm_u64())
}
