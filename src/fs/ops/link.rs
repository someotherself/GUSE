use std::ffi::{OsStr, OsString};

use anyhow::bail;

use crate::{
    fs::{
        GitFs,
        fileattr::{Dentry, FileAttr},
    },
    inodes::NormalIno,
    mount::InvalMsg,
};

pub fn link_git(
    fs: &GitFs,
    source_ino: NormalIno,
    newparent: NormalIno,
    newname: &OsStr,
) -> anyhow::Result<FileAttr> {
    if !fs.is_in_build(source_ino)? {
        tracing::error!("This directory is read only");
        bail!(std::io::Error::from_raw_os_error(libc::EACCES))
    }
    if !fs.is_in_build(newparent)? {
        tracing::error!("This directory is read only");
        bail!(std::io::Error::from_raw_os_error(libc::EACCES))
    }
    let repo = fs.get_repo(source_ino.to_norm_u64())?;
    let commit_oid = fs.get_oid_from_db(source_ino.into())?;

    let original = {
        let build_root = fs.get_path_to_build_folder(source_ino)?;
        let session = repo.get_or_init_build_session(commit_oid, &build_root)?;
        session.finish_path(fs, source_ino)?
    };

    let link = {
        let ino = newparent;
        let build_root = fs.get_path_to_build_folder(ino)?;
        let session = repo.get_or_init_build_session(commit_oid, &build_root)?;
        session.finish_path(fs, ino)?.join(newname)
    };
    std::fs::hard_link(&original, &link)?;
    fs.write_dentry(Dentry {
        target_ino: source_ino.into(),
        parent_ino: newparent.into(),
        target_name: newname.to_os_string(),
    })?;
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
