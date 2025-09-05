use std::{fs::File, os::unix::fs::PermissionsExt};

use anyhow::{anyhow, bail};

use crate::{
    fs::{
        GitFs,
        fileattr::FileAttr,
        ops::readdir::{DirCase, classify_inode},
    },
    inodes::NormalIno,
    mount::file_attr,
};

pub fn create_live(
    fs: &GitFs,
    parent: u64,
    name: &str,
    read: bool,
    write: bool,
) -> anyhow::Result<(FileAttr, u64)> {
    if !read && !write {
        bail!("read and write cannot be false at the same time")
    };
    let ino = fs.next_inode_checked(parent)?;
    let mut attr: FileAttr = file_attr().into();
    attr.ino = ino;
    let file_path = fs.build_path(parent, name)?;

    let file = std::fs::File::create_new(&file_path)?;
    std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o775))?;
    file.sync_all()?;
    File::open(file_path.parent().ok_or_else(|| anyhow!("No parent"))?)?.sync_all()?;

    let nodes = vec![(parent, name.into(), attr)];
    fs.write_inodes_to_db(nodes)?;

    let fh = fs.open(ino, read, write, false)?;
    Ok((attr, fh))
}

pub fn create_git(
    fs: &GitFs,
    parent: NormalIno,
    _name: &str,
    _read: bool,
    _write: bool,
) -> anyhow::Result<(FileAttr, u64)> {
    let res = classify_inode(fs, parent.to_norm_u64())?;
    match res {
        DirCase::Month { year: _, month: _ } => {
            bail!("This folder is read only!")
        }
        DirCase::Commit { oid: _ } => {
            todo!()
        }
    }
}
