use std::{fs::File, os::unix::fs::PermissionsExt};

use anyhow::{anyhow, bail};
use libc::EPERM;

use crate::{
    fs::{
        GitFs,
        builds::BuildOperationCtx,
        fileattr::{FileAttr, file_attr},
    },
    inodes::NormalIno,
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
    let file_path = fs.get_path_by_name_in_live(parent, name)?;

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
    name: &str,
    read: bool,
    write: bool,
) -> anyhow::Result<(FileAttr, u64)> {
    if !read && !write {
        bail!("read and write cannot be false at the same time")
    };

    let Some(ctx) = BuildOperationCtx::new(fs, parent)? else {
        bail!(std::io::Error::from_raw_os_error(EPERM))
    };

    let file_path = ctx.child_in_temp(name);
    let ino = fs.next_inode_checked(parent.to_norm_u64())?;
    let mut attr: FileAttr = file_attr().into();
    attr.ino = ino;
    let file = std::fs::File::create_new(&file_path)?;
    std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o775))?;
    file.sync_all()?;
    File::open(file_path.parent().ok_or_else(|| anyhow!("No parent"))?)?.sync_all()?;

    let nodes = vec![(parent.to_norm_u64(), name.into(), attr)];
    fs.write_inodes_to_db(nodes)?;

    let fh = fs.open(ino, read, write, false)?;
    Ok((attr, fh))
}
