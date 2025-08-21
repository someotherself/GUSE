use std::{fs::File, os::unix::fs::PermissionsExt};

use anyhow::{anyhow, bail};

use crate::{
    fs::{GitFs, fileattr::FileAttr},
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
    let ino = fs.next_inode(parent)?;
    let mut attr: FileAttr = file_attr().into();
    attr.inode = ino;
    let file_path = fs.build_path(parent, name)?;

    let file = std::fs::File::create_new(&file_path)?;
    std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o775))?;
    file.sync_all()?;
    File::open(file_path.parent().ok_or_else(|| anyhow!("No parent"))?)?.sync_all()?;

    let node = (parent, name.into(), attr);
    fs.write_inodes_to_db(node)?;

    let fh = fs.open(ino, read, write, false)?;
    Ok((attr, fh))
}
