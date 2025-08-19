use std::{fs::File, os::unix::fs::PermissionsExt};

use crate::{
    fs::{FsError, FsResult, GitFs, MyBacktrace, fileattr::FileAttr},
    mount::file_attr,
};

pub fn create_live(
    fs: &GitFs,
    parent: u64,
    name: &str,
    read: bool,
    write: bool,
) -> FsResult<(FileAttr, u64)> {
    if !read && !write {
        return Err(FsError::Internal(
            "read and write cannot be false at the same time".to_string(),
        ));
    };
    let ino = fs.next_inode(parent)?;
    let mut attr: FileAttr = file_attr().into();
    attr.inode = ino;
    let file_path = fs.build_path(parent, name)?;

    let file = std::fs::File::create_new(&file_path)?;
    std::fs::set_permissions(&file_path, std::fs::Permissions::from_mode(0o775)).map_err(|s| {
        FsError::Io {
            source: s,
            my_backtrace: MyBacktrace::capture(),
        }
    })?;
    file.sync_all()?;
    File::open(file_path.parent().unwrap())?.sync_all()?;

    let nodes = vec![(parent, name.into(), attr)];
    fs.write_inodes_to_db(nodes)?;

    let fh = fs.open(ino, read, write)?;

    Ok((attr, fh))
}
