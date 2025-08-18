use crate::fs::{FsResult, GitFs, fileattr::FileAttr};

pub fn create_live(fs: &GitFs, parent: u64, name: &str) -> FsResult<(FileAttr, u64)> {
    let _dir_path = fs.build_path(parent, name)?;

    todo!()
}
