use crate::{
    fs::{FileAttr, GitFs},
    inodes::NormalIno,
};

pub fn getattr_live_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<FileAttr> {
    fs.refresh_metadata_from_disk(ino) // TODO: Update DB?
}

pub fn getattr_git_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<FileAttr> {
    fs.get_metadata(ino.to_norm_u64())
}
