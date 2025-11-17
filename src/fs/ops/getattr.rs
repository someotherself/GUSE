use crate::{
    fs::{FileAttr, GitFs, fileattr::InoFlag},
    inodes::NormalIno,
};

pub fn getattr_live_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<FileAttr> {
    fs.get_metadata(ino.to_norm_u64())
}

pub fn getattr_git_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<FileAttr> {
    let attr = fs.get_metadata(ino.to_norm_u64())?;
    let target_ino: NormalIno = attr.ino.into();
    if attr.ino_flag == InoFlag::SnapFolder && fs.read_children(target_ino)?.is_empty() {
        // First time opening a Snap folder
        // Walk all the folders inside and add entries to DB
        fs.cache_snap_readdir(target_ino, true)?;
    };
    Ok(attr)
}
