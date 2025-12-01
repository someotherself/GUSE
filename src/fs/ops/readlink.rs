use anyhow::bail;

use crate::{
    fs::{self, GitFs},
    inodes::NormalIno,
};

pub fn readlink_git(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Vec<u8>> {
    let file_size = fs.get_file_size_from_db(ino)?;
    let fh = match fs.open(ino.to_norm_u64(), true, false, false) {
        Ok(fh) => fh,
        Err(e) => {
            tracing::error!("Error opening symlink {e}");
            bail!(e)
        }
    };
    let mut buf = vec![0u8; file_size as usize];
    let bytes = fs::ops::read::read_git(fs, ino.to_norm_u64().into(), 0, &mut buf, fh)?;
    Ok(buf[..bytes].to_vec())
}
