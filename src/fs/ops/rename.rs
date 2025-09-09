use anyhow::{anyhow, bail};

use crate::{
    fs::{FileAttr, GitFs, fileattr::FileType},
    inodes::NormalIno,
};

pub fn rename_live(
    fs: &GitFs,
    parent: NormalIno,
    name: &str,
    new_parent: NormalIno,
    new_name: &str,
) -> anyhow::Result<()> {
    if !fs.is_in_live(new_parent.to_norm_u64()) {
        bail!(format!("New parent {} not allowed", new_parent));
    }

    let src_attr = fs
        .lookup(parent.to_norm_u64(), name)?
        .ok_or_else(|| anyhow!("Source does not exist"))?;

    let mut dest_exists = false;
    let mut dest_old_ino: u64 = 0;

    if let Some(dest_attr) = fs.lookup(new_parent.to_norm_u64(), new_name)? {
        dest_exists = true;
        dest_old_ino = dest_attr.ino;

        if dest_attr.kind == FileType::Directory && fs.readdir(new_parent.to_norm_u64())?.is_empty()
        {
            bail!("Directory is not empty")
        }
        if dest_attr.kind != src_attr.kind {
            bail!("Source and destination are not the same type")
        }
    }

    let src = fs.build_full_path(parent.to_norm_u64())?.join(name);
    let dest = fs.build_full_path(new_parent.to_norm_u64())?.join(new_name);

    std::fs::rename(src, &dest)?;

    fs.remove_db_record(src_attr.ino)?;
    if dest_exists {
        fs.remove_db_record(dest_old_ino)?;
    }

    let new_attr = fs.attr_from_path(dest)?;

    let nodes: Vec<(u64, String, FileAttr)> =
        vec![(new_parent.to_norm_u64(), new_name.to_string(), new_attr)];
    fs.write_inodes_to_db(nodes)?;

    Ok(())
}
