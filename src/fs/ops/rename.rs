use anyhow::{anyhow, bail};

use crate::fs::{FileAttr, GitFs, fileattr::FileType};

pub fn rename_live(
    fs: &GitFs,
    parent: u64,
    name: &str,
    new_parent: u64,
    new_name: &str,
) -> anyhow::Result<()> {
    if parent == new_parent && name == new_name {
        return Ok(());
    }

    let src_attr = fs
        .find_by_name(parent, name)?
        .ok_or_else(|| anyhow!("Source does not exist"))?;

    let mut dest_exists = false;
    let mut dest_old_ino: u64 = 0;

    if let Some(dest_attr) = fs.find_by_name(new_parent, new_name)? {
        dest_exists = true;
        dest_old_ino = dest_attr.inode;

        if dest_attr.kind == FileType::Directory && fs.readdir(new_parent)?.is_empty() {
            bail!("Directory is not empty")
        }
        if dest_attr.kind != src_attr.kind {
            bail!("Source and destination are not the same type")
        }
    }

    let src = fs.build_full_path(parent)?.join(name);
    let dest = fs.build_full_path(new_parent)?.join(new_name);

    std::fs::rename(src, dest.clone())?;

    fs.remove_db_record(src_attr.inode)?;
    if dest_exists {
        fs.remove_db_record(dest_old_ino)?;
    }

    let new_attr = fs.attr_from_path(dest)?;

    let nodes: Vec<(u64, String, FileAttr)> = vec![(new_parent, new_name.to_string(), new_attr)];
    fs.write_inodes_to_db(nodes)?;

    Ok(())
}
