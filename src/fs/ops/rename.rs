use anyhow::{anyhow, bail};
use tracing::info;

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
    if !fs.is_in_live(new_parent.to_norm_u64()) && !fs.is_in_build(new_parent)? {
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

    let mut new_attr = fs.attr_from_path(dest)?;
    let new_ino = fs.next_inode_checked(new_parent.to_norm_u64())?;
    new_attr.ino = new_ino;

    let nodes: Vec<(u64, String, FileAttr)> =
        vec![(new_parent.to_norm_u64(), new_name.to_string(), new_attr)];
    fs.write_inodes_to_db(nodes)?;

    Ok(())
}

pub fn rename_git_build(
    fs: &GitFs,
    parent: NormalIno,
    name: &str,
    new_parent: NormalIno,
    new_name: &str,
) -> anyhow::Result<()> {
    let dst_in_build = fs.is_in_build(new_parent)?;
    let oid = fs.get_oid_from_db(new_parent.into())?;
    let is_commit_folder = fs.is_commit(new_parent, oid)?;
    if !dst_in_build && !is_commit_folder && !fs.is_in_live(new_parent.to_norm_u64()) {
        bail!(format!("New parent {} not allowed", new_parent));
    }
    info!("rename - 1");
    let src_attr = fs
        .lookup(parent.to_norm_u64(), name)?
        .ok_or_else(|| anyhow!("Source does not exist"))?;
    info!("rename - 1");

    let mut dest_exists = false;
    let mut dest_old_ino: u64 = 0;
    info!("rename - 2");

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
    info!("rename - 3");

    let src = {
        let ino = parent;
        let parent_oid = fs.parent_commit_build_session(ino)?;
        let build_root = fs.get_path_to_build_folder(ino)?;
        let repo = fs.get_repo(ino.to_norm_u64())?;
        let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let session = repo.get_or_init_build_session(parent_oid, &build_root)?;
        drop(repo);
        session.finish_path(fs, ino)?.join(name)
    };
    info!("rename - 4");
    info!("src path {}", src.display());

    let dest = if dst_in_build {
        let ino = new_parent;
        let parent_oid = fs.parent_commit_build_session(ino)?;
        let build_root = fs.get_path_to_build_folder(ino)?;
        let repo = fs.get_repo(ino.to_norm_u64())?;
        let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let session = repo.get_or_init_build_session(parent_oid, &build_root)?;
        drop(repo);
        session.finish_path(fs, ino)?.join(new_name)
    } else {
        fs.build_full_path(new_parent.to_norm_u64())?.join(new_name)
    };
    info!("rename - 5");
    info!("src path {}", dest.display());

    std::fs::rename(src, &dest)?;
    info!("rename - 6");

    fs.remove_db_record(src_attr.ino)?;
    info!("rename - 7");

    if dest_exists {
        fs.remove_db_record(dest_old_ino)?;
    }

    let mut new_attr = fs.attr_from_path(dest)?;
    info!("rename - 8");
    let new_ino = fs.next_inode_checked(new_parent.to_norm_u64())?;
    new_attr.ino = new_ino;
    info!("rename - 9");

    let nodes: Vec<(u64, String, FileAttr)> =
        vec![(new_parent.to_norm_u64(), new_name.to_string(), new_attr)];
    fs.write_inodes_to_db(nodes)?;

    Ok(())
}
