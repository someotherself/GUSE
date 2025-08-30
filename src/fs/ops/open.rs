#![allow(unused_variables)]
use std::{fs::OpenOptions, sync::Arc};

use anyhow::{anyhow, bail};
use git2::Oid;

use crate::{
    fs::{GitFs, Handle, SourceTypes},
    inodes::{NormalIno, VirtualIno},
};

pub fn open_live(
    fs: &GitFs,
    ino: NormalIno,
    read: bool,
    write: bool,
    truncate: bool,
) -> anyhow::Result<u64> {
    let ino = u64::from(ino);
    let path = fs.build_full_path(ino)?;
    let file = OpenOptions::new()
        .read(read)
        .write(write)
        .truncate(truncate)
        .open(path)?;
    let fh = fs.next_file_handle();
    let handle = Handle {
        ino,
        file: SourceTypes::RealFile(file),
        read,
        write,
    };
    {
        let mut guard = fs.handles.write().map_err(|_| anyhow!("Lock poisoned"))?;
        guard.insert(fh, handle);
    }
    Ok(fh)
}

pub fn open_git(
    fs: &GitFs,
    ino: NormalIno,
    read: bool,
    write: bool,
    truncate: bool,
) -> anyhow::Result<u64> {
    let ino = u64::from(ino);
    let oid = fs.get_oid_from_db(ino)?;
    open_blob(fs, oid, ino, read)
}

pub fn open_vdir(
    fs: &GitFs,
    ino: NormalIno,
    read: bool,
    write: bool,
    truncate: bool,
    parent: VirtualIno,
) -> anyhow::Result<u64> {
    let ino = u64::from(ino);
    let name = fs.get_name_from_db(ino)?;
    let repo = fs.get_repo(ino)?;
    let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
    let Some(v_node) = repo.vdir_cache.get(&parent) else {
        tracing::error!("Open - no v_node for {} and {}", name, u64::from(parent));
        bail!("File not found!")
    };
    tracing::info!("{}", v_node.log.is_empty());
    let Some((_, object)) = v_node.log.get(&name) else {
        tracing::error!("Open - no log for {}", name);
        bail!("File not found!")
    };
    let oid = object.oid;
    drop(repo);
    open_blob(fs, oid, ino, read)
}

fn open_blob(fs: &GitFs, oid: Oid, ino: u64, read: bool) -> anyhow::Result<u64> {
    let buf = {
        let repo = fs.get_repo(ino)?;
        let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let blob = repo.inner.find_blob(oid)?;
        blob.content().to_vec()
    };
    let blob_file = SourceTypes::RoBlob {
        oid,
        data: Arc::new(buf),
    };
    let fh = fs.next_file_handle();
    let handle = Handle {
        ino,
        file: blob_file,
        read,
        write: false,
    };
    {
        let mut guard = fs.handles.write().map_err(|_| anyhow!("Lock poisoned"))?;
        guard.insert(fh, handle);
    }
    Ok(fh)
}
