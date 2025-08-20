#![allow(unused_variables)]
use std::{fs::OpenOptions, sync::Arc};

use anyhow::anyhow;

use crate::fs::{GitFs, Handle, SourceTypes};

pub fn open_live(
    fs: &GitFs,
    ino: u64,
    read: bool,
    write: bool,
    truncate: bool,
) -> anyhow::Result<u64> {
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
    ino: u64,
    read: bool,
    write: bool,
    truncate: bool,
) -> anyhow::Result<u64> {
    let oid = fs.get_oid_from_db(ino)?;
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
