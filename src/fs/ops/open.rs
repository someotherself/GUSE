#![allow(unused_variables)]
use std::{
    fs::OpenOptions,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::{anyhow, bail};
use git2::{Oid, Time};
use tracing::{Level, instrument};

use crate::{
    fs::{
        GitFs, Handle, SourceTypes,
        fileattr::ObjectAttr,
        ops::readdir::{DirCase, classify_inode},
    },
    inodes::{Inodes, NormalIno, VirtualIno},
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

pub fn open_git(fs: &GitFs, ino: NormalIno, read: bool, write: bool) -> anyhow::Result<u64> {
    let ino = u64::from(ino);
    let oid = fs.get_oid_from_db(ino)?;
    open_blob(fs, oid, ino, read)
}

#[instrument(level = "debug", skip(fs), fields(ino), ret(level = Level::DEBUG), err(Display))]
pub fn open_vfile(fs: &GitFs, ino: Inodes, read: bool, write: bool) -> anyhow::Result<u64> {
    let res = classify_inode(fs, ino.to_u64_v())?;
    match res {
        // In both cases
        // Create the summary and save it in a vec
        // Issue the handle
        DirCase::Month { year, month } => {
            let entries = {
                let repo = fs.get_repo(ino.to_u64_n())?;
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.month_commits(&format!("{year:04}-{month:02}"))?
            };
            let contents = build_commits_text(fs, entries, ino.to_u64_n())?;
            let blob_file = SourceTypes::RoBlob {
                oid: Oid::zero(),
                data: Arc::new(contents),
            };
            let fh = fs.next_file_handle();
            let handle = Handle {
                ino: ino.to_u64_v(),
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
        DirCase::Commit { oid } => {
            todo!()
        }
    }
}

#[instrument(level = "debug", skip(fs), fields(ino), ret(level = Level::DEBUG), err(Display))]
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

fn short_oid(oid: Oid) -> String {
    let s = oid.to_string();
    s[..7].to_string()
}

fn git_commit_time(t: Time) -> String {
    let secs = t.seconds() as u64;
    let st = UNIX_EPOCH + Duration::new(secs, 0);
    let dt = chrono::DateTime::<chrono::Utc>::from(st);
    dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn build_commits_text(fs: &GitFs, entries: Vec<ObjectAttr>, ino: u64) -> anyhow::Result<Vec<u8>> {
    // Pre-size roughly: header + ~100 bytes/row
    let mut out = String::with_capacity(64 + entries.len() * 128);
    out.push_str("iso8601_utc\tshort_oid\tfolder_name\tsubject\n");

    for e in entries {
        let ts = git_commit_time(e.commit_time);
        let soid = short_oid(e.oid);
        let subject = {
            let repo = fs.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.inner
                .find_commit(e.oid)
                .ok()
                .and_then(|c| c.summary().map(|s| s.to_string()))
                .unwrap_or_default()
        };

        // sanitize tabs/newlines so TSV stays one-line/row
        let clean_name = e.name.replace(['\n', '\t'], " ");
        let clean_subject = subject.replace(['\n', '\t'], " ");

        out.push_str("{ts}\t{soid}\t{clean_name}\t{clean_subject}");
    }

    Ok(out.into_bytes())
}
