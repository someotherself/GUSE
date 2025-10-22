#![allow(unused_variables)]
use std::{
    fs::OpenOptions,
    sync::{Arc, OnceLock},
    time::{Duration, UNIX_EPOCH},
};

use anyhow::{anyhow, bail};
use git2::{Oid, Time};

use crate::{
    fs::{
        GitFs, Handle, SourceTypes, VFileEntry,
        fileattr::{InoFlag, ObjectAttr},
        ops::readdir::{DirCase, build_dot_git_path, classify_inode},
    },
    inodes::{Inodes, NormalIno, VirtualIno},
    namespec,
};

pub fn open_live(
    fs: &GitFs,
    ino: NormalIno,
    read: bool,
    write: bool,
    truncate: bool,
) -> anyhow::Result<u64> {
    let ino = u64::from(ino);
    let path = fs.get_live_path(ino.into())?;
    let file = OpenOptions::new()
        .read(true)
        .write(write)
        .truncate(write && truncate)
        .open(path)?;
    let handle = Handle {
        ino,
        source: SourceTypes::RealFile(Arc::new(file)),
        write,
    };
    fs.handles.open(handle)
}

pub fn open_git(
    fs: &GitFs,
    ino: NormalIno,
    read: bool,
    write: bool,
    truncate: bool,
) -> anyhow::Result<u64> {
    let metadata = fs.get_builctx_metadata(ino)?;
    match metadata.ino_flag {
        InoFlag::InsideBuild => {
            let file = match fs.take_file_from_cache(ino.into()) {
                Ok(file) => file,
                Err(_) => {
                    let repo = fs.get_repo(ino.to_norm_u64())?;
                    let build_root = &repo.build_dir;
                    let dentry = fs.get_single_dentry(ino.into())?;
                    let session = repo.get_or_init_build_session(metadata.oid, build_root)?;
                    let path = session
                        .finish_path(fs, dentry.parent_ino.into())?
                        .join(&dentry.target_name);
                    // Always open with write(true) because the file is cached
                    // Let the Handle decide if write is allowed
                    let open_file = OpenOptions::new()
                        .read(true)
                        .write(true)
                        .truncate(write && truncate)
                        .open(path)?;

                    // Cache the file
                    if let Ok(cloned_file) = open_file.try_clone() {
                        let file_clone = SourceTypes::RealFile(Arc::new(cloned_file));
                        repo.file_cache.insert(ino.into(), file_clone);
                    }

                    SourceTypes::RealFile(Arc::new(open_file))
                }
            };

            let handle = Handle {
                ino: ino.to_norm_u64(),
                source: file,
                write,
            };
            fs.handles.open(handle)
        }
        InoFlag::InsideDotGit => {
            let file = {
                let path = build_dot_git_path(fs, ino)?;
                let open_file = OpenOptions::new().read(true).open(path)?;
                SourceTypes::RealFile(Arc::new(open_file))
            };
            let handle = Handle {
                ino: ino.to_norm_u64(),
                source: file,
                write: false,
            };
            fs.handles.open(handle)
        }
        InoFlag::HeadFile => {
            let file = {
                let commit = fs.get_parent_commit(ino.into())?.to_string();
                let mut contents: Vec<u8> = vec![];
                contents.extend_from_slice(commit.as_bytes());
                contents.push(b'\n');
                SourceTypes::RoBlob {
                    oid: metadata.oid,
                    data: Arc::new(contents),
                }
            };
            let handle = Handle {
                ino: ino.to_norm_u64(),
                source: file,
                write: false,
            };
            fs.handles.open(handle)
        }
        _ => open_blob(fs, metadata.oid, ino.to_norm_u64(), read),
    }
}

pub fn open_vfile(fs: &GitFs, ino: Inodes, read: bool, write: bool) -> anyhow::Result<u64> {
    let metadata = fs.get_builctx_metadata(ino.to_norm())?;
    let res = classify_inode(&metadata)?;
    match res {
        DirCase::Month { year, month } => {
            let mut contents = {
                let map = fs
                    .vfile_entry
                    .read()
                    .map_err(|_| anyhow!("Lock poisoned"))?;
                map.get(&ino.to_virt()).and_then(|e| e.data.get()).cloned()
            };
            if contents.is_none() {
                let entries = {
                    let repo = fs.get_repo(ino.to_u64_n())?;
                    repo.month_commits(&format!("{year:04}-{month:02}"))?
                };
                contents = Some(build_commits_text(fs, entries, ino.to_u64_n())?);
            }
            let data = contents.ok_or_else(|| anyhow!("No data"))?;
            let blob_file = SourceTypes::RoBlob {
                oid: Oid::zero(),
                data,
            };
            let handle = Handle {
                ino: ino.to_u64_v(),
                source: blob_file,
                write: false,
            };
            fs.handles.open(handle)
        }
        DirCase::Commit { oid } => {
            let mut contents = {
                let map = fs
                    .vfile_entry
                    .read()
                    .map_err(|_| anyhow!("Lock poisoned"))?;
                map.get(&ino.to_virt()).and_then(|e| e.data.get()).cloned()
            };
            if contents.is_none() {
                let repo = fs.get_repo(ino.to_u64_n())?;
                let summary = repo.with_repo(|r| -> anyhow::Result<String> {
                    let commit = r.find_commit(oid)?;
                    let summary = commit.summary().unwrap_or_default().to_owned();
                    Ok(summary)
                })?;
                contents = Some(Arc::new(Vec::from(summary.as_bytes())));
            }
            let data = contents.ok_or_else(|| anyhow!("No data"))?;
            let blob_file = SourceTypes::RoBlob {
                oid: Oid::zero(),
                data,
            };
            let handle = Handle {
                ino: ino.to_u64_v(),
                source: blob_file,
                write: false,
            };
            fs.handles.open(handle)
        }
    }
}

/// Saved the file in the vfile_entry and returns the size of the content
pub fn create_vfile_entry(fs: &GitFs, ino: VirtualIno) -> anyhow::Result<u64> {
    let metadata = fs.get_builctx_metadata(ino.to_norm())?;
    let res = classify_inode(&metadata)?;
    let (entry, len) = match res {
        DirCase::Month { year, month } => {
            let entries = {
                let repo = fs.get_repo(ino.to_norm_u64())?;
                repo.month_commits(&format!("{year:04}-{month:02}"))?
            };
            let contents = build_commits_text(fs, entries, ino.to_norm_u64())?;
            let data = OnceLock::new();
            let _ = data.set(contents.clone());
            let len = contents.len() as u64;
            let entry = VFileEntry {
                kind: crate::fs::VFile::Month,
                len,
                data,
            };
            (entry, len)
        }
        DirCase::Commit { oid } => {
            let repo = fs.get_repo(ino.into())?;
            let summary = repo.with_repo(|r| -> anyhow::Result<String> {
                let commit = r.find_commit(oid)?;
                let summary = commit.summary().unwrap_or_default().to_owned();
                Ok(summary)
            })?;
            let data = OnceLock::new();
            let len = summary.len() as u64;
            let entry = VFileEntry {
                kind: crate::fs::VFile::Commit,
                len,
                data,
            };
            (entry, len)
        }
    };
    {
        let mut guard = fs
            .vfile_entry
            .write()
            .map_err(|_| anyhow!("Lock poisoned"))?;
        guard.insert(ino, entry);
    }
    Ok(len)
}

pub fn open_vdir(
    fs: &GitFs,
    ino: NormalIno,
    read: bool,
    write: bool,
    truncate: bool,
    parent: VirtualIno,
) -> anyhow::Result<u64> {
    let name = fs.get_name_from_db(ino.into())?;
    let repo = fs.get_repo(ino.into())?;
    let v_node_opt = repo.with_state(|s| s.vdir_cache.get(&parent).cloned());
    let Some(v_node) = v_node_opt else {
        bail!("File not found!")
    };
    let Some((_, object)) = v_node.log.get(&name) else {
        bail!("File not found!")
    };
    let oid = object.oid;
    open_blob(fs, oid, ino.into(), read)
}

fn open_blob(fs: &GitFs, oid: Oid, ino: u64, read: bool) -> anyhow::Result<u64> {
    let buf = {
        let repo = fs.get_repo(ino)?;
        repo.with_repo(|r| -> anyhow::Result<Vec<u8>> {
            let blob = r.find_blob(oid)?;
            Ok(blob.content().to_vec())
        })?
    };
    let blob_file = SourceTypes::RoBlob {
        oid,
        data: Arc::new(buf),
    };
    let handle = Handle {
        ino,
        source: blob_file,
        write: false,
    };
    fs.handles.open(handle)
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

fn build_commits_text(
    fs: &GitFs,
    entries: Vec<ObjectAttr>,
    ino: u64,
) -> anyhow::Result<Arc<Vec<u8>>> {
    let mut contents: Vec<u8> = Vec::new();

    for e in entries {
        let ts = git_commit_time(e.commit_time);
        let soid = short_oid(e.oid);
        let repo = fs.get_repo(ino)?;
        let (subject, committer) =
            repo.with_repo(|r| -> Result<(String, String), git2::Error> {
                let c = r.find_commit(e.oid)?;
                let subject = c.summary().unwrap_or_default().to_owned();
                let committer = c.author().name().unwrap_or_default().to_owned();
                Ok((subject, committer))
            })?;

        let clean_name = namespec::clean_name(&e.name);
        let clean_subject = subject.replace(['\n', '\t'], " ");

        let row = format!(
            "{ts}\t{soid}\t{}\t{committer}\t{clean_subject}\n",
            clean_name.display()
        );
        contents.extend_from_slice(row.as_bytes());
    }

    Ok(Arc::new(contents))
}
