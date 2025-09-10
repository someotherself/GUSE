use std::{collections::btree_map::Entry, ffi::OsString, path::Path};

use anyhow::{anyhow, bail};
use git2::{FileMode, Oid};
use tracing::{instrument, Level};

use crate::{
    fs::{
        FileAttr, GitFs, REPO_SHIFT,
        builds::BuildOperationCtx,
        fileattr::{FileType, ObjectAttr},
    },
    inodes::{Inodes, NormalIno, VirtualIno},
};

#[derive(Debug)]
pub struct DirectoryEntry {
    pub ino: u64,
    // The git Oid (SHA-1)
    pub oid: Oid,
    // The real filename
    pub name: String,
    // File (Blob), Dir (Tree), or Symlink
    pub kind: FileType,
    // Mode (permissions)
    pub filemode: u32,
}

impl DirectoryEntry {
    pub fn new(ino: u64, oid: Oid, name: String, kind: FileType, filemode: u32) -> Self {
        Self {
            ino,
            oid,
            name,
            kind,
            filemode,
        }
    }
}

pub struct DirectoryEntryPlus {
    pub entry: DirectoryEntry,
    pub attr: FileAttr,
}

#[derive(Debug)]
pub enum DirCase {
    /// The month folders
    Month { year: i32, month: u32 },
    /// Everything else
    /// Can be a commit, tree or Oid::zero()
    Commit { oid: Oid },
}

#[instrument(level = "debug", skip(fs), ret(level = Level::DEBUG), err(Display))]
pub fn readdir_root_dir(fs: &GitFs) -> anyhow::Result<Vec<DirectoryEntry>> {
    let mut entries: Vec<DirectoryEntry> = vec![];
    for repo in fs.repos_list.values() {
        let (repo_dir, repo_ino) = {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            (repo.repo_dir.clone(), GitFs::repo_id_to_ino(repo.repo_id))
        };
        let dir_entry = DirectoryEntry::new(
            repo_ino,
            Oid::zero(),
            repo_dir,
            FileType::Directory,
            libc::S_IFDIR,
        );
        entries.push(dir_entry);
    }
    Ok(entries)
}

#[instrument(level = "debug", skip(fs), fields(ino = %ino), ret(level = Level::DEBUG), err(Display))]
pub fn readdir_repo_dir(fs: &GitFs, ino: u64) -> anyhow::Result<Vec<DirectoryEntry>> {
    let repo_id = (ino >> REPO_SHIFT) as u16;

    if !fs.repos_list.contains_key(&repo_id) {
        bail!("Repo not found!")
    }

    let mut entries: Vec<DirectoryEntry> = vec![];

    let live_ino = fs.get_ino_from_db(ino, "live")?;
    let live_entry = DirectoryEntry::new(
        live_ino,
        Oid::zero(),
        "live".to_string(),
        FileType::Directory,
        libc::S_IFDIR,
    );

    let build_ino = fs.get_ino_from_db(ino, "build")?;
    let build_entry = DirectoryEntry::new(
        build_ino,
        Oid::zero(),
        "build".to_string(),
        FileType::Directory,
        libc::S_IFDIR,
    );

    entries.push(live_entry);
    entries.push(build_entry);

    let object_entries = {
        let repo = fs.get_repo(ino)?;
        let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        repo.month_folders()?
    };

    if !object_entries.is_empty() {
        let mut nodes: Vec<(u64, String, FileAttr)> = vec![];
        for month in object_entries {
            let dir_entry = match fs.exists_by_name(ino, &month.name)? {
                Some(i) => {
                    let mut attr = fs.object_to_file_attr(i, &month)?;
                    attr.perm = 0o555;
                    DirectoryEntry::new(i, attr.oid, month.name.clone(), attr.kind, attr.mode)
                }
                None => {
                    let entry_ino = fs.next_inode_checked(ino)?;
                    let mut attr = fs.object_to_file_attr(entry_ino, &month)?;
                    attr.perm = 0o555;
                    nodes.push((ino, month.name.clone(), attr));
                    DirectoryEntry::new(
                        entry_ino,
                        attr.oid,
                        month.name.clone(),
                        attr.kind,
                        attr.mode,
                    )
                }
            };
            entries.push(dir_entry);
        }
        fs.write_inodes_to_db(nodes)?;
    }
    Ok(entries)
}

#[instrument(level = "debug", skip(fs), fields(ino = %ino), ret(level = Level::DEBUG), err(Display))]
pub fn readdir_live_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let ino = u64::from(ino);
    let ignore_list = [OsString::from(".git"), OsString::from("fs_meta.db")];
    let path = fs.build_full_path(ino)?;
    let mut entries: Vec<DirectoryEntry> = vec![];
    for node in path.read_dir()? {
        let node = node?;
        let node_name = node.file_name();
        let node_name_str = node_name.to_string_lossy();
        if ignore_list.contains(&node_name) {
            continue;
        }
        let (kind, filemode) = if node.file_type()?.is_dir() {
            (FileType::Directory, libc::S_IFDIR)
        } else if node.file_type()?.is_file() {
            (FileType::RegularFile, libc::S_IFREG)
        } else {
            (FileType::Symlink, libc::S_IFLNK)
        };
        let attr = fs.lookup(ino, &node_name_str)?;
        let Some(attr) = attr else { continue };
        let entry =
            DirectoryEntry::new(attr.ino, Oid::zero(), node_name_str.into(), kind, filemode);
        entries.push(entry);
    }
    Ok(entries)
}

// Two branches
// 1 - ino is for a month folder -> show days folders
// 2 - ino is for a commit or inside a commit -> show commit contents
pub fn classify_inode(fs: &GitFs, ino: u64) -> anyhow::Result<DirCase> {
    let mode = fs.get_mode_from_db(ino)?;
    let oid = fs.get_oid_from_db(ino)?;
    let target_name = fs.get_name_from_db(ino)?;
    if (mode == FileMode::Tree || mode == FileMode::Commit) && oid == Oid::zero() {
        // Branch 1
        if let Some((y, m)) = target_name.split_once('-')
            && let (Ok(year), Ok(month)) = (y.parse::<i32>(), m.parse::<u32>())
        {
            return Ok(DirCase::Month { year, month });
        }
    }

    // Branch 2
    // Will be a commit_id for the root folder of the commit
    // Or a Tree or Blob for anything inside
    Ok(DirCase::Commit { oid })
}

fn read_build_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let mut out = Vec::new();

    let Some(ctx) = BuildOperationCtx::new(fs, ino)? else {
        return Ok(out);
    };

    let entries = populate_build_entries(fs, ino, &ctx.path())?;
    out.extend(entries);
    Ok(out)
}

fn populate_build_entries(
    fs: &GitFs,
    ino: NormalIno,
    build_path: &Path,
) -> anyhow::Result<Vec<DirectoryEntry>> {
    let mut out: Vec<DirectoryEntry> = Vec::new();

    for node in build_path.read_dir()? {
        let node = node?;
        let node_name = node.file_name();
        let node_name_str = node_name.to_string_lossy();
        let (kind, filemode) = if node.file_type()?.is_dir() {
            (FileType::Directory, libc::S_IFDIR)
        } else if node.file_type()?.is_file() {
            (FileType::RegularFile, libc::S_IFREG)
        } else {
            (FileType::Symlink, libc::S_IFLNK)
        };
        let entry_ino = fs.get_ino_from_db(ino.to_norm_u64(), &node_name_str)?;
        let entry =
            DirectoryEntry::new(entry_ino, Oid::zero(), node_name_str.into(), kind, filemode);
        out.push(entry);
    }
    Ok(out)
}

#[instrument(level = "debug", skip(fs), fields(ino = %ino), ret(level = Level::DEBUG), err(Display))]
pub fn readdir_git_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let ino = ino.to_norm_u64();
    let repo = fs.get_repo(ino)?;
    let git_objects = match classify_inode(fs, ino)? {
        DirCase::Month { year, month } => {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.month_commits(&format!("{year:04}-{month:02}"))?
        }
        DirCase::Commit { oid } => {
            let (commit_oid, _) = fs.get_parent_commit(ino)?;
            // The root of a commit will have the commit_id as attr.oid
            if commit_oid == oid {
                // parent tree_oid is the commit.tree_oid()
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.list_tree(commit_oid, None)?
            } else {
                // else, get parent oid from db
                let tree_oid = oid;
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.list_tree(commit_oid, Some(tree_oid)).unwrap_or_default()
            }
        }
    };

    let mut nodes: Vec<(u64, String, FileAttr)> = vec![];

    let mut entries: Vec<DirectoryEntry> = vec![];
    for entry in git_objects {
        let dir_entry = match fs.exists_by_name(ino, &entry.name)? {
            Some(i) => {
                let attr = fs.object_to_file_attr(i, &entry)?;
                DirectoryEntry::new(i, entry.oid, entry.name.clone(), attr.kind, entry.filemode)
            }
            None => {
                let entry_ino = fs.next_inode_checked(ino)?;
                let attr = fs.object_to_file_attr(entry_ino, &entry)?;
                nodes.push((ino, entry.name.clone(), attr));
                DirectoryEntry::new(
                    entry_ino,
                    entry.oid,
                    entry.name.clone(),
                    attr.kind,
                    entry.filemode,
                )
            }
        };
        entries.push(dir_entry);
    }
    drop(repo);

    let inode: Inodes = ino.into();
    let build_nodes = read_build_dir(fs, inode.to_norm())?;
    entries.extend(build_nodes);

    fs.write_inodes_to_db(nodes)?;
    Ok(entries)
}

fn get_history_objects(fs: &GitFs, ino: u64, oid: Oid) -> anyhow::Result<Vec<ObjectAttr>> {
    let repo = fs.get_repo(ino)?;
    let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
    repo.blob_history_objects(oid)
}

fn log_entries(
    fs: &GitFs,
    ino: u64,
    origin_oid: Oid,
) -> anyhow::Result<Vec<(String, (u64, ObjectAttr))>> {
    let entries = get_history_objects(fs, ino, origin_oid)?;

    let mut log_entries: Vec<(String, (u64, ObjectAttr))> = vec![];
    for e in entries {
        let new_ino = fs.next_inode_checked(ino)?;
        log_entries.push((e.name.clone(), (new_ino, e)));
    }
    Ok(log_entries)
}

pub fn read_virtual_dir(fs: &GitFs, ino: VirtualIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let repo = fs.get_repo(u64::from(ino))?;
    let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
    let v_node = match repo.vdir_cache.get(&ino) {
        Some(o) => o,
        None => bail!("Oid missing"),
    };
    let origin_oid = v_node.oid;
    let is_empty = v_node.log.is_empty();
    drop(repo);

    let mut dir_entries = vec![];
    let parent = fs.get_path_from_db(ino.to_virt_u64())?;
    let file_ext = match parent.extension().unwrap_or_default().to_str() {
        Some(e) => format!(".{e}"),
        None => String::new(),
    };

    if is_empty {
        let mut nodes: Vec<(u64, String, FileAttr)> = vec![];
        let log_entries = log_entries(fs, ino.to_norm_u64(), origin_oid)?;
        let repo = fs.get_repo(u64::from(ino))?;
        let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let v_node = match repo.vdir_cache.get_mut(&ino) {
            Some(o) => o,
            None => bail!("Oid missing"),
        };
        for (name, entry) in log_entries {
            let name = format!("{name}{file_ext}");
            if let Entry::Vacant(e) = v_node.log.entry(name.clone()) {
                e.insert(entry.clone());
                let mut attr = fs.object_to_file_attr(entry.0, &entry.1.clone())?;
                attr.perm = 0o555;
                nodes.push((ino.to_norm_u64(), name.clone(), attr));
            }
            dir_entries.push(DirectoryEntry::new(
                entry.0,
                entry.1.oid,
                name.clone(),
                FileType::RegularFile,
                entry.1.filemode,
            ));
        }
        drop(repo);
        fs.write_inodes_to_db(nodes)?;
    } else {
        let repo = fs.get_repo(u64::from(ino))?;
        let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let v_node = match repo.vdir_cache.get(&ino) {
            Some(o) => o,
            None => bail!("Oid missing"),
        };
        for (ino, entry) in v_node.log.values() {
            let name = format!("{}{file_ext}", entry.name);
            dir_entries.push(DirectoryEntry::new(
                *ino,
                entry.oid,
                name.clone(),
                FileType::RegularFile,
                entry.filemode,
            ));
        }
    }
    Ok(dir_entries)
}
