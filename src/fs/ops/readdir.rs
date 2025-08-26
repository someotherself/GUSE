use std::ffi::OsString;

use anyhow::{anyhow, bail};
use git2::Oid;

use crate::fs::{FileAttr, GitFs, REPO_SHIFT, fileattr::FileType};

pub struct DirectoryEntry {
    pub inode: u64,
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
    pub fn new(inode: u64, oid: Oid, name: String, kind: FileType, filemode: u32) -> Self {
        Self {
            inode,
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
enum DirCase {
    Month { year: i32, month: u32 },
    // Day { year: i32, month: u32, day: u32 },
    Commit { oid: Oid },
}

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

    entries.push(live_entry);
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

pub fn readdir_live_dir(fs: &GitFs, ino: u64) -> anyhow::Result<Vec<DirectoryEntry>> {
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
        let attr = fs.find_by_name(ino, &node_name_str)?;
        let Some(attr) = attr else { continue };
        let entry = DirectoryEntry::new(
            attr.inode,
            Oid::zero(),
            node_name_str.into(),
            kind,
            filemode,
        );
        entries.push(entry);
    }
    Ok(entries)
}

// Two branches
// 1 - ino is for a month folder -> show days folders
// 2 - ino is for a commit or inside a commit -> show commit contents
fn classify_inode(fs: &GitFs, ino: u64) -> anyhow::Result<DirCase> {
    let attr = fs.getattr(ino)?;
    let target_name = fs.get_name_from_db(ino)?;

    if attr.kind == FileType::Directory && attr.oid.is_zero() {
        // Branch 1
        if let Some((y, m)) = target_name.split_once('-')
            && let (Ok(year), Ok(month)) = (y.parse::<i32>(), m.parse::<u32>())
        {
            return Ok(DirCase::Month { year, month });
        }
    }

    // Branch 3
    // Will be a commit_id for the root folder of the commit
    // Or a Tree or Blob for anything inside
    Ok(DirCase::Commit { oid: attr.oid })
}

pub fn readdir_git_dir(fs: &GitFs, ino: u64) -> anyhow::Result<Vec<DirectoryEntry>> {
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
                repo.list_tree(commit_oid, Some(tree_oid))?
            }
        }
    };

    let mut nodes: Vec<(u64, String, FileAttr)> = vec![];

    let mut entries: Vec<DirectoryEntry> = vec![];
    for entry in git_objects {
        let dir_entry = match fs.exists_by_name(ino, &entry.name)? {
            Some(i) => {
                let mut attr = fs.object_to_file_attr(i, &entry)?;
                if attr.kind == FileType::Directory {
                    attr.perm = 0o555;
                }
                DirectoryEntry::new(i, entry.oid, entry.name.clone(), attr.kind, entry.filemode)
            }
            None => {
                let entry_ino = fs.next_inode_checked(ino)?;
                let mut attr = fs.object_to_file_attr(entry_ino, &entry)?;
                if attr.kind == FileType::Directory {
                    attr.perm = 0o555;
                }
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
    fs.write_inodes_to_db(nodes)?;
    Ok(entries)
}
