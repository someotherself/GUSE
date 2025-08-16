use std::ffi::OsString;

use anyhow::bail;
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
    // The the attributes in the normal struct
    pub entry: DirectoryEntry,
    // Plus the file attributes
    pub attr: FileAttr,
}

pub fn readdir_root_dir(fs: &GitFs) -> anyhow::Result<Vec<DirectoryEntry>> {
    let mut entries: Vec<DirectoryEntry> = vec![];
    for repo in fs.repos_list.values() {
        let (repo_dir, repo_ino) = {
            let repo = repo
                .lock()
                .map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
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
    if fs.repos_list.contains_key(&repo_id) {
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
            let repo = repo
                .lock()
                .map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
            repo.read_log()?
        };
        let mut nodes: Vec<(u64, String, FileAttr)> = vec![];
        for commit in object_entries {
            let entry_ino = fs.next_inode(ino)?;
            let mut attr = fs.object_to_file_attr(entry_ino, &commit)?;
            attr.perm = 0o555;
            let entry = DirectoryEntry::new(
                entry_ino,
                attr.oid,
                commit.name.clone(),
                attr.kind,
                attr.mode,
            );
            nodes.push((ino, commit.name, attr));
            entries.push(entry);
        }
        fs.write_inodes_to_db(nodes)?;
        Ok(entries)
    } else {
        bail!("Repo is not found!");
    }
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
        let attr = match attr {
            Some(attr) => attr,
            None => continue,
        };
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

pub fn readdir_git_dir(fs: &GitFs, ino: u64) -> anyhow::Result<Vec<DirectoryEntry>> {
    let repo = fs.get_repo(ino)?;
    let (commit_oid, _) = fs.find_commit_in_gitdir(ino)?;
    let oid = fs.get_oid_from_db(ino)?;

    // If parent ino is gitdir
    let parent_tree_oid = if oid == commit_oid {
        // parent tree_oid is the commit.tree_oid()
        let repo = repo
            .lock()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
        let commit = repo.inner.find_commit(commit_oid)?;
        commit.tree_id()
    } else {
        // else, get parent oid from db
        fs.get_oid_from_db(ino)?
    };

    let git_objects = if parent_tree_oid == commit_oid {
        let repo = repo
            .lock()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
        repo.list_tree(commit_oid, None)?
    } else {
        let repo = repo
            .lock()
            .map_err(|e| anyhow::anyhow!("lock poisoned: {e}"))?;
        repo.list_tree(commit_oid, Some(parent_tree_oid))?
    };
    let mut nodes: Vec<(u64, String, FileAttr)> = vec![];

    let mut entries: Vec<DirectoryEntry> = vec![];
    for entry in git_objects {
        let entry_ino = fs.next_inode(ino)?;
        let mut attr = fs.object_to_file_attr(entry_ino, &entry)?;
        attr.inode = entry_ino;
        if attr.kind == FileType::Directory {
            attr.perm = 0o555;
        }
        let dir_entry = DirectoryEntry::new(
            entry_ino,
            entry.oid,
            entry.name.clone(),
            attr.kind,
            entry.filemode,
        );

        nodes.push((ino, entry.name, attr));
        entries.push(dir_entry);
    }
    fs.write_inodes_to_db(nodes)?;
    Ok(entries)
}
