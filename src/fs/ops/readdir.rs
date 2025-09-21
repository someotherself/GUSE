use std::{collections::btree_map::Entry, ffi::OsString, path::Path};

use anyhow::{anyhow, bail};
use git2::{FileMode, ObjectType, Oid};

use crate::{
    fs::{
        FileAttr, GitFs,
        builds::BuildOperationCtx,
        fileattr::{FileType, InoFlag, ObjectAttr, StorageNode, dir_attr, file_attr},
    },
    inodes::{NormalIno, VirtualIno},
};

#[derive(Debug)]
pub struct DirectoryEntry {
    pub ino: u64,
    pub oid: Oid,
    pub name: String,
    pub kind: FileType,
    pub git_mode: u32,
}

impl DirectoryEntry {
    pub fn new(ino: u64, oid: Oid, name: String, kind: FileType, git_mode: u32) -> Self {
        Self {
            ino,
            oid,
            name,
            kind,
            git_mode,
        }
    }
}

pub struct DirectoryEntryPlus {
    pub entry: DirectoryEntry,
    pub attr: FileAttr,
}

impl From<ObjectAttr> for DirectoryEntry {
    fn from(attr: ObjectAttr) -> Self {
        let kind = match attr.kind {
            ObjectType::Blob => FileType::RegularFile,
            ObjectType::Tree => FileType::Directory,
            ObjectType::Commit => FileType::RegularFile,
            ObjectType::Tag => FileType::RegularFile,
            _ => FileType::RegularFile,
        };

        DirectoryEntry {
            ino: 0, // default as requested
            oid: attr.oid,
            name: attr.name,
            kind,
            git_mode: attr.git_mode,
        }
    }
}

#[derive(Debug)]
pub enum DirCase {
    /// The month folders
    Month { year: i32, month: u32 },
    /// Everything else
    /// Can be a commit, tree or Oid::zero()
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

// TODO: DOUBLE CHECK
pub fn readdir_repo_dir(fs: &GitFs, parent: NormalIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let parent = parent.to_norm_u64();
    let repo_id = GitFs::ino_to_repo_id(parent);

    if !fs.repos_list.contains_key(&repo_id) {
        bail!("Repo not found!")
    }

    let mut entries: Vec<DirectoryEntry> = vec![];

    let live_ino = fs.get_ino_from_db(parent, "live")?;
    let live_entry = DirectoryEntry::new(
        live_ino,
        Oid::zero(),
        "live".to_string(),
        FileType::Directory,
        libc::S_IFDIR,
    );

    let build_ino = fs.get_ino_from_db(parent, "build")?;
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
        let repo = fs.get_repo(parent)?;
        let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        repo.month_folders()?
    };

    let mut nodes: Vec<StorageNode> = vec![];
    if !object_entries.is_empty() {
        for month in object_entries {
            let dir_entry = match fs.exists_by_name(parent, &month.name)? {
                Some(i) => DirectoryEntry::new(
                    i,
                    Oid::zero(),
                    month.name.clone(),
                    FileType::Directory,
                    month.git_mode,
                ),
                None => {
                    let entry_ino = fs.next_inode_checked(parent)?;
                    let mut attr: FileAttr = dir_attr(InoFlag::MonthFolder).into();
                    attr.ino = entry_ino;
                    nodes.push(StorageNode {
                        parent_ino: parent,
                        name: month.name.clone(),
                        attr: attr.into(),
                    });
                    DirectoryEntry::new(
                        entry_ino,
                        attr.oid,
                        month.name.clone(),
                        attr.kind,
                        attr.git_mode,
                    )
                }
            };
            entries.push(dir_entry);
        }
    }
    fs.write_inodes_to_db(nodes)?;
    Ok(entries)
}

// Live folder persists between sessions
// Always get metadata from disk and update DB
// Performance is not a priority
pub fn readdir_live_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let ino = u64::from(ino);
    let ignore_list = [
        OsString::from("build"),
        OsString::from(".git"),
        OsString::from("fs_meta.db"),
    ];
    let path = fs.get_live_path(ino.into())?;
    let mut entries: Vec<DirectoryEntry> = vec![];
    let mut nodes: Vec<StorageNode> = vec![];
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
        let mut attr = fs.refresh_medata_using_path(node.path(), InoFlag::InsideLive)?;
        // It is reasonable to expect the user could add entries bypassing fuse
        match fs.get_ino_from_db(ino, &node_name_str) {
            Ok(ino) => attr.ino = ino,
            Err(_) => {
                let new_ino = fs.next_inode_checked(ino)?;
                attr.ino = new_ino;
                nodes.push(StorageNode {
                    parent_ino: ino,
                    name: node_name_str.clone().into(),
                    attr: attr.into(),
                });
            }
        };
        let entry =
            DirectoryEntry::new(attr.ino, Oid::zero(), node_name_str.into(), kind, filemode);
        entries.push(entry);
    }
    fs.write_inodes_to_db(nodes)?;
    Ok(entries)
}

// Two branches
// 1 - ino is for a month folder -> show days folders
// 2 - ino is for a commit or inside a commit -> show commit contents
pub fn classify_inode(fs: &GitFs, ino: u64) -> anyhow::Result<DirCase> {
    let mode = fs.get_mode_from_db(ino.into())?;
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

// Performance is a priority
// Build folder does not persist on disk
// Get metadata from DB, do not check files on disk for metadata
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
        let entry_ino = fs.get_ino_from_db(ino.into(), &node_name_str)?;
        let entry =
            DirectoryEntry::new(entry_ino, Oid::zero(), node_name_str.into(), kind, filemode);
        out.push(entry);
    }
    Ok(out)
}

pub fn readdir_git_dir(fs: &GitFs, parent: NormalIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let ino_flag = fs.get_ino_flag_from_db(parent)?;
    let repo = fs.get_repo(parent.into())?;
    let dir_entries = match ino_flag {
        InoFlag::MonthFolder => {
            // The objects are Snap folders
            let Ok(DirCase::Month { year, month }) = classify_inode(fs, parent.to_norm_u64())
            else {
                bail!("")
            };
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            let objects = repo.month_commits(&format!("{year:04}-{month:02}"))?;
            drop(repo);
            objects_to_dir_entries(fs, parent, objects, InoFlag::SnapFolder)?
        }
        InoFlag::SnapFolder => {
            // The Oid will be a commit oid
            // Will also contain everything in the build folder
            let oid = fs.get_oid_from_db(parent.into())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            let objects = repo.list_tree(oid, None)?;
            drop(repo);
            let mut dir_entries = objects_to_dir_entries(fs, parent, objects, InoFlag::InsideSnap)?;
            let build_objects = read_build_dir(fs, parent)?;
            dir_entries.extend(build_objects);
            dir_entries
        }
        InoFlag::InsideSnap => {
            // The Oid will be a tree oid
            // Is one of the folders (Tree) inside Snap. Only list git objects in it
            let oid = fs.get_oid_from_db(parent.into())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            let objects = repo.list_tree(oid, Some(oid)).unwrap_or_default();
            drop(repo);
            objects_to_dir_entries(fs, parent, objects, InoFlag::InsideSnap)?
        }
        InoFlag::InsideBuild | InoFlag::BuildRoot => {
            // Only contains the build folder
            // InoFlag::BuildRoot - only happens when accessing the build folder from RepoRoot
            read_build_dir(fs, parent)?
        }
        _ => {
            tracing::error!("WRONG BRANCH");
            bail!("Wrong ino_flag")
        }
    };
    Ok(dir_entries)
}

/// Takes in Vec<ObjectAttr> and converts them to Vec<DirectoryEntry>
///
///  Checks if they exist in DB and assigns ino according.
fn objects_to_dir_entries(
    fs: &GitFs,
    parent: NormalIno,
    objects: Vec<ObjectAttr>,
    ino_flag: InoFlag,
) -> anyhow::Result<Vec<DirectoryEntry>> {
    let mut nodes: Vec<StorageNode> = vec![];
    let mut dir_entries: Vec<DirectoryEntry> = vec![];
    for entry in objects {
        let ino = match fs.exists_by_name(parent.to_norm_u64(), &entry.name)? {
            Some(i) => i,
            None => {
                let ino = fs.next_inode_checked(parent.to_norm_u64())?;
                let mut attr: FileAttr = match entry.kind {
                    ObjectType::Tree | ObjectType::Commit => dir_attr(ino_flag).into(),
                    _ => file_attr(ino_flag).into(),
                };
                attr.oid = entry.oid;
                attr.ino = ino;
                attr.size = entry.size;
                nodes.push(StorageNode {
                    parent_ino: parent.to_norm_u64(),
                    name: entry.name.clone(),
                    attr: attr.into(),
                });
                ino
            }
        };
        let mut dir_entry: DirectoryEntry = entry.into();
        dir_entry.ino = ino;
        dir_entries.push(dir_entry);
    }
    fs.write_inodes_to_db(nodes)?;
    Ok(dir_entries)
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
    let parent = fs.get_path_from_db(ino.to_norm())?;
    let file_ext = match parent.extension().unwrap_or_default().to_str() {
        Some(e) => format!(".{e}"),
        None => String::new(),
    };

    if is_empty {
        let mut nodes: Vec<StorageNode> = vec![];
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
                let attr =
                    fs.object_to_file_attr(entry.0, &entry.1.clone(), InoFlag::InsideSnap)?;
                nodes.push(StorageNode {
                    parent_ino: ino.to_norm_u64(),
                    name: name.clone(),
                    attr: attr.into(),
                });
            }
            dir_entries.push(DirectoryEntry::new(
                entry.0,
                entry.1.oid,
                name.clone(),
                FileType::RegularFile,
                entry.1.git_mode,
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
                entry.git_mode,
            ));
        }
    }
    Ok(dir_entries)
}
