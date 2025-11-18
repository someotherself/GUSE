use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    sync::Arc,
};

use anyhow::bail;
use git2::{FileMode, ObjectType, Oid};

use crate::{
    fs::{
        CHASE_FOLDER, FileAttr, GitFs, LIVE_FOLDER,
        fileattr::{FileType, InoFlag, ObjectAttr, dir_attr, file_attr},
        repo::git2time_to_system,
    },
    inodes::{NormalIno, VirtualIno},
    namespec,
};

pub struct DirectoryStreamCookie {
    pub next_name: Option<OsString>,
    pub last_stream: Vec<OsString>,
    pub dir_stream: Option<Arc<[DirectoryEntry]>>,
}

#[derive(Debug, Clone)]
pub struct DirectoryEntry {
    pub ino: u64,
    pub oid: Oid,
    pub name: OsString,
    pub kind: FileType,
    pub git_mode: u32,
}

impl DirectoryEntry {
    pub fn new(ino: u64, oid: Oid, name: OsString, kind: FileType, git_mode: u32) -> Self {
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
            ObjectType::Commit => FileType::Directory,
            ObjectType::Tag => FileType::RegularFile,
            _ => FileType::RegularFile,
        };

        DirectoryEntry {
            ino: 0,
            oid: attr.oid,
            name: attr.name,
            kind,
            git_mode: attr.git_mode,
        }
    }
}

pub struct BuildCtxMetadata {
    pub mode: git2::FileMode,
    pub oid: Oid,
    pub name: OsString,
    pub ino_flag: InoFlag,
}

#[derive(Debug)]
pub enum DirCase {
    /// The month folders
    Month {
        year: i32,
        month: u32,
    },
    Pr,
    PrMerge,
    Branches,
    Tags,
    /// Everything else
    /// Can be a commit, tree or Oid::zero()
    Commit {
        oid: Oid,
    },
}

pub fn readdir_root_dir(fs: &GitFs) -> anyhow::Result<Vec<DirectoryEntry>> {
    let mut entries: Vec<DirectoryEntry> = vec![];
    for repo in fs.repos_list.iter().map(|e| e.value().clone()) {
        let (repo_dir, repo_ino) = { (repo.repo_dir.clone(), GitFs::repo_id_to_ino(repo.repo_id)) };
        let dir_entry = DirectoryEntry::new(
            repo_ino,
            Oid::zero(),
            OsString::from(repo_dir),
            FileType::Directory,
            libc::S_IFDIR,
        );
        entries.push(dir_entry);
    }
    Ok(entries)
}

// We are in repo root. This should show:
// . .. MONTH MONTH MONTH Tags Branches Pr Prmerge chase live
pub fn readdir_repo_dir(fs: &GitFs, parent: NormalIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let parent = parent.to_norm_u64();
    let repo_id = GitFs::ino_to_repo_id(parent);

    if !fs.repos_list.contains_key(&repo_id) {
        bail!("Repo does not exist. ID: {}", repo_id)
    }

    let mut entries: Vec<DirectoryEntry> = vec![];

    // Add the chase folder
    let Ok(chase_ino) = fs.get_ino_from_db(parent, OsStr::new(CHASE_FOLDER)) else {
        tracing::error!("Chase entry not found");
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    let chase_entry = DirectoryEntry::new(
        chase_ino,
        Oid::zero(),
        OsString::from(CHASE_FOLDER),
        FileType::Directory,
        libc::S_IFDIR,
    );

    // Add the live folder
    let Ok(live_ino) = fs.get_ino_from_db(parent, OsStr::new(LIVE_FOLDER)) else {
        tracing::error!("Live entry not found");
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    let live_entry = DirectoryEntry::new(
        live_ino,
        Oid::zero(),
        OsString::from(LIVE_FOLDER),
        FileType::Directory,
        libc::S_IFDIR,
    );

    entries.push(live_entry);
    entries.push(chase_entry);

    // Add the MONTH folders
    let repo = fs.get_repo(parent)?;
    let object_entries = { repo.month_folders()? };

    let mut nodes: Vec<FileAttr> = vec![];
    if !object_entries.is_empty() {
        for (_, month) in object_entries {
            let dir_entry = match fs.exists_by_name(parent, &month.name) {
                Ok(i) => DirectoryEntry::new(
                    i,
                    Oid::zero(),
                    month.name,
                    FileType::Directory,
                    month.git_mode,
                ),
                Err(_) => {
                    let entry_ino = fs.next_inode_checked(parent)?;
                    let attr = FileAttr::new(
                        dir_attr(InoFlag::MonthFolder),
                        entry_ino,
                        &month.name,
                        parent,
                        Oid::zero(),
                        None,
                    );
                    let entry = DirectoryEntry::new(
                        entry_ino,
                        attr.oid,
                        month.name,
                        attr.kind,
                        attr.git_mode,
                    );
                    nodes.push(attr);
                    entry
                }
            };
            entries.push(dir_entry);
        }
    }

    // Add the Tags/Brances/Pr/PrMerge (if they exist)
    let mut folders = Vec::new();
    repo.with_state(|s| {
        for rf in s.unique_namespaces.iter() {
            match rf.as_str() {
                "Branches" => folders.push((OsString::from("Branches"), InoFlag::BranchesRoot)),
                "Tags" => folders.push((OsString::from("Tags"), InoFlag::TagsRoot)),
                "PrMerge" => {
                    // Only push Pr is there PrMerges too
                    folders.push((OsString::from("Pr"), InoFlag::PrRoot));
                    folders.push((OsString::from("PrMerge"), InoFlag::PrMergeRoot));
                }
                _ => continue,
            }
        }
    });
    for (ref_name, flag) in folders {
        let dir_entry = match fs.exists_by_name(parent, &ref_name) {
            Ok(i) => DirectoryEntry::new(
                i,
                Oid::zero(),
                ref_name.clone(),
                FileType::Directory,
                libc::S_IFDIR,
            ),
            Err(_) => {
                let entry_ino = fs.next_inode_checked(parent)?;
                let attr = FileAttr::new(
                    dir_attr(flag),
                    entry_ino,
                    &ref_name,
                    parent,
                    Oid::zero(),
                    None,
                );
                let entry =
                    DirectoryEntry::new(entry_ino, attr.oid, ref_name, attr.kind, attr.git_mode);
                nodes.push(attr);
                entry
            }
        };
        entries.push(dir_entry);
    }

    fs.write_inodes_to_db(nodes)?;
    Ok(entries)
}

// Live folder persists between sessions
// Always get metadata from disk and update DB
// Performance is not a priority
pub fn readdir_live_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let ino = u64::from(ino);
    let path = fs.get_live_path(ino.into())?;
    let mut entries: Vec<DirectoryEntry> = vec![];
    let mut nodes: Vec<FileAttr> = vec![];
    for node in path.read_dir()? {
        let node = node?;
        let node_name = node.file_name();
        let (kind, filemode) = if node.file_type()?.is_dir() {
            (FileType::Directory, libc::S_IFDIR)
        } else {
            (FileType::RegularFile, libc::S_IFREG)
        };

        let mut attr = fs.refresh_medata_using_path(node.path(), InoFlag::InsideLive)?;
        let ino = match fs.get_ino_from_db(ino, &node_name) {
            Ok(ino) => {
                attr.ino = ino;
                ino
            }
            Err(_) => {
                let new_ino = fs.next_inode_checked(ino)?;
                attr.ino = new_ino;
                attr.parent_ino = ino;
                nodes.push(attr);
                new_ino
            }
        };
        let entry = DirectoryEntry::new(ino, Oid::zero(), node_name, kind, filemode);
        entries.push(entry);
    }
    fs.write_inodes_to_db(nodes)?;
    entries.sort_unstable_by(|a, b| a.name.as_encoded_bytes().cmp(b.name.as_encoded_bytes()));
    Ok(entries)
}

// Two branches
// 1 - ino is for a month folder -> show days folders
// 2 - ino is for a commit or inside a commit -> show commit contents
pub fn classify_inode(meta: &BuildCtxMetadata) -> anyhow::Result<DirCase> {
    if (meta.mode == FileMode::Tree || meta.mode == FileMode::Commit)
        && meta.oid == Oid::zero()
        && let Some((y, m)) = namespec::split_once_os(&meta.name, b'-')
        && let (Some(year), Some(month)) = (namespec::parse_i32_os(&y), namespec::parse_u32_os(&m))
    {
        return Ok(DirCase::Month { year, month });
    }

    // Add Pr, Pr-Merge, Branch, Tags
    if meta.ino_flag == InoFlag::PrRoot {
        return Ok(DirCase::Pr);
    };
    if meta.ino_flag == InoFlag::PrMergeRoot {
        return Ok(DirCase::PrMerge);
    }
    if meta.ino_flag == InoFlag::BranchesRoot {
        return Ok(DirCase::Branches);
    }
    if meta.ino_flag == InoFlag::TagsRoot {
        return Ok(DirCase::Tags);
    }

    // Will be a commit_id for the root folder of the commit
    // Or a Tree or Blob for anything inside
    Ok(DirCase::Commit { oid: meta.oid })
}

// Performance is a priority
// Build folder does not persist on disk
// Get metadata from DB, do not check files on disk for metadata
fn read_build_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let mut out = Vec::new();

    let ino_flag = fs.get_ino_flag_from_db(ino)?;
    let mut entries = fs.read_children(ino)?;

    if ino_flag == InoFlag::SnapFolder {
        let snap_oid = fs.get_oid_from_db(ino.into())?;
        entries.retain(|e| e.oid == snap_oid);
    };
    out.extend(entries);
    Ok(out)
}

pub fn build_chase_path(fs: &GitFs, ino: NormalIno) -> anyhow::Result<PathBuf> {
    let repo = fs.get_repo(ino.into())?;
    let chase_folder = repo.chase_dir.as_path();

    let mut out: Vec<OsString> = vec![];

    let mut cur_ino = ino.to_norm_u64();
    let mut cur_flag = fs.get_ino_flag_from_db(cur_ino.into())?;
    let mut cur_name = fs.get_name_from_db(cur_ino)?;

    if cur_flag == InoFlag::ChaseRoot {
        return Ok(chase_folder.to_path_buf());
    }

    let max_loops = 1000;
    for _ in 0..max_loops {
        out.push(cur_name.clone());
        cur_ino = fs.get_single_parent(cur_ino)?;
        cur_flag = fs.get_ino_flag_from_db(cur_ino.into())?;
        cur_name = fs.get_name_from_db(cur_ino)?;
        if cur_flag == InoFlag::ChaseRoot {
            break;
        }
    }

    out.reverse();
    Ok(chase_folder.join(out.iter().collect::<PathBuf>()))
}

fn populate_entries_by_path(
    fs: &GitFs,
    ino: NormalIno,
    path: &Path,
) -> anyhow::Result<Vec<DirectoryEntry>> {
    let mut out: Vec<DirectoryEntry> = Vec::new();
    for node in path.read_dir()? {
        let node = node?;
        let node_name = node.file_name();
        let (kind, filemode) = if node.file_type()?.is_dir() {
            (FileType::Directory, libc::S_IFDIR)
        } else {
            (FileType::RegularFile, libc::S_IFREG)
        };
        let Ok(entry_ino) = fs.exists_by_name(ino.into(), &node_name) else {
            continue;
        };
        let entry = DirectoryEntry::new(entry_ino, Oid::zero(), node_name, kind, filemode);
        out.push(entry);
    }
    Ok(out)
}

pub fn build_dot_git_path(fs: &GitFs, target_ino: NormalIno) -> anyhow::Result<PathBuf> {
    let repo_path = {
        let repo_dir = fs.get_repo(target_ino.into())?.repo_dir.clone();
        fs.repos_dir.join(repo_dir)
    };
    let dot_git_path = repo_path.join(LIVE_FOLDER).join(".git");

    let mut out: Vec<OsString> = vec![];

    let mut cur_ino = target_ino.to_norm_u64();
    let mut cur_name = fs.get_name_from_db(cur_ino)?;

    if cur_name == ".git" {
        return Ok(dot_git_path);
    }

    let max_loops = 1000;
    for _ in 0..max_loops {
        out.push(cur_name.clone());
        cur_ino = fs.get_single_parent(cur_ino)?;
        cur_name = fs.get_name_from_db(cur_ino)?;
        if cur_name == ".git" {
            break;
        }
    }

    out.reverse();
    Ok(dot_git_path.join(out.iter().collect::<PathBuf>()))
}

fn read_inside_dot_git(fs: &GitFs, parent_ino: NormalIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let mut entries: Vec<DirectoryEntry> = vec![];
    let mut nodes: Vec<FileAttr> = vec![];

    let path = build_dot_git_path(fs, parent_ino)?;
    for node in path.read_dir()? {
        let node = node?;
        let node_name = node.file_name();

        let ino_flag = if node_name == "HEAD" {
            InoFlag::HeadFile
        } else {
            InoFlag::InsideDotGit
        };
        let (kind, filemode) = if node.file_type()?.is_dir() {
            (FileType::Directory, libc::S_IFDIR)
        } else {
            (FileType::RegularFile, libc::S_IFREG)
        };

        let mut attr = fs.refresh_medata_using_path(node.path(), ino_flag)?;

        let ino = match fs.get_ino_from_db(parent_ino.into(), &node_name) {
            Ok(ino) => {
                attr.ino = ino;
                ino
            }
            Err(_) => {
                let new_ino = fs.next_inode_checked(parent_ino.into())?;
                attr.ino = new_ino;
                attr.parent_ino = parent_ino.into();
                nodes.push(attr);
                new_ino
            }
        };

        let entry = DirectoryEntry::new(ino, Oid::zero(), node_name, kind, filemode);
        entries.push(entry);
    }

    fs.write_inodes_to_db(nodes)?;
    entries.sort_unstable_by(|a, b| a.name.as_encoded_bytes().cmp(b.name.as_encoded_bytes()));
    Ok(entries)
}

fn dot_git_root(fs: &GitFs, parent_ino: u64) -> anyhow::Result<DirectoryEntry> {
    let perms = 0o775;
    let st_mode = libc::S_IFDIR | perms;

    let name = OsStr::new(".git");
    let entry_ino = match fs.exists_by_name(parent_ino, name) {
        Ok(ino) => ino,
        Err(_) => {
            let ino = fs.next_inode_checked(parent_ino)?;
            let attr = FileAttr::new(
                dir_attr(InoFlag::DotGitRoot),
                ino,
                name,
                parent_ino,
                Oid::zero(),
                None,
            );
            fs.write_inodes_to_db(vec![attr])?;
            ino
        }
    };
    let entry: DirectoryEntry = DirectoryEntry {
        ino: entry_ino,
        oid: Oid::zero(),
        name: name.to_os_string(),
        kind: FileType::Directory,
        git_mode: st_mode,
    };
    Ok(entry)
}

pub fn readdir_git_dir(fs: &GitFs, parent: NormalIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let metadata = fs.get_builctx_metadata(parent)?;
    let repo = fs.get_repo(parent.into())?;
    let mut dir_entries = match metadata.ino_flag {
        InoFlag::MonthFolder => {
            // The objects are Snap folders
            let Ok(DirCase::Month { year, month }) = classify_inode(&metadata) else {
                bail!("Invalid MONTH folder name")
            };
            let objects = repo.month_commits(&format!("{year:04}-{month:02}"))?;
            objects_to_dir_entries(fs, parent, objects, InoFlag::SnapFolder)?
        }
        InoFlag::BranchesRoot | InoFlag::PrRoot => {
            // Snaps folders (number of the PR)
            let objects = repo.non_branch_folders(metadata.ino_flag)?;
            let flag = if metadata.ino_flag == InoFlag::BranchesRoot {
                InoFlag::BranchFolder
            } else {
                InoFlag::PrFolder
            };
            objects_to_dir_entries(fs, parent, objects, flag)?
        }
        InoFlag::TagsRoot | InoFlag::PrMergeRoot => {
            let objects = repo.non_branch_folders(metadata.ino_flag)?;
            objects_to_dir_entries(fs, parent, objects, InoFlag::SnapFolder)?
        }
        // Treat Branch folder separately
        // Try to find merge_base with main and list Snap folders if succesfull
        // If it fails, list everything in MONTH folders
        InoFlag::PrFolder | InoFlag::BranchFolder => {
            let objects = repo.branch_snaps(&metadata.name, metadata.ino_flag)?;
            objects_to_dir_entries(fs, parent, objects, InoFlag::SnapFolder)?
        }
        InoFlag::SnapFolder => {
            // The Oid will be a commit oid
            // Will also contain everything in the build folder
            let objects = repo.list_tree(metadata.oid, None)?;
            // git objects
            let mut dir_entries = objects_to_dir_entries(fs, parent, objects, InoFlag::InsideSnap)?;
            // build files/folders
            let build_objects = read_build_dir(fs, parent)?;
            dir_entries.extend(build_objects);
            // .git folder
            dir_entries.push(dot_git_root(fs, parent.into())?);

            dir_entries
        }
        InoFlag::InsideSnap => {
            // The Oid will be a tree oid
            // Is one of the folders (Tree) inside Snap. Only list git objects in it
            let commit_oid = fs.get_parent_commit(parent.into())?;
            let objects = repo
                .list_tree(commit_oid, Some(metadata.oid))
                .unwrap_or_default();
            objects_to_dir_entries(fs, parent, objects, InoFlag::InsideSnap)?
        }
        InoFlag::InsideBuild => {
            // Only contains the build folder
            read_build_dir(fs, parent)?
        }
        InoFlag::InsideChase | InoFlag::ChaseRoot => {
            // read_chase_dir(fs, parent)?
            let path = build_chase_path(fs, parent)?;
            populate_entries_by_path(fs, parent, &path)?
        }
        InoFlag::DotGitRoot | InoFlag::InsideDotGit => read_inside_dot_git(fs, parent)?,
        _ => {
            tracing::error!("WRONG BRANCH");
            bail!("Wrong ino_flag")
        }
    };
    dir_entries.sort_unstable_by(|a, b| a.name.as_encoded_bytes().cmp(b.name.as_encoded_bytes()));
    Ok(dir_entries)
}

/// Takes in Vec<ObjectAttr> and converts them to Vec<DirectoryEntry>
///
/// Checks if they exist in DB and assigns ino according.
fn objects_to_dir_entries(
    fs: &GitFs,
    parent: NormalIno,
    objects: Vec<ObjectAttr>,
    ino_flag: InoFlag,
) -> anyhow::Result<Vec<DirectoryEntry>> {
    let mut nodes: Vec<FileAttr> = vec![];
    let mut dir_entries: Vec<DirectoryEntry> = vec![];
    for entry in objects {
        let ino = match fs.exists_by_name(parent.to_norm_u64(), &entry.name) {
            Ok(i) => i,
            Err(_) => {
                let ino = fs.next_inode_checked(parent.to_norm_u64())?;
                let create = match entry.kind {
                    ObjectType::Tree | ObjectType::Commit => dir_attr(ino_flag),
                    _ => file_attr(ino_flag),
                };
                let mut attr = FileAttr::new(
                    create,
                    ino,
                    &entry.name,
                    parent.to_norm_u64(),
                    entry.oid,
                    None,
                );
                attr.size = entry.size;
                attr.atime = git2time_to_system(entry.commit_time);
                attr.mtime = git2time_to_system(entry.commit_time);
                attr.ctime = git2time_to_system(entry.commit_time);
                nodes.push(attr);
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

pub fn read_virtual_dir(fs: &GitFs, ino: VirtualIno) -> anyhow::Result<Vec<DirectoryEntry>> {
    let repo = fs.get_repo(u64::from(ino))?;

    let mut dir_entries = vec![];

    let v_node_opt = repo.with_state(|s| s.vdir_cache.get(&ino).cloned());
    let v_node = match v_node_opt {
        Some(o) => o,
        None => bail!("Oid missing"),
    };
    for (name, (ino, entry)) in v_node.log.iter() {
        dir_entries.push(DirectoryEntry::new(
            *ino,
            entry.oid,
            name.clone(),
            FileType::RegularFile,
            entry.git_mode,
        ));
    }
    Ok(dir_entries)
}
