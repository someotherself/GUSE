use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{Duration, UNIX_EPOCH};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

use anyhow::{Context, anyhow, bail};
use dashmap::DashMap;
use git2::{FileMode, ObjectType, Oid};
use tracing::{Level, field, info, instrument};

use crate::fs;
use crate::fs::fileattr::{
    CreateFileAttr, Dentry, FileAttr, FileType, InoFlag, ObjectAttr, SetFileAttr, StorageNode,
    dir_attr, file_attr,
};
use crate::fs::handles::FileHandles;
use crate::fs::meta_db::{DbReturn, DbWriteMsg, MetaDb, oneshot, set_conn_pragmas, set_wal_once};
use crate::fs::ops::readdir::{
    BuildCtxMetadata, DirectoryEntry, DirectoryEntryPlus, DirectoryStreamCookie,
};
use crate::fs::repo::{GitRepo, State};
use crate::inodes::{Inodes, NormalIno, VirtualIno};
use crate::internals::cache::LruCache;
use crate::internals::cache_dentry::DentryLru;
use crate::mount::InvalMsg;
use crate::namespec::NameSpec;

pub mod builds;
pub mod fileattr;
pub mod handles;
pub mod meta_db;
pub mod ops;
pub mod repo;

#[cfg(test)]
mod test;

const META_STORE: &str = "fs_meta.db";
const LIVE_FOLDER: &str = "live";
const BUILD_FOLDER: &str = "build";
const CHASE_FOLDER: &str = "chase";
const TEMP_FOLDER: &str = ".temp";
pub const REPO_SHIFT: u8 = 48;
pub const ROOT_INO: u64 = 1;
pub const VDIR_BIT: u64 = 1u64 << 47;
const ATTR_LRU: usize = 12000;
const DENTRY_LRU: usize = 12000;
const FILE_LRU: usize = 800;

enum FsOperationContext {
    /// Is the root directory
    Root,
    /// Is one of the directories holding a repo
    RepoDir,
    /// Dir or File inside the live dir
    InsideLiveDir,
    /// Dir or File inside a repo dir
    InsideGitDir,
}

impl FsOperationContext {
    fn get_operation(fs: &GitFs, inode: Inodes) -> anyhow::Result<Self> {
        let ino = u64::from(inode.to_norm());
        let mask: u64 = (1u64 << 48) - 1;
        let repo_dir = GitFs::ino_to_repo_id(ino);
        if ino == ROOT_INO {
            Ok(FsOperationContext::Root)
        } else if ino & mask == 0 && fs.repos_list.contains_key(&repo_dir) {
            // If the least significant 48 bits are 0
            Ok(FsOperationContext::RepoDir)
        } else if fs.is_in_live(inode.to_norm())? {
            Ok(FsOperationContext::InsideLiveDir)
        } else {
            Ok(FsOperationContext::InsideGitDir)
        }
    }
}

// Real disk structure
// MOUNT_POINT/
// repos/repo_dir1/
//---------├── .temp                <- used for storing temp files created during a session
//---------├── live/.git
//---------├── build/
//---------------└── build_<commit_oid>/    <- Will show in the Snap folder
//---------------------└── target/  <- Will show in the Snap folder for HASH (commit oid)
//---------└── meta_fs.db
//---------All other contents will show under /live
//
// Perceived disk structure
// repos/repo_dir1/
//---------├── live/            <- everything in repo_dir1 including for .git
//---------├── build/           <- used for running builds
//---------├── YYYY-MM/         <- List month groups where commits were made
//---------------├── Snaps_on_MM.DD.YYYY/   <- List day  groups where commits were made
//---------------└── Snaps_on_MM.DD.YYYY/   <-
//---------------------├── Snap001_OID      <- Commit
//---------------------├── Snap002_OID      <- Commit
//---------------------└── Snap003_OID      <- Commit
//---------├── YYYY-MM/  <-
//---------├── YYYY-MM/  <-
//---------└── YYYY-MM/  <-
//
// Structure of INODES
// Each repo has a repo_id--<16bits repo-id><48 bits for ino>
// repo_id for repo 1       0000000000000001
// ino for repo 1 root dir  0000000000000001000000000....0000
// ino for repo_1 live dir: 0000000000000001000000000....0001
// ino for repo_1 build dir:0000000000000001000000000....0010
// ino for repo_1 folder1:  0000000000000001000000000....0011
// ino for repo_1 folder2:  0000000000000001000000000....0100

// repo_id for repo 2       0000000000000000
// ino for repo 2 root dir  0000000000000010000000000....0000
// ino for repo_2 live dir: 0000000000000010000000000....0001
// ino for repo_2 build dir:0000000000000010000000000....0010
// ino for repo_2 folder1:  0000000000000010000000000....0011

// Virtual inodes
// The 48th bit is reserved for virtual inoded.
// A normal (Inodes::NormalIno) ino will have it set to 0
//                          <16bits repo-id><48 bits for ino>
// An ino of a real file    0000000000000001000000000....0111
// The Ino of the virt dir  0000000000000001100000000....0111
// The virtual directory can be accessed by adding @ at the end of the name
// and it is used for example, when trying to use cat on a file (or cd on a folder)

// ino for repo folder  = (repo_id as u64) << 48 (see REPO_SHIFT)
// repo_id from ino     = (ino >> REPO_SHIFT) as u16
pub struct GitFs {
    pub repos_dir: PathBuf,
    /// Use helpers `self.insert_repo` and `self.delete_repo`
    /// <repo_id, repo>
    repos_list: DashMap<u16, Arc<GitRepo>>,
    /// <repo_id, connections>
    conn_list: DashMap<u16, Arc<MetaDb>>,
    /// Use helpers `self.insert_repo` and `self.delete_repo`
    /// <repo_name, repo_id>
    repos_map: DashMap<String, u16>,
    /// Each Repo has a set of inodes
    next_inode: DashMap<u16, AtomicU64>,
    pub handles: FileHandles,
    read_only: bool,
    vfile_entry: RwLock<HashMap<VirtualIno, VFileEntry>>,
    notifier: crossbeam_channel::Sender<InvalMsg>,
}

pub struct Handle {
    pub ino: u64,
    pub source: SourceTypes,
    write: bool,
}

/// Used by `Handle` to hold various data, like files/blobs and directory entries (readdir)
#[derive(Clone)]
pub enum SourceTypes {
    RealFile(Arc<File>),
    Blob {
        oid: Oid,
        data: Arc<[u8]>,
    },
    /// Created by opendir, populated readdir with directory entries
    DirSnapshot {
        entries: Arc<parking_lot::Mutex<DirectoryStreamCookie>>,
    },
    Closed,
}

/// Used for creating virtual files.
///
/// These files are made usign commit data.
/// Data generated during getattr/lookup, served during open/read, deleted at release.
///
/// To read the files correctly, getattr and lookup need the content file size
#[derive(Clone)]
enum VFile {
    Month,
    Commit,
}

#[derive(Clone)]
struct VFileEntry {
    kind: VFile,
    len: u64,
    data: OnceLock<Arc<[u8]>>,
}

impl SourceTypes {
    #[inline]
    pub fn is_file(&self) -> bool {
        matches!(self, SourceTypes::RealFile(_))
    }

    #[inline]
    pub fn is_blob(&self) -> bool {
        matches!(self, SourceTypes::Blob { oid: _, data: _ })
    }

    #[inline]
    pub fn is_dir(&self) -> bool {
        matches!(self, SourceTypes::DirSnapshot { entries: _ })
    }

    pub fn try_clone(&self) -> anyhow::Result<Self> {
        match self {
            Self::RealFile(file) => Ok(Self::RealFile(Arc::new(file.try_clone()?))),
            _ => bail!(std::io::Error::from_raw_os_error(libc::EROFS)),
        }
    }

    pub fn trucate(&self, size: u64) -> anyhow::Result<()> {
        match self {
            Self::RealFile(file) => file.set_len(size).context("Failed to truncate the file"),
            _ => bail!(std::io::Error::from_raw_os_error(libc::EROFS)),
        }
    }

    pub fn size(&self) -> anyhow::Result<u64> {
        match self {
            Self::RealFile(file) => Ok(file.metadata()?.size()),
            Self::Blob { oid: _, data } => Ok(data.len() as u64),
            _ => bail!(std::io::Error::from_raw_os_error(libc::EROFS)),
        }
    }
}

impl FileExt for SourceTypes {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        match self {
            Self::RealFile(file) => file.read_at(buf, offset),
            Self::Blob { oid: _, data } => {
                let start = offset as usize;
                if start >= data.len() {
                    return Ok(0);
                }
                let end = (start + buf.len()).min(data.len());
                let src = &data.as_ref()[start..end];
                buf[..src.len()].copy_from_slice(src);
                Ok(src.len())
            }
            _ => Err(std::io::Error::from_raw_os_error(libc::EROFS)),
        }
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        match self {
            Self::RealFile(file) => file.write_at(buf, offset),
            _ => Err(std::io::Error::from_raw_os_error(libc::EROFS)),
        }
    }
}

// gitfs_fuse_functions
impl GitFs {
    pub fn new(
        repos_dir: PathBuf,
        read_only: bool,
        notifier: Arc<OnceLock<fuser::Notifier>>,
    ) -> anyhow::Result<Arc<Self>> {
        let (tx_inval, rx_inval) = crossbeam_channel::unbounded::<InvalMsg>();

        let fs = Self {
            repos_dir: repos_dir.clone(),
            repos_list: DashMap::new(),
            conn_list: DashMap::new(),
            repos_map: DashMap::new(),
            read_only,
            handles: FileHandles::default(),
            next_inode: DashMap::new(),
            vfile_entry: RwLock::new(HashMap::new()),
            notifier: tx_inval.clone(),
        };

        let fs = Arc::new(fs);

        std::thread::spawn(move || {
            while notifier.get().is_none() {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            let Some(n) = notifier.get() else {
                return;
            };
            for msg in rx_inval.iter() {
                match msg {
                    InvalMsg::Entry { parent, name } => {
                        if let Err(e) = n.inval_entry(parent, &name) {
                            tracing::debug!("inval_entry failed: {e}");
                        }
                    }
                    InvalMsg::Inode { ino, off, len } => {
                        if let Err(e) = n.inval_inode(ino, off, len) {
                            tracing::debug!("inval_inode failed: {e}");
                        }
                    }
                    InvalMsg::Delete {
                        parent,
                        child,
                        name,
                    } => {
                        if let Err(e) = n.delete(parent, child, &name) {
                            tracing::debug!("inval_delete failed: {e}");
                        }
                    }
                    InvalMsg::Store { ino, off, data } => {
                        if let Err(e) = n.store(ino, off, &data) {
                            tracing::debug!("inval_store failed: {e}");
                        }
                    }
                }
            }
        });

        fs.ensure_base_dirs_exist()?;
        for entry in repos_dir.read_dir()? {
            let entry = entry?;
            let repo_name_os = entry.file_name();
            let repo_name = repo_name_os.to_str().context("Not a valid UTF-8 name")?;
            let repo_path = entry.path();
            // TODO: Allow orphaned repos?
            if !repo_path.join(META_STORE).exists() {
                continue;
            }
            fs.load_repo(repo_name)?;
        }

        for (repo_id, repo) in fs.repos_list.clone() {
            let repo_ino = GitFs::repo_id_to_ino(repo_id);
            let live_ino = GitFs::get_live_ino(repo_ino);
            let repo_name = repo.repo_dir.clone();
            let live_path = repos_dir.join(&repo_name).join(LIVE_FOLDER);
            let chase_path = repos_dir.join(&repo_name).join(CHASE_FOLDER);

            // Read contents of live
            fs.read_dir_to_db(&live_path, &fs, InoFlag::InsideLive, live_ino)?;

            // Read contents of chase
            if let DbReturn::Found { value: chase_ino } =
                fs.exists_by_name(repo_ino, OsStr::new(CHASE_FOLDER))?
            {
                fs.read_dir_to_db(&chase_path, &fs, InoFlag::InsideChase, chase_ino)?;
            }
            // Discover contents of repo root
            let entries = fs::ops::readdir::readdir_repo_dir(&fs, repo_ino.into())?;
            // Discover contents until we reach the Snap folders
            for e1 in entries {
                let entries = fs.readdir(e1.ino)?;
                for e2 in entries {
                    if e2.kind != FileType::Directory {
                        continue;
                    };
                    if e2.name.as_bytes().starts_with(b"Snap") {
                        continue;
                    }
                    let _ = fs.readdir(e2.ino);
                }
            }
        }
        Ok(fs)
    }

    /// Loads the repo with empty database.
    fn load_repo_connection(&self, repo_name: &str) -> anyhow::Result<u16> {
        let repo_path = self.repos_dir.join(repo_name);

        // Assign repo id
        let repo_id = self.next_repo_id();
        let repo_ino = (repo_id as u64) << REPO_SHIFT;
        self.next_inode
            .insert(repo_id, AtomicU64::from(repo_ino + 1));

        let db_path = repo_path.join(META_STORE);
        // Initialize the database (create file and schema)
        self.init_meta_db(&db_path)?;
        // Initialize the ro_pool and writer_tx
        let db_conn = meta_db::new_repo_db(&db_path)?;
        self.conn_list.insert(repo_id, db_conn);

        let live_path = repo_path.join(LIVE_FOLDER);
        let build_dir = repo_path.join(BUILD_FOLDER);
        let chase_dir = repo_path.join(CHASE_FOLDER);
        let repo = git2::Repository::init(live_path)?;

        let _ = std::fs::create_dir(&build_dir);
        let _ = std::fs::create_dir(&chase_dir);

        let state: State = State {
            res_inodes: HashSet::new(),
            vdir_cache: BTreeMap::new(),
            build_sessions: HashMap::new(),
            snaps_map: HashMap::new(),
            refs_to_snaps: HashMap::new(),
            snaps_to_ref: HashMap::new(),
            unique_namespaces: HashSet::new(),
        };

        let git_repo = GitRepo {
            repo_dir: repo_name.to_owned(),
            build_dir,
            chase_dir,
            repo_id,
            inner: parking_lot::Mutex::new(repo),
            state: parking_lot::RwLock::new(state),
            attr_cache: LruCache::new(ATTR_LRU),
            dentry_cache: DentryLru::new(DENTRY_LRU),
            file_cache: LruCache::new(FILE_LRU),
            injected_files: DashMap::new(),
        };

        let repo_rc = Arc::from(git_repo);
        self.insert_repo(&repo_rc, repo_name, repo_id)?;
        Ok(repo_id)
    }

    /// Create folders and write entries in DB. Also clears the build folder
    ///
    /// It write ROOT_INO, Repo Root, Live and Build folder in db
    fn populate_repo_database(&self, repo_id: u16, repo_name: &str) -> anyhow::Result<()> {
        let repo_ino = GitFs::repo_id_to_ino(repo_id);
        let repo_path = self.repos_dir.join(repo_name);

        // Write the ROOT_INO in db
        self.db_ensure_root(repo_ino)?;

        // Write the Repo Root in db
        let mut repo_attr: FileAttr = dir_attr(InoFlag::RepoRoot).into();
        repo_attr.ino = repo_ino;
        let nodes: Vec<StorageNode> = vec![StorageNode {
            parent_ino: ROOT_INO,
            name: repo_name.into(),
            attr: repo_attr,
        }];
        self.write_inodes_to_db(nodes)?;

        // Clean the build folder
        let build_name = OsString::from(BUILD_FOLDER);
        let build_path = repo_path.join(&build_name);
        let _ = std::fs::remove_dir_all(&build_path);
        let chase_name = OsString::from(CHASE_FOLDER);

        // Create the temp folder
        let temp_path = repo_path.join(TEMP_FOLDER);
        let _ = std::fs::remove_dir_all(&temp_path);
        std::fs::create_dir(&temp_path)?;

        // Prepare the live and build folders
        let live_ino = self.next_inode_raw(repo_ino)?;
        let build_ino = self.next_inode_raw(repo_ino)?;
        let chase_ino = self.next_inode_raw(repo_ino)?;

        let live_name = OsString::from(LIVE_FOLDER);

        let perms = 0o775;
        let st_mode = libc::S_IFDIR | perms;

        let mut live_attr: FileAttr = dir_attr(InoFlag::LiveRoot).into();
        live_attr.ino = live_ino;
        live_attr.git_mode = st_mode;

        let mut build_attr: FileAttr = dir_attr(InoFlag::BuildRoot).into();
        build_attr.ino = build_ino;
        build_attr.git_mode = st_mode;

        let mut chase_attr: FileAttr = dir_attr(InoFlag::ChaseRoot).into();
        chase_attr.ino = chase_ino;
        chase_attr.git_mode = st_mode;

        let repo = self.get_repo(repo_ino)?;
        repo.refresh_refs()?;
        repo.with_state_mut(|s| {
            s.res_inodes.insert(live_ino);
            s.res_inodes.insert(build_ino);
            s.res_inodes.insert(chase_ino);
        });

        let nodes: Vec<StorageNode> = vec![
            StorageNode {
                parent_ino: repo_ino,
                name: live_name,
                attr: live_attr,
            },
            StorageNode {
                parent_ino: repo_ino,
                name: chase_name,
                attr: chase_attr,
            },
            StorageNode {
                parent_ino: repo_ino,
                name: build_name,
                attr: build_attr,
            },
        ];

        self.write_inodes_to_db(nodes)?;

        // Create build folder again
        std::fs::create_dir(&build_path)?;

        Ok(())
    }

    fn read_dir_to_db(
        &self,
        path: &Path,
        fs: &GitFs,
        ino_flag: InoFlag,
        parent_ino: u64,
    ) -> anyhow::Result<()> {
        let mut stack: Vec<(PathBuf, u64)> = vec![(path.into(), parent_ino)];
        while let Some((cur_path, cur_parent)) = stack.pop() {
            let mut nodes: Vec<StorageNode> = vec![];
            for entry in cur_path.read_dir()? {
                let entry = entry?;
                let mut attr = self.refresh_medata_using_path(entry.path(), ino_flag)?;
                let ino = self.next_inode_checked(cur_parent)?;

                if entry.file_type()?.is_dir() {
                    stack.push((entry.path(), ino));
                }

                attr.ino = ino;
                nodes.push(StorageNode {
                    parent_ino: cur_parent,
                    name: entry.file_name(),
                    attr,
                });
            }
            fs.write_inodes_to_db(nodes)?;
        }
        Ok(())
    }

    pub fn load_repo(&self, repo_name: &str) -> anyhow::Result<()> {
        let repo_id = self.load_repo_connection(repo_name)?;

        self.populate_repo_database(repo_id, repo_name)?;
        Ok(())
    }

    pub fn new_repo(&self, repo_name: &str, url: Option<&str>) -> anyhow::Result<Arc<GitRepo>> {
        let tmpdir = tempfile::Builder::new()
            .rand_bytes(4)
            .tempdir_in(&self.repos_dir)?;
        let repo_id = self.new_repo_connection(repo_name, tmpdir.path())?;
        self.fetch_repo(repo_id, repo_name, tmpdir.path(), url)
    }

    fn new_repo_connection(&self, repo_name: &str, tmpdir: &Path) -> anyhow::Result<u16> {
        // Assign repo id
        let repo_id = self.next_repo_id();
        let repo_ino = (repo_id as u64) << REPO_SHIFT;
        self.next_inode
            .insert(repo_id, AtomicU64::from(repo_ino + 1));

        let db_path = tmpdir.join(META_STORE);
        // Initialize the database (create file and schema)
        self.init_meta_db(&db_path)?;
        // Initialize the ro_pool and writer_tx
        let db_conn = meta_db::new_repo_db(&db_path)?;
        self.conn_list.insert(repo_id, db_conn);

        // Create the live folder on disk (real, current path)
        let live_path = tmpdir.join(LIVE_FOLDER);
        // Save the build and chase dir paths (future path, after tmp rename)
        let build_dir = self.repos_dir.join(repo_name).join(BUILD_FOLDER);
        let chase_dir = self.repos_dir.join(repo_name).join(CHASE_FOLDER);
        std::fs::create_dir(&live_path)?;
        let repo = git2::Repository::init(live_path)?;

        let state: State = State {
            res_inodes: HashSet::new(),
            vdir_cache: BTreeMap::new(),
            build_sessions: HashMap::new(),
            snaps_map: HashMap::new(),
            refs_to_snaps: HashMap::new(),
            snaps_to_ref: HashMap::new(),
            unique_namespaces: HashSet::new(),
        };

        let git_repo = GitRepo {
            repo_dir: repo_name.to_owned(),
            build_dir,
            chase_dir,
            repo_id,
            inner: parking_lot::Mutex::new(repo),
            state: parking_lot::RwLock::new(state),
            attr_cache: LruCache::new(ATTR_LRU),
            dentry_cache: DentryLru::new(DENTRY_LRU),
            file_cache: LruCache::new(FILE_LRU),
            injected_files: DashMap::new(),
        };

        let repo_rc = Arc::from(git_repo);
        self.insert_repo(&repo_rc, repo_name, repo_id)?;
        Ok(repo_id)
    }

    fn fetch_repo(
        &self,
        repo_id: u16,
        repo_name: &str,
        tmp_path: &Path,
        url: Option<&str>,
    ) -> anyhow::Result<Arc<GitRepo>> {
        let repo_ino = GitFs::repo_id_to_ino(repo_id);

        // Write the ROOT_INO in db
        self.db_ensure_root(repo_ino)?;

        // Write the Repo Root in db
        let mut repo_attr: FileAttr = dir_attr(InoFlag::RepoRoot).into();
        repo_attr.ino = repo_ino;
        let nodes: Vec<StorageNode> = vec![StorageNode {
            parent_ino: ROOT_INO,
            name: repo_name.into(),
            attr: repo_attr,
        }];
        self.write_inodes_to_db(nodes)?;

        // Prepare the live and chase folders

        let live_ino = self.next_inode_raw(repo_ino)?;
        let build_ino = self.next_inode_raw(repo_ino)?;
        let chase_ino = self.next_inode_raw(repo_ino)?;

        let chase_name = OsString::from(CHASE_FOLDER);
        let build_name = OsString::from(BUILD_FOLDER);
        let live_name = OsString::from(LIVE_FOLDER);

        let perms = 0o775;
        let st_mode = libc::S_IFDIR | perms;

        let mut live_attr: FileAttr = dir_attr(InoFlag::LiveRoot).into();
        live_attr.ino = live_ino;
        live_attr.git_mode = st_mode;

        let mut build_attr: FileAttr = dir_attr(InoFlag::BuildRoot).into();
        build_attr.ino = build_ino;
        build_attr.git_mode = st_mode;

        // Create build folder on disk
        std::fs::create_dir(tmp_path.join(BUILD_FOLDER))?;

        let mut chase_attr: FileAttr = dir_attr(InoFlag::ChaseRoot).into();
        chase_attr.ino = chase_ino;
        chase_attr.git_mode = st_mode;

        // Create build folder on disk
        std::fs::create_dir(tmp_path.join(CHASE_FOLDER))?;

        // Create the temp folder
        let temp_path = tmp_path.join(TEMP_FOLDER);
        let _ = std::fs::remove_dir_all(&temp_path);
        std::fs::create_dir(&temp_path)?;

        let repo = self.get_repo(repo_ino)?;
        repo.with_state_mut(|s| {
            s.res_inodes.insert(live_ino);
            s.res_inodes.insert(build_ino);
            s.res_inodes.insert(chase_ino);
        });

        let nodes: Vec<StorageNode> = vec![
            StorageNode {
                parent_ino: repo_ino,
                name: live_name,
                attr: live_attr,
            },
            StorageNode {
                parent_ino: repo_ino,
                name: chase_name,
                attr: chase_attr,
            },
            StorageNode {
                parent_ino: repo_ino,
                name: build_name,
                attr: build_attr,
            },
        ];

        self.write_inodes_to_db(nodes)?;

        if let Some(url) = url {
            repo.fetch_anon(url)?;
            repo.refresh_refs()?;
        };

        let final_path = self.repos_dir.join(repo_name);
        std::fs::rename(tmp_path, &final_path)?;

        {
            let live_path = final_path.join(LIVE_FOLDER);
            // Refresh repo and db to new path
            let repo = self.get_repo(repo_ino)?;
            repo.with_repo_mut(|r| -> anyhow::Result<()> {
                *r = git2::Repository::init(&live_path)?;
                Ok(())
            })?;
            let db_conn = meta_db::new_repo_db(final_path.join(META_STORE))?;
            self.conn_list.insert(repo_id, db_conn);
        }

        // Discover contents of repo root
        let entries = fs::ops::readdir::readdir_repo_dir(self, repo_ino.into())?;
        // Discover contents until we reach the Snap folders
        for e1 in entries {
            let entries = self.readdir(e1.ino)?;
            for e2 in entries {
                if e2.kind != FileType::Directory {
                    continue;
                };
                if e2.name.as_bytes().starts_with(b"Snap") {
                    continue;
                }
                let _ = self.readdir(e2.ino);
            }
        }

        Ok(repo)
    }

    /// Must take in the name of the folder of the REPO --
    /// data_dir/repo_name1
    ///
    ///------------------├── fs_meta.db
    ///
    ///------------------└── .git/
    pub fn init_meta_db<P: AsRef<Path>>(&self, db_path: P) -> anyhow::Result<()> {
        let dbp = db_path.as_ref();

        if dbp.exists() {
            std::fs::remove_file(dbp)?;
            let wal = dbp.with_extension(format!(
                "{}-wal",
                dbp.extension().and_then(|s| s.to_str()).unwrap_or("")
            ));
            let shm = dbp.with_extension(format!(
                "{}-shm",
                dbp.extension().and_then(|s| s.to_str()).unwrap_or("")
            ));
            let _ = std::fs::remove_file(dbp.with_extension("db-wal"));
            let _ = std::fs::remove_file(dbp.with_extension("db-shm"));
            let _ = std::fs::remove_file(wal);
            let _ = std::fs::remove_file(shm);
        }

        let conn = rusqlite::Connection::open(dbp)?;

        set_wal_once(&conn)?;
        set_conn_pragmas(&conn)?;

        // DB layout
        // INODE storage
        //   inode        INTEGER   PRIMARY KEY,    -> the u64 inode
        //   git_mode     INTEGER   NOT NULL        -> the raw Git filemode
        //   oid          TEXT      NOT NULL        -> the Git OID
        //   size         INTEGER   NOT NULL        -> real size of the file/git object
        //   inode_flag   INTEGER   NOT NULL        -> InoFlag
        //   uid          INTEGER   NOT NULL
        //   gid          INTEGER   NOT NULL
        //   atime_secs   INTEGER   NOT NULL
        //   atime_nsecs  INTEGER   NOT NULL
        //   mtime_secs   INTEGER   NOT NULL
        //   mtime_nsecs  INTEGER   NOT NULL
        //   ctime_secs   INTEGER   NOT NULL
        //   ctime_nsecs  INTEGER   NOT NULL
        //   nlink        INTEGER   NOT NULL        -> calculated by sql
        //   rdev         INTEGER   NOT NULL
        //   flags        INTEGER   NOT NULL
        //
        // Directory Entries Storage
        //  target_inode INTEGER   NOT NULL       -> inode from inode_map
        //  parent_inode INTEGER   NOT NULL       -> the parent directory’s inode
        //  name         BLOB      NOT NULL       -> the filename or directory name
        conn.execute_batch(
            r#"
                CREATE TABLE IF NOT EXISTS inode_map (
                    inode        INTEGER PRIMARY KEY,
                    oid          TEXT    NOT NULL,
                    git_mode     INTEGER NOT NULL,
                    size         INTEGER NOT NUll,
                    inode_flag   INTEGER NOT NUll,
                    uid          INTEGER NOT NULL,
                    gid          INTEGER NOT NULL,
                    atime_secs   INTEGER NOT NULL,
                    atime_nsecs  INTEGER NOT NULL,
                    mtime_secs   INTEGER NOT NULL,
                    mtime_nsecs  INTEGER NOT NULL,
                    ctime_secs   INTEGER NOT NULL,
                    ctime_nsecs  INTEGER NOT NULL,
                    nlink        INTEGER NOT NULL,
                    rdev         INTEGER NOT NULL,
                    flags        INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS dentries (
                parent_inode INTEGER NOT NULL,
                target_inode INTEGER NOT NULL,
                name         BLOB    NOT NULL,
                is_active    INTEGER NOT NULL,
                PRIMARY KEY (parent_inode, name),
                FOREIGN KEY (parent_inode) REFERENCES inode_map(inode) ON DELETE RESTRICT,
                FOREIGN KEY (target_inode) REFERENCES inode_map(inode) ON DELETE RESTRICT
                ) WITHOUT ROWID;

                CREATE INDEX IF NOT EXISTS dentries_by_target ON dentries(target_inode);
                CREATE INDEX IF NOT EXISTS dentries_by_parent ON dentries(parent_inode);
                CREATE INDEX IF NOT EXISTS dentries_active_by_target ON dentries(target_inode) WHERE is_active = 1;
            "#,
        )?;
        Ok(())
    }

    #[instrument(level = "debug", skip(self), fields(ino = %ino), ret(level = Level::DEBUG), err(Display))]
    pub fn open(&self, ino: u64, read: bool, write: bool, truncate: bool) -> anyhow::Result<u64> {
        let ino: Inodes = ino.into();

        if write && self.read_only {
            bail!("Filesystem is in read only");
        }

        let parent = self.get_single_parent(ino.to_u64_n())?;
        let par_mode = self.get_mode_from_db(parent.into())?;
        let parent_kind = match par_mode {
            git2::FileMode::Tree | git2::FileMode::Commit => FileType::Directory,
            _ => FileType::RegularFile,
        };

        let target_mode = self.get_mode_from_db(ino.to_norm())?;
        let target_kind = match target_mode {
            git2::FileMode::Tree | git2::FileMode::Commit => FileType::Directory,
            _ => FileType::RegularFile,
        };

        let parent: Inodes = parent.into();

        let ctx = FsOperationContext::get_operation(self, ino);
        match ctx? {
            FsOperationContext::Root => bail!("Target is a directory"),
            FsOperationContext::RepoDir => bail!("Target is a directory"),
            FsOperationContext::InsideLiveDir => match parent_kind {
                FileType::Directory => {
                    ops::open::open_live(self, ino.to_norm(), read, write, truncate)
                }
                FileType::RegularFile => ops::open::open_vdir(
                    self,
                    ino.to_norm(),
                    read,
                    write,
                    truncate,
                    parent.to_virt(),
                ),
                _ => bail!("Invalid filemode"),
            },
            FsOperationContext::InsideGitDir => {
                match parent_kind {
                    // If parent is a dir
                    FileType::Directory => match target_kind {
                        // and target is a file, open the blob as normal
                        FileType::RegularFile => {
                            ops::open::open_git(self, ino.to_norm(), read, write, truncate)
                        }
                        // and target is a directory, open as vfile (to create commit summary etc)
                        FileType::Directory => ops::open::open_vfile(self, ino, read, write),
                        _ => bail!("Invalid filemode"),
                    },
                    // If parent is a file, open target as vdir
                    FileType::RegularFile => ops::open::open_vdir(
                        self,
                        ino.to_norm(),
                        read,
                        write,
                        truncate,
                        parent.to_virt(),
                    ),
                    _ => bail!("Invalid filemode"),
                }
            }
        }
    }

    #[instrument(level = "debug", skip(self), fields(ino = %ino), err(Display))]
    pub fn opendir(&self, ino: u64) -> anyhow::Result<u64> {
        let ino: Inodes = ino.into();

        let ctx = FsOperationContext::get_operation(self, ino);
        match ctx? {
            FsOperationContext::Root => ops::opendir::opendir_root(self, ino.to_norm()),
            FsOperationContext::RepoDir => ops::opendir::opendir_repo(self, ino.to_norm()),
            FsOperationContext::InsideLiveDir => match ino {
                Inodes::NormalIno(_) => ops::opendir::opendir_live(self, ino.to_norm()),
                Inodes::VirtualIno(_) => {
                    ops::opendir::opendir_vdir_file_commits(self, ino.to_virt())
                }
            },
            FsOperationContext::InsideGitDir => match ino {
                Inodes::NormalIno(_) => ops::opendir::opendir_git(self, ino.to_norm()),
                Inodes::VirtualIno(_) => {
                    ops::opendir::opendir_vdir_file_commits(self, ino.to_virt())
                }
            },
        }
    }

    #[instrument(level = "debug", skip(self, buf), fields(ino = %ino), err(Display))]
    pub fn read(&self, ino: u64, offset: u64, buf: &mut [u8], fh: u64) -> anyhow::Result<usize> {
        let ret: anyhow::Result<usize> = {
            let ino: Inodes = ino.into();
            let ctx = FsOperationContext::get_operation(self, ino);

            match ctx? {
                FsOperationContext::Root => bail!("Not allowed"),
                FsOperationContext::RepoDir => bail!("Not allowed"),
                FsOperationContext::InsideLiveDir => {
                    ops::read::read_live(self, ino, offset, buf, fh)
                }
                FsOperationContext::InsideGitDir => ops::read::read_git(self, ino, offset, buf, fh),
            }
        };
        if let Ok(ref bytes_read) = ret {
            tracing::Span::current().record("return_len", tracing::field::display(bytes_read));
            tracing::debug!(len = *bytes_read, "Read ok");
        }
        ret
    }

    #[instrument(level = "debug", skip(self, buf), fields(ino = %ino), err(Display))]
    pub fn write(&self, ino: u64, offset: u64, buf: &[u8], fh: u64) -> anyhow::Result<usize> {
        let ret: anyhow::Result<usize> = {
            let ino = ino.into();
            let ctx = FsOperationContext::get_operation(self, ino);
            match ctx? {
                FsOperationContext::Root => bail!("Not allowed"),
                FsOperationContext::RepoDir => bail!("Not allowed"),
                FsOperationContext::InsideLiveDir => {
                    ops::write::write_live(self, ino.into(), offset, buf, fh)
                }
                FsOperationContext::InsideGitDir => {
                    ops::write::write_git(self, ino.to_norm(), offset, buf, fh)
                }
            }
        };
        if let Ok(ref bytes_written) = ret {
            tracing::Span::current().record("return_len", tracing::field::display(bytes_written));
            tracing::debug!(len = *bytes_written, "Write ok");
        }
        ret
    }

    pub fn truncate(&self, ino: u64, size: u64, fh: Option<u64>) -> anyhow::Result<()> {
        let ino = ino.into();
        let ctx = FsOperationContext::get_operation(self, ino)?;
        match ctx {
            FsOperationContext::Root => bail!("Not allowed"),
            FsOperationContext::RepoDir => bail!("Not allowed"),
            FsOperationContext::InsideLiveDir => {
                ops::truncate::truncate_live(self, ino.to_norm(), size, fh)
            }
            FsOperationContext::InsideGitDir => {
                ops::truncate::truncate_git(self, ino.to_norm(), size, fh)
            }
        }
    }

    #[instrument(level = "debug", skip(self), ret(level = Level::DEBUG), err(Display))]
    pub fn release(&self, fh: u64) -> anyhow::Result<bool> {
        if fh == 0 {
            return Ok(true);
        }
        let Some(ino) = self.handles.exists(fh) else {
            return Ok(false);
        };
        let writer_tx = if ino == ROOT_INO {
            None
        } else {
            Some(self.prepare_writemsg(ino.into())?)
        };
        self.handles.close(fh, writer_tx)?;
        {
            let mut guard = self
                .vfile_entry
                .write()
                .map_err(|_| anyhow!("Lock poisoned"))?;
            let ino: Inodes = ino.into();
            guard.remove(&ino.to_virt());
        };
        Ok(true)
    }

    fn object_to_file_attr(
        &self,
        ino: u64,
        git_attr: &ObjectAttr,
        ino_flag: InoFlag,
    ) -> anyhow::Result<FileAttr> {
        let blocks = git_attr.size.div_ceil(512);

        // Compute atime and mtime from commit_time
        let commit_secs = u64::try_from(git_attr.commit_time.seconds())?;
        let time = UNIX_EPOCH + Duration::from_secs(commit_secs);

        let kind = match git_attr.kind {
            ObjectType::Blob if git_attr.git_mode == 0o120000 => FileType::Symlink,
            ObjectType::Tree | ObjectType::Commit => FileType::Directory,
            _ => FileType::RegularFile,
        };
        let perm = 0o775;

        let nlink = if kind == FileType::Directory { 2 } else { 1 };

        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let rdev = 0;
        let blksize = 4096;
        let flags = 0;

        Ok(FileAttr {
            ino,
            ino_flag,
            oid: git_attr.oid,
            size: git_attr.size,
            blocks,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind,
            perm,
            git_mode: git_attr.git_mode,
            nlink,
            uid,
            gid,
            rdev,
            blksize,
            flags,
        })
    }

    #[instrument(level = "debug", skip(self), fields(target = %target), ret(level = Level::DEBUG), err(Display))]
    pub fn getattr(&self, target: u64) -> anyhow::Result<FileAttr> {
        let perms = 0o775;
        let st_mode = libc::S_IFDIR | perms;

        let ino: Inodes = target.into();

        if !self.exists(ino)? {
            bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
        }

        let ctx = FsOperationContext::get_operation(self, ino);
        match ctx? {
            FsOperationContext::Root => {
                let mut attr: FileAttr = dir_attr(InoFlag::Root).into();
                attr.ino = ROOT_INO;
                attr.git_mode = st_mode;
                Ok(attr)
            }
            FsOperationContext::RepoDir => {
                let mut attr: FileAttr = dir_attr(InoFlag::RepoRoot).into();
                attr.ino = ino.into();
                attr.git_mode = st_mode;
                let ino: Inodes = attr.ino.into();
                match ino {
                    Inodes::NormalIno(_) => Ok(attr),
                    Inodes::VirtualIno(_) => self.prepare_virtual_file(ino.to_virt()),
                }
            }
            FsOperationContext::InsideLiveDir => match ino {
                Inodes::NormalIno(_) => ops::getattr::getattr_live_dir(self, ino.to_norm()),
                Inodes::VirtualIno(_) => {
                    let attr = ops::getattr::getattr_live_dir(self, ino.to_norm())?;
                    let ino: Inodes = attr.ino.into();
                    match attr.kind {
                        // If original is a file, create a virtual directory attr
                        // Used when trying to cd into a file
                        FileType::RegularFile => self.prepare_virtual_folder(attr),
                        // If original is a directory, create a virtual file
                        // Used when trying to cat a directory
                        FileType::Directory => self.prepare_virtual_file(ino.to_virt()),
                        _ => bail!("Invalid attr"),
                    }
                }
            },
            FsOperationContext::InsideGitDir => match ino {
                Inodes::NormalIno(_) => ops::getattr::getattr_git_dir(self, ino.to_norm()),
                Inodes::VirtualIno(_) => {
                    let attr = ops::getattr::getattr_git_dir(self, ino.to_norm())?;
                    let ino: Inodes = attr.ino.into();
                    match attr.kind {
                        // If original is a file, create a virtual directory
                        // Used when trying to cd into a file
                        FileType::RegularFile => self.prepare_virtual_folder(attr),
                        // If original is a directory, create a virtual file
                        // Used when trying to cat a directory
                        FileType::Directory => self.prepare_virtual_file(ino.to_virt()),
                        _ => bail!("Invalid attr"),
                    }
                }
            },
        }
    }

    // When fetching a repo takes name as:
    // website.accoount.repo_name
    // example:github.tokio.tokio-rs.git -> https://github.com/tokio-rs/tokio.git
    #[instrument(level = "debug", skip(self), fields(parent = %parent), ret(level = Level::DEBUG), err(Display))]
    pub fn mkdir(&self, parent: u64, os_name: &OsStr) -> anyhow::Result<FileAttr> {
        let parent: Inodes = parent.into();
        if self.read_only {
            bail!("Filesystem is in read only");
        }
        if !self.exists(parent)? {
            bail!(format!("Parent {} does not exist", parent));
        }
        if !self.is_dir(parent)? {
            bail!(format!("Parent {} is not a directory", parent));
        }
        let name = os_name
            .to_str()
            .ok_or_else(|| anyhow!("Not a valid UTF-8 name"))?;

        let ctx = FsOperationContext::get_operation(self, parent);
        match ctx? {
            FsOperationContext::Root => {
                ops::mkdir::mkdir_root(self, ROOT_INO, name, dir_attr(InoFlag::Root))
            }
            FsOperationContext::RepoDir => {
                ops::mkdir::mkdir_repo(self, parent.into(), name, dir_attr(InoFlag::RepoRoot))
            }
            FsOperationContext::InsideLiveDir => {
                ops::mkdir::mkdir_live(self, parent.into(), name, dir_attr(InoFlag::InsideLive))
            }
            FsOperationContext::InsideGitDir => {
                ops::mkdir::mkdir_git(self, parent.to_norm(), name, dir_attr(InoFlag::InsideBuild))
            }
        }
    }

    #[instrument(level = "debug", skip(self), fields(ino = %ino, newparent = %newparent), ret(level = Level::DEBUG), err(Display))]
    pub fn link(&self, ino: u64, newparent: u64, newname: &OsStr) -> anyhow::Result<FileAttr> {
        let ino: Inodes = ino.into();
        let newparent: Inodes = newparent.into();
        if newname.is_empty() || newname == "." || newname == ".." {
            bail!(std::io::Error::from_raw_os_error(libc::EINVAL));
        }

        if memchr::memchr2(b'/', b'\\', newname.as_bytes()).is_some() {
            tracing::error!("invalid name: contains '/' or '\\' {}", newname.display());
            bail!(format!("Invalid name {}", newname.display()));
        }

        if self.read_only {
            bail!("Filesystem is in read only");
        }
        if !self.exists(ino)? {
            bail!(format!("Parent {} does not exist", ino));
        }

        let ctx = FsOperationContext::get_operation(self, ino);
        match ctx? {
            FsOperationContext::Root => {
                tracing::error!("This directory is read only");
                bail!("This directory is read only")
            }
            FsOperationContext::RepoDir => {
                tracing::error!("This directory is read only");
                bail!("This directory is read only")
            }
            FsOperationContext::InsideLiveDir => {
                ops::link::link_live(self, ino.to_norm(), newparent.to_norm(), newname)
            }
            FsOperationContext::InsideGitDir => {
                ops::link::link_git(self, ino.to_norm(), newparent.to_norm(), newname)
            }
        }
    }

    #[instrument(level = "debug", skip(self), fields(parent = %parent), ret(level = Level::DEBUG), err(Display))]
    pub fn create(
        &self,
        parent: u64,
        name: &OsStr,
        read: bool,
        write: bool,
    ) -> anyhow::Result<(FileAttr, u64)> {
        let parent = parent.into();

        if self.read_only {
            bail!("Filesystem is in read only");
        }
        if !self.exists(parent)? {
            bail!(format!("Parent {} does not exist", parent));
        }

        let ctx = FsOperationContext::get_operation(self, parent);
        match ctx? {
            FsOperationContext::Root | FsOperationContext::RepoDir => {
                bail!("This directory is read only")
            }
            FsOperationContext::InsideLiveDir => {
                ops::create::create_live(self, parent.into(), name, write)
            }
            FsOperationContext::InsideGitDir => {
                ops::create::create_git(self, parent.to_norm(), name, write)
            }
        }
    }

    #[instrument(
        level = "debug",
        skip(self, name),
        fields(
            parent = %parent,
            name = %name.to_string_lossy(),
            read_only = self.read_only
        ),
        ret(level = Level::DEBUG),
        err(Display)
    )]
    pub fn unlink(&self, parent: u64, name: &OsStr) -> anyhow::Result<()> {
        let parent: Inodes = parent.into();

        if self.read_only {
            tracing::error!("Filesystem is in read only");
            bail!(std::io::Error::from_raw_os_error(libc::EACCES))
        }
        if !self.exists(parent)? {
            tracing::error!("Parent {} does not exist", parent);
            bail!(std::io::Error::from_raw_os_error(libc::EIO))
        }
        if name == "." || name == ".." {
            tracing::error!("invalid name");
            bail!(std::io::Error::from_raw_os_error(libc::EIO))
        }

        let ctx = FsOperationContext::get_operation(self, parent);
        match ctx? {
            FsOperationContext::Root => {
                tracing::error!("This directory is read only");
                bail!(std::io::Error::from_raw_os_error(libc::EACCES))
            }
            FsOperationContext::RepoDir => {
                tracing::error!("Not allowed");
                bail!(std::io::Error::from_raw_os_error(libc::EACCES))
            }
            FsOperationContext::InsideLiveDir => {
                ops::unlink::unlink_live(self, parent.into(), name)
            }
            FsOperationContext::InsideGitDir => {
                ops::unlink::unlink_build_dir(self, parent.to_norm(), name)
            }
        }
    }

    #[instrument(level = "debug", skip(self), fields(parent = %parent, new_parent = %new_parent), ret(level = Level::DEBUG), err(Display))]
    pub fn rename(
        &self,
        parent: u64,
        name: &OsStr,
        new_parent: u64,
        new_name: &OsStr,
    ) -> anyhow::Result<()> {
        let parent: Inodes = parent.into();
        let new_parent: Inodes = new_parent.into();
        if self.read_only {
            bail!("Filesystem is in read only");
        }
        if !self.exists(parent)? {
            bail!(format!("Parent {} does not exist", parent));
        }
        if !self.exists(new_parent)? {
            bail!(format!("New parent {} does not exist", new_parent));
        }

        if self.lookup(parent.to_u64_n(), name).is_err() {
            bail!(format!("Source {} does not exist", name.display()));
        }

        if name == "." || name == ".." || new_name == "." || new_name == ".." {
            bail!("invalid name");
        }

        if memchr::memchr2(b'/', b'\\', name.as_bytes()).is_some() {
            tracing::error!("invalid name: contains '/' or '\\' {}", name.display());
            bail!(format!("Invalid name {}", name.display()));
        }

        if memchr::memchr2(b'/', b'\\', new_name.as_bytes()).is_some() {
            tracing::error!("invalid name: contains '/' or '\\' {}", new_name.display());
            bail!(format!("Invalid name {}", new_name.display()));
        }

        if parent.to_norm() == new_parent.to_norm() && name == new_name {
            return Ok(());
        }

        let ctx = FsOperationContext::get_operation(self, parent);
        match ctx? {
            FsOperationContext::Root => {
                bail!("This directory is read only")
            }
            FsOperationContext::RepoDir => {
                bail!("Not allowed")
            }
            FsOperationContext::InsideLiveDir => ops::rename::rename_live(
                self,
                parent.to_norm(),
                name,
                new_parent.to_norm(),
                new_name,
            ),
            FsOperationContext::InsideGitDir => ops::rename::rename_git_build(
                self,
                parent.to_norm(),
                name,
                new_parent.to_norm(),
                new_name,
            ),
        }
    }

    #[instrument(level = "debug", skip(self), fields(parent = %parent, name = %name.display()), ret(level = Level::DEBUG), err(Display))]
    pub fn rmdir(&self, parent: u64, name: &OsStr) -> anyhow::Result<()> {
        let parent = parent.into();

        if self.read_only {
            bail!("Filesystem is in read only");
        }
        if !self.exists(parent)? {
            bail!(format!("Parent {} does not exist", parent));
        }

        if name == "." || name == ".." {
            bail!("invalid name");
        }

        let ctx = FsOperationContext::get_operation(self, parent);
        match ctx? {
            FsOperationContext::Root | FsOperationContext::RepoDir => bail!("Not allowed"),
            FsOperationContext::InsideLiveDir => {
                ops::rmdir::rmdir_live(self, parent.to_norm(), name)
            }
            FsOperationContext::InsideGitDir => ops::rmdir::rmdir_git(self, parent.to_norm(), name),
        }
    }

    pub fn readdir(&self, parent: u64) -> anyhow::Result<Vec<DirectoryEntry>> {
        let ret: anyhow::Result<Vec<DirectoryEntry>> = {
            let parent: Inodes = parent.into();

            let ctx = FsOperationContext::get_operation(self, parent);
            match ctx? {
                FsOperationContext::Root => ops::readdir::readdir_root_dir(self),
                FsOperationContext::RepoDir => {
                    ops::readdir::readdir_repo_dir(self, parent.to_norm())
                }
                FsOperationContext::InsideLiveDir => match parent {
                    Inodes::NormalIno(_) => ops::readdir::readdir_live_dir(self, parent.to_norm()),
                    Inodes::VirtualIno(_) => ops::readdir::read_virtual_dir(self, parent.to_virt()),
                },
                FsOperationContext::InsideGitDir => match parent {
                    Inodes::NormalIno(_) => ops::readdir::readdir_git_dir(self, parent.to_norm()),
                    Inodes::VirtualIno(_) => ops::readdir::read_virtual_dir(self, parent.to_virt()),
                },
            }
        };
        if let Ok(ref entries) = ret {
            tracing::Span::current().record("return_len", field::display(entries.len()));
            tracing::debug!(len = entries.len(), parent, "readdir ok");
        }
        ret
    }

    pub fn readdirplus(&self, parent: u64) -> anyhow::Result<Vec<DirectoryEntryPlus>> {
        let mut entries_plus: Vec<DirectoryEntryPlus> = vec![];
        let entries = self.readdir(parent)?;
        for entry in entries {
            let attr = self
                .lookup(parent, &entry.name)?
                .ok_or_else(|| anyhow!("Repo not found"))?;
            let entry_plus = DirectoryEntryPlus { entry, attr };
            entries_plus.push(entry_plus);
        }
        Ok(entries_plus)
    }

    #[instrument(level = "debug", skip(self), fields(parent = %parent), ret(level = Level::DEBUG), err(Display))]
    pub fn lookup(&self, parent: u64, name: &OsStr) -> anyhow::Result<Option<FileAttr>> {
        // Check if name if a virtual dir
        // If not, check if the parent is a virtual dir
        // If not, treat as regular

        let spec = NameSpec::parse(name);
        let name = if spec.is_virtual() { spec.name } else { name };
        let parent: Inodes = parent.into();

        if !self.exists(parent)? {
            bail!(format!("Parent {} does not exist", parent));
        }
        if !self.is_dir(parent)? {
            bail!(format!("Parent {} is not a directory", parent));
        }

        let ctx = FsOperationContext::get_operation(self, parent);
        match ctx? {
            FsOperationContext::Root => ops::lookup::lookup_root(self, name),
            FsOperationContext::RepoDir => {
                let Some(attr) = ops::lookup::lookup_repo(self, parent.to_norm(), name)? else {
                    return Ok(None);
                };
                if spec.is_virtual() && attr.kind == FileType::Directory {
                    let ino: Inodes = attr.ino.into();
                    return Ok(Some(self.prepare_virtual_file(ino.to_virt())?));
                }
                Ok(Some(attr))
            }
            FsOperationContext::InsideLiveDir => {
                // If the target is a virtual File or Dir
                if spec.is_virtual() {
                    let Some(attr) = ops::lookup::lookup_live(self, parent.to_norm(), name)? else {
                        return Ok(None);
                    };
                    let ino: Inodes = attr.ino.into();
                    match attr.kind {
                        // User is trying to cd into a file. Prepare an attr for the virt dir
                        FileType::RegularFile => {
                            return Ok(Some(self.prepare_virtual_folder(attr)?));
                        }
                        FileType::Directory => {
                            return Ok(Some(self.prepare_virtual_file(ino.to_virt())?));
                        }
                        _ => bail!("Invalid attr"),
                    }
                }
                // If the parent dir is virtual
                match parent {
                    Inodes::NormalIno(_) => {
                        let Some(attr) = ops::lookup::lookup_live(self, parent.to_norm(), name)?
                        else {
                            return Ok(None);
                        };
                        Ok(Some(attr))
                    }
                    Inodes::VirtualIno(_) => ops::lookup::lookup_vdir(self, parent.to_virt(), name),
                }
            }
            FsOperationContext::InsideGitDir => {
                if spec.is_virtual() {
                    let Some(attr) = ops::lookup::lookup_git(self, parent.to_norm(), name)? else {
                        return Ok(None);
                    };
                    let ino: Inodes = attr.ino.into();
                    match attr.kind {
                        FileType::RegularFile => {
                            return Ok(Some(self.prepare_virtual_folder(attr)?));
                        }
                        FileType::Directory => {
                            return Ok(Some(self.prepare_virtual_file(ino.to_virt())?));
                        }
                        _ => bail!("Invalid attr"),
                    }
                }
                match parent {
                    Inodes::NormalIno(_) => ops::lookup::lookup_git(self, parent.to_norm(), name),
                    Inodes::VirtualIno(_) => ops::lookup::lookup_vdir(self, parent.to_virt(), name),
                }
            }
        }
    }

    pub fn prepare_virtual_file(&self, ino: VirtualIno) -> anyhow::Result<FileAttr> {
        if let Some(size) = {
            let guard = self
                .vfile_entry
                .read()
                .map_err(|_| anyhow!("Lock poisoned"))?;
            guard.get(&ino).map(|e| e.len)
        } {
            return GitFs::create_vfile_attr(ino, size);
        }

        let size = ops::open::create_vfile_entry(self, ino)?;
        GitFs::create_vfile_attr(ino, size)
    }

    fn create_vfile_attr(ino: VirtualIno, size: u64) -> anyhow::Result<FileAttr> {
        let mut new_attr: FileAttr = file_attr(InoFlag::VirtualFile).into();

        let v_ino = ino.to_virt_u64();

        new_attr.size = size;
        new_attr.git_mode = 0o100444;
        new_attr.kind = FileType::RegularFile;
        new_attr.perm = 0o444;
        new_attr.nlink = 1;
        new_attr.ino = v_ino;
        new_attr.blksize = u32::try_from(size.div_ceil(512))?;

        Ok(new_attr)
    }

    pub fn prepare_virtual_folder(&self, attr: FileAttr) -> anyhow::Result<FileAttr> {
        let mut new_attr = attr;
        let v_ino: VirtualIno = attr.ino.into();
        new_attr.ino = v_ino.to_virt_u64();
        new_attr.perm = 0o555;
        new_attr.size = 0;
        new_attr.kind = FileType::Directory;
        new_attr.nlink = 2;
        Ok(new_attr)
    }
}

// gitfs_path_builders
impl GitFs {
    /// Build path to a folder or file that exists in the live folder
    fn get_live_path(&self, target: NormalIno) -> anyhow::Result<PathBuf> {
        let live_ino = GitFs::get_live_ino(target.into());
        let repo_name = {
            let repo = &self.get_repo(target.to_norm_u64())?;
            repo.repo_dir.clone()
        };
        let path_to_live = PathBuf::from(&self.repos_dir)
            .join(repo_name)
            .join(LIVE_FOLDER);

        if target.to_norm_u64() == live_ino {
            return Ok(path_to_live);
        }

        let mut out: Vec<OsString> = vec![];

        let mut cur_ino = target.to_norm_u64();
        let mut cur_name = self.get_name_from_db(cur_ino)?;

        let max_loops = 1000;
        for _ in 0..max_loops {
            out.push(cur_name);
            cur_ino = self.get_single_parent(cur_ino)?;
            if cur_ino == live_ino {
                break;
            }
            cur_name = self.get_name_from_db(cur_ino)?;
        }

        out.reverse();
        Ok(path_to_live.join(out.iter().collect::<PathBuf>()))
    }

    fn build_full_path(&self, ino: NormalIno) -> anyhow::Result<PathBuf> {
        let repo_ino = {
            let repo = self.get_repo(ino.into())?;
            GitFs::repo_id_to_ino(repo.repo_id)
        };
        let path = PathBuf::from(&self.repos_dir);
        if repo_ino == ino.to_norm_u64() {
            return Ok(path);
        }
        let db_path = &self.get_path_from_db(ino)?;
        let final_path = path.join(db_path);
        Ok(final_path)
    }
}

// gitfs_helpers
impl GitFs {
    pub fn insert_repo(
        &self,
        repo: &Arc<GitRepo>,
        repo_name: &str,
        repo_id: u16,
    ) -> anyhow::Result<()> {
        if let dashmap::Entry::Vacant(entry) = self.repos_list.entry(repo_id) {
            entry.insert(repo.clone());
            self.repos_map.insert(repo_name.to_string(), repo_id);
            info!("Repo {repo_name} added");
        } else {
            bail!("Repo id already exists");
        }
        Ok(())
    }

    pub fn delete_repo(&self, repo_name: &str) -> anyhow::Result<()> {
        if let Some(repo_id) = self.repos_map.get(repo_name) {
            self.repos_list.remove(&repo_id);
        } else {
            bail!("Repo does not exist");
        }
        self.repos_map.remove(repo_name);
        {
            let _ = self.notifier.try_send(InvalMsg::Entry {
                parent: ROOT_INO,
                name: OsString::from(repo_name),
            });
        }
        Ok(())
    }

    pub fn refresh_medata_using_path<P: AsRef<Path>>(
        &self,
        path: P,
        ino_flag: InoFlag,
    ) -> anyhow::Result<FileAttr> {
        let metadata = path.as_ref().metadata()?;
        let std_type = metadata.file_type();

        let mut attr: FileAttr = if std_type.is_dir() {
            dir_attr(ino_flag).into()
        } else if std_type.is_file() {
            file_attr(ino_flag).into()
        } else {
            bail!("Invalid input")
        };

        let atime: SystemTime = metadata.accessed()?;
        let mtime: SystemTime = metadata.modified()?;
        let crtime: SystemTime = metadata.created()?;
        let secs = metadata.ctime();
        let nsecs = u32::try_from(metadata.ctime_nsec())?;
        let ctime: SystemTime = if secs >= 0 {
            UNIX_EPOCH + Duration::new(u64::try_from(secs)?, nsecs)
        } else {
            UNIX_EPOCH - Duration::new(u64::try_from(-secs)?, nsecs)
        };

        attr.atime = atime;
        attr.mtime = mtime;
        attr.crtime = crtime;
        attr.ctime = ctime;
        attr.uid = unsafe { libc::getuid() } as u32;
        attr.gid = unsafe { libc::getgid() } as u32;
        attr.size = metadata.size();
        if std_type.is_dir() {
            attr.blksize = 4096;
        }

        Ok(attr)
    }

    /// Finds the file on disk using an inode
    pub fn refresh_metadata_from_disk(&self, ino: NormalIno) -> anyhow::Result<FileAttr> {
        let path = if self.is_in_live(ino)? {
            self.get_live_path(ino)?
        } else if self.is_in_build(ino)? {
            let commit_oid = self.get_oid_from_db(ino.into())?;
            let repo = self.get_repo(ino.into())?;
            let build_root = &repo.build_dir;
            let session = repo.get_or_init_build_session(commit_oid, build_root)?;
            session.finish_path(self, ino)?
        } else {
            bail!(std::io::Error::from_raw_os_error(libc::EPERM));
        };
        let ino_flag = self.get_ino_flag_from_db(ino)?;
        let mut attr = self.refresh_medata_using_path(path, ino_flag)?;
        attr.ino = ino.into();

        Ok(attr)
    }

    #[instrument(level = "debug", skip(self, stored_attr), fields(ino = %stored_attr.ino), err(Display))]
    pub fn update_db_metadata(
        &self,
        stored_attr: SetFileAttr,
    ) -> anyhow::Result<DbReturn<FileAttr>> {
        let target_ino = stored_attr.ino;
        let cache_res = self.update_attr_in_cache(&stored_attr.clone());
        // Update the DB
        let repo_id = GitFs::ino_to_repo_id(target_ino);
        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let (tx, rx) = oneshot::<()>();
        let tx = if cache_res.is_ok() { None } else { Some(tx) };

        let msg = DbWriteMsg::UpdateMetadata {
            attr: stored_attr,
            resp: tx,
        };
        writer_tx
            .send(msg)
            .context("writer_tx error on update_db_metadata")?;

        if cache_res.is_err() {
            rx.recv()
                .context("writer_rx disc on update_db_metadata")??;
        }

        // Fetch the new metadata
        match cache_res {
            Ok(attr) => Ok(DbReturn::Found { value: attr }),
            Err(_) => self.get_metadata(target_ino),
        }
    }

    fn attr_from_path(ino_flag: InoFlag, path: &Path) -> anyhow::Result<FileAttr> {
        let metadata = path.metadata()?;
        let atime: SystemTime = metadata.accessed()?;
        let mtime: SystemTime = metadata.modified()?;
        let crtime: SystemTime = metadata.created()?;

        let secs = metadata.ctime();
        let nsecs = metadata.ctime_nsec() as u32;
        let ctime: SystemTime = if secs >= 0 {
            UNIX_EPOCH + Duration::new(secs as u64, nsecs)
        } else {
            UNIX_EPOCH - Duration::new((-secs) as u64, nsecs)
        };

        let (kind, mode) = if metadata.is_dir() {
            (FileType::Directory, libc::S_IFDIR)
        } else if metadata.is_file() {
            (FileType::RegularFile, libc::S_IFREG)
        } else {
            (FileType::Symlink, libc::S_IFLNK)
        };

        let perms = 0o775;
        let st_mode = mode | perms;

        Ok(FileAttr {
            ino: 0,
            ino_flag,
            oid: Oid::zero(),
            size: metadata.size(),
            blocks: metadata.blocks(),
            atime,
            mtime,
            ctime,
            crtime,
            kind,
            perm: 0o775,
            git_mode: st_mode,
            nlink: metadata.nlink() as u32,
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getgid() },
            rdev: metadata.rdev() as u32,
            blksize: metadata.blksize() as u32,
            flags: 0,
        })
    }

    fn get_repo(&self, ino: u64) -> anyhow::Result<Arc<GitRepo>> {
        let repo_id = (ino >> REPO_SHIFT) as u16;
        let repo = self
            .repos_list
            .get(&repo_id)
            .ok_or_else(|| anyhow!("No repo for {ino}"))?;
        Ok(repo.clone())
    }

    /// Used in `InoFlag::InsideSnap`.
    ///
    /// Will go up the directory tree and the `commit_oid` from the root Snap folder
    pub fn get_parent_commit(&self, ino: u64) -> anyhow::Result<Oid> {
        let repo = self.get_repo(ino)?;

        let mut cur = ino;
        let mut oid = self.get_oid_from_db(ino)?;
        let max_steps = 1000;
        let mut i = 0;
        while repo.with_repo(|r| r.find_commit(oid).is_err()) {
            i += 1;
            let parent_ino = self.get_single_parent(cur)?;
            oid = self.get_oid_from_db(parent_ino)?;

            cur = parent_ino;
            if i == max_steps {
                bail!("Parent commit not found")
            }
        }
        Ok(oid)
    }

    fn is_in_build(&self, ino: NormalIno) -> anyhow::Result<bool> {
        match self.get_ino_flag_from_db(ino)? {
            InoFlag::BuildRoot | InoFlag::SnapFolder | InoFlag::InsideBuild => Ok(true),
            _ => Ok(false),
        }
    }

    fn is_commit(&self, ino: NormalIno, oid: Oid) -> anyhow::Result<bool> {
        let repo = self.get_repo(ino.to_norm_u64())?;
        Ok(repo.with_repo(|r| r.find_commit(oid).is_ok()))
    }

    fn is_in_live(&self, ino: NormalIno) -> anyhow::Result<bool> {
        match self.get_ino_flag_from_db(ino)? {
            InoFlag::LiveRoot | InoFlag::InsideLive => Ok(true),
            _ => Ok(false),
        }
    }

    fn next_inode_checked(&self, parent: u64) -> anyhow::Result<u64> {
        let repo = self.get_repo(parent)?;

        loop {
            let ino = self.next_inode_raw(parent)?;

            if repo.with_state_mut(|s| s.res_inodes.insert(ino)) {
                return Ok(ino);
            }
        }
    }

    fn next_inode_raw(&self, parent: u64) -> anyhow::Result<u64> {
        let repo_id = GitFs::ino_to_repo_id(parent);
        let ino = self
            .next_inode
            .get(&repo_id)
            .ok_or_else(|| anyhow!("No repo for id {repo_id}"))?
            .fetch_add(1, Ordering::SeqCst);
        Ok(ino)
    }

    fn next_repo_id(&self) -> u16 {
        let max = self.repos_list.iter().map(|e| *e.key()).max();

        match max {
            Some(i) => {
                let next = i
                    .checked_add(1)
                    .expect("Congrats. Repo ids have overflowed a u16.");
                assert!(next <= 32767);
                next
            }
            None => 1,
        }
    }

    pub fn get_dir_parent(&self, ino: u64) -> anyhow::Result<u64> {
        if !self.is_dir(ino.into())? {
            bail!("Not a directory")
        }

        if ROOT_INO == ino {
            return Ok(ROOT_INO);
        }

        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_dir_parent(&conn, ino.into())
    }

    pub fn count_children(&self, ino: NormalIno) -> anyhow::Result<usize> {
        let repo_id = GitFs::ino_to_repo_id(ino.into());
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::count_children(&conn, ino.to_norm_u64())
    }

    pub fn read_children(&self, parent_ino: NormalIno) -> anyhow::Result<Vec<DirectoryEntry>> {
        let repo_id = GitFs::ino_to_repo_id(parent_ino.into());
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::read_children(&conn, parent_ino.to_norm_u64())
    }

    pub fn get_all_parents(&self, ino: u64) -> anyhow::Result<Vec<u64>> {
        let repo = self.get_repo(ino)?;
        if let Some(parents) = repo.dentry_cache.get_all_parents(ino) {
            return Ok(parents);
        }
        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_all_parents(&conn, ino)
    }

    pub fn get_single_parent(&self, ino: u64) -> anyhow::Result<u64> {
        let repo = self.get_repo(ino)?;
        if let Some(entries) = repo.dentry_cache.get_by_target(ino)
            && !entries.is_empty()
        {
            return Ok(entries[0].parent_ino);
        }
        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_single_parent(&conn, ino)
    }

    #[inline]
    fn repo_id_to_ino(repo_id: u16) -> u64 {
        (u64::from(repo_id)) << REPO_SHIFT
    }

    #[inline]
    fn ino_to_repo_id(ino: u64) -> u16 {
        (ino >> REPO_SHIFT) as u16
    }

    fn ensure_base_dirs_exist(&self) -> anyhow::Result<()> {
        if !self.repos_dir.exists() {
            let mut attr: FileAttr = CreateFileAttr {
                kind: FileType::Directory,
                ino_flag: InoFlag::Root,
                perm: 0o775,
                mode: libc::S_IFDIR,
                uid: 0,
                gid: 0,
                rdev: 0,
                flags: 0,
            }
            .into();
            unsafe {
                attr.uid = libc::getuid();
                attr.gid = libc::getgid();
            }

            let repos_dir = &self.repos_dir;
            std::fs::create_dir_all(repos_dir)?;
        }
        Ok(())
    }

    fn get_live_ino(ino: u64) -> u64 {
        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_ino = u64::from(repo_id) << REPO_SHIFT;

        repo_ino + 1
    }

    #[allow(dead_code)]
    fn get_build_ino(&self, ino: NormalIno) -> anyhow::Result<u64> {
        let repo_ino = self.get_repo_ino(ino.to_norm_u64())?;
        self.get_ino_from_db(repo_ino, OsStr::new(BUILD_FOLDER))?
            .into()
    }

    fn exists_by_name(&self, parent: u64, name: &OsStr) -> anyhow::Result<DbReturn<u64>> {
        let repo = self.get_repo(parent)?;
        match repo.dentry_cache.get_by_parent_and_name(parent, name) {
            DbReturn::Found { value: d } => {
                return Ok(DbReturn::Found {
                    value: d.target_ino,
                });
            }
            DbReturn::Negative => {
                return Ok(DbReturn::Negative);
            }
            DbReturn::Missing => {}
        }

        let repo_id = GitFs::ino_to_repo_id(parent);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::exists_by_name(&conn, parent.into(), name)
    }

    pub fn get_metadata_by_name(
        &self,
        parent_ino: NormalIno,
        child_name: &OsStr,
    ) -> anyhow::Result<DbReturn<FileAttr>> {
        if let Ok(res) = self.lookup_in_cache(parent_ino.into(), child_name) {
            match res {
                DbReturn::Found { value: attr } => return Ok(DbReturn::Found { value: attr }),
                DbReturn::Negative => return Ok(DbReturn::Negative),
                DbReturn::Missing => {}
            }
        }
        let repo_id = GitFs::ino_to_repo_id(parent_ino.into());
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_metadata_by_name(&conn, parent_ino.to_norm_u64(), child_name)
    }

    pub fn get_metadata(&self, target_ino: u64) -> anyhow::Result<DbReturn<FileAttr>> {
        if let Ok(res) = self.get_attr_from_cache(target_ino) {
            match res {
                DbReturn::Found { value: attr } => return Ok(DbReturn::Found { value: attr }),
                DbReturn::Negative => return Ok(DbReturn::Negative),
                DbReturn::Missing => {}
            }
        }
        let repo_id = GitFs::ino_to_repo_id(target_ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        let attr = MetaDb::get_metadata(&conn, target_ino)?;
        Ok(attr)
    }

    fn get_builctx_metadata(&self, ino: NormalIno) -> anyhow::Result<BuildCtxMetadata> {
        if let Ok(meta) = self.get_builctx_metadata_from_cache(ino) {
            return Ok(meta);
        }
        let repo_id = GitFs::ino_to_repo_id(ino.into());
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;

        let meta = MetaDb::get_builctx_metadata(&conn, ino.into())?;
        Ok(meta)
    }

    /// Takes Inodes as virtual inodes do not "exist"
    pub fn exists(&self, ino: Inodes) -> anyhow::Result<bool> {
        let ino = ino.to_u64_n();
        if ino == ROOT_INO {
            return Ok(true);
        }

        self.inode_exists(ino)
    }

    fn is_dir(&self, ino: Inodes) -> anyhow::Result<bool> {
        if ino == ROOT_INO {
            return Ok(true);
        }
        let repo_id = GitFs::ino_to_repo_id(ino.to_u64_n());
        if ino == GitFs::repo_id_to_ino(repo_id) {
            return Ok(true);
        }
        let mode = self.get_mode_from_db(ino.to_norm())?;
        let ino: Inodes = ino;
        match mode {
            FileMode::Tree | FileMode::Commit => match ino {
                Inodes::NormalIno(_) => Ok(true),
                Inodes::VirtualIno(_) => Ok(false),
            },
            FileMode::Blob | FileMode::BlobExecutable => match ino {
                Inodes::NormalIno(_) => Ok(false),
                Inodes::VirtualIno(_) => Ok(true),
            },
            _ => Ok(false),
        }
    }

    /// Needs to be passed the actual u64 inode
    #[allow(dead_code)]
    fn is_file(&self, ino: NormalIno) -> anyhow::Result<bool> {
        if ino.to_norm_u64() == ROOT_INO {
            return Ok(false);
        }
        let mode = self.get_mode_from_db(ino)?;
        Ok(mode == FileMode::Blob || mode == FileMode::BlobExecutable)
    }

    /// Needs to be passed the actual u64 inode
    #[allow(dead_code)]
    fn is_link(&self, ino: NormalIno) -> anyhow::Result<bool> {
        let ino = ino.to_norm_u64();
        if ino == ROOT_INO {
            return Ok(false);
        }
        let mode = self.get_mode_from_db(ino.into())?;
        Ok(mode == FileMode::Link)
    }

    pub fn is_virtual(&self, ino: u64) -> bool {
        (ino & VDIR_BIT) != 0
    }

    fn get_ino_from_db(&self, parent: u64, name: &OsStr) -> anyhow::Result<DbReturn<u64>> {
        let repo = self.get_repo(parent)?;
        match repo.dentry_cache.get_by_parent_and_name(parent, name) {
            DbReturn::Found { value: d } => {
                return Ok(DbReturn::Found {
                    value: d.target_ino,
                });
            }
            DbReturn::Negative => return Ok(DbReturn::Negative),
            DbReturn::Missing => {}
        }

        let repo_id = GitFs::ino_to_repo_id(parent);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db for get_ino_from_db - repo {}", repo_id))?;
        let conn = repo_db.ro_pool.get()?;
        let target_ino = MetaDb::get_ino_from_db(&conn, parent, name)?;

        Ok(target_ino)
    }

    pub fn get_file_size_from_db(&self, ino: NormalIno) -> anyhow::Result<u64> {
        let repo = self.get_repo(ino.into())?;
        if let Some(size) = repo.attr_cache.with_get_mut(&ino.to_norm_u64(), |a| a.size) {
            return Ok(size);
        }

        let repo_id = GitFs::ino_to_repo_id(ino.into());
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_size_from_db(&conn, ino.into())
    }

    /// Send and forget but will log errors as `tracing::error!`
    pub fn update_size_in_db(&self, ino: NormalIno, size: u64) -> anyhow::Result<()> {
        let size_res = self.update_size_in_cache(ino.into(), size);
        let repo_id = GitFs::ino_to_repo_id(ino.into());
        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let (tx, rx) = oneshot::<()>();
        let tx = if size_res.is_ok() { None } else { Some(tx) };

        let msg = DbWriteMsg::UpdateSize {
            ino,
            size,
            resp: tx,
        };
        writer_tx
            .send(msg)
            .context("writer_tx error on update_size_in_db")?;

        if size_res.is_err() {
            rx.recv()
                .context("writer_rx disc on update_size_in_db for target")??;
        }

        Ok(())
    }

    /// Removes the directory entry (from dentries) for the target and decrements nlinks
    ///
    /// Send and forget but will log errors as `tracing::error!`
    fn remove_db_dentry(&self, parent_ino: NormalIno, target_name: &OsStr) -> anyhow::Result<()> {
        let repo_id = GitFs::ino_to_repo_id(parent_ino.into());

        self.remove_inode_from_cache(parent_ino.into(), target_name);
        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let msg = DbWriteMsg::RemoveDentry {
            parent_ino,
            target_name: target_name.to_os_string(),
            resp: None,
        };
        writer_tx
            .send(msg)
            .context("writer_tx error on remove_db_record")?;

        Ok(())
    }

    /// Returns a sender for `DbWriteMsg` to be used when no reference to `GitFs` is available
    fn prepare_writemsg(
        &self,
        ino: NormalIno,
    ) -> anyhow::Result<crossbeam_channel::Sender<DbWriteMsg>> {
        let repo_id = GitFs::ino_to_repo_id(ino.into());
        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };
        Ok(writer_tx)
    }

    /// Must be passed a sender from [`crate::fs::GitFs::prepare_writemsg`]
    fn cleanup_entry_with_writemsg(
        target_ino: NormalIno,
        writer_tx: &crossbeam_channel::Sender<DbWriteMsg>,
    ) -> anyhow::Result<()> {
        let msg = DbWriteMsg::CleanupEntry {
            target_ino,
            resp: None,
        };
        writer_tx
            .send(msg)
            .context("writer_tx error on cleanup_dentry")?;

        Ok(())
    }

    /// Checks and removes the inode record from `inode_map`
    ///
    /// Must have nlinks == 0 and is only called when there are no open file handles
    ///
    /// Send and forget but will log errors as `tracing::error!`
    fn cleanup_dentry(&self, target_ino: NormalIno) -> anyhow::Result<()> {
        let repo_id = GitFs::ino_to_repo_id(target_ino.into());
        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let msg = DbWriteMsg::CleanupEntry {
            target_ino,
            resp: None,
        };
        writer_tx
            .send(msg)
            .context("writer_tx error on cleanup_dentry")?;

        Ok(())
    }

    /// Send and forget but will log errors as `tracing::error!`
    fn update_db_record(
        &self,
        old_parent: NormalIno,
        old_name: &OsStr,
        node: StorageNode,
    ) -> anyhow::Result<DbReturn<()>> {
        let repo_id = GitFs::ino_to_repo_id(old_parent.into());
        if let DbReturn::Negative =
            self.update_cache_record(old_parent.into(), old_name, node.clone())?
        {
            return Ok(DbReturn::Negative);
        }
        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let msg = DbWriteMsg::UpdateRecord {
            old_parent,
            old_name: old_name.to_os_string(),
            node,
            resp: None,
        };
        writer_tx
            .send(msg)
            .context("writer_tx error on update_db_record")?;

        Ok(DbReturn::Found { value: () })
    }

    /// If ino is a Snap folder, it will walk the folders and add all entries to database
    fn cache_snap_readdir(&self, ino: NormalIno, deep_seek: bool) -> anyhow::Result<()> {
        let repo_id = GitFs::ino_to_repo_id(ino.into());

        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let mut stack: Vec<u64> = vec![ino.into()];
        let mut nodes: Vec<StorageNode> = Vec::new();

        while let Some(cur_dir) = stack.pop() {
            let direntries = self.readdirplus(cur_dir)?;
            nodes.clear();
            nodes.reserve(direntries.len());

            for e in direntries {
                if e.entry.kind == FileType::Directory && deep_seek {
                    stack.push(e.entry.ino);
                }

                nodes.push(StorageNode {
                    parent_ino: cur_dir,
                    name: e.entry.name.clone(),
                    attr: e.attr,
                });
            }

            let batch = std::mem::take(&mut nodes);
            writer_tx
                .send(DbWriteMsg::WriteInodes {
                    nodes: batch,
                    resp: None,
                })
                .context("writer_tx error on cache_snap_readdir")?;

            if !deep_seek {
                break;
            }
        }

        Ok(())
    }

    pub fn write_dentry(&self, dentry: Dentry) -> anyhow::Result<()> {
        let repo_id = GitFs::ino_to_repo_id(dentry.parent_ino);
        let write_res = self.write_dentry_to_cache(dentry.clone());

        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let (tx, rx) = oneshot::<()>();
        let tx = if write_res.is_ok() { None } else { Some(tx) };

        let msg = DbWriteMsg::WriteDentry { dentry, resp: tx };
        writer_tx
            .send(msg)
            .context("writer_tx error on write_dentry")?;

        if write_res.is_err() {
            rx.recv().context("writer_rx disc on write_dentry")??;
        }

        Ok(())
    }

    pub fn parent_commit_build_session(&self, ino: NormalIno) -> anyhow::Result<Oid> {
        let oid = self.get_oid_from_db(ino.to_norm_u64())?;

        let mut cur_oid = oid;
        let mut cur_ino = ino.to_norm_u64();

        let max_loops = 1000;
        for _ in 0..max_loops {
            if cur_oid != Oid::zero() {
                break;
            }

            cur_ino = self.get_single_parent(cur_ino)?;
            cur_oid = self.get_oid_from_db(cur_ino)?;
        }

        Ok(cur_oid)
    }

    fn get_path_from_db(&self, ino: NormalIno) -> anyhow::Result<PathBuf> {
        if let Ok(path) = self.get_path_from_cache(ino.into()) {
            return Ok(path);
        }
        let repo_id = GitFs::ino_to_repo_id(ino.into());
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_path_from_db(&conn, ino.into())
    }

    fn get_oid_from_db(&self, ino: u64) -> anyhow::Result<Oid> {
        let repo = self.get_repo(ino)?;
        if let Some(oid) = repo.attr_cache.with_get_mut(&ino, |a| a.oid) {
            return Ok(oid);
        }

        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_oid_from_db(&conn, ino)
    }

    fn inode_exists(&self, ino: u64) -> anyhow::Result<bool> {
        let ino: Inodes = ino.into();
        let repo = self.get_repo(ino.to_u64_n())?;
        if repo.attr_cache.get(&ino).is_found() {
            return Ok(true);
        }
        let repo_id = GitFs::ino_to_repo_id(ino.to_u64_n());
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::inode_exists(&conn, ino.to_u64_n())
    }

    pub fn get_ino_flag_from_db(&self, ino: NormalIno) -> anyhow::Result<InoFlag> {
        let repo = self.get_repo(ino.into())?;
        if let Some(ino_flag) = repo.attr_cache.with_get_mut(&ino.into(), |a| a.ino_flag) {
            return Ok(ino_flag);
        }

        let repo_id = GitFs::ino_to_repo_id(ino.into());
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        let mask: InoFlag = MetaDb::get_ino_flag_from_db(&conn, ino.to_norm_u64())?
            .try_into()
            .map_err(|_| anyhow!("Invalid ino mask"))?;
        Ok(mask)
    }

    fn get_mode_from_db(&self, ino: NormalIno) -> anyhow::Result<git2::FileMode> {
        if ino.to_norm_u64() == 0 {
            return Ok(FileMode::Tree);
        }
        let repo = self.get_repo(ino.into())?;
        if let Some(mode) = repo.attr_cache.with_get_mut(&ino.into(), |a| a.git_mode) {
            return repo::try_into_filemode(u64::from(mode))
                .ok_or_else(|| anyhow!("Invalid filemode"));
        }

        let repo_id = GitFs::ino_to_repo_id(ino.into());
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        let mode = MetaDb::get_mode_from_db(&conn, ino.into())?;
        repo::try_into_filemode(mode).ok_or_else(|| anyhow!("Invalid filemode"))
    }

    fn get_name_from_db(&self, ino: u64) -> anyhow::Result<OsString> {
        let repo = self.get_repo(ino)?;
        if let Some(entries) = repo.dentry_cache.get_by_target(ino)
            && !entries.is_empty()
        {
            return Ok(entries[0].target_name.clone());
        }
        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_name_from_db(&conn, ino)
    }

    /// Write the `ROOT_INO` in db for parent mapping purposes
    fn db_ensure_root(&self, ino: u64) -> anyhow::Result<()> {
        let repo_id = GitFs::ino_to_repo_id(ino);
        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let (tx, rx) = oneshot::<()>();
        let msg = DbWriteMsg::EnsureRoot { resp: tx };

        writer_tx
            .send(msg)
            .context("writer_tx error on ensure_root")?;

        rx.recv().context("writer_rx disc on ensure_root")??;

        Ok(())
    }

    fn write_inodes_to_db(&self, nodes: Vec<StorageNode>) -> anyhow::Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        let ino = nodes[0].attr.ino;
        let mut cache_res = false;
        if self.write_inodes_to_cache(ino, nodes.clone()).is_ok() {
            cache_res = true;
        }

        let repo_id = GitFs::ino_to_repo_id(ino);
        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let (tx, rx) = oneshot::<()>();
        let tx = if cache_res { None } else { Some(tx) };
        let msg = DbWriteMsg::WriteInodes { nodes, resp: tx };

        writer_tx
            .send(msg)
            .context("writer_tx error on write_dentry")?;

        if !cache_res {
            rx.recv().context("writer_rx disc on write_inodes")??;
        }

        Ok(())
    }

    fn get_repo_ino(&self, ino: u64) -> anyhow::Result<u64> {
        let repo_id = self.get_repo(ino)?.repo_id;
        Ok(GitFs::repo_id_to_ino(repo_id))
    }

    fn write_inodes_to_cache(&self, ino: u64, entries: Vec<StorageNode>) -> anyhow::Result<()> {
        let repo = self.get_repo(ino)?;
        let mut attrs: Vec<(u64, FileAttr)> = Vec::new();
        let mut dentries: Vec<Dentry> = Vec::new();
        #[allow(clippy::let_unit_value)]
        let () = entries
            .into_iter()
            .map(|e| {
                let dentry = Dentry {
                    target_ino: e.attr.ino,
                    parent_ino: e.parent_ino,
                    target_name: e.name.clone(),
                    is_active: true,
                };
                dentries.push(dentry);
                attrs.push((e.attr.ino, e.attr));
            })
            .collect::<()>();
        repo.attr_cache.insert_many(attrs);
        repo.dentry_cache.insert_many(dentries);
        Ok(())
    }

    fn lookup_in_cache(
        &self,
        parent_ino: u64,
        target_name: &OsStr,
    ) -> anyhow::Result<DbReturn<FileAttr>> {
        let repo = self.get_repo(parent_ino)?;
        let target = match repo
            .dentry_cache
            .get_by_parent_and_name(parent_ino, target_name)
        {
            DbReturn::Found { value } => value,
            DbReturn::Negative => return Ok(DbReturn::Negative),
            DbReturn::Missing => return Ok(DbReturn::Missing),
        };
        Ok(repo.attr_cache.get(&target.target_ino))
    }

    fn remove_dentry_from_cache(&self, target_ino: u64, target_name: &OsStr) -> anyhow::Result<()> {
        let repo = self.get_repo(target_ino)?;
        repo.dentry_cache.remove_by_target(target_ino, target_name);
        Ok(())
    }

    fn remove_inode_from_cache(&self, parent_ino: u64, target_name: &OsStr) {
        let Ok(repo) = self.get_repo(parent_ino) else {
            return;
        };
        let Ok(target_ino) = self.get_ino_by_parent_cache(parent_ino, target_name) else {
            return;
        };
        repo.dentry_cache.remove_by_parent(parent_ino, target_name);
        repo.attr_cache.remove(&target_ino);
        repo.file_cache.remove(&target_ino);
    }

    pub fn update_attr_in_cache(&self, attr: &SetFileAttr) -> anyhow::Result<FileAttr> {
        let ino = attr.ino;
        let repo = self.get_repo(ino)?;
        repo.attr_cache.with_get_mut(&ino, |a| {
            if let Some(ino_flag) = attr.ino_flag {
                a.ino_flag = ino_flag;
            }
            if let Some(oid) = attr.oid {
                a.oid = oid;
            }
            if let Some(size) = attr.size {
                a.size = size;
            }
            if let Some(blocks) = attr.blocks {
                a.blocks = blocks;
            }
            if let Some(atime) = attr.atime {
                a.atime = atime;
            }
            if let Some(mtime) = attr.mtime {
                a.mtime = mtime;
            }
            if let Some(ctime) = attr.ctime {
                a.ctime = ctime;
            }
            if let Some(perm) = attr.perm {
                a.perm = perm;
            }
            if let Some(flags) = attr.flags {
                a.flags = flags;
            }
        });
        repo.attr_cache.get(&ino).into()
    }

    fn update_size_in_cache(&self, ino: u64, size: u64) -> anyhow::Result<u64> {
        let repo = self.get_repo(ino)?;
        let final_size = repo.attr_cache.with_get_mut(&ino, |a| {
            a.size = size;
            a.size
        });
        if let Some(s) = final_size {
            Ok(s)
        } else {
            bail!("Could not set size in cache")
        }
    }

    fn write_dentry_to_cache(&self, dentry: Dentry) -> anyhow::Result<()> {
        let parent_ino = dentry.parent_ino;
        let repo = self.get_repo(parent_ino)?;
        repo.dentry_cache.insert(dentry);
        Ok(())
    }

    fn get_single_dentry(&self, target_ino: u64) -> anyhow::Result<Dentry> {
        let repo = self.get_repo(target_ino)?;
        if let Some(entries) = repo.dentry_cache.get_by_target(target_ino)
            && !entries.is_empty()
        {
            return Ok(entries[0].clone());
        }

        let repo_id = GitFs::ino_to_repo_id(target_ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        let dentry = MetaDb::get_single_dentry(&conn, target_ino)?;
        repo.dentry_cache.insert(dentry.clone());
        Ok(dentry)
    }

    fn get_attr_from_cache(&self, ino: u64) -> anyhow::Result<DbReturn<FileAttr>> {
        let repo = self.get_repo(ino)?;
        let attr = repo.attr_cache.get(&ino);
        Ok(attr)
    }

    fn get_builctx_metadata_from_cache(&self, ino: NormalIno) -> anyhow::Result<BuildCtxMetadata> {
        let ino = ino.to_norm_u64();
        let repo = self.get_repo(ino)?;
        let dentries = repo
            .dentry_cache
            .get_by_target(ino)
            .ok_or_else(|| anyhow!("Cannot find dentry in cache"))?;
        let name = if dentries.is_empty() {
            bail!("No dentries found")
        } else {
            dentries[0].target_name.clone()
        };
        let DbReturn::Found { value: attr } = repo.attr_cache.get(&ino) else {
            bail!("Attribute not found for {ino}")
        };
        let mode = repo::try_into_filemode(u64::from(attr.git_mode))
            .ok_or_else(|| anyhow!("Invalid filemode"))?;
        Ok(BuildCtxMetadata {
            mode,
            oid: attr.oid,
            name,
            ino_flag: attr.ino_flag,
        })
    }

    /// Equivalent to `get_path_from_db`
    fn get_path_from_cache(&self, target_ino: u64) -> anyhow::Result<PathBuf> {
        let repo = self.get_repo(target_ino)?;

        let mut components: Vec<OsString> = Vec::new();
        let mut curr = target_ino;

        while let Some(dentry) = repo.dentry_cache.get_single_dentry(curr) {
            components.push(dentry.target_name);
            curr = dentry.parent_ino;
        }
        if components.is_empty() && target_ino != ROOT_INO {
            bail!(format!("Could not build path for {target_ino}"))
        }

        components.reverse();

        Ok(components.iter().collect::<PathBuf>())
    }

    fn get_ino_by_parent_cache(&self, parent_ino: u64, target_name: &OsStr) -> anyhow::Result<u64> {
        let repo = self.get_repo(parent_ino)?;
        let dentry = repo
            .dentry_cache
            .get_by_parent_and_name(parent_ino, target_name);
        if let DbReturn::Found { value: dentry } = dentry {
            Ok(dentry.target_ino)
        } else {
            bail!("Could not find dentry in cache {}", target_name.display());
        }
    }

    // If not in cache, insert it in cache
    fn set_entry_negative(&self, parent_ino: NormalIno, name: &OsStr) -> anyhow::Result<()> {
        let repo = self.get_repo(parent_ino.into())?;
        let cache_res = repo
            .dentry_cache
            .set_inactive(parent_ino.into(), name)
            .is_found();

        let repo_id = GitFs::ino_to_repo_id(parent_ino.into());
        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let (tx, rx) = oneshot::<()>();
        let tx = if cache_res { None } else { Some(tx) };
        let msg = DbWriteMsg::SetNegative {
            parent_ino,
            target_name: name.to_owned(),
            resp: tx,
        };

        writer_tx
            .send(msg)
            .context("writer_tx error on write_dentry")?;

        if !cache_res {
            rx.recv().context("writer_rx disc on write_inodes")??;

            let repo_db = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("no db"))?;
            let conn = repo_db.ro_pool.get()?;
            if let DbReturn::Found { value: dentry } =
                MetaDb::get_dentry_from_db(&conn, parent_ino.into(), name)?
            {
                repo.dentry_cache.insert(dentry);
            }
        }

        Ok(())
    }

    // `(target_ino, parent_ino, target_name)`
    fn cleanup_neg_entries(
        &self,
        entries: &[(u64, u64, &OsStr)],
        repo_id: u16,
    ) -> anyhow::Result<()> {
        let repo = self.get_repo(GitFs::repo_id_to_ino(repo_id))?;

        let mut attr_targets = Vec::with_capacity(entries.len());
        let mut cache_dentries = Vec::with_capacity(entries.len());
        let mut db_entries = Vec::with_capacity(entries.len());

        for &(target_ino, parent_ino, name) in entries {
            attr_targets.push(target_ino);
            cache_dentries.push((parent_ino, name));
            db_entries.push((parent_ino, target_ino, name.to_os_string()));
        }

        repo.attr_cache.remove_many(&attr_targets);
        // Dentries can only be uniquely identified by (parent_ino, target_name)
        repo.dentry_cache.remove_many_by_parent(&cache_dentries);

        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        // Dentries can only be uniquely identified by (parent_ino, target_name)
        let msg = DbWriteMsg::CleanNegative {
            entries: db_entries,
            resp: None,
        };

        writer_tx
            .send(msg)
            .context("writer_tx error on write_dentry")?;

        Ok(())
    }

    fn update_cache_record(
        &self,
        old_parent: u64,
        old_name: &OsStr,
        node: StorageNode,
    ) -> anyhow::Result<DbReturn<()>> {
        let new_ino = node.attr.ino;
        let repo = self.get_repo(new_ino)?;

        match repo
            .dentry_cache
            .get_by_parent_and_name(old_parent, old_name)
        {
            DbReturn::Found { value: d } => {
                // If old entry is found remove it and create the new entry
                repo.dentry_cache.remove_by_parent(old_parent, old_name);
                repo.attr_cache.remove(&d.target_ino);
                self.write_inodes_to_cache(new_ino, vec![node])?;
                Ok(DbReturn::Found { value: () })
            }
            DbReturn::Negative => Ok(DbReturn::Negative),
            DbReturn::Missing => Ok(DbReturn::Missing),
        }
    }

    fn clone_file_from_cache(&self, ino: u64) -> anyhow::Result<SourceTypes> {
        let repo = self.get_repo(ino)?;
        let file_clone = repo
            .file_cache
            .with_get_mut(&ino, |v| -> anyhow::Result<SourceTypes> { v.try_clone() })
            .ok_or(|| anyhow!("Failed to find file in cache"))
            .map_err(|_| anyhow!("Failed to clone file"))??;
        Ok(file_clone)
    }
}
