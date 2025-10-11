use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{FileExt, MetadataExt};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, UNIX_EPOCH};
use std::{
    collections::{BTreeMap, HashMap, HashSet, btree_map::Entry},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

use anyhow::{Context, anyhow, bail};
use dashmap::DashMap;
use git2::{FileMode, ObjectType, Oid};
use tracing::{Level, field, info, instrument};

use crate::fs::fileattr::{
    CreateFileAttr, FileAttr, FileType, InoFlag, ObjectAttr, SetStoredAttr, StorageNode, dir_attr,
    file_attr,
};
use crate::fs::handles::FileHandles;
use crate::fs::meta_db::{DbWriteMsg, MetaDb, oneshot, set_conn_pragmas, set_wal_once};
use crate::fs::ops::readdir::{DirectoryEntry, DirectoryEntryPlus, DirectoryStreamCookie};
use crate::fs::repo::{GitRepo, State, VirtualNode};
use crate::inodes::{Inodes, NormalIno, VirtualIno};
use crate::internals::cache::LruCache;
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
pub const REPO_SHIFT: u8 = 48;
pub const ROOT_INO: u64 = 1;
pub const VDIR_BIT: u64 = 1u64 << 47;
const ATTR_LRU: usize = 10000;
const DENTRY_LRU: usize = 10000;

enum FsOperationContext {
    /// Is the root directory
    Root,
    /// Is one of the directories holding a repo
    RepoDir { ino: u64 },
    /// Dir or File inside the live dir
    InsideLiveDir { ino: u64 },
    /// Dir or File inside a repo dir
    InsideGitDir { ino: u64 },
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
            Ok(FsOperationContext::RepoDir { ino })
        } else if fs.is_in_live(inode.to_norm())? {
            Ok(FsOperationContext::InsideLiveDir { ino })
        } else {
            Ok(FsOperationContext::InsideGitDir { ino })
        }
    }
}

// Real disk structure
// MOUNT_POINT/
// repos/repo_dir1/
//---------├── .git/
//---------├── build/           <- contents will show under each Snap folder
//---------------└── build_HASH/    <- Will show in the Snap folder
//---------------------└── target/    <- Will show in the Snap folder for HASH (commit oid)
//---------└── meta_fs.db
//---------All other contents will show under /live
//
// Perceived disk structure
// repos/repo_dir1/
//---------├── live/            <- everything in repo_dir1 except for .git and fs_meta.db
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
    read: bool,
    write: bool,
}

pub enum SourceTypes {
    RealFile(File),
    RoBlob {
        oid: Oid,
        data: Arc<Vec<u8>>,
    },
    DirSnapshot {
        entries: Arc<Mutex<DirectoryStreamCookie>>,
    },
}

/// Used for creating virtual files.
///
/// These files are made usign commit data.
/// Data generated during getattr/lookup, served during open/read, deleted at release.
///
/// To read the files correctly, getattr and lookup needs the content size
enum VFile {
    Month,
    Commit,
}

struct VFileEntry {
    kind: VFile,
    len: u64,
    data: OnceLock<Arc<Vec<u8>>>,
}

impl SourceTypes {
    pub fn is_file(&self) -> bool {
        matches!(self, SourceTypes::RealFile(_))
    }

    pub fn is_blob(&self) -> bool {
        matches!(self, SourceTypes::RoBlob { oid: _, data: _ })
    }

    pub fn is_dir(&self) -> bool {
        matches!(self, SourceTypes::DirSnapshot { entries: _ })
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
            Self::RoBlob { oid: _, data } => Ok(data.len() as u64),
            Self::DirSnapshot { entries: _ } => {
                bail!(std::io::Error::from_raw_os_error(libc::EROFS))
            }
        }
    }
}

impl FileExt for SourceTypes {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> std::io::Result<usize> {
        match self {
            Self::RealFile(file) => file.read_at(buf, offset),
            Self::RoBlob { oid: _, data } => {
                let start = offset as usize;
                if start >= data.len() {
                    return Ok(0);
                }
                let end = (start + buf.len()).min(data.len());
                let src = &data[start..end];
                buf[..src.len()].copy_from_slice(src);
                Ok(src.len())
            }
            Self::DirSnapshot { entries: _ } => Err(std::io::Error::from_raw_os_error(libc::EROFS)),
        }
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        match self {
            Self::RealFile(file) => file.write_at(buf, offset),
            Self::RoBlob { oid: _, data: _ } => Err(std::io::Error::from_raw_os_error(libc::EROFS)),
            Self::DirSnapshot { entries: _ } => Err(std::io::Error::from_raw_os_error(libc::EROFS)),
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
        let (tx, rx) = crossbeam_channel::unbounded::<InvalMsg>();

        let fs = Self {
            repos_dir,
            repos_list: DashMap::new(),
            conn_list: DashMap::new(),
            repos_map: DashMap::new(),
            read_only,
            handles: FileHandles::default(),
            next_inode: DashMap::new(),
            vfile_entry: RwLock::new(HashMap::new()),
            notifier: tx.clone(),
        };

        std::thread::spawn(move || {
            while notifier.get().is_none() {
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            let n = notifier.get().unwrap().clone();
            for msg in rx.iter() {
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
                }
            }
        });

        fs.ensure_base_dirs_exist()?;
        for entry in fs.repos_dir.read_dir()? {
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
            let live_ino = fs.get_live_ino(repo_ino);
            let repo_name = repo.repo_dir.clone();
            let live_path = fs.repos_dir.join(repo_name).join(LIVE_FOLDER);

            // Read contents of live
            fs.read_dir_to_db(&live_path, &fs, InoFlag::InsideLive, live_ino)?;
        }
        Ok(Arc::from(fs))
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

        // Create the live folder on disk
        let live_path = tmpdir.join(LIVE_FOLDER);
        std::fs::create_dir(&live_path)?;
        let repo = git2::Repository::init(live_path)?;

        let state: State = State {
            head: None,
            snapshots: BTreeMap::new(),
            res_inodes: HashSet::new(),
            vdir_cache: BTreeMap::new(),
            build_sessions: HashMap::new(),
            attr_cache: LruCache::new(ATTR_LRU),
            dentry_cache: LruCache::new(DENTRY_LRU),
        };

        let git_repo = GitRepo {
            repo_dir: repo_name.to_owned(),
            repo_id,
            inner: parking_lot::Mutex::new(repo),
            state: parking_lot::RwLock::new(state),
        };

        let repo_rc = Arc::from(git_repo);
        self.insert_repo(repo_rc, repo_name, repo_id)?;
        Ok(repo_id)
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
        let repo = git2::Repository::init(live_path)?;

        let state: State = State {
            head: None,
            snapshots: BTreeMap::new(),
            res_inodes: HashSet::new(),
            vdir_cache: BTreeMap::new(),
            build_sessions: HashMap::new(),
            attr_cache: LruCache::new(ATTR_LRU),
            dentry_cache: LruCache::new(DENTRY_LRU),
        };

        let git_repo = GitRepo {
            repo_dir: repo_name.to_owned(),
            repo_id,
            inner: parking_lot::Mutex::new(repo),
            state: parking_lot::RwLock::new(state),
        };
        tracing::info!("6");

        // Find HEAD in the git repo
        {
            let head = git_repo.with_repo_mut(|r| {
                if let Ok(head) = r.revparse_single("HEAD") {
                    Some(head.id())
                } else {
                    None
                }
            });
            git_repo.with_state_mut(|s| s.head = head);
            if head.is_some() {
                git_repo.refresh_snapshots()?;
            }
        }

        let repo_rc = Arc::from(git_repo);
        self.insert_repo(repo_rc, repo_name, repo_id)?;
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
            attr: repo_attr.into(),
        }];
        self.write_inodes_to_db(nodes)?;

        // Clean the build folder
        let build_name = OsString::from("build");
        let build_path = repo_path.join(&build_name);
        if build_path.exists() {
            std::fs::remove_dir_all(&build_path)?;
        }

        // Prepare the live and build folders
        let live_ino = self.next_inode_raw(repo_ino)?;
        let build_ino = self.next_inode_raw(repo_ino)?;

        let live_name = OsString::from("live");

        let perms = 0o775;
        let st_mode = libc::S_IFDIR | perms;

        let mut live_attr: FileAttr = dir_attr(InoFlag::LiveRoot).into();
        live_attr.ino = live_ino;
        live_attr.git_mode = st_mode;

        let mut build_attr: FileAttr = dir_attr(InoFlag::BuildRoot).into();
        build_attr.ino = build_ino;
        build_attr.git_mode = st_mode;

        let repo = self.get_repo(repo_ino)?;
        repo.with_state_mut(|s| {
            s.res_inodes.insert(live_ino);
            s.res_inodes.insert(build_ino);
        });

        let nodes: Vec<StorageNode> = vec![
            StorageNode {
                parent_ino: repo_ino,
                name: live_name,
                attr: live_attr.into(),
            },
            StorageNode {
                parent_ino: repo_ino,
                name: build_name,
                attr: build_attr.into(),
            },
        ];

        self.write_inodes_to_db(nodes)?;

        // Create build folder again
        std::fs::create_dir(&build_path)?;

        Ok(())
    }

    pub fn load_repo(&self, repo_name: &str) -> anyhow::Result<()> {
        let repo_id = self.load_repo_connection(repo_name)?;

        self.populate_repo_database(repo_id, repo_name)?;
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
                let ino = self.next_inode_checked(parent_ino)?;

                if entry.file_type()?.is_dir() {
                    stack.push((entry.path(), ino));
                }

                attr.ino = ino;
                nodes.push(StorageNode {
                    parent_ino: cur_parent,
                    name: entry.file_name(),
                    attr: attr.into(),
                });
            }
            fs.write_inodes_to_db(nodes)?;
        }
        Ok(())
    }

    pub fn new_repo(&self, repo_name: &str, url: Option<&str>) -> anyhow::Result<Arc<GitRepo>> {
        let tmpdir = tempfile::Builder::new()
            .rand_bytes(4)
            .tempdir_in(&self.repos_dir)?;
        let repo_id = self.new_repo_connection(repo_name, tmpdir.path())?;
        self.fetch_repo(repo_id, repo_name, tmpdir.path(), url)
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
            attr: repo_attr.into(),
        }];
        self.write_inodes_to_db(nodes)?;

        // Prepare the live and build folders

        let live_ino = self.next_inode_raw(repo_ino)?;
        let build_ino = self.next_inode_raw(repo_ino)?;

        let build_name = OsString::from("build");
        let live_name = OsString::from("live");

        let perms = 0o775;
        let st_mode = libc::S_IFDIR | perms;

        let mut live_attr: FileAttr = dir_attr(InoFlag::LiveRoot).into();
        live_attr.ino = live_ino;
        live_attr.git_mode = st_mode;

        let mut build_attr: FileAttr = dir_attr(InoFlag::BuildRoot).into();
        build_attr.ino = build_ino;
        build_attr.git_mode = st_mode;

        // Create build folder on disk
        std::fs::create_dir(tmp_path.join("build"))?;

        let repo = self.get_repo(repo_ino)?;
        {
            // repo.res_inodes.insert(live_ino);
            // repo.res_inodes.insert(build_ino);
        }

        let nodes: Vec<StorageNode> = vec![
            StorageNode {
                parent_ino: repo_ino,
                name: live_name,
                attr: live_attr.into(),
            },
            StorageNode {
                parent_ino: repo_ino,
                name: build_name,
                attr: build_attr.into(),
            },
        ];

        self.write_inodes_to_db(nodes)?;

        if let Some(url) = url {
            repo.fetch_anon(url)?;
            repo.refresh_snapshots()?;
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
                PRIMARY KEY (parent_inode, name),
                FOREIGN KEY (parent_inode) REFERENCES inode_map(inode) ON DELETE RESTRICT,
                FOREIGN KEY (target_inode) REFERENCES inode_map(inode) ON DELETE RESTRICT
                ) WITHOUT ROWID;

                CREATE INDEX IF NOT EXISTS dentries_by_target ON dentries(target_inode);
                CREATE INDEX IF NOT EXISTS dentries_by_parent ON dentries(parent_inode);
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
        if self.is_dir(ino)? {
            bail!("Target is a directory");
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
            FsOperationContext::RepoDir { ino: _ } => bail!("Target is a directory"),
            FsOperationContext::InsideLiveDir { ino: _ } => match parent_kind {
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
            FsOperationContext::InsideGitDir { ino: _ } => {
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
            FsOperationContext::RepoDir { ino: _ } => {
                ops::opendir::opendir_repo(self, ino.to_norm())
            }
            FsOperationContext::InsideLiveDir { ino: _ } => {
                ops::opendir::opendir_live(self, ino.to_norm())
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
                ops::opendir::opendir_git(self, ino.to_norm())
            }
        }
    }

    #[instrument(level = "debug", skip(self, buf), fields(ino = %ino), err(Display))]
    pub fn read(&self, ino: u64, offset: u64, buf: &mut [u8], fh: u64) -> anyhow::Result<usize> {
        let ret: anyhow::Result<usize> = {
            let ino: Inodes = ino.into();
            let ctx = FsOperationContext::get_operation(self, ino);

            match ctx? {
                FsOperationContext::Root => bail!("Not allowed"),
                FsOperationContext::RepoDir { ino: _ } => bail!("Not allowed"),
                FsOperationContext::InsideLiveDir { ino: _ } => {
                    ops::read::read_live(self, ino, offset, buf, fh)
                }
                FsOperationContext::InsideGitDir { ino: _ } => {
                    ops::read::read_git(self, ino, offset, buf, fh)
                }
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
                FsOperationContext::RepoDir { ino: _ } => bail!("Not allowed"),
                FsOperationContext::InsideLiveDir { ino } => {
                    ops::write::write_live(self, ino, offset, buf, fh)
                }
                FsOperationContext::InsideGitDir { ino: _ } => {
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
            FsOperationContext::RepoDir { ino: _ } => bail!("Not allowed"),
            FsOperationContext::InsideLiveDir { ino: _ } => {
                ops::truncate::truncate_live(self, ino.to_norm(), size, fh)
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
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
        let commit_secs = git_attr.commit_time.seconds() as u64;
        let time = UNIX_EPOCH + Duration::from_secs(commit_secs);

        let kind = match git_attr.kind {
            ObjectType::Blob if git_attr.git_mode == 0o120000 => FileType::Symlink,
            ObjectType::Tree => FileType::Directory,
            ObjectType::Commit => FileType::Directory,
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

    pub fn getattr(&self, target: u64) -> anyhow::Result<FileAttr> {
        let perms = 0o775;
        let st_mode = libc::S_IFDIR | perms;

        let ino: Inodes = target.into();

        if !self.exists(ino)? {
            bail!(format!("Inode {} does not exist", ino));
        }

        let ctx = FsOperationContext::get_operation(self, ino);
        match ctx? {
            FsOperationContext::Root => {
                let mut attr: FileAttr = dir_attr(InoFlag::Root).into();
                attr.ino = ROOT_INO;
                attr.git_mode = st_mode;
                Ok(attr)
            }
            FsOperationContext::RepoDir { ino: _ } => {
                let mut attr: FileAttr = dir_attr(InoFlag::RepoRoot).into();
                attr.ino = ino.into();
                attr.git_mode = st_mode;
                let ino: Inodes = attr.ino.into();
                match ino {
                    Inodes::NormalIno(_) => Ok(attr),
                    Inodes::VirtualIno(_) => self.prepare_virtual_file(ino.to_virt()),
                }
            }
            FsOperationContext::InsideLiveDir { ino: _ } => match ino {
                Inodes::NormalIno(_) => ops::getattr::getattr_live_dir(self, ino.to_norm()),
                Inodes::VirtualIno(_) => {
                    let attr = ops::getattr::getattr_live_dir(self, ino.to_norm())?;
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
            FsOperationContext::InsideGitDir { ino: _ } => match ino {
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
            FsOperationContext::RepoDir { ino } => {
                ops::mkdir::mkdir_repo(self, ino, name, dir_attr(InoFlag::RepoRoot))
            }
            FsOperationContext::InsideLiveDir { ino } => {
                ops::mkdir::mkdir_live(self, ino, name, dir_attr(InoFlag::InsideLive))
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
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
        };

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
            FsOperationContext::RepoDir { ino: _ } => {
                tracing::error!("This directory is read only");
                bail!("This directory is read only")
            }
            FsOperationContext::InsideLiveDir { ino: _ } => {
                tracing::error!("This directory is read only");
                bail!("This directory is read only")
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
                ops::link::link_git(self, ino.to_norm(), newparent.to_norm(), newname)
            }
        }
    }

    #[instrument(level = "debug", skip(self), fields(parent = %parent), ret(level = Level::DEBUG), err(Display))]
    pub fn create(
        &self,
        parent: u64,
        os_name: &OsStr,
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
        let name = os_name
            .to_str()
            .ok_or_else(|| anyhow!("Not a valid UTF-8 name"))?;

        let ctx = FsOperationContext::get_operation(self, parent);
        match ctx? {
            FsOperationContext::Root => {
                bail!("This directory is read only")
            }
            FsOperationContext::RepoDir { ino: _ } => {
                bail!("This directory is read only")
            }
            FsOperationContext::InsideLiveDir { ino } => {
                ops::create::create_live(self, ino, name, write)
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
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
            FsOperationContext::RepoDir { ino: _ } => {
                tracing::error!("Not allowed");
                bail!(std::io::Error::from_raw_os_error(libc::EACCES))
            }
            FsOperationContext::InsideLiveDir { ino } => ops::unlink::unlink_live(self, ino, name),
            FsOperationContext::InsideGitDir { ino: _ } => {
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
        };

        if memchr::memchr2(b'/', b'\\', new_name.as_bytes()).is_some() {
            tracing::error!("invalid name: contains '/' or '\\' {}", new_name.display());
            bail!(format!("Invalid name {}", new_name.display()));
        };

        if parent.to_norm() == new_parent.to_norm() && name == new_name {
            return Ok(());
        }

        let ctx = FsOperationContext::get_operation(self, parent);
        match ctx? {
            FsOperationContext::Root => {
                bail!("This directory is read only")
            }
            FsOperationContext::RepoDir { ino: _ } => {
                bail!("Not allowed")
            }
            FsOperationContext::InsideLiveDir { ino: _ } => ops::rename::rename_live(
                self,
                parent.to_norm(),
                name,
                new_parent.to_norm(),
                new_name,
            ),
            FsOperationContext::InsideGitDir { ino: _ } => ops::rename::rename_git_build(
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
            FsOperationContext::Root => {
                bail!("Not allowed")
            }
            FsOperationContext::RepoDir { ino: _ } => {
                bail!("Not allowed")
            }
            FsOperationContext::InsideLiveDir { ino: _ } => {
                ops::rmdir::rmdir_live(self, parent.to_norm(), name)
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
                ops::rmdir::rmdir_git(self, parent.to_norm(), name)
            }
        }
    }

    pub fn readdir(&self, parent: u64) -> anyhow::Result<Vec<DirectoryEntry>> {
        let ret: anyhow::Result<Vec<DirectoryEntry>> = {
            let parent: Inodes = parent.into();

            let ctx = FsOperationContext::get_operation(self, parent);
            match ctx? {
                FsOperationContext::Root => ops::readdir::readdir_root_dir(self),
                FsOperationContext::RepoDir { ino: _ } => {
                    ops::readdir::readdir_repo_dir(self, parent.to_norm())
                }
                FsOperationContext::InsideLiveDir { ino: _ } => match parent {
                    Inodes::NormalIno(_) => ops::readdir::readdir_live_dir(self, parent.to_norm()),
                    Inodes::VirtualIno(_) => ops::readdir::read_virtual_dir(self, parent.to_virt()),
                },
                FsOperationContext::InsideGitDir { ino: _ } => match parent {
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

    pub fn lookup(&self, parent: u64, name: &OsStr) -> anyhow::Result<Option<FileAttr>> {
        // Check if name if a virtual dir
        // If not, check if the parent is a virtual dir
        // If not, treat as regular
        // If the parent a virtual directory

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
            FsOperationContext::RepoDir { ino: _ } => {
                let Some(attr) = ops::lookup::lookup_repo(self, parent.to_norm(), name)? else {
                    return Ok(None);
                };
                if spec.is_virtual() && attr.kind == FileType::Directory {
                    let ino: Inodes = attr.ino.into();
                    return Ok(Some(self.prepare_virtual_file(ino.to_virt())?));
                }
                Ok(Some(attr))
            }
            FsOperationContext::InsideLiveDir { ino: _ } => {
                // If the target has is a virtual, either File or Dir
                if spec.is_virtual() {
                    let Some(attr) = ops::lookup::lookup_live(self, parent.to_norm(), name)? else {
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
                // If the parent has is a virtual. Only supports dir parents
                match parent {
                    Inodes::NormalIno(_) => {
                        let attr = match ops::lookup::lookup_live(self, parent.to_norm(), name)? {
                            Some(attr) => attr,
                            None => return Ok(None),
                        };
                        Ok(Some(attr))
                    }
                    Inodes::VirtualIno(_) => ops::lookup::lookup_vdir(self, parent.to_virt(), name),
                }
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
                if spec.is_virtual() {
                    let attr = match ops::lookup::lookup_git(self, parent.to_norm(), name)? {
                        Some(attr) => attr,
                        None => return Ok(None),
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
                    Inodes::NormalIno(_) => {
                        let attr = match ops::lookup::lookup_git(self, parent.to_norm(), name)? {
                            Some(attr) => attr,
                            None => return Ok(None),
                        };
                        Ok(Some(attr))
                    }
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
            return self.create_vfile_attr(ino, size);
        }

        let size = ops::open::create_vfile_entry(self, ino)?;
        self.create_vfile_attr(ino, size)
    }

    fn create_vfile_attr(&self, ino: VirtualIno, size: u64) -> anyhow::Result<FileAttr> {
        let mut new_attr: FileAttr = file_attr(InoFlag::VirtualFile).into();

        let v_ino = ino.to_virt_u64();

        new_attr.size = size;
        new_attr.git_mode = 0o100444;
        new_attr.kind = FileType::RegularFile;
        new_attr.perm = 0o444;
        new_attr.nlink = 1;
        new_attr.ino = v_ino;
        new_attr.blksize = size.div_ceil(512) as u32;

        Ok(new_attr)
    }

    pub fn prepare_virtual_folder(&self, attr: FileAttr) -> anyhow::Result<FileAttr> {
        let repo = self.get_repo(attr.ino)?;
        let mut new_attr = attr;
        let ino: Inodes = attr.ino.into();
        let v_ino = ino.to_u64_v();

        // Check if the entry is alread saved in vdir_cache
        {
            let cached = repo.with_state_mut(|s| s.vdir_cache.get(&ino.to_virt()).cloned());

            if let Some(entry) = cached {
                new_attr.ino = entry.ino;
                new_attr.perm = 0o555;
                new_attr.size = 0;
                new_attr.kind = FileType::Directory;
                new_attr.nlink = 2;
                debug_assert!(self.is_virtual(new_attr.ino));
                return Ok(new_attr);
            }
        }

        // If not, create it and save the VirtualNode in vdir_cache
        {
            repo.with_state_mut(|s| {
                match s.vdir_cache.entry(ino.to_virt()) {
                    Entry::Occupied(e) => {
                        // Another thread alread inserted an entry
                        let v = e.get();
                        new_attr.ino = v.ino;
                    }
                    Entry::Vacant(slot) => {
                        let v_node = VirtualNode {
                            real: ino.to_u64_n(),
                            ino: v_ino,
                            oid: attr.oid,
                            log: BTreeMap::new(),
                        };
                        slot.insert(v_node);
                        new_attr.ino = v_ino;
                    }
                }
            });
            new_attr.kind = FileType::Directory;
            new_attr.perm = 0o555;
            new_attr.size = 0;
            new_attr.nlink = 2;

            Ok(new_attr)
        }
    }
}

// gitfs_path_builders
impl GitFs {
    /// Build path to a folder or file that exists in the live folder
    fn get_live_path(&self, target: NormalIno) -> anyhow::Result<PathBuf> {
        let live_ino = self.get_live_ino(target.into());
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

    fn get_path_to_build_folder(&self, ino: NormalIno) -> anyhow::Result<PathBuf> {
        let repo_dir = {
            let repo = self.get_repo(ino.to_norm_u64())?;
            repo.repo_dir.clone()
        };
        let repo_dir_path = self.repos_dir.join(repo_dir).join("build");
        Ok(repo_dir_path)
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
        repo: Arc<GitRepo>,
        repo_name: &str,
        repo_id: u16,
    ) -> anyhow::Result<()> {
        if let dashmap::Entry::Vacant(entry) = self.repos_list.entry(repo_id) {
            entry.insert(repo.clone());
            self.repos_map.insert(repo_name.to_string(), repo_id);
            info!("Repo {repo_name} added with id {repo_id}");
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
        let nsecs = metadata.ctime_nsec() as u32;
        let ctime: SystemTime = if secs >= 0 {
            UNIX_EPOCH + Duration::new(secs as u64, nsecs)
        } else {
            UNIX_EPOCH - Duration::new((-secs) as u64, nsecs)
        };

        attr.atime = atime;
        attr.mtime = mtime;
        attr.crtime = crtime;
        attr.ctime = ctime;
        attr.uid = unsafe { libc::getuid() } as u32;
        attr.gid = unsafe { libc::getgid() } as u32;
        attr.size = metadata.size();

        Ok(attr)
    }

    /// Finds the file on disk using an inode
    pub fn refresh_metadata_from_disk(&self, ino: NormalIno) -> anyhow::Result<FileAttr> {
        let path = if self.is_in_live(ino)? {
            self.get_live_path(ino)?
        } else if self.is_in_build(ino)? {
            let parent_oid = self.get_oid_from_db(ino.into())?;
            let build_root = self.get_path_to_build_folder(ino)?;
            let repo = self.get_repo(ino.into())?;
            let session = repo.get_or_init_build_session(parent_oid, &build_root)?;
            drop(repo);
            session.finish_path(self, ino)?
        } else {
            bail!(std::io::Error::from_raw_os_error(libc::EPERM));
        };
        let ino_flag = self.get_ino_flag_from_db(ino)?;
        let mut attr = self.refresh_medata_using_path(path, ino_flag)?;
        attr.ino = ino.into();

        {
            let parents = self.get_all_parents(ino.into())?;
            let name = self.get_name_from_db(attr.ino)?;
            for parent in parents {
                self.notifier.try_send(InvalMsg::Entry {
                    parent,
                    name: OsString::from(&name),
                })?;
                self.notifier.try_send(InvalMsg::Inode {
                    ino: parent,
                    off: 0,
                    len: 0,
                })?;
            }
        }

        Ok(attr)
    }

    #[instrument(level = "debug", skip(self, stored_attr), fields(ino = %stored_attr.ino), err(Display))]
    pub fn update_db_metadata(&self, stored_attr: SetStoredAttr) -> anyhow::Result<FileAttr> {
        let target_ino = stored_attr.ino;

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

        let msg = DbWriteMsg::UpdateMetadata {
            attr: stored_attr,
            resp: tx,
        };
        writer_tx
            .send(msg)
            .context("writer_tx error on update_db_metadata")?;

        rx.recv()
            .context("writer_rx disc on update_db_metadata")??;

        // Fetch the new metadata
        let stored_attr = self.get_metadata(target_ino)?;
        Ok(stored_attr)
    }

    fn attr_from_path(&self, ino_flag: InoFlag, path: PathBuf) -> anyhow::Result<FileAttr> {
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
        };

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

    pub fn list_dentries_for_inode(&self, ino: NormalIno) -> anyhow::Result<Vec<(u64, OsString)>> {
        let repo_id = GitFs::ino_to_repo_id(ino.into());
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::list_dentries_for_inode(&conn, ino.to_norm_u64())
    }

    pub fn get_all_parents(&self, ino: u64) -> anyhow::Result<Vec<u64>> {
        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_all_parents(&conn, ino)
    }

    pub fn get_single_parent(&self, ino: u64) -> anyhow::Result<u64> {
        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_single_parent(&conn, ino)
    }

    fn repo_id_to_ino(repo_id: u16) -> u64 {
        (repo_id as u64) << REPO_SHIFT
    }

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

    fn get_live_ino(&self, ino: u64) -> u64 {
        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_ino = (repo_id as u64) << REPO_SHIFT;

        repo_ino + 1
    }

    #[allow(dead_code)]
    fn get_build_ino(&self, ino: NormalIno) -> anyhow::Result<u64> {
        let repo_ino = self.get_repo_ino(ino.to_norm_u64())?;
        self.get_ino_from_db(repo_ino, OsStr::new("build"))
    }

    fn exists_by_name(&self, parent: u64, name: &OsStr) -> anyhow::Result<Option<u64>> {
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
    ) -> anyhow::Result<FileAttr> {
        let repo_id = GitFs::ino_to_repo_id(parent_ino.into());
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_metadata_by_name(&conn, parent_ino.to_norm_u64(), child_name)
    }

    #[instrument(level = "debug", skip(self), fields(ino = %target_ino), err(Display))]
    pub fn get_metadata(&self, target_ino: u64) -> anyhow::Result<FileAttr> {
        let repo_id = GitFs::ino_to_repo_id(target_ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_metadata(&conn, target_ino)
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

    fn get_ino_from_db(&self, parent: u64, name: &OsStr) -> anyhow::Result<u64> {
        let repo_id = GitFs::ino_to_repo_id(parent);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_ino_from_db(&conn, parent, name)
    }

    /// Send and forget but will log errors as tracing::error!
    pub fn update_size_in_db(&self, ino: NormalIno, size: u64) -> anyhow::Result<()> {
        let repo_id = GitFs::ino_to_repo_id(ino.into());
        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let (tx, rx) = oneshot::<()>();

        let msg = DbWriteMsg::UpdateSize {
            ino,
            size,
            resp: tx,
        };
        writer_tx
            .send(msg)
            .context("writer_tx error on update_size_in_db")?;

        rx.recv()
            .context("writer_rx disc on update_size_in_db for target")??;

        Ok(())
    }

    /// Removes the directory entry (from dentries) for the target and decrements nlinks
    ///
    /// Send and forget but will log errors as tracing::error!
    fn remove_db_dentry(&self, parent_ino: NormalIno, target_name: &OsStr) -> anyhow::Result<()> {
        let repo_id = GitFs::ino_to_repo_id(parent_ino.into());
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

    /// Returns a sender for DbWriteMsg to be used when no reference to GitFs is available
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
        writer_tx: crossbeam_channel::Sender<DbWriteMsg>,
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

    /// Checks and removes the inode record from inode_map
    ///
    /// Must have nlinks == 0 and is only called when there are no open file handles
    ///
    /// Send and forget but will log errors as tracing::error!
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

    /// Send and forget but will log errors as tracing::error!
    fn update_db_record(
        &self,
        old_parent: NormalIno,
        old_name: &OsStr,
        node: StorageNode,
    ) -> anyhow::Result<()> {
        let repo_id = GitFs::ino_to_repo_id(old_parent.into());
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

        Ok(())
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
                    name: e.entry.name,
                    attr: e.attr.into(),
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

    pub fn write_dentry(
        &self,
        parent_ino: NormalIno,
        target_ino: NormalIno,
        target_name: &OsStr,
    ) -> anyhow::Result<()> {
        let repo_id = GitFs::ino_to_repo_id(parent_ino.into());
        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let (tx, rx) = oneshot::<()>();

        let msg = DbWriteMsg::WriteDentry {
            parent_ino,
            target_ino,
            target_name: target_name.to_os_string(),
            resp: tx,
        };
        writer_tx
            .send(msg)
            .context("writer_tx error on write_dentry")?;

        rx.recv().context(format!(
            "writer_rx disc on write_dentry for target {}",
            target_name.display()
        ))??;

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

    // TODO: FIX NEW SQL
    fn get_path_from_db(&self, ino: NormalIno) -> anyhow::Result<PathBuf> {
        let repo_id = GitFs::ino_to_repo_id(ino.into());
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_path_from_db(&conn, ino.into())
    }

    fn get_oid_from_db(&self, ino: u64) -> anyhow::Result<Oid> {
        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_oid_from_db(&conn, ino)
    }

    fn inode_exists(&self, ino: u64) -> anyhow::Result<bool> {
        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::inode_exists(&conn, ino)
    }

    pub fn get_ino_flag_from_db(&self, ino: NormalIno) -> anyhow::Result<InoFlag> {
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
        // Live directory does not exist in the DB. Handle it separately.
        if ino.to_norm_u64() == 0 {
            return Ok(FileMode::Tree);
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
        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_db = self
            .conn_list
            .get(&repo_id)
            .ok_or_else(|| anyhow::anyhow!("no db"))?;
        let conn = repo_db.ro_pool.get()?;
        MetaDb::get_name_from_db(&conn, ino)
    }

    /// Write the ROOT_INO in db for parent mapping purposes
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

        let repo_id = GitFs::ino_to_repo_id(nodes[0].attr.ino);
        let writer_tx = {
            let guard = self
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("No db for repo id {repo_id}"))?;
            guard.writer_tx.clone()
        };

        let (tx, rx) = oneshot::<()>();
        let msg = DbWriteMsg::WriteInodes {
            nodes,
            resp: Some(tx),
        };

        writer_tx
            .send(msg)
            .context("writer_tx error on write_dentry")?;

        rx.recv().context("writer_rx disc on write_inodes")??;

        Ok(())
    }

    fn get_repo_ino(&self, ino: u64) -> anyhow::Result<u64> {
        let repo_id = self.get_repo(ino)?.repo_id;
        Ok(GitFs::repo_id_to_ino(repo_id))
    }
}
