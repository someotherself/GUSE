use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
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
use git2::{FileMode, ObjectType, Oid};
use rusqlite::Connection;
use tracing::{Level, field, info, instrument};

use crate::fs::fileattr::{
    CreateFileAttr, FileAttr, FileType, InoFlag, ObjectAttr, SetStoredAttr, StorageNode,
    StoredAttr, build_attr_dir, dir_attr, file_attr,
};
use crate::fs::meta_db::MetaDb;
use crate::fs::ops::readdir::{DirectoryEntry, DirectoryEntryPlus};
use crate::fs::repo::{GitRepo, VirtualNode};
use crate::inodes::{Inodes, NormalIno, VirtualIno};
use crate::mount::InvalMsg;
use crate::namespec::NameSpec;

pub mod builds;
pub mod fileattr;
pub mod meta_db;
pub mod ops;
pub mod repo;

#[cfg(test)]
mod test;

const META_STORE: &str = "fs_meta.db";
pub const REPO_SHIFT: u8 = 48;
pub const ROOT_INO: u64 = 1;
pub const VDIR_BIT: u64 = 1u64 << 47;
const IGNORE_LIST: &[&str] = &[".git", "fs_meta.db"];

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
//---------└── fs_meta.db
// repos/repo_dir2/
//---------├── .git/
//---------└── fs_meta.db
//
// Perceived disk structure
// repos/repo_dir1/
//---------├── live/            <- everything in repo_dir1 except for .git and fs_meta.db
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
// ino for repo_1 folder1:  0000000000000001000000000....0010
// ino for repo_1 folder2:  0000000000000001000000000....0011

// repo_id for repo 2       0000000000000000
// ino for repo 2 root dir  0000000000000010000000000....0000
// ino for repo_2 live dir: 0000000000000010000000000....0001
// ino for repo_2 folder1:  0000000000000010000000000....0010

// ino for repo folder  = (repo_id as u64) << 48 (see REPO_SHIFT)
// repo_id from ino     = (ino >> REPO_SHIFT) as u16
pub struct GitFs {
    pub repos_dir: PathBuf,
    /// Use helpers `self.insert_repo` and `self.delete_repo`
    repos_list: BTreeMap<u16, Arc<Mutex<GitRepo>>>, // <repo_id, repo>
    /// Use helpers `self.insert_repo` and `self.delete_repo`
    repos_map: HashMap<String, u16>, // <repo_name, repo_id>
    next_inode: HashMap<u16, AtomicU64>, // Each Repo has a set of inodes
    current_handle: AtomicU64,
    handles: RwLock<HashMap<u64, Handle>>, // (fh, Handle)
    read_only: bool,
    vfile_entry: RwLock<HashMap<VirtualIno, VFileEntry>>,
    notifier: crossbeam_channel::Sender<InvalMsg>,
}

struct Handle {
    ino: u64,
    file: SourceTypes,
    read: bool,
    write: bool,
}

enum SourceTypes {
    RealFile(File),
    RoBlob { oid: Oid, data: Arc<Vec<u8>> },
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

    pub fn size(&self) -> anyhow::Result<u64> {
        match self {
            Self::RealFile(file) => Ok(file.metadata()?.size()),
            Self::RoBlob { oid: _, data } => Ok(data.len() as u64),
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
        }
    }

    fn write_at(&self, buf: &[u8], offset: u64) -> std::io::Result<usize> {
        match self {
            Self::RealFile(file) => file.write_at(buf, offset),
            Self::RoBlob { oid: _, data: _ } => Err(std::io::Error::from_raw_os_error(libc::EROFS)),
        }
    }
}

// gitfs_fuse_functions
impl GitFs {
    pub fn new(
        repos_dir: PathBuf,
        read_only: bool,
        notifier: Arc<OnceLock<fuser::Notifier>>,
    ) -> anyhow::Result<Arc<Mutex<Self>>> {
        let (tx, rx) = crossbeam_channel::unbounded::<InvalMsg>();

        let mut fs = Self {
            repos_dir,
            repos_list: BTreeMap::new(),
            repos_map: HashMap::new(),
            read_only,
            handles: RwLock::new(HashMap::new()),
            current_handle: AtomicU64::new(1),
            next_inode: HashMap::new(),
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
                        let _ = n.inval_entry(parent, &name);
                    }
                    InvalMsg::Inode { ino, off, len } => {
                        let _ = n.inval_inode(ino, off, len);
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
            if !repo_path.join(META_STORE).exists() {
                continue;
            }
            fs.load_repo(repo_name)?;
        }

        for (&repo_id, repo) in &fs.repos_list {
            let repo_ino = GitFs::repo_id_to_ino(repo_id);
            let live_ino = fs.get_live_ino(repo_ino);
            let repo_name = {
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.repo_dir.clone()
            };
            let repo_path = fs.repos_dir.join(repo_name);

            // Read contents of live
            let nodes = fs.read_dir_to_db(&repo_path, InoFlag::InsideLive, live_ino)?;
            fs.write_inodes_to_db(nodes)?;
        }
        Ok(Arc::from(Mutex::new(fs)))
    }

    pub fn load_repo(&mut self, repo_name: &str) -> anyhow::Result<()> {
        let repo_path = self.repos_dir.join(repo_name);

        let mut connection = self.init_meta_db(repo_name)?;
        connection.ensure_root()?;

        let repo = git2::Repository::init(&repo_path)?;
        let mut res_inodes = HashSet::new();

        let repo_id = self.next_repo_id();
        let repo_ino = (repo_id as u64) << REPO_SHIFT;
        self.next_inode
            .insert(repo_id, AtomicU64::from(repo_ino + 1));

        // Write repo root to db
        let mut repo_attr: FileAttr = dir_attr(InoFlag::RepoRoot).into();
        repo_attr.ino = repo_ino;
        let nodes: Vec<StorageNode> = vec![StorageNode {
            parent_ino: ROOT_INO,
            name: repo_name.into(),
            attr: repo_attr.into(),
        }];
        connection.write_inodes_to_db(nodes)?;

        // Clean the build folder
        let build_name = "build".to_string();
        let build_path = repo_path.join(&build_name);
        if build_path.exists() {
            std::fs::remove_dir_all(&build_path)?;
        }

        // Prepare the live and build folders
        let live_ino = self.next_inode_raw(repo_ino)?;
        let build_ino = self.next_inode_raw(repo_ino)?;

        let live_name = "live".to_string();

        let perms = 0o775;
        let st_mode = libc::S_IFDIR | perms;
        let live_attr = build_attr_dir(live_ino, InoFlag::LiveRoot, st_mode);
        let build_attr = build_attr_dir(build_ino, InoFlag::BuildRoot, st_mode);

        res_inodes.insert(live_ino);
        res_inodes.insert(build_ino);
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

        connection.write_inodes_to_db(nodes)?;

        // Create build folder again
        std::fs::create_dir(&build_path)?;
        std::fs::set_permissions(&build_path, std::fs::Permissions::from_mode(0o775))?;

        let mut git_repo = GitRepo {
            connection: Arc::new(Mutex::new(connection)),
            repo_dir: repo_name.to_owned(),
            repo_id,
            inner: repo,
            head: None,
            snapshots: BTreeMap::new(),
            res_inodes,
            vdir_cache: BTreeMap::new(),
            build_sessions: HashMap::new(),
        };

        {
            let head_res = git_repo.inner.revparse_single("HEAD");
            if head_res.is_ok() {
                git_repo.head = Some(head_res?.id());
            };
        }
        if git_repo.head.is_some() {
            git_repo.refresh_snapshots()?;
        }

        let repo_rc = Arc::from(Mutex::from(git_repo));
        self.insert_repo(repo_rc, repo_name, repo_id)?;
        Ok(())
    }

    fn read_dir_to_db(
        &self,
        path: &Path,
        ino_flag: InoFlag,
        parent_ino: u64,
    ) -> anyhow::Result<Vec<StorageNode>> {
        let mut nodes: Vec<StorageNode> = vec![];
        for entry in path.read_dir()? {
            let entry = entry?;
            if IGNORE_LIST.contains(&entry.file_name().to_str().unwrap_or_default()) {
                continue;
            }
            if entry.file_type()?.is_dir() {
                let ino = self.next_inode_checked(parent_ino)?;
                let mut attr: FileAttr = dir_attr(ino_flag).into();
                attr.ino = ino;
                nodes.push(StorageNode {
                    parent_ino,
                    name: entry.file_name().to_string_lossy().into(),
                    attr: attr.into(),
                });
                nodes.extend(self.read_dir_to_db(&entry.path(), ino_flag, ino)?);
            } else {
                let ino = self.next_inode_checked(parent_ino)?;
                let mut attr: FileAttr = file_attr(ino_flag).into();
                attr.ino = ino;
                nodes.push(StorageNode {
                    parent_ino,
                    name: entry.file_name().to_string_lossy().into(),
                    attr: attr.into(),
                });
            }
        }
        Ok(nodes)
    }

    pub fn new_repo(&mut self, repo_name: &str) -> anyhow::Result<Arc<Mutex<GitRepo>>> {
        let repo_path = self.repos_dir.join(repo_name);
        if repo_path.exists() {
            bail!("Repo already exists");
        }
        std::fs::create_dir(&repo_path)?;
        std::fs::set_permissions(&repo_path, std::fs::Permissions::from_mode(0o775))?;
        let mut connection = self.init_meta_db(repo_name)?;
        connection.ensure_root()?;

        let repo_id = self.next_repo_id();
        let repo_ino = GitFs::repo_id_to_ino(repo_id);
        self.next_inode
            .insert(repo_id, AtomicU64::from(repo_ino + 1));

        let mut repo_attr: FileAttr = dir_attr(InoFlag::RepoRoot).into();
        repo_attr.ino = repo_ino;
        let mut nodes: Vec<StorageNode> = vec![StorageNode {
            parent_ino: ROOT_INO,
            name: repo_name.into(),
            attr: repo_attr.into(),
        }];

        let mut res_inodes = HashSet::new();

        let live_ino = self.next_inode_raw(repo_ino)?;
        res_inodes.insert(live_ino);
        let build_ino = self.next_inode_raw(repo_ino)?;
        res_inodes.insert(build_ino);

        let repo = git2::Repository::init(&repo_path)?;

        let live_name = "live".to_string();
        let build_name = "build".to_string();
        let build_path = self.repos_dir.join(repo_name).join(&build_name);
        std::fs::create_dir(&build_path)?;
        std::fs::set_permissions(&build_path, std::fs::Permissions::from_mode(0o775))?;

        let perms = 0o775;
        let st_mode = libc::S_IFDIR | perms;
        let live_attr = build_attr_dir(live_ino, InoFlag::LiveRoot, st_mode);
        let build_attr = build_attr_dir(build_ino, InoFlag::BuildRoot, st_mode);
        nodes.push(StorageNode {
            parent_ino: repo_ino,
            name: live_name,
            attr: live_attr.into(),
        });
        nodes.push(StorageNode {
            parent_ino: repo_ino,
            name: build_name,
            attr: build_attr.into(),
        });
        connection.write_inodes_to_db(nodes)?;

        let connection = Arc::from(Mutex::from(connection));

        let git_repo = GitRepo {
            connection,
            repo_dir: repo_name.to_owned(),
            repo_id,
            inner: repo,
            head: None,
            snapshots: BTreeMap::new(),
            res_inodes,
            vdir_cache: BTreeMap::new(),
            build_sessions: HashMap::new(),
        };

        let repo_rc = Arc::from(Mutex::from(git_repo));
        self.insert_repo(repo_rc.clone(), repo_name, repo_id)?;
        Ok(repo_rc)
    }

    // pub fn open_repo(&self, repo_name: &str) -> anyhow::Result<GitRepo> {
    //     let repo_path = PathBuf::from(&self.repos_dir).join(repo_name).join("git");
    //     let repo = Repository::open(&repo_path)?;
    //     let head = repo.revparse_single("HEAD")?.id();
    //     let db = self.open_meta_db(repo_name)?;
    //     db.execute_batch("PRAGMA foreign_keys = ON;")?;
    //
    //     let mut stmt = db.conn.prepare(
    //         "SELECT inode
    //            FROM inode_map
    //           LIMIT 1",
    //     )?;

    //     let opt: Option<i64> = stmt.query_row(params![], |row| row.get(0)).optional()?;

    //     let ino = opt.ok_or_else(|| anyhow!("no inodes in inode_map"))?;
    //     let repo_id = (ino >> REPO_SHIFT) as u16;
    //     drop(stmt);

    //     Ok(GitRepo {
    //         connection: Arc::from(Mutex::from(db)),
    //         repo_dir: repo_name.to_string(),
    //         repo_id,
    //         inner: repo,
    //         head: Some(head),
    //         snapshots: BTreeMap::new(),
    //         res_inodes: HashSet::new(),
    //         vdir_cache: BTreeMap::new(),
    //         build_sessions: HashMap::new(),
    //     })
    // }

    // pub fn open_meta_db<P: AsRef<Path>>(&self, repo_name: P) -> anyhow::Result<MetaDb> {
    //     let db_path = PathBuf::from(&self.repos_dir)
    //         .join(repo_name)
    //         .join(META_STORE);
    //     let conn = Connection::open(&db_path)?;
    //     conn.execute_batch("PRAGMA foreign_keys = ON;")?;
    //     Ok(MetaDb { conn })
    // }

    /// Must take in the name of the folder of the REPO --
    /// data_dir/repo_name1
    ///
    ///------------------├── fs_meta.db
    ///
    ///------------------└── .git/
    pub fn init_meta_db<P: AsRef<Path>>(&self, repo_name: P) -> anyhow::Result<MetaDb> {
        let db_path = PathBuf::from(&self.repos_dir)
            .join(repo_name)
            .join(META_STORE);
        if db_path.exists() {
            std::fs::remove_file(&db_path)?;
        }
        let conn = Connection::open(&db_path)?;

        conn.execute_batch("PRAGMA foreign_keys = ON;")?;

        // DB layout
        // INODE storage
        //   inode        INTEGER   PRIMARY KEY,    -> the u64 inode
        //   git_mode     INTEGER   NOT NULL        -> the raw Git filemode
        //   oid          TEXT      NOT NULL        -> the Git OID
        //   size         INTEGER   NOT NULL        -> real size of the file/git object
        //   inode_flag   INTEGER   NOT NULL        -> InoFlag
        //   uid          INTEGER   NOT NULL
        //   gid          INTEGER   NOT NULL
        //   nlink        INTEGER   NOT NULL        -> calculated by sql
        //   rdev         INTEGER   NOT NULL
        //   flags        INTEGER   NOT NULL
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
                    nlink        INTEGER NOT NULL,
                    rdev         INTEGER NOT NULL,
                    flags        INTEGER NOT NULL
                );
            "#,
        )?;

        // Directory Entries Storage
        //  target_inode INTEGER   NOT NULL       -> inode from inode_map
        //  parent_inode INTEGER   NOT NULL       -> the parent directory’s inode
        //  name         TEXT      NOT NULL       -> the filename or directory name
        conn.execute_batch(
            r#"
                CREATE TABLE IF NOT EXISTS dentries (
                parent_inode INTEGER NOT NULL,
                target_inode INTEGER NOT NULL,
                name         TEXT    NOT NULL,
                PRIMARY KEY (parent_inode, name),
                FOREIGN KEY (parent_inode) REFERENCES inode_map(inode) ON DELETE CASCADE,
                FOREIGN KEY (target_inode) REFERENCES inode_map(inode) ON DELETE RESTRICT
                );
            "#,
        )?;

        conn.execute_batch(
            r#"
            CREATE INDEX dentries_by_target ON dentries(target_inode);
            "#,
        )?;
        Ok(MetaDb { conn })
    }

    #[instrument(level = "debug", skip(self), fields(ino = %ino), ret(level = Level::DEBUG), err(Display))]
    pub fn open(&self, ino: u64, read: bool, write: bool, truncate: bool) -> anyhow::Result<u64> {
        let ino: Inodes = ino.into();

        if write && self.read_only {
            bail!("Filesystem is in read only");
        }
        if !write && !read {
            bail!("Read and write cannot be false at the same time");
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

    pub fn read(&self, ino: u64, offset: u64, buf: &mut [u8], fh: u64) -> anyhow::Result<usize> {
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
    }

    pub fn write(&self, ino: u64, offset: u64, buf: &[u8], fh: u64) -> anyhow::Result<usize> {
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
    }

    #[instrument(level = "debug", skip(self), ret(level = Level::DEBUG), err(Display))]
    pub fn release(&self, fh: u64) -> anyhow::Result<bool> {
        if fh == 0 {
            return Ok(true);
        }
        let ino = {
            let mut guard = self.handles.write().map_err(|_| anyhow!("Lock poisoned"))?;
            match guard.remove(&fh) {
                Some(h) => h.ino,
                None => return Ok(false),
            }
        };
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

    #[instrument(level = "debug", skip(self), fields(target = %target), ret(level = Level::DEBUG), err(Display))]
    pub fn getattr(&self, target: u64) -> anyhow::Result<FileAttr> {
        let perms = 0o775;
        let st_mode = libc::S_IFDIR | perms;

        let ino: Inodes = target.into();

        if !self.exists(ino)? {
            bail!(format!("Inode {} does not exist", ino));
        }

        let ctx = FsOperationContext::get_operation(self, ino);
        match ctx? {
            FsOperationContext::Root => Ok(build_attr_dir(ROOT_INO, InoFlag::Root, st_mode)),
            FsOperationContext::RepoDir { ino: _ } => {
                let attr = build_attr_dir(ino.to_u64_n(), InoFlag::RepoRoot, st_mode);
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
    pub fn mkdir(&mut self, parent: u64, os_name: &OsStr) -> anyhow::Result<FileAttr> {
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
        let newname = newname
            .to_str()
            .ok_or_else(|| anyhow!("Not a valid UTF-8 name"))?;
        if newname.is_empty() || newname == "." || newname == ".." || newname.contains('/') {
            bail!(std::io::Error::from_raw_os_error(libc::EINVAL));
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
                ops::create::create_live(self, ino, name, read, write)
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
                ops::create::create_git(self, parent.to_norm(), name, read, write)
            }
        }
    }

    #[instrument(
        level = "debug",
        skip(self, os_name),
        fields(
            parent,
            name = %os_name.to_string_lossy(),
            read_only = self.read_only
        ),
        ret(level = Level::DEBUG),
        err(Display)
    )]
    pub fn unlink(&self, parent: u64, os_name: &OsStr) -> anyhow::Result<()> {
        let parent: Inodes = parent.into();

        if self.read_only {
            tracing::error!("Filesystem is in read only");
            bail!(std::io::Error::from_raw_os_error(libc::EACCES))
        }
        if !self.exists(parent)? {
            tracing::error!("Parent {} does not exist", parent);
            bail!(std::io::Error::from_raw_os_error(libc::EIO))
        }
        let name = os_name.to_str().ok_or_else(|| {
            tracing::error!("Not a valid UTF-8 name");
            anyhow!(std::io::Error::from_raw_os_error(libc::EIO))
        })?;
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
        os_name: &OsStr,
        new_parent: u64,
        os_new_name: &OsStr,
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

        let name = os_name
            .to_str()
            .ok_or_else(|| anyhow!("Not a valid UTF-8 name"))?;
        let new_name = os_new_name
            .to_str()
            .ok_or_else(|| anyhow!("Not a valid UTF-8 name"))?;

        if self.lookup(parent.to_u64_n(), name).is_err() {
            bail!(format!("Source {} does not exist", name));
        }

        if name == "." || name == ".." || new_name == "." || new_name == ".." {
            bail!("invalid name");
        }

        if name.contains('/') || name.contains('\\') {
            tracing::error!(%name, "invalid name: contains '/' or '\\'");
            bail!(format!("Invalid name {}", name));
        }

        if new_name.contains('/') || new_name.contains('\\') {
            tracing::error!(%new_name, "invalid name: contains '/' or '\\'");
            bail!(format!("Invalid name {}", new_name));
        }

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

    #[instrument(level = "debug", skip(self), fields(name= %os_name.display()), ret(level = Level::DEBUG), err(Display))]
    pub fn rmdir(&self, parent: u64, os_name: &OsStr) -> anyhow::Result<()> {
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

    #[instrument(level = "debug", skip(self), fields(parent, return_len = field::Empty), err(Display))]
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

    #[instrument(level = "debug", skip(self), fields(name= %name), ret(level = Level::DEBUG), err(Display))]
    pub fn lookup(&self, parent: u64, name: &str) -> anyhow::Result<Option<FileAttr>> {
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
                return Ok(Some(attr));
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
        let repo_arc = self.get_repo(attr.ino)?;
        let mut new_attr = attr;
        let ino: Inodes = attr.ino.into();
        let v_ino = ino.to_u64_v();

        // Check if the entry is alread saved in vdir_cache
        {
            let repo = repo_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            if let Some(e) = repo.vdir_cache.get(&ino.to_virt()) {
                new_attr.ino = e.ino;
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
            let mut repo = repo_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            match repo.vdir_cache.entry(ino.to_virt()) {
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
            };
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
    fn get_live_path(&self, parent: NormalIno) -> anyhow::Result<PathBuf> {
        let live_ino = self.get_live_ino(parent.to_norm_u64());
        let repo_name = {
            let repo = &self.get_repo(parent.to_norm_u64())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.repo_dir.clone()
        };
        let path_to_repo = PathBuf::from(&self.repos_dir).join(repo_name);

        // live folder must be skipped. It doesn't exist on disk
        if live_ino == parent.to_norm_u64() {
            return Ok(path_to_repo);
        }

        let conn_arc = {
            let repo = &self.get_repo(parent.into())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };

        let parent_name = {
            let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            conn.get_parent_name_from_ino(parent.into())?
        };

        let mut out: Vec<String> = vec![];

        let mut cur_par_ino = parent.to_norm_u64();
        let mut cur_par_name = parent_name;

        out.push(cur_par_name.clone());

        let max_loops = 1000;
        for _ in 0..max_loops {
            (cur_par_ino, cur_par_name) = {
                let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                conn.get_parent_name_from_child(cur_par_ino, &cur_par_name)?
            };

            // live folder must be skipped. It doesn't exist on disk
            if live_ino == cur_par_ino {
                break;
            }

            out.push(cur_par_name.clone());
        }

        out.reverse();
        Ok(path_to_repo.join(out.iter().collect::<PathBuf>()))
    }

    fn get_path_to_build_folder(&self, ino: NormalIno) -> anyhow::Result<PathBuf> {
        let repo_dir = {
            let repo = self.get_repo(ino.to_norm_u64())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.repo_dir.clone()
        };
        let repo_dir_path = self.repos_dir.join(repo_dir).join("build");
        Ok(repo_dir_path)
    }

    // As "live" does not exist on disk, it will remove it from the path
    fn build_full_path(&self, ino: NormalIno) -> anyhow::Result<PathBuf> {
        let repo_ino = {
            let repo = self.get_repo(ino.into())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            GitFs::repo_id_to_ino(repo.repo_id)
        };
        let path = PathBuf::from(&self.repos_dir);
        if repo_ino == ino.to_norm_u64() {
            return Ok(path);
        }
        let db_path = &self.get_path_from_db(ino)?;
        let filename = db_path.file_name().ok_or_else(|| anyhow!("No filename"))?;
        if filename == OsStr::new("live") {
            // If path ends with the live dir, remove it.
            Ok(path)
        } else {
            // Otherwise, use the path from DB, as meta_db will remove live by itself
            Ok(path.join(db_path))
        }
    }
}

// gitfs_helpers
impl GitFs {
    pub fn insert_repo(
        &mut self,
        repo: Arc<Mutex<GitRepo>>,
        repo_name: &str,
        repo_id: u16,
    ) -> anyhow::Result<()> {
        if let Entry::Vacant(entry) = self.repos_list.entry(repo_id) {
            entry.insert(repo.clone());
            self.repos_map.insert(repo_name.to_string(), repo_id);
            info!("Repo {repo_name} added with id {repo_id}");
        } else {
            bail!("Repo id already exists");
        }
        Ok(())
    }

    pub fn delete_repo(&mut self, repo_name: &str) -> anyhow::Result<()> {
        if let Some(repo_id) = self.repos_map.get(repo_name) {
            self.repos_list.remove(repo_id);
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
            let parent_oid = self.parent_commit_build_session(ino)?;
            let build_root = self.get_path_to_build_folder(ino)?;
            let repo = self.get_repo(ino.into())?;
            let mut repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
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
                let _ = self.notifier.send(InvalMsg::Entry {
                    parent,
                    name: OsString::from(&name),
                });
                let _ = self.notifier.send(InvalMsg::Inode {
                    ino: parent,
                    off: 0,
                    len: 0,
                });
            }
            let _ = self.notifier.send(InvalMsg::Inode {
                ino: attr.ino,
                off: 0,
                len: 0,
            });
        }

        Ok(attr)
    }

    pub fn update_db_metadata(&self, stored_attr: SetStoredAttr) -> anyhow::Result<FileAttr> {
        let ino = stored_attr.ino;
        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let mut conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.update_inodes_table(stored_attr)?;
        let stored_attr = conn.get_metadata(ino)?;
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

    fn get_repo(&self, ino: u64) -> anyhow::Result<Arc<Mutex<GitRepo>>> {
        let repo_id = (ino >> REPO_SHIFT) as u16;
        let repo = self
            .repos_list
            .get(&repo_id)
            .ok_or_else(|| anyhow!("No repo for {ino}"))?;
        Ok(repo.clone())
    }

    pub fn get_parent_commit(&self, ino: u64) -> anyhow::Result<Oid> {
        let repo_arc = self.get_repo(ino)?;

        let mut cur = ino;
        let mut oid = self.get_oid_from_db(ino)?;
        let max_steps = 1000;
        let mut i = 0;
        while {
            let repo = repo_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.inner.find_commit(oid).is_err()
        } {
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
        let repo_arc = self.get_repo(ino.to_norm_u64())?;
        let repo = repo_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        Ok(repo.inner.find_commit(oid).is_ok())
    }

    fn is_in_live(&self, ino: NormalIno) -> anyhow::Result<bool> {
        match self.get_ino_flag_from_db(ino)? {
            InoFlag::LiveRoot | InoFlag::InsideLive => Ok(true),
            _ => Ok(false),
        }
    }

    fn next_inode_checked(&self, parent: u64) -> anyhow::Result<u64> {
        let repo_arc = self.get_repo(parent)?;
        let mut repo = repo_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;

        loop {
            let ino = self.next_inode_raw(parent)?;

            if repo.res_inodes.insert(ino) {
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

    fn next_file_handle(&self) -> u64 {
        self.current_handle.fetch_add(1, Ordering::SeqCst)
    }

    fn next_repo_id(&self) -> u16 {
        match self.repos_list.keys().next_back() {
            Some(&i) => {
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

        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_dir_parent(ino.into())
    }

    pub fn count_children(&self, ino: NormalIno) -> anyhow::Result<usize> {
        let conn_arc = {
            let repo = &self.get_repo(ino.to_norm_u64())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };

        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.count_children(ino.to_norm_u64())
    }

    pub fn get_all_parents(&self, ino: u64) -> anyhow::Result<Vec<u64>> {
        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };

        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_all_parents(ino)
    }

    /// Only used in 2 situatsions:
    ///
    /// On files when it will 100% only have one parent (git objects)
    ///
    /// Or when ommiting other parents doesn't matter (writing to a hard link)
    pub fn get_single_parent(&self, ino: u64) -> anyhow::Result<u64> {
        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };

        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_single_parent(ino)
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

    fn get_build_ino(&self, ino: NormalIno) -> anyhow::Result<u64> {
        let repo_ino = self.get_repo_ino(ino.to_norm_u64())?;
        self.get_ino_from_db(repo_ino, "build")
    }

    fn exists_by_name(&self, parent: u64, name: &str) -> anyhow::Result<Option<u64>> {
        let conn_arc = {
            let repo = &self.get_repo(parent)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.exists_by_name(parent.into(), name)
    }

    pub fn get_metadata_by_name(
        &self,
        parent_ino: NormalIno,
        child_name: &str,
    ) -> anyhow::Result<FileAttr> {
        let conn_arc = {
            let repo = &self.get_repo(parent_ino.to_norm_u64())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_metadata_by_name(parent_ino.to_norm_u64(), child_name)
    }

    pub fn get_metadata(&self, target_ino: u64) -> anyhow::Result<FileAttr> {
        let conn_arc = {
            let repo = &self.get_repo(target_ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_metadata(target_ino)
    }

    pub fn get_stored_attr_by_name(&self, target_ino: u64) -> anyhow::Result<StoredAttr> {
        let conn_arc = {
            let repo = &self.get_repo(target_ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_storage_node_from_db(target_ino)
    }

    /// Takes Inodes as virtual inodes do not "exist"
    pub fn exists(&self, ino: Inodes) -> anyhow::Result<bool> {
        let ino = ino.to_u64_n();
        if ino == ROOT_INO {
            return Ok(true);
        }

        let res = self.get_oid_from_db(ino);
        Ok(res.is_ok())
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
    fn is_file(&self, ino: NormalIno) -> anyhow::Result<bool> {
        if ino.to_norm_u64() == ROOT_INO {
            return Ok(false);
        }
        let mode = self.get_mode_from_db(ino)?;
        Ok(mode == FileMode::Blob || mode == FileMode::BlobExecutable)
    }

    /// Needs to be passed the actual u64 inode
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

    fn get_ino_from_db(&self, parent: u64, name: &str) -> anyhow::Result<u64> {
        let parent: Inodes = parent.into();
        let conn_arc = {
            let repo = &self.get_repo(parent.into())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_ino_from_db(parent.into(), name)
    }

    pub fn update_size_in_db(&self, ino: NormalIno, size: u64) -> anyhow::Result<()> {
        let conn_arc = {
            let repo = &self.get_repo(ino.into())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.update_size_in_db(ino.into(), size)
    }

    /// Removes the directory entry (from dentries) for the target
    ///
    /// If it's the only directory entry for this inode, it will remove the inode entry as well
    ///
    /// TODO: Do not delete inode entry only when open fh are 0
    /// TODO: Perform that in the fn release - using a channel
    fn remove_db_record(&self, parent_ino: NormalIno, target_name: &str) -> anyhow::Result<()> {
        let conn_arc = {
            let repo = &self.get_repo(parent_ino.to_norm_u64())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let mut conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.remove_db_record(parent_ino.to_norm_u64(), target_name)
    }

    pub fn write_dentry(
        &self,
        parent_ino: NormalIno,
        target_ino: NormalIno,
        target_name: &str,
    ) -> anyhow::Result<()> {
        let conn_arc = {
            let repo = &self.get_repo(parent_ino.to_norm_u64())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let mut conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.write_dentry(
            parent_ino.to_norm_u64(),
            target_ino.to_norm_u64(),
            target_name,
        )
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
        let conn_arc = {
            let repo = &self.get_repo(ino.into())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_path_from_db(ino.into())
    }

    fn get_oid_from_db(&self, ino: u64) -> anyhow::Result<Oid> {
        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_oid_from_db(ino)
    }

    pub fn get_ino_flag_from_db(&self, ino: NormalIno) -> anyhow::Result<InoFlag> {
        let conn_arc = {
            let repo = &self.get_repo(ino.to_norm_u64())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let mask: InoFlag = conn
            .get_ino_flag_from_db(ino.to_norm_u64())?
            .try_into()
            .map_err(|_| anyhow!("Invalid ino mask"))?;
        Ok(mask)
    }

    fn get_mode_from_db(&self, ino: NormalIno) -> anyhow::Result<git2::FileMode> {
        // Live directory does not exist in the DB. Handle it separately.
        if ino.to_norm_u64() == 0 {
            return Ok(FileMode::Tree);
        }
        let conn_arc = {
            let repo = &self.get_repo(ino.into())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let mode = conn.get_mode_from_db(ino.into())?;
        repo::try_into_filemode(mode).ok_or_else(|| anyhow!("Invalid filemode"))
    }

    fn get_name_from_db(&self, ino: u64) -> anyhow::Result<String> {
        let ino: Inodes = ino.into();
        let conn_arc = {
            let repo = &self.get_repo(ino.into())?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_name_from_db(ino.into())
    }

    fn write_inodes_to_db(&self, nodes: Vec<StorageNode>) -> anyhow::Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        let conn_arc = {
            let repo = &self.get_repo(nodes[0].parent_ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let mut conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.write_inodes_to_db(nodes)
    }

    fn get_repo_ino(&self, ino: u64) -> anyhow::Result<u64> {
        let repo_id = {
            let repo = self.get_repo(ino)?;
            repo.lock().map_err(|_| anyhow!("Lock poisoned"))?.repo_id
        };
        Ok(GitFs::repo_id_to_ino(repo_id))
    }
}
