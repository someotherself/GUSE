use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, HashSet};
use std::ffi::OsStr;
use std::fs::File;
use std::os::unix::fs::{FileExt, MetadataExt, PermissionsExt};
use std::path::Path;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, UNIX_EPOCH};
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

use anyhow::{Context, anyhow, bail};
use git2::{FileMode, ObjectType, Oid, Repository};
use rusqlite::{Connection, OptionalExtension, params};
use tracing::{Level, debug, field, info, instrument};

use crate::fs::fileattr::{CreateFileAttr, FileAttr, FileType, ObjectAttr, build_attr_dir};
use crate::fs::meta_db::MetaDb;
use crate::fs::ops::readdir::{DirectoryEntry, DirectoryEntryPlus};
use crate::fs::repo::{GitRepo, VirtualNode};
use crate::inodes::{Inodes, VirtualIno};
use crate::mount::{self, file_attr};
use crate::namespec::NameSpec;

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
    fn get_operation(fs: &GitFs, ino: Inodes) -> anyhow::Result<Self> {
        let ino = u64::from(ino.to_norm());
        let mask: u64 = (1u64 << 48) - 1;
        let repo_dir = GitFs::ino_to_repo_id(ino);
        if ino == ROOT_INO {
            Ok(FsOperationContext::Root)
        } else if ino & mask == 0 && fs.repos_list.contains_key(&repo_dir) {
            // If the least significant 48 bits are 0
            Ok(FsOperationContext::RepoDir { ino })
        } else if fs.is_in_live(ino) {
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
// Each repo has a repo_id--<16bits repo-id>
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
    pub repos_list: BTreeMap<u16, Arc<Mutex<GitRepo>>>, // <repo_id, repo>
    next_inode: HashMap<u16, AtomicU64>,                // Each Repo has a set of inodes
    current_handle: AtomicU64,
    handles: RwLock<HashMap<u64, Handle>>, // (fh, Handle)
    read_only: bool,
    vfile_entry: RwLock<HashMap<VirtualIno, VFileEntry>>,
    pub notifier: Arc<OnceLock<fuser::Notifier>>,
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
        let mut fs = Self {
            repos_dir,
            repos_list: BTreeMap::new(),
            read_only,
            handles: RwLock::new(HashMap::new()),
            current_handle: AtomicU64::new(1),
            next_inode: HashMap::new(),
            vfile_entry: RwLock::new(HashMap::new()),
            notifier,
        };
        fs.ensure_base_dirs_exist()?;
        for entry in fs.repos_dir.read_dir()? {
            let entry = entry?;
            let repo_name_os = entry.file_name();
            let repo_name = repo_name_os.to_str().context("Not a valid UTF-8 name")?;
            let repo_path = entry.path();
            if !repo_path.join(META_STORE).exists() {
                continue;
            }
            let repo = fs.load_repo(repo_name)?;
            let repo_id = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?.repo_id;
            fs.repos_list.insert(repo_id, repo);
            info!("Repo {repo_name} added with id {repo_id}");
        }
        Ok(Arc::from(Mutex::new(fs)))
    }

    pub fn load_repo(&mut self, repo_name: &str) -> anyhow::Result<Arc<Mutex<GitRepo>>> {
        let repo_path = self.repos_dir.join(repo_name);

        let mut connection = self.init_meta_db(repo_name)?;

        let repo_id = connection.get_repo_id()?;
        let repo_ino = GitFs::repo_id_to_ino(repo_id);
        self.next_inode
            .insert(repo_id, AtomicU64::from(repo_ino + 1));
        if self.repos_list.contains_key(&repo_id) {
            info!("{repo_id} already exists");
            connection.change_repo_id(repo_id)?;
        }

        let repo = git2::Repository::init(repo_path)?;
        let res_inodes = connection.populate_res_inodes()?;
        info!(
            "Populated {} inodes into repo {}",
            res_inodes.len(),
            repo_name
        );

        let mut git_repo = GitRepo {
            connection: Arc::new(Mutex::new(connection)),
            repo_dir: repo_name.to_owned(),
            repo_id,
            live_exists: true,
            inner: repo,
            head: None,
            snapshots: BTreeMap::new(),
            res_inodes,
            vdir_cache: BTreeMap::new(),
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
        self.repos_list.insert(repo_id, repo_rc.clone());
        Ok(repo_rc)
    }

    pub fn new_repo(&mut self, repo_name: &str) -> anyhow::Result<Arc<Mutex<GitRepo>>> {
        let repo_path = self.repos_dir.join(repo_name);
        if repo_path.exists() {
            bail!("Repo already exists");
        }
        std::fs::create_dir(&repo_path)?;
        std::fs::set_permissions(&repo_path, std::fs::Permissions::from_mode(0o775))?;
        let mut connection = self.init_meta_db(repo_name)?;

        let repo_id = self.next_repo_id();
        let repo_ino = (repo_id as u64) << REPO_SHIFT;
        self.next_inode
            .insert(repo_id, AtomicU64::from(repo_ino + 1));

        let mut repo_attr: FileAttr = mount::dir_attr().into();
        repo_attr.ino = repo_ino;
        let mut nodes: Vec<(u64, String, FileAttr)> = vec![(ROOT_INO, repo_name.into(), repo_attr)];

        let live_ino = self.next_inode_raw(repo_ino)?;

        let repo = git2::Repository::init(repo_path)?;

        let live_name = "live".to_string();

        let perms = 0o775;
        let st_mode = libc::S_IFDIR | perms;
        let live_attr = build_attr_dir(live_ino, st_mode);

        nodes.push((repo_ino, live_name, live_attr));
        connection.write_inodes_to_db(nodes)?;

        let connection = Arc::from(Mutex::from(connection));

        let mut git_repo = GitRepo {
            connection,
            repo_dir: repo_name.to_owned(),
            repo_id,
            live_exists: true,
            inner: repo,
            head: None,
            snapshots: BTreeMap::new(),
            res_inodes: HashSet::new(),
            vdir_cache: BTreeMap::new(),
        };

        git_repo.res_inodes.insert(live_ino);
        let repo_rc = Arc::from(Mutex::from(git_repo));
        self.repos_list.insert(repo_id, repo_rc.clone());
        Ok(repo_rc)
    }

    pub fn open_repo(&self, repo_name: &str) -> anyhow::Result<GitRepo> {
        let repo_path = PathBuf::from(&self.repos_dir).join(repo_name).join("git");
        let repo = Repository::open(&repo_path)?;
        let head = repo.revparse_single("HEAD")?.id();
        let db = self.open_meta_db(repo_name)?;

        let mut stmt = db.conn.prepare(
            "SELECT inode
               FROM inode_map
              LIMIT 1",
        )?;

        let opt: Option<i64> = stmt.query_row(params![], |row| row.get(0)).optional()?;

        let ino = opt.ok_or_else(|| anyhow!("no inodes in inode_map"))?;
        let repo_id = (ino >> REPO_SHIFT) as u16;
        drop(stmt);

        Ok(GitRepo {
            connection: Arc::from(Mutex::from(db)),
            repo_dir: repo_name.to_string(),
            repo_id,
            live_exists: true,
            inner: repo,
            head: Some(head),
            snapshots: BTreeMap::new(),
            res_inodes: HashSet::new(),
            vdir_cache: BTreeMap::new(),
        })
    }

    pub fn open_meta_db<P: AsRef<Path>>(&self, repo_name: P) -> anyhow::Result<MetaDb> {
        let db_path = PathBuf::from(&self.repos_dir)
            .join(repo_name)
            .join(META_STORE);
        let conn = Connection::open(&db_path)?;
        Ok(MetaDb { conn })
    }

    pub fn init_meta_db<P: AsRef<Path>>(&self, repo_name: P) -> anyhow::Result<MetaDb> {
        // Must take in the name of the folder of the REPO
        // repos_dir/repo_name1
        //------------------├── fs_meta.db
        //------------------└── git/
        let db_path = PathBuf::from(&self.repos_dir)
            .join(repo_name)
            .join(META_STORE);
        let conn = Connection::open(&db_path)?;

        // DB layout
        //   inode        INTEGER   PRIMARY KEY,    -> the u64 inode
        //   parent_inode INTEGER   NOT NULL,       -> the parent directory’s inode
        //   name         TEXT      NOT NULL,       -> the filename or directory name
        //   oid          TEXT      NOT NULL,       -> the Git OID
        //   filemode     INTEGER   NOT NULL        -> the raw Git filemode
        conn.execute_batch(
            r#"
                CREATE TABLE IF NOT EXISTS inode_map (
                    inode        INTEGER PRIMARY KEY,
                    parent_inode INTEGER NOT NULL,
                    name         TEXT    NOT NULL,
                    oid          TEXT    NOT NULL,
                    filemode     INTEGER NOT NULL
                );
            "#,
        )?;

        Ok(MetaDb { conn })
    }

    #[instrument(level = "debug", skip(self), fields(ino), ret(level = Level::DEBUG), err(Display))]
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

        let parent = self.get_parent_ino(ino.to_u64_n())?;
        let par_mode = self.get_mode_from_db(parent)?;
        let parent_kind = match par_mode {
            git2::FileMode::Tree | git2::FileMode::Commit => FileType::Directory,
            _ => FileType::RegularFile,
        };

        let target_mode = self.get_mode_from_db(ino.to_u64_n())?;
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
                            ops::open::open_git(self, ino.to_norm(), read, write)
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
        // let parent = self.get_parent_ino(ino)?;
        // let if self.is_file(parent) {

        // }
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
            FsOperationContext::InsideGitDir { ino } => {
                ops::write::write_git(self, ino, offset, buf, fh)
            }
        }
    }

    #[instrument(level = "debug", skip(self), fields(ino), ret(level = Level::DEBUG), err(Display))]
    pub fn release(&self, fh: u64) -> anyhow::Result<bool> {
        let mut guard = self.handles.write().map_err(|_| anyhow!("Lock poisoned"))?;
        Ok(guard.remove(&fh).is_some())
    }

    fn object_to_file_attr(&self, ino: u64, git_attr: &ObjectAttr) -> anyhow::Result<FileAttr> {
        let blocks = git_attr.size.div_ceil(512);

        // Compute atime and mtime from commit_time
        let commit_secs = git_attr.commit_time.seconds() as u64;
        let time = UNIX_EPOCH + Duration::from_secs(commit_secs);

        let kind = match git_attr.kind {
            ObjectType::Blob if git_attr.filemode == 0o120000 => FileType::Symlink,
            ObjectType::Tree => FileType::Directory,
            ObjectType::Commit => FileType::Directory,
            _ => FileType::RegularFile,
        };
        let mut perm = 0o555;
        if kind != FileType::Directory {
            perm = 0o655
        }

        let nlink = if kind == FileType::Directory { 2 } else { 1 };

        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };
        let rdev = 0;
        let blksize = 4096;
        let flags = 0;

        Ok(FileAttr {
            ino,
            oid: git_attr.oid,
            size: git_attr.size,
            blocks,
            atime: time,
            mtime: time,
            ctime: time,
            crtime: time,
            kind,
            perm,
            mode: git_attr.filemode,
            nlink,
            uid,
            gid,
            rdev,
            blksize,
            flags,
        })
    }

    #[instrument(level = "debug", skip(self), fields(ino), ret(level = Level::DEBUG), err(Display))]
    pub fn getattr(&self, target: u64) -> anyhow::Result<FileAttr> {
        let perms = 0o775;
        let st_mode = libc::S_IFDIR | perms;

        let ino: Inodes = target.into();

        if !self.exists(ino)? {
            bail!(format!("Inode {} does not exist", ino));
        }

        let ctx = FsOperationContext::get_operation(self, ino);
        match ctx? {
            FsOperationContext::Root => Ok(build_attr_dir(ROOT_INO, st_mode)),
            FsOperationContext::RepoDir { ino: _ } => {
                let attr = build_attr_dir(ino.to_u64_n(), st_mode);
                match ino {
                    Inodes::NormalIno(_) => Ok(attr),
                    Inodes::VirtualIno(_) => self.prepare_virtual_file(attr),
                }
            }
            FsOperationContext::InsideLiveDir { ino: _ } => match ino {
                Inodes::NormalIno(_) => ops::getattr::getattr_live_dir(self, ino.to_norm()),
                Inodes::VirtualIno(_) => {
                    let attr = ops::getattr::getattr_live_dir(self, ino.to_norm())?;
                    match attr.kind {
                        // If original is a file, create a virtual directory
                        // Used when trying to cd into a file
                        FileType::RegularFile => self.prepare_virtual_folder(attr),
                        // If original is a directory, create a virtual file
                        // Used when trying to cat a directory
                        FileType::Directory => self.prepare_virtual_file(attr),
                        _ => bail!("Invalid attr"),
                    }
                }
            },
            FsOperationContext::InsideGitDir { ino: _ } => match ino {
                Inodes::NormalIno(_) => ops::getattr::getattr_git_dir(self, ino.to_norm()),
                Inodes::VirtualIno(_) => {
                    let attr = ops::getattr::getattr_git_dir(self, ino.to_norm())?;
                    match attr.kind {
                        // If original is a file, create a virtual directory
                        // Used when trying to cd into a file
                        FileType::RegularFile => self.prepare_virtual_folder(attr),
                        // If original is a directory, create a virtual file
                        // Used when trying to cat a directory
                        FileType::Directory => self.prepare_virtual_file(attr),
                        _ => bail!("Invalid attr"),
                    }
                }
            },
        }
    }

    // When fetching a repo takes name as:
    // website.accoount.repo_name
    // example:github.tokio.tokio-rs.git -> https://github.com/tokio-rs/tokio.git
    pub fn mkdir(
        &mut self,
        parent: u64,
        os_name: &OsStr,
        create_attr: CreateFileAttr,
    ) -> anyhow::Result<FileAttr> {
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
            FsOperationContext::Root => ops::mkdir::mkdir_root(self, ROOT_INO, name, create_attr),
            FsOperationContext::RepoDir { ino } => {
                ops::mkdir::mkdir_repo(self, ino, name, create_attr)
            }
            FsOperationContext::InsideLiveDir { ino } => {
                ops::mkdir::mkdir_live(self, ino, name, create_attr)
            }
            FsOperationContext::InsideGitDir { ino } => {
                ops::mkdir::mkdir_git(self, ino, name, create_attr)
            }
        }
    }

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
                bail!("This directory is read only")
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
                bail!("This directory is read only")
            }
            FsOperationContext::RepoDir { ino: _ } => {
                bail!("Not allowed")
            }
            FsOperationContext::InsideLiveDir { ino } => ops::unlink::unlink_live(self, ino, name),
            FsOperationContext::InsideGitDir { ino: _ } => {
                bail!("This directory is read only")
            }
        }
    }

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
            FsOperationContext::InsideGitDir { ino: _ } => bail!("This directory is read only"),
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
                ops::rmdir::rmdir_live(self, parent.to_u64_n(), name)
            }
            FsOperationContext::InsideLiveDir { ino: _ } => {
                ops::rmdir::rmdir_live(self, parent.to_u64_n(), name)
            }
            FsOperationContext::InsideGitDir { ino: _ } => Ok(()),
        }
    }

    #[instrument(level = "debug", skip(self), fields(parent, return_len = field::Empty), err(Display))]
    pub fn readdir(&self, parent: u64) -> anyhow::Result<Vec<DirectoryEntry>> {
        let ret: anyhow::Result<Vec<DirectoryEntry>> = {
            let parent: Inodes = parent.into();

            let ctx = FsOperationContext::get_operation(self, parent);
            match ctx? {
                FsOperationContext::Root => ops::readdir::readdir_root_dir(self),
                FsOperationContext::RepoDir { ino } => ops::readdir::readdir_repo_dir(self, ino),
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
                let Some(attr) = ops::lookup::lookup_repo(self, parent.to_u64_n(), name)? else {
                    return Ok(None);
                };
                if spec.is_virtual() && attr.kind == FileType::Directory {
                    return Ok(Some(self.prepare_virtual_file(attr)?));
                }
                return Ok(Some(attr));
            }
            FsOperationContext::InsideLiveDir { ino: _ } => {
                // If the target has is a virtual, either File or Dir
                if spec.is_virtual() {
                    let Some(attr) = ops::lookup::lookup_live(self, parent.to_norm(), name)? else {
                        return Ok(None);
                    };
                    match attr.kind {
                        FileType::RegularFile => {
                            return Ok(Some(self.prepare_virtual_folder(attr)?));
                        }
                        FileType::Directory => return Ok(Some(self.prepare_virtual_file(attr)?)),
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
                    match attr.kind {
                        FileType::RegularFile => {
                            return Ok(Some(self.prepare_virtual_folder(attr)?));
                        }
                        FileType::Directory => return Ok(Some(self.prepare_virtual_file(attr)?)),
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
                    Inodes::VirtualIno(_) => {
                        debug!("Looking up {} {}", parent.to_virt().0, name);
                        ops::lookup::lookup_vdir(self, parent.to_virt(), name)
                    }
                }
            }
        }
    }

    pub fn name_selection(&self, attr: FileAttr, spec: NameSpec) -> anyhow::Result<FileAttr> {
        if attr.oid == Oid::zero() && attr.kind != FileType::RegularFile {
            return Ok(attr);
        }

        match spec.line {
            Some(Some(_)) => {
                todo!()
            }
            Some(None) => self.prepare_virtual_folder(attr),
            None => Ok(attr),
        }
    }

    pub fn prepare_virtual_file(&self, attr: FileAttr) -> anyhow::Result<FileAttr> {
        let mut new_attr: FileAttr = file_attr().into();
        let ino: Inodes = attr.ino.into();
        let v_ino = ino.to_u64_v();

        new_attr.size = 512;
        new_attr.mode = 0o100444;
        new_attr.kind = FileType::RegularFile;
        new_attr.perm = 0o444;
        new_attr.nlink = 1;
        new_attr.ino = v_ino;
        new_attr.blksize = 4096;

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

// gitfs_helpers
impl GitFs {
    pub fn set_vdir_bit(&self, ino: u64) -> u64 {
        ino | VDIR_BIT
    }

    pub fn clear_vdir_bit(&self, ino: u64) -> u64 {
        ino & !VDIR_BIT
    }

    pub fn refresh_attr(&self, attr: &mut FileAttr) -> anyhow::Result<FileAttr> {
        let path = self.build_full_path(attr.ino)?;
        let metadata = path.metadata()?;
        let std_type = metadata.file_type();
        let actual = if std_type.is_dir() {
            FileType::Directory
        } else if std_type.is_file() {
            FileType::RegularFile
        } else if std_type.is_symlink() {
            FileType::Symlink
        } else {
            bail!("Invalid input")
        };
        if attr.kind != actual {
            bail!("Invalid input")
        }

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

        Ok(*attr)
    }

    fn attr_from_path(&self, path: PathBuf) -> anyhow::Result<FileAttr> {
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
            oid: Oid::zero(),
            size: metadata.size(),
            blocks: metadata.blocks(),
            atime,
            mtime,
            ctime,
            crtime,
            kind,
            perm: 0o775,
            mode: st_mode,
            nlink: metadata.nlink() as u32,
            uid: unsafe { libc::geteuid() },
            gid: unsafe { libc::getgid() },
            rdev: metadata.rdev() as u32,
            blksize: metadata.blksize() as u32,
            flags: 0,
        })
    }

    fn build_path(&self, parent: u64, name: &str) -> anyhow::Result<PathBuf> {
        let parent = self.clear_vdir_bit(parent);
        let repo_name = {
            let repo = &self.get_repo(parent)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.repo_dir.clone()
        };
        let path_to_repo = PathBuf::from(&self.repos_dir).join(repo_name);

        let live_ino = self.get_live_ino(parent);
        if parent == live_ino {
            return Ok(path_to_repo.join(name));
        }

        let conn_arc = {
            let repo = &self.get_repo(parent)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let db_path = conn.get_path_from_db(parent)?;
        Ok(PathBuf::from(&self.repos_dir).join(db_path).join(name))
    }

    fn get_repo(&self, ino: u64) -> anyhow::Result<Arc<Mutex<GitRepo>>> {
        let repo_id = (ino >> REPO_SHIFT) as u16;
        let repo = self
            .repos_list
            .get(&repo_id)
            .ok_or_else(|| anyhow!("No repo for {ino}"))?;
        Ok(repo.clone())
    }

    pub fn get_parent_commit(&self, ino: u64) -> anyhow::Result<(Oid, String)> {
        let ino = self.clear_vdir_bit(ino);
        let repo_arc = self.get_repo(ino)?;

        let mut cur = ino;
        let mut oid = self.get_oid_from_db(ino)?;
        while {
            let repo = repo_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.inner.find_commit(oid).is_err()
        } {
            let parent_ino = self.get_parent_ino(cur)?;
            oid = self.get_oid_from_db(parent_ino)?;
            if oid == Oid::zero() {
                bail!("Parent commit not found");
            }
            cur = parent_ino;
        }
        Ok((oid, self.get_name_from_db(cur)?))
    }

    fn is_in_live(&self, ino: u64) -> bool {
        let live_ino = self.get_live_ino(ino);
        if live_ino == ino || self.is_virtual(ino) {
            return true;
        }
        let mut target_ino = ino;

        loop {
            let parent = match self.get_parent_ino(target_ino) {
                Ok(p) => p,
                Err(_) => return false,
            };
            if parent == live_ino {
                return true;
            }
            target_ino = parent;
        }
    }

    fn next_inode_checked(&self, parent: u64) -> anyhow::Result<u64> {
        let repo_arc = self.get_repo(parent)?;
        let mut repo = repo_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;

        loop {
            let ino = self.next_inode_raw(parent)?;

            if repo.res_inodes.insert(ino) {
                info!("Issuing ino {ino}");
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

    pub fn get_parent_ino(&self, ino: u64) -> anyhow::Result<u64> {
        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_parent_ino(self.clear_vdir_bit(ino))
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

    fn exists_by_name(&self, parent: u64, name: &str) -> anyhow::Result<Option<u64>> {
        let conn_arc = {
            let repo = &self.get_repo(parent)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.exists_by_name(self.clear_vdir_bit(parent), name)
    }

    /// Takes Inodes as virtual inodes do not "exist"
    pub fn exists(&self, ino: Inodes) -> anyhow::Result<bool> {
        let ino = ino.to_u64_n();
        let ino = self.clear_vdir_bit(ino);
        if ino == ROOT_INO {
            return Ok(true);
        }

        let ino: Inodes = ino.into();

        let ctx = FsOperationContext::get_operation(self, ino);
        match ctx? {
            FsOperationContext::Root => Ok(true),
            FsOperationContext::RepoDir { ino: _ } => Ok(true),
            FsOperationContext::InsideLiveDir { ino } => {
                let path = self.build_full_path(ino)?;
                Ok(path.exists())
            }
            FsOperationContext::InsideGitDir { ino } => Ok(self.build_full_path(ino).is_ok()),
        }
    }

    /// Needs to be passed the actual u64 inode
    fn is_dir(&self, ino: Inodes) -> anyhow::Result<bool> {
        // let ino = self.clear_vdir_bit(ino);
        if ino == ROOT_INO {
            return Ok(true);
        }
        let repo_id = GitFs::ino_to_repo_id(ino.to_u64_n());
        if ino == GitFs::repo_id_to_ino(repo_id) {
            return Ok(true);
        }
        let mode = self.get_mode_from_db(ino.to_u64_n())?;
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
    fn is_file(&self, ino: u64) -> anyhow::Result<bool> {
        let ino = self.clear_vdir_bit(ino);
        if ino == ROOT_INO {
            return Ok(false);
        }
        let mode = self.get_mode_from_db(ino)?;
        Ok(mode == FileMode::Blob || mode == FileMode::BlobExecutable)
    }

    /// Needs to be passed the actual u64 inode
    fn is_link(&self, ino: u64) -> anyhow::Result<bool> {
        let ino = self.clear_vdir_bit(ino);
        if ino == ROOT_INO {
            return Ok(false);
        }
        let mode = self.get_mode_from_db(ino)?;
        Ok(mode == FileMode::Link)
    }

    pub fn is_virtual(&self, ino: u64) -> bool {
        (ino & VDIR_BIT) != 0
    }

    fn get_ino_from_db(&self, parent: u64, name: &str) -> anyhow::Result<u64> {
        let parent = self.clear_vdir_bit(parent);
        let conn_arc = {
            let repo = &self.get_repo(parent)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_ino_from_db(parent, name)
    }

    fn remove_db_record(&self, ino: u64) -> anyhow::Result<()> {
        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.remove_db_record(self.clear_vdir_bit(ino))
    }

    fn build_full_path(&self, ino: u64) -> anyhow::Result<PathBuf> {
        let ino = self.clear_vdir_bit(ino);
        let repo_ino = {
            let repo = self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            GitFs::repo_id_to_ino(repo.repo_id)
        };
        let path = PathBuf::from(&self.repos_dir);
        if ino == repo_ino {
            return Ok(path);
        }
        let db_path = &self.get_path_from_db(ino)?;
        let filename = db_path.file_name().ok_or_else(|| anyhow!("No filename"))?;
        if filename == OsStr::new("live") {
            Ok(path)
        } else {
            Ok(path.join(db_path))
        }
    }

    fn get_path_from_db(&self, ino: u64) -> anyhow::Result<PathBuf> {
        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_path_from_db(self.clear_vdir_bit(ino))
    }

    fn get_oid_from_db(&self, ino: u64) -> anyhow::Result<Oid> {
        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_oid_from_db(self.clear_vdir_bit(ino))
    }

    fn get_mode_from_db(&self, ino: u64) -> anyhow::Result<git2::FileMode> {
        // Live directory does not exist in the DB. Handle it separately.
        if ino == 0 {
            return Ok(FileMode::Tree);
        }
        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let mode = conn.get_mode_from_db(self.clear_vdir_bit(ino))?;
        repo::try_into_filemode(mode).ok_or_else(|| anyhow!("Invalid filemode"))
    }

    fn get_name_from_db(&self, ino: u64) -> anyhow::Result<String> {
        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_name_from_db(self.clear_vdir_bit(ino))
    }

    /// nodes = Vec<parent inode, entry name, entry attr>
    fn write_inodes_to_db(&self, nodes: Vec<(u64, String, FileAttr)>) -> anyhow::Result<()> {
        if nodes.is_empty() {
            return Ok(());
        }
        let conn_arc = {
            let repo = &self.get_repo(nodes[0].0)?;
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
