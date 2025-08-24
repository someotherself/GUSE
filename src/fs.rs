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
use tracing::info;

use crate::fs::fileattr::{CreateFileAttr, FileAttr, FileType, ObjectAttr, build_attr_dir};
use crate::fs::meta_db::MetaDb;
use crate::fs::ops::readdir::{DirectoryEntry, DirectoryEntryPlus};
use crate::fs::repo::{GitRepo, VirtualNode};
use crate::mount;
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

enum FsOperationContext {
    // Is the root directory
    Root,
    // Is one of the directories holding a repo
    RepoDir { ino: u64 },
    // Dir or File inside the live dir
    InsideLiveDir { ino: u64 },
    // Dir or File inside a repo dir
    InsideGitDir { ino: u64 },
}

impl FsOperationContext {
    fn get_operation(fs: &GitFs, ino: u64) -> anyhow::Result<Self> {
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
// ino for repo_1 folder1:  0000000000000001000000000....0002
// ino for repo_1 folder2:  0000000000000001000000000....0003

// repo_id for repo 2       0000000000000002
// ino for repo 2 root dir  0000000000000002000000000....0000
// ino for repo_2 live dir: 0000000000000002000000000....0001
// ino for repo_2 folder1:  0000000000000002000000000....0002

// ino for repo folder  = (repo_id as u64) << 48 (see REPO_SHIFT)
// repo_id from ino     = (ino >> REPO_SHIFT) as u16
pub struct GitFs {
    pub repos_dir: PathBuf,
    pub repos_list: BTreeMap<u16, Arc<Mutex<GitRepo>>>, // <repo_id, repo>
    next_inode: HashMap<u16, AtomicU64>,                // Each Repo has a set of inodes
    current_handle: AtomicU64,
    handles: RwLock<HashMap<u64, Handle>>, // (fh, Handle)
    read_only: bool,
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
    pub fn new(repos_dir: PathBuf, read_only: bool) -> anyhow::Result<Arc<Mutex<Self>>> {
        let mut fs = Self {
            repos_dir,
            repos_list: BTreeMap::new(),
            read_only,
            handles: RwLock::new(HashMap::new()),
            current_handle: AtomicU64::new(1),
            next_inode: HashMap::new(),
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

        let mut git_repo = GitRepo {
            connection: Arc::new(Mutex::new(connection)),
            repo_dir: repo_name.to_owned(),
            repo_id,
            inner: repo,
            head: None,
            snapshots: BTreeMap::new(),
            res_inodes,
            vdir_cache: BTreeMap::new(),
            vdir_map: BTreeMap::new(),
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
        repo_attr.inode = repo_ino;
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

        let git_repo = GitRepo {
            connection,
            repo_dir: repo_name.to_owned(),
            repo_id,
            inner: repo,
            head: None,
            snapshots: BTreeMap::new(),
            res_inodes: HashSet::new(),
            vdir_cache: BTreeMap::new(),
            vdir_map: BTreeMap::new(),
        };

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

        let inode = opt.ok_or_else(|| anyhow!("no inodes in inode_map"))?;
        let repo_id = (inode >> REPO_SHIFT) as u16;
        drop(stmt);

        Ok(GitRepo {
            connection: Arc::from(Mutex::from(db)),
            repo_dir: repo_name.to_string(),
            repo_id,
            inner: repo,
            head: Some(head),
            snapshots: BTreeMap::new(),
            res_inodes: HashSet::new(),
            vdir_cache: BTreeMap::new(),
            vdir_map: BTreeMap::new(),
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

    pub fn open(&self, ino: u64, read: bool, write: bool, truncate: bool) -> anyhow::Result<u64> {
        if write && self.read_only {
            bail!("Filesystem is in read only");
        }
        if !write && !read {
            bail!("Read and write cannot be false at the same time");
        }
        if self.is_dir(ino)? {
            bail!("Target is a directory");
        }
        let ctx = FsOperationContext::get_operation(self, ino);
        match ctx? {
            FsOperationContext::Root => bail!("Target is a directory"),
            FsOperationContext::RepoDir { ino: _ } => bail!("Target is a directory"),
            FsOperationContext::InsideLiveDir { ino: _ } => {
                ops::open::open_live(self, ino, read, write, truncate)
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
                ops::open::open_git(self, ino, read, write, truncate)
            }
        }
    }

    pub fn read(&self, ino: u64, offset: u64, buf: &mut [u8], fh: u64) -> anyhow::Result<usize> {
        let ctx = FsOperationContext::get_operation(self, ino);
        match ctx? {
            FsOperationContext::Root => bail!("Not allowed"),
            FsOperationContext::RepoDir { ino: _ } => bail!("Not allowed"),
            FsOperationContext::InsideLiveDir { ino } => {
                ops::read::read_live(self, ino, offset, buf, fh)
            }
            FsOperationContext::InsideGitDir { ino } => {
                ops::read::read_git(self, ino, offset, buf, fh)
            }
        }
    }

    pub fn write(&self, ino: u64, offset: u64, buf: &[u8], fh: u64) -> anyhow::Result<usize> {
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

    pub fn release(&self, fh: u64) -> anyhow::Result<bool> {
        let mut guard = self.handles.write().map_err(|_| anyhow!("Lock poisoned"))?;
        Ok(guard.remove(&fh).is_some())
    }

    fn object_to_file_attr(&self, inode: u64, git_attr: &ObjectAttr) -> anyhow::Result<FileAttr> {
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
            inode,
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

    pub fn getattr(&self, inode: u64) -> anyhow::Result<FileAttr> {
        if !self.exists(inode)? {
            bail!(format!("Inode {} does not exist", inode));
        }
        let perms = 0o775;
        let st_mode = libc::S_IFDIR | perms;

        let (is_vdir, inode) = if inode != ROOT_INO {
            let repo = self.get_repo(inode)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            if let Some(real_ino) = repo.vdir_map.get(&inode) {
                (true, *real_ino)
            } else {
                (false, inode)
            }
        } else {
            (false, inode)
        };

        let ctx = FsOperationContext::get_operation(self, inode);
        match ctx? {
            FsOperationContext::Root => Ok(build_attr_dir(ROOT_INO, st_mode)),
            FsOperationContext::RepoDir { ino } => Ok(build_attr_dir(ino, st_mode)),
            FsOperationContext::InsideLiveDir { ino } => {
                let attr = ops::getattr::getattr_live_dir(self, ino)?;
                if !is_vdir {
                    return Ok(attr);
                }

                self.prepare_virtual_folder(attr)
            }
            FsOperationContext::InsideGitDir { ino } => {
                let attr = ops::getattr::getattr_git_dir(self, ino)?;
                if !is_vdir {
                    return Ok(attr);
                }

                self.prepare_virtual_folder(attr)
            }
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

    pub fn unlink(&self, parent: u64, os_name: &OsStr) -> anyhow::Result<()> {
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
        if self.read_only {
            bail!("Filesystem is in read only");
        }
        if !self.exists(parent)? {
            bail!(format!("Parent {} does not exist", parent));
        }
        if !self.exists(new_parent)? {
            bail!(format!("New parent {} does not exist", new_parent));
        }
        if !self.is_in_live(new_parent) {
            bail!(format!("New parent {} not allowed", new_parent));
        }

        let name = os_name
            .to_str()
            .ok_or_else(|| anyhow!("Not a valid UTF-8 name"))?;
        let new_name = os_new_name
            .to_str()
            .ok_or_else(|| anyhow!("Not a valid UTF-8 name"))?;

        if self.find_by_name(parent, name).is_err() {
            bail!(format!("Source {} does not exist", name));
        }

        if name == "." || name == ".." || new_name == "." || new_name == ".." {
            bail!("invalid name");
        }

        if name.contains('/') || name.contains('\\') {
            bail!(format!("Invalid name {}", name));
        }

        if new_name.contains('/') || new_name.contains('\\') {
            bail!(format!("Invalid name {}", new_name));
        }

        let ctx = FsOperationContext::get_operation(self, parent);
        match ctx? {
            FsOperationContext::Root => {
                bail!("This directory is read only")
            }
            FsOperationContext::RepoDir { ino: _ } => {
                bail!("Not allowed")
            }
            FsOperationContext::InsideLiveDir { ino } => {
                ops::rename::rename_live(self, ino, name, new_parent, new_name)
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
                bail!("This directory is read only")
            }
        }
    }

    pub fn rmdir(&self, parent: u64, os_name: &OsStr) -> anyhow::Result<()> {
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
                bail!("This directory is read only")
            }
            FsOperationContext::InsideLiveDir { ino } => ops::rmdir::rmdir_live(self, ino, name),
            FsOperationContext::InsideGitDir { ino: _ } => {
                bail!("This directory is read only")
            }
        }
    }

    pub fn readdir(&self, parent: u64) -> anyhow::Result<Vec<DirectoryEntry>> {
        let ctx = FsOperationContext::get_operation(self, parent);
        match ctx? {
            FsOperationContext::Root => ops::readdir::readdir_root_dir(self),
            FsOperationContext::RepoDir { ino } => ops::readdir::readdir_repo_dir(self, ino),
            FsOperationContext::InsideLiveDir { ino } => ops::readdir::readdir_live_dir(self, ino),
            FsOperationContext::InsideGitDir { ino } => ops::readdir::readdir_git_dir(self, ino),
        }
    }

    pub fn readdirplus(&self, parent: u64) -> anyhow::Result<Vec<DirectoryEntryPlus>> {
        let mut entries_plus: Vec<DirectoryEntryPlus> = vec![];
        let entries = self.readdir(parent)?;
        for entry in entries {
            let attr = self
                .find_by_name(parent, &entry.name)?
                .ok_or_else(|| anyhow!("Repo not found"))?;
            let entry_plus = DirectoryEntryPlus { entry, attr };
            entries_plus.push(entry_plus);
        }
        Ok(entries_plus)
    }

    pub fn find_by_name(&self, parent: u64, name: &str) -> anyhow::Result<Option<FileAttr>> {
        if !self.exists(parent)? {
            bail!(format!("Parent {} does not exist", parent));
        }
        if !self.is_dir(parent)? {
            bail!(format!("Parent {} is not a directory", parent));
        }

        let spec = NameSpec::parse(name);

        let ctx = FsOperationContext::get_operation(self, parent);
        match ctx? {
            FsOperationContext::Root => ops::lookup::lookup_root(self, name),
            FsOperationContext::RepoDir { ino } => ops::lookup::lookup_repo(self, ino, name),
            FsOperationContext::InsideLiveDir { ino } => {
                let attr = match ops::lookup::lookup_live(self, ino, name)? {
                    Some(attr) => attr,
                    None => return Ok(None),
                };
                if !spec.is_virtual() {
                    return Ok(Some(attr));
                }
                Ok(Some(self.prepare_virtual_folder(attr)?))
            }
            FsOperationContext::InsideGitDir { ino } => {
                let attr = match ops::lookup::lookup_git(self, ino, name)? {
                    Some(attr) => attr,
                    None => return Ok(None),
                };
                if !spec.is_virtual() {
                    return Ok(Some(attr));
                }
                Ok(Some(self.prepare_virtual_folder(attr)?))
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

    pub fn prepare_virtual_folder(&self, attr: FileAttr) -> anyhow::Result<FileAttr> {
        let repo_arc = self.get_repo(attr.inode)?;
        let mut new_attr = attr;

        {
            let repo = repo_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            if let Some(e) = repo.vdir_cache.get(&attr.inode) {
                new_attr.inode = e.inode;
                new_attr.perm = 0o555;
                new_attr.size = 0;
                new_attr.kind = FileType::Directory;
                new_attr.nlink = 2;
                return Ok(new_attr);
            }
        }

        let v_ino = self.next_inode_checked(attr.inode)?;

        {
            let mut repo = repo_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            match repo.vdir_cache.entry(attr.inode) {
                Entry::Occupied(e) => {
                    // Another thread alread inserted an entry
                    let v = e.get();
                    new_attr.inode = v.inode;
                }
                Entry::Vacant(slot) => {
                    let v_node = VirtualNode {
                        real: attr.inode,
                        inode: v_ino,
                        oid: attr.oid,
                        log: vec![],
                    };
                    slot.insert(v_node);
                    new_attr.inode = v_ino;
                    repo.vdir_map.insert(v_ino, attr.inode);
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
    pub fn refresh_attr(&self, attr: &mut FileAttr) -> anyhow::Result<FileAttr> {
        let path = self.build_full_path(attr.inode)?;
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
            inode: 0,
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

    fn get_repo(&self, inode: u64) -> anyhow::Result<Arc<Mutex<GitRepo>>> {
        let repo_id = (inode >> REPO_SHIFT) as u16;
        let repo = self
            .repos_list
            .get(&repo_id)
            .ok_or_else(|| anyhow!("No repo for {inode}"))?;
        Ok(repo.clone())
    }

    pub fn get_parent_commit(&self, ino: u64) -> anyhow::Result<(Oid, String)> {
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
        if live_ino == ino {
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
        let mut inode = self.next_inode_raw(parent)?;
        let repo = self.get_repo(inode)?;
        while {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.res_inodes.contains(&inode)
        } {
            inode = self.next_inode_raw(parent)?;
        }
        Ok(inode)
    }

    fn next_inode_raw(&self, parent: u64) -> anyhow::Result<u64> {
        let repo_id = GitFs::ino_to_repo_id(parent);
        let inode = self
            .next_inode
            .get(&repo_id)
            .ok_or_else(|| anyhow!("No repo for id {repo_id}"))?
            .fetch_add(1, Ordering::SeqCst);
        Ok(inode)
    }

    fn next_file_handle(&self) -> u64 {
        self.current_handle.fetch_add(1, Ordering::SeqCst)
    }

    // TODO: Check if not over 32767
    fn next_repo_id(&self) -> u16 {
        match self.repos_list.keys().next_back() {
            Some(&i) => i
                .checked_add(1)
                .expect("Congrats. Repo ids have overflowed a u16."),
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
        conn.get_parent_ino(ino)
    }

    fn pack_inode(repo_id: u16, sub_ino: u64) -> u64 {
        ((repo_id as u64) << REPO_SHIFT) | (sub_ino & ((1 << REPO_SHIFT) - 1))
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
        conn.exists_by_name(parent, name)
    }

    pub fn exists(&self, ino: u64) -> anyhow::Result<bool> {
        if ino == ROOT_INO {
            return Ok(true);
        }
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

    fn is_dir(&self, ino: u64) -> anyhow::Result<bool> {
        if ino == ROOT_INO {
            return Ok(true);
        }
        let repo_id = GitFs::ino_to_repo_id(ino);
        if ino == GitFs::repo_id_to_ino(repo_id) {
            return Ok(true);
        }
        println!("Searching for ino {ino}");
        let mode = self.get_mode_from_db(ino)?;
        Ok(mode == FileMode::Tree || mode == FileMode::Commit)
    }

    fn is_file(&self, ino: u64) -> anyhow::Result<bool> {
        if ino == ROOT_INO {
            return Ok(false);
        }
        let mode = self.get_mode_from_db(ino)?;
        Ok(mode == FileMode::Blob || mode == FileMode::BlobExecutable)
    }

    fn is_link(&self, ino: u64) -> anyhow::Result<bool> {
        if ino == ROOT_INO {
            return Ok(false);
        }
        let mode = self.get_mode_from_db(ino)?;
        Ok(mode == FileMode::Link)
    }

    fn get_ino_from_db(&self, parent: u64, name: &str) -> anyhow::Result<u64> {
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
        conn.remove_db_record(ino)
    }

    fn build_full_path(&self, ino: u64) -> anyhow::Result<PathBuf> {
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
        conn.get_path_from_db(ino)
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

    fn get_mode_from_db(&self, ino: u64) -> anyhow::Result<git2::FileMode> {
        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        let mode = conn.get_mode_from_db(ino)?;
        repo::try_into_filemode(mode).ok_or_else(|| anyhow!("Invalid filemode"))
    }

    fn get_name_from_db(&self, ino: u64) -> anyhow::Result<String> {
        let conn_arc = {
            let repo = &self.get_repo(ino)?;
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            std::sync::Arc::clone(&repo.connection)
        };
        let conn = conn_arc.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        conn.get_name_from_db(ino)
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
