use std::collections::{BTreeMap, HashSet};
use std::ffi::{OsStr, OsString};
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{BufReader, BufWriter, Write};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
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
use git2::{ObjectType, Oid, Repository};
use rusqlite::{Connection, OptionalExtension, params};

use crate::fs::meta_db::MetaDb;
use crate::fs::repo::GitRepo;

pub mod meta_db;
pub mod repo;

#[cfg(test)]
mod test;

const META_STORE: &str = "fs_meta.db";
pub const REPO_SHIFT: u8 = 48;
pub const ROOT_INO: u64 = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FileAttr {
    // Inode in the fuse fs
    pub inode: u64,
    // SHA-1 in git
    pub oid: Oid,
    // Blob size
    pub size: u64,
    pub blocks: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
    pub kind: FileType,
    pub perm: u16,
    pub mode: u32,
    pub nlink: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub blksize: u32,
    pub flags: u32,
}

#[derive(Clone, Copy, Debug)]
pub struct ObjectAttr {
    pub oid: Oid,
    pub kind: git2::ObjectType,
    pub filemode: u32,
    pub size: u64,
    pub commit_time: git2::Time,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum FileType {
    RegularFile,
    Directory,
    Symlink,
}

#[derive(Clone)]
struct TimesFileAttr {
    pub size: u64,
    pub atime: SystemTime,
    pub mtime: SystemTime,
    pub ctime: SystemTime,
    pub crtime: SystemTime,
}

struct ReadHandleContext {
    ino: u64,
    attr: TimesFileAttr,
    reader: Option<BufReader<File>>,
}

struct WriteHandleContext {
    ino: u64,
    attr: TimesFileAttr,
    writer: Option<BufWriter<File>>,
}

impl FileType {
    pub fn from_filemode(mode: ObjectType) -> anyhow::Result<FileType> {
        match mode {
            ObjectType::Blob => Ok(FileType::RegularFile),
            ObjectType::Tree => Ok(FileType::Directory),
            ObjectType::Tag => Ok(FileType::Symlink),
            _ => bail!("Invalid file type {:?}", mode),
        }
    }
}

impl From<FileAttr> for TimesFileAttr {
    fn from(value: FileAttr) -> Self {
        Self {
            atime: value.atime,
            mtime: value.mtime,
            ctime: value.ctime,
            crtime: value.crtime,
            size: value.size,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CreateFileAttr {
    pub kind: FileType,
    pub perm: u16,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub rdev: u32,
    pub flags: u32,
}

impl From<CreateFileAttr> for FileAttr {
    fn from(value: CreateFileAttr) -> Self {
        let now = SystemTime::now();
        Self {
            inode: 0,
            oid: Oid::zero(),
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: value.kind,
            perm: value.perm,
            mode: value.mode,
            nlink: if value.kind == FileType::Directory {
                2
            } else {
                1
            },
            uid: value.uid,
            gid: value.gid,
            rdev: value.rdev,
            blksize: 0,
            flags: value.flags,
        }
    }
}

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
    fn get_operation(fs: &GitFs, ino: u64, _is_parent: bool) -> anyhow::Result<Self> {
        let mask: u64 = (1u64 << 48) - 1;
        let repo_dir = GitFs::ino_to_repo_id(ino);
        if ino == ROOT_INO {
            Ok(FsOperationContext::Root)
        } else if ino & mask == 0 && fs.repos_list.contains_key(&repo_dir) {
            // If the least significant 48 bits are 0
            Ok(FsOperationContext::RepoDir { ino })
        } else if fs.is_in_live(ino)? {
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
//---------├── live/      <- everything in repo_dir1 except for .git and fs_meta.db
//---------├── commit_1/  <- List of all the commits, served as folders
//---------├── commit_2/  <-
//---------└── commit_3/  <-
//
// Structure of INODES
// Each repo has a repo_id--<16bits repo-id>
// repo_id for repo 1       0000000000000001
// ino for repo 1 root dir  0000000000000001000000000....0000
// ino for repo_1 folder1:  0000000000000001000000000....0001
// ino for repo_1 folder2:  0000000000000001000000000....0002

// repo_id for repo 2       0000000000000002
// ino for repo 2 root dir  0000000000000002000000000....0000
// ino for repo_2 folder1:  0000000000000002000000000....0001

// ino for repo folder  = (repo_id as u64) << 48 (see REPO_SHIFT)
// repo_id from ino     = (ino >> REPO_SHIFT) as u16

pub struct GitFs {
    pub repos_dir: PathBuf,
    pub repos_list: BTreeMap<u16, Arc<GitRepo>>, // <repo_id, repo>
    next_inode: HashMap<u16, AtomicU64>,         // Each Repo has a set of inodes
    current_handle: AtomicU64,
    read_handles: RwLock<HashMap<u64, Mutex<ReadHandleContext>>>, // ino
    write_handles: RwLock<HashMap<u64, Mutex<WriteHandleContext>>>, // ino
    opened_handes_for_read: RwLock<HashMap<u64, HashSet<u64>>>,   // (ino, fh)
    opened_handes_for_write: RwLock<HashMap<u64, HashSet<u64>>>,  // (ino, fh)
    read_only: bool,
}

// gitfs_fuse_functions
impl GitFs {
    pub fn new(repos_dir: PathBuf, read_only: bool) -> anyhow::Result<Arc<Mutex<Self>>> {
        let fs = Self {
            repos_dir,
            repos_list: BTreeMap::new(),
            read_only,
            read_handles: RwLock::new(HashMap::new()),
            write_handles: RwLock::new(HashMap::new()),
            current_handle: AtomicU64::new(1),
            opened_handes_for_read: RwLock::new(HashMap::new()),
            opened_handes_for_write: RwLock::new(HashMap::new()),
            next_inode: HashMap::new(),
        };
        fs.ensure_base_dirs_exist()
            .context("Failed to initialize base directories")?;
        Ok(Arc::from(Mutex::new(fs)))
    }

    pub fn new_repo(&mut self, repo_name: &str) -> anyhow::Result<Arc<GitRepo>> {
        let repo_path = self.repos_dir.join(repo_name);
        if repo_path.exists() {
            bail!("Repo name already exists!")
        }
        std::fs::create_dir(&repo_path).context("Could not create repo dir")?;
        std::fs::set_permissions(&repo_path, std::fs::Permissions::from_mode(0o774))?;
        let mut connection = self.init_meta_db(repo_name)?;

        let repo_id = self.next_repo_id();
        let repo_ino = (repo_id as u64) << REPO_SHIFT;
        let live_ino = repo_ino + 1;

        self.next_inode.insert(repo_id, AtomicU64::from(live_ino));

        let repo = git2::Repository::init(repo_path)?;

        let live_name = "live".to_string();

        let perms = 0o774;
        let st_mode = libc::S_IFDIR | perms;
        let live_attr = build_attr_dir(live_ino, st_mode);
        connection.write_inodes_to_db(vec![(repo_ino, &live_name, live_attr)])?;
        let connection = Arc::from(RwLock::from(connection));

        let git_repo = GitRepo {
            connection,
            repo_dir: repo_name.to_owned(),
            repo_id,
            inner: repo,
            head: None,
        };

        let repo_rc = Arc::from(git_repo);
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

        let opt: Option<i64> = stmt
            .query_row(params![], |row| row.get(0))
            .optional()
            .map_err(|e| anyhow!("DB error fetching an inode: {}", e))?;

        let inode = opt.ok_or_else(|| anyhow!("no inodes in inode_map"))?;
        let repo_id = (inode >> REPO_SHIFT) as u16;
        drop(stmt);

        Ok(GitRepo {
            connection: Arc::from(RwLock::from(db)),
            repo_dir: repo_name.to_string(),
            repo_id,
            inner: repo,
            head: Some(head),
        })
    }

    pub fn open_meta_db<P: AsRef<Path>>(&self, repo_name: P) -> anyhow::Result<MetaDb> {
        let db_path = PathBuf::from(&self.repos_dir)
            .join(repo_name)
            .join(META_STORE);
        let conn = Connection::open(&db_path)
            .with_context(|| format!("Could not find db at path {}", db_path.display()))?;
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
        let conn = Connection::open(&db_path)
            .with_context(|| format!("Could not find db at path {}", db_path.display()))?;

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
                CREATE TABLE IF NOT EXISTS meta (
                    key   TEXT    PRIMARY KEY,
                    value INTEGER NOT NULL
                );
            "#,
        )?;

        Ok(MetaDb { conn })
    }

    pub fn open(&self, ino: u64, read: bool, write: bool) -> anyhow::Result<u64> {
        if write && self.read_only {
            bail!("Filesystem is in read only!")
        }
        if !write && !read {
            bail!("Read and write cannot be false at the same time!")
        }
        if self.is_dir(ino)? {
            bail!("Target must be a file!")
        }
        let ctx = FsOperationContext::get_operation(self, ino, false);
        match ctx? {
            FsOperationContext::Root => {
                bail!("Target must be a file!")
            }
            FsOperationContext::RepoDir { ino: _ } => {
                bail!("Target must be a file!")
            }
            FsOperationContext::InsideLiveDir { ino: _ } => {
                let mut handle: Option<u64> = None;
                if read {
                    handle = Some(self.next_file_handle());
                    self.register_read(ino, handle.unwrap())?;
                }
                if write {
                    if handle.is_none() {
                        handle = Some(self.next_file_handle());
                    }
                    self.register_write(ino, handle.unwrap())?;
                }
                Ok(handle.unwrap())
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
                bail!("GitDir for open is not implemented")
            }
        }
    }

    fn register_read(&self, ino: u64, fh: u64) -> anyhow::Result<()> {
        let attr = self.getattr(ino)?.into();
        let path = self.get_path_from_db(ino)?;
        let reader = std::io::BufReader::new(OpenOptions::new().read(true).open(&path)?);
        let ctx = ReadHandleContext {
            ino,
            attr,
            reader: Some(reader),
        };
        self.read_handles
            .write()
            .unwrap()
            .insert(fh, Mutex::from(ctx));
        self.opened_handes_for_read
            .write()
            .unwrap()
            .entry(ino)
            .or_default()
            .insert(fh);
        Ok(())
    }

    fn register_write(&self, ino: u64, fh: u64) -> anyhow::Result<()> {
        let attr = self.getattr(ino)?.into();
        let path = self.get_path_from_db(ino)?;
        let writer =
            std::io::BufWriter::new(OpenOptions::new().read(true).write(true).open(&path)?);
        let ctx = WriteHandleContext {
            ino,
            attr,
            writer: Some(writer),
        };
        self.write_handles
            .write()
            .unwrap()
            .insert(fh, Mutex::from(ctx));
        self.opened_handes_for_write
            .write()
            .unwrap()
            .entry(ino)
            .or_default()
            .insert(fh);
        Ok(())
    }

    pub fn release(&self, fh: u64) -> anyhow::Result<()> {
        // TODO: Double check
        if fh == 0 {
            return Ok(());
        }

        let mut valid_fh = false;

        let ctx = self.read_handles.write().unwrap().remove(&fh);
        if let Some(ctx) = ctx {
            let ctx = ctx.lock().unwrap();
            let mut opened_files_for_read = self.opened_handes_for_read.write().unwrap();
            opened_files_for_read
                .get_mut(&ctx.ino)
                .context("handle is missing")?
                .remove(&fh);
            if opened_files_for_read
                .get(&ctx.ino)
                .context("handle is missing")?
                .is_empty()
            {
                opened_files_for_read.remove(&ctx.ino);
            }
            valid_fh = true;
        }

        let ctx = self.write_handles.write().unwrap().remove(&fh);
        if let Some(ctx) = ctx {
            let ctx = ctx.lock().unwrap();
            let mut opened_files_for_write = self.opened_handes_for_write.write().unwrap();
            opened_files_for_write
                .get_mut(&ctx.ino)
                .context("handle is missing")?
                .remove(&fh);
            if opened_files_for_write
                .get(&ctx.ino)
                .context("handle is missing")?
                .is_empty()
            {
                opened_files_for_write.remove(&ctx.ino);
            }
            valid_fh = true;
        }
        if !valid_fh {
            bail!("Fine handle is not valid!")
        }
        Ok(())
    }

    pub fn flush(&self, fh: u64) -> anyhow::Result<()> {
        // TODO: Double check
        if self.read_only {
            bail!("Filesystem is in read-only mode")
        }
        if fh == 0 {
            return Ok(());
        }
        let read_lock = self.read_handles.read().unwrap();
        let mut valid_fh = read_lock.get(&fh).is_some();
        let write_lock = self.write_handles.read().unwrap();
        if let Some(ctx) = write_lock.get(&fh) {
            let mut ctx = ctx.lock().unwrap();
            // Read-write locks not implemented. Write operations not allowed yet.
            ctx.writer.as_mut().context("No writer")?.flush()?;
            let path = self.get_path_from_db(ctx.ino)?;
            File::open(path)?.sync_all()?;
            drop(ctx);
            self.reset_handles()?;
            valid_fh = true;
        }

        if !valid_fh {
            bail!("Fine handle is not valid!")
        }
        Ok(())
    }

    fn reset_handles(&self) -> anyhow::Result<()> {
        todo!()
    }

    fn object_to_file_attr(&self, inode: u64, git_attr: &ObjectAttr) -> anyhow::Result<FileAttr> {
        let blocks = git_attr.size.div_ceil(512);

        // Compute atime and mtime from commit_time
        let commit_secs = git_attr.commit_time.seconds() as u64;
        let time = UNIX_EPOCH + Duration::from_secs(commit_secs);

        let kind = match git_attr.filemode & 0o170000 {
            libc::S_IFDIR => FileType::Directory,
            libc::S_IFLNK => FileType::Symlink,
            _ => FileType::RegularFile,
        };
        let perm = (git_attr.filemode & 0o774) as u16;

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
        // if !self.exists(inode)? {
        //     bail!("Inode not found!")
        // }
        let perms = 0o774;
        let st_mode = libc::S_IFDIR | perms;
        let ctx = FsOperationContext::get_operation(self, inode, false);
        match ctx? {
            FsOperationContext::Root => Ok(build_attr_dir(ROOT_INO, st_mode)),
            FsOperationContext::RepoDir { ino } => Ok(build_attr_dir(ino, st_mode)),
            FsOperationContext::InsideLiveDir { ino } => {
                dbg!("get attr with {ino} inside livedir");
                let path = self.build_full_path(ino)?;
                dbg!(&path);
                let mut attr: FileAttr = self.attr_from_dir(path)?;
                attr.inode = ino;
                Ok(attr)
            }
            FsOperationContext::InsideGitDir { ino } => {
                dbg!("get attr with {ino} inside gitdir");
                // TODO: Double check this
                let repo = self.get_repo(ino)?;
                let db_conn = self.open_meta_db(&repo.repo_dir)?;
                let path = db_conn.get_path_from_db(ino)?;

                let git_attr = repo.getattr(path)?;
                self.object_to_file_attr(ino, &git_attr)
            }
        }
    }

    // When fetching a repo takes name as:
    // website.accoount.repo_name
    // example:github.tokio.tokio-rs.git -> https://github.com/tokio-rs/tokio.git
    pub fn mkdir(
        &mut self,
        parent: u64,
        name: &OsStr,
        create_attr: CreateFileAttr,
    ) -> anyhow::Result<FileAttr> {
        if self.read_only {
            bail!("Filesystem is in read only!")
        }
        if !self.exists(parent)? {
            bail!("Parent does not exist!")
        }
        if !self.is_dir(parent)? {
            bail!("Parent must be a folder!")
        }
        let name = name.to_str().unwrap();

        let ctx = FsOperationContext::get_operation(self, parent, true);
        match ctx? {
            FsOperationContext::Root => {
                let (url, repo_name) = repo::parse_mkdir_url(name)?;
                // initialize repo
                let repo = self.new_repo(&repo_name)?;

                // fetch
                repo.fetch_anon(&url)?;
                let attr = self.getattr((repo.repo_id as u64) << REPO_SHIFT)?;
                Ok(attr)
            }
            FsOperationContext::RepoDir { ino: _ } => {
                bail!("This directory is read only.")
            }
            FsOperationContext::InsideLiveDir { ino } => {
                if self.exists_by_name(ino, name)? {
                    bail!("Name already exists!")
                }

                let dir_path = self.build_path(ino, name)?;
                std::fs::create_dir(dir_path)?;

                let ino = self.next_inode(ino)?;

                let mut attr: FileAttr = create_attr.into();
                attr.inode = ino;

                let nodes = vec![(ino, name, attr)];
                self.write_inodes_to_db(ino, nodes)?;

                Ok(attr)
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
                bail!("This directory is read only!")
            }
        }
    }

    pub fn readdir(&self, parent: u64) -> anyhow::Result<Vec<DirectoryEntry>> {
        let ctx = FsOperationContext::get_operation(self, parent, true);
        match ctx? {
            FsOperationContext::Root => {
                let mut entries: Vec<DirectoryEntry> = vec![];
                for repo in self.repos_list.values() {
                    let repo_ino = GitFs::repo_id_to_ino(repo.repo_id);
                    let dir_entry = DirectoryEntry::new(
                        repo_ino,
                        Oid::zero(),
                        repo.repo_dir.clone(),
                        FileType::Directory,
                        libc::S_IFDIR,
                    );
                    entries.push(dir_entry);
                }
                Ok(entries)
            }
            FsOperationContext::RepoDir { ino } => {
                let repo_id = (ino >> REPO_SHIFT) as u16;
                if self.repos_list.contains_key(&repo_id) {
                    let mut entries: Vec<DirectoryEntry> = vec![];
                    let live_ino = self.get_ino_from_db(ino, "live")?;
                    let live_entry = DirectoryEntry::new(
                        live_ino,
                        Oid::zero(),
                        "live".to_string(),
                        FileType::Directory,
                        libc::S_IFDIR,
                    );
                    entries.push(live_entry);
                    Ok(entries)
                } else {
                    bail!("Repo is not found!");
                }
            }
            FsOperationContext::InsideLiveDir { ino } => {
                let ignore_list = [OsString::from(".git"), OsString::from("fs_meta.db")];
                let db_path = self.get_path_from_db(ino)?;
                let path = self.repos_dir.join(&self.get_repo(ino)?.repo_dir);
                let path = if db_path == PathBuf::from("live") {
                    path
                } else {
                    path.join(db_path)
                };
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
                    let entry =
                        DirectoryEntry::new(ino, Oid::zero(), node_name_str.into(), kind, filemode);
                    entries.push(entry);
                }
                Ok(entries)
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
                bail!("readdir inside GitDir not implemented");
            }
        }
    }

    pub fn find_by_name(&self, parent: u64, name: &str) -> anyhow::Result<Option<FileAttr>> {
        if !self.exists(parent)? {
            bail!("Parent does not exist!")
        }
        if !self.is_dir(parent)? {
            bail!("Parent must be a dir!")
        }
        let ctx = FsOperationContext::get_operation(self, parent, true);
        match ctx? {
            FsOperationContext::Root => {
                // Handle a look-up for url -> github.tokio-rs.tokio.git
                let attr = self.repos_list.values().find_map(|repo| {
                    if repo.repo_dir == name {
                        let perms = 0o774;
                        let st_mode = libc::S_IFDIR | perms;
                        let repo_ino = (repo.repo_id as u64) << REPO_SHIFT;
                        Some(build_attr_dir(repo_ino, st_mode))
                    } else {
                        None
                    }
                });
                Ok(attr)
            }
            FsOperationContext::RepoDir { ino } => {
                let repo_id = GitFs::ino_to_repo_id(ino);
                match self.repos_list.get(&repo_id) {
                    Some(_) => {}
                    None => return Ok(None),
                };
                let repo_ino = self.get_ino_from_db(ino, name)?;
                let path = if name == "live" {
                    self.build_full_path(ino)?
                } else {
                    self.build_full_path(ino)?.join(name)
                };
                let mut attr = self.attr_from_dir(path)?;
                attr.inode = repo_ino;

                Ok(Some(attr))
            }
            FsOperationContext::InsideLiveDir { ino } => {
                let repo_id = GitFs::ino_to_repo_id(ino);
                match self.repos_list.get(&repo_id) {
                    Some(_) => {}
                    None => return Ok(None),
                };
                let repo_ino = self.get_ino_from_db(ino, name)?;
                let path = self.build_full_path(ino)?.join(name);
                let mut attr = self.attr_from_dir(path)?;
                attr.inode = repo_ino;

                Ok(Some(attr))
            }
            FsOperationContext::InsideGitDir { ino: _ } => {
                // let parent_attr = self.getattr(ino)?;
                // let repo = self.get_repo(ino)?;
                // let git_attr = repo.find_by_name(parent_attr.oid, name)?;
                // let conn = self.open_meta_db(&repo.repo_dir)?;
                // let inode = conn.get_ino_from_db(ino, name)?;
                // let file_attr = self.object_to_file_attr(inode, &git_attr)?;
                // Ok(Some(file_attr))
                bail!("Not implemented")
            }
        }
    }
}

// gitfs_helpers
impl GitFs {
    fn attr_from_dir(&self, path: PathBuf) -> anyhow::Result<FileAttr> {
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

        let perms = 0o774;
        let st_mode = libc::S_IFDIR | perms;

        Ok(FileAttr {
            inode: 0,
            oid: Oid::zero(),
            size: metadata.size(),
            blocks: metadata.blocks(),
            atime,
            mtime,
            ctime,
            crtime,
            kind: FileType::Directory,
            perm: 0o774,
            mode: st_mode,
            nlink: metadata.nlink() as u32,
            uid: metadata.uid(),
            gid: metadata.gid(),
            rdev: metadata.rdev() as u32,
            blksize: metadata.blksize() as u32,
            flags: 0,
        })
    }

    fn build_path(&self, parent: u64, name: &str) -> anyhow::Result<PathBuf> {
        let repo_name = &self.get_repo(parent)?.repo_dir;
        let path_to_repo = PathBuf::from(&self.repos_dir).join(repo_name);

        let live_ino = self.get_live_ino(parent);
        if parent == live_ino {
            return Ok(path_to_repo.join(name));
        }

        let conn = &self.get_repo(parent)?.connection;
        let conn = conn.read().unwrap();
        // TODO: Test how get_path_from_db returns
        let db_path = conn.get_path_from_db(parent)?;
        Ok(PathBuf::from(&self.repos_dir).join(db_path).join(name))
    }

    fn get_repo(&self, inode: u64) -> anyhow::Result<Arc<GitRepo>> {
        let repo_id = (inode >> REPO_SHIFT) as u16;
        let repo = self
            .repos_list
            .get(&repo_id)
            .ok_or_else(|| anyhow!("No repo found for this ID"))?;
        Ok(repo.clone())
    }

    fn is_in_live(&self, ino: u64) -> anyhow::Result<bool> {
        let live_ino = self.get_live_ino(ino);
        if live_ino == ino {
            return Ok(true)
        }
        let mut target_ino = ino;

        loop {
            let parent = match self.get_parent_ino(target_ino) {
                Ok(p) => p,
                Err(_) => return Ok(false),
            };
            if parent == live_ino {
                return Ok(true);
            }
            target_ino = parent;
        }
    }

    fn next_inode(&self, parent: u64) -> anyhow::Result<u64> {
        let repo_id = (parent >> REPO_SHIFT) as u16;
        let inode = self
            .next_inode
            .get(&repo_id)
            .ok_or_else(|| anyhow!("No repo found for this ID"))?
            .fetch_add(1, Ordering::SeqCst);
        Ok(inode)
    }

    fn next_file_handle(&self) -> u64 {
        self.current_handle.fetch_add(1, Ordering::SeqCst)
    }

    fn next_repo_id(&self) -> u16 {
        match self.repos_list.keys().next_back() {
            Some(&i) => i
                .checked_add(1)
                .expect("Congrats. Repo ids have overflowed a u16."),
            None => 1,
        }
    }

    pub fn get_parent_ino(&self, ino: u64) -> anyhow::Result<u64> {
        let repo = self.get_repo(ino)?;
        let conn = &repo.connection.read().unwrap();
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
                perm: 0o774,
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
            std::fs::create_dir_all(repos_dir)
                .with_context(|| format!("Failed to create repos dir {repos_dir:?}"))?;
        }
        Ok(())
    }

    fn get_live_ino(&self, ino: u64) -> u64 {
        let repo_id = GitFs::ino_to_repo_id(ino);
        let repo_ino = (repo_id as u64) << REPO_SHIFT;

        repo_ino + 1
    }

    fn exists_by_name(&self, parent: u64, name: &str) -> anyhow::Result<bool> {
        let conn = &self.get_repo(parent)?.connection;
        let conn = conn.read().unwrap();
        conn.exists_by_name(parent, name)
    }

    pub fn exists(&self, ino: u64) -> anyhow::Result<bool> {
        if ino == ROOT_INO {
            return Ok(true);
        }
        let repo_id = GitFs::ino_to_repo_id(ino);
        if self.repos_list.contains_key(&repo_id) {
            return Ok(true);
        }
        if ino == (repo_id as u64) + 1 {
            return Ok(true);
        }
        Ok(self.get_path_from_db(ino).is_ok())
    }

    fn is_dir(&self, ino: u64) -> anyhow::Result<bool> {
        if ino == ROOT_INO {
            return Ok(true);
        }
        let repo_id = GitFs::ino_to_repo_id(ino);
        if self.repos_list.contains_key(&repo_id) {
            return Ok(true);
        }
        let repo = self.get_repo(ino)?;
        let path = self.get_path_from_db(ino)?;

        let git_attr = repo.getattr(path)?;
        Ok(git_attr.kind == ObjectType::Tree)
    }

    fn is_file(&self, inode: u64) -> anyhow::Result<bool> {
        let repo = self.get_repo(inode)?;
        let path = self.get_path_from_db(inode)?;

        let git_attr = repo.getattr(path)?;
        Ok(git_attr.kind == ObjectType::Blob && git_attr.filemode != libc::S_IFLNK)
    }

    fn is_link(&self, inode: u64) -> anyhow::Result<bool> {
        let repo = self.get_repo(inode)?;
        let path = self.get_path_from_db(inode)?;

        let git_attr = repo.getattr(path)?;
        Ok(git_attr.kind == ObjectType::Blob && git_attr.filemode == libc::S_IFLNK)
    }

    fn get_ino_from_db(&self, parent: u64, name: &str) -> anyhow::Result<u64> {
        let conn = &self.get_repo(parent)?.connection;
        let conn = conn.read().unwrap();
        conn.get_ino_from_db(parent, name)
    }

    fn build_full_path(&self, ino: u64) -> anyhow::Result<PathBuf> {
        let repo = self.get_repo(ino)?;
        let repo_ino = GitFs::repo_id_to_ino(repo.repo_id);
        let repo_name = &self.get_repo(ino)?.repo_dir;
        let path = PathBuf::from(&self.repos_dir).join(repo_name);
        if ino == repo_ino {
            return Ok(path);
        }
        let db_path = self.get_path_from_db(ino)?;
        let path = if db_path == PathBuf::from("live") {
            path
        } else {
            path.join(self.get_path_from_db(ino)?)
        };
        Ok(path)
    }

    fn get_path_from_db(&self, inode: u64) -> anyhow::Result<PathBuf> {
        let repo = self.get_repo(inode)?;
        let conn = repo.connection.read().unwrap();
        conn.get_path_from_db(inode)
    }

    fn write_inodes_to_db(
        &self,
        parent: u64,
        nodes: Vec<(u64, &str, FileAttr)>,
    ) -> anyhow::Result<()> {
        let conn = &self.get_repo(parent)?.connection;
        let mut conn = conn.write().unwrap();
        conn.write_inodes_to_db(nodes)
    }
}

fn build_attr_file(inode: u64, st_mode: u32) -> FileAttr {
    let now = SystemTime::now();
    FileAttr {
        inode,
        oid: Oid::zero(),
        size: 0,
        blocks: 0,
        atime: now,
        mtime: now,
        ctime: now,
        crtime: now,
        kind: FileType::RegularFile,
        perm: 0o644,
        mode: st_mode,
        nlink: 2,
        uid: unsafe { libc::getuid() } as u32,
        gid: unsafe { libc::getgid() } as u32,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}

fn build_attr_dir(inode: u64, st_mode: u32) -> FileAttr {
    let now = SystemTime::now();
    FileAttr {
        inode,
        oid: Oid::zero(),
        size: 0,
        blocks: 0,
        atime: now,
        mtime: now,
        ctime: now,
        crtime: now,
        kind: FileType::Directory,
        perm: 0o774,
        mode: st_mode,
        nlink: 2,
        uid: unsafe { libc::getuid() } as u32,
        gid: unsafe { libc::getgid() } as u32,
        rdev: 0,
        blksize: 4096,
        flags: 0,
    }
}
