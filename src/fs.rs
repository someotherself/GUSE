use std::collections::{BTreeMap, HashSet};
use std::fs::{File, OpenOptions};
use std::hash::Hash;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use std::rc::Rc;
use std::sync::atomic::AtomicU16;
use std::sync::{Mutex, RwLock};
use std::time::{Duration, UNIX_EPOCH};
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::SystemTime,
};

use anyhow::{Context, Ok, anyhow, bail};
use git2::{ObjectType, Oid, Repository};
use rusqlite::{Connection, OptionalExtension, Transaction, params};
use tracing::instrument;

use crate::repo::GitRepo;

const META_STORE: &str = "fs_meta.db";
const REPO_SHIFT: u8 = 48;
pub const ROOT_INO: u64 = 1;

// Disk structure
// MOUNT_POINT/
// repos_dir/repo_name1
//------------├── git/
//------------└── fs_meta.db
// repos_dir/repo_name2
//------------├── git/
//------------└── fs_meta.db

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
    File,
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
            ObjectType::Blob => Ok(FileType::File),
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

pub struct DirectoryEntryIterator(VecDeque<DirectoryEntry>);

impl Iterator for DirectoryEntryIterator {
    type Item = DirectoryEntry;

    #[instrument(name = "DirectoryEntryIterator::next", skip(self))]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front()
    }
}

pub struct DirectoryEntryPlus {
    // The the attributes in the normal struct
    pub entry: DirectoryEntry,
    // Plus the file attributes
    pub attr: FileAttr,
}

pub struct DirectoryEntryPlusIterator(VecDeque<DirectoryEntryPlus>);

impl Iterator for DirectoryEntryPlusIterator {
    type Item = DirectoryEntryPlus;

    #[instrument(name = "DirectoryEntryPlusIterator::next", skip(self))]
    fn next(&mut self) -> Option<Self::Item> {
        self.0.pop_front()
    }
}

pub struct MetaDb {
    pub conn: Connection,
}

impl MetaDb {
    // DB layout
    //   inode        INTEGER   PRIMARY KEY,    -> the u64 inode
    //   parent_inode INTEGER   NOT NULL,       -> the parent directory’s inode
    //   name         TEXT      NOT NULL,       -> the filename or directory name
    //   oid          TEXT      NOT NULL,       -> the Git OID
    //   filemode     INTEGER   NOT NULL        -> the raw Git filemode
    // nodes: Vec<(parent inode, parent name, FileAttr)>
    pub fn write_inodes_to_db(
        &mut self,
        nodes: Vec<(u64, String, FileAttr)>,
    ) -> anyhow::Result<()> {
        let tx: Transaction<'_> = self.conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO inode_map
            (inode, repo_id, parent_inode, name, oid, filemode)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            )?;

            for (parent_inode, name, fileattr) in nodes {
                stmt.execute(params![
                    fileattr.inode as i64,
                    parent_inode as i64,
                    name,
                    fileattr.oid.to_string(),
                    fileattr.mode as i64,
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    pub fn get_ino_from_db(&self, parent: u64, name: &str) -> anyhow::Result<u64> {
        let mut stmt = self.conn.prepare(
            "SELECT inode
               FROM inode_map
              WHERE parent_inode = ?1 AND name = ?2",
        )?;

        let ino_opt: Option<i64> = stmt
            .query_row(rusqlite::params![parent as i64, name], |row| row.get(0))
            .optional()?;
        if let Some(ino) = ino_opt {
            Ok(ino as u64)
        } else {
            Err(anyhow!(
                "inode not found for parent={} name={}",
                parent,
                name
            ))
        }
    }

    pub fn get_path_from_db(&self, inode: u64) -> anyhow::Result<PathBuf> {
        let mut stmt = self.conn.prepare(
            "SELECT parent_inode, name
               FROM inode_map
              WHERE inode = ?1",
        )?;

        let mut components = Vec::new();
        let mut curr = inode as i64;

        loop {
            let row: Option<(i64, String)> = stmt
                .query_row(params![curr], |r| {
                    rusqlite::Result::Ok((r.get(0)?, r.get(1)?))
                })
                .optional()?;

            match row {
                Some((parent, name)) => {
                    components.push(name);
                    curr = parent;
                }
                None => break,
            }
        }

        if components.is_empty() && inode != ROOT_INO {
            return Err(anyhow!("inode {} not found in meta-db", inode));
        }

        components.reverse();

        let path: PathBuf = components.iter().collect();
        Ok(path)
    }
}

pub struct GitFs {
    repos_dir: PathBuf,
    repos_list: BTreeMap<u16, Rc<GitRepo>>,
    next_inode: HashMap<u16, AtomicU64>, // Each Repo has a set of inodes
    current_handle: AtomicU64,
    read_handles: RwLock<HashMap<u64, Mutex<ReadHandleContext>>>, // ino
    write_handles: RwLock<HashMap<u64, Mutex<WriteHandleContext>>>, // ino
    opened_handes_for_read: RwLock<HashMap<u64, HashSet<u64>>>,   // (ino, fh)
    opened_handes_for_write: RwLock<HashMap<u64, HashSet<u64>>>,  // (ino, fh)
    read_only: bool,
}

impl GitFs {
    pub fn new(repos_dir: PathBuf, read_only: bool) -> anyhow::Result<Rc<Self>> {
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
        Ok(Rc::new(fs).clone())
    }

    pub fn new_repo(&mut self, repo_name: &str) -> anyhow::Result<Rc<GitRepo>> {
        let repo_path = self.repos_dir.join(repo_name);
        if repo_path.exists() {
            bail!("Repo name already exists!")
        }
        std::fs::create_dir(&repo_path).context("context")?;

        let connection = self.init_meta_db(repo_name)?;

        let repo_id = self.next_repo_id();

        let repo = git2::Repository::init(repo_path)?;

        let git_repo = GitRepo {
            connection,
            repo_dir: repo_name.to_owned(),
            repo_id,
            inner: repo,
            head: None,
        };

        let repo_rc = Rc::new(git_repo);
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
            connection: db,
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

    fn get_repo(&self, inode: u64) -> anyhow::Result<&GitRepo> {
        let repo_id = (inode >> REPO_SHIFT) as u16;
        let repo = self
            .repos_list
            .get(&repo_id)
            .ok_or_else(|| anyhow!("No repo found for this ID"))?;
        Ok(repo)
    }

    fn pack_inode(repo_id: u16, sub_ino: u64) -> u64 {
        ((repo_id as u64) << REPO_SHIFT) | (sub_ino & ((1 << REPO_SHIFT) - 1))
    }

    fn ensure_base_dirs_exist(&self) -> anyhow::Result<()> {
        if !self.repos_dir.exists() {
            let mut attr: FileAttr = CreateFileAttr {
                kind: FileType::Directory,
                perm: 0o755,
                mode: 0o040000,
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

    pub fn exists(&self, inode: u64) -> anyhow::Result<bool> {
        let repo = self.get_repo(inode)?;
        Ok(repo.connection.get_path_from_db(inode).is_ok())
    }

    pub fn is_dir(&self, inode: u64) -> anyhow::Result<bool> {
        let repo = self.get_repo(inode)?;
        let path = repo.connection.get_path_from_db(inode)?;

        let git_attr = repo.getattr(path)?;
        Ok(git_attr.kind == ObjectType::Tree)
    }

    pub fn is_file(&self, inode: u64) -> anyhow::Result<bool> {
        let repo = self.get_repo(inode)?;
        let path = repo.connection.get_path_from_db(inode)?;

        let git_attr = repo.getattr(path)?;
        Ok(git_attr.kind == ObjectType::Blob && git_attr.filemode != 0o120000)
    }

    pub fn is_link(&self, inode: u64) -> anyhow::Result<bool> {
        let repo = self.get_repo(inode)?;
        let path = repo.connection.get_path_from_db(inode)?;

        let git_attr = repo.getattr(path)?;
        Ok(git_attr.kind == ObjectType::Blob && git_attr.filemode == 0o120000)
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

    fn register_read(&self, ino: u64, fh: u64) -> anyhow::Result<()> {
        let attr = self.getattr(ino)?.into();
        let path = self.get_repo(ino)?.connection.get_path_from_db(ino)?;
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
        let path = self.get_repo(ino)?.connection.get_path_from_db(ino)?;
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
            let path = self
                .get_repo(ctx.ino)?
                .connection
                .get_path_from_db(ctx.ino)?;
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
            0o040000 => FileType::Directory,
            0o120000 => FileType::Symlink,
            _ => FileType::File,
        };
        let perm = (git_attr.filemode & 0o777) as u16;

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
        if inode == ROOT_INO {
            let now = SystemTime::now();
            return Ok(FileAttr {
                inode: ROOT_INO,
                oid: Oid::zero(), // no real Git object
                size: 0,
                blocks: 0,
                atime: now,
                mtime: now,
                ctime: now,
                crtime: now,
                kind: FileType::Directory,
                perm: 0o644,
                mode: 0o040000,
                nlink: 2,
                uid: unsafe { libc::getuid() } as u32,
                gid: unsafe { libc::getgid() } as u32,
                rdev: 0,
                blksize: 4096,
                flags: 0,
            });
        }

        // Check inode exists
        if !self.exists(inode)? {
            bail!("Inode not found!")
        }
        // Get ObjectAttr from git2
        let repo = self.get_repo(inode)?;
        let db_conn = self.open_meta_db(&repo.repo_dir)?;
        let path = db_conn.get_path_from_db(inode)?;

        let git_attr = repo.getattr(path)?;
        self.object_to_file_attr(inode, &git_attr)
    }

    pub fn find_by_name(&self, parent: u64, name: &str) -> anyhow::Result<Option<FileAttr>> {
        // Do not look into parent. ROOT_DIR should not be accessible
        if parent == ROOT_INO {
            return Ok(None);
        }
        if !self.exists(parent)? {
            bail!("Inode not found!")
        }

        if !self.is_dir(parent)? {
            bail!("Inode not found!")
        }

        let parent_attr = self.getattr(parent)?;
        let repo = self.get_repo(parent)?;
        let git_attr = repo.find_by_name(parent_attr.oid, name)?;
        let conn = self.open_meta_db(&repo.repo_dir)?;
        let inode = conn.get_ino_from_db(parent, name)?;
        let file_attr = self.object_to_file_attr(inode, &git_attr)?;

        Ok(Some(file_attr))
    }
}

// lookup               -> git ls-tree
// getattr              -> git cat-file -p <object>
// readdir              -> git ls-tree <tree>
// readdirplus          -> git ls-tree + git catfile -p <object>
// open                 -> no-op
// read                 -> git cat-file --batch / git cat-file -p <blob>
// create               -> Not allowed in fuse. git hash-object --stdin -w + git update-index --add <path>
// write                -> buffer in memory then on flush: git hash-object --stdin -w
// flush / release      -> git update-index --add <path>
// unlink               -> Not allowed in fuse. git update-index --remove <path>
// mkdir                -> Not allowed in fuse. update in mem tree, commit w/: git write-tree
// rmdir                -> Not allowed in fuse. update in mem tree, commit w/: git write-tree
// rename               -> Not allowed in fuse. git mv <old> <new> or idx update + working tree rename
// statfs               -> fuse3::statfs::Statfs or derive from git repo
