use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, Datelike};
use git2::{
    Commit, Delta, DiffFindOptions, DiffOptions, Direction, ErrorClass, ErrorCode, FetchOptions,
    FileMode, ObjectType, Oid, RemoteCallbacks, Repository, Sort, Time, Tree, TreeWalkMode,
    TreeWalkResult,
};
use std::{
    collections::{hash_map::Entry, BTreeMap, HashMap, HashSet},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicUsize}, Arc, Mutex
    },
};

use crate::{
    fs::{ObjectAttr, builds::BuildSession, meta_db::MetaDb},
    inodes::VirtualIno,
};

pub struct GitRepo {
    // Caching the database connection for reads.
    // Must be refreshed after every write.
    pub connection: Arc<Mutex<MetaDb>>,
    pub repo_dir: String,
    pub repo_id: u16,
    pub inner: Repository,
    pub head: Option<Oid>,
    /// i64 -> commit_time -> seconds since EPOCH
    ///
    /// Vec<Oid> -> Vec<commit_oid> -> In case commits are made at the same time
    pub snapshots: BTreeMap<i64, Vec<Oid>>,
    /// Used inodes to prevent reading from DB
    ///
    /// Gets populated from DB when loading a repo
    /// TODO: Remove inodes of virtual files at startup
    pub res_inodes: HashSet<u64>,
    /// key: inode of the virtual directory
    pub vdir_cache: BTreeMap<VirtualIno, VirtualNode>,
    /// Oid = Commit Oid
    pub build_sessions: HashMap<Oid, Arc<BuildSession>>,
}

/// Insert/get a node during getattr/lookup
///
/// Fill log during readdir
pub struct VirtualNode {
    /// Inode of the real file
    pub real: u64,
    /// Inode of the virtual file
    pub ino: u64,
    /// Oid of the file
    pub oid: Oid,
    /// Oids of the file history (if any)
    ///
    /// key: full file name, values: <file entry_ino, ObjectAttr>
    pub log: BTreeMap<String, (u64, ObjectAttr)>,
}

impl GitRepo {
    pub fn refresh_snapshots(&mut self) -> anyhow::Result<()> {
        let head = match self.inner.head() {
            Ok(h) => h,
            Err(_) => {
                // empty repo
                self.head = None;
                self.snapshots.clear();
                return Ok(());
            }
        };

        let head_commit = head.peel_to_commit()?;
        self.head = Some(head_commit.id());

        let mut walk = self.inner.revwalk()?;
        walk.set_sorting(git2::Sort::TOPOLOGICAL | git2::Sort::TIME)?;
        walk.push(head_commit.id())?;

        self.snapshots.clear();

        for oid_res in walk {
            let oid = oid_res?; // keep as-is
            let c = self.inner.find_commit(oid)?;
            let t = c.time();
            let secs = t.seconds();
            self.snapshots.entry(secs).or_default().push(oid);
        }
        Ok(())
    }

    pub fn months_from_cache(&self, use_offset: bool) -> BTreeMap<String, Vec<git2::Oid>> {
        let mut buckets: BTreeMap<String, Vec<git2::Oid>> = BTreeMap::new();

        for (&secs, oids) in &self.snapshots {
            let adj = if use_offset {
                let t = self
                    .inner
                    .find_commit(oids[0])
                    .ok()
                    .map(|c| c.time().offset_minutes())
                    .unwrap_or(0);
                secs + (t as i64) * 60
            } else {
                secs
            };

            let dt = DateTime::from_timestamp(adj, 0)
                .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());

            let key = format!("{:04}-{:02}", dt.year(), dt.month());

            buckets.entry(key).or_default().extend(oids.iter().copied());
        }
        buckets
    }

    pub fn month_folders(&self) -> anyhow::Result<Vec<ObjectAttr>> {
        let mut out = Vec::new();

        for secs in self.snapshots.keys() {
            let dt = chrono::DateTime::from_timestamp(*secs, 0)
                .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
            let folder_name = format!("{:04}-{:02}", dt.year(), dt.month());

            // No duplicates
            if out.iter().any(|attr: &ObjectAttr| attr.name == folder_name) {
                continue;
            }

            out.push(ObjectAttr {
                name: folder_name,
                oid: Oid::zero(),
                kind: ObjectType::Tree,
                filemode: 0o040000,
                size: 0,
                commit_time: git2::Time::new(*secs, 0),
            });
        }

        Ok(out)
    }

    fn parse_month_key(key: &str) -> Option<(i32, u32)> {
        let (y, m) = key.split_once('-')?;
        Some((y.parse().ok()?, m.parse().ok()?))
    }

    pub fn month_commits(&self, month_key: &str) -> anyhow::Result<Vec<ObjectAttr>> {
        // let (year, month, day) =
        //     Self::parse_day_key(day_key).ok_or_else(|| anyhow!("Invalid input"))?;
        let (year, month) =
            Self::parse_month_key(month_key).ok_or_else(|| anyhow!("Invalid input"))?;

        let mut out: Vec<ObjectAttr> = Vec::new();
        let mut commit_num = 0;

        for (&secs_utc, oids) in &self.snapshots {
            for commit_oid in oids {
                let dt = DateTime::from_timestamp(secs_utc, 0)
                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());

                if dt.year() != year || dt.month() != month {
                    continue;
                }

                commit_num += 1;
                let folder_name = format!("Snap{commit_num:03}_{commit_oid:.7}");

                out.push(ObjectAttr {
                    name: folder_name,
                    oid: *commit_oid,
                    kind: ObjectType::Tree,
                    filemode: 0o040000,
                    size: 0,
                    commit_time: git2::Time::new(secs_utc, 0),
                });
            }
        }
        Ok(out)
    }

    pub fn fetch_anon(&self, url: &str) -> anyhow::Result<()> {
        let repo = &self.inner;
        // Set up the anonymous remote and callbacks
        let mut remote = repo.remote_anonymous(url)?;
        let mut cbs = RemoteCallbacks::new();
        cbs.sideband_progress(|d| {
            print!("remote: {}", std::str::from_utf8(d).unwrap_or(""));
            true
        });
        cbs.update_tips(|name, a, b| {
            println!("update {a:.10}..{b:.10} {name}");
            true
        });
        cbs.transfer_progress(|s| {
            if s.total_objects() > 0 {
                print!(
                    "\rReceived {}/{} (idx {}) {} bytes",
                    s.received_objects(),
                    s.total_objects(),
                    s.indexed_objects(),
                    s.received_bytes()
                );
            }
            std::io::Write::flush(&mut std::io::stdout()).ok();
            true
        });
        let mut fo = FetchOptions::new();
        fo.remote_callbacks(cbs);

        remote.connect(Direction::Fetch)?;
        let default_branch = remote.default_branch().ok(); // Optional but nice
        remote.disconnect()?;

        let mut refspecs = vec![
            "+refs/heads/*:refs/remotes/anon/*".to_string(),
            "+refs/tags/*:refs/tags/*".to_string(),
        ];
        if let Some(ref buf) = default_branch {
            if let Ok(src) = std::str::from_utf8(buf.as_ref()) {
                refspecs.push(format!(
                    "+{}:refs/remotes/anon/{}",
                    src,
                    src.rsplit('/')
                        .next()
                        .ok_or_else(|| anyhow!("Invalid ref"))?
                ));
                refspecs.push("+HEAD:refs/remotes/anon/HEAD".to_string());
            }
        } else {
            refspecs.push("+HEAD:refs/remotes/anon/HEAD".to_string());
        }
        let refs_as_str: Vec<&str> = refspecs.iter().map(|s| s.as_str()).collect();

        remote.fetch(&refs_as_str, Some(&mut fo), None)?;

        if repo.head().is_err() {
            if let Some(ref buf) = default_branch {
                if let Ok(src) = std::str::from_utf8(buf.as_ref()) {
                    let short = src
                        .rsplit('/')
                        .next()
                        .ok_or_else(|| anyhow!("Invalid ref"))?;
                    let target = format!("refs/remotes/anon/{short}");
                    // If that ref exists, point HEAD to it; else fall back to anon/HEAD
                    if repo.refname_to_id(&target).is_ok() {
                        repo.set_head(&target)?;
                    } else if let Ok(r) = repo.find_reference("refs/remotes/anon/HEAD") {
                        if let Some(sym) = r.symbolic_target() {
                            repo.set_head(sym)?;
                        } else if let Some(oid) = r.target() {
                            repo.set_head_detached(oid)?;
                        }
                    }
                }
            } else if let Ok(r) = repo.find_reference("refs/remotes/anon/HEAD") {
                if let Some(sym) = r.symbolic_target() {
                    repo.set_head(sym)?;
                } else if let Some(oid) = r.target() {
                    repo.set_head_detached(oid)?;
                }
            }
        }
        Ok(())
    }

    /// Read_dir
    ///
    /// Called with tree_oid: None if we are at the root of the commit
    pub fn list_tree(&self, commit: Oid, tree_oid: Option<Oid>) -> anyhow::Result<Vec<ObjectAttr>> {
        let commit = self.inner.find_commit(commit)?;
        let mut entries = vec![];
        let tree = match tree_oid {
            Some(tree_oid) => self.inner.find_tree(tree_oid)?,
            None => commit.tree()?,
        };
        for entry in tree.iter() {
            let name = entry.name().unwrap_or("").to_string();
            entries.push(ObjectAttr {
                name,
                oid: entry.id(),
                kind: entry.kind().ok_or_else(|| anyhow!("Invalid object"))?,
                filemode: entry.filemode() as u32,
                size: 0,
                commit_time: Time::new(0, 0),
            });
        }
        Ok(entries)
    }

    pub fn find_by_name(&self, tree_oid: Oid, name: &str) -> anyhow::Result<ObjectAttr> {
        let tree = self.inner.find_tree(tree_oid)?;

        let entry = tree
            .get_name(name)
            .ok_or_else(|| anyhow!(format!("{name} not found in tree {tree_oid}")))?;
        let size = match entry.kind().ok_or_else(|| anyhow!("Invalid object"))? {
            ObjectType::Blob => entry
                .to_object(&self.inner)
                .map_err(|_| anyhow!("Invalid object"))?
                .into_blob()
                .map_err(|_| anyhow!("Invalid object"))?
                .size(),
            _ => 0,
        };
        let commit = self.inner.head()?.peel_to_commit()?;
        let commit_time = commit.time();
        Ok(ObjectAttr {
            name: name.into(),
            oid: entry.id(),
            kind: entry.kind().ok_or_else(|| anyhow!("Invalid object"))?,
            filemode: entry.filemode() as u32,
            size: size as u64,
            commit_time,
        })
    }

    pub fn find_in_commit(&self, commit_id: Oid, oid: Oid) -> anyhow::Result<ObjectAttr> {
        let commit_obj = self.inner.find_commit(commit_id)?;
        let commit_time = commit_obj.time();
        let tree = commit_obj.tree()?;

        // If they asked for the root tree itself
        if tree.id() == oid {
            return Ok(ObjectAttr {
                name: ".".into(),
                oid,
                kind: ObjectType::Tree,
                filemode: 0o040000,
                size: 0,
                commit_time,
            });
        }
        // Search recursively for a matching entry id
        let mut found: Option<ObjectAttr> = None;
        let walk_res = tree.walk(TreeWalkMode::PreOrder, |root, entry| {
            if entry.id() == oid {
                // ... build ObjectAttr into `found` exactly like you already do ...
                found = Some(ObjectAttr {
                    name: format!("{}{}", root, entry.name().unwrap_or("<non-utf8>")),
                    oid,
                    kind: entry.kind().unwrap_or(ObjectType::Any),
                    filemode: entry.filemode() as u32,
                    size: if entry.kind() == Some(ObjectType::Blob) {
                        self.inner
                            .find_blob(entry.id())
                            .map(|b| b.size() as u64)
                            .unwrap_or(0)
                    } else {
                        0
                    },
                    commit_time,
                });
                return TreeWalkResult::Abort; // triggers GIT_EUSER (-7)
            }
            TreeWalkResult::Ok
        });

        if let Err(e) = walk_res
            && !(e.class() == ErrorClass::Callback && e.code() == ErrorCode::User)
        {
            return Err(e.into());
        }
        found.ok_or_else(|| {
            anyhow!(format!(
                "oid {} not found in commit {}",
                oid,
                commit_obj.id()
            ))
        })
    }

    pub fn read_log(&self) -> anyhow::Result<Vec<ObjectAttr>> {
        let limit = 20;
        let head_commit = self.head_commit()?;

        let mut walk = self.inner.revwalk()?;
        walk.set_sorting(Sort::TOPOLOGICAL | Sort::TIME)?;
        walk.push(head_commit.id())?;

        let mut out = Vec::with_capacity(limit.min(256));
        for (i, oid_res) in walk.enumerate() {
            if out.len() >= limit {
                break;
            }
            let oid = oid_res?;
            let commit = self.inner.find_commit(oid)?;
            let commit_name = format!("snap{}_{:.7}", i + 1, commit.id());
            out.push(ObjectAttr {
                name: commit_name,
                oid,
                kind: ObjectType::Commit,
                filemode: 0u32,
                size: 0u64,
                commit_time: commit.time(),
            });
        }
        Ok(out)
    }

    pub fn readdir_commit(&self) -> anyhow::Result<Vec<ObjectAttr>> {
        let mut entries: Vec<ObjectAttr> = vec![];
        let commit = self.head_commit()?;
        let root_tree = commit.tree()?;
        for entry in root_tree.iter() {
            let oid = entry.id();
            let kind = entry.kind().ok_or_else(|| anyhow!("Invalid object"))?;
            let filemode = entry.filemode() as u32;
            let name = entry.name().unwrap_or("").to_string();
            let commit_time = commit.time();
            entries.push(ObjectAttr {
                name,
                oid,
                kind,
                filemode,
                size: 0,
                commit_time,
            });
        }
        Ok(entries)
    }

    pub fn attr_from_snap(&self, commit_oid: Oid, name: &str) -> anyhow::Result<ObjectAttr> {
        let commit = self.inner.find_commit(commit_oid)?;
        let name = name.to_string();
        let oid = commit.id();
        let kind = ObjectType::Commit;
        let filemode = libc::S_IFDIR;
        let size = 0;
        let commit_time = commit.time();

        Ok(ObjectAttr {
            name,
            oid,
            kind,
            filemode,
            size,
            commit_time,
        })
    }

    pub fn blob_history_objects(&self, target_blob: Oid) -> anyhow::Result<Vec<ObjectAttr>> {
        let repo = &self.inner;

        let mut commit = self
            .find_newest_commit_containing_blob_fp(target_blob, 50_000)
            .context("Blob not found (first-parent scan)")?;

        let mut current_path = self
            .find_path_of_blob_in_tree(&commit.tree()?, target_blob)
            .ok_or_else(|| anyhow!("Could not determine initial path for blob"))?;

        const MAX_STEPS: usize = 200_000;
        let mut steps = 0usize;
        let mut blob_count = 1usize;
        let mut out = Vec::new();
        let mut last_pushed_oid: Option<Oid> = None;

        loop {
            steps += 1;
            if steps > MAX_STEPS {
                bail!(
                    "Aborting blob history: exceeded {MAX_STEPS} steps (possible pathological history)"
                );
            }
            let tree = commit.tree()?;
            if let Some(attr) = {
                self.object_attr_for_path_in_tree(repo, &tree, &current_path, &commit, blob_count)?
            } {
                if last_pushed_oid != Some(attr.oid) {
                    last_pushed_oid = Some(attr.oid);
                    blob_count += 1;
                    out.push(attr);
                }
            } else {
                break;
            }

            let pcount = commit.parent_count();
            if pcount == 0 {
                break;
            }

            let parent0 = commit.parent(0)?;
            let parent0_tree = parent0.tree()?;

            if self
                .get_blob_oid_at_path(&parent0_tree, &current_path)
                .is_some()
            {
                commit = parent0;
                continue;
            }

            if let Some(old_path) =
                self.find_renamed_parent_path(&parent0_tree, &tree, &current_path)?
            {
                current_path = old_path;
                commit = parent0;
                continue;
            }

            let mut advanced = false;
            for i in 1..pcount {
                let p = commit.parent(i)?;
                let pt = p.tree()?;

                if self.get_blob_oid_at_path(&pt, &current_path).is_some() {
                    commit = p;
                    advanced = true;
                    break;
                }
                if let Some(old_path) = self.find_renamed_parent_path(&pt, &tree, &current_path)? {
                    current_path = old_path;
                    commit = p;
                    advanced = true;
                    break;
                }
            }

            if !advanced {
                break;
            }
        }

        Ok(out)
    }

    fn object_attr_for_path_in_tree(
        &self,
        repo: &Repository,
        tree: &Tree,
        path: &str,
        commit: &Commit,
        count: usize,
    ) -> anyhow::Result<Option<ObjectAttr>> {
        let entry = match tree.get_path(std::path::Path::new(path)) {
            Ok(e) => e,
            Err(_) => return Ok(None),
        };
        if entry.kind() != Some(ObjectType::Blob) {
            return Ok(None);
        }
        let oid = entry.id();
        let blob = repo.find_blob(oid)?;
        let filemode = entry.filemode() as u32;
        let commit_time = commit.time();
        let name = format!("{count:04}_{:.7}", commit.id());

        Ok(Some(ObjectAttr {
            name,
            oid,
            kind: ObjectType::Blob,
            filemode,
            size: blob.size() as u64,
            commit_time,
        }))
    }

    fn find_renamed_parent_path(
        &self,
        parent_tree: &Tree,
        tree: &Tree,
        current_path: &str,
    ) -> anyhow::Result<Option<String>> {
        let repo = &self.inner;

        let mut diff_opts = DiffOptions::new();
        diff_opts.include_typechange(true);
        // Do NOT pathspec-limit here; it can hide the "old" side needed for rename pairing.

        let mut diff =
            repo.diff_tree_to_tree(Some(parent_tree), Some(tree), Some(&mut diff_opts))?;

        let mut find_opts = DiffFindOptions::new();
        find_opts
            .renames(true)
            .renames_from_rewrites(true)
            .copies(true);

        diff.find_similar(Some(&mut find_opts))?;

        for d in diff.deltas() {
            let newp = d.new_file().path();
            let oldp = d.old_file().path();

            if let (Some(np), Some(op)) = (newp, oldp)
                && np == std::path::Path::new(current_path)
            {
                match d.status() {
                    Delta::Renamed
                    | Delta::Copied
                    | Delta::Typechange
                    | Delta::Modified
                    | Delta::Added => {
                        return Ok(Some(op.to_string_lossy().into_owned()));
                    }
                    _ => {}
                }
            }
        }

        Ok(None)
    }

    fn find_newest_commit_containing_blob_fp(
        &self,
        blob_oid: Oid,
        max_steps: usize,
    ) -> anyhow::Result<Commit<'_>> {
        let repo = &self.inner;
        let mut commit = repo.head()?.peel_to_commit()?;
        let mut steps = 0usize;

        let mut tree_hit: HashMap<Oid, bool> = HashMap::new();

        loop {
            steps += 1;
            if steps > max_steps {
                bail!("find_newest_commit_containing_blob_fp: exceeded {max_steps} steps");
            }

            let tid = commit.tree_id();
            let contains = *tree_hit.entry(tid).or_insert_with(|| {
                let tree = repo.find_tree(tid).expect("missing tree");
                self.tree_contains_blob(&tree, blob_oid)
            });

            if contains {
                return Ok(commit);
            }

            if commit.parent_count() == 0 {
                bail!("Blob not found on first-parent chain from HEAD");
            }
            commit = commit.parent(0)?;
        }
    }

    fn tree_contains_blob(&self, tree: &Tree, blob_oid: Oid) -> bool {
        let mut found = false;
        let _ = tree.walk(git2::TreeWalkMode::PreOrder, |_, entry| {
            if entry.kind() == Some(ObjectType::Blob) && entry.id() == blob_oid {
                found = true;
                return git2::TreeWalkResult::Abort;
            }
            git2::TreeWalkResult::Ok
        });
        found
    }

    fn find_path_of_blob_in_tree(&self, tree: &Tree, blob_oid: Oid) -> Option<String> {
        let mut result: Option<String> = None;
        let _ = tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
            if entry.kind() == Some(ObjectType::Blob) && entry.id() == blob_oid {
                let name = entry.name().unwrap_or_default();
                let mut path = String::new();
                path.push_str(root);
                path.push_str(name);
                if let Some(s) = path.strip_prefix("./") {
                    result = Some(s.to_string());
                } else {
                    result = Some(path);
                }
                return git2::TreeWalkResult::Abort;
            }
            git2::TreeWalkResult::Ok
        });
        result
    }

    fn get_blob_oid_at_path(&self, tree: &Tree, path: &str) -> Option<Oid> {
        let entry = tree.get_path(std::path::Path::new(path)).ok()?;
        if entry.kind() == Some(ObjectType::Blob) {
            Some(entry.id())
        } else {
            None
        }
    }

    fn head_commit(&self) -> anyhow::Result<Commit<'_>> {
        let repo = &self.inner;
        Ok(repo.head()?.peel_to_commit()?)
    }

    fn head_tree(&self) -> anyhow::Result<Tree<'_>> {
        Ok(self.head_commit()?.tree()?)
    }

    pub fn get_build_state(&mut self, commit_oid: Oid, build_folder: &Path) -> anyhow::Result<PathBuf> {
        match self.build_sessions.entry(commit_oid) {
            Entry::Occupied(entry) => {
                let session = entry.get();
                Ok(session.folder.path().into())
            }
            Entry::Vacant(slot) => {
                let folder = tempfile::Builder::new()
                    .prefix(&format!("build_{}", &commit_oid.to_string()[..=7]))
                    .rand_bytes(4)
                    .tempdir_in(build_folder)?;
                let folder_name = folder.path().to_path_buf();
                let session = BuildSession {
                    folder,
                    open_count: AtomicUsize::new(0),
                    pinned: AtomicBool::new(false),
                };
                slot.insert(Arc::new(session));
                Ok(folder_name)
            }
        }
    }
}

/// If the name supplied follows the format:
///      --> github.tokio-rs.tokio.git <br>
/// It will parse it and return the fetch url. <br>
/// Otherwise, it will return None
/// This will signal that we create a normal folder <br>
pub fn parse_mkdir_url(name: &str) -> anyhow::Result<Option<(String, String)>> {
    if (!name.starts_with("github.") || !name.starts_with("gitlab.")) && !name.ends_with(".git") {
        return Ok(None);
    }
    let mut comp = name.splitn(4, ".");
    if comp.clone().count() != 4 {
        bail!("Invalid input")
    }
    let website = comp
        .next()
        .ok_or_else(|| anyhow!("Error getting website from url"))?;
    let account = comp
        .next()
        .ok_or_else(|| anyhow!("Error getting account from url"))?;
    let repo = comp
        .next()
        .ok_or_else(|| anyhow!("Error getting repo name from url"))?;
    let git = comp
        .next()
        .ok_or_else(|| anyhow!("Error getting .git name from url"))?;

    let url = format!("https://{website}.com/{account}/{repo}.{git}");
    Ok(Some((url, repo.into())))
}

impl PartialEq for GitRepo {
    fn eq(&self, other: &Self) -> bool {
        self.repo_id == other.repo_id
    }
}

pub fn try_into_filemode(mode: i64) -> Option<FileMode> {
    let m = u32::try_from(mode).ok()?;
    // Exact matches first
    match m {
        0o040000 => Some(FileMode::Tree),
        0o100644 => Some(FileMode::Blob),
        0o100755 => Some(FileMode::BlobExecutable),
        0o120000 => Some(FileMode::Link),
        0o160000 => Some(FileMode::Commit),
        0 => Some(FileMode::Unreadable),
        _ => {
            // Normalize common stat-like modes if they sneak in
            let typ = m & 0o170000;
            match typ {
                0o040000 => Some(FileMode::Tree),
                0o120000 => Some(FileMode::Link),
                0o160000 => Some(FileMode::Commit),
                0o100000 => {
                    if (m & 0o111) != 0 {
                        Some(FileMode::BlobExecutable)
                    } else {
                        Some(FileMode::Blob)
                    }
                }
                _ => None,
            }
        }
    }
}
