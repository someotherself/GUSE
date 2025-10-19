use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, Datelike};
use git2::{
    Commit, Delta, Direction, FileMode, ObjectType, Oid, Repository, Sort, Time, Tree,
    TreeWalkResult,
};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{BTreeMap, HashMap, HashSet, hash_map::Entry},
    ffi::OsString,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    },
};

use crate::{
    fs::{ObjectAttr, SourceTypes, builds::BuildSession, fileattr::FileAttr},
    inodes::VirtualIno,
    internals::{cache::LruCache, cache_dentry::DentryLru},
};

pub struct GitRepo {
    pub repo_dir: String,
    pub build_dir: PathBuf,
    pub repo_id: u16,
    pub inner: Mutex<Repository>,
    pub state: RwLock<State>,
    /// LruCache<target_ino, FileAttr>
    pub attr_cache: LruCache<u64, FileAttr>,
    /// LruCache<(parent, target_name), Dentry>
    pub dentry_cache: DentryLru,
    /// LruCache<ino, >
    pub file_cache: LruCache<u64, SourceTypes>,
}

pub struct State {
    pub head: Option<Oid>,
    /// i64 -> commit_time -> seconds since EPOCH
    ///
    /// Vec<Oid> -> Vec<commit_oid> -> In case commits are made at the same time
    pub snapshots: BTreeMap<i64, Vec<Oid>>,
    /// Used inodes to prevent reading from DB
    pub res_inodes: HashSet<u64>,
    /// key: inode of the virtual directory
    pub vdir_cache: BTreeMap<VirtualIno, VirtualNode>,
    /// Oid = Commit Oid
    pub build_sessions: HashMap<Oid, Arc<BuildSession>>,
}

/// Insert/get a node during getattr/lookup
///
/// Fill log during readdir
#[derive(Debug, Clone)]
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
    pub log: BTreeMap<OsString, (u64, ObjectAttr)>,
}

impl GitRepo {
    pub fn with_repo<R>(&self, f: impl FnOnce(&Repository) -> R) -> R {
        let guard = self.inner.lock();
        f(&guard)
    }

    pub fn with_repo_mut<R>(&self, f: impl FnOnce(&mut Repository) -> R) -> R {
        let mut guard = self.inner.lock();
        f(&mut guard)
    }

    pub fn with_state<R>(&self, f: impl FnOnce(&State) -> R) -> R {
        let guard = self.state.write();
        f(&guard)
    }

    pub fn with_state_mut<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let mut guard = self.state.write();
        f(&mut guard)
    }

    pub fn refresh_snapshots(&self) -> anyhow::Result<()> {
        let head_oid = self.with_repo(|r| match r.head() {
            Ok(h) => h.target(),
            Err(_) => None,
        });

        self.with_state_mut(|s| {
            s.head = head_oid;
            s.snapshots.clear();
        });

        self.with_repo(|r| -> Result<(), git2::Error> {
            let mut walk = r.revwalk()?;
            walk.set_sorting(git2::Sort::TOPOLOGICAL | git2::Sort::TIME)?;
            if let Some(oid) = head_oid {
                walk.push(oid)?;
            };
            for oid_res in walk {
                let oid = oid_res?;
                let secs = {
                    let c = r.find_commit(oid)?;
                    let t = c.time();
                    t.seconds()
                };
                self.with_state_mut(|s| s.snapshots.entry(secs).or_default().push(oid));
            }
            Ok(())
        })?;
        Ok(())
    }

    pub fn months_from_cache(&self, use_offset: bool) -> BTreeMap<String, Vec<git2::Oid>> {
        let mut buckets: BTreeMap<String, Vec<git2::Oid>> = BTreeMap::new();

        self.with_state(|s| {
            for (&secs, oids) in s.snapshots.iter() {
                let adj = if use_offset {
                    let t = self.with_repo(|r| {
                        r.find_commit(oids[0])
                            .ok()
                            .map(|c| c.time().offset_minutes())
                            .unwrap_or(0)
                    });
                    secs + (t as i64) * 60
                } else {
                    secs
                };

                let dt = DateTime::from_timestamp(adj, 0)
                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());

                let key = format!("{:04}-{:02}", dt.year(), dt.month());

                buckets.entry(key).or_default().extend(oids.iter().copied());
            }
        });
        buckets
    }

    pub fn month_folders(&self) -> anyhow::Result<Vec<ObjectAttr>> {
        let mut out = Vec::new();

        self.with_state(|s| {
            for secs in s.snapshots.keys() {
                let dt = chrono::DateTime::from_timestamp(*secs, 0)
                    .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                let folder_name = OsString::from(format!("{:04}-{:02}", dt.year(), dt.month()));

                if out.iter().any(|attr: &ObjectAttr| attr.name == folder_name) {
                    continue;
                }

                out.push(ObjectAttr {
                    name: folder_name,
                    oid: Oid::zero(),
                    kind: ObjectType::Tree,
                    git_mode: 0o040000,
                    size: 0,
                    commit_time: git2::Time::new(*secs, 0),
                });
            }
        });

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

        self.with_state(|s| {
            for (&secs_utc, oids) in &s.snapshots {
                for commit_oid in oids {
                    let dt = DateTime::from_timestamp(secs_utc, 0)
                        .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());

                    if dt.year() != year || dt.month() != month {
                        continue;
                    }

                    commit_num += 1;
                    let folder_name =
                        OsString::from(format!("Snap{commit_num:03}_{commit_oid:.7}"));

                    out.push(ObjectAttr {
                        name: folder_name,
                        oid: *commit_oid,
                        kind: ObjectType::Commit,
                        git_mode: 0o040000,
                        size: 0,
                        commit_time: git2::Time::new(secs_utc, 0),
                    });
                }
            }
        });
        Ok(out)
    }

    pub fn fetch_anon(&self, url: &str) -> anyhow::Result<()> {
        // Set up the anonymous remote and callbacks self
        let repo = self.inner.lock();
        let mut remote = repo.remote_anonymous(url)?;
        let mut cbs = git2::RemoteCallbacks::new();
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
        let mut fo = git2::FetchOptions::new();
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
            if let Some(ref buf) = default_branch
                && let Ok(src) = std::str::from_utf8(buf.as_ref())
            {
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
        Ok(())
    }

    /// Read_dir
    ///
    /// Called with tree_oid: None if we are at the root of the commit
    pub fn list_tree(&self, commit: Oid, tree_oid: Option<Oid>) -> anyhow::Result<Vec<ObjectAttr>> {
        let repo = self.inner.lock();
        let commit = repo.find_commit(commit)?;
        let mut entries = vec![];
        let tree = match tree_oid {
            Some(tree_oid) => repo.find_tree(tree_oid)?,
            None => commit.tree()?,
        };
        for entry in tree.iter() {
            let name = OsString::from(entry.name().unwrap_or(""));
            let kind = entry.kind().ok_or_else(|| anyhow!("Invalid object"))?;

            let mut size = 0;
            if kind == git2::ObjectType::Blob {
                let blob = repo.find_blob(entry.id())?;
                size = blob.size() as u64;
            }
            entries.push(ObjectAttr {
                name,
                oid: entry.id(),
                kind: entry.kind().ok_or_else(|| anyhow!("Invalid object"))?,
                git_mode: entry.filemode() as u32,
                size,
                commit_time: Time::new(0, 0),
            });
        }
        Ok(entries)
    }

    pub fn find_by_name(&self, tree_oid: Oid, name: &str) -> anyhow::Result<ObjectAttr> {
        let repo = self.inner.lock();
        let tree = repo.find_tree(tree_oid)?;

        let entry = tree
            .get_name(name)
            .ok_or_else(|| anyhow!(format!("{name} not found in tree {tree_oid}")))?;
        let size = match entry.kind().ok_or_else(|| anyhow!("Invalid object"))? {
            ObjectType::Blob => entry
                .to_object(&repo)
                .map_err(|_| anyhow!("Invalid object"))?
                .into_blob()
                .map_err(|_| anyhow!("Invalid object"))?
                .size(),
            _ => 0,
        };
        let commit = repo.head()?.peel_to_commit()?;
        let commit_time = commit.time();
        Ok(ObjectAttr {
            name: name.into(),
            oid: entry.id(),
            kind: entry.kind().ok_or_else(|| anyhow!("Invalid object"))?,
            git_mode: entry.filemode() as u32,
            size: size as u64,
            commit_time,
        })
    }

    pub fn find_in_commit(&self, commit_id: Oid, oid: Oid) -> anyhow::Result<ObjectAttr> {
        let repo = self.inner.lock();
        let commit_obj = repo.find_commit(commit_id)?;
        let commit_time = commit_obj.time();
        let tree = commit_obj.tree()?;

        if commit_id == oid {
            return Ok(ObjectAttr {
                name: ".".into(),
                oid,
                kind: ObjectType::Tree,
                git_mode: 0o040000,
                size: 0,
                commit_time,
            });
        }
        let mut found: Option<ObjectAttr> = None;
        let walk_res = tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
            if entry.id() == oid {
                let mut name = OsString::from(root);
                name.push(entry.name().unwrap_or("<non-utf8>"));
                found = Some(ObjectAttr {
                    name,
                    oid,
                    kind: entry.kind().unwrap_or(ObjectType::Any),
                    git_mode: entry.filemode() as u32,
                    size: if entry.kind() == Some(ObjectType::Blob) {
                        repo.find_blob(entry.id())
                            .map(|b| b.size() as u64)
                            .unwrap_or(0)
                    } else {
                        0
                    },
                    commit_time,
                });
                return TreeWalkResult::Abort;
            }
            TreeWalkResult::Ok
        });

        if let Err(e) = walk_res
            && !(e.class() == git2::ErrorClass::Callback && e.code() == git2::ErrorCode::User)
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
        let repo = self.inner.lock();
        let limit = 20;
        let head_commit = self.head_commit()?;

        let mut walk = repo.revwalk()?;
        walk.set_sorting(Sort::TOPOLOGICAL | Sort::TIME)?;
        walk.push(head_commit)?;

        let mut out = Vec::with_capacity(limit.min(256));
        for (i, oid_res) in walk.enumerate() {
            if out.len() >= limit {
                break;
            }
            let oid = oid_res?;
            let commit = repo.find_commit(oid)?;

            let mut commit_name = OsString::from("snap");
            commit_name.push((i + 1).to_string());
            commit_name.push("_");
            commit_name.push(&commit.id().to_string()[..7]);

            out.push(ObjectAttr {
                name: commit_name,
                oid,
                kind: ObjectType::Commit,
                git_mode: 0u32,
                size: 0u64,
                commit_time: commit.time(),
            });
        }
        Ok(out)
    }

    pub fn readdir_commit(&self) -> anyhow::Result<Vec<ObjectAttr>> {
        let mut entries: Vec<ObjectAttr> = vec![];
        let commit_oid = self.head_commit()?;
        let repo = self.inner.lock();
        let commit = repo.find_commit(commit_oid)?;
        let root_tree = commit.tree()?;
        for entry in root_tree.iter() {
            let oid = entry.id();
            let kind = entry.kind().ok_or_else(|| anyhow!("Invalid object"))?;
            let git_mode = entry.filemode() as u32;
            let name = OsString::from(entry.name().unwrap_or(""));
            let commit_time = commit.time();
            entries.push(ObjectAttr {
                name,
                oid,
                kind,
                git_mode,
                size: 0,
                commit_time,
            });
        }
        Ok(entries)
    }

    pub fn blob_history_objects(&self, target_blob: Oid) -> anyhow::Result<Vec<ObjectAttr>> {
        let oid = self
            .find_newest_commit_containing_blob_fp(target_blob, 50_000)
            .context("Blob not found (first-parent scan)")?;
        let repo = self.inner.lock();
        let mut commit = repo.find_commit(oid)?;

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
                self.object_attr_for_path_in_tree(&repo, &tree, &current_path, &commit, blob_count)?
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
        let git_mode = entry.filemode() as u32;
        let commit_time = commit.time();
        let name = OsString::from(format!("{count:04}_{:.7}", commit.id()));

        Ok(Some(ObjectAttr {
            name,
            oid,
            kind: ObjectType::Blob,
            git_mode,
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
        let repo = &self.inner.lock();

        let mut diff_opts = git2::DiffOptions::new();
        diff_opts.include_typechange(true);
        // Do NOT pathspec-limit here; it can hide the "old" side needed for rename pairing.

        let mut diff =
            repo.diff_tree_to_tree(Some(parent_tree), Some(tree), Some(&mut diff_opts))?;

        let mut find_opts = git2::DiffFindOptions::new();
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
    ) -> anyhow::Result<Oid> {
        let repo = &self.inner.lock();
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
                return Ok(commit.id());
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

    fn head_commit(&self) -> anyhow::Result<Oid> {
        let repo = &self.inner.lock();
        Ok(repo.head()?.peel_to_commit()?.id())
    }

    #[allow(dead_code)]
    fn head_tree(&self) -> anyhow::Result<Oid> {
        let oid = self.head_commit()?;
        self.with_repo(|r| -> anyhow::Result<Oid> {
            let commit = r.find_commit(oid)?;
            let tree = commit.tree()?.id();
            Ok(tree)
        })
    }

    pub fn get_or_init_build_session(
        &self,
        commit_oid: Oid,
        build_folder: &Path,
    ) -> anyhow::Result<Arc<BuildSession>> {
        self.with_state_mut(|s| match s.build_sessions.entry(commit_oid) {
            Entry::Occupied(entry) => {
                let session = entry.get();
                Ok(session.clone())
            }
            Entry::Vacant(slot) => {
                let folder = tempfile::Builder::new()
                    .prefix(&format!("build_{}", &commit_oid.to_string()[..=7]))
                    .tempdir_in(build_folder)?;
                let session = Arc::new(BuildSession {
                    folder,
                    open_count: AtomicUsize::new(0),
                    pinned: AtomicBool::new(false),
                });
                slot.insert(session.clone());
                Ok(session)
            }
        })
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

pub fn try_into_filemode(mode: u64) -> Option<FileMode> {
    let m = u32::try_from(mode).ok()?;
    match m {
        0o040000 => Some(FileMode::Tree),
        0o100644 => Some(FileMode::Blob),
        0o100755 => Some(FileMode::BlobExecutable),
        0o120000 => Some(FileMode::Link),
        0o160000 => Some(FileMode::Commit),
        0 => Some(FileMode::Unreadable),
        _ => {
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
