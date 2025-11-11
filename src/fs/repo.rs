use anyhow::{Context, anyhow, bail};
use chrono::{DateTime, Datelike};
use dashmap::DashMap;
use git2::{Commit, Delta, Direction, FileMode, ObjectType, Oid, Repository, Time, Tree};
use parking_lot::{Mutex, RwLock};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize},
    },
};

use crate::{
    fs::{
        GitFs, LIVE_FOLDER, ObjectAttr, SourceTypes,
        builds::{BuildSession, inject::InjectedMetadata},
        fileattr::{FileAttr, InoFlag},
    },
    inodes::VirtualIno,
    internals::{cache::LruCache, cache_dentry::DentryLru},
};

pub struct GitRepo {
    pub repo_dir: String,
    /// Used for generating storing files created in a Snap folder (and builds, compilations...)
    pub build_dir: PathBuf,
    /// Used for guse chase templates, configuration and results
    pub chase_dir: PathBuf,
    pub repo_id: u16,
    pub inner: Mutex<Repository>,
    pub state: RwLock<State>,
    /// LruCache<target_ino, FileAttr>
    pub attr_cache: LruCache<u64, FileAttr>,
    /// LruCache<Dentry>
    pub dentry_cache: DentryLru,
    /// LruCache<ino, SourceTypes::RealFile>
    pub file_cache: LruCache<u64, SourceTypes>,
    pub injected_files: DashMap<u64, InjectedMetadata>,
}

pub struct State {
    /// Used inodes to prevent reading from DB
    pub res_inodes: HashSet<u64>,
    /// key: inode of the virtual directory
    pub vdir_cache: BTreeMap<VirtualIno, VirtualNode>,
    /// Oid = Commit Oid
    pub build_sessions: HashMap<Oid, Arc<BuildSession>>,
    /// Map of the Snap and Month folders
    ///
    /// <folder name, (MONTH name, Snap name)>
    /// TODO: remove and add to snapshots
    pub snaps_map: HashMap<Oid, (OsString, OsString)>,
    /// Maps all the commits to one of more refs
    pub snaps_to_ref: HashMap<Oid, BTreeSet<RefKind>>,
    /// Lists all the commits for a respective ref
    ///
    /// i64 -> commit_time -> seconds since EPOCH. Used to create MONTH and Snap folders
    pub refs_to_snaps: HashMap<RefKind, Vec<(i64, Oid)>>, // HashMap<RefKind, Vec<(Oid, short name)>>
    /// Lists the unique namespace types
    ///
    /// To keep track which kinds of refs are available (branches, tags, pr, pr-merge)
    pub unique_namespaces: HashSet<String>,
}

/// Create the Virtual Node during opendir
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
///Stores short name
pub enum RefKind {
    Branch(Arc<str>),
    Tag(Arc<str>),
    Pr(Arc<str>),
    PrMerge(Arc<str>),
    Head(Arc<str>),
    Main(Arc<str>),
}

impl RefKind {
    pub const fn as_str(&self) -> &'static str {
        match *self {
            Self::Branch(_) => "Branches",
            Self::Head(_) => "Head",
            Self::Pr(_) => "Pr",
            Self::PrMerge(_) => "PrMerge",
            Self::Tag(_) => "Tags",
            Self::Main(_) => "main",
        }
    }
}

impl RefKind {
    // TODO: Make this better at identifying main and head???
    fn classify_ref(full: &str) -> Option<Self> {
        let mut split = full.rsplit('/');
        let short = split.next()?;
        if full.contains("HEAD") {
            return Some(RefKind::Head(short.into()));
        }
        if full.ends_with("main") || full.ends_with("master") {
            return Some(RefKind::Main(short.into()));
        };
        if full.starts_with("refs/heads/") {
            return Some(RefKind::Branch(short.into()));
        }
        if full.starts_with("refs/tags/") {
            return Some(RefKind::Tag(short.into()));
        }
        if full.starts_with("refs/remotes/") {
            if full.contains("/pr/") {
                Some(RefKind::Pr(short.into()))
            } else if full.contains("/pr-merge/") {
                Some(RefKind::PrMerge(short.into()))
            } else {
                Some(RefKind::Branch(short.into()))
            }
        } else {
            None
        }
    }
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

    /// Updates these fields in State: snaps_to_ref, refs_to_snaps and unique_namespaces. They are not updated anywhere else (which means the folder structure is only updated on fetch, or on a new session)
    ///
    /// Handles Branches, Tags, Pr and Pr-Merges
    ///
    /// - Branches: It does a merge_base against main (or master) and does a revwalk if it finds something. If that fails, it will merge_base against the other branches. If that also fails, it walk the entire history.
    ///
    /// - Pr: Checks if a Pr-Merge exists and if it does, walks between the head and base parent. Otherwise, they are ignored
    ///
    /// - Tags and Pr-Merges: Does not walk the history. Only saves the tip
    pub fn refresh_refs(&self) -> anyhow::Result<()> {
        let repo = self.inner.lock();

        let mut ref_tips = Vec::new();
        let mut all_namespaces: Vec<(String, RefKind)> = Vec::new();

        let mut revwalk = repo.revwalk()?;
        revwalk.set_sorting(git2::Sort::TOPOLOGICAL | git2::Sort::TIME)?;

        for r in repo.references()? {
            let r = r?;

            let Some(name) = r.name() else {
                continue;
            };

            let Some(ref_data) = RefKind::classify_ref(name) else {
                continue;
            };
            all_namespaces.push((name.to_string(), ref_data.clone()));

            if let Ok(tip) = r
                .resolve()
                .and_then(|rr| rr.peel(git2::ObjectType::Commit).map(|obj| obj.id()))
            {
                ref_tips.push((ref_data, tip, name.to_string()))
            }
        }

        let mut refs_to_snaps: HashMap<RefKind, Vec<(i64, Oid)>> = HashMap::new();
        let mut snaps_to_ref: HashMap<Oid, BTreeSet<RefKind>> = HashMap::new();
        let mut unique_namespaces: HashSet<String> = HashSet::new();

        for (rf, tip, name) in ref_tips {
            revwalk.reset()?;
            revwalk.push(tip)?;

            let entry = refs_to_snaps.entry(rf.clone()).or_default();
            unique_namespaces.insert(rf.as_str().to_string());

            // Do not walk the tags or pr-merges
            if matches!(rf, RefKind::Tag(_)) || matches!(rf, RefKind::PrMerge(_)) {
                let time = {
                    let commit = repo.find_commit(tip)?;
                    let t = commit.time();
                    t.seconds()
                };

                entry.push((time, tip));
                snaps_to_ref.entry(tip).or_default().insert(rf.clone());
                continue;
            }

            if matches!(rf, RefKind::Pr(_)) {
                let merge_name = name.replace("/pr/", "/pr-merge/");
                // If pr merge ref does not exist, PR was probably closed or merged. Ignore these for now.
                let Ok(merge_ref) = repo.find_reference(&merge_name) else {
                    continue;
                };
                let merge_commit = merge_ref.peel_to_commit()?;
                let base = merge_commit.parent(0)?;
                let head = merge_commit.parent(1)?;
                revwalk.reset()?;
                revwalk.push(head.id())?;
                revwalk.hide(base.id())?;
            }

            if matches!(rf, RefKind::Branch(_)) {
                let mut target_ref = all_namespaces
                    .iter()
                    .filter(|&(_, kind)| matches!(kind, RefKind::Main(_)))
                    .find_map(|(name, _)| repo.find_reference(name).ok());
                if target_ref.is_none() {
                    // Fallback, check the rest of the branches if not found against main/master
                    target_ref = all_namespaces
                        .iter()
                        .filter(|&(_, kind)| matches!(kind, RefKind::Branch(_)))
                        .find_map(|(name, _)| repo.find_reference(name).ok());
                }

                if let Some(main_ref) = target_ref {
                    let main_commit = main_ref.peel_to_commit()?.id();

                    let base = repo.merge_base(tip, main_commit)?;
                    revwalk.reset()?;
                    revwalk.push_range(&format!("{}..{}", base, tip))?;
                }
            }

            for oid in revwalk.by_ref() {
                let oid = oid?;

                let time = {
                    let commit = repo.find_commit(oid)?;
                    let t = commit.time();
                    t.seconds()
                };

                entry.push((time, oid));
                snaps_to_ref.entry(oid).or_default().insert(rf.clone());
            }
        }

        self.with_state_mut(|s| {
            s.refs_to_snaps = refs_to_snaps;
            s.snaps_to_ref = snaps_to_ref;
            s.unique_namespaces = unique_namespaces;
        });
        Ok(())
    }

    // Looks for the commits under `main` and splits them by months
    pub fn month_folders(&self) -> anyhow::Result<BTreeMap<OsString, ObjectAttr>> {
        let mut out: BTreeMap<OsString, ObjectAttr> = BTreeMap::new();

        self.with_state(|s| {
            let Some(objects) = s
                .refs_to_snaps
                .iter()
                .find(|(k, _)| matches!(k, RefKind::Main(_)))
            else {
                return;
            };
            for (secs, _) in objects.1 {
                let dt = chrono::DateTime::from_timestamp(*secs, 0)
                    .unwrap_or_else(|| chrono::DateTime::from_timestamp(0, 0).unwrap());
                let folder_name = OsString::from(format!("{:04}-{:02}", dt.year(), dt.month()));

                if out.iter().any(|(_, attr)| attr.name == folder_name) {
                    continue;
                }

                out.insert(
                    folder_name.clone(),
                    ObjectAttr {
                        name: folder_name,
                        oid: Oid::zero(),
                        kind: ObjectType::Tree,
                        git_mode: 0o040000,
                        size: 0,
                        commit_time: git2::Time::new(*secs, 0),
                    },
                );
            }
        });

        Ok(out)
    }

    fn parse_month_key(key: &str) -> Option<(i32, u32)> {
        let (y, m) = key.split_once('-')?;
        Some((y.parse().ok()?, m.parse().ok()?))
    }

    /// Similar to month_commits, but only returns the Oid and folder name of each commit instead of ObjectAttr
    fn month_oid(&self, month_key: &str) -> anyhow::Result<Vec<(Oid, OsString)>> {
        let (year, month) =
            Self::parse_month_key(month_key).ok_or_else(|| anyhow!("Invalid input"))?;

        let mut out: Vec<(Oid, OsString)> = Vec::new();
        let mut commit_num = 0;

        self.with_state(|s| {
            let Some(objects) = s
                .refs_to_snaps
                .iter()
                .find(|(k, _)| matches!(k, RefKind::Main(_)))
            else {
                return;
            };
            for (secs_utc, commit_oid) in objects.1 {
                let dt = DateTime::from_timestamp(*secs_utc, 0)
                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());

                if dt.year() != year || dt.month() != month {
                    continue;
                }

                commit_num += 1;
                let folder_name = OsString::from(format!("Snap{commit_num:03}_{commit_oid:.7}"));

                out.push((*commit_oid, folder_name));
            }
        });
        Ok(out)
    }

    pub fn month_commits(&self, month_key: &str) -> anyhow::Result<Vec<ObjectAttr>> {
        let (year, month) =
            Self::parse_month_key(month_key).ok_or_else(|| anyhow!("Invalid input"))?;

        let mut out: Vec<ObjectAttr> = Vec::new();
        let mut commit_num = 0;

        self.with_state(|s| {
            let Some(objects) = s
                .refs_to_snaps
                .iter()
                .find(|(k, _)| matches!(k, RefKind::Main(_)))
            else {
                return;
            };
            for (secs_utc, commit_oid) in objects.1 {
                let dt = DateTime::from_timestamp(*secs_utc, 0)
                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());

                if dt.year() != year || dt.month() != month {
                    continue;
                }

                commit_num += 1;
                let folder_name = OsString::from(format!("Snap{commit_num:03}_{commit_oid:.7}"));

                out.push(ObjectAttr {
                    name: folder_name,
                    oid: *commit_oid,
                    kind: ObjectType::Commit,
                    git_mode: 0o040000,
                    size: 0,
                    commit_time: git2::Time::new(*secs_utc, 0),
                });
            }
        });
        Ok(out)
    }

    /// Used for finding and creating folders for any commit in a tag or pr-merge
    /// Creates Snap folders
    pub fn non_branch_folders(&self, flag: InoFlag) -> anyhow::Result<Vec<ObjectAttr>> {
        let mut out: Vec<ObjectAttr> = Vec::new();

        let matches_flag = |k: &RefKind| match flag {
            InoFlag::TagsRoot => matches!(k, RefKind::Tag(_)),
            InoFlag::PrRoot => matches!(k, RefKind::Pr(_)),
            InoFlag::PrMergeRoot => matches!(k, RefKind::PrMerge(_)),
            InoFlag::BranchesRoot => matches!(k, RefKind::Branch(_)),
            _ => false,
        };

        self.with_state(|s| {
            for (ref_kind, objects) in s.refs_to_snaps.iter().filter(|(k, _)| matches_flag(k)) {
                let name = match ref_kind {
                    RefKind::Tag(n)
                    | RefKind::Pr(n)
                    | RefKind::PrMerge(n)
                    | RefKind::Branch(n)
                    | RefKind::Main(n)
                    | RefKind::Head(n) => n,
                };

                let folder_name = OsString::from(name.to_string());
                let Some(tip) = objects.first() else {
                    continue;
                };

                out.push(ObjectAttr {
                    name: folder_name,
                    oid: tip.1,
                    kind: ObjectType::Commit,
                    git_mode: 0o040000,
                    size: 0,
                    commit_time: git2::Time::new(tip.0, 0),
                });
            }
        });
        Ok(out)
    }

    /// Create folders (similar to MONTH folders) which contain Snaps
    pub fn branch_snaps(&self, name: &OsStr, flag: InoFlag) -> anyhow::Result<Vec<ObjectAttr>> {
        let mut out: Vec<ObjectAttr> = Vec::new();
        let mut commit_num = 0;

        // Look for the ref with the exact short name
        let rf_kind = if flag == InoFlag::BranchFolder {
            RefKind::Branch(name.to_string_lossy().into())
        } else {
            RefKind::Pr(name.to_string_lossy().into())
        };

        self.with_state(|s| {
            if let Some(objects) = s.refs_to_snaps.iter().find(|k| *k.0 == rf_kind) {
                let mut sorted = objects.1.clone();
                sorted.sort_unstable_by(|a, b| a.0.cmp(&b.0));
                for (secs_utc, commit_oid) in sorted {
                    commit_num += 1;
                    let folder_name =
                        OsString::from(format!("Snap{commit_num:03}_{commit_oid:.7}"));

                    out.push(ObjectAttr {
                        name: folder_name,
                        oid: commit_oid,
                        kind: ObjectType::Commit,
                        git_mode: 0o040000,
                        size: 0,
                        commit_time: git2::Time::new(secs_utc, 0),
                    });
                }
            };
        });

        Ok(out)
    }

    pub fn fetch_anon(&self, url: &str) -> anyhow::Result<()> {
        let repo = self.inner.lock();
        let mut remote = repo.remote_anonymous(url)?;
        let cbs = git2::RemoteCallbacks::new();
        let mut fo = git2::FetchOptions::new();
        fo.remote_callbacks(cbs);

        remote.connect(Direction::Fetch)?;
        // What branch does HEAD point to
        let default_branch = remote.default_branch();
        remote.disconnect()?;

        let mut refspecs = vec![
            "+refs/heads/*:refs/remotes/upstream/*".to_string(),
            "+refs/tags/*:refs/tags/*".to_string(),
            "+HEAD:refs/remotes/upstream/HEAD".to_string(),
        ];
        if url.contains("github") {
            refspecs.push("+refs/pull/*/head:refs/remotes/upstream/pr/*".to_string());
            refspecs.push("+refs/pull/*/merge:refs/remotes/upstream/pr-merge/*".to_string());
        }

        let refs_as_str: Vec<&str> = refspecs.iter().map(|s| s.as_str()).collect();

        remote.fetch(&refs_as_str, Some(&mut fo), None)?;

        if repo.head().is_err() {
            if let Ok(ref buf) = default_branch
                && let Ok(src) = std::str::from_utf8(buf.as_ref())
            {
                let short = src
                    .rsplit('/')
                    .next()
                    .ok_or_else(|| anyhow!("Invalid ref"))?;
                let target = format!("refs/remotes/upstream/{short}");
                if repo.refname_to_id(&target).is_ok() {
                    repo.set_head(&target)?;
                } else if let Ok(r) = repo.find_reference("refs/remotes/upstream/HEAD") {
                    if let Some(sym) = r.symbolic_target() {
                        repo.set_head(sym)?;
                    } else if let Some(oid) = r.target() {
                        repo.set_head_detached(oid)?;
                    }
                }
            }
        } else if let Ok(r) = repo.find_reference("refs/remotes/upstream/HEAD") {
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

    /// Similar to `get_or_init_build_session` but does not insert into build_sessions map
    pub fn new_build_session(
        &self,
        commit_oid: Oid,
        build_folder: &Path,
    ) -> anyhow::Result<Arc<BuildSession>> {
        let folder = tempfile::Builder::new()
            .prefix(&format!("build_{}", &commit_oid.to_string()[..=7]))
            .tempdir_in(build_folder)?;
        let session = Arc::new(BuildSession {
            folder,
            open_count: AtomicUsize::new(0),
            pinned: AtomicBool::new(false),
        });
        Ok(session)
    }

    pub fn get_or_init_build_session(
        &self,
        commit_oid: Oid,
        build_folder: &Path,
    ) -> anyhow::Result<Arc<BuildSession>> {
        self.with_state_mut(|s| match s.build_sessions.entry(commit_oid) {
            std::collections::hash_map::Entry::Occupied(entry) => {
                let session = entry.get();
                Ok(session.clone())
            }
            std::collections::hash_map::Entry::Vacant(slot) => {
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

    /// Changes the target commit for an existing build session.
    ///
    /// If a build session already exists for the new commit, it will return the old session for it.
    pub fn move_build_session(
        &self,
        commit_oid: Oid,
        new_commit_oid: Oid,
        build_folder: &Path,
    ) -> anyhow::Result<(Arc<BuildSession>, Option<Arc<BuildSession>>)> {
        let ctx = self.with_state_mut(
            |s| -> anyhow::Result<(Arc<BuildSession>, Option<Arc<BuildSession>>)> {
                let Some(entry) = s.build_sessions.remove(&commit_oid) else {
                    bail!(
                        "Build session does not exist for {commit_oid:?} and {}",
                        build_folder.display()
                    )
                };
                let old = s.build_sessions.insert(new_commit_oid, entry.clone());
                Ok((entry, old))
            },
        )?;
        Ok(ctx)
    }

    pub fn print_commit_summary(fs: &GitFs, repo_id: u16, oid: Oid) -> anyhow::Result<Vec<u8>> {
        let repo = fs
            .repos_list
            .get(&repo_id)
            .ok_or_else(|| anyhow!("Repo name not found"))?;

        let repo_root = std::fs::canonicalize(fs.repos_dir.join(&repo.repo_dir))?;
        let live_path = repo_root.join(LIVE_FOLDER);

        let output = std::process::Command::new("git")
            .arg("-C")
            .arg(&repo_root)
            .arg("--work-tree")
            .arg(&live_path)
            .arg("show")
            .arg("--patch")
            .arg("--color=always")
            .arg(oid.to_string())
            .output()?;

        if !output.status.success() {
            return Err(anyhow!(
                "git failed: {}",
                String::from_utf8_lossy(&output.stderr)
            ));
        }

        Ok(output.stdout)
    }
}

/// If the name supplied follows the format:
/// github.tokio-rs.tokio.git
///
/// It will parse it and return the fetch url.
///
/// Otherwise, it will return None
///
/// This will signal that we create a normal folder
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
