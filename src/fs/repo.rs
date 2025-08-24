use anyhow::{anyhow, bail};
use chrono::{DateTime, Datelike};
use git2::{
    Commit, Direction, ErrorClass, ErrorCode, FetchOptions, FileMode, ObjectType, Oid,
    RemoteCallbacks, Repository, Sort, Time, Tree, TreeWalkMode, TreeWalkResult,
};
use std::{
    collections::{BTreeMap, HashSet},
    sync::{Arc, Mutex},
};

use crate::fs::{ObjectAttr, meta_db::MetaDb};

pub struct GitRepo {
    // Caching the database connection for reads.
    // Must be refreshed after every write.
    pub connection: Arc<Mutex<MetaDb>>,
    pub repo_dir: String,
    pub repo_id: u16,
    pub inner: Repository,
    pub head: Option<Oid>,
    // i64 -> commit_time -> seconds since EPOCH
    // Vec<Oid> -> Vec<commit_oid> -> In case commits are made at the same time
    pub snapshots: BTreeMap<i64, Vec<Oid>>,
    pub res_inodes: HashSet<u64>,
    /// u64: inode of the real file
    pub vdir_cache: BTreeMap<u64, VirtualNode>,
}

/// Insert/get a node during getattr/lookup
///
/// Fill log during readdir
pub struct VirtualNode {
    /// Inode of the virtual directory
    pub inode: u64,
    /// Oid of the file
    pub oid: Oid,
    /// Oids of the file history (if any)
    pub log: Vec<Oid>,
}

// For customized fetch
pub struct Remote {
    arg_remote: Option<String>,
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

    fn parse_day_key(key: &str) -> Option<(i32, u32, u32)> {
        let mut comp = key.splitn(3, '-');
        let y = comp.next()?;
        let m = comp.next()?;
        let d = comp.next()?;
        Some((y.parse().ok()?, m.parse().ok()?, d.parse().ok()?))
    }

    // Takes a month kay -> "YYYY-MM".
    // Returns a vec of commits whose commit_time match that day
    // Folder name format: "Snaps on Aug 6, 2025".
    pub fn day_folders(&self, month_key: &str) -> anyhow::Result<Vec<ObjectAttr>> {
        let (year, month) =
            Self::parse_month_key(month_key).ok_or_else(|| anyhow!("Invalid input"))?;

        let mut out: Vec<ObjectAttr> = Vec::new();
        let mut seen_days: HashSet<(i32, u32, u32)> = HashSet::new();

        // self.snapshots: BTreeMap<i64 /*secs UTC*/, Vec<Oid>>; iterate newest -> oldest
        for (&secs_utc, oids) in self.snapshots.iter().rev() {
            for _ in oids {
                // Do not handle the offset. Only UTC time.
                let dt = DateTime::from_timestamp(secs_utc, 0)
                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());

                if dt.year() != year || dt.month() != month {
                    continue;
                }

                let day_key = (dt.year(), dt.month(), dt.day());
                // No duplicates
                if !seen_days.insert(day_key) {
                    continue;
                }

                let folder_name = format!("Snaps_on_{}", dt.format("%b.%-d.%Y"));

                out.push(ObjectAttr {
                    name: folder_name,
                    oid: git2::Oid::zero(),
                    kind: ObjectType::Tree,
                    filemode: 0o040000,
                    size: 0,
                    commit_time: git2::Time::new(secs_utc, 0),
                });
            }
        }

        Ok(out)
    }

    pub fn day_commits(&self, day_key: &str) -> anyhow::Result<Vec<ObjectAttr>> {
        let (year, month, day) =
            Self::parse_day_key(day_key).ok_or_else(|| anyhow!("Invalid input"))?;

        let mut out: Vec<ObjectAttr> = Vec::new();
        let mut commit_num = 0;

        for (&secs_utc, oids) in &self.snapshots {
            for commit_oid in oids {
                let dt = DateTime::from_timestamp(secs_utc, 0)
                    .unwrap_or_else(|| DateTime::from_timestamp(0, 0).unwrap());

                if dt.year() != year || dt.month() != month || dt.day() != day {
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

    // The FileAttr for the snap folder will contain the commit id instead of tree
    // This makes walking and getting info for the Fileattr easier
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

    fn head_commit(&self) -> anyhow::Result<Commit<'_>> {
        let repo = &self.inner;
        Ok(repo.head()?.peel_to_commit()?)
    }

    fn head_tree(&self) -> anyhow::Result<Tree<'_>> {
        Ok(self.head_commit()?.tree()?)
    }
}

/// If the name supplied follows the format:
///      --> github.tokio-rs.tokio.git <br>
/// It will parse it and return the fetch url. <br>
/// Otherwise, it will return None
/// This will signal that we create a normal folder <br>
pub fn parse_mkdir_url(name: &str) -> anyhow::Result<Option<(String, String)>> {
    if !name.starts_with("github.") && !name.ends_with(".git") {
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
