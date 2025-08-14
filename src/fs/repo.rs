#![allow(unused_imports)]

use anyhow::{Context, anyhow, bail};
use git2::{
    Commit, Direction, ErrorClass, ErrorCode, FetchOptions, ObjectType, Oid, Reference,
    RemoteCallbacks, Repository, Sort, Time, Tree, TreeWalkMode, TreeWalkResult,
};
use std::{
    ffi::OsStr,
    path::PathBuf,
    sync::{Arc, RwLock},
};
use tracing::info;

use std::{io::Write, path::Path};

use crate::fs::{DirectoryEntry, DirectoryEntryPlus, FileType, GitFs, ObjectAttr, meta_db::MetaDb};

pub struct GitRepo {
    // Caching the database connection for reads.
    // Must be refreshed after every write.
    pub connection: Arc<RwLock<MetaDb>>,
    pub repo_dir: String,
    pub repo_id: u16,
    pub inner: Repository,
    pub head: Option<Oid>,
}

// For customized fetch
pub struct Remote {
    arg_remote: Option<String>,
}

impl GitRepo {
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
                    src.rsplit('/').next().unwrap()
                ));
                refspecs.push("+HEAD:refs/remotes/anon/HEAD".to_string());
            }
        } else {
            refspecs.push("+HEAD:refs/remotes/anon/HEAD".to_string());
        }
        let refs_as_str: Vec<&str> = refspecs.iter().map(|s| s.as_str()).collect();

        remote
            .fetch(&refs_as_str, Some(&mut fo), None)
            .context("anonymous fetch failed")?;

        if repo.head().is_err() {
            if let Some(ref buf) = default_branch {
                if let Ok(src) = std::str::from_utf8(buf.as_ref()) {
                    let short = src.rsplit('/').next().unwrap();
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

    pub fn getattr<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<ObjectAttr> {
        // Get the commit the head points to
        let commit = self.inner.head()?.peel_to_commit()?;
        let commit_time = commit.time();
        let mut tree = commit.tree()?;
        let last_comp = path
            .as_ref()
            .components()
            .next_back()
            .ok_or_else(|| anyhow!("empty path"))?;

        let mut last_name = None;

        for comp in path.as_ref().components() {
            let name = comp.as_os_str().to_str().unwrap();
            let is_last_comp = comp == last_comp;

            // Lookup this name in the current tree
            let entry = tree.clone();
            let entry = entry
                .get_name(name)
                .ok_or_else(|| anyhow!("component {:?} not found in tree", name))?;

            if entry.kind() == Some(ObjectType::Tree) {
                if is_last_comp {
                    // Final component is a directory
                    last_name = Some(name.to_string());
                    tree = self.inner.find_tree(entry.id())?;
                } else {
                    // Descend into the sub-tree for next components
                    tree = self.inner.find_tree(entry.id())?;
                }
            } else {
                // If it's the last component
                if is_last_comp {
                    // and a blob. return the ObjectAttr
                    if entry.kind().unwrap() == git2::ObjectType::Blob {
                        let blob = self.inner.find_blob(entry.id())?;
                        let size = blob.size() as u64;
                        // blob with mode 0o120000 will be a symlink
                        return Ok(ObjectAttr {
                            name: entry.name().unwrap().into(),
                            oid: entry.id(),
                            kind: entry.kind().unwrap(),
                            filemode: entry.filemode() as u32,
                            size,
                            commit_time,
                        });
                    }
                // Not a final component and not a tree either. Something is wrong
                } else {
                    return Err(anyhow!("path {:?} is not a directory", path.as_ref()));
                }
            }
        }
        Ok(ObjectAttr {
            name: last_name.unwrap(),
            oid: tree.id(),
            kind: ObjectType::Tree,
            filemode: libc::S_IFDIR,
            size: 0,
            commit_time,
        })
    }

    // Read_dir
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
                kind: entry.kind().unwrap(),
                filemode: entry.filemode() as u32,
                size: 0,
                commit_time: Time::new(0, 0),
            });
        }
        Ok(entries)
    }

    // read_dir plus
    // pub fn list_tree_plus(
    //     &self,
    //     fs: &GitFs,
    //     tree_oid: Oid,
    //     tree_inode: u64,
    // ) -> anyhow::Result<Vec<DirectoryEntryPlus>> {
    //     let list_tree = self.list_tree(fs, tree_oid, tree_inode)?;
    //     let mut list_tree_plus: Vec<DirectoryEntryPlus> = Vec::new();
    //     for entry in list_tree {
    //         let attr = fs.find_by_name(tree_inode, &entry.name)?.ok_or_else(|| {
    //             anyhow!(
    //                 "no entry named {:?} in tree inode {}",
    //                 entry.name,
    //                 tree_inode
    //             )
    //         })?;
    //         list_tree_plus.push(DirectoryEntryPlus { entry, attr });
    //     }
    //     Ok(list_tree_plus)
    // }

    pub fn find_by_name(&self, tree_oid: Oid, name: &str) -> anyhow::Result<ObjectAttr> {
        let tree = self
            .inner
            .find_tree(tree_oid)
            .context("Parent tree does not exist")?;

        let entry = tree
            .get_name(name)
            .ok_or_else(|| anyhow!("{} not found in tree {}", name, tree_oid))?;
        let size = match entry.kind().unwrap() {
            ObjectType::Blob => entry
                .to_object(&self.inner)
                .unwrap()
                .into_blob()
                .unwrap()
                .size(),
            _ => 0,
        };
        let commit = self.inner.head()?.peel_to_commit()?;
        let commit_time = commit.time();
        Ok(ObjectAttr {
            name: name.into(),
            oid: entry.id(),
            kind: entry.kind().unwrap(),
            filemode: entry.filemode() as u32,
            size: size as u64,
            commit_time,
        })
    }

    pub fn find_by_path<P: AsRef<Path>>(&self, path: P, oid: Oid) -> anyhow::Result<ObjectAttr> {
        // HEAD → commit → root tree
        let commit = self.inner.head()?.peel_to_commit()?;
        let commit_time = commit.time();
        let mut tree = commit.tree()?;

        let d = path.as_ref();
        let rel: Option<PathBuf> =
            if d.as_os_str().is_empty() || d == Path::new(".") || d == Path::new("/") {
                None
            } else {
                Some(d.strip_prefix("/").unwrap_or(d).to_path_buf())
            };

        if let Some(ref relp) = rel {
            let e = tree
                .get_path(relp)
                .map_err(|_| anyhow!("directory {:?} not found in HEAD", relp))?;
            if e.kind() != Some(ObjectType::Tree) {
                return Err(anyhow!("'{}' is not a directory (tree)", relp.display()));
            }
            tree = self.inner.find_tree(e.id())?;
        }

        for entry in tree.iter() {
            if entry.id() == oid {
                let kind = entry.kind().ok_or_else(|| anyhow!("unknown entry kind"))?;
                let size = if kind == ObjectType::Blob {
                    self.inner.find_blob(entry.id())?.size() as u64
                } else {
                    0
                };
                let filemode = entry.filemode() as u32;

                // Build full repo-relative path for name
                let name = if let Some(ref relp) = rel {
                    let mut full = relp.clone();
                    full.push(entry.name().unwrap_or("<non-utf8>"));
                    full.to_string_lossy().into_owned()
                } else {
                    entry.name().unwrap_or("<non-utf8>").to_string()
                };

                return Ok(ObjectAttr {
                    name,
                    oid: entry.id(),
                    kind,
                    filemode,
                    size,
                    commit_time,
                });
            }
        }

        Err(anyhow!(
            "oid {} not found directly under {}",
            oid,
            rel.as_deref()
                .map(|p| p.to_string_lossy().into_owned())
                .unwrap_or_else(|| "<root>".into())
        ))
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

        // Treat the intentional abort as success; propagate anything else
        if let Err(e) = walk_res {
            if !(e.class() == ErrorClass::Callback && e.code() == ErrorCode::User) {
                return Err(e.into());
            }
        }
        found.ok_or_else(|| anyhow!("oid {} not found in commit {}", oid, commit_obj.id()))
    }

    pub fn read_log(&self) -> anyhow::Result<Vec<ObjectAttr>> {
        let limit = 20;
        let head = self.inner.head()?;
        let head_commit = head.peel_to_commit()?;

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
            let kind = entry.kind().unwrap();
            let filemode = entry.filemode() as u32;
            let name = entry.name().unwrap().to_string();
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

    fn head_commit(&self) -> anyhow::Result<Commit> {
        let repo = &self.inner;
        Ok(repo.head()?.peel_to_commit()?)
    }

    fn head_tree(&self) -> anyhow::Result<Tree> {
        Ok(self.head_commit()?.tree()?)
    }
}

/// If the name supplier follows the format:
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
        bail!("Incorrect url format!");
    }
    let website = comp
        .next()
        .ok_or_else(|| anyhow!("Error getting website from url"))?;
    let account = comp
        .next()
        .ok_or_else(|| anyhow!("Error getting account from url"))?;
    let repo = comp
        .next()
        .ok_or_else(|| anyhow!("Error gettingrepo name from url"))?;
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
