#![allow(unused_imports)]

use anyhow::{Context, anyhow, bail};
use git2::{
    Commit, Direction, FetchOptions, ObjectType, Oid, Reference, RemoteCallbacks, Repository, Sort,
    Tree,
};
use std::{
    ffi::OsStr,
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

        // Discover the remote's default branch (e.g., "refs/heads/main")
        remote.connect(Direction::Fetch)?;
        let default_branch = remote.default_branch().ok(); // Optional but nice
        remote.disconnect()?;

        // Build refspecs to create remote-tracking refs under refs/remotes/anon/*
        // Force-update (+) so repeated runs work.
        let mut refspecs = vec![
            "+refs/heads/*:refs/remotes/anon/*".to_string(),
            "+refs/tags/*:refs/tags/*".to_string(),
        ];
        if let Some(ref buf) = default_branch {
            if let Ok(src) = std::str::from_utf8(buf.as_ref()) {
                // Also fetch the remote's HEAD explicitly into refs/remotes/anon/HEAD
                refspecs.push(format!(
                    "+{}:refs/remotes/anon/{}",
                    src,
                    src.rsplit('/').next().unwrap()
                ));
                refspecs.push("+HEAD:refs/remotes/anon/HEAD".to_string());
            }
        } else {
            // Still add a HEAD mapping; some servers expose it
            refspecs.push("+HEAD:refs/remotes/anon/HEAD".to_string());
        }
        let refs_as_str: Vec<&str> = refspecs.iter().map(|s| s.as_str()).collect();

        // High-level fetch does download + update_tips for provided refspecs
        remote
            .fetch(&refs_as_str, Some(&mut fo), None)
            .context("anonymous fetch failed")?;

        // Ensure HEAD exists and is usable (no unpacking; just refs)
        // Prefer the discovered default branch under refs/remotes/anon/*
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
        // Get the root tree
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
    pub fn list_tree(
        &self,
        fs: &GitFs,
        tree_oid: Oid,
        tree_inode: u64,
    ) -> anyhow::Result<Vec<DirectoryEntry>> {
        let tree: Tree = self.inner.find_tree(tree_oid)?;
        let mut entries = Vec::with_capacity(tree.len());
        for entry in tree.iter() {
            let name = entry.name().unwrap_or("").to_string();
            let conn = fs.open_meta_db(&self.repo_dir)?;
            let inode = conn.get_ino_from_db(tree_inode, &name)?;
            entries.push(DirectoryEntry {
                name,
                inode,
                oid: entry.id(),
                filemode: entry.filemode() as u32,
                kind: FileType::from_filemode(entry.kind().unwrap())?,
            });
        }
        Ok(entries)
    }

    // read_dir plus
    pub fn list_tree_plus(
        &self,
        fs: &GitFs,
        tree_oid: Oid,
        tree_inode: u64,
    ) -> anyhow::Result<Vec<DirectoryEntryPlus>> {
        let list_tree = self.list_tree(fs, tree_oid, tree_inode)?;
        let mut list_tree_plus: Vec<DirectoryEntryPlus> = Vec::new();
        for entry in list_tree {
            let attr = fs.find_by_name(tree_inode, &entry.name)?.ok_or_else(|| {
                anyhow!(
                    "no entry named {:?} in tree inode {}",
                    entry.name,
                    tree_inode
                )
            })?;
            list_tree_plus.push(DirectoryEntryPlus { entry, attr });
        }
        Ok(list_tree_plus)
    }

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
                // Use the commit summary as a human-friendly "name"
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
