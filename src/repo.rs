#![allow(unused_imports)]

use anyhow::{Context, anyhow, bail};
use git2::{ObjectType, Oid, Repository, Tree};
use std::ffi::OsStr;
use tracing::info;

use std::{io::Write, path::Path};

use crate::fs::{DirectoryEntry, DirectoryEntryPlus, FileType, GitFs, ObjectAttr, meta_db::MetaDb};

pub struct GitRepo {
    // Caching the database connection for reads.
    // Must be refreshed after every write.
    pub connection: MetaDb,
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
        let mut callbacks = git2::RemoteCallbacks::new();
        let mut remote = self.inner.remote_anonymous(url)?;

        callbacks.sideband_progress(|data| {
            print!("remote: {}", str::from_utf8(data).unwrap());
            std::io::stdout().flush().unwrap();
            true
        });

        callbacks.update_tips(|refname, a, b| {
            if a.is_zero() {
                println!("[new]     {b:20} {refname}");
            } else {
                println!("[updated] {a:10}..{b:10} {refname}");
            }
            true
        });

        callbacks.transfer_progress(|stats| {
            if stats.received_objects() == stats.total_objects() {
                print!(
                    "Resolving deltas {}/{}\r",
                    stats.indexed_deltas(),
                    stats.total_deltas()
                );
            } else if stats.total_objects() > 0 {
                print!(
                    "Received {}/{} objects ({}) in {} bytes\r",
                    stats.received_objects(),
                    stats.total_objects(),
                    stats.indexed_objects(),
                    stats.received_bytes()
                );
            }
            std::io::stdout().flush().unwrap();
            true
        });

        let mut fo = git2::FetchOptions::new();
        fo.remote_callbacks(callbacks);
        remote.download(&[] as &[&str], Some(&mut fo))?;

        {
            let stats = remote.stats();
            if stats.local_objects() > 0 {
                println!(
                    "\rReceived {}/{} objects in {} bytes (used {} local \
                 objects)",
                    stats.indexed_objects(),
                    stats.total_objects(),
                    stats.received_bytes(),
                    stats.local_objects()
                );
            } else {
                println!(
                    "\rReceived {}/{} objects in {} bytes",
                    stats.indexed_objects(),
                    stats.total_objects(),
                    stats.received_bytes()
                );
            }
        }
        remote.disconnect()?;
        remote.update_tips(
            None,
            git2::RemoteUpdateFlags::UPDATE_FETCHHEAD,
            git2::AutotagOption::Unspecified,
            None,
        )?;

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

        for comp in path.as_ref().components() {
            let name = comp.as_os_str().to_str().unwrap();
            let is_last_comp = comp == last_comp;

            // Lookup this name in the current tree
            let entry = tree.clone();
            let entry = entry
                .get_name(name)
                .ok_or_else(|| anyhow!("component {:?} not found in tree", name))?;

            if entry.kind() == Some(ObjectType::Tree) {
                // Descend into that sub-tree for further components
                tree = self.inner.find_tree(entry.id())?;
            } else {
                // If it's the last componentn
                if is_last_comp {
                    // and a blob. return the ObjectAttr
                    if entry.kind().unwrap() == git2::ObjectType::Blob {
                        let blob = self.inner.find_blob(entry.id())?;
                        let size = blob.size() as u64;
                        // blob with mode 0o120000 will be a symlink
                        return Ok(ObjectAttr {
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
            oid: entry.id(),
            kind: entry.kind().unwrap(),
            filemode: entry.filemode() as u32,
            size: size as u64,
            commit_time,
        })
    }
}

pub fn parse_mkdir_url(name: &OsStr) -> anyhow::Result<(String, String)> {
    let name = name.to_string_lossy();
    // let git = name.strip_prefix(".git").ok_or_else(|| anyhow!("URL missing .git suffix"))?;
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
    Ok((url, repo.into()))
}

impl PartialEq for GitRepo {
    fn eq(&self, other: &Self) -> bool {
        self.repo_id == other.repo_id
    }
}
