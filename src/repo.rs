use anyhow::anyhow;
use git2::{ObjectType, Oid, Repository, Tree};

use std::path::Path;

use crate::fs::{DirectoryEntry, DirectoryEntryPlus, FileType, GitFs, ObjectAttr};

pub struct GitRepo {
    pub repo_id: u16,
    pub inner: Repository,
    pub head: Oid,
}

impl GitRepo {
    pub fn new() -> Self {
        todo!()
    }

    pub fn open<P: AsRef<Path>>(path: P, repo_id: u16) -> anyhow::Result<Self> {
        // TODO: read root folder and get repo_id from the inode
        let repo = Repository::open(path)?;
        let head = repo.revparse_single("HEAD")?.id();

        Ok(GitRepo {
            repo_id,
            inner: repo,
            head,
        })
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
                            filemode: entry.filemode(),
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
            filemode: 0o040000,
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
            let inode = fs.get_ino_from_db(tree_inode, &name)?;
            entries.push(DirectoryEntry {
                name,
                inode,
                oid: entry.id(),
                filemode: entry.filemode(),
                kind: FileType::from_filemode(entry.kind().unwrap())?,
            });
        }
        Ok(entries)
    }

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
}

impl PartialEq for GitRepo {
    fn eq(&self, other: &Self) -> bool {
        self.repo_id == other.repo_id
    }
}
