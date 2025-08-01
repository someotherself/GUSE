use anyhow::anyhow;
use git2::{Oid, Repository, Tree};

use std::path::Path;

use crate::fs::{DirectoryEntry, DirectoryEntryPlus, FileType, GitFs};

pub struct GitRepo {
    inner: Repository,
    head: Oid,
}

impl GitRepo {
    pub fn new() -> Self {
        todo!()
    }

    pub fn open<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let repo = Repository::open(path)?;
        let head = repo.revparse_single("HEAD")?.id();

        Ok(GitRepo { inner: repo, head })
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
