use crate::fs::{FileAttr, GitFs};

impl GitFs {
    pub fn getattr_live_dir(&self, ino: u64) -> anyhow::Result<FileAttr> {
        let path = self.build_full_path(ino)?;
        let mut attr: FileAttr = self.attr_from_dir(path)?;
        attr.inode = ino;
        Ok(attr)
    }

    pub fn getattr_git_dir(&self, ino: u64) -> anyhow::Result<FileAttr> {
        let repo = self.get_repo(ino)?;
        let (commit_id, snap_name) = self.find_commit_in_gitdir(ino)?;
        let oid = self.get_oid_from_db(ino)?;
        let gitdir_commit = repo.inner.find_commit(commit_id)?;
        if oid == commit_id {
            // We are looking at a commit
            let commit_attr = repo.attr_from_snap(gitdir_commit.id(), &snap_name)?;
            let attr = self.object_to_file_attr(ino, &commit_attr)?;
            Ok(attr)
        } else {
            let git_attr = repo.find_in_commit(gitdir_commit.id(), oid)?;
            let mut attr = self.object_to_file_attr(ino, &git_attr)?;
            attr.inode = ino;

            Ok(attr)
        }
    }
}
