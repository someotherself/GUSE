use anyhow::anyhow;
use git2::Oid;

use crate::{
    fs::{
        FileAttr, GitFs,
        fileattr::{dir_attr, file_attr},
    },
    inodes::NormalIno,
};

pub struct GetAttrOperationCtx {
    ino: NormalIno,
    object_oid: Oid,
    parent_commit: Oid,
    snap_name: String,
}

impl GetAttrOperationCtx {
    pub fn new(fs: &GitFs, ino: NormalIno) -> anyhow::Result<Self> {
        let oid = fs.get_oid_from_db(ino.to_norm_u64())?;

        let (parent_commit, snap_name) = match fs.get_parent_commit(ino.to_norm_u64()) {
            Ok((p, s)) => (p, s),
            Err(_) => {
                // Ino is the snap folder. Use the oid and name of the ino
                let name = fs.get_name_from_db(ino.to_norm_u64())?;
                (oid, name)
            }
        };

        Ok(Self {
            ino,
            object_oid: oid,
            parent_commit,
            snap_name,
        })
    }

    pub fn is_git_object(&self) -> bool {
        self.object_oid != Oid::zero()
    }

    pub fn is_snap_commit_root(&self) -> bool {
        self.object_oid == self.parent_commit
    }

    pub fn oid(&self) -> Oid {
        self.object_oid
    }
}

pub fn getattr_live_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<FileAttr> {
    let ino = u64::from(ino);
    let filemode = fs.get_mode_from_db(ino)?;
    let mut attr: FileAttr = match filemode {
        git2::FileMode::Tree => dir_attr().into(),
        git2::FileMode::Commit => dir_attr().into(),
        _ => file_attr().into(),
    };
    attr.ino = ino;
    let attr = fs.refresh_attr(&mut attr)?;
    Ok(attr)
}

pub fn getattr_git_dir(fs: &GitFs, ino: NormalIno) -> anyhow::Result<FileAttr> {
    let ctx = GetAttrOperationCtx::new(fs, ino)?;

    if !ctx.is_git_object() {
        let mut attr: FileAttr = dir_attr().into();
        attr.ino = ino.to_norm_u64();
        return Ok(attr);
    }

    let repo = fs.get_repo(ino.to_norm_u64())?;
    let gitdir_commit_id = {
        let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        repo.inner.find_commit(ctx.parent_commit)?.id()
    };

    if ctx.is_snap_commit_root() {
        let commit_attr = {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.attr_from_snap(gitdir_commit_id, &ctx.snap_name)?
        };
        let attr = fs.object_to_file_attr(ino.to_norm_u64(), &commit_attr)?;
        Ok(attr)
    } else {
        let git_attr = {
            let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            repo.find_in_commit(gitdir_commit_id, ctx.object_oid)?
        };
        let attr = fs.object_to_file_attr(ino.to_norm_u64(), &git_attr)?;

        Ok(attr)
    }
}
