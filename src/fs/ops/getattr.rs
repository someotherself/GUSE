use anyhow::anyhow;

use crate::{
    fs::{
        FileAttr, GitFs,
        fileattr::{dir_attr, file_attr},
        ops::lookup::{AttrOperationCtx, TargetAttr},
    },
    inodes::NormalIno,
};

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
    let ctx = AttrOperationCtx::get_target(fs, ino)?;

    match ctx {
        TargetAttr::Month(_) | TargetAttr::Snap(_) => {
            let mut attr: FileAttr = dir_attr().into();
            attr.ino = ino.to_norm_u64();
            Ok(attr)
        }
        TargetAttr::Build(ctx) => {
            let mut attr = fs.attr_from_path(ctx.path())?;
            attr.ino = ino.to_norm_u64();
            Ok(attr)
        }
        TargetAttr::InsideCommit(ctx) => {
            let oid = fs.get_oid_from_db(ino.to_norm_u64())?;
            let object_attr = {
                let repo = fs.get_repo(ino.to_norm_u64())?;
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.find_in_commit(ctx.parent_commit(), oid)?
            };
            let mut attr = fs.object_to_file_attr(ino.to_norm_u64(), &object_attr)?;
            attr.ino = ino.to_norm_u64();
            Ok(attr)
        }
    }
}
