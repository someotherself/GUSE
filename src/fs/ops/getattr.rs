use anyhow::anyhow;

use crate::{
    fs::{
        FileAttr, GitFs,
        fileattr::{dir_attr, file_attr},
        ops::lookup::AttrOperationCtx,
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
    let ctx = AttrOperationCtx::new(fs, ino)?;

    if ctx.is_month() {
        let mut attr: FileAttr = dir_attr().into();
        attr.ino = ino.to_norm_u64();
        return Ok(attr);
    }

    let oid = fs.get_oid_from_db(ino.to_norm_u64())?;
    let object_attr_res = {
        let repo = fs.get_repo(ino.to_norm_u64())?;
        let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        repo.find_in_commit(ctx.parent_commit(), oid)
    };

    let mut attr = match object_attr_res {
        Ok(a) => fs.object_to_file_attr(ino.to_norm_u64(), &a)?,
        Err(_) => {
            let filemode = fs.get_mode_from_db(ino.to_norm_u64())?;
            match filemode {
                git2::FileMode::Tree => dir_attr().into(),
                git2::FileMode::Commit => dir_attr().into(),
                _ => file_attr().into(),
            }
        }
    };
    attr.ino = ino.to_norm_u64();
    Ok(attr)
}
