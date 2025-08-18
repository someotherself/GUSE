use crate::fs::fileattr::FileAttr;
use crate::fs::{CreateFileAttr, FsError, FsResult, GitFs, MyBacktrace, REPO_SHIFT, repo};

pub fn mkdir_root(
    fs: &mut GitFs,
    _parent: u64,
    name: &str,
    _create_attr: CreateFileAttr,
) -> FsResult<FileAttr> {
    match repo::parse_mkdir_url(name)? {
        Some((url, repo_name)) => {
            println!("fetching repo {}", &repo_name);
            let repo = fs.new_repo(&repo_name)?;
            {
                let mut repo = repo.lock().map_err(|_| FsError::LockPoisoned)?;
                repo.fetch_anon(&url)?;
                repo.refresh_snapshots()?;
            }
            let repo_id = {
                let repo = repo.lock().map_err(|_| FsError::LockPoisoned)?;
                repo.repo_id
            };
            let attr = fs.getattr((repo_id as u64) << REPO_SHIFT)?;
            Ok(attr)
        }
        None => {
            println!("Creating repo {name}");
            let repo_id = {
                let repo = fs.new_repo(name)?;
                let repo = repo.lock().map_err(|_| FsError::LockPoisoned)?;
                repo.repo_id
            };
            let attr = fs.getattr((repo_id as u64) << REPO_SHIFT)?;

            Ok(attr)
        }
    }
}

pub fn mkdir_repo(
    _fs: &GitFs,
    _parent: u64,
    _name: &str,
    _create_attr: CreateFileAttr,
) -> FsResult<FileAttr> {
    Err(FsError::Internal("This directory is read only".to_string()))
}

pub fn mkdir_live(
    fs: &GitFs,
    parent: u64,
    name: &str,
    create_attr: CreateFileAttr,
) -> FsResult<FileAttr> {
    if fs.exists_by_name(parent, name)? {
        return Err(FsError::AlreadyExists);
    }

    let dir_path = fs.build_path(parent, name)?;
    std::fs::create_dir(dir_path).map_err(|s| FsError::Io {
        source: s,
        my_backtrace: MyBacktrace::capture(),
    })?;

    let new_ino = fs.next_inode(parent)?;

    let mut attr: FileAttr = create_attr.into();

    attr.inode = new_ino;

    let nodes = vec![(parent, name.into(), attr)];
    fs.write_inodes_to_db(nodes)?;

    Ok(attr)
}

pub fn mkdir_git(
    _fs: &GitFs,
    _parent: u64,
    _name: &str,
    _create_attr: CreateFileAttr,
) -> FsResult<FileAttr> {
    Err(FsError::Internal("This directory is read only".to_string()))
}
