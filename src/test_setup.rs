use std::sync::{Arc, Mutex, OnceLock};

use anyhow::anyhow;
use tempfile::TempDir;
use thread_local::ThreadLocal;

use crate::fs::GitFs;

pub static GITFS_SETUP_RESULT: ThreadLocal<Mutex<Option<GitFsSetupResult>>> = ThreadLocal::new();

#[derive(Debug, Clone)]
pub struct GitFsTestSetup {
    #[allow(dead_code)]
    pub key: &'static str,
    pub read_only: bool,
}

pub struct GitFsSetupResult {
    pub fs: Option<Arc<GitFs>>,
    _tmpdir: TempDir,
}

fn git_fs_setup(setup: GitFsTestSetup) -> GitFsSetupResult {
    let tmpdir = tempfile::Builder::new()
        .prefix(setup.key)
        .tempdir()
        .expect("could not create tmpdir");

    let fs = GitFs::new(
        tmpdir.path().to_path_buf(),
        setup.read_only,
        Arc::new(OnceLock::new()),
    )
    .expect("failed to init GitFs");

    GitFsSetupResult {
        fs: Some(fs),
        _tmpdir: tmpdir,
    }
}

pub fn run_git_fs_test<T>(init: GitFsTestSetup, t: T) -> anyhow::Result<()>
where
    T: Fn(&Mutex<Option<GitFsSetupResult>>) -> anyhow::Result<()>,
{
    let s = GITFS_SETUP_RESULT.get_or(|| Mutex::new(None));
    {
        let mut s = s.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        *s = Some(git_fs_setup(init));
    }
    t(s)?;

    {
        let mut guard = s.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        *guard = None;
    }

    Ok(())
}

pub fn get_fs() -> Arc<GitFs> {
    let fs = GITFS_SETUP_RESULT.get_or(|| Mutex::new(None));
    let mut fs = fs.lock().unwrap();
    fs.as_mut().unwrap().fs.clone().unwrap()
}
