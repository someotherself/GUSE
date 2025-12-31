use std::{
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use parking_lot::Mutex;
use tempfile::TempDir;

use crate::fs::GitFs;

thread_local! {
    pub static GITFS_SETUP_RESULT: Mutex<Option<SetupResult>> = const { Mutex::new(None) };
}

#[derive(Debug, Clone)]
pub struct GitFsTestSetup {
    #[allow(dead_code)]
    pub key: &'static str,
    pub read_only: bool,
}

pub struct SetupResult {
    pub fs: Option<Arc<GitFs>>,
    _tmpdir: TempDir,
}

fn git_fs_setup(setup: GitFsTestSetup) -> SetupResult {
    let tmpdir = tempfile::Builder::new()
        .prefix(setup.key)
        .tempdir()
        .expect("could not create tmpdir");

    let fs = GitFs::new(
        tmpdir.path().to_path_buf(),
        PathBuf::new(),
        setup.read_only,
        Arc::new(OnceLock::new()),
    )
    .expect("failed to init GitFs");

    SetupResult {
        fs: Some(fs),
        _tmpdir: tmpdir,
    }
}

pub fn run_git_fs_test<T>(init: GitFsTestSetup, t: T) -> anyhow::Result<()>
where
    T: Fn(&Mutex<Option<SetupResult>>) -> anyhow::Result<()>,
{
    GITFS_SETUP_RESULT.with(|s| -> anyhow::Result<()> {
        {
            let mut guard = s.lock();
            *guard = Some(git_fs_setup(init));
        }

        t(s)?;

        {
            let mut guard = s.lock();
            *guard = None;
        }

        Ok(())
    })?;

    Ok(())
}

pub fn get_fs() -> Arc<GitFs> {
    GITFS_SETUP_RESULT.with(|m| {
        let mut guard = m.lock();
        guard.as_mut().unwrap().fs.clone().unwrap()
    })
}
