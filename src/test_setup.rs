use std::sync::{Arc, Mutex};

use tempfile::TempDir;
use thread_local::ThreadLocal;

use crate::fs::{FsError, FsResult, GitFs};

pub static SETUP_RESULT: ThreadLocal<Mutex<Option<SetupResult>>> = ThreadLocal::new();

#[derive(Debug, Clone)]
pub struct TestSetup {
    #[allow(dead_code)]
    pub key: &'static str,
    pub read_only: bool,
}

pub struct SetupResult {
    pub fs: Option<Arc<Mutex<GitFs>>>,
    _tmpdir: TempDir,
    setup: TestSetup,
}

fn setup(setup: TestSetup) -> SetupResult {
    let tmpdir = tempfile::Builder::new()
        .prefix(setup.key)
        .tempdir()
        .expect("could not create tmpdir");

    let fs =
        GitFs::new(tmpdir.path().to_path_buf(), setup.read_only).expect("failed to init GitFs");

    SetupResult {
        fs: Some(fs),
        _tmpdir: tmpdir,
        setup,
    }
}

pub fn run_test<T>(init: TestSetup, t: T) -> FsResult<()>
where
    T: Fn(&Mutex<Option<SetupResult>>) -> FsResult<()>,
{
    let s = SETUP_RESULT.get_or(|| Mutex::new(None));
    {
        let mut s = s.lock().map_err(|_| FsError::LockPoisoned)?;
        *s = Some(setup(init));
    }
    t(s)?;

    {
        let mut guard = s.lock().map_err(|_| FsError::LockPoisoned)?;
        *guard = None;
    }

    Ok(())
}

pub fn get_fs() -> Arc<Mutex<GitFs>> {
    let fs = SETUP_RESULT.get_or(|| Mutex::new(None));
    let mut fs = fs.lock().unwrap();
    fs.as_mut().unwrap().fs.clone().unwrap()
}
