use std::sync::{Arc, Mutex, OnceLock};

use anyhow::anyhow;
use tempfile::TempDir;
use thread_local::ThreadLocal;

use crate::fs::GitFs;

pub static SETUP_RESULT: ThreadLocal<Mutex<Option<SetupResult>>> = ThreadLocal::new();

#[derive(Debug, Clone)]
pub struct TestSetup {
    #[allow(dead_code)]
    pub key: &'static str,
    pub read_only: bool,
}

pub struct SetupResult {
    pub fs: Option<Arc<GitFs>>,
    _tmpdir: TempDir,
    setup: TestSetup,
}

fn setup(setup: TestSetup) -> SetupResult {
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

    SetupResult {
        fs: Some(fs),
        _tmpdir: tmpdir,
        setup,
    }
}

pub fn run_test<T>(init: TestSetup, t: T) -> anyhow::Result<()>
where
    T: Fn(&Mutex<Option<SetupResult>>) -> anyhow::Result<()>,
{
    let s = SETUP_RESULT.get_or(|| Mutex::new(None));
    {
        let mut s = s.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        *s = Some(setup(init));
    }
    t(s)?;

    {
        let mut guard = s.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        *guard = None;
    }

    Ok(())
}

pub fn get_fs() -> Arc<GitFs> {
    let fs = SETUP_RESULT.get_or(|| Mutex::new(None));
    let mut fs = fs.lock().unwrap();
    fs.as_mut().unwrap().fs.clone().unwrap()
}
