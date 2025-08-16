use std::{
    path::PathBuf,
    sync::{Arc, LazyLock, Mutex},
};

use thread_local::ThreadLocal;

use crate::fs::{FsError, FsResult, GitFs};

pub static SETUP_RESULT: ThreadLocal<Mutex<Option<SetupResult>>> = ThreadLocal::new();

pub static TESTS_DATA_DIR: LazyLock<PathBuf> = LazyLock::new(|| {
    std::env::current_dir()
        .unwrap_or(PathBuf::from("."))
        .join("Test_dir")
});

#[derive(Debug, Clone)]
pub struct TestSetup {
    #[allow(dead_code)]
    pub key: &'static str,
    pub read_only: bool,
}

pub struct SetupResult {
    pub fs: Option<Arc<Mutex<GitFs>>>,
    setup: TestSetup,
}

fn setup(setup: TestSetup) -> SetupResult {
    let path = TESTS_DATA_DIR.join(setup.key);
    let read_only = setup.read_only;
    let data_dir_str = path.to_str().unwrap();
    let _ = std::fs::remove_dir_all(data_dir_str);
    let _ = std::fs::create_dir_all(data_dir_str);

    let fs = GitFs::new(data_dir_str.into(), read_only).unwrap();

    SetupResult {
        fs: Some(fs),
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
    std::fs::remove_dir_all(TESTS_DATA_DIR.as_path()).unwrap();
    Ok(())
}

pub fn get_fs() -> Arc<Mutex<GitFs>> {
    let fs = SETUP_RESULT.get_or(|| Mutex::new(None));
    let mut fs = fs.lock().unwrap();
    fs.as_mut().unwrap().fs.clone().unwrap()
}
