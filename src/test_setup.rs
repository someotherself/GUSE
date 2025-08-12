use std::{
    cell::RefCell,
    path::PathBuf,
    sync::{Arc, LazyLock, Mutex},
};

use crate::fs::GitFs;

thread_local! {
    pub static SETUP_RESULT: RefCell<Option<SetupResult>> = const { RefCell::new(None) };
}

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

pub fn run_test<T>(init: TestSetup, t: T) -> anyhow::Result<()>
where
    T: Fn(Option<SetupResult>) -> anyhow::Result<()>,
{
    let setup = SETUP_RESULT.replace(Some(setup(init)));
    t(setup)?;
    std::fs::remove_dir_all(TESTS_DATA_DIR.as_path()).unwrap();
    Ok(())
}

pub fn get_fs() -> Option<Arc<Mutex<GitFs>>> {
    let fs = SETUP_RESULT.take();
    fs.unwrap().fs
}
