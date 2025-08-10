use std::ffi::OsStr;

use crate::{
    fs::REPO_SHIFT,
    mount::dir_attr,
    test_setup::{TestSetup, get_fs, run_test},
};

const ROOT_INO: u64 = 1;
const REPO_DIR_INO: u64 = (1 as u64) << REPO_SHIFT;
const LIVE_DIR_INO: u64 = ((1 as u64) << REPO_SHIFT) + 1;

#[test]
fn test_initialization() {
    run_test(
        TestSetup {
            key: "test_initialization",
            read_only: false,
        },
        |_| {
            let fs = get_fs().unwrap();
            let mut fs = fs.lock().unwrap();
            dbg!(&fs.repos_dir);

            let create_attr = dir_attr();
            let name = OsStr::new("github.tokio-rs.mio.git");
            fs.mkdir(1, name, create_attr).unwrap();

            let root_attr = fs.getattr(ROOT_INO).unwrap();
            dbg!(root_attr.inode);
            dbg!(root_attr.kind);

            let repo_attr = fs.getattr(REPO_DIR_INO).unwrap();
            dbg!(repo_attr.inode);
            dbg!(repo_attr.kind);

            let live_attr = fs.getattr(LIVE_DIR_INO).unwrap();
            dbg!(live_attr.inode);
            dbg!(live_attr.kind);
        },
    );
}
