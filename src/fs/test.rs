use std::ffi::OsStr;

use crate::{
    fs::REPO_SHIFT,
    mount::dir_attr,
    test_setup::{TestSetup, get_fs, run_test},
};

use crate::fs::ROOT_INO;
const REPO_DIR_INO: u64 = (1 as u64) << REPO_SHIFT;
const LIVE_DIR_INO: u64 = ((1 as u64) << REPO_SHIFT) + 1;

#[test]
fn test_mkdir_fetch() {
    run_test(
        TestSetup {
            key: "test_mkdir_fetch",
            read_only: false,
        },
        |_| {
            let fs = get_fs().unwrap();
            let mut fs = fs.lock().unwrap();

            let create_attr = dir_attr();
            let name = OsStr::new("github.tokio-rs.mio.git");
            fs.mkdir(1, name, create_attr).unwrap();

            let root_attr = fs.getattr(ROOT_INO).unwrap();
            assert_eq!(root_attr.inode, ROOT_INO);
            dbg!(root_attr.kind);

            let repo_attr = fs.getattr(REPO_DIR_INO).unwrap();
            assert_eq!(repo_attr.inode, REPO_DIR_INO);
            dbg!(repo_attr.kind);

            let live_attr = fs.getattr(LIVE_DIR_INO).unwrap();
            assert_eq!(live_attr.inode, LIVE_DIR_INO);
            dbg!(live_attr.kind);

            let mio_attr = fs.find_by_name(ROOT_INO, "mio").unwrap();
            assert!(mio_attr.is_some());
            let mio_attr = mio_attr.unwrap();
            assert_eq!(mio_attr.inode, REPO_DIR_INO);

            // FIND BY NAME
            let live_attr = fs.find_by_name(REPO_DIR_INO, "live").unwrap();
            assert!(live_attr.is_some());
            let live_attr = live_attr.unwrap();
            assert_eq!(live_attr.inode, LIVE_DIR_INO);

            // READ DIR
            let read_dir = fs.readdir(ROOT_INO).unwrap();
            assert_eq!(read_dir.len(), 1);

            assert_eq!(read_dir[0].name, "mio");

            let read_dir = fs.readdir(REPO_DIR_INO).unwrap();
            assert_eq!(read_dir[0].name, "live");
            for node in read_dir {
                dbg!(node.name);
            }
            // assert_eq!(read_dir.len(), 1);
        },
    );
}

#[test]
fn test_mkdir_normal() {
    run_test(
        TestSetup {
            key: "test_mkdir_normal",
            read_only: false,
        },
        |_| {
            let fs = get_fs().unwrap();
            let mut fs = fs.lock().unwrap();

            let create_attr = dir_attr();
            let name = OsStr::new("new_folder");
            fs.mkdir(ROOT_INO, name, create_attr).unwrap();

            let root_attr = fs.getattr(ROOT_INO).unwrap();
            assert_eq!(root_attr.inode, ROOT_INO);
            dbg!(root_attr.kind);

            let repo_attr = fs.getattr(REPO_DIR_INO).unwrap();
            assert_eq!(repo_attr.inode, REPO_DIR_INO);
            dbg!(repo_attr.kind);

            // FIND BY NAME
            let live_attr = fs.find_by_name(REPO_DIR_INO, "live").unwrap();
            assert!(live_attr.is_some());
            let live_attr = live_attr.unwrap();
            assert_eq!(live_attr.inode, LIVE_DIR_INO);

            // READ DIR
            let read_dir = fs.readdir(ROOT_INO).unwrap();
            assert_eq!(read_dir.len(), 1);

            assert_eq!(read_dir[0].name, "new_folder");

            let read_dir = fs.readdir(REPO_DIR_INO).unwrap();
            assert_eq!(read_dir.len(), 1);

            assert_eq!(read_dir[0].name, "live");

            let create_attr = dir_attr();
            let dir_name1 = OsStr::new("dir_in_live_1");
            let dir1_attr = fs.mkdir(LIVE_DIR_INO, dir_name1, create_attr).unwrap();

            let dir1_ino = LIVE_DIR_INO + 1;
            assert_eq!(dir1_attr.inode, dir1_ino);
        },
    );
}
