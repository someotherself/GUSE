use std::ffi::OsStr;

use anyhow::{Context, anyhow};

use crate::{
    fs::{FileType, REPO_SHIFT},
    mount::dir_attr,
    test_setup::{TestSetup, get_fs, run_test},
};

use crate::fs::ROOT_INO;
const REPO_DIR_INO: u64 = (1 as u64) << REPO_SHIFT;
const LIVE_DIR_INO: u64 = REPO_DIR_INO + 1;

#[test]
fn test_mkdir_fetch() -> anyhow::Result<()> {
    run_test(
        TestSetup {
            key: "test_mkdir_fetch",
            read_only: false,
        },
        |_| -> anyhow::Result<()> {
            let fs = get_fs();
            let mut fs = fs.lock().unwrap();

            let create_attr = dir_attr();
            // let name = OsStr::new("github.tokio-rs.mio.git");
            let name = OsStr::new("github.someotherself.git_rust.git");
            fs.mkdir(ROOT_INO, name, create_attr)?;

            // // GET ATTR ROOT
            // let root_attr = fs.getattr(ROOT_INO)?;
            // assert_eq!(root_attr.inode, ROOT_INO);
            // assert_eq!(root_attr.kind, FileType::Directory);

            // READ DIR ROOT
            let read_dir_root = fs.readdir(ROOT_INO)?;
            for node in &read_dir_root {
                dbg!(&node.name);
            }
            assert_eq!(read_dir_root.len(), 1);
            assert_eq!(read_dir_root[0].inode, REPO_DIR_INO);
            assert_eq!(read_dir_root[0].name, "git_rust");

            // FIND BY NAME ROOT
            let mio_attr = fs.find_by_name(ROOT_INO, "git_rust")?;
            assert!(mio_attr.is_some());
            let mio_attr = mio_attr.unwrap();
            assert_eq!(mio_attr.inode, REPO_DIR_INO);

            // GET ATTR REPO_DIR
            let repo_attr = fs.getattr(REPO_DIR_INO)?;
            assert_eq!(repo_attr.inode, REPO_DIR_INO);
            assert_eq!(repo_attr.kind, FileType::Directory);

            // READ DIR REPO_DIR
            let read_dir_repo = fs.readdir(REPO_DIR_INO)?;
            assert_eq!(read_dir_repo[0].name, "live");
            assert_eq!(read_dir_repo.len(), 21);
            let snap_1_parent = &read_dir_repo[1];
            let snap_1_parent_name: &String = &snap_1_parent.name;
            let parent_snap_attr = fs.find_by_name(REPO_DIR_INO, snap_1_parent_name)?.unwrap();
            let parent_snap_ino = parent_snap_attr.inode;

            for a in fs.readdir(parent_snap_ino)? {
                let a_attr_1 = fs.getattr(a.inode)?;
                assert_eq!(a.inode, a_attr_1.inode);
                if a.kind == FileType::Directory {
                    let a_attr = fs
                        .find_by_name(parent_snap_ino, &a.name)?
                        .with_context(|| anyhow!(""))?;
                    assert_eq!(a.inode, a_attr.inode);

                    for b in fs.readdir(a_attr.inode)? {
                        let b_attr_1 = fs.getattr(b.inode)?;
                        assert_eq!(b.inode, b_attr_1.inode);
                        if b.kind == FileType::Directory {
                            let b_attr = fs
                                .find_by_name(a_attr.inode, &b.name)?
                                .with_context(|| anyhow!(""))?;
                            assert_eq!(b.inode, b_attr.inode);
                            for c in fs.readdir(b_attr.inode)? {
                                let c_attr_1 = fs.getattr(c.inode)?;
                                assert_eq!(c.inode, c_attr_1.inode);
                                if c.kind == FileType::Directory {
                                    let _c_attr = fs.getattr(c.inode)?;
                                    let c_attr = fs
                                        .find_by_name(b_attr.inode, &c.name)?
                                        .with_context(|| anyhow!(""))?;
                                    assert_eq!(c.inode, c_attr.inode);
                                }
                            }
                        }
                    }
                }
            }

            // FIND BY NAME REPO_DIR
            let live_attr = fs.find_by_name(REPO_DIR_INO, "live")?;
            assert!(live_attr.is_some());
            let live_attr = live_attr.unwrap();
            assert_eq!(live_attr.inode, LIVE_DIR_INO);

            assert_eq!(read_dir_root[0].name, "git_rust");
            let parent_for_mio = {
                let repo = fs.get_repo(read_dir_root[0].inode)?;
                let repo = repo.lock().unwrap();
                repo.connection
                    .lock()
                    .unwrap()
                    .get_parent_ino(read_dir_root[0].inode)?
            };
            assert_eq!(parent_for_mio, ROOT_INO);

            // GET ATTR LIVE_DIR
            let live_attr = fs.getattr(LIVE_DIR_INO)?;
            assert_eq!(live_attr.inode, LIVE_DIR_INO);
            assert_eq!(live_attr.kind, FileType::Directory);

            // READ DIR LIVE
            let read_dir_live = fs.readdir(LIVE_DIR_INO)?;
            assert_eq!(read_dir_live.len(), 0);

            Ok(())
        },
    )?;
    Ok(())
}

// #[test]
// fn test_mkdir_normal() -> anyhow::Result<()> {
//     run_test(
//         TestSetup {
//             key: "test_mkdir_normal",
//             read_only: false,
//         },
//         |_| -> anyhow::Result<()> {
//             let fs = get_fs();
//             let mut fs = fs.lock().unwrap();

//             let create_attr = dir_attr();
//             let name = OsStr::new("new_folder");
//             fs.mkdir(ROOT_INO, name, create_attr)?;

//             let root_attr = fs.getattr(ROOT_INO)?;
//             assert_eq!(root_attr.inode, ROOT_INO);

//             let repo_attr = fs.getattr(REPO_DIR_INO)?;
//             assert_eq!(repo_attr.inode, REPO_DIR_INO);

//             // FIND BY NAME
//             let live_attr = fs.find_by_name(REPO_DIR_INO, "live")?;
//             assert!(live_attr.is_some());
//             let live_attr = live_attr.unwrap();
//             assert_eq!(live_attr.inode, LIVE_DIR_INO);

//             // READ DIR
//             let read_dir = fs.readdir(ROOT_INO)?;
//             assert_eq!(read_dir.len(), 1);

//             assert_eq!(read_dir[0].name, "new_folder");

//             let read_dir = fs.readdir(REPO_DIR_INO)?;
//             assert_eq!(read_dir.len(), 1);

//             assert_eq!(read_dir[0].name, "live");

//             let create_attr = dir_attr();
//             let dir_name1 = OsStr::new("dir_in_live_1");
//             let dir1_attr = fs.mkdir(LIVE_DIR_INO, dir_name1, create_attr)?;

//             let dir1_ino = LIVE_DIR_INO + 1;
//             assert_eq!(dir1_attr.inode, dir1_ino);
//             Ok(())
//         },
//     )?;
//     Ok(())
// }
