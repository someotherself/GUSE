use std::ffi::OsStr;

use anyhow::{anyhow, Context};

use crate::{
    fs::{FileType, REPO_SHIFT},
    mount::dir_attr,
    test_setup::{TestSetup, get_fs, run_test},
};

use crate::fs::ROOT_INO;
const REPO_DIR_INO: u64 = (1 as u64) << REPO_SHIFT;
const LIVE_DIR_INO: u64 = ((1 as u64) << REPO_SHIFT) + 1;

#[test]
fn test_mkdir_fetch() -> anyhow::Result<()> {
    run_test(
        TestSetup {
            key: "test_mkdir_fetch",
            read_only: false,
        },
        |_| -> anyhow::Result<()> {
            let fs = get_fs().unwrap();
            let mut fs = fs.lock().unwrap();

            let create_attr = dir_attr();
            let name = OsStr::new("github.tokio-rs.mio.git");
            fs.mkdir(1, name, create_attr)?;

            let root_attr = fs.getattr(ROOT_INO)?;
            assert_eq!(root_attr.inode, ROOT_INO);
            assert_eq!(root_attr.kind, FileType::Directory);

            let repo_attr = fs.getattr(REPO_DIR_INO)?;
            assert_eq!(repo_attr.inode, REPO_DIR_INO);
            assert_eq!(repo_attr.kind, FileType::Directory);

            let live_attr = fs.getattr(LIVE_DIR_INO)?;
            assert_eq!(live_attr.inode, LIVE_DIR_INO);
            assert_eq!(live_attr.kind, FileType::Directory);

            let mio_attr = fs.find_by_name(ROOT_INO, "mio")?;
            assert!(mio_attr.is_some());
            let mio_attr = mio_attr.unwrap();
            assert_eq!(mio_attr.inode, REPO_DIR_INO);

            // FIND BY NAME
            let live_attr = fs.find_by_name(REPO_DIR_INO, "live")?;
            assert!(live_attr.is_some());
            let live_attr = live_attr.unwrap();
            assert_eq!(live_attr.inode, LIVE_DIR_INO);

            // READ DIR - ROOT
            let read_dir_root = fs.readdir(ROOT_INO)?;
            assert_eq!(read_dir_root.len(), 1);
            dbg!(&read_dir_root[0].name);
            dbg!(&read_dir_root[0].inode);

            assert_eq!(read_dir_root[0].name, "mio");
            let repo = fs.get_repo(read_dir_root[0].inode)?;
            let parent_for_mio = repo
                .connection
                .read()
                .unwrap()
                .get_parent_ino(read_dir_root[0].inode)?;
            dbg!(parent_for_mio);

            // READ DIR - REPO_DIR
            let read_dir = fs.readdir(REPO_DIR_INO)?;
            assert_eq!(read_dir[0].name, "live");
            assert_eq!(read_dir.len(), 21);
            let commit_1 = &read_dir[1];
            dbg!(commit_1.inode);
            let name: &String = &commit_1.name;
            let commit_attr = fs.find_by_name(REPO_DIR_INO, name)?.unwrap();
            assert_eq!(commit_attr.inode, read_dir[1].inode);
            assert_eq!(commit_attr.kind, FileType::Directory);

            // READ DIR LIVE
            let read_dir_live = fs.readdir(LIVE_DIR_INO)?;
            dbg!(read_dir_live.len());

            // READ DIR - Commit
            let read_dir_commit = fs.readdir(commit_attr.inode)?;
            dbg!(read_dir_commit.len());

            // let commit_attr =  fs.find_by_name(REPO_DIR_INO, &read_dir_commit[0].name).with_context(|| format!("while finding attr for {name}"))?
            //                                 .with_context(|| format!("no entry named {name} in repo dir"))?;
            // let read_dir_into_commit = fs.readdir(com)

            Ok(())
        },
    )?;
    Ok(())
}

#[test]
fn test_mkdir_normal() -> anyhow::Result<()> {
    run_test(
        TestSetup {
            key: "test_mkdir_normal",
            read_only: false,
        },
        |_| -> anyhow::Result<()> {
            let fs = get_fs().unwrap();
            let mut fs = fs.lock().unwrap();

            let create_attr = dir_attr();
            let name = OsStr::new("new_folder");
            fs.mkdir(ROOT_INO, name, create_attr)?;

            let root_attr = fs.getattr(ROOT_INO)?;
            assert_eq!(root_attr.inode, ROOT_INO);
            dbg!(root_attr.kind);

            let repo_attr = fs.getattr(REPO_DIR_INO)?;
            assert_eq!(repo_attr.inode, REPO_DIR_INO);
            dbg!(repo_attr.kind);

            // FIND BY NAME
            let live_attr = fs.find_by_name(REPO_DIR_INO, "live")?;
            assert!(live_attr.is_some());
            let live_attr = live_attr.unwrap();
            assert_eq!(live_attr.inode, LIVE_DIR_INO);

            // READ DIR
            let read_dir = fs.readdir(ROOT_INO)?;
            assert_eq!(read_dir.len(), 1);

            assert_eq!(read_dir[0].name, "new_folder");

            let read_dir = fs.readdir(REPO_DIR_INO)?;
            assert_eq!(read_dir.len(), 1);

            assert_eq!(read_dir[0].name, "live");

            let create_attr = dir_attr();
            let dir_name1 = OsStr::new("dir_in_live_1");
            let dir1_attr = fs.mkdir(LIVE_DIR_INO, dir_name1, create_attr)?;

            let dir1_ino = LIVE_DIR_INO + 1;
            assert_eq!(dir1_attr.inode, dir1_ino);
            Ok(())
        },
    )?;
    Ok(())
}
