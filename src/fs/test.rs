use std::ffi::OsStr;

use anyhow::{Context, anyhow};

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

            // GET ATTR ROOT
            let root_attr = fs.getattr(ROOT_INO)?;
            assert_eq!(root_attr.inode, ROOT_INO);
            assert_eq!(root_attr.kind, FileType::Directory);

            // READ DIR ROOT
            let read_dir_root = fs.readdir(ROOT_INO)?;
            assert_eq!(read_dir_root.len(), 1);
            assert_eq!(read_dir_root[0].inode, REPO_DIR_INO);
            assert_eq!(read_dir_root[0].name, "mio");

            // FIND BY NAME ROOT
            let mio_attr = fs.find_by_name(ROOT_INO, "mio")?;
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
            let commit_1 = &read_dir_repo[1];
            let name: &String = &commit_1.name;
            let commit_attr = fs.find_by_name(REPO_DIR_INO, name)?.unwrap();
            assert_eq!(commit_attr.inode, read_dir_repo[1].inode);
            assert_eq!(commit_attr.kind, FileType::Directory);

            // FIND BY NAME REPO_DIR
            let live_attr = fs.find_by_name(REPO_DIR_INO, "live")?;
            assert!(live_attr.is_some());
            let live_attr = live_attr.unwrap();
            assert_eq!(live_attr.inode, LIVE_DIR_INO);

            assert_eq!(read_dir_root[0].name, "mio");
            let repo = fs.get_repo(read_dir_root[0].inode)?;
            let parent_for_mio = repo
                .connection
                .read()
                .unwrap()
                .get_parent_ino(read_dir_root[0].inode)?;
            assert_eq!(parent_for_mio, ROOT_INO);

            // GET ATTR LIVE_DIR
            let live_attr = fs.getattr(LIVE_DIR_INO)?;
            assert_eq!(live_attr.inode, LIVE_DIR_INO);
            assert_eq!(live_attr.kind, FileType::Directory);

            // READ DIR LIVE
            let read_dir_live = fs.readdir(LIVE_DIR_INO)?;
            assert_eq!(read_dir_live.len(), 0);

            // FIND BY NAME LIVE
            // no files in live

            // READ DIR - GIT_DIR
            let commit_dir_ino = commit_attr.inode;

            // level 1: entries under the commit dir
            for e1 in fs.readdir(commit_dir_ino)? {
                // verify: e1 is a direct child of commit_dir_ino
                let a1 = fs
                    .find_by_name(commit_dir_ino, &e1.name)?
                    .with_context(|| {
                        format!("missing '{}' under commit dir {}", e1.name, commit_dir_ino)
                    })?;
                assert_eq!(a1.inode, e1.inode);

                if e1.kind == FileType::Directory {
                    let d1_ino = e1.inode;

                    // level 2: entries under that dir (e.g., "examples/*")
                    for e2 in fs.readdir(d1_ino)? {
                        // parent is d1_ino here
                        let a2 = fs
                            .find_by_name(d1_ino, &e2.name)?
                            .with_context(|| format!("missing '{}' under {}", e2.name, d1_ino))?;
                        assert_eq!(a2.inode, e2.inode);

                        if e2.kind == FileType::Directory {
                            let d2_ino = e2.inode;

                            // level 3
                            for e3 in fs.readdir(d2_ino)? {
                                let a3 = fs.find_by_name(d2_ino, &e3.name)?.with_context(|| {
                                    format!("missing '{}' under {}", e3.name, d2_ino)
                                })?;
                                assert_eq!(a3.inode, e3.inode);
                            }
                        }
                    }
                }
            }

            // // GET ATTR - GIT DIR
            // let read_dir_commit_1 = fs.getattr(read_dir_commit[0].inode)?;
            // dbg!(read_dir_commit_1.inode);
            // dbg!(read_dir_commit_1.perm);

            // // FIND BY NAME - GIT DIR
            // let attr_3 = fs
            //     .find_by_name(read_dir_repo[1].inode, &read_dir_commit[0].name)
            //     .with_context(|| anyhow!("Failed to unwrap the result!"))?
            //     .with_context(|| anyhow!("Failed to unwrap the option"))?;
            // dbg!(attr_3.inode);
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

            let repo_attr = fs.getattr(REPO_DIR_INO)?;
            assert_eq!(repo_attr.inode, REPO_DIR_INO);

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
