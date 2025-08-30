use std::ffi::OsStr;

use anyhow::anyhow;
use git2::Oid;

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
            let mut fs = fs.lock().map_err(|_| anyhow!("Lock poisoned"))?;
            let create_attr = dir_attr();
            // let name = OsStr::new("github.tokio-rs.mio.git");
            let name = OsStr::new("github.someotherself.git_rust.git");
            fs.mkdir(ROOT_INO, name, create_attr)?;

            // GET ATTR ROOT
            let root_attr = fs.getattr(ROOT_INO)?;
            assert_eq!(root_attr.ino, ROOT_INO);
            assert_eq!(root_attr.kind, FileType::Directory);

            // READ DIR ROOT
            let read_dir_root = fs.readdir(ROOT_INO)?;
            for node in &read_dir_root {
                dbg!(&node.name);
            }
            assert_eq!(read_dir_root.len(), 1);
            assert_eq!(read_dir_root[0].ino, REPO_DIR_INO);
            assert_eq!(read_dir_root[0].name, "git_rust");

            // FIND BY NAME ROOT
            let mio_attr = fs.lookup(ROOT_INO, "git_rust")?;
            assert!(mio_attr.is_some());
            let mio_attr = mio_attr.unwrap();
            assert_eq!(mio_attr.ino, REPO_DIR_INO);

            // GET ATTR REPO_DIR
            let repo_attr = fs.getattr(REPO_DIR_INO)?;

            assert_eq!(repo_attr.ino, REPO_DIR_INO);
            assert_eq!(repo_attr.kind, FileType::Directory);

            // READ DIR REPO_DIR
            let read_dir_repo = fs.readdir(REPO_DIR_INO)?;
            assert_eq!(read_dir_repo[0].name, "live");
            assert_eq!(read_dir_repo.len(), 3);
            let snap_1_parent = &read_dir_repo[1];
            let snap_1_parent_name: &String = &snap_1_parent.name;
            let parent_snap_attr = fs.lookup(REPO_DIR_INO, snap_1_parent_name)?.unwrap();
            let parent_snap_ino = parent_snap_attr.ino;

            for a in fs.readdir(parent_snap_ino)? {
                let a_attr_1 = fs.getattr(a.ino)?;
                assert_eq!(a.ino, a_attr_1.ino);
                if a.kind == FileType::Directory {
                    let a_attr = fs
                        .lookup(parent_snap_ino, &a.name)?
                        .ok_or_else(|| anyhow!("Invalid input"))?;
                    assert_eq!(a.ino, a_attr.ino);

                    for b in fs.readdir(a_attr.ino)? {
                        let b_attr_1 = fs.getattr(b.ino)?;
                        assert_eq!(b.ino, b_attr_1.ino);
                        if b.kind == FileType::Directory {
                            let b_attr = fs
                                .lookup(a_attr.ino, &b.name)?
                                .ok_or_else(|| anyhow!("Invalid input"))?;
                            assert_eq!(b.ino, b_attr.ino);
                            for c in fs.readdir(b_attr.ino)? {
                                let c_attr_1 = fs.getattr(c.ino)?;
                                assert_eq!(c.ino, c_attr_1.ino);
                                if c.kind == FileType::Directory {
                                    let _c_attr = fs.getattr(c.ino)?;
                                    let c_attr = fs
                                        .lookup(b_attr.ino, &c.name)?
                                        .ok_or_else(|| anyhow!("Invalid input"))?;
                                    assert_eq!(c.ino, c_attr.ino);
                                }
                                dbg!("startig v_dir search");
                                if c.oid != Oid::zero() && c.kind == FileType::RegularFile {
                                    let name = format!("{}@", c.name);
                                    let v_dir_attr = fs.lookup(b_attr.ino, &name)?.unwrap();
                                    dbg!(v_dir_attr.ino);
                                    let v_dir_entries = fs.readdir(v_dir_attr.ino)?;
                                    dbg!(v_dir_entries.len());

                                    dbg!(&v_dir_entries[0].name);
                                    dbg!(&v_dir_entries[0].ino);
                                    dbg!(v_dir_attr.ino);

                                    let lookup_attr =
                                        fs.lookup(v_dir_attr.ino, &v_dir_entries[0].name)?.unwrap();
                                    dbg!(lookup_attr.ino);

                                    let getattr_attr = fs.getattr(v_dir_entries[0].ino)?;
                                    dbg!(getattr_attr.ino);

                                    return Ok(());
                                }
                            }
                        }
                    }
                }
            }

            // FIND BY NAME REPO_DIR
            let live_attr = fs.lookup(REPO_DIR_INO, "live")?;
            assert!(live_attr.is_some());
            let live_attr = live_attr.unwrap();
            assert_eq!(live_attr.ino, LIVE_DIR_INO);

            assert_eq!(read_dir_root[0].name, "git_rust");
            let parent_for_mio = {
                let repo = fs.get_repo(read_dir_root[0].ino)?;
                let repo = repo.lock().map_err(|_| anyhow!("Lock poisoned"))?;
                repo.connection
                    .lock()
                    .unwrap()
                    .get_parent_ino(read_dir_root[0].ino)?
            };
            assert_eq!(parent_for_mio, ROOT_INO);

            // GET ATTR LIVE_DIR
            let live_attr = fs.getattr(LIVE_DIR_INO)?;
            assert_eq!(live_attr.ino, LIVE_DIR_INO);
            assert_eq!(live_attr.kind, FileType::Directory);

            // READ DIR LIVE
            let read_dir_live = fs.readdir(LIVE_DIR_INO)?;
            assert_eq!(read_dir_live.len(), 0);
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
            let fs = get_fs();
            let mut fs = fs.lock().map_err(|_| anyhow!("Lock posoned"))?;

            let create_attr = dir_attr();
            let name = OsStr::new("new_repo");
            fs.mkdir(ROOT_INO, name, create_attr)?;

            let root_attr = fs.getattr(ROOT_INO)?;
            assert_eq!(root_attr.ino, ROOT_INO);

            let repo_attr = fs.getattr(REPO_DIR_INO)?;
            assert_eq!(repo_attr.ino, REPO_DIR_INO);

            // FIND BY NAME
            let live_attr = fs.lookup(REPO_DIR_INO, "live")?;
            assert!(live_attr.is_some());
            let live_attr = live_attr.unwrap();
            assert_eq!(live_attr.ino, LIVE_DIR_INO);

            // READ DIR
            let read_dir = fs.readdir(ROOT_INO)?;
            assert_eq!(read_dir.len(), 1);

            assert_eq!(read_dir[0].name, "new_repo");
            let _folder_attr = fs.lookup(ROOT_INO, "new_repo")?.unwrap();

            let read_dir = fs.readdir(REPO_DIR_INO)?;
            assert_eq!(read_dir.len(), 1);

            assert_eq!(read_dir[0].name, "live");

            let create_attr = dir_attr();
            let dir_name1 = OsStr::new("dir_in_live_1");
            let dir1_attr = fs.mkdir(live_attr.ino, dir_name1, create_attr)?;
            let dir1_ino = LIVE_DIR_INO + 1;

            assert!(fs.exists(dir1_attr.ino)?);

            let find_dir1 = fs.lookup(LIVE_DIR_INO, "dir_in_live_1")?.unwrap();
            assert_eq!(find_dir1.ino, dir1_ino);
            let getattr_dir1 = fs.getattr(find_dir1.ino)?;
            assert_eq!(getattr_dir1.ino, dir1_ino);
            assert_eq!(dir1_attr.ino, dir1_ino);

            let file1 = OsStr::new("txt.txt");
            let (file1_attr, fh) = fs.create(LIVE_DIR_INO, file1, true, true)?;
            dbg!(file1_attr.ino);
            dbg!(fh);
            fs.release(fh)?;
            let fh = fs.open(file1_attr.ino, true, true, false)?;
            dbg!(fh);
            let write_buf = b"some text";
            fs.write(file1_attr.ino, 0, write_buf, fh)?;
            let mut buffer = [0u8; 100];
            let bytes_read = fs.read(file1_attr.ino, 0, &mut buffer, fh)?;
            dbg!(buffer.len());
            let text = String::from_utf8(buffer[..bytes_read].to_vec()).unwrap();
            println!("{}", text);
            dbg!(text.len());
            fs.release(fh)?;
            assert_eq!(text, "some text");

            let test_file1 = OsStr::new("new_file_1");
            fs.create(LIVE_DIR_INO, test_file1, true, true)?;
            let attr_real_file = fs.lookup(LIVE_DIR_INO, "new_file_1")?;
            assert!(attr_real_file.is_some());
            let attr_vdir = fs.lookup(LIVE_DIR_INO, "new_file_1@");
            assert!(attr_vdir.is_ok());
            let attr_vdir = attr_vdir?;
            let attr_vdir = attr_vdir.unwrap();
            assert!(fs.is_virtual(attr_vdir.ino));

            dbg!(attr_real_file.unwrap().ino);
            dbg!(attr_vdir.ino);

            dbg!(attr_real_file.unwrap().kind);
            assert!(!fs.is_virtual(attr_real_file.unwrap().ino));
            dbg!(attr_vdir.kind);

            let attr_vdir_getattr = fs.getattr(attr_vdir.ino)?;
            assert_eq!(attr_vdir_getattr.ino, attr_vdir.ino);

            Ok(())
        },
    )?;
    Ok(())
}
