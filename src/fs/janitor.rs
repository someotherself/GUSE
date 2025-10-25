use std::{ffi::OsString, sync::Weak};

use anyhow::anyhow;
use crossbeam_channel::Sender;

use crate::fs::GitFs;

pub struct Janitor {
    tx: Sender<Jobs>,
    handle: std::thread::JoinHandle<()>,
}

pub enum Jobs {
    RmdirGit { parent_ino: u64, name: OsString },
    RmdirLive { parent_ino: u64, name: OsString },
    UnlinkGit { parent_ino: u64, name: OsString },
    UnlinkLive { parent_ino: u64, name: OsString },
}

impl Jobs {
    pub fn run_job(fs: Weak<GitFs>, job: Jobs) -> anyhow::Result<()> {
        match job {
            Jobs::RmdirGit { parent_ino, name } => {
                let fs = fs.upgrade().ok_or_else(|| anyhow!("Cannot upgrade ref"))?;
                let path = {
                    let commit_oid = fs.get_oid_from_db(parent_ino)?;
                    let repo = fs.get_repo(parent_ino)?;
                    let build_root = &repo.build_dir;
                    let session = repo.get_or_init_build_session(commit_oid, build_root)?;
                    drop(repo);
                    session.finish_path(&fs, parent_ino.into())?.join(name)
                };

                std::fs::remove_dir(path)?;
                Ok(())
            }
            Jobs::RmdirLive { parent_ino, name } => {
                let fs = fs.upgrade().ok_or_else(|| anyhow!("Cannot upgrade ref"))?;
                let path = fs.get_live_path(parent_ino.into())?.join(name);
                std::fs::remove_dir(path)?;
                Ok(())
            }
            Jobs::UnlinkGit { parent_ino, name } => {
                let fs = fs.upgrade().ok_or_else(|| anyhow!("Cannot upgrade ref"))?;
                let path = {
                    let commit_oid = fs.get_oid_from_db(parent_ino)?;
                    let repo = fs.get_repo(parent_ino)?;
                    let build_root = &repo.build_dir;
                    let session = repo.get_or_init_build_session(commit_oid, build_root)?;
                    session.finish_path(&fs, parent_ino.into())?.join(name)
                };
                std::fs::remove_file(path)?;
                Ok(())
            }
            Jobs::UnlinkLive {
                parent_ino,
                name,
            } => {
                let fs = fs.upgrade().ok_or_else(|| anyhow!("Cannot upgrade ref"))?;
                let path = fs.build_full_path(parent_ino.into())?.join(name);
                std::fs::remove_file(path)?;
                Ok(())
            }
        }
    }
}
