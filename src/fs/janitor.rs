use std::{
    ffi::OsStr,
    path::Path,
    sync::Weak,
    thread,
    time::{Duration, Instant},
};

use anyhow::bail;
use crossbeam_channel::Sender;
use rand::{Rng, distr::Alphanumeric};

use crate::fs::{GitFs, TRASH_FOLDER, meta_db::MetaDb};

const TRASH_PER: Duration = Duration::from_secs(10);
const NEG_PER: Duration = Duration::from_secs(10);

pub struct Janitor {
    tx: Sender<Jobs>,
    handle: std::thread::JoinHandle<()>,
}

pub enum Jobs {}
    
impl Jobs {
    pub fn _run_job(_fs: Weak<GitFs>) -> anyhow::Result<()> {
        Ok(())
    }

    fn clean_trash(fs: &GitFs) -> anyhow::Result<()> {
        let trash_folder = &fs.repos_dir.join(TRASH_FOLDER);
        if !trash_folder.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(trash_folder)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            if metadata.is_dir() {
                let _ = std::fs::remove_dir_all(entry.path());
            } else {
                let _ = std::fs::remove_file(entry.path());
            }
        }
        Ok(())
    }

    fn clean_neg(fs: &GitFs) -> anyhow::Result<()> {
        let repos: Vec<u16> = fs.repos_list.iter().map(|r| r.repo_id).collect();
        if repos.is_empty() {
            return Ok(());
        }
        for repo_id in repos {
            let repo_db = fs
                .conn_list
                .get(&repo_id)
                .ok_or_else(|| anyhow::anyhow!("no db"))?;
            let conn = repo_db.ro_pool.get()?;
            let neg_list = MetaDb::get_inactive_dentries(&conn)?;
            let iter = neg_list
                .iter()
                .map(|d| (d.target_ino, d.parent_ino, d.target_name.as_os_str()))
                .collect::<Vec<(u64, u64, &OsStr)>>();
            fs.cleanup_neg_entries(&iter, repo_id)?;
        }

        Ok(())
    }

    pub fn spawn_worker(fs: Weak<GitFs>) {
        thread::spawn(move || {
            let mut next_trash = Instant::now() + TRASH_PER;
            let mut next_neg = Instant::now() + NEG_PER;

            while let Some(fs) = fs.upgrade() {
                let now = Instant::now();

                if now >= next_trash {
                    let _ = Jobs::clean_trash(&fs);
                    while next_trash <= now {
                        next_trash += TRASH_PER;
                    }
                }

                if now >= next_neg {
                    let _ = Jobs::clean_neg(&fs);
                    while next_neg <= now {
                        next_neg += NEG_PER;
                    }
                }
            }
        });
    }
}

// Creates a random string 6 characters long and alphanumerical
pub fn random_string() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(6)
        .map(char::from)
        .collect()
}

pub fn rename_to_trash(fs: &GitFs, from: &Path, name: &OsStr) -> anyhow::Result<()> {
    let repo_dir = &fs.repos_dir;
    let mut to =
        repo_dir
            .join(TRASH_FOLDER)
            .join(format!("{}_{}", name.display(), random_string()));
    while let Err(e) = std::fs::rename(from, to) {
        if e.kind() == std::io::ErrorKind::AlreadyExists {
            to =
                repo_dir
                    .join(TRASH_FOLDER)
                    .join(format!("{}_{}", name.display(), random_string()));
        } else {
            tracing::error!("Cannot remove {}. Error: {e}", name.display());
            bail!(e);
        }
    }
    Ok(())
}
