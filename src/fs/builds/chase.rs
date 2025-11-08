use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use anyhow::{anyhow, bail};
use git2::Oid;

use crate::fs::{
    GitFs,
    builds::{BuildSession, runtime::LuaConfig},
};

struct Chase {
    commits: VecDeque<Oid>,
    timeout: usize,
    max_parallel: usize,
    /// Old sessions returned by `move_build_session`.
    ///
    /// Should be returned at the end of the chase
    moved_sessions: HashMap<Oid, Arc<BuildSession>>,
}

pub fn start_chase(fs: &GitFs, repo_name: &str, build: &str) -> anyhow::Result<()> {
    let Some(repo_entry) = fs.repos_map.get(repo_name) else {
        bail!("Repo does not exist")
    };
    let repo_ino = GitFs::repo_id_to_ino(*repo_entry.value());
    let repo = fs.get_repo(repo_ino)?;

    let script_path = repo.chase_dir.join(build);
    let cfg = LuaConfig::read_lua(&script_path).map_err(|e| {
        println!("ERROR: {e}");
        anyhow!("Error processing lua scripts - {e}")
    })?;

    for commit in cfg.commits {
        println!("{commit}");
    }

    if let Some(mode) = cfg.mode {
        println!("Mode is {:?}", mode);
    }

    Ok(())
}
