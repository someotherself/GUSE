use std::{
    collections::{HashMap, VecDeque},
    os::unix::net::UnixStream,
    sync::Arc,
};

use anyhow::{anyhow, bail};
use git2::Oid;

use crate::fs::{
    GitFs,
    builds::{BuildSession, reporter::{Reporter, color_green}, runtime::LuaConfig},
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

pub fn start_chase(
    fs: &GitFs,
    repo_name: &str,
    script: &str,
    stream: &mut UnixStream,
) -> anyhow::Result<()> {
    let Some(repo_entry) = fs.repos_map.get(repo_name) else {
        bail!("Repo does not exist")
    };
    let repo_ino = GitFs::repo_id_to_ino(*repo_entry.value());
    let repo = fs.get_repo(repo_ino)?;

    let script_path = repo.chase_dir.join(script);

    stream.update(&start_message(script))?;

    let cfg = LuaConfig::read_lua(&script_path).map_err(|e| {
        println!("ERROR: {e}");
        anyhow!("Error processing lua scripts - {e}")
    })?;

    let bytes = color_green(&format!("{}", cfg.commits.len()));
    let msg = format!("Found {} commits in script \n", bytes);
    stream.update(&msg)?;
    Ok(())
}

fn start_message(script: &str) -> String {
    format!("STARTING GUSE CHASE {} \n", script)
}

fn validate_commits(fs: &GitFs, commits: &[String], repo_ino: u64) -> anyhow::Result<()> {
    let repo = fs.get_repo(repo_ino)?;
    let _res = repo.find_snap_in_repo(commits)?;
    Ok(())
}


fn build_commit_update(commits: &[String]) -> Vec<u8> {
    let parts: Vec<String> = commits.iter().map(|c| color_green(c)).collect();

    let msg = format!("Commits found in script: {}\n", parts.join(", "));
    msg.into_bytes()
}
