use std::{
    collections::{HashMap, VecDeque},
    os::unix::net::UnixStream,
    path::PathBuf,
};

use anyhow::bail;
use git2::Oid;

use crate::fs::{
    GitFs,
    builds::{
        chase_resolver::{
            cleanup_builds, resolve_path_for_refs, validate_commit_refs, validate_commits,
        },
        reporter::{ErrorResolver, Reporter},
        runtime::LuaConfig,
    },
};

pub struct Chase {
    // Makes sure Oids are read in the correct order, as they were input by the user
    pub commits: VecDeque<Oid>,
    // Holds the path for the Snap folders and the ino of the snap folders
    pub commit_paths: HashMap<Oid, (PathBuf, u64)>,
}

pub fn start_chase(
    fs: &GitFs,
    repo_name: &str,
    script: &str,
    stream: &mut UnixStream,
) -> anyhow::Result<()> {
    let Some(repo_entry) = fs.repos_map.get(repo_name) else {
        tracing::error!("Repo does not exist. Please check correct spelling");
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    let repo_ino = GitFs::repo_id_to_ino(*repo_entry.value());
    let repo = fs.get_repo(repo_ino)?;

    // 1 - Get script path
    let script_path = repo.chase_dir.join(script);
    stream.update(&start_message(script))?;

    // 2 - Read and parse the script
    let cfg = LuaConfig::read_lua(&script_path).resolve(stream)?;
    let msg = format!("Found {} commits in script \n", cfg.commits.len());
    stream.update(&msg)?;

    // 3 - Validate the commits, find the Oid
    let commits = validate_commits(fs, repo_ino, &cfg.commits).resolve(stream)?;
    // TODO: Update message
    let c_oid_vec = commits.iter().collect::<Vec<&Oid>>();

    // 4 - Find the Snap folders on disk
    let c_refs = validate_commit_refs(fs, repo_ino, &c_oid_vec)?;
    let paths = resolve_path_for_refs(fs, repo_ino, c_refs)?;

    // 5 - Prepare the build ctx
    let chase: Chase = Chase {
        commits,
        commit_paths: paths,
    };

    // 6 - Cleanup any existing files
    cleanup_builds(fs, repo_ino, &chase)?;

    Ok(())
}

/// "Starting GUSE chase
fn start_message(script: &str) -> String {
    format!("Starting GUSE chase {} \n", script)
}
