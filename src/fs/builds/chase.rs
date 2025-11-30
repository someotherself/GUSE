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
        chase_runner::ChaseRunner,
        reporter::{ErrorResolver, Updater},
        runtime::{ChaseRunMode, ChaseStopMode, LuaConfig},
    },
};

#[derive(Clone)]
pub struct Chase {
    // Makes sure Oids are read in the correct order, as they were input by the user
    pub commits: VecDeque<Oid>,
    pub commands: VecDeque<String>,
    pub run_mode: ChaseRunMode,
    pub stop_mode: ChaseStopMode,
    // Holds the path for the Snap folders and the ino of the snap folders
    pub commit_paths: HashMap<Oid, (PathBuf, u64)>,
    // Logging to file enabled/disabled
    pub log: bool,
}

pub fn start_chase(
    fs: &GitFs,
    repo_name: &str,
    script: &str,
    stream: &mut UnixStream,
    log: bool,
) -> anyhow::Result<()> {
    let repo_ino = get_repo_ino(fs, repo_name, stream)?;
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
    let commands: VecDeque<String> = cfg.commands.into();
    // TODO: Update message
    let c_oid_vec = commits.iter().collect::<Vec<&Oid>>();

    // 4 - Find the Snap folders on disk
    let c_refs = validate_commit_refs(fs, repo_ino, &c_oid_vec)?;
    let paths = resolve_path_for_refs(fs, repo_ino, c_refs)?;

    // 5 - Prepare the build ctx
    let chase: Chase = Chase {
        commits,
        commands,
        run_mode: cfg.run_mode,
        stop_mode: cfg.stop_mode,
        commit_paths: paths,
        log,
    };

    // 6 - Cleanup any existing files
    cleanup_builds(fs, repo_ino, &chase)?;

    // 7 - run chase
    // Name of a folder so save logs to (if enabled)
    let name = format!("{}", chrono::offset::Utc::now());
    let dir_path = script_path.join(name);
    let mut chase_runner = ChaseRunner::new(&dir_path, fs, repo_ino, stream, chase.clone());
    let _ = chase_runner.run();

    // 8 - Cleanup any
    cleanup_builds(fs, repo_ino, &chase)?;

    Ok(())
}

fn get_repo_ino(fs: &GitFs, repo_name: &str, stream: &mut UnixStream) -> anyhow::Result<u64> {
    let Some(repo_entry) = fs.repos_map.get(repo_name) else {
        stream.update(&format!(
            "Repo {} does not exist. Please check correct spelling\n",
            repo_name
        ))?;
        match &fs.repos_map.len() {
            0 => stream.update("No repos exist.")?,
            _ => {
                stream.update("Existing repos:")?;
                for e in &fs.repos_map {
                    stream.update(&format!(" {:?}", e.key()))?;
                }
                stream.update(".\n")?;
            }
        };
        bail!(std::io::Error::from_raw_os_error(libc::ENOENT))
    };
    Ok(GitFs::repo_id_to_ino(*repo_entry.value()))
}

/// "Starting GUSE chase
fn start_message(script: &str) -> String {
    format!("Starting GUSE chase {} \n", script)
}
