use std::{
    collections::{HashMap, VecDeque},
    io::Write,
    os::unix::net::UnixStream,
    path::PathBuf,
    sync::{Arc, OnceLock, atomic::AtomicU64},
};

use anyhow::bail;
use git2::Oid;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::{
    fs::{
        GitFs,
        builds::{
            chase_handle::ChaseHandle,
            chase_resolver::{
                cleanup_builds, resolve_path_for_refs, validate_commit_refs, validate_commits,
            },
            chase_runner::ChaseRunner,
            logger::CmdResult,
            reporter::{ErrorResolver, Updater},
            runtime::{ChaseRunMode, ChaseStopMode, LuaConfig},
        },
    },
    internals::sock::ControlRes,
};

pub type ChaseId = u64;

pub static CHASE_ID: AtomicU64 = AtomicU64::new(1);
pub static CHASE_STOP_FLAGS: OnceLock<Mutex<HashMap<ChaseId, Arc<ChaseHandle>>>> = OnceLock::new();

pub fn next_chase_id() -> u64 {
    CHASE_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

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
    pub args: ChaseArgs,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "chase-args", rename_all = "snake_case")]
/// Created by Clap to collect arguments and pass them on
pub struct ChaseArgs {
    pub log: bool,
    pub no_move: bool,
}

// Accepts a handshake between "client" and "server"
// The Chase id is used for client to find and manage the chase once started
pub fn start_chase_connection(stream: &mut UnixStream) -> anyhow::Result<()> {
    if let CmdResult::Ok(id) = ChaseHandle::register_chase_id() {
        let res = ControlRes::Accept { id };
        let buf = serde_json::to_vec(&res)?;
        stream.write_all(&buf)?;
    }
    Ok(())
}

pub fn start_chase(
    fs: &GitFs,
    repo_name: &str,
    script: &str,
    stream: &mut UnixStream,
    args: ChaseArgs,
    chase_id: ChaseId,
) -> anyhow::Result<()> {
    if let CmdResult::Err(e) = ChaseHandle::start_run(chase_id) {
        stream.update(&format!("Chase id not found. Unable to cancel run: {e}"))?;
    };
    let repo_ino = get_repo_ino(fs, repo_name, stream)?;
    let repo = fs.get_repo(repo_ino)?;

    // 1 - Get script path
    let script_path = repo.chase_dir.join(script);
    stream.update(&start_message(script))?;

    // 2 - Read and parse the script
    let cfg = LuaConfig::read_lua(&script_path).resolve(stream)?;

    // 3 - Validate the commits, find the Oid
    let commits = validate_commits(fs, repo_ino, &cfg.commits).resolve(stream)?;
    let commands: VecDeque<String> = cfg.commands.into();
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
        args,
    };

    // 6 - Cleanup any existing files
    cleanup_builds(fs, repo_ino, &chase)?;

    // 7 - run chase
    // Name of a folder to save logs to (if enabled)
    let name = format!("{}", chrono::offset::Utc::now());
    let dir_path = script_path.join(name);
    let Some(handle) = ChaseHandle::get_handle(&chase_id) else {
        bail!("Error. Chase handle does not exist for id {chase_id}")
    };
    let mut chase_runner: ChaseRunner<'_, UnixStream> =
        ChaseRunner::new(&dir_path, fs, stream, chase.clone(), handle);
    let _ = chase_runner.run();

    // 8 - Cleanup all the files created during the chase
    cleanup_builds(fs, repo_ino, &chase)?;

    ChaseHandle::deregister_chase_id(chase_id);
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
