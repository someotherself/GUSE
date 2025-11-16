use std::{
    collections::{HashMap, VecDeque},
    os::unix::net::UnixStream,
    path::PathBuf,
    sync::Arc,
};

use anyhow::bail;
use git2::Oid;

use crate::fs::{
    GitFs,
    builds::{
        BuildSession,
        chase_resolver::{resolve_path_for_refs, validate_commit_refs, validate_commits},
        reporter::{ErrorResolver, Reporter},
        runtime::LuaConfig,
    },
};

struct Chase {
    commits: VecDeque<Oid>,
    commit_paths: HashMap<Oid, PathBuf>,
    /// Old session returned by `move_build_session`.
    ///
    /// One commit can only be linked to one BuildSession. If a commit already has one, save it here and fix it when the commit is done with the chase build.
    moved_session: Option<(Oid, Arc<BuildSession>)>,
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

    let cfg = LuaConfig::read_lua(&script_path).resolve(stream)?;

    let msg = format!("Found {} commits in script \n", cfg.commits.len());
    stream.update(&msg)?;

    let commits = validate_commits(fs, repo_ino, &cfg.commits).resolve(stream)?;
    // TODO: Update message
    let c_oid_vec = commits.iter().collect::<Vec<&Oid>>();
    let c_refs = validate_commit_refs(fs, repo_ino, &c_oid_vec)?;

    let paths = resolve_path_for_refs(fs, repo_ino, c_refs)?;

    // Prepare the build ctx
    let _chase: Chase = Chase {
        commits,
        commit_paths: paths,
        moved_session: None,
    };

    Ok(())
}

fn start_message(script: &str) -> String {
    format!("Starting GUSE chase {} \n", script)
}
