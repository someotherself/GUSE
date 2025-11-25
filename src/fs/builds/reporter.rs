use std::{
    ffi::OsString,
    io::Write,
    os::unix::net::UnixStream,
    path::{Path, PathBuf},
};

use anyhow::bail;
use thiserror::Error;

use crate::internals::sock::ControlRes;

pub type GuseResult<T> = core::result::Result<T, ChaseError>;

#[derive(Error, Debug)]
pub enum ChaseError {
    #[error("Error parsing script")]
    ParsingMisc { msg: String },
    #[error("Script not found at path")]
    ScriptNotFound { path: PathBuf },
    #[error("")]
    LuaError {
        #[source]
        source: mlua::Error,
        msg: String,
    },
    #[error("No commits provided")]
    NoCommits,
    #[error("No commands provided")]
    NoCommands,
}

pub type GuseGitResult<T> = core::result::Result<T, ChaseGitError>;

#[derive(Error, Debug)]
pub enum ChaseGitError {
    #[error("Ambiguous commit hash")] // git2::ErrorCode::Ambiguous
    GitAmbiguousCommit { commit: String },
    #[error("Commit not found")] // git2::ErrorCode::NotFound
    CommitNotFound { commit: String },
    #[error("Git error")] // Other git errors
    GitError {
        message: String,
        #[source]
        source: git2::Error,
    },
    #[error("No results in snaps_to_ref")]
    NoRefKindsFound,
    #[error("Snap folder not found")]
    RefKindNotFound { commit: String },
    #[error("GitFs error")]
    FsError { msg: String },
}

pub type GuseFsResult<T> = core::result::Result<T, ChaseFsError>;

#[derive(Error, Debug)]
pub enum ChaseFsError {
    #[error("Lookup returned None")]
    NoneFound { target: OsString },
    #[error("Snap not found")]
    SnapNotFound { msg: String },
    #[error("GitFs error")]
    FsError { msg: String },
}

pub trait Reporter {
    fn update(&mut self, msg: &str) -> anyhow::Result<()>;
}

impl Reporter for UnixStream {
    fn update(&mut self, msg: &str) -> anyhow::Result<()> {
        let res = ControlRes::Update {
            message: msg.as_bytes().to_vec(),
        };
        let out = serde_json::to_vec(&res)?;
        self.write_all(&out)?;
        Ok(())
    }
}

pub trait ErrorResolver<T> {
    fn resolve(self, stream: &mut UnixStream) -> anyhow::Result<T>;
}

impl<T> ErrorResolver<T> for GuseResult<T> {
    fn resolve(self, stream: &mut UnixStream) -> anyhow::Result<T> {
        match self {
            Ok(r) => Ok(r),
            Err(e) => match e {
                ChaseError::LuaError { source, msg } => {
                    stream.update(&prepare_lua_error(&source, &msg))?;
                    bail!("")
                }
                ChaseError::ParsingMisc { msg } => {
                    // TODO. What was this for anyway?
                    bail!("Error parsing script: {msg}")
                }
                ChaseError::ScriptNotFound { path } => {
                    stream.update(&prepare_not_found_error(&path))?;
                    bail!("")
                }
                ChaseError::NoCommits => {
                    stream.update("No COMMIT hashes were found in the script.\n")?;
                    bail!("")
                }
                ChaseError::NoCommands => {
                    stream.update("No COMMANDS were found in the script.\n")?;
                    bail!("")
                }
            },
        }
    }
}

/// ChaseError::LuaError
fn prepare_lua_error(source: &mlua::Error, msg: &str) -> String {
    let error = "The chase script failed to load due to a Lua error.\nVerify your scripts syntax and ensure all required fields and value types are correct.";
    let error = color_red(error);
    format!("{}\n{} {}\n", error, source, msg)
}

/// ChaseError::ScriptNotFound
fn prepare_not_found_error(path: &Path) -> String {
    let text1 = white_underline("Could not find the script as path:");
    let text2 = white_underline("Please double check the name.\n");
    format!("{} {}\n{}", text1, path.display(), text2)
}

impl<T> ErrorResolver<T> for GuseGitResult<T> {
    fn resolve(self, stream: &mut UnixStream) -> anyhow::Result<T> {
        match self {
            Ok(r) => Ok(r),
            Err(e) => match e {
                ChaseGitError::GitAmbiguousCommit { commit } => {
                    stream.update(&prepare_ambiguous_error(&commit))?;
                    bail!("")
                }
                ChaseGitError::CommitNotFound { commit } => {
                    stream.update(&prepare_no_commit_error(&commit))?;
                    bail!("")
                }
                ChaseGitError::GitError { message, source } => {
                    stream.update(&prepare_git_error(&message, source))?;
                    bail!("")
                }
                ChaseGitError::NoRefKindsFound => {
                    stream.update(&prepare_norefs_error())?;
                    bail!("")
                }
                ChaseGitError::RefKindNotFound { commit } => {
                    stream.update(&prepare_miss_ref_error(&commit))?;
                    bail!("")
                }
                ChaseGitError::FsError { msg: _ } => {
                    // stream.update(&prepare_miss_ref_error(&commit))?;
                    bail!("")
                }
            },
        }
    }
}

/// ChaseGitError::GitAmbiguousCommit
fn prepare_ambiguous_error(commit: &str) -> String {
    let text1 = "Multiple commits were found for hash:";
    let text2 = "Please use more HASH characters and try again.";
    format!("{} {}.\n{}", text1, commit, text2)
}

/// ChaseGitError::CommitNotFound
fn prepare_no_commit_error(commit: &str) -> String {
    let text1 = "No commits found for hash:";
    let text2 = "Please double check the hash or ensure the branch was fetched.";
    let text3 = "Make sure to restart the session after a new fetch.";
    format!("{} {}.\n{}\n{}", text1, commit, text2, text3)
}

/// ChaseGitError::GitError
fn prepare_git_error(msg: &str, source: git2::Error) -> String {
    let text1 = "Git Error with message:";
    format!("{} {}.\n{}", text1, msg, source)
}

/// ChaseGitError::NoRefKindsFound
fn prepare_norefs_error() -> String {
    "Snap folders not found on disk. Try restarting the session".to_string()
}

/// ChaseGitError::RefKindNotFound
fn prepare_miss_ref_error(commit: &str) -> String {
    format!(
        "Commit {} not found on disk. Try restarting the session",
        commit
    )
}

impl<T> ErrorResolver<T> for GuseFsResult<T> {
    fn resolve(self, stream: &mut UnixStream) -> anyhow::Result<T> {
        match self {
            Ok(r) => Ok(r),
            Err(e) => match e {
                ChaseFsError::NoneFound { target } => {
                    stream.update(&format!(
                        "Lookup returned to results for name {}",
                        target.display()
                    ))?;
                    bail!("")
                }
                ChaseFsError::SnapNotFound { msg } => {
                    stream.update(&format!("Error searching for the Snap folder: {}", msg))?;
                    bail!("")
                }
                ChaseFsError::FsError { msg: _ } => {
                    bail!("")
                }
            },
        }
    }
}

// https://gist.github.com/JBlond/2fea43a3049b38287e5e9cefc87b2124
fn color_red(s: &str) -> String {
    format!("\x1b[31m{s}\x1b[0m")
}

fn color_green(s: &str) -> String {
    format!("\x1b[32m{s}\x1b[0m")
}

fn color_yellow(s: &str) -> String {
    format!("\x1b[33m{s}\x1b[0m")
}

fn white_underline(s: &str) -> String {
    format!("\x1b[4;37m{s}\x1b[0m")
}
