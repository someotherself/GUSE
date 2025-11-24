use std::path::PathBuf;
use std::process::Command;
use std::{os::unix::net::UnixStream, path::Path};

use git2::Oid;

use crate::fs;
use crate::fs::{GitFs, builds::chase::Chase};

enum CommandResult {
    StdSuccess(Vec<u8>),
    // Command was found and succefully ran, but returned an error
    StdError(Vec<u8>),
    // Error was caused by something else (Ex:command not found)
    Error(Vec<u8>),
}

struct ChaseTarget {
    snap_ino: u64,
    // The path INSIDE guse
    path: PathBuf,
}

impl ChaseTarget {
    pub fn new(ino: u64, path: &Path) -> Self {
        Self {
            snap_ino: ino,
            path: path.to_path_buf(),
        }
    }
}

pub struct ChaseRunner<'a> {
    fs: &'a GitFs,
    repo_ino: u64,
    stream: &'a mut UnixStream,
    chase: Chase,
}

impl<'a> ChaseRunner<'a> {
    pub fn new(fs: &'a GitFs, repo_ino: u64, stream: &'a mut UnixStream, chase: Chase) -> Self {
        Self {
            fs,
            repo_ino,
            stream,
            chase,
        }
    }

    pub fn run(&mut self) -> anyhow::Result<Vec<(Oid, Vec<u8>)>> {
        let mut curr_run = 0;
        let mut prev_target: Option<ChaseTarget> = None;
        let total = self.chase.commits.len();

        let mut out: Vec<(Oid, Vec<u8>)> = vec![];

        let mut commit_list = self.chase.commits.clone();

        // RUN THROUGH EACH COMMIT
        while let Some(oid) = commit_list.pop_front() {
            curr_run += 1;
            let mut cmd_output: Vec<u8> = vec![];

            cmd_output.extend(
                format!(
                    "==> Starting chase for commit {} ({}/{})\n",
                    oid, curr_run, total
                )
                .as_bytes(),
            );
            let Some((cur_path, cur_ino)) = self.chase.commit_paths.get(&oid) else {
                continue;
            };

            // MOVE build contents from previous commit
            let cur_target: ChaseTarget = ChaseTarget::new(*cur_ino, cur_path);
            if let Some(ref prev_target) = prev_target {
                let _ = move_chase_target(self.fs, prev_target, &cur_target);
            }

            let mut commands = self.chase.commands.clone();

            // RUN COMMANDS
            while let Some(command) = commands.pop_front() {
                cmd_output.extend(format!("Command: {}\n", command).as_bytes());
                let Some(output) = run_command_on_snap(cur_path, &command) else {
                    cmd_output.extend(b"GUSE detected no output\n");
                    continue;
                };
                if output.is_empty() {
                    cmd_output.extend(b"GUSE detected no output\n");
                } else {
                    cmd_output.extend(output);
                }
            }
            out.push((oid, cmd_output));
            prev_target = Some(cur_target);
        }
        Ok(out)
    }
}

fn run_command_on_snap(path: &Path, command: &str) -> Option<Vec<u8>> {
    let mut split = command.split_whitespace();
    let prog = split.next()?;
    let args: Vec<&str> = split.collect();
    let output = Command::new(prog).current_dir(path).args(args).output();

    let output = match output {
        Ok(out) => out,
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                return Some(format!("Command not found: {}\n", prog).into_bytes());
            }
            return Some(format!("Failed to run {}: {}\n", prog, e).into_bytes());
        }
    };

    Some(output.stderr)
}

fn move_chase_target(fs: &GitFs, old: &ChaseTarget, new: &ChaseTarget) -> anyhow::Result<()> {
    let entries = fs.readdir(old.snap_ino)?;
    for e in entries {
        if !fs.is_in_build(e.ino.into())? {
            continue;
        };
        fs::ops::rename::rename_git_build(
            fs,
            old.snap_ino.into(),
            &e.name,
            new.snap_ino.into(),
            &e.name,
        )?;
    }
    Ok(())
}
