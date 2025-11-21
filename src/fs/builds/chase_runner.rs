use std::process::Command;
use std::{os::unix::net::UnixStream, path::Path};

use git2::Oid;

use crate::fs::{GitFs, builds::chase::Chase};

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
        let mut current = 0;
        let total = self.chase.commits.len();

        let mut out: Vec<(Oid, Vec<u8>)> = vec![];

        let mut commit_list = self.chase.commits.clone();

        // RUN THROUGH EACH COMMIT
        while let Some(oid) = commit_list.pop_front() {
            current += 1;

            let mut cmd_output: Vec<u8> = vec![];

            cmd_output.extend(
                format!(
                    "==> Starting chase for commit {} ({}/{})\n",
                    oid, current, total
                )
                .as_bytes(),
            );
            let Some((cur_path, _)) = self.chase.commit_paths.get(&oid) else {
                continue;
            };
            let mut commands = self.chase.commands.clone();

            // RUN COMMANDS
            while let Some(command) = commands.pop_front() {
                cmd_output.extend(format!("Command: {}\n", command).as_bytes());
                let Some(output) = run_command_on_snap(cur_path, &command) else {
                    continue;
                };
                cmd_output.extend(output);
            }
            out.push((oid, cmd_output));
        }
        Ok(out)
    }
}

fn run_command_on_snap(path: &Path, command: &str) -> Option<Vec<u8>> {
    let mut split = command.split(" ");
    let prog = split.next()?;
    let arg = split.next()?;
    let output = Command::new(prog)
        .current_dir(path)
        .arg(arg)
        .output()
        .ok()?;

    if output.status.success() {
        Some(output.stdout)
    } else {
        None
    }
}
