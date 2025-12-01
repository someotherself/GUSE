use std::path::Path;
use std::path::PathBuf;

use git2::Oid;

use crate::fs::builds::reporter::Reporter;
use crate::fs::builds::reporter::{Updater, color_red};
use crate::fs::{self, builds::logger::run_command_on_snap};
use crate::fs::{GitFs, builds::chase::Chase};

// Holds the inode of the target Snap folder
struct ChaseTarget(u64);

impl ChaseTarget {
    pub fn new(ino: u64) -> Self {
        Self(ino)
    }
}

pub struct ChaseRunner<'a, R: Updater> {
    dir_path: PathBuf,
    fs: &'a GitFs,
    repo_ino: u64,
    pub reporter: &'a mut R,
    chase: Chase,
    pub curr_log_file: Option<std::fs::File>,
}

impl<'a, R: Updater> ChaseRunner<'a, R> {
    pub fn new(
        dir: &Path,
        fs: &'a GitFs,
        repo_ino: u64,
        reporter: &'a mut R,
        mut chase: Chase,
    ) -> Self {
        // Folder where the logs will be saved
        if chase.log && std::fs::create_dir(dir).is_err() {
            // If the folder can't be created, don't try to log
            let _ = reporter.update(&color_red("COULD NOT CREATE LOGGING DIRECTORY."));
            chase.log = false;
        }
        Self {
            dir_path: dir.to_path_buf(),
            fs,
            repo_ino,
            reporter,
            chase,
            curr_log_file: None,
        }
    }

    pub fn run(&mut self) -> anyhow::Result<Vec<(Oid, Vec<u8>)>> {
        let mut prev_target: Option<ChaseTarget> = None;
        let mut curr_run = 0;
        let total = self.chase.commits.len();

        let mut out: Vec<(Oid, Vec<u8>)> = vec![];
        let mut commit_list = self.chase.commits.clone();

        // RUN THROUGH EACH COMMIT
        while let Some(oid) = commit_list.pop_front() {
            curr_run += 1;
            let mut cmd_output: Vec<u8> = vec![];

            // Log file
            if self.chase.log {
                let name = format!("{:02}_{oid:.7}", curr_run);
                if let Ok(file) = std::fs::OpenOptions::new()
                    .write(true)
                    .truncate(true)
                    .create(true)
                    .open(self.dir_path.join(name))
                {
                    self.curr_log_file = Some(file)
                };
            }

            self.report(&format!(
                "==> Starting chase for commit {} ({}/{})\n",
                oid, curr_run, total
            ))?;

            let Some((cur_path, cur_ino)) = self.chase.commit_paths.get(&oid).cloned() else {
                continue;
            };

            // MOVE build contents from previous commit
            let cur_target: ChaseTarget = ChaseTarget::new(cur_ino);
            if let Some(ref prev_target) = prev_target {
                // TODO: Log an error
                let _ = move_chase_target(self.fs, prev_target, &cur_target);
            }

            let mut commands = self.chase.commands.clone();

            // RUN COMMANDS
            while let Some(command) = commands.pop_front() {
                self.report(&format!(
                    "==> Running command {:?} for {} ({}/{})\n",
                    command, oid, curr_run, total
                ))?;
                cmd_output.extend(format!("Command: {}", command).as_bytes());
                let Some(output) = run_command_on_snap(&cur_path, &command, self.reporter) else {
                    self.report("GUSE detected no output\n")?;
                    continue;
                };
                if output.is_empty() {
                    self.report("GUSE detected no output\n")?;
                } else {
                    self.report(str::from_utf8(&output)?)?;
                }
                self.report(&format!("--> FINISHED command {} for {}\n", command, oid))?;
            }
            out.push((oid, cmd_output));
            prev_target = Some(cur_target);
            self.curr_log_file = None;
        }
        Ok(out)
    }
}

fn move_chase_target(fs: &GitFs, old: &ChaseTarget, new: &ChaseTarget) -> anyhow::Result<()> {
    let entries = fs.readdir(old.0)?;
    for e in entries {
        if !fs.is_in_build(e.ino.into())? {
            continue;
        };
        fs::ops::rename::rename_git_build(fs, old.0.into(), &e.name, new.0.into(), &e.name)?;
    }
    Ok(())
}
