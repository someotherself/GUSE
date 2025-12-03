use std::path::Path;
use std::path::PathBuf;

use crate::fs::{
    self,
    builds::reporter::{Reporter, Updater, color_red},
};
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
    pub reporter: &'a mut R,
    pub chase: Chase,
    pub curr_log_file: Option<std::fs::File>,
    pub run: bool,
}

impl<'a, R: Updater> ChaseRunner<'a, R> {
    pub fn new(dir: &Path, fs: &'a GitFs, reporter: &'a mut R, mut chase: Chase) -> Self {
        // Folder where the logs will be saved
        if chase.log && std::fs::create_dir(dir).is_err() {
            // If the folder can't be created, don't try to log
            let _ = reporter.update(&color_red("COULD NOT CREATE LOGGING DIRECTORY."));
            chase.log = false;
        }
        Self {
            dir_path: dir.to_path_buf(),
            fs,
            reporter,
            chase,
            curr_log_file: None,
            run: true,
        }
    }

    pub fn run(&mut self) -> anyhow::Result<()> {
        let mut prev_target: Option<ChaseTarget> = None;
        let mut curr_run = 0;
        let total = self.chase.commits.len();

        let mut commit_list = self.chase.commits.clone();

        // RUN THROUGH EACH COMMIT
        while let Some(oid) = commit_list.pop_front()
            && self.run
        {
            curr_run += 1;

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
            if let Some(ref prev_target) = prev_target
                && let Err(e) = move_chase_target(self.fs, prev_target, &cur_target)
            {
                self.report(&format!(
                    "Could not move target contents due to error:\n{e}"
                ))?;
            }

            let mut commands = self.chase.commands.clone();

            // RUN COMMANDS
            while let Some(command) = commands.pop_front() {
                self.report(&format!(
                    "==> Running command {:?} for {} ({}/{})\n",
                    command, oid, curr_run, total
                ))?;
                let _ = self.run_command_on_snap(&cur_path, &command).egress(self);
                self.report(&format!("--> FINISHED command {} for {}\n", command, oid))?;
            }
            prev_target = Some(cur_target);
            self.curr_log_file = None;
        }
        Ok(())
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
