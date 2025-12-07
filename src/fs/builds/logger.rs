use std::{
    collections::VecDeque,
    fmt::Display,
    io::{BufRead, BufReader},
    ops::Deref,
    path::Path,
    process::{Command, ExitStatus},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::fs::builds::{
    chase_runner::ChaseRunner,
    job::Job,
    reporter::{Reporter, Updater},
    runtime::ChaseStopMode,
};

#[derive(Debug, Clone)]
pub struct LogLine {
    pub t_stmp: u128,
    pub line: Vec<u8>,
}

impl LogLine {
    pub fn new(line: &[u8]) -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();

        Self {
            t_stmp: stamp,
            line: Vec::from(line),
        }
    }
}

#[derive(Debug, Clone)]
pub enum CmdResult<T> {
    Ok(T),
    Err(String),
    ExitFail(ExitStatus),
}

impl<T> Display for CmdResult<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Ok(_) => write!(f, "SUCCESS"),
            Self::Err(_) => write!(f, "FAIL with I/O err"),
            Self::ExitFail(e) => write!(f, "FAIL {e}"),
        }
    }
}

impl<T> CmdResult<T> {
    pub fn egress<'a, U: Updater>(self, runner: &mut ChaseRunner<'a, U>) -> anyhow::Result<Self> {
        if self.is_err() && runner.chase.stop_mode == ChaseStopMode::FirstFailure {
            // Stop the guse chase on this loop
            runner
                .stop_flag
                .store(true, std::sync::atomic::Ordering::SeqCst);
        };
        match &self {
            Self::Ok(_) => {}
            Self::Err(e) => {
                let _ = runner.reporter.update("Run failed due to an I/O error.\n");
                let _ = runner.reporter.update(e);
            }
            Self::ExitFail(e) => {
                let _ = runner
                    .reporter
                    .update("Run failed with non-zero exit status\n");
                if let Some(code) = e.code() {
                    let _ = runner.reporter.update(&format!("Exit code: {code}\n"));
                } else {
                    let _ = runner.reporter.update("Terminated by signal.\n");
                    // !!! Also terminate the run on this loop
                    runner
                        .stop_flag
                        .store(true, std::sync::atomic::Ordering::SeqCst);
                }
            }
        }
        Ok(self)
    }

    fn is_err(&self) -> bool {
        matches!(self, Self::Err(_)) || matches!(self, Self::ExitFail(_))
    }
}

impl<'a, R: Updater> ChaseRunner<'a, R> {
    pub fn run_command_on_snap(&mut self, path: &Path, command: &str) -> CmdResult<()> {
        let parts = match shell_words::split(command) {
            Ok(p) => p,
            Err(_) => return CmdResult::Err("Error parsing command.\n".to_string()),
        };
        let Some((prog, args)) = parts.split_first() else {
            return CmdResult::Err(format!("Could not parse chase command: {command}"));
        };
        let mut command = Command::new(prog);
        command.current_dir(path).args(args);

        let mut job = match Job::spawn(command) {
            CmdResult::Ok(val) => val,
            CmdResult::Err(e) => return CmdResult::Err(e),
            CmdResult::ExitFail(e) => return CmdResult::ExitFail(e),
        };

        let mut out_lines = Vec::new();

        let Some(out) = job.child.stdout.take() else {
            return CmdResult::Err("Could not read stdout.\n".to_string());
        };
        let Some(err) = job.child.stderr.take() else {
            return CmdResult::Err("Could not read stderr.\n".to_string());
        };

        let (tx, rx) = crossbeam_channel::unbounded::<LogLine>();

        let mut interrupted = false;

        std::thread::scope(|s| {
            {
                let tx = tx.clone();
                s.spawn(move || {
                    let mut reader = BufReader::new(out);
                    let mut buf = Vec::new();
                    while reader.read_until(b'\n', &mut buf).unwrap_or(0) != 0 {
                        let line = LogLine::new(&buf);
                        let _ = tx.send(line);
                        buf.clear();
                    }
                });
            }
            {
                let tx = tx.clone();
                s.spawn(move || {
                    let mut reader = BufReader::new(err);
                    let mut buf = Vec::new();
                    while reader.read_until(b'\n', &mut buf).unwrap_or(0) != 0 {
                        let line = LogLine::new(&buf);
                        let _ = tx.send(line);
                        buf.clear();
                    }
                });
            }
            drop(tx);

            while let Ok(line) = rx.recv_timeout(Duration::from_secs(5)) {
                out_lines.push(line.clone());
                let _ = self.report(str::from_utf8(&line.line).unwrap());
                let status = self.stop_flag.load(std::sync::atomic::Ordering::Relaxed);
                if status {
                    job.terminate();
                    interrupted = true;
                    return;
                };
            }
        });

        if !interrupted {
            match job.child.wait() {
                Ok(a) if a.success() => CmdResult::Ok(()),
                Ok(a) => CmdResult::ExitFail(a),
                Err(e) => CmdResult::Err(e.to_string()),
            }
        } else {
            CmdResult::Err("Terminated by ctrl+c".to_string())
        }
    }
}

// https://users.rust-lang.org/t/the-best-ring-buffer-library/58489/5
#[derive(Debug, Clone)]
pub struct RingBuffer<T> {
    inner: VecDeque<T>,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: VecDeque::with_capacity(capacity),
        }
    }

    pub fn push(&mut self, item: T) {
        while self.inner.len() >= self.inner.capacity() {
            self.inner.pop_front();
        }
        self.inner.push_back(item);
    }

    pub fn pop(&mut self) -> Option<T> {
        self.inner.pop_front()
    }
}

impl<T> Deref for RingBuffer<T> {
    type Target = VecDeque<T>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
