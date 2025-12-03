use std::{
    collections::VecDeque,
    io::{BufRead, BufReader},
    ops::Deref,
    path::Path,
    process::{Command, ExitStatus, Stdio},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use crate::fs::builds::{
    chase_runner::ChaseRunner,
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
pub enum CmdResult {
    Ok(()),
    Err(String),
    ExitFail(ExitStatus),
}

impl CmdResult {
    pub fn egress<'a, U: Updater>(self, runner: &mut ChaseRunner<'a, U>) -> anyhow::Result<Self> {
        if self.is_err() && runner.chase.stop_mode == ChaseStopMode::FirstFailure {
            // Stop the guse chase on this loop
            runner.run = false
        };
        match &self {
            Self::Ok(_) => {}
            Self::Err(e) => {
                let _ = runner.reporter.update("Run failed due to an I/O error.\n");
                runner.reporter.update(&e)?;
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
                    runner.run = false;
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
    pub fn run_command_on_snap(&mut self, path: &Path, command: &str) -> CmdResult {
        let mut split = command.split_whitespace();
        let Some(prog) = split.next() else {
            return CmdResult::Err("Error parsing command.\n".to_string());
        };
        let args: Vec<&str> = split.collect();
        let output = Command::new(prog)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .current_dir(path)
            .args(args)
            .spawn();

        let mut output = match output {
            Ok(o) => o,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return CmdResult::Err(format!("Command not found: {}\n", prog));
                }
                return CmdResult::Err(format!("Failed to run {}: {}\n", prog, e));
            }
        };

        let mut out_lines = Vec::new();
        let mut _log_buf: RingBuffer<LogLine> = RingBuffer::new(5);

        let Some(out) = output.stdout.take() else {
            return CmdResult::Err("Could not read stdout.\n".to_string());
        };
        let Some(err) = output.stderr.take() else {
            return CmdResult::Err("Could not read stderr.\n".to_string());
        };

        let (tx, rx) = crossbeam_channel::unbounded::<LogLine>();

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
                // log_buf.push(line);
                // let _ = reporter.refresh_cli(log_buf.iter().cloned().collect());
            }
        });

        match output.wait() {
            Ok(a) if a.success() => {
                // out_lines.sort_by_key(|a| a.t_stmp);
                // let out = out_lines.into_iter().flat_map(|a| a.line).collect();
                CmdResult::Ok(())
            }
            Ok(a) => CmdResult::ExitFail(a),
            Err(e) => CmdResult::Err(e.to_string()),
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
