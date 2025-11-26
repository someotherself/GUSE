use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Command, Stdio},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::fs::builds::reporter::Reporter;

#[derive(Clone)]
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

pub fn run_command_on_snap<R: Reporter>(
    path: &Path,
    command: &str,
    reporter: &mut R,
) -> Option<Vec<u8>> {
    let mut split = command.split_whitespace();
    let prog = split.next()?;
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
                return Some(format!("Command not found: {}\n", prog).into_bytes());
            }
            return Some(format!("Failed to run {}: {}\n", prog, e).into_bytes());
        }
    };

    let mut out_lines = Vec::new();

    let out = output.stdout.take()?;
    let err = output.stderr.take()?;

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

        while let Ok(line) = rx.recv() {
            out_lines.push(line.clone());
            let _ = reporter.refresh_cli(line);
        }
    });

    out_lines.sort_by_key(|a| a.t_stmp);
    let out = out_lines.into_iter().flat_map(|a| a.line).collect();

    Some(out)
}
