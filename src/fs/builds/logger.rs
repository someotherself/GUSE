use std::{io::{BufRead, BufReader}, path::Path, process::{Command, Stdio}, time::{SystemTime, UNIX_EPOCH}};

use parking_lot::Mutex;

struct LogLine {
    pub t_stmp: u128,
    line: Vec<u8>,
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

pub fn run_command_on_snap(path: &Path, command: &str) -> Option<Vec<u8>> {
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

    let combined = Mutex::new(Vec::new());

    let out = output.stdout.take()?;
    let err = output.stderr.take()?;

    std::thread::scope(|s| {
        let combined = &combined;
        {
            s.spawn(|| {
                let mut reader = BufReader::new(out);
                let mut buf = Vec::new();
                while reader.read_until(b'\n', &mut buf).unwrap() != 0 {
                    combined.lock().push(LogLine::new(&buf));
                    buf.clear();
                }
            });
        }
        let combined = &combined;
        {
            s.spawn(|| {
                let mut reader = BufReader::new(err);
                let mut buf = Vec::new();
                while reader.read_until(b'\n', &mut buf).unwrap() != 0 {
                    combined.lock().push(LogLine::new(&buf));
                    buf.clear();
                }
            });
        }
    });

    let mut out = combined.into_inner();
    out.sort_by_key(|a| a.t_stmp);
    let out = out.into_iter().flat_map(|a| a.line).collect();

    Some(out)
}
