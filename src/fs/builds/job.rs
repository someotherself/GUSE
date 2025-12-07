use std::{
    os::unix::process::CommandExt,
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

use crate::fs::builds::logger::CmdResult;

pub struct Job {
    pub child: Child,
    pub pgid: libc::pid_t,
}

impl Job {
    pub fn spawn(mut cmd: Command) -> CmdResult<Self> {
        // https://users.rust-lang.org/t/prevent-program-from-exiting-on-child-sigint/7685/8
        let output = unsafe {
            cmd.stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .pre_exec(|| {
                    // Make the child process its own process group leader
                    libc::setpgid(0, 0);
                    Ok(())
                })
                .spawn()
        };

        let child = match output {
            Ok(o) => o,
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    return CmdResult::Err(format!("Command not found: {:?}\n", cmd));
                }
                return CmdResult::Err(format!("Failed to run {:?}: {}\n", cmd, e));
            }
        };

        let pgid = unsafe { libc::getpgid(child.id() as libc::pid_t) };
        if pgid < 0 {
            return CmdResult::Err(format!("{}", std::io::Error::last_os_error()));
        }

        CmdResult::Ok(Self { child, pgid })
    }

    pub fn terminate(&mut self) -> CmdResult<()> {
        let _ = unsafe { libc::kill(-(self.child.id() as i32), libc::SIGKILL) };

        let stop_time = Instant::now() + Duration::from_secs(5);
        loop {
            match self.child.try_wait() {
                Ok(Some(_)) => return CmdResult::Ok(()),
                Ok(None) => {
                    if Instant::now() >= stop_time {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(50));
                }
                Err(_) => return CmdResult::Ok(()),
            }
        }
        let _ = unsafe { libc::kill(-(self.child.id() as i32), libc::SIGKILL) };
        let _ = self.child.wait();
        CmdResult::Ok(())
    }
}
