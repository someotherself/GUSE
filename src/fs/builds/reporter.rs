use std::{io::Write, os::unix::net::UnixStream};

use crate::internals::sock::ControlRes;

pub trait Reporter {
    fn update(&mut self, msg: &str) -> anyhow::Result<()>;

    fn progress_error(&mut self, msg: &str) -> anyhow::Result<()>;
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

    fn progress_error(&mut self, msg: &str) -> anyhow::Result<()> {
        self.update(msg)
    }
}

pub fn color_green(s: &str) -> String {
    format!("\x1b[32m{s}\x1b[0m")
}
