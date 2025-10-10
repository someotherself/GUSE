use std::{
    io::{Read, Write},
    os::unix::{
        fs::PermissionsExt,
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    thread,
};

use anyhow::{Context, anyhow};
use serde::{Deserialize, Serialize};
use tracing::{Level, instrument};

use crate::mount::GitFsAdapter;

pub fn socket_path() -> anyhow::Result<PathBuf> {
    let home = std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or(PathBuf::from("/tmp"));
    let sock_dir = home.join(".local").join("share").join("guse");
    std::fs::create_dir_all(&sock_dir)?;
    Ok(sock_dir.join("control.sock"))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum ControlReq {
    RepoList,
    RepoDelete { name: String },
    Status,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ControlRes {
    Ok,
    Error { error: String },
    RepoList { repos: Vec<String> },
    Status { running: bool, mount_point: String },
}

pub fn start_control_server(
    fs: GitFsAdapter,
    socket_path: PathBuf,
    mountpoint: String,
) -> anyhow::Result<()> {
    let listener = bind_socket(&socket_path)?;

    thread::spawn(move || {
        for incomming in listener.into_iter() {
            match incomming {
                Ok(stream) => {
                    let fs = fs.clone();
                    let mp = mountpoint.clone();
                    thread::spawn(move || {
                        if let Err(e) = handle_client(fs, stream, mp) {
                            tracing::error!(e = %e, "Control client error");
                        }
                    });
                }
                Err(e) => tracing::error!(e = %e, "Control accept error"),
            }
        }
    });
    Ok(())
}

#[instrument(level = "debug", skip(inner), ret(level = Level::DEBUG), err(Display))]
#[allow(unused_variables)]
fn handle_client(
    inner: GitFsAdapter,
    mut stream: UnixStream,
    mount_point: String,
) -> anyhow::Result<()> {
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    let req: ControlReq = serde_json::from_slice(&buf)?;

    let res: ControlRes = (|| -> anyhow::Result<ControlRes> {
        match req {
            ControlReq::RepoDelete { name } => {
                // TODO. Make a 2 step process, with confirmation.
                let fs = inner.getfs();
                let Ok(_) = fs.delete_repo(&name) else {
                    return Ok(ControlRes::Ok);
                };
                let repo_path = fs.repos_dir.join(&name);
                if repo_path.exists() {
                    println!("Deleting repo at {}", repo_path.display());
                    std::fs::remove_dir_all(&repo_path)
                        .with_context(|| format!("remove_dir_all({})", repo_path.display()))?;
                }
                fs.delete_repo(&name)?;
                Ok(ControlRes::Ok)
            }
            ControlReq::RepoList => {
                dbg!("Not implemented!");
                tracing::info!("Not implemented!");
                Ok(ControlRes::Ok)
            }
            ControlReq::Status => {
                dbg!("Not implemented!");
                tracing::info!("Not implemented!");
                Ok(ControlRes::Ok)
            }
        }
    })()
    .unwrap_or_else(|e| ControlRes::Error {
        error: e.to_string(),
    });

    let out = serde_json::to_vec(&res)?;
    stream.write_all(&out)?;
    Ok(())
}

fn bind_socket(socket_path: &Path) -> anyhow::Result<UnixListener> {
    if socket_path.exists() {
        match UnixStream::connect(socket_path) {
            Ok(_) => anyhow::bail!("Already running at {}", socket_path.display()),
            Err(_) => std::fs::remove_file(socket_path)?,
        }
    }
    let listener = UnixListener::bind(socket_path)?;
    let mut p = std::fs::metadata(socket_path)?.permissions();
    p.set_mode(0o600);
    std::fs::set_permissions(socket_path, p)?;
    Ok(listener)
}

#[instrument(level = "debug", ret(level = Level::DEBUG), err(Display))]
pub fn send_req(sock: &Path, req: &ControlReq) -> anyhow::Result<ControlRes> {
    let mut s = UnixStream::connect(sock).map_err(|_| anyhow!("Daemon not running!"))?;
    let data = serde_json::to_vec(req)?;
    s.write_all(&data)?;
    s.shutdown(std::net::Shutdown::Write)?;
    let mut resp_buf = vec![];
    s.read_to_end(&mut resp_buf)?;
    let resp: ControlRes = serde_json::from_slice(&resp_buf).map_err(|e| {
        anyhow::anyhow!(
            "invalid response from daemon: {e}. raw={:?}",
            String::from_utf8_lossy(&resp_buf)
        )
    })?;
    Ok(resp)
}
