use std::{
    io::{BufRead, BufReader, Read, Write},
    os::unix::{
        fs::{FileTypeExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    thread,
};

use anyhow::{Context, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{fs::builds::chase::start_chase, mount::GitFsAdapter};

pub fn socket_path() -> anyhow::Result<PathBuf> {
    let home = std::env::var_os("HOME").map_or(PathBuf::from("/tmp"), PathBuf::from);
    let sock_dir = home.join(".local").join("share").join("guse");
    std::fs::create_dir_all(&sock_dir)?;
    Ok(sock_dir.join("control.sock"))
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum ControlReq<'a> {
    RepoList,
    RepoDelete { name: &'a str },
    Chase { repo: &'a str, build: &'a str },
    Status,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ControlRes {
    Ok,
    // User to print error messages to the user cli
    Error { error: String },
    // Used to print progress to the user cli
    Update { message: Vec<u8> },
    RepoList { repos: Vec<String> },
    Status { running: bool, mount_point: String },
}

pub fn start_control_server(
    fs: GitFsAdapter,
    socket_path: &Path,
    mountpoint: String,
) -> anyhow::Result<()> {
    let listener = bind_socket(socket_path)?;

    thread::spawn(move || {
        for incomming in &listener {
            match incomming {
                Ok(stream) => {
                    let fs = fs.clone();
                    let mp = mountpoint.clone();
                    thread::spawn(move || {
                        if let Err(e) = handle_client(&fs, stream, &mp) {
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

fn handle_client(
    inner: &GitFsAdapter,
    mut stream: UnixStream,
    _mount_point: &str,
) -> anyhow::Result<()> {
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
    let req: ControlReq = serde_json::from_slice(&buf)?;

    let res: ControlRes = (|| -> anyhow::Result<ControlRes> {
        match req {
            ControlReq::RepoDelete { name } => {
                // TODO. Make a 2 step process, with confirmation.
                let name = name.strip_suffix("/").unwrap_or(name);
                let fs = inner.getfs();
                let Ok(()) = fs.delete_repo(name) else {
                    return Ok(ControlRes::Ok);
                };
                let repo_path = fs.repos_dir.join(name);
                if repo_path.exists() {
                    println!("Deleting repo at {}", repo_path.display());
                    std::fs::remove_dir_all(&repo_path)
                        .with_context(|| format!("remove_dir_all({})", repo_path.display()))?;
                }
                fs.delete_repo(name)?;
                Ok(ControlRes::Ok)
            }
            ControlReq::RepoList => {
                dbg!("Not implemented!");
                tracing::info!("Not implemented!");
                Ok(ControlRes::Ok)
            }
            ControlReq::Chase { repo, build } => {
                let repo = repo.strip_suffix("/").unwrap_or(repo);
                let fs = inner.getfs();
                start_chase(&fs, repo, build, &mut stream)?;
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
    match UnixListener::bind(socket_path) {
        Ok(listener) => {
            let mut p = std::fs::metadata(socket_path)?.permissions();
            p.set_mode(0o600);
            std::fs::set_permissions(socket_path, p)?;
            Ok(listener)
        }
        Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
            match UnixStream::connect(socket_path) {
                Ok(_) => {
                    anyhow::bail!("Already running at {}", socket_path.display());
                }
                Err(_) => {
                    if let Ok(meta) = std::fs::symlink_metadata(socket_path) {
                        if meta.file_type().is_socket() {
                            let _ = std::fs::remove_file(socket_path);
                        } else {
                            anyhow::bail!(
                                "Refusing to remove non-socket at {}",
                                socket_path.display()
                            );
                        }
                    }
                    let listener = UnixListener::bind(socket_path)?;
                    let mut p = std::fs::metadata(socket_path)?.permissions();
                    p.set_mode(0o600);
                    std::fs::set_permissions(socket_path, p)?;
                    Ok(listener)
                }
            }
        }
        Err(e) => Err(e.into()),
    }
}

pub fn send_req(sock: &Path, req: &ControlReq) -> anyhow::Result<ControlRes> {
    let mut s = UnixStream::connect(sock).map_err(|_| anyhow!("GUSE is not running!"))?;
    let data = serde_json::to_vec(req)?;
    s.write_all(&data)?;
    s.shutdown(std::net::Shutdown::Write)?;
    let mut reader = BufReader::new(s);
    let mut final_res: Option<ControlRes> = None;

    loop {
        let mut buf = Vec::new();
        let n = reader.read_until(b'\n', &mut buf)?;
        if n == 0 {
            break;
        }
        let iter = Deserializer::from_slice(&buf).into_iter::<ControlRes>();

        for item in iter {
            let msg = item.map_err(|e| {
                anyhow::anyhow!(
                    "invalid response from daemon: {e}. raw={:?}",
                    String::from_utf8_lossy(&buf)
                )
            })?;
            match msg {
                ControlRes::Update { message } => {
                    print!("{}", String::from_utf8_lossy(&message));
                }
                other => {
                    final_res = Some(other);
                    println!("Ending GUSE command.")
                }
            }
        }

        if final_res.is_some() {
            break;
        }
    }
    final_res.ok_or_else(|| anyhow::anyhow!("daemon sent no final response"))
}
