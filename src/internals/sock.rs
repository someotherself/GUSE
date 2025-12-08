use std::{
    io::{BufRead, BufReader, ErrorKind, Read, Write, stdout},
    os::unix::{
        fs::{FileExt, FileTypeExt, PermissionsExt},
        net::{UnixListener, UnixStream},
    },
    path::{Path, PathBuf},
    thread,
};

use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{
    fs::{
        GitFs,
        builds::{
            chase::{ChaseId, start_chase, start_chase_connection},
            chase_handle::{ChaseHandle, ChaseState},
            logger::CmdResult,
            reporter::Updater,
        },
    },
    mount::GitFsAdapter,
};

const LUA_TEMPLATE: &str = include_str!("../.././src/fs/builds/chase.lua");

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
    RepoDelete {
        name: &'a str,
    },
    RepoUpdate {
        name: &'a str,
        remote: Option<String>,
    },
    Chase {
        repo: &'a str,
        build: &'a str,
        log: bool,
        chase_id: ChaseId,
    },
    NewScript {
        repo: &'a str,
        build: &'a str,
    },
    RemoveScript {
        repo: &'a str,
        build: &'a str,
    },
    RenameScript {
        repo: &'a str,
        old_build: &'a str,
        new_build: &'a str,
    },
    StopChase {
        id: ChaseId,
    },
    Connect,
    Status,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum ControlRes {
    Ok,
    Accept { id: ChaseId },
    // Used to print progress to the user cli
    Update { message: Vec<u8> },
    // Used to print progress during a build/compilatin - re-drawing the last 5 lines
    ChaseStop,
    Draw { message: Vec<Vec<u8>> },
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
                            if let Some(ioe) = e.downcast_ref::<std::io::Error>() {
                                if ioe.kind() == std::io::ErrorKind::BrokenPipe
                                    || ioe.kind() == std::io::ErrorKind::ConnectionReset
                                {
                                    tracing::error!("control client disconnected");
                                }
                            } else {
                                tracing::error!(e = %e, "Control client error");
                            }
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
                let name = name.strip_suffix("/").unwrap_or(name);
                let fs = inner.getfs();
                let Ok(()) = fs.delete_repo(name) else {
                    stream.update(&format!("Repo {name} does not exist!\n"))?;
                    return Ok(ControlRes::Ok);
                };
                let repo_path = fs.repos_dir.join(name);
                if repo_path.exists() {
                    std::fs::remove_dir_all(&repo_path)?;
                }
                fs.delete_repo(name)?;
                stream.update(&format!("Removed repo at {}\n", repo_path.display()))?;
                Ok(ControlRes::Ok)
            }
            ControlReq::RepoUpdate { name, remote } => {
                let name = name.strip_suffix("/").unwrap_or(name);
                let fs = inner.getfs();
                if let Err(e) = fs.update_repo(name, remote) {
                    stream.update(&format!("Error fetching repo {name}: {e}\n"))?;
                    return Ok(ControlRes::Ok);
                }
                stream.update(&format!("Repo {name} has been fetched!\n"))?;
                Ok(ControlRes::Ok)
            }
            ControlReq::RepoList => {
                stream.update("Not implemented!\n")?;
                Ok(ControlRes::Ok)
            }
            ControlReq::Connect => {
                start_chase_connection(&mut stream)?;
                Ok(ControlRes::Ok)
            }
            ControlReq::Chase {
                repo,
                build,
                log,
                chase_id,
            } => {
                let repo = repo.strip_suffix("/").unwrap_or(repo);
                let fs = inner.getfs();
                let _ = start_chase(&fs, repo, build, &mut stream, log, chase_id);
                Ok(ControlRes::Ok)
            }
            ControlReq::StopChase { id } => {
                if let CmdResult::Err(e) = ChaseHandle::set_stop_flag(id) {
                    let _ =
                        stream.update(&format!("Chase id not found. Could not stop chase: {e}"));
                    return Ok(ControlRes::Ok);
                };

                let Some(handle) = ChaseHandle::get_handle(&id) else {
                    let _ = stream.update("Chase id not found. Could not stop chase.");
                    return Ok(ControlRes::Ok);
                };

                let mut state = handle.state.lock();
                handle
                    .cv
                    .wait_while(&mut state, |s| !matches!(*s, ChaseState::Stopped));

                Ok(ControlRes::ChaseStop)
            }
            ControlReq::NewScript { repo, build } => {
                let repo_name = repo.strip_suffix("/").unwrap_or(repo);
                let fs = inner.getfs();
                let Some(repo_entry) = fs.repos_map.get(repo_name) else {
                    stream.update(&format!("Repo {repo} does not exist!\n"))?;
                    return Ok(ControlRes::Ok);
                };
                let repo = fs.get_repo(GitFs::repo_id_to_ino(*repo_entry.value()))?;
                let script_dir = &repo.chase_dir.join(build);
                match std::fs::create_dir(script_dir) {
                    Ok(_) => {}
                    Err(e) if e.kind() == ErrorKind::AlreadyExists => {
                        stream.update(&format!("Script {build} already exists!\n"))?;
                        return Ok(ControlRes::Ok);
                    }
                    Err(e) => {
                        stream.update(&format!("Error creating build script: {e}\n"))?;
                        return Ok(ControlRes::Ok);
                    }
                }
                let script_path = script_dir.join("chase.lua");
                let script_file = match std::fs::File::create_new(script_path) {
                    Ok(f) => f,
                    Err(e) => {
                        stream.update(&format!("Error creating build script: {e}\n"))?;
                        return Ok(ControlRes::Ok);
                    }
                };
                script_file.write_at(LUA_TEMPLATE.as_bytes(), 0)?;
                Ok(ControlRes::Ok)
            }
            ControlReq::RemoveScript { repo, build } => {
                let repo_name = repo.strip_suffix("/").unwrap_or(repo);
                let fs = inner.getfs();
                let Some(repo_entry) = fs.repos_map.get(repo_name) else {
                    stream.update(&format!("Repo {repo} does not exist!\n"))?;
                    return Ok(ControlRes::Ok);
                };
                let repo = fs.get_repo(GitFs::repo_id_to_ino(*repo_entry.value()))?;
                let script_dir = &repo.chase_dir.join(build);
                match std::fs::remove_dir_all(script_dir) {
                    Ok(_) => stream.update(&format!("Script {build} succesfully removed\n"))?,
                    Err(e) if e.kind() == ErrorKind::NotFound => {
                        stream.update(&format!("Script {build} does not exist!\n"))?
                    }
                    Err(e) => stream.update(&format!("Error removing {build}: {e}\n"))?,
                }
                Ok(ControlRes::Ok)
            }
            ControlReq::RenameScript {
                repo,
                old_build,
                new_build,
            } => {
                let repo_name = repo.strip_suffix("/").unwrap_or(repo);
                let fs = inner.getfs();
                let Some(repo_entry) = fs.repos_map.get(repo_name) else {
                    stream.update(&format!("Repo {repo} does not exist!\n"))?;
                    return Ok(ControlRes::Ok);
                };
                let repo = fs.get_repo(GitFs::repo_id_to_ino(*repo_entry.value()))?;
                let old_script_dir = &repo.chase_dir.join(old_build);
                let new_script_dir = &repo.chase_dir.join(new_build);
                if !old_script_dir.exists() {
                    stream.update(&format!("Script {old_build} does not exist!\n"))?;
                    return Ok(ControlRes::Ok);
                }
                if new_script_dir.exists() {
                    stream.update(&format!("Script {new_build} already exists!\n"))?;
                    return Ok(ControlRes::Ok);
                }
                match std::fs::rename(old_script_dir, new_script_dir) {
                    Ok(_) => {}
                    Err(e) => stream.update(&format!("Error renaming scripts: {e}\n"))?,
                }
                Ok(ControlRes::Ok)
            }
            ControlReq::Status => {
                stream.update("Not implemented!\n")?;
                Ok(ControlRes::Ok)
            }
        }
    })()
    .unwrap_or_else(|e| {
        let _ = stream.update(&format!("Socket connection error: {e}"));
        ControlRes::Ok
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
                // Redirected to ControlRes::Update.
                // TODO: Fix.
                // Not printing reliably.
                ControlRes::Draw { message } => {
                    let mut out = stdout();
                    let len = message.len();
                    for _ in 0..len {
                        write!(out, "\x1b[1A")?;
                    }
                    for (i, line) in message.iter().enumerate() {
                        write!(out, "\x1b[2K\x1b[G")?; // Clear line, return to 0
                        let mut text = String::from_utf8_lossy(line).to_string();
                        while text.ends_with(['\n', '\r']) {
                            text.pop();
                        }
                        if i + 1 < len {
                            writeln!(out, "{}", text)?;
                        } else {
                            write!(out, "{}", text)?;
                        }
                    }
                    out.flush()?;
                }
                ControlRes::Accept { id } => return Ok(ControlRes::Accept { id }),
                ControlRes::ChaseStop => {}
                other => {
                    println!("Ending GUSE command");
                    final_res = Some(other);
                }
            }
        }

        if final_res.is_some() {
            break;
        }
    }
    final_res.ok_or_else(|| anyhow::anyhow!("daemon sent no final response"))
}
