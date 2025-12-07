#![allow(dead_code)]

use std::{
    path::PathBuf,
    sync::{Arc, atomic::AtomicBool},
    thread,
};

use anyhow::{anyhow, bail};
use clap::{Arg, ArgAction, ArgMatches, Command, command, crate_authors, crate_version};

use guse::internals::sock::{ControlReq, ControlRes, send_req, socket_path};
use tracing_subscriber::{EnvFilter, filter::Directive};

fn main() -> anyhow::Result<()> {
    let matches = handle_cli_args();

    match matches.subcommand() {
        Some(("run", m)) => {
            let log_level = m.get_count("verbose");
            init_logging(log_level);

            start_app(m)?;
        }
        Some(("repo", m)) => match m.subcommand() {
            Some(("remove", rm)) => {
                let repo_name = rm
                    .get_one::<String>("repo-name")
                    .ok_or_else(|| anyhow!("Cannot parse argument"))?;
                let sock = socket_path()?;
                let req = ControlReq::RepoDelete { name: repo_name };
                send_req(&sock, &req)?;
            }
            Some(("update", up)) => {
                let repo_name = up
                    .get_one::<String>("repo-name")
                    .ok_or_else(|| anyhow!("Cannot parse argument"))?;
                let remote: Option<String> = up.get_one::<String>("remote").cloned();
                let sock = socket_path()?;
                let req = ControlReq::RepoUpdate {
                    name: repo_name,
                    remote,
                };
                send_req(&sock, &req)?;
            }
            _ => {
                dbg!("Wrong command!");
                tracing::error!("Wrong command!")
            }
        },
        Some(("chase", m)) => {
            let sock = socket_path()?;
            let repo = m
                .get_one::<String>("repo")
                .ok_or_else(|| anyhow!("Cannot parse argument"))?;
            let build = m
                .get_one::<String>("build")
                .ok_or_else(|| anyhow!("Cannot parse argument"))?;
            let log = m.get_flag("log");

            // Send connection request
            let conn_req = ControlReq::Connect;
            let accept_res = send_req(&sock, &conn_req)?;

            let ControlRes::Accept { id } = accept_res else {
                bail!("")
            };

            println!("Received ID: {id}");
            let stop_signal = Arc::new(AtomicBool::new(false));
            let signal_clone = stop_signal.clone();

            let _ = ctrlc::set_handler(move || {
                signal_clone.store(true, std::sync::atomic::Ordering::SeqCst);
            });

            let work_sock = sock.clone();
            let repo_clone = repo.clone();
            let build_clone = build.clone();
            let worker = thread::spawn(move || {
                let chase_req = ControlReq::Chase {
                    repo: &repo_clone,
                    build: &build_clone,
                    log,
                    chase_id: id,
                };
                let _ = send_req(&work_sock, &chase_req);
            });

            loop {
                if worker.is_finished() {
                    break;
                }
                if stop_signal.load(std::sync::atomic::Ordering::Relaxed) {
                    let stop_req = ControlReq::StopChase { id };
                    let _ = send_req(&sock.clone(), &stop_req);
                    break;
                }
            }
        }
        Some(("script", m)) => match m.subcommand() {
            Some(("new", s)) => {
                let sock = socket_path()?;
                let repo = s
                    .get_one::<String>("repo")
                    .ok_or_else(|| anyhow!("Cannot parse argument"))?;
                let build = s
                    .get_one::<String>("build")
                    .ok_or_else(|| anyhow!("Cannot parse argument"))?;
                let req = ControlReq::NewScript { repo, build };
                send_req(&sock, &req)?;
            }
            Some(("remove", s)) => {
                let sock = socket_path()?;
                let repo = s
                    .get_one::<String>("repo")
                    .ok_or_else(|| anyhow!("Cannot parse argument"))?;
                let build = s
                    .get_one::<String>("build")
                    .ok_or_else(|| anyhow!("Cannot parse argument"))?;
                let req = ControlReq::RemoveScript { repo, build };
                send_req(&sock, &req)?;
            }
            Some(("rename", s)) => {
                let sock = socket_path()?;
                let repo = s
                    .get_one::<String>("repo")
                    .ok_or_else(|| anyhow!("Cannot parse argument"))?;
                let old_build = s
                    .get_one::<String>("old_build")
                    .ok_or_else(|| anyhow!("Cannot parse argument"))?;
                let new_build = s
                    .get_one::<String>("new_build")
                    .ok_or_else(|| anyhow!("Cannot parse argument"))?;
                let req = ControlReq::RenameScript {
                    repo,
                    old_build,
                    new_build,
                };
                send_req(&sock, &req)?;
            }
            _ => {
                dbg!("Wrong command!");
                tracing::error!("Wrong command!")
            }
        },
        _ => {
            dbg!("Wrong command!");
            tracing::error!("Wrong command!")
        }
    };
    Ok(())
}

fn start_app(matches: &ArgMatches) -> anyhow::Result<()> {
    run_mount(matches)?;
    Ok(())
}

fn handle_cli_args() -> ArgMatches {
    command!()
        .version(crate_version!())
        .author(crate_authors!())
        .subcommand(
            Command::new("run")
                .about("Mount the FUSE filesystem")
                .arg(
                    Arg::new("mount-point")
                        .value_name("MOUNT_POINT")
                        .help("The path where FUSE will be mounted.")
                        .index(1),
                )
                .arg(
                    Arg::new("repos-dir")
                        .help("The folder where the repositories will be stored")
                        .value_name("REPOS_DIR")
                        .index(2),
                )
                .arg(
                    Arg::new("read-only")
                        .long("read-only")
                        .short('s')
                        .action(ArgAction::SetTrue)
                        .requires("mount-point")
                        .help("Set the filesystem read-only."),
                )
                .arg(
                    Arg::new("allow-root")
                        .long("allow-root")
                        .short('r')
                        .action(ArgAction::SetTrue)
                        .requires("mount-point")
                        .help("Allow the root user to access filesystem."),
                )
                .arg(
                    Arg::new("allow-other")
                        .long("allow-other")
                        .short('o')
                        .action(ArgAction::SetTrue)
                        .requires("mount-point")
                        .help("Allow other users to access filesystem."),
                )
                .arg(
                    Arg::new("verbose")
                        .short('v')
                        .action(ArgAction::Count)
                        .global(true)
                        .help("Increase log verbosity (can be used multiple times)"),
                )
                .arg(
                    Arg::new("disable-socket")
                        .short('t')
                        .action(ArgAction::SetTrue)
                        .requires("mount-point")
                        .help("Disables the socket communication, needed for commands such as `guse repo remove <repo-name>`"),
                ),
        )
        .subcommand(
            Command::new("repo")
                .about("Manage repositories in the running daemon")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .subcommand(
                    Command::new("remove")
                        .about("Delete a repository by name")
                        .arg(Arg::new("repo-name").value_name("REPO_NAME").global(true)),
                )
                .subcommand(
                    Command::new("update")
                        .about("Perform a new fetch to update the repository")
                        .arg(Arg::new("repo-name").value_name("REPO_NAME").global(true))
                        .arg(Arg::new("remote").value_name("REMOTE").required(false))
            ),
        )
        .subcommand(
            Command::new("chase")
            .about("Run an automated build for a repo")
            .arg_required_else_help(true)
            .arg(
                Arg::new("repo")
                    .value_name("REPO")
                    .required(true)
                    .help("The repo to run the automated build on")
            )
            .arg(
                Arg::new("build")
                    .value_name("BUILD")
                    .help("The name of the automated build")
                    .required(true)
                )
                .arg(
                    Arg::new("log")
                        .long("log")
                        .short('l')
                        .action(ArgAction::SetTrue)
                        .help("Enable logging chase results to disk (in chase folder).")
                )
        )
        .subcommand(
            Command::new("script")
            .about("Manage GUSE chase scripts")
            .subcommand(
            Command::new("new")
            .about("Add a new GUSE chase script")
            .arg_required_else_help(true)
            .arg(
                Arg::new("repo")
                    .value_name("REPO")
                    .required(true)
                    .help("The repo that the script will be created for")
            )
            .arg(
                Arg::new("build")
                    .value_name("BUILD")
                    .help("The name of the script")
                    .required(true)
            ))
            .subcommand(
                Command::new("remove")
            .arg(
                Arg::new("repo")
                    .value_name("REPO")
                    .required(true)
                    .help("The repo that the script will be created for")
            )
            .arg(
                Arg::new("build")
                    .value_name("BUILD")
                    .help("The name of the script")
                    .required(true)
            ))
            .subcommand(
                Command::new("rename")
            .arg(
                Arg::new("repo")
                    .value_name("REPO")
                    .required(true)
                    .help("The repo that the script will be created for")
            )
            .arg(
                Arg::new("old_build")
                    .value_name("OLD_BUILD")
                    .help("The name of the script")
                    .required(true)
            )
            .arg(
                Arg::new("new_build")
                    .value_name("NEW_BUILD")
                    .help("The old name of the script")
                    .required(true)
            ))
        )
        .get_matches()
}

fn run_mount(matches: &ArgMatches) -> anyhow::Result<()> {
    let mountpoint = matches
        .get_one::<String>("mount-point")
        .ok_or_else(|| anyhow!("Cannot parse argument"))?;
    let mountpoint = PathBuf::from(mountpoint);
    let repos_dir = matches
        .get_one::<String>("repos-dir")
        .ok_or_else(|| anyhow!("Cannot parse argument"))?;
    let repos_dir = PathBuf::from(repos_dir);
    let read_only = matches.get_flag("read-only");
    let allow_other = matches.get_flag("allow-other");
    let allow_root = matches.get_flag("allow-root");
    let disable_socket = matches.get_flag("disable-socket");
    let mount_point = guse::mount::MountPoint::new(
        mountpoint,
        repos_dir,
        read_only,
        allow_root,
        allow_other,
        disable_socket,
    );

    let bg = match guse::mount::mount_fuse(mount_point) {
        Ok(bg) => bg,
        Err(e) => {
            tracing::error!("Failed to mount: {e:?}");
            return Err(e);
        }
    };
    bg.join();

    Ok(())
}

pub fn init_logging(verbosity: u8) {
    let my_level = match verbosity {
        0 => "info",
        1 => "debug",
        _ => "trace",
    };
    let my_crate = "guse";

    let mut filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn,fuser=warn"));

    if let Ok(directive) = format!("{my_crate}={my_level}").parse::<Directive>() {
        filter = filter.add_directive(directive);
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}
