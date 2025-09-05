#![allow(dead_code)]

use std::{path::PathBuf, thread};

use anyhow::anyhow;
use clap::{Arg, ArgAction, ArgMatches, Command, command, crate_authors, crate_version};

use guse::{logging, mount, tui};

fn main() -> anyhow::Result<()> {
    let matches = handle_cli_args();

    match matches.subcommand() {
        Some(("run", m)) => {
            let log_level = m.get_count("verbose") as u8;
            logging::init_logging(log_level);

            start_app(m)?;
        }
        Some(("repo", _)) => {
            todo!()
        }
        _ => unreachable!(),
    };
    Ok(())
}

fn start_app(matches: &ArgMatches) -> anyhow::Result<()> {
    run_mount(matches)?;
    // setup_tui(matches)?;
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
                ),
        )
        .subcommand(
            Command::new("repo")
                .about("Manage repositories in the running daemon")
                .subcommand_required(true)
                .arg_required_else_help(true)
                .short_flag('p')
                .subcommand(
                    Command::new("remove")
                        .about("Delete a repository by name")
                        .arg(Arg::new("name").required(true).value_name("NAME")),
                ),
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
    let mount_point =
        mount::MountPoint::new(mountpoint, repos_dir, read_only, allow_root, allow_other);
    mount::mount_fuse(mount_point)?;
    Ok(())
}

fn setup_tui(matches: &ArgMatches) -> anyhow::Result<()> {
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
    let mount_point =
        mount::MountPoint::new(mountpoint, repos_dir, read_only, allow_root, allow_other);
    let handle = thread::spawn(move || -> anyhow::Result<()> {
        tui::run_tui_app()?;
        Ok(())
    });
    mount::mount_fuse(mount_point)?;

    handle.join().unwrap()?;
    Ok(())
}
