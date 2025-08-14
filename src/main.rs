#![allow(dead_code)]

use std::{path::PathBuf, thread};

use clap::{Arg, ArgAction, ArgMatches, command, crate_authors, crate_version};

use guse::{logging, mount, tui};

fn main() -> anyhow::Result<()> {
    let matches = handle_cli_args();

    let log_level = matches.get_count("verbose") as u8;
    logging::init_logging(log_level);

    start_app(&matches)?;
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
        .arg(
            Arg::new("mount-point")
                .required(true)
                .short('m')
                .long("mount-point")
                .value_name("MOUNT_POINT")
                .help("The path where FUSE will be mounted."),
        )
        .arg(
            Arg::new("repos-dir")
                .help("The folder where the repositories will be stored")
                .value_name("REPOS_DIR")
                .required(true)
                .index(1),
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
                .long("verbose")
                .action(ArgAction::Count)
                .help("Increase log verbosity (can be used multiple times)"),
        )
        .get_matches()
}

fn run_mount(matches: &ArgMatches) -> anyhow::Result<()> {
    let mountpoint = matches.get_one::<String>("mount-point").unwrap();
    let mountpoint = PathBuf::from(mountpoint);
    let repos_dir = matches.get_one::<String>("repos-dir").unwrap();
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
    let mountpoint = matches.get_one::<String>("mount-point").unwrap();
    let mountpoint = PathBuf::from(mountpoint);
    let repos_dir = matches.get_one::<String>("repos-dir").unwrap();
    let repos_dir = PathBuf::from(repos_dir);
    let read_only = matches.get_flag("read-only");
    let allow_other = matches.get_flag("allow-other");
    let allow_root = matches.get_flag("allow-root");
    let mount_point =
        mount::MountPoint::new(mountpoint, repos_dir, read_only, allow_root, allow_other);
    let handle = thread::spawn(move || {
        tui::run_tui_app().unwrap();
    });
    mount::mount_fuse(mount_point).unwrap();

    handle.join().unwrap();
    Ok(())
}
