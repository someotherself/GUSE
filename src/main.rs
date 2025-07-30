#![allow(dead_code)]

use clap::{Arg, ArgMatches, command, crate_authors, crate_version};
use tracing::Level;

mod fs;
mod mount;
mod repo;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    log_init();
    let matches = handle_cli_args();
    start_app(&matches).await?;
    Ok(())
}

async fn start_app(matches: &ArgMatches) -> anyhow::Result<()> {
    run_mount(&matches).await?;
    Ok(())
}

fn handle_cli_args() -> ArgMatches {
command!()
        .version(crate_version!())
        .author(crate_authors!())
        .arg(
            Arg::new("path")
                .required(true)
                .short('m')
                .long("mount-point")
                .value_name("MOUNT_POINT")
                .help("The path where FUSE will be mounted."),
        )
        .get_matches()
}

async fn run_mount(_matches: &ArgMatches) -> anyhow::Result<()> {
    Ok(())
}

fn log_init() {
    let subscriber = tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .finish();
    tracing::subscriber::set_global_default(subscriber).unwrap();
}
