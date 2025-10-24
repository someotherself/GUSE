use std::{
    path::PathBuf,
    sync::{Arc, Mutex, OnceLock, mpsc},
    time::{Duration, Instant},
};

use anyhow::{Context, anyhow};
use tempfile::TempDir;
use thread_local::ThreadLocal;

use crate::{
    fs::GitFs,
    mount::{MountPoint, mount_fuse},
};

pub static GITFS_SETUP_RESULT: ThreadLocal<Mutex<Option<SetupResult>>> = ThreadLocal::new();
pub static FUSE_SETUP_RESULT: ThreadLocal<Mutex<Option<FuseSetupResult>>> = ThreadLocal::new();

#[derive(Debug, Clone)]
pub struct GitFsTestSetup {
    #[allow(dead_code)]
    pub key: &'static str,
    pub read_only: bool,
}

pub struct SetupResult {
    pub fs: Option<Arc<GitFs>>,
    _tmpdir: TempDir,
}

fn git_fs_setup(setup: GitFsTestSetup) -> SetupResult {
    let tmpdir = tempfile::Builder::new()
        .prefix(setup.key)
        .tempdir()
        .expect("could not create tmpdir");

    let fs = GitFs::new(
        tmpdir.path().to_path_buf(),
        setup.read_only,
        Arc::new(OnceLock::new()),
    )
    .expect("failed to init GitFs");

    SetupResult {
        fs: Some(fs),
        _tmpdir: tmpdir,
    }
}

pub fn run_git_fs_test<T>(init: GitFsTestSetup, t: T) -> anyhow::Result<()>
where
    T: Fn(&Mutex<Option<SetupResult>>) -> anyhow::Result<()>,
{
    let s = GITFS_SETUP_RESULT.get_or(|| Mutex::new(None));
    {
        let mut s = s.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        *s = Some(git_fs_setup(init));
    }
    t(s)?;

    {
        let mut guard = s.lock().map_err(|_| anyhow!("Lock poisoned"))?;
        *guard = None;
    }

    Ok(())
}

pub fn get_fs() -> Arc<GitFs> {
    let fs = GITFS_SETUP_RESULT.get_or(|| Mutex::new(None));
    let mut fs = fs.lock().unwrap();
    fs.as_mut().unwrap().fs.clone().unwrap()
}

#[derive(Debug, Clone)]
pub struct FuseTestSetup {
    #[allow(dead_code)]
    pub key: &'static str,
}

pub struct FuseSetupResult {
    tmpdir: TempDir,
    pub mountpoint: MountPoint,
    setup: FuseTestSetup,
}

struct MountGuard {
    mount_dir: PathBuf,
    th: Option<std::thread::JoinHandle<anyhow::Result<()>>>,
    rx_done: mpsc::Receiver<anyhow::Result<()>>,
}

impl MountGuard {
    fn spawn(opts: MountPoint) -> anyhow::Result<(Self, PathBuf)> {
        let (tx_done, rx_done) = mpsc::channel();
        let mount_dir = opts.mountpoint.clone();
        let th = std::thread::spawn(move || {
            let res = mount_fuse(opts).context("mount_fuse failed");
            let _ = tx_done.send(
                res.as_ref()
                    .map(|_| ())
                    .map_err(|e| anyhow::anyhow!("{e:#}")),
            );
            res
        });
        Ok((
            Self {
                mount_dir: mount_dir.clone(),
                th: Some(th),
                rx_done,
            },
            mount_dir,
        ))
    }
}

impl Drop for MountGuard {
    fn drop(&mut self) {
        let candidates = [
            ("fusermount3", &["-u"][..]),
            ("fusermount", &["-u"][..]),
            ("umount", &[][..]),
        ];
        for (bin, args) in candidates {
            let status = std::process::Command::new(bin)
                .args(args)
                .arg(&self.mount_dir)
                .status();
            if matches!(status, Ok(s) if s.success()) {
                break;
            }
        }
        if let Some(h) = self.th.take() {
            let _ = h.join();
        }
    }
}

fn wait_until_fuse_mounted_or_failed(
    mount_dir: &std::path::Path,
    rx_done: &std::sync::mpsc::Receiver<anyhow::Result<()>>,
    timeout: Duration,
) -> anyhow::Result<()> {
    let start = Instant::now();
    loop {
        if let Ok(res) = rx_done.try_recv() {
            res?;
            anyhow::bail!("mount thread ended unexpectedly without a live FUSE mount");
        }

        if is_fuse_mount(mount_dir) {
            return Ok(());
        }

        if start.elapsed() > timeout {
            let _ = dump_mountinfo_hint(mount_dir);
            anyhow::bail!("timeout: FUSE mount never appeared in /proc/self/mountinfo");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn is_fuse_mount(p: &std::path::Path) -> bool {
    if let Ok(mi) = std::fs::read_to_string("/proc/self/mountinfo") {
        let needle = p.to_string_lossy();
        for line in mi.lines() {
            if let Some((left, right)) = line.split_once(" - ")
                && left.split_whitespace().nth(4) == Some(needle.as_ref())
            {
                let fstype = right.split_whitespace().next().unwrap_or("");
                if fstype.starts_with("fuse") {
                    return true;
                }
            }
        }
    }
    false
}

fn dump_mountinfo_hint(p: &std::path::Path) -> std::io::Result<()> {
    let mi = std::fs::read_to_string("/proc/self/mountinfo")?;
    eprintln!("--- mountinfo (looking for {}) ---", p.display());
    for l in mi.lines() {
        if l.contains(" fuse") || l.contains(" FUSE") || l.contains(p.to_string_lossy().as_ref()) {
            eprintln!("{l}");
        }
    }
    eprintln!("----------------------------------");
    Ok(())
}
const MOUNT: &str = "MOUNT";
const DATA_DIR: &str = "data_dir";

fn fuse_test_setup(setup: FuseTestSetup) -> anyhow::Result<FuseSetupResult> {
    let cwd = std::env::current_dir().unwrap();
    let tmpdir = tempfile::Builder::new()
        .prefix(setup.key)
        .tempdir_in(cwd)
        .expect("could not create tmpdir");

    let mountpoint = tmpdir.path().join(MOUNT);
    let repos_dir = tmpdir.path().join(DATA_DIR);

    let mountpoint: MountPoint = MountPoint {
        mountpoint,
        repos_dir,
        read_only: false,
        allow_root: false,
        allow_other: false,
        disable_socket: true,
    };

    let res = FuseSetupResult {
        tmpdir,
        mountpoint,
        setup,
    };
    Ok(res)
}

pub fn run_fuse_fs_test<T>(init: FuseTestSetup, t: T) -> anyhow::Result<()>
where
    T: Fn(&FuseSetupResult) -> anyhow::Result<()>,
{
    let ctx = fuse_test_setup(init)?;

    let (guard, mount_root) = MountGuard::spawn(ctx.mountpoint.clone())?;

    wait_until_fuse_mounted_or_failed(&mount_root, &guard.rx_done, Duration::from_secs(10))?;

    let res = t(&ctx);

    drop(guard);
    res
}
