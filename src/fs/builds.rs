use std::sync::atomic::{AtomicBool, AtomicUsize};

use tempfile::TempDir;

pub struct BuildSession {
    pub folder: TempDir,
    pub open_count: AtomicUsize,
    pub pinned: AtomicBool,
}
