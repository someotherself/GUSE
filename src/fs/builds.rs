use std::{path::PathBuf, sync::atomic::AtomicUsize};

pub struct BuildSession {
    folder: PathBuf,
    open_count: AtomicUsize,
}
