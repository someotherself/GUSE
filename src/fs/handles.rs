use std::sync::atomic::{AtomicU64, Ordering};

use dashmap::DashMap;

use crate::fs::{GitFs, Handle, meta_db::DbWriteMsg};

pub struct FileHandles {
    current_handle: AtomicU64,
    /// (fh, Handle)
    handles: DashMap<u64, Handle>,
    /// (ino, open file handles)
    open_counts: DashMap<u64, AtomicU64>,
}

impl FileHandles {
    pub fn init() -> Self {
        Self {
            current_handle: AtomicU64::new(1),
            handles: DashMap::new(),
            open_counts: DashMap::new(),
        }
    }

    /// Opens a file handle
    pub fn open(&self, handle: Handle) -> anyhow::Result<u64> {
        let fh = self.next_handle();
        let ino = handle.ino;

        self.handles.insert(fh, handle);
        self.register_open(ino);

        Ok(fh)
    }

    /// Closes a file handle
    pub fn close(
        &self,
        fh: u64,
        writer_tx: crossbeam_channel::Sender<DbWriteMsg>,
    ) -> anyhow::Result<bool> {
        let ino = match self.handles.remove(&fh) {
            Some(h) => h.1.ino,
            None => return Ok(false),
        };
        if self.register_close(ino)?.is_some() {
            GitFs::cleanup_entry_with_writemsg(ino.into(), writer_tx)?;
        };
        Ok(true)
    }

    fn register_open(&self, ino: u64) {
        match self.open_counts.entry(ino) {
            dashmap::Entry::Occupied(e) => {
                e.get().fetch_add(1, Ordering::SeqCst);
            }
            dashmap::Entry::Vacant(s) => {
                s.insert(AtomicU64::new(1));
            }
        }
    }

    /// Returns OK(Some(())) if the DB entry needs to be removed from inode_map
    fn register_close(&self, ino: u64) -> anyhow::Result<Option<()>> {
        match self.open_counts.entry(ino) {
            dashmap::Entry::Occupied(e) => {
                let counter = e.get();
                let prev = counter.load(Ordering::Acquire);
                if prev == 0 {
                    e.remove();
                    return Ok(Some(()));
                }
                if counter.fetch_sub(1, Ordering::AcqRel) == 1 {
                    e.remove();
                    return Ok(Some(()));
                };
                Ok(None)
            }
            dashmap::Entry::Vacant(_) => {
                tracing::error!("Inode {ino} has no open filehandles");
                Ok(None)
            }
        }
    }

    fn next_handle(&self) -> u64 {
        self.current_handle.fetch_add(1, Ordering::SeqCst)
    }
}
