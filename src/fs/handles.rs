use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

use dashmap::DashMap;

use crate::fs::{GitFs, Handle, meta_db::DbWriteMsg};

pub struct FileHandles {
    current_handle: AtomicU64,
    /// (fh, Handle)
    handles: DashMap<u64, Arc<Handle>>,
    /// (ino, open file handles)
    open_counts: DashMap<u64, AtomicU64>,
}

impl Default for FileHandles {
    fn default() -> Self {
        Self {
            current_handle: AtomicU64::new(1),
            handles: DashMap::new(),
            open_counts: DashMap::new(),
        }
    }
}

impl FileHandles {
    pub fn exists(&self, fh: u64) -> Option<u64> {
        self.handles.get(&fh).map(|entry| entry.ino)
    }

    /// Opens a file handle
    pub fn open(&self, handle: Handle) -> anyhow::Result<u64> {
        let fh = self.next_handle();
        let ino = handle.ino;

        self.handles.insert(fh, Arc::new(handle));
        self.register_open(ino);

        Ok(fh)
    }

    /// Closes a file handle
    pub fn close(
        &self,
        fh: u64,
        writer_tx: Option<crossbeam_channel::Sender<DbWriteMsg>>,
    ) -> anyhow::Result<bool> {
        if let Some((_, handle)) = self.handles.remove(&fh) {
            let ino = handle.ino;
            if let Some(writer_tx) = writer_tx
                && let Err(e) = GitFs::cleanup_entry_with_writemsg(ino.into(), writer_tx)
            {
                tracing::error!("cleanup_entry_with_writemsg failed for ino {ino}: {e}");
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn get_context(&self, fh: u64) -> Option<Arc<Handle>> {
        self.handles.get_mut(&fh).map(|e| e.value().clone())
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

    /// Returns Some(()) if the DB entry needs to be removed from inode_map
    fn register_close(&self, ino: u64) -> Option<()> {
        match self.open_counts.entry(ino) {
            dashmap::Entry::Occupied(e) => {
                let counter = e.get();
                match counter
                    .fetch_update(Ordering::AcqRel, Ordering::Acquire, |v| v.checked_sub(1))
                {
                    // Counter is now zero
                    Ok(1) => {
                        e.remove();
                        None
                    }
                    // Counter is still higher than zero
                    Ok(_) => None,
                    // Counter was already zero
                    Err(0) => {
                        e.remove();
                        Some(())
                    }
                    Err(_) => {
                        tracing::error!("Unreacheable. Atomic returned Err with non-zero value");
                        None
                    }
                }
            }
            dashmap::Entry::Vacant(_) => {
                tracing::error!("Inode {ino} has no open filehandles");
                None
            }
        }
    }

    fn next_handle(&self) -> u64 {
        self.current_handle.fetch_add(1, Ordering::SeqCst)
    }
}
