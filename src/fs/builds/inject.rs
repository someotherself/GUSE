use std::sync::Arc;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BlobKind {
    Original,
    Modified,
    Build,
}

/// Contained by SourceTypes::RoBlob
#[derive(Debug)]
pub struct BlobVersions {
    /// Contains the original blob data from the commit
    original: Arc<[u8]>,
    /// The original blob data is modified. Persists for the
    modified: Option<Arc<[u8]>>,
    /// Data is modified for use in a build / guse chase. Removed outside of that.
    build: Option<Arc<[u8]>>,
}

#[derive(Debug, Clone)]
pub struct BlobView {
    versions: Arc<BlobVersions>,
    kind: BlobKind,
}

impl BlobView {
    pub fn new(versions: BlobVersions, kind: BlobKind) -> Self {
        Self {
            versions: Arc::new(versions),
            kind,
        }
    }

    pub fn new_original(data: Arc<[u8]>) -> Self {
        let versions = BlobVersions {
            original: data,
            modified: None,
            build: None,
        };
        Self {
            versions: Arc::new(versions),
            kind: BlobKind::Original,
        }
    }

    pub fn len(&self) -> usize {
        match self.kind {
            BlobKind::Original => self.versions.original.len(),
            BlobKind::Modified => self
                .versions
                .modified
                .as_deref()
                .unwrap_or(&self.versions.original)
                .len(),
            BlobKind::Build => self
                .versions
                .build
                .as_deref()
                .unwrap_or(&self.versions.original)
                .len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self.kind {
            BlobKind::Original => self.versions.original.is_empty(),
            BlobKind::Modified => self
                .versions
                .modified
                .as_deref()
                .unwrap_or(&self.versions.original)
                .is_empty(),
            BlobKind::Build => self
                .versions
                .build
                .as_deref()
                .unwrap_or(&self.versions.original)
                .is_empty(),
        }
    }
}

impl AsRef<Arc<[u8]>> for BlobView {
    fn as_ref(&self) -> &Arc<[u8]> {
        match self.kind {
            BlobKind::Original => &self.versions.original,
            BlobKind::Modified => self
                .versions
                .modified
                .as_ref()
                .unwrap_or(&self.versions.original),
            BlobKind::Build => self
                .versions
                .build
                .as_ref()
                .unwrap_or(&self.versions.original),
        }
    }
}
