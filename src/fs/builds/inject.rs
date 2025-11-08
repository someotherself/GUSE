use std::{ffi::OsString, fs::OpenOptions, os::unix::fs::FileExt, path::PathBuf};

use git2::Oid;

use crate::fs::{GitFs, TEMP_FOLDER, janitor::random_string};

/// Holds information about and keeps track of modified blobs.
///
/// Created by `open_git` the first time a blob is modified
///
/// Priority when opening is: build > modified > original blob
pub struct InjectedMetadata {
    /// Created by a user
    pub modified: InjectedFile,
    /// Created and cleaned up by a chase
    pub build: Option<InjectedFile>,
}

pub struct InjectedFile {
    pub name: OsString,
    pub path: PathBuf,
}

impl InjectedMetadata {
    /// Used when a blob is opened with write == true
    ///
    /// It creates a new file in repo_dir/.temp and copies the blob in it.
    ///
    /// For as long as the file exists, it will replace the blob and will be used in builds
    pub fn create_modified(fs: &GitFs, oid: Oid, ino: u64) -> anyhow::Result<Self> {
        let filename = OsString::from(random_string());
        let repo = fs.get_repo(ino)?;
        let path = fs
            .repos_dir
            .join(&repo.repo_dir)
            .join(TEMP_FOLDER)
            .join(&filename);
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&path)?;

        repo.with_repo(|r| -> anyhow::Result<()> {
            let blob = r.find_blob(oid)?;
            file.write_at(blob.content(), 0)?;
            Ok(())
        })?;

        let metadata = InjectedMetadata {
            modified: InjectedFile {
                name: filename,
                path,
            },
            build: None,
        };
        Ok(metadata)
    }
}
