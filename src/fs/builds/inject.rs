use std::{ffi::OsString, fs::OpenOptions, os::unix::fs::FileExt, path::PathBuf};

use dashmap::Entry;
use git2::Oid;
use rand::{Rng, distr::Alphanumeric};

use crate::fs::{GitFs, TEMP_FOLDER};

/// Holds information about and keeps track of modified blobs.
///
/// Created by `open_git` the first time a blob is modified
///
/// Priority when opening is: build > modified > original blob
pub struct InjectedMetadata {
    /// Created by a user
    pub modified: Option<InjectedFile>,
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
            modified: Some(InjectedFile {
                name: filename,
                path,
            }),
            build: None,
        };
        Ok(metadata)
    }

    pub fn create_build(fs: &GitFs, oid: Oid, ino: u64) -> anyhow::Result<()> {
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

        let inj_file: InjectedFile = InjectedFile {
            name: filename,
            path,
        };

        let repo = fs.get_repo(ino)?;
        match repo.injected_files.entry(ino) {
            Entry::Occupied(mut e) => {
                let entry = e.get_mut();
                entry.build = Some(inj_file);
            }
            Entry::Vacant(s) => {
                let metadata = InjectedMetadata {
                    modified: None,
                    build: Some(inj_file),
                };
                s.insert(metadata);
            }
        }
        Ok(())
    }

    pub fn cleanup_builds(fs: &GitFs, repo_ino: u64) -> anyhow::Result<()> {
        let repo = fs.get_repo(repo_ino)?;
        for mut entry in repo.injected_files.iter_mut() {
            if let Some(file_entry) = &entry.build {
                let _ = std::fs::remove_file(&file_entry.path);
                entry.build = None;
            };
        }
        Ok(())
    }
}

// Creates a random string 6 characters long and alphanumerical
pub fn random_string() -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(6)
        .map(char::from)
        .collect()
}
