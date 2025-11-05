use std::{ffi::OsString, fs::OpenOptions, os::unix::fs::FileExt, path::PathBuf};

use git2::Oid;

use crate::fs::{GitFs, TEMP_FOLDER, janitor::random_string};

pub struct InjectedMetadata {
    pub modified: InjectedFile,
    pub build: Option<InjectedFile>,
}

pub struct InjectedFile {
    pub name: OsString,
    pub path: PathBuf,
}

impl InjectedMetadata {
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

    pub fn open_build() {
        todo!()
    }
}
