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
            .read(true)
            .write(true)
            .truncate(true)
            .create(true)
            .open(&path)?;

        let blob = repo.with_repo(|r| -> anyhow::Result<Vec<u8>> {
            let blob = r.find_blob(oid)?;
            Ok(blob.content().to_vec())
        })?;
        file.write_at(blob.as_slice(), 0)?;

        let metadata: InjectedMetadata = InjectedMetadata {
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
