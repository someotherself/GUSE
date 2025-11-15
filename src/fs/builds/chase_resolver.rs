use std::{
    collections::{BTreeSet, HashMap},
    ffi::{OsStr, OsString},
    path::PathBuf,
};

use chrono::Datelike;
use git2::Oid;

use crate::fs::{
    self, GitFs,
    builds::reporter::{ChaseFsError, ChaseGitError, GuseFsResult, GuseGitResult},
    repo::RefKind,
};

pub fn validate_commits(
    fs: &GitFs,
    repo_ino: u64,
    commits: &[String],
) -> GuseGitResult<Vec<(Oid, BTreeSet<RefKind>, i64)>> {
    let Ok(repo) = fs.get_repo(repo_ino) else {
        return Err(ChaseGitError::FsError {
            msg: "Repo not found. Try restarting the session".to_string(),
        });
    };
    let mut c_oids = vec![];
    repo.with_repo(|r| -> GuseGitResult<()> {
        for commit in commits {
            let commit = r
                .find_commit_by_prefix(commit)
                .map_err(|e| map_git_error(commit, e))?;
            c_oids.push((commit.id(), commit.time().seconds()));
        }
        Ok(())
    })?;
    let kinds = repo.with_state(|s| -> GuseGitResult<Vec<(Oid, BTreeSet<RefKind>, i64)>> {
        let mut kinds = vec![];
        for (c, time) in c_oids {
            let ref_kind = s
                .snaps_to_ref
                .iter()
                .find_map(|(oid, rf)| (oid == &c).then(|| (*oid, rf.clone())));
            if let Some((oid, rf)) = ref_kind {
                if rf.is_empty() {
                    return Err(ChaseGitError::RefKindNotFound {
                        commit: c.to_string(),
                    });
                }
                kinds.push((oid, rf, time));
            };
        }
        Ok(kinds)
    })?;
    if kinds.is_empty() {
        return Err(ChaseGitError::NoRefKindsFound);
    }
    Ok(kinds)
}

// Returns a list of commits and the path to their Snap folder
// The path is RELATIVE to the repo root (join to data_dir/repo_dir)
//
// Uses: find_path_in_main, find_path_in_pr as helpers find_path_in_branch
pub fn resolve_path_for_refs(
    fs: &GitFs,
    repo_ino: u64,
    commits: Vec<(Oid, BTreeSet<RefKind>, i64)>,
) -> GuseFsResult<HashMap<Oid, PathBuf>> {
    let mut out = HashMap::new();
    for (commit, rf_kinds, c_time) in commits {
        // TODO: Maybe find a better way search with this priority
        for rf in &rf_kinds {
            if matches!(rf, RefKind::Main(_)) {
                let path = find_path_in_main(fs, repo_ino, commit, c_time)?;
                out.entry(commit).or_insert(path);
                break;
            };
        }
        for rf in &rf_kinds {
            if matches!(rf, RefKind::Pr(_)) {
                let path = PathBuf::from(rf.as_str()).join(rf.get());
                out.entry(commit).or_insert(path);
                break;
            };
        }
        for rf in &rf_kinds {
            if matches!(rf, RefKind::PrMerge(_)) {
                let path = PathBuf::from(rf.as_str()).join(rf.get());
                out.entry(commit).or_insert(path);
                break;
            };
        }
        for rf in &rf_kinds {
            if matches!(rf, RefKind::Branch(_)) {
                let branches_root = rf.get();
                let branch_folder = find_path_in_branch(fs, repo_ino, commit, branches_root)?;
                let path = PathBuf::from(rf.as_str())
                    .join(branches_root)
                    .join(branch_folder);
                out.entry(commit).or_insert(path);
                break;
            }
        }
    }
    Ok(out)
}

fn find_path_in_main(fs: &GitFs, repo_ino: u64, commit: Oid, c_time: i64) -> GuseFsResult<PathBuf> {
    let Ok(repo) = fs.get_repo(repo_ino) else {
        return Err(ChaseFsError::FsError {
            msg: "Repo not found. Try restarting the session".to_string(),
        });
    };
    let Some(dt) = chrono::DateTime::from_timestamp(c_time, 0) else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from("Date-time"),
        });
    };
    let month_folder = OsString::from(format!("{:04}-{:02}", dt.year(), dt.month()));
    let repo_ino = GitFs::repo_id_to_ino(repo.repo_id);
    let Ok(month) = fs::ops::lookup::lookup_repo(fs, repo_ino.into(), OsStr::new(&month_folder))
    else {
        return Err(ChaseFsError::NoneFound {
            target: month_folder,
        });
    };
    let Some(month) = month else {
        return Err(ChaseFsError::NoneFound {
            target: month_folder,
        });
    };

    let Ok(entries) = fs.readdir(month.ino) else {
        return Err(ChaseFsError::SnapNotFound {
            msg: format!(
                "No entries found inside the MONTH folder {}",
                month_folder.display()
            ),
        });
    };

    let Some(snap_folder) = entries.into_iter().find(|e| e.oid == commit) else {
        return Err(ChaseFsError::SnapNotFound {
            msg: format!(
                "Commit {} not found in folder {}",
                commit,
                month_folder.display()
            ),
        });
    };
    let path = PathBuf::from(month_folder).join(snap_folder.name);
    Ok(path)
}

fn find_path_in_branch(
    fs: &GitFs,
    repo_ino: u64,
    commit: Oid,
    branch_name: &str,
) -> GuseFsResult<PathBuf> {
    let Ok(branchroot) = fs::ops::lookup::lookup_repo(&fs, repo_ino.into(), OsStr::new("Branches"))
    else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from("Branches"),
        });
    };
    let Some(branchroot) = branchroot else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from("Branches"),
        });
    };

    let Ok(branch_folder) =
        fs::ops::lookup::lookup_repo(&fs, branchroot.ino.into(), OsStr::new(branch_name))
    else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from(branch_name),
        });
    };
    let Some(branch_folder) = branch_folder else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from(branch_name),
        });
    };

    let Ok(entries) = fs.readdir(branch_folder.ino) else {
        return Err(ChaseFsError::SnapNotFound {
            msg: format!("No entries found inside the MONTH folder {}", branch_name),
        });
    };

    let Some(snap_folder) = entries.into_iter().find(|e| e.oid == commit) else {
        return Err(ChaseFsError::SnapNotFound {
            msg: format!("Commit {} not found in folder {}", commit, branch_name),
        });
    };

    Ok(PathBuf::from(snap_folder.name))
}

/// Used to convert git2 errors to ChaseGitError used by the Reported in a GUSE chase
fn map_git_error(commit: &str, e: git2::Error) -> ChaseGitError {
    match e.code() {
        git2::ErrorCode::Ambiguous => ChaseGitError::GitAmbiguousCommit {
            commit: commit.to_string(),
        },
        git2::ErrorCode::NotFound => ChaseGitError::CommitNotFound {
            commit: commit.to_string(),
        },
        _ => ChaseGitError::GitError {
            message: format!("Error looking up commit `{commit}`: {}", e.message()),
            source: e,
        },
    }
}
