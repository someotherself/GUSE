use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    ffi::{OsStr, OsString},
    path::PathBuf,
};

use chrono::Datelike;
use git2::Oid;

use crate::fs::{
    self, GitFs,
    builds::{
        chase::Chase,
        reporter::{ChaseFsError, ChaseGitError, GuseFsResult, GuseGitResult},
        runtime::InputTypes,
    },
    fileattr::FileType,
    repo::RefKind,
};

pub fn validate_commits(
    fs: &GitFs,
    repo_ino: u64,
    commits: &[(InputTypes, String)],
) -> GuseGitResult<VecDeque<Oid>> {
    let Ok(repo) = fs.get_repo(repo_ino) else {
        return Err(ChaseGitError::FsError {
            msg: "Repo not found. Try restarting the session".to_string(),
        });
    };
    let mut c_oids = VecDeque::new();
    repo.with_repo(|r| -> GuseGitResult<()> {
        for (itype, commit) in commits {
            // commit can mean a few things
            // range -> oid...oid
            // branch/pr -> name
            // or a single short hash as string
            match itype {
                InputTypes::Commit => {
                    let commit = r
                        .find_commit_by_prefix(commit)
                        .map_err(|e| map_git_error(commit, e))?;
                    c_oids.push_back(commit.id())
                }
                &InputTypes::Branch => {
                    repo.with_ref_state(|s| -> GuseGitResult<()> {
                        if let Some(commits) = s
                            .refs_to_snaps
                            .get(&RefKind::Branch(commit.to_string().into()))
                        {
                            for (_, oid) in commits {
                                c_oids.push_back(*oid)
                            }
                        } else {
                            return Err(ChaseGitError::BranchNotFound {
                                branch_type: "Branch".to_string(),
                                branch_name: commit.to_string(),
                            });
                        };
                        Ok(())
                    })?;
                }
                &InputTypes::Pr => {
                    repo.with_ref_state(|s| -> GuseGitResult<()> {
                        if let Some(commits) =
                            s.refs_to_snaps.get(&RefKind::Pr(commit.to_string().into()))
                        {
                            for (_, oid) in commits {
                                c_oids.push_back(*oid)
                            }
                        } else {
                            return Err(ChaseGitError::BranchNotFound {
                                branch_type: "Pr".to_string(),
                                branch_name: commit.to_string(),
                            });
                        }
                        Ok(())
                    })?;
                }
                &InputTypes::Range => {
                    if !commit.contains("..") {
                        return Err(ChaseGitError::BadCommitRange {
                            input: commit.to_string(),
                        });
                    }
                    let mut oids = commit.split("..");
                    let Some(start) = oids.next() else {
                        return Err(ChaseGitError::BadCommitRange {
                            input: commit.to_string(),
                        });
                    };
                    let Some(end) = oids.next() else {
                        return Err(ChaseGitError::BadCommitRange {
                            input: commit.to_string(),
                        });
                    };
                    let start_commit = {
                        let start_obj = r
                            .revparse_single(start)
                            .map_err(|e| map_git_error(start, e))?;
                        let oid = start_obj
                            .peel_to_commit()
                            .map_err(|e| map_git_error(start, e))?
                            .id();
                        r.find_commit(oid)
                            .map_err(|e| map_git_error(start, e))?
                            .id()
                    };
                    let end_commit = {
                        let start_obj =
                            r.revparse_single(end).map_err(|e| map_git_error(end, e))?;
                        let oid = start_obj
                            .peel_to_commit()
                            .map_err(|e| map_git_error(end, e))?
                            .id();
                        r.find_commit(oid).map_err(|e| map_git_error(end, e))?.id()
                    };
                    let common_ref = repo.with_ref_state(|s| -> GuseGitResult<RefKind> {
                        let Some(start_refs) = s.snaps_to_ref.get(&start_commit) else {
                            return Err(ChaseGitError::CommitNotFound {
                                commit: start_commit.to_string(),
                            });
                        };
                        let Some(end_refs) = s.snaps_to_ref.get(&end_commit) else {
                            return Err(ChaseGitError::CommitNotFound {
                                commit: end_commit.to_string(),
                            });
                        };
                        let Some(common_ref_kind) = start_refs.intersection(end_refs).next() else {
                            return Err(ChaseGitError::NoCommonRef {
                                oid1: start_commit.to_string(),
                                oid2: end_commit.to_string(),
                            });
                        };
                        Ok(common_ref_kind.clone())
                    })?;
                    let range = repo.with_ref_state(|s| -> GuseGitResult<Vec<Oid>> {
                        let Some(refs) = s.refs_to_snaps.get(&common_ref) else {
                            // Should be uncreachable.
                            return Err(ChaseGitError::NoCommonRef {
                                oid1: start_commit.to_string(),
                                oid2: end_commit.to_string(),
                            });
                        };
                        let Some(pos1) = refs.iter().position(|(_, oid)| *oid == start_commit)
                        else {
                            return Err(ChaseGitError::CommitNotFound {
                                commit: start_commit.to_string(),
                            });
                        };
                        let Some(pos2) = refs.iter().position(|(_, oid)| *oid == end_commit) else {
                            return Err(ChaseGitError::CommitNotFound {
                                commit: start_commit.to_string(),
                            });
                        };
                        let range = refs[pos1.min(pos2)..=pos1.max(pos2)]
                            .iter()
                            .map(|(_, oid)| *oid)
                            .collect::<Vec<Oid>>();
                        Ok(range)
                    })?;
                    c_oids.extend(range);
                }
                _ => unreachable!(),
            }
        }
        Ok(())
    })?;
    Ok(c_oids)
}

pub fn validate_commit_refs(
    fs: &GitFs,
    repo_ino: u64,
    commits: &[&Oid],
) -> GuseGitResult<Vec<(Oid, BTreeSet<RefKind>, i64)>> {
    let Ok(repo) = fs.get_repo(repo_ino) else {
        return Err(ChaseGitError::FsError {
            msg: "Repo not found. Try restarting the session".to_string(),
        });
    };
    let mut c_oids = vec![];
    repo.with_repo(|r| -> GuseGitResult<()> {
        for &commit in commits {
            let commit = r
                .find_commit(*commit)
                .map_err(|e| map_git_error(&commit.to_string(), e))?;
            c_oids.push((commit.id(), commit.time().seconds()));
        }
        Ok(())
    })?;
    let kinds = repo.with_ref_state(|s| -> GuseGitResult<Vec<(Oid, BTreeSet<RefKind>, i64)>> {
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

// Returns a list of commits as key
// and the path to their Snap folder and the inode of that folder
// The path is RELATIVE to the repo root (join to data_dir/repo_dir)
//
// Uses: find_path_in_main, find_path_in_pr as helpers find_path_in_branch
pub fn resolve_path_for_refs(
    fs: &GitFs,
    repo_ino: u64,
    commits: Vec<(Oid, BTreeSet<RefKind>, i64)>,
) -> GuseFsResult<HashMap<Oid, (PathBuf, u64)>> {
    let mut out: HashMap<Oid, (PathBuf, u64)> = HashMap::new();
    for (commit, rf_kinds, c_time) in commits {
        let Ok(repo) = fs.get_repo(repo_ino) else {
            return Err(ChaseFsError::FsError {
                msg: "Repo not found".to_string(),
            });
        };
        let root = fs.mount_point.join(&repo.repo_dir);

        // TODO: Maybe find a better way search with this priority
        for rf in &rf_kinds {
            if matches!(rf, RefKind::Main(_)) {
                let (path, ino) = find_path_in_main(fs, repo_ino, commit, c_time)?;
                out.entry(commit).or_insert((root.join(path), ino));
                break;
            };
        }
        for rf in &rf_kinds {
            if matches!(rf, RefKind::Pr(_)) {
                let (path, ino) = find_path_in_pr_merge(fs, repo_ino, rf.get())?;
                out.entry(commit).or_insert((root.join(path), ino));
                break;
            };
        }
        for rf in &rf_kinds {
            if matches!(rf, RefKind::PrMerge(_)) {
                let (path, ino) = find_path_in_pr(fs, repo_ino, rf.get(), commit)?;
                out.entry(commit).or_insert((root.join(path), ino));
                break;
            };
        }
        for rf in &rf_kinds {
            if matches!(rf, RefKind::Branch(_)) {
                let branches_root = rf.get();
                let (branch_folder, ino) =
                    find_path_in_branch(fs, repo_ino, commit, branches_root)?;
                let path = PathBuf::from(rf.as_str())
                    .join(branches_root)
                    .join(branch_folder);
                out.entry(commit).or_insert((root.join(path), ino));
                break;
            }
        }
    }
    Ok(out)
}

fn find_path_in_main(
    fs: &GitFs,
    repo_ino: u64,
    commit: Oid,
    c_time: i64,
) -> GuseFsResult<(PathBuf, u64)> {
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
    Ok((path, snap_folder.ino))
}

fn find_path_in_branch(
    fs: &GitFs,
    repo_ino: u64,
    commit: Oid,
    branch_name: &str,
) -> GuseFsResult<(PathBuf, u64)> {
    let Ok(branchroot) = fs::ops::lookup::lookup_repo(fs, repo_ino.into(), OsStr::new("Branches"))
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
        fs::ops::lookup::lookup_repo(fs, branchroot.ino.into(), OsStr::new(branch_name))
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

    Ok((PathBuf::from(snap_folder.name), snap_folder.ino))
}

// folder name = name of the - via ref.get()
fn find_path_in_pr_merge(
    fs: &GitFs,
    repo_ino: u64,
    folder_name: &str,
) -> GuseFsResult<(PathBuf, u64)> {
    let Ok(main_root) = fs::ops::lookup::lookup_repo(fs, repo_ino.into(), OsStr::new("PrMerge"))
    else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from("PrMerge"),
        });
    };
    let Some(main_root) = main_root else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from("PrMerge"),
        });
    };
    let Ok(folder_root) =
        fs::ops::lookup::lookup_repo(fs, main_root.ino.into(), OsStr::new(folder_name))
    else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from(folder_name),
        });
    };
    let Some(folder_root) = folder_root else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from(folder_name),
        });
    };

    // The root folder will be the Snap folder. Return the path to it.
    let path = PathBuf::from("PrMerge").join(folder_name);
    Ok((path, folder_root.ino))
}

// folder name = name of the - via ref.get()
fn find_path_in_pr(
    fs: &GitFs,
    repo_ino: u64,
    folder_name: &str,
    commit: Oid,
) -> GuseFsResult<(PathBuf, u64)> {
    let Ok(main_root) = fs::ops::lookup::lookup_repo(fs, repo_ino.into(), OsStr::new("Pr")) else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from("Pr"),
        });
    };
    let Some(main_root) = main_root else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from("Pr"),
        });
    };
    let Ok(folder_root) =
        fs::ops::lookup::lookup_repo(fs, main_root.ino.into(), OsStr::new(folder_name))
    else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from(folder_name),
        });
    };
    let Some(folder_root) = folder_root else {
        return Err(ChaseFsError::NoneFound {
            target: OsString::from(folder_name),
        });
    };

    // The root folder will contain Snap folders. Readdir and find the one we need.
    let Ok(entries) = fs.readdir(folder_root.ino) else {
        return Err(ChaseFsError::SnapNotFound {
            msg: format!("No entries found inside the Pr folder {}", folder_name),
        });
    };

    let Some(snap_folder) = entries.into_iter().find(|e| e.oid == commit) else {
        return Err(ChaseFsError::SnapNotFound {
            msg: format!("Commit {} not found in folder {}", commit, folder_name),
        });
    };

    let path = PathBuf::from("Pr").join(folder_name).join(snap_folder.name);
    Ok((path, snap_folder.ino))
}

pub fn cleanup_builds(fs: &GitFs, repo_ino: u64, chase: &Chase) -> anyhow::Result<()> {
    let repo = fs.get_repo(repo_ino)?;
    for oid in chase.commits.iter() {
        let guard = repo.inostate.read();
        let exists = guard.build_sessions.contains_key(oid);
        drop(guard);
        if exists {
            let Some(&(_, parent)) = chase.commit_paths.get(oid) else {
                continue;
            };
            let Ok(entries) = fs.readdir(parent) else {
                continue;
            };
            for e in entries {
                if !fs.is_in_build(e.ino.into())? {
                    continue;
                };
                match e.kind {
                    FileType::Directory => {
                        remove_dir_all(fs, e.ino)?;
                    }
                    _ => {
                        fs.unlink(parent, &e.name)?;
                    }
                }
            }
        };
    }
    Ok(())
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

fn remove_dir_all(fs: &GitFs, parent: u64) -> anyhow::Result<()> {
    let mut stack: Vec<u64> = Vec::new();
    let mut dirs: Vec<(u64, OsString)> = Vec::new(); // (parent_ino, folder_name)
    stack.push(parent);
    while let Some(cur_par) = stack.pop() {
        let Ok(entries) = fs.readdir(cur_par) else {
            continue;
        };
        for e in entries {
            match e.kind {
                FileType::Directory => {
                    stack.push(e.ino);
                    dirs.push((cur_par, e.name));
                }
                _ => {
                    fs.unlink(cur_par, &e.name)?;
                }
            }
        }
    }
    for (par, name) in dirs.into_iter().rev() {
        fs.rmdir(par, &name)?;
    }
    let par_parent = fs.get_dir_parent(parent)?;
    let par_name = fs.get_name_from_db(parent)?;
    fs.rmdir(par_parent, &par_name)?;
    Ok(())
}
