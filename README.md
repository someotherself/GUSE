# GUSE (goo͞s) is a git based filesystem, written in rust and mouted with FUSE.

## Developed as a hobby project. See Usage for current functionality

## OS
This project has support for linux/macOS. On windows, consider using WSL2.

## How to install and use
- [Basic usage](readme/usage.md)
- [Managing git in each repo and commit files](readme/git_and_commit_files.md)
- [GUSE chase and scripts](readme/chase.md)

## INFO
GUSE is built to map the entire git repo on the disk available at once. It allows navigating it with cli commands, along with other features.

GUSE allows mounting of multiple repositories and manage them independently

Git authentication is currently not supported

### The disk structure, as observed by the user is:
``` text
data_dir (Root folder. Can be renamed)
    ├── repository_1/
            ├── Pr                       # Github PR's (if any) # The folder will contain only commits made in this PR (since creation)
                └── 150/                 # Contains folders with the number of the PR
                    └── Snap_001_HASH/ 
                └── 151/                 # Contains only Snap folders for the synthetic commits
            ├── PrMerge                  # Github PrMerges (if any open Pr's)
                    ├── 150/             # Contains only Snap folders for the synthetic commits.
                    └── 151/
            ├── Branches                 # Remote branches
                    ├── Branch_1         # Contains folders with the name of the branches. Each folder contains Snap folders.
                        └── Snap_001_HASH/ # The folder will contain only commits made in this branch (since creation)
                    └── Branch_2
            ├── Tags                     # Remote Tags
                └── Snap_001_HASH/       # Contains only Snap folders for the synthetic commits
            ├── live                     # working files and folders
                    ├── user_file1.md
                    └── user_file2.md
            ├── chase                       # Scripts for a GUSE chase
            ├── YYYY-MM/                    # a list of months where commits were made. The MONTH folders will show the comits on main/master
                    ├── Snap_001_HASH/      # list of commits for the respective month
                        ├── user_file1.md   # previous versions of the live files.
                        └── user_file2.md
                    └── Snap_002_HASH/
            ├── YYYY-MM/
                    ├── Snap_001_HASH/
                        ├── user_file1.md
                        └── user_file2.md
                    └── Snap_002_HASH/
            ├── YYYY-MM/                
                    ├── Snaps_on_HASH/
                        ├── user_file1.md
                        └── user_file2.md
                    └── Snaps_on_HASH/
            ...
```
