# GUSE (goo͞s) is a git based filesystem, written in rust and mouted with FUSE.

## Under development. See Usage for current functionality

## Dependencies

This project uses [libgit2](https://libgit2.org/) (via the `git2` crate) and
[SQLite](https://sqlite.org/) (via the `rusqlite` crate).

## OS
This project has support for linux/macOS. On windows, consider using WSL2.

## How to install and use
- [Usage](readme/usage.md)

## INFO
GUSE is a learning project. It is built to allow user to interact with git directly from the filesystem, using commands such as cd, ls, mkdir, touch, cat etc

Inspired by [rencfs](https://github.com/xoriors/rencfs)

GUSE allows mounting of multiple 'repositories' and manage them independently
### The disk structure, as observed by the user is:
``` text
data_dir (Root folder. Can be renamed)
    ├── repository_1/
            ├── live                    # working files and folders
                    ├── user_file1.md
                    └── user_file2.md
            ├── YYYY-MM/                # a list of months where commits were made
                    ├── Snap_001_HASH/   #  list of commits for the respective month
                        ├── user_file1.md   # previous versions of the live files. Read only versions
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
    ├── repository_2/
            ├── live
            ├── YYYY-MM/
            ├── YYYY-MM/
            ├── YYYY-MM/
            ...
    ├── repository_3/
            ├── live
            ├── YYYY-MM/
            ├── YYYY-MM/
            ├── YYYY-MM/
            ...
```
