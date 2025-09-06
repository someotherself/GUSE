# GUSE (goo͞s) is a git based filesystem, written in rust and mouted with FUSE.

## Under development. See Usage for current functionality

## Dependencies

This project uses [libgit2](https://libgit2.org/) (via the `git2` crate) and
[SQLite](https://sqlite.org/) (via the `rusqlite` crate).

## OS
This project has support for linux/macOS. On windows, consider using WSL2.

## How to install and use
- [Usage](Readme/usage.md)

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

### The filesystem commands interact with git by performing different actions depending on WHERE and on WHAT they are used on.
The locations include:

1. `root` – root of the filesystem

2. `repository` folder

3. `live` folder

4. `snapshot` folder

## Commands available
```
    - mkdir
        location 1: root folder
                    - Using a name with the format github.tokio-rs.tokio.git, will initialize a repository and perform a fetch on the remote repo.
                    - Otherwise, a normal folder name will initialize an fresh repository
        location 2: repository folder
                    - This folder is read only
        location 3: live folder
                    - Will always create normal folders
        location 3: snapshot folder
                    - This folder is read only

    - cd
        Normal operation, will navigate the filesystem

    - ls
        When used on a folder:
            location 1: Normal operation
            location 2: Will perform a git log (showing the snapshots)
            location 3: Normal operation
            location 4: Normal operation
        When used on a file:
            Will try and perform a git blame
        - ls -a
            location 3: repository folder
                        - Will perform a git reflog

    - cat
        Normal operation.
        When used on a snapshot: 
            Will print a summary of the snapshot.
