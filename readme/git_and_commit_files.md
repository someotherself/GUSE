# Managing repos
## .git folders

```text
There will several ways to interact with git via the cli.
The .git folder is available in seveveral locations:
- the Live folder: This is the "true" .git folder and behaves like any other .git folder
- the Snap folders: Every Snap folder will contain a read-only copy of the .git folder
- A virtual folder with a commit history (see Usage - Commit-history)
The Snap folders will have a detached HEAD to the commit of that snap folder.
They will also have an index set to the parent tree. When opened in a code editor, diffs between the Snap commit and parent commit are highlighted.
```

## Fetching

More details about how guse fetches via mkdir, see fetching section here: [Fetching](usage.md###Fetching).

```text
An empty repository will still have an empty, initialized repo and a regular fetch can still be done. But by default, a git fetch will not include PRs which are fetched by guse. After a manual fetch, a session restart will be needed to update the folder structure.
A git clone should not be performed here.
```

## Snap folders
```text
guse is built to allow builds and compilations to be ran inside each Snap folder. This means that each Snap folders has write permissions, with some restrictions.

The files and folders in a commit, cannot be deleted and files cannot be created inside a commit directory.
However, the commit files can be modified and modifications will persist for the duration of the session.
These changes will also be used in a build/compilation.
In the root of the Snap folders, files and folders can be created freely. These will also only persist for the duration of the session.
```