# guse Chase

## INFO: This feature is still experimental and is geting improved.
## SAFETY: The safety will execute cli commands that you pass in. The safety of this feature is the same as anything else you run on your computer. Do not copy and run code or cli commands that you do not understand.

## Intro
```text
The app has the ability to run automated builds for a list of commits.
For each commit, a series of shell commands can be ran, including compilations and builds.
By default, the files and folders created by one target, get moved to the next one to speed up subsequent builds (incremental compilations).
The chase uses simple scripts written in Lua.
```

## Running a chase

```text
Once created, a script can be ran using:

guse chase <REPO_NAME> <SCRIPT_NAME>

To enable saving logs to file, use the '-l' flag.
The logs will be saved in a folder named with a timestamp, next to the script.lua.

guse chase <REPO_NAME> <SCRIPT_NAME> -l
```

## Managing scripts

```text
Each repo will have a folder called chase where all the chase scripts and logs will be found.

A new script template can be created using:
$ guse script new REPO_NAME SCRIPT_NAME
This will create the folder and place a blank script inside:
> REPO_NAME/chase/SCRIPT_NAME/script.lua

A script can then be deleted (this will delete the entire script folder):
$ guse script remove REPO_NAME SCRIPT_NAME

It can also be renamed:
$ guse script remove REPO_NAME OLD_SCRIPT_NAME NEW_SCRIPT_NAME
```

A blank script will look like this:
```lua
local commits = {
}

local commands = {
}

-- Sets the run mode. For the moment, only "Continuous" mode is implemented. "Binary" mode is a work in progress.
local run_mode = "Continuous"
-- Sets the build mode. Can be "FirstFailure" or "Continuous"
local stop_mode = "FirstFailure"

-- Load functions (Not to be changed by the user)
for input_type, oid in pairs(commits) do
  if type(input_type) == "number" then 
    cfg.add_commit("commit", oid)
  else
    cfg.add_commit(input_type, oid)
  end
end

for _, command in ipairs(commands) do
  cfg.add_command(command)
end

if run_mode ~= nil then
  cfg.set_run_mode(run_mode)
end

if stop_mode ~= nil then
  cfg.set_stop_mode(stop_mode)
end
```

### Adding commits
```text
Commits can be input in multiple ways: as single commits, a range of commits or a whole Branch or Pr. Or any combination of these.
```
```lua
local commits = {"b074789", "0f9cd69", "c155149", "b2e00c3"}
local commits = {Branch = "branch_name" }
local commits = {Pr = "pr_name" }
local commits = {Range = "b074789..b2e00c3" }
local commits = {Branch = "branch_name", Range = "b074789..b2e00c3", "10a4g89" }

```

### Adding commands

Guse does not modify or check the commands the commands you pass in. They are parsed using the  [shell_words](https://crates.io/crates/shell-words) crate and passed in directly to `std::process::Command`.

Example:
```lua
local commands = { "echo some", "pwd", "cargo test" }
```

## Run mode
```text
Run mode means the order in which the commits will be ran.
This field is optional. The default value will be Continuous
- Continuous is the order in which they are provided
- Binary is is a binary search (Not implemented)
```

## Stop mode
```text
Stop mode is the failure behavious, in case of a exit failure from the cli.
This field is optional. The default value will be Continuous
- Continuous will not stop for failures and run through the whole list
- FirstFailure will stop at the first exit failure.
```

## Patches

```text
Patches are a way to automatically add code to the files in a commit, to aid during a chase, typically, tests.
Currently, all the code is added at the bottom of the file. The path to the file is inserted once, and the patch will be inserted in all the versions of the file in each commit added to the chase.
The files will be restored to the original state once the chase is over.
If the file is not found in any of the commits, the chase will fail.
```

Example:

```lua

local patches = {{
  path = "src/fs/tests.rs", code = [[

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
]]
  },
}
```
