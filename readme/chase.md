# guse Chase

## INFO: This feature is still experimental and is geting improved.

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
