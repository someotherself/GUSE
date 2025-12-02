# GUSE Chase

## Intro
```text
The app has the ability to run automated builds for a list of commits.
For each commit, a series of shell commands can be ran, including compilations and builds.
Each commit can share the same target, to speed up subsequent builds (incremental compilations).
The chase uses simple scripts written in Lua.
```

## Managing scripts

```text
Each repo will have a folder called chase where all the chase scripts and logs will be found.

A new script template can be created using:
$ GUSE script new REPO_NAME SCRIPT_NAME
This will create the folder and place a blank script inside:
> REPO_NAME/chase/SCRIPT_NAME/script.lua

A script can then be deleted:
$ GUSE script remove REPO_NAME SCRIPT_NAME

It can also be renamed:
$ GUSE script remove REPO_NAME OLD_SCRIPT_NAME NEW_SCRIPT_NAME
```

A blank script will look like this:
```lua
local commits = {
}

local commands = {
}

-- Sets the run mode. Can be "Binary" or "Continuous"
local run_mode = "Continuous"
-- Sets the build mode. Can be "FirstFailure" or "Continuous"
local stop_mode = "FirstFailure"

-- Load functions
for _, oid in ipairs(commits) do
  cfg.add_commit(oid)
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
For the moment, commits can only be added manually, one by one. Commit ranges are not supported.
They can be entered as:
```
```lua
local commits = {"b074789", "0f9cd69", "c155149", "b2e00c3"}
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
- Binary is is a binary order (same as git bisect)
```

## Stop mode
```text
Stop mode is the failure behavious, in case of a exit failure from the cli.
This field is optional. The default value will be Continuous
- Continuous will not stop for failures and run through the whole list
- FirstFailure will stop at the first exit failure.
```
