local commits = {
}

local commands = {
}

-- Sets the build mode. Can be "Binary" or "Continuous"
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
