local commits = {
}

local commands = {
}

-- Sets the run mode. Can be "Binary" or "Continuous"
local run_mode = "Continuous"
-- Sets the build mode. Can be "FirstFailure" or "Continuous"
local stop_mode = "Continuous"

  -- HOW TO ADD COMMITS:
  -- single commits:            local commits = {"hash", "hash", "hash",}
  -- a complete branch or Pr:   local commits = {Branch = "branch_name"} / { Pr = "Pr_number" }
  -- a range of commits:        local commits = {Range = "hash...hash"}
  -- Or any combination of them

-- Load functions
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
