use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use mlua::Lua;
use parking_lot::Mutex;

use crate::fs::builds::reporter::{ChaseError, GuseResult};

#[derive(Debug)]
pub enum InputTypes {
    Commit,
    Range,
    Pr,
    Branch,
    Unknown(String),
}

impl InputTypes {
    fn from_str(itype: &str) -> Self {
        match itype.to_lowercase().as_str() {
            "range" => Self::Range,
            "pr" => Self::Pr,
            "branch" => Self::Branch,
            "commit" => Self::Commit,
            _ => Self::Unknown(itype.to_string()),
        }
    }

    fn is_unknown(&self) -> bool {
        matches!(self, Self::Unknown(_))
    }

    fn get_input(&self) -> Option<String> {
        match self {
            Self::Unknown(input) => Some(input.to_string()),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub enum ChaseRunMode {
    #[default]
    Continuous,
    Binary,
}

impl ChaseRunMode {
    fn from_str(mode: &str) -> Option<Self> {
        match mode.to_lowercase().as_str() {
            "continuous" => Some(Self::Continuous),
            "binary" => Some(Self::Binary),
            _ => None,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum ChaseStopMode {
    #[default]
    Continuous,
    FirstFailure,
}

impl ChaseStopMode {
    fn from_str(mode: &str) -> Option<Self> {
        match mode.to_lowercase().as_str() {
            "firstfailure" => Some(Self::FirstFailure),
            "continuous" => Some(Self::Continuous),
            _ => None,
        }
    }
}

#[derive(Default, Debug)]
pub struct LuaConfig {
    pub commits: Vec<(InputTypes, String)>,
    pub commands: Vec<String>,
    pub run_mode: ChaseRunMode,
    pub stop_mode: ChaseStopMode,
    pub patches: Vec<(PathBuf, String)>,
}

impl LuaConfig {
    pub fn read_lua(path: &Path) -> GuseResult<Self> {
        let lua = Lua::new();
        let script_path = path.join("chase.lua");
        let lua_src = std::fs::read_to_string(&script_path)
            .map_err(|_| ChaseError::ScriptNotFound { path: script_path })?;
        let globals = lua.globals();

        let lua_config = Arc::new(Mutex::new(LuaConfig::default()));

        let scope = |scope: &Lua| -> GuseResult<()> {
            let cfg = lua.create_table().map_err(|e| ChaseError::LuaError {
                source: e,
                msg: "Could not create cfg table: ".to_string(),
            })?;

            // ADD_COMMITS
            {
                let commits_ref = Arc::clone(&lua_config);
                let add_commit = scope
                    .create_function(move |_, (input_type, oid): (String, String)| {
                        let input_type = InputTypes::from_str(&input_type);
                        commits_ref.lock().commits.push((input_type, oid));
                        Ok(())
                    })
                    .map_err(|e| ChaseError::LuaError {
                        source: e,
                        msg: "Could not create add_commit function: ".to_string(),
                    })?;
                cfg.set("add_commit", add_commit)
                    .map_err(|e| ChaseError::LuaError {
                        source: e,
                        msg: "Error setting cfg table: ".to_string(),
                    })?;
            }

            // ADD_COMMANDS
            {
                let commands_ref = Arc::clone(&lua_config);
                let add_command = scope
                    .create_function(move |_, command: String| {
                        commands_ref.lock().commands.push(command);
                        Ok(())
                    })
                    .map_err(|e| ChaseError::LuaError {
                        source: e,
                        msg: "Could not create add_command function: ".to_string(),
                    })?;
                cfg.set("add_command", add_command)
                    .map_err(|e| ChaseError::LuaError {
                        source: e,
                        msg: "Error setting cfg table: ".to_string(),
                    })?;
            }

            // RUN_MODE
            {
                let run_mode_ref = Arc::clone(&lua_config);
                let set_run_mode = scope
                    .create_function(move |_, run_mode: String| {
                        let chase_run_mode = ChaseRunMode::from_str(&run_mode);
                        let run_opt = chase_run_mode.unwrap_or_default();
                        run_mode_ref.lock().run_mode = run_opt;
                        Ok(())
                    })
                    .map_err(|e| ChaseError::LuaError {
                        source: e,
                        msg: "Could not create set_run_mode function".to_string(),
                    })?;
                cfg.set("set_run_mode", set_run_mode)
                    .map_err(|e| ChaseError::LuaError {
                        source: e,
                        msg: "Error setting cfg table: ".to_string(),
                    })?;
            }

            // STOP_MODE
            {
                let stop_mode_ref = Arc::clone(&lua_config);
                let set_stop_mode = scope
                    .create_function(move |_, stop_mode: String| {
                        let chase_mode = ChaseStopMode::from_str(&stop_mode);
                        let stop_opt = chase_mode.unwrap_or_default();
                        stop_mode_ref.lock().stop_mode = stop_opt;
                        Ok(())
                    })
                    .map_err(|e| ChaseError::LuaError {
                        source: e,
                        msg: "Could not create set_stop_mode function".to_string(),
                    })?;
                cfg.set("set_stop_mode", set_stop_mode)
                    .map_err(|e| ChaseError::LuaError {
                        source: e,
                        msg: "Error setting cfg table: ".to_string(),
                    })?;
            }

            // ADD_
            {
                let patches_ref = Arc::clone(&lua_config);
                let set_patches = scope
                    .create_function(move |_, (path, patch): (String, String)| {
                        patches_ref
                            .lock()
                            .patches
                            .push((PathBuf::from(path), patch));
                        Ok(())
                    })
                    .map_err(|e| ChaseError::LuaError {
                        source: e,
                        msg: "Could not create add_patch function".to_string(),
                    })?;
                cfg.set("add_patch", set_patches)
                    .map_err(|e| ChaseError::LuaError {
                        source: e,
                        msg: "Error setting cfg table: ".to_string(),
                    })?;
            }

            globals.set("cfg", cfg).map_err(|e| ChaseError::LuaError {
                source: e,
                msg: "Error setting cfg table: ".to_string(),
            })?;

            lua.load(&lua_src)
                .set_name("chase.lua")
                .exec()
                .map_err(|e| ChaseError::LuaError {
                    source: e,
                    msg: "Error running exec on cfg table.".to_string(),
                })?;

            globals
                .set("cfg", mlua::Value::Nil)
                .map_err(|e| ChaseError::LuaError {
                    source: e,
                    msg: "Error setting cfg table.".to_string(),
                })?;
            Ok(())
        };

        scope(&lua)?;

        lua.gc_collect().map_err(|e| ChaseError::LuaError {
            source: e,
            msg: "Could not run lua GC: ".to_string(),
        })?;
        lua.gc_collect().map_err(|e| ChaseError::LuaError {
            source: e,
            msg: "Could not run lua GC: ".to_string(),
        })?;

        let config = Arc::try_unwrap(lua_config).unwrap().into_inner();
        config.check_config_fields()?;
        Ok(config)
    }

    fn check_config_fields(&self) -> GuseResult<()> {
        for (input, oid) in &self.commits {
            if input.is_unknown() {
                let input_string = input.get_input().unwrap_or("".to_string());
                return Err(ChaseError::BadInputType {
                    input: input_string,
                    oid: oid.to_string(),
                });
            }
        }

        if self.commits.is_empty() {
            return Err(ChaseError::NoCommits);
        }
        if self.commands.is_empty() {
            return Err(ChaseError::NoCommands);
        }
        Ok(())
    }
}
