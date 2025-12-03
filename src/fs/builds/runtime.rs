use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use mlua::Lua;

use crate::fs::builds::reporter::{ChaseError, GuseResult};

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
    pub commits: Vec<String>,
    pub commands: Vec<String>,
    pub run_mode: ChaseRunMode,
    pub stop_mode: ChaseStopMode,
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

            {
                let commits_ref = Arc::clone(&lua_config);
                let add_commit = scope
                    .create_function(move |_, oid: String| {
                        commits_ref.lock().unwrap().commits.push(oid);
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

            {
                let commands_ref = Arc::clone(&lua_config);
                let add_command = scope
                    .create_function(move |_, oid: String| {
                        commands_ref.lock().unwrap().commands.push(oid);
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

            {
                let run_mode_ref = Arc::clone(&lua_config);
                let set_run_mode = scope
                    .create_function(move |_, run_mode: String| {
                        let chase_run_mode = ChaseRunMode::from_str(&run_mode);
                        let run_opt = chase_run_mode.unwrap_or_default();
                        run_mode_ref.lock().unwrap().run_mode = run_opt;
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

            {
                let stop_mode_ref = Arc::clone(&lua_config);
                let set_stop_mode = scope
                    .create_function(move |_, stop_mode: String| {
                        let chase_mode = ChaseStopMode::from_str(&stop_mode);
                        let stop_opt = chase_mode.unwrap_or_default();
                        stop_mode_ref.lock().unwrap().stop_mode = stop_opt;
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

        let config = Arc::try_unwrap(lua_config).unwrap().into_inner().unwrap();
        config.check_config_fields()?;
        Ok(config)
    }

    fn check_config_fields(&self) -> GuseResult<()> {
        if self.commits.is_empty() {
            return Err(ChaseError::NoCommits);
        }
        if self.commands.is_empty() {
            return Err(ChaseError::NoCommands);
        }
        Ok(())
    }
}
