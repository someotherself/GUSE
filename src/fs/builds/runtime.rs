use std::{
    path::Path,
    sync::{Arc, Mutex},
};

use mlua::Lua;

#[derive(Debug)]
pub enum ChaseMode {
    Continuous,
    Binary,
}

impl ChaseMode {
    fn from_str(mode: &str) -> Option<Self> {
        match mode.to_lowercase().as_str() {
            "continuous" => Some(Self::Continuous),
            "binary" => Some(Self::Binary),
            _ => None,
        }
    }
}

#[derive(Default, Debug)]
pub struct LuaConfig {
    pub commits: Vec<String>,
    pub mode: Option<ChaseMode>,
}

impl LuaConfig {
    pub fn read_lua(path: &Path) -> mlua::Result<Self> {
        let lua = Lua::new();
        let lua_src = std::fs::read_to_string(path.join("chase.lua"))?;
        let globals = lua.globals();

        let lua_config = Arc::new(Mutex::new(LuaConfig::default()));

        lua.scope(|scope| {
            let cfg = lua.create_table()?;

            {
                let commits_ref = Arc::clone(&lua_config);
                let add_commit = scope.create_function(move |_, oid: String| {
                    commits_ref.lock().unwrap().commits.push(oid);
                    Ok(())
                })?;
                cfg.set("add_commit", add_commit)?;
            }

            {
                let mode_ref = Arc::clone(&lua_config);
                let set_mode = scope.create_function(move |_, mode: String| {
                    let chase_mode = ChaseMode::from_str(&mode);
                    mode_ref.lock().unwrap().mode = chase_mode;
                    Ok(())
                })?;
                cfg.set("set_mode", set_mode)?;
            }

            globals.set("cfg", cfg)?;

            lua.load(&lua_src).set_name("chase.lua").exec()?;

            globals.set("cfg", mlua::Value::Nil)?;
            Ok(())
        })?;

        lua.gc_collect()?;
        lua.gc_collect()?;

        let config = Arc::try_unwrap(lua_config).unwrap().into_inner().unwrap();
        Ok(config)
    }
}
