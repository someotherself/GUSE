use std::{
    collections::HashMap,
    sync::{Arc, atomic::AtomicBool},
};

use parking_lot::{Condvar, Mutex};

use crate::fs::builds::{
    chase::{CHASE_STOP_FLAGS, ChaseId, next_chase_id},
    logger::CmdResult,
};

/// Holds the state of a process (job) started by a ChaseRunner::run_command_on_snap
pub enum ChaseState {
    Running,
    Stopping,
    Stopped,
}

/// Holds the context of a process (job) started by a ChaseRunner::run_command_on_snap
pub struct ChaseHandle {
    pub stop_flag: Arc<AtomicBool>,
    pub state: Mutex<ChaseState>,
    pub cv: Condvar,
}

// The purpose of ChaseHandle and the ChaseState is to provide a more graceful termination of the process.
// In order to perform the cleanup and to finish logging everything
// The process is as follows:
// User types the GUSE chase command
// ControlReq::Connect is sent. A ChaseHandle::new() and new ChaseId are added to CHASE_STOP_FLAGS
// ControlRes::Accept is received with the ChaseId
// ControlReq::Chase is sent and ChaseHandle is set to ChaseState::Running
// In the event of a ctrlc, a ControlReq::StopChase { ChaseId } is sent, the ChaseHandle is set to ChaseState::Stopping
// ChaseHandle::stop_flag is set to true, and process the is killed.
// After ChaseRunner is dropped, the ChaseHandle is set to ChaseState::Stopped. The socked is kept alive until now.
impl ChaseHandle {
    pub fn new(state: ChaseState) -> Self {
        Self {
            stop_flag: Arc::new(AtomicBool::new(false)),
            state: Mutex::new(state),
            cv: Condvar::new(),
        }
    }

    pub fn get_handle(id: &ChaseId) -> Option<Arc<ChaseHandle>> {
        if let Some(reg) = CHASE_STOP_FLAGS.get()
            && let Some(handle) = reg.lock().get(id)
        {
            return Some(handle.clone());
        }
        None
    }

    pub fn set_stop_flag(id: ChaseId) -> CmdResult<()> {
        let Some(map) = CHASE_STOP_FLAGS.get() else {
            return CmdResult::Err(format!("Chade Id: {} does not exist", id));
        };
        let guard = map.lock();
        let Some(handle) = guard.get(&id) else {
            return CmdResult::Err(format!("Chade Id: {} does not exist", id));
        };

        handle
            .stop_flag
            .store(true, std::sync::atomic::Ordering::Relaxed);

        {
            let mut state = handle.state.lock();
            *state = ChaseState::Stopping;
            handle.cv.notify_all();
        }
        CmdResult::Ok(())
    }

    pub fn start_run(id: ChaseId) -> CmdResult<()> {
        let Some(map) = CHASE_STOP_FLAGS.get() else {
            return CmdResult::Err(format!("Chade Id: {} does not exist", id));
        };
        let guard = map.lock();
        let Some(handle) = guard.get(&id) else {
            return CmdResult::Err(format!("Chade Id: {} does not exist", id));
        };
        let mut state = handle.state.lock();
        *state = ChaseState::Running;
        CmdResult::Ok(())
    }

    fn chase_id_reg() -> &'static Mutex<HashMap<ChaseId, Arc<ChaseHandle>>> {
        CHASE_STOP_FLAGS.get_or_init(|| Mutex::new(HashMap::new()))
    }

    pub fn register_chase_id() -> CmdResult<ChaseId> {
        let mut guard = ChaseHandle::chase_id_reg().lock();
        let id = next_chase_id();
        let handle: ChaseHandle = ChaseHandle::new(ChaseState::Stopped);
        guard.insert(id, Arc::new(handle));
        CmdResult::Ok(id as ChaseId)
    }

    pub fn deregister_chase_id(id: ChaseId) -> CmdResult<()> {
        let Some(map) = CHASE_STOP_FLAGS.get() else {
            return CmdResult::Err(format!("Chade Id: {} does not exist", id));
        };
        map.lock().remove(&id);
        CmdResult::Ok(())
    }
}
