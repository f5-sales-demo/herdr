use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::api::schema::{
    AgentTurnActionAckParams, AgentTurnActionRecord, AgentTurnActionState, AgentTurnActionTarget,
    ExecutionCommand, ExecutionRecord, ExecutionResumeParams, ExecutionStartParams, ExecutionState,
    NativeExecutableBinding, NativeLaunchV3, NativeSessionHeaderBinding,
};

const MAX_RECORDS: usize = 256;
const OUTPUT_TAIL_BYTES: usize = 4096;
const MAX_NATIVE_EXECUTABLE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Clone)]
pub(crate) struct ExecutionManager(Arc<Inner>);
struct Inner {
    path: PathBuf,
    state: Mutex<State>,
    pane_executions: Mutex<HashMap<u32, String>>,
    pending_pane_output: Mutex<HashMap<u32, CapturedOutput>>,
    native_capabilities: Mutex<HashMap<String, String>>,
}
#[derive(Default)]
struct CapturedOutput {
    bytes: u64,
    tail: String,
    truncated: bool,
}
#[derive(Default, serde::Serialize, serde::Deserialize)]
struct State {
    revision: u64,
    records: Vec<ExecutionRecord>,
    /// Native semantic identities outlive the bounded visible-record window.
    /// This is deliberately separate from tombstones: a semantic task may
    /// continue with a newer generation, but no earlier generation may be
    /// relaunched and no ordinary execution may claim its identity.
    #[serde(default)]
    native_reservations: BTreeMap<String, u64>,
    #[serde(default)]
    tombstones: Vec<ExecutionTombstone>,
    #[serde(default)]
    expired_id_filter: Vec<u8>,
    #[serde(default)]
    native_capability_verifiers: BTreeMap<String, String>,
    #[serde(default)]
    native_actions: BTreeMap<String, AgentTurnActionRecord>,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct ExecutionTombstone {
    execution_id: String,
    #[serde(default)]
    semantic_execution_id: Option<String>,
    #[serde(default)]
    generation: Option<u64>,
    command: ExecutionCommand,
    cwd: String,
}

impl ExecutionManager {
    pub(crate) fn global() -> &'static Self {
        static MANAGER: std::sync::OnceLock<ExecutionManager> = std::sync::OnceLock::new();
        MANAGER.get_or_init(Self::load)
    }
    pub(crate) fn load() -> Self {
        Self::load_at(crate::session::data_dir().join("executions.json"))
    }

    fn load_at(path: PathBuf) -> Self {
        let mut state: State = std::fs::read(&path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let now = now_ms();
        let mut changed = false;
        for record in &mut state.records {
            if matches!(
                record.state,
                ExecutionState::Starting | ExecutionState::Running
            ) {
                state.revision += 1;
                record.revision = state.revision;
                record.state = ExecutionState::Lost;
                record.finished_at_unix_ms = Some(now);
                record.evidence_gap = Some("server restarted without retained child ownership; process survival and exit status are unknown".into());
                changed = true;
            }
        }
        let manager = Self(Arc::new(Inner {
            path,
            state: Mutex::new(state),
            pane_executions: Mutex::new(HashMap::new()),
            pending_pane_output: Mutex::new(HashMap::new()),
            native_capabilities: Mutex::new(HashMap::new()),
        }));
        if changed {
            let _ = manager.persist();
        }
        manager
    }

    pub(crate) fn admit_visible(
        &self,
        params: &ExecutionStartParams,
    ) -> Result<(ExecutionRecord, bool), String> {
        validate(params)?;
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| "execution state lock poisoned")?;
        if let Some(existing) = state
            .records
            .iter()
            .find(|record| record.execution_id == params.execution_id)
        {
            if existing.cwd == params.cwd && existing.command == params.command {
                return Ok((existing.clone(), false));
            }
            return Err("execution_id_conflict: execution id is already admitted with a different specification".into());
        }
        if state
            .records
            .iter()
            .any(|record| record.semantic_execution_id.as_deref() == Some(&params.execution_id))
            || state.native_reservations.contains_key(&params.execution_id)
            || state.tombstones.iter().any(|tombstone| {
                tombstone.semantic_execution_id.as_deref() == Some(&params.execution_id)
            })
        {
            return Err("execution_namespace_conflict: execution id is reserved by a native semantic execution".into());
        }
        if state
            .tombstones
            .iter()
            .any(|old| old.execution_id == params.execution_id)
            || expired_filter_contains(&state.expired_id_filter, &params.execution_id)
        {
            return Err("execution_expired: execution id was previously admitted but its record expired; use a new id".into());
        }
        state.revision += 1;
        let record = ExecutionRecord {
            execution_id: params.execution_id.clone(),
            backend_execution_id: None,
            semantic_execution_id: None,
            generation: None,
            native_producer: None,
            native_executable: None,
            native_launch: None,
            producer_session_id: None,
            injected_env: BTreeMap::new(),
            superseded_by_backend_execution_id: None,
            cwd: params.cwd.clone(),
            command: params.command.clone(),
            state: ExecutionState::Starting,
            revision: state.revision,
            admitted_at_unix_ms: now_ms(),
            started_at_unix_ms: None,
            finished_at_unix_ms: None,
            pid: None,
            pane_id: None,
            tab_id: None,
            exit_code: None,
            signal: None,
            signal_name: None,
            cancel_requested: false,
            stdout_bytes: 0,
            stderr_bytes: 0,
            stdout_tail: String::new(),
            stderr_tail: String::new(),
            output_truncated: false,
            output_complete: false,
            evidence_gap: None,
        };
        state.records.push(record.clone());
        self.enforce_retention(&mut state);
        self.persist_locked(&state)?;
        Ok((record, true))
    }

    /// Persist the generation claim before an app runtime creates a child.
    /// A retry returns precisely the previously claimed child; a changed
    /// specification, a stale generation, or a producer-owned foreign binding
    /// is rejected rather than being allowed to run a second child.
    pub(crate) fn admit_xcsh_resume(
        &self,
        params: &ExecutionResumeParams,
    ) -> Result<(ExecutionRecord, bool, Option<ExecutionRecord>), String> {
        validate_resume(params)?;
        let executable = measure_xcsh_executable(&params.native_launch.xcsh_executable)?;
        let session_header = measure_xcsh_session_header(&params.native_launch)?;
        let backend_execution_id = native_backend_id(&params.execution_id, params.generation);
        let command = ExecutionCommand::Argv {
            argv: native_xcsh_argv(&executable, &params.native_launch, &params.text)?,
        };
        let injected_env = BTreeMap::from([
            ("HERDR_EXECUTION_ID".into(), params.execution_id.clone()),
            (
                "HERDR_EXECUTION_GENERATION".into(),
                params.generation.to_string(),
            ),
        ]);
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| "execution state lock poisoned")?;
        if state.records.iter().any(|record| {
            record.execution_id == params.execution_id && record.semantic_execution_id.is_none()
        }) {
            return Err("execution_namespace_conflict: semantic execution id is already owned by an ordinary execution".into());
        }
        if let Some(existing) = state.records.iter().find(|record| {
            record.semantic_execution_id.as_deref() == Some(&params.execution_id)
                && record.generation == Some(params.generation)
        }) {
            if existing.backend_execution_id.as_deref() == Some(&backend_execution_id)
                && existing.cwd == params.cwd
                && existing.command == command
                && existing.producer_session_id.as_deref() == Some(&session_header.id)
                && existing.native_executable.as_ref() == Some(&executable)
                && existing.native_launch.as_ref() == Some(&params.native_launch)
                && existing.injected_env == injected_env
            {
                return Ok((existing.clone(), false, None));
            }
            return Err("execution_generation_conflict: semantic generation is already bound to a different child specification".into());
        }
        if state.records.iter().any(|record| {
            record.semantic_execution_id.as_deref() == Some(&params.execution_id)
                && record
                    .generation
                    .is_some_and(|generation| generation > params.generation)
        }) {
            return Err(
                "execution_generation_stale: a newer semantic generation is already bound".into(),
            );
        }
        let retained_generation = state
            .native_reservations
            .get(&params.execution_id)
            .copied()
            .into_iter()
            .chain(
                state
                    .tombstones
                    .iter()
                    .filter(|tombstone| {
                        tombstone.semantic_execution_id.as_deref() == Some(&params.execution_id)
                    })
                    .filter_map(|tombstone| tombstone.generation),
            )
            .max();
        if let Some(retained_generation) = retained_generation {
            if retained_generation == params.generation {
                return Err("execution_expired: semantic generation was previously admitted but its visible record expired; use a newer generation".into());
            }
            if retained_generation > params.generation {
                return Err("execution_generation_stale: a newer retained semantic generation is already bound".into());
            }
        }
        if state
            .records
            .iter()
            .any(|record| record.execution_id == backend_execution_id)
        {
            return Err(
                "execution_backend_id_conflict: generated backend execution id is already owned"
                    .into(),
            );
        }
        let previous_index = state.records.iter().position(|record| {
            record.semantic_execution_id.as_deref() == Some(&params.execution_id)
                && matches!(
                    record.state,
                    ExecutionState::Starting | ExecutionState::Running
                )
        });
        state.revision += 1;
        let revision = state.revision;
        state
            .native_reservations
            .insert(params.execution_id.clone(), params.generation);
        let native_capability = uuid::Uuid::new_v4().simple().to_string();
        state
            .native_capability_verifiers
            .insert(backend_execution_id.clone(), sha256_hex(&native_capability));
        self.0
            .native_capabilities
            .lock()
            .map_err(|_| "native capability lock poisoned")?
            .insert(backend_execution_id.clone(), native_capability);
        let record = ExecutionRecord {
            execution_id: backend_execution_id.clone(),
            backend_execution_id: Some(backend_execution_id.clone()),
            semantic_execution_id: Some(params.execution_id.clone()),
            generation: Some(params.generation),
            native_producer: Some("xcsh".into()),
            native_executable: Some(executable),
            native_launch: Some(params.native_launch.clone()),
            producer_session_id: Some(session_header.id),
            injected_env,
            superseded_by_backend_execution_id: None,
            cwd: params.cwd.clone(),
            command,
            state: ExecutionState::Starting,
            revision,
            admitted_at_unix_ms: now_ms(),
            started_at_unix_ms: None,
            finished_at_unix_ms: None,
            pid: None,
            pane_id: None,
            tab_id: None,
            exit_code: None,
            signal: None,
            signal_name: None,
            cancel_requested: false,
            stdout_bytes: 0,
            stderr_bytes: 0,
            stdout_tail: String::new(),
            stderr_tail: String::new(),
            output_truncated: false,
            output_complete: false,
            evidence_gap: None,
        };
        let previous = previous_index.map(|index| {
            let old = &mut state.records[index];
            old.cancel_requested = true;
            old.superseded_by_backend_execution_id = Some(backend_execution_id.clone());
            old.revision = revision;
            old.clone()
        });
        state.records.push(record.clone());
        self.enforce_retention(&mut state);
        self.persist_locked(&state)?;
        Ok((record, true, previous))
    }

    pub(crate) fn attach_visible(
        &self,
        id: &str,
        pane_raw: u32,
        pane_id: String,
        tab_id: String,
        pid: Option<u32>,
    ) -> Result<ExecutionRecord, String> {
        self.0
            .pane_executions
            .lock()
            .map_err(|_| "execution pane map lock poisoned")?
            .insert(pane_raw, id.into());
        let pending = self
            .0
            .pending_pane_output
            .lock()
            .ok()
            .and_then(|mut pending| pending.remove(&pane_raw));
        self.mutate(id, |record| {
            record.pane_id = Some(pane_id);
            record.tab_id = Some(tab_id);
            record.pid = pid;
            record.started_at_unix_ms = Some(now_ms());
            record.state = ExecutionState::Running;
            if let Some(pending) = pending {
                record.stdout_bytes = pending.bytes;
                record.stdout_tail = pending.tail;
                record.output_truncated = pending.truncated;
            }
        })?;
        self.get(id).ok_or_else(|| "execution_not_found".into())
    }

    pub(crate) fn finish_visible(
        &self,
        pane_raw: u32,
        status: Option<&portable_pty::ExitStatus>,
        error: Option<&str>,
    ) {
        let id = self
            .0
            .pane_executions
            .lock()
            .ok()
            .and_then(|map| map.get(&pane_raw).cloned());
        let Some(id) = id else {
            if let Ok(mut pending) = self.0.pending_pane_output.lock() {
                pending.remove(&pane_raw);
            }
            return;
        };
        let _ = self.mutate(&id, |record| {
            record.finished_at_unix_ms = Some(now_ms());
            match status {
                Some(status) => {
                    if let Some(signal) = status.signal() {
                        record.signal_name = Some(signal.into());
                        record.state = if record.cancel_requested {
                            ExecutionState::Cancelled
                        } else {
                            ExecutionState::Exited
                        };
                    } else {
                        record.exit_code = i32::try_from(status.exit_code()).ok();
                        record.state = if record.cancel_requested && !status.success() {
                            ExecutionState::Cancelled
                        } else {
                            ExecutionState::Exited
                        };
                    }
                }
                None => {
                    record.state = ExecutionState::Lost;
                    record.evidence_gap = Some(
                        error
                            .unwrap_or("PTY child wait failed; exit status is unknown")
                            .into(),
                    );
                }
            }
        });
        if self.get(&id).is_some_and(|record| record.output_complete) {
            if let Ok(mut map) = self.0.pane_executions.lock() {
                map.remove(&pane_raw);
            }
        }
    }

    pub(crate) fn observe_visible_output(&self, pane_raw: u32, bytes: &[u8]) {
        let id = self
            .0
            .pane_executions
            .lock()
            .ok()
            .and_then(|map| map.get(&pane_raw).cloned());
        let Some(id) = id else {
            if let Ok(mut pending) = self.0.pending_pane_output.lock() {
                if pending.len() >= MAX_RECORDS && !pending.contains_key(&pane_raw) {
                    if let Some(oldest) = pending.keys().next().copied() {
                        pending.remove(&oldest);
                    }
                }
                append_captured(pending.entry(pane_raw).or_default(), bytes);
            }
            return;
        };
        let _ = self.mutate(&id, |record| {
            record.stdout_bytes = record.stdout_bytes.saturating_add(bytes.len() as u64);
            append_bounded_tail(&mut record.stdout_tail, &mut record.output_truncated, bytes);
        });
    }

    pub(crate) fn finish_visible_output(&self, pane_raw: u32) {
        let id = self
            .0
            .pane_executions
            .lock()
            .ok()
            .and_then(|map| map.get(&pane_raw).cloned());
        if let Some(id) = id {
            let _ = self.mutate(&id, |record| record.output_complete = true);
            if self.get(&id).is_some_and(|record| {
                !matches!(
                    record.state,
                    ExecutionState::Starting | ExecutionState::Running
                )
            }) {
                if let Ok(mut map) = self.0.pane_executions.lock() {
                    map.remove(&pane_raw);
                }
            }
        }
        if let Ok(mut pending) = self.0.pending_pane_output.lock() {
            pending.remove(&pane_raw);
        }
    }

    pub(crate) fn request_visible_cancel(&self, id: &str) -> Result<ExecutionRecord, String> {
        let record = self.get(id).ok_or("execution_not_found")?;
        if !matches!(
            record.state,
            ExecutionState::Starting | ExecutionState::Running
        ) {
            return Ok(record);
        }
        self.mutate(id, |record| record.cancel_requested = true)?;
        self.get(id).ok_or_else(|| "execution_not_found".into())
    }
    pub(crate) fn native_capability(&self, id: &str) -> Option<String> {
        self.0.native_capabilities.lock().ok()?.get(id).cloned()
    }
    pub(crate) fn request_native_cancel(&self, id: &str) -> Result<ExecutionRecord, String> {
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| "execution state lock poisoned")?;
        let index = state
            .records
            .iter()
            .position(|record| record.execution_id == id)
            .ok_or("execution_not_found")?;
        if state.records[index].native_launch.is_none()
            || !matches!(
                state.records[index].state,
                ExecutionState::Starting | ExecutionState::Running
            )
        {
            return Ok(state.records[index].clone());
        }
        state.revision += 1;
        let revision = state.revision;
        {
            let record = &mut state.records[index];
            record.cancel_requested = true;
            record.revision = revision;
        }
        state
            .native_actions
            .entry(id.into())
            .or_insert(AgentTurnActionRecord {
                backend_execution_id: id.into(),
                action_id: "cancel".into(),
                action_revision: 1,
                state: AgentTurnActionState::Requested,
                requested_at_unix_ms: now_ms(),
                acknowledged_at_unix_ms: None,
            });
        let result = state.records[index].clone();
        self.persist_locked(&state)?;
        Ok(result)
    }
    pub(crate) fn native_actions(
        &self,
        target: &AgentTurnActionTarget,
    ) -> Result<Vec<AgentTurnActionRecord>, String> {
        let record = self.authorize_native_action(target)?;
        let state = self
            .0
            .state
            .lock()
            .map_err(|_| "execution state lock poisoned")?;
        Ok(state
            .native_actions
            .get(&record.execution_id)
            .filter(|action| action.action_revision > target.after_revision)
            .cloned()
            .into_iter()
            .collect())
    }
    pub(crate) fn acknowledge_native_action(
        &self,
        params: &AgentTurnActionAckParams,
    ) -> Result<(AgentTurnActionRecord, bool), String> {
        let record = self.authorize_native_action(&params.target)?;
        if params.action_id != "cancel" || params.action_revision != 1 {
            return Err("agent_turn_action_not_found".into());
        }
        if params.state != AgentTurnActionState::SafePoint {
            return Err(
                "agent_turn_action_invalid_state: only safe_point acknowledgement is accepted"
                    .into(),
            );
        }
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| "execution state lock poisoned")?;
        let action = state
            .native_actions
            .get_mut(&record.execution_id)
            .ok_or("agent_turn_action_not_found")?;
        if action.state == AgentTurnActionState::SafePoint {
            return Ok((action.clone(), false));
        }
        if now_ms().saturating_sub(action.requested_at_unix_ms) > 30_000 {
            return Err("agent_turn_action_safe_point_timeout".into());
        }
        action.state = AgentTurnActionState::SafePoint;
        action.acknowledged_at_unix_ms = Some(now_ms());
        let action = action.clone();
        self.persist_locked(&state)?;
        Ok((action, true))
    }
    fn authorize_native_action(
        &self,
        target: &AgentTurnActionTarget,
    ) -> Result<ExecutionRecord, String> {
        let record = self.resolve_agent_turn_execution(
            &target.execution_id,
            &target.producer,
            &target.session_id,
            target.generation,
        )?;
        if record.pane_id.as_deref() != Some(target.pane_id.as_str()) {
            return Err("agent_turn_provenance_mismatch: pane is not owned by execution".into());
        }
        let state = self
            .0
            .state
            .lock()
            .map_err(|_| "execution state lock poisoned")?;
        if state.native_capability_verifiers.get(&record.execution_id)
            != Some(&sha256_hex(&target.native_capability))
        {
            return Err("agent_turn_native_capability_mismatch".into());
        }
        Ok(record)
    }
    pub(crate) fn authorize_native_report_capability(
        &self,
        record: &ExecutionRecord,
        capability: Option<&str>,
    ) -> Result<(), String> {
        if record.native_launch.is_none() {
            return if capability.is_some() {
                Err("agent_turn_native_capability_unexpected".into())
            } else {
                Ok(())
            };
        }
        let state = self
            .0
            .state
            .lock()
            .map_err(|_| "execution state lock poisoned")?;
        if state
            .native_capability_verifiers
            .get(&record.execution_id)
            .map(String::as_str)
            == capability.map(sha256_hex).as_deref()
        {
            Ok(())
        } else {
            Err("agent_turn_native_capability_mismatch".into())
        }
    }
    pub(crate) fn mark_start_failed(&self, id: &str, message: &str) {
        let _ = self.mutate(id, |record| {
            record.state = ExecutionState::Lost;
            record.finished_at_unix_ms = Some(now_ms());
            record.evidence_gap = Some(format!("visible PTY start failed: {message}"));
        });
    }

    pub(crate) fn get(&self, id: &str) -> Option<ExecutionRecord> {
        self.0
            .state
            .lock()
            .ok()?
            .records
            .iter()
            .find(|r| r.execution_id == id)
            .cloned()
    }

    /// Native children report their semantic id from immutable environment,
    /// while Herdr owns a separate backend child id. Resolve and validate the
    /// durable binding instead of trusting reporter-provided provenance.
    pub(crate) fn resolve_agent_turn_execution(
        &self,
        semantic_or_backend_id: &str,
        producer: &str,
        session_id: &str,
        generation: u64,
    ) -> Result<ExecutionRecord, String> {
        let state = self
            .0
            .state
            .lock()
            .map_err(|_| "execution state lock poisoned")?;
        if let Some(record) = state.records.iter().find(|record| {
            record.semantic_execution_id.as_deref() == Some(semantic_or_backend_id)
                && record.generation == Some(generation)
        }) {
            if record.native_producer.as_deref() != Some(producer)
                || record.producer_session_id.as_deref() != Some(session_id)
            {
                return Err("agent_turn_native_binding_mismatch: reporter does not match durable native binding".into());
            }
            return Ok(record.clone());
        }
        state
            .records
            .iter()
            .find(|record| {
                record.execution_id == semantic_or_backend_id
                    && record.semantic_execution_id.is_none()
            })
            .cloned()
            .ok_or_else(|| "agent_turn_execution_not_found".into())
    }
    pub(crate) fn list_since(&self, revision: u64) -> Vec<ExecutionRecord> {
        self.0
            .state
            .lock()
            .map(|s| {
                s.records
                    .iter()
                    .filter(|r| r.revision > revision)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }
    pub(crate) fn wait_since(&self, revision: u64, timeout_ms: u64) -> Vec<ExecutionRecord> {
        let deadline =
            std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms.min(300_000));
        loop {
            let records = self.list_since(revision);
            if !records.is_empty() || std::time::Instant::now() >= deadline {
                return records;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
    fn mutate(&self, id: &str, f: impl FnOnce(&mut ExecutionRecord)) -> Result<(), String> {
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| "execution state lock poisoned")?;
        let next = state.revision + 1;
        state.revision = next;
        let record = state
            .records
            .iter_mut()
            .find(|r| r.execution_id == id)
            .ok_or("execution_not_found")?;
        f(record);
        record.revision = next;
        self.persist_locked(&state)
    }
    fn enforce_retention(&self, state: &mut State) {
        while state.records.len() > MAX_RECORDS {
            let Some(index) = state.records.iter().position(|record| {
                !matches!(
                    record.state,
                    ExecutionState::Starting | ExecutionState::Running
                )
            }) else {
                // Active records carry the durable ownership required for
                // reporter validation, cancellation, and native handoff. Do
                // not trade that authority for a bounded history window.
                break;
            };
            let record = state.records.remove(index);
            expired_filter_insert(&mut state.expired_id_filter, &record.execution_id);
            state.tombstones.push(ExecutionTombstone {
                execution_id: record.execution_id,
                semantic_execution_id: record.semantic_execution_id,
                generation: record.generation,
                command: record.command,
                cwd: record.cwd,
            });
        }
        const MAX_TOMBSTONES: usize = 4096;
        if state.tombstones.len() > MAX_TOMBSTONES {
            state
                .tombstones
                .drain(..state.tombstones.len() - MAX_TOMBSTONES);
        }
    }
    fn persist(&self) -> Result<(), String> {
        let state = self
            .0
            .state
            .lock()
            .map_err(|_| "execution state lock poisoned")?;
        self.persist_locked(&state)
    }
    fn persist_locked(&self, state: &State) -> Result<(), String> {
        let bytes = serde_json::to_vec(state).map_err(|e| e.to_string())?;
        if let Some(parent) = self.0.path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
        let tmp = self
            .0
            .path
            .with_extension(format!("json.{}.tmp", std::process::id()));
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&tmp)
            .map_err(|error| error.to_string())?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.set_permissions(std::fs::Permissions::from_mode(0o600))
                .map_err(|error| error.to_string())?;
        }
        file.write_all(&bytes).map_err(|error| error.to_string())?;
        file.sync_all().map_err(|error| error.to_string())?;
        std::fs::rename(&tmp, &self.0.path).map_err(|error| error.to_string())?;
        #[cfg(unix)]
        if let Some(parent) = self.0.path.parent() {
            std::fs::File::open(parent)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| error.to_string())?;
        }
        Ok(())
    }
}

fn append_captured(captured: &mut CapturedOutput, bytes: &[u8]) {
    captured.bytes = captured.bytes.saturating_add(bytes.len() as u64);
    append_bounded_tail(&mut captured.tail, &mut captured.truncated, bytes);
}

fn append_bounded_tail(tail: &mut String, truncated: &mut bool, bytes: &[u8]) {
    tail.push_str(&redact(&String::from_utf8_lossy(bytes)));
    if tail.len() > OUTPUT_TAIL_BYTES {
        let mut drop_at = tail.len() - OUTPUT_TAIL_BYTES;
        while !tail.is_char_boundary(drop_at) {
            drop_at += 1;
        }
        tail.drain(..drop_at);
        *truncated = true;
    }
}

const EXPIRED_FILTER_BYTES: usize = 8192;

fn expired_filter_indexes(id: &str) -> [usize; 4] {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(id.as_bytes());
    std::array::from_fn(|index| {
        let offset = index * 2;
        u16::from_le_bytes([digest[offset], digest[offset + 1]]) as usize
    })
}

fn expired_filter_insert(filter: &mut Vec<u8>, id: &str) {
    if filter.len() != EXPIRED_FILTER_BYTES {
        filter.resize(EXPIRED_FILTER_BYTES, 0);
    }
    for bit in expired_filter_indexes(id) {
        filter[bit / 8] |= 1 << (bit % 8);
    }
}

fn expired_filter_contains(filter: &[u8], id: &str) -> bool {
    filter.len() == EXPIRED_FILTER_BYTES
        && expired_filter_indexes(id)
            .into_iter()
            .all(|bit| filter[bit / 8] & (1 << (bit % 8)) != 0)
}

fn validate(p: &ExecutionStartParams) -> Result<(), String> {
    if p.execution_id.is_empty()
        || p.execution_id.len() > 80
        || !p
            .execution_id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err("invalid_execution_id".into());
    }
    let cwd = Path::new(&p.cwd);
    if !cwd.is_absolute() || !cwd.is_dir() {
        return Err("invalid_cwd: cwd must be an existing absolute directory".into());
    }
    match &p.command {
        ExecutionCommand::Argv { argv } if argv.is_empty() || argv[0].is_empty() => {
            Err("invalid_argv".into())
        }
        ExecutionCommand::Argv { argv } if argv.len() > 256 => {
            Err("invalid_argv: too many arguments".into())
        }
        ExecutionCommand::Shell { shell, text }
            if !matches!(shell.as_str(), "bash" | "zsh") || text.is_empty() =>
        {
            Err("invalid_shell_command".into())
        }
        _ => Ok(()),
    }
}

fn validate_resume(p: &ExecutionResumeParams) -> Result<(), String> {
    validate(&ExecutionStartParams {
        execution_id: p.execution_id.clone(),
        cwd: p.cwd.clone(),
        workspace_id: None,
        label: None,
        command: ExecutionCommand::Argv {
            argv: vec![p.native_launch.xcsh_executable.clone()],
        },
    })?;
    if p.text.is_empty() || p.text.len() > 65_536 {
        return Err("invalid_execution_resume".into());
    }
    if p.native_launch.version != 3 {
        return Err(
            "invalid_native_launch: execution.resume requires native_launch version 3".into(),
        );
    }
    if !is_canonical_xcsh_session_id(&p.native_launch.session_header.id) {
        return Err("invalid_xcsh_session_id: execution.resume requires the canonical 16-character lowercase hexadecimal XCSH SessionHeader ID; ID prefixes and session paths cannot bind reporter provenance exactly".into());
    }
    if p.native_launch.model.is_empty()
        || p.native_launch.model.len() > 256
        || p.native_launch.model.chars().any(char::is_control)
    {
        return Err("invalid_native_launch: model must be a non-empty non-secret selector".into());
    }
    if !is_sha256_hex(&p.native_launch.session_header.sha256) {
        return Err("invalid_native_launch: session_header.sha256 must be 64 lowercase hexadecimal characters".into());
    }
    // XCSH serializes generations as JavaScript Number. Larger values round
    // and could silently select a different durable generation binding.
    if p.generation > 9_007_199_254_740_991 {
        return Err(
            "invalid_execution_generation: generation exceeds JavaScript safe integer range".into(),
        );
    }
    Ok(())
}

/// Canonicalize and bind the exact XCSH JSONL SessionHeader. The digest is of
/// the first line's original bytes including the LF separator; this makes the
/// durable receipt unambiguous and catches a header rewrite before launch.
pub(crate) fn measure_xcsh_session_header(
    launch: &NativeLaunchV3,
) -> Result<NativeSessionHeaderBinding, String> {
    let requested = Path::new(&launch.session_path);
    if !requested.is_absolute() {
        return Err("invalid_xcsh_session_path: session_path must be an absolute path".into());
    }
    let canonical = std::fs::canonicalize(requested).map_err(|error| {
        format!("invalid_xcsh_session_path: cannot resolve session path: {error}")
    })?;
    let metadata = std::fs::metadata(&canonical).map_err(|error| {
        format!("invalid_xcsh_session_path: cannot inspect session path: {error}")
    })?;
    if !metadata.is_file() {
        return Err("invalid_xcsh_session_path: session_path must name a regular file".into());
    }
    if canonical.to_string_lossy() != launch.session_path {
        return Err("invalid_xcsh_session_path: session_path must already be canonical".into());
    }
    let requested_dir = Path::new(&launch.session_dir);
    if !requested_dir.is_absolute() {
        return Err("invalid_xcsh_session_dir: session_dir must be an absolute path".into());
    }
    let canonical_dir = std::fs::canonicalize(requested_dir).map_err(|error| {
        format!("invalid_xcsh_session_dir: cannot resolve session dir: {error}")
    })?;
    if !canonical_dir.is_dir()
        || canonical_dir.to_string_lossy() != launch.session_dir
        || canonical.parent() != Some(canonical_dir.as_path())
    {
        return Err("invalid_xcsh_session_dir: session_dir must be canonical and contain session_path directly".into());
    }
    let mut file = std::fs::File::open(&canonical).map_err(|error| {
        format!("invalid_xcsh_session_path: cannot read session header: {error}")
    })?;
    let mut header_line = Vec::new();
    std::io::Read::by_ref(&mut file)
        .take(1024 * 1024)
        .read_to_end(&mut header_line)
        .map_err(|error| format!("invalid_xcsh_session_header: cannot read header: {error}"))?;
    let Some(newline) = header_line.iter().position(|byte| *byte == b'\n') else {
        return Err("invalid_xcsh_session_header: first JSONL header must end with LF".into());
    };
    header_line.truncate(newline + 1);
    let value: serde_json::Value = serde_json::from_slice(&header_line[..newline])
        .map_err(|_| "invalid_xcsh_session_header: first line is not JSON".to_string())?;
    if value.get("type").and_then(serde_json::Value::as_str) != Some("session") {
        return Err(
            "invalid_xcsh_session_header: first JSONL line is not an XCSH session header".into(),
        );
    }
    let id = value
        .get("id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "invalid_xcsh_session_header: session header has no id".to_string())?;
    if id != launch.session_header.id || !is_canonical_xcsh_session_id(id) {
        return Err(
            "invalid_xcsh_session_header: session header id does not match canonical binding"
                .into(),
        );
    }
    use sha2::Digest;
    let measured = NativeSessionHeaderBinding {
        id: id.into(),
        sha256: format!("{:x}", sha2::Sha256::digest(&header_line)),
    };
    if measured != launch.session_header {
        return Err("invalid_xcsh_session_header: session header digest does not match first line including LF".into());
    }
    Ok(measured)
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn sha256_hex(value: &str) -> String {
    use sha2::Digest;
    format!("{:x}", sha2::Sha256::digest(value.as_bytes()))
}

/// Measure an explicit XCSH executable without consulting PATH. The binding is
/// persisted in the native execution receipt and checked again at effect time,
/// so a symlink replacement or in-place update cannot silently launch another
/// program after the durable claim.
pub(crate) fn measure_xcsh_executable(path: &str) -> Result<NativeExecutableBinding, String> {
    let requested = Path::new(path);
    if !requested.is_absolute() {
        return Err(
            "invalid_xcsh_executable: execution.resume requires an absolute XCSH executable path"
                .into(),
        );
    }
    let canonical = std::fs::canonicalize(requested)
        .map_err(|error| format!("invalid_xcsh_executable: cannot resolve executable: {error}"))?;
    let metadata = std::fs::metadata(&canonical)
        .map_err(|error| format!("invalid_xcsh_executable: cannot inspect executable: {error}"))?;
    if !metadata.is_file() {
        return Err("invalid_xcsh_executable: path must name a regular executable file".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o111 == 0 {
            return Err("invalid_xcsh_executable: path is not executable".into());
        }
    }
    if metadata.len() > MAX_NATIVE_EXECUTABLE_BYTES {
        return Err(
            "invalid_xcsh_executable: executable exceeds the maximum measurable size".into(),
        );
    }
    let modified = metadata.modified().ok();
    let mut file = std::fs::File::open(&canonical)
        .map_err(|error| format!("invalid_xcsh_executable: cannot read executable: {error}"))?;
    let mut digest = sha2::Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    let mut bytes_hashed = 0_u64;
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("invalid_xcsh_executable: cannot hash executable: {error}"))?;
        if read == 0 {
            break;
        }
        bytes_hashed = bytes_hashed.saturating_add(read as u64);
        if bytes_hashed > MAX_NATIVE_EXECUTABLE_BYTES {
            return Err(
                "invalid_xcsh_executable: executable exceeds the maximum measurable size".into(),
            );
        }
        use sha2::Digest;
        digest.update(&buffer[..read]);
    }
    let after = std::fs::metadata(&canonical)
        .map_err(|error| format!("invalid_xcsh_executable: cannot recheck executable: {error}"))?;
    if after.len() != metadata.len() || after.modified().ok() != modified {
        return Err("invalid_xcsh_executable: executable changed while it was measured".into());
    }
    use sha2::Digest;
    Ok(NativeExecutableBinding {
        canonical_path: canonical.to_string_lossy().into_owned(),
        sha256: format!("{:x}", digest.finalize()),
    })
}

/// Recheck a durable binding immediately before launch. This detects ordinary
/// replacement or mutation between admission and effect; it does not claim an
/// atomic object-handle-to-exec guarantee against a concurrently hostile owner.
pub(crate) fn verify_xcsh_executable_binding(
    expected: &NativeExecutableBinding,
) -> Result<bool, String> {
    Ok(measure_xcsh_executable(&expected.canonical_path)? == *expected)
}

fn is_canonical_xcsh_session_id(value: &str) -> bool {
    value.len() == 16
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

/// The complete supported XCSH argv derived from protocol-22's typed launch.
/// No caller-controlled argv or environment reaches the child. `managed_turn_v1`
/// is a durable Herdr/producer lifecycle contract, not an undocumented XCSH
/// command-line option.
fn native_xcsh_argv(
    executable: &NativeExecutableBinding,
    launch: &NativeLaunchV3,
    text: &str,
) -> Result<Vec<String>, String> {
    let mut argv = vec![
        executable.canonical_path.clone(),
        "--mode".into(),
        "json".into(),
        "--session-dir".into(),
        launch.session_dir.clone(),
        "--resume".into(),
        launch.session_path.clone(),
        "--model".into(),
        launch.model.clone(),
        "--tools".into(),
        "read".into(),
        "--no-mcp".into(),
        "--no-lsp".into(),
        "--no-pty".into(),
    ];
    if !launch.interactive {
        argv.push("--print".into());
    }
    argv.push(text.into());
    Ok(argv)
}

fn native_backend_id(execution_id: &str, generation: u64) -> String {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(format!("xcsh\0{execution_id}\0{generation}").as_bytes());
    let suffix: String = digest[..12]
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    format!("xcsh-{suffix}")
}
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}
fn redact(value: &str) -> String {
    value
        .lines()
        .map(|line| {
            let lower = line.to_ascii_lowercase();
            if ["token=", "password=", "authorization:", "api_key="]
                .iter()
                .any(|marker| lower.contains(marker))
            {
                "[redacted]\n".to_string()
            } else {
                format!("{line}\n")
            }
        })
        .collect()
}
#[cfg(test)]
mod tests {
    use super::*;
    fn temp(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("herdr-execution-{name}-{}", std::process::id()))
    }
    fn params(id: &str) -> ExecutionStartParams {
        ExecutionStartParams {
            execution_id: id.into(),
            cwd: "/tmp".into(),
            workspace_id: None,
            label: None,
            command: ExecutionCommand::Argv {
                argv: vec!["/bin/true".into()],
            },
        }
    }
    fn resume_params(generation: u64) -> ExecutionResumeParams {
        let session_path = test_session_path();
        ExecutionResumeParams {
            execution_id: "semantic-task".into(),
            generation,
            native_launch: NativeLaunchV3 {
                version: 3,
                xcsh_executable: std::env::current_exe()
                    .expect("test executable path")
                    .to_string_lossy()
                    .into_owned(),
                session_dir: std::fs::canonicalize(session_path.parent().expect("session parent"))
                    .expect("canonical session directory")
                    .to_string_lossy()
                    .into_owned(),
                session_path: session_path.to_string_lossy().into_owned(),
                session_header: test_session_header_binding(),
                model: "test/model".into(),
                discovery: crate::api::schema::NativeDiscoveryPolicy::ReducedV1,
                tools: crate::api::schema::NativeToolsPolicy::Read,
                interactive: false,
                lifecycle_mode: crate::api::schema::NativeLifecycleMode::ManagedTurnV1,
            },
            text: "continue the task".into(),
            cwd: "/tmp".into(),
            workspace_id: None,
            label: None,
        }
    }

    fn test_session_path() -> &'static PathBuf {
        static PATH: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
        PATH.get_or_init(|| {
            let path = temp("native-session.jsonl");
            std::fs::write(
                &path,
                "{\"type\":\"session\",\"version\":3,\"id\":\"0123abcd4567ef89\",\"cwd\":\"/tmp\"}\n",
            )
            .expect("write native session header");
            std::fs::canonicalize(path).expect("canonical session path")
        })
    }

    fn test_session_header_binding() -> NativeSessionHeaderBinding {
        use sha2::Digest;
        let bytes = std::fs::read(test_session_path()).expect("read native session header");
        NativeSessionHeaderBinding {
            id: "0123abcd4567ef89".into(),
            sha256: format!("{:x}", sha2::Sha256::digest(bytes)),
        }
    }

    fn copied_test_executable(name: &str) -> PathBuf {
        let path = temp(name);
        std::fs::copy(std::env::current_exe().expect("test executable"), &path)
            .expect("copy test executable");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755))
                .expect("mark copied executable");
        }
        path
    }

    #[test]
    fn native_generation_admission_is_idempotent_and_preserves_provenance() {
        let path = temp("native-idempotency");
        let manager = ExecutionManager::load_at(path.clone());
        let params = resume_params(4);
        let (first, admitted, old) = manager.admit_xcsh_resume(&params).unwrap();
        assert!(admitted);
        assert!(old.is_none());
        assert_ne!(first.execution_id, params.execution_id);
        assert_eq!(
            first.semantic_execution_id.as_deref(),
            Some("semantic-task")
        );
        assert_eq!(first.generation, Some(4));
        assert_eq!(first.native_producer.as_deref(), Some("xcsh"));
        let executable = first.native_executable.as_ref().expect("native executable");
        assert!(Path::new(&executable.canonical_path).is_absolute());
        assert_eq!(executable.sha256.len(), 64);
        assert_eq!(first.injected_env["HERDR_EXECUTION_ID"], "semantic-task");
        assert_eq!(first.injected_env["HERDR_EXECUTION_GENERATION"], "4");
        let (retry, admitted, _) = manager.admit_xcsh_resume(&params).unwrap();
        assert!(!admitted);
        assert_eq!(retry.execution_id, first.execution_id);
        let mut conflict = params;
        conflict.text = "different replay".into();
        assert!(manager
            .admit_xcsh_resume(&conflict)
            .unwrap_err()
            .contains("generation_conflict"));
        let alternate = copied_test_executable("native-alternate-binding");
        let mut binding_conflict = resume_params(4);
        binding_conflict.native_launch.xcsh_executable = alternate.to_string_lossy().into_owned();
        assert!(manager
            .admit_xcsh_resume(&binding_conflict)
            .unwrap_err()
            .contains("generation_conflict"));
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(alternate);
    }

    #[test]
    fn native_executable_binding_detects_prelaunch_mutation() {
        let path = copied_test_executable("native-binding-mutation");
        let binding = measure_xcsh_executable(path.to_str().expect("utf8 path")).unwrap();
        assert!(verify_xcsh_executable_binding(&binding).unwrap());
        std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap()
            .write_all(b"mutation")
            .unwrap();
        assert!(!verify_xcsh_executable_binding(&binding).unwrap());
        let _ = std::fs::remove_file(path);
    }

    #[cfg(windows)]
    #[test]
    fn windows_native_executable_binding_accepts_regular_executable_file() {
        let binding = measure_xcsh_executable(
            std::env::current_exe()
                .expect("test executable")
                .to_str()
                .expect("utf8 executable path"),
        )
        .unwrap();
        assert!(Path::new(&binding.canonical_path).is_file());
    }

    #[test]
    fn native_generation_rejects_values_xcsh_cannot_represent_exactly() {
        let path = temp("native-safe-integer");
        let manager = ExecutionManager::load_at(path.clone());
        assert!(manager
            .admit_xcsh_resume(&resume_params(9_007_199_254_740_991))
            .is_ok());
        assert!(manager
            .admit_xcsh_resume(&resume_params(9_007_199_254_740_992))
            .unwrap_err()
            .contains("safe integer"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn native_resume_requires_a_canonical_xcsh_session_header_id() {
        let path = temp("native-canonical-session");
        let manager = ExecutionManager::load_at(path.clone());
        for invalid in [
            "0123abcd",
            "/tmp/xcsh-session.jsonl",
            "0123ABCD4567EF89",
            "123e4567-e89b-12d3-a456-426614174000",
        ] {
            let mut params = resume_params(1);
            params.native_launch.session_header.id = invalid.into();
            let error = manager.admit_xcsh_resume(&params).unwrap_err();
            assert!(error.starts_with("invalid_xcsh_session_id"));
            assert!(error.contains("prefixes and session paths"));
        }
        let (record, admitted, _) = manager.admit_xcsh_resume(&resume_params(1)).unwrap();
        assert!(admitted);
        assert_eq!(
            record.producer_session_id.as_deref(),
            Some("0123abcd4567ef89")
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn ordinary_and_native_execution_namespaces_cannot_collide_in_either_order() {
        let path = temp("native-ordinary-collision");
        let manager = ExecutionManager::load_at(path.clone());
        manager.admit_visible(&params("semantic-task")).unwrap();
        assert!(manager
            .admit_xcsh_resume(&resume_params(1))
            .unwrap_err()
            .contains("namespace_conflict"));
        let other_path = temp("ordinary-native-collision");
        let other = ExecutionManager::load_at(other_path.clone());
        other.admit_xcsh_resume(&resume_params(1)).unwrap();
        assert!(other
            .admit_visible(&params("semantic-task"))
            .unwrap_err()
            .contains("namespace_conflict"));
        let _ = std::fs::remove_file(path);
        let _ = std::fs::remove_file(other_path);
    }

    #[test]
    fn native_generation_handoff_marks_old_then_rejects_stale_and_foreign_bindings() {
        let path = temp("native-handoff");
        let manager = ExecutionManager::load_at(path.clone());
        let first = resume_params(1);
        let (old, _, _) = manager.admit_xcsh_resume(&first).unwrap();
        manager
            .attach_visible(
                &old.execution_id,
                7,
                "w1:p7".into(),
                "w1:t1".into(),
                Some(7),
            )
            .unwrap();
        let (current, admitted, previous) = manager.admit_xcsh_resume(&resume_params(2)).unwrap();
        assert!(admitted);
        assert_eq!(previous.unwrap().execution_id, old.execution_id);
        let old = manager.get(&old.execution_id).unwrap();
        assert!(old.cancel_requested);
        assert_eq!(
            old.superseded_by_backend_execution_id.as_deref(),
            Some(current.execution_id.as_str())
        );
        let mut conflicting_retry = resume_params(1);
        conflicting_retry.native_launch.model = "other/model".into();
        assert!(manager
            .admit_xcsh_resume(&conflicting_retry)
            .unwrap_err()
            .contains("generation_conflict"));
        assert!(manager
            .admit_xcsh_resume(&resume_params(0))
            .unwrap_err()
            .contains("generation_stale"));
        manager.finish_visible(
            7,
            Some(&portable_pty::ExitStatus::with_signal("Terminated")),
            None,
        );
        assert_eq!(
            manager.get(&old.execution_id).unwrap().state,
            ExecutionState::Cancelled
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn native_cancel_action_requires_owned_capability_and_is_idempotent() {
        let path = temp("native-cancel-action");
        let manager = ExecutionManager::load_at(path.clone());
        let (claimed, admitted, _) = manager.admit_xcsh_resume(&resume_params(12)).unwrap();
        assert!(admitted);
        manager
            .attach_visible(
                &claimed.execution_id,
                91,
                "w1:p91".into(),
                "w1:t1".into(),
                Some(91),
            )
            .unwrap();
        let capability = manager.native_capability(&claimed.execution_id).unwrap();
        let target = AgentTurnActionTarget {
            execution_id: "semantic-task".into(),
            pane_id: "w1:p91".into(),
            producer: "xcsh".into(),
            session_id: "0123abcd4567ef89".into(),
            generation: 12,
            native_capability: capability.clone(),
            after_revision: 0,
        };
        assert!(manager.native_actions(&target).unwrap().is_empty());
        assert!(
            manager
                .request_native_cancel(&claimed.execution_id)
                .unwrap()
                .cancel_requested
        );
        let actions = manager.native_actions(&target).unwrap();
        assert_eq!(actions.len(), 1);
        assert_eq!(actions[0].state, AgentTurnActionState::Requested);
        let ack = AgentTurnActionAckParams {
            target: target.clone(),
            action_id: "cancel".into(),
            action_revision: 1,
            state: AgentTurnActionState::SafePoint,
        };
        assert!(manager.acknowledge_native_action(&ack).unwrap().1);
        assert!(!manager.acknowledge_native_action(&ack).unwrap().1);
        let mut foreign = target;
        foreign.native_capability = "wrong".into();
        assert!(manager
            .native_actions(&foreign)
            .unwrap_err()
            .contains("capability"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn restart_reconciles_claimed_child_before_effect_as_lost() {
        let path = temp("native-crash-claim");
        let manager = ExecutionManager::load_at(path.clone());
        let (claimed, _, _) = manager.admit_xcsh_resume(&resume_params(9)).unwrap();
        drop(manager);
        let reloaded = ExecutionManager::load_at(path.clone());
        let recovered = reloaded.get(&claimed.execution_id).unwrap();
        assert_eq!(recovered.state, ExecutionState::Lost);
        assert!(recovered
            .evidence_gap
            .as_deref()
            .unwrap()
            .contains("restarted"));
        let _ = std::fs::remove_file(path);
    }
    #[test]
    fn idempotent_admission_and_conflict() {
        let path = temp("idem");
        let m = ExecutionManager::load_at(path.clone());
        let p = params("same");
        assert!(m.admit_visible(&p).unwrap().1);
        assert!(!m.admit_visible(&p).unwrap().1);
        let mut q = p;
        q.command = ExecutionCommand::Argv {
            argv: vec!["/bin/false".into()],
        };
        assert!(m.admit_visible(&q).unwrap_err().contains("conflict"));
        let _ = std::fs::remove_file(path);
    }
    #[test]
    fn visible_exit_status_and_cancel_are_structural() {
        let path = temp("status");
        let m = ExecutionManager::load_at(path.clone());
        m.admit_visible(&params("visible")).unwrap();
        m.attach_visible("visible", 42, "w1:p2".into(), "w1:t2".into(), Some(123))
            .unwrap();
        m.request_visible_cancel("visible").unwrap();
        m.finish_visible(
            42,
            Some(&portable_pty::ExitStatus::with_signal("Terminated")),
            None,
        );
        let record = m.get("visible").unwrap();
        assert_eq!(record.state, ExecutionState::Cancelled);
        assert_eq!(record.signal_name.as_deref(), Some("Terminated"));
        assert_eq!(record.pane_id.as_deref(), Some("w1:p2"));
        let _ = std::fs::remove_file(path);
    }
    #[test]
    fn expired_id_cannot_silently_execute_again() {
        let path = temp("tombstone");
        let m = ExecutionManager::load_at(path.clone());
        for index in 0..=MAX_RECORDS {
            let id = format!("id-{index}");
            m.admit_visible(&params(&id)).unwrap();
            m.mark_start_failed(&id, "test settlement");
        }
        let error = m.admit_visible(&params("id-0")).unwrap_err();
        assert!(error.starts_with("execution_expired"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn evicted_native_generation_remains_reserved_across_restart() {
        let path = temp("evicted-native-generation");
        let manager = ExecutionManager::load_at(path.clone());
        let (claimed, admitted, _) = manager.admit_xcsh_resume(&resume_params(6)).unwrap();
        assert!(admitted);
        manager.mark_start_failed(&claimed.execution_id, "test settlement");
        for index in 0..MAX_RECORDS {
            manager
                .admit_visible(&params(&format!("eviction-{index}")))
                .unwrap();
        }
        assert!(manager.get(&claimed.execution_id).is_none());
        drop(manager);

        let reloaded = ExecutionManager::load_at(path.clone());
        let retry = reloaded.admit_xcsh_resume(&resume_params(6)).unwrap_err();
        assert!(retry.starts_with("execution_expired"));
        let collision = reloaded
            .admit_visible(&params("semantic-task"))
            .unwrap_err();
        assert!(collision.starts_with("execution_namespace_conflict"));
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn active_native_record_survives_history_churn_and_restart() {
        let path = temp("active-native-retention");
        let manager = ExecutionManager::load_at(path.clone());
        let (claimed, admitted, _) = manager.admit_xcsh_resume(&resume_params(6)).unwrap();
        assert!(admitted);
        manager
            .attach_visible(
                &claimed.execution_id,
                77,
                "w1:p77".into(),
                "w1:t1".into(),
                Some(77),
            )
            .unwrap();
        for index in 0..=MAX_RECORDS {
            let id = format!("settled-{index}");
            manager.admit_visible(&params(&id)).unwrap();
            manager.mark_start_failed(&id, "test settlement");
        }
        assert_eq!(
            manager.get(&claimed.execution_id).unwrap().state,
            ExecutionState::Running
        );
        drop(manager);

        let reloaded = ExecutionManager::load_at(path.clone());
        let retained = reloaded.get(&claimed.execution_id).unwrap();
        assert_eq!(retained.state, ExecutionState::Lost);
        assert_eq!(
            reloaded
                .resolve_agent_turn_execution("semantic-task", "xcsh", "0123abcd4567ef89", 6,)
                .unwrap()
                .execution_id,
            claimed.execution_id
        );
        let _ = std::fs::remove_file(path);
    }
    #[test]
    fn restart_marks_running_lost() {
        let path = temp("lost");
        let state = State {
            revision: 1,
            native_capability_verifiers: BTreeMap::new(),
            native_actions: BTreeMap::new(),
            records: vec![ExecutionRecord {
                execution_id: "old".into(),
                backend_execution_id: None,
                semantic_execution_id: None,
                generation: None,
                native_producer: None,
                native_executable: None,
                native_launch: None,
                producer_session_id: None,
                injected_env: BTreeMap::new(),
                superseded_by_backend_execution_id: None,
                cwd: "/tmp".into(),
                command: ExecutionCommand::Argv {
                    argv: vec!["x".into()],
                },
                state: ExecutionState::Running,
                revision: 1,
                admitted_at_unix_ms: 1,
                started_at_unix_ms: Some(1),
                finished_at_unix_ms: None,
                pid: Some(1),
                pane_id: Some("w1:p2".into()),
                tab_id: Some("w1:t2".into()),
                exit_code: None,
                signal: None,
                signal_name: None,
                cancel_requested: false,
                stdout_bytes: 0,
                stderr_bytes: 0,
                stdout_tail: String::new(),
                stderr_tail: String::new(),
                output_truncated: false,
                output_complete: false,
                evidence_gap: None,
            }],
            native_reservations: BTreeMap::new(),
            tombstones: Vec::new(),
            expired_id_filter: Vec::new(),
        };
        std::fs::write(&path, serde_json::to_vec(&state).unwrap()).unwrap();
        let m = ExecutionManager::load_at(path.clone());
        assert_eq!(m.get("old").unwrap().state, ExecutionState::Lost);
        let _ = std::fs::remove_file(path);
    }

    fn tail_with_multibyte_prefix_at_cap_boundary() -> String {
        format!("é{}", "x".repeat(OUTPUT_TAIL_BYTES - 3))
    }

    #[test]
    fn pending_output_tail_keeps_a_utf8_safe_bounded_suffix() {
        let mut captured = CapturedOutput {
            bytes: 41,
            tail: tail_with_multibyte_prefix_at_cap_boundary(),
            truncated: false,
        };

        append_captured(&mut captured, b"x");

        assert_eq!(captured.bytes, 42);
        assert!(captured.truncated);
        assert!(captured.tail.len() <= OUTPUT_TAIL_BYTES);
        assert_eq!(
            captured.tail,
            format!("{}x\n", "x".repeat(OUTPUT_TAIL_BYTES - 3))
        );
    }

    #[test]
    fn tracked_output_tail_keeps_a_utf8_safe_bounded_suffix() {
        let path = temp("tracked-utf8-tail");
        let manager = ExecutionManager::load_at(path.clone());
        manager.admit_visible(&params("tracked")).unwrap();
        manager
            .attach_visible("tracked", 7, "w1:p7".into(), "w1:t1".into(), Some(7))
            .unwrap();
        manager
            .mutate("tracked", |record| {
                record.stdout_tail = tail_with_multibyte_prefix_at_cap_boundary();
            })
            .unwrap();

        manager.observe_visible_output(7, b"x");

        let record = manager.get("tracked").unwrap();
        assert_eq!(record.stdout_bytes, 1);
        assert!(record.output_truncated);
        assert!(record.stdout_tail.len() <= OUTPUT_TAIL_BYTES);
        assert_eq!(
            record.stdout_tail,
            format!("{}x\n", "x".repeat(OUTPUT_TAIL_BYTES - 3))
        );
        let _ = std::fs::remove_file(path);
    }
}
