use std::collections::{BTreeMap, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::api::schema::{
    ExecutionCommand, ExecutionRecord, ExecutionResumeParams, ExecutionStartParams, ExecutionState,
};

const MAX_RECORDS: usize = 256;
const OUTPUT_TAIL_BYTES: usize = 4096;

#[derive(Clone)]
pub(crate) struct ExecutionManager(Arc<Inner>);
struct Inner {
    path: PathBuf,
    state: Mutex<State>,
    pane_executions: Mutex<HashMap<u32, String>>,
    pending_pane_output: Mutex<HashMap<u32, CapturedOutput>>,
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
    #[serde(default)]
    tombstones: Vec<ExecutionTombstone>,
    #[serde(default)]
    expired_id_filter: Vec<u8>,
}
#[derive(serde::Serialize, serde::Deserialize)]
struct ExecutionTombstone {
    execution_id: String,
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
        let backend_execution_id = native_backend_id(&params.execution_id, params.generation);
        let command = ExecutionCommand::Argv {
            argv: vec![
                "xcsh".into(),
                "--resume".into(),
                params.session_id.clone(),
                params.text.clone(),
            ],
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
                && existing.producer_session_id.as_deref() == Some(&params.session_id)
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
        let record = ExecutionRecord {
            execution_id: backend_execution_id.clone(),
            backend_execution_id: Some(backend_execution_id.clone()),
            semantic_execution_id: Some(params.execution_id.clone()),
            generation: Some(params.generation),
            native_producer: Some("xcsh".into()),
            producer_session_id: Some(params.session_id.clone()),
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
            let record = state.records.remove(0);
            expired_filter_insert(&mut state.expired_id_filter, &record.execution_id);
            state.tombstones.push(ExecutionTombstone {
                execution_id: record.execution_id,
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
            argv: vec!["xcsh".into()],
        },
    })?;
    if p.session_id.is_empty()
        || p.session_id.len() > 512
        || p.text.is_empty()
        || p.text.len() > 65_536
    {
        return Err("invalid_execution_resume".into());
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
        ExecutionResumeParams {
            execution_id: "semantic-task".into(),
            generation,
            session_id: "xcsh-session".into(),
            text: "continue the task".into(),
            cwd: "/tmp".into(),
            workspace_id: None,
            label: None,
        }
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
        let _ = std::fs::remove_file(path);
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
        conflicting_retry.session_id = "foreign-session".into();
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
            m.admit_visible(&params(&format!("id-{index}"))).unwrap();
        }
        let error = m.admit_visible(&params("id-0")).unwrap_err();
        assert!(error.starts_with("execution_expired"));
        let _ = std::fs::remove_file(path);
    }
    #[test]
    fn restart_marks_running_lost() {
        let path = temp("lost");
        let state = State {
            revision: 1,
            records: vec![ExecutionRecord {
                execution_id: "old".into(),
                backend_execution_id: None,
                semantic_execution_id: None,
                generation: None,
                native_producer: None,
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
