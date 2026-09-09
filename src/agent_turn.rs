use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

use crate::api::schema::{AgentTurnRecord, AgentTurnReportParams, AgentTurnState, AgentTurnTarget};

const MAX_RECORDS: usize = 1024;
const MAX_TEXT_BYTES: usize = 8_000;
const MAX_ID_BYTES: usize = 256;
const MAX_TOMBSTONES: usize = 4096;

#[derive(Clone)]
pub(crate) struct AgentTurnManager(Arc<Inner>);

struct Inner {
    path: PathBuf,
    state: Mutex<State>,
}

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct State {
    revision: u64,
    records: Vec<AgentTurnRecord>,
    #[serde(default)]
    tombstones: Vec<AgentTurnTarget>,
}

impl AgentTurnManager {
    pub(crate) fn global() -> &'static Self {
        static MANAGER: std::sync::OnceLock<AgentTurnManager> = std::sync::OnceLock::new();
        MANAGER.get_or_init(Self::load)
    }

    fn load() -> Self {
        Self::load_at(crate::session::data_dir().join("agent-turns.json"))
    }

    fn load_at(path: PathBuf) -> Self {
        let mut state: State = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default();
        let active: Vec<_> = state
            .records
            .iter()
            .filter(|record| {
                is_latest(&state.records, record) && !record.report.state.is_terminal()
            })
            .cloned()
            .collect();
        for record in active {
            state.revision += 1;
            let mut report = record.report;
            report.event_revision += 1;
            report.state = AgentTurnState::Lost;
            report.result = None;
            report.reason =
                Some("Herdr restarted before the semantic turn reached a terminal state".into());
            report.result_digest = None;
            state.records.push(AgentTurnRecord {
                report,
                revision: state.revision,
                reported_at_unix_ms: now_ms(),
            });
        }
        enforce_retention(&mut state);
        let manager = Self(Arc::new(Inner {
            path,
            state: Mutex::new(state),
        }));
        let _ = manager.persist();
        manager
    }

    pub(crate) fn report(
        &self,
        mut report: AgentTurnReportParams,
    ) -> Result<(AgentTurnRecord, bool), String> {
        validate(&report)?;
        let execution = crate::execution::ExecutionManager::global().resolve_agent_turn_execution(
            &report.execution_id,
            &report.producer,
            &report.session_id,
            report.generation,
        )?;
        if execution.pane_id.as_deref() != Some(report.pane_id.as_str()) {
            return Err("agent_turn_provenance_mismatch: pane is not owned by execution".into());
        }
        crate::execution::ExecutionManager::global()
            .authorize_native_report_capability(&execution, report.native_capability.as_deref())?;
        crate::execution::ExecutionManager::global().register_native_start(&execution, &report)?;
        crate::execution::ExecutionManager::global()
            .validate_native_cancelled_report(&execution, &report)?;
        // The credential is request-only. It must never enter the durable
        // journal, replay identity, response, or log surface.
        report.native_capability = None;
        let mut state = self
            .0
            .state
            .lock()
            .map_err(|_| "agent turn state lock poisoned")?;
        // `load_at` appends a synthetic Lost event for an interrupted server.
        // A native child may have already persisted its authenticated starting
        // frame while Herdr crashed before cross-ledger confirmation. Only the
        // same authenticated revision-1 starting frame may remove that exact
        // synthetic marker and resume confirmation; no terminal producer event
        // is reset or fabricated.
        if report.state == AgentTurnState::Starting && report.event_revision == 1 {
            if let Some(index) = state.records.iter().rposition(|candidate| {
                same_turn(&candidate.report, &report)
                    && candidate.report.state == AgentTurnState::Lost
                    && candidate.report.reason.as_deref()
                        == Some("Herdr restarted before the semantic turn reached a terminal state")
                    && candidate.report.event_revision == 2
            }) {
                if state.records[..index].iter().rev().any(|candidate| {
                    same_turn(&candidate.report, &report)
                        && candidate.report.state == AgentTurnState::Starting
                        && candidate.report.event_revision == 1
                        && candidate.report == report
                }) {
                    state.records.remove(index);
                }
            }
        }
        let latest = state
            .records
            .iter()
            .rev()
            .find(|record| same_turn(&record.report, &report));
        if latest.is_none()
            && state
                .tombstones
                .iter()
                .any(|target| matches_target(target, &report))
        {
            return Err(
                "agent_turn_expired: turn identity has expired; use a new turn id or generation"
                    .into(),
            );
        }
        if let Some(latest) = latest {
            if report.event_revision < latest.report.event_revision {
                return Err("agent_turn_stale_revision: event revision regressed".into());
            }
            if report.event_revision == latest.report.event_revision {
                if latest.report == report {
                    crate::execution::ExecutionManager::global()
                        .confirm_native_start_journaled(&execution, &latest.report)?;
                    crate::execution::ExecutionManager::global()
                        .settle_native_cancelled_report(&execution, &latest.report)?;
                    return Ok((latest.clone(), false));
                }
                return Err("agent_turn_revision_conflict: revision content differs".into());
            }
            if latest.report.state.is_terminal() {
                return Err("agent_turn_terminal_conflict: terminal state is immutable".into());
            }
            if report.state == AgentTurnState::Starting {
                return Err(
                    "agent_turn_invalid_transition: starting is only valid for the first event"
                        .into(),
                );
            }
        } else if report.event_revision != 1 {
            return Err("agent_turn_missing_revision: first event revision must be 1".into());
        } else if report.state != AgentTurnState::Starting {
            return Err("agent_turn_invalid_transition: first event must be starting".into());
        }
        state.revision += 1;
        let record = AgentTurnRecord {
            report,
            revision: state.revision,
            reported_at_unix_ms: now_ms(),
        };
        state.records.push(record.clone());
        enforce_retention(&mut state);
        self.persist_locked(&state)?;
        drop(state);
        let executions = crate::execution::ExecutionManager::global();
        executions.confirm_native_start_journaled(&execution, &record.report)?;
        executions.settle_native_cancelled_report(&execution, &record.report)?;
        Ok((record, true))
    }

    pub(crate) fn get(&self, target: &AgentTurnTarget) -> Option<AgentTurnRecord> {
        self.0
            .state
            .lock()
            .ok()?
            .records
            .iter()
            .rev()
            .find(|record| {
                record.report.producer == target.producer
                    && record.report.session_id == target.session_id
                    && record.report.turn_id == target.turn_id
                    && record.report.generation == target.generation
            })
            .cloned()
    }

    pub(crate) fn list_since(&self, revision: u64) -> Vec<AgentTurnRecord> {
        self.0
            .state
            .lock()
            .map(|state| {
                state
                    .records
                    .iter()
                    .filter(|record| record.revision > revision)
                    .cloned()
                    .collect()
            })
            .unwrap_or_default()
    }

    pub(crate) fn wait_since(&self, revision: u64, timeout_ms: u64) -> Vec<AgentTurnRecord> {
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

    fn persist(&self) -> Result<(), String> {
        let state = self
            .0
            .state
            .lock()
            .map_err(|_| "agent turn state lock poisoned")?;
        self.persist_locked(&state)
    }

    fn persist_locked(&self, state: &State) -> Result<(), String> {
        if let Some(parent) = self.0.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let bytes = serde_json::to_vec(state).map_err(|error| error.to_string())?;
        let temp = self.0.path.with_extension("json.tmp");
        std::fs::write(&temp, bytes).map_err(|error| error.to_string())?;
        std::fs::rename(temp, &self.0.path).map_err(|error| error.to_string())
    }
}

fn validate(report: &AgentTurnReportParams) -> Result<(), String> {
    for (name, value) in [
        ("execution_id", report.execution_id.as_str()),
        ("pane_id", report.pane_id.as_str()),
        ("producer", report.producer.as_str()),
        ("session_id", report.session_id.as_str()),
        ("turn_id", report.turn_id.as_str()),
    ] {
        if value.is_empty() || value.len() > MAX_ID_BYTES || value.chars().any(char::is_control) {
            return Err(format!("invalid_agent_turn: {name} is invalid"));
        }
    }
    if report.event_revision == 0 {
        return Err("invalid_agent_turn: event_revision must be positive".into());
    }
    for (name, value) in [("result", &report.result), ("reason", &report.reason)] {
        if value
            .as_ref()
            .is_some_and(|value| value.len() > MAX_TEXT_BYTES)
        {
            return Err(format!(
                "agent_turn_payload_too_large: {name} exceeds 8000 bytes"
            ));
        }
    }
    if report.state == AgentTurnState::Completed && report.result.is_none() {
        return Err("invalid_agent_turn: completed requires result".into());
    }
    if report.state != AgentTurnState::Completed && report.result.is_some() {
        return Err("invalid_agent_turn: result is only valid for completed".into());
    }
    if report.result.is_some() != report.result_digest.is_some() {
        return Err(
            "invalid_agent_turn: result and result_digest must be provided together".into(),
        );
    }
    if report.result_digest.as_ref().is_some_and(|digest| {
        digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    }) {
        return Err("invalid_agent_turn: result_digest must be 64 hexadecimal characters".into());
    }
    if let (Some(result), Some(digest)) = (&report.result, &report.result_digest) {
        let actual = format!("{:x}", Sha256::digest(result.as_bytes()));
        if &actual != digest {
            return Err("invalid_agent_turn: result_digest does not match result".into());
        }
    }
    Ok(())
}

fn same_turn(left: &AgentTurnReportParams, right: &AgentTurnReportParams) -> bool {
    left.producer == right.producer
        && left.session_id == right.session_id
        && left.turn_id == right.turn_id
        && left.generation == right.generation
}

fn matches_target(target: &AgentTurnTarget, report: &AgentTurnReportParams) -> bool {
    target.producer == report.producer
        && target.session_id == report.session_id
        && target.turn_id == report.turn_id
        && target.generation == report.generation
}

fn is_latest(records: &[AgentTurnRecord], candidate: &AgentTurnRecord) -> bool {
    !records.iter().any(|other| {
        same_turn(&other.report, &candidate.report)
            && other.report.event_revision > candidate.report.event_revision
    })
}

fn enforce_retention(state: &mut State) {
    while state.records.len() > MAX_RECORDS {
        let removed = state.records.remove(0);
        if !state
            .records
            .iter()
            .any(|record| same_turn(&record.report, &removed.report))
        {
            state.tombstones.push(AgentTurnTarget {
                producer: removed.report.producer,
                session_id: removed.report.session_id,
                turn_id: removed.report.turn_id,
                generation: removed.report.generation,
            });
            if state.tombstones.len() > MAX_TOMBSTONES {
                state.tombstones.remove(0);
            }
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn restart_marks_latest_nonterminal_turn_lost_and_persists_revision() {
        let root = std::env::temp_dir().join(format!(
            "herdr-agent-turn-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let path = root.join("agent-turns.json");
        let report = AgentTurnReportParams {
            execution_id: "execution-1".into(),
            pane_id: "w1:p1".into(),
            producer: "xcsh".into(),
            session_id: "session-1".into(),
            turn_id: "turn-1".into(),
            generation: 0,
            event_revision: 1,
            state: AgentTurnState::Working,
            result: None,
            reason: None,
            result_digest: None,
            native_capability: None,
        };
        let state = State {
            revision: 4,
            records: vec![AgentTurnRecord {
                report,
                revision: 4,
                reported_at_unix_ms: now_ms(),
            }],
            tombstones: Vec::new(),
        };
        std::fs::write(&path, serde_json::to_vec(&state).unwrap()).unwrap();

        let manager = AgentTurnManager::load_at(path.clone());
        let records = manager.list_since(4);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].revision, 5);
        assert_eq!(records[0].report.event_revision, 2);
        assert_eq!(records[0].report.state, AgentTurnState::Lost);
        assert!(records[0]
            .report
            .reason
            .as_deref()
            .unwrap()
            .contains("restarted"));
        drop(manager);
        let persisted: State = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        assert_eq!(persisted.revision, 5);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn completed_result_requires_matching_digest_and_byte_bound() {
        let result = "accepted".to_string();
        let mut report = AgentTurnReportParams {
            execution_id: "execution-1".into(),
            pane_id: "w1:p1".into(),
            producer: "xcsh".into(),
            session_id: "session-1".into(),
            turn_id: "turn-1".into(),
            generation: 0,
            event_revision: 2,
            state: AgentTurnState::Completed,
            result: Some(result.clone()),
            reason: None,
            result_digest: Some(format!("{:x}", Sha256::digest(result.as_bytes()))),
            native_capability: None,
        };
        assert!(validate(&report).is_ok());
        report.result_digest = Some("0".repeat(64));
        assert!(validate(&report)
            .unwrap_err()
            .contains("does not match result"));
        report.result = Some("x".repeat(MAX_TEXT_BYTES + 1));
        assert!(validate(&report).unwrap_err().contains("payload_too_large"));
    }
}
