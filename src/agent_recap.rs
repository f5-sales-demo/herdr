use std::path::PathBuf;

use crate::api::schema::{AgentRecapRecord, AgentRecapReportParams};

const MAX_RECORDS: usize = 1024;
const MAX_TOMBSTONES: usize = 4096;

#[derive(Default, serde::Serialize, serde::Deserialize)]
struct State {
    records: Vec<AgentRecapRecord>,
    #[serde(default)]
    expired_ids: Vec<String>,
}

pub(crate) struct AgentRecapStore {
    path: PathBuf,
    state: State,
}

impl AgentRecapStore {
    pub(crate) fn load() -> Self {
        Self::load_at(crate::session::data_dir().join("agent-recaps.json"))
    }

    pub(crate) fn load_at(path: PathBuf) -> Self {
        let mut state: State = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default();
        if state.records.len() > MAX_RECORDS {
            let expired = state.records.drain(..state.records.len() - MAX_RECORDS);
            state
                .expired_ids
                .extend(expired.map(|record| record.report.id));
        }
        if state.expired_ids.len() > MAX_TOMBSTONES {
            state
                .expired_ids
                .drain(..state.expired_ids.len() - MAX_TOMBSTONES);
        }
        Self { path, state }
    }

    pub(crate) fn report(
        &mut self,
        report: AgentRecapReportParams,
    ) -> Result<(AgentRecapRecord, bool), String> {
        validate(&report)?;
        let record = AgentRecapRecord { report };
        if let Some(existing) = self
            .state
            .records
            .iter()
            .find(|existing| existing.report.id == record.report.id)
        {
            return if existing == &record {
                Ok((existing.clone(), false))
            } else {
                Err("agent_recap_conflict: recap id already has different content".into())
            };
        }
        if self.state.expired_ids.contains(&record.report.id) {
            return Err("agent_recap_expired: recap id has expired".into());
        }
        let mut next = State {
            records: self.state.records.clone(),
            expired_ids: self.state.expired_ids.clone(),
        };
        next.records.push(record.clone());
        if next.records.len() > MAX_RECORDS {
            let expired = next.records.drain(..next.records.len() - MAX_RECORDS);
            next.expired_ids
                .extend(expired.map(|record| record.report.id));
        }
        if next.expired_ids.len() > MAX_TOMBSTONES {
            next.expired_ids
                .drain(..next.expired_ids.len() - MAX_TOMBSTONES);
        }
        self.persist(&next)?;
        self.state = next;
        Ok((record, true))
    }

    pub(crate) fn latest(&self, source: &str, session_id: &str) -> Option<AgentRecapRecord> {
        self.state
            .records
            .iter()
            .rev()
            .find(|record| record.report.source == source && record.report.session_id == session_id)
            .cloned()
    }

    fn persist(&self, state: &State) -> Result<(), String> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        }
        let bytes = serde_json::to_vec(state).map_err(|error| error.to_string())?;
        let temp = self.path.with_extension("json.tmp");
        std::fs::write(&temp, bytes).map_err(|error| error.to_string())?;
        std::fs::rename(temp, &self.path).map_err(|error| error.to_string())
    }
}

fn validate(report: &AgentRecapReportParams) -> Result<(), String> {
    for (name, value) in [
        ("pane_id", report.pane_id.as_str()),
        ("source", report.source.as_str()),
        ("session_id", report.session_id.as_str()),
        ("id", report.id.as_str()),
        ("created_at", report.created_at.as_str()),
    ] {
        if value.is_empty() || value.len() > 4096 || value.chars().any(char::is_control) {
            return Err(format!("invalid_agent_recap: {name} is invalid"));
        }
    }
    if report.id.len() > 256 || report.source.len() > 256 || report.created_at.len() > 64 {
        return Err("invalid_agent_recap: identifier is too long".into());
    }
    let summary = report.summary.trim();
    if summary.is_empty() || summary.chars().count() > 700 || summary.chars().any(char::is_control)
    {
        return Err("invalid_agent_recap: summary must contain 1 to 700 characters".into());
    }
    if report.next_action.as_deref().is_some_and(|next| {
        next.trim().is_empty() || next.chars().count() > 200 || next.chars().any(char::is_control)
    }) {
        return Err("invalid_agent_recap: next_action must contain 1 to 200 characters".into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::AgentRecapTrigger;

    fn temp_path() -> PathBuf {
        std::env::temp_dir().join(format!("herdr-recaps-{}.json", uuid::Uuid::new_v4()))
    }

    fn report(id: &str, session_id: &str) -> AgentRecapReportParams {
        AgentRecapReportParams {
            pane_id: "w1:p1".into(),
            source: "herdr:xcsh".into(),
            session_id: session_id.into(),
            id: id.into(),
            trigger: AgentRecapTrigger::Manual,
            summary: "The work is complete.".into(),
            next_action: Some("Review tests.".into()),
            completed_turn_count: 3,
            created_at: "2026-09-25T12:00:00Z".into(),
        }
    }

    #[test]
    fn duplicate_delivery_replays_and_conflict_rejects() {
        let mut store = AgentRecapStore::load_at(temp_path());
        let (record, admitted) = store.report(report("one", "session-a")).expect("first");
        assert!(admitted);
        assert!(!store.report(record.report.clone()).expect("retry").1);
        let mut conflict = record.report;
        conflict.summary = "Different.".into();
        assert!(store.report(conflict).is_err());
        assert_eq!(store.state.records.len(), 1);
    }

    #[test]
    fn restart_replay_and_session_switch() {
        let path = temp_path();
        AgentRecapStore::load_at(path.clone())
            .report(report("one", "session-a"))
            .expect("write");
        let mut restored = AgentRecapStore::load_at(path);
        // Public pane identity can change on move while the session stays the same.
        assert!(restored.latest("herdr:xcsh", "session-a").is_some());
        assert!(restored.latest("herdr:xcsh", "session-a").is_some());
        assert!(restored.latest("herdr:xcsh", "session-b").is_none());
        restored.report(report("two", "session-b")).expect("switch");
        assert_eq!(
            restored
                .latest("herdr:xcsh", "session-b")
                .expect("latest")
                .report
                .id,
            "two"
        );
    }

    #[test]
    fn evicted_ids_remain_rejected_after_restart() {
        let path = temp_path();
        let mut store = AgentRecapStore::load_at(path.clone());
        store.state.records = (0..MAX_RECORDS)
            .map(|index| AgentRecapRecord {
                report: report(&format!("recap-{index}"), "session-a"),
            })
            .collect();
        store.report(report("latest", "session-a")).expect("admit");
        assert_eq!(store.state.expired_ids, ["recap-0"]);
        let mut restored = AgentRecapStore::load_at(path);
        assert!(restored.report(report("recap-0", "session-a")).is_err());
        assert_eq!(
            restored
                .latest("herdr:xcsh", "session-a")
                .expect("latest")
                .report
                .id,
            "latest"
        );
    }

    #[test]
    fn rejects_unbounded_or_empty_content() {
        let mut store = AgentRecapStore::load_at(temp_path());
        let mut invalid = report("one", "session-a");
        invalid.summary = "x".repeat(701);
        assert!(store.report(invalid).is_err());
        let mut invalid = report("two", "session-a");
        invalid.next_action = Some(" ".into());
        assert!(store.report(invalid).is_err());
    }
}
