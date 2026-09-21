use std::collections::BTreeMap;

use std::fmt;

use serde::{Deserialize, Serialize};

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct WorkerContextIssueParams {
    pub pane_id: String,
    pub context_capability: String,
}

impl fmt::Debug for WorkerContextIssueParams {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerContextIssueParams")
            .field("pane_id", &self.pane_id)
            .field("context_capability", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct WorkerContextClaimParams {
    pub pairing_token: String,
    pub consumer_id: String,
}

impl fmt::Debug for WorkerContextClaimParams {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerContextClaimParams")
            .field("pairing_token", &"<redacted>")
            .field("consumer_id", &self.consumer_id)
            .finish()
    }
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct WorkerContextLeaseParams {
    pub lease: String,
    pub consumer_id: String,
}

impl fmt::Debug for WorkerContextLeaseParams {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkerContextLeaseParams")
            .field("lease", &"<redacted>")
            .field("consumer_id", &self.consumer_id)
            .finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct WorkerContextPane {
    pub workspace_id: String,
    pub tab_id: String,
    pub pane_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct WorkerContext {
    pub capabilities: ServerWorkerContextCapabilities,
    pub pane: WorkerContextPane,
    pub environment: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ServerWorkerContextCapabilities {
    pub worker_context_handoff: u32,
    pub xcsh_semantic_tracking: u32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_bearing_parameters_redact_debug_output() {
        let issue = WorkerContextIssueParams {
            pane_id: "w1:p1".into(),
            context_capability: "root-secret".into(),
        };
        let claim = WorkerContextClaimParams {
            pairing_token: "pair-secret".into(),
            consumer_id: "window-a".into(),
        };
        let lease = WorkerContextLeaseParams {
            lease: "lease-secret".into(),
            consumer_id: "window-a".into(),
        };

        let rendered = format!("{issue:?} {claim:?} {lease:?}");
        assert!(!rendered.contains("root-secret"));
        assert!(!rendered.contains("pair-secret"));
        assert!(!rendered.contains("lease-secret"));
        assert!(rendered.contains("<redacted>"));
    }
}
