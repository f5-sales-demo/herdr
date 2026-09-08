use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AgentTurnState {
    Starting,
    Working,
    WaitingInput,
    Completed,
    Failed,
    Cancelled,
    Interrupted,
    Lost,
}

impl AgentTurnState {
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Interrupted | Self::Lost
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentTurnReportParams {
    pub execution_id: String,
    pub pane_id: String,
    pub producer: String,
    pub session_id: String,
    pub turn_id: String,
    #[serde(default)]
    pub generation: u64,
    pub event_revision: u64,
    pub state: AgentTurnState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result_digest: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentTurnTarget {
    pub producer: String,
    pub session_id: String,
    pub turn_id: String,
    #[serde(default)]
    pub generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct AgentTurnListParams {
    #[serde(default)]
    pub since_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentTurnWaitParams {
    pub after_revision: u64,
    #[serde(default = "default_wait_timeout_ms")]
    pub timeout_ms: u64,
}

fn default_wait_timeout_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AgentTurnRecord {
    #[serde(flatten)]
    pub report: AgentTurnReportParams,
    pub revision: u64,
    pub reported_at_unix_ms: u64,
}
