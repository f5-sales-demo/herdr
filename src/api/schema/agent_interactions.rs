use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct InteractionOwner {
    pub execution_id: String,
    pub pane_id: String,
    pub producer: String,
    pub session_id: String,
    pub generation: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct InteractionTarget {
    pub owner: InteractionOwner,
    pub request_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InteractionState {
    Pending,
    Answered,
    Cancelled,
    Interrupted,
    Dismissed,
    Expired,
    Superseded,
    OwnerLost,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InteractionKind {
    Waiting,
    Async,
    PlanDecision,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct InteractionReportParams {
    pub target: InteractionTarget,
    pub thread_id: String,
    pub turn_id: String,
    pub item_id: String,
    pub question_ids: Vec<String>,
    pub event_revision: u64,
    pub kind: InteractionKind,
    /// Codex payload, unchanged. Never include responses or local drafts here.
    pub payload: serde_json::Value,
    pub state: InteractionState,
    /// Private per-execution producer capability. Accepted on requests only and never journaled.
    #[serde(default, skip_serializing)]
    pub native_capability: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct InteractionRecord {
    pub report: InteractionReportParams,
    pub revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InteractionDeliveryState {
    Queued,
    Accepted,
    Rejected,
    OwnerLost,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct InteractionReceipt {
    pub target: InteractionTarget,
    pub response_id: String,
    pub state: InteractionDeliveryState,
    pub revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct InteractionRespondParams {
    pub target: InteractionTarget,
    pub response_id: String,
    pub answer: serde_json::Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct InteractionDeliveryTarget {
    pub owner: InteractionOwner,
    /// Private per-execution producer capability. Accepted on requests only and never returned.
    #[serde(default, skip_serializing)]
    pub native_capability: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct InteractionDelivery {
    pub receipt: InteractionReceipt,
    /// Private producer response, never included in records, events or the journal.
    pub answer: serde_json::Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct InteractionAckParams {
    pub producer: InteractionDeliveryTarget,
    pub request_id: String,
    pub response_id: String,
    pub accepted: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct InteractionListParams {
    #[serde(default)]
    pub after_revision: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct InteractionWaitParams {
    pub after_revision: u64,
    #[serde(default = "default_wait")]
    pub timeout_ms: u64,
}
fn default_wait() -> u64 {
    30_000
}
