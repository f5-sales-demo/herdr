use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct PingParams {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ServerLiveHandoffParams {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub import_exe: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_protocol: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ServerCapabilities {
    pub live_handoff: bool,
    #[serde(default)]
    pub detached_server_daemon: bool,
    /// Stable client-owned endpoint generation supported by this server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint_protocol_generation: Option<u32>,
    /// Whether this server supports explicit client-shell surface interest.
    #[serde(default)]
    pub surface_interest: bool,
    /// Whether this server supports endpoint health probes.
    #[serde(default)]
    pub health_check: bool,
    /// Whether durable execution lifecycle methods are available.
    #[serde(default)]
    pub tracked_executions: bool,
    /// Whether semantic agent-turn reporting and replay are available.
    #[serde(default)]
    pub agent_turn_journal: bool,
    /// Version of the session-bound agent recap API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_recaps: Option<u32>,
    /// Version of the independent observation and producer-acknowledged interaction API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_interactions: Option<u32>,
    /// Version of the external worker context pairing and renewable lease API.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_context_handoff: Option<u32>,
    /// Version of the semantic xcsh execution reporting contract.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub xcsh_semantic_tracking: Option<u32>,
}
