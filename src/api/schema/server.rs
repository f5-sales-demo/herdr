use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct PingParams {}

crate::cli::projection::cli_params! {
    #[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
    pub struct ServerLiveHandoffParams => ServerLiveHandoffProjection {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub import_exe: Option<String> => crate::cli::projection::Field::new(None).input(
            crate::cli::projection::Input::value("import-exe", "path", |slot, value| {
                *slot = Some(value.to_owned());
                Ok(())
            }).path()
        ),
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub expected_protocol: Option<u32> => crate::cli::projection::Field::new(None).input(
            crate::cli::projection::Input::value("expected-protocol", "n", |slot, value| {
                *slot = Some(value.parse::<u32>().map_err(|_| {
                    crate::cli::projection::ParseError::Usage(format!("invalid expected protocol: {value}"))
                })?);
                Ok(())
            })
        ),
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub expected_version: Option<String> => crate::cli::projection::Field::new(None).input(
            crate::cli::projection::Input::value("expected-version", "version", |slot, value| {
                *slot = Some(value.to_owned());
                Ok(())
            })
        ),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ServerSshAgentRegisterParams {
    /// Absolute remote-host agent socket. Registration lasts until this API connection closes.
    pub socket_path: String,
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
    /// Typed native producer acceptance controls beyond managed_turn_v1.
    #[serde(default)]
    pub native_lifecycle_controls: bool,
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
    /// Supports connection-scoped `server.ssh_agent.register` on the local JSON API.
    #[serde(default)]
    pub ssh_agent_registration: bool,
}
