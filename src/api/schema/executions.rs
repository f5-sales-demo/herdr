use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionState {
    Starting,
    Running,
    Exited,
    Cancelled,
    Lost,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "mode", rename_all = "snake_case")]
pub enum ExecutionCommand {
    Argv { argv: Vec<String> },
    Shell { shell: String, text: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionStartParams {
    pub execution_id: String,
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
    #[serde(flatten)]
    pub command: ExecutionCommand,
}

/// Admit an XCSH child for one immutable semantic execution generation.
///
/// `execution_id` is owned by the producer's semantic task. Herdr assigns a
/// different `backend_execution_id` for the visible child it launches.
///
/// `native_launch` is versioned and fully typed. Herdr never accepts a
/// caller-provided shell command, environment, or opaque XCSH argv here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionResumeParams {
    pub execution_id: String,
    pub generation: u64,
    pub native_launch: NativeLaunchV3,
    pub text: String,
    pub cwd: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// Version 3 native XCSH launch contract for protocol 23.
///
/// `session_path` is a canonical absolute JSONL path. `session_header.sha256`
/// is the SHA-256 of exactly the first JSONL line, including its terminating
/// LF byte. The header's `id` is XCSH's canonical 16-character lowercase hex
/// SessionHeader ID; selector prefixes and paths are never reporter IDs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct NativeLaunchV3 {
    pub version: u8,
    pub xcsh_executable: String,
    /// Canonical absolute directory containing `session_path`.
    pub session_dir: String,
    pub session_path: String,
    pub session_header: NativeSessionHeaderBinding,
    /// Configured, non-secret XCSH model selector. It is not a user identity.
    pub model: String,
    pub discovery: NativeDiscoveryPolicy,
    pub tools: NativeToolsPolicy,
    pub interactive: bool,
    pub lifecycle_mode: NativeLifecycleMode,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct NativeSessionHeaderBinding {
    pub id: String,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "kebab-case")]
pub enum NativeDiscoveryPolicy {
    ReducedV1,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NativeToolsPolicy {
    Read,
}

/// Semantic lifecycle behavior, retained as durable producer contract rather
/// than translated into an undocumented XCSH command-line flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum NativeLifecycleMode {
    ManagedTurnV1,
}

/// Immutable measurement of the XCSH program admitted for a native child.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct NativeExecutableBinding {
    pub canonical_path: String,
    pub sha256: String,
}

/// Durable proof that the launched native child has made its first
/// authenticated `starting` report. The PID is Herdr's PTY-child ownership
/// evidence; the inherited capability authenticates the reporting process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct NativeProducerRegistration {
    pub producer: String,
    pub session_id: String,
    pub generation: u64,
    pub pane_id: String,
    pub pid: u32,
    pub turn_id: String,
    pub registered_at_unix_ms: u64,
    /// Set only after the matching starting frame is durable in the turn journal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub journaled_at_unix_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionTarget {
    pub execution_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema, Default)]
pub struct ExecutionListParams {
    #[serde(default)]
    pub since_revision: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionWaitParams {
    pub after_revision: u64,
    #[serde(default = "default_wait_timeout_ms")]
    pub timeout_ms: u64,
}
fn default_wait_timeout_ms() -> u64 {
    30_000
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ExecutionRecord {
    pub execution_id: String,
    /// Workspace selected for the visible child. Native consumers use this
    /// durable receipt to prove the child was attached to the claimed
    /// workspace rather than merely a similarly named tab or pane.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace_id: Option<String>,
    /// Herdr-owned visible-child identity. It differs from `execution_id` for
    /// native resumes and is the target for cancellation and observation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_execution_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_producer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_executable: Option<NativeExecutableBinding>,
    /// Complete protocol-23 typed native launch receipt. Kept alongside the
    /// normalized executable/session fields for compatibility with existing
    /// execution consumers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_launch: Option<NativeLaunchV3>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_registration: Option<NativeProducerRegistration>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub producer_session_id: Option<String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub injected_env: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub superseded_by_backend_execution_id: Option<String>,
    pub cwd: String,
    pub command: ExecutionCommand,
    pub state: ExecutionState,
    pub revision: u64,
    pub admitted_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub started_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pane_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tab_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub exit_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signal_name: Option<String>,
    #[serde(default)]
    pub cancel_requested: bool,
    #[serde(default)]
    pub stdout_bytes: u64,
    #[serde(default)]
    pub stderr_bytes: u64,
    #[serde(default)]
    pub stdout_tail: String,
    #[serde(default)]
    pub stderr_tail: String,
    #[serde(default)]
    pub output_truncated: bool,
    #[serde(default)]
    pub output_complete: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub evidence_gap: Option<String>,
}
