use crate::terminal::TerminalId;

/// Viewport state for a pane.
///
/// Terminal identity, cwd, labels, and agent metadata live in TerminalState.
pub struct PaneState {
    pub attached_terminal_id: TerminalId,
    /// Whether the user has seen this pane since its last state change to Idle.
    /// False = "Done" (agent finished while user was in another workspace).
    pub seen: bool,
    /// Whether unmodified right-click gestures should be forwarded to the pane application.
    pub right_click_passthrough: bool,
    /// SHA-256 verifier for the root context capability injected into this pane.
    pub(crate) context_capability_verifier: Option<String>,
}

impl PaneState {
    pub fn new(attached_terminal_id: TerminalId) -> Self {
        Self {
            attached_terminal_id,
            seen: true,
            right_click_passthrough: false,
            context_capability_verifier: None,
        }
    }

    pub(crate) fn with_context_capability_verifier(mut self, verifier: Option<String>) -> Self {
        self.context_capability_verifier = verifier;
        self
    }
}
