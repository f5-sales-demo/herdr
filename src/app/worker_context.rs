use std::collections::BTreeMap;

use super::App;
use crate::api::schema::{
    ResponseResult, ServerWorkerContextCapabilities, WorkerContext, WorkerContextClaimParams,
    WorkerContextIssueParams, WorkerContextLeaseParams, WorkerContextPane,
};

impl App {
    #[cfg(unix)]
    pub(crate) fn worker_context_handoff_state(
        &self,
    ) -> crate::worker_context::WorkerContextHandoffState {
        let pane_verifiers = self
            .state
            .workspaces
            .iter()
            .flat_map(|workspace| workspace.tabs.iter())
            .flat_map(|tab| tab.panes.iter())
            .filter_map(|(pane_id, pane)| {
                pane.context_capability_verifier
                    .clone()
                    .map(|verifier| (pane_id.raw(), verifier))
            })
            .collect();
        crate::worker_context::WorkerContextHandoffState {
            state: self.worker_context.clone(),
            pane_verifiers,
        }
    }

    #[cfg(unix)]
    pub(crate) fn restore_worker_context_handoff(
        &mut self,
        mut handoff: crate::worker_context::WorkerContextHandoffState,
        aliases: &std::collections::HashMap<u32, crate::layout::PaneId>,
    ) {
        handoff.state.rebind_panes(aliases);
        for (old_id, verifier) in handoff.pane_verifiers {
            let Some(pane_id) = aliases.get(&old_id).copied() else {
                continue;
            };
            if let Some((ws_idx, _)) = self.find_pane(pane_id) {
                if let Some(pane) = self.state.workspaces[ws_idx].pane_state_mut(pane_id) {
                    pane.context_capability_verifier = Some(verifier);
                }
            }
        }
        handoff
            .state
            .retain_live(|pane_id| self.find_pane(pane_id).is_some());
        self.worker_context = handoff.state;
    }

    fn worker_context_pane(&self, pane_id: crate::layout::PaneId) -> Option<WorkerContextPane> {
        let (ws_idx, _) = self.find_pane(pane_id)?;
        let workspace = self.state.workspaces.get(ws_idx)?;
        let tab_idx = workspace.find_tab_index_for_pane(pane_id)?;
        Some(WorkerContextPane {
            workspace_id: self.public_workspace_id(ws_idx),
            tab_id: self.public_tab_id(ws_idx, tab_idx)?,
            pane_id: self.public_pane_id(ws_idx, pane_id)?,
        })
    }

    pub(super) fn handle_worker_context_issue(
        &mut self,
        id: String,
        params: WorkerContextIssueParams,
    ) -> String {
        let Some((_, pane_id)) = self.parse_current_public_pane_id(&params.pane_id) else {
            return super::api::responses::encode_error(
                id,
                "worker_context_unauthorized",
                "the requesting pane is not live",
            );
        };
        let expected = crate::worker_context::verifier(&params.context_capability);
        let authorized = self
            .find_pane(pane_id)
            .and_then(|(_, pane)| pane.context_capability_verifier.as_deref())
            == Some(expected.as_str());
        if !authorized {
            return super::api::responses::encode_error(
                id,
                "worker_context_unauthorized",
                "the pane context capability is invalid",
            );
        }
        let Some(pane) = self.worker_context_pane(pane_id) else {
            return super::api::responses::encode_error(
                id,
                "worker_context_stale",
                "the requesting pane is no longer live",
            );
        };
        let pairing_token = self.worker_context.issue(pane_id);
        super::api::responses::encode_success(
            id,
            ResponseResult::WorkerContextPairing {
                pairing_token,
                pane,
            },
        )
    }

    pub(super) fn handle_worker_context_claim(
        &mut self,
        id: String,
        params: WorkerContextClaimParams,
    ) -> String {
        let Some(pane_id) = self.worker_context.pane_for_pairing(&params.pairing_token) else {
            return super::api::responses::encode_error(
                id,
                "worker_context_stale",
                "the pairing token is invalid, expired, or already claimed",
            );
        };
        let Some(pane) = self.worker_context_pane(pane_id) else {
            self.worker_context.revoke_pane(pane_id);
            return super::api::responses::encode_error(
                id,
                "worker_context_stale",
                "the paired pane is no longer live",
            );
        };
        let Some(lease) = self
            .worker_context
            .claim(&params.pairing_token, &params.consumer_id)
        else {
            return super::api::responses::encode_error(
                id,
                "worker_context_stale",
                "the pairing token is invalid, expired, or already claimed",
            );
        };
        super::api::responses::encode_success(
            id,
            ResponseResult::WorkerContextLease { lease, pane },
        )
    }

    pub(super) fn handle_worker_context_resolve(
        &mut self,
        id: String,
        params: WorkerContextLeaseParams,
    ) -> String {
        let Some(pane_id) = self
            .worker_context
            .pane_for_lease(&params.lease, &params.consumer_id)
        else {
            return super::api::responses::encode_error(
                id,
                "worker_context_stale",
                "the worker context lease is invalid, expired, or revoked",
            );
        };
        let Some(pane) = self.worker_context_pane(pane_id) else {
            self.worker_context.revoke_pane(pane_id);
            return super::api::responses::encode_error(
                id,
                "worker_context_stale",
                "the paired pane is no longer live",
            );
        };
        if !self
            .worker_context
            .renew(&params.lease, &params.consumer_id)
        {
            return super::api::responses::encode_error(
                id,
                "worker_context_stale",
                "the worker context lease could not be renewed",
            );
        }
        let mut environment = BTreeMap::new();
        environment.insert(
            crate::HERDR_ENV_VAR.to_owned(),
            crate::HERDR_ENV_VALUE.to_owned(),
        );
        environment.insert(
            crate::api::SOCKET_PATH_ENV_VAR.to_owned(),
            crate::api::socket_path().display().to_string(),
        );
        environment.insert(
            crate::integration::HERDR_WORKSPACE_ID_ENV_VAR.to_owned(),
            pane.workspace_id.clone(),
        );
        environment.insert(
            crate::integration::HERDR_TAB_ID_ENV_VAR.to_owned(),
            pane.tab_id.clone(),
        );
        environment.insert(
            crate::integration::HERDR_PANE_ID_ENV_VAR.to_owned(),
            pane.pane_id.clone(),
        );
        if let Ok(executable) = crate::platform::launch_executable() {
            environment.insert(
                "HERDR_BIN_PATH".to_owned(),
                executable.display().to_string(),
            );
        }
        super::api::responses::encode_success(
            id,
            ResponseResult::WorkerContext {
                context: WorkerContext {
                    capabilities: ServerWorkerContextCapabilities {
                        worker_context_handoff: 1,
                        xcsh_semantic_tracking: 1,
                    },
                    pane,
                    environment,
                },
            },
        )
    }

    pub(super) fn handle_worker_context_revoke(
        &mut self,
        id: String,
        params: WorkerContextLeaseParams,
    ) -> String {
        let revoked = self
            .worker_context
            .revoke(&params.lease, &params.consumer_id);
        super::api::responses::encode_success(id, ResponseResult::WorkerContextRevoked { revoked })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::schema::{ErrorResponse, SuccessResponse};

    fn app_with_capability() -> (App, String, String) {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &crate::config::Config::default(),
            crate::app::AppPolicy::TEST,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        app.state.workspaces = vec![crate::workspace::Workspace::test_new("context")];
        app.state.ensure_test_terminals();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let public_id = app.public_pane_id(0, pane_id).expect("public pane id");
        let capability = crate::worker_context::new_secret();
        app.state.workspaces[0]
            .pane_state_mut(pane_id)
            .expect("pane")
            .context_capability_verifier = Some(crate::worker_context::verifier(&capability));
        (app, public_id, capability)
    }

    #[test]
    fn issue_claim_resolve_and_revoke_keep_secrets_out_of_context() {
        let (mut app, pane_id, capability) = app_with_capability();
        let issued: SuccessResponse = serde_json::from_str(&app.handle_worker_context_issue(
            "issue".into(),
            WorkerContextIssueParams {
                pane_id: pane_id.clone(),
                context_capability: capability.clone(),
            },
        ))
        .expect("issue response");
        let ResponseResult::WorkerContextPairing { pairing_token, .. } = issued.result else {
            panic!("expected pairing response");
        };
        let claimed: SuccessResponse = serde_json::from_str(&app.handle_worker_context_claim(
            "claim".into(),
            WorkerContextClaimParams {
                pairing_token: pairing_token.clone(),
                consumer_id: "window-a".into(),
            },
        ))
        .expect("claim response");
        let ResponseResult::WorkerContextLease { lease, .. } = claimed.result else {
            panic!("expected lease response");
        };
        let replay = app.handle_worker_context_claim(
            "replay".into(),
            WorkerContextClaimParams {
                pairing_token: pairing_token.clone(),
                consumer_id: "window-a".into(),
            },
        );
        assert!(serde_json::from_str::<ErrorResponse>(&replay).is_ok());

        let resolved = app.handle_worker_context_resolve(
            "resolve".into(),
            WorkerContextLeaseParams {
                lease: lease.clone(),
                consumer_id: "window-a".into(),
            },
        );
        assert!(!resolved.contains(&capability));
        assert!(!resolved.contains(&pairing_token));
        assert!(!resolved.contains(&lease));
        let resolved: SuccessResponse = serde_json::from_str(&resolved).expect("resolve response");
        let ResponseResult::WorkerContext { context } = resolved.result else {
            panic!("expected context response");
        };
        assert_eq!(context.pane.pane_id, pane_id);
        assert_eq!(context.capabilities.worker_context_handoff, 1);
        assert_eq!(context.capabilities.xcsh_semantic_tracking, 1);
        assert_eq!(context.environment.len(), 6);
        assert!(!context
            .environment
            .contains_key(crate::worker_context::CAPABILITY_ENV_VAR));

        let revoked: SuccessResponse = serde_json::from_str(&app.handle_worker_context_revoke(
            "revoke".into(),
            WorkerContextLeaseParams {
                lease: lease.clone(),
                consumer_id: "window-a".into(),
            },
        ))
        .expect("revoke response");
        assert!(matches!(
            revoked.result,
            ResponseResult::WorkerContextRevoked { revoked: true }
        ));
        let stale = app.handle_worker_context_resolve(
            "stale".into(),
            WorkerContextLeaseParams {
                lease,
                consumer_id: "window-a".into(),
            },
        );
        assert!(serde_json::from_str::<ErrorResponse>(&stale).is_ok());
    }

    #[test]
    fn issue_rejects_forged_or_wrong_pane_capability() {
        let (mut app, pane_id, capability) = app_with_capability();
        let forged = app.handle_worker_context_issue(
            "forged".into(),
            WorkerContextIssueParams {
                pane_id: pane_id.clone(),
                context_capability: "forged".into(),
            },
        );
        assert!(serde_json::from_str::<ErrorResponse>(&forged).is_ok());
        let wrong_pane = app.handle_worker_context_issue(
            "wrong".into(),
            WorkerContextIssueParams {
                pane_id: "w-missing:p1".into(),
                context_capability: capability,
            },
        );
        assert!(serde_json::from_str::<ErrorResponse>(&wrong_pane).is_ok());
    }
}
