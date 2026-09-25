use crate::api::schema::{AgentRecapReportParams, AgentTarget, ResponseResult};
use crate::app::App;

use super::responses::{encode_error, encode_error_body, encode_success};

impl App {
    pub(super) fn handle_agent_recap_report(
        &mut self,
        id: String,
        params: AgentRecapReportParams,
    ) -> String {
        let Some((ws_idx, pane_id)) = self.parse_pane_id(&params.pane_id) else {
            return encode_error(id, "pane_not_found", "recap pane was not found");
        };
        let Some(terminal_id) = self
            .state
            .workspaces
            .get(ws_idx)
            .and_then(|workspace| workspace.pane_state(pane_id))
            .map(|pane| pane.attached_terminal_id.clone())
        else {
            return encode_error(id, "pane_not_found", "recap pane was not found");
        };
        let Some(terminal) = self.state.terminals.get(&terminal_id) else {
            return encode_error(id, "pane_not_found", "recap terminal was not found");
        };
        let authorized = terminal.hook_authority.as_ref().is_some_and(|authority| {
            authority.source == params.source
                && authority
                    .session_ref
                    .as_ref()
                    .is_some_and(|session| session.value == params.session_id)
        });
        if !authorized {
            return encode_error(
                id,
                "agent_recap_authority_mismatch",
                "recap source and session must match the pane's active reporter",
            );
        }
        match self.agent_recaps.report(params) {
            Ok((recap, admitted)) => {
                self.render_dirty.request_generic();
                self.render_notify.notify_one();
                encode_success(id, ResponseResult::AgentRecap { recap, admitted })
            }
            Err(error) => encode_error(id, "invalid_agent_recap", error),
        }
    }

    pub(super) fn handle_agent_recap_get(&mut self, id: String, target: AgentTarget) -> String {
        self.reconcile_managed_agent_target(&target.target);
        let agent = match self.agent_info_for_target(&target.target) {
            Ok(agent) => agent,
            Err(error) => return encode_error_body(id, self.agent_target_error_body(error)),
        };
        match agent.latest_recap {
            Some(recap) => encode_success(
                id,
                ResponseResult::AgentRecap {
                    recap,
                    admitted: false,
                },
            ),
            None => encode_error(
                id,
                "agent_recap_not_found",
                "no recap for the current session",
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        api::schema::{AgentRecapTrigger, ErrorResponse, SuccessResponse},
        config::Config,
        detect::AgentState,
        workspace::Workspace,
    };

    fn app_with_reporter() -> (App, String) {
        let (_api_tx, api_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut app = App::new(
            &Config::default(),
            crate::app::AppPolicy::TEST,
            None,
            api_rx,
            crate::api::EventHub::default(),
        );
        app.agent_recaps = crate::agent_recap::AgentRecapStore::load_at(
            std::env::temp_dir().join(format!("herdr-app-recaps-{}.json", uuid::Uuid::new_v4())),
        );
        app.state.workspaces = vec![Workspace::test_new("recap")];
        app.state.ensure_test_terminals();
        let pane_id = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&pane_id]
            .attached_terminal_id
            .clone();
        app.state
            .terminals
            .get_mut(&terminal_id)
            .expect("terminal")
            .set_detected_state(Some(crate::detect::Agent::Xcsh), AgentState::Idle);
        app.state
            .terminals
            .get_mut(&terminal_id)
            .expect("terminal")
            .set_agent_session_ref_for_session_start(
                "herdr:xcsh".into(),
                "xcsh".into(),
                crate::agent_resume::AgentSessionRef::id("session-a"),
                Some(1),
                Some("startup".into()),
            );
        app.state
            .terminals
            .get_mut(&terminal_id)
            .expect("terminal")
            .set_hook_authority_with_session_ref(
                "herdr:xcsh".into(),
                "xcsh".into(),
                AgentState::Idle,
                None,
                crate::agent_resume::AgentSessionRef::id("session-a"),
                Some(2),
            );
        assert!(
            app.state.terminals[&terminal_id].hook_authority.is_some(),
            "hook authority missing"
        );
        {
            let public = app.public_pane_id(0, pane_id).expect("pane");
            (app, public)
        }
    }

    fn params(pane_id: String) -> AgentRecapReportParams {
        AgentRecapReportParams {
            pane_id,
            source: "herdr:xcsh".into(),
            session_id: "session-a".into(),
            id: uuid::Uuid::new_v4().to_string(),
            trigger: AgentRecapTrigger::Manual,
            summary: "Completed the implementation.".into(),
            next_action: None,
            completed_turn_count: 3,
            created_at: "2026-09-25T12:00:00Z".into(),
        }
    }

    #[test]
    fn report_requires_authoritative_source_and_current_session() {
        let (mut app, _) = app_with_reporter();
        let pane_id = app
            .public_pane_id(0, app.state.workspaces[0].tabs[0].root_pane)
            .expect("pane");
        for (source, session_id) in [("other", "session-a"), ("herdr:xcsh", "session-old")] {
            let mut report = params(pane_id.clone());
            report.source = source.into();
            report.session_id = session_id.into();
            let response = app.handle_agent_recap_report("report".into(), report);
            let error: ErrorResponse = serde_json::from_str(&response).expect("rejection");
            assert_eq!(error.error.code, "agent_recap_authority_mismatch");
        }
        let accepted = app.handle_agent_recap_report("report".into(), params(pane_id));
        let success: SuccessResponse = serde_json::from_str(&accepted)
            .unwrap_or_else(|error| panic!("accepted: {error}; {accepted}"));
        assert!(matches!(
            success.result,
            ResponseResult::AgentRecap { admitted: true, .. }
        ));
    }

    #[test]
    fn get_projects_latest_for_current_session_without_changing_status() {
        let (mut app, _) = app_with_reporter();
        let internal_pane = app.state.workspaces[0].tabs[0].root_pane;
        let pane_id = app.public_pane_id(0, internal_pane).expect("pane");
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&internal_pane]
            .attached_terminal_id
            .clone();
        let before = app.state.terminals[&terminal_id].state;
        let response = app.handle_agent_recap_report("report".into(), params(pane_id.clone()));
        assert!(
            serde_json::from_str::<SuccessResponse>(&response).is_ok(),
            "{response}"
        );
        assert_eq!(app.state.terminals[&terminal_id].state, before);
        let target = AgentTarget {
            target: pane_id.clone(),
        };
        let get = app.handle_agent_get("get".into(), target.clone());
        let success: SuccessResponse = serde_json::from_str(&get).expect("agent");
        let ResponseResult::AgentInfo { agent } = success.result else {
            panic!("agent info")
        };
        assert!(agent.latest_recap.is_some());
        let recap_get = app.handle_agent_recap_get("recap".into(), target.clone());
        let success: SuccessResponse = serde_json::from_str(&recap_get).expect("recap");
        assert!(matches!(
            success.result,
            ResponseResult::AgentRecap {
                admitted: false,
                ..
            }
        ));
        app.state
            .terminals
            .get_mut(&terminal_id)
            .expect("terminal")
            .hook_authority
            .as_mut()
            .expect("authority")
            .session_ref = crate::agent_resume::AgentSessionRef::id("session-b");
        let get = app.handle_agent_get("get".into(), target.clone());
        let success: SuccessResponse = serde_json::from_str(&get).expect("agent");
        let ResponseResult::AgentInfo { agent } = success.result else {
            panic!("agent info")
        };
        assert!(agent.latest_recap.is_none());
        let recap_get = app.handle_agent_recap_get("recap".into(), target);
        let error: ErrorResponse = serde_json::from_str(&recap_get).expect("not found");
        assert_eq!(error.error.code, "agent_recap_not_found");
    }

    #[test]
    fn xcsh_session_switch_hides_old_recap_and_rejects_late_old_reports() {
        let (mut app, pane_id) = app_with_reporter();
        let first = app.handle_agent_recap_report("first".into(), params(pane_id.clone()));
        assert!(
            serde_json::from_str::<SuccessResponse>(&first).is_ok(),
            "{first}"
        );

        let internal_pane = app.state.workspaces[0].tabs[0].root_pane;
        let terminal_id = app.state.workspaces[0].tabs[0].panes[&internal_pane]
            .attached_terminal_id
            .clone();
        let terminal = app.state.terminals.get_mut(&terminal_id).expect("terminal");
        let switched = terminal.set_agent_session_ref_for_session_start(
            "herdr:xcsh".into(),
            "xcsh".into(),
            crate::agent_resume::AgentSessionRef::id("session-b"),
            Some(3),
            Some("new".into()),
        );
        assert!(
            switched.is_some(),
            "xcsh should rebind on an explicit new session"
        );
        assert!(terminal
            .set_hook_authority_with_session_ref(
                "herdr:xcsh".into(),
                "xcsh".into(),
                AgentState::Idle,
                None,
                crate::agent_resume::AgentSessionRef::id("session-b"),
                Some(4),
            )
            .is_some());
        let late_old_state = terminal.set_hook_authority_with_session_ref(
            "herdr:xcsh".into(),
            "xcsh".into(),
            AgentState::Working,
            None,
            crate::agent_resume::AgentSessionRef::id("session-a"),
            Some(5),
        );
        assert!(late_old_state.is_none());
        assert_eq!(terminal.state, AgentState::Idle);

        let current = app.handle_agent_get(
            "current".into(),
            AgentTarget {
                target: pane_id.clone(),
            },
        );
        let success: SuccessResponse = serde_json::from_str(&current).expect("agent");
        let ResponseResult::AgentInfo { agent } = success.result else {
            panic!("agent info")
        };
        assert!(agent.latest_recap.is_none());

        let old = app.handle_agent_recap_report("old".into(), params(pane_id.clone()));
        let error: ErrorResponse = serde_json::from_str(&old).expect("old recap rejection");
        assert_eq!(error.error.code, "agent_recap_authority_mismatch");

        let mut new = params(pane_id.clone());
        new.session_id = "session-b".into();
        let accepted = app.handle_agent_recap_report("new".into(), new.clone());
        assert!(
            serde_json::from_str::<SuccessResponse>(&accepted).is_ok(),
            "{accepted}"
        );
        let duplicate = app.handle_agent_recap_report("duplicate".into(), new);
        let success: SuccessResponse = serde_json::from_str(&duplicate).expect("duplicate");
        assert!(matches!(
            success.result,
            ResponseResult::AgentRecap {
                admitted: false,
                ..
            }
        ));
        let current = app.handle_agent_get("current".into(), AgentTarget { target: pane_id });
        let success: SuccessResponse = serde_json::from_str(&current).expect("agent");
        let ResponseResult::AgentInfo { agent } = success.result else {
            panic!("agent info")
        };
        assert_eq!(
            agent
                .latest_recap
                .as_ref()
                .map(|recap| recap.report.session_id.as_str()),
            Some("session-b")
        );
    }
}
