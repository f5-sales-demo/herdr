use std::path::PathBuf;

use super::responses::{encode_error, encode_success};
use super::App;
use crate::api::schema::{ExecutionCommand, ExecutionStartParams, ResponseResult};

impl App {
    pub(super) fn handle_execution_start(
        &mut self,
        id: String,
        params: ExecutionStartParams,
    ) -> String {
        let manager = crate::execution::ExecutionManager::global();
        let (existing, admitted) = match manager.admit_visible(&params) {
            Ok(result) => result,
            Err(error) => {
                let code = error
                    .split(':')
                    .next()
                    .unwrap_or("execution_error")
                    .to_string();
                return encode_error(id, &code, error);
            }
        };
        if !admitted {
            return encode_success(
                id,
                ResponseResult::Execution {
                    execution: existing,
                    admitted: false,
                },
            );
        }
        let ws_idx = if let Some(workspace_id) = params.workspace_id.as_deref() {
            match self.parse_workspace_id(workspace_id) {
                Some(index) => index,
                None => {
                    return self.fail_visible_execution(
                        id,
                        &params.execution_id,
                        "workspace_not_found",
                        "workspace not found",
                    )
                }
            }
        } else if let Some(active) = self.state.active {
            active
        } else {
            return self.fail_visible_execution(
                id,
                &params.execution_id,
                "workspace_not_found",
                "no active workspace",
            );
        };
        let argv = match visible_argv(&params.command) {
            Ok(argv) => argv,
            Err(error) => {
                return self.fail_visible_execution(
                    id,
                    &params.execution_id,
                    "invalid_command",
                    &error,
                )
            }
        };
        let (rows, cols) = self.state.estimate_pane_size();
        let result = self
            .state
            .workspaces
            .get_mut(ws_idx)
            .ok_or_else(|| std::io::Error::other("workspace disappeared"))
            .and_then(|workspace| {
                workspace.create_tab_argv_command(
                    rows,
                    cols,
                    PathBuf::from(&params.cwd),
                    &argv,
                    vec![("HERDR_EXECUTION_ID".into(), params.execution_id.clone())],
                    self.state.pane_scrollback_limit_bytes,
                    self.state.host_terminal_theme,
                )
            });
        let (tab_idx, terminal, runtime) = match result {
            Ok(created) => created,
            Err(error) => {
                return self.fail_visible_execution(
                    id,
                    &params.execution_id,
                    "execution_start_failed",
                    &error.to_string(),
                )
            }
        };
        let pane_raw = self.state.workspaces[ws_idx].tabs[tab_idx].root_pane;
        let pid = runtime.child_pid();
        self.terminal_runtimes.insert(terminal.id.clone(), runtime);
        self.state.terminals.insert(terminal.id.clone(), terminal);
        self.state.remove_alias_shadowed_by_new_pane(pane_raw);
        if let Some(label) = params.label {
            self.state.workspaces[ws_idx].tabs[tab_idx].set_custom_name(label);
        }
        let pane_id = self
            .public_pane_id(ws_idx, pane_raw)
            .expect("new execution pane has public id");
        let tab_id = self
            .public_tab_id(ws_idx, tab_idx)
            .expect("new execution tab has public id");
        let execution = match manager.attach_visible(
            &params.execution_id,
            pane_raw.raw(),
            pane_id,
            tab_id,
            pid,
        ) {
            Ok(record) => record,
            Err(error) => return encode_error(id, "execution_tracking_failed", error),
        };
        self.schedule_session_save();
        self.emit_tab_created_events(ws_idx, tab_idx);
        encode_success(
            id,
            ResponseResult::Execution {
                execution,
                admitted: true,
            },
        )
    }

    fn fail_visible_execution(
        &self,
        id: String,
        execution_id: &str,
        code: &str,
        message: &str,
    ) -> String {
        crate::execution::ExecutionManager::global().mark_start_failed(execution_id, message);
        encode_error(id, code, message)
    }

    pub(super) fn handle_execution_cancel(&mut self, id: String, execution_id: String) -> String {
        let manager = crate::execution::ExecutionManager::global();
        let record = match manager.request_visible_cancel(&execution_id) {
            Ok(record) => record,
            Err(error) => {
                let code = error
                    .split(':')
                    .next()
                    .unwrap_or("execution_error")
                    .to_string();
                return encode_error(id, &code, error);
            }
        };
        if !matches!(
            record.state,
            crate::api::schema::ExecutionState::Starting
                | crate::api::schema::ExecutionState::Running
        ) {
            return encode_success(
                id,
                ResponseResult::Execution {
                    execution: record,
                    admitted: false,
                },
            );
        }
        if let Some(pane_id) = record.pane_id.as_deref() {
            let Some((ws_idx, pane_raw)) = self.parse_pane_id(pane_id) else {
                return encode_error(
                    id,
                    "execution_not_owned",
                    "execution pane is no longer present",
                );
            };
            let Some((runtime, _)) = self.lookup_runtime(ws_idx, pane_raw) else {
                return encode_error(
                    id,
                    "execution_not_owned",
                    "execution pane runtime is no longer present",
                );
            };
            if let Err(error) = runtime.terminate_child() {
                return encode_error(id, "cancel_failed", error.to_string());
            }
        }
        encode_success(
            id,
            ResponseResult::Execution {
                execution: manager.get(&execution_id).unwrap_or(record),
                admitted: false,
            },
        )
    }
}

fn visible_argv(command: &ExecutionCommand) -> Result<Vec<String>, String> {
    match command {
        ExecutionCommand::Argv { argv } => Ok(argv.clone()),
        ExecutionCommand::Shell { shell, text } => {
            let program = match shell.as_str() {
                "bash" => "/usr/bin/bash",
                "zsh" => "/usr/bin/zsh",
                _ => return Err("shell must be bash or zsh".into()),
            };
            Ok(vec![program.into(), "-lc".into(), text.clone()])
        }
    }
}
