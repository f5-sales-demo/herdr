use super::harness::*;

fn native_launch(base: &Path, executable: &Path, session_id: &str) -> serde_json::Value {
    use sha2::Digest;

    let session_dir = base.join("native-sessions");
    fs::create_dir_all(&session_dir).unwrap();
    let session_path = session_dir.join("session.jsonl");
    let header = format!(
        "{{\"type\":\"session\",\"version\":3,\"id\":\"{session_id}\",\"cwd\":\"{}\"}}\n",
        base.display()
    );
    fs::write(&session_path, &header).unwrap();
    serde_json::json!({
        "version": 3,
        "xcsh_executable": executable.canonicalize().unwrap(),
        "session_dir": session_dir.canonicalize().unwrap(),
        "session_path": session_path.canonicalize().unwrap(),
        "session_header": {"id": session_id, "sha256": format!("{:x}", sha2::Sha256::digest(header.as_bytes()))},
        "model": "test/model",
        "discovery": "reduced-v1",
        "tools": "read",
        "interactive": false,
        "lifecycle_mode": "managed_turn_v1"
    })
}

#[test]
fn native_resume_socket_claim_is_idempotent_and_rejects_replay_conflicts() {
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let socket_path = runtime_dir.join("herdr.sock");
    let herdr = spawn_herdr(&config_home, &runtime_dir, &socket_path);
    wait_for_socket(&socket_path, Duration::from_secs(5));
    let request = |id: &str, text: &str| {
        serde_json::json!({
            "id": id, "method": "execution.resume", "params": {
                "execution_id": "semantic-replay", "generation": 3,
                "native_launch": native_launch(&base, Path::new("/bin/true"), "0123abcd4567ef89"), "text": text, "cwd": base,
            }
        })
    };
    // No workspace means the post-claim launch is deliberately rejected. This
    // exercises the socket contract without opening an external XCSH session.
    let first = send_request(
        &socket_path,
        &request("native-first", "continue").to_string(),
    );
    assert_eq!(first["error"]["code"], "workspace_not_found");
    let backend = send_request(
        &socket_path,
        r#"{"id":"native-list","method":"execution.list","params":{"since_revision":0}}"#,
    );
    let execution = &backend["result"]["executions"][0];
    assert_ne!(execution["execution_id"], "semantic-replay");
    assert_eq!(execution["semantic_execution_id"], "semantic-replay");
    assert_eq!(execution["generation"], 3);
    assert_eq!(
        execution["injected_env"]["HERDR_EXECUTION_ID"],
        "semantic-replay"
    );
    assert_eq!(execution["injected_env"]["HERDR_EXECUTION_GENERATION"], "3");
    let duplicate = send_request(
        &socket_path,
        &request("native-retry", "continue").to_string(),
    );
    assert_eq!(duplicate["result"]["admitted"], false);
    let conflict = send_request(
        &socket_path,
        &request("native-conflict", "different").to_string(),
    );
    assert_eq!(conflict["error"]["code"], "execution_generation_conflict");
    cleanup_spawned_herdr(herdr, base);
}

#[cfg(unix)]
#[test]
fn native_xcsh_fixture_child_receives_contract_and_replays_semantic_reports() {
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let socket_path = runtime_dir.join("herdr.sock");
    let bin_dir = base.join("bin");
    let args_path = base.join("xcsh-args");
    let env_path = base.join("xcsh-env");
    fs::create_dir_all(&bin_dir).unwrap();
    let fixture = bin_dir.join("xcsh");
    fs::write(
        &fixture,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$@\" > '{}'\nprintf '%s\\n%s\\n%s\\n%s\\n%s\\n' \"$HERDR_EXECUTION_ID\" \"$HERDR_EXECUTION_GENERATION\" \"$HERDR_SOCKET_PATH\" \"$HERDR_PANE_ID\" \"$HERDR_NATIVE_CAPABILITY\" > '{}'\nsleep 1\npython3 - \"$HERDR_SOCKET_PATH\" \"$HERDR_PANE_ID\" \"$HERDR_EXECUTION_ID\" \"$HERDR_EXECUTION_GENERATION\" \"$HERDR_NATIVE_CAPABILITY\" <<'PY'\nimport json, socket, sys\nsock, pane, execution, generation, capability = sys.argv[1:]\nframe = {{'method':'agent.turn.report','params':{{'execution_id':execution,'pane_id':pane,'producer':'xcsh','session_id':'0123abcd4567ef89','turn_id':'fixture-turn','generation':int(generation),'event_revision':1,'state':'starting','native_capability':capability}}}}\nfor request_id in ('fixture-first', 'fixture-replay'):\n    frame['id'] = request_id\n    client = socket.socket(socket.AF_UNIX)\n    client.connect(sock)\n    client.sendall((json.dumps(frame) + '\\n').encode())\n    client.recv(65536)\n    client.close()\nPY\nsleep 30\n",
            args_path.display(),
            env_path.display(),
        ),
    ).unwrap();
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(&fixture, fs::Permissions::from_mode(0o755)).unwrap();
    // Deliberately do not put the fixture on PATH. Native resume must launch
    // the durable measured executable binding, not a server-global lookup.
    let herdr = spawn_herdr(&config_home, &runtime_dir, &socket_path);
    wait_for_socket(&socket_path, Duration::from_secs(5));
    assert!(run_cli(
        &socket_path,
        &[
            "workspace",
            "create",
            "--cwd",
            base.to_str().unwrap(),
            "--focus"
        ]
    )
    .status
    .success());
    for invalid_session in [
        "0123abcd",
        "/tmp/xcsh-session.jsonl",
        "0123ABCD4567EF89",
        "123e4567-e89b-12d3-a456-426614174000",
    ] {
        let rejected = send_request(
            &socket_path,
            &serde_json::json!({
                "id": format!("invalid-{invalid_session}"), "method": "execution.resume", "params": {
                    "execution_id": "semantic-rejected", "generation": 10,
                    "native_launch": native_launch(&base, &fixture, invalid_session), "text": "fixture", "cwd": base,
                }
            })
            .to_string(),
        );
        assert_eq!(rejected["error"]["code"], "invalid_xcsh_session_id");
        assert!(
            !args_path.exists(),
            "invalid session selector launched xcsh"
        );
    }
    let mut bad_header = native_launch(&base, &fixture, "0123abcd4567ef89");
    bad_header["session_header"]["sha256"] = serde_json::Value::String("0".repeat(64));
    let rejected = send_request(
        &socket_path,
        &serde_json::json!({
            "id": "invalid-header", "method": "execution.resume", "params": {
                "execution_id": "semantic-header-rejected", "generation": 10,
                "native_launch": bad_header, "text": "fixture", "cwd": base,
            }
        })
        .to_string(),
    );
    assert_eq!(rejected["error"]["code"], "invalid_xcsh_session_header");
    assert!(
        !args_path.exists(),
        "invalid session header binding launched xcsh"
    );
    let resumed = send_request(
        &socket_path,
        &serde_json::json!({
            "id": "native-child", "method": "execution.resume", "params": {
                "execution_id": "semantic-child", "generation": 11,
            "native_launch": native_launch(&base, &fixture, "0123abcd4567ef89"), "text": "fixture", "cwd": base,
            }
        })
        .to_string(),
    );
    let execution = &resumed["result"]["execution"];
    assert_ne!(execution["execution_id"], "semantic-child");
    assert_eq!(
        execution["native_executable"]["canonical_path"],
        fixture.canonicalize().unwrap().to_string_lossy().as_ref()
    );
    assert_eq!(execution["native_launch"]["version"], 3);
    assert_eq!(
        execution["native_launch"]["session_header"]["id"],
        "0123abcd4567ef89"
    );
    assert_eq!(execution["native_launch"]["discovery"], "reduced-v1");
    assert_eq!(execution["native_launch"]["tools"], "read");
    assert_eq!(
        execution["native_launch"]["lifecycle_mode"],
        "managed_turn_v1"
    );
    assert_eq!(
        execution["command"]["argv"][0],
        fixture.canonicalize().unwrap().to_string_lossy().as_ref()
    );
    assert_eq!(
        execution["native_executable"]["sha256"]
            .as_str()
            .map(str::len),
        Some(64)
    );
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        if args_path.exists() && env_path.exists() {
            let turns = send_request(
                &socket_path,
                r#"{"id":"fixture-turns","method":"agent.turn.list","params":{"since_revision":0}}"#,
            );
            if turns["result"]["turns"]
                .as_array()
                .is_some_and(|turns| turns.len() == 1)
            {
                break;
            }
        }
        assert!(Instant::now() < deadline, "fixture child did not report");
        thread::sleep(Duration::from_millis(25));
    }
    let session_dir = base.join("native-sessions").canonicalize().unwrap();
    let session_path = session_dir.join("session.jsonl");
    assert_eq!(
        fs::read_to_string(&args_path).unwrap(),
        format!(
            "--mode\njson\n--session-dir\n{}\n--resume\n{}\n--model\ntest/model\n--tools\nread\n--no-mcp\n--no-lsp\n--no-pty\n--print\nfixture\n",
            session_dir.display(),
            session_path.display(),
        )
    );
    let env = fs::read_to_string(&env_path).unwrap();
    let lines: Vec<_> = env.lines().collect();
    assert_eq!(&lines[..2], ["semantic-child", "11"]);
    assert!(!lines[2].is_empty() && !lines[3].is_empty());
    assert!(!lines[4].is_empty());
    let capability = lines[4];
    let backend_id = execution["execution_id"].as_str().unwrap();
    for index in 0..=256 {
        let churn = send_request(
            &socket_path,
            &serde_json::json!({
                "id": format!("churn-{index}"), "method": "execution.start", "params": {
                    "execution_id": format!("settled-history-{index}"),
                    "cwd": base, "workspace_id": "w999", "mode": "argv", "argv": ["/bin/true"]
                }
            })
            .to_string(),
        );
        assert_eq!(churn["error"]["code"], "workspace_not_found");
    }
    let retained = send_request(
        &socket_path,
        &format!(
            r#"{{"id":"fixture-retained","method":"execution.get","params":{{"execution_id":"{backend_id}"}}}}"#
        ),
    );
    assert_eq!(retained["result"]["execution"]["state"], "running");
    let post_churn_report = send_request(
        &socket_path,
        &serde_json::json!({
            "id": "fixture-after-churn", "method": "agent.turn.report", "params": {
                "execution_id": "semantic-child", "pane_id": execution["pane_id"], "producer": "xcsh",
                "session_id": "0123abcd4567ef89", "turn_id": "fixture-turn",
                "generation": 11, "event_revision": 2, "state": "working", "native_capability": capability
            }
        })
        .to_string(),
    );
    assert!(
        post_churn_report.get("error").is_none(),
        "post-churn reporter was rejected: {post_churn_report}"
    );
    let second = send_request(&socket_path, &serde_json::json!({
        "id":"fixture-next", "method":"execution.resume", "params": {
            "execution_id":"semantic-child", "generation":12, "native_launch":native_launch(&base, &fixture, "0123abcd4567ef89"), "text":"fixture", "cwd":base
        }
    }).to_string());
    assert_eq!(second["result"]["admitted"], true);
    let duplicate_second = send_request(&socket_path, &serde_json::json!({
        "id":"fixture-next-retry", "method":"execution.resume", "params": {
            "execution_id":"semantic-child", "generation":12, "native_launch":native_launch(&base, &fixture, "0123abcd4567ef89"), "text":"fixture", "cwd":base
        }
    }).to_string());
    assert_eq!(duplicate_second["result"]["admitted"], false);
    let old_after_retry = send_request(
        &socket_path,
        &format!(
            r#"{{"id":"fixture-handoff-old-retry","method":"execution.get","params":{{"execution_id":"{backend_id}"}}}}"#
        ),
    );
    assert_eq!(old_after_retry["result"]["execution"]["state"], "running");
    assert!(old_after_retry["result"]["execution"]["cancel_requested"]
        .as_bool()
        .unwrap());
    assert_eq!(
        old_after_retry["result"]["execution"]["superseded_by_backend_execution_id"],
        second["result"]["execution"]["execution_id"]
    );
    let current_backend_id = second["result"]["execution"]["execution_id"]
        .as_str()
        .unwrap();
    let cancelled = send_request(
        &socket_path,
        &format!(
            r#"{{"id":"fixture-current-cancel","method":"execution.cancel","params":{{"execution_id":"{current_backend_id}"}}}}"#
        ),
    );
    assert!(cancelled.get("error").is_none());
    // Protocol 22 never force-terminates a native child. Cancellation is a
    // durable producer action and only an authenticated producer report can
    // settle the semantic result.
    assert!(cancelled["result"]["execution"]["cancel_requested"]
        .as_bool()
        .unwrap());
    assert_eq!(cancelled["result"]["execution"]["state"], "running");
    let old = send_request(
        &socket_path,
        &format!(
            r#"{{"id":"fixture-old","method":"execution.get","params":{{"execution_id":"{backend_id}"}}}}"#
        ),
    );
    assert!(old["result"]["execution"]["cancel_requested"]
        .as_bool()
        .unwrap());
    let action_target = serde_json::json!({
        "execution_id":"semantic-child", "pane_id":execution["pane_id"], "producer":"xcsh",
        "session_id":"0123abcd4567ef89", "generation":11, "native_capability":capability
    });
    let actions = send_request(
        &socket_path,
        &serde_json::json!({
            "id":"fixture-old-actions", "method":"agent.turn.action.get", "params":action_target
        })
        .to_string(),
    );
    assert_eq!(actions["result"]["actions"][0]["state"], "requested");
    let ack = send_request(
        &socket_path,
        &serde_json::json!({
            "id":"fixture-old-ack", "method":"agent.turn.action.ack", "params":{
                "execution_id":"semantic-child", "pane_id":execution["pane_id"], "producer":"xcsh",
                "session_id":"0123abcd4567ef89", "generation":11, "native_capability":capability,
                "action_id":"cancel", "action_revision":1, "state":"safe_point"
            }
        })
        .to_string(),
    );
    assert_eq!(ack["result"]["action"]["state"], "safe_point");
    let terminal = send_request(
        &socket_path,
        &serde_json::json!({
            "id":"fixture-old-terminal", "method":"agent.turn.report", "params":{
                "execution_id":"semantic-child", "pane_id":execution["pane_id"], "producer":"xcsh",
                "session_id":"0123abcd4567ef89", "turn_id":"fixture-turn", "generation":11,
                "event_revision":3, "state":"cancelled", "native_capability":capability
            }
        })
        .to_string(),
    );
    assert_eq!(terminal["result"]["turn"]["state"], "cancelled");
    let settled = send_request(
        &socket_path,
        &format!(
            r#"{{"id":"fixture-old-settled","method":"execution.get","params":{{"execution_id":"{backend_id}"}}}}"#
        ),
    );
    assert_eq!(settled["result"]["execution"]["state"], "cancelled");
    cleanup_spawned_herdr(herdr, base);
}

#[test]
fn visible_execution_owns_a_background_tab_input_exit_and_idempotency() {
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let socket_path = runtime_dir.join("herdr.sock");
    let herdr = spawn_herdr(&config_home, &runtime_dir, &socket_path);
    wait_for_socket(&socket_path, Duration::from_secs(5));
    assert!(run_cli(
        &socket_path,
        &[
            "workspace",
            "create",
            "--cwd",
            base.to_str().unwrap(),
            "--focus"
        ]
    )
    .status
    .success());

    let args = [
        "execution",
        "start",
        "visible-one",
        "--cwd",
        base.to_str().unwrap(),
        "--",
        "/bin/sh",
        "-c",
        "read value; printf 'got=%s\\n' \"$value\"; exit 7",
    ];
    let started = run_cli(&socket_path, &args);
    assert!(
        started.status.success(),
        "{}",
        String::from_utf8_lossy(&started.stderr)
    );
    let start: serde_json::Value = serde_json::from_slice(&started.stdout).unwrap();
    assert_eq!(start["result"]["execution"]["state"], "running");
    assert_eq!(start["result"]["admitted"], true);
    let pane = start["result"]["execution"]["pane_id"].as_str().unwrap();

    let duplicate = run_cli(&socket_path, &args);
    let duplicate: serde_json::Value = serde_json::from_slice(&duplicate.stdout).unwrap();
    assert_eq!(duplicate["result"]["admitted"], false);
    let tabs = run_cli(&socket_path, &["tab", "list"]);
    let tabs: serde_json::Value = serde_json::from_slice(&tabs.stdout).unwrap();
    assert_eq!(tabs["result"]["tabs"].as_array().unwrap().len(), 2);
    assert_eq!(tabs["result"]["tabs"][1]["focused"], false);

    assert!(
        run_cli(&socket_path, &["pane", "send-text", pane, "hello world"])
            .status
            .success()
    );
    assert!(run_cli(&socket_path, &["pane", "send-keys", pane, "enter"])
        .status
        .success());
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let get = run_cli(&socket_path, &["execution", "get", "visible-one"]);
        let get: serde_json::Value = serde_json::from_slice(&get.stdout).unwrap();
        if get["result"]["execution"]["state"] == "exited"
            && get["result"]["execution"]["output_complete"] == true
            && get["result"]["execution"]["stdout_tail"]
                .as_str()
                .unwrap()
                .contains("got=hello world")
        {
            assert_eq!(get["result"]["execution"]["exit_code"], 7);
            break;
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(20));
    }
    cleanup_spawned_herdr(herdr, base);
}

#[test]
fn visible_execution_cancel_reports_child_wait_signal() {
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let socket_path = runtime_dir.join("herdr.sock");
    let herdr = spawn_herdr(&config_home, &runtime_dir, &socket_path);
    wait_for_socket(&socket_path, Duration::from_secs(5));
    assert!(run_cli(
        &socket_path,
        &[
            "workspace",
            "create",
            "--cwd",
            base.to_str().unwrap(),
            "--focus"
        ]
    )
    .status
    .success());
    assert!(run_cli(
        &socket_path,
        &[
            "execution",
            "start",
            "cancel-one",
            "--cwd",
            base.to_str().unwrap(),
            "--",
            "/bin/sh",
            "-c",
            "sleep 30"
        ]
    )
    .status
    .success());
    assert!(
        run_cli(&socket_path, &["execution", "cancel", "cancel-one"])
            .status
            .success()
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let get = run_cli(&socket_path, &["execution", "get", "cancel-one"]);
        let get: serde_json::Value = serde_json::from_slice(&get.stdout).unwrap();
        if get["result"]["execution"]["state"] == "cancelled" {
            assert!(get["result"]["execution"]["signal_name"].is_string());
            break;
        }
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(20));
    }
    cleanup_spawned_herdr(herdr, base);
}

#[test]
fn semantic_turn_journal_enforces_provenance_order_idempotence_and_terminal_state() {
    let base = unique_test_dir();
    let config_home = base.join("config");
    let runtime_dir = base.join("runtime");
    let socket_path = runtime_dir.join("herdr.sock");
    let herdr = spawn_herdr(&config_home, &runtime_dir, &socket_path);
    wait_for_socket(&socket_path, Duration::from_secs(5));
    assert!(run_cli(
        &socket_path,
        &[
            "workspace",
            "create",
            "--cwd",
            base.to_str().unwrap(),
            "--focus"
        ]
    )
    .status
    .success());
    let started = run_cli(
        &socket_path,
        &[
            "execution",
            "start",
            "turn-execution",
            "--cwd",
            base.to_str().unwrap(),
            "--",
            "/bin/sh",
            "-c",
            "sleep 30",
        ],
    );
    let started: serde_json::Value = serde_json::from_slice(&started.stdout).unwrap();
    let pane = started["result"]["execution"]["pane_id"].as_str().unwrap();

    let report = |id: &str, event_revision: u64, state: &str, result: Option<&str>| {
        send_request(
            &socket_path,
            &serde_json::json!({
                "id": id,
                "method": "agent.turn.report",
                "params": {
                    "execution_id": "turn-execution",
                    "pane_id": pane,
                    "producer": "xcsh",
                    "session_id": "session-1",
                    "turn_id": "turn-1",
                    "generation": 0,
                    "event_revision": event_revision,
                    "state": state,
                    "result": result,
                    "result_digest": result.map(|_| "dcf69326b6e243c1ff2e85dc9cb39dd2f3edefb314f7f677a6bc4182f8d7a240"),
                }
            })
            .to_string(),
        )
    };

    let first = report("turn-1", 1, "starting", None);
    assert_eq!(first["result"]["admitted"], true);
    let duplicate = report("turn-duplicate", 1, "starting", None);
    assert_eq!(duplicate["result"]["admitted"], false);
    let conflict = report("turn-conflict", 1, "working", None);
    assert_eq!(conflict["error"]["code"], "agent_turn_revision_conflict");
    let completed = report("turn-complete", 2, "completed", Some("bounded result"));
    assert_eq!(completed["result"]["turn"]["state"], "completed");
    let terminal_conflict = report("turn-after-terminal", 3, "failed", None);
    assert_eq!(
        terminal_conflict["error"]["code"],
        "agent_turn_terminal_conflict"
    );
    let list = send_request(
        &socket_path,
        r#"{"id":"turn-list","method":"agent.turn.list","params":{"since_revision":0}}"#,
    );
    assert_eq!(list["result"]["turns"].as_array().unwrap().len(), 2);
    let get = send_request(
        &socket_path,
        r#"{"id":"turn-get","method":"agent.turn.get","params":{"producer":"xcsh","session_id":"session-1","turn_id":"turn-1","generation":0}}"#,
    );
    assert_eq!(get["result"]["turn"]["state"], "completed");
    assert_eq!(get["result"]["turn"]["event_revision"], 2);
    let wait = send_request(
        &socket_path,
        r#"{"id":"turn-wait","method":"agent.turn.wait","params":{"after_revision":1,"timeout_ms":100}}"#,
    );
    let waited = wait["result"]["turns"].as_array().unwrap();
    assert_eq!(waited.len(), 1);
    assert_eq!(waited[0]["state"], "completed");
    assert_eq!(waited[0]["revision"], 2);

    let waiting = serde_json::json!({
        "id": "turn-2-start",
        "method": "agent.turn.report",
        "params": {
            "execution_id": "turn-execution", "pane_id": pane, "producer": "xcsh",
            "session_id": "session-1", "turn_id": "turn-2", "generation": 1,
            "event_revision": 1, "state": "starting"
        }
    });
    assert_eq!(
        send_request(&socket_path, &waiting.to_string())["result"]["admitted"],
        true
    );
    let waiting = serde_json::json!({
        "id": "turn-2-waiting",
        "method": "agent.turn.report",
        "params": {
            "execution_id": "turn-execution", "pane_id": pane, "producer": "xcsh",
            "session_id": "session-1", "turn_id": "turn-2", "generation": 1,
            "event_revision": 2, "state": "waiting_input", "reason": "approval required"
        }
    });
    assert_eq!(
        send_request(&socket_path, &waiting.to_string())["result"]["turn"]["state"],
        "waiting_input"
    );
    let cancelled = serde_json::json!({
        "id": "turn-2-cancelled",
        "method": "agent.turn.report",
        "params": {
            "execution_id": "turn-execution", "pane_id": pane, "producer": "xcsh",
            "session_id": "session-1", "turn_id": "turn-2", "generation": 1,
            "event_revision": 3, "state": "cancelled", "reason": "cancel acknowledged"
        }
    });
    assert_eq!(
        send_request(&socket_path, &cancelled.to_string())["result"]["turn"]["state"],
        "cancelled"
    );

    let wrong_pane = serde_json::json!({
        "id": "wrong-pane",
        "method": "agent.turn.report",
        "params": {
            "execution_id": "turn-execution", "pane_id": "w999:p999", "producer": "xcsh",
            "session_id": "session-2", "turn_id": "turn-2", "event_revision": 1,
            "state": "starting"
        }
    });
    let wrong_pane = send_request(&socket_path, &wrong_pane.to_string());
    assert_eq!(
        wrong_pane["error"]["code"],
        "agent_turn_provenance_mismatch"
    );

    let _ = run_cli(&socket_path, &["execution", "cancel", "turn-execution"]);
    cleanup_spawned_herdr(herdr, base);
}
