use super::harness::*;

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
                "session_id": "xcsh-session", "text": text, "cwd": base,
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

#[test]
fn native_xcsh_child_accepts_semantic_report_only_with_durable_binding() {
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
    let resumed = send_request(
        &socket_path,
        &serde_json::json!({
            "id": "native-child", "method": "execution.resume", "params": {
                "execution_id": "semantic-child", "generation": 11,
                "session_id": "fixture-session", "text": "fixture", "cwd": base,
            }
        })
        .to_string(),
    );
    let execution = &resumed["result"]["execution"];
    assert_ne!(execution["execution_id"], "semantic-child");
    let pane = execution["pane_id"].as_str().expect("native child pane");
    let report = |id: &str, producer: &str, session_id: &str, generation: u64| {
        serde_json::json!({
            "id": id, "method": "agent.turn.report", "params": {
                "execution_id": "semantic-child", "pane_id": pane, "producer": producer,
                "session_id": session_id, "turn_id": "real-child-turn", "generation": generation,
                "event_revision": 1, "state": "starting"
            }
        })
    };
    let accepted = send_request(
        &socket_path,
        &report("native-report", "xcsh", "fixture-session", 11).to_string(),
    );
    assert_eq!(accepted["result"]["admitted"], true);
    let bad_session = send_request(
        &socket_path,
        &report("native-bad-session", "xcsh", "foreign", 11).to_string(),
    );
    assert_eq!(
        bad_session["error"]["code"],
        "agent_turn_native_binding_mismatch"
    );
    let bad_producer = send_request(
        &socket_path,
        &report("native-bad-producer", "foreign", "fixture-session", 11).to_string(),
    );
    assert_eq!(
        bad_producer["error"]["code"],
        "agent_turn_native_binding_mismatch"
    );
    let bad_generation = send_request(
        &socket_path,
        &report("native-bad-generation", "xcsh", "fixture-session", 12).to_string(),
    );
    assert_eq!(
        bad_generation["error"]["code"],
        "agent_turn_execution_not_found"
    );
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
