import hashlib
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

from xcsh_installed_uat import native_case_lifecycle, CATALOG, PreflightError, execute_case, preflight, validate_journal, validate_xcsh_archive_provenance


ROOT = Path(__file__).parent


def manifest():
    digest = "a" * 64
    value = "xcsh-uat-test-value"
    return {"schema_version": 1, "isolation": "dedicated_disposable_runtime",
            "artifacts": {name: {"release_version": "1.0.0", "artifact_uri": f"file:///isolated/{name}", "sha256": digest}
                          for name in ("xcsh", "herdr", "manager")},
            "runtime": {"broker_socket": "/isolated/control.sock", "herdr_socket": "/isolated/herdr.sock", "workspace_id": "w-isolated", "xcsh_session_dir":"/isolated/sessions", "xcsh_model":"fixture/model", "fixture":{"path":"/isolated/fixture.txt","value":value,"sha256":hashlib.sha256((value+"\n").encode()).hexdigest()}, "xcsh_archive_path":"/isolated/xcsh.tar.gz", "xcsh_archive_member":"xcsh", "xcsh_archive_member_sha256":"a" * 64, "xcsh_executable": "/isolated/xcsh", "xcsh_executable_sha256": "a" * 64},
            "required_capabilities": ["native_xcsh_admit", "agent_turn_journal"]}


class InstalledPromptUatTests(unittest.TestCase):
    def test_generation_oracle_retains_binding_and_distinct_progress_events(self):
        case = next(case for case in json.loads(CATALOG.read_text())["scenarios"] if case["id"] == "continuation")
        states = [(0, "starting"), (0, "working"), (0, "waiting_input"), (0, "working"),
                  (1, "starting"), (1, "working"), (0, "cancelled"), (1, "working"), (1, "completed")]
        event_revisions = {0: 0, 1: 0}
        records = []
        result = manifest()["runtime"]["fixture"]["value"]
        for revision, (generation, state) in enumerate(states, 1):
            event_revisions[generation] += 1
            report = {"execution_id": "task-1", "producer": "xcsh", "session_id": "0123abcd4567ef89",
                "pane_id": f"pane-{generation}", "turn_id": f"turn-{generation}", "generation": generation,
                "event_revision": event_revisions[generation], "state": state}
            if state == "cancelled": report["reason"] = "superseded"
            if state == "completed": report |= {"result": result, "result_digest": hashlib.sha256(result.encode()).hexdigest()}
            records.append({"revision": revision, "report": report})
        task = {"id": "task-1", "pane_id": "pane-1", "agent_session_id": "0123abcd4567ef89", "run_generation": 1}
        bindings = {generation: {"execution_id": "task-1", "pane_id": f"pane-{generation}", "session_id": "0123abcd4567ef89"} for generation in (0, 1)}
        validate_journal(case, records, task, None, fixture=manifest()["runtime"]["fixture"], bindings=bindings)
        with self.assertRaisesRegex(AssertionError, "admission binding"):
            validate_journal(case, records, task, None, fixture=manifest()["runtime"]["fixture"])
        reason = records[6]["report"].pop("reason")
        with self.assertRaisesRegex(AssertionError, "terminal semantic reason"):
            validate_journal(case, records, task, None, fixture=manifest()["runtime"]["fixture"], bindings=bindings)
        records[6]["report"]["reason"] = reason
        early = [records[0], records[1], records[4], records[2], records[3], *records[5:]]
        early = [dict(record, revision=index) for index, record in enumerate(early, 1)]
        with self.assertRaisesRegex(AssertionError, "continuation boundary"):
            validate_journal(case, early, task, None, fixture=manifest()["runtime"]["fixture"], bindings=bindings)
        records[0]["report"]["pane_id"] = "pane-1"
        with self.assertRaisesRegex(AssertionError, "provenance"):
            validate_journal(case, records, task, None, fixture=manifest()["runtime"]["fixture"], bindings=bindings)
        records[0]["report"]["pane_id"] = "pane-0"
        records[7]["report"]["event_revision"] = records[5]["report"]["event_revision"]
        with self.assertRaisesRegex(AssertionError, "logical producer event"):
            validate_journal(case, records, task, None, fixture=manifest()["runtime"]["fixture"], bindings=bindings)

    def test_execute_case_admits_exact_catalog_lifecycle_modes(self):
        from unittest.mock import patch
        import xcsh_installed_uat as driver
        class Controller:
            def create_xcsh_session(self, *_):
                return {"session_id": "0123abcd4567ef89", "session_dir": "/synthetic",
                    "session_path": "/synthetic/session.jsonl", "session_header": {"id": "0123abcd4567ef89", "sha256": "a"*64},
                    "xcsh_executable": "/isolated/xcsh", "xcsh_executable_sha256": "a"*64}
        catalog = json.loads(CATALOG.read_text())["scenarios"]
        for case_id, mode, interactive in [("waiting_input", "await_user_v1", True),
                ("continuation", "await_user_v1", True), ("generation_supersession", "await_user_v1", True),
                ("failure", "local_operation_failure_v1", False), ("success", "managed_turn_v1", True)]:
            case = next(case for case in catalog if case["id"] == case_id)
            with tempfile.TemporaryDirectory() as raw:
                with patch.object(driver, "unix_request", side_effect=RuntimeError("captured admission")) as request:
                    with self.assertRaisesRegex(RuntimeError, "captured admission"):
                        execute_case(self.prepared_manifest(Path(raw)), case, run_id="selection", controller=Controller())
                    args = request.call_args.args
                    self.assertEqual(args[1], "native_xcsh_admit")
                    self.assertEqual(args[2]["native_launch"]["lifecycle_mode"], mode)
                    self.assertEqual(args[2]["native_launch"]["interactive"], interactive)

    def test_native_case_lifecycle_selection(self):
        for case in ("waiting_input", "continuation", "generation_supersession"):
            self.assertEqual(native_case_lifecycle(case), ("await_user_v1", True))
        self.assertEqual(native_case_lifecycle("failure"), ("local_operation_failure_v1", False))
        self.assertEqual(native_case_lifecycle("success"), ("managed_turn_v1", True))

    def test_catalog_requires_exact_generation_trace_for_every_case(self):
        catalog = json.loads(CATALOG.read_text())
        for case in catalog["scenarios"]:
            self.assertEqual(len(case["expected_generations"]), len(case["expected_states"]))
    def prepared_manifest(self, root: Path):
        candidate = manifest()
        value = candidate["runtime"]["fixture"]["value"]
        path = root / "fixture.txt"
        path.write_text(value + "\n")
        candidate["runtime"]["fixture"]["path"] = str(path)
        return candidate

    def test_preflight_binds_all_artifacts_and_all_required_prompt_boundaries(self):
        receipt = preflight(manifest(), json.loads(CATALOG.read_text()))
        self.assertEqual(receipt["preflight"], "passed")
        self.assertEqual(receipt["live_execution"], "not_started")
        self.assertEqual(len(receipt["scenario_ids"]), 9)
        self.assertIn("native_xcsh_admit", receipt["required_capabilities"])
        self.assertEqual(json.loads(CATALOG.read_text())["native_resume_contract"]["herdr_protocol_minimum"], 26)

    def test_preflight_rejects_nonisolated_or_incomplete_catalog(self):
        invalid = manifest(); invalid["isolation"] = "shared_runtime"
        with self.assertRaisesRegex(PreflightError, "dedicated disposable"):
            preflight(invalid, json.loads(CATALOG.read_text()))
        invalid = manifest(); invalid["required_capabilities"] = ["agent_turn_journal"]
        with self.assertRaisesRegex(PreflightError, "native_xcsh_admit"):
            preflight(invalid, json.loads(CATALOG.read_text()))
        invalid = manifest(); invalid["runtime"]["xcsh_executable_sha256"] = "b" * 64
        with self.assertRaisesRegex(PreflightError, "archive path"):
            preflight(invalid, json.loads(CATALOG.read_text()))
        invalid_catalog = json.loads(CATALOG.read_text())
        invalid_catalog["native_resume_contract"]["herdr_protocol_minimum"] = 20
        with self.assertRaisesRegex(PreflightError, "protocol-26"):
            preflight(manifest(), invalid_catalog)

    def test_fixture_must_bind_a_random_expected_value_and_exact_file(self):
        value = "xcsh-uat-random-fixture"
        with tempfile.TemporaryDirectory() as raw:
            path = Path(raw) / "fixture.txt"
            path.write_text(value + "\n")
            candidate = manifest()
            candidate["runtime"]["fixture"] = {"path": str(path), "value": value,
                "sha256": hashlib.sha256((value + "\n").encode()).hexdigest()}
            self.assertEqual(preflight(candidate, json.loads(CATALOG.read_text()))["fixture_sha256"], candidate["runtime"]["fixture"]["sha256"])
            path.write_text("different\n")
            with self.assertRaisesRegex(PreflightError, "content"):
                from xcsh_installed_uat import validate_fixture
                validate_fixture(candidate["runtime"], verify_file=True)

    def test_archive_member_provenance_rejects_wrong_member_and_changed_archive(self):
        with tempfile.TemporaryDirectory() as raw:
            root=Path(raw); payload=root/'payload'; payload.write_bytes(b'xcsh fixture')
            archive=root/'xcsh.tar.gz'
            with tarfile.open(archive,'w:gz') as out: out.add(payload,arcname='xcsh')
            member=hashlib.sha256(payload.read_bytes()).hexdigest(); asset=hashlib.sha256(archive.read_bytes()).hexdigest()
            runtime={'xcsh_archive_path':str(archive),'xcsh_archive_member':'xcsh','xcsh_archive_member_sha256':member,'xcsh_executable_sha256':member}
            validate_xcsh_archive_provenance(runtime,{'sha256':asset},verify_archive=True)
            wrong=dict(runtime); wrong['xcsh_archive_member']='other'
            with self.assertRaisesRegex(PreflightError,'exact safe regular'):
                validate_xcsh_archive_provenance(wrong,{'sha256':asset},verify_archive=True)
            with self.assertRaisesRegex(PreflightError,'published asset digest'):
                validate_xcsh_archive_provenance(runtime,{'sha256':'0'*64},verify_archive=True)

    def test_real_v21190_archive_member_matches_published_asset(self):
        archive=Path('/data/robin-GIT/xcsh-codex-login/xcsh/.local/native-lifecycle-artifacts/v21.19.0/xcsh-linux-x64.tar.gz')
        if not archive.is_file(): self.skipTest('retained immutable v21.19.0 archive absent')
        validate_xcsh_archive_provenance({'xcsh_archive_path':str(archive),'xcsh_archive_member':'xcsh','xcsh_archive_member_sha256':'8b2502a07ae1bafa66ee71e4003c61ccc46293143950acf8a27182c04102df04','xcsh_executable_sha256':'8b2502a07ae1bafa66ee71e4003c61ccc46293143950acf8a27182c04102df04'}, {'sha256':'34a413a4839286607334b58e263626f60a59c5a693add4a199494ff12e89a467'}, verify_archive=True)

    def test_oracle_requires_real_provenance_not_a_sentinel_substring(self):
        case = next(item for item in json.loads(CATALOG.read_text())["scenarios"] if item["id"] == "success")
        task = {"id": "task-1", "pane_id": "pane-1", "agent_session_id": "0123abcd4567ef89"}
        result = manifest()["runtime"]["fixture"]["value"]
        reports = [
            {"revision": 1, "report": {"execution_id": "task-1", "pane_id": "pane-1", "producer": "xcsh", "turn_id": "turn-1", "event_revision": 1, "session_id": "0123abcd4567ef89", "generation": 0, "state": "starting"}},
            {"revision": 2, "report": {"execution_id": "task-1", "pane_id": "pane-1", "producer": "xcsh", "turn_id": "turn-1", "event_revision": 2, "session_id": "0123abcd4567ef89", "generation": 0, "state": "working"}},
            {"revision": 3, "report": {"execution_id": "task-1", "pane_id": "pane-1", "producer": "xcsh", "turn_id": "turn-1", "event_revision": 3, "session_id": "0123abcd4567ef89", "generation": 0, "state": "completed", "result": result, "result_digest": hashlib.sha256(result.encode()).hexdigest()}},
        ]
        validate_journal(case, reports, task, {"task_id": "task-1", "stage": "consumed"}, fixture=manifest()["runtime"]["fixture"])
        reports[-1]["report"]["result"] = "arbitrary completed text"
        reports[-1]["report"]["result_digest"] = hashlib.sha256(b"arbitrary completed text").hexdigest()
        with self.assertRaisesRegex(AssertionError, "prepared fixture"):
            validate_journal(case, reports, task, {"task_id": "task-1", "stage": "consumed"}, fixture=manifest()["runtime"]["fixture"])
        reports[-1]["report"]["result"] = result
        reports[-1]["report"]["result_digest"] = hashlib.sha256(result.encode()).hexdigest()
        reports[-1]["synthetic"] = True
        with self.assertRaisesRegex(AssertionError, "synthetic journal"):
            validate_journal(case, reports, task, {"task_id": "task-1", "stage": "consumed"}, fixture=manifest()["runtime"]["fixture"])
        reports[-1].pop("synthetic")
        reports[-1]["report"]["pane_id"] = "wrong-pane"
        with self.assertRaisesRegex(AssertionError, "provenance"):
            validate_journal(case, reports, task, {"task_id": "task-1", "stage": "consumed"}, fixture=manifest()["runtime"]["fixture"])

    def test_controlled_transport_driver_uses_adapter_and_observed_consumption(self):
        self._check_controlled_transport_driver(True)

    def test_controlled_transport_driver_accepts_durable_already_consumed_progress(self):
        self._check_controlled_transport_driver(False)

    def _check_controlled_transport_driver(self, fresh_applied):
        import xcsh_installed_uat as driver
        case = next(item for item in json.loads(CATALOG.read_text())["scenarios"] if item["id"] == "success")
        result = manifest()["runtime"]["fixture"]["value"]
        records = [
            {"revision": 1, "report": {"execution_id": "task-1", "pane_id": "pane-1", "producer": "xcsh", "session_id": "0123abcd4567ef89", "turn_id": "turn-1", "generation": 0, "event_revision": 1, "state": "starting"}},
            {"revision": 2, "report": {"execution_id": "task-1", "pane_id": "pane-1", "producer": "xcsh", "session_id": "0123abcd4567ef89", "turn_id": "turn-1", "generation": 0, "event_revision": 2, "state": "working"}},
            {"revision": 3, "report": {"execution_id": "task-1", "pane_id": "pane-1", "producer": "xcsh", "session_id": "0123abcd4567ef89", "turn_id": "turn-1", "generation": 0, "event_revision": 3, "state": "completed", "result": result, "result_digest": hashlib.sha256(result.encode()).hexdigest()}},
        ]
        calls = []
        def fake_broker(path, method, params):
            calls.append((method, params))
            if method == "native_xcsh_admit": return {"id": "task-1", "state": "starting", "workspace_id":"w-isolated","tab_id":"tab-1", "pane_id": "pane-1", "agent_session_id": None, "native_executable": {"canonical_path": params["native_launch"]["xcsh_executable"], "sha256": params["xcsh_executable_sha256"]}, "native_launch": params["native_launch"]}
            if method == "consume_native_turns": return {"applied": [{"task_id": "task-1", "state": "completed"}] if fresh_applied else []}
            if method == "status": return {"tasks": [{"id": "task-1", "pane_id": "pane-1", "agent_session_id": "0123abcd4567ef89", "run_generation": 0, "native_turn_id": "turn-1", "native_event_revision": 3}], "pending_completions": [{"task_id": "task-1", "delivery_state": "consumed"}]}
            raise AssertionError(method)
        def fake_herdr(path, method, params):
            self.assertIn(method, driver.ALLOWED_HERDR_METHODS)
            if method == "agent.turn.list": return {"turns": records}
            raise AssertionError(method)
        original_broker, original_herdr = driver.unix_request, driver.herdr_request
        driver.unix_request, driver.herdr_request = fake_broker, fake_herdr
        with tempfile.TemporaryDirectory() as raw:
            try:
                class SyntheticController:
                    token = "synthetic"
                    def create_xcsh_session(self, *_): return {"session_id": "0123abcd4567ef89", "session_dir":"/synthetic", "session_path": "/synthetic/session.jsonl", "session_header":{"id":"0123abcd4567ef89","sha256":"a" * 64}, "xcsh_executable": "/isolated/xcsh", "xcsh_executable_sha256": "a" * 64}
                    def register_execution(self, task): self.task = task
                receipt = execute_case(self.prepared_manifest(Path(raw)), case, run_id="stable", controller=SyntheticController())
            finally:
                driver.unix_request, driver.herdr_request = original_broker, original_herdr
        self.assertTrue(receipt["pass"])
        self.assertFalse(receipt["accepted"])
        self.assertEqual(calls[0][0], "native_xcsh_admit")
        self.assertEqual(calls[0][1]["native_launch"]["xcsh_executable"], "/isolated/xcsh")
        self.assertEqual(calls[0][1]["xcsh_executable_sha256"], "a" * 64)
        self.assertTrue(calls[0][1]["native_launch"]["interactive"])
        self.assertFalse(any(method == "ack_completion" for method, _ in calls))

    def test_restart_boundary_requires_and_records_authenticated_controller_action(self):
        import xcsh_installed_uat as driver
        case = next(item for item in json.loads(CATALOG.read_text())["scenarios"] if item["id"] == "restart_loss")
        with tempfile.TemporaryDirectory() as raw:
            with self.assertRaisesRegex(PreflightError, "authenticated dedicated-runtime controller"):
                execute_case(self.prepared_manifest(Path(raw)), case, run_id="stable")
        reports = [
            {"revision": 1, "report": {"execution_id":"task-1","pane_id":"pane-1","producer":"xcsh","session_id":"0123abcd4567ef89","turn_id":"turn-1","generation":0,"event_revision":1,"state":"starting"}},
            {"revision": 2, "report": {"execution_id":"task-1","pane_id":"pane-1","producer":"xcsh","session_id":"0123abcd4567ef89","turn_id":"turn-1","generation":0,"event_revision":2,"state":"working"}},
            {"revision": 3, "report": {"execution_id":"task-1","pane_id":"pane-1","producer":"xcsh","session_id":"0123abcd4567ef89","turn_id":"turn-1","generation":0,"event_revision":3,"state":"interrupted","reason":"owned runtime restarted"}},
        ]
        class Controller:
            token="token"
            calls=[]
            def create_xcsh_session(self, *_): return {"session_id": "0123abcd4567ef89", "session_dir":"/synthetic", "session_path": "/synthetic/session.jsonl", "session_header":{"id":"0123abcd4567ef89","sha256":"a" * 64}, "xcsh_executable": "/isolated/xcsh", "xcsh_executable_sha256": "a" * 64}
            def register_execution(self, task): self.registered=task
            def real_action(self, kind, pane, key, token, external=None):
                self.calls.append((kind,pane,key,token)); return {"kind":kind,"execution_id":"task-1","workspace_id":"w-isolated","tab_id":"tab-1","pane_id":pane,"session_id":"owned","effect":{"action":"producer_process_cutpoint","producer_session_id":"0123abcd4567ef89","producer_pid":123,"execution_id":"task-1","before_revision":2,"process_exited":True,"stop_exit":0,"after_socket":"/isolated/herdr.sock"}}
        controller=Controller(); ticks=[0]
        def fake_broker(path, method, params):
            if method == "native_xcsh_admit": return {"id":"task-1","state":"starting","workspace_id":"w-isolated","tab_id":"tab-1","pane_id":"pane-1","agent_session_id":"0123abcd4567ef89", "native_executable": {"canonical_path": params["native_launch"]["xcsh_executable"], "sha256": params["xcsh_executable_sha256"]}, "native_launch":params["native_launch"]}
            if method == "consume_native_turns": return {"applied":[{"task_id":"task-1","state":reports[max(0,ticks[0]-1)]["report"]["state"]}]}
            if method == "status": return {"tasks":[{"id":"task-1","pane_id":"pane-1","agent_session_id":"0123abcd4567ef89"}],"pending_completions":[{"task_id":"task-1","delivery_state":"consumed"}]}
            raise AssertionError(method)
        def fake_herdr(path, method, params):
            if method == "agent.turn.list":
                ticks[0]+=1
                visible=reports[:ticks[0]]
                return {"turns":[r for r in visible if r["revision"] > params.get("since_revision", 0)]}
            if method == "agent.turn.wait": return {"turns": []}
            if method == "ping": return {"protocol":26,"capabilities":{"agent_turn_journal":True}}
            raise AssertionError(method)
        original_broker,original_herdr=driver.unix_request,driver.herdr_request
        driver.unix_request,driver.herdr_request=fake_broker,fake_herdr
        with tempfile.TemporaryDirectory() as raw:
            try: receipt=execute_case(self.prepared_manifest(Path(raw)),case,run_id="stable",controller=controller)
            finally: driver.unix_request,driver.herdr_request=original_broker,original_herdr
        self.assertTrue(receipt["pass"])
        self.assertEqual(controller.calls[0][0],"restart_loss")
        self.assertEqual(receipt["controller_receipt"]["effect"]["stop_exit"],0)
