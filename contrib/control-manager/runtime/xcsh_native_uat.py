#!/usr/bin/env python3
"""Deterministic synthetic protocol-20 UAT fixtures for the XCSH boundary.

This runner intentionally exercises only the manager's persisted semantic-turn
consumer.  It creates no PTY, downloads no artifact, and cannot produce
installed-runtime or live-UAT evidence.  Its receipt consequently labels every
passing case ``synthetic_fixture`` rather than an end-to-end acceptance result.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path
from typing import Any

from control_broker import StateDB
from control_portable import package_root


RUNNER_VERSION = 1
CATALOG = package_root() / "xcsh-native-uat" / "scenarios-v1.json"


def _report(case: dict[str, Any], event: dict[str, Any], task_id: str, pane: str,
            session: str, current_turn: str) -> tuple[dict[str, Any], str]:
    state = event["state"]
    report: dict[str, Any] = {
        "execution_id": task_id, "pane_id": pane, "producer": "xcsh",
        "session_id": session, "turn_id": event.get("turn_id", current_turn),
        "generation": event["generation"], "event_revision": event["revision"],
        "state": state,
    }
    if state == "completed":
        result = event.get("result", "synthetic semantic success")
        report["result"] = result
        report["result_digest"] = hashlib.sha256(result.encode()).hexdigest()
    elif state != "starting" and state != "working":
        report["reason"] = event.get("reason", f"synthetic {state}")
    return report, report["turn_id"]


def run_case(case: dict[str, Any]) -> dict[str, Any]:
    """Run one durable journal trace and return a non-promoting fixture receipt."""
    with tempfile.TemporaryDirectory(prefix="xcsh-native-uat-") as raw:
        db = StateDB(Path(raw) / "state.sqlite3")
        task_id = f"fixture-{case['id']}"
        pane, session, turn = f"pane-{case['id']}", f"session-{case['id']}", "turn-0"
        task = db.add_task({"id": task_id, "target": task_id, "cwd": raw,
                            "prompt": "synthetic protocol-20 fixture", "summary": "queued",
                            "parent_id": None, "priority": "routine", "work_kind": "xcsh"})
        db.update(task["id"], state="starting", pane_id=pane, agent_session_id=session,
                  native_turn_id=turn)
        assertions: list[str] = []
        try:
            for event in case["events"]:
                if event.get("continue_to_generation") is not None:
                    turn = event.get("turn_id", f"turn-{event['continue_to_generation']}")
                    db.update(task_id, state="working", run_generation=event["continue_to_generation"],
                              native_turn_id=turn, event="synthetic_continuation")
                    assertions.append("continuation_generation_persisted")
                if event.get("replay_of"):
                    report, _ = _report(case, event, task_id, pane, session, turn)
                    changed = db.apply_native_turn({"revision": event["revision"], "report": report})
                    if changed is not None:
                        raise AssertionError("duplicate journal event changed task state")
                    assertions.append("duplicate_replay_ignored")
                    continue
                report, turn = _report(case, event, task_id, pane, session, turn)
                try:
                    changed = db.apply_native_turn({"revision": event["revision"], "report": report})
                except ValueError:
                    if not event.get("expect_rejected"):
                        raise
                    assertions.append("stale_or_conflicting_generation_rejected")
                    continue
                if event.get("expect_rejected"):
                    raise AssertionError("expected stale/conflicting event to be rejected")
                if changed is None:
                    raise AssertionError("non-replay journal event was ignored")
            final = db.task(task_id)
            assert final is not None
            if final["state"] != case["expected_final_state"]:
                raise AssertionError(f"expected {case['expected_final_state']}, got {final['state']}")
            if case.get("requires_digest") and not final["output_excerpt"]:
                raise AssertionError("completed semantic result did not persist")
            return {"id": case["id"], "pass": True, "final_state": final["state"],
                    "assertions": assertions, "evidence_class": "synthetic_fixture"}
        except Exception as exc:
            return {"id": case["id"], "pass": False, "error": str(exc),
                    "evidence_class": "synthetic_fixture"}
        finally:
            db.close()


def run_catalog(path: Path = CATALOG) -> dict[str, Any]:
    catalog = json.loads(path.read_text())
    cases = [run_case(case) for case in catalog["scenarios"]]
    passed = all(case["pass"] for case in cases)
    return {
        "runner": "xcsh_native_uat", "runner_version": RUNNER_VERSION,
        "fixture_mode": "synthetic_process_and_journal", "catalog_version": catalog["catalog_version"],
        "pass": passed, "accepted": False,
        "limitations": [
            "No XCSH or Herdr artifact was downloaded or installed.",
            "No live process, PTY, service restart, release, or external deployment was used.",
            "A passing receipt is manager-boundary fixture evidence only, never feature acceptance.",
        ], "cases": cases,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--catalog", type=Path, default=CATALOG)
    parser.add_argument("--receipt", type=Path, help="optional JSON receipt path")
    args = parser.parse_args()
    receipt = run_catalog(args.catalog)
    text = json.dumps(receipt, indent=2, sort_keys=True) + "\n"
    if args.receipt:
        args.receipt.parent.mkdir(parents=True, exist_ok=True)
        args.receipt.write_text(text)
    print(text, end="")
    return 0 if receipt["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
