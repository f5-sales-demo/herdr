"""Opt-in smoke test of Herdr context in real Codex command subprocesses.

Run from a disposable Herdr pane with an authenticated Codex CLI. Run it again
after moving that pane, or after reconnecting to its server. It prints only
session and pane IDs, never the socket, capability, pairing, or lease values.
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import tempfile
from pathlib import Path


KEYS = ("HERDR_ENV", "HERDR_WORKSPACE_ID", "HERDR_TAB_ID", "HERDR_PANE_ID")
PROBE_CODE = """import json, os, subprocess
raw = {key: os.environ.get(key) for key in ("HERDR_ENV", "HERDR_WORKSPACE_ID", "HERDR_TAB_ID", "HERDR_PANE_ID")}
reply = subprocess.check_output(["herdr", "pane", "current", "--current"], text=True)
pane = json.loads(reply)["result"]["pane"]
print(json.dumps({"raw": raw, "current": {key: pane[key] for key in ("workspace_id", "tab_id", "pane_id")}}))
"""
PROMPT = (
    "Run exactly one shell tool command, then reply done. Do not inspect other "
    "environment variables. Command: python3 -c " + shlex.quote(PROBE_CODE)
)


def current_pane() -> dict[str, str]:
    result = subprocess.run(
        ["herdr", "pane", "current", "--current"],
        check=True,
        capture_output=True,
        text=True,
    )
    pane = json.loads(result.stdout)["result"]["pane"]
    return {key: pane[key] for key in ("workspace_id", "tab_id", "pane_id")}


def run_codex(args: list[str], cwd: Path) -> tuple[str, dict[str, object]]:
    result = subprocess.run(args, cwd=cwd, capture_output=True, text=True)
    if result.returncode:
        raise RuntimeError(f"Codex exited with status {result.returncode}; inspect local CLI diagnostics")
    events = [json.loads(line) for line in result.stdout.splitlines()]
    thread_ids = [event["thread_id"] for event in events if event.get("type") == "thread.started"]
    outputs = [
        event["item"].get("aggregated_output", "")
        for event in events
        if event.get("type") == "item.completed"
        and event.get("item", {}).get("type") == "command_execution"
    ]
    if len(thread_ids) != 1 or len(outputs) != 1:
        raise RuntimeError("Codex did not record exactly one shell tool subprocess")
    try:
        return thread_ids[0], json.loads(outputs[0].strip())
    except json.JSONDecodeError as exc:
        raise RuntimeError("shell tool output was not the expected redacted JSON") from exc


def check_probe(label: str, output: dict[str, object], expected: dict[str, str]) -> None:
    raw = output["raw"]
    assert isinstance(raw, dict)
    if raw.get("HERDR_ENV") != "1":
        raise RuntimeError(f"{label}: Codex shell tool lost HERDR_ENV")
    if output["current"] != expected:
        raise RuntimeError(f"{label}: Codex shell tool resolved a different live pane")
    print(
        json.dumps(
            {
                "phase": label,
                "raw": {key: raw.get(key) for key in KEYS},
                "resolved": expected,
                "raw_ids_current": (
                    raw.get("HERDR_WORKSPACE_ID") == expected["workspace_id"]
                    and raw.get("HERDR_TAB_ID") == expected["tab_id"]
                    and raw.get("HERDR_PANE_ID") == expected["pane_id"]
                ),
            },
            sort_keys=True,
        )
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", help="Codex model override; defaults to local configuration")
    options = parser.parse_args()
    if os.environ.get("HERDR_ENV") != "1":
        raise SystemExit("missing caller binding: run this test inside a Herdr-managed pane")
    expected = current_pane()
    model = ["--model", options.model] if options.model else []
    with tempfile.TemporaryDirectory(prefix="herdr-codex-context-") as temp:
        cwd = Path(temp)
        base = ["codex", "exec", "--json", "--skip-git-repo-check", "-c", "features.hooks=false", *model]
        thread_id, direct = run_codex([*base, "-C", temp, PROMPT], cwd)
        check_probe("launch", direct, expected)
        resumed_id, resumed = run_codex(
            ["codex", "exec", "resume", "--json", "--skip-git-repo-check", "-c", "features.hooks=false", *model, thread_id, PROMPT],
            cwd,
        )
        if resumed_id != thread_id:
            raise RuntimeError("Codex resumed a different session")
        check_probe("resume", resumed, expected)
    print(json.dumps({"codex_session_id": thread_id, "pane_id": expected["pane_id"]}))


if __name__ == "__main__":
    main()
