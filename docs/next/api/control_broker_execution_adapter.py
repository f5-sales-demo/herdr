"""Review fixture for replacing Control Broker's command wrapper with Herdr execution.*.

This is intentionally not imported by the live broker.  The broker owns task
policy and persistence; Herdr owns only the visible PTY process lifecycle.
"""

from __future__ import annotations

from typing import Any, Protocol


class HerdrRPC(Protocol):
    async def request(self, method: str, params: dict[str, Any], timeout: float = 65) -> Any: ...


async def admit_command(
    herdr: HerdrRPC,
    *,
    execution_id: str,
    workspace_id: str,
    cwd: str,
    label: str,
    shell: str,
    command: str,
) -> dict[str, Any]:
    """Idempotently create the broker-visible no-focus command tab."""
    result = await herdr.request(
        "execution.start",
        {
            "execution_id": execution_id,
            "workspace_id": workspace_id,
            "cwd": cwd,
            "label": label,
            "mode": "shell",
            "shell": shell,
            "text": command,
        },
    )
    execution = result["execution"]
    if not execution.get("pane_id") or not execution.get("tab_id"):
        raise RuntimeError("Herdr admitted command without visible pane identity")
    return execution


async def changes_after(herdr: HerdrRPC, revision: int) -> list[dict[str, Any]]:
    """Reconnect/replay cursor; consumers de-duplicate by id and revision."""
    result = await herdr.request("execution.list", {"since_revision": revision})
    return result["executions"]


def process_outcome(execution: dict[str, Any]) -> tuple[str, str]:
    """Map only process truth; never infer Codex/XCSH semantic task success."""
    state = execution["state"]
    if state in {"starting", "running"}:
        return "working", "native PTY process is running"
    if state == "cancelled":
        return "cancelled", f"native PTY process cancelled ({execution.get('signal_name', 'status observed')})"
    if state == "lost":
        return "failed", f"native PTY lifecycle evidence lost: {execution.get('evidence_gap', 'unknown gap')}"
    code = execution.get("exit_code")
    signal = execution.get("signal_name")
    if signal:
        return "failed", f"native PTY process exited by signal {signal}"
    if code == 0:
        return "completed", "native PTY process exited with status 0"
    return "failed", f"native PTY process exited with status {code}"


# Removal contract in control_broker.py:
# 1. Persist the broker task/execution_id before admit_command.
# 2. Replace tab.create + pane.send_text(wrapper argv) + pane.send_keys(enter)
#    with one execution.start call and store its returned tab_id/pane_id/revision.
# 3. Reconcile at startup and after disconnect with execution.list; optionally
#    long-poll execution.wait.  Apply only revisions newer than the task row.
# 4. Route interactive XCSH input through the returned pane_id.  Track XCSH's
#    semantic job/result separately from this process outcome.
# 5. Delete command_event handling, wrapper spool recovery, and terminal marker
#    scraping only after isolated broker UAT proves duplicate/replay/cancel.
