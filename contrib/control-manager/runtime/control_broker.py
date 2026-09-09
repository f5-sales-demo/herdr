#!/usr/bin/env python3
"""Persistent Control Manager dispatch and attention broker.

The broker stores bounded task metadata in SQLite, controls Herdr through its
newline-delimited JSON socket API, and exposes a small owner-only Unix socket
for controlctl and control-report.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import hmac
import json
import logging
import os
import re
import shlex
import signal
import shutil
import socket
import sqlite3
import stat
import struct
import subprocess
import time
import uuid
from pathlib import Path
from typing import Any

from control_portable import (
    STATE_SCHEMA_VERSION,
    machine_config_path,
    package_root,
    runtime_root,
    state_root,
    terminal_appserver_disconnect,
)


LOG = logging.getLogger("control-broker")

TASK_STATES = {
    "queued",
    "starting",
    "working",
    "waiting_human",
    "blocked",
    "failed",
    "completed",
    "cancelled",
    "unknown",
}
TERMINAL_STATES = {"failed", "completed", "cancelled"}
RUNNING_STATES = {"starting", "working", "waiting_human", "blocked"}
WORK_KINDS = {"codex", "command", "xcsh"}
REPORT_STATES = {"working", "waiting_human", "blocked", "failed", "completed", "cancelled", "unknown"}
PRIORITIES = {"routine": 0, "normal": 1, "attention": 2, "critical": 3}
TARGET_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
MAX_PROMPT = 32_000
MAX_REPLY = 16_000
MAX_SUMMARY = 8_000
MAX_QUESTION = 2_000
MAX_REQUEST = 65_536
MAX_COMMAND = 32_000
MAX_LABEL = 96
MAX_OUTPUT_EXCERPT = 8_000
COMMAND_SHELLS = {"zsh": "/usr/bin/zsh", "bash": "/usr/bin/bash"}
WORKER_MODELS = {
    "gpt-5.6-sol": "low",
    "gpt-5.6-terra": "medium",
    "gpt-5.6-luna": "medium",
}
WORKFLOW_STAGES = (
    "exploration", "plan", "implementation", "tests", "review", "ci",
    "merge", "release", "install", "live_uat", "accepted",
)
# A terminal worker report is useful evidence for knowledge work, but it is not
# evidence that a change passed an independent gate or is installed/running.
GATE_EVIDENCE = {
    "review": "review", "ci": "ci", "merge": "merge", "release": "artifact",
    "install": "installed_runtime", "live_uat": "live_uat",
}
REASONING_EFFORTS = {"low", "medium", "high", "xhigh", "max"}
CLEANUP_DELAY_SECONDS = 28.0
COMMAND_EXIT_REPORT_GRACE_SECONDS = 20.0
SESSION_RETENTION_SECONDS = 30 * 86400
OUTBOX_LEASE_SECONDS = 60.0
OUTBOX_RETRY_MAX_SECONDS = 300.0
AUTONOMOUS_COMMIT_MESSAGE_POLICY = "autonomous_conventional"
AUTONOMOUS_COMMIT_MESSAGE_GUIDANCE = (
    "Standing user policy: for authorized repository changes, select a concise Conventional Commit "
    "message and any repository-required body autonomously. Do not pause for human commit-wording "
    "alignment. Preserve actual repository rights, branch protections, review, CI, release, and all "
    "other consequential safeguards."
)


def now() -> float:
    return time.time()


def shared_commit_message_policy() -> str:
    """Read the installed shared policy, with a safe packaged default for tests.

    The portable installer validates the artifact.  The fallback preserves the
    user direction for ephemeral fixture roots that intentionally contain only
    the broker source.
    """
    policy_path = package_root() / "control-policy.json"
    try:
        policy = json.loads(policy_path.read_text())
    except (OSError, json.JSONDecodeError):
        return AUTONOMOUS_COMMIT_MESSAGE_POLICY
    if policy.get("commit_message_policy") != AUTONOMOUS_COMMIT_MESSAGE_POLICY:
        raise RuntimeError("unsupported shared commit-message policy")
    if policy.get("commit_message_alignment_required") is not False:
        raise RuntimeError("shared policy must not require commit-message alignment")
    return AUTONOMOUS_COMMIT_MESSAGE_POLICY


def voice_safe_text(value: Any, limit: int = 360) -> str:
    """Bound user-facing text without leaking runtime correlation details."""
    text = " ".join(str(value or "").split())
    text = re.sub(r"\b(?:evt|ack|ctl|cmd)-[A-Za-z0-9-]{8,}\b", "", text, flags=re.I)
    text = re.sub(r"\b[0-9a-f]{16,}\b", "", text, flags=re.I)
    text = re.sub(r"(?:^|\s)/[^\s]+", "", text)
    text = re.sub(r"\bw[\w.-]+:p\d+\b", "", text, flags=re.I)
    return " ".join(text.split())[:limit]


def completion_kind(summary: Any, stage: str | None = None, evidence_kind: str | None = None,
                    work_kind: str | None = None, command_exit_status: int | None = None) -> str:
    """Classify only typed/evidenced success; prose cannot promote a gate."""
    if stage == "tests":
        return "validation_work"
    if stage == "implementation":
        return "implementation_work"
    if stage == "native_consumer":
        return "integration" if evidence_kind else "integration_pending"
    gate_kinds = {
        "review": ("review", "review_pending"), "ci": ("ci", "ci_pending"),
        "merge": ("merge", "merge_pending"), "release": ("release", "release_pending"),
        "install": ("install", "install_pending"), "live_uat": ("uat", "uat_pending"),
    }
    if stage in gate_kinds:
        success, pending = gate_kinds[stage]
        required_evidence = GATE_EVIDENCE.get(stage)
        return success if required_evidence and evidence_kind == required_evidence else pending
    if work_kind == "command" and command_exit_status == 0:
        return "command_exit"
    # A terminal summary is not authoritative for external success.  It may
    # contain negations, historical PRs, quoted test names, or an unrelated
    # target, so untyped completions must remain explicitly unverified.
    return "unverified_completion"


COMPLETION_VOICE = {
    "command_exit": ("A visible command exited successfully.", "Its process result is recorded, but the requested effect is not independently verified.", "Use authoritative task or runtime evidence before advancing."),
    "integration": ("Installed native capability verification completed.", "The required native integration evidence is recorded.", "Proceed to the remaining UAT gate when authorized."),
    "validation_work": ("Focused validation work completed.", "Its task result is recorded, while independent gates remain separate.", "Proceed only when the required review and CI evidence is recorded."),
    "implementation_work": ("Implementation work completed.", "Its task result is recorded, while independent validation remains required.", "Run the required tests and review before promotion."),
    "review": ("Independent review completed.", "Its findings are recorded for the delivery decision.", "Address any recorded findings, then proceed only through the authorized CI gate."),
    "merge": ("An authorized change merged.", "The merged revision is available for release gating.", "Verify release requirements before creating an artifact."),
    "release": ("A release artifact was produced.", "An installable delivery candidate is now available.", "Install only in the approved isolated target and verify the runtime."),
    "install": ("The approved artifact was installed and verified.", "The installed runtime is ready for the remaining native capability check.", "Verify the installed protocol capability before live UAT."),
    "uat": ("Runtime acceptance testing completed.", "The live behavior has new acceptance evidence.", "Close the feature only after every required gate is complete."),
    "integration_pending": ("Native integration work completed without installed-runtime proof.", "The installed capability remains unverified.", "Keep the native capability and UAT gates closed until authoritative evidence exists."),
    "review_pending": ("A review work item completed, but review evidence is still pending.", "The independent review gate is not yet satisfied.", "Record authoritative review evidence or correct the documented blocker."),
    "ci_pending": ("A CI work item completed, but CI evidence is still pending.", "The CI gate is not yet satisfied.", "Record the authoritative CI result before progressing."),
    "merge_pending": ("Merge work completed without merge evidence.", "The change is not verified as merged for this feature.", "Record authoritative merge evidence or keep the gate open."),
    "release_pending": ("Release work completed without an immutable artifact receipt.", "No release is verified for this feature.", "Record artifact evidence before installation."),
    "install_pending": ("Installation work completed without installed-runtime evidence.", "No installed runtime is verified.", "Keep installation and UAT gates closed until runtime evidence exists."),
    "uat_pending": ("UAT work completed without live acceptance evidence.", "Runtime acceptance is still unverified.", "Record authoritative UAT evidence before feature closure."),
    "unverified_completion": ("A task reported completion, but its outcome is not independently verified.", "No external success or runtime effect is being claimed.", "Use durable stage and evidence records before advancing."),
}


def completion_presentation(payload: dict[str, Any], priority: str, *, stage: str | None = None,
                            evidence_kind: str | None = None, work_kind: str | None = None,
                            command_exit_status: int | None = None) -> dict[str, Any]:
    """Create deterministic, non-verbatim voice guidance for one event.

    Raw worker summaries remain only in the journal payload.  A semantic key is
    audit metadata, never text intended for speech.
    """
    state = str(payload.get("state") or "unknown")
    if state == "completed":
        outcome, significance, next_action = COMPLETION_VOICE[completion_kind(payload.get("summary"), stage, evidence_kind, work_kind, command_exit_status)]
    elif state == "waiting_human":
        outcome, significance, next_action = (
            "A required task needs your input.",
            "The workflow cannot safely continue without that decision.",
            "Answer the recorded question before resuming the task.",
        )
    elif state in {"failed", "blocked", "unknown", "lost"}:
        outcome, significance, next_action = (
            "A required task is blocked or could not be verified.",
            "The affected lifecycle stage remains open.",
            "Review the recorded blocker and apply a bounded correction or provide direction.",
        )
    elif state in {"cancelled", "interrupted"}:
        outcome, significance, next_action = (
            "A task was interrupted.",
            "Its required work remains unresolved.",
            "Confirm whether to resume it or record a different authorized path.",
        )
    else:
        outcome, significance, next_action = (
            "A task changed state.",
            "Its durable result requires review before workflow promotion.",
            "Inspect the recorded state and advance only when authorized.",
        )
    question = voice_safe_text(payload.get("question"), 240)
    # Fingerprint normalized content rather than volatile task/event identity.
    kind = completion_kind(payload.get("summary"), stage, evidence_kind, work_kind, command_exit_status)
    semantic_source = "|".join((state, stage or "", evidence_kind or "", work_kind or "", str(command_exit_status), kind, priority, voice_safe_text(payload.get("summary"), 800), question))
    return {
        "semantic_key": hashlib.sha256(semantic_source.encode()).hexdigest(),
        "kind": kind,
        "material": True,
        "outcome": outcome,
        "significance": significance,
        "blocker": question if state == "waiting_human" and question else None,
        "next_action": next_action,
    }


def bounded(value: str | None, limit: int, field: str, *, required: bool = False) -> str | None:
    if value is None:
        if required:
            raise ValueError(f"{field} is required")
        return None
    value = value.strip()
    if required and not value:
        raise ValueError(f"{field} is required")
    if len(value) > limit:
        raise ValueError(f"{field} exceeds {limit} characters")
    if "\x00" in value:
        raise ValueError(f"{field} contains a NUL byte")
    return value


def normalized_cwd(raw: str) -> str:
    if not raw or "\x00" in raw or "\n" in raw or "\r" in raw:
        raise ValueError("cwd must be a single non-empty path")
    candidate = Path(raw).expanduser()
    if not candidate.is_absolute():
        raise ValueError("cwd must be absolute")
    resolved = candidate.resolve(strict=True)
    if not resolved.is_dir():
        raise ValueError("cwd must name an existing directory")
    return str(resolved)


def configured_codex_binary(config: dict[str, Any]) -> str:
    """Resolve only an explicit machine binding or a discoverable executable.

    Service PATHs are intentionally often minimal, so never assume bare
    ``codex`` can be executed.  A missing binding is a delivery error that the
    outbox will retain/retry, not a false dispatched result.
    """
    candidate = str(config.get("codex_binary") or os.environ.get("CODEX_BINARY") or "")
    if candidate:
        path = Path(candidate).expanduser()
        if path.is_file() and os.access(path, os.X_OK):
            return str(path.resolve())
        raise RuntimeError(f"configured Codex binary is not executable: {path}")
    discovered = shutil.which("codex")
    if discovered:
        return discovered
    raise RuntimeError("Codex binary is not configured and is absent from service PATH")


def ensure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)
    info = path.stat()
    if info.st_uid == os.getuid():
        os.chmod(path, 0o700)


def public_task(row: sqlite3.Row) -> dict[str, Any]:
    keys = (
        "id",
        "parent_id",
        "target",
        "cwd",
        "state",
        "priority",
        "agent_kind",
        "agent_name",
        "agent_session_id",
        "native_turn_id",
        "native_runtime_json",
        "model",
        "reasoning_effort",
        "workspace_id",
        "tab_id",
        "pane_id",
        "herdr_state",
        "summary",
        "question",
        "created_at",
        "updated_at",
        "started_at",
        "finished_at",
        "stop_requested_at",
        "work_kind",
        "command_exit_status",
        "cleanup_deadline",
        "resume_count",
        "session_state",
        "output_excerpt",
        "terminal_reported_at",
        "task_version",
        "run_generation",
        "terminal_event_id",
    )
    return {key: row[key] for key in keys}


class StateDB:
    def __init__(self, path: Path, *, clock=now):
        ensure_directory(path.parent)
        self.path = path
        self.clock = clock
        self.conn = sqlite3.connect(path)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS targets (
                target TEXT PRIMARY KEY,
                cwd TEXT NOT NULL,
                workspace_id TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                parent_id TEXT REFERENCES tasks(id),
                target TEXT NOT NULL,
                cwd TEXT NOT NULL,
                state TEXT NOT NULL,
                priority TEXT NOT NULL,
                prompt_pending TEXT,
                agent_kind TEXT,
                agent_name TEXT,
                agent_session_id TEXT,
                native_turn_id TEXT,
                native_runtime_json TEXT,
                model TEXT,
                reasoning_effort TEXT,
                workspace_id TEXT,
                tab_id TEXT,
                pane_id TEXT,
                herdr_state TEXT,
                summary TEXT NOT NULL,
                question TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                started_at REAL,
                finished_at REAL,
                stop_requested_at REAL,
                work_kind TEXT NOT NULL DEFAULT 'codex',
                command_exit_status INTEGER,
                cleanup_deadline REAL,
                resume_count INTEGER NOT NULL DEFAULT 0,
                session_state TEXT NOT NULL DEFAULT 'live',
                output_excerpt TEXT,
                output_expires_at REAL,
                terminal_reported_at REAL
            );
            CREATE INDEX IF NOT EXISTS tasks_state_idx ON tasks(state, priority, created_at);
            CREATE INDEX IF NOT EXISTS tasks_pane_idx ON tasks(pane_id);
            CREATE TABLE IF NOT EXISTS notifications (
                dedupe_key TEXT PRIMARY KEY,
                task_id TEXT NOT NULL,
                state TEXT NOT NULL,
                last_sent_at REAL NOT NULL,
                repeat_count INTEGER NOT NULL DEFAULT 1,
                manager_queue_count INTEGER NOT NULL DEFAULT 0,
                last_manager_queue_at REAL
            );
            CREATE TABLE IF NOT EXISTS metadata (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS transitions (
                seq INTEGER PRIMARY KEY AUTOINCREMENT,
                task_id TEXT NOT NULL,
                at REAL NOT NULL,
                old_state TEXT,
                new_state TEXT NOT NULL,
                event TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS event_journal (
                event_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES tasks(id),
                generation INTEGER NOT NULL,
                task_version INTEGER NOT NULL,
                session_id TEXT,
                turn_id TEXT,
                kind TEXT NOT NULL,
                priority TEXT NOT NULL,
                payload_json TEXT NOT NULL,
                payload_sha256 TEXT NOT NULL,
                produced_at REAL NOT NULL,
                superseded_by TEXT,
                obsolete_at REAL,
                obsolete_reason TEXT,
                UNIQUE(task_id,generation,kind,task_version)
            );
            CREATE TABLE IF NOT EXISTS completion_outbox (
                event_id TEXT PRIMARY KEY REFERENCES event_journal(event_id),
                state TEXT NOT NULL DEFAULT 'pending',
                attempt_count INTEGER NOT NULL DEFAULT 0,
                next_attempt_at REAL NOT NULL,
                lease_token TEXT,
                lease_expires_at REAL,
                dispatched_at REAL,
                delivered_at REAL,
                consumed_at REAL,
                response_produced_at REAL,
                client_delivered_at REAL,
                last_error TEXT
            );
            CREATE TABLE IF NOT EXISTS completion_acks (
                ack_id TEXT PRIMARY KEY,
                event_id TEXT NOT NULL REFERENCES event_journal(event_id),
                stage TEXT NOT NULL,
                trusted_source TEXT NOT NULL,
                manager_turn_id TEXT,
                evidence_id TEXT,
                created_at REAL NOT NULL,
                UNIQUE(event_id,stage,trusted_source,evidence_id)
            );
            CREATE TABLE IF NOT EXISTS completion_presentations (
                semantic_key TEXT PRIMARY KEY,
                first_event_id TEXT NOT NULL REFERENCES event_journal(event_id),
                last_event_id TEXT NOT NULL REFERENCES event_journal(event_id),
                first_consumed_at REAL,
                last_consumed_at REAL
            );
            CREATE TABLE IF NOT EXISTS feature_observations (
                observation_id TEXT PRIMARY KEY,
                feature_id TEXT NOT NULL REFERENCES features(feature_id),
                stage TEXT NOT NULL,
                kind TEXT NOT NULL,
                evidence_id TEXT NOT NULL,
                detail TEXT NOT NULL,
                observed_at REAL NOT NULL,
                UNIQUE(feature_id,stage,kind,evidence_id)
            );
            CREATE INDEX IF NOT EXISTS completion_outbox_pending_idx
              ON completion_outbox(state,next_attempt_at);
            CREATE TABLE IF NOT EXISTS features (
                feature_id TEXT PRIMARY KEY,
                target TEXT NOT NULL,
                cwd TEXT NOT NULL,
                title TEXT NOT NULL,
                scope TEXT NOT NULL,
                state TEXT NOT NULL DEFAULT 'open',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                accepted_at REAL
            );
            CREATE TABLE IF NOT EXISTS feature_stages (
                feature_id TEXT NOT NULL REFERENCES features(feature_id),
                stage TEXT NOT NULL,
                ordinal INTEGER NOT NULL,
                required INTEGER NOT NULL DEFAULT 1,
                state TEXT NOT NULL DEFAULT 'pending',
                task_id TEXT REFERENCES tasks(id),
                action_json TEXT,
                evidence_json TEXT,
                claim_key TEXT,
                attempts INTEGER NOT NULL DEFAULT 0,
                blocker TEXT,
                updated_at REAL NOT NULL,
                PRIMARY KEY(feature_id,stage),
                UNIQUE(feature_id,ordinal)
            );
            CREATE INDEX IF NOT EXISTS feature_stages_ready_idx
              ON feature_stages(feature_id,ordinal,state);
            CREATE TABLE IF NOT EXISTS native_turn_cursors (
                producer TEXT PRIMARY KEY,
                last_revision INTEGER NOT NULL DEFAULT 0,
                updated_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS admission_idempotency (
                idempotency_key TEXT PRIMARY KEY,
                method TEXT NOT NULL,
                request_sha256 TEXT NOT NULL,
                task_id TEXT NOT NULL REFERENCES tasks(id),
                execution_evidence TEXT NOT NULL DEFAULT 'admitted',
                created_at REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS followup_deliveries (
                followup_id TEXT PRIMARY KEY,
                task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                idempotency_key TEXT NOT NULL,
                request_sha256 TEXT NOT NULL,
                client_user_message_id TEXT NOT NULL,
                delivery_kind TEXT NOT NULL,
                state TEXT NOT NULL,
                text TEXT NOT NULL,
                expected_turn_id TEXT,
                detail TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(task_id,idempotency_key)
            );
            CREATE INDEX IF NOT EXISTS followup_deliveries_task_idx
              ON followup_deliveries(task_id,created_at);
            CREATE TABLE IF NOT EXISTS preserved_legacy_followups (
                task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
                thread_id TEXT NOT NULL,
                queued_submission_id TEXT NOT NULL,
                client_user_message_id TEXT NOT NULL,
                input_json TEXT NOT NULL,
                state TEXT NOT NULL,
                detail TEXT,
                preserved_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                PRIMARY KEY(task_id,thread_id,queued_submission_id)
            );
            CREATE TABLE IF NOT EXISTS manager_attachment_recoveries (
                action_id TEXT PRIMARY KEY,
                claim_sha256 TEXT NOT NULL,
                logical_thread_id TEXT NOT NULL,
                expected_binding_generation INTEGER NOT NULL,
                old_workspace_id TEXT NOT NULL,
                old_pane_id TEXT NOT NULL,
                state TEXT NOT NULL,
                replacement_workspace_id TEXT,
                replacement_tab_id TEXT,
                replacement_pane_id TEXT,
                manager_execution_id TEXT,
                runtime_generation TEXT,
                receipt_json TEXT,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );
            """
        )
        existing_columns = {
            row["name"] for row in self.conn.execute("PRAGMA table_info(tasks)").fetchall()
        }
        migrations = {
            "work_kind": "TEXT NOT NULL DEFAULT 'codex'",
            "command_exit_status": "INTEGER",
            "cleanup_deadline": "REAL",
            "resume_count": "INTEGER NOT NULL DEFAULT 0",
            "session_state": "TEXT NOT NULL DEFAULT 'live'",
            "output_excerpt": "TEXT",
            "output_expires_at": "REAL",
            "terminal_reported_at": "REAL",
            "model": "TEXT",
            "reasoning_effort": "TEXT",
            "native_turn_id": "TEXT",
            "native_runtime_json": "TEXT",
            "task_version": "INTEGER NOT NULL DEFAULT 0",
            "run_generation": "INTEGER NOT NULL DEFAULT 0",
            "terminal_event_id": "TEXT",
        }
        for column, definition in migrations.items():
            if column not in existing_columns:
                self.conn.execute(f"ALTER TABLE tasks ADD COLUMN {column} {definition}")
        notification_columns = {
            row["name"] for row in self.conn.execute("PRAGMA table_info(notifications)").fetchall()
        }
        if "manager_queue_count" not in notification_columns:
            self.conn.execute(
                "ALTER TABLE notifications ADD COLUMN manager_queue_count INTEGER NOT NULL DEFAULT 0"
            )
        if "last_manager_queue_at" not in notification_columns:
            self.conn.execute("ALTER TABLE notifications ADD COLUMN last_manager_queue_at REAL")
        attachment_columns = {row["name"] for row in self.conn.execute("PRAGMA table_info(manager_attachment_recoveries)")}
        if attachment_columns and "logical_thread_id" not in attachment_columns:
            self.conn.execute("ALTER TABLE manager_attachment_recoveries ADD COLUMN logical_thread_id TEXT NOT NULL DEFAULT ''")
        if attachment_columns and "manager_execution_id" not in attachment_columns:
            self.conn.execute("ALTER TABLE manager_attachment_recoveries ADD COLUMN manager_execution_id TEXT")
        schema = self.conn.execute("SELECT value FROM metadata WHERE key='schema_version'").fetchone()
        if schema is not None and int(schema["value"]) > STATE_SCHEMA_VERSION:
            raise RuntimeError(f"state schema {schema['value']} is newer than this package ({STATE_SCHEMA_VERSION})")
        self.conn.execute(
            f"INSERT INTO metadata(key,value) VALUES('schema_version','{STATE_SCHEMA_VERSION}') "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value"
        )
        self.conn.execute(
            "INSERT OR IGNORE INTO metadata(key,value) VALUES('completion_outbox_cutover_at',?)",
            (str(self.clock()),),
        )
        self.conn.commit()
        os.chmod(path, 0o600)

    def manager_attachment(self, action_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM manager_attachment_recoveries WHERE action_id=?", (action_id,)
        ).fetchone()

    def claim_manager_attachment(self, *, action_id: str, claim_sha256: str,
                                 logical_thread_id: str,
                                 expected_binding_generation: int,
                                 old_workspace_id: str, old_pane_id: str) -> sqlite3.Row:
        """Durably reserve one lost-binding replacement before any effect.

        The action capability is checked by the broker before this method.  A
        second caller can only observe the original reservation; it cannot
        alter its old binding or substitute a fresh terminal allocation.
        """
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            prior=self.manager_attachment(action_id)
            if prior is not None:
                if not hmac.compare_digest(str(prior["claim_sha256"]),claim_sha256):
                    raise PermissionError("manager attachment claim capability does not own this action")
                # The intent is immutable. A replay commonly observes the
                # replacement config after an atomic config write but before
                # its broker receipt; compare it below against this record,
                # never against today's ephemeral binding.
                self.conn.commit(); return prior
            conflict=self.conn.execute(
                """SELECT action_id,state FROM manager_attachment_recoveries
                   WHERE logical_thread_id=? AND expected_binding_generation=?
                     AND old_workspace_id=? AND old_pane_id=?
                     AND state IN ('intent','create_intent','created','execution_intent','execution_admitted','binding_intent','binding_persisted',
                                   'launch_intent','launch_text_sent','launch_enter_sent','uncertain')
                   ORDER BY created_at DESC LIMIT 1""",
                (logical_thread_id,expected_binding_generation,old_workspace_id,old_pane_id),
            ).fetchone()
            if conflict is not None:
                raise RuntimeError(
                    f"manager attachment is already uncertain under action {conflict['action_id']}; refusing another allocation"
                )
            stamp=self.clock()
            self.conn.execute(
                """INSERT INTO manager_attachment_recoveries(
                    action_id,claim_sha256,logical_thread_id,expected_binding_generation,old_workspace_id,old_pane_id,
                    state,created_at,updated_at
                ) VALUES(?,?,?,?,?,?,'intent',?,?)""",
                (action_id,claim_sha256,logical_thread_id,expected_binding_generation,old_workspace_id,old_pane_id,stamp,stamp),
            )
            self.conn.commit()
            return self.manager_attachment(action_id)  # type: ignore[return-value]
        except Exception:
            if self.conn.in_transaction: self.conn.rollback()
            raise

    def advance_manager_attachment(self, action_id: str, *, states: set[str], state: str,
                                   receipt: dict[str, Any] | None = None, **values: Any) -> sqlite3.Row:
        """CAS-transition a replacement receipt; never overwrite uncertainty."""
        allowed={"intent","create_intent","created","execution_intent","execution_admitted",
                 "binding_intent","binding_persisted","launch_intent","launch_text_sent",
                 "launch_enter_sent","verified","uncertain","rejected"}
        if state not in allowed or not states or not states <= allowed:
            raise ValueError("invalid manager attachment receipt transition")
        assignments=["state=?","updated_at=?"]; args: list[Any]=[state,self.clock()]
        for key,value in values.items():
            if key not in {"replacement_workspace_id","replacement_tab_id","replacement_pane_id",
                           "manager_execution_id","runtime_generation"}:
                raise ValueError("invalid manager attachment receipt field")
            assignments.append(f"{key}=?"); args.append(value)
        if receipt is not None:
            assignments.append("receipt_json=?"); args.append(json.dumps(receipt,sort_keys=True,separators=(",",":")))
        placeholders=",".join("?" for _ in states)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            changed=self.conn.execute(
                f"UPDATE manager_attachment_recoveries SET {','.join(assignments)} WHERE action_id=? AND state IN ({placeholders})",
                (*args,action_id,*sorted(states)),
            ).rowcount
            if changed != 1:
                self.conn.rollback()
                current=self.manager_attachment(action_id)
                if current is None: raise RuntimeError("manager attachment intent disappeared")
                return current
            self.conn.commit()
            return self.manager_attachment(action_id)  # type: ignore[return-value]
        except Exception:
            if self.conn.in_transaction: self.conn.rollback()
            raise

    def create_feature(self, data: dict[str, Any]) -> dict[str, Any]:
        """Create an auditable lifecycle.  `scope=plan_only` deliberately
        stops promotion after plan; `end_to_end` permits the listed actions."""
        feature_id = data["feature_id"]
        scope = data.get("scope", "plan_only")
        if scope not in {"plan_only", "end_to_end"}:
            raise ValueError("scope must be plan_only or end_to_end")
        ts = self.clock()
        self.conn.execute("INSERT INTO features(feature_id,target,cwd,title,scope,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                          (feature_id, data["target"], data["cwd"], data["title"], scope, ts, ts))
        actions = data.get("actions", {})
        children = data.get("children", {})
        required = set(data.get("required_stages", WORKFLOW_STAGES))
        for ordinal, stage in enumerate(WORKFLOW_STAGES):
            self.conn.execute("""INSERT INTO feature_stages(feature_id,stage,ordinal,required,action_json,updated_at)
                              VALUES(?,?,?,?,?,?)""",
                              (feature_id, stage, ordinal, int(stage in required),
                               json.dumps(actions.get(stage), sort_keys=True) if actions.get(stage) else None, ts))
        for stage, task_id in children.items():
            if stage not in WORKFLOW_STAGES or self.task(task_id) is None:
                raise ValueError("feature child must name an existing task and workflow stage")
            self.conn.execute("UPDATE feature_stages SET task_id=?,state='working',updated_at=? WHERE feature_id=? AND stage=?",
                              (task_id, ts, feature_id, stage))
        self.conn.commit()
        return self.feature(feature_id)

    def feature(self, feature_id: str) -> dict[str, Any]:
        row = self.conn.execute("SELECT * FROM features WHERE feature_id=?", (feature_id,)).fetchone()
        if row is None:
            raise ValueError(f"feature {feature_id!r} does not exist")
        result = dict(row)
        stages = self.conn.execute("SELECT * FROM feature_stages WHERE feature_id=? ORDER BY ordinal", (feature_id,)).fetchall()
        result["stages"] = [dict(stage) | {"action": json.loads(stage["action_json"]) if stage["action_json"] else None,
                                           "evidence": json.loads(stage["evidence_json"]) if stage["evidence_json"] else None}
                            for stage in stages]
        observations = self.conn.execute(
            "SELECT stage,kind,evidence_id,detail,observed_at FROM feature_observations WHERE feature_id=? ORDER BY observed_at",
            (feature_id,),
        ).fetchall()
        result["observations"] = [dict(item) for item in observations]
        return result

    def attach_feature_task(self, feature_id: str, stage: str, task_id: str) -> None:
        row = self.conn.execute("SELECT state,task_id FROM feature_stages WHERE feature_id=? AND stage=?", (feature_id, stage)).fetchone()
        if row is None:
            raise ValueError("unknown feature stage")
        if row["task_id"] and row["task_id"] != task_id:
            raise ValueError("stage already has a different task")
        self.conn.execute("UPDATE feature_stages SET task_id=?,state='working',updated_at=? WHERE feature_id=? AND stage=?",
                          (task_id, self.clock(), feature_id, stage))
        self.conn.commit()

    def ensure_feature_dependency(self, feature_id: str, stage: str, *, before_stage: str,
                                  blocker: str) -> dict[str, Any]:
        """Durably add one named, required integration dependency.

        This is deliberately idempotent: a reconnect or repeated manager turn
        can update its current blocker but cannot add another prerequisite or
        create a worker.  Dependencies have no action until an authoritative
        evidence record completes them.
        """
        if not re.fullmatch(r"[a-z][a-z0-9_]{0,39}", stage):
            raise ValueError("invalid feature dependency stage")
        feature = self.conn.execute("SELECT feature_id FROM features WHERE feature_id=?", (feature_id,)).fetchone()
        if feature is None:
            raise ValueError("unknown feature")
        existing = self.conn.execute(
            "SELECT * FROM feature_stages WHERE feature_id=? AND stage=?", (feature_id, stage)
        ).fetchone()
        if existing is not None:
            self._move_feature_stage_before(feature_id, stage, before_stage)
            if existing["state"] != "completed":
                self.conn.execute(
                    "UPDATE feature_stages SET required=1,state='blocked',blocker=?,updated_at=? WHERE feature_id=? AND stage=?",
                    (blocker[:500], self.clock(), feature_id, stage),
                )
                self.conn.commit()
            return self.feature(feature_id)
        anchor = self.conn.execute(
            "SELECT ordinal FROM feature_stages WHERE feature_id=? AND stage=?", (feature_id, before_stage)
        ).fetchone()
        if anchor is None:
            raise ValueError("unknown dependency insertion stage")
        # Move first to a disjoint ordinal range so UNIQUE(feature_id,ordinal)
        # remains valid while every later stage shifts by one.
        ordinal = int(anchor["ordinal"])
        self.conn.execute("UPDATE feature_stages SET ordinal=ordinal+100 WHERE feature_id=? AND ordinal>=?", (feature_id, ordinal))
        self.conn.execute("UPDATE feature_stages SET ordinal=ordinal-99 WHERE feature_id=? AND ordinal>=?", (feature_id, ordinal + 100))
        self.conn.execute(
            "INSERT INTO feature_stages(feature_id,stage,ordinal,required,state,blocker,updated_at) VALUES(?,?,?,?,?,?,?)",
            (feature_id, stage, ordinal, 1, "blocked", blocker[:500], self.clock()),
        )
        self.conn.commit()
        return self.feature(feature_id)

    def _move_feature_stage_before(self, feature_id: str, stage: str, before_stage: str) -> None:
        """Reposition an existing dependency without creating another stage.

        An installed-runtime capability check belongs after the release/install
        it needs, but before live UAT.  Repositioning preserves its current
        blocker/evidence and is safe to repeat after reconnect.
        """
        row = self.conn.execute("SELECT ordinal FROM feature_stages WHERE feature_id=? AND stage=?", (feature_id, stage)).fetchone()
        anchor = self.conn.execute("SELECT ordinal FROM feature_stages WHERE feature_id=? AND stage=?", (feature_id, before_stage)).fetchone()
        if row is None or anchor is None:
            raise ValueError("unknown dependency insertion stage")
        old, target = int(row["ordinal"]), int(anchor["ordinal"])
        if old == target - 1:
            return
        # A temporary negative ordinal avoids the unique(feature_id, ordinal)
        # constraint while shifting the contiguous range.
        self.conn.execute("UPDATE feature_stages SET ordinal=-1 WHERE feature_id=? AND stage=?", (feature_id, stage))
        if old < target:
            self.conn.execute("UPDATE feature_stages SET ordinal=ordinal-1 WHERE feature_id=? AND ordinal>? AND ordinal<?", (feature_id, old, target))
            target -= 1
        else:
            self.conn.execute("UPDATE feature_stages SET ordinal=ordinal+1 WHERE feature_id=? AND ordinal>=? AND ordinal<?", (feature_id, target, old))
        self.conn.execute("UPDATE feature_stages SET ordinal=? WHERE feature_id=? AND stage=?", (target, feature_id, stage))

    def sync_feature_child_state(self, task_id: str) -> list[dict[str, Any]]:
        """Reflect a nonterminal child wait in its feature without promotion."""
        child = self.task(task_id)
        if child is None:
            return []
        rows = self.conn.execute("SELECT * FROM feature_stages WHERE task_id=?", (task_id,)).fetchall()
        changed: list[dict[str, Any]] = []
        waiting_prefix = f"child {task_id} waiting_human:"
        for row in rows:
            if row["state"] == "completed" or child["state"] in TERMINAL_STATES:
                continue
            if child["state"] == "waiting_human":
                blocker = f"{waiting_prefix} {(child['question'] or child['summary'] or 'human input required')[:400]}"
                self.conn.execute(
                    "UPDATE feature_stages SET state='blocked',blocker=?,updated_at=? WHERE feature_id=? AND stage=?",
                    (blocker, self.clock(), row["feature_id"], row["stage"]),
                )
                changed.append({"feature_id": row["feature_id"], "stage": row["stage"], "state": "blocked"})
            elif row["state"] == "blocked" and str(row["blocker"] or "").startswith(waiting_prefix):
                self.conn.execute(
                    "UPDATE feature_stages SET state='working',blocker=NULL,updated_at=? WHERE feature_id=? AND stage=?",
                    (self.clock(), row["feature_id"], row["stage"]),
                )
                changed.append({"feature_id": row["feature_id"], "stage": row["stage"], "state": "working"})
        if changed:
            self.conn.commit()
        return changed

    def sync_all_feature_child_states(self) -> list[dict[str, Any]]:
        """Reconstruct nonterminal feature blockers after a broker restart."""
        task_ids = self.conn.execute(
            "SELECT DISTINCT task_id FROM feature_stages WHERE task_id IS NOT NULL"
        ).fetchall()
        changed: list[dict[str, Any]] = []
        for row in task_ids:
            changed.extend(self.sync_feature_child_state(row["task_id"]))
        return changed

    def record_feature_evidence(self, feature_id: str, stage: str, kind: str, evidence_id: str) -> dict[str, Any]:
        expected = GATE_EVIDENCE.get(stage)
        if expected and kind != expected:
            raise ValueError(f"{stage} requires {expected} evidence")
        if not evidence_id:
            raise ValueError("evidence_id is required")
        row = self.conn.execute("SELECT * FROM feature_stages WHERE feature_id=? AND stage=?", (feature_id, stage)).fetchone()
        if row is None:
            raise ValueError("unknown feature stage")
        evidence = {"kind": kind, "id": evidence_id, "recorded_at": self.clock()}
        self.conn.execute("UPDATE feature_stages SET evidence_json=?,state='completed',blocker=NULL,updated_at=? WHERE feature_id=? AND stage=?",
                          (json.dumps(evidence, sort_keys=True), self.clock(), feature_id, stage))
        self._feature_close_if_accepted(feature_id)
        self.conn.commit()
        return self.feature(feature_id)

    def record_feature_observation(self, feature_id: str, stage: str, kind: str,
                                   evidence_id: str, detail: str) -> None:
        """Persist authoritative external facts without prematurely passing a gate."""
        if self.conn.execute("SELECT 1 FROM feature_stages WHERE feature_id=? AND stage=?", (feature_id, stage)).fetchone() is None:
            raise ValueError("unknown feature stage")
        self.conn.execute(
            """INSERT OR IGNORE INTO feature_observations
               (observation_id,feature_id,stage,kind,evidence_id,detail,observed_at)
               VALUES(?,?,?,?,?,?,?)""",
            (f"obs-{uuid.uuid4().hex}", feature_id, stage, kind[:40], evidence_id[:500], detail[:2000], self.clock()),
        )
        self.conn.commit()

    def set_feature_stage_action(self, feature_id: str, stage: str, action: dict[str, Any]) -> None:
        if not isinstance(action.get("prompt"), str) or not action["prompt"].strip():
            raise ValueError("stage action requires a prompt")
        if self.conn.execute("SELECT 1 FROM feature_stages WHERE feature_id=? AND stage=?", (feature_id, stage)).fetchone() is None:
            raise ValueError("unknown feature stage")
        self.conn.execute("UPDATE feature_stages SET action_json=?,updated_at=? WHERE feature_id=? AND stage=?",
                          (json.dumps(action, sort_keys=True), self.clock(), feature_id, stage))
        self.conn.commit()

    def feature_prompt_binding(self, feature_id: str) -> dict[str, Any]:
        feature = self.conn.execute("SELECT feature_id,title,target,cwd FROM features WHERE feature_id=?", (feature_id,)).fetchone()
        if feature is None:
            raise ValueError("unknown feature")
        observations = self.conn.execute(
            "SELECT stage,kind,evidence_id FROM feature_observations WHERE feature_id=? AND kind!='scope_mismatch' ORDER BY observed_at",
            (feature_id,),
        ).fetchall()
        return dict(feature) | {"artifact_identities": [dict(row) for row in observations]}

    def retry_feature_stage(self, feature_id: str, stage: str, blocker: str) -> None:
        """Retry an unsatisfied gate once with an explicit durable reason.

        A completed worker that reviewed a different target is not gate evidence.
        Clearing only the old stage claim permits one newly claimed replacement;
        the old task and the mismatch observation remain auditable.
        """
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute("SELECT state,evidence_json,task_id FROM feature_stages WHERE feature_id=? AND stage=?", (feature_id, stage)).fetchone()
            if row is None:
                raise ValueError("unknown feature stage")
            if row["evidence_json"]:
                raise ValueError("cannot retry a stage with accepted evidence")
            if row["state"] not in {"awaiting_evidence", "blocked"}:
                raise ValueError("stage is not awaiting corrective retry")
            if row["task_id"]:
                child = self.conn.execute("SELECT state,run_generation,terminal_reported_at FROM tasks WHERE id=?", (row["task_id"],)).fetchone()
                if child is None or child["state"] in RUNNING_STATES or child["terminal_reported_at"] is None:
                    raise ValueError("cannot replace a child that is active or resumed; preserve its current generation")
            ts = self.clock()
            self.conn.execute(
                """INSERT OR IGNORE INTO feature_observations
                   (observation_id,feature_id,stage,kind,evidence_id,detail,observed_at)
                   VALUES(?,?,?,?,?,?,?)""",
                (f"obs-{uuid.uuid4().hex}", feature_id, stage, "scope_mismatch", row["task_id"] or "no-child", blocker[:2000], ts),
            )
            self.conn.execute(
                "UPDATE feature_stages SET state='pending',task_id=NULL,claim_key=NULL,blocker=?,updated_at=? WHERE feature_id=? AND stage=?",
                (blocker[:500], ts, feature_id, stage),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def feature_task_terminal(self, task_id: str, state: str, summary: str) -> list[dict[str, Any]]:
        """Consume a durable child result once. Failed/deferred children keep
        the feature open; success only completes non-gated work stages."""
        rows = self.conn.execute("SELECT * FROM feature_stages WHERE task_id=?", (task_id,)).fetchall()
        changed = []
        for row in rows:
            if row["state"] in {"completed", "blocked"}:
                continue
            if state != "completed":
                self.conn.execute("UPDATE feature_stages SET state='blocked',blocker=?,updated_at=? WHERE feature_id=? AND stage=?",
                                  (f"child {task_id} ended {state}: {summary[:500]}", self.clock(), row["feature_id"], row["stage"]))
            elif row["stage"] in GATE_EVIDENCE:
                self.conn.execute("UPDATE feature_stages SET state='awaiting_evidence',updated_at=? WHERE feature_id=? AND stage=?",
                                  (self.clock(), row["feature_id"], row["stage"]))
            else:
                evidence = json.dumps({"kind": "task_completion", "id": task_id, "summary": summary[:500]}, sort_keys=True)
                self.conn.execute("UPDATE feature_stages SET state='completed',evidence_json=?,updated_at=? WHERE feature_id=? AND stage=?",
                                  (evidence, self.clock(), row["feature_id"], row["stage"]))
            changed.append({"feature_id": row["feature_id"], "stage": row["stage"]})
        for item in changed:
            self._feature_close_if_accepted(item["feature_id"])
        self.conn.commit()
        return changed

    def claim_next_feature_action(self, feature_id: str) -> dict[str, Any] | None:
        feature = self.conn.execute("SELECT * FROM features WHERE feature_id=?", (feature_id,)).fetchone()
        if feature is None or feature["state"] != "open": return None
        stages = self.conn.execute("SELECT * FROM feature_stages WHERE feature_id=? ORDER BY ordinal", (feature_id,)).fetchall()
        for row in stages:
            if not row["required"]: continue
            if row["state"] != "pending":
                if row["state"] != "completed": return None
                continue
            if feature["scope"] == "plan_only" and row["stage"] not in {"exploration", "plan"}:
                return None
            if not row["action_json"]: return None
            key = f"{feature_id}:{row['stage']}"
            # Claim before dispatch: reconnect/replayed terminal events cannot
            # create a duplicate worker.
            updated = self.conn.execute("""UPDATE feature_stages SET state='claimed',claim_key=?,attempts=attempts+1,updated_at=?
                                      WHERE feature_id=? AND stage=? AND state='pending' AND claim_key IS NULL""",
                                      (key, self.clock(), feature_id, row["stage"])).rowcount
            if not updated: return None
            self.conn.commit()
            return {"feature_id": feature_id, "stage": row["stage"], "claim_key": key,
                    "action": json.loads(row["action_json"]), "target": feature["target"], "cwd": feature["cwd"], "title": feature["title"]}
        return None

    def fail_feature_claim(self, claim: dict[str, Any], error: str) -> None:
        self.conn.execute("UPDATE feature_stages SET state='blocked',blocker=?,updated_at=? WHERE feature_id=? AND stage=? AND claim_key=?",
                          (error[:500], self.clock(), claim["feature_id"], claim["stage"], claim["claim_key"]))
        self.conn.commit()

    def _feature_close_if_accepted(self, feature_id: str) -> None:
        rows = self.conn.execute("SELECT stage,required,state,evidence_json FROM feature_stages WHERE feature_id=?", (feature_id,)).fetchall()
        accepted = next((row for row in rows if row["stage"] == "accepted"), None)
        if accepted and accepted["state"] == "completed" and all(not row["required"] or row["state"] == "completed" for row in rows):
            self.conn.execute("UPDATE features SET state='accepted',accepted_at=?,updated_at=? WHERE feature_id=?", (self.clock(), self.clock(), feature_id))

    def close(self) -> None:
        self.conn.close()

    def task(self, task_id: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM tasks WHERE id=?", (task_id,)).fetchone()

    def claim_followup(
        self,
        *,
        task_id: str,
        idempotency_key: str,
        request_sha256: str,
        client_user_message_id: str,
        delivery_kind: str,
        text: str,
        expected_turn_id: str | None,
    ) -> tuple[sqlite3.Row, bool]:
        existing = self.conn.execute(
            "SELECT * FROM followup_deliveries WHERE task_id=? AND idempotency_key=?",
            (task_id, idempotency_key),
        ).fetchone()
        if existing is not None:
            if existing["request_sha256"] != request_sha256:
                raise ValueError("continue_task idempotency key was reused with different arguments")
            return existing, False
        ts = self.clock()
        followup_id = f"followup-{uuid.uuid4().hex}"
        self.conn.execute(
            """INSERT INTO followup_deliveries(
                 followup_id,task_id,idempotency_key,request_sha256,client_user_message_id,
                 delivery_kind,state,text,expected_turn_id,created_at,updated_at
               ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
            (
                followup_id,
                task_id,
                idempotency_key,
                request_sha256,
                client_user_message_id,
                delivery_kind,
                "preparing",
                text,
                expected_turn_id,
                ts,
                ts,
            ),
        )
        self.conn.commit()
        return self.conn.execute(
            "SELECT * FROM followup_deliveries WHERE followup_id=?", (followup_id,)
        ).fetchone(), True

    def update_followup(self, followup_id: str, state: str, detail: str | None = None) -> sqlite3.Row:
        self.conn.execute(
            "UPDATE followup_deliveries SET state=?,detail=?,updated_at=? WHERE followup_id=?",
            (state, detail, self.clock(), followup_id),
        )
        self.conn.commit()
        row = self.conn.execute(
            "SELECT * FROM followup_deliveries WHERE followup_id=?", (followup_id,)
        ).fetchone()
        if row is None:
            raise KeyError(followup_id)
        return row

    def followups(self, task_id: str) -> list[dict[str, Any]]:
        return [dict(row) for row in self.conn.execute(
            "SELECT * FROM followup_deliveries WHERE task_id=? ORDER BY created_at",
            (task_id,),
        ).fetchall()]

    def followup_by_idempotency(self, task_id: str, idempotency_key: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM followup_deliveries WHERE task_id=? AND idempotency_key=?",
            (task_id, idempotency_key),
        ).fetchone()

    def preserve_legacy_followup(
        self, task_id: str, thread_id: str, submission: dict[str, Any]
    ) -> sqlite3.Row:
        queued_id = str(submission["id"])
        client_id = str(submission["clientUserMessageId"])
        payload = json.dumps(submission["input"], separators=(",", ":"), sort_keys=True)
        ts = self.clock()
        self.conn.execute(
            """INSERT INTO preserved_legacy_followups(
                 task_id,thread_id,queued_submission_id,client_user_message_id,input_json,
                 state,preserved_at,updated_at
               ) VALUES(?,?,?,?,?,'preserved',?,?)
               ON CONFLICT(task_id,thread_id,queued_submission_id) DO NOTHING""",
            (task_id, thread_id, queued_id, client_id, payload, ts, ts),
        )
        self.conn.commit()
        return self.conn.execute(
            """SELECT * FROM preserved_legacy_followups
               WHERE task_id=? AND thread_id=? AND queued_submission_id=?""",
            (task_id, thread_id, queued_id),
        ).fetchone()

    def update_preserved_legacy(
        self, task_id: str, thread_id: str, queued_id: str, state: str, detail: str | None = None
    ) -> None:
        self.conn.execute(
            """UPDATE preserved_legacy_followups SET state=?,detail=?,updated_at=?
               WHERE task_id=? AND thread_id=? AND queued_submission_id=?""",
            (state, detail, self.clock(), task_id, thread_id, queued_id),
        )
        self.conn.commit()

    def preserved_legacy_followups(self, task_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM preserved_legacy_followups WHERE task_id=? ORDER BY preserved_at",
            (task_id,),
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["input"] = json.loads(item.pop("input_json"))
            result.append(item)
        return result

    def target(self, target: str) -> sqlite3.Row | None:
        return self.conn.execute("SELECT * FROM targets WHERE target=?", (target,)).fetchone()

    def add_task(self, data: dict[str, Any], *, commit: bool = True) -> sqlite3.Row:
        work_kind = data.get("work_kind", "codex")
        if work_kind not in WORK_KINDS:
            raise ValueError(f"unsupported work kind {work_kind!r}")
        ts = self.clock()
        if work_kind == "codex":
            existing = self.target(data["target"])
            if existing and existing["cwd"] != data["cwd"]:
                raise ValueError(
                    f"target {data['target']!r} is already bound to {existing['cwd']}; use a distinct target key"
                )
            if not existing:
                self.conn.execute(
                    "INSERT INTO targets(target,cwd,created_at,updated_at) VALUES(?,?,?,?)",
                    (data["target"], data["cwd"], ts, ts),
                )
        self.conn.execute(
            """INSERT INTO tasks(
                id,parent_id,target,cwd,state,priority,prompt_pending,summary,
                created_at,updated_at,work_kind,session_state,model,reasoning_effort,native_runtime_json
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                data["id"],
                data.get("parent_id"),
                data["target"],
                data["cwd"],
                "queued",
                data["priority"],
                data["prompt"],
                data["summary"],
                ts,
                ts,
                work_kind,
                data.get("session_state", "live"),
                data.get("model"),
                data.get("reasoning_effort"),
                data.get("native_runtime_json"),
            ),
        )
        self.record_transition(data["id"], None, "queued", "created", at=ts)
        if commit:
            self.conn.commit()
        return self.task(data["id"])  # type: ignore[return-value]

    def update(self, task_id: str, *, event: str | None = None, **values: Any) -> sqlite3.Row:
        if not values:
            row = self.task(task_id)
            if row is None:
                raise KeyError(task_id)
            return row
        previous = self.task(task_id)
        if previous is None:
            raise KeyError(task_id)
        values["updated_at"] = self.clock()
        values["task_version"] = int(previous["task_version"]) + 1
        columns = ", ".join(f"{key}=?" for key in values)
        params = [*values.values(), task_id]
        self.conn.execute(f"UPDATE tasks SET {columns} WHERE id=?", params)
        state_changed = "state" in values and values["state"] != previous["state"]
        if int(values.get("run_generation", previous["run_generation"])) > int(previous["run_generation"]):
            self.conn.execute(
                """UPDATE event_journal SET obsolete_at=?,obsolete_reason='superseded_by_continuation'
                   WHERE task_id=? AND obsolete_at IS NULL AND event_id IN
                     (SELECT event_id FROM completion_outbox WHERE client_delivered_at IS NULL)""",
                (values["updated_at"], task_id),
            )
        if event is not None or state_changed:
            self.record_transition(
                task_id,
                previous["state"],
                values.get("state", previous["state"]),
                event or "state_update",
                at=values["updated_at"],
            )
        new_state = values.get("state", previous["state"])
        if previous["state"] not in TERMINAL_STATES and new_state in TERMINAL_STATES:
            event_id = f"evt-{uuid.uuid4().hex}"
            payload = json.dumps(
                {
                    "task_id": task_id,
                    "state": new_state,
                    "summary": values.get("summary", previous["summary"]),
                    "question": values.get("question", previous["question"]),
                    "cwd": previous["cwd"],
                    "workspace_id": previous["workspace_id"],
                    "tab_id": previous["tab_id"],
                    "pane_id": previous["pane_id"],
                },
                separators=(",", ":"),
                sort_keys=True,
            )
            self.conn.execute(
                """INSERT INTO event_journal(
                    event_id,task_id,generation,task_version,session_id,turn_id,kind,
                    priority,payload_json,payload_sha256,produced_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?)""",
                (event_id, task_id, previous["run_generation"], values["task_version"],
                 previous["agent_session_id"], previous["native_turn_id"], "terminal_result",
                 values.get("priority", previous["priority"]), payload,
                 hashlib.sha256(payload.encode()).hexdigest(), values["updated_at"]),
            )
            self.conn.execute(
                "INSERT INTO completion_outbox(event_id,next_attempt_at) VALUES(?,?)",
                (event_id, values["updated_at"]),
            )
            self.conn.execute("UPDATE tasks SET terminal_event_id=? WHERE id=?", (event_id, task_id))
        self.conn.commit()
        row = self.task(task_id)
        if row is None:
            raise KeyError(task_id)
        return row

    def pending_completions(self, *, limit: int = 20, include_consumed: bool = False) -> list[dict[str, Any]]:
        states = ("pending", "leased", "dispatched", "delivered")
        if include_consumed:
            states += ("consumed", "response_produced")
        marks = ",".join("?" for _ in states)
        rows = self.conn.execute(
            f"""SELECT j.*,o.state AS delivery_state,o.attempt_count,o.dispatched_at,
                       o.delivered_at,o.consumed_at,o.response_produced_at,o.client_delivered_at
                FROM event_journal j JOIN completion_outbox o USING(event_id)
                WHERE o.state IN ({marks}) AND j.obsolete_at IS NULL
                ORDER BY j.produced_at LIMIT ?""", (*states, limit)
        ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item.pop("payload_json"))
            result.append(item)
        return result

    def lease_outbox(self) -> sqlite3.Row | None:
        ts = self.clock()
        row = self.conn.execute(
            """SELECT j.*,o.state AS delivery_state,o.attempt_count FROM event_journal j
               JOIN completion_outbox o USING(event_id)
               WHERE j.obsolete_at IS NULL AND o.state IN ('pending','dispatched','leased')
                 AND o.consumed_at IS NULL AND o.next_attempt_at<=?
                 AND (o.lease_expires_at IS NULL OR o.lease_expires_at<=?)
               ORDER BY j.produced_at LIMIT 1""", (ts, ts)
        ).fetchone()
        if row is None:
            return None
        token = uuid.uuid4().hex
        self.conn.execute(
            """UPDATE completion_outbox SET state='leased',lease_token=?,lease_expires_at=?,
                      attempt_count=attempt_count+1 WHERE event_id=?""",
            (token, ts + OUTBOX_LEASE_SECONDS, row["event_id"]),
        )
        self.conn.commit()
        return self.conn.execute(
            """SELECT j.*,o.state AS delivery_state,o.attempt_count,o.lease_token
               FROM event_journal j JOIN completion_outbox o USING(event_id) WHERE event_id=?""",
            (row["event_id"],),
        ).fetchone()

    def finish_dispatch(self, event_id: str, token: str, ok: bool, error: str | None = None) -> None:
        row = self.conn.execute(
            "SELECT attempt_count FROM completion_outbox WHERE event_id=? AND lease_token=?",
            (event_id, token),
        ).fetchone()
        if row is None:
            return
        ts = self.clock()
        # Give an accepted manager turn time to drain/ack before retrying. This
        # still replays after disconnect/crash without producing alert storms.
        delay = max(60.0, min(OUTBOX_RETRY_MAX_SECONDS, float(2 ** min(int(row["attempt_count"]), 8))))
        self.conn.execute(
            """UPDATE completion_outbox SET state=?,dispatched_at=CASE WHEN ? THEN ? ELSE dispatched_at END,
                   next_attempt_at=?,lease_token=NULL,lease_expires_at=NULL,last_error=? WHERE event_id=?""",
            ("dispatched" if ok else "pending", ok, ts, ts + delay, error, event_id),
        )
        self.conn.commit()

    def ack_completion(self, event_id: str, stage: str, source: str, evidence_id: str | None,
                       manager_turn_id: str | None) -> dict[str, Any]:
        allowed = ("delivered", "consumed", "response_produced", "client_delivered")
        if stage not in allowed:
            raise ValueError(f"stage must be one of {', '.join(allowed)}")
        row = self.conn.execute(
            "SELECT j.*,o.state AS delivery_state FROM event_journal j JOIN completion_outbox o USING(event_id) WHERE event_id=?",
            (event_id,),
        ).fetchone()
        if row is None:
            raise ValueError(f"completion event {event_id!r} does not exist")
        ordering = {"pending": -2, "leased": -1, "dispatched": 0, **{name: index + 1 for index, name in enumerate(allowed)}}
        if stage != "delivered" and ordering[stage] > ordering.get(row["delivery_state"], -2) + 1:
            raise ValueError("completion acknowledgment is out of order")
        ts = self.clock()
        evidence_id = evidence_id or f"{source}:{stage}"
        ack_id = f"ack-{uuid.uuid4().hex}"
        self.conn.execute(
            """INSERT OR IGNORE INTO completion_acks
               (ack_id,event_id,stage,trusted_source,manager_turn_id,evidence_id,created_at)
               VALUES(?,?,?,?,?,?,?)""",
            (ack_id,event_id,stage,source,manager_turn_id,evidence_id,ts),
        )
        column = stage + "_at"
        current_order = ordering.get(row["delivery_state"], -2)
        next_state = stage if ordering[stage] > current_order else row["delivery_state"]
        self.conn.execute(
            f"UPDATE completion_outbox SET state=?,{column}=COALESCE({column},?),lease_token=NULL,lease_expires_at=NULL WHERE event_id=?",
            (next_state, ts, event_id),
        )
        self.conn.commit()
        if stage == "consumed":
            event = self.conn.execute(
                "SELECT j.payload_json,j.priority FROM event_journal j WHERE j.event_id=?", (event_id,)
            ).fetchone()
            if event:
                context = self.presentation_context(event_id)
                presentation = completion_presentation(json.loads(event["payload_json"]), event["priority"], **context)
                self.conn.execute(
                    """INSERT INTO completion_presentations(semantic_key,first_event_id,last_event_id,first_consumed_at,last_consumed_at)
                       VALUES(?,?,?,?,?) ON CONFLICT(semantic_key) DO UPDATE SET
                       last_event_id=excluded.last_event_id,last_consumed_at=excluded.last_consumed_at""",
                    (presentation["semantic_key"], event_id, event_id, ts, ts),
                )
                self.conn.commit()
        return dict(self.conn.execute(
            "SELECT * FROM completion_outbox WHERE event_id=?", (event_id,)
        ).fetchone())

    def presentation_context(self, event_id: str) -> dict[str, str | None]:
        """Return non-spoken typed feature evidence for this immutable event."""
        row = self.conn.execute(
            """SELECT s.stage,s.evidence_json,t.work_kind,t.command_exit_status FROM event_journal j
               JOIN tasks t ON t.id=j.task_id
               LEFT JOIN feature_stages s ON s.task_id=j.task_id
               WHERE j.event_id=? ORDER BY s.ordinal LIMIT 1""",
            (event_id,),
        ).fetchone()
        if row is None:
            return {"stage": None, "evidence_kind": None, "work_kind": None, "command_exit_status": None}
        evidence = json.loads(row["evidence_json"]) if row["evidence_json"] else {}
        return {"stage": row["stage"], "evidence_kind": evidence.get("kind"),
                "work_kind": row["work_kind"], "command_exit_status": row["command_exit_status"]}

    def deliver_inbox(self, limit: int = 20) -> list[dict[str, Any]]:
        items = self.pending_completions(limit=limit)
        for item in items:
            context = self.presentation_context(item["event_id"])
            presentation = completion_presentation(item["payload"], item["priority"], **context)
            item["presentation_context"] = context
            seen = self.conn.execute(
                "SELECT first_event_id FROM completion_presentations WHERE semantic_key=?",
                (presentation["semantic_key"],),
            ).fetchone()
            if seen and seen["first_event_id"] != item["event_id"]:
                presentation["material"] = False
                presentation["duplicate_of_event_id"] = seen["first_event_id"]
            item["presentation"] = presentation
            if item["delivery_state"] in {"pending", "leased", "dispatched"}:
                self.ack_completion(
                    item["event_id"], "delivered", "manager_mcp",
                    f"inbox:{item['event_id']}", None,
                )
                item["delivery_state"] = "delivered"
        return items

    def record_manager_response(self, manager_turn_id: str, item_id: str, text: str,
                                event_ids: list[str] | None = None) -> list[str]:
        """Record response production from trusted structured IDs when present.

        Text matching is retained solely for legacy app-server observations. New
        voice-safe delivery must not include identifiers in spoken text.
        """
        matched: list[str] = []
        requested = set(event_ids or [])
        for item in self.pending_completions(limit=50, include_consumed=True):
            if requested:
                if item["event_id"] not in requested:
                    continue
            elif item["event_id"] not in text and item["task_id"] not in text:
                continue
            event_id = item["event_id"]
            state = self.conn.execute(
                "SELECT state FROM completion_outbox WHERE event_id=?", (event_id,)
            ).fetchone()["state"]
            if state in {"pending", "leased", "dispatched"}:
                self.ack_completion(event_id, "delivered", "manager_appserver", item_id, manager_turn_id)
                state = "delivered"
            if state == "delivered":
                self.ack_completion(event_id, "consumed", "manager_appserver", item_id, manager_turn_id)
            self.ack_completion(event_id, "response_produced", "manager_appserver", item_id, manager_turn_id)
            matched.append(event_id)
        return matched

    def set_target_workspace(self, target: str, workspace_id: str | None) -> None:
        self.conn.execute(
            "UPDATE targets SET workspace_id=?,updated_at=? WHERE target=?",
            (workspace_id, self.clock(), target),
        )
        self.conn.commit()

    def next_queued_codex(self) -> sqlite3.Row | None:
        return self.conn.execute(
            """SELECT * FROM tasks WHERE state='queued' AND work_kind='codex'
               ORDER BY CASE priority
                   WHEN 'critical' THEN 3 WHEN 'attention' THEN 2
                   WHEN 'normal' THEN 1 ELSE 0 END DESC,
                   created_at ASC LIMIT 1"""
        ).fetchone()

    def unsupported_work_kind_tasks(self) -> list[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM tasks WHERE work_kind NOT IN ('codex','command','xcsh')"
        ).fetchall()

    def native_turn_cursor(self, producer: str) -> int:
        row = self.conn.execute("SELECT last_revision FROM native_turn_cursors WHERE producer=?", (producer,)).fetchone()
        return int(row["last_revision"]) if row else 0

    def apply_native_turn(self, record: dict[str, Any]) -> tuple[str, str] | None:
        """Persist one verified Herdr semantic record exactly once.

        Global journal revision, execution/pane provenance, identity, and the
        completed-result digest are checked before a broker task changes state.
        Process exit and pane idle are deliberately irrelevant here.
        """
        report = record.get("report") if isinstance(record.get("report"), dict) else record
        producer = str(report.get("producer", ""))
        revision = record.get("revision")
        if not producer or not isinstance(revision, int) or revision <= 0:
            raise ValueError("native turn lacks producer/global revision")
        cursor_key = "__herdr_global__"
        cursor = self.native_turn_cursor(cursor_key)
        if revision <= cursor:
            return None
        if revision != cursor + 1:
            raise ValueError("native turn global revision gap")
        task_id = str(report.get("execution_id", ""))
        row = self.task(task_id)
        if row is None or row["work_kind"] != "xcsh":
            raise ValueError("native turn execution is not an admitted xcsh task")
        if row["pane_id"] != report.get("pane_id"):
            raise ValueError("native turn task identity/provenance mismatch")
        # The first observed semantic record may bind a freshly admitted XCSH
        # session/turn, but only from its owned execution pane and only as a
        # starting/working report. Later reports are exact identity matches.
        if row["native_turn_id"] is None:
            if str(report.get("state", "")) not in {"starting", "working"}:
                raise ValueError("native turn initial identity requires starting/working")
            if not report.get("session_id") or not report.get("turn_id"):
                raise ValueError("native turn initial identity is incomplete")
            if row["agent_session_id"] is not None and row["agent_session_id"] != report.get("session_id"):
                raise ValueError("native continuation session identity mismatch")
            row = self.update(task_id, event="xcsh_identity_bound",
                              agent_session_id=row["agent_session_id"] or report["session_id"], native_turn_id=report["turn_id"])
        if row["agent_session_id"] != report.get("session_id") or row["native_turn_id"] != report.get("turn_id"):
            raise ValueError("native turn task identity/provenance mismatch")
        if report.get("generation") != row["run_generation"]:
            raise ValueError("native turn generation mismatch")
        result = report.get("result")
        digest = report.get("result_digest")
        if result is not None:
            if not isinstance(result, str) or len(result) > MAX_SUMMARY or not isinstance(digest, str) or hashlib.sha256(result.encode()).hexdigest() != digest:
                raise ValueError("native completed result/digest mismatch")
        state = str(report.get("state", ""))
        mapped = {"starting": "starting", "working": "working", "waiting_input": "waiting_human", "completed": "completed", "failed": "failed", "cancelled": "cancelled", "interrupted": "unknown", "lost": "unknown"}.get(state)
        if mapped is None:
            raise ValueError("unknown native semantic state")
        if row["state"] in TERMINAL_STATES:
            raise ValueError("native turn attempted to change terminal task")
        summary = (result if state == "completed" else str(report.get("reason") or f"Native XCSH turn {state}."))[:MAX_SUMMARY]
        values: dict[str, Any] = {"state": mapped, "summary": summary, "output_excerpt": result if state == "completed" else row["output_excerpt"]}
        if mapped == "waiting_human": values["question"] = summary
        if mapped in TERMINAL_STATES: values["finished_at"] = self.clock(); values["terminal_reported_at"] = self.clock()
        self.update(task_id, event=f"xcsh_turn_{state}", **values)
        self.conn.execute("INSERT INTO native_turn_cursors(producer,last_revision,updated_at) VALUES(?,?,?) ON CONFLICT(producer) DO UPDATE SET last_revision=excluded.last_revision,updated_at=excluded.updated_at", (cursor_key, revision, self.clock()))
        self.conn.commit()
        return task_id, mapped

    def running_count(self) -> int:
        placeholders = ",".join("?" for _ in RUNNING_STATES)
        row = self.conn.execute(
            f"""SELECT COUNT(*) AS n FROM tasks
                WHERE state IN ({placeholders})
                   OR (herdr_state='working' AND state IN ('completed','failed','cancelled'))""",
            tuple(RUNNING_STATES),
        ).fetchone()
        return int(row["n"])

    def list_tasks(self, task_id: str | None = None) -> list[sqlite3.Row]:
        if task_id:
            row = self.task(task_id)
            return [] if row is None else [row]
        cutoff = self.clock() - SESSION_RETENTION_SECONDS
        return self.conn.execute(
            """SELECT * FROM tasks
               WHERE state NOT IN ('completed','failed','cancelled') OR finished_at>=?
               ORDER BY CASE state WHEN 'waiting_human' THEN 0 WHEN 'blocked' THEN 1
                            WHEN 'working' THEN 2 WHEN 'starting' THEN 3 WHEN 'queued' THEN 4 ELSE 5 END,
                        updated_at DESC""",
            (cutoff,),
        ).fetchall()

    def active_tasks(self) -> list[sqlite3.Row]:
        placeholders = ",".join("?" for _ in RUNNING_STATES)
        return self.conn.execute(
            f"SELECT * FROM tasks WHERE state IN ({placeholders})", tuple(RUNNING_STATES)
        ).fetchall()

    def reconcilable_tasks(self) -> list[sqlite3.Row]:
        placeholders = ",".join("?" for _ in RUNNING_STATES)
        return self.conn.execute(
            f"""SELECT * FROM tasks WHERE state IN ({placeholders})
                OR (state IN ('completed','failed','cancelled') AND herdr_state='working')
                OR (work_kind='command' AND state='unknown' AND terminal_reported_at IS NULL)""",
            tuple(RUNNING_STATES),
        ).fetchall()

    def task_for_pane(self, pane_id: str) -> sqlite3.Row | None:
        return self.conn.execute(
            "SELECT * FROM tasks WHERE pane_id=? ORDER BY created_at DESC LIMIT 1", (pane_id,)
        ).fetchone()

    def prune(self) -> int:
        cutoff = self.clock() - SESSION_RETENTION_SECONDS
        cur = self.conn.execute(
            """UPDATE tasks SET session_state='expired',agent_session_id=NULL,
                      agent_name=NULL,pane_id=NULL,tab_id=NULL,output_excerpt=NULL,
                      output_expires_at=NULL,cleanup_deadline=NULL,prompt_pending=NULL
               WHERE work_kind='codex' AND session_state!='expired' AND updated_at<?""",
            (cutoff,),
        )
        self.conn.execute(
            """UPDATE tasks SET output_excerpt=NULL,output_expires_at=NULL
               WHERE output_expires_at IS NOT NULL AND output_expires_at<?""",
            (self.clock(),),
        )
        self.conn.execute("DELETE FROM notifications WHERE last_sent_at<?", (cutoff,))
        self.conn.commit()
        return cur.rowcount

    def should_notify(self, task_id: str, state: str, summary: str, question: str | None) -> bool:
        digest = hashlib.sha256(f"{task_id}\0{state}\0{summary}\0{question or ''}".encode()).hexdigest()
        row = self.conn.execute("SELECT * FROM notifications WHERE dedupe_key=?", (digest,)).fetchone()
        ts = self.clock()
        if row and ts - row["last_sent_at"] < 300:
            self.conn.execute(
                "UPDATE notifications SET repeat_count=repeat_count+1 WHERE dedupe_key=?", (digest,)
            )
            self.conn.commit()
            return False
        self.conn.execute(
            """INSERT INTO notifications(dedupe_key,task_id,state,last_sent_at,repeat_count)
               VALUES(?,?,?,?,1)
               ON CONFLICT(dedupe_key) DO UPDATE SET last_sent_at=excluded.last_sent_at,repeat_count=1""",
            (digest, task_id, state, ts),
        )
        self.conn.commit()
        return True

    def record_manager_queue(
        self, task_id: str, state: str, summary: str, question: str | None
    ) -> None:
        digest = hashlib.sha256(
            f"{task_id}\0{state}\0{summary}\0{question or ''}".encode()
        ).hexdigest()
        self.conn.execute(
            """UPDATE notifications
               SET manager_queue_count=manager_queue_count+1,last_manager_queue_at=?
               WHERE dedupe_key=?""",
            (self.clock(), digest),
        )
        self.conn.commit()

    def record_transition(
        self,
        task_id: str,
        old_state: str | None,
        new_state: str,
        event: str,
        *,
        at: float | None = None,
    ) -> None:
        self.conn.execute(
            "INSERT INTO transitions(task_id,at,old_state,new_state,event) VALUES(?,?,?,?,?)",
            (task_id, self.clock() if at is None else at, old_state, new_state, event[:120]),
        )

    def transitions(self, task_id: str) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT at,old_state,new_state,event FROM transitions WHERE task_id=? ORDER BY seq", (task_id,)
        ).fetchall()
        return [dict(row) for row in rows]


class HerdrRPC:
    def __init__(self, socket_path: Path):
        self.socket_path = socket_path

    async def request(self, method: str, params: dict[str, Any] | None = None, timeout: float = 65) -> Any:
        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(str(self.socket_path), limit=4 * 1024 * 1024), timeout=5
        )
        request_id = f"control-{uuid.uuid4().hex}"
        payload = {"id": request_id, "method": method, "params": params or {}}
        writer.write(json.dumps(payload, separators=(",", ":")).encode() + b"\n")
        await writer.drain()
        try:
            while True:
                raw = await asyncio.wait_for(reader.readline(), timeout=timeout)
                if not raw:
                    raise RuntimeError("Herdr closed the socket before responding")
                message = json.loads(raw)
                if message.get("id") != request_id:
                    continue
                if "error" in message:
                    error = message["error"]
                    raise RuntimeError(f"Herdr {error.get('code', 'error')}: {error.get('message', error)}")
                return message.get("result")
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    async def subscribe(self, pane_ids: list[str]):
        reader, writer = await asyncio.open_unix_connection(str(self.socket_path), limit=4 * 1024 * 1024)
        request_id = f"control-sub-{uuid.uuid4().hex}"
        subscriptions = [
            {"type": "pane.updated"},
            {"type": "pane.exited"},
            {"type": "pane.closed"},
            {"type": "pane.agent_detected"},
        ]
        subscriptions.extend(
            {"type": "pane.agent_status_changed", "pane_id": pane_id} for pane_id in pane_ids
        )
        writer.write(
            json.dumps(
                {"id": request_id, "method": "events.subscribe", "params": {"subscriptions": subscriptions}},
                separators=(",", ":"),
            ).encode()
            + b"\n"
        )
        await writer.drain()
        ack = json.loads(await asyncio.wait_for(reader.readline(), timeout=10))
        if ack.get("id") != request_id or "error" in ack:
            writer.close()
            raise RuntimeError(f"Herdr event subscription failed: {ack}")
        return reader, writer


class Broker:
    def __init__(
        self,
        socket_path: Path,
        db_path: Path,
        herdr_socket: Path,
        config_path: Path,
        *,
        clock=now,
        cleanup_delay: float = CLEANUP_DELAY_SECONDS,
    ):
        self.socket_path = socket_path
        # Kept only to settle command rows launched by a pre-native broker
        # during a rolling upgrade. New commands use Herdr execution records.
        self.command_event_dir = socket_path.parent / "command-events"
        self.clock = clock
        self.db = StateDB(db_path, clock=clock)
        self.herdr = HerdrRPC(herdr_socket)
        self.config_path = config_path
        self.cleanup_delay = cleanup_delay
        self.scheduler_event = asyncio.Event()
        self.subscription_refresh = asyncio.Event()
        self.stopping = asyncio.Event()
        self.settle_timers: dict[str, asyncio.Task[None]] = {}
        self.native_turn_timers: dict[str, asyncio.Task[None]] = {}
        self.cleanup_timers: dict[str, asyncio.Task[None]] = {}
        self.start_tasks: dict[str, asyncio.Task[None]] = {}
        self.target_topology_locks: dict[str, asyncio.Lock] = {}
        self.control_topology_lock = asyncio.Lock()
        # The reserved manager binding is a single authority.  In particular,
        # a periodic inventory must not replace it while a supervisor-owned
        # native resume is waiting to become visible in Herdr.
        self.manager_topology_lock = asyncio.Lock()
        self.manager_reconnect_task: asyncio.Task[None] | None = None
        self.manager_launching = False
        self.manager_runtime_task: asyncio.Task[None] | None = None
        self.manager_runtime_process: asyncio.subprocess.Process | None = None
        self.manager_runtime_ready = asyncio.Event()

    def config(self) -> dict[str, Any]:
        if not self.config_path.exists():
            return {}
        try:
            return json.loads(self.config_path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            LOG.warning("cannot load %s: %s", self.config_path, exc)
            return {}

    def _appserver_env(self) -> dict[str, str]:
        config = self.config()
        env = os.environ.copy()
        socket_path = config.get("app_server_socket")
        remote = config.get("app_server_remote")
        if isinstance(socket_path, str) and socket_path:
            env["CODEX_APP_SERVER_SOCKET"] = socket_path
        if isinstance(remote, str) and remote:
            env["CODEX_APP_SERVER_REMOTE"] = remote
        return env

    def _verified_supervisor_attachment_claim(self, action_id: str, claim_key: str,
                                              owner_generation: str | None = None) -> str:
        """Return the durable capability digest for a replacement request.

        An owner-only broker socket is not, by itself, evidence that a request
        came from the supervisor action that owns recovery.  The supervisor's
        owner-only SQLite journal is therefore the authority for the action
        id, random claim capability and specialized action kind.
        """
        config=self.config()
        # ``supervisor_database`` is the portable/bootstrap contract. Retain
        # the early experimental spelling only for an already-written local
        # fixture; new deployments must not require an invented field.
        raw=str(config.get("supervisor_database") or config.get("supervisor_recovery_db_path") or "")
        if not raw:
            raise PermissionError("supervisor recovery journal path is not configured")
        path=Path(raw)
        try:
            info=path.stat()
            if (not path.is_file() or info.st_uid != os.getuid() or info.st_mode & 0o077):
                raise PermissionError("supervisor recovery journal has unsafe ownership or permissions")
            connection=sqlite3.connect(f"file:{path}?mode=ro",uri=True)
            connection.row_factory=sqlite3.Row
            try:
                row=connection.execute(
                    "SELECT action_id,claim_key,kind,state,owner_generation,lease_expires_at FROM recovery_actions WHERE action_id=?", (action_id,)
                ).fetchone()
                paused=connection.execute("SELECT 1 FROM recovery_settings WHERE key='paused'").fetchone() is not None
                owner=connection.execute("SELECT value FROM recovery_settings WHERE key='owner_generation'").fetchone()
            finally:
                connection.close()
        except PermissionError:
            raise
        except Exception as exc:
            raise RuntimeError(f"cannot authoritatively read supervisor recovery claim: {type(exc).__name__}: {exc}") from exc
        if paused:
            raise PermissionError("deployment owner paused recovery before this effect")
        if (row is None or row["kind"] != "recover_manager_binding" or row["state"] not in {"claimed","recovering"}
                or row["lease_expires_at"] is None or float(row["lease_expires_at"]) <= time.time()):
            raise PermissionError("recovery action is not an active manager-binding claim")
        if (owner_generation is not None and (
                not hmac.compare_digest(str(row["owner_generation"] or ""),owner_generation)
                or owner is None or not hmac.compare_digest(str(owner["value"] or ""),owner_generation))):
            raise PermissionError("recovery action belongs to a different supervisor generation")
        saved=str(row["claim_key"] or "")
        if not saved or not hmac.compare_digest(saved,claim_key):
            raise PermissionError("recovery claim capability is not owned by the active supervisor action")
        return hashlib.sha256(claim_key.encode()).hexdigest()

    def _idempotent_admission(self, method: str, params: dict[str, Any]) -> dict[str, Any] | None:
        """Return the original durable admission for a caller key.

        A lost socket response is not permission to submit another worker or
        shell.  Keys are deliberately caller supplied; reuse with a different
        request is rejected rather than silently creating ambiguous work.
        """
        key = params.get("idempotency_key")
        if key is None:
            return None
        key = bounded(key, 160, "idempotency_key", required=True)
        canonical = dict(params)
        canonical.pop("idempotency_key", None)
        digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        row = self.db.conn.execute("SELECT * FROM admission_idempotency WHERE idempotency_key=?", (key,)).fetchone()
        if row is None:
            return None
        if row["method"] != method or row["request_sha256"] != digest:
            raise ValueError("idempotency_key was already used for a different admission")
        task = self.db.task(row["task_id"])
        if task is None:
            raise RuntimeError("admission evidence was retained but its task has expired; execution is uncertain")
        return public_task(task) | {"admitted": False, "idempotency_replayed": True}

    def _remember_admission(self, method: str, params: dict[str, Any], task_id: str, *, commit: bool = True) -> None:
        key = params.get("idempotency_key")
        if key is None:
            return
        key = bounded(key, 160, "idempotency_key", required=True)
        canonical = dict(params)
        canonical.pop("idempotency_key", None)
        digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        self.db.conn.execute("INSERT INTO admission_idempotency(idempotency_key,method,request_sha256,task_id,created_at) VALUES(?,?,?,?,?)",
                             (key, method, digest, task_id, self.clock()))
        if commit:
            self.db.conn.commit()

    def _admit_task(self, method: str, params: dict[str, Any], data: dict[str, Any]) -> sqlite3.Row:
        """Atomically retain task and caller idempotency evidence.

        A crash must leave either neither record or both records.  A durable
        task without its idempotency mapping is ambiguous admission evidence
        and could otherwise be submitted a second time after a lost response.
        """
        if params.get("idempotency_key") is None:
            return self.db.add_task(data)
        self.db.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.db.add_task(data, commit=False)
            self._remember_admission(method, params, data["id"], commit=False)
            self.db.conn.commit()
            return row
        except Exception:
            self.db.conn.rollback()
            raise

    async def serve(self) -> None:
        self.db.prune()
        await self._fail_unsupported_work_kinds("startup recovery")
        ensure_directory(self.socket_path.parent)
        if self.socket_path.exists() or self.socket_path.is_socket():
            info = self.socket_path.lstat()
            if not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid():
                raise RuntimeError(f"refusing to replace unsafe socket path {self.socket_path}")
            self.socket_path.unlink()
        old_umask = os.umask(0o077)
        try:
            server = await asyncio.start_unix_server(self._client, path=str(self.socket_path), limit=MAX_REQUEST + 1)
        finally:
            os.umask(old_umask)
        os.chmod(self.socket_path, 0o600)

        await self.reconcile()
        for row in self.db.conn.execute(
            """SELECT * FROM tasks
               WHERE work_kind='codex' AND terminal_reported_at IS NOT NULL
                 AND state IN ('failed','completed','cancelled')
                 AND herdr_state NOT IN ('idle','done','closed')"""
        ).fetchall():
            self._schedule_reported_codex_reconcile(row["id"])
        self.manager_runtime_task = asyncio.create_task(
            self._manager_runtime_loop(), name="manager-runtime"
        )
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(self.manager_runtime_ready.wait(), timeout=20)
        for cleanup_row in self.db.conn.execute(
            "SELECT * FROM tasks WHERE cleanup_deadline IS NOT NULL"
        ).fetchall():
            delay = max(0.0, float(cleanup_row["cleanup_deadline"]) - self.clock())
            self.cleanup_timers[cleanup_row["id"]] = asyncio.create_task(
                self._cleanup_after(cleanup_row["id"], delay), name=f"cleanup-{cleanup_row['id']}"
            )
        scheduler = asyncio.create_task(self._scheduler(), name="scheduler")
        command_spooler = asyncio.create_task(
            self._command_event_spool_loop(), name="command-event-spool"
        )
        subscriber = asyncio.create_task(self._event_loop(), name="herdr-events")
        outbox_dispatcher = asyncio.create_task(self._outbox_loop(), name="completion-outbox")
        self.scheduler_event.set()
        LOG.info("listening on %s", self.socket_path)
        try:
            async with server:
                await self.stopping.wait()
        finally:
            runtime_process = self.manager_runtime_process
            server.close()
            await server.wait_closed()
            scheduler.cancel()
            command_spooler.cancel()
            subscriber.cancel()
            outbox_dispatcher.cancel()
            for task in self.settle_timers.values():
                task.cancel()
            for task in self.native_turn_timers.values():
                task.cancel()
            for task in self.cleanup_timers.values():
                task.cancel()
            for task in self.start_tasks.values():
                task.cancel()
            if self.manager_reconnect_task:
                self.manager_reconnect_task.cancel()
            if self.manager_runtime_task:
                self.manager_runtime_task.cancel()
            if runtime_process and runtime_process.returncode is None:
                runtime_process.terminate()
            await asyncio.gather(
                scheduler,
                command_spooler,
                subscriber,
                outbox_dispatcher,
                *self.settle_timers.values(),
                *self.native_turn_timers.values(),
                *self.cleanup_timers.values(),
                *self.start_tasks.values(),
                *([self.manager_reconnect_task] if self.manager_reconnect_task else []),
                *([self.manager_runtime_task] if self.manager_runtime_task else []),
                return_exceptions=True,
            )
            if runtime_process:
                try:
                    await asyncio.wait_for(runtime_process.wait(), timeout=5)
                except asyncio.TimeoutError:
                    runtime_process.kill()
                    await runtime_process.wait()
            with contextlib.suppress(FileNotFoundError):
                self.socket_path.unlink()
            self.db.close()

    def stop(self) -> None:
        self.stopping.set()

    async def _manager_runtime_loop(self) -> None:
        """Hold the observer only while the broker is its configured owner.

        Ownership is deliberately reread between bounded waits: guarded
        handoff/rollback changes the machine binding without restarting this
        broker.  A broker which has yielded ownership remains available for a
        *claimed* supervisor topology reconciliation, but never independently
        relaunches the manager.
        """
        delay = 1.0
        while not self.stopping.is_set():
            config = self.config()
            if config.get("supervisor_owns_recovery"):
                process = self.manager_runtime_process
                if process is not None and process.returncode is None:
                    LOG.info("yielding manager observer ownership to recovery supervisor")
                    process.terminate()
                    try:
                        await asyncio.wait_for(process.wait(), timeout=5)
                    except asyncio.TimeoutError:
                        process.kill()
                        await process.wait()
                if self.manager_runtime_process is process:
                    self.manager_runtime_process = None
                self.manager_runtime_ready.set()
                # Do not return here. Rollback can restore broker ownership in
                # the same process, and must restart the bounded observer.
                try:
                    await asyncio.wait_for(self.stopping.wait(), timeout=1)
                except asyncio.TimeoutError:
                    pass
                continue
            thread_id = config.get("manager_thread_id")
            if not thread_id:
                self.manager_runtime_ready.set()
                try:
                    await asyncio.wait_for(self.stopping.wait(), timeout=1)
                except asyncio.TimeoutError:
                    pass
                continue
            process = None
            try:
                process = await asyncio.create_subprocess_exec(
                    "/usr/bin/python3",
                    str(runtime_root() / "appserver_manager.py"),
                    "hold",
                    str(thread_id),
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.STDOUT,
                    env=self._appserver_env()
                    | {
                        "CODEX_CONTROL_CONFIG_PATH": str(self.config_path),
                        "CODEX_CONTROL_MANAGER_CWD": str(config.get("manager_cwd", self.config_path.parent)),
                        "CODEX_CONTROL_HERDR_SOCKET": str(self.herdr.socket_path),
                        "CONTROL_BROKER_SOCKET": str(self.socket_path),
                    },
                )
                self.manager_runtime_process = process
                if process.stdout is None:
                    raise RuntimeError("manager runtime has no status pipe")
                raw = await asyncio.wait_for(process.stdout.readline(), timeout=30)
                status = json.loads(raw)
                if not status.get("ready") or status.get("thread_id") != thread_id:
                    raise RuntimeError(f"unexpected manager runtime status: {status}")
                self.manager_runtime_ready.set()
                LOG.info("holding canonical Control Manager runtime %s", thread_id)
                delay = 1.0
                while not self.stopping.is_set() and process.returncode is None:
                    if self.config().get("supervisor_owns_recovery"):
                        LOG.info("yielding manager observer ownership to recovery supervisor")
                        process.terminate()
                        try:
                            await asyncio.wait_for(process.wait(), timeout=5)
                        except asyncio.TimeoutError:
                            process.kill()
                            await process.wait()
                        break
                    try:
                        await asyncio.wait_for(process.wait(), timeout=1)
                    except asyncio.TimeoutError:
                        pass
                if process.returncode is not None and not self.stopping.is_set() and not self.config().get("supervisor_owns_recovery"):
                    LOG.warning("manager runtime exited with status %s; retrying", process.returncode)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("manager runtime unavailable: %s; retrying in %.1fs", exc, delay)
            finally:
                if self.manager_runtime_process is process:
                    self.manager_runtime_process = None
            if self.config().get("supervisor_owns_recovery"):
                delay = 1.0
                continue
            await asyncio.sleep(delay)
            delay = min(30.0, delay * 2)

    async def _client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            sock = writer.get_extra_info("socket")
            if sock is not None and hasattr(socket, "SO_PEERCRED"):
                _pid, uid, _gid = struct.unpack("3i", sock.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12))
                if uid != os.getuid():
                    raise PermissionError("peer uid is not the broker owner")
            raw = await asyncio.wait_for(reader.readline(), timeout=30)
            if not raw or len(raw) > MAX_REQUEST:
                raise ValueError("request is empty or too large")
            request = json.loads(raw)
            if not isinstance(request, dict):
                raise ValueError("request must be a JSON object")
            result = await self.handle(request.get("method"), request.get("params") or {})
            response = {"ok": True, "result": result}
        except Exception as exc:
            LOG.info("request failed: %s", exc)
            response = {"ok": False, "error": str(exc)}
        writer.write(json.dumps(response, separators=(",", ":")).encode() + b"\n")
        with contextlib.suppress(Exception):
            await writer.drain()
        writer.close()
        with contextlib.suppress(Exception):
            await writer.wait_closed()

    async def handle(self, method: Any, params: Any) -> Any:
        if not isinstance(method, str) or not isinstance(params, dict):
            raise ValueError("method and params are required")
        if method == "ping":
            config = self.config()
            return {"status": "ok", "running": self.db.running_count(), "capabilities": {
                "native_xcsh_admit": True,
                "native_turn_consumer": bool(config.get("agent_turn_consumer_enabled")),
                "native_turn_producer": str(config.get("agent_turn_producer", "xcsh")),
                "native_turn_cursor": self.db.native_turn_cursor("__herdr_global__"),
            }}
        if method == "dispatch":
            return await self.dispatch(params)
        if method == "native_xcsh_admit":
            return await self.native_xcsh_admit(params)
        if method == "create_feature":
            return await self.create_feature(params)
        if method == "feature_status":
            feature_id = bounded(params.get("feature_id"), 120, "feature_id", required=True)
            return self.db.feature(feature_id or "")
        if method == "ensure_feature_dependency":
            feature_id = bounded(params.get("feature_id"), 120, "feature_id", required=True)
            stage = bounded(params.get("stage"), 40, "stage", required=True)
            before_stage = bounded(params.get("before_stage"), 40, "before_stage", required=True)
            blocker = bounded(params.get("blocker"), 500, "blocker", required=True)
            result = self.db.ensure_feature_dependency(feature_id or "", stage or "", before_stage=before_stage or "", blocker=blocker or "")
            self.db.sync_all_feature_child_states()
            return self.db.feature(feature_id or "")
        if method == "record_feature_evidence":
            feature_id = bounded(params.get("feature_id"), 120, "feature_id", required=True)
            stage = bounded(params.get("stage"), 40, "stage", required=True)
            kind = bounded(params.get("kind"), 40, "kind", required=True)
            evidence_id = bounded(params.get("evidence_id"), 500, "evidence_id", required=True)
            result = self.db.record_feature_evidence(feature_id or "", stage or "", kind or "", evidence_id or "")
            await self.advance_feature(feature_id or "")
            return result
        if method == "record_feature_observation":
            feature_id = bounded(params.get("feature_id"), 120, "feature_id", required=True)
            stage = bounded(params.get("stage"), 40, "stage", required=True)
            kind = bounded(params.get("kind"), 40, "kind", required=True)
            evidence_id = bounded(params.get("evidence_id"), 500, "evidence_id", required=True)
            detail = bounded(params.get("detail"), 2000, "detail", required=True)
            self.db.record_feature_observation(feature_id or "", stage or "", kind or "", evidence_id or "", detail or "")
            return {"recorded": True}
        if method == "set_feature_stage_action":
            feature_id = bounded(params.get("feature_id"), 120, "feature_id", required=True)
            stage = bounded(params.get("stage"), 40, "stage", required=True)
            action = params.get("action")
            if not isinstance(action, dict):
                raise ValueError("action must be an object")
            self.db.set_feature_stage_action(feature_id or "", stage or "", action)
            return self.db.feature(feature_id or "")
        if method == "retry_feature_stage":
            feature_id = bounded(params.get("feature_id"), 120, "feature_id", required=True)
            stage = bounded(params.get("stage"), 40, "stage", required=True)
            blocker = bounded(params.get("blocker"), 500, "blocker", required=True)
            self.db.retry_feature_stage(feature_id or "", stage or "", blocker or "")
            return self.db.feature(feature_id or "")
        if method == "advance_feature":
            feature_id = bounded(params.get("feature_id"), 120, "feature_id", required=True)
            await self.advance_feature(feature_id or "")
            return self.db.feature(feature_id or "")
        if method == "run_command":
            return await self.run_command(params)
        if method == "continue_task":
            return await self.continue_task(params)
        if method == "status":
            return self.status(params.get("task_id"))
        if method == "completion_inbox":
            limit = params.get("limit", 20)
            if not isinstance(limit, int) or not 1 <= limit <= 50:
                raise ValueError("limit must be an integer from 1 through 50")
            return {"completions": self.db.deliver_inbox(limit)}
        if method == "ack_completion":
            event_id = bounded(params.get("event_id"), 80, "event_id", required=True)
            stage = params.get("stage")
            source = bounded(params.get("source") or "manager_mcp", 80, "source", required=True)
            evidence_id = bounded(params.get("evidence_id"), 160, "evidence_id")
            manager_turn_id = bounded(params.get("manager_turn_id"), 80, "manager_turn_id")
            result = self.db.ack_completion(event_id or "", stage, source or "manager_mcp", evidence_id, manager_turn_id)
            # The result is already durable.  Replayed acknowledgements are
            # harmless because stage claims are transactional and unique.
            if stage == "consumed":
                event = self.db.conn.execute("SELECT task_id FROM event_journal WHERE event_id=?", (event_id,)).fetchone()
                if event:
                    await self.advance_for_task(event["task_id"])
            return result
        if method == "manager_response":
            manager_turn_id = bounded(params.get("manager_turn_id"), 80, "manager_turn_id", required=True)
            item_id = bounded(params.get("item_id"), 160, "item_id", required=True)
            text = bounded(params.get("text"), MAX_PROMPT, "text", required=True)
            event_ids = params.get("event_ids")
            if event_ids is not None:
                if not isinstance(event_ids, list) or not 1 <= len(event_ids) <= 50:
                    raise ValueError("event_ids must be a non-empty array of at most 50 event IDs")
                event_ids = [bounded(value, 80, "event_id", required=True) for value in event_ids]
            return {"matched_event_ids": self.db.record_manager_response(
                manager_turn_id or "", item_id or "", text or "", event_ids
            )}
        if method == "reply":
            return await self.reply(params)
        if method == "request_stop":
            return await self.request_stop(params)
        if method == "report":
            return await self.report(params)
        if method == "command_event":
            return await self.command_event(params)
        if method == "consume_native_turns":
            return await self.consume_native_turns()
        if method == "reconcile":
            # The independent supervisor invokes this before a verified
            # same-manager continuation. It is idempotent and re-reads durable
            # task/feature state; it never replays a user request.
            await self.reconcile(strict=True)
            return {"reconciled": True, "running": self.db.running_count()}
        if method == "reconcile_topology":
            # This method is reached only through the owner-only broker socket
            # by a supervisor action already globally claimed in its durable
            # journal.  It may reattach the *exact* canonical thread when
            # Herdr reports its reserved pane missing/shell, but it cannot
            # create a manager or replay a model request independently.
            action_id = bounded(params.get("recovery_action_id"), 80, "recovery_action_id", required=True)
            if not re.fullmatch(r"recovery-[0-9a-f]{32}", action_id or ""):
                raise ValueError("invalid supervisor recovery action id")
            claim_sha256=None
            claim_owner_generation=None
            if params.get("recovery_claim_key") is not None:
                claim_key = bounded(params.get("recovery_claim_key"), 160, "recovery_claim_key", required=True)
                if not re.fullmatch(r"[0-9a-f]{64}", claim_key or ""):
                    raise ValueError("invalid supervisor recovery claim capability")
                claim_owner_generation=bounded(params.get("recovery_owner_generation"),80,"recovery_owner_generation",required=True)
                if not re.fullmatch(r"[0-9a-f]{32}",claim_owner_generation or ""):
                    raise ValueError("invalid supervisor owner generation")
                claim_sha256=self._verified_supervisor_attachment_claim(action_id,claim_key,claim_owner_generation)
            evidence = await self.reconcile(strict=True, allow_supervisor_reattach=True,
                                            require_manager_reattach=True,
                                            recovery_action_id=action_id,
                                            recovery_claim_sha256=claim_sha256,
                                            recovery_claim_key=claim_key if claim_sha256 else None,
                                            recovery_owner_generation=claim_owner_generation)
            if evidence.get("manager_reattach", {}).get("state") != "verified":
                raise RuntimeError(f"canonical manager topology is unverified: {evidence.get('manager_reattach')}")
            return {"reconciled": True, "topology_reattach": "verified", "recovery_action_id": action_id, "running": self.db.running_count()}
        raise ValueError(f"unknown method {method!r}")

    async def dispatch(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self._idempotent_admission("dispatch", params)
        if existing is not None:
            return existing
        target = bounded(params.get("target"), 64, "target", required=True)
        if not TARGET_RE.fullmatch(target or ""):
            raise ValueError("target must match [A-Za-z0-9][A-Za-z0-9_.-]{0,63}")
        cwd = normalized_cwd(params.get("cwd", ""))
        priority = params.get("priority", "normal")
        if priority not in PRIORITIES:
            raise ValueError(f"priority must be one of {', '.join(PRIORITIES)}")
        prompt = bounded(params.get("prompt"), MAX_PROMPT, "prompt", required=True)
        model = params.get("model", "gpt-5.6-sol")
        if model not in WORKER_MODELS:
            raise ValueError(
                "delegated model must be one of " + ", ".join(WORKER_MODELS)
            )
        reasoning_effort = params.get("reasoning_effort") or WORKER_MODELS[model]
        if reasoning_effort not in REASONING_EFFORTS:
            raise ValueError(
                "reasoning_effort must be one of " + ", ".join(sorted(REASONING_EFFORTS))
            )
        parent_id = params.get("parent_id")
        if parent_id is not None:
            parent_id = bounded(parent_id, 80, "parent_id", required=True)
            if self.db.task(parent_id) is None:
                raise ValueError(f"parent task {parent_id!r} does not exist")
        task_id = f"ctl-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
        one_line = " ".join((prompt or "").split())
        row = self._admit_task("dispatch", params,
            {
                "id": task_id,
                "parent_id": parent_id,
                "target": target,
                "cwd": cwd,
                "priority": priority,
                "prompt": prompt,
                "summary": f"Queued: {one_line[:480]}",
                "work_kind": "codex",
                "model": model,
                "reasoning_effort": reasoning_effort,
            }
        )
        result = public_task(row)
        self.scheduler_event.set()
        return result

    async def native_xcsh_admit(self, params: dict[str, Any]) -> dict[str, Any]:
        """Atomically admit one installed XCSH execution through Herdr.

        This is intentionally separate from Codex dispatch.  It records a
        durable task/idempotency claim before ``execution.start`` and waits for
        protocol-20 semantic reports to settle it; process exit/output is not
        task success evidence.
        """
        existing = self._idempotent_admission("native_xcsh_admit", params)
        if existing is not None:
            return existing
        target = bounded(params.get("target"), 64, "target", required=True)
        if not TARGET_RE.fullmatch(target or ""):
            raise ValueError("target must match task target rules")
        cwd = normalized_cwd(params.get("cwd", ""))
        prompt = bounded(params.get("prompt"), MAX_PROMPT, "prompt", required=True)
        priority = params.get("priority", "normal")
        if priority not in PRIORITIES:
            raise ValueError(f"priority must be one of {', '.join(PRIORITIES)}")
        workspace_id = bounded(params.get("workspace_id"), 160, "workspace_id", required=True)
        argv = params.get("argv")
        if not isinstance(argv, list) or not 1 <= len(argv) <= 64 or any(not isinstance(value, str) or not value or len(value) > 4096 for value in argv):
            raise ValueError("argv must contain 1..64 non-empty bounded strings")
        runtime_identity = params.get("runtime_identity")
        if not isinstance(runtime_identity, dict):
            raise ValueError("runtime_identity object is required")
        required_identity = ("xcsh_artifact", "herdr_artifact", "manager_artifact")
        if any(not isinstance(runtime_identity.get(key), str) or not runtime_identity[key] for key in required_identity):
            raise ValueError("runtime_identity must bind xcsh_artifact, herdr_artifact, and manager_artifact")
        persisted_identity = runtime_identity | {
            "workspace_id": workspace_id,
            "argv_sha256": hashlib.sha256(json.dumps(argv, separators=(",", ":")).encode()).hexdigest(),
        }
        identity_json = json.dumps(persisted_identity, sort_keys=True, separators=(",", ":"))
        if len(identity_json) > 4000:
            raise ValueError("runtime_identity is too large")
        try:
            await self.herdr.request("workspace.get", {"workspace_id": workspace_id}, timeout=10)
        except Exception as exc:
            raise ValueError(f"native XCSH workspace is not available: {exc}") from exc
        task_id = f"xut-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
        row = self._admit_task("native_xcsh_admit", params, {
            "id": task_id, "parent_id": None, "target": target, "cwd": cwd,
            "priority": priority, "prompt": prompt,
            "summary": "Installed XCSH semantic UAT admission claimed.", "work_kind": "xcsh",
            "native_runtime_json": identity_json,
        })
        self.db.update(row["id"], event="xcsh_admission_claimed", state="starting", started_at=self.clock(),
                       workspace_id=workspace_id, agent_kind="xcsh", herdr_state="admitting",
                       session_state="admitting", native_runtime_json=identity_json)
        try:
            result = await self.herdr.request("execution.start", {
                "execution_id": row["id"], "workspace_id": workspace_id, "cwd": cwd,
                "label": "xcsh-native-uat", "mode": "argv", "argv": argv,
            }, timeout=30)
            execution = result.get("execution", result)
            if execution.get("execution_id") != row["id"] or not execution.get("pane_id") or not execution.get("tab_id"):
                raise RuntimeError("Herdr returned incomplete native execution provenance")
            updated = self.db.update(row["id"], event="xcsh_execution_admitted", state="starting",
                                     pane_id=execution["pane_id"], tab_id=execution["tab_id"],
                                     herdr_state=execution.get("state", "starting"), session_state="live",
                                     summary="Installed XCSH execution admitted; awaiting semantic journal identity.")
            return public_task(updated) | {"admitted": bool(result.get("admitted", True))}
        except Exception as exc:
            # A transport failure can happen after Herdr creates the process.
            # Preserve the single claim and require journal/reconciliation; do
            # not retry into a second XCSH process or mark it as a failure.
            updated = self.db.update(row["id"], event="xcsh_admission_unverified", state="unknown",
                                     herdr_state="unknown", session_state="unknown",
                                     summary=f"Installed XCSH admission outcome is unverified: {str(exc)[:900]}")
            asyncio.create_task(self.emit_attention(updated))
            return public_task(updated) | {"admitted": False, "admission_uncertain": True}

    async def create_feature(self, params: dict[str, Any]) -> dict[str, Any]:
        feature_id = bounded(params.get("feature_id"), 120, "feature_id", required=True)
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,119}", feature_id or ""):
            raise ValueError("feature_id contains unsupported characters")
        target = bounded(params.get("target"), 64, "target", required=True)
        if not TARGET_RE.fullmatch(target or ""): raise ValueError("target must match task target rules")
        cwd = normalized_cwd(params.get("cwd", ""))
        title = bounded(params.get("title"), 500, "title", required=True)
        scope = params.get("scope", "plan_only")
        actions = params.get("actions", {})
        children = params.get("children", {})
        if not isinstance(actions, dict): raise ValueError("actions must be an object")
        if not isinstance(children, dict): raise ValueError("children must be an object")
        for stage, action in actions.items():
            if stage not in WORKFLOW_STAGES or not isinstance(action, dict): raise ValueError("invalid stage action")
            bounded(action.get("prompt"), MAX_PROMPT, f"{stage} action prompt", required=True)
        feature = self.db.create_feature({"feature_id": feature_id, "target": target, "cwd": cwd, "title": title,
                                          "scope": scope, "actions": actions, "children": children,
                                          "required_stages": params.get("required_stages", WORKFLOW_STAGES)})
        await self.advance_feature(feature_id or "")
        return feature

    async def advance_for_task(self, task_id: str) -> None:
        rows = self.db.conn.execute("SELECT DISTINCT feature_id FROM feature_stages WHERE task_id=?", (task_id,)).fetchall()
        for row in rows:
            await self.advance_feature(row["feature_id"])

    async def advance_feature(self, feature_id: str) -> None:
        """Launch at most one claimed next step.  The claim survives a crash;
        manual recovery can see a blocked claim instead of silently retrying."""
        claim = self.db.claim_next_feature_action(feature_id)
        if not claim:
            return
        action = claim["action"]
        try:
            # Lifecycle-generated workers inherit the versioned user policy.
            # This makes a stale repository instruction unable to reintroduce
            # a human wording-approval pause into an otherwise authorized flow.
            if shared_commit_message_policy() != AUTONOMOUS_COMMIT_MESSAGE_POLICY:
                raise RuntimeError("unsupported shared commit-message policy")
            binding = self.db.feature_prompt_binding(feature_id)
            artifacts = binding["artifact_identities"]
            artifact_text = json.dumps(artifacts, sort_keys=True)
            prompt = (
                f"{action['prompt']}\n\n"
                f"DURABLE FEATURE BINDING (do not substitute another feature, PR, branch, or artifact): "
                f"feature={binding['feature_id']}; acceptance={binding['title']}; target={binding['target']}; "
                f"stage={claim['stage']}; authoritative artifact observations={artifact_text}. "
                "If these identities are insufficient for the requested gate, report the exact blocker; do not review or validate a different target.\n\n"
                f"{AUTONOMOUS_COMMIT_MESSAGE_GUIDANCE}"
            )
            child = await self.dispatch({
                "target": action.get("target", claim["target"]), "cwd": action.get("cwd", claim["cwd"]),
                "priority": action.get("priority", "normal"), "prompt": prompt,
                "model": action.get("model", "gpt-5.6-sol"), "reasoning_effort": action.get("reasoning_effort"),
            })
            self.db.attach_feature_task(feature_id, claim["stage"], child["id"])
        except Exception as exc:
            self.db.fail_feature_claim(claim, f"automatic dispatch failed: {exc}")
            raise

    async def consume_native_turns(self) -> dict[str, Any]:
        """Consume the supported Herdr protocol-20 agent-turn journal.

        Disabled unless the machine binding explicitly enables it: a running
        protocol-18 Herdr must never be guessed into a semantic integration.
        """
        config = self.config()
        if not config.get("agent_turn_consumer_enabled"):
            return {"enabled": False, "applied": []}
        producer = str(config.get("agent_turn_producer", "xcsh"))
        cursor_key = "__herdr_global__"
        cursor = self.db.native_turn_cursor(cursor_key)
        pong = await self.herdr.request("ping", {}, timeout=10)
        caps = (pong or {}).get("capabilities") or {}
        if int((pong or {}).get("protocol", 0)) < 20 or not caps.get("agent_turn_journal"):
            raise RuntimeError("Herdr agent-turn journal capability is unavailable")
        result = await self.herdr.request("agent.turn.list", {"since_revision": cursor}, timeout=30)
        turns = (result or {}).get("turns", [])
        if not isinstance(turns, list):
            raise RuntimeError("Herdr returned invalid agent-turn list")
        applied: list[dict[str, str]] = []
        for record in turns:
            if not isinstance(record, dict): raise RuntimeError("Herdr returned invalid agent-turn record")
            report = record.get("report") if isinstance(record.get("report"), dict) else record
            revision = record.get("revision")
            if not isinstance(revision, int) or revision <= cursor:
                continue
            if revision != cursor + 1:
                raise RuntimeError("Herdr global agent-turn revision gap")
            if report.get("producer") != producer:
                self.db.conn.execute("INSERT INTO native_turn_cursors(producer,last_revision,updated_at) VALUES(?,?,?) ON CONFLICT(producer) DO UPDATE SET last_revision=excluded.last_revision,updated_at=excluded.updated_at", (cursor_key, revision, self.clock()))
                self.db.conn.commit()
                cursor = revision
                continue
            changed = self.db.apply_native_turn(record)
            cursor = revision
            if changed:
                task_id, state = changed
                if state in TERMINAL_STATES or state == "unknown":
                    self.db.feature_task_terminal(task_id, state, self.db.task(task_id)["summary"])
                    await self.advance_for_task(task_id)
                applied.append({"task_id": task_id, "state": state})
        return {"enabled": True, "applied": applied, "last_revision": self.db.native_turn_cursor(cursor_key)}

    def status(self, task_id: str | None) -> dict[str, Any]:
        if task_id is not None:
            bounded(task_id, 80, "task_id", required=True)
        rows = self.db.list_tasks(task_id)
        if task_id and not rows:
            raise ValueError(f"task {task_id!r} does not exist")
        counts = {state: 0 for state in TASK_STATES}
        for row in rows:
            counts[row["state"]] += 1
        return {
            "tasks": [
                public_task(row)
                | {
                    "transitions": self.db.transitions(row["id"]),
                    "followups": self.db.followups(row["id"]),
                    "preserved_legacy_followups": self.db.preserved_legacy_followups(row["id"]),
                }
                for row in rows
            ],
            "counts": counts,
            "running": self.db.running_count(),
            "pending_completions": self.db.pending_completions(limit=50, include_consumed=True),
        }

    async def _outbox_loop(self) -> None:
        """At-least-once nudge into the canonical manager thread.

        Queue success is deliberately only `dispatched`; the manager must fetch
        the durable inbox before it becomes `delivered`.
        """
        while not self.stopping.is_set():
            row = self.db.lease_outbox()
            if row is None:
                await asyncio.sleep(1)
                continue
            event_id = row["event_id"]
            token = row["lease_token"]
            try:
                payload = json.loads(row["payload_json"])
                config = self.config()
                thread_id = config.get("manager_thread_id")
                if not thread_id:
                    raise RuntimeError("canonical manager thread is not configured")
                message = (
                    "A durable Control completion is available. "
                    "Call control_broker.completion_inbox now, then call control_broker.ack_completion(stage=consumed) "
                    "for every incorporated event. In user-facing or voice delivery, use only the event presentation: "
                    "synthesize material outcome, significance, any human blocker, and next action. Do not read worker "
                    "prose verbatim or speak task/event IDs, hashes, pane details, paths, or meaningless counts. Suppress "
                    "presentation.material=false duplicates while still consuming them durably. Before any claim that this "
                    "task is still working, read fresh control_broker.status. A consume acknowledgment proves runtime "
                    "handling, not client display or human cognition; the current app-server has no guaranteed structured "
                    "response metadata, so never claim response/client/voice delivery without it. "
                    "Never run control_client/controlctl through run_command for inbox maintenance. If this "
                    "event is absent from completion_inbox, it is a stale accepted duplicate: do not re-report it."
                )
                await self._run_required(
                    [configured_codex_binary(config), "queue", "--remote",
                     str(config.get("app_server_remote", "unix://")), "--thread", str(thread_id),
                     "--message", message], timeout=30,
                )
                self.db.finish_dispatch(event_id, token, True)
            except Exception as exc:
                self.db.finish_dispatch(event_id, token, False, str(exc)[:500])
            await asyncio.sleep(0.1)

    async def run_command(self, params: dict[str, Any]) -> dict[str, Any]:
        existing = self._idempotent_admission("run_command", params)
        if existing is not None:
            return existing
        label = bounded(params.get("label"), MAX_LABEL, "label", required=True)
        cwd = normalized_cwd(params.get("cwd", ""))
        shell_name = params.get("shell", "zsh")
        if shell_name not in COMMAND_SHELLS:
            raise ValueError("shell must be zsh or bash")
        command = bounded(params.get("command"), MAX_COMMAND, "command", required=True)
        priority = params.get("priority", "normal")
        if priority not in PRIORITIES:
            raise ValueError(f"priority must be one of {', '.join(PRIORITIES)}")
        task_id = f"cmd-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:8]}"
        row = self._admit_task("run_command", params,
            {
                "id": task_id,
                "parent_id": None,
                "target": "control",
                "cwd": cwd,
                "priority": priority,
                "prompt": None,
                "summary": f"Starting command work unit: {label}",
                "work_kind": "command",
            }
        )
        # Commit admission evidence before touching Herdr.  If the process
        # dies after this point, a retry returns this task and exposes the
        # uncertainty instead of launching a second shell.
        try:
            await self._start_command(row, label or "command", shell_name, command or "")
        except Exception as exc:
            failed = self.db.update(
                task_id,
                event="command_start_failed",
                state="failed",
                summary=f"Command startup failed: {str(exc)[:1200]}",
                finished_at=self.clock(),
                terminal_reported_at=self.clock(),
                session_state="dormant",
            )
            asyncio.create_task(self.emit_attention(failed))
            return public_task(failed)
        result = public_task(self.db.task(task_id))  # type: ignore[arg-type]
        return result

    async def _start_command(self, row: sqlite3.Row, label: str, shell_name: str, command: str) -> None:
        current = self.db.task(row["id"])
        if current is None or current["work_kind"] != "command":
            raise RuntimeError("command launch refused: task is not command-owned")
        if current["state"] != "queued":
            raise RuntimeError(f"command launch refused from state {current['state']}")
        # Claim the row before any topology operation.  Commands are launched
        # solely by this structural wrapper path; the Codex scheduler never
        # owns command rows.
        row = self.db.update(
            row["id"], event="command_launch_claimed", state="starting",
            started_at=self.clock(), summary=f"Starting command work unit: {label}",
        )
        control = await self._control_workspace()
        created = await self.herdr.request(
            "execution.start",
            {
                "execution_id": row["id"],
                "workspace_id": control["workspace_id"],
                "cwd": row["cwd"],
                "label": self._safe_label(label),
                "mode": "shell",
                "shell": shell_name,
                "text": command,
            },
        )
        execution = created["execution"]
        self.db.update(
            row["id"],
            event="command_native_admitted",
            state="working" if execution["state"] == "running" else "starting",
            started_at=self.clock(),
            workspace_id=control["workspace_id"],
            tab_id=execution.get("tab_id"),
            pane_id=execution.get("pane_id"),
            herdr_state=execution["state"],
            session_state="live",
            summary=f"Command work unit running: {label}",
        )

    @staticmethod
    def _safe_label(label: str) -> str:
        safe = re.sub(r"[^A-Za-z0-9_. -]+", "", label).strip()
        return safe[:MAX_LABEL] or "command"

    async def command_event(self, params: dict[str, Any]) -> dict[str, Any]:
        task_id = bounded(params.get("task_id"), 80, "task_id", required=True)
        phase = params.get("phase")
        if phase not in {"started", "exited"}:
            raise ValueError("phase must be started or exited")
        row = self.db.task(task_id or "")
        if row is None or row["work_kind"] != "command":
            raise ValueError(f"command task {task_id!r} does not exist")
        if phase == "started":
            updated = self.db.update(
                task_id or "", event="wrapper_started", state="working", herdr_state="working",
                summary="Command wrapper reported start.",
            )
        else:
            exit_status = params.get("exit_status")
            if not isinstance(exit_status, int) or not -255 <= exit_status <= 255:
                raise ValueError("exit_status must be an integer from -255 through 255")
            if row["state"] in TERMINAL_STATES:
                if row["command_exit_status"] == exit_status:
                    return public_task(row)
                raise ValueError(
                    f"command task {task_id!r} already has exit status {row['command_exit_status']}"
                )
            if row["stop_requested_at"] is not None and exit_status in {130, 143}:
                state = "cancelled"
            else:
                state = "completed" if exit_status == 0 else "failed"
            updated = self.db.update(
                task_id or "",
                event="wrapper_exited",
                state=state,
                command_exit_status=exit_status,
                terminal_reported_at=self.clock(),
                finished_at=self.clock(),
                summary=(
                    "Command completed successfully (exit 0)."
                    if exit_status == 0
                    else (
                        f"Command stopped gracefully with exit status {exit_status}."
                        if state == "cancelled"
                        else f"Command failed with exit status {exit_status}."
                    )
                ),
            )
            asyncio.create_task(self._verify_command_completion(task_id or ""))
            if state == "failed":
                asyncio.create_task(self.emit_attention(updated))
        return public_task(updated)

    async def _command_event_spool_loop(self) -> None:
        ensure_directory(self.command_event_dir)
        while not self.stopping.is_set():
            await self._drain_command_events()
            await self._recover_unreported_command_exits()
            await self._reconcile_native_commands()
            await asyncio.sleep(0.5)

    async def _reconcile_native_commands(self) -> None:
        rows = self.db.conn.execute(
            """SELECT * FROM tasks WHERE work_kind='command'
               AND (state IN ('starting','working','unknown')
                    OR (state IN ('completed','failed','cancelled') AND herdr_state='working'))"""
        ).fetchall()
        for row in rows:
            events = self.db.transitions(row["id"])
            if not any(event["event"] == "command_native_admitted" for event in events):
                continue
            try:
                result = await self.herdr.request(
                    "execution.get", {"execution_id": row["id"]}, timeout=10
                )
                await self._apply_native_execution(row["id"], result["execution"])
            except Exception as exc:
                LOG.debug("native command reconciliation pending for %s: %s", row["id"], exc)

    async def _apply_native_execution(self, task_id: str, execution: dict[str, Any]) -> None:
        row = self.db.task(task_id)
        if row is None or row["work_kind"] != "command":
            return
        native_state = execution["state"]
        values: dict[str, Any] = {
            "workspace_id": row["workspace_id"],
            "tab_id": execution.get("tab_id") or row["tab_id"],
            "pane_id": execution.get("pane_id") or row["pane_id"],
            "herdr_state": native_state,
        }
        if native_state in {"starting", "running"}:
            values["state"] = "working" if native_state == "running" else "starting"
            self.db.update(task_id, event="command_native_observed", **values)
            return
        if native_state == "lost":
            # Missing exit evidence cannot become complete by waiting for an
            # output-drain flag. Preserve uncertainty without replay or a
            # fabricated terminal result, even when output remains incomplete.
            summary = f"Command lifecycle evidence is incomplete: {execution.get('evidence_gap') or 'unknown gap'}"
            if row["state"] == "unknown" and row["herdr_state"] == "lost" and row["summary"] == summary:
                return
            values.update(state="unknown", command_exit_status=None, summary=summary)
            updated = self.db.update(task_id, event="command_native_lost", **values)
            await self.emit_attention(updated)
            return
        if not execution.get("output_complete", False):
            self.db.update(task_id, event="command_native_exit_draining", **values)
            return
        values.update(
            terminal_reported_at=self.clock(),
            finished_at=self.clock(),
            output_excerpt=str(execution.get("stdout_tail") or "")[-MAX_OUTPUT_EXCERPT:],
            output_expires_at=self.clock() + 3600,
        )
        exit_code = execution.get("exit_code")
        signal_name = execution.get("signal_name")
        if native_state == "cancelled":
            values.update(state="cancelled", command_exit_status=exit_code,
                          summary=f"Command cancelled with structural signal evidence: {signal_name or 'unknown signal'}.")
        elif exit_code == 0:
            values.update(state="completed", command_exit_status=0,
                          summary="Command completed successfully (native exit 0).")
        else:
            detail = f"signal {signal_name}" if signal_name else f"exit status {exit_code}"
            values.update(state="failed", command_exit_status=exit_code,
                          summary=f"Command failed with native {detail}.")
        updated = self.db.update(task_id, event=f"command_native_{native_state}", **values)
        if updated["state"] in TERMINAL_STATES:
            await self._verified_terminal(task_id)
            if updated["state"] == "failed":
                asyncio.create_task(self.emit_attention(updated))
        else:
            asyncio.create_task(self.emit_attention(updated))

    async def _drain_command_events(self) -> None:
        if not self.command_event_dir.exists():
            return
        for path in sorted(self.command_event_dir.glob("*.json")):
            try:
                info = path.lstat()
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_uid != os.getuid()
                    or stat.S_IMODE(info.st_mode) & 0o077
                    or info.st_size > 4096
                ):
                    raise ValueError("unsafe command event file")
                payload = json.loads(path.read_text())
                task_id = bounded(payload.get("task_id"), 80, "task_id", required=True)
                if path.name != f"{task_id}.json":
                    raise ValueError("command event filename does not match task id")
                await self.command_event(
                    {
                        "task_id": task_id,
                        "phase": payload.get("phase"),
                        "exit_status": payload.get("exit_status"),
                    }
                )
                path.unlink(missing_ok=True)
            except Exception as exc:
                LOG.warning("cannot consume command event %s: %s", path, exc)
                with contextlib.suppress(OSError):
                    path.unlink()

    async def _recover_unreported_command_exits(self) -> None:
        """Fail closed when an owned wrapper vanished without exit evidence."""
        rows = self.db.conn.execute(
            """SELECT * FROM tasks
               WHERE work_kind='command' AND state IN ('starting','working')
                 AND terminal_reported_at IS NULL AND pane_id IS NOT NULL"""
        ).fetchall()
        for row in rows:
            if self.clock() - float(row["updated_at"]) < COMMAND_EXIT_REPORT_GRACE_SECONDS:
                continue
            try:
                result = await self.herdr.request(
                    "pane.process_info", {"pane_id": row["pane_id"]}, timeout=10
                )
                info = result.get("process_info", result)
                foreground = info.get("foreground_processes") or []
                names = {str(process.get("name", "")) for process in foreground}
                if not foreground or not names <= {"sh", "bash", "zsh", "fish"}:
                    continue
            except Exception:
                continue
            updated = self.db.update(
                row["id"],
                event="command_exit_report_missing",
                state="failed",
                herdr_state="idle",
                summary=(
                    "Command returned to its shell without a structural exit report; "
                    "its result and exit status are unverified."
                ),
                finished_at=self.clock(),
                terminal_reported_at=self.clock(),
            )
            await self._verified_terminal(updated["id"])
            asyncio.create_task(self.emit_attention(updated))

    async def reply(self, params: dict[str, Any]) -> dict[str, Any]:
        task_id = bounded(params.get("task_id"), 80, "task_id", required=True)
        text = bounded(params.get("text"), MAX_REPLY, "text", required=True)
        row = self.db.task(task_id or "")
        if row is None:
            raise ValueError(f"task {task_id!r} does not exist")
        if row["state"] not in {"waiting_human", "blocked", "unknown"}:
            raise ValueError(f"task {task_id} is not waiting for a reply (state={row['state']})")
        target = row["agent_name"] or row["pane_id"]
        if not target:
            raise ValueError("task has no live worker target")
        live_agent_state = row["herdr_state"]
        with contextlib.suppress(Exception):
            result = await self.herdr.request("agent.get", {"target": target}, timeout=10)
            live_agent_state = result.get("agent", result).get(
                "agent_status", live_agent_state
            )
        event = "reply_prompted"
        if live_agent_state == "blocked":
            # Herdr deliberately rejects agent.prompt while an interactive UI
            # is blocked. Send the user's exact answer to that existing UI.
            await self.herdr.request("pane.send_text", {"pane_id": row["pane_id"], "text": text})
            await self.herdr.request("pane.send_keys", {"pane_id": row["pane_id"], "keys": ["enter"]})
            next_state = "starting" if row["prompt_pending"] else "working"
            event = "reply_sent_to_blocked_pane"
        elif live_agent_state == "working":
            if not row["agent_session_id"]:
                raise RuntimeError("waiting worker has no native Codex session id")
            await self._run_required(
                [
                    str(self.config().get("codex_binary") or shutil.which("codex") or "codex"),
                    "queue",
                    "--remote",
                    str(self.config().get("app_server_remote", "unix://")),
                    "--thread",
                    row["agent_session_id"],
                    "--message",
                    f"Control Manager reply for task {task_id}:\n\n{text}\n\nContinue the same task and keep using control-report checkpoints.",
                ],
                timeout=30,
            )
            next_state = "working"
            event = "reply_queued"
        else:
            agent = await self._prompt_verified_agent(
                row,
                f"Control Manager reply for task {task_id}:\n\n{text}\n\nContinue the same task and keep using control-report checkpoints.",
            )
            next_state = "working"
        updated = self.db.update(
            task_id or "",
            event=event,
            state=next_state,
            question=None,
            summary=f"Reply delivered; worker resumed: {' '.join((text or '').split())[:400]}",
            herdr_state="working",
            stop_requested_at=None,
        )
        if next_state == "starting":
            asyncio.create_task(self._resume_startup(task_id or "", target))
        return public_task(updated)

    async def continue_task(self, params: dict[str, Any]) -> dict[str, Any]:
        task_id = bounded(params.get("task_id"), 80, "task_id", required=True)
        text = bounded(params.get("text"), MAX_REPLY, "text", required=True)
        row = self.db.task(task_id or "")
        if row is None:
            raise ValueError(f"task {task_id!r} does not exist or its tracking has expired")
        if row["work_kind"] == "xcsh":
            return await self._continue_xcsh_task(row, text or "")
        if row["work_kind"] != "codex":
            raise ValueError("continue_task is supported only for Codex work")
        supplied_idempotency = params.get("idempotency_key")
        idempotency_key = (
            bounded(supplied_idempotency, 160, "idempotency_key", required=True)
            if supplied_idempotency is not None
            else f"compat-{uuid.uuid4().hex}"
        )
        supersede_pending = params.get("supersede_pending", False)
        if not isinstance(supersede_pending, bool):
            raise ValueError("supersede_pending must be a boolean")
        request_sha256 = hashlib.sha256(
            json.dumps(
                {
                    "task_id": task_id,
                    "text": text,
                    "supersede_pending": supersede_pending,
                },
                separators=(",", ":"),
                sort_keys=True,
            ).encode()
        ).hexdigest()
        existing = self.db.followup_by_idempotency(task_id or "", idempotency_key or "")
        if existing is not None:
            if existing["request_sha256"] != request_sha256:
                raise ValueError("continue_task idempotency key was reused with different arguments")
            if existing["state"] == "preparing":
                existing = self.db.update_followup(
                    existing["followup_id"],
                    "uncertain",
                    "broker restarted or caller retried after a response gap; delivery was not replayed",
                )
                current = self.db.task(task_id or "")
                if current is not None and current["state"] not in TERMINAL_STATES:
                    current = self.db.update(
                        task_id or "",
                        event="followup_delivery_uncertain",
                        state="unknown",
                        priority="attention",
                        herdr_state="unknown",
                        summary="Follow-up delivery is uncertain and was not replayed; inspect durable followup intent.",
                    )
                    asyncio.create_task(self.emit_attention(current))
            return public_task(self.db.task(task_id or "")) | {"followup": dict(existing)}
        if row["session_state"] == "expired" or self.clock() - row["updated_at"] > SESSION_RETENTION_SECONDS:
            if row["session_state"] != "expired":
                self.db.update(task_id or "", session_state="expired", agent_session_id=None)
            raise ValueError(f"task {task_id} tracking expired after 30 days; no new session was created")
        session_id = row["agent_session_id"]
        if not session_id:
            raise ValueError(f"task {task_id} has no recorded native Codex session; no new session was created")
        if not self._native_session_exists(session_id):
            self.db.update(task_id or "", session_state="missing")
            raise ValueError(f"native Codex session {session_id} no longer exists; no new session was created")
        self._cancel_cleanup(task_id or "")
        followup = self._followup_prompt(row, text or "")
        client_user_message_id = f"control-followup-{uuid.uuid4()}"
        delivery, _created = self.db.claim_followup(
            task_id=task_id or "",
            idempotency_key=idempotency_key or "",
            request_sha256=request_sha256,
            client_user_message_id=client_user_message_id,
            delivery_kind="tracked_task_continuation",
            text=followup,
            expected_turn_id=str(row["native_turn_id"] or "") or None,
        )
        if supersede_pending:
            try:
                await self._supersede_legacy_followups(row)
            except Exception as exc:
                return self._uncertain_followup(
                    row, delivery, f"legacy queue reconciliation is uncertain: {exc}"
                )
        provenance, detail = await self._worker_binding_provenance(row)
        if provenance == "owned":
            try:
                result = await self.herdr.request(
                    "agent.get", {"target": row["agent_name"] or row["pane_id"]}, timeout=10
                )
                agent = result.get("agent", result)
                if agent.get("pane_id") != row["pane_id"] or agent.get("agent") != "codex":
                    detail = "the owned pane no longer exposes its recorded Codex agent"
                    delivery = self.db.update_followup(delivery["followup_id"], "rejected", detail)
                    return self._resume_admission_unverified(row, detail) | {"followup": dict(delivery)}
                live_agent_state = agent.get("agent_status", row["herdr_state"])
            except Exception as exc:
                # Do not create another client for a thread merely because a
                # Herdr read was interrupted.  That is transport uncertainty,
                # not proof that the previous client is gone.
                detail = f"Herdr could not verify the retained owned agent: {str(exc)[:500]}"
                delivery = self.db.update_followup(delivery["followup_id"], "rejected", detail)
                return self._resume_admission_unverified(row, detail) | {"followup": dict(delivery)}
        elif provenance in {"foreign", "missing"}:
            # A recycled pane ID or a disappeared pane is safe to replace with
            # a new task tab.  _resume_task still uses this task's recorded
            # native thread; it never adopts the pane's identity.
            resumed = await self._resume_task(
                row,
                followup,
                prior_binding=provenance,
                prior_detail=detail,
                client_user_message_id=client_user_message_id,
            )
            if resumed["state"] == "working":
                delivery = self.db.update_followup(
                    delivery["followup_id"], "accepted", "new continuation turn accepted after exact-thread rebind"
                )
            elif resumed["state"] == "unknown":
                delivery = self.db.update_followup(
                    delivery["followup_id"], "uncertain", "continuation admission may have reached the rebound thread"
                )
            else:
                delivery = self.db.update_followup(
                    delivery["followup_id"], "rejected", "rebind failed before follow-up delivery"
                )
            return resumed | {"followup": dict(delivery)}
        else:
            delivery = self.db.update_followup(delivery["followup_id"], "rejected", detail)
            return self._resume_admission_unverified(row, detail) | {"followup": dict(delivery)}
        if live_agent_state == "working":
            expected_turn_id = str(row["native_turn_id"] or "")
            if not expected_turn_id:
                detail = "the active worker has no authoritative current turn id"
                delivery = self.db.update_followup(delivery["followup_id"], "rejected", detail)
                return self._resume_admission_unverified(row, detail) | {"followup": dict(delivery)}
            try:
                result = await self._run_json_required(
                    [
                        "/usr/bin/python3",
                        str(runtime_root() / "worker_appserver.py"),
                        "steer",
                        "--thread-id",
                        session_id,
                        "--expected-turn-id",
                        expected_turn_id,
                        "--client-user-message-id",
                        client_user_message_id,
                        "--text",
                        followup,
                    ],
                    timeout=30,
                    env=self._appserver_env(),
                )
            except Exception as exc:
                return self._uncertain_followup(row, delivery, str(exc))
            if result.get("delivery") != "accepted":
                detail = str(result.get("reason") or "active turn rejected steering")[:1000]
                delivery = self.db.update_followup(delivery["followup_id"], "rejected", detail)
                current = self.db.task(task_id or "")
                if current is not None and current["state"] not in TERMINAL_STATES:
                    current = self.db.update(
                        task_id or "", event="followup_steer_rejected", state="unknown",
                        priority="attention", herdr_state="unknown",
                        summary=f"Follow-up was not delivered because the expected active turn changed: {detail}",
                    )
                    asyncio.create_task(self.emit_attention(current))
                return public_task(self.db.task(task_id or "")) | {"followup": dict(delivery)}
            if (
                result.get("thread_id") != session_id
                or result.get("turn_id") != expected_turn_id
                or result.get("client_user_message_id") != client_user_message_id
            ):
                return self._uncertain_followup(
                    row, delivery, "app-server steering acknowledgement identity mismatch"
                )
            delivery = self.db.update_followup(
                delivery["followup_id"], "accepted", "accepted by turn/steer on the exact active turn"
            )
            current = self.db.task(task_id or "")
            if current is None:
                raise RuntimeError("task disappeared after accepted steering")
            self.db.conn.execute(
                """UPDATE event_journal SET obsolete_at=?,obsolete_reason='superseded_by_same_turn_steer'
                   WHERE task_id=? AND obsolete_at IS NULL AND event_id IN
                     (SELECT event_id FROM completion_outbox WHERE client_delivered_at IS NULL)""",
                (self.clock(), task_id),
            )
            self.db.conn.commit()
            updated = self.db.update(
                task_id or "", event="followup_steered", state="working", herdr_state="working",
                finished_at=None, terminal_reported_at=None, cleanup_deadline=None,
                output_excerpt=None, output_expires_at=None, terminal_event_id=None,
                stop_requested_at=None, session_state="live",
                summary="Follow-up correction accepted by the same active Codex turn.",
            )
            return public_task(updated) | {"followup": dict(delivery)}
        try:
            agent = await self._prompt_verified_agent(
                row, followup, client_user_message_id=client_user_message_id
            )
        except Exception as exc:
            return self._uncertain_followup(row, delivery, str(exc))
        session = agent.get("agent_session") or {}
        observed = session.get("value")
        if observed and observed != row["agent_session_id"]:
            return self._resume_admission_unverified(
                row, "the retained Codex agent reported a different native thread"
            )
        updated = self.db.update(
            task_id or "", event="followup_prompted", state="working", herdr_state="working",
            finished_at=None, terminal_reported_at=None, cleanup_deadline=None,
            output_excerpt=None, output_expires_at=None,
            run_generation=int(row["run_generation"]) + 1, terminal_event_id=None,
            stop_requested_at=None,
            agent_session_id=row["agent_session_id"],
            session_state="live", summary="Follow-up delivered to the same live Codex pane.",
        )
        delivery = self.db.update_followup(
            delivery["followup_id"], "accepted", "new continuation turn accepted"
        )
        return public_task(updated) | {"followup": dict(delivery)}

    def _uncertain_followup(
        self, row: sqlite3.Row, delivery: sqlite3.Row, detail: str
    ) -> dict[str, Any]:
        delivery = self.db.update_followup(
            delivery["followup_id"], "uncertain", detail[:1000]
        )
        current = self.db.task(row["id"])
        if current is not None and current["state"] not in TERMINAL_STATES:
            current = self.db.update(
                row["id"], event="followup_delivery_uncertain", state="unknown",
                priority="attention", herdr_state="unknown", cleanup_deadline=None,
                summary="Follow-up delivery is uncertain and will not be replayed; inspect durable followup intent.",
            )
            asyncio.create_task(self.emit_attention(current))
        return public_task(self.db.task(row["id"])) | {"followup": dict(delivery)}

    @staticmethod
    def _owned_legacy_followup(task_id: str, submission: dict[str, Any]) -> bool:
        inputs = submission.get("input")
        if not isinstance(inputs, list) or len(inputs) != 1 or not isinstance(inputs[0], dict):
            return False
        text = inputs[0].get("text")
        prefix = f"Control Manager follow-up for existing task {task_id}. Continue this same workstream "
        checkpoint = f"control-report --task-id {task_id} "
        return (
            inputs[0].get("type") == "text"
            and isinstance(text, str)
            and text.startswith(prefix)
            and checkpoint in text
            and isinstance(submission.get("id"), str)
            and isinstance(submission.get("clientUserMessageId"), str)
        )

    async def _supersede_legacy_followups(self, row: sqlite3.Row) -> None:
        thread_id = str(row["agent_session_id"] or "")
        result = await self._run_json_required(
            [
                "/usr/bin/python3", str(runtime_root() / "worker_appserver.py"),
                "queue-list", "--thread-id", thread_id,
            ],
            timeout=30,
            env=self._appserver_env(),
        )
        if result.get("thread_id") != thread_id or not isinstance(result.get("submissions"), list):
            raise RuntimeError("app-server returned an invalid queue inventory")
        for submission in result["submissions"]:
            if not isinstance(submission, dict) or not self._owned_legacy_followup(row["id"], submission):
                continue
            queued_id = submission["id"]
            self.db.preserve_legacy_followup(row["id"], thread_id, submission)
            try:
                deleted = await self._run_json_required(
                    [
                        "/usr/bin/python3", str(runtime_root() / "worker_appserver.py"),
                        "queue-delete", "--thread-id", thread_id,
                        "--queued-submission-id", queued_id,
                    ],
                    timeout=30,
                    env=self._appserver_env(),
                )
            except Exception as exc:
                self.db.update_preserved_legacy(
                    row["id"], thread_id, queued_id, "delete_uncertain", str(exc)[:1000]
                )
                raise RuntimeError(f"delete outcome for queued submission {queued_id} is uncertain") from exc
            if (
                deleted.get("thread_id") != thread_id
                or deleted.get("queued_submission_id") != queued_id
                or deleted.get("deleted") is not True
            ):
                self.db.update_preserved_legacy(
                    row["id"], thread_id, queued_id, "delete_uncertain",
                    "native delete did not affirm exact queued submission removal",
                )
                raise RuntimeError(f"queued submission {queued_id} was not affirmatively deleted")
            self.db.update_preserved_legacy(
                row["id"], thread_id, queued_id, "superseded",
                "full payload preserved before exact native queue deletion",
            )

    def _native_session_exists(self, session_id: str) -> bool:
        if not re.fullmatch(r"[A-Za-z0-9-]{20,80}", session_id):
            return False
        root = Path(os.environ.get("CODEX_SESSIONS_ROOT", str(Path.home() / ".codex/sessions")))
        return any(root.glob(f"*/*/*/*{session_id}*.jsonl"))

    async def _task_tab_live(self, row: sqlite3.Row) -> bool:
        if not row["tab_id"] or not row["pane_id"]:
            return False
        try:
            await self.herdr.request("tab.get", {"tab_id": row["tab_id"]}, timeout=10)
            await self.herdr.request("pane.get", {"pane_id": row["pane_id"]}, timeout=10)
            return True
        except Exception:
            return False

    @staticmethod
    def _exact_native_resume_process(process_info: dict[str, Any], thread_id: str) -> bool:
        """Return true only for one foreground Codex CLI resuming ``thread_id``.

        Pane/workspace IDs are allocator-local and can be reused after Herdr
        restarts.  They are never sufficient evidence of a worker binding.
        """
        foreground = process_info.get("foreground_processes") or []
        if len(foreground) != 1 or foreground[0].get("name") != "codex":
            return False
        argv = foreground[0].get("argv")
        if not isinstance(argv, list) or not all(isinstance(value, str) for value in argv):
            return False
        return (
            len(argv) >= 4
            and argv[-2:] == ["resume", thread_id]
            and argv.count("resume") == 1
            and "--remote" in argv
        )

    async def _worker_binding_provenance(self, row: sqlite3.Row) -> tuple[str, str]:
        """Classify a persisted Codex pane without trusting recycled IDs.

        ``owned`` requires all of the durable task mapping, exact topology,
        and a foreground process that resumes this task's recorded thread.
        ``foreign`` and ``missing`` are affirmative observations; transport or
        incomplete observations remain ``uncertain`` and must not trigger a
        prompt replay or a second native client.
        """
        if row["work_kind"] != "codex" or not row["pane_id"] or not row["tab_id"]:
            return "missing", "no persisted Codex pane binding"
        owner = self.db.task_for_pane(row["pane_id"])
        if owner is None or owner["id"] != row["id"]:
            return "foreign", "the pane ID is now bound to a different durable task"
        try:
            got_tab = await self.herdr.request("tab.get", {"tab_id": row["tab_id"]}, timeout=10)
            got_pane = await self.herdr.request("pane.get", {"pane_id": row["pane_id"]}, timeout=10)
            tab = got_tab.get("tab", got_tab)
            pane = got_pane.get("pane", got_pane)
        except Exception as exc:
            if "not_found" in str(exc):
                return "missing", f"retained topology is absent: {str(exc)[:300]}"
            return "uncertain", f"retained topology could not be read: {str(exc)[:300]}"
        if (
            tab.get("workspace_id") != row["workspace_id"]
            or pane.get("pane_id") != row["pane_id"]
            or pane.get("tab_id") != row["tab_id"]
            or pane.get("workspace_id") != row["workspace_id"]
        ):
            return "foreign", "retained pane topology no longer matches the durable task binding"
        try:
            if str(Path(pane.get("cwd", "")).resolve()) != row["cwd"]:
                return "foreign", "retained pane CWD no longer matches the durable task binding"
        except Exception:
            return "uncertain", "retained pane CWD could not be normalized"
        session = pane.get("agent_session") or {}
        observed = session.get("value")
        if observed and observed != row["agent_session_id"]:
            return "foreign", "retained pane declares a different native Codex thread"
        try:
            process = await self.herdr.request(
                "pane.process_info", {"pane_id": row["pane_id"]}, timeout=10
            )
            process_info = process.get("process_info", process)
        except Exception as exc:
            return "uncertain", f"retained pane process could not be read: {str(exc)[:300]}"
        foreground = process_info.get("foreground_processes") or []
        shell_names = {"sh", "bash", "zsh", "fish"}
        names = {str(item.get("name", "")) for item in foreground}
        if foreground and names <= shell_names:
            return "missing", "retained pane has returned to its shell"
        if self._exact_native_resume_process(process_info, str(row["agent_session_id"] or "")):
            return "owned", "durable task mapping and exact native resume process match"
        if any(item.get("name") == "codex" for item in foreground):
            return "foreign", "retained pane runs a different or incomplete Codex command"
        return "uncertain", "retained pane foreground process is not attributable to this task"

    async def _isolate_unowned_binding(
        self, row: sqlite3.Row, provenance: str, detail: str, *, boundary: str
    ) -> sqlite3.Row:
        """Quarantine stale topology without changing another task's pane."""
        state = row["state"]
        values: dict[str, Any] = {
            "event": f"{boundary}_binding_{provenance}_isolated",
            "herdr_state": "missing" if provenance == "missing" else "unknown",
            "session_state": "missing" if provenance == "missing" else "unknown",
            "cleanup_deadline": None,
            "summary": (
                f"Worker pane binding is {provenance}; no lifecycle or cleanup action was applied to it: "
                f"{detail[:700]}"
            ),
        }
        # Do not let a recycled pane rewrite a completed result.  Active work
        # is explicitly uncertain so the manager can make the next decision.
        if state not in TERMINAL_STATES and state not in {"waiting_human", "blocked"}:
            values["state"] = "unknown"
        updated = self.db.update(row["id"], **values)
        if updated["state"] == "unknown":
            asyncio.create_task(self.emit_attention(updated))
        return updated

    def _followup_prompt(self, row: sqlite3.Row, text: str) -> str:
        return (
            f"Control Manager follow-up for existing task {row['id']}. Continue this same workstream "
            "and preserve its context. Use the existing control-report checkpoint contract. "
            f"For every checkpoint invoke `control-report --task-id {row['id']} "
            f"--socket {self.socket_path}` so native resume never depends on inherited environment.\n\n"
            f"{text}"
        )

    def _resume_admission_unverified(self, row: sqlite3.Row, detail: str, *, prompted: bool = False) -> dict[str, Any]:
        """Record an unproven continuation without fabricating worker progress."""
        values: dict[str, Any] = {
            "event": "native_resume_admission_unverified",
            "herdr_state": "unknown",
            "session_state": "unknown",
            "cleanup_deadline": None,
            "summary": (
                f"Native continuation admission is unverified; {'a follow-up may have been sent' if prompted else 'no follow-up was sent'}: "
                f"{detail[:900]}"
            ),
        }
        # A completed result is durable evidence.  A failed attempt to admit a
        # *new* follow-up cannot erase it.  If a prompt may have escaped, the
        # task is instead honestly unknown.
        if prompted:
            values["state"] = "unknown"
        updated = self.db.update(row["id"], **values)
        if updated["state"] == "unknown":
            asyncio.create_task(self.emit_attention(updated))
        return public_task(updated)

    async def _resume_task(
        self,
        row: sqlite3.Row,
        followup: str,
        *,
        prior_binding: str,
        prior_detail: str,
        client_user_message_id: str | None = None,
    ) -> dict[str, Any]:
        worker_env = {
            "CONTROL_TASK_ID": row["id"],
            "CONTROL_BROKER_SOCKET": str(self.socket_path),
            "CONTROL_PARENT_ID": row["parent_id"] or "",
        }
        # The persisted pane is demonstrably foreign or gone.  Never wait for
        # or attach to it: Herdr may have recycled its IDs after a restart.
        workspace_id, tab, pane = await self._create_worker_pane(row, worker_env)
        agent_name = f"ctl_{uuid.uuid4().hex[:12]}"
        self.db.update(
            row["id"], event=f"native_resume_rebound_{prior_binding}", workspace_id=workspace_id,
            tab_id=tab["tab_id"], pane_id=pane["pane_id"], agent_name=agent_name,
            herdr_state="admitting", session_state="admitting", cleanup_deadline=None,
            summary=f"Rebinding native Codex session after {prior_binding} retained-pane evidence: {prior_detail[:300]}",
        )
        self.subscription_refresh.set()
        args = [
            "--remote",
            str(self.config().get("app_server_remote", "unix://")),
            "resume",
            row["agent_session_id"],
        ]
        prompt_attempted = False
        try:
            await self._start_visible_codex(agent_name=agent_name, pane_id=pane["pane_id"], args=args)
            agent = await self._wait_agent_ready(agent_name)
            session = agent.get("agent_session") or {}
            resumed_id = session.get("value")
            if resumed_id and resumed_id != row["agent_session_id"]:
                raise RuntimeError(
                    f"Codex resume identity mismatch: expected {row['agent_session_id']}, observed {resumed_id}"
                )
            rebound = self.db.task(row["id"])
            if rebound is None:
                raise RuntimeError("task disappeared while native resume was being admitted")
            proof, proof_detail = await self._worker_binding_provenance(rebound)
            if proof != "owned":
                raise RuntimeError(f"new resume binding is {proof}: {proof_detail}")
            prompt_attempted = True
            prompted = await self._prompt_verified_agent(
                rebound, followup, client_user_message_id=client_user_message_id
            )
            prompted_session = prompted.get("agent_session") or {}
            observed = prompted_session.get("value")
            if observed and observed != row["agent_session_id"]:
                raise RuntimeError("follow-up target reported a different native thread")
        except Exception as exc:
            return self._resume_admission_unverified(row, str(exc), prompted=prompt_attempted)
        return public_task(
            self.db.update(
                row["id"], event="native_resumed", state="working", herdr_state="working",
                agent_session_id=row["agent_session_id"], session_state="live",
                resume_count=int(row["resume_count"]) + 1,
                finished_at=None, terminal_reported_at=None, cleanup_deadline=None,
                output_excerpt=None, output_expires_at=None,
                run_generation=int(row["run_generation"]) + 1, terminal_event_id=None,
                stop_requested_at=None, summary="Native Codex session resumed for follow-up.",
            )
        )

    async def _continue_xcsh_task(self, row: sqlite3.Row, text: str) -> dict[str, Any]:
        """Continue only the same admitted XCSH pane/session generation."""
        if row["state"] not in {"waiting_human", "blocked"}:
            raise ValueError("XCSH continuation requires a semantic waiting state")
        if not row["pane_id"] or not row["agent_session_id"]:
            raise RuntimeError("XCSH continuation lacks an admitted semantic identity")
        # The installed reporter creates the new turn identity.  Clear only
        # the old turn, retain its session/pane provenance, and require the
        # next starting/working report to bind the incremented generation.
        await self.herdr.request("pane.send_text", {"pane_id": row["pane_id"], "text": text}, timeout=10)
        await self.herdr.request("pane.send_keys", {"pane_id": row["pane_id"], "keys": ["enter"]}, timeout=10)
        updated = self.db.update(
            row["id"], event="xcsh_continuation_sent", state="working", question=None,
            native_turn_id=None, run_generation=int(row["run_generation"]) + 1,
            terminal_reported_at=None, finished_at=None, cleanup_deadline=None,
            summary="Continuation sent to the same admitted XCSH session; awaiting semantic journal.",
        )
        return public_task(updated)

    async def request_stop(self, params: dict[str, Any]) -> dict[str, Any]:
        task_id = bounded(params.get("task_id"), 80, "task_id", required=True)
        row = self.db.task(task_id or "")
        if row is None:
            raise ValueError(f"task {task_id!r} does not exist")
        if row["state"] in TERMINAL_STATES:
            return public_task(row)
        if not row["pane_id"]:
            if row["state"] == "queued":
                updated = self.db.update(
                    task_id or "", state="cancelled", summary="Cancelled before worker start.", finished_at=self.clock()
                )
                self.scheduler_event.set()
                return public_task(updated)
            raise ValueError("task has no worker pane")
        if row["work_kind"] in {"command", "xcsh"}:
            events = self.db.transitions(task_id or "")
            if row["work_kind"] == "xcsh" or any(event["event"] == "command_native_admitted" for event in events):
                self.db.update(
                    task_id or "",
                    summary="Graceful native cancellation requested. Forced termination requires separate explicit confirmation.",
                    stop_requested_at=self.clock(),
                )
                result = await self.herdr.request(
                    "execution.cancel", {"execution_id": task_id}, timeout=10
                )
                if row["work_kind"] == "command":
                    await self._apply_native_execution(task_id or "", result["execution"])
                else:
                    self.db.update(
                        task_id or "", event="xcsh_cancel_requested", stop_requested_at=self.clock(),
                        herdr_state=result.get("execution", result).get("state", "cancelling"),
                        summary="Graceful XCSH cancellation requested; awaiting semantic cancelled report.",
                    )
                return public_task(self.db.task(task_id or ""))
            else:
                await self.herdr.request("pane.send_keys", {"pane_id": row["pane_id"], "keys": ["ctrl+c"]})
        else:
            await self.herdr.request("pane.send_keys", {"pane_id": row["pane_id"], "keys": ["ctrl+c"]})
        updated = self.db.update(
            task_id or "",
            summary="Graceful stop requested. Forced termination requires separate explicit confirmation.",
            stop_requested_at=self.clock(),
        )
        return public_task(updated)

    async def report(self, params: dict[str, Any]) -> dict[str, Any]:
        task_id = bounded(params.get("task_id"), 80, "task_id", required=True)
        state = params.get("state")
        if state not in REPORT_STATES:
            raise ValueError(f"state must be one of {', '.join(sorted(REPORT_STATES))}")
        summary = bounded(params.get("summary"), MAX_SUMMARY, "summary", required=True)
        question = bounded(params.get("question"), MAX_QUESTION, "question")
        if state == "waiting_human" and not question:
            raise ValueError("waiting_human requires --question")
        priority = params.get("priority")
        if priority is not None and priority not in PRIORITIES:
            raise ValueError(f"priority must be one of {', '.join(PRIORITIES)}")
        row = self.db.task(task_id or "")
        if row is None:
            raise ValueError(f"task {task_id!r} does not exist")
        if row["work_kind"] != "codex":
            raise ValueError("control-report is only valid for Codex worker tasks")
        if row["state"] in TERMINAL_STATES:
            if row["state"] == state and row["summary"] == summary:
                return public_task(row)
            raise ValueError(f"task {task_id} is already terminal ({row['state']})")
        values: dict[str, Any] = {"state": state, "summary": summary, "question": question}
        if row["agent_name"] or row["pane_id"]:
            try:
                result = await self.herdr.request(
                    "agent.get", {"target": row["agent_name"] or row["pane_id"]}, timeout=10
                )
                agent = result.get("agent", result)
                session = agent.get("agent_session") or {}
                if session.get("value"):
                    values["agent_session_id"] = session["value"]
            except Exception:
                pass
        if priority is not None:
            values["priority"] = priority
        if state in TERMINAL_STATES:
            values["finished_at"] = self.clock()
            values["terminal_reported_at"] = self.clock()
            values["prompt_pending"] = None
        updated = self.db.update(task_id or "", event=f"control_report_{state}", **values)
        # A human-input wait is a durable feature blocker, not merely a pane
        # status.  Conversely, a resumed child clears only its own wait
        # blocker; terminal handling below remains the sole promotion path.
        self.db.sync_feature_child_state(updated["id"])
        if state in TERMINAL_STATES:
            self.db.feature_task_terminal(updated["id"], state, summary or "")
            # This is also invoked after durable inbox consumption.  The
            # transactionally claimed action makes both paths idempotent.
            await self.advance_for_task(updated["id"])
            self.scheduler_event.set()
            if updated["herdr_state"] in {"idle", "done"}:
                await self._verified_terminal(updated["id"])
            elif updated["work_kind"] == "codex":
                self._schedule_reported_codex_reconcile(updated["id"])
        effective_priority = priority or updated["priority"]
        if state in {"waiting_human", "blocked", "failed"} or (
            state == "completed" and PRIORITIES[effective_priority] >= PRIORITIES["attention"]
        ):
            asyncio.create_task(self.emit_attention(updated))
        return public_task(updated)

    async def _verify_command_completion(self, task_id: str) -> None:
        for _ in range(40):
            row = self.db.task(task_id)
            if row is None or not row["pane_id"] or row["state"] not in TERMINAL_STATES:
                return
            try:
                result = await self.herdr.request(
                    "pane.process_info", {"pane_id": row["pane_id"]}, timeout=10
                )
                info = result.get("process_info", result)
                foreground = info.get("foreground_processes") or []
                names = {str(process.get("name", "")) for process in foreground}
                if foreground and names <= {"sh", "bash", "zsh", "fish"}:
                    self.db.update(
                        task_id,
                        herdr_state="idle",
                        event="command_shell_returned",
                    )
                    await self._verified_terminal(task_id)
                    return
            except Exception as exc:
                LOG.debug("command shell verification pending for %s: %s", task_id, exc)
            await asyncio.sleep(0.25)
        row = self.db.task(task_id)
        if row:
            self.db.update(
                task_id,
                state="unknown",
                event="command_shell_not_observed",
                summary="Command exit was reported, but the pane did not return to its shell.",
            )

    async def _verified_terminal(self, task_id: str) -> None:
        row = self.db.task(task_id)
        if row is None or row["state"] not in TERMINAL_STATES:
            return
        if row["work_kind"] == "codex" and not (
            row["terminal_reported_at"] and row["herdr_state"] in {"idle", "done"}
        ):
            return
        if row["work_kind"] == "command" and not row["terminal_reported_at"]:
            return
        if row["pane_id"] and row["output_excerpt"] is None:
            if row["work_kind"] == "command" and any(
                item["event"] == "command_native_admitted" for item in self.db.transitions(task_id)
            ):
                self._schedule_cleanup(task_id)
                return
            try:
                result = await self.herdr.request(
                    "pane.read",
                    {
                        "pane_id": row["pane_id"],
                        "source": "recent_unwrapped",
                        "lines": 120,
                        "format": "text",
                        "strip_ansi": True,
                    },
                    timeout=10,
                )
                read = result.get("read", result)
                excerpt = read.get("text") or read.get("output") or read.get("content") or ""
                if not isinstance(excerpt, str):
                    excerpt = json.dumps(excerpt, separators=(",", ":"))
                excerpt = excerpt[-MAX_OUTPUT_EXCERPT:]
            except Exception as exc:
                excerpt = f"[output excerpt unavailable: {str(exc)[:300]}]"
            self.db.update(
                task_id,
                output_excerpt=excerpt,
                output_expires_at=self.clock() + 3600,
                event="completion_evidence_captured",
            )
        self._schedule_cleanup(task_id)

    def _schedule_cleanup(self, task_id: str) -> None:
        existing = self.cleanup_timers.get(task_id)
        if existing and not existing.done():
            return
        deadline = self.clock() + self.cleanup_delay
        self.db.update(task_id, cleanup_deadline=deadline, event="cleanup_scheduled")
        self.cleanup_timers[task_id] = asyncio.create_task(
            self._cleanup_after(task_id, self.cleanup_delay), name=f"cleanup-{task_id}"
        )

    def _cancel_cleanup(self, task_id: str) -> None:
        timer = self.cleanup_timers.pop(task_id, None)
        if timer:
            timer.cancel()
        row = self.db.task(task_id)
        if row and row["cleanup_deadline"] is not None:
            self.db.update(task_id, cleanup_deadline=None, event="cleanup_cancelled_for_followup")

    async def _cleanup_after(self, task_id: str, delay: float) -> None:
        try:
            await asyncio.sleep(max(0, delay))
            row = self.db.task(task_id)
            if row is None or row["state"] not in TERMINAL_STATES or row["cleanup_deadline"] is None:
                return
            config = self.config()
            if row["tab_id"] == config.get("manager_tab_id") or row["pane_id"] == config.get("manager_pane_id"):
                LOG.error("refusing cleanup of reserved control/manager")
                return
            if not row["tab_id"]:
                return
            try:
                got = await self.herdr.request("tab.get", {"tab_id": row["tab_id"]}, timeout=10)
                tab = got.get("tab", got)
                if int(tab.get("pane_count", 1)) != 1:
                    LOG.warning("refusing cleanup of task %s tab containing extra panes", task_id)
                    return
                if row["work_kind"] == "codex":
                    provenance, detail = await self._worker_binding_provenance(row)
                    if provenance != "owned":
                        await self._isolate_unowned_binding(
                            row, provenance, detail, boundary="cleanup"
                        )
                        return
                await self.herdr.request("tab.close", {"tab_id": row["tab_id"]}, timeout=10)
            except Exception as exc:
                if "not_found" not in str(exc):
                    LOG.warning("cleanup failed for %s: %s", task_id, exc)
                    return
            self.db.update(
                task_id,
                cleanup_deadline=None,
                session_state="dormant" if row["work_kind"] == "codex" else "closed",
                herdr_state="closed",
                event="owned_tab_cleaned",
            )
        finally:
            self.cleanup_timers.pop(task_id, None)

    async def _scheduler(self) -> None:
        while not self.stopping.is_set():
            await self.scheduler_event.wait()
            self.scheduler_event.clear()
            while True:
                row = self.db.next_queued_codex()
                if row is None:
                    break
                self.db.update(
                    row["id"], event="codex_dispatch_admitted", state="starting", started_at=self.clock(),
                    summary="Starting Codex worker.",
                )
                task = asyncio.create_task(self._start_task(self.db.task(row["id"])), name=f"start-{row['id']}")
                self.start_tasks[row["id"]] = task
                task.add_done_callback(lambda _task, ident=row["id"]: self.start_tasks.pop(ident, None))

    async def _workspace_exists(self, workspace_id: str) -> bool:
        try:
            await self.herdr.request("workspace.get", {"workspace_id": workspace_id}, timeout=10)
            return True
        except Exception:
            return False

    async def _control_workspace(self) -> dict[str, Any]:
        result = await self.herdr.request("session.snapshot", {}, timeout=15)
        snapshot = result.get("snapshot", result)
        return await self._reconcile_root_topology(snapshot)

    def _is_control_cwd(self, cwd: str) -> bool:
        """Whether a task belongs in the canonical control workspace."""
        config = self.config()
        root = normalized_cwd(
            str(config.get("control_root", config.get("manager_cwd", str(package_root()))))
        )
        try:
            Path(cwd).resolve(strict=True).relative_to(root)
            return True
        except ValueError:
            return False

    def _persist_config_updates(self, **updates: Any) -> None:
        payload = self.config() | updates | {"updated_at": int(self.clock())}
        temporary = self.config_path.with_suffix(".json.new")
        old_umask = os.umask(0o077)
        try:
            temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
            os.replace(temporary, self.config_path)
            os.chmod(self.config_path, 0o600)
        finally:
            os.umask(old_umask)

    def _persist_replacement_manager_binding(self, *, action_id: str, expected_generation: int,
                                             old_workspace_id: str, old_pane_id: str,
                                             workspace_id: str, tab_id: str, pane_id: str) -> bool:
        """CAS the ephemeral terminal binding while retaining logical thread identity."""
        current=self.config()
        try: generation=int(current.get("manager_binding_generation",0))
        except (TypeError,ValueError): raise RuntimeError("manager binding generation is invalid")
        if (generation != expected_generation or current.get("manager_workspace_id") != old_workspace_id
                or current.get("manager_pane_id") != old_pane_id):
            # An interrupted replace may have committed the atomic config file
            # just before the broker journal receipt.  Its action marker is
            # durable confirmation, not an invitation to allocate again.
            return (current.get("manager_attachment_action_id") == action_id
                    and current.get("manager_workspace_id") == workspace_id
                    and current.get("manager_tab_id") == tab_id
                    and current.get("manager_pane_id") == pane_id
                    and generation == expected_generation + 1)
        self._persist_config_updates(
            manager_workspace_id=workspace_id, manager_tab_id=tab_id, manager_pane_id=pane_id,
            manager_binding_generation=expected_generation + 1,
            manager_attachment_action_id=action_id,
            manager_attachment_old_binding={"workspace_id":old_workspace_id,"pane_id":old_pane_id,
                                            "generation":expected_generation},
        )
        return True

    @staticmethod
    def _exact_not_found(exc: Exception, resource: str) -> bool:
        text=str(exc).lower().replace(" ", "_")
        return f"{resource}_not_found" in text or f"{resource}:_not_found" in text

    @staticmethod
    def _manager_attachment_execution_id(action_id: str) -> str:
        # Stable across broker restarts and below Herdr's 80-byte execution-id
        # bound. This is a capability-validated action id, never user input.
        return f"manager-resume-{action_id}"

    async def _admit_manager_attachment_execution(
        self, *, action_id: str, record: sqlite3.Row, config: dict[str, Any], workspace_id: str,
    ) -> sqlite3.Row:
        """Use Herdr's durable execution admission for one exact Codex resume.

        ``execution.start`` is safe to resend after a lost response: Herdr's
        ExecutionManager returns the same durable record instead of creating a
        second tab. The caller has already directly proven ``workspace_id``;
        this method never uses Herdr's active-workspace fallback.
        """
        execution_id=str(record["manager_execution_id"] or self._manager_attachment_execution_id(action_id))
        state=str(record["state"])
        if state != "execution_intent":
            record=self.db.advance_manager_attachment(action_id,states={state},state="execution_intent",
                manager_execution_id=execution_id,receipt={"manager_execution_id":execution_id,"workspace_id":workspace_id})
        try:
            codex=str(Path(configured_codex_binary(config)))
            cwd=normalized_cwd(str(config["manager_cwd"]))
        except (RuntimeError,ValueError) as exc:
            raise RuntimeError(f"canonical manager execution is not configured: {exc}") from exc
        remote=str(config.get("app_server_remote","unix://")); profile=str(config.get("profile","control-manager"))
        argv=[codex,"--disable","hooks","--remote",remote,"--profile",profile,"-C",cwd,"resume",str(config["manager_thread_id"])]
        result=await self.herdr.request("execution.start",{
            "execution_id":execution_id,"workspace_id":workspace_id,"cwd":cwd,"label":"manager",
            "mode":"argv","argv":argv,
        },timeout=15)
        execution=result.get("execution") or {}
        pane_id=str(execution.get("pane_id") or ""); tab_id=str(execution.get("tab_id") or "")
        if not pane_id or not tab_id:
            # Herdr persisted an admission but cannot provide its native
            # binding. Do not substitute a pane or issue another execution.
            raise RuntimeError("durable manager execution has no pane/tab receipt")
        pane=(await self.herdr.request("pane.get",{"pane_id":pane_id},timeout=10)).get("pane",{})
        if str(pane.get("workspace_id") or "") != workspace_id:
            raise RuntimeError("durable manager execution pane is bound to a different workspace")
        return self.db.advance_manager_attachment(action_id,states={"execution_intent","execution_admitted"},
            state="execution_admitted",replacement_workspace_id=workspace_id,
            replacement_tab_id=tab_id,replacement_pane_id=pane_id,manager_execution_id=execution_id,
            receipt={"manager_execution_id":execution_id,"workspace_id":workspace_id,
                     "tab_id":tab_id,"pane_id":pane_id,"execution_state":execution.get("state")})

    async def _manager_terminal_disconnect_evidence(
        self, pane_id: str, config: dict[str, Any],
    ) -> dict[str, Any]:
        """Prove that the exact idle canonical Codex client cannot reconnect."""
        try:
            agent_result = await self.herdr.request("agent.get", {"target": pane_id}, timeout=5)
            agent = agent_result.get("agent", agent_result)
            process_result = await self.herdr.request(
                "pane.process_info", {"pane_id": pane_id}, timeout=10
            )
            process_info = process_result.get("process_info", process_result)
        except Exception as exc:
            return {"proven": False, "reason": f"canonical pane identity is unreadable: {type(exc).__name__}: {exc}"}
        thread_id = str(config.get("manager_thread_id") or "")
        session = agent.get("agent_session") or {}
        if str(session.get("value") or "") != thread_id:
            return {"proven": False, "reason": "native pane does not carry the exact canonical thread"}
        status = str(agent.get("agent_status") or "")
        if status not in {"idle", "done"}:
            return {"proven": False, "reason": "canonical pane is busy, blocked, or has ambiguous lifecycle state"}
        try:
            codex = str(Path(configured_codex_binary(config)))
            cwd = normalized_cwd(str(config.get("manager_cwd") or ""))
        except (RuntimeError, ValueError) as exc:
            return {"proven": False, "reason": f"canonical runtime binding is invalid: {exc}"}
        expected = [codex, "--disable", "hooks", "--remote", str(config.get("app_server_remote") or ""),
                    "--profile", str(config.get("profile") or "control-manager"), "-C", cwd,
                    "resume", thread_id]
        foreground = process_info.get("foreground_processes") or []
        if (len(foreground) != 1 or foreground[0].get("name") != "codex"
                or foreground[0].get("argv") != expected):
            return {"proven": False, "reason": "canonical pane lacks exact remote-process proof"}
        try:
            read_result = await self.herdr.request("agent.read", {
                "target": pane_id, "source": "detection", "lines": 120,
                "format": "text", "strip_ansi": True,
            }, timeout=5)
            terminal = str((read_result.get("read", read_result)).get("text") or "")
        except Exception as exc:
            return {"proven": False, "reason": f"canonical terminal evidence is unreadable: {type(exc).__name__}: {exc}"}
        if not terminal_appserver_disconnect(terminal):
            return {"proven": False, "reason": "canonical terminal does not show explicit reconnect failure"}
        return {"proven": True, "reason": "exact idle canonical client shows explicit app-server reconnect failure"}

    async def _recover_proven_disconnected_manager(
        self, snapshot: dict[str, Any], *, action_id: str, claim_sha256: str,
        claim_key: str | None, owner_generation: str | None,
    ) -> dict[str, str]:
        """Close one proven-dead client, then use the durable lost-pane repair."""
        config = self.config()
        if not claim_key or not owner_generation:
            return {"state": "unverified", "reason": "terminal replacement requires an active supervisor claim capability"}
        try:
            expected_generation = int(config.get("manager_binding_generation", 0))
        except (TypeError, ValueError):
            return {"state": "unverified", "reason": "manager binding generation is invalid"}
        pane_id = str(config.get("manager_pane_id") or "")
        workspace_id = str(config.get("manager_workspace_id") or "")
        thread_id = str(config.get("manager_thread_id") or "")
        if not pane_id or not workspace_id or not thread_id:
            return {"state": "unverified", "reason": "canonical manager bindings are incomplete"}
        record = self.db.claim_manager_attachment(
            action_id=action_id, claim_sha256=claim_sha256, logical_thread_id=thread_id,
            expected_binding_generation=expected_generation,
            old_workspace_id=workspace_id, old_pane_id=pane_id,
        )
        if str(record["state"]) != "intent":
            return await self._recover_proven_lost_manager_binding(
                snapshot, action_id=action_id, claim_sha256=claim_sha256,
                claim_key=claim_key, owner_generation=owner_generation,
            )
        panes = {str(item.get("pane_id")) for item in snapshot.get("panes", [])}
        if pane_id in panes:
            evidence = await self._manager_terminal_disconnect_evidence(pane_id, config)
            if not evidence.get("proven"):
                return {"state": "unverified", "reason": str(evidence.get("reason") or "terminal disconnect is unverified")}
            self._verified_supervisor_attachment_claim(action_id, claim_key, owner_generation)
            try:
                await self.herdr.request("pane.close", {"pane_id": pane_id}, timeout=10)
            except Exception as exc:
                try:
                    await self.herdr.request("pane.get", {"pane_id": pane_id}, timeout=5)
                except Exception as absent:
                    if not self._exact_not_found(absent, "pane"):
                        return {"state": "unverified", "reason": "canonical pane close outcome is uncertain"}
                else:
                    return {"state": "unverified", "reason": f"canonical disconnected pane close failed: {type(exc).__name__}: {exc}"}
        # pane.close is synchronous.  The lost-binding path below performs its
        # own direct pane/workspace reads and a fresh snapshot before creating
        # anything, so remove only the just-proven old pane from this input
        # instead of adding another unjournaled read-failure window here.
        fresh = dict(snapshot)
        fresh["panes"] = [item for item in snapshot.get("panes", [])
                          if str(item.get("pane_id")) != pane_id]
        return await self._recover_proven_lost_manager_binding(
            fresh, action_id=action_id, claim_sha256=claim_sha256,
            claim_key=claim_key, owner_generation=owner_generation,
        )

    async def _recover_proven_lost_manager_binding(
        self, snapshot: dict[str, Any], *, action_id: str, claim_sha256: str,
        claim_key: str | None, owner_generation: str | None,
    ) -> dict[str, str]:
        """Replace only a manager terminal proven gone by exact live reads.

        Local receipts deliberately stop at uncertainty around Herdr effects:
        the current protocol has no idempotency key for ``workspace.create``.
        That makes a crash after a create request an operator-visible
        uncertainty, rather than a reason to allocate a second manager.
        """
        config=self.config()
        def fence() -> None:
            if not claim_key or not owner_generation:
                raise PermissionError("lost manager replacement requires an active supervisor claim capability")
            # Re-read the supervisor journal at each mutation boundary so a
            # pause, lease expiry, or owner-generation change wins over an
            # already-admitted broker RPC.
            self._verified_supervisor_attachment_claim(action_id,claim_key,owner_generation)
        required=("manager_thread_id","manager_cwd","manager_workspace_id","manager_pane_id")
        if any(not config.get(key) for key in required):
            return {"state":"unverified","reason":"canonical manager bindings are incomplete"}
        prior=self.db.manager_attachment(action_id)
        if prior is not None:
            if not hmac.compare_digest(str(prior["claim_sha256"]),claim_sha256):
                return {"state":"unverified","reason":"manager attachment action is owned by a different claim"}
            old_workspace_id=str(prior["old_workspace_id"]); old_pane_id=str(prior["old_pane_id"])
            expected_generation=int(prior["expected_binding_generation"])
            logical_thread_id=str(prior["logical_thread_id"])
        else:
            old_workspace_id=str(config["manager_workspace_id"]); old_pane_id=str(config["manager_pane_id"])
            try: expected_generation=int(config.get("manager_binding_generation",0))
            except (TypeError,ValueError): return {"state":"unverified","reason":"manager binding generation is invalid"}
            logical_thread_id=str(config["manager_thread_id"])
        record=self.db.claim_manager_attachment(action_id=action_id,claim_sha256=claim_sha256,
                                                logical_thread_id=logical_thread_id,
                                                expected_binding_generation=expected_generation,
                                                old_workspace_id=old_workspace_id,old_pane_id=old_pane_id)
        state=str(record["state"])
        if str(config.get("manager_thread_id") or "") != logical_thread_id:
            return {"state":"unverified","reason":"logical manager thread changed since replacement intent"}
        if state in {"uncertain","rejected"}:
            return {"state":"unverified","reason":"prior manager replacement effect is uncertain; refusing duplicate allocation"}

        # Once a replacement has been created, only its durable receipt/config
        # can be used. Never re-run workspace.create for this action.
        if state in {"created","execution_intent","execution_admitted","binding_intent","binding_persisted","launch_intent","launch_text_sent","launch_enter_sent","verified"}:
            workspace_id=str(record["replacement_workspace_id"] or "")
            tab_id=str(record["replacement_tab_id"] or "")
            pane_id=str(record["replacement_pane_id"] or "")
            if not workspace_id:
                self.db.advance_manager_attachment(action_id,states={state},state="uncertain")
                return {"state":"unverified","reason":"replacement receipt is incomplete; refusing duplicate allocation"}
            if state in {"created","execution_intent"}:
                try:
                    live_workspace=(await self.herdr.request("workspace.get",{"workspace_id":workspace_id},timeout=10)).get("workspace",{})
                    if str(live_workspace.get("workspace_id") or "") != workspace_id:
                        raise RuntimeError("different workspace")
                    fence()
                    record=await self._admit_manager_attachment_execution(
                        action_id=action_id,record=record,config=config,workspace_id=workspace_id,
                    )
                except Exception as exc:
                    return {"state":"unverified","reason":f"durable manager execution is unverified: {type(exc).__name__}: {exc}"}
                state=str(record["state"]); tab_id=str(record["replacement_tab_id"] or ""); pane_id=str(record["replacement_pane_id"] or "")
            if not tab_id or not pane_id:
                self.db.advance_manager_attachment(action_id,states={state},state="uncertain")
                return {"state":"unverified","reason":"durable manager execution receipt is incomplete; refusing duplicate allocation"}
            current_generation=config.get("manager_binding_generation",0)
            expected_config=(str(config.get("manager_workspace_id") or "") == workspace_id
                             and str(config.get("manager_tab_id") or "") == tab_id
                             and str(config.get("manager_pane_id") or "") == pane_id
                             and config.get("manager_attachment_action_id") == action_id)
            try: expected_config = expected_config and int(current_generation) == expected_generation + 1
            except (TypeError,ValueError): expected_config=False
            if state in {"binding_intent","binding_persisted","launch_intent","launch_text_sent","launch_enter_sent","verified"} and not expected_config:
                self.db.advance_manager_attachment(action_id,states={state},state="rejected")
                return {"state":"unverified","reason":"replacement binding was changed by another owner"}
            if state in {"launch_intent","launch_text_sent","launch_enter_sent","verified"}:
                # A send may have reached the PTY despite cancellation. Only
                # inspect for the exact thread; never send resume a second time.
                try:
                    pane=(await self.herdr.request("pane.get",{"pane_id":pane_id},timeout=10)).get("pane",{})
                    status=await self._ensure_manager({"panes":[pane]}, {pane_id:pane},
                        allow_supervisor_reattach=True, require_verified=True,
                        attachment_action_id=action_id, allow_new_launch=False, attachment_fence=fence)
                    return status
                except Exception:
                    return {"state":"unverified","reason":"previous manager launch is unverified; refusing replay"}
            fence()
            if not self._persist_replacement_manager_binding(action_id=action_id,expected_generation=expected_generation,
                    old_workspace_id=old_workspace_id,old_pane_id=old_pane_id,workspace_id=workspace_id,tab_id=tab_id,pane_id=pane_id):
                self.db.advance_manager_attachment(action_id,states={state},state="uncertain")
                return {"state":"unverified","reason":"replacement binding compare-and-set failed"}
            self.db.advance_manager_attachment(action_id,states={state},state="binding_persisted")
            state="binding_persisted"
        if state == "binding_persisted":
            pane_id=str(self.db.manager_attachment(action_id)["replacement_pane_id"] or "")
            try:
                pane=(await self.herdr.request("pane.get",{"pane_id":pane_id},timeout=10)).get("pane",{})
            except Exception:
                return {"state":"unverified","reason":"persisted replacement pane is unavailable"}
            self.db.advance_manager_attachment(action_id,states={"binding_persisted"},state="launch_intent")
            return await self._ensure_manager({"panes":[pane]}, {pane_id:pane},allow_supervisor_reattach=True,
                require_verified=True,attachment_action_id=action_id,attachment_fence=fence)

        # Exact pane absence is required. The workspace may still be directly
        # live (pane-only loss), in which case only a new owned manager tab is
        # admitted. A timeout, protocol change or stale inventory is not loss.
        inventory_panes={str(item.get("pane_id")) for item in snapshot.get("panes",[])}
        if old_pane_id in inventory_panes:
            return {"state":"unverified","reason":"configured manager binding is still present in authoritative inventory"}
        workspace_alive=False
        try:
            workspace=(await self.herdr.request("workspace.get",{"workspace_id":old_workspace_id},timeout=10)).get("workspace",{})
            if str(workspace.get("workspace_id") or "") != old_workspace_id:
                return {"state":"unverified","reason":"configured manager workspace read returned a different identity"}
            workspace_alive=True
        except Exception as exc:
            if not self._exact_not_found(exc,"workspace"):
                return {"state":"unverified","reason":"configured manager workspace state is not exact"}
        try:
            await self.herdr.request("pane.get",{"pane_id":old_pane_id},timeout=10)
            return {"state":"unverified","reason":"configured manager pane still resolves"}
        except Exception as exc:
            if not self._exact_not_found(exc,"pane"):
                return {"state":"unverified","reason":"configured manager pane absence is not exact"}
        try:
            fresh=(await self.herdr.request("session.snapshot",{},timeout=15)).get("snapshot",{})
        except Exception:
            return {"state":"unverified","reason":"post-proof authoritative inventory is unavailable"}
        if old_pane_id in {str(item.get("pane_id")) for item in fresh.get("panes",[])}:
            return {"state":"unverified","reason":"configured manager binding reappeared during proof"}
        if not workspace_alive and old_workspace_id in {str(item.get("workspace_id")) for item in fresh.get("workspaces",[])}:
            return {"state":"unverified","reason":"configured manager workspace reappeared during proof"}
        # Installed Herdr session snapshots have no server-incarnation field.
        # Their revision is a layout/event revision, not restart evidence, so
        # do not manufacture an incarnation fence from it.
        if state != "intent":
            return {"state":"unverified","reason":"prior replacement creation is unverified; refusing duplicate allocation"}
        if workspace_alive:
            # The workspace is directly verified. Herdr's durable execution
            # record now owns the new no-focus manager tab and its replay.
            try:
                fence()
                record=await self._admit_manager_attachment_execution(
                    action_id=action_id,record=record,config=config,workspace_id=old_workspace_id,
                )
            except Exception as exc:
                return {"state":"unverified","reason":f"durable manager execution is unverified: {type(exc).__name__}: {exc}"}
        else:
            self.db.advance_manager_attachment(action_id,states={"intent"},state="create_intent",
                runtime_generation=None)
            try:
                fence()
                created=await self.herdr.request("workspace.create",{
                    "cwd":str(config["manager_cwd"]),"label":"control","env":{},"focus":False,
                },timeout=15)
            except BaseException:
                self.db.advance_manager_attachment(action_id,states={"create_intent"},state="uncertain")
                raise
            workspace=created.get("workspace") or {}; tab=created.get("tab") or {}; pane=created.get("root_pane") or {}
            workspace_id=str(workspace.get("workspace_id") or ""); tab_id=str(tab.get("tab_id") or ""); pane_id=str(pane.get("pane_id") or "")
            if not workspace_id or not tab_id or not pane_id or pane.get("workspace_id") != workspace_id:
                self.db.advance_manager_attachment(action_id,states={"create_intent"},state="uncertain",receipt={"create_response":created})
                return {"state":"unverified","reason":"Herdr returned an incomplete replacement workspace receipt"}
            record=self.db.advance_manager_attachment(action_id,states={"create_intent"},state="created",
                replacement_workspace_id=workspace_id,replacement_tab_id=tab_id,replacement_pane_id=pane_id,
                receipt={"creation_kind":"workspace.create","workspace_id":workspace_id,"tab_id":tab_id,"pane_id":pane_id})
            try:
                fence()
                record=await self._admit_manager_attachment_execution(
                    action_id=action_id,record=record,config=config,workspace_id=workspace_id,
                )
            except Exception as exc:
                return {"state":"unverified","reason":f"durable manager execution is unverified: {type(exc).__name__}: {exc}"}
        workspace_id=str(record["replacement_workspace_id"] or "")
        tab_id=str(record["replacement_tab_id"] or ""); pane_id=str(record["replacement_pane_id"] or "")
        self.db.advance_manager_attachment(action_id,states={"execution_admitted"},state="binding_intent")
        fence()
        if not self._persist_replacement_manager_binding(action_id=action_id,expected_generation=expected_generation,
                old_workspace_id=old_workspace_id,old_pane_id=old_pane_id,workspace_id=workspace_id,tab_id=tab_id,pane_id=pane_id):
            self.db.advance_manager_attachment(action_id,states={"binding_intent"},state="uncertain")
            return {"state":"unverified","reason":"replacement binding compare-and-set failed"}
        self.db.advance_manager_attachment(action_id,states={"binding_intent"},state="binding_persisted")
        # Cosmetic placement is intentionally after the durable binding; it
        # cannot influence ownership or cause a second allocation.
        fence(); await self.herdr.request("workspace.move",{"workspace_id":workspace_id,"insert_index":0})
        fence(); await self.herdr.request("tab.rename",{"tab_id":tab_id,"label":"manager"})
        fence(); await self.herdr.request("tab.move",{"tab_id":tab_id,"insert_index":0})
        self.db.advance_manager_attachment(action_id,states={"binding_persisted"},state="launch_intent")
        pane=(await self.herdr.request("pane.get",{"pane_id":pane_id},timeout=10)).get("pane",{})
        return await self._ensure_manager({"panes":[pane]}, {pane_id:pane},allow_supervisor_reattach=True,
            require_verified=True,attachment_action_id=action_id,attachment_fence=fence)

    async def _reconcile_root_topology(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        config = self.config()
        workspaces = list(snapshot.get("workspaces", []))
        configured_id = config.get("manager_workspace_id")
        configured_pane_id = config.get("manager_pane_id")
        has_workspace_binding = bool(configured_id)
        has_pane_binding = bool(configured_pane_id)
        if has_workspace_binding != has_pane_binding:
            raise RuntimeError("canonical manager bindings are incomplete; refusing topology mutation")

        # Once a manager has a persisted workspace/pane binding, only those
        # exact IDs are authoritative.  A restore can briefly return an empty
        # or stale snapshot while the native resume is in flight.  Label-based
        # discovery or allocation here would create a new manager and replace
        # the evidence that the supervisor is trying to verify.
        if has_workspace_binding:
            control = next((w for w in workspaces if w.get("workspace_id") == configured_id), None)
            manager_pane = next(
                (p for p in snapshot.get("panes", []) if p.get("pane_id") == configured_pane_id), None
            )
            if control is None or manager_pane is None or manager_pane.get("workspace_id") != configured_id:
                if self.manager_launching:
                    raise RuntimeError(
                        "canonical manager topology observation is transient while reattachment is in progress; refusing replacement"
                    )
                try:
                    live_control = await self.herdr.request(
                        "workspace.get", {"workspace_id": configured_id}, timeout=10
                    )
                    live_pane = await self.herdr.request(
                        "pane.get", {"pane_id": configured_pane_id}, timeout=10
                    )
                    control = live_control.get("workspace", live_control)
                    manager_pane = live_pane.get("pane", live_pane)
                except Exception as exc:
                    raise RuntimeError(
                        "configured canonical manager topology is unavailable; refusing replacement"
                    ) from exc
                if (control.get("workspace_id") != configured_id
                        or manager_pane.get("pane_id") != configured_pane_id
                        or manager_pane.get("workspace_id") != configured_id):
                    raise RuntimeError("configured canonical manager topology is inconsistent; refusing replacement")
            manager_tab = next(
                (t for t in snapshot.get("tabs", []) if t.get("tab_id") == manager_pane.get("tab_id")),
                {"tab_id": manager_pane.get("tab_id")},
            )
            await self.herdr.request(
                "workspace.move", {"workspace_id": configured_id, "insert_index": 0}
            )
            await self.herdr.request("tab.rename", {"tab_id": manager_tab["tab_id"], "label": "manager"})
            await self.herdr.request("tab.move", {"tab_id": manager_tab["tab_id"], "insert_index": 0})
            return control

        control = next((w for w in workspaces if w.get("workspace_id") == configured_id), None)
        if control is None:
            controls = [w for w in workspaces if w.get("label") == "control"]
            if len(controls) > 1:
                raise RuntimeError("multiple control workspaces exist; refusing ambiguous reconciliation")
            control = controls[0] if controls else None
        created_manager = False
        if control is None:
            created = await self.herdr.request(
                "workspace.create",
                {
                    "cwd": str(config.get("manager_cwd", str(package_root()))),
                    "label": "control",
                    "env": {},
                    "focus": False,
                },
            )
            control = created["workspace"]
            manager_tab = created["tab"]
            manager_pane = created["root_pane"]
            created_manager = True
        else:
            if control.get("label") != "control":
                await self.herdr.request(
                    "workspace.rename", {"workspace_id": control["workspace_id"], "label": "control"}
                )
            manager_pane_id = config.get("manager_pane_id")
            manager_pane = next(
                (p for p in snapshot.get("panes", []) if p.get("pane_id") == manager_pane_id), None
            )
            if manager_pane is None:
                manager_agent = next(
                    (
                        a for a in snapshot.get("agents", [])
                        if a.get("name") == "control-manager"
                        and a.get("workspace_id") == control["workspace_id"]
                    ),
                    None,
                )
                if manager_agent:
                    manager_pane = next(
                        (p for p in snapshot.get("panes", []) if p.get("pane_id") == manager_agent["pane_id"]),
                        manager_agent,
                    )
            if manager_pane is None:
                made = await self.herdr.request(
                    "tab.create",
                    {
                        "workspace_id": control["workspace_id"],
                        "cwd": str(config.get("manager_cwd", str(package_root()))),
                        "label": "manager",
                        "env": {},
                        "focus": False,
                    },
                )
                manager_tab = made["tab"]
                manager_pane = made["root_pane"]
                created_manager = True
            else:
                manager_tab = next(
                    (t for t in snapshot.get("tabs", []) if t.get("tab_id") == manager_pane.get("tab_id")),
                    {"tab_id": manager_pane.get("tab_id")},
                )
        await self.herdr.request(
            "workspace.move", {"workspace_id": control["workspace_id"], "insert_index": 0}
        )
        await self.herdr.request("tab.rename", {"tab_id": manager_tab["tab_id"], "label": "manager"})
        await self.herdr.request("tab.move", {"tab_id": manager_tab["tab_id"], "insert_index": 0})
        self._persist_config_updates(
            manager_workspace_id=control["workspace_id"],
            manager_tab_id=manager_tab["tab_id"],
            manager_pane_id=manager_pane["pane_id"],
            manager_binding_generation=int(config.get("manager_binding_generation",0)) + 1,
        )
        if created_manager:
            LOG.info("created reserved control/manager pane %s", manager_pane["pane_id"])
        return control

    @staticmethod
    def _tab_label(row: sqlite3.Row) -> str:
        summary = row["summary"].removeprefix("Queued: ")
        safe = re.sub(r"[^A-Za-z0-9_. -]+", "", summary).strip()
        return f"{row['id'][-8:]} {safe[:38]}".strip()

    async def _create_worker_pane(
        self, row: sqlite3.Row, env: dict[str, str]
    ) -> tuple[str, dict[str, Any], dict[str, Any]]:
        """Allocate a task tab while keeping one workspace per target.

        Only topology allocation is serialized for a given target.  Native
        thread creation and all worker execution happen after this lock and
        remain unlimited/concurrent.
        """
        if self._is_control_cwd(row["cwd"]):
            # Control work shares the manager workspace, but each task keeps
            # its own no-focus tab, CWD, and task identity.
            async with self.control_topology_lock:
                control = await self._control_workspace()
                created = await self.herdr.request(
                    "tab.create",
                    {
                        "workspace_id": control["workspace_id"],
                        "cwd": row["cwd"],
                        "label": self._tab_label(row),
                        "env": env,
                        "focus": False,
                    },
                )
                return control["workspace_id"], created["tab"], created["root_pane"]
        lock = self.target_topology_locks.setdefault(row["target"], asyncio.Lock())
        async with lock:
            target = self.db.target(row["target"])
            workspace_id = target["workspace_id"] if target else None
            if not workspace_id or not await self._workspace_exists(workspace_id):
                created = await self.herdr.request(
                    "workspace.create",
                    {"cwd": row["cwd"], "label": row["target"], "env": env, "focus": False},
                )
                workspace = created["workspace"]
                tab = created["tab"]
                pane = created["root_pane"]
                workspace_id = workspace["workspace_id"]
                self.db.set_target_workspace(row["target"], workspace_id)
                await self.herdr.request(
                    "tab.rename", {"tab_id": tab["tab_id"], "label": self._tab_label(row)}
                )
            else:
                created = await self.herdr.request(
                    "tab.create",
                    {
                        "workspace_id": workspace_id,
                        "cwd": row["cwd"],
                        "label": self._tab_label(row),
                        "env": env,
                        "focus": False,
                    },
                )
                tab = created["tab"]
                pane = created["root_pane"]
            return workspace_id, tab, pane

    async def _start_task(self, row: sqlite3.Row) -> None:
        task_id = row["id"]
        current = self.db.task(task_id)
        if current is None or current["work_kind"] != "codex":
            raise RuntimeError("native Codex launch refused: task is not Codex-owned")
        row = current
        if row["state"] != "starting":
            self.db.update(task_id, event="dispatch_starting", state="starting", started_at=self.clock(), summary="Starting Codex worker.")
        try:
            env = {
                "CONTROL_TASK_ID": task_id,
                "CONTROL_BROKER_SOCKET": str(self.socket_path),
                "CONTROL_PARENT_ID": row["parent_id"] or "",
            }
            workspace_id, tab, pane = await self._create_worker_pane(row, env)
            # Herdr creates the pane before its foreground-process classifier
            # necessarily observes the initial shell.  Starting an agent before
            # that point can fail with agent_pane_busy even though the pane is
            # otherwise brand new and broker-owned.
            await self._wait_pane_shell(pane["pane_id"])
            agent_name = f"ctl_{uuid.uuid4().hex[:12]}"
            worker_prompt = self._worker_prompt(row, row["prompt_pending"])
            native_session, native_turn_id = await self._create_native_worker(row, worker_prompt)
            self.db.update(
                task_id,
                workspace_id=workspace_id,
                tab_id=tab["tab_id"],
                pane_id=pane["pane_id"],
                agent_kind="codex",
                agent_name=agent_name,
                agent_session_id=native_session,
                native_turn_id=native_turn_id,
                herdr_state="starting",
                prompt_pending=None,
                summary="Native worker turn started; attaching visible Codex TUI.",
            )
            self.subscription_refresh.set()
            self._schedule_native_turn_monitor(task_id, native_turn_id)
            try:
                agent_args = [
                    "--remote",
                    str(self.config().get("app_server_remote", "unix://")),
                    "resume",
                    native_session,
                ]
                started = await self._start_visible_codex(
                    agent_name=agent_name,
                    pane_id=pane["pane_id"],
                    args=agent_args,
                )
            except RuntimeError as exc:
                if "agent_not_ready" not in str(exc):
                    raise
                blocked = self.db.update(
                    task_id,
                    state="blocked",
                    priority="attention" if row["priority"] in {"routine", "normal"} else row["priority"],
                    herdr_state="blocked",
                    summary="Worker startup needs interactive input in its Herdr tab.",
                )
                asyncio.create_task(self.emit_attention(blocked))
                return
            agent = await self._wait_agent_ready(agent_name)
            session = agent.get("agent_session") or {}
            if agent.get("agent_status") == "blocked" or self.db.task(task_id)["state"] == "blocked":
                blocked = self.db.update(
                    task_id,
                    state="blocked",
                    priority="attention" if row["priority"] in {"routine", "normal"} else row["priority"],
                    agent_session_id=session.get("value"),
                    herdr_state="blocked",
                    summary="Worker startup needs interactive input in its Herdr tab.",
                )
                asyncio.create_task(self.emit_attention(blocked))
                return
            observed_session = session.get("value")
            if observed_session and observed_session != native_session:
                raise RuntimeError(
                    f"worker identity mismatch: expected {native_session}, observed {observed_session}"
                )
            live_state = agent.get("agent_status", "working")
            fresh = self.db.task(task_id)
            if fresh is not None and fresh["state"] in TERMINAL_STATES:
                # A short app-server turn can send its terminal control-report
                # while the visible TUI is still attaching.  Preserve that
                # terminal evidence instead of regressing the task to working.
                self.db.update(
                    task_id,
                    event="native_tui_attached_after_terminal",
                    agent_session_id=native_session,
                    herdr_state=live_state,
                )
                await self._verified_terminal(task_id)
                return
            self.db.update(
                task_id,
                event="native_turn_started",
                state="working",
                prompt_pending=None,
                agent_session_id=native_session,
                herdr_state=live_state,
                summary="Worker accepted the task through app-server.",
            )
        except Exception as exc:
            LOG.exception("failed to start task %s", task_id)
            observed_state = "unknown"
            fresh = self.db.task(task_id)
            if fresh and fresh["agent_name"]:
                with contextlib.suppress(Exception):
                    result = await self.herdr.request(
                        "agent.get", {"target": fresh["agent_name"]}, timeout=10
                    )
                    observed_state = result.get("agent", result).get("agent_status", "unknown")
            failed = self.db.update(
                task_id,
                state="failed",
                summary=f"Worker startup failed: {str(exc)[:1200]}",
                prompt_pending=None,
                finished_at=self.clock(),
                terminal_reported_at=self.clock(),
                herdr_state=observed_state,
            )
            asyncio.create_task(self.emit_attention(failed))
            if observed_state in {"idle", "done"}:
                await self._verified_terminal(task_id)
            self.scheduler_event.set()

    async def _fail_unsupported_work_kinds(self, boundary: str) -> None:
        """Keep corrupt/future task kinds recorded but fail them before ownership.

        A task kind is a persisted ownership boundary, not a presentation hint.
        Unknown rows must never be guessed as Codex or command work during
        startup recovery, scheduling, reconciliation, or monitoring.
        """
        for row in self.db.unsupported_work_kind_tasks():
            if row["state"] in TERMINAL_STATES:
                continue
            updated = self.db.update(
                row["id"], event="unsupported_work_kind_failed", state="failed",
                summary=(f"Unsupported persisted work kind {row['work_kind']!r} at {boundary}; "
                         "launch was refused."),
                finished_at=self.clock(), terminal_reported_at=self.clock(),
                session_state="closed", herdr_state="unknown",
            )
            asyncio.create_task(self.emit_attention(updated))

    async def _wait_agent_ready(self, agent_name: str, timeout: float = 60) -> dict[str, Any]:
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                result = await self.herdr.request("agent.get", {"target": agent_name}, timeout=10)
                agent = result.get("agent", result)
                if agent.get("agent_status") == "blocked":
                    return agent
                if agent.get("interactive_ready") and not agent.get("launch_pending"):
                    return agent
            except Exception as exc:
                last_error = exc
            await asyncio.sleep(0.25)
        raise RuntimeError(f"Codex worker did not become ready: {last_error or 'timeout'}")

    async def _wait_pane_shell(self, pane_id: str, timeout: float = 10) -> None:
        deadline = time.monotonic() + timeout
        last: list[dict[str, Any]] = []
        while time.monotonic() < deadline:
            result = await self.herdr.request(
                "pane.process_info", {"pane_id": pane_id}, timeout=10
            )
            info = result.get("process_info", result)
            last = info.get("foreground_processes") or []
            names = {str(item.get("name", "")) for item in last}
            if last and names <= {"sh", "bash", "zsh", "fish"}:
                return
            await asyncio.sleep(0.1)
        raise RuntimeError(f"pane {pane_id} did not reach an available shell: {last}")

    async def _start_visible_codex(
        self, *, agent_name: str, pane_id: str, args: list[str], timeout: float = 15
    ) -> dict[str, Any]:
        """Attach one TUI to its already-owned pane despite classifier jitter.

        This retries only the idempotent attachment to the same pane and native
        session.  It never creates another task, pane, or Codex thread.
        """
        deadline = time.monotonic() + timeout
        last_error: Exception | None = None
        while time.monotonic() < deadline:
            try:
                await self._wait_pane_shell(
                    pane_id, timeout=min(3, max(0.2, deadline - time.monotonic()))
                )
            except RuntimeError as exc:
                last_error = exc
                await asyncio.sleep(0.2)
                continue
            try:
                return await self.herdr.request(
                    "agent.start",
                    {
                        "name": agent_name,
                        "kind": "codex",
                        "pane_id": pane_id,
                        "args": args,
                        "timeout_ms": 60_000,
                    },
                    timeout=70,
                )
            except RuntimeError as exc:
                if "agent_pane_busy" not in str(exc):
                    raise
                last_error = exc
                await asyncio.sleep(0.2)
        raise RuntimeError(
            f"visible Codex TUI could not attach to pane {pane_id}: {last_error or 'timeout'}"
        )

    async def _deliver_pending_prompt(self, task_id: str) -> None:
        fresh = self.db.task(task_id)
        if fresh is None or not fresh["prompt_pending"]:
            return
        worker_prompt = self._worker_prompt(fresh, fresh["prompt_pending"])
        agent = await self._prompt_verified_agent(fresh, worker_prompt)
        session = agent.get("agent_session") or {}
        self.db.update(
            task_id,
            state="working",
            herdr_state="working",
            summary="Worker accepted the task.",
            prompt_pending=None,
            agent_session_id=session.get("value") or fresh["agent_session_id"],
        )

    async def _prompt_verified_agent(
        self,
        row: sqlite3.Row,
        text: str,
        *,
        timeout: float = 30,
        client_user_message_id: str | None = None,
    ) -> dict[str, Any]:
        """Prompt one exact live Codex pane and verify that pane changed state.

        Herdr's high-level prompt *wait* observes session lifecycle and can be
        satisfied by the already-working manager. The atomic prompt operation
        is still the correct safe-paste primitive, so call it without a wait and
        then bind observation to this target's own state-change sequence.
        """
        target = row["agent_name"] or row["pane_id"]
        pane_id = row["pane_id"]
        if not target or not pane_id:
            raise RuntimeError("worker has no verified live agent pane")
        result = await self.herdr.request("agent.get", {"target": target}, timeout=10)
        before = result.get("agent", result)
        if before.get("agent") != "codex" or before.get("pane_id") != pane_id:
            raise RuntimeError("worker pane no longer contains the expected Codex agent")
        if before.get("agent_status") not in {"idle", "done", "unknown"}:
            raise RuntimeError(
                f"worker is not promptable (state={before.get('agent_status')})"
            )
        before_seq = int(before.get("state_change_seq") or 0)
        if not text.strip():
            raise ValueError("worker prompt is empty")
        turn_id = await self._start_native_turn(
            row, text, client_user_message_id=client_user_message_id
        )
        self.db.update(row["id"], native_turn_id=turn_id)
        self._schedule_native_turn_monitor(row["id"], turn_id)
        deadline = time.monotonic() + timeout
        last = before
        while time.monotonic() < deadline:
            result = await self.herdr.request("agent.get", {"target": target}, timeout=10)
            last = result.get("agent", result)
            status = last.get("agent_status")
            sequence = int(last.get("state_change_seq") or 0)
            if status in {"working", "blocked"} or sequence > before_seq:
                return last
            await asyncio.sleep(0.1)
        raise RuntimeError(
            f"target worker did not acknowledge prompt within {timeout:g}s "
            f"(state={last.get('agent_status')}, seq={last.get('state_change_seq')})"
        )

    async def _create_native_worker(self, row: sqlite3.Row, text: str) -> tuple[str, str]:
        result = await self._run_json_required(
            [
                "/usr/bin/python3",
                str(runtime_root() / "worker_appserver.py"),
                "create",
                "--cwd",
                row["cwd"],
                "--task-id",
                row["id"],
                "--broker-socket",
                str(self.socket_path),
                "--parent-id",
                row["parent_id"] or "",
                "--model",
                row["model"] or "gpt-5.6-sol",
                "--reasoning-effort",
                row["reasoning_effort"] or "low",
                "--name",
                f"Control worker {row['id']}",
                "--text",
                text,
            ],
            timeout=30,
            env=self._appserver_env(),
        )
        session_id = result.get("thread_id")
        if not isinstance(session_id, str) or not re.fullmatch(
            r"[A-Za-z0-9-]{20,80}", session_id
        ):
            raise RuntimeError("app-server returned an invalid worker thread id")
        if not result.get("turn_id"):
            raise RuntimeError("app-server did not start the initial worker turn")
        if result.get("cwd") and str(Path(result["cwd"]).resolve()) != row["cwd"]:
            raise RuntimeError("app-server created worker in an unexpected CWD")
        return session_id, str(result["turn_id"])

    async def _start_native_turn(
        self, row: sqlite3.Row, text: str, *, client_user_message_id: str | None = None
    ) -> str:
        session_id = row["agent_session_id"]
        if not session_id:
            raise RuntimeError("worker has no native Codex session id")
        argv = [
                "/usr/bin/python3",
                str(runtime_root() / "worker_appserver.py"),
                "prompt",
                "--thread-id",
                session_id,
                "--task-id",
                row["id"],
                "--broker-socket",
                str(self.socket_path),
                "--parent-id",
                row["parent_id"] or "",
                "--model",
                row["model"] or "gpt-5.6-sol",
                "--reasoning-effort",
                row["reasoning_effort"] or "low",
                "--text",
                text,
            ]
        if client_user_message_id:
            argv.extend(["--client-user-message-id", client_user_message_id])
        result = await self._run_json_required(
            argv,
            timeout=30,
            env=self._appserver_env(),
        )
        if result.get("thread_id") != session_id or not result.get("turn_id"):
            raise RuntimeError("app-server did not start the expected worker turn")
        return str(result["turn_id"])

    def _schedule_native_turn_monitor(self, task_id: str, turn_id: str) -> None:
        """Observe the exact app-server turn independently of Herdr's TUI events."""
        old = self.native_turn_timers.pop(task_id, None)
        if old:
            old.cancel()
        self.native_turn_timers[task_id] = asyncio.create_task(
            self._monitor_native_turn(task_id, turn_id), name=f"native-turn-{task_id}"
        )

    async def _monitor_native_turn(self, task_id: str, turn_id: str) -> None:
        """Fail closed when a worker omits its final report.

        A native terminal state proves only that a turn stopped. Explicit
        control-report remains authoritative for semantic success or failure.
        """
        try:
            deadline = time.monotonic() + 45
            while time.monotonic() < deadline:
                row = self.db.task(task_id)
                if row is not None and row["work_kind"] != "codex":
                    if row["state"] not in TERMINAL_STATES:
                        updated = self.db.update(
                            task_id, event="native_monitor_ownership_refused", state="failed",
                            summary="Native Codex monitor refused a non-Codex task ownership boundary.",
                            finished_at=self.clock(), terminal_reported_at=self.clock(),
                        )
                        asyncio.create_task(self.emit_attention(updated))
                    return
                if row is None or row["native_turn_id"] != turn_id:
                    return
                if row["terminal_reported_at"] is not None or row["state"] in {"waiting_human", "blocked"}:
                    return
                if not row["agent_session_id"]:
                    return
                try:
                    result = await self._run_json_required(
                        ["/usr/bin/python3", str(runtime_root() / "worker_appserver.py"),
                         "status", "--thread-id", row["agent_session_id"], "--turn-id", turn_id], timeout=10,
                        env=self._appserver_env(),
                    )
                    status = result.get("turn_status")
                    if status in {"completed", "failed", "interrupted"}:
                        current = self.db.task(task_id)
                        if current is None or current["native_turn_id"] != turn_id or current["terminal_reported_at"] is not None:
                            return
                        updated = self.db.update(
                            task_id, event="native_turn_report_missing", state="unknown",
                            priority="attention" if current["priority"] in {"routine", "normal"} else current["priority"],
                            summary=("Native Codex turn %s finished (%s) without the required final "
                                     "control-report; no semantic result is trusted. Inspect or continue this "
                                     "same task/session to recover its context.") % (turn_id[-12:], status),
                        )
                        asyncio.create_task(self.emit_attention(updated))
                        self.scheduler_event.set()
                        return
                except Exception as exc:
                    LOG.debug("native turn monitor pending for %s: %s", task_id, exc)
                await asyncio.sleep(0.5)
        finally:
            if self.native_turn_timers.get(task_id) is asyncio.current_task():
                self.native_turn_timers.pop(task_id, None)

    def _schedule_reported_codex_reconcile(self, task_id: str) -> None:
        old = self.settle_timers.pop(task_id, None)
        if old:
            old.cancel()
        self.settle_timers[task_id] = asyncio.create_task(
            self._reconcile_reported_codex_terminal(task_id),
            name=f"codex-terminal-{task_id}",
        )

    async def _reconcile_reported_codex_terminal(self, task_id: str) -> None:
        """Bridge a missed TUI settle event from official app-server state."""
        try:
            deadline = time.monotonic() + 30
            while time.monotonic() < deadline:
                row = self.db.task(task_id)
                if row is None or row["state"] not in TERMINAL_STATES:
                    return
                if row["herdr_state"] in {"idle", "done", "closed"}:
                    await self._verified_terminal(task_id)
                    return
                if not row["agent_session_id"] or not row["pane_id"]:
                    return
                try:
                    result = await self._run_json_required(
                        [
                            "/usr/bin/python3",
                            str(runtime_root() / "worker_appserver.py"),
                            "status",
                            "--thread-id",
                            row["agent_session_id"],
                        ],
                        timeout=10,
                        env=self._appserver_env(),
                    )
                    turn_status = result.get("turn_status")
                    if turn_status in {"completed", "failed", "interrupted"}:
                        await self.herdr.request(
                            "pane.report_agent",
                            {
                                "pane_id": row["pane_id"],
                                "source": f"control-worker-{task_id[-12:]}",
                                "agent": "codex",
                                "state": "idle",
                                "message": f"Codex app-server turn {turn_status}",
                                "seq": int(self.clock() * 1000),
                                "agent_session_id": row["agent_session_id"],
                            },
                            timeout=10,
                        )
                        observed = "idle"
                        with contextlib.suppress(Exception):
                            got = await self.herdr.request(
                                "agent.get",
                                {"target": row["agent_name"] or row["pane_id"]},
                                timeout=10,
                            )
                            observed = got.get("agent", got).get("agent_status", observed)
                        if observed in {"idle", "done"}:
                            self.db.update(
                                task_id,
                                event="appserver_turn_settled",
                                herdr_state=observed,
                            )
                            await self._verified_terminal(task_id)
                            return
                except Exception as exc:
                    LOG.debug("app-server terminal reconciliation pending for %s: %s", task_id, exc)
                await asyncio.sleep(0.5)
        finally:
            if self.settle_timers.get(task_id) is asyncio.current_task():
                self.settle_timers.pop(task_id, None)

    async def _resume_startup(self, task_id: str, agent_name: str) -> None:
        try:
            await self._wait_agent_ready(agent_name)
            await self._deliver_pending_prompt(task_id)
        except Exception as exc:
            row = self.db.task(task_id)
            if row and row["state"] not in TERMINAL_STATES:
                failed = self.db.update(
                    task_id,
                    state="failed",
                    summary=f"Worker could not resume after interactive startup: {str(exc)[:1200]}",
                    finished_at=self.clock(),
                )
                asyncio.create_task(self.emit_attention(failed))
                self.scheduler_event.set()

    def _worker_prompt(self, row: sqlite3.Row, prompt: str) -> str:
        report = (
            f"control-report --task-id {row['id']} "
            f"--socket {self.socket_path}"
        )
        return f"""You are a Codex worker dispatched by the Control Manager.

Control task ID: {row['id']}
Parent task ID: {row['parent_id'] or 'none'}
Target key: {row['target']}
Working directory: {row['cwd']}

Follow the global and repository-local AGENTS instructions that apply in this working directory. The Control Manager policy does not apply to you. Complete the requested work autonomously and verify it in proportion to risk.

The broker already owns and verifies your Herdr workspace, tab, pane, naming, visibility, and task identity. Do not try to discover, rename, or validate those topology details from inside the worker, and never report blocked merely because you cannot introspect them. Concentrate on the substantive requested work and its target files.

Checkpoint contract:
- Run `{report} --state working --summary TEXT` for a meaningful long-running checkpoint.
- If a new consequential action falls outside already confirmed scope, run `{report} --state waiting_human --summary TEXT --question TEXT --priority attention`, then stop the turn and wait.
- On success, run `{report} --state completed --summary TEXT` immediately before your final response. The summary may be up to 8000 characters and must be a self-contained, concise version of the requested deliverable so the manager can relay it without reading a hidden transcript.
- If work cannot proceed at the end of this turn because of any dependency, external system, permission, or other hold, run `{report} --state blocked --summary TEXT --priority attention`; reserve `failed` for a failed attempted operation. Do not leave a recoverable hold as `working` and end the turn.
- Use `{report} --state waiting_human --summary TEXT --question TEXT --priority attention` only when a human decision/input is required; `waiting_human` always requires a concrete question. Never end after only a `working` checkpoint: without a terminal `completed`, `blocked`, `failed`, or `waiting_human` report, a settled worker is deliberately recorded as `unknown`.
- Summaries and questions must be bounded and must never contain credentials or complete transcripts.

Requested task:
{prompt}
"""

    async def reconcile(self, *, allow_supervisor_reattach: bool = False,
                        strict: bool = False, require_manager_reattach: bool = False,
                        recovery_action_id: str | None = None,
                        recovery_claim_sha256: str | None = None,
                        recovery_claim_key: str | None = None,
                        recovery_owner_generation: str | None = None) -> dict[str, Any]:
        """Reconcile durable work, optionally requiring authoritative topology evidence.

        Periodic reconciliation remains best-effort so an unavailable Herdr
        does not take down scheduling. Supervisor RPC callers pass ``strict``;
        they must receive no affirmative recovery evidence without both fresh
        snapshots and (for topology) a verified canonical native pane.
        """
        return await self._reconcile_locked(
            allow_supervisor_reattach=allow_supervisor_reattach,
            strict=strict,
            require_manager_reattach=require_manager_reattach,
            recovery_action_id=recovery_action_id,
            recovery_claim_sha256=recovery_claim_sha256,
            recovery_claim_key=recovery_claim_key,
            recovery_owner_generation=recovery_owner_generation,
        )

    async def _reconcile_locked(self, *, allow_supervisor_reattach: bool,
                                strict: bool, require_manager_reattach: bool,
                                recovery_action_id: str | None, recovery_claim_sha256: str | None,
                                recovery_claim_key: str | None, recovery_owner_generation: str | None) -> dict[str, Any]:
        await self._fail_unsupported_work_kinds("reconciliation")
        self.db.sync_all_feature_child_states()
        if self.config().get("agent_turn_consumer_enabled"):
            try:
                await self.consume_native_turns()
            except Exception as exc:
                LOG.warning("native agent-turn reconciliation deferred: %s", exc)
        topology = await self._reconcile_manager_topology(
            allow_supervisor_reattach=allow_supervisor_reattach, strict=strict,
            require_manager_reattach=require_manager_reattach,
            recovery_action_id=recovery_action_id, recovery_claim_sha256=recovery_claim_sha256,
            recovery_claim_key=recovery_claim_key, recovery_owner_generation=recovery_owner_generation,
        )
        if topology.get("deferred"):
            return {"reconciled": False, "reason": topology["reason"]}
        snapshot = topology["snapshot"]
        panes = topology["panes"]
        workspaces = topology["workspaces"]
        manager_reattach = topology["manager_reattach"]
        # A supervisor topology claim is deliberately bounded to its exact
        # native proof.  Historical worker reconciliation can contain many
        # slow agent.get calls and must neither hold the topology lock nor
        # consume the supervisor RPC deadline after proof is available.
        if require_manager_reattach:
            return {"reconciled": True, "manager_reattach": manager_reattach}
        for target in self.db.conn.execute("SELECT * FROM targets").fetchall():
            if target["workspace_id"] and target["workspace_id"] not in workspaces:
                self.db.set_target_workspace(target["target"], None)
        for pane_id, pane in panes.items():
            row = self.db.task_for_pane(pane_id)
            session = pane.get("agent_session") or {}
            if row is not None and row["work_kind"] == "command" and row["agent_session_id"]:
                # A TUI launched inside a shell command is not the broker's
                # native worker session. Retain the command record, but remove
                # this misleading continuation identity during reconciliation.
                self.db.update(row["id"], event="command_non_native_session_cleared", agent_session_id=None)
            # A Codex session value from a pane snapshot is not adopted here.
            # The pane could be a recycled ID; provenance is checked below
            # before any lifecycle or native-thread mutation.
        for row in self.db.reconcilable_tasks():
            pane = panes.get(row["pane_id"])
            if pane is None:
                if row["work_kind"] == "command" and any(
                    item["event"] == "command_native_admitted"
                    for item in self.db.transitions(row["id"])
                ):
                    try:
                        result = await self.herdr.request(
                            "execution.get", {"execution_id": row["id"]}, timeout=10
                        )
                        await self._apply_native_execution(row["id"], result["execution"])
                    except Exception as exc:
                        LOG.debug("native command reconnect pending for %s: %s", row["id"], exc)
                    continue
                if row["state"] == "starting" and self.clock() - row["updated_at"] < 5:
                    # A layout event can trigger reconciliation between the tab
                    # creation response and its appearance in a fresh snapshot.
                    continue
                if row["state"] in TERMINAL_STATES:
                    self.db.update(
                        row["id"], herdr_state="closed",
                        session_state="dormant" if row["work_kind"] == "codex" else "closed",
                        cleanup_deadline=None, event="terminal_pane_absent_on_reconcile",
                    )
                elif row["state"] in {"waiting_human", "blocked"}:
                    # A durable human/governance decision is not in-flight
                    # model work.  Losing its Herdr pane removes only the
                    # local session binding; retain the semantic blocker,
                    # question, feature claim, and task identity verbatim.
                    self.db.update(
                        row["id"], herdr_state="missing", session_state="dormant",
                        cleanup_deadline=None, event="settled_wait_pane_absent_on_reconcile",
                    )
                else:
                    if (
                        row["state"] == "unknown"
                        and row["herdr_state"] == "missing"
                        and row["summary"] == "Worker pane is absent after reconciliation."
                    ):
                        continue
                    updated = self.db.update(
                        row["id"], state="unknown", herdr_state="missing",
                        summary="Worker pane is absent after reconciliation.",
                        event="active_pane_missing_on_reconcile",
                    )
                    asyncio.create_task(self.emit_attention(updated))
                continue
            herdr_state = pane.get("agent_status", "unknown")
            if row["work_kind"] == "codex":
                provenance, detail = await self._worker_binding_provenance(row)
                if provenance != "owned":
                    await self._isolate_unowned_binding(
                        row, provenance, detail, boundary="reconcile"
                    )
                    continue
            values: dict[str, Any] = {"herdr_state": herdr_state}
            self.db.update(row["id"], **values)
            await self._apply_lifecycle(row["id"], herdr_state, binding_verified=True)
        for row in self.db.conn.execute(
            """SELECT * FROM tasks WHERE terminal_reported_at IS NOT NULL
               AND state IN ('completed','failed','cancelled') AND session_state='live'"""
        ).fetchall():
            pane = panes.get(row["pane_id"])
            if row["work_kind"] == "codex" and pane and pane.get("agent_status") in {"idle", "done"}:
                self.db.update(row["id"], herdr_state=pane.get("agent_status"))
                await self._verified_terminal(row["id"])
            elif row["work_kind"] == "command" and pane:
                asyncio.create_task(self._verify_command_completion(row["id"]))
        self.scheduler_event.set()
        return {"reconciled": True, "manager_reattach": manager_reattach}

    async def _reconcile_manager_topology(
        self, *, allow_supervisor_reattach: bool, strict: bool, require_manager_reattach: bool,
        recovery_action_id: str | None, recovery_claim_sha256: str | None,
        recovery_claim_key: str | None, recovery_owner_generation: str | None,
    ) -> dict[str, Any]:
        """Run only the short, manager-owned topology/proof critical section."""
        async with self.manager_topology_lock:
            try:
                result = await self.herdr.request("session.snapshot", {}, timeout=15)
                snapshot = result.get("snapshot", result)
            except Exception as exc:
                LOG.warning("Herdr reconciliation deferred: %s", exc)
                if strict:
                    raise RuntimeError(f"authoritative Herdr snapshot unavailable: {type(exc).__name__}: {exc}") from exc
                return {"deferred": True, "reason": f"Herdr snapshot unavailable: {type(exc).__name__}: {exc}"}
            # A claimed supervisor topology repair must fail closed on a missing
            # reserved pane. It must not let the broker's ordinary root-layout
            # helper create a substitute workspace/pane and call that evidence.
            if not require_manager_reattach:
                try:
                    await self._reconcile_root_topology(snapshot)
                except RuntimeError as exc:
                    if strict:
                        raise
                    LOG.warning("root topology reconciliation deferred: %s", exc)
                    return {"deferred": True, "reason": str(exc)}
            try:
                result = await self.herdr.request("session.snapshot", {}, timeout=15)
                snapshot = result.get("snapshot", result)
            except Exception as exc:
                if strict:
                    raise RuntimeError(f"authoritative post-reconcile Herdr snapshot unavailable: {type(exc).__name__}: {exc}") from exc
                LOG.warning("Herdr post-reconciliation snapshot deferred: %s", exc)
                return {"deferred": True, "reason": f"post-reconcile Herdr snapshot unavailable: {type(exc).__name__}: {exc}"}
            panes = {pane["pane_id"]: pane for pane in snapshot.get("panes", [])}
            config=self.config()
            configured_workspace=str(config.get("manager_workspace_id") or "")
            configured_pane=str(config.get("manager_pane_id") or "")
            # A workspace can survive a PTY/pane crash.  The recovery proof
            # distinguishes that pane-only loss and creates only a new tab in
            # the surviving workspace; ordinary reconciliation still refuses
            # both forms of absence.
            missing_binding=(configured_workspace and configured_pane and configured_pane not in panes)
            pending_replacement=(recovery_action_id is not None
                                 and config.get("manager_attachment_action_id") == recovery_action_id
                                 and self.db.manager_attachment(recovery_action_id) is not None)
            if (require_manager_reattach and allow_supervisor_reattach and missing_binding
                    and recovery_action_id and recovery_claim_sha256) or (
                        require_manager_reattach and allow_supervisor_reattach and pending_replacement
                        and recovery_action_id and recovery_claim_sha256):
                manager_reattach=await self._recover_proven_lost_manager_binding(
                    snapshot,action_id=recovery_action_id,claim_sha256=recovery_claim_sha256,
                    claim_key=recovery_claim_key,owner_generation=recovery_owner_generation,
                )
            elif (require_manager_reattach and allow_supervisor_reattach and configured_pane in panes
                  and recovery_action_id and recovery_claim_sha256):
                disconnect = await self._manager_terminal_disconnect_evidence(configured_pane, config)
                if disconnect.get("proven"):
                    manager_reattach = await self._recover_proven_disconnected_manager(
                        snapshot, action_id=recovery_action_id, claim_sha256=recovery_claim_sha256,
                        claim_key=recovery_claim_key, owner_generation=recovery_owner_generation,
                    )
                else:
                    manager_reattach = await self._ensure_manager(
                        snapshot, panes, allow_supervisor_reattach=allow_supervisor_reattach,
                        require_verified=require_manager_reattach,
                    )
            else:
                manager_reattach = await self._ensure_manager(
                    snapshot, panes, allow_supervisor_reattach=allow_supervisor_reattach,
                    require_verified=require_manager_reattach,
                )
            return {
                "snapshot": snapshot, "panes": panes,
                "workspaces": {workspace["workspace_id"] for workspace in snapshot.get("workspaces", [])},
                "manager_reattach": manager_reattach,
            }

    async def _ensure_manager(
        self, snapshot: dict[str, Any], panes: dict[str, dict[str, Any]], *, allow_supervisor_reattach: bool = False,
        require_verified: bool = False, attachment_action_id: str | None = None,
        allow_new_launch: bool = True, attachment_fence: Any = None,
    ) -> dict[str, str]:
        """Resume the canonical manager in its reserved pane when it is at a shell.

        This deliberately does not use Herdr's generic agent restore machinery: the
        broker owns the exact remote endpoint, profile, CWD, thread, and hook flags.
        """
        config = self.config()
        if config.get("supervisor_owns_recovery") and not allow_supervisor_reattach:
            return {"state": "not_requested", "reason": "supervisor owns automatic recovery"}
        if self.manager_launching:
            return {"state": "unverified", "reason": "canonical manager reattachment is already in progress"}
        required = ("manager_thread_id", "manager_cwd", "manager_workspace_id", "manager_pane_id")
        if any(not config.get(key) for key in required):
            return {"state": "unverified", "reason": "canonical manager bindings are incomplete"}

        thread_id = bounded(str(config["manager_thread_id"]), 80, "manager_thread_id", required=True)
        if not re.fullmatch(r"[A-Za-z0-9-]{20,80}", thread_id or ""):
            LOG.warning("manager recovery disabled: invalid thread id")
            return {"state": "unverified", "reason": "invalid canonical manager thread id"}
        manager_cwd = normalized_cwd(str(config["manager_cwd"]))
        profile = str(config.get("profile", "control-manager"))
        if not TARGET_RE.fullmatch(profile):
            LOG.warning("manager recovery disabled: invalid profile")
            return {"state": "unverified", "reason": "invalid canonical manager profile"}
        remote = str(config.get("app_server_remote", "unix://"))
        if remote != "unix://" and not re.fullmatch(r"unix:///[A-Za-z0-9_./-]{1,400}", remote):
            LOG.warning("manager recovery disabled: invalid app-server remote")
            return {"state": "unverified", "reason": "invalid canonical manager app-server binding"}

        pane_id = str(config["manager_pane_id"])
        pane = panes.get(pane_id)
        if pane is None:
            # Persisted manager IDs are an authority boundary.  A sole pane in
            # the same workspace is not proof that it owns this canonical
            # thread, especially during restore inventory churn.
            LOG.warning("manager recovery deferred: configured control pane is unavailable")
            return {"state": "unverified", "reason": "configured canonical pane is missing"}

        try:
            result = await self.herdr.request("pane.process_info", {"pane_id": pane_id}, timeout=10)
            process_info = result.get("process_info", result)
        except Exception as exc:
            LOG.warning("manager recovery deferred: cannot inspect %s: %s", pane_id, exc)
            return {"state": "unverified", "reason": "cannot inspect configured canonical pane"}
        foreground = process_info.get("foreground_processes") or []
        if any(process.get("name") == "codex" for process in foreground):
            try:
                result = await self.herdr.request("agent.get", {"target": pane_id}, timeout=5)
                agent = result.get("agent", result)
                if agent.get("agent_status") in {"idle", "done", "working"}:
                    if attachment_fence: attachment_fence()
                    verified = await self._publish_manager_session_if_canonical(
                        pane_id, config, process_info, agent
                    )
                    if verified:
                        if attachment_action_id:
                            self.db.advance_manager_attachment(attachment_action_id,
                                states={"launch_intent","launch_text_sent","launch_enter_sent","verified"},state="verified")
                        return {"state": "verified", "reason": "exact canonical pane has a verified native Codex session"}
            except Exception:
                pass
            return {"state": "unverified", "reason": "canonical pane has Codex foreground but no exact native session evidence"}
        shell_names = {"sh", "bash", "zsh", "fish"}
        if not foreground or any(process.get("name") not in shell_names for process in foreground):
            LOG.warning("manager recovery deferred: pane %s is not at an interactive shell", pane_id)
            return {"state": "unverified", "reason": "configured canonical pane is not at an interactive shell"}
        if not allow_new_launch:
            return {"state":"unverified","reason":"previous manager launch is unverified; refusing replay"}

        try:
            codex = Path(configured_codex_binary(config))
        except RuntimeError as exc:
            LOG.warning("manager recovery disabled: %s", exc)
            return {"state": "unverified", "reason": str(exc)}
        argv = [
            str(codex),
            "--disable",
            "hooks",
            "--remote",
            remote,
            "--profile",
            profile,
            "-C",
            manager_cwd,
            "resume",
            thread_id or "",
        ]
        command = shlex.join(argv)
        self.manager_launching = True
        command_sent = False
        try:
            await self.herdr.request("pane.send_text", {"pane_id": pane_id, "text": command}, timeout=10)
            command_sent = True
            if attachment_action_id:
                self.db.advance_manager_attachment(attachment_action_id,states={"launch_intent"},state="launch_text_sent")
            await asyncio.sleep(0.1)
            await self.herdr.request("pane.send_keys", {"pane_id": pane_id, "keys": ["enter"]}, timeout=10)
            if attachment_action_id:
                self.db.advance_manager_attachment(attachment_action_id,states={"launch_text_sent"},state="launch_enter_sent")
        except BaseException:
            # Cancellation can arrive after text reaches the pane but before
            # this coroutine sees the enter acknowledgement.  Treat that as
            # an uncertain admission: keep the launch gate closed and let the
            # bounded verifier settle it, rather than blindly sending resume
            # again on a later snapshot.
            if command_sent and (self.manager_reconnect_task is None or self.manager_reconnect_task.done()):
                self.manager_reconnect_task = asyncio.create_task(
                    self._finish_manager_reconnect(pane_id,attachment_action_id), name="manager-reconnect"
                )
            elif not command_sent:
                self.manager_launching = False
            raise
        LOG.info("resuming canonical Control Manager in %s", pane_id)
        if self.manager_reconnect_task is None or self.manager_reconnect_task.done():
            self.manager_reconnect_task = asyncio.create_task(
                self._finish_manager_reconnect(pane_id,attachment_action_id), name="manager-reconnect"
            )
        if not require_verified:
            return {"state": "admitted", "reason": "exact canonical thread reattachment admitted"}
        try:
            verified = await asyncio.wait_for(asyncio.shield(self.manager_reconnect_task), timeout=5)
        except asyncio.TimeoutError:
            return {"state": "unverified", "reason": "canonical native pane did not verify within 5 seconds"}
        return ({"state": "verified", "reason": "exact canonical thread reattached to native pane"}
                if verified else {"state": "unverified", "reason": "canonical native pane failed reattachment verification"})

    async def _finish_manager_reconnect(self, pane_id: str, attachment_action_id: str | None = None) -> bool:
        try:
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline and not self.stopping.is_set():
                try:
                    result = await self.herdr.request("agent.get", {"target": pane_id}, timeout=10)
                    agent = result.get("agent", result)
                    if agent.get("agent_status") in {"idle", "done", "working"}:
                        process_result = await self.herdr.request(
                            "pane.process_info", {"pane_id": pane_id}, timeout=10
                        )
                        if await self._publish_manager_session_if_canonical(
                            pane_id, self.config(), process_result.get("process_info", process_result), agent
                        ):
                            if attachment_action_id:
                                self.db.advance_manager_attachment(attachment_action_id,
                                    states={"launch_intent","launch_text_sent","launch_enter_sent","verified"},state="verified")
                            await self.herdr.request(
                                "agent.rename", {"target": pane_id, "name": "control-manager"}, timeout=10
                            )
                            return True
                    if agent.get("agent_status") == "blocked":
                        LOG.warning("Control Manager reattachment is blocked in %s", pane_id)
                        return False
                except Exception:
                    pass
                await asyncio.sleep(0.5)
            LOG.warning("Control Manager was not ready within the recovery timeout")
            return False
        finally:
            self.manager_launching = False

    async def _publish_manager_session_if_canonical(
        self, pane_id: str, config: dict[str, Any], process_info: dict[str, Any], agent: dict[str, Any]
    ) -> bool:
        """Repair absent Herdr session metadata only for our exact remote resume.

        A native CLI can be alive before Herdr records its session.  The broker
        may publish that missing metadata only after matching the configured
        pane, canonical thread, and complete foreground command.  Existing
        foreign identity is never renamed or overwritten.
        """
        thread_id = str(config.get("manager_thread_id") or "")
        if pane_id != str(config.get("manager_pane_id") or "") or not thread_id:
            return False
        status = str(agent.get("agent_status") or "")
        if status not in {"idle", "done", "working"}:
            return False
        session = agent.get("agent_session") or {}
        actual = str(session.get("value") or "")
        if actual:
            return actual == thread_id
        declared_agent = agent.get("agent")
        if declared_agent not in {None, "", "codex"}:
            return False
        try:
            codex = str(Path(configured_codex_binary(config)))
        except RuntimeError:
            return False
        remote = str(config.get("app_server_remote") or "")
        try:
            cwd = normalized_cwd(str(config.get("manager_cwd") or ""))
        except ValueError:
            return False
        profile = str(config.get("profile") or "control-manager")
        expected = [codex, "--disable", "hooks", "--remote", remote, "--profile", profile,
                    "-C", cwd, "resume", thread_id]
        foreground = process_info.get("foreground_processes") or []
        if len(foreground) != 1 or foreground[0].get("name") != "codex":
            return False
        argv = foreground[0].get("argv")
        if not isinstance(argv, list) or argv != expected:
            return False
        await self.herdr.request(
            "pane.report_agent_session",
            # Herdr's Codex integration channel is required for resumable
            # session identity; custom lifecycle-source names are ignored by
            # session_ref_from_report. Publish only after the proof above.
            {"pane_id": pane_id, "source": "herdr:codex", "agent": "codex",
             "agent_session_id": thread_id, "seq": time.time_ns()}, timeout=10,
        )
        result = await self.herdr.request("agent.get", {"target": pane_id}, timeout=10)
        published = result.get("agent", result).get("agent_session") or {}
        return str(published.get("value") or "") == thread_id

    async def _event_loop(self) -> None:
        delay = 1.0
        while not self.stopping.is_set():
            writer: asyncio.StreamWriter | None = None
            try:
                self.subscription_refresh.clear()
                pane_ids = await self._live_subscription_pane_ids()
                reader, writer = await self.herdr.subscribe(pane_ids)
                delay = 1.0
                await self.reconcile()
                while not self.stopping.is_set():
                    try:
                        raw = await asyncio.wait_for(reader.readline(), timeout=1)
                    except asyncio.TimeoutError:
                        if self.subscription_refresh.is_set():
                            break
                        continue
                    if not raw:
                        raise RuntimeError("Herdr event stream closed")
                    message = json.loads(raw)
                    if "event" in message:
                        await self._handle_event(message["event"], message.get("data") or {})
                if self.subscription_refresh.is_set():
                    continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                LOG.warning("Herdr event connection lost: %s; retrying in %.1fs", exc, delay)
                await asyncio.sleep(delay)
                delay = min(30.0, delay * 2)
            finally:
                if writer is not None:
                    writer.close()
                    with contextlib.suppress(Exception):
                        await writer.wait_closed()

    async def _live_subscription_pane_ids(self) -> list[str]:
        """Return only tracked panes which still exist in the live Herdr snapshot.

        A stale persisted pane must not make the whole event subscription fail.
        Missing tasks remain available to reconciliation and attention handling;
        this merely prevents them from poisoning the subscription request.
        """
        tracked = [
            row["pane_id"]
            for row in self.db.conn.execute(
                """SELECT pane_id FROM tasks WHERE pane_id IS NOT NULL
                   AND (state NOT IN ('completed','failed','cancelled') OR herdr_state='working')"""
            ).fetchall()
        ]
        result = await self.herdr.request("session.snapshot", {}, timeout=15)
        snapshot = result.get("snapshot", result)
        live = {pane["pane_id"] for pane in snapshot.get("panes", [])}
        return [pane_id for pane_id in tracked if pane_id in live]

    async def _handle_event(self, event: str, data: dict[str, Any]) -> None:
        pane = data.get("pane") if isinstance(data.get("pane"), dict) else None
        pane_id = data.get("pane_id") or (pane or {}).get("pane_id")
        if not pane_id:
            return
        if pane_id == self.config().get("manager_pane_id"):
            await self.reconcile()
            return
        row = self.db.task_for_pane(pane_id)
        if row is None:
            return
        if event in {"pane.agent_status_changed", "pane.updated", "pane.agent_detected"}:
            status = data.get("agent_status") or (pane or {}).get("agent_status") or "unknown"
            session = data.get("agent_session") or (pane or {}).get("agent_session") or {}
            if row["work_kind"] == "codex":
                provenance, detail = await self._worker_binding_provenance(row)
                if provenance != "owned":
                    await self._isolate_unowned_binding(
                        row, provenance, detail, boundary="lifecycle"
                    )
                    return
            values: dict[str, Any] = {"herdr_state": status}
            self.db.update(row["id"], **values)
            await self._apply_lifecycle(row["id"], status, binding_verified=True)
        elif event in {"pane.exited", "pane.closed"} and row["state"] not in TERMINAL_STATES:
            if row["work_kind"] == "command" and any(
                item["event"] == "command_native_admitted" for item in self.db.transitions(row["id"])
            ):
                await self._reconcile_native_commands()
                return
            updated = self.db.update(
                row["id"],
                state="failed",
                herdr_state="exited",
                summary="Worker pane exited before semantic completion.",
                finished_at=self.clock(),
                terminal_reported_at=self.clock(),
                cleanup_deadline=None,
                session_state="dormant",
            )
            asyncio.create_task(self.emit_attention(updated))
            self.scheduler_event.set()

    async def _apply_lifecycle(self, task_id: str, status: str, *, binding_verified: bool = False) -> None:
        row = self.db.task(task_id)
        if row is None:
            return
        if row["work_kind"] == "codex" and not binding_verified:
            provenance, detail = await self._worker_binding_provenance(row)
            if provenance != "owned":
                await self._isolate_unowned_binding(row, provenance, detail, boundary="lifecycle")
                return
        # A replacement tab is deliberately bound before a native CLI can be
        # attached, so an asynchronous pane event can find the correct task.
        # It is not lifecycle evidence for a new follow-up until admission and
        # prompt delivery complete; otherwise an old completed outcome could
        # schedule cleanup underneath the new attach attempt.
        if row["work_kind"] == "codex" and row["session_state"] == "admitting":
            return
        if row["herdr_state"] != status:
            row = self.db.update(task_id, herdr_state=status)
        if row["work_kind"] == "command":
            # Only the wrapper's structural exit plus shell verification can
            # settle a command. An embedded interactive TUI becoming idle is
            # not completion evidence.
            if row["state"] == "unknown" and row["terminal_reported_at"] is None:
                events = self.db.transitions(task_id)
                if any(event["event"] == "wrapper_started" for event in events):
                    self.db.update(
                        task_id,
                        event="command_idle_ignored",
                        state="working",
                        summary=("Command wrapper start is recorded; awaiting its structural exit report. "
                                 "Herdr agent state is not command completion evidence."),
                    )
            self.scheduler_event.set()
            return
        if row["state"] in TERMINAL_STATES:
            if status in {"idle", "done"}:
                self.scheduler_event.set()
                await self._verified_terminal(task_id)
            return
        if status == "working" and row["state"] in {"starting", "unknown"}:
            self.db.update(task_id, event="herdr_worker_working", state="working", summary="Worker is running.")
        elif status == "blocked" and row["state"] in {"starting", "working"}:
            updated = self.db.update(
                task_id,
                state="blocked",
                priority="attention" if row["priority"] in {"routine", "normal"} else row["priority"],
                summary="Herdr reports that the worker needs input or approval.",
            )
            asyncio.create_task(self.emit_attention(updated))
        elif status in {"idle", "done"} and row["state"] == "working":
            old = self.settle_timers.pop(task_id, None)
            if old:
                old.cancel()
            self.settle_timers[task_id] = asyncio.create_task(self._settle_without_report(task_id))
        self.scheduler_event.set()

    async def _settle_without_report(self, task_id: str) -> None:
        try:
            await asyncio.sleep(10)
            row = self.db.task(task_id)
            if row and row["state"] in {"starting", "working"} and row["herdr_state"] in {"idle", "done"}:
                updated = self.db.update(
                    task_id,
                    state="unknown",
                    summary="Worker settled without the required final control-report checkpoint.",
                )
                asyncio.create_task(self.emit_attention(updated))
                self.scheduler_event.set()
        finally:
            self.settle_timers.pop(task_id, None)

    @staticmethod
    async def _run_required(argv: list[str], timeout: float = 30) -> None:
        process = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
        if process.returncode:
            detail = (stderr or stdout).decode(errors="replace")[:1000]
            raise RuntimeError(f"command failed ({process.returncode}): {detail}")

    @staticmethod
    async def _run_json_required(
        argv: list[str], timeout: float = 30, env: dict[str, str] | None = None
    ) -> dict[str, Any]:
        process = await asyncio.create_subprocess_exec(
            *argv, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE, env=env
        )
        stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
        if process.returncode:
            detail = (stderr or stdout).decode(errors="replace")[:1000]
            raise RuntimeError(f"command failed ({process.returncode}): {detail}")
        try:
            value = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise RuntimeError("command returned invalid JSON") from exc
        if not isinstance(value, dict):
            raise RuntimeError("command returned a non-object JSON value")
        return value

    async def emit_attention(self, row: sqlite3.Row) -> None:
        if not self.db.should_notify(row["id"], row["state"], row["summary"], row["question"]):
            return
        title = f"Control Manager: {row['state']}"
        body = f"[{row['id']}] {row['summary']}"
        if row["question"]:
            body += f"\nQuestion: {row['question']}"
        await self._run_optional(["notify-send", "--app-name=Control Manager", "--urgency=critical" if row["priority"] == "critical" else "--urgency=normal", title, body])

        config = self.config()
        thread_id = config.get("manager_thread_id")
        remote = config.get("app_server_remote", "unix://")
        if thread_id:
            chat_message = (
                f"Broker attention event for task {row['id']}: state={row['state']}, "
                f"priority={row['priority']}. {row['summary']}"
            )
            if row["question"]:
                chat_message += f" Question: {row['question']}"
            chat_message += " Use control_broker.status with task_id for current details; relay the question or result concisely."
            queued = await self._run_optional(
                [configured_codex_binary(config), "queue", "--remote", remote, "--thread", thread_id, "--message", chat_message],
                timeout=30,
            )
            if queued:
                self.db.record_manager_queue(
                    row["id"], row["state"], row["summary"], row["question"]
                )

    @staticmethod
    async def _run_optional(argv: list[str], timeout: float = 10) -> bool:
        try:
            process = await asyncio.create_subprocess_exec(
                *argv, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE
            )
            _stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=timeout)
            if process.returncode:
                LOG.warning("optional command failed (%s): %s", process.returncode, stderr.decode(errors="replace")[:1000])
                return False
            return True
        except (FileNotFoundError, asyncio.TimeoutError) as exc:
            LOG.warning("optional command unavailable: %s", exc)
            return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--socket", type=Path, default=Path(os.environ.get("CONTROL_BROKER_SOCKET", str(state_root() / "control.sock"))))
    parser.add_argument(
        "--database", type=Path, default=Path(os.environ.get("CONTROL_BROKER_DATABASE", str(state_root() / "tasks.sqlite3")))
    )
    parser.add_argument("--herdr-socket", type=Path, default=Path(os.environ.get("CODEX_CONTROL_HERDR_SOCKET", str(Path.home() / ".config/herdr/herdr.sock"))))
    parser.add_argument("--config", type=Path, default=machine_config_path())
    parser.add_argument("--cleanup-delay", type=float, default=CLEANUP_DELAY_SECONDS, help=argparse.SUPPRESS)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


async def amain() -> None:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    if not 0 <= args.cleanup_delay <= 30:
        raise SystemExit("--cleanup-delay must be from 0 through 30 seconds")
    broker = Broker(
        args.socket, args.database, args.herdr_socket, args.config, cleanup_delay=args.cleanup_delay
    )
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, broker.stop)
    await broker.serve()


if __name__ == "__main__":
    asyncio.run(amain())
