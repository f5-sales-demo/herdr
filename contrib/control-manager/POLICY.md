# Control Manager package policy

This package is a portable runtime, not a portable live session. Keep package
files separate from machine-local sockets, credentials, state databases, task
history, terminal sessions, and project checkout bindings.

Use durable task stages and fail closed for review, CI, merge, release,
installation, and live-UAT evidence. A worker summary alone is not proof of a
stage. Do not recreate a consequential action after a restart merely because a
local process binding is missing or uncertain.

`continue_task` corrects the current tracked workstream. While a Codex turn is
active, deliver it only through exact-turn steering with a durable caller
idempotency identity; never place it in a native future-turn queue. Preserve an
uncertain delivery for inspection and never replay it. Future independent work
must be a distinct broker task or feature stage. Legacy broker-owned queued
followups may be superseded only by an explicit request, only after their full
payloads are durably preserved, and never by an ordinary status read.

Automatic recovery must preserve the canonical Control Manager thread
identity. Capability refreshes resume and reload the exact thread in place;
candidate creation or promotion is a separately authorized manual operation.
Liveness checks use metadata-only reads and subscribed lifecycle events, not
repeated full-history hydration.

For authorized repository changes, choose Conventional Commit wording
autonomously. This does not weaken repository permissions, review, CI, release,
or safety requirements. Before destructive, security-sensitive,
privacy-sensitive, irreversible, physical, or outage-risk work, identify the
target, impact, rollback, and obtain the operator's confirmation.

User-facing completion reports must be concise and evidence-based. Summarize
the outcome, material limitation, and next action without exposing credentials,
session identifiers, or raw worker transcripts.
