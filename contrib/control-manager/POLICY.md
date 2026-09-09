# Control Manager package policy

This package is a portable runtime, not a portable live session. Keep package
files separate from machine-local sockets, credentials, state databases, task
history, terminal sessions, and project checkout bindings.

Use durable task stages and fail closed for review, CI, merge, release,
installation, and live-UAT evidence. A worker summary alone is not proof of a
stage. Do not recreate a consequential action after a restart merely because a
local process binding is missing or uncertain.

For authorized repository changes, choose Conventional Commit wording
autonomously. This does not weaken repository permissions, review, CI, release,
or safety requirements. Before destructive, security-sensitive,
privacy-sensitive, irreversible, physical, or outage-risk work, identify the
target, impact, rollback, and obtain the operator's confirmation.

User-facing completion reports must be concise and evidence-based. Summarize
the outcome, material limitation, and next action without exposing credentials,
session identifiers, or raw worker transcripts.
