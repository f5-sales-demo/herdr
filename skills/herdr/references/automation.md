# Durable automation and native interactions

Read this reference before using tracked executions, native semantic turns, interaction delivery, revisions, or replay after reconnect.

## Negotiate named capabilities

An API client must read the server's `ping` capabilities before using an advanced contract. Require the relevant advertised name:

- `tracked_executions` for `execution.*` lifecycle records;
- `agent_turn_journal` for `agent.turn.*` semantic records;
- `agent_interactions` with the required version for native questions and delivery receipts;
- `worker_context_handoff` with the required version for external pairing;
- `xcsh_semantic_tracking` with the required version for the documented xcsh native contract.

Treat an absent name as unsupported. Older servers can still support core pane and agent operations. Do not infer an advanced contract from a numeric protocol, probe it by mutating state, stop the server, or update it without explicit consent.

## Separate execution evidence

Generate and persist an `execution_id` before admission. `execution.start` is idempotent only when the same ID is reused with an identical specification; a changed specification conflicts. Record the returned pane/tab IDs instead of deriving them.

Track these as separate facts:

1. admission and PTY spawn;
2. process terminal state (`exited`, `cancelled`, or `lost`);
3. output drainage (`output_complete` after EOF);
4. native semantic terminal state for the intended turn.

Process exit, a shell prompt, matching output, and drained bytes do not establish semantic success. A cancellation response proves a request, not termination; wait for the child watcher. After a server restart, an in-flight record can become `lost` with an evidence gap. Do not claim whether the child survived or completed.

Use `execution.get`, `execution.list`, and `execution.wait` to reconcile. Persist the greatest accepted revision, tolerate duplicate records, and let a later revision for the same ID replace earlier state. Retention is bounded; an expired ID must not be silently rerun.

## Require the intended semantic turn

When `agent_turn_journal` is present, accept a task result only from the terminal semantic event bound to the admitted execution, pane ownership, producer, native session, turn, and generation. Semantic states include `starting`, `working`, `waiting_input`, `completed`, `failed`, `cancelled`, `interrupted`, and `lost`.

Only `completed` carries a result. Producer revisions and server journal revisions are monotonic and serve different purposes. Persist accepted terminal events; the journal is replayable but not indefinite storage. Never synthesize semantic completion from pane lifecycle, terminal output, process exit, or metadata tokens.

## Deliver interactions privately

When the named interaction capability is present, treat questions as server-owned records independent of immutable turn records. Read public records with `agent.interaction.read`, `list`, or `wait`; after reconnect, resume from the greatest revision. A response with `reset: true` replaces the local retained snapshot.

`agent.interaction.respond` queues a response but does not prove delivery. Only the owning producer's acknowledgement can mark the receipt accepted. A rejection leaves the request pending. Keep the response ID stable for idempotent retry and never move it to another target.

Answers and local drafts are private. Do not print, log, journal, place in metadata, or expose them to public observers. Producer capabilities and queued answers belong only on the authenticated private delivery path and must never appear in skill output. Restart closes pending requests as `owner_lost` and loses unacknowledged deliveries; accepted answers do not reopen. Re-read the request and owner before any retry.
