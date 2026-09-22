---
title: Agent interactions
---

Protocol 26 advertises `agent_interactions: 1`. Interactions are server-owned records independent of immutable semantic turn records: a question may remain pending after the originating turn completes.

Native xcsh launches use the version 4 launch contract. `herdr execution resume`
defaults to `--tools read`; pass `--tools read_interactions` when the native
session must ask the user a blocking or asynchronous question. Herdr derives
that policy as the exact xcsh allow-list
`read,request_user_input,request_user_input_async` while continuing to disable
MCP, LSP, memories, skills, rules, and PTY access. Other policy names are
rejected.

| Method | Purpose |
| --- | --- |
| `agent.interaction.report` | Report a pending question or plan decision, or its explicit resolution. |
| `agent.interaction.read` | Read an exact request target. |
| `agent.interaction.list` | Read public records and delivery receipts after a revision, or a reset snapshot when that cursor has expired. |
| `agent.interaction.wait` | Wait up to 30 seconds for revision changes; disconnect cancels waiting and expired cursors return a reset snapshot. |
| `agent.interaction.respond` | Queue an answer for the owning producer. |
| `agent.interaction.delivery.get` | Retrieve private queued answers for the authenticated producer. |
| `agent.interaction.delivery.ack` | Report whether the producer's completion owner accepted the answer. |

A target contains an `owner` (execution, pane, producer, session, generation) and `request_id`. Reports additionally carry thread, turn, item and question identities. Codex question and plan content is carried unchanged in `payload`; this API's revision and resolution metadata is separate.

`respond` returns a receipt with state `queued`. Async responses use `{ "questionId": "...", "answer": "..." }`, waiting responses use the Codex answer object, and plan responses are one of `implement`, `fresh`, or `stay`. Only a producer acknowledgement changes a receipt to `accepted`; a rejected answer leaves the question pending. A response ID is an idempotency key and cannot be moved to another target. Every producer report, delivery read, and acknowledgement requires the private capability injected into its execution, including ordinary non-native executions. Every mutation validates the active execution, provenance, and generation before changing interaction state.

Public records and the durable journal contain no answers or local drafts. Answers are held only in the private producer queue and erased immediately after either an accepted or rejected acknowledgement. Restart closes pending requests as `owner_lost` and marks unacknowledged deliveries lost. Accepted answers do not reopen. Execution loss is reconciled when records are read or awaited; authorization, provenance, persistence, and transient errors are returned without changing the request to `owner_lost`. Records and receipts are bounded. A list or wait response with `reset: true` is a complete retained snapshot and replaces consumer state because the requested cursor predates retained history.
