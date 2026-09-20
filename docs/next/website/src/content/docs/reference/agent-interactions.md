---
title: Agent interactions
---

Protocol 25 advertises `agent_interactions: 1`. Interactions are server-owned records independent of immutable semantic turn records: a question may remain pending after the originating turn completes.

| Method | Purpose |
| --- | --- |
| `agent.interaction.report` | Report a pending question or plan decision, or its explicit resolution. |
| `agent.interaction.read` | Read an exact request target. |
| `agent.interaction.list` | Read public records and delivery receipts after a revision. |
| `agent.interaction.wait` | Wait up to 30 seconds for revision changes; disconnect cancels waiting. |
| `agent.interaction.respond` | Queue an answer for the owning producer. |
| `agent.interaction.delivery.get` | Retrieve private queued answers for the authenticated producer. |
| `agent.interaction.delivery.ack` | Report whether the producer's completion owner accepted the answer. |

A target contains an `owner` (execution, pane, producer, session, generation) and `request_id`. Reports additionally carry thread, turn, item and question identities. Codex question and plan content is carried unchanged in `payload`; this API's revision and resolution metadata is separate.

`respond` returns a receipt with state `queued`. Only a producer acknowledgement changes that receipt to `accepted`. A rejected answer leaves the question pending. Reusing a response ID with a different answer is rejected. Native producer operations require the registered child's capability. Every mutation validates the active execution and generation before changing interaction state.

Public records and the durable journal contain no answers or local drafts. Answers are held in the private producer queue. Restart closes pending requests as `owner_lost` and marks unacknowledged deliveries lost. Accepted answers do not reopen. Execution loss is reconciled when records are read or awaited. Journal errors remain visible through the API and do not grant an answer or infer a highlighted selection.
