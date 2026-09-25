---
title: Agent recaps
---

Protocol 27 advertises `agent_recaps: 1` in the `ping` capabilities object. Agents may report a short recap of their current conversation without changing their lifecycle state. Herdr stores the recap in a bounded journal for its named server session, replays it after restart, and shows the latest recap for an agent's current session in `agent.get`, `herdr agent get`, `herdr agent recap get`, and the agent sidebar.

The producer calls `agent.recap.report` with:

```json
{
  "pane_id": "w1:p1",
  "source": "herdr:xcsh",
  "session_id": "/absolute/path/to/session.jsonl",
  "id": "recap-unique-id",
  "trigger": "manual",
  "summary": "Completed the implementation. Validation still needs a live run.",
  "next_action": "Run the live check.",
  "completed_turn_count": 3,
  "created_at": "2026-09-25T12:00:00Z"
}
```

`trigger` is `manual` or `automatic`. `summary` must contain 1–700 characters; `next_action` is optional and may contain 1–200. Use the same session reference value reported through the pane's agent session hook: xcsh reports its absolute session file path when available and otherwise its session ID. Herdr accepts the report only while that pane's active hook authority has the same `source` and session reference. A retry with the same ID and identical content returns `admitted: false`; changed content with that ID is rejected.

The response has `type: "agent_recap"`, `recap` with the submitted fields, and `admitted`. `agent.recap.get` takes `{"target":"<agent-name-or-pane-id>"}` and returns the latest recap for the current session with `admitted: false`. It returns `agent_recap_not_found` when that session has no recap. `agent.get` includes an optional `agent.latest_recap` field. A session switch hides the prior recap. Clients should check the advertised capability before sending reports so older servers continue to work.
