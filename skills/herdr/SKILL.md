---
name: herdr
description: "Operate an existing Herdr terminal-multiplexer session through its CLI or an explicitly paired external worker. Use when the user asks to inspect or control Herdr workspaces, tabs, panes, commands, or coding agents. Do not invoke merely because background or parallel work might help."
---

# Herdr

Herdr organizes terminals into workspaces, tabs, and panes, recognizes coding agents in panes, and exposes the current server through the `herdr` CLI.

## Establish control authority

Control Herdr only in one of these contexts:

1. **Managed pane:** verify `test "${HERDR_ENV:-}" = 1`. The inherited binary, socket, session, and pane context target that pane's server.
2. **Explicitly paired external consumer:** use a live, consumer-bound lease created from the intended pane. Read [external-workers.md](references/external-workers.md) before using or implementing this path.

If neither applies, say this process is not paired with Herdr and do not issue control commands. An isolated worker lacking `HERDR_ENV` does not prove that the human has no Herdr session.

Never set `HERDR_ENV` globally, copy another process's Herdr environment, scan for sockets, or infer a target from cwd. Do not turn a teaching or setup question into live control; `distribution/agent-guide.md` is for helping a human learn Herdr.

## Discover the installed surface

Treat the installed client and connected server as authoritative. Start with:

```bash
herdr --help
herdr status server --json
```

Print only the relevant command group for further syntax:

```bash
herdr agent
herdr pane
herdr workspace
herdr tab
herdr worktree
herdr execution
herdr terminal
herdr notification
herdr integration
herdr session
herdr machine
herdr context
herdr api
```

Do not run bare `herdr` for discovery; it launches or attaches the TUI. Do not probe a nested command that may execute with defaults by omitting arguments. Parse IDs and state from JSON responses rather than predicting them.

Before advanced behavior, require its named server capability. API consumers read the `ping` response; CLI consumers use the relevant status or discovery response. Treat a missing name as unsupported even when a numeric protocol looks new enough. Use numeric compatibility only where the installed interface documents that fallback. Do not stop, replace, or update an older server merely because a capability is absent.

When a pane API advertises optional terminal features, use only those advertised values. For graphics, query `pane.graphics.info`; use its limits and transport acknowledgement rather than assuming Kitty graphics support from the outer terminal name.

## Target topology and identity explicitly

- Workspaces, tabs, and panes organize terminal locations.
- Pane commands control raw terminals, shells, tests, servers, input, and output.
- Agent commands control a recognized coding agent in a pane.

Public IDs are opaque, stable handles such as workspace `w1`, tab `w1:t1`, and pane `w1:p1`. Closed tab and pane IDs are not reused. IDs and agent names are scoped to one server; different machines may both have `w1:p1` or `reviewer`.

Herdr injects caller context into managed panes:

```bash
printf '%s\n' "$HERDR_WORKSPACE_ID" "$HERDR_TAB_ID" "$HERDR_PANE_ID"
```

Prefer `--current`, an explicit ID, or a unique live agent name. Omitting a target may select a pane focused by the user or another client. Discover state with:

```bash
herdr workspace list
herdr tab list --workspace "$HERDR_WORKSPACE_ID"
herdr pane current --current
herdr pane list --workspace "$HERDR_WORKSPACE_ID"
herdr agent list
```

Creation responses contain the next IDs: `workspace create` returns `.result.workspace`, `.result.tab`, and `.result.root_pane`; `tab create` returns `.result.tab` and `.result.root_pane`; `pane split` returns `.result.pane`.

After `pane move`, use `.result.move_result.pane.pane_id` or the live agent name. Do not reuse `.result.move_result.previous_pane_id` as a general target. For named sessions, saved SSH machines, cross-server identity, and persistence choices, read [remote-and-persistence.md](references/remote-and-persistence.md).

## Operate a pane without stealing focus

Default to a sibling pane in the current tab and `$PWD`. Create another workspace, tab, worktree, or cwd only when the user requests it. Honor a requested split direction; otherwise inspect the layout and split a wide pane right or a narrow/tall pane down:

```bash
herdr pane layout --pane "$HERDR_PANE_ID"
herdr pane split --current --direction right --cwd "$PWD" --no-focus
```

Read the new pane ID from `.result.pane.pane_id`. For an ordinary process:

```bash
herdr pane run <pane-id> "just test"
herdr pane wait-output <pane-id> --match "test result" --timeout 120000
herdr pane read <pane-id> --source recent-unwrapped --lines 120
```

`pane run` sends command text and Enter atomically. `wait-output` can match existing text and does not prove process completion. Use `visible` for the viewport, `recent` for rendered rows, `recent-unwrapped` for logs, and `detection` for the plain bottom-buffer agent snapshot. Use `--format ansi` only when styling is evidence.

Alternate-screen rows that have left the application cannot be recovered from host scrollback. If a larger read still misses a completed response, ask the agent to write the complete response to a temporary Markdown file and reply only with its path; use this fallback only after the read fails.

## Start and coordinate an agent

An available agent pane must be at its interactive shell prompt. Discover supported kinds from the installed `agent` command, then use the kind the user requested or selected. Codex, Claude Code, OpenCode, and xcsh are examples, not defaults:

```bash
herdr agent start reviewer --kind <discovered-kind> --pane <pane-id> -- <native-args...>
herdr agent prompt reviewer "Review the current diff and report actionable findings." --wait --timeout 120000
```

`agent start` does not create layout. It succeeds only after the expected agent is detected and ready; a blocked startup remains inspectable but is not ready for prompts.

`agent prompt` submits text and encoded Enter as one ordered operation. With `--wait`, it requires observed `working` or `blocked` activity and then waits for `idle`, `done`, or `blocked`. It tracks lifecycle state, not a unique semantic turn. A timeout or `agent_prompt_stalled` does not prove the prompt was undelivered, so inspect before retrying. A blocked target rejects prompts; read the visible question and obtain the user's decision before answering it.

```bash
herdr agent get reviewer
herdr agent read reviewer --source recent-unwrapped --lines 120
herdr agent wait reviewer --until blocked --timeout 120000
herdr agent send-keys reviewer esc
```

Use logical keys. Use pane input only when raw terminal control is intentional. Read [agents.md](references/agents.md) when detection authority, integration support, native restoration, blocked states, or retry behavior matters. Read [automation.md](references/automation.md) for durable executions, semantic completion, interactions, acknowledgements, revisions, or reconnect/replay.

## Preserve ownership and safety

- Use `--no-focus` for background work unless the user asked to switch context.
- Never rely on another client's focused pane.
- Do not close workspaces, tabs, panes, sessions, or saved machines you did not create unless the user explicitly asked.
- Do not add `workspace close --group` to bypass `workspace_group_close_required`.
- Use `--trust-repository` only after the user has verified that repository.
- Do not install integrations, add machines, update binaries, replace servers, or approve a restart without explicit user intent. A server replacement can terminate pane processes.
- Never run `herdr server stop` from an active session unless the user explicitly intends to stop that server and its panes. Never kill the main Herdr process; use named test sessions for experiments.
- Treat `unknown`, terminal text, a shell prompt, process exit, output drainage, and submission acknowledgement as distinct evidence; none alone proves semantic task success.
- CLI server errors are JSON on stderr with exit status 1. CLI syntax errors exit with status 2.

If this file was printed by `herdr --skill`, it is the self-contained core. Folder installations also include the linked references; when they are unavailable, stay within the core behavior and do not improvise an advanced protocol.
