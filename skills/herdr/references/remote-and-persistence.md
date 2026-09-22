# Remote targeting and persistence

Read this reference for named sessions, saved SSH machines, cross-machine control, reconnects, updates, or restoration.

## Keep server identity with every target

A named session is an independent Herdr server namespace with its own sockets, panes, IDs, and runtime state:

```bash
herdr session list --json
herdr session attach work
```

Saved machine profiles target one server/session. Discover profile IDs with `herdr machine list --json`. For remote automation, prefix every discovery and control command with the same saved machine selector:

```bash
herdr --machine <label-or-id> agent list
herdr --machine <label-or-id> pane get <remote-pane-id>
```

The selector is an enabled profile ID or unique case-sensitive label, not an arbitrary hostname. Without `--machine`, inherited local session/socket routing remains in effect. Selecting a machine in the TUI does not retarget a CLI already running in a pane.

Workspace, tab, pane, and agent names are server-scoped. Two machines can both report `w1:p1` or `reviewer`; store the server/profile identity with every ID. Remote `--current` cannot mean a local caller pane. After reconnect, refresh live state before acting. After a pane move, use the returned new pane ID or a rediscovered live name.

Adding, disabling, removing, or updating a saved machine changes client configuration and requires user intent. Removing or disabling a profile disconnects the client but does not stop the remote server or its pane processes.

## Distinguish persistence paths

| Event | Processes | Layout | Conversation |
| --- | --- | --- | --- |
| Client detach and reattach | keep running | remains live | remains live |
| Server restart | stop | restored from snapshot | resumes only with valid native integration state |
| Compatible update without handoff | server may keep running | unchanged or restored if restart is required | follows process/native restore behavior |
| Explicit successful live handoff | best-effort process preservation | preserved | preserved with the live process |

Pane history replay restores terminal contents, not processes or proof of task completion. It can persist sensitive terminal output and is disabled by default. Native agent restoration and live handoff are separate mechanisms.

Reconnects can interrupt requests, waits, subscriptions, and messages even when long-lived state survives. Re-establish the connection, rediscover the target, and resume revision-based consumers from their last accepted revision. Do not blindly retry a command that may already have changed state.

## Require update consent

A version difference alone is not permission to replace a server. Background reconnects must not install, update, restart, approve host keys, or hand off. Missing named capabilities disable only their corresponding behavior.

Interactive setup defaults to No before replacing an incompatible server because replacement can end pane processes. Live handoff is experimental and opt-in through the documented `--handoff` path; never add it to ordinary machine setup, authentication repair, or retry logic.
