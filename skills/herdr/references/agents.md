# Agents, integrations, and lifecycle

Read this reference when agent detection, integration authority, restoration, blocked states, or safe retries affect the task.

## Discover kind and authority

Discover supported kinds and installed integrations from the running installation:

```bash
herdr agent
herdr integration status
herdr agent get <target>
herdr agent explain <target> --json
```

Do not infer features from an agent brand. Codex, Claude Code, OpenCode, xcsh, and an unknown harness can expose different detection, lifecycle, session, and interaction capabilities.

Herdr first identifies the foreground process. A live lifecycle reporter or plugin can then become the pane's state authority. While it is authoritative, do not combine its state with screen-manifest guesses. Integrations that report only a native session reference enable restoration but leave lifecycle authority with screen detection. `agent explain` identifies the active authority, manifest evidence, and fallback reason.

Reporter authority must be live and bound to the current pane occupant. After a reporter releases, expires, disconnects, or the occupant changes, rediscover the agent. Never carry state from a prior occupant into a new prompt or wait.

## Interpret lifecycle conservatively

- `working`: the authority reports or detects activity.
- `blocked`: a known approval, question, or permission surface needs a decision.
- `idle`: the agent is ready for input and seen according to the server view.
- `done`: the same ready condition with unseen completion in the server view.
- `unknown`: an agent exists but its lifecycle cannot be classified confidently.

Individual TUI clients track viewed completions separately, so their Done badges can differ from CLI/server state. Reads do not mark work seen; explicit focus does. `unknown` is not completion. Screen detection uses the live bottom buffer, not a user's scrolled viewport, and an unfamiliar blocked UI can fall back to idle. Inspect before sending any consequential input.

## Prompt and wait safely

`agent prompt --wait` first submits the prompt, then requires fresh `working` or `blocked` activity when admission started from a non-working state. It returns `agent_prompt_stalled` when that admission signal does not appear. A caller timeout includes submission time.

Neither a stalled result nor a timeout proves that submission failed. Read `agent get` and `agent read`, confirm the live occupant and authority, and look for the requested work before retrying. If the agent was already working, a wait can settle on completion of the active turn rather than the newly submitted request; use durable semantic-turn tracking when that distinction matters.

If the target is already blocked, `agent prompt` returns `agent_blocked` without answering. Read the question, preserve its choices exactly, and ask the user when the decision is not already authorized. Send deliberate UI keys only after resolving that decision.

## Restore native sessions

Snapshot restoration recreates layout but not arbitrary processes. Herdr resumes an agent conversation only when a current integration supplied a valid native session reference and the installed kind supports a resume command. Check `integration status` instead of assuming that detection implies restoration.

Unsupported, missing, stale, duplicated, or invalid session references restore as shells. After restoration, rediscover the pane occupant and lifecycle authority before prompting. A live handoff preserves processes only when explicitly requested and successful; it is not the same as native session restoration.
