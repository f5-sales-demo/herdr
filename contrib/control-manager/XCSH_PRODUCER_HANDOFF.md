# XCSH producer requirements for native lifecycle UAT

The packaged installed-UAT driver is intentionally blocked until a released
XCSH artifact provides these real execution boundaries. Declarations in a UAT
manifest are configuration, not evidence; the producer worker must implement
and release the behavior, with its own tests.

## Current supported CLI evidence and required producer dependency

Immutable XCSH v21.19.5 (PR3782) retains the documented supported probe
`xcsh --mode json --session-dir <fresh-absolute-dir>` with no prompt. It emits
a first `type=session` JSON header whose canonical `id` is a 16-character
lowercase hexadecimal `sessionManager` identity, now durably persists the
matching JSONL header, and includes generation replay. The controller runs the
measured executable directly, binds the header, exact working directory and
persisted JSONL provenance, and never trusts a manifest session id or
capabilities array.

There is no `--create-session-json` or generic capability command. The driver
therefore invokes only the documented JSON-mode/session-directory interface
and proves its observed behavior from the exact executable hash. Herdr PR35
accepts the corresponding canonical 16-hex SessionHeader grammar. This
removes the former prompt-free durable-session source dependency. The released
source does not yet expose the versioned failure/await/cancel/reply-loss and
process-cutpoint actions required below. The installed driver therefore fails
closed unless the producer adapter supplies causal receipts for those actions;
it does not turn a source smoke or a synthetic component fixture into
installed-UAT acceptance.

Herdr v0.13.0's immutable API schema was protocol 20/schema 1 (schema hash
`88e6f9f583f56e5d7708f6cfd6ec62250ea72ce85dfc61d7a4d04d7c43d42806`) and
defines both `ExecutionResumeParams` and `AgentTurnReportParams`, including
the canonical 16-hex XCSH session identity. The PR44 follow-on contract is
protocol 21: `execution.resume` requires an absolute `xcsh_executable`, and
each receipt carries `native_executable.canonical_path` and SHA-256. The
manager gates new native admissions on advertised protocol >=21 plus the
existing `tracked_executions` and `agent_turn_journal` capabilities; PR44 does
not define a separate capability bit. The controller measures the published
executable, the manager durably records that canonical path/hash for generation
zero and continuations, re-measures before every effect/replay, and validates
the returned binding, argv[0], environment, generation, workspace/tab/pane,
and producer session provenance. It never falls back to bare `xcsh`.

- A supported deterministic offline backend/test executor, selected by an
  explicit documented argv/configuration surface. Prompt wording must not be
  treated as a backend.
- A fixture action that reads one supplied local path and returns exactly its
  contents; an unhandled local-operation action that emits `turn_phase=error`.
- A real awaiting-user action/API which emits `turn_phase=awaiting_user`, and
  a continuation API that starts a new semantic turn with generation increased
  from zero to one.
- A cancellation binding proving Herdr `execution.cancel` reaches
  `turn_phase=cancelled` for the owned execution.
- A controllable reporter transport interruption/reconnect that replays its
  persisted producer event(s) into the Herdr journal, where duplicate replay
  is observable and deduplicated. Re-reading `agent.turn.list` is not replay.
- A documented restart contract. The producer supports `interrupted`, not `lost`;
  the current catalog therefore requires `interrupted`. Do not emit `lost`
  unless a released producer and Herdr contract support it.

The UAT controller may create the random local fixture using
`xcsh_installed_uat.py --prepare-fixture-dir <owned-dir>`. Copy its path,
value, and digest into the machine-local manifest. This creates no backend and
does not provide UAT acceptance evidence.
