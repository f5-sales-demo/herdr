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
the canonical 16-hex XCSH session identity. The historical PR44 follow-on
contract was protocol 21: `execution.resume` required an absolute
`xcsh_executable`, and each receipt carried
`native_executable.canonical_path` and SHA-256. The current un-released
protocol-22 source contract superseded that request with the closed
`native_launch` v3 value: canonical executable/session directory/session file,
the SHA-256 of the first raw JSONL header line including its LF, a configured
nonsecret model selector, `reduced-v1` discovery, read-only tools, interactive
mode, and `managed_turn_v1`. The manager gates new native admissions on
advertised protocol >=23 plus existing `tracked_executions` and
`agent_turn_journal`, measures the executable and header before every
effect/replay, and validates the complete returned launch, binding, exact argv,
environment, generation, workspace/tab/pane, and producer session provenance.
It never falls back to bare `xcsh`.

Protocol 23 adds `workspace_id` to every `ExecutionRecord` receipt and makes
it part of idempotent native-generation equality. This preserves the manager's
immutable workspace/tab/pane comparison; it does not infer workspace identity
from public tab or pane identifiers. New native admissions require advertised
protocol >=23 plus the same capabilities.

Merged but unreleased backend PR49
`bc78d41b182a791eec1f2178d9933c1260a4ca8d` (reviewed source
`bedb87c7253dbde84b456ea451b854de9c2f4ec8`) exposes request-only native
capabilities and `agent.turn.action.get`/`agent.turn.action.ack` with
authenticated durable starting registration, safe-point-bound cancellation,
deadline reconciliation, and cooperative supersession. Producer PR3792 source
`60f0fe768ba4530d65e100e676cd0a0b59b0d876` is merged, but its release remains
pending. Do not use either source state as acceptance evidence or substitute a
PTY exit for producer cancellation. The manager does not receive, persist, or
invent the one-time `HERDR_NATIVE_CAPABILITY`; it verifies the normal
manager-visible cross-ledger consequence (the immutable tracked execution
receipt) and needs a released producer/runtime callback surface for the real
action, reply-loss, and restart oracles.

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
