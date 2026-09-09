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
removes the former prompt-free durable-session source dependency; it does not
turn a source smoke or a synthetic component fixture into installed-UAT
acceptance.

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
