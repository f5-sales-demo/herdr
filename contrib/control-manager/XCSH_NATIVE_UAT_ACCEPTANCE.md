# XCSH native semantic-turn acceptance matrix

This is the durable acceptance record for feature `xcsh-native-lifecycle-20260908`. It separates evidence classes deliberately: a completed source child, a synthetic fixture, an installed artifact, and a live semantic turn are not interchangeable.

Package v18 was rejected by independent review and is not releasable: its
installed-driver wire identity mismatched broker admission and its scenario
control/consumption proof was incomplete. Source v19 corrects the identity
schema, applies journal transitions incrementally before continuation, checks
consumer capabilities before launching a prompt, strengthens the oracle, and
adds an offline real Broker/DB/outbox/inbox/ack chain. It still is not an
installed artifact or live-UAT receipt. Isolated-controller actions for
restart/loss and cleanup need a real installed controller and receipt before
those live cases can satisfy their gate; a manifest flag or prose is not proof.
Source package v20 adds `xcsh_isolated_uat_controller.py`: it verifies the
measured Herdr executable, creates its own random `xcsh-uat-*` systemd user
unit/session/workspace/pane, validates unit provenance before every action,
and persists idempotent receipts for reconnect/replay observation, a created
and closed owned cleanup tab, restart/reconnect, and broker-backed generation
supersession. It has been exercised only against the named disposable Herdr
v0.10.1 binary; it is not an installed XCSH/manager artifact or a live prompt
receipt.

| Requirement | Required authoritative evidence | Current evidence | Status |
| --- | --- | --- | --- |
| Linked issue and source delivery | Linked XCSH/Herdr issue/PR identities and merged commits | XCSH PR3769 merged at `ae39a166`; Herdr PR21 merged as `f1815b818cbfbd4b13343a823bfcdc05c3e11efe`; Herdr issue #5 closed | Complete source/merge evidence; not release evidence |
| Independent review | Review evidence covering XCSH reporter, Herdr journal, and manager consumer | Combined retained review evidence recorded | Complete gate evidence; does not prove installation |
| CI and post-merge workflows | Exact-head CI plus post-merge workflow evidence for both projects | XCSH exact merge CI was green; Herdr PR21 CI was green before merge | Open: post-merge workflow evidence remains required |
| Conventional version/release | Immutable release/version and artifact identity for XCSH, Herdr, and portable manager package | No release artifacts | Open |
| Exact artifact installation | Download receipt, checksum/version, installed XCSH/Herdr capability and installed manager package identity | No installed artifact evidence | Open |
| Protocol-20 consumer | Installed Herdr protocol >=20 with `agent_turn_journal`, consumer enabled, and persisted cursor/result-digest receipt | Manager boundary code plus synthetic fixture only | Blocked pending installed capability |
| Semantic success/failure/input/cancel/continuation/replay/supersession/cleanup/loss | Installed-artifact isolated UAT receipt showing the specified semantic transitions | No installed receipt in this source package | Open; source does not satisfy live UAT |
| Accepted | Every required lifecycle stage complete with matching gate evidence | Merge/release/install/native consumer/live UAT remain incomplete | Fail closed |

## Installed-runtime synthesized-prompt runner (required, not yet runnable)

`xcsh_installed_uat.py` and `xcsh-installed-uat/scenarios-v1.json` are the
separate real-runtime driver and safe synthesized prompt catalog. They bind
immutable XCSH, Herdr, and manager artifact versions/checksums; require a
dedicated disposable runtime; query real protocol-20 journal records; and
validate exact state traces, revision monotonicity, task/pane/session
provenance, completed-result digests, and manager-consumed evidence. They do
not infer success from a sentinel substring and never call `agent.turn.report`.

For XCSH, the published artifact is an archive, not the launched executable.
The manifest therefore retains the published archive version/URI/SHA-256 in
`artifacts.xcsh`, and separately binds a local verified archive path, the exact
regular member name `xcsh`, and that member's SHA-256. Probe validation hashes
the archive against the published asset, rejects traversal/link/wrong-member
archives, streams the sole regular member, and requires its measured digest to
match the launched executable. It never substitutes an arbitrary executable
digest for the published archive identity.

The manager source now provides `native_xcsh_admit`: it atomically creates an
admitted `work_kind=xcsh` task with durable idempotency/runtime identity, then
starts its matching Herdr execution. First semantic starting/working report
binds the XCSH session/turn only from the owned execution pane; completion is
still only a journal/result-digest fact. Calling Herdr `execution.start`
directly remains forbidden because it would lose broker correlation. The
currently installed manager must advertise this adapter after the matching
package artifact is installed; until then the driver fails preflight. Once it
does, run:

`python3 xcsh_installed_uat.py --manifest /approved/isolated/manifest.json --probe-installed`

followed by `--execute --run-id <stable-run-id>` only in the approved disposable
runtime. The execute path does not self-acknowledge manager consumption and
requires an authenticated `DisposableHerdrController` for reconnect/replay,
supersession, cleanup, and restart/loss. The controller refuses default,
control, shared, or unproven caller-labelled targets; neither condition can be
bypassed with a shared restart. This remains an incomplete
feature gate until an installed artifact run records the required evidence.

Before execution, prepare the controller with the measured installed Herdr
binary: `python3 xcsh_isolated_uat_controller.py --prepare --binary <herdr>
--sha256 <measured-sha256> --version <declared-Herdr-version>
--cwd <isolated-cwd> --state-db <local-actions-db>
--receipt <local-mode-0600-receipt>`. Copy only the returned workspace/socket
values and the two local receipt paths into the machine-local UAT manifest;
the receipt token stays in that 0600 file and is never exported in a package,
release manifest, or report.

No source-only fixture, local state, receipt, or historical validation result
is included in this distributable package. The required installed-artifact
validation remains a later operator action; it is not evidence supplied by
this source child.

## Required later release/install actions

The durable feature's later actions must bind all three deliverables, not XCSH alone:

- XCSH: immutable release artifact/version corresponding to merged PR3769 commit `ae39a166`.
- Herdr: merged PR21 commit `f1815b818cbfbd4b13343a823bfcdc05c3e11efe` (from exact reviewed head `5e9dea`), then immutable release artifact/version exposing protocol 20 and `agent_turn_journal`.
- Control Manager: the versioned portable package artifact containing the matching consumer and UAT catalog; its local install must be identified separately from machine-local sockets, credentials, runtime processes, and state.

Install must use an approved isolated target and record exact artifact identities/checksums, XCSH version, Herdr protocol/capability handshake, portable-package version, and consumer receipt. Only then may `native_consumer` be evidenced and the installed-artifact live-UAT action run. No source checkout, child completion, synthetic fixture, mocked process, or historical CI result is installation or semantic end-to-end success.
