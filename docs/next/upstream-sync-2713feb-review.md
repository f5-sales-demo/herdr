# Official Herdr sync through 2713feb

Fork base: `665f81f70de038e7b8e6cb88e84652bb17fe6c0b`. Official head: `2713feb56e9c9db33992db0a262c68f7426d46dd`. Common ancestor: `e7e3dfa60e359404def46503dd105c165d02a561`.

The official range has 192 commits across 786 paths. This ledger covers all 149 jointly modified paths: 75 direct conflicts and 74 semantic auto-merges. The integration preserves official ancestry with a non-squash merge and targets only `build-xcsh`.

Source version remains `0.19.4`; private client/server protocol is 28 and endpoint generation is 1. Private protocol-27 peers are rejected. Standard Kitty output replaces the four custom pane graphics methods. Native xcsh provenance, configured resume arguments, independent interactions and fork publication remain supported.

## Validation receipts

- Baseline `just check`: 3,750 Rust tests passed, plus maintenance, integration, documentation and Windows cross-lint checks.
- Resolved `just check`: 4,224 Rust tests passed, plus 179 maintenance tests, integration/documentation contracts and Windows cross-lint.
- Control Manager baseline and candidate: 268 tests completed successfully, with two explicitly gated real-binary cases skipped. The candidate archive passed checksum/provenance verification and relocatable installer/bootstrap attestation.
- Native xcsh resume precedence, generic configured arguments, closed-pane/group admission cleanup, replacement-process rejection and fallible queued waiters have focused coverage.
- The paired xcsh fix is tracked in f5-sales-demo/xcsh#5091. Protocol-28 ordinary negotiation failed before the fix and passed afterward; protocol 29 remains rejected. All 83 focused Herdr tests and TypeScript checks passed. The broader package run had 10,332 passing tests and one unrelated host sandbox failure (`setpriv` permission denial), preserved privately.

## Remaining delivery gates

Current-head hosted CI, benchmark regression remediation, native graphics/runtime UAT, release audit, immutable v0.20.0 publication, all 18 asset/provenance checks, and installed macOS/Linux/hosted-Windows acceptance remain to be recorded. No release or installed-artifact acceptance is implied by the source checks above.

Japanese and Chinese locale trees retain their fork baseline for the separate localization process. English release checks pass. Localization parity is available separately through `just translation-docs-check` and does not gate fork releases.

## Matched performance sample

The five-sample baseline/candidate run alternated order and measured 100 updates per sample. All rendered terminal bodies were byte-equivalent and peak memory was 31.42%. It did not pass the no-p95-regression gate.

| Metric | Baseline p95 | Candidate p95 | Change |
| --- | --- | --- | --- |
| Rendered marker | 18.008357 ms | 18.020122 ms | +0.065% |
| API interaction | 0.421817 ms | 0.453226 ms | +7.446% |

These measurements bind baseline `665f81f` and candidate `6f6338c4a`. The byte comparison covers the same two-row terminal body; UI chrome and intentionally optimized ANSI encoding are excluded. Raw captures and request timing samples remain private. Performance remains a release gate.

## Path dispositions

| Path | Merge classification | Disposition |
| --- | --- | --- |
| `.github/APPROVED_CONTRIBUTORS` | direct conflict | Preserve fork maintainer approval list. |
| `.github/workflows/ci.yml` | semantic auto merge | Official workspace/cache improvements; retain build-xcsh, Control Manager and ARC trust conditions. |
| `.github/workflows/nix.yml` | semantic auto merge | Official crates triggers and action pin; retain fork target branch. |
| `.github/workflows/preview.yml` | semantic auto merge | Official action pins; retain guards that disable upstream preview publication in the fork. |
| `.github/workflows/release.yml` | direct conflict | Preserve disabled upstream release workflow; fork publication remains downstream-release. |
| `AGENTS.md` | direct conflict | Preserve fork governance, autonomous commits, build-xcsh and release policy. |
| `CHANGELOG.md` | direct conflict | Retain fork history and stage English sync notes with source version still 0.19.4. |
| `Cargo.lock` | direct conflict | Official pinned dependency graph, fork version 0.19.4 and uuid dependency; regenerated from merged manifest. |
| `Cargo.toml` | direct conflict | Adopt official workspace ghostty-vt and dependencies while retaining 0.19.4 and fork package URLs. |
| `README.md` | semantic auto merge | Adopt official agent-support links; retain fork repository and installation contracts. |
| `distribution/latest.json` | direct conflict | Preserve immutable fork stable manifest; updated only by fork publication. |
| `distribution/preview.json` | direct conflict | Preserve disabled fork preview manifest. |
| `docs/next/CHANGELOG.md` | direct conflict | Finalize English sync notes and keep root/next changelogs identical. |
| `docs/next/README.md` | semantic auto merge | Adopt official English agent-support links; retain fork installation contracts. |
| `docs/next/api/herdr-api.schema.json` | semantic auto merge | Regenerate from resolved Rust schema at protocol 28; fork runtime APIs included and custom graphics APIs removed. |
| `docs/next/website/src/content/docs/agents.mdx` | direct conflict | Adopt official English guidance with fork xcsh and graphics-capability context; preserve disabled preview publication. |
| `docs/next/website/src/content/docs/cli-reference.mdx` | direct conflict | Adopt official English guidance with fork xcsh and graphics-capability context; preserve disabled preview publication. |
| `docs/next/website/src/content/docs/configuration.mdx` | semantic auto merge | Official prefix, clear/graphics, startup spacing and platform settings; native xcsh precedence and configured args documented. |
| `docs/next/website/src/content/docs/install.mdx` | direct conflict | Adopt official English guidance with fork xcsh and graphics-capability context; preserve disabled preview publication. |
| `docs/next/website/src/content/docs/ja/cli-reference.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/ja/configuration.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/ja/connecting-machines.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/ja/integrations.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/ja/keyboard.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/ja/persistence-remote.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/ja/quick-start.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/ja/session-state.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/ja/socket-api.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/ja/windows-beta.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/keyboard.mdx` | direct conflict | Adopt official English guidance with fork xcsh and graphics-capability context; preserve disabled preview publication. |
| `docs/next/website/src/content/docs/persistence-remote.mdx` | semantic auto merge | Official shutdown, remote preferences, discovery, Windows hosts and terminal mouse controls; fork URLs retained. |
| `docs/next/website/src/content/docs/session-state.mdx` | semantic auto merge | Official unavailable-pane retention, snapshot recovery, history fingerprint and generic resume guidance; fork native session semantics retained. |
| `docs/next/website/src/content/docs/socket-api.mdx` | semantic auto merge | Official typed contracts, generic resume and event loss recovery; custom graphics docs removed and native xcsh precedence clarified. |
| `docs/next/website/src/content/docs/windows-beta.mdx` | semantic auto merge | Official opt-in cross-elevation and Win32 paste behavior; fork Windows package identities retained. |
| `docs/next/website/src/content/docs/zh-cn/cli-reference.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/zh-cn/configuration.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/zh-cn/connecting-machines.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/zh-cn/integrations.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/zh-cn/keyboard.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/zh-cn/persistence-remote.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/zh-cn/quick-start.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/zh-cn/session-state.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/zh-cn/socket-api.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/content/docs/zh-cn/windows-beta.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/next/website/src/data/config-reference.json` | direct conflict | Regenerate 215 merged-model entries, preserving fork admission and resume-argument documentation. |
| `docs/preview/website/src/content/docs/agent-automation.mdx` | semantic auto merge | Preserve generated fork preview until generation from resolved next source. |
| `docs/preview/website/src/content/docs/agents.mdx` | direct conflict | Preserve generated fork preview until generation from resolved next source. |
| `docs/preview/website/src/content/docs/cli-reference.mdx` | semantic auto merge | Preserve generated fork preview until generation from resolved next source. |
| `docs/preview/website/src/content/docs/configuration.mdx` | direct conflict | Preserve generated fork preview until generation from resolved next source. |
| `docs/preview/website/src/content/docs/connecting-machines.mdx` | direct conflict | Preserve generated fork preview until generation from resolved next source. |
| `docs/preview/website/src/content/docs/integrations.mdx` | direct conflict | Preserve generated fork preview until generation from resolved next source. |
| `docs/preview/website/src/content/docs/ja/agent-automation.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/ja/agents.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/ja/cli-reference.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/ja/configuration.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/ja/connecting-machines.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/ja/integrations.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/ja/keyboard.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/ja/persistence-remote.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/ja/session-state.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/keyboard.mdx` | semantic auto merge | Preserve generated fork preview until generation from resolved next source. |
| `docs/preview/website/src/content/docs/persistence-remote.mdx` | direct conflict | Preserve generated fork preview until generation from resolved next source. |
| `docs/preview/website/src/content/docs/quick-start.mdx` | direct conflict | Preserve generated fork preview until generation from resolved next source. |
| `docs/preview/website/src/content/docs/session-state.mdx` | direct conflict | Preserve generated fork preview until generation from resolved next source. |
| `docs/preview/website/src/content/docs/socket-api.mdx` | semantic auto merge | Preserve generated fork preview until generation from resolved next source. |
| `docs/preview/website/src/content/docs/windows-beta.mdx` | semantic auto merge | Preserve generated fork preview until generation from resolved next source. |
| `docs/preview/website/src/content/docs/zh-cn/agent-automation.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/zh-cn/agents.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/zh-cn/cli-reference.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/zh-cn/configuration.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/zh-cn/connecting-machines.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/zh-cn/integrations.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/zh-cn/keyboard.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/zh-cn/persistence-remote.mdx` | semantic auto merge | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/content/docs/zh-cn/session-state.mdx` | direct conflict | Preserve fork locale bytes for the separate localization process; no manual locale edits. |
| `docs/preview/website/src/data/config-reference.json` | semantic auto merge | Preserve generated fork preview until generation from resolved next source. |
| `docs/versions/manifest.json` | direct conflict | Exclude official 0.9.2-0.9.5 snapshots and preserve fork immutable release namespace. |
| `justfile` | direct conflict | Union official Windows input tests and recipes with fork maintenance, agent skill and downstream release tests; upstream release test remains excluded. |
| `scripts/docs/versions.integration.test.ts` | semantic auto merge | Official Windows junction rejection; retain fork English-only README and manifest guards. |
| `scripts/package_windows_conpty.py` | semantic auto merge | Official additive build-local support; retain fork HTTPError.fp handling and pinned packaging. |
| `scripts/test_release.py` | direct conflict | Retain fork deletion of upstream-only release tests; fork publication tests remain authoritative. |
| `skills/herdr/SKILL.md` | direct conflict | Preserve current stable guidance; stage native Kitty/protocol-28 guidance under docs/next and promote it with the release version bump. |
| `src/agent_resume.rs` | semantic auto merge | Official reported-command validation with fork xcsh path/ID support and native --resume planning retained. |
| `src/api/mod.rs` | direct conflict | Use official operation metadata for presentation impact; fork operations unioned in schema. |
| `src/api/schema.rs` | direct conflict | Adopt official operation catalog and union fork execution/turn/interactions/recaps/worker-context/heartbeat; remove custom graphics methods. |
| `src/api/schema/agents.rs` | direct conflict | Use official CLI/API field projection with fork provider budget and queued/liveness fields plus completion sequence. |
| `src/api/schema/common.rs` | semantic auto merge | Official projected common CLI/API contracts; retain fork queued status and runtime types. |
| `src/api/schema/panes.rs` | direct conflict | Adopt official projected pane contracts/reported resume; preserve heartbeat; remove custom graphics parameter types. |
| `src/api/schema/response.rs` | semantic auto merge | Remove custom graphics responses; retain fork execution, turns, interactions, recaps and worker-context responses. |
| `src/api/schema/server.rs` | direct conflict | Union fork runtime capabilities and official SSH agent registration. |
| `src/api/schema/tests.rs` | direct conflict | Union runtime capabilities; preserve recap typing test and official CLI read-value coverage as independent tests. |
| `src/api/server.rs` | direct conflict | Preserve independent durable execution/interaction/turn dispatch while adopting official ServerStop, named fallible accept/connection threads, SSH agent registration and operation catalog. |
| `src/app/actions.rs` | semantic auto merge | Official completion guards, resume persistence and worktree validation; retain heartbeat/liveness and authority release. |
| `src/app/agent_resume.rs` | semantic auto merge | Official staggered startup and unavailable-directory handling; retain context capability verifier. |
| `src/app/agents.rs` | direct conflict | Retain fork reporter liveness/admission state plus official completion sequence. |
| `src/app/api.rs` | semantic auto merge | Official asynchronous worktree/shutdown handling; retain fork dispatch. Repair queued waiter ordering and retire stale process/session admission. |
| `src/app/api/agents.rs` | direct conflict | Validate before waiter creation and create fallible waiter before admission/input; retain provider queue and release failed admissions. |
| `src/app/api/layouts.rs` | semantic auto merge | Official final-size layout spawning; ordinary layout panes remain outside execution tracking. |
| `src/app/api/panes.rs` | direct conflict | Keep fork editing/paging key coverage and official pane-clear history test as independent tests. |
| `src/app/api/plugins/panes.rs` | direct conflict | Adopt captured host appearance while preserving optional managed owner argument. |
| `src/app/api/tabs.rs` | semantic auto merge | Official geometry/appearance; retain worker-context revocation and admission cleanup after close authorization. |
| `src/app/api/workspaces.rs` | semantic auto merge | Official group-close validation and parent identity; retain worker revocation and repair closed-pane admission cleanup. |
| `src/app/mod.rs` | direct conflict | Preserve shared runtime/admission/worker context with official fallible startup, multiple prefixes and handoff plugin retention; remove custom graphics runtime. |
| `src/app/popup.rs` | semantic auto merge | Official geometry/appearance; ordinary popup launch excludes execution tracking. |
| `src/app/runtime.rs` | semantic auto merge | Official fallible background threads and deadlines; retain reporter-liveness deadline. |
| `src/cli.rs` | direct conflict | Adopt official shared numeric/equals parsers and projections; queued status handled in typed agent bindings. |
| `src/cli/agent.rs` | direct conflict | Adopt official CLI projection/contracts while retaining fork agent recap access. |
| `src/cli/status.rs` | direct conflict | Expose fork capability versions alongside official SSH agent registration. |
| `src/cli/target.rs` | semantic auto merge | Official transport recovery; preserve machine forwarding for fork execution commands. |
| `src/client/shell.rs` | semantic auto merge | Official shell projection; preserve queued icon, priority, text and color. |
| `src/client/shell/endpoint_agent_state.rs` | semantic auto merge | Official boot/revision/connection completion scoping; preserve recap projection and noncompletion queued status. |
| `src/client/shell/tests/agents_worktrees_notifications.rs` | semantic auto merge | Official navigation/notification tests with fork recap rendering coverage. |
| `src/client/shell/tests/endpoints.rs` | semantic auto merge | Official endpoint coverage with fork recap fixture fields. |
| `src/client/shell/tests/popup_focus_projection.rs` | semantic auto merge | Official focus/scrollbar coverage with fork recap fixture fields. |
| `src/config/keybinds.rs` | semantic auto merge | Official multiple-prefix configuration with fork navigation aliases retained. |
| `src/config/model.rs` | direct conflict | Preserve validated resume-agent args and admission config while adopting restore startup spacing. |
| `src/detect/manifest.rs` | semantic auto merge | Official engine changes with bundled xcsh manifest registration retained. |
| `src/detect/mod.rs` | semantic auto merge | Official process identity/liveness with xcsh enumeration, executable lookup and lifecycle authority retained. |
| `src/events.rs` | direct conflict | Retain fork authoritative heartbeat event and add official reported-resume/shell-return events. |
| `src/main.rs` | direct conflict | Retain fork execution module and use official workspace ghostty-vt alias. |
| `src/pane.rs` | direct conflict | Adopt official job/process tracking and terminal identity clearing; preserve fork lifecycle reporter authority and child Kitty capability/owner-token tests. |
| `src/persist.rs` | direct conflict | Expose official restored worktree validation with fork restore wrappers retained. |
| `src/persist/restore.rs` | direct conflict | Preserve capability verifier while restoring handoff state; native xcsh provenance outranks reported commands; generic commands retain configured args and dedupe/history suppression. |
| `src/platform/fallback.rs` | semantic auto merge | Official unsupported-platform contracts with managed launch environment preserved. |
| `src/platform/linux.rs` | semantic auto merge | Official identity, power, clipboard and SSH work with fork environment forwarding preserved. |
| `src/platform/macos.rs` | semantic auto merge | Official input, launchd and shutdown work with fork environment forwarding preserved. |
| `src/platform/mod.rs` | semantic auto merge | Official shared platform contracts with fork environment validation, quoting and owner-token tests. |
| `src/platform/windows.rs` | semantic auto merge | Official process/input/security with fork PowerShell/cmd managed environment behavior retained. |
| `src/protocol/wire.rs` | semantic auto merge | Official input and bulk graphics codec with fork recap projection; protocol 28 rejects private clients 24-27; endpoint generation 1. |
| `src/pty/actor/unix.rs` | semantic auto merge | Official input preflight and shared descriptor ownership with queued-input ordering retained. |
| `src/remote/attach.rs` | semantic auto merge | Official recovery/discovery with fork manifest URL and disabled preview bootstrap preserved. |
| `src/server/client_shell.rs` | semantic auto merge | Official snapshot/completion projection with fork recap projection. |
| `src/server/client_transport.rs` | direct conflict | Adopt official bounded socket-buffer slow observer test with realistic progress timeout. |
| `src/server/handoff.rs` | direct conflict | Union worker-context live handoff and official host shutdown intent in manifests and callers. |
| `src/server/headless.rs` | direct conflict | Keep reporter liveness expiry independently of official restored-worktree retries and headless git refresh. |
| `src/server/headless/bootstrap.rs` | direct conflict | Bind JSON API and App to the same execution state while adopting official ServerStop and fallible startup/handoff. |
| `src/server/headless/lifecycle.rs` | direct conflict | Preserve worker context in handoff plus host shutdown intent and shared execution state when recreating sockets. |
| `src/server/headless/notifications.rs` | semantic auto merge | Official completion and async worktree focus forwarding with fork liveness forwarding retained. |
| `src/session.rs` | semantic auto merge | Official connection errors with fork native execution session arguments retained. |
| `src/terminal/runtime.rs` | semantic auto merge | Official terminal/graphics/input adapters with execution-manager forwarding and termination retained. |
| `src/terminal/state.rs` | direct conflict | Clear fork reporter liveness and official reported-resume state together. |
| `src/update.rs` | direct conflict | Preserve fork manifest/Homebrew URLs and disabled preview; adopt compatible official download/restart changes; exclude upstream preview selection. |
| `tests/api_ping.rs` | semantic auto merge | Official isolated subprocess shutdown/startup coverage; ping fixture explicitly expects protocol 28. |
| `tests/cli/mod.rs` | direct conflict | Run fork execution CLI suite and official shared contract tests. |
| `tests/cli/panes.rs` | semantic auto merge | Official shared CLI source validation errors; retained fork pane tests. |
| `tests/cli/surface.rs` | semantic auto merge | Official removed-command error contract; retained fork public CLI coverage. |
| `tests/client_mode.rs` | semantic auto merge | Official isolation, reconnect, palette, unavailable-cwd and remote recovery coverage; retained fork fixtures. |
| `tests/machine_api.rs` | direct conflict | Keep no-replay execution path recovery test independently of official frozen bridge/cold discovery coverage. |
| `tests/support/mod.rs` | semantic auto merge | Official SIGTERM-based owned cleanup and env isolation; fixture protocol 28 and endpoint generation 1. |
