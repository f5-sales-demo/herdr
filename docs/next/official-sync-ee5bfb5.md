# Official synchronization through ee5bfb5

This fork merges official `herdrdev/herdr` commit
`ee5bfb5041bb3e4a49d591c72308aae3c81ecdde` with two parents, preserving fork
history and the coordinated native xcsh runtime.

The three jointly modified paths were resolved as follows:

- `src/cli.rs` adopts the unified official agent guidance and keeps source and
  documentation links routed to `f5-sales-demo/herdr`.
- `tests/cli/surface.rs` verifies the unified guidance and fork-owned links.
- `docs/next/website/src/content/docs/agent-skill.mdx` removes the obsolete
  standalone-guide paragraph while retaining fork-specific material.

The English onboarding index links to the fork source. The four Japanese and
Chinese paths in the official delta retain their fork baseline content.
The stale-issue workflow retains the official repository guard; disable its
fork workflow after GitHub registers it, preserving the six existing disabled
official-only workflows.

Native execution now suppresses automatic xcsh titles so the first persisted
session header remains an immutable resume anchor. The manager accepts valid
admitted predecessor-generation progress during handoff without altering the
current generation's task projection. Post-terminal writes and unadmitted
future generations remain invalid. Protocol 28, endpoint generation 1 and
state schema 12 remain unchanged. Release automation owns the version bump.
