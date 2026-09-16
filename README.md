# herdr


<p align="center">
  <img src="assets/logo.png" alt="herdr" width="100" />
</p>

<p align="center">
  <a href="https://github.com/f5-sales-demo/herdr">maintained fork</a> · <a href="#install">install</a> · <a href="docs/next/website/src/content/docs/quick-start.mdx">quick start</a> · <a href="docs/next/website/src/content/docs/">docs</a>
</p>

<p align="center">
  English · <a href="README.zh-CN.md">简体中文</a>
</p>

<p align="center">
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-666666?labelColor=333333" alt="Apache 2.0 license" /></a>
  <a href="https://github.com/f5-sales-demo/herdr/releases"><img src="https://img.shields.io/github/downloads/f5-sales-demo/herdr/total?labelColor=333333&color=666666" alt="total GitHub release downloads" /></a>
  <a href="https://github.com/f5-sales-demo/herdr/releases/latest"><img src="https://img.shields.io/github/v/release/f5-sales-demo/herdr?label=release&labelColor=333333&color=666666" alt="latest stable release" /></a>
</p>

---

https://github.com/user-attachments/assets/043ec09f-4bdd-41d5-aee0-8fda6b83e267

**the runtime your coding agents live on.**

- **detach without stopping work** — herdr keeps terminals running in a background server when you close the client or lose your SSH connection. after a server or machine restart, herdr restores the saved layout and can resume supported agent sessions; the original processes do not survive. [session state →](docs/next/website/src/content/docs/session-state.mdx)
- **several machines, one window** — keep local work and saved ssh machines together, with a combined agent list and independent reconnects. [remote machines →](docs/next/website/src/content/docs/connecting-machines.mdx)
- **never hunt for the stuck one** — every pane is marked working, blocked, or idle. when an agent stops and needs an answer, herdr says so.
- **agent-native** — agents drive herdr through the cli and socket api: they can spawn panes, prompt each other, and wait until another agent is genuinely blocked. [agent skill →](docs/next/website/src/content/docs/agent-skill.mdx)
- **runs what you already run** — claude code, codex, cursor, opencode, grok and the rest. herdr doesn't wrap or replace them; it owns their terminals.
- **keyboard and mouse, both first-class** — tmux-style prefix keys *and* click, drag, split. pick per moment, not per tool.
- **plugins** — extend panes and workflows. [plugin documentation →](docs/next/website/src/content/docs/plugins.mdx)
- **one rust binary, no electron** — runs in whatever terminal you already use.

---

## install

```bash
curl -fsSL https://raw.githubusercontent.com/f5-sales-demo/herdr/build-xcsh/distribution/install.sh | sh
```

or `brew install f5-sales-demo/tap/herdr` · windows: `powershell -ExecutionPolicy Bypass -c "irm https://raw.githubusercontent.com/f5-sales-demo/herdr/build-xcsh/distribution/install.ps1 | iex"` · [endpoint-protected Windows](docs/next/website/src/content/docs/windows-beta.mdx) · [binaries](https://github.com/f5-sales-demo/herdr/releases)

then start it where the work lives:

```bash
herdr
```

run your agents, split panes, walk away. `ctrl+b q` detaches, `herdr` reattaches. [quick start →](docs/next/website/src/content/docs/quick-start.mdx)

## docs

The maintained documentation is staged under [`docs/next`](docs/next/website/src/content/docs/).

## thanks

every past sponsor and backer is listed in [SPONSORS.md](./SPONSORS.md) — thank you 🐑

## agent instructions

if you are an ai agent helping with this repository, read [`AGENTS.md`](./AGENTS.md) before making changes and read [`CONTRIBUTING.md`](./CONTRIBUTING.md) before opening issues or PRs.

## development

```bash
git clone --branch build-xcsh https://github.com/f5-sales-demo/herdr
cd herdr
cargo build --release

just test        # unit tests
just check       # formatting, tests, and maintenance checks
```

## license

Herdr is licensed under the [Apache License 2.0](LICENSE).
