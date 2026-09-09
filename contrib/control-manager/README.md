# Control Manager

Control Manager is a portable, versioned task-orchestration runtime for Herdr
and Codex. A release archive contains only reusable source, policy, fixtures,
and tests. It never contains credentials, machine configuration, SQLite state,
terminal/session history, screenshots, or personal operational documents.

## Verify and install a release

Download `control-manager-<version>.tar.gz`, its `.sha256` sidecar, and its
`.provenance.json` from the same GitHub Release. Verify the sidecar before
extracting:

```sh
sha256sum -c control-manager-<version>.tar.gz.sha256
tar -xzf control-manager-<version>.tar.gz
python3 control-manager-<version>/scripts/control_archive.py verify \
  --archive control-manager-<version>.tar.gz \
  --checksum control-manager-<version>.tar.gz.sha256 \
  --provenance control-manager-<version>.provenance.json \
  --expected-tag v<version> --expected-source-sha <release-commit-sha>
```

Install only into a new empty directory and create fresh local state:

```sh
python3 control-manager-<version>/runtime/control_portable.py install \
  --source "$PWD/control-manager-<version>" --target /opt/control-manager
CODEX_CONTROL_ROOT=/opt/control-manager \
CODEX_CONTROL_STATE_DIR=/var/lib/control-manager \
  python3 /opt/control-manager/runtime/control_portable.py bootstrap
```

The bootstrap config has mode `0600` and defaults to observation-only recovery.
Set machine bindings and credential-provider integration locally; do not copy a
live state directory from another machine. Start the broker with the generated
machine config, for example:

```sh
python3 /opt/control-manager/runtime/control_broker.py \
  --socket /var/lib/control-manager/control.sock \
  --database /var/lib/control-manager/tasks.sqlite3 \
  --config /var/lib/control-manager/machine.json
```

## Validation boundary

`scripts/control_archive.py test` runs the portable unit suite. The included
native journal fixtures validate protocol boundaries but cannot prove an
installed terminal, provider prompt, or visual TUI. Operators must separately
run the release-specific installed prompt/visual UAT against their authorized
environment and retain its evidence before accepting a durable recovery claim.
