# Explicit external-worker pairing

Read this reference only for a worker that is outside a Herdr-managed pane and whose launcher has an explicit pairing workflow.

Require the server to advertise `worker_context_handoff` at the version understood by the launcher. If the name is absent or too old, leave the worker unbound; do not infer support from the numeric protocol.

## Pair once from the intended pane

1. In the target pane, run `herdr context issue`.
2. Transfer the one-time JSON payload directly to the intended launcher over a private channel.
3. Have that launcher claim the payload once with a stable consumer ID by sending it on stdin to `herdr context claim --consumer-id <id>`.
4. Store the returned opaque renewable lease in OS or application secret storage.

Never put the payload or lease in argv, logs, shell history, source control, workspace files, telemetry, chat, or a global environment variable. Do not inspect its contents or treat it as a reusable bearer string for another consumer.

## Resolve every launch

Before every worker launch or restart, send the lease on stdin to:

```bash
herdr context resolve --endpoint <paired-endpoint> --consumer-id <id>
```

Use only the allowlisted environment returned by that resolution for the one child launch. Do not merge arbitrary parent environment entries, cache a socket path as authority, or export `HERDR_ENV` globally. Re-resolve after a worker crash, extension-host restart, Herdr reconnect, pane move, or session change.

The consumer ID, lease, endpoint, live server, and target ownership must all agree. A failed or expired resolution leaves the worker unbound. Fall back to ordinary non-Herdr behavior and ask for a new explicit pairing; never scan config directories or sockets and never guess from cwd.

## Revoke and handle owner loss

On explicit disconnect, send the lease on stdin to:

```bash
herdr context revoke --endpoint <paired-endpoint> --consumer-id <id>
```

Erase the stored lease after confirmed revocation or when the user removes the connection. Pane/session teardown, revocation, expiry, or an ownership mismatch invalidates control. Do not keep launching with stale resolved context, silently re-pair, or transfer the lease to a replacement consumer.
