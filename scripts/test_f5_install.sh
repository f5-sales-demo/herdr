#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
grep -q 'f5-latest.json' "$ROOT/install.sh"
grep -q 'sha256' "$ROOT/install.sh"
grep -q 'mv --' "$ROOT/install.sh"
! grep -q 'herdr.dev' "$ROOT/install.sh"

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
mkdir -p "$tmp/release" "$tmp/bin"
printf '#!/bin/sh\necho new\n' >"$tmp/release/herdr-linux-x86_64"
chmod +x "$tmp/release/herdr-linux-x86_64"
sum=$(sha256sum "$tmp/release/herdr-linux-x86_64" | awk '{print $1}')
printf 'old\n' >"$tmp/bin/herdr"

sed -e "s|FIXTURE_URL|file://$tmp/release/herdr-linux-x86_64|" -e "s|FIXTURE_SHA|$sum|" \
  "$ROOT/scripts/fixtures/install-manifest.json" >"$tmp/release/f5-latest.json"
HERDR_MANIFEST_URL="file://$tmp/release/f5-latest.json" HERDR_INSTALL_DIR="$tmp/bin" "$ROOT/install.sh"
test "$("$tmp/bin/herdr")" = new

printf 'old-again\n' >"$tmp/bin/herdr"
sed -e "s|FIXTURE_URL|file://$tmp/release/herdr-linux-x86_64|" -e 's|FIXTURE_SHA|ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff|' \
  "$ROOT/scripts/fixtures/install-manifest.json" >"$tmp/release/f5-latest.json"
if HERDR_MANIFEST_URL="file://$tmp/release/f5-latest.json" HERDR_INSTALL_DIR="$tmp/bin" "$ROOT/install.sh"; then
  echo 'installer accepted a bad checksum' >&2
  exit 1
fi
grep -qx 'old-again' "$tmp/bin/herdr"
echo 'F5 installer tests: OK'
