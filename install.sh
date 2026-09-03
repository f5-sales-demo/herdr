#!/usr/bin/env bash
# Installer for future signed F5 releases. This is intentionally separate from
# website/install.sh and remains inactive until an operator publishes the first release.
set -euo pipefail

MANIFEST_URL=${HERDR_MANIFEST_URL:-https://github.com/f5-sales-demo/herdr/releases/latest/download/f5-latest.json}
INSTALL_DIR=${HERDR_INSTALL_DIR:-/usr/local/bin}
case "$(uname -s)-$(uname -m)" in
  Linux-x86_64) platform=linux-x86_64 ;;
  Linux-aarch64|Linux-arm64) platform=linux-aarch64 ;;
  Darwin-x86_64) platform=macos-x86_64 ;;
  Darwin-arm64) platform=macos-aarch64 ;;
  *) echo "unsupported platform: $(uname -s)-$(uname -m)" >&2; exit 2 ;;
esac

mkdir -p "$INSTALL_DIR"
tmp=$(mktemp -d "$INSTALL_DIR/.herdr-install.XXXXXX")
trap 'rm -rf "$tmp"' EXIT
curl --fail --silent --show-error --location "$MANIFEST_URL" --output "$tmp/f5-latest.json"
read -r asset_url expected_sha < <(python3 - "$tmp/f5-latest.json" "$platform" <<'PY'
import json, sys
entry = json.load(open(sys.argv[1], encoding="utf-8"))["platforms"][sys.argv[2]]["binary"]
print(entry["url"], entry["sha256"])
PY
)
curl --fail --silent --show-error --location "$asset_url" --output "$tmp/herdr.download"
actual_sha=$(sha256sum "$tmp/herdr.download" 2>/dev/null | awk '{print $1}' || shasum -a 256 "$tmp/herdr.download" | awk '{print $1}')
if [ "$actual_sha" != "$expected_sha" ]; then
  echo "checksum verification failed for $asset_url" >&2
  exit 1
fi
install -m 0755 "$tmp/herdr.download" "$tmp/herdr.staged"
mv -- "$tmp/herdr.staged" "$INSTALL_DIR/herdr"
echo "installed Herdr from $MANIFEST_URL to $INSTALL_DIR/herdr"
