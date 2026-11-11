#!/usr/bin/env bash
# Validate notarization credentials once and retain them in the ephemeral job keychain.
set -euo pipefail

if [ "$#" -ne 2 ] || [ -z "$1" ] || [ -z "$2" ]; then
  echo "usage: $0 KEYCHAIN PROFILE" >&2
  exit 2
fi
keychain=$1
profile=$2
umask 077

if [ -n "${APPLE_NOTARY_KEY_BASE64:-}${APPLE_NOTARY_KEY_ID:-}${APPLE_NOTARY_ISSUER:-}" ]; then
  if [ -z "${APPLE_NOTARY_KEY_BASE64:-}" ] || [ -z "${APPLE_NOTARY_KEY_ID:-}" ]; then
    echo "A complete notarization API key and key ID are required; refusing credential fallback" >&2
    exit 1
  fi
  private_key=$(mktemp "${TMPDIR:-/tmp}/herdr-notary-key.XXXXXX")
  trap 'rm -f "$private_key"' EXIT
  printf '%s' "$APPLE_NOTARY_KEY_BASE64" | base64 --decode >"$private_key"
  test -s "$private_key"
  auth=(--key "$private_key" --key-id "$APPLE_NOTARY_KEY_ID")
  if [ -n "${APPLE_NOTARY_ISSUER:-}" ]; then
    auth+=(--issuer "$APPLE_NOTARY_ISSUER")
  fi
else
  if [ -z "${APPLE_ID:-}" ] || [ -z "${APPLE_PASSWORD:-}" ] || [ -z "${APPLE_TEAM_ID:-}" ]; then
    echo "Complete notarization credentials are required" >&2
    exit 1
  fi
  auth=(--apple-id "$APPLE_ID" --password "$APPLE_PASSWORD" --team-id "$APPLE_TEAM_ID")
fi

# Validation is enabled by default. Do not echo credential arguments or disable validation.
xcrun notarytool store-credentials "$profile" --keychain "$keychain" "${auth[@]}"
