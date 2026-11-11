#!/bin/sh
# Validate legacy signing containers without placing their passwords in argv.
set -eu

if [ "$#" -ne 2 ]; then
  echo "Usage: verify-pkcs12.sh FILE PASSWORD_ENVIRONMENT_VARIABLE" >&2
  exit 64
fi
pkcs12_path=$1
password_env=$2
case "$password_env" in
'' | *[!A-Z0-9_]* | [0-9]*)
  echo "Invalid password environment variable name" >&2
  exit 64
  ;;
esac
test -r "$pkcs12_path"

# OpenSSL 3+ requires its legacy provider for RC2 containers. Older OpenSSL
# and LibreSSL do not advertise this option and continue using their reader.
help_output=$(openssl pkcs12 -help 2>&1 || :)
set --
case "$help_output" in
*-legacy*) set -- -legacy ;;
esac
exec openssl pkcs12 "$@" -info -in "$pkcs12_path" -noout -passin "env:$password_env"
