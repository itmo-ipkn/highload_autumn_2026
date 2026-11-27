#!/bin/sh
set -eu

password_file=${CREDENTIALS_DIR:-/credentials}/postgres-password
if [ ! -f "$password_file" ] || [ ! -r "$password_file" ]; then
  echo "postgres: password secret is missing or unreadable" >&2
  exit 1
fi

POSTGRES_PASSWORD=$(cat "$password_file")
if [ -z "$POSTGRES_PASSWORD" ]; then
  echo "postgres: password secret is empty" >&2
  exit 1
fi
export POSTGRES_PASSWORD

exec /usr/local/bin/docker-entrypoint.sh "$@"
