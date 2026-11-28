#!/bin/sh
set -eu

password_file=${CREDENTIALS_DIR:-/credentials}/postgres-password
if [ ! -f "$password_file" ] || [ ! -r "$password_file" ]; then
  echo "consumer: Postgres password secret is missing or unreadable" >&2
  exit 1
fi

password=$(cat "$password_file")
if [ -z "$password" ]; then
  echo "consumer: Postgres password secret is empty" >&2
  exit 1
fi

POSTGRES_URL="postgres://${POSTGRES_USER:-events}:${password}@${POSTGRES_HOST:-postgres}:${POSTGRES_PORT:-5432}/${POSTGRES_DB:-events}?sslmode=disable"
export POSTGRES_URL

exec "$@"
