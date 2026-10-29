#!/bin/sh
# Read credential files as data and export them before exec.
# The file contents must stay quoted data and must not be executed by the shell.
set -eu

dir=${GRAFANA_CREDENTIALS_DIR:-/credentials}
user_file=$dir/username
password_file=$dir/password

if [ ! -f "$user_file" ] || [ ! -f "$password_file" ]; then
  echo "grafana credentials: username or password file is missing" >&2
  exit 1
fi
if [ ! -r "$user_file" ] || [ ! -r "$password_file" ]; then
  echo "grafana credentials: username or password file is not readable" >&2
  exit 1
fi

username=$(cat "$user_file") || {
  echo "grafana credentials: failed to read username" >&2
  exit 1
}
password=$(cat "$password_file") || {
  echo "grafana credentials: failed to read password" >&2
  exit 1
}

if [ -z "$username" ] || [ -z "$password" ]; then
  echo "grafana credentials: username or password is empty" >&2
  exit 1
fi

case ${GRAFANA_CREDENTIALS_EXPORT:-} in
  grafana)
    export GF_SECURITY_ADMIN_USER="$username"
    export GF_SECURITY_ADMIN_PASSWORD="$password"
    ;;
  generator)
    export GRAFANA_ADMIN_USER="$username"
    export GRAFANA_ADMIN_PASSWORD="$password"
    ;;
  *)
    echo "grafana credentials: set GRAFANA_CREDENTIALS_EXPORT to grafana or generator" >&2
    exit 1
    ;;
esac

if [ "$#" -lt 1 ]; then
  echo "grafana credentials: command is required" >&2
  exit 1
fi

exec "$@"
