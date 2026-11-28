#!/bin/sh
set -eu

credentials_dir=${CREDENTIALS_DIR:-/credentials}
grafana_data_dir=${GRAFANA_DATA_DIR:-/var/lib/grafana}
postgres_data_dir=${POSTGRES_DATA_DIR:-/var/lib/postgresql/data}
group_id=${CREDENTIALS_GROUP_ID:-10000}
grafana_user=${GRAFANA_ADMIN_USER:-admin}
grafana_password=${GRAFANA_ADMIN_PASSWORD:-}
postgres_password=${POSTGRES_PASSWORD:-}

case "$grafana_user" in
  ""|*[!A-Za-z0-9._@-]*)
    echo "secrets-init: GRAFANA_ADMIN_USER contains unsupported characters" >&2
    exit 1
    ;;
esac
case "$postgres_password" in
  *[!A-Za-z0-9._~-]*)
    echo "secrets-init: POSTGRES_PASSWORD must use URL-safe characters (letters, digits, dot, underscore, tilde, hyphen)" >&2
    exit 1
    ;;
esac

mkdir -p "$credentials_dir" "$grafana_data_dir"
chmod 0750 "$credentials_dir"
chown "0:$group_id" "$credentials_dir"
chown 472:0 "$grafana_data_dir"
chmod 0770 "$grafana_data_dir"

provided_value() {
  case "$1" in
    username) printf '%s' "$grafana_user" ;;
    password) printf '%s' "$grafana_password" ;;
    postgres-password) printf '%s' "$postgres_password" ;;
    *) return 1 ;;
  esac
}

existing=0
for name in username password postgres-password; do
  path=$credentials_dir/$name
  if [ -L "$path" ] || { [ -e "$path" ] && [ ! -f "$path" ]; }; then
    echo "secrets-init: $name is not a regular file" >&2
    exit 1
  fi
  if [ -f "$path" ]; then
    existing=$((existing + 1))
  fi
done

if [ "$existing" -ne 0 ] && [ "$existing" -ne 3 ]; then
  echo "secrets-init: credentials volume is incomplete; restore it or remove it together with application data" >&2
  exit 1
fi

if [ "$existing" -eq 0 ]; then
  if [ -f "$grafana_data_dir/grafana.db" ] && [ -z "$grafana_password" ]; then
    echo "secrets-init: Grafana data exists without saved credentials; restore its password or set GRAFANA_ADMIN_PASSWORD" >&2
    exit 1
  fi
  if [ -s "$postgres_data_dir/PG_VERSION" ] && [ -z "$postgres_password" ]; then
    echo "secrets-init: Postgres data exists without saved credentials; restore its password or set POSTGRES_PASSWORD" >&2
    exit 1
  fi
fi

if [ "$existing" -eq 3 ]; then
  for name in username password postgres-password; do
    path=$credentials_dir/$name
    saved=$(cat "$path")
    provided=$(provided_value "$name")
    if [ -z "$saved" ]; then
      echo "secrets-init: $name is empty" >&2
      exit 1
    fi
    if [ -n "$provided" ] && [ "$provided" != "$saved" ]; then
      echo "secrets-init: $name differs from the persisted value; reset credentials and data volumes together to change it" >&2
      exit 1
    fi
    chmod 0640 "$path"
    chown "0:$group_id" "$path"
  done
  echo "secrets-init: persisted credentials reused"
  exit 0
fi

generate_password() {
  od -An -N32 -tx1 /dev/urandom | tr -d ' \n'
}

write_secret() {
  name=$1
  value=$(provided_value "$name")
  if [ -z "$value" ]; then
    value=$(generate_password)
  fi
  if [ -z "$value" ]; then
    echo "secrets-init: failed to generate $name" >&2
    return 1
  fi

  path=$credentials_dir/$name
  temp=$(mktemp "$credentials_dir/.${name}.XXXXXX")
  printf '%s' "$value" > "$temp"
  chmod 0640 "$temp"
  chown "0:$group_id" "$temp"
  mv "$temp" "$path"
}

write_secret username
write_secret password
write_secret postgres-password
echo "secrets-init: credentials generated and persisted"
