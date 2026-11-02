#!/usr/bin/env sh
# Lightweight Gradle wrapper bootstrap. Downloads the pinned distribution on first run.
set -eu
GRADLE_VERSION="9.8.2"
GRADLE_USER_HOME="${GRADLE_USER_HOME:-$HOME/.gradle}"
DIST_DIR="$GRADLE_USER_HOME/wrapper/dists/gradle-$GRADLE_VERSION-bin/manual"
GRADLE_BIN="$DIST_DIR/gradle-$GRADLE_VERSION/bin/gradle"
EXPECTED_SHA256="21bd5da2de49b642904077f38c0efdc7778e92b3ed0889392dca61b97e389607"
if [ ! -x "$GRADLE_BIN" ]; then
  mkdir -p "$DIST_DIR"
  ZIP="$DIST_DIR/gradle-$GRADLE_VERSION-bin.zip"
  if [ ! -f "$ZIP" ]; then
    echo "Downloading Gradle $GRADLE_VERSION..."
    curl -fL "https://services.gradle.org/distributions/gradle-$GRADLE_VERSION-bin.zip" -o "$ZIP"
  fi
  echo "Verifying Gradle distribution checksum..."
  if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL_SHA256="$(sha256sum "$ZIP" | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    ACTUAL_SHA256="$(shasum -a 256 "$ZIP" | awk '{print $1}')"
  else
    echo "ERROR: sha256sum or shasum is required to verify Gradle." >&2
    exit 1
  fi
  if [ "$ACTUAL_SHA256" != "$EXPECTED_SHA256" ]; then
    echo "ERROR: Gradle distribution checksum mismatch." >&2
    rm -f "$ZIP"
    exit 1
  fi
  unzip -q -o "$ZIP" -d "$DIST_DIR"
fi
exec "$GRADLE_BIN" "$@"
