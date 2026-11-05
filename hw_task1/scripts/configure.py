#!/usr/bin/env python3
"""Заменяет плейсхолдеры GENERATE_LOCALLY в .env на криптостойкие случайные значения."""

import os
import secrets
import stat
import sys

ENV_PATH = ".env"
PLACEHOLDER = "GENERATE_LOCALLY"


def main() -> int:
    if not os.path.exists(ENV_PATH):
        print(f"error: {ENV_PATH} not found, run: cp .env.example .env", file=sys.stderr)
        return 1

    with open(ENV_PATH, encoding="utf-8") as handle:
        lines = handle.readlines()

    changed = 0
    output: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, _, value = stripped.partition("=")
            if value.strip() == PLACEHOLDER:
                # 32 случайных байта -> 64 hex-символа
                line = f"{key}={secrets.token_hex(32)}\n"
                changed += 1
        output.append(line)

    with open(ENV_PATH, "w", encoding="utf-8") as handle:
        handle.writelines(output)

    os.chmod(ENV_PATH, stat.S_IRUSR | stat.S_IWUSR)  # 0600
    print(f"configured {changed} secret(s) in {ENV_PATH} (mode 0600)")
    return 0


if __name__ == "__main__":
    sys.exit(main())