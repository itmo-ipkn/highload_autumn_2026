"""Create a persistent Grafana password without putting it in logs or Git."""
from __future__ import annotations

import os
from pathlib import Path
import secrets
import tempfile


def ensure_password(path: Path, initial: str = "", owner: int | None = None) -> bool:
    path.parent.mkdir(parents=True, exist_ok=True)
    if "\n" in initial or "\r" in initial or initial != initial.strip():
        raise ValueError("The initial password must be a non-empty single line without surrounding whitespace")
    if path.exists():
        saved = path.read_text().strip()
        if not saved:
            raise ValueError("The saved password is empty; restore it instead of rotating an existing Grafana database")
        if initial and initial != saved:
            raise ValueError("The supplied initial password differs from the saved password; automatic rotation is disabled")
        return False
    password = initial or secrets.token_urlsafe(32)
    descriptor, filename = tempfile.mkstemp(prefix=".password-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w") as stream:
            stream.write(password + "\n")
            stream.flush()
            os.fsync(stream.fileno())
            if owner is not None:
                os.fchown(stream.fileno(), owner, owner)
            os.fchmod(stream.fileno(), 0o400)
        try:
            # Publish only complete contents; competing startup processes reuse
            # the winner's file instead of overwriting it with another password.
            os.link(filename, path)
        except FileExistsError:
            return ensure_password(path, initial, owner)
        return True
    finally:
        Path(filename).unlink(missing_ok=True)


def main() -> None:
    path = Path(os.getenv("GRAFANA_ADMIN_PASSWORD_FILE", "/secrets/grafana_admin_password"))
    created = ensure_password(path, os.getenv("GRAFANA_ADMIN_PASSWORD", ""), owner=472)
    print("Grafana password " + ("created" if created else "reused") + "; stored in the local credentials volume", flush=True)


if __name__ == "__main__":
    main()
