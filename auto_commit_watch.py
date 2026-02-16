#!/usr/bin/env python3
import os
import subprocess
import time
from datetime import datetime
from pathlib import Path
import fcntl


REPO_DIR = Path(__file__).resolve().parent
LOG_FILE = REPO_DIR / "logs" / "auto_commit.log"
LOCK_FILE = Path("/tmp/gravix_autocommit.lock")
POLL_SECONDS = 3600


def _log(msg: str):
    LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}\n"
    if not LOG_FILE.exists():
        LOG_FILE.write_text(line, encoding="utf-8")
        return
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line)


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, cwd=str(REPO_DIR), capture_output=True, text=True)


def _has_changes() -> bool:
    res = _run(["git", "status", "--porcelain"])
    return bool(res.stdout.strip())


def _commit_and_push():
    _run(["git", "add", "-A"])
    msg = f"Auto-commit {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    commit = _run(["git", "commit", "-m", msg])
    if commit.returncode != 0:
        return
    _run(["git", "push", "origin", "main"])


def main():
    with LOCK_FILE.open("w") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            return

        _log("auto-commit watcher started (hourly)")
        while True:
            try:
                if _has_changes():
                    _commit_and_push()
            except Exception as e:
                _log(f"error: {e}")
            time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
