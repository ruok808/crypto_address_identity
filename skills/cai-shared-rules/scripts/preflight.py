#!/usr/bin/env python3
"""Read-only repository preflight for crypto_address_identity work."""

from __future__ import annotations

import argparse
import json
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


def _run(*args: str) -> str:
    completed = subprocess.run(
        args,
        cwd=ROOT,
        check=True,
        text=True,
        capture_output=True,
    )
    return completed.stdout.rstrip("\n")


def _changed_paths() -> list[str]:
    status = _run("git", "status", "--porcelain=v1", "--untracked-files=all")
    paths: list[str] = []
    for line in status.splitlines():
        if not line:
            continue
        path = line[3:]
        if " -> " in path:
            path = path.rsplit(" -> ", 1)[-1]
        paths.append(path)
    return sorted(paths)


def _is_owned(path: str, owned_paths: list[str]) -> bool:
    return any(path == owned or path.startswith(f"{owned}/") for owned in owned_paths)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--owned-path", action="append", default=[])
    parser.add_argument("--strict-owned", action="store_true")
    parser.add_argument("--check-staged", action="store_true")
    args = parser.parse_args()

    cwd = Path.cwd().resolve()
    git_root = Path(_run("git", "rev-parse", "--show-toplevel")).resolve()
    owned_paths = sorted({path.strip("/") for path in args.owned_path if path.strip("/")})
    invalid_paths = [path for path in owned_paths if path == ".." or path.startswith("../")]
    if cwd != ROOT or git_root != ROOT or invalid_paths:
        print(
            json.dumps(
                {
                    "status": "blocked",
                    "expected_root": str(ROOT),
                    "cwd": str(cwd),
                    "git_root": str(git_root),
                    "invalid_owned_paths": invalid_paths,
                },
                sort_keys=True,
            )
        )
        return 2

    changed_paths = _changed_paths()
    outside_owned = [path for path in changed_paths if not _is_owned(path, owned_paths)]
    staged_check = "not_requested"
    if args.check_staged:
        try:
            _run("git", "diff", "--cached", "--check")
            staged_check = "ok"
        except subprocess.CalledProcessError as error:
            staged_check = error.stderr.strip() or "failed"

    result = {
        "status": "ok" if staged_check == "ok" or staged_check == "not_requested" else "blocked",
        "workflow": args.workflow,
        "repository_root": str(ROOT),
        "owned_paths": owned_paths,
        "changed_paths": changed_paths,
        "outside_owned_paths": outside_owned,
        "staged_diff_check": staged_check,
    }
    print(json.dumps(result, sort_keys=True))
    if staged_check != "ok" and staged_check != "not_requested":
        return 2
    if args.strict_owned and outside_owned:
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
