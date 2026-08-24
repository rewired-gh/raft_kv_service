#!/usr/bin/env python3
"""Enforce the coverage policy against an llvm-cov summary export."""

from __future__ import annotations

import json
import pathlib
import sys


OVERALL_LINES = 80.0
OVERALL_BRANCHES = 70.0
CRITICAL_LINES = 90.0
CRITICAL_BRANCHES = 80.0
CRITICAL_FILES = {
    "src/raft/election.rs",
    "src/raft/log.rs",
    "src/raft/runtime.rs",
    "src/storage/sqlite.rs",
}


def percentage(covered: int, count: int) -> float:
    return 100.0 if count == 0 else covered * 100.0 / count


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: check-coverage.py REPORT.json", file=sys.stderr)
        return 64

    report = json.loads(pathlib.Path(sys.argv[1]).read_text(encoding="utf-8"))
    data = report["data"][0]
    totals = data["totals"]
    root = pathlib.Path.cwd().resolve()

    critical = {
        "lines": {"covered": 0, "count": 0},
        "branches": {"covered": 0, "count": 0},
    }
    found: set[str] = set()
    for item in data["files"]:
        path = pathlib.Path(item["filename"]).resolve()
        try:
            relative = path.relative_to(root).as_posix()
        except ValueError:
            continue
        if relative not in CRITICAL_FILES:
            continue
        found.add(relative)
        for metric in critical:
            critical[metric]["covered"] += item["summary"][metric]["covered"]
            critical[metric]["count"] += item["summary"][metric]["count"]

    missing = CRITICAL_FILES - found
    if missing:
        print(f"coverage report is missing critical files: {sorted(missing)}", file=sys.stderr)
        return 1

    measured = {
        "overall lines": totals["lines"]["percent"],
        "overall branches": totals["branches"]["percent"],
        "critical lines": percentage(**critical["lines"]),
        "critical branches": percentage(**critical["branches"]),
    }
    required = {
        "overall lines": OVERALL_LINES,
        "overall branches": OVERALL_BRANCHES,
        "critical lines": CRITICAL_LINES,
        "critical branches": CRITICAL_BRANCHES,
    }
    failed = False
    for label, actual in measured.items():
        threshold = required[label]
        print(f"{label}: {actual:.2f}% (required {threshold:.2f}%)")
        failed |= actual + 1e-9 < threshold
    return int(failed)


if __name__ == "__main__":
    raise SystemExit(main())
