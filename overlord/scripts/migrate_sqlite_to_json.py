#!/usr/bin/env python3
"""Standalone migration script: export job definitions from SQLite to JSON files.

This script reads jobs from a legacy Overlord SQLite database and writes them
to the JSON file-based job store.  It is self-contained and does not import
from the ``overlord`` package's (removed) ``database`` module.

Supported SQLite schema versions: up to v7.

Usage::

    python scripts/migrate_sqlite_to_json.py [--db PATH] [--data-dir PATH]

Options:
    --db        Path to the SQLite database file.
                Default: $XDG_DATA_HOME/overlord/overlord.db
    --data-dir  Path to the JSON data directory (where jobs/ lives).
                Default: $XDG_DATA_HOME/overlord
"""

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path

EXPECTED_SCHEMA_VERSION = 7

DEFAULT_DB_PATH = Path(
    os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")
) / "overlord" / "overlord.db"

DEFAULT_DATA_DIR = Path(
    os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share")
) / "overlord"


def _read_jobs_from_sqlite(db_path: Path) -> list[dict]:
    """Open the SQLite database and return all job rows as dicts."""
    if not db_path.exists():
        print(f"Error: SQLite database not found at {db_path}", file=sys.stderr)
        sys.exit(1)

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row

    # Verify schema version.
    try:
        row = conn.execute("SELECT MAX(version) FROM schema_version").fetchone()
        version = row[0] if row else None
    except sqlite3.OperationalError:
        print("Error: schema_version table not found — is this an Overlord database?",
              file=sys.stderr)
        conn.close()
        sys.exit(1)

    if version is None:
        print("Error: no schema version recorded in database.", file=sys.stderr)
        conn.close()
        sys.exit(1)

    if version > EXPECTED_SCHEMA_VERSION:
        print(f"Warning: database schema v{version} is newer than expected "
              f"v{EXPECTED_SCHEMA_VERSION}. Proceeding anyway.", file=sys.stderr)

    rows = conn.execute("SELECT * FROM jobs ORDER BY id").fetchall()
    jobs = []
    for r in rows:
        consumes_raw = r["consumes"]
        consumes = json.loads(consumes_raw) if consumes_raw else []
        jobs.append({
            "name": r["name"],
            "cron_expression": r["cron_expression"],
            "command": r["command"],
            "status": r["status"],
            "exclusive_lock": r["exclusive_lock"],
            "timeout_seconds": r["timeout_seconds"],
            "max_retries": r["max_retries"],
            "retry_delay_seconds": r["retry_delay_seconds"],
            "consumes": consumes,
            "queue_name": r["queue_name"],
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
        })
    conn.close()
    return jobs


def _write_job_json(jobs_dir: Path, job: dict) -> bool:
    """Write a single job to a JSON file. Returns True if written, False if skipped."""
    path = jobs_dir / f"{job['name']}.json"
    if path.exists():
        return False
    content = json.dumps(job, indent=2, default=str) + "\n"
    path.write_text(content, encoding="utf-8")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Migrate Overlord jobs from SQLite to JSON file store.",
    )
    parser.add_argument(
        "--db", metavar="PATH",
        help=f"Path to SQLite database file (default: {DEFAULT_DB_PATH})",
    )
    parser.add_argument(
        "--data-dir", metavar="PATH",
        help=f"Path to JSON data directory (default: {DEFAULT_DATA_DIR})",
    )
    args = parser.parse_args()

    db_path = Path(args.db) if args.db else DEFAULT_DB_PATH
    data_dir = Path(args.data_dir) if args.data_dir else DEFAULT_DATA_DIR
    jobs_dir = data_dir / "jobs"
    jobs_dir.mkdir(parents=True, exist_ok=True)

    print(f"Reading jobs from SQLite: {db_path}")
    jobs = _read_jobs_from_sqlite(db_path)

    if not jobs:
        print("No jobs found in SQLite database.")
        return

    print(f"Writing jobs to JSON store: {jobs_dir}")
    migrated = 0
    skipped = 0
    for job in jobs:
        if _write_job_json(jobs_dir, job):
            print(f"  migrated: {job['name']}")
            migrated += 1
        else:
            print(f"  skip: {job['name']} (already exists)")
            skipped += 1

    print(f"\nDone. Migrated {migrated} job(s), skipped {skipped}.")


if __name__ == "__main__":
    main()
