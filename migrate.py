#!/usr/bin/env python3

from __future__ import annotations

import argparse

from db import applied_versions, connect_db, migrate_db


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Apply SQLite migrations.")
    parser.add_argument(
        "--db",
        default="repos.db",
        help="SQLite database path. Default: repos.db.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    conn = connect_db(args.db)
    try:
        migrate_db(conn)
        versions = sorted(applied_versions(conn))
    finally:
        conn.close()

    print(f"{args.db}: {', '.join(versions)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
