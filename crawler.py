#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sqlite3
import sys
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from db import connect_db, migrate_db


USER_AGENT = "dialtoneapp.com crawler v0.0.1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch homepage URLs from the repositories table and make HTTP "
            "requests to each one."
        )
    )
    parser.add_argument(
        "--db",
        default="repos.db",
        help="SQLite database path. Default: repos.db.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of homepage URLs to fetch.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Per-request timeout in seconds. Default: 10.",
    )
    return parser.parse_args()


def load_homepage_urls(conn: sqlite3.Connection, limit: int | None) -> list[str]:
    query = "select homepage_url from repositories where homepage_url > ''"
    params: tuple[int, ...] | tuple[()] = ()

    if limit is not None:
        query = f"{query} limit ?"
        params = (limit,)

    rows = conn.execute(query, params).fetchall()
    return [row[0] for row in rows]


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme:
        return f"https://{url}"

    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"unsupported URL scheme: {parsed.scheme}")

    return url


def store_crawl(
    conn: sqlite3.Connection,
    homepage_url: str,
    http_code: int | None,
    response_bytes: int,
) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO crawls (homepage_url, http_code, response_bytes, crawled_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                homepage_url,
                http_code,
                response_bytes,
                datetime.now(timezone.utc).isoformat(),
            ),
        )


def fetch_homepage(url: str, timeout: float) -> tuple[bool, int | None, int, str]:
    normalized_url = normalize_url(url)
    request = Request(
        normalized_url,
        headers={"User-Agent": USER_AGENT},
        method="GET",
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read()
            response_bytes = len(body)
            return (
                True,
                response.status,
                response_bytes,
                f"{response.status} {response.geturl()} ({response_bytes} bytes)",
            )
    except HTTPError as exc:
        body = b""
        try:
            body = exc.read()
        except OSError:
            pass
        response_bytes = len(body)
        return (
            False,
            exc.code,
            response_bytes,
            f"{exc.code} {exc.geturl()} ({response_bytes} bytes)",
        )
    except URLError as exc:
        return False, None, 0, f"ERROR {normalized_url} - {exc.reason}"


def main() -> int:
    args = parse_args()

    if args.limit is not None and args.limit <= 0:
        print("--limit must be > 0", file=sys.stderr)
        return 2

    if args.timeout <= 0:
        print("--timeout must be > 0", file=sys.stderr)
        return 2

    conn = connect_db(args.db)
    try:
        migrate_db(conn)
        urls = load_homepage_urls(conn, args.limit)

        success_count = 0
        failure_count = 0

        for url in urls:
            try:
                ok, http_code, response_bytes, message = fetch_homepage(url, args.timeout)
            except ValueError as exc:
                ok = False
                http_code = None
                response_bytes = 0
                message = f"ERROR {url} - {exc}"

            store_crawl(conn, url, http_code, response_bytes)

            if ok:
                success_count += 1
                print(message, flush=True)
            else:
                failure_count += 1
                print(message, file=sys.stderr, flush=True)
    finally:
        conn.close()

    print(
        (
            f"Fetched {len(urls)} homepage URLs from {args.db} "
            f"({success_count} succeeded, {failure_count} failed)"
        ),
        file=sys.stderr,
    )
    return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
