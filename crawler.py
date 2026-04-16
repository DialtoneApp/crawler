#!/usr/bin/env python3

from __future__ import annotations

import argparse
import sqlite3
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen


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


def load_homepage_urls(db_path: str, limit: int | None) -> list[str]:
    query = "select homepage_url from repositories where homepage_url > ''"
    params: tuple[int, ...] | tuple[()] = ()

    if limit is not None:
        query = f"{query} limit ?"
        params = (limit,)

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(query, params).fetchall()
    finally:
        conn.close()

    return [row[0] for row in rows]


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme:
        return f"https://{url}"

    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"unsupported URL scheme: {parsed.scheme}")

    return url


def fetch_homepage(url: str, timeout: float) -> tuple[bool, str]:
    normalized_url = normalize_url(url)
    request = Request(
        normalized_url,
        headers={"User-Agent": USER_AGENT},
        method="GET",
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            # Read a byte so we actually consume part of the response body.
            response.read(1)
            return True, f"{response.status} {response.geturl()}"
    except HTTPError as exc:
        try:
            exc.read(1)
        except OSError:
            pass
        return False, f"{exc.code} {exc.geturl()}"
    except URLError as exc:
        return False, f"ERROR {normalized_url} - {exc.reason}"


def main() -> int:
    args = parse_args()

    if args.limit is not None and args.limit <= 0:
        print("--limit must be > 0", file=sys.stderr)
        return 2

    if args.timeout <= 0:
        print("--timeout must be > 0", file=sys.stderr)
        return 2

    urls = load_homepage_urls(args.db, args.limit)
    success_count = 0
    failure_count = 0

    for url in urls:
        try:
            ok, message = fetch_homepage(url, args.timeout)
        except ValueError as exc:
            ok = False
            message = f"ERROR {url} - {exc}"

        if ok:
            success_count += 1
            print(message, flush=True)
        else:
            failure_count += 1
            print(message, file=sys.stderr, flush=True)

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
