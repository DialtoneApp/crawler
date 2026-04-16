#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from db import connect_db, migrate_db


API_BASE_URL = "https://api.github.com"
USER_AGENT = "repos.py"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Find the most-starred GitHub repositories created recently, "
            "fetch repository metadata, and store the results in SQLite."
        )
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Only include repositories created within the last N days. Default: 7.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=30,
        help="Maximum number of repositories to fetch. Default: 30.",
    )
    parser.add_argument(
        "--db",
        default="repos.db",
        help="SQLite database path. Default: repos.db.",
    )
    return parser.parse_args()


def github_headers() -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": USER_AGENT,
        "X-GitHub-Api-Version": "2022-11-28",
    }

    token = os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    return headers


def format_rate_limit_reset(value: str | None) -> str | None:
    if not value:
        return None

    try:
        reset_at = datetime.fromtimestamp(int(value), tz=timezone.utc)
    except ValueError:
        return None

    return reset_at.isoformat()


def github_get(path: str, params: dict[str, str] | None = None) -> dict[str, Any]:
    url = f"{API_BASE_URL}{path}"
    if params:
        url = f"{url}?{urlencode(params)}"

    request = Request(url, headers=github_headers(), method="GET")

    try:
        with urlopen(request) as response:
            return json.load(response)
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace").strip()
        remaining = exc.headers.get("X-RateLimit-Remaining")
        reset_at = format_rate_limit_reset(exc.headers.get("X-RateLimit-Reset"))

        rate_limit_bits = []
        if remaining is not None:
            rate_limit_bits.append(f"remaining={remaining}")
        if reset_at:
            rate_limit_bits.append(f"reset_at={reset_at}")

        rate_limit_suffix = ""
        if rate_limit_bits:
            rate_limit_suffix = f" ({', '.join(rate_limit_bits)})"

        message = details or exc.reason
        raise SystemExit(
            f"GitHub API request failed ({exc.code}){rate_limit_suffix}: {message}"
        ) from exc
    except URLError as exc:
        raise SystemExit(f"GitHub API request failed: {exc.reason}") from exc


def search_repositories(days: int, limit: int) -> list[dict[str, Any]]:
    created_after = (date.today() - timedelta(days=days)).isoformat()
    repositories: list[dict[str, Any]] = []
    page = 1

    while len(repositories) < limit:
        page_size = min(100, limit - len(repositories))
        payload = github_get(
            "/search/repositories",
            {
                "q": f"created:>{created_after}",
                "sort": "stars",
                "order": "desc",
                "per_page": str(page_size),
                "page": str(page),
            },
        )

        items = payload.get("items", [])
        if not items:
            break

        repositories.extend(items)
        if len(items) < page_size:
            break

        page += 1

    return repositories[:limit]


def fetch_repository(full_name: str) -> dict[str, Any]:
    return github_get(f"/repos/{full_name}")


def upsert_repository(
    conn: sqlite3.Connection,
    repo: dict[str, Any],
    search_rank: int,
    fetched_at: str,
) -> None:
    owner = repo.get("owner") or {}
    topics = repo.get("topics") or []

    with conn:
        conn.execute(
            """
            INSERT INTO repositories (
                full_name,
                owner_login,
                name,
                html_url,
                description,
                homepage_url,
                stars,
                forks,
                open_issues,
                watchers,
                language,
                default_branch,
                created_at,
                updated_at,
                pushed_at,
                archived,
                is_fork,
                search_rank,
                fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(full_name) DO UPDATE SET
                owner_login = excluded.owner_login,
                name = excluded.name,
                html_url = excluded.html_url,
                description = excluded.description,
                homepage_url = excluded.homepage_url,
                stars = excluded.stars,
                forks = excluded.forks,
                open_issues = excluded.open_issues,
                watchers = excluded.watchers,
                language = excluded.language,
                default_branch = excluded.default_branch,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                pushed_at = excluded.pushed_at,
                archived = excluded.archived,
                is_fork = excluded.is_fork,
                search_rank = excluded.search_rank,
                fetched_at = excluded.fetched_at
            """,
            (
                repo["full_name"],
                owner.get("login", ""),
                repo["name"],
                repo["html_url"],
                repo.get("description"),
                repo.get("homepage"),
                repo["stargazers_count"],
                repo["forks_count"],
                repo["open_issues_count"],
                repo["watchers_count"],
                repo.get("language"),
                repo.get("default_branch"),
                repo.get("created_at"),
                repo.get("updated_at"),
                repo.get("pushed_at"),
                int(bool(repo.get("archived"))),
                int(bool(repo.get("fork"))),
                search_rank,
                fetched_at,
            ),
        )
        conn.execute(
            "DELETE FROM repository_topics WHERE full_name = ?",
            (repo["full_name"],),
        )
        conn.executemany(
            """
            INSERT INTO repository_topics (full_name, topic)
            VALUES (?, ?)
            """,
            ((repo["full_name"], topic) for topic in topics),
        )


def main() -> int:
    args = parse_args()

    if args.days < 0:
        print("--days must be >= 0", file=sys.stderr)
        return 2

    if args.limit <= 0:
        print("--limit must be > 0", file=sys.stderr)
        return 2

    repositories = search_repositories(args.days, args.limit)
    fetched_at = datetime.now(timezone.utc).isoformat()

    conn = connect_db(args.db)
    try:
        migrate_db(conn)

        for index, repo_summary in enumerate(repositories, start=1):
            repo = fetch_repository(repo_summary["full_name"])
            upsert_repository(conn, repo, index, fetched_at)
            print(
                f'{repo["full_name"]} ({repo["stargazers_count"]}⭐) - {repo["html_url"]}',
                flush=True,
            )
    finally:
        conn.close()

    print(
        f"Saved {len(repositories)} repositories to {args.db}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
