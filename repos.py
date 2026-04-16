#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, timedelta
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


API_URL = "https://api.github.com/search/repositories"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="List the most-starred GitHub repositories created recently."
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
        help="Maximum number of repositories to return (1-100). Default: 30.",
    )
    return parser.parse_args()


def build_request(days: int, limit: int) -> Request:
    created_after = (date.today() - timedelta(days=days)).isoformat()
    query = urlencode(
        {
            "q": f"created:>{created_after}",
            "sort": "stars",
            "order": "desc",
            "per_page": str(limit),
        }
    )

    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "repos.py",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    token = os.getenv("GH_TOKEN") or os.getenv("GITHUB_TOKEN")
    if token:
        headers["Authorization"] = f"Bearer {token}"

    return Request(f"{API_URL}?{query}", headers=headers, method="GET")


def fetch_repositories(request: Request) -> dict[str, Any]:
    try:
        with urlopen(request) as response:
            return json.load(response)
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace")
        raise SystemExit(f"GitHub API request failed ({exc.code}): {details}") from exc
    except URLError as exc:
        raise SystemExit(f"GitHub API request failed: {exc.reason}") from exc


def main() -> int:
    args = parse_args()

    if args.days < 0:
        print("--days must be >= 0", file=sys.stderr)
        return 2

    if not 1 <= args.limit <= 100:
        print("--limit must be between 1 and 100", file=sys.stderr)
        return 2

    payload = fetch_repositories(build_request(args.days, args.limit))

    for item in payload.get("items", []):
        print(f'{item["full_name"]} ({item["stargazers_count"]}⭐) - {item["html_url"]}')

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
