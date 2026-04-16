#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


GITHUB_API_BASE_URL = "https://api.github.com"
APP_API_BASE_URL = os.getenv("DIALTONE_API_BASE_URL", "http://localhost:5173")
APP_API_TIMEOUT = 30.0
USER_AGENT = "repos.py"
APP_USER_AGENT = "dialtoneapp repo sync v0.0.1"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Find the most-starred GitHub repositories created recently, "
            "fetch repository metadata, and POST the results to dialtoneapp."
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
        "--api-base-url",
        default=APP_API_BASE_URL,
        help=f"Dialtone API base URL. Default: {APP_API_BASE_URL}.",
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
    url = f"{GITHUB_API_BASE_URL}{path}"
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


def app_request(
    api_base_url: str,
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    params: dict[str, str | int] | None = None,
) -> Any:
    base_url = api_base_url.rstrip("/")
    url = f"{base_url}{path}"
    if params:
        encoded = urlencode({key: value for key, value in params.items() if value is not None})
        if encoded:
            url = f"{url}?{encoded}"

    headers = {
        "Accept": "application/json",
        "User-Agent": APP_USER_AGENT,
    }
    body: bytes | None = None

    if payload is not None:
        headers["Content-Type"] = "application/json"
        body = json.dumps(payload).encode("utf-8")

    request = Request(url, headers=headers, data=body, method=method)

    try:
        with urlopen(request, timeout=APP_API_TIMEOUT) as response:
            raw = response.read()
    except HTTPError as exc:
        details = exc.read().decode("utf-8", errors="replace").strip()
        message = details or exc.reason
        raise SystemExit(f"Dialtone API request failed ({exc.code}): {message}") from exc
    except URLError as exc:
        raise SystemExit(f"Dialtone API request failed: {exc.reason}") from exc

    if not raw:
        return None

    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise SystemExit(f"Dialtone API returned invalid JSON for {path}") from exc


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


def sync_repository(
    api_base_url: str,
    repo: dict[str, Any],
    search_rank: int,
    fetched_at: str,
) -> None:
    app_request(
        api_base_url,
        "/api/v1/crawler/repositories",
        method="POST",
        payload={
            "repo": repo,
            "search_rank": search_rank,
            "fetched_at": fetched_at,
        },
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

    for index, repo_summary in enumerate(repositories, start=1):
        repo = fetch_repository(repo_summary["full_name"])
        sync_repository(args.api_base_url, repo, index, fetched_at)
        print(
            f'{repo["full_name"]} ({repo["stargazers_count"]}⭐) - {repo["html_url"]}',
            flush=True,
        )

    print(
        f"Saved {len(repositories)} repositories to {args.api_base_url.rstrip('/')}",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
