#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from app_api import (
    APP_API_BASE_URL_ENV_VAR,
    DEFAULT_APP_API_BASE_URL,
    allowed_app_api_base_urls_text,
    get_app_api_base_url,
    normalize_app_api_base_url,
)
from crawler import crawl_site


DEFAULT_INPUT_PATH = Path("~/Downloads/websites.json").expanduser()


def parse_args() -> argparse.Namespace:
    try:
        default_app_api_base_url = get_app_api_base_url()
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc

    parser = argparse.ArgumentParser(
        description=(
            "Read domains from a local websites.json file, crawl each site, "
            "and POST compact crawl summaries back to the app."
        )
    )
    parser.add_argument(
        "--api-base-url",
        type=normalize_app_api_base_url,
        default=default_app_api_base_url,
        help=(
            "Dialtone API base URL. "
            f"Allowed: {allowed_app_api_base_urls_text()}. "
            f"Default: value from ${APP_API_BASE_URL_ENV_VAR}, "
            f"otherwise {DEFAULT_APP_API_BASE_URL}."
        ),
    )
    parser.add_argument(
        "--input",
        type=lambda value: Path(value).expanduser(),
        default=DEFAULT_INPUT_PATH,
        help=f"Path to the websites JSON file. Default: {DEFAULT_INPUT_PATH}.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of domains to crawl from the input file.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Per-request timeout in seconds. Default: 10.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Maximum number of allowed pages to fetch per site. Default: 10.",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=1,
        help="Maximum crawl depth from the homepage. Default: 1.",
    )
    return parser.parse_args()


def rows_payload_from_json(payload: Any) -> list[Any]:
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in ("results", "items", "websites"):
            value = payload.get(key)
            if isinstance(value, list):
                return value

    raise RuntimeError("Input JSON must be a list of website objects.")


def load_domains(input_path: Path, limit: int | None) -> list[str]:
    try:
        raw = input_path.read_text(encoding="utf-8")
    except FileNotFoundError as exc:
        raise RuntimeError(f"Input file not found: {input_path}") from exc
    except OSError as exc:
        raise RuntimeError(f"Failed to read input file {input_path}: {exc}") from exc

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Input file contains invalid JSON: {input_path}") from exc

    rows = rows_payload_from_json(payload)
    domains: list[str] = []

    for row in rows:
        if not isinstance(row, dict):
            continue
        domain = row.get("domain")
        if isinstance(domain, str) and domain.strip():
            domains.append(domain.strip())

    if limit is not None:
        return domains[:limit]

    return domains


def main() -> int:
    args = parse_args()

    if args.limit is not None and args.limit <= 0:
        print("--limit must be > 0", file=sys.stderr)
        return 2

    if args.timeout <= 0:
        print("--timeout must be > 0", file=sys.stderr)
        return 2

    if args.max_pages <= 0:
        print("--max-pages must be > 0", file=sys.stderr)
        return 2

    if args.max_depth < 0:
        print("--max-depth must be >= 0", file=sys.stderr)
        return 2

    try:
        domains = load_domains(args.input, args.limit)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    success_count = 0
    failure_count = 0

    for domain in domains:
        try:
            ok, message = crawl_site(
                args.api_base_url,
                repository_full_name=None,
                domain=domain,
                search_rank=None,
                timeout=args.timeout,
                max_pages=args.max_pages,
                max_depth=args.max_depth,
            )
        except ValueError as exc:
            ok = False
            message = f"{domain} - {exc}"
        except Exception as exc:
            ok = False
            message = f"{domain} - crawl failed: {exc}"

        if ok:
            success_count += 1
            print(message, flush=True)
        else:
            failure_count += 1
            print(message, file=sys.stderr, flush=True)

    print(
        (
            f"Crawled {len(domains)} sites from {args.input} via {args.api_base_url} "
            f"({success_count} succeeded, {failure_count} failed)"
        ),
        file=sys.stderr,
    )
    return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
