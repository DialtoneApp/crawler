#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import sys
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen
import http.client
import ssl


USER_AGENT = "dialtoneapp.com crawler v0.0.1"

WELL_KNOWN_PATHS = (
    "/.well-known/x402.json",
    "/.well-known/agent.json",
    "/.well-known/agent-card.json",
    "/.well-known/commerce",
    "/.well-known/ucp",
)

ALLOWED_CONTENT_TYPES = {
    "application/ecmascript",
    "application/javascript",
    "application/json",
    "application/x-javascript",
    "application/x-json",
    "text/ecmascript",
    "text/javascript",
    "text/json",
    "text/plain",
}


@dataclass(frozen=True)
class CrawlResult:
    domain: str
    path: str | None
    content_type: str | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe top domains for supported .well-known files over HTTPS."
    )
    parser.add_argument("--csv", default="./top-1m.csv", help="CSV file containing domains.")
    parser.add_argument(
        "--results-dir",
        default="./results",
        help="Directory where <domain>.txt result files are written.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=10,
        help="Maximum number of domains to probe concurrently.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of domains to process.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=1000,
        help="Print progress every N completed domains. Use 0 to disable.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start at the beginning instead of resuming after the newest result file.",
    )
    return parser.parse_args()


def normalize_domain(value: str) -> str | None:
    domain = value.strip().lower().strip(".")
    if not domain or domain in {"domain", "root_domain", "host"}:
        return None

    if "://" in domain:
        parsed = urlsplit(domain)
        domain = parsed.netloc or parsed.path
    else:
        domain = domain.split("/", 1)[0]

    domain = domain.split("@")[-1].split(":", 1)[0].strip().strip(".")
    if not domain:
        return None

    try:
        return domain.encode("idna").decode("ascii")
    except UnicodeError:
        return None


def iter_domains(csv_path: Path) -> Iterator[str]:
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as csv_file:
        reader = csv.reader(csv_file)
        for row in reader:
            if not row:
                continue

            candidate = row[1] if len(row) > 1 and row[0].strip().isdigit() else row[0]
            domain = normalize_domain(candidate)
            if domain:
                yield domain


def latest_result_domain(results_dir: Path) -> str | None:
    newest_file: Path | None = None
    newest_mtime_ns = -1

    for path in results_dir.glob("*.txt"):
        if not path.is_file():
            continue

        try:
            mtime_ns = path.stat().st_mtime_ns
        except OSError:
            continue

        if mtime_ns > newest_mtime_ns:
            newest_file = path
            newest_mtime_ns = mtime_ns

    if newest_file is None:
        return None

    return normalize_domain(newest_file.stem)


def iter_domains_after(csv_path: Path, resume_after: str) -> Iterator[str]:
    domains = iter_domains(csv_path)

    for skipped, domain in enumerate(domains, start=1):
        if domain == resume_after:
            print(
                f"resuming after {resume_after} from {csv_path} (skipped {skipped})",
                file=sys.stderr,
            )
            break
    else:
        print(
            f"resume marker {resume_after} was not found in {csv_path}; starting at beginning",
            file=sys.stderr,
        )
        yield from iter_domains(csv_path)
        return

    yield from domains


def is_allowed_content_type(content_type: str | None) -> bool:
    if not content_type:
        return False

    media_type = content_type.split(";", 1)[0].strip().lower()
    if "html" in media_type:
        return False

    return (
        media_type in ALLOWED_CONTENT_TYPES
        or media_type.endswith("+json")
        or media_type.endswith("+javascript")
    )


def get_content_type(url: str, timeout: float) -> str | None:
    headers = {
        "Accept": "application/json, text/plain, application/javascript, text/javascript, */*;q=0.1",
        "Connection": "close",
        "User-Agent": USER_AGENT,
    }

    for method in ("HEAD", "GET"):
        request = Request(url, headers=headers, method=method)
        try:
            with urlopen(request, timeout=timeout) as response:
                if response.status != 200:
                    return None

                content_type = response.headers.get("Content-Type")
                return content_type if is_allowed_content_type(content_type) else None
        except HTTPError as error:
            if method == "HEAD" and error.code in {405, 501}:
                continue
            return None
        except (
            TimeoutError,
            URLError,
            http.client.HTTPException,
            OSError,
            ssl.SSLError,
            UnicodeError,
        ):
            return None

    return None


def result_path(results_dir: Path, domain: str) -> Path:
    safe_domain = domain.replace("/", "_")
    return results_dir / f"{safe_domain}.txt"


def probe_domain(domain: str, results_dir: Path, timeout: float) -> CrawlResult:
    for path in WELL_KNOWN_PATHS:
        content_type = get_content_type(f"https://{domain}{path}", timeout)
        if content_type:
            result_path(results_dir, domain).write_text(f"{path}\n", encoding="utf-8")
            return CrawlResult(domain=domain, path=path, content_type=content_type)

    return CrawlResult(domain=domain, path=None)


def submit_next(
    executor: ThreadPoolExecutor,
    futures: dict[Future[CrawlResult], str],
    domains: Iterator[str],
    results_dir: Path,
    timeout: float,
) -> bool:
    try:
        domain = next(domains)
    except StopIteration:
        return False

    future = executor.submit(probe_domain, domain, results_dir, timeout)
    futures[future] = domain
    return True


def crawl(args: argparse.Namespace) -> int:
    csv_path = Path(args.csv)
    results_dir = Path(args.results_dir)

    if args.concurrency < 1:
        print("--concurrency must be at least 1", file=sys.stderr)
        return 2
    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}", file=sys.stderr)
        return 2

    results_dir.mkdir(parents=True, exist_ok=True)

    resume_after = None if args.no_resume else latest_result_domain(results_dir)
    domains = iter_domains_after(csv_path, resume_after) if resume_after else iter_domains(csv_path)
    if args.limit is not None:
        domains = iter_limited(domains, args.limit)

    completed = 0
    found = 0
    started_at = time.monotonic()
    futures: dict[Future[CrawlResult], str] = {}

    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        for _ in range(args.concurrency):
            if not submit_next(executor, futures, domains, results_dir, args.timeout):
                break

        while futures:
            done, _ = wait(futures, return_when=FIRST_COMPLETED)
            for future in done:
                domain = futures.pop(future)
                completed += 1

                try:
                    result = future.result()
                except Exception as error:  # Keep the crawl moving on unexpected edge cases.
                    print(f"ERROR {domain}: {error}", file=sys.stderr)
                    result = CrawlResult(domain=domain, path=None)

                if result.path:
                    found += 1
                    print(f"FOUND {result.domain} {result.path} {result.content_type}")

                if args.progress_every and completed % args.progress_every == 0:
                    elapsed = max(time.monotonic() - started_at, 0.001)
                    rate = completed / elapsed
                    print(
                        f"progress completed={completed} found={found} rate={rate:.1f}/s",
                        file=sys.stderr,
                    )

                submit_next(executor, futures, domains, results_dir, args.timeout)

    print(f"done completed={completed} found={found}", file=sys.stderr)
    return 0


def iter_limited(domains: Iterator[str], limit: int) -> Iterator[str]:
    if limit < 0:
        return

    for index, domain in enumerate(domains):
        if index >= limit:
            break
        yield domain


def main() -> int:
    return crawl(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
