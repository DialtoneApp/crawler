from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Iterator
from urllib.parse import urlsplit

from .models import DomainInput


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe top domains and emit compact machine-surface receipts."
    )
    parser.add_argument("--csv", default="./top-1m.csv", help="CSV file containing domains.")
    parser.add_argument(
        "--results-dir",
        default="./results",
        help="Directory where receipt artifacts are written.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=24,
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
        default=250,
        help="Print progress every N completed domains. Use 0 to disable.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=100,
        help="Persist checkpoint metadata every N completed domains.",
    )
    parser.add_argument(
        "--stalled-log-every",
        type=float,
        default=10.0,
        help="Log pending in-flight domains every N seconds when no domains are completing. Use 0 to disable.",
    )
    parser.add_argument(
        "--domain-wall-clock-limit",
        type=float,
        default=30.0,
        help="Best-effort per-domain wall-clock limit in seconds. Use 0 to disable.",
    )
    parser.add_argument(
        "--receipt-shard-max-bytes",
        type=int,
        default=128 * 1024 * 1024,
        help="Rotate receipt NDJSON shards after this many bytes. Use 0 to disable byte-based rotation.",
    )
    parser.add_argument(
        "--receipt-shard-max-records",
        type=int,
        default=100_000,
        help="Rotate receipt NDJSON shards after this many records. Use 0 to disable record-based rotation.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start at the beginning instead of resuming from checkpoint.json.",
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


def parse_rank_and_candidate(row: list[str]) -> tuple[int | None, str | None]:
    if not row:
        return None, None

    if len(row) > 1 and row[0].strip().isdigit():
        return int(row[0].strip()), row[1]

    raw = row[0].strip()
    if not raw:
        return None, None

    if "\t" in raw:
        tab_parts = [part.strip() for part in raw.split("\t") if part.strip()]
        if len(tab_parts) >= 2 and tab_parts[0].isdigit():
            return int(tab_parts[0]), tab_parts[1]

    whitespace_parts = raw.split(None, 1)
    if len(whitespace_parts) == 2 and whitespace_parts[0].isdigit():
        return int(whitespace_parts[0]), whitespace_parts[1]

    return None, raw


def iter_domains(csv_path: Path) -> Iterator[DomainInput]:
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as csv_file:
        reader = csv.reader(csv_file)
        for row_index, row in enumerate(reader):
            if not row:
                continue

            rank, candidate = parse_rank_and_candidate(row)
            if not candidate:
                continue

            domain = normalize_domain(candidate)
            if domain:
                yield DomainInput(row_index=row_index, rank=rank, domain=domain)


def iter_domains_from_offset(csv_path: Path, start_row_index: int) -> Iterator[DomainInput]:
    for item in iter_domains(csv_path):
        if item.row_index >= start_row_index:
            yield item


def iter_limited(domains: Iterator[DomainInput], limit: int) -> Iterator[DomainInput]:
    if limit < 0:
        return

    for index, item in enumerate(domains):
        if index >= limit:
            break
        yield item
