from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from concurrent_crawl_app.inputs import iter_domains


DEFAULT_LABELS = ("catalog_surface", "callable_surface", "rate_limited")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a targeted rerun CSV from prior crawl receipts."
    )
    parser.add_argument("--csv", default="./top-1m.csv", help="Source CSV used for rank/domain order.")
    parser.add_argument(
        "--results-dir",
        default="./results",
        help="Results directory containing receipt shards under receipts/.",
    )
    parser.add_argument(
        "--output",
        required=True,
        help="Output CSV path for the targeted rerun slice.",
    )
    parser.add_argument(
        "--label",
        action="append",
        default=[],
        help="Repeatable label filter. Defaults to catalog_surface, callable_surface, and rate_limited.",
    )
    parser.add_argument(
        "--strong-commerce-hints",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Include domains with strong commerce hints even if their current label is not directly selected.",
    )
    parser.add_argument(
        "--strong-commerce-max-rank",
        type=int,
        default=None,
        help="Optional max rank for strong-commerce-hint inclusion.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of CSV rows to emit after filtering.",
    )
    return parser.parse_args()


def is_valid_probe(row: dict[str, Any], key: str) -> bool:
    probes = row.get("probes")
    if not isinstance(probes, dict):
        return False
    outcome = probes.get(key)
    return isinstance(outcome, dict) and outcome.get("status") == "valid"


def has_strong_commerce_hints(row: dict[str, Any]) -> bool:
    aggregates = row.get("aggregates")
    if not isinstance(aggregates, dict):
        aggregates = {}

    if row.get("label") in {"catalog_surface", "offer_surface", "machine_payable"}:
        return True

    if any(
        is_valid_probe(row, key)
        for key in ("well_known_ucp", "cart_json", "products_json", "api_products")
    ):
        return True

    if int(aggregates.get("product_count") or 0) > 0:
        return True
    if int(aggregates.get("priced_variant_count") or 0) > 0:
        return True
    if int(aggregates.get("checkout_url_count") or 0) > 0:
        return True
    if int(aggregates.get("commerce_priced_offer_count") or 0) > 0:
        return True
    if isinstance(aggregates.get("payment_rail_hints"), list) and aggregates["payment_rail_hints"]:
        return True
    if isinstance(aggregates.get("ucp_capability_names"), list) and aggregates["ucp_capability_names"]:
        return True
    if isinstance(aggregates.get("sample_offers"), list) and aggregates["sample_offers"]:
        return True
    return bool(aggregates.get("payment_surface"))


def load_selected_domains(
    *,
    receipts_dir: Path,
    labels: set[str],
    include_strong_commerce_hints: bool,
    strong_commerce_max_rank: int | None,
) -> tuple[set[str], dict[str, int]]:
    selected_domains: set[str] = set()
    counts = {
        "matched_label": 0,
        "matched_strong_commerce_hints": 0,
    }

    for path in sorted(receipts_dir.glob("receipt-*.ndjson")):
        with path.open(encoding="utf-8") as receipt_file:
            for line in receipt_file:
                line = line.strip()
                if not line:
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue

                domain = row.get("domain")
                if not isinstance(domain, str) or not domain:
                    continue

                if row.get("label") in labels:
                    if domain not in selected_domains:
                        counts["matched_label"] += 1
                    selected_domains.add(domain)
                    continue

                if not include_strong_commerce_hints:
                    continue

                rank = row.get("rank")
                if (
                    isinstance(strong_commerce_max_rank, int)
                    and isinstance(rank, int)
                    and rank > strong_commerce_max_rank
                ):
                    continue
                if has_strong_commerce_hints(row):
                    if domain not in selected_domains:
                        counts["matched_strong_commerce_hints"] += 1
                    selected_domains.add(domain)

    return selected_domains, counts


def main() -> int:
    args = parse_args()
    csv_path = Path(args.csv)
    results_dir = Path(args.results_dir)
    receipts_dir = results_dir / "receipts"
    output_path = Path(args.output)

    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}", file=sys.stderr)
        return 2
    if not receipts_dir.exists():
        print(f"Receipt directory not found: {receipts_dir}", file=sys.stderr)
        return 2

    labels = {value.strip() for value in args.label if isinstance(value, str) and value.strip()}
    if not labels:
        labels = set(DEFAULT_LABELS)

    selected_domains, counts = load_selected_domains(
        receipts_dir=receipts_dir,
        labels=labels,
        include_strong_commerce_hints=bool(args.strong_commerce_hints),
        strong_commerce_max_rank=args.strong_commerce_max_rank,
    )
    if not selected_domains:
        print("No domains matched the rerun slice filters.", file=sys.stderr)
        return 1

    output_path.parent.mkdir(parents=True, exist_ok=True)
    emitted = 0
    written_domains: set[str] = set()
    with output_path.open("w", encoding="utf-8", newline="") as output_file:
        for domain_input in iter_domains(csv_path):
            if domain_input.domain not in selected_domains or domain_input.domain in written_domains:
                continue
            rank_text = str(domain_input.rank) if isinstance(domain_input.rank, int) else ""
            output_file.write(f"{rank_text},{domain_input.domain}\n")
            written_domains.add(domain_input.domain)
            emitted += 1
            if isinstance(args.limit, int) and args.limit >= 0 and emitted >= args.limit:
                break

    print(
        json.dumps(
            {
                "output": str(output_path),
                "emitted": emitted,
                "selected_domain_count": len(selected_domains),
                "labels": sorted(labels),
                "strong_commerce_hints": bool(args.strong_commerce_hints),
                "strong_commerce_max_rank": args.strong_commerce_max_rank,
                "match_counts": counts,
            },
            indent=2,
            sort_keys=True,
        ),
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
