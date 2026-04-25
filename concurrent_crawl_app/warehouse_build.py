from __future__ import annotations

import argparse
import csv
import json
import shutil
import sqlite3
import subprocess
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .public_export import (
    as_int,
    as_string_list,
    best_probe,
    build_public_receipt,
    has_valid_probe,
    json_text,
    sample_list,
)


LABEL_WEIGHTS = {
    "unreachable": 0,
    "no_clear_signal": 1,
    "crawl_basics_only": 2,
    "rate_limited": 2,
    "ai_readable": 3,
    "callable_surface": 4,
    "catalog_surface": 5,
    "machine_payable": 6,
}

CHECKOUT_BOUNDARIES = {
    "checkout_redirect",
    "payment_challenge",
    "auth_boundary",
    "input_validation_boundary",
}

CHECKOUT_PROBE_RESULTS = {
    "auth_or_method_boundary",
    "browser_checkout_handoff",
    "browser_checkout_redirect",
    "input_validation",
    "payment_challenge",
    "success_without_payment",
}

SURFACE_FLAG_FIELDS = [
    "robots_present",
    "sitemap_present",
    "llms_present",
    "llms_full_present",
    "openapi_present",
    "agent_present",
    "agent_card_present",
    "ucp_present",
    "commerce_present",
    "x402_present",
    "products_present",
]

EVENT_COLUMNS: list[tuple[str, str]] = [
    ("domain", "TEXT"),
    ("canonical_domain", "TEXT"),
    ("rank", "INTEGER"),
    ("label", "TEXT"),
    ("title", "TEXT"),
    ("favicon_url", "TEXT"),
    ("og_image_url", "TEXT"),
    ("crawled_at", "TEXT"),
    ("crawled_at_epoch", "INTEGER"),
    ("source_receipt_file", "TEXT"),
    ("source_line_number", "INTEGER"),
    ("row_index", "INTEGER"),
    ("tags_json", "TEXT"),
    ("aggregates_json", "TEXT"),
    ("probes_json", "TEXT"),
    ("score_overall", "INTEGER"),
    ("score_readable", "INTEGER"),
    ("score_callable", "INTEGER"),
    ("score_commerce", "INTEGER"),
    ("score_payment", "INTEGER"),
    ("purchase_boundary", "TEXT"),
    ("control_boundary", "TEXT"),
    ("payment_surface", "TEXT"),
    ("crypto_only", "INTEGER"),
    ("verified_payment_surface", "INTEGER"),
    ("robots_present", "INTEGER"),
    ("sitemap_present", "INTEGER"),
    ("llms_present", "INTEGER"),
    ("llms_full_present", "INTEGER"),
    ("openapi_present", "INTEGER"),
    ("agent_present", "INTEGER"),
    ("agent_card_present", "INTEGER"),
    ("ucp_present", "INTEGER"),
    ("commerce_present", "INTEGER"),
    ("x402_present", "INTEGER"),
    ("products_present", "INTEGER"),
    ("cart_present", "INTEGER"),
    ("product_count", "INTEGER"),
    ("variant_count", "INTEGER"),
    ("priced_variant_count", "INTEGER"),
    ("currency_count", "INTEGER"),
    ("offer_count", "INTEGER"),
    ("priced_offer_count", "INTEGER"),
    ("priced_action_count", "INTEGER"),
    ("checkout_url_count", "INTEGER"),
    ("public_endpoint_count", "INTEGER"),
    ("payment_rail_hints_json", "TEXT"),
    ("payment_provider_hints_json", "TEXT"),
    ("payment_methods_json", "TEXT"),
    ("payment_protocols_json", "TEXT"),
    ("payment_assets_json", "TEXT"),
    ("payment_networks_json", "TEXT"),
    ("payment_flow_types_json", "TEXT"),
    ("capability_names_json", "TEXT"),
    ("sample_products_json", "TEXT"),
    ("sample_offers_json", "TEXT"),
    ("sample_actions_json", "TEXT"),
    ("sample_checkout_urls_json", "TEXT"),
    ("payment_probe_status", "TEXT"),
    ("payment_probe_result", "TEXT"),
    ("payment_probe_candidate_source", "TEXT"),
    ("payment_probe_http_status", "INTEGER"),
    ("payment_probe_final_url", "TEXT"),
    ("receipt_r2_key", "TEXT"),
    ("robots_r2_key", "TEXT"),
    ("llms_r2_key", "TEXT"),
    ("llms_full_r2_key", "TEXT"),
    ("evidence_r2_key", "TEXT"),
    ("detail_eligible", "INTEGER"),
    ("surface_count", "INTEGER"),
    ("valid_probe_count", "INTEGER"),
    ("label_weight", "INTEGER"),
    ("checkout_signal", "INTEGER"),
]

DUPLICATE_COLUMNS: list[tuple[str, str]] = [
    ("domain", "TEXT"),
    ("receipt_count", "INTEGER"),
    ("first_crawled_at", "TEXT"),
    ("last_crawled_at", "TEXT"),
    ("best_rank", "INTEGER"),
    ("canonical_label", "TEXT"),
    ("canonical_purchase_boundary", "TEXT"),
    ("canonical_control_boundary", "TEXT"),
    ("canonical_payment_surface", "TEXT"),
    ("canonical_score_overall", "INTEGER"),
    ("canonical_verified_payment_surface", "INTEGER"),
    ("has_conflict", "INTEGER"),
    ("conflict_fields_json", "TEXT"),
    ("label_values_json", "TEXT"),
    ("purchase_boundary_values_json", "TEXT"),
    ("control_boundary_values_json", "TEXT"),
    ("payment_surface_values_json", "TEXT"),
    ("payment_probe_result_values_json", "TEXT"),
]

ARTICLE_METRIC_COLUMNS: list[tuple[str, str]] = [
    ("metric_group", "TEXT"),
    ("metric_name", "TEXT"),
    ("metric_value", "INTEGER"),
]

LEADERBOARD_COLUMNS: list[tuple[str, str]] = [
    ("leaderboard_slug", "TEXT"),
    ("leaderboard_title", "TEXT"),
    ("position", "INTEGER"),
    ("domain", "TEXT"),
    ("rank", "INTEGER"),
    ("label", "TEXT"),
    ("score_overall", "INTEGER"),
    ("purchase_boundary", "TEXT"),
    ("control_boundary", "TEXT"),
    ("payment_surface", "TEXT"),
    ("verified_payment_surface", "INTEGER"),
    ("product_count", "INTEGER"),
    ("priced_variant_count", "INTEGER"),
    ("priced_action_count", "INTEGER"),
    ("checkout_url_count", "INTEGER"),
    ("payment_probe_result", "TEXT"),
    ("payment_provider_hints_json", "TEXT"),
    ("payment_rail_hints_json", "TEXT"),
    ("title", "TEXT"),
]

EVENT_FIELDS = [name for name, _type in EVENT_COLUMNS]
CANONICAL_FIELDS = EVENT_FIELDS + ["duplicate_count", "duplicate_conflict", "conflict_fields_json"]
DUPLICATE_FIELDS = [name for name, _type in DUPLICATE_COLUMNS]
ARTICLE_METRIC_FIELDS = [name for name, _type in ARTICLE_METRIC_COLUMNS]
LEADERBOARD_FIELDS = [name for name, _type in LEADERBOARD_COLUMNS]

ARTICLE_BUCKETS = [
    (
        "browseable_now",
        "Domains with enough structure for a bot to read or navigate, based on the current label model.",
    ),
    (
        "callable_now",
        "Domains exposing callable API, agent, commerce, or x402 surfaces.",
    ),
    (
        "commerce_legible_now",
        "Domains with catalog, UCP, product, cart, or checkout evidence.",
    ),
    (
        "almost_buyable_now",
        "Domains that reach checkout, payment challenge, or another concrete purchase boundary.",
    ),
    (
        "buyable_today",
        "Domains currently labeled machine_payable by the crawler model.",
    ),
]

LEADERBOARD_DEFINITIONS = [
    {
        "slug": "overall",
        "title": "Overall",
        "where": "1=1",
    },
    {
        "slug": "catalog_surface",
        "title": "Catalog Surface",
        "where": "label = 'catalog_surface'",
    },
    {
        "slug": "callable_surface",
        "title": "Callable Surface",
        "where": "label = 'callable_surface'",
    },
    {
        "slug": "machine_payable",
        "title": "Machine Payable",
        "where": "label = 'machine_payable'",
    },
    {
        "slug": "verified_payment_surface",
        "title": "Verified Payment Surface",
        "where": "verified_payment_surface = 1",
    },
    {
        "slug": "checkout_redirect",
        "title": "Checkout Redirect",
        "where": "purchase_boundary = 'checkout_redirect'",
    },
    {
        "slug": "browseable_now",
        "title": "Browseable Now",
        "where": "label IN ('ai_readable','callable_surface','catalog_surface','machine_payable')",
    },
    {
        "slug": "almost_buyable_now",
        "title": "Almost Buyable Now",
        "where": (
            "checkout_url_count > 0 "
            "OR purchase_boundary IN ('checkout_redirect','payment_challenge','auth_boundary','input_validation_boundary') "
            "OR verified_payment_surface = 1"
        ),
    },
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build a local warehouse, deduped rows, and article stats from crawl receipts."
    )
    parser.add_argument(
        "--results-dir",
        default="./results",
        help="Crawler results directory containing receipts/, positives/, and evidence/.",
    )
    parser.add_argument(
        "--output-dir",
        default="./results/warehouse",
        help="Directory where warehouse artifacts are written.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete the output directory before writing fresh warehouse artifacts.",
    )
    parser.add_argument(
        "--leaderboard-limit",
        type=int,
        default=50,
        help="Maximum rows to keep in each leaderboard export.",
    )
    parser.add_argument(
        "--skip-duckdb",
        action="store_true",
        help="Skip creation of the receipts.duckdb warehouse file.",
    )
    parser.add_argument(
        "--duckdb-bin",
        default="duckdb",
        help="DuckDB CLI binary to use when creating receipts.duckdb.",
    )
    return parser.parse_args()


def iter_receipts_with_source(receipts_dir: Path) -> Iterable[tuple[Path, int, dict[str, Any]]]:
    for path in sorted(receipts_dir.glob("receipt-*.ndjson")):
        with path.open(encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    yield path, line_number, json.loads(line)
                except json.JSONDecodeError:
                    continue


def parse_epoch_us(value: Any) -> int | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1_000_000)


def normalize_receipt_event(
    receipt: dict[str, Any],
    *,
    source_receipt_file: str,
    source_line_number: int,
    evidence_dir: Path,
    positive_domains: set[str],
) -> dict[str, Any] | None:
    domain = str(receipt.get("domain") or "").strip().lower()
    if not domain:
        return None

    source_evidence_dir = evidence_dir / domain
    detail_eligible = domain in positive_domains
    index_row, detail = build_public_receipt(
        receipt,
        source_evidence_dir=source_evidence_dir,
        detail_eligible=detail_eligible,
    )

    probes = receipt.get("probes") if isinstance(receipt.get("probes"), dict) else {}
    aggregates = receipt.get("aggregates") if isinstance(receipt.get("aggregates"), dict) else {}
    payment_probe = best_probe(probes, ("payment_probe",))
    payment_probe_facts = payment_probe.get("facts") if isinstance(payment_probe, dict) and isinstance(payment_probe.get("facts"), dict) else {}
    valid_probe_count = sum(
        1
        for outcome in probes.values()
        if isinstance(outcome, dict) and outcome.get("status") == "valid"
    )
    surface_count = sum(int(index_row[field] or 0) for field in SURFACE_FLAG_FIELDS)
    checkout_url_count = as_int(
        aggregates.get("checkout_url_count")
        or aggregates.get("purchase_intent_url_count")
    )
    public_endpoint_count = as_int(
        aggregates.get("agent_public_endpoint_count")
        or aggregates.get("public_endpoint_count")
    )
    payment_probe_result = payment_probe_facts.get("probe_result") if isinstance(payment_probe_facts, dict) else None
    checkout_signal = int(
        bool(checkout_url_count)
        or detail["boundaries"]["purchase_boundary"] in CHECKOUT_BOUNDARIES
        or payment_probe_result in CHECKOUT_PROBE_RESULTS
    )

    row = {
        "domain": index_row["domain"],
        "canonical_domain": index_row["canonical_domain"],
        "rank": index_row["rank"],
        "label": index_row["label"],
        "title": index_row["title"],
        "favicon_url": index_row["favicon_url"],
        "og_image_url": index_row["og_image_url"],
        "crawled_at": index_row["crawled_at"],
        "crawled_at_epoch": parse_epoch_us(index_row["crawled_at"]),
        "source_receipt_file": source_receipt_file,
        "source_line_number": source_line_number,
        "row_index": as_int(receipt.get("row_index")),
        "tags_json": index_row["tags_json"],
        "aggregates_json": json_text(aggregates),
        "probes_json": json_text(probes),
        "score_overall": index_row["score_overall"],
        "score_readable": index_row["score_readable"],
        "score_callable": index_row["score_callable"],
        "score_commerce": index_row["score_commerce"],
        "score_payment": index_row["score_payment"],
        "purchase_boundary": index_row["purchase_boundary"],
        "control_boundary": index_row["control_boundary"],
        "payment_surface": index_row["payment_surface"],
        "crypto_only": index_row["crypto_only"],
        "verified_payment_surface": index_row["verified_payment_surface"],
        "robots_present": index_row["robots_present"],
        "sitemap_present": index_row["sitemap_present"],
        "llms_present": index_row["llms_present"],
        "llms_full_present": index_row["llms_full_present"],
        "openapi_present": index_row["openapi_present"],
        "agent_present": index_row["agent_present"],
        "agent_card_present": index_row["agent_card_present"],
        "ucp_present": index_row["ucp_present"],
        "commerce_present": index_row["commerce_present"],
        "x402_present": index_row["x402_present"],
        "products_present": index_row["products_present"],
        "cart_present": int(has_valid_probe(probes, ("cart_json",))),
        "product_count": index_row["product_count"],
        "variant_count": index_row["variant_count"],
        "priced_variant_count": index_row["priced_variant_count"],
        "currency_count": index_row["currency_count"],
        "offer_count": index_row["offer_count"],
        "priced_offer_count": index_row["priced_offer_count"],
        "priced_action_count": index_row["priced_action_count"],
        "checkout_url_count": checkout_url_count,
        "public_endpoint_count": public_endpoint_count,
        "payment_rail_hints_json": index_row["payment_rail_hints_json"],
        "payment_provider_hints_json": index_row["payment_provider_hints_json"],
        "payment_methods_json": index_row["payment_methods_json"],
        "payment_protocols_json": index_row["payment_protocols_json"],
        "payment_assets_json": index_row["payment_assets_json"],
        "payment_networks_json": index_row["payment_networks_json"],
        "payment_flow_types_json": index_row["payment_flow_types_json"],
        "capability_names_json": index_row["capability_names_json"],
        "sample_products_json": json_text(detail["samples"]["products"]),
        "sample_offers_json": json_text(detail["samples"]["offers"]),
        "sample_actions_json": json_text(detail["samples"]["actions"]),
        "sample_checkout_urls_json": json_text(as_string_list(aggregates.get("sample_checkout_urls"))),
        "payment_probe_status": payment_probe.get("status") if isinstance(payment_probe, dict) else None,
        "payment_probe_result": payment_probe_result,
        "payment_probe_candidate_source": payment_probe_facts.get("candidate_source") if isinstance(payment_probe_facts, dict) else None,
        "payment_probe_http_status": payment_probe.get("http_status") if isinstance(payment_probe, dict) else None,
        "payment_probe_final_url": payment_probe.get("final_url") if isinstance(payment_probe, dict) else None,
        "receipt_r2_key": index_row["receipt_r2_key"],
        "robots_r2_key": index_row["robots_r2_key"],
        "llms_r2_key": index_row["llms_r2_key"],
        "llms_full_r2_key": index_row["llms_full_r2_key"],
        "evidence_r2_key": index_row["evidence_r2_key"],
        "detail_eligible": int(detail_eligible),
        "surface_count": surface_count,
        "valid_probe_count": valid_probe_count,
        "label_weight": LABEL_WEIGHTS.get(index_row["label"], 0),
        "checkout_signal": checkout_signal,
    }
    return row


def ensure_output_dir(output_dir: Path, *, clean: bool) -> None:
    if clean and output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    leaderboards_dir = output_dir / "leaderboards"
    if leaderboards_dir.exists():
        shutil.rmtree(leaderboards_dir)
    leaderboards_dir.mkdir(parents=True, exist_ok=True)


def create_sqlite_connection(sqlite_path: Path) -> sqlite3.Connection:
    if sqlite_path.exists():
        sqlite_path.unlink()
    connection = sqlite3.connect(sqlite_path)
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA journal_mode=WAL")
    connection.execute("PRAGMA synchronous=NORMAL")
    connection.execute("PRAGMA temp_store=MEMORY")
    return connection


def create_table(connection: sqlite3.Connection, name: str, columns: list[tuple[str, str]]) -> None:
    definition = ", ".join(f"{column} {column_type}" for column, column_type in columns)
    connection.execute(f"DROP TABLE IF EXISTS {name}")
    connection.execute(f"CREATE TABLE {name} ({definition})")


def sql_placeholders(count: int) -> str:
    return ", ".join("?" for _ in range(count))


def batched(values: Iterable[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    for value in values:
        batch.append(value)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def ingest_events(
    *,
    connection: sqlite3.Connection,
    receipts_dir: Path,
    evidence_dir: Path,
    positive_domains: set[str],
    events_csv_path: Path,
) -> dict[str, int]:
    create_table(connection, "receipt_events", EVENT_COLUMNS)
    insert_sql = (
        f"INSERT INTO receipt_events ({', '.join(EVENT_FIELDS)}) "
        f"VALUES ({sql_placeholders(len(EVENT_FIELDS))})"
    )
    counts = {
        "raw_receipt_count": 0,
        "receipt_file_count": 0,
    }
    seen_receipt_files: set[str] = set()

    def rows() -> Iterable[dict[str, Any]]:
        for path, line_number, receipt in iter_receipts_with_source(receipts_dir):
            row = normalize_receipt_event(
                receipt,
                source_receipt_file=path.name,
                source_line_number=line_number,
                evidence_dir=evidence_dir,
                positive_domains=positive_domains,
            )
            if row is None:
                continue
            counts["raw_receipt_count"] += 1
            if path.name not in seen_receipt_files:
                seen_receipt_files.add(path.name)
                counts["receipt_file_count"] += 1
            yield row

    with events_csv_path.open("w", newline="", encoding="utf-8") as csv_handle:
        writer = csv.DictWriter(csv_handle, fieldnames=EVENT_FIELDS)
        writer.writeheader()
        for batch in batched(rows(), 500):
            connection.executemany(
                insert_sql,
                [[row[field] for field in EVENT_FIELDS] for row in batch],
            )
            writer.writerows(batch)
            connection.commit()

    connection.execute("CREATE INDEX receipt_events_domain_idx ON receipt_events(domain)")
    connection.execute("CREATE INDEX receipt_events_label_idx ON receipt_events(label)")
    connection.execute("CREATE INDEX receipt_events_score_idx ON receipt_events(score_overall)")
    connection.commit()
    return counts


def build_canonical_table(connection: sqlite3.Connection) -> None:
    column_projection = ", ".join(f"ranked.{field}" for field in EVENT_FIELDS)
    connection.execute("DROP TABLE IF EXISTS canonical_receipts")
    connection.execute(
        f"""
        CREATE TABLE canonical_receipts AS
        WITH ranked AS (
          SELECT
            receipt_events.*,
            ROW_NUMBER() OVER (
              PARTITION BY domain
              ORDER BY
                verified_payment_surface DESC,
                checkout_signal DESC,
                label_weight DESC,
                score_overall DESC,
                score_payment DESC,
                surface_count DESC,
                valid_probe_count DESC,
                COALESCE(checkout_url_count, 0) DESC,
                COALESCE(product_count, 0) DESC,
                COALESCE(priced_variant_count, 0) DESC,
                COALESCE(priced_action_count, 0) DESC,
                COALESCE(crawled_at_epoch, 0) DESC,
                CASE WHEN rank IS NULL THEN 1 ELSE 0 END ASC,
                rank ASC,
                source_receipt_file DESC,
                source_line_number DESC
            ) AS canonical_row_number
          FROM receipt_events
        )
        SELECT {column_projection}
        FROM ranked
        WHERE canonical_row_number = 1
        """
    )
    connection.execute("CREATE INDEX canonical_receipts_domain_idx ON canonical_receipts(domain)")
    connection.execute("CREATE INDEX canonical_receipts_label_idx ON canonical_receipts(label)")
    connection.commit()


def split_group_concat(value: Any) -> list[str]:
    if not isinstance(value, str) or not value:
        return []
    parts = [part.strip() for part in value.split(",")]
    return sorted({part for part in parts if part})


def decode_json_text(value: Any) -> Any:
    if not isinstance(value, str) or not value:
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def expand_json_columns(row: dict[str, Any]) -> dict[str, Any]:
    expanded: dict[str, Any] = {}
    for key, value in row.items():
        if key.endswith("_json"):
            expanded[key[:-5]] = decode_json_text(value)
        else:
            expanded[key] = value
    return expanded


def build_duplicate_tables(connection: sqlite3.Connection, output_dir: Path) -> dict[str, int]:
    create_table(connection, "duplicate_domains", DUPLICATE_COLUMNS)
    query = """
        SELECT
          e.domain AS domain,
          COUNT(*) AS receipt_count,
          MIN(e.crawled_at) AS first_crawled_at,
          MAX(e.crawled_at) AS last_crawled_at,
          MIN(e.rank) AS best_rank,
          c.label AS canonical_label,
          c.purchase_boundary AS canonical_purchase_boundary,
          c.control_boundary AS canonical_control_boundary,
          c.payment_surface AS canonical_payment_surface,
          c.score_overall AS canonical_score_overall,
          c.verified_payment_surface AS canonical_verified_payment_surface,
          GROUP_CONCAT(DISTINCT e.label) AS label_values,
          GROUP_CONCAT(DISTINCT COALESCE(e.purchase_boundary, '')) AS purchase_boundary_values,
          GROUP_CONCAT(DISTINCT COALESCE(e.control_boundary, '')) AS control_boundary_values,
          GROUP_CONCAT(DISTINCT COALESCE(e.payment_surface, '')) AS payment_surface_values,
          GROUP_CONCAT(DISTINCT COALESCE(e.payment_probe_result, '')) AS payment_probe_result_values
        FROM receipt_events e
        JOIN canonical_receipts c ON c.domain = e.domain
        GROUP BY e.domain
        HAVING COUNT(*) > 1
        ORDER BY receipt_count DESC, e.domain ASC
    """
    insert_sql = (
        f"INSERT INTO duplicate_domains ({', '.join(DUPLICATE_FIELDS)}) "
        f"VALUES ({sql_placeholders(len(DUPLICATE_FIELDS))})"
    )
    duplicate_stats = {
        "duplicate_domain_count": 0,
        "duplicate_row_count": 0,
        "conflicting_domain_count": 0,
    }
    duplicate_rows: list[dict[str, Any]] = []

    for raw_row in connection.execute(query):
        row = dict(raw_row)
        label_values = split_group_concat(row.pop("label_values"))
        purchase_boundary_values = split_group_concat(row.pop("purchase_boundary_values"))
        control_boundary_values = split_group_concat(row.pop("control_boundary_values"))
        payment_surface_values = split_group_concat(row.pop("payment_surface_values"))
        payment_probe_result_values = split_group_concat(row.pop("payment_probe_result_values"))

        conflict_fields: list[str] = []
        if len(label_values) > 1:
            conflict_fields.append("label")
        if len(purchase_boundary_values) > 1:
            conflict_fields.append("purchase_boundary")
        if len(control_boundary_values) > 1:
            conflict_fields.append("control_boundary")
        if len(payment_surface_values) > 1:
            conflict_fields.append("payment_surface")
        if len(payment_probe_result_values) > 1:
            conflict_fields.append("payment_probe_result")

        row["has_conflict"] = int(bool(conflict_fields))
        row["conflict_fields_json"] = json_text(conflict_fields)
        row["label_values_json"] = json_text(label_values)
        row["purchase_boundary_values_json"] = json_text(purchase_boundary_values)
        row["control_boundary_values_json"] = json_text(control_boundary_values)
        row["payment_surface_values_json"] = json_text(payment_surface_values)
        row["payment_probe_result_values_json"] = json_text(payment_probe_result_values)
        duplicate_rows.append({field: row.get(field) for field in DUPLICATE_FIELDS})

        duplicate_stats["duplicate_domain_count"] += 1
        duplicate_stats["duplicate_row_count"] += max(int(row["receipt_count"] or 0) - 1, 0)
        duplicate_stats["conflicting_domain_count"] += int(bool(conflict_fields))

    if duplicate_rows:
        connection.executemany(
            insert_sql,
            [[row[field] for field in DUPLICATE_FIELDS] for row in duplicate_rows],
        )
        connection.execute("CREATE INDEX duplicate_domains_domain_idx ON duplicate_domains(domain)")
        connection.commit()

    duplicate_csv_path = output_dir / "duplicate_domains.csv"
    duplicate_ndjson_path = output_dir / "duplicate_domains.ndjson"
    with (
        duplicate_csv_path.open("w", newline="", encoding="utf-8") as csv_handle,
        duplicate_ndjson_path.open("w", encoding="utf-8") as ndjson_handle,
    ):
        writer = csv.DictWriter(csv_handle, fieldnames=DUPLICATE_FIELDS)
        writer.writeheader()
        for row in duplicate_rows:
            writer.writerow(row)
            ndjson_handle.write(
                json.dumps(expand_json_columns(row), separators=(",", ":"), sort_keys=True)
            )
            ndjson_handle.write("\n")

    return duplicate_stats


def load_json_array(text: Any) -> list[str]:
    if not isinstance(text, str) or not text:
        return []
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return []
    if not isinstance(value, list):
        return []
    results: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        cleaned = item.strip()
        if cleaned and cleaned not in results:
            results.append(cleaned)
    return results


def canonical_row_is_browseable(row: sqlite3.Row) -> bool:
    return row["label"] in {"ai_readable", "callable_surface", "catalog_surface", "machine_payable"}


def canonical_row_is_callable(row: sqlite3.Row) -> bool:
    return bool(
        row["openapi_present"]
        or row["agent_present"]
        or row["agent_card_present"]
        or row["commerce_present"]
        or row["x402_present"]
        or row["label"] in {"callable_surface", "catalog_surface", "machine_payable"}
    )


def canonical_row_is_commerce_legible(row: sqlite3.Row) -> bool:
    return bool(
        row["products_present"]
        or row["ucp_present"]
        or row["cart_present"]
        or (row["checkout_url_count"] or 0) > 0
        or row["label"] in {"catalog_surface", "machine_payable"}
    )


def canonical_row_is_almost_buyable(row: sqlite3.Row) -> bool:
    return bool(
        row["verified_payment_surface"]
        or (row["checkout_url_count"] or 0) > 0
        or row["purchase_boundary"] in CHECKOUT_BOUNDARIES
        or row["payment_probe_result"] in CHECKOUT_PROBE_RESULTS
    )


def canonical_row_is_buyable_today(row: sqlite3.Row) -> bool:
    return row["label"] == "machine_payable"


def top_counter(counter: Counter[str], *, limit: int = 20) -> list[dict[str, Any]]:
    return [
        {"name": name, "count": count}
        for name, count in counter.most_common(limit)
    ]


def build_article_metrics_rows(stats: dict[str, Any]) -> list[dict[str, Any]]:
    metric_rows: list[dict[str, Any]] = []

    def add_mapping(group: str, mapping: dict[str, Any]) -> None:
        for key, value in mapping.items():
            if isinstance(value, bool):
                metric_rows.append(
                    {
                        "metric_group": group,
                        "metric_name": key,
                        "metric_value": int(value),
                    }
                )
            elif isinstance(value, int):
                metric_rows.append(
                    {
                        "metric_group": group,
                        "metric_name": key,
                        "metric_value": value,
                    }
                )

    add_mapping("counts", stats["counts"])
    add_mapping("labels", stats["labels"])
    add_mapping("purchase_boundary", stats["purchase_boundaries"])
    add_mapping("control_boundary", stats["control_boundaries"])
    add_mapping("payment_probe_status", stats["payment_probe_statuses"])
    add_mapping("payment_probe_result", stats["payment_probe_results"])
    add_mapping("payment_surface", stats["payment_surfaces"])
    add_mapping("surfaces", stats["surfaces"])
    add_mapping("article_bucket", stats["article_buckets"])
    add_mapping("duplicates", stats["duplicates"])

    for group_name, key in (
        ("payment_provider_hint", "payment_provider_hints"),
        ("payment_rail_hint", "payment_rail_hints"),
        ("payment_method", "payment_methods"),
        ("payment_protocol", "payment_protocols"),
        ("payment_asset", "payment_assets"),
        ("payment_network", "payment_networks"),
        ("capability_name", "capability_names"),
    ):
        for item in stats["top_values"][key]:
            metric_rows.append(
                {
                    "metric_group": group_name,
                    "metric_name": item["name"],
                    "metric_value": item["count"],
                }
            )

    return metric_rows


def export_canonical_and_stats(
    connection: sqlite3.Connection,
    *,
    output_dir: Path,
    ingest_counts: dict[str, int],
    duplicate_stats: dict[str, int],
) -> dict[str, Any]:
    canonical_csv_path = output_dir / "canonical_receipts.csv"
    canonical_ndjson_path = output_dir / "canonical_receipts.ndjson"

    label_counter: Counter[str] = Counter()
    purchase_boundary_counter: Counter[str] = Counter()
    control_boundary_counter: Counter[str] = Counter()
    payment_probe_status_counter: Counter[str] = Counter()
    payment_probe_result_counter: Counter[str] = Counter()
    payment_surface_counter: Counter[str] = Counter()
    surface_counter: Counter[str] = Counter()
    provider_counter: Counter[str] = Counter()
    rail_counter: Counter[str] = Counter()
    method_counter: Counter[str] = Counter()
    protocol_counter: Counter[str] = Counter()
    asset_counter: Counter[str] = Counter()
    network_counter: Counter[str] = Counter()
    capability_counter: Counter[str] = Counter()
    article_bucket_counts = {
        "browseable_now": 0,
        "callable_now": 0,
        "commerce_legible_now": 0,
        "almost_buyable_now": 0,
        "buyable_today": 0,
    }

    query = """
        SELECT
          c.*,
          COALESCE(d.receipt_count, 1) AS duplicate_count,
          COALESCE(d.has_conflict, 0) AS duplicate_conflict,
          COALESCE(d.conflict_fields_json, '[]') AS conflict_fields_json
        FROM canonical_receipts c
        LEFT JOIN duplicate_domains d ON d.domain = c.domain
        ORDER BY c.domain ASC
    """

    with (
        canonical_csv_path.open("w", newline="", encoding="utf-8") as csv_handle,
        canonical_ndjson_path.open("w", encoding="utf-8") as ndjson_handle,
    ):
        writer = csv.DictWriter(csv_handle, fieldnames=CANONICAL_FIELDS)
        writer.writeheader()
        for row in connection.execute(query):
            row_dict = dict(row)
            writer.writerow({field: row_dict.get(field) for field in CANONICAL_FIELDS})
            ndjson_handle.write(
                json.dumps(expand_json_columns(row_dict), separators=(",", ":"), sort_keys=True)
            )
            ndjson_handle.write("\n")

            label_counter[row["label"] or "unknown"] += 1
            purchase_boundary_counter[row["purchase_boundary"] or "unknown"] += 1
            control_boundary_counter[row["control_boundary"] or "unknown"] += 1
            payment_probe_status_counter[row["payment_probe_status"] or "none"] += 1
            payment_probe_result_counter[row["payment_probe_result"] or "none"] += 1
            payment_surface_counter[row["payment_surface"] or "none"] += 1

            for field in SURFACE_FLAG_FIELDS + ["cart_present", "verified_payment_surface"]:
                if row[field]:
                    surface_counter[field] += 1

            for item in load_json_array(row["payment_provider_hints_json"]):
                provider_counter[item] += 1
            for item in load_json_array(row["payment_rail_hints_json"]):
                rail_counter[item] += 1
            for item in load_json_array(row["payment_methods_json"]):
                method_counter[item] += 1
            for item in load_json_array(row["payment_protocols_json"]):
                protocol_counter[item] += 1
            for item in load_json_array(row["payment_assets_json"]):
                asset_counter[item] += 1
            for item in load_json_array(row["payment_networks_json"]):
                network_counter[item] += 1
            for item in load_json_array(row["capability_names_json"]):
                capability_counter[item] += 1

            if canonical_row_is_browseable(row):
                article_bucket_counts["browseable_now"] += 1
            if canonical_row_is_callable(row):
                article_bucket_counts["callable_now"] += 1
            if canonical_row_is_commerce_legible(row):
                article_bucket_counts["commerce_legible_now"] += 1
            if canonical_row_is_almost_buyable(row):
                article_bucket_counts["almost_buyable_now"] += 1
            if canonical_row_is_buyable_today(row):
                article_bucket_counts["buyable_today"] += 1

    canonical_domain_count = sum(label_counter.values())
    stats = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "counts": {
            "raw_receipts": ingest_counts["raw_receipt_count"],
            "receipt_files": ingest_counts["receipt_file_count"],
            "canonical_domains": canonical_domain_count,
            "detail_eligible_domains": int(
                connection.execute(
                    "SELECT COUNT(*) FROM canonical_receipts WHERE detail_eligible = 1"
                ).fetchone()[0]
            ),
        },
        "duplicates": {
            "duplicate_domains": duplicate_stats["duplicate_domain_count"],
            "duplicate_rows": duplicate_stats["duplicate_row_count"],
            "conflicting_domains": duplicate_stats["conflicting_domain_count"],
        },
        "labels": dict(sorted(label_counter.items())),
        "purchase_boundaries": dict(sorted(purchase_boundary_counter.items())),
        "control_boundaries": dict(sorted(control_boundary_counter.items())),
        "payment_probe_statuses": dict(sorted(payment_probe_status_counter.items())),
        "payment_probe_results": dict(sorted(payment_probe_result_counter.items())),
        "payment_surfaces": dict(sorted(payment_surface_counter.items())),
        "surfaces": dict(sorted(surface_counter.items())),
        "article_buckets": article_bucket_counts,
        "article_bucket_definitions": [
            {"name": name, "definition": definition}
            for name, definition in ARTICLE_BUCKETS
        ],
        "top_values": {
            "payment_provider_hints": top_counter(provider_counter),
            "payment_rail_hints": top_counter(rail_counter),
            "payment_methods": top_counter(method_counter),
            "payment_protocols": top_counter(protocol_counter),
            "payment_assets": top_counter(asset_counter),
            "payment_networks": top_counter(network_counter),
            "capability_names": top_counter(capability_counter),
        },
    }

    article_metrics_rows = build_article_metrics_rows(stats)
    article_metrics_csv_path = output_dir / "article_metrics.csv"
    with article_metrics_csv_path.open("w", newline="", encoding="utf-8") as csv_handle:
        writer = csv.DictWriter(csv_handle, fieldnames=ARTICLE_METRIC_FIELDS)
        writer.writeheader()
        writer.writerows(article_metrics_rows)

    stats["paths"] = {
        "receipt_events_csv": "receipt_events.csv",
        "canonical_receipts_csv": "canonical_receipts.csv",
        "canonical_receipts_ndjson": "canonical_receipts.ndjson",
        "duplicate_domains_csv": "duplicate_domains.csv",
        "duplicate_domains_ndjson": "duplicate_domains.ndjson",
        "article_metrics_csv": "article_metrics.csv",
        "leaderboard_rows_csv": "leaderboard_rows.csv",
        "duckdb": "receipts.duckdb",
        "sqlite": "receipts.sqlite",
    }
    return stats


def leaderboard_entry(row: sqlite3.Row, *, slug: str, title: str, position: int) -> dict[str, Any]:
    row_dict = dict(row)
    return {
        "leaderboard_slug": slug,
        "leaderboard_title": title,
        "position": position,
        "domain": row_dict["domain"],
        "rank": row_dict["rank"],
        "label": row_dict["label"],
        "score_overall": row_dict["score_overall"],
        "purchase_boundary": row_dict["purchase_boundary"],
        "control_boundary": row_dict["control_boundary"],
        "payment_surface": row_dict["payment_surface"],
        "verified_payment_surface": row_dict["verified_payment_surface"],
        "product_count": row_dict["product_count"],
        "priced_variant_count": row_dict["priced_variant_count"],
        "priced_action_count": row_dict["priced_action_count"],
        "checkout_url_count": row_dict["checkout_url_count"],
        "payment_probe_result": row_dict["payment_probe_result"],
        "payment_provider_hints_json": row_dict["payment_provider_hints_json"],
        "payment_rail_hints_json": row_dict["payment_rail_hints_json"],
        "title": row_dict["title"],
    }


def build_leaderboards(
    connection: sqlite3.Connection,
    *,
    output_dir: Path,
    leaderboard_limit: int,
) -> dict[str, list[dict[str, Any]]]:
    leaderboards_dir = output_dir / "leaderboards"
    leaderboard_rows_csv_path = output_dir / "leaderboard_rows.csv"
    leaderboard_map: dict[str, list[dict[str, Any]]] = {}

    with leaderboard_rows_csv_path.open("w", newline="", encoding="utf-8") as csv_handle:
        writer = csv.DictWriter(csv_handle, fieldnames=LEADERBOARD_FIELDS)
        writer.writeheader()
        for definition in LEADERBOARD_DEFINITIONS:
            query = f"""
                SELECT *
                FROM canonical_receipts
                WHERE {definition['where']}
                ORDER BY
                  verified_payment_surface DESC,
                  checkout_signal DESC,
                  score_overall DESC,
                  score_payment DESC,
                  label_weight DESC,
                  COALESCE(checkout_url_count, 0) DESC,
                  COALESCE(product_count, 0) DESC,
                  COALESCE(priced_variant_count, 0) DESC,
                  CASE WHEN rank IS NULL THEN 1 ELSE 0 END ASC,
                  rank ASC,
                  domain ASC
                LIMIT ?
            """
            rows: list[dict[str, Any]] = []
            json_rows: list[dict[str, Any]] = []
            for position, row in enumerate(
                connection.execute(query, [leaderboard_limit]),
                start=1,
            ):
                entry = leaderboard_entry(
                    row,
                    slug=definition["slug"],
                    title=definition["title"],
                    position=position,
                )
                rows.append(entry)
                json_rows.append(expand_json_columns(entry))
                writer.writerow(entry)

            leaderboard_map[definition["slug"]] = json_rows
            payload = {
                "slug": definition["slug"],
                "title": definition["title"],
                "rows": json_rows,
            }
            (leaderboards_dir / f"{definition['slug']}.json").write_text(
                json.dumps(payload, indent=2, sort_keys=True),
                encoding="utf-8",
            )

    return leaderboard_map


def duckdb_literal(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/").replace("'", "''")


def build_duckdb(
    *,
    output_dir: Path,
    duckdb_bin: str,
) -> bool:
    duckdb_path = output_dir / "receipts.duckdb"
    if duckdb_path.exists():
        duckdb_path.unlink()

    if shutil.which(duckdb_bin) is None:
        print(
            f"DuckDB binary not found: {duckdb_bin}. Skipping receipts.duckdb creation.",
            file=sys.stderr,
        )
        return False

    sql = f"""
        CREATE TABLE receipt_events AS
        SELECT * FROM read_csv_auto('{duckdb_literal(output_dir / "receipt_events.csv")}', header = true, sample_size = -1);
        CREATE TABLE canonical_receipts AS
        SELECT * FROM read_csv_auto('{duckdb_literal(output_dir / "canonical_receipts.csv")}', header = true, sample_size = -1);
        CREATE TABLE duplicate_domains AS
        SELECT * FROM read_csv_auto('{duckdb_literal(output_dir / "duplicate_domains.csv")}', header = true, sample_size = -1);
        CREATE TABLE article_metrics AS
        SELECT * FROM read_csv_auto('{duckdb_literal(output_dir / "article_metrics.csv")}', header = true, sample_size = -1);
        CREATE TABLE leaderboard_rows AS
        SELECT * FROM read_csv_auto('{duckdb_literal(output_dir / "leaderboard_rows.csv")}', header = true, sample_size = -1);

        CREATE OR REPLACE VIEW label_counts AS
        SELECT label, COUNT(*) AS domain_count
        FROM canonical_receipts
        GROUP BY 1
        ORDER BY domain_count DESC, label ASC;

        CREATE OR REPLACE VIEW purchase_boundary_counts AS
        SELECT purchase_boundary, COUNT(*) AS domain_count
        FROM canonical_receipts
        GROUP BY 1
        ORDER BY domain_count DESC, purchase_boundary ASC;

        CREATE OR REPLACE VIEW control_boundary_counts AS
        SELECT control_boundary, COUNT(*) AS domain_count
        FROM canonical_receipts
        GROUP BY 1
        ORDER BY domain_count DESC, control_boundary ASC;
    """

    subprocess.run(
        [duckdb_bin, str(duckdb_path), "-c", sql],
        check=True,
        capture_output=True,
        text=True,
    )
    return True


def main() -> int:
    args = parse_args()
    results_dir = Path(args.results_dir)
    receipts_dir = results_dir / "receipts"
    positives_dir = results_dir / "positives"
    evidence_dir = results_dir / "evidence"
    output_dir = Path(args.output_dir)

    if not receipts_dir.exists():
        print(f"Receipt directory not found: {receipts_dir}", file=sys.stderr)
        return 2

    ensure_output_dir(output_dir, clean=bool(args.clean))
    positive_domains = {
        path.stem.strip().lower()
        for path in positives_dir.glob("*.json")
        if path.stem.strip()
    }

    sqlite_path = output_dir / "receipts.sqlite"
    events_csv_path = output_dir / "receipt_events.csv"
    connection = create_sqlite_connection(sqlite_path)

    try:
        ingest_counts = ingest_events(
            connection=connection,
            receipts_dir=receipts_dir,
            evidence_dir=evidence_dir,
            positive_domains=positive_domains,
            events_csv_path=events_csv_path,
        )
        build_canonical_table(connection)
        duplicate_stats = build_duplicate_tables(connection, output_dir)
        stats = export_canonical_and_stats(
            connection,
            output_dir=output_dir,
            ingest_counts=ingest_counts,
            duplicate_stats=duplicate_stats,
        )
        leaderboards = build_leaderboards(
            connection,
            output_dir=output_dir,
            leaderboard_limit=max(int(args.leaderboard_limit), 1),
        )
    finally:
        connection.close()

    stats["leaderboards"] = {
        slug: rows[:10]
        for slug, rows in leaderboards.items()
    }

    duckdb_created = False
    if not args.skip_duckdb:
        try:
            duckdb_created = build_duckdb(
                output_dir=output_dir,
                duckdb_bin=args.duckdb_bin,
            )
        except subprocess.CalledProcessError as exc:
            print(exc.stderr or exc.stdout or str(exc), file=sys.stderr)
            return 1

    stats["counts"]["positive_domains"] = len(positive_domains)
    stats["paths"]["duckdb_created"] = int(duckdb_created)
    (output_dir / "article_stats.json").write_text(
        json.dumps(stats, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(
        json.dumps(
            {
                "output_dir": str(output_dir),
                "raw_receipts": stats["counts"]["raw_receipts"],
                "canonical_domains": stats["counts"]["canonical_domains"],
                "duplicate_domains": stats["duplicates"]["duplicate_domains"],
                "conflicting_domains": stats["duplicates"]["conflicting_domains"],
                "duckdb_created": duckdb_created,
            },
            indent=2,
            sort_keys=True,
        ),
        file=sys.stderr,
    )
    return 0
