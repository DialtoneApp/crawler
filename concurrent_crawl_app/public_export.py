from __future__ import annotations

import argparse
import hashlib
import json
import shutil
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

PUBLIC_EXPORT_VERSION = "v1"
PUBLIC_EXPORT_PREFIX = f"top-sites/{PUBLIC_EXPORT_VERSION}"
DEFAULT_PAGE_SIZE = 30
SITEMAP_PAGE_SIZE = 1000

INDEX_FIELDS = [
    "domain",
    "canonical_domain",
    "rank",
    "label",
    "title",
    "favicon_url",
    "og_image_url",
    "crawled_at",
    "tags_json",
    "score_overall",
    "score_readable",
    "score_callable",
    "score_commerce",
    "score_payment",
    "purchase_boundary",
    "control_boundary",
    "payment_surface",
    "crypto_only",
    "verified_payment_surface",
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
    "product_count",
    "variant_count",
    "priced_variant_count",
    "currency_count",
    "offer_count",
    "priced_offer_count",
    "priced_action_count",
    "payment_rail_hints_json",
    "payment_provider_hints_json",
    "payment_methods_json",
    "payment_protocols_json",
    "payment_assets_json",
    "payment_networks_json",
    "payment_flow_types_json",
    "capability_names_json",
    "receipt_r2_key",
    "robots_r2_key",
    "llms_r2_key",
    "llms_full_r2_key",
    "evidence_r2_key",
]

SURFACE_GROUPS = {
    "robots_present": ("robots_txt",),
    "sitemap_present": ("sitemap_xml",),
    "llms_present": ("llms_txt",),
    "llms_full_present": ("llms_full_txt",),
    "openapi_present": ("openapi_json", "api_openapi_json"),
    "agent_present": ("well_known_agent_json", "root_agent_json", "well_known_agents_json", "root_agents_json"),
    "agent_card_present": ("well_known_agent_card",),
    "ucp_present": ("well_known_ucp",),
    "commerce_present": ("well_known_commerce",),
    "x402_present": ("x402_json", "x402_well_known", "remote_x402"),
    "products_present": ("products_json", "api_products"),
}

DETAIL_PROBE_GROUPS = {
    "homepage": ("homepage",),
    "robots_txt": ("robots_txt",),
    "sitemap_xml": ("sitemap_xml",),
    "llms_txt": ("llms_txt",),
    "llms_full_txt": ("llms_full_txt",),
    "openapi": ("api_openapi_json", "openapi_json"),
    "agent": ("well_known_agent_json", "root_agent_json", "well_known_agents_json", "root_agents_json"),
    "agent_card": ("well_known_agent_card",),
    "ucp": ("well_known_ucp",),
    "commerce": ("well_known_commerce",),
    "x402": ("remote_x402", "x402_json", "x402_well_known"),
    "products": ("api_products", "products_json"),
    "cart": ("cart_json",),
    "payment_probe": ("payment_probe",),
}

EVIDENCE_FILE_NAMES = {
    "robots_txt": "robots.txt",
    "llms_txt": "llms.txt",
    "llms_full_txt": "llms-full.txt",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build compact public receipt exports from crawler results."
    )
    parser.add_argument(
        "--results-dir",
        default="./results",
        help="Crawler results directory containing receipts/, positives/, and evidence/.",
    )
    parser.add_argument(
        "--output-dir",
        default="./results/exports/public",
        help="Directory where public export artifacts are written.",
    )
    parser.add_argument(
        "--clean",
        action="store_true",
        help="Delete the output directory before writing fresh export artifacts.",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=DEFAULT_PAGE_SIZE,
        help="Number of top-site rows per exported page.",
    )
    return parser.parse_args()


def iter_receipts(receipts_dir: Path) -> Iterable[dict[str, Any]]:
    for path in sorted(receipts_dir.glob("receipt-*.ndjson")):
        with path.open(encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                yield json.loads(line)


def json_text(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True)


def domain_prefix(domain: str) -> str:
    return hashlib.sha1(domain.encode("utf-8")).hexdigest()[:2]


def public_export_key(*parts: str) -> str:
    return "/".join([PUBLIC_EXPORT_PREFIX, *[part.strip("/") for part in parts if part]])


def document_preview(value: str | None, max_length: int = 180) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(line.strip() for line in value.splitlines() if line.strip())
    normalized = " ".join(normalized.split())
    if not normalized:
        return None
    if len(normalized) <= max_length:
        return normalized
    return f"{normalized[: max_length - 3].rstrip()}..."


def read_text(path: Path) -> str | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return None


def parse_epoch(value: Any) -> int:
    if not isinstance(value, str) or not value:
        return 0
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return 0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def parse_json_list_text(value: Any) -> list[str]:
    if not isinstance(value, str) or not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return as_string_list(parsed)


def format_label(value: Any) -> str:
    normalized = str(value or "").strip()
    return normalized.replace("_", " ") if normalized else "unknown"


def summarize_index_row(row: dict[str, Any]) -> str | None:
    label = format_label(row.get("label"))
    payment_surface = format_label(row.get("payment_surface")) if row.get("payment_surface") else None
    purchase_boundary = format_label(row.get("purchase_boundary")) if row.get("purchase_boundary") else None
    control_boundary = format_label(row.get("control_boundary")) if row.get("control_boundary") else None
    pieces: list[str] = []

    if label != "unknown":
        pieces.append(label)
    if payment_surface:
        pieces.append(payment_surface)
    if row.get("score_overall") is not None:
        pieces.append(f"score {row['score_overall']}")
    if row.get("product_count"):
        pieces.append(f"{row['product_count']} products")
    elif row.get("offer_count"):
        pieces.append(f"{row['offer_count']} offers")
    elif row.get("priced_action_count"):
        pieces.append(f"{row['priced_action_count']} priced actions")
    if purchase_boundary and purchase_boundary != "unknown":
        pieces.append(f"purchase {purchase_boundary}")
    if control_boundary and control_boundary != "unknown":
        pieces.append(f"control {control_boundary}")

    return " | ".join(pieces) if pieces else None


def best_probe(probes: dict[str, Any], keys: tuple[str, ...]) -> dict[str, Any] | None:
    for key in keys:
        outcome = probes.get(key)
        if isinstance(outcome, dict) and outcome.get("status") == "valid":
            return outcome
    for key in keys:
        outcome = probes.get(key)
        if isinstance(outcome, dict):
            return outcome
    return None


def has_valid_probe(probes: dict[str, Any], keys: tuple[str, ...]) -> bool:
    outcome = best_probe(probes, keys)
    return isinstance(outcome, dict) and outcome.get("status") == "valid"


def sample_list(values: Any, limit: int = 3) -> list[Any]:
    if not isinstance(values, list):
        return []
    result: list[Any] = []
    for value in values:
        if isinstance(value, dict):
            result.append(dict(value))
        elif isinstance(value, str):
            result.append(value)
        if len(result) >= limit:
            break
    return result


def as_string_list(value: Any, *, limit: int = 12) -> list[str]:
    if not isinstance(value, list):
        return []
    results: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        cleaned = item.strip()
        if cleaned and cleaned not in results:
            results.append(cleaned)
        if len(results) >= limit:
            break
    return results


def as_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return None


def as_float(value: Any) -> float | None:
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    return None


def build_scores(label: str, aggregates: dict[str, Any], surface_flags: dict[str, bool]) -> dict[str, int]:
    readable = 0
    if surface_flags["robots_present"]:
        readable += 10
    if surface_flags["sitemap_present"]:
        readable += 10
    if surface_flags["llms_present"]:
        readable += 45
    if surface_flags["llms_full_present"]:
        readable += 35
    readable = min(readable, 100)

    callable_score = 0
    if surface_flags["openapi_present"]:
        callable_score += 40
    if surface_flags["agent_present"]:
        callable_score += 25
    if surface_flags["agent_card_present"]:
        callable_score += 15
    if surface_flags["commerce_present"]:
        callable_score += 10
    if surface_flags["x402_present"]:
        callable_score += 10
    callable_score = min(callable_score, 100)

    commerce_score = 0
    if surface_flags["products_present"]:
        commerce_score += 40
    if surface_flags["ucp_present"]:
        commerce_score += 30
    if surface_flags["commerce_present"]:
        commerce_score += 30
    if aggregates.get("sample_offers"):
        commerce_score += 10
    if aggregates.get("sample_products"):
        commerce_score += 10
    commerce_score = min(commerce_score, 100)

    payment_score = 0
    if bool(aggregates.get("verified_payment_surface")):
        payment_score += 70
    if label == "machine_payable":
        payment_score += 20
    if aggregates.get("payment_surface"):
        payment_score += 10
    if aggregates.get("priced_action_count"):
        payment_score += 10
    payment_score = min(payment_score, 100)

    overall = int(round((readable * 0.3) + (callable_score * 0.25) + (commerce_score * 0.25) + (payment_score * 0.2)))
    return {
        "score_overall": overall,
        "score_readable": readable,
        "score_callable": callable_score,
        "score_commerce": commerce_score,
        "score_payment": payment_score,
    }


def build_surface_flags(probes: dict[str, Any]) -> dict[str, bool]:
    return {
        name: has_valid_probe(probes, keys)
        for name, keys in SURFACE_GROUPS.items()
    }


def build_probe_summary(probes: dict[str, Any]) -> dict[str, Any]:
    summary: dict[str, Any] = {}
    for public_key, keys in DETAIL_PROBE_GROUPS.items():
        outcome = best_probe(probes, keys)
        if not isinstance(outcome, dict):
            continue
        summary[public_key] = {
            "status": outcome.get("status"),
            "http_status": outcome.get("http_status"),
            "content_type": outcome.get("content_type"),
            "final_url": outcome.get("final_url"),
            "byte_count": outcome.get("byte_count"),
            "body_sha256": outcome.get("body_sha256"),
            "detail": outcome.get("detail"),
        }
    return summary


def build_r2_keys(domain: str, source_evidence_dir: Path, detail_eligible: bool, receipt_label: str, rank: int | None) -> dict[str, str | None]:
    if not detail_eligible:
        return {
            "receipt_r2_key": None,
            "evidence_r2_key": None,
            "robots_r2_key": None,
            "llms_r2_key": None,
            "llms_full_r2_key": None,
        }

    prefix = domain_prefix(domain)
    receipt_key = public_export_key("receipts", prefix, f"{domain}.json")
    evidence_key = public_export_key("evidence", prefix, domain, "proof.json")

    robots_key = None
    llms_key = None
    llms_full_key = None

    if (source_evidence_dir / "robots.txt").exists():
        robots_key = public_export_key("evidence", prefix, domain, "robots.txt")
    if (source_evidence_dir / "llms.txt").exists():
        llms_key = public_export_key("evidence", prefix, domain, "llms.txt")
    llms_full_path = source_evidence_dir / "llms-full.txt"
    if llms_full_path.exists():
        should_export_llms_full = (
            receipt_label in {"machine_payable", "callable_surface"}
            or (rank is not None and rank <= 10_000)
            or llms_full_path.stat().st_size <= 32_768
        )
        if should_export_llms_full:
            llms_full_key = public_export_key("evidence", prefix, domain, "llms-full.txt")

    return {
        "receipt_r2_key": receipt_key,
        "evidence_r2_key": evidence_key,
        "robots_r2_key": robots_key,
        "llms_r2_key": llms_key,
        "llms_full_r2_key": llms_full_key,
    }


def build_public_receipt(
    receipt: dict[str, Any],
    *,
    source_evidence_dir: Path,
    detail_eligible: bool,
) -> tuple[dict[str, Any], dict[str, Any]]:
    domain = str(receipt.get("domain") or "").strip().lower()
    rank = as_int(receipt.get("rank"))
    label = str(receipt.get("label") or "no_clear_signal")
    probes = receipt.get("probes") if isinstance(receipt.get("probes"), dict) else {}
    aggregates = receipt.get("aggregates") if isinstance(receipt.get("aggregates"), dict) else {}
    tags = as_string_list(receipt.get("tags"))
    surface_flags = build_surface_flags(probes)
    scores = build_scores(label, aggregates, surface_flags)
    r2_keys = build_r2_keys(domain, source_evidence_dir, detail_eligible, label, rank)

    detail = {
        "domain": domain,
        "canonical_domain": domain,
        "requested_domain": domain,
        "rank": rank,
        "label": label,
        "title": receipt.get("title"),
        "favicon_url": aggregates.get("favicon_url"),
        "og_image_url": aggregates.get("og_image_url"),
        "site_url": f"https://{domain}" if domain else None,
        "homepage_url": f"https://{domain}" if domain else None,
        "homepage_final_url": f"https://{domain}" if domain else None,
        "crawled_at": receipt.get("crawled_at"),
        "scanned_at": receipt.get("crawled_at"),
        "tags": tags,
        "scores": scores,
        "boundaries": {
            "purchase_boundary": aggregates.get("purchase_boundary") or "unknown",
            "control_boundary": aggregates.get("control_boundary") or "unknown",
        },
        "flags": {
            **surface_flags,
            "verified_payment_surface": bool(aggregates.get("verified_payment_surface")),
            "crypto_only": bool(aggregates.get("crypto_only")),
        },
        "counts": {
            "product_count": as_int(aggregates.get("product_count")),
            "variant_count": as_int(aggregates.get("variant_count")),
            "priced_variant_count": as_int(aggregates.get("priced_variant_count")),
            "currency_count": as_int(aggregates.get("currency_count")),
            "offer_count": as_int(aggregates.get("commerce_offer_count") or aggregates.get("offer_count")),
            "priced_offer_count": as_int(aggregates.get("commerce_priced_offer_count") or aggregates.get("priced_offer_count")),
            "priced_action_count": as_int(aggregates.get("priced_action_count")),
            "min_price": as_float(aggregates.get("min_price")),
            "max_price": as_float(aggregates.get("max_price")),
        },
        "payment": {
            "payment_surface": aggregates.get("payment_surface"),
            "provider_hints": as_string_list(aggregates.get("payment_provider_hints")),
            "rail_hints": as_string_list(aggregates.get("payment_rail_hints")),
            "methods": as_string_list(aggregates.get("observed_payment_methods")),
            "protocols": as_string_list(aggregates.get("observed_payment_protocols")),
            "assets": as_string_list(aggregates.get("observed_payment_assets")),
            "networks": as_string_list(aggregates.get("observed_payment_networks")),
            "flow_types": as_string_list(aggregates.get("observed_payment_flow_types")),
            "requirement_keys": as_string_list(aggregates.get("observed_payment_requirement_keys")),
            "endpoint_hosts": as_string_list(aggregates.get("payment_endpoint_hosts")),
        },
        "capabilities": as_string_list(aggregates.get("observed_capability_names")),
        "samples": {
            "products": sample_list(aggregates.get("sample_products")),
            "offers": sample_list(aggregates.get("sample_offers")),
            "actions": sample_list(aggregates.get("sample_actions")),
        },
        "probe_summary": build_probe_summary(probes),
        "evidence": r2_keys,
        "source": "top_site_receipt",
    }

    index_row = {
        "domain": domain,
        "canonical_domain": domain,
        "rank": rank,
        "label": label,
        "title": receipt.get("title") or None,
        "favicon_url": aggregates.get("favicon_url") or None,
        "og_image_url": aggregates.get("og_image_url") or None,
        "crawled_at": receipt.get("crawled_at") or None,
        "tags_json": json_text(tags),
        **scores,
        "purchase_boundary": detail["boundaries"]["purchase_boundary"],
        "control_boundary": detail["boundaries"]["control_boundary"],
        "payment_surface": detail["payment"]["payment_surface"],
        "crypto_only": int(detail["flags"]["crypto_only"]),
        "verified_payment_surface": int(detail["flags"]["verified_payment_surface"]),
        **{key: int(value) for key, value in surface_flags.items()},
        "product_count": detail["counts"]["product_count"],
        "variant_count": detail["counts"]["variant_count"],
        "priced_variant_count": detail["counts"]["priced_variant_count"],
        "currency_count": detail["counts"]["currency_count"],
        "offer_count": detail["counts"]["offer_count"],
        "priced_offer_count": detail["counts"]["priced_offer_count"],
        "priced_action_count": detail["counts"]["priced_action_count"],
        "payment_rail_hints_json": json_text(detail["payment"]["rail_hints"]),
        "payment_provider_hints_json": json_text(detail["payment"]["provider_hints"]),
        "payment_methods_json": json_text(detail["payment"]["methods"]),
        "payment_protocols_json": json_text(detail["payment"]["protocols"]),
        "payment_assets_json": json_text(detail["payment"]["assets"]),
        "payment_networks_json": json_text(detail["payment"]["networks"]),
        "payment_flow_types_json": json_text(detail["payment"]["flow_types"]),
        "capability_names_json": json_text(detail["capabilities"]),
        **r2_keys,
    }
    detail["description"] = summarize_index_row(index_row)
    return index_row, detail


def canonical_sort_key(index_row: dict[str, Any], detail_eligible: bool) -> tuple[Any, ...]:
    rank = as_int(index_row.get("rank"))
    return (
        int(detail_eligible),
        int(index_row.get("verified_payment_surface") or 0),
        as_int(index_row.get("score_overall")) or 0,
        as_int(index_row.get("score_payment")) or 0,
        as_int(index_row.get("score_callable")) or 0,
        as_int(index_row.get("score_commerce")) or 0,
        parse_epoch(index_row.get("crawled_at")),
        -(rank if rank is not None else 10**12),
    )


def page_sort_key(item: dict[str, Any]) -> tuple[Any, ...]:
    row = item["index_row"]
    return (
        -(as_int(row.get("score_overall")) or 0),
        row.get("domain") or "",
    )


def latest_crawled_at(rows: list[dict[str, Any]]) -> str | None:
    values = [
        row["index_row"].get("crawled_at")
        for row in rows
        if isinstance(row["index_row"].get("crawled_at"), str)
    ]
    return max(values) if values else None


def build_list_entry(index_row: dict[str, Any], *, list_position: int, source_evidence_dir: Path) -> dict[str, Any]:
    robots_text = read_text(source_evidence_dir / "robots.txt")
    llms_text = read_text(source_evidence_dir / "llms.txt")
    llms_full_text = read_text(source_evidence_dir / "llms-full.txt")
    domain = index_row.get("domain")

    return {
        "domain": domain,
        "canonical_domain": index_row.get("canonical_domain") or domain,
        "rank": index_row.get("rank"),
        "list_position": list_position,
        "label": index_row.get("label") or "no_clear_signal",
        "title": index_row.get("title") or domain,
        "favicon_url": index_row.get("favicon_url"),
        "og_image_url": index_row.get("og_image_url"),
        "description": summarize_index_row(index_row),
        "site_url": f"https://{domain}" if domain else None,
        "robots": document_preview(robots_text),
        "llms": document_preview(llms_text) or document_preview(llms_full_text),
        "crawled_at": index_row.get("crawled_at"),
        "score_overall": index_row.get("score_overall"),
        "score_readable": index_row.get("score_readable"),
        "score_callable": index_row.get("score_callable"),
        "score_commerce": index_row.get("score_commerce"),
        "score_payment": index_row.get("score_payment"),
        "payment_surface": index_row.get("payment_surface"),
        "purchase_boundary": index_row.get("purchase_boundary"),
        "control_boundary": index_row.get("control_boundary"),
        "crypto_only": bool(index_row.get("crypto_only")),
        "verified_payment_surface": bool(index_row.get("verified_payment_surface")),
        "payment_provider_hints": parse_json_list_text(index_row.get("payment_provider_hints_json")),
        "payment_rail_hints": parse_json_list_text(index_row.get("payment_rail_hints_json")),
        "tags": parse_json_list_text(index_row.get("tags_json")),
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, separators=(",", ":"), sort_keys=True), encoding="utf-8")


def write_pretty_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def write_page_files(output_dir: Path, public_rows: list[dict[str, Any]], page_size: int) -> dict[str, Any]:
    total = len(public_rows)
    total_pages = max(1, (total + page_size - 1) // page_size)
    pages_dir = output_dir / public_export_key("pages")

    for page_number in range(1, total_pages + 1):
        start = (page_number - 1) * page_size
        page_items = public_rows[start : start + page_size]
        results = [
            build_list_entry(
                item["index_row"],
                list_position=start + index + 1,
                source_evidence_dir=item["source_evidence_dir"],
            )
            for index, item in enumerate(page_items)
        ]
        write_json(
            pages_dir / f"{page_number:06d}.json",
            {
                "pagination": {
                    "page": page_number,
                    "pageSize": page_size,
                    "total": total,
                    "totalPages": total_pages,
                },
                "sort": "score_overall_desc",
                "results": results,
            },
        )

    return {
        "page_size": page_size,
        "path_prefix": public_export_key("pages"),
        "total": total,
        "total_pages": total_pages,
    }


def write_sitemap_domain_files(output_dir: Path, public_rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(public_rows)
    total_pages = max(1, (total + SITEMAP_PAGE_SIZE - 1) // SITEMAP_PAGE_SIZE)
    sitemap_dir = output_dir / public_export_key("sitemaps")

    for page_number in range(1, total_pages + 1):
        start = (page_number - 1) * SITEMAP_PAGE_SIZE
        domains = [
            item["index_row"]["domain"]
            for item in public_rows[start : start + SITEMAP_PAGE_SIZE]
            if item["index_row"].get("domain")
        ]
        write_json(
            sitemap_dir / f"{page_number:06d}.json",
            {
                "pagination": {
                    "page": page_number,
                    "pageSize": SITEMAP_PAGE_SIZE,
                    "total": total,
                    "totalPages": total_pages,
                },
                "domains": domains,
            },
        )

    return {
        "page_size": SITEMAP_PAGE_SIZE,
        "path_prefix": public_export_key("sitemaps"),
        "total": total,
        "total_pages": total_pages,
    }


def ensure_clean_output(output_dir: Path) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)


def maybe_copy_evidence(source_evidence_dir: Path, output_dir: Path, receipt_detail: dict[str, Any]) -> None:
    evidence = receipt_detail.get("evidence") if isinstance(receipt_detail.get("evidence"), dict) else {}
    proof_key = evidence.get("evidence_r2_key")
    if isinstance(proof_key, str) and proof_key:
        proof_path = output_dir / proof_key
        proof_path.parent.mkdir(parents=True, exist_ok=True)
        proof_payload = {
            "boundaries": receipt_detail.get("boundaries"),
            "payment": receipt_detail.get("payment"),
            "samples": receipt_detail.get("samples"),
            "probe_summary": receipt_detail.get("probe_summary"),
        }
        proof_path.write_text(json.dumps(proof_payload, indent=2, sort_keys=True), encoding="utf-8")

    for artifact_key, file_name in EVIDENCE_FILE_NAMES.items():
        target_key = {
            "robots_txt": "robots_r2_key",
            "llms_txt": "llms_r2_key",
            "llms_full_txt": "llms_full_r2_key",
        }[artifact_key]
        relative_key = evidence.get(target_key)
        if not isinstance(relative_key, str) or not relative_key:
            continue
        source_path = source_evidence_dir / file_name
        if not source_path.exists():
            continue
        target_path = output_dir / relative_key
        target_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source_path, target_path)


def export_public(args: argparse.Namespace) -> int:
    results_dir = Path(args.results_dir)
    receipts_dir = results_dir / "receipts"
    positives_dir = results_dir / "positives"
    evidence_dir = results_dir / "evidence"
    output_dir = Path(args.output_dir)

    if not receipts_dir.exists():
        raise SystemExit(f"receipts directory not found: {receipts_dir}")

    if args.clean:
        ensure_clean_output(output_dir)
    else:
        output_dir.mkdir(parents=True, exist_ok=True)
        for child_name in ("d1", "receipts", "evidence", "explorer", "top-sites", "manifests"):
            child_path = output_dir / child_name
            if child_path.exists():
                shutil.rmtree(child_path)

    manifests_dir = output_dir / "manifests" / "v1"
    manifests_dir.mkdir(parents=True, exist_ok=True)

    positive_domains = {
        path.stem.strip().lower()
        for path in positives_dir.glob("*.json")
        if path.stem.strip()
    }

    canonical_receipts: dict[str, dict[str, Any]] = {}
    raw_receipt_count = 0

    for receipt in iter_receipts(receipts_dir):
        domain = str(receipt.get("domain") or "").strip().lower()
        if not domain:
            continue
        raw_receipt_count += 1

        source_evidence_dir = evidence_dir / domain
        detail_eligible = domain in positive_domains
        index_row, detail = build_public_receipt(
            receipt,
            source_evidence_dir=source_evidence_dir,
            detail_eligible=detail_eligible,
        )
        candidate = {
            "index_row": index_row,
            "detail": detail,
            "detail_eligible": detail_eligible,
            "source_evidence_dir": source_evidence_dir,
            "canonical_key": canonical_sort_key(index_row, detail_eligible),
        }
        existing = canonical_receipts.get(domain)
        if existing is None or candidate["canonical_key"] > existing["canonical_key"]:
            canonical_receipts[domain] = candidate

    canonical_rows = list(canonical_receipts.values())
    public_rows = [
        row
        for row in canonical_rows
        if row["detail_eligible"] and row["index_row"].get("receipt_r2_key")
    ]
    public_rows.sort(key=page_sort_key)
    page_size = max(1, int(args.page_size or DEFAULT_PAGE_SIZE))
    list_manifest = write_page_files(output_dir, public_rows, page_size)
    sitemap_manifest = write_sitemap_domain_files(output_dir, public_rows)

    label_counts: Counter[str] = Counter()
    for item in public_rows:
        index_row = item["index_row"]
        detail = item["detail"]
        label_counts[index_row["label"]] += 1
        relative_key = detail["evidence"]["receipt_r2_key"]
        if isinstance(relative_key, str) and relative_key:
            write_pretty_json(output_dir / relative_key, detail)
            maybe_copy_evidence(item["source_evidence_dir"], output_dir, detail)

    manifest = {
        "version": 1,
        "export_version": PUBLIC_EXPORT_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "sort": "score_overall_desc",
        "raw_receipt_count": raw_receipt_count,
        "total_rows": len(canonical_rows),
        "public_receipt_rows": len(public_rows),
        "latest_crawled_at": latest_crawled_at(public_rows),
        "label_counts": dict(sorted(label_counts.items())),
        "pages": list_manifest,
        "sitemaps": sitemap_manifest,
        "paths": {
            "manifest": public_export_key("manifest.json"),
            "page_prefix": public_export_key("pages"),
            "receipt_prefix": public_export_key("receipts"),
            "evidence_prefix": public_export_key("evidence"),
            "sitemap_prefix": public_export_key("sitemaps"),
            "latest_manifest": "manifests/v1/latest.json",
        },
    }
    write_pretty_json(output_dir / public_export_key("manifest.json"), manifest)
    write_pretty_json(
        manifests_dir / "latest.json",
        {
            "version": 1,
            "export_version": PUBLIC_EXPORT_VERSION,
            "generated_at": manifest["generated_at"],
            "top_sites_manifest": public_export_key("manifest.json"),
            "manifest": "manifests/v1/latest.json",
            "total_rows": manifest["total_rows"],
            "public_receipt_rows": manifest["public_receipt_rows"],
        },
    )
    return 0


def main() -> int:
    return export_public(parse_args())
