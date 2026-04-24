from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

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
    receipt_key = f"receipts/v1/{prefix}/{domain}.json"
    evidence_key = f"evidence/v1/{prefix}/{domain}/proof.json"

    robots_key = None
    llms_key = None
    llms_full_key = None

    if (source_evidence_dir / "robots.txt").exists():
        robots_key = f"evidence/v1/{prefix}/{domain}/robots.txt"
    if (source_evidence_dir / "llms.txt").exists():
        llms_key = f"evidence/v1/{prefix}/{domain}/llms.txt"
    llms_full_path = source_evidence_dir / "llms-full.txt"
    if llms_full_path.exists():
        should_export_llms_full = (
            receipt_label in {"machine_payable", "callable_surface"}
            or (rank is not None and rank <= 10_000)
            or llms_full_path.stat().st_size <= 32_768
        )
        if should_export_llms_full:
            llms_full_key = f"evidence/v1/{prefix}/{domain}/llms-full.txt"

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
        "rank": rank,
        "label": label,
        "title": receipt.get("title"),
        "favicon_url": aggregates.get("favicon_url"),
        "og_image_url": aggregates.get("og_image_url"),
        "crawled_at": receipt.get("crawled_at"),
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
    return index_row, detail


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
        for child_name in ("d1", "receipts", "evidence", "manifests"):
            child_path = output_dir / child_name
            if child_path.exists():
                shutil.rmtree(child_path)

    d1_dir = output_dir / "d1"
    receipts_out_dir = output_dir / "receipts" / "v1"
    manifests_dir = output_dir / "manifests" / "v1"
    d1_dir.mkdir(parents=True, exist_ok=True)
    receipts_out_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir.mkdir(parents=True, exist_ok=True)

    positive_domains = {
        path.stem
        for path in positives_dir.glob("*.json")
    }

    latest_receipts: dict[str, dict[str, Any]] = {}
    label_counts: Counter[str] = Counter()
    detail_count = 0

    for receipt in iter_receipts(receipts_dir):
        domain = str(receipt.get("domain") or "").strip().lower()
        if not domain:
            continue
        latest_receipts[domain] = receipt

    csv_path = d1_dir / "site_receipts.csv"
    ndjson_path = d1_dir / "site_receipts.ndjson"
    with (
        csv_path.open("w", newline="", encoding="utf-8") as csv_handle,
        ndjson_path.open("w", encoding="utf-8") as ndjson_handle,
    ):
        writer = csv.DictWriter(csv_handle, fieldnames=INDEX_FIELDS)
        writer.writeheader()
        for domain, receipt in latest_receipts.items():
            source_evidence_dir = evidence_dir / domain
            detail_eligible = domain in positive_domains
            index_row, detail = build_public_receipt(
                receipt,
                source_evidence_dir=source_evidence_dir,
                detail_eligible=detail_eligible,
            )
            label_counts[index_row["label"]] += 1

            writer.writerow({field: index_row.get(field) for field in INDEX_FIELDS})
            ndjson_handle.write(json.dumps(index_row, separators=(",", ":"), sort_keys=True))
            ndjson_handle.write("\n")

            if detail_eligible:
                relative_key = detail["evidence"]["receipt_r2_key"]
                if isinstance(relative_key, str) and relative_key:
                    detail_path = output_dir / relative_key
                    detail_path.parent.mkdir(parents=True, exist_ok=True)
                    detail_path.write_text(
                        json.dumps(detail, indent=2, sort_keys=True),
                        encoding="utf-8",
                    )
                    maybe_copy_evidence(source_evidence_dir, output_dir, detail)
                    detail_count += 1

    manifest = {
        "version": 1,
        "receipt_count": len(latest_receipts),
        "detail_receipt_count": detail_count,
        "label_counts": dict(sorted(label_counts.items())),
        "paths": {
            "site_receipts_csv": str(csv_path.relative_to(output_dir)),
            "site_receipts_ndjson": str(ndjson_path.relative_to(output_dir)),
            "receipt_prefix": "receipts/v1/",
            "manifest": "manifests/v1/latest.json",
        },
    }
    (manifests_dir / "latest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    return 0


def main() -> int:
    return export_public(parse_args())
