from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

from .helpers import (
    final_host,
    is_cross_host_redirect,
    is_login_like_host,
    normalize_content_type,
)
from .models import CrawlReceipt, FetchResponse, ProbeOutcome, ProbeSpec
from .validation import VALIDATORS

VALIDATOR_HTTP_STATUSES: dict[str, set[int]] = {
    "commerce": {200, 402},
    "payment_probe": {200, 400, 401, 402, 403, 405, 409, 422, 429, 503},
    "x402": {200, 402},
}

ALLOW_EMPTY_BODY_VALIDATORS = {"payment_probe", "x402"}


def extract_retry_after_seconds(fetch: FetchResponse) -> int | None:
    raw_value = fetch.headers.get("retry-after")
    if not isinstance(raw_value, str):
        return None

    cleaned = raw_value.strip()
    if not cleaned:
        return None

    if cleaned.isdigit():
        try:
            return max(int(cleaned), 0)
        except ValueError:
            return None

    try:
        retry_after_at = parsedate_to_datetime(cleaned)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None
    if retry_after_at.tzinfo is None:
        return None
    try:
        seconds = int(retry_after_at.timestamp() - datetime.now(timezone.utc).timestamp())
    except (OverflowError, OSError):
        return None
    return max(seconds, 0)


def merge_ucp_facts(primary: dict[str, Any], enriched: dict[str, Any]) -> dict[str, Any]:
    merged = dict(primary)
    for key, value in enriched.items():
        if key not in merged:
            merged[key] = value
            continue
        if key in {"payment_handler_count", "service_count", "capability_count"}:
            primary_value = merged.get(key)
            if (not primary_value and value) or (isinstance(primary_value, int) and isinstance(value, int) and value > primary_value):
                merged[key] = value
            continue
        if key in {
            "payment_handler_names",
            "payment_handler_ids",
            "payment_endpoint_samples",
            "capability_names",
            "shopping_mcp_endpoints",
            "payment_provider_hints",
            "payment_rail_hints",
            "payment_endpoint_hosts",
        } or key.startswith("observed_"):
            primary_list = merged.get(key) if isinstance(merged.get(key), list) else []
            new_list = value if isinstance(value, list) else []
            merged[key] = sorted(dict.fromkeys(primary_list + new_list))[:12]
            continue
        if not merged.get(key) and value:
            merged[key] = value
    return merged


def build_outcome(
    spec: ProbeSpec,
    fetch: FetchResponse,
    control: FetchResponse | None = None,
) -> ProbeOutcome:
    if spec.key != "homepage" and is_cross_host_redirect(fetch) and is_login_like_host(final_host(fetch.final_url)):
        return ProbeOutcome(
            key=spec.key,
            path=spec.path,
            status="gated",
            http_status=fetch.status,
            content_type=fetch.content_type,
            final_url=fetch.final_url,
            byte_count=fetch.byte_count,
            body_sha256=fetch.body_sha256,
            detail="Cross-host redirect to login/auth host",
        )

    if fetch.status == 404:
        return ProbeOutcome(
            key=spec.key,
            path=spec.path,
            status="missing",
            http_status=fetch.status,
            content_type=fetch.content_type,
            final_url=fetch.final_url,
            byte_count=fetch.byte_count,
            body_sha256=fetch.body_sha256,
            detail="404 not found",
        )

    if fetch.status == 429:
        retry_after_seconds = extract_retry_after_seconds(fetch)
        facts: dict[str, Any] = {}
        detail = "HTTP 429"
        if retry_after_seconds is not None:
            facts["retry_after_seconds"] = retry_after_seconds
            detail = f"HTTP 429 (Retry-After: {retry_after_seconds}s)"
        return ProbeOutcome(
            key=spec.key,
            path=spec.path,
            status="rate_limited",
            http_status=fetch.status,
            content_type=fetch.content_type,
            final_url=fetch.final_url,
            byte_count=fetch.byte_count,
            body_sha256=fetch.body_sha256,
            detail=detail,
            facts=facts,
        )

    if fetch.status is None and fetch.error:
        return ProbeOutcome(
            key=spec.key,
            path=spec.path,
            status="error",
            http_status=None,
            content_type=fetch.content_type,
            final_url=fetch.final_url,
            byte_count=fetch.byte_count,
            body_sha256=fetch.body_sha256,
            detail=fetch.error,
        )

    allowed_statuses = VALIDATOR_HTTP_STATUSES.get(spec.validator, {200})
    if fetch.status not in allowed_statuses:
        return ProbeOutcome(
            key=spec.key,
            path=spec.path,
            status="http_error",
            http_status=fetch.status,
            content_type=fetch.content_type,
            final_url=fetch.final_url,
            byte_count=fetch.byte_count,
            body_sha256=fetch.body_sha256,
            detail=f"HTTP {fetch.status}",
        )

    if spec.key != "homepage" and fetch.byte_count == 0 and spec.validator not in ALLOW_EMPTY_BODY_VALIDATORS:
        return ProbeOutcome(
            key=spec.key,
            path=spec.path,
            status="invalid",
            http_status=fetch.status,
            content_type=fetch.content_type,
            final_url=fetch.final_url,
            byte_count=fetch.byte_count,
            body_sha256=fetch.body_sha256,
            detail="Empty response body",
        )

    if control:
        from .http_client import responses_match

        if responses_match(fetch, control):
            return ProbeOutcome(
                key=spec.key,
                path=spec.path,
                status="fallback",
                http_status=fetch.status,
                content_type=fetch.content_type,
                final_url=fetch.final_url,
                byte_count=fetch.byte_count,
                body_sha256=fetch.body_sha256,
                detail="Response matched control-path fallback",
            )

    validator = VALIDATORS[spec.validator]
    is_valid, detail, facts = validator(fetch)
    return ProbeOutcome(
        key=spec.key,
        path=spec.path,
        status="valid" if is_valid else "invalid",
        http_status=fetch.status,
        content_type=fetch.content_type,
        final_url=fetch.final_url,
        byte_count=fetch.byte_count,
        body_sha256=fetch.body_sha256,
        detail=detail,
        facts=facts,
    )


def serialize_probe_outcome(outcome: ProbeOutcome) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "path": outcome.path,
        "status": outcome.status,
        "http_status": outcome.http_status,
        "content_type": normalize_content_type(outcome.content_type),
        "final_url": outcome.final_url,
        "byte_count": outcome.byte_count,
        "body_sha256": outcome.body_sha256,
        "detail": outcome.detail,
    }
    if outcome.facts:
        payload["facts"] = outcome.facts
    return payload


def serialize_receipt(receipt: CrawlReceipt) -> dict[str, Any]:
    return {
        "domain": receipt.domain,
        "rank": receipt.rank,
        "row_index": receipt.row_index,
        "crawled_at": receipt.crawled_at,
        "label": receipt.label,
        "tags": receipt.tags,
        "title": receipt.title,
        "aggregates": receipt.aggregates,
        "probes": {key: serialize_probe_outcome(value) for key, value in receipt.probes.items()},
    }
