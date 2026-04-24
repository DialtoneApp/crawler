from __future__ import annotations

import json
import time
from dataclasses import replace
from typing import Any

from .classification import classify_receipt, merge_agent_facts, should_probe_products
from .constants import BASE_PROBES, CART_PROBE, PRODUCTS_PROBE
from .helpers import extract_template_parameters, fill_template_parameters, parse_price
from .http_client import build_control_fetch, fetch_url, parse_json_body
from .models import DomainInput, FetchResponse, ProbeOutcome, ProbeSpec
from .outcomes import build_outcome, merge_ucp_facts
from .ucp_catalog import probe_ucp_catalog_products
from .validators_content import validate_ucp

STORED_ARTIFACT_KEYS = frozenset({"robots_txt", "llms_txt", "llms_full_txt"})
RETRYABLE_FETCH_ERRORS = frozenset({"timeout", "url_error:timed out"})


def finalize_receipt(
    domain_input: DomainInput,
    outcomes: dict[str, ProbeOutcome],
    artifacts: dict[str, bytes],
    *,
    started_at: float,
    wall_clock_limit: float | None,
    wall_clock_timeout: bool = False,
):
    receipt = classify_receipt(domain_input, outcomes)
    if wall_clock_timeout:
        aggregates = dict(receipt.aggregates)
        aggregates["domain_wall_clock_timeout"] = True
        aggregates["domain_wall_clock_elapsed_seconds"] = round(max(time.monotonic() - started_at, 0.0), 3)
        if wall_clock_limit and wall_clock_limit > 0:
            aggregates["domain_wall_clock_limit_seconds"] = round(wall_clock_limit, 3)
        receipt = replace(receipt, aggregates=aggregates)
    return replace(receipt, artifacts=artifacts)


def wall_clock_deadline_reached(started_at: float, wall_clock_limit: float | None) -> bool:
    return bool(wall_clock_limit and wall_clock_limit > 0 and (time.monotonic() - started_at) >= wall_clock_limit)


def maybe_abort_for_wall_clock(
    domain_input: DomainInput,
    outcomes: dict[str, ProbeOutcome],
    artifacts: dict[str, bytes],
    *,
    started_at: float,
    wall_clock_limit: float | None,
    detail: str,
):
    if not wall_clock_deadline_reached(started_at, wall_clock_limit):
        return None

    if "payment_probe" not in outcomes:
        outcomes["payment_probe"] = ProbeOutcome(
            key="payment_probe",
            path="dynamic",
            status="skipped",
            http_status=None,
            content_type=None,
            detail=detail,
        )

    return finalize_receipt(
        domain_input,
        outcomes,
        artifacts,
        started_at=started_at,
        wall_clock_limit=wall_clock_limit,
        wall_clock_timeout=True,
    )


def build_dynamic_probe_outcome(
    *,
    key: str,
    url: str,
    validator: str,
    timeout: float,
    max_bytes: int,
    method: str = "GET",
    body: bytes | None = None,
    content_type: str | None = None,
) -> ProbeOutcome:
    fetch = fetch_with_retry(
        url,
        timeout=timeout,
        max_bytes=max_bytes,
        method=method,
        body=body,
        content_type=content_type,
    )
    spec = ProbeSpec(key=key, path=url, validator=validator, max_bytes=max_bytes)
    return build_outcome(spec, fetch)


def fetch_with_retry(
    url: str,
    *,
    timeout: float,
    max_bytes: int,
    method: str = "GET",
    body: bytes | None = None,
    content_type: str | None = None,
) -> FetchResponse:
    fetch = fetch_url(
        url,
        timeout=timeout,
        max_bytes=max_bytes,
        method=method,
        body=body,
        content_type=content_type,
    )
    if fetch.error in RETRYABLE_FETCH_ERRORS and method.upper() in {"GET", "HEAD"}:
        fetch = fetch_url(
            url,
            timeout=timeout,
            max_bytes=max_bytes,
            method=method,
            body=body,
            content_type=content_type,
        )
    return fetch


def build_payment_probe_body(candidate: dict[str, object]) -> bytes | None:
    body = candidate.get("body")
    if body is None:
        return None
    if isinstance(body, bytes):
        return body
    if isinstance(body, str):
        return body.encode("utf-8")
    return json.dumps(body, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def score_payment_probe_candidate(candidate: dict[str, object]) -> int:
    score = 0
    url = candidate.get("url")
    title = candidate.get("title")
    source = candidate.get("source")
    text = " ".join(
        value.lower()
        for value in (url, title)
        if isinstance(value, str) and value
    )
    negative_markers = ("status", "read", "messages", "list", "health", "stats")
    high_intent_markers = ("buy", "order", "checkout", "purchase", "send", "report")
    medium_intent_markers = ("search",)
    low_intent_markers = ("scrape", "crawl")
    if any(marker in text for marker in high_intent_markers):
        score += 50
    if any(marker in text for marker in medium_intent_markers):
        score += 40
    if any(marker in text for marker in low_intent_markers):
        score += 20
    if any(marker in text for marker in negative_markers):
        score -= 20
    if candidate.get("amount") is not None:
        score += 15
    body = candidate.get("body")
    if isinstance(body, dict) and body:
        score += 10
    elif body is not None:
        score += 5
    if isinstance(source, str):
        if source == "openapi":
            score += 10
        elif source == "agent_x402":
            score += 8
        elif source == "x402":
            score += 6
    return score


def iter_payment_probe_candidates(outcomes: dict[str, ProbeOutcome]):
    ranked_candidates: list[dict[str, object]] = []
    for key in (
        "api_openapi_json",
        "openapi_json",
        "x402_json",
        "x402_well_known",
        "remote_x402",
        "well_known_agent_json",
        "root_agent_json",
        "well_known_agent_card",
    ):
        outcome = outcomes.get(key)
        if not outcome or outcome.status != "valid":
            continue
        outcome_candidates = outcome.facts.get("payment_probe_candidates")
        if not isinstance(outcome_candidates, list):
            continue
        for candidate in outcome_candidates:
            if isinstance(candidate, dict):
                ranked_candidates.append(candidate)
    yield from sorted(ranked_candidates, key=score_payment_probe_candidate, reverse=True)


def iter_collection_items(payload: Any, *, depth: int = 0):
    if depth > 4:
        return
    if isinstance(payload, list):
        for item in payload[:25]:
            if isinstance(item, dict):
                yield item
            elif isinstance(item, (list, dict)):
                yield from iter_collection_items(item, depth=depth + 1)
        return
    if isinstance(payload, dict):
        for value in payload.values():
            if isinstance(value, dict):
                yield from iter_collection_items(value, depth=depth + 1)
            elif isinstance(value, list):
                for item in value[:25]:
                    if isinstance(item, dict):
                        yield item
                    elif isinstance(item, (list, dict)):
                        yield from iter_collection_items(item, depth=depth + 1)


def candidate_keys_for_parameter(parameter: str) -> list[str]:
    lowered = parameter.strip().lower().replace("-", "_")
    if not lowered:
        return []
    results = [lowered]
    if lowered == "address":
        results.extend(
            [
                "btc_address",
                "btcaddress",
                "stx_address",
                "stxaddress",
                "wallet_address",
                "wallet",
                "recipient_address",
            ]
        )
    if lowered == "slug":
        results.extend(["handle"])
    if lowered.endswith("_id"):
        stem = lowered[:-3]
        if stem:
            results.extend([f"{stem}_id", stem, f"{stem}id"])
    elif lowered.endswith("id") and lowered != "id":
        stem = lowered[:-2].rstrip("_")
        if stem:
            results.extend([lowered, f"{stem}_id", stem])
    elif lowered != "id":
        results.extend([f"{lowered}_id", f"{lowered}id"])
    results.append("id")
    deduped: list[str] = []
    for value in results:
        if value and value not in deduped:
            deduped.append(value)
    return deduped


def default_value_for_parameter(parameter: str) -> Any | None:
    lowered = parameter.strip().lower().replace("-", "_")
    if lowered == "domain":
        return "example.com"
    if lowered in {"slug", "handle"}:
        return "sample-name"
    return None


def extract_parameter_value(item: dict[str, Any], parameter: str) -> Any:
    lowered_item = {
        str(key).strip().lower().replace("-", "_"): value
        for key, value in item.items()
        if isinstance(key, str)
    }
    for candidate_key in candidate_keys_for_parameter(parameter):
        value = lowered_item.get(candidate_key)
        if value is None:
            continue
        if isinstance(value, str):
            cleaned = value.strip()
            if cleaned:
                return cleaned
        elif isinstance(value, (int, float)):
            return value
    return None


def extract_sample_title(item: dict[str, Any]) -> str | None:
    for key in ("title", "name", "label", "product_title", "artifact_title"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()[:120]
    return None


def extract_sample_currency(item: dict[str, Any]) -> str | None:
    for key in ("currency", "currency_code", "currencyCode", "priceCurrency"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip().upper()[:16]
    pricing = item.get("pricing")
    if isinstance(pricing, dict):
        for key in ("currency", "currency_code", "currencyCode", "asset", "name"):
            value = pricing.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip().upper()[:16]
    return None


def extract_sample_amount(item: dict[str, Any]) -> str | None:
    for key in ("price", "amount", "min_price", "max_price", "cost"):
        parsed = parse_price(item.get(key))
        if parsed is not None:
            if parsed.is_integer():
                return str(int(parsed))
            return str(parsed)
    pricing = item.get("pricing")
    if isinstance(pricing, dict):
        for key in ("amount", "price"):
            parsed = parse_price(pricing.get(key))
            if parsed is not None:
                if parsed.is_integer():
                    return str(int(parsed))
                return str(parsed)
    return None


def extract_price_payload_details(fetch: FetchResponse) -> tuple[str | None, str | None]:
    try:
        payload = parse_json_body(fetch)
    except (ValueError, json.JSONDecodeError):
        return None, None
    if not isinstance(payload, dict):
        return None, None

    amount = extract_sample_amount(payload)
    currency = extract_sample_currency(payload)
    return amount, currency


def resolve_template_candidate(
    candidate: dict[str, object],
    timeout: float,
    *,
    fallback_discovery_urls: list[str] | None = None,
) -> tuple[dict[str, object] | None, str | None]:
    candidate_url = candidate.get("url")
    if not isinstance(candidate_url, str) or not candidate_url:
        return None, "Skipped payment probe candidate without a URL"

    template_parameters = candidate.get("template_parameters")
    if isinstance(template_parameters, list):
        parameter_names = [str(value).strip() for value in template_parameters if str(value).strip()]
    else:
        parameter_names = extract_template_parameters(candidate_url)
    if not parameter_names:
        return dict(candidate), None

    discovery_urls: list[str] = []
    primary_discovery_url = candidate.get("discovery_url")
    if isinstance(primary_discovery_url, str) and primary_discovery_url:
        discovery_urls.append(primary_discovery_url)
    candidate_discovery_urls = candidate.get("discovery_urls")
    if isinstance(candidate_discovery_urls, list):
        for value in candidate_discovery_urls:
            if isinstance(value, str) and value and value not in discovery_urls:
                discovery_urls.append(value)
    for value in fallback_discovery_urls or []:
        if isinstance(value, str) and value and value not in discovery_urls:
            discovery_urls.append(value)
    if not discovery_urls:
        default_replacements: dict[str, Any] = {}
        for parameter_name in parameter_names:
            default_value = default_value_for_parameter(parameter_name)
            if default_value is None:
                return None, "Skipped templated payment probe candidate without a discovery URL"
            default_replacements[parameter_name] = default_value
        resolved_url = fill_template_parameters(candidate_url, default_replacements)
        if not isinstance(resolved_url, str) or not resolved_url or extract_template_parameters(resolved_url):
            return None, "Skipped templated payment probe candidate because the resource URL could not be resolved"
        resolved_candidate = dict(candidate)
        resolved_candidate["url"] = resolved_url
        resolved_candidate["resolved_from_template"] = True
        resolved_candidate["resolved_parameters"] = default_replacements
        return resolved_candidate, None

    selected_item: dict[str, Any] | None = None
    replacements: dict[str, Any] = {}
    resolved_discovery_url: str | None = None
    resolution_detail = "Skipped templated payment probe candidate because the discovery response did not expose concrete resource IDs"
    for discovery_url in discovery_urls:
        discovery_fetch = fetch_url(discovery_url, timeout=timeout, max_bytes=131_072)
        if discovery_fetch.status != 200:
            resolution_detail = f"Skipped templated payment probe candidate after discovery fetch returned HTTP {discovery_fetch.status}"
            continue

        try:
            payload = parse_json_body(discovery_fetch)
        except (ValueError, json.JSONDecodeError):
            resolution_detail = "Skipped templated payment probe candidate because the discovery URL did not return JSON"
            continue

        for item in iter_collection_items(payload):
            candidate_replacements: dict[str, Any] = {}
            for parameter_name in parameter_names:
                parameter_value = extract_parameter_value(item, parameter_name)
                if parameter_value is None:
                    break
                candidate_replacements[parameter_name] = parameter_value
            else:
                selected_item = item
                replacements = candidate_replacements
                resolved_discovery_url = discovery_url
                break
        if selected_item and replacements:
            break

    if not selected_item or not replacements:
        return None, resolution_detail

    resolved_url = fill_template_parameters(candidate_url, replacements)
    if not isinstance(resolved_url, str) or not resolved_url or extract_template_parameters(resolved_url):
        return None, "Skipped templated payment probe candidate because the resource URL could not be resolved"

    resolved_candidate = dict(candidate)
    resolved_candidate["url"] = resolved_url
    resolved_candidate["resolved_from_template"] = True
    if resolved_discovery_url:
        resolved_candidate["discovery_url"] = resolved_discovery_url
    resolved_candidate["resolved_parameters"] = replacements

    if not resolved_candidate.get("title"):
        title = extract_sample_title(selected_item)
        if title:
            resolved_candidate["title"] = title
    sample_title = extract_sample_title(selected_item)
    if sample_title:
        resolved_candidate["sample_title"] = sample_title
    if resolved_candidate.get("amount") is None:
        amount = extract_sample_amount(selected_item)
        if amount is not None:
            resolved_candidate["amount"] = amount
    if not resolved_candidate.get("currency"):
        currency = extract_sample_currency(selected_item)
        if currency:
            resolved_candidate["currency"] = currency

    price_lookup_url = candidate.get("price_lookup_url")
    if isinstance(price_lookup_url, str) and price_lookup_url:
        resolved_price_lookup_url = fill_template_parameters(price_lookup_url, replacements)
        if isinstance(resolved_price_lookup_url, str) and resolved_price_lookup_url and not extract_template_parameters(resolved_price_lookup_url):
            price_fetch = fetch_url(resolved_price_lookup_url, timeout=timeout, max_bytes=16_384)
            if price_fetch.status == 200:
                amount, currency = extract_price_payload_details(price_fetch)
                if amount is not None:
                    resolved_candidate["amount"] = amount
                if currency and not resolved_candidate.get("currency"):
                    resolved_candidate["currency"] = currency
                resolved_candidate["price_lookup_url"] = resolved_price_lookup_url

    return resolved_candidate, None


def iter_remote_x402_candidate_urls(
    domain: str,
    outcomes: dict[str, ProbeOutcome],
    agent_facts: dict[str, Any],
):
    seen: set[str] = set()
    local_urls = {
        f"https://{domain}/.well-known/x402",
        f"https://{domain}/.well-known/x402.json",
    }

    def append_values(target: list[str], value: object) -> list[str]:
        if isinstance(value, str) and value.strip():
            target.append(value.strip())
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.strip():
                    target.append(item.strip())
        return target

    candidates: list[str] = []
    for key in ("llms_txt", "llms_full_txt"):
        outcome = outcomes.get(key)
        if outcome and outcome.status == "valid":
            candidates = append_values(candidates, outcome.facts.get("x402_urls"))

    candidates = append_values(candidates, agent_facts.get("x402_url"))
    candidates = append_values(candidates, agent_facts.get("x402_urls"))

    for candidate_url in candidates:
        if candidate_url in seen or candidate_url in local_urls:
            continue
        seen.add(candidate_url)
        yield candidate_url


def probe_domain(
    domain_input: DomainInput,
    timeout: float,
    run_token: str,
    wall_clock_limit: float | None = None,
):
    domain = domain_input.domain
    outcomes: dict[str, ProbeOutcome] = {}
    control_cache: dict[str, FetchResponse] = {}
    artifacts: dict[str, bytes] = {}
    started_at = time.monotonic()

    homepage_spec = next(spec for spec in BASE_PROBES if spec.key == "homepage")
    other_specs = [spec for spec in BASE_PROBES if spec.key != "homepage"]

    # Crawl cheap crawl/AI/discovery docs first so a slow homepage does not zero out the receipt.
    for spec in other_specs:
        timeout_receipt = maybe_abort_for_wall_clock(
            domain_input,
            outcomes,
            artifacts,
            started_at=started_at,
            wall_clock_limit=wall_clock_limit,
            detail="Skipped after per-domain wall-clock limit was reached before base probe fetches completed",
        )
        if timeout_receipt is not None:
            return timeout_receipt
        fetch = fetch_with_retry(
            f"https://{domain}{spec.path}",
            timeout=timeout,
            max_bytes=spec.max_bytes,
        )
        control = None
        if spec.control_group and fetch.status == 200:
            control = build_control_fetch(domain, spec.control_group, run_token, timeout, control_cache)
        outcome = build_outcome(spec, fetch, control)
        outcomes[spec.key] = outcome
        if (
            spec.key in STORED_ARTIFACT_KEYS
            and outcome.status == "valid"
            and not fetch.truncated
            and fetch.body
        ):
            artifacts[spec.key] = fetch.body

    timeout_receipt = maybe_abort_for_wall_clock(
        domain_input,
        outcomes,
        artifacts,
        started_at=started_at,
        wall_clock_limit=wall_clock_limit,
        detail="Skipped after per-domain wall-clock limit was reached before homepage fetch",
    )
    if timeout_receipt is not None:
        outcomes["homepage"] = ProbeOutcome(
            key="homepage",
            path=homepage_spec.path,
            status="skipped",
            http_status=None,
            content_type=None,
            detail="Skipped after per-domain wall-clock limit was reached before homepage fetch",
        )
        return finalize_receipt(
            domain_input,
            outcomes,
            artifacts,
            started_at=started_at,
            wall_clock_limit=wall_clock_limit,
            wall_clock_timeout=True,
        )

    homepage_fetch = fetch_with_retry(
        f"https://{domain}{homepage_spec.path}",
        timeout=timeout,
        max_bytes=homepage_spec.max_bytes,
    )
    homepage_outcome = build_outcome(homepage_spec, homepage_fetch)
    outcomes[homepage_spec.key] = homepage_outcome

    root_ucp_outcome = outcomes.get("well_known_ucp")
    if root_ucp_outcome and root_ucp_outcome.status == "valid":
        timeout_receipt = maybe_abort_for_wall_clock(
            domain_input,
            outcomes,
            artifacts,
            started_at=started_at,
            wall_clock_limit=wall_clock_limit,
            detail="Skipped after per-domain wall-clock limit was reached during UCP enrichment",
        )
        if timeout_receipt is not None:
            return timeout_receipt
        current_version_url = root_ucp_outcome.facts.get("current_version_url")
        if isinstance(current_version_url, str) and current_version_url and current_version_url != root_ucp_outcome.final_url:
            versioned_fetch = fetch_with_retry(
                current_version_url,
                timeout=timeout,
                max_bytes=BASE_PROBES[6].max_bytes,
            )
            versioned_valid, _, versioned_facts = validate_ucp(versioned_fetch)
            if versioned_valid:
                outcomes["well_known_ucp"] = ProbeOutcome(
                    key=root_ucp_outcome.key,
                    path=root_ucp_outcome.path,
                    status=root_ucp_outcome.status,
                    http_status=root_ucp_outcome.http_status,
                    content_type=root_ucp_outcome.content_type,
                    final_url=root_ucp_outcome.final_url,
                    byte_count=root_ucp_outcome.byte_count,
                    body_sha256=root_ucp_outcome.body_sha256,
                    detail=f"{root_ucp_outcome.detail}; enriched from versioned UCP document",
                    facts=merge_ucp_facts(root_ucp_outcome.facts, versioned_facts),
                )

    agent_facts = merge_agent_facts(outcomes)
    openapi_candidate_urls = [
        url
        for url in [
            agent_facts.get("openapi_url"),
            *(
                homepage_outcome.facts.get("service_desc_urls")
                if isinstance(homepage_outcome.facts.get("service_desc_urls"), list)
                else []
            ),
        ]
        if isinstance(url, str) and url
    ]
    if outcomes.get("openapi_json") and outcomes["openapi_json"].status != "valid" and openapi_candidate_urls:
        for candidate_url in openapi_candidate_urls:
            timeout_receipt = maybe_abort_for_wall_clock(
                domain_input,
                outcomes,
                artifacts,
                started_at=started_at,
                wall_clock_limit=wall_clock_limit,
                detail="Skipped after per-domain wall-clock limit was reached during OpenAPI discovery",
            )
            if timeout_receipt is not None:
                return timeout_receipt
            if candidate_url == outcomes["openapi_json"].final_url or candidate_url == f"https://{domain}/openapi.json":
                continue
            api_openapi_outcome = build_dynamic_probe_outcome(
                key="api_openapi_json",
                url=candidate_url,
                validator="openapi",
                timeout=timeout,
                max_bytes=BASE_PROBES[11].max_bytes,
            )
            outcomes["api_openapi_json"] = api_openapi_outcome
            if api_openapi_outcome.status == "valid":
                break

    local_x402_outcome = next(
        (
            outcomes[key]
            for key in ("x402_json", "x402_well_known")
            if key in outcomes and outcomes[key].status == "valid"
        ),
        None,
    )
    if local_x402_outcome is None:
        for candidate_url in iter_remote_x402_candidate_urls(domain, outcomes, agent_facts):
            timeout_receipt = maybe_abort_for_wall_clock(
                domain_input,
                outcomes,
                artifacts,
                started_at=started_at,
                wall_clock_limit=wall_clock_limit,
                detail="Skipped after per-domain wall-clock limit was reached during remote x402 discovery",
            )
            if timeout_receipt is not None:
                return timeout_receipt
            remote_x402_outcome = build_dynamic_probe_outcome(
                key="remote_x402",
                url=candidate_url,
                validator="x402",
                timeout=timeout,
                max_bytes=BASE_PROBES[12].max_bytes,
            )
            outcomes["remote_x402"] = remote_x402_outcome
            if remote_x402_outcome.status == "valid":
                break

    api_product_candidate_urls = [
        url
        for url in (
            agent_facts.get("product_urls")
            if isinstance(agent_facts.get("product_urls"), list)
            else []
        )
        if isinstance(url, str) and url
    ]

    if should_probe_products(homepage_fetch, homepage_outcome, outcomes):
        timeout_receipt = maybe_abort_for_wall_clock(
            domain_input,
            outcomes,
            artifacts,
            started_at=started_at,
            wall_clock_limit=wall_clock_limit,
            detail="Skipped after per-domain wall-clock limit was reached before product catalog probe",
        )
        if timeout_receipt is not None:
            return timeout_receipt
        fetch = fetch_with_retry(
            f"https://{domain}{PRODUCTS_PROBE.path}",
            timeout=timeout,
            max_bytes=PRODUCTS_PROBE.max_bytes,
        )
        control = None
        if PRODUCTS_PROBE.control_group and fetch.status == 200:
            control = build_control_fetch(domain, PRODUCTS_PROBE.control_group, run_token, timeout, control_cache)
        outcomes[PRODUCTS_PROBE.key] = build_outcome(PRODUCTS_PROBE, fetch, control)
    else:
        outcomes[PRODUCTS_PROBE.key] = ProbeOutcome(
            key=PRODUCTS_PROBE.key,
            path=PRODUCTS_PROBE.path,
            status="skipped",
            http_status=None,
            content_type=None,
            detail="Skipped because homepage/UCP did not suggest a product catalog",
        )

    if outcomes[PRODUCTS_PROBE.key].status != "valid" and api_product_candidate_urls:
        for candidate_url in api_product_candidate_urls:
            timeout_receipt = maybe_abort_for_wall_clock(
                domain_input,
                outcomes,
                artifacts,
                started_at=started_at,
                wall_clock_limit=wall_clock_limit,
                detail="Skipped after per-domain wall-clock limit was reached during API product discovery",
            )
            if timeout_receipt is not None:
                return timeout_receipt
            if candidate_url == f"https://{domain}{PRODUCTS_PROBE.path}":
                continue
            api_products_outcome = build_dynamic_probe_outcome(
                key="api_products",
                url=candidate_url,
                validator="products",
                timeout=timeout,
                max_bytes=PRODUCTS_PROBE.max_bytes,
            )
            outcomes["api_products"] = api_products_outcome
            if api_products_outcome.status == "valid":
                break

    current_ucp_outcome = outcomes.get("well_known_ucp")
    if (
        outcomes[PRODUCTS_PROBE.key].status != "valid"
        and outcomes.get("api_products", ProbeOutcome("", "", "", None, None)).status != "valid"
        and current_ucp_outcome
        and current_ucp_outcome.status == "valid"
    ):
        timeout_receipt = maybe_abort_for_wall_clock(
            domain_input,
            outcomes,
            artifacts,
            started_at=started_at,
            wall_clock_limit=wall_clock_limit,
            detail="Skipped after per-domain wall-clock limit was reached during UCP catalog enrichment",
        )
        if timeout_receipt is not None:
            return timeout_receipt
        ucp_products_outcome = probe_ucp_catalog_products(
            domain=domain,
            homepage_title=homepage_outcome.facts.get("title") if homepage_outcome and isinstance(homepage_outcome.facts.get("title"), str) else None,
            ucp_facts=current_ucp_outcome.facts,
            timeout=timeout,
        )
        if ucp_products_outcome:
            outcomes["api_products"] = ucp_products_outcome

    if (
        outcomes[PRODUCTS_PROBE.key].status == "valid"
        or outcomes.get("api_products", ProbeOutcome("", "", "", None, None)).status == "valid"
    ):
        timeout_receipt = maybe_abort_for_wall_clock(
            domain_input,
            outcomes,
            artifacts,
            started_at=started_at,
            wall_clock_limit=wall_clock_limit,
            detail="Skipped after per-domain wall-clock limit was reached before cart probe",
        )
        if timeout_receipt is not None:
            return timeout_receipt
        fetch = fetch_with_retry(
            f"https://{domain}{CART_PROBE.path}",
            timeout=timeout,
            max_bytes=CART_PROBE.max_bytes,
        )
        control = None
        if CART_PROBE.control_group and fetch.status == 200:
            control = build_control_fetch(domain, CART_PROBE.control_group, run_token, timeout, control_cache)
        outcomes[CART_PROBE.key] = build_outcome(CART_PROBE, fetch, control)
    else:
        outcomes[CART_PROBE.key] = ProbeOutcome(
            key=CART_PROBE.key,
            path=CART_PROBE.path,
            status="skipped",
            http_status=None,
            content_type=None,
            detail="Skipped because no valid public catalog was detected",
        )

    payment_probe_skip_detail = "Skipped because no payment probe candidate was discovered"
    fallback_discovery_urls: list[str] = []
    for fact_key in ("public_endpoint_urls", "product_urls", "order_urls", "register_urls"):
        values = agent_facts.get(fact_key)
        if isinstance(values, list):
            for value in values:
                if isinstance(value, str) and value and value not in fallback_discovery_urls:
                    fallback_discovery_urls.append(value)
    best_payment_probe_outcome: ProbeOutcome | None = None
    for raw_candidate in iter_payment_probe_candidates(outcomes):
        timeout_receipt = maybe_abort_for_wall_clock(
            domain_input,
            outcomes,
            artifacts,
            started_at=started_at,
            wall_clock_limit=wall_clock_limit,
            detail="Skipped after per-domain wall-clock limit was reached during payment probe resolution",
        )
        if timeout_receipt is not None:
            return timeout_receipt
        resolved_candidate, resolution_detail = resolve_template_candidate(
            raw_candidate,
            timeout,
            fallback_discovery_urls=fallback_discovery_urls,
        )
        if resolved_candidate is None:
            if resolution_detail:
                payment_probe_skip_detail = resolution_detail
            continue

        candidate_url = resolved_candidate.get("url")
        candidate_method = resolved_candidate.get("method")
        candidate_content_type = resolved_candidate.get("content_type")
        if isinstance(candidate_url, str) and candidate_url:
            payment_probe_outcome = build_dynamic_probe_outcome(
                key="payment_probe",
                url=candidate_url,
                validator="payment_probe",
                timeout=timeout,
                max_bytes=65_536,
                method=candidate_method if isinstance(candidate_method, str) and candidate_method else "GET",
                body=build_payment_probe_body(resolved_candidate),
                content_type=candidate_content_type if isinstance(candidate_content_type, str) and candidate_content_type else "application/json",
            )
            payment_probe_facts = dict(payment_probe_outcome.facts)
            for fact_key in (
                "source",
                "title",
                "amount",
                "currency",
                "sample_title",
                "discovery_url",
                "price_lookup_url",
                "resolved_from_template",
                "resolved_parameters",
            ):
                if fact_key in resolved_candidate:
                    payment_probe_facts[f"candidate_{fact_key}"] = resolved_candidate[fact_key]
            if "body" in resolved_candidate:
                payment_probe_facts["candidate_body"] = resolved_candidate["body"]
            candidate_outcome = ProbeOutcome(
                key=payment_probe_outcome.key,
                path=payment_probe_outcome.path,
                status=payment_probe_outcome.status,
                http_status=payment_probe_outcome.http_status,
                content_type=payment_probe_outcome.content_type,
                final_url=payment_probe_outcome.final_url,
                byte_count=payment_probe_outcome.byte_count,
                body_sha256=payment_probe_outcome.body_sha256,
                detail=payment_probe_outcome.detail,
                facts=payment_probe_facts,
            )
            if best_payment_probe_outcome is None:
                best_payment_probe_outcome = candidate_outcome
            if candidate_outcome.facts.get("probe_result") in {"payment_challenge", "success_without_payment"}:
                best_payment_probe_outcome = candidate_outcome
                break
        else:
            if best_payment_probe_outcome is None:
                best_payment_probe_outcome = ProbeOutcome(
                    key="payment_probe",
                    path="dynamic",
                    status="skipped",
                    http_status=None,
                    content_type=None,
                    detail="Skipped because candidate URL was missing",
                )
    if best_payment_probe_outcome is not None:
        outcomes["payment_probe"] = best_payment_probe_outcome
    else:
        outcomes["payment_probe"] = ProbeOutcome(
                key="payment_probe",
                path="dynamic",
                status="skipped",
                http_status=None,
                content_type=None,
                detail=payment_probe_skip_detail,
            )

    return finalize_receipt(
        domain_input,
        outcomes,
        artifacts,
        started_at=started_at,
        wall_clock_limit=wall_clock_limit,
    )
