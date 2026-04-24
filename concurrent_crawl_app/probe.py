from __future__ import annotations

import json

from .classification import classify_receipt, merge_agent_facts, should_probe_products
from .constants import BASE_PROBES, CART_PROBE, PRODUCTS_PROBE
from .http_client import build_control_fetch, fetch_url
from .models import DomainInput, FetchResponse, ProbeOutcome, ProbeSpec
from .outcomes import build_outcome, merge_ucp_facts
from .validators_content import validate_ucp


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
    fetch = fetch_url(
        url,
        timeout=timeout,
        max_bytes=max_bytes,
        method=method,
        body=body,
        content_type=content_type,
    )
    spec = ProbeSpec(key=key, path=url, validator=validator, max_bytes=max_bytes)
    return build_outcome(spec, fetch)


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


def probe_domain(domain_input: DomainInput, timeout: float, run_token: str):
    domain = domain_input.domain
    outcomes: dict[str, ProbeOutcome] = {}
    control_cache: dict[str, FetchResponse] = {}

    homepage_spec = BASE_PROBES[0]
    homepage_fetch = fetch_url(f"https://{domain}{homepage_spec.path}", timeout=timeout, max_bytes=homepage_spec.max_bytes)
    homepage_outcome = build_outcome(homepage_spec, homepage_fetch)
    outcomes[homepage_spec.key] = homepage_outcome

    root_failed_hard = homepage_fetch.status is None and homepage_fetch.error is not None
    if root_failed_hard:
        for spec in BASE_PROBES[1:]:
            outcomes[spec.key] = ProbeOutcome(
                key=spec.key,
                path=spec.path,
                status="skipped",
                http_status=None,
                content_type=None,
                detail="Skipped after homepage network failure",
            )
        outcomes[PRODUCTS_PROBE.key] = ProbeOutcome(
            key=PRODUCTS_PROBE.key,
            path=PRODUCTS_PROBE.path,
            status="skipped",
            http_status=None,
            content_type=None,
            detail="Skipped after homepage network failure",
        )
        return classify_receipt(domain_input, outcomes)

    for spec in BASE_PROBES[1:]:
        fetch = fetch_url(f"https://{domain}{spec.path}", timeout=timeout, max_bytes=spec.max_bytes)
        control = None
        if spec.control_group and fetch.status == 200:
            control = build_control_fetch(domain, spec.control_group, run_token, timeout, control_cache)
        outcomes[spec.key] = build_outcome(spec, fetch, control)

    root_ucp_outcome = outcomes.get("well_known_ucp")
    if root_ucp_outcome and root_ucp_outcome.status == "valid":
        current_version_url = root_ucp_outcome.facts.get("current_version_url")
        if isinstance(current_version_url, str) and current_version_url and current_version_url != root_ucp_outcome.final_url:
            versioned_fetch = fetch_url(current_version_url, timeout=timeout, max_bytes=BASE_PROBES[6].max_bytes)
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
        ]
        if isinstance(url, str) and url
    ]
    if outcomes.get("openapi_json") and outcomes["openapi_json"].status != "valid" and openapi_candidate_urls:
        for candidate_url in openapi_candidate_urls:
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
        fetch = fetch_url(f"https://{domain}{PRODUCTS_PROBE.path}", timeout=timeout, max_bytes=PRODUCTS_PROBE.max_bytes)
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

    if outcomes[PRODUCTS_PROBE.key].status == "valid":
        fetch = fetch_url(f"https://{domain}{CART_PROBE.path}", timeout=timeout, max_bytes=CART_PROBE.max_bytes)
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

    payment_probe_candidate = next(iter_payment_probe_candidates(outcomes), None)
    if payment_probe_candidate:
        candidate_url = payment_probe_candidate.get("url")
        candidate_method = payment_probe_candidate.get("method")
        candidate_content_type = payment_probe_candidate.get("content_type")
        if isinstance(candidate_url, str) and candidate_url:
            payment_probe_outcome = build_dynamic_probe_outcome(
                key="payment_probe",
                url=candidate_url,
                validator="payment_probe",
                timeout=timeout,
                max_bytes=65_536,
                method=candidate_method if isinstance(candidate_method, str) and candidate_method else "GET",
                body=build_payment_probe_body(payment_probe_candidate),
                content_type=candidate_content_type if isinstance(candidate_content_type, str) and candidate_content_type else "application/json",
            )
            payment_probe_facts = dict(payment_probe_outcome.facts)
            for fact_key in ("source", "title", "amount", "currency"):
                if fact_key in payment_probe_candidate:
                    payment_probe_facts[f"candidate_{fact_key}"] = payment_probe_candidate[fact_key]
            if "body" in payment_probe_candidate:
                payment_probe_facts["candidate_body"] = payment_probe_candidate["body"]
            outcomes["payment_probe"] = ProbeOutcome(
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
        else:
            outcomes["payment_probe"] = ProbeOutcome(
                key="payment_probe",
                path="dynamic",
                status="skipped",
                http_status=None,
                content_type=None,
                detail="Skipped because candidate URL was missing",
            )
    else:
        outcomes["payment_probe"] = ProbeOutcome(
            key="payment_probe",
            path="dynamic",
            status="skipped",
            http_status=None,
            content_type=None,
            detail="Skipped because no payment probe candidate was discovered",
        )

    return classify_receipt(domain_input, outcomes)
