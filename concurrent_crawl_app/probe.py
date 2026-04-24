from __future__ import annotations

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
) -> ProbeOutcome:
    fetch = fetch_url(url, timeout=timeout, max_bytes=max_bytes)
    spec = ProbeSpec(key=key, path=url, validator=validator, max_bytes=max_bytes)
    return build_outcome(spec, fetch)


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

    return classify_receipt(domain_input, outcomes)
