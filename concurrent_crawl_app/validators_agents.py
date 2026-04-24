from __future__ import annotations

import json
from typing import Any

from .helpers import (
    collect_payment_hints,
    derive_x402_discovery_url,
    extract_observed_json_schema_facts,
    extract_template_parameters,
    final_host,
    merge_unique_limited,
    resolve_openapi_path,
    resolve_url,
)
from .http_client import parse_json_body
from .models import FetchResponse
from .validators_support import (
    build_action_sample,
    build_probe_candidate,
    extract_x402_actions,
    merge_action_samples,
    merge_probe_candidates,
)


def first_resolved_url(base_reference: str, *candidates: Any) -> str | None:
    for candidate in candidates:
        resolved = resolve_url(base_reference, candidate)
        if isinstance(resolved, str) and resolved:
            return resolved
    return None


def iter_named_endpoints(value: Any, prefix: str = ""):
    if isinstance(value, str):
        if prefix:
            yield prefix, {"path": value}
        return
    if not isinstance(value, dict):
        return

    raw_path = value.get("path") or value.get("url") or value.get("endpoint") or value.get("href")
    if isinstance(raw_path, str) and raw_path.strip():
        yield prefix, value
        return

    for key, child in value.items():
        child_prefix = f"{prefix}.{key}" if prefix else str(key)
        yield from iter_named_endpoints(child, child_prefix)


def validate_agent(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    if fetch.truncated:
        return False, f"agent response truncated at {fetch.byte_count} bytes", {"truncated": True}

    try:
        payload = parse_json_body(fetch)
    except ValueError as error:
        return False, str(error), {}
    except json.JSONDecodeError as error:
        return False, f"invalid json: {error.msg}", {}

    if not isinstance(payload, dict):
        return False, "agent payload was not an object", {}
    observed_schema_facts = extract_observed_json_schema_facts(payload)

    keys = set(payload.keys())
    expected = {
        "name",
        "description",
        "url",
        "version",
        "protocols",
        "capabilities",
        "tools",
        "server_info",
        "skills",
        "auth",
        "endpoints",
    }
    if not (keys & expected):
        return False, "agent JSON lacked expected agent-like keys", {"top_level_keys": sorted(str(key) for key in keys)[:12]}

    base_reference = fetch.final_url or fetch.requested_url
    top_level_url = resolve_url(base_reference, payload.get("url"))
    api = payload.get("api") if isinstance(payload.get("api"), dict) else {}
    authentication = payload.get("authentication") if isinstance(payload.get("authentication"), dict) else {}
    capabilities = payload.get("capabilities") if isinstance(payload.get("capabilities"), dict) else {}
    discovery = payload.get("discovery") if isinstance(payload.get("discovery"), dict) else {}
    documentation = payload.get("documentation") if isinstance(payload.get("documentation"), dict) else {}
    endpoints = payload.get("endpoints") if isinstance(payload.get("endpoints"), dict) else {}
    links = payload.get("links") if isinstance(payload.get("links"), dict) else {}
    payment = payload.get("payment") if isinstance(payload.get("payment"), dict) else {}
    x402 = payload.get("x402") if isinstance(payload.get("x402"), dict) else {}
    skills = payload.get("skills") if isinstance(payload.get("skills"), list) else []

    api_base_url = resolve_url(base_reference, api.get("base_url"))
    docs_url = first_resolved_url(
        base_reference,
        api.get("docs"),
        discovery.get("docs"),
        payload.get("documentationUrl"),
        documentation.get("quickStart"),
        documentation.get("fullReference"),
        links.get("documentation"),
    )
    openapi_url = first_resolved_url(
        base_reference,
        discovery.get("openapi"),
        api.get("openapi"),
        payload.get("openApiUrl"),
        payload.get("openapiUrl"),
        documentation.get("openApiSpec"),
        links.get("documentation"),
    )
    x402_url = (
        resolve_url(base_reference, discovery.get("x402"))
        or resolve_url(base_reference, x402.get("url"))
        or resolve_url(base_reference, x402.get("discovery"))
    )
    brand_facts_url = resolve_url(base_reference, discovery.get("brand_facts"))
    llms_url = resolve_url(base_reference, discovery.get("llms_txt"))
    x402_urls: list[str] = []
    if x402_url:
        x402_urls = merge_unique_limited(x402_urls, [x402_url], limit=8)

    public_endpoint_urls: list[str] = []
    product_urls: list[str] = []
    docs_urls: list[str] = []
    order_urls: list[str] = []
    register_urls: list[str] = []
    wallet_guides_urls: list[str] = []

    for endpoint_name, endpoint in iter_named_endpoints(endpoints):
        endpoint_name_leaf = endpoint_name.rsplit(".", 1)[-1] if endpoint_name else ""
        raw_path = (
            endpoint.get("path")
            or endpoint.get("url")
            or endpoint.get("endpoint")
            or endpoint.get("href")
        )
        resolved_url = resolve_openapi_path(api_base_url or base_reference, raw_path)
        auth_required = endpoint.get("auth")
        if resolved_url and auth_required is not True:
            public_endpoint_urls = merge_unique_limited(public_endpoint_urls, [resolved_url], limit=12)

        if endpoint_name_leaf == "products" and resolved_url:
            product_urls = merge_unique_limited(product_urls, [resolved_url], limit=6)
        elif endpoint_name_leaf == "docs" and resolved_url:
            docs_urls = merge_unique_limited(docs_urls, [resolved_url], limit=6)
        elif endpoint_name_leaf == "orders" and resolved_url:
            order_urls = merge_unique_limited(order_urls, [resolved_url], limit=6)
        elif endpoint_name_leaf == "register" and resolved_url:
            register_urls = merge_unique_limited(register_urls, [resolved_url], limit=6)
        elif endpoint_name_leaf == "wallet_guides" and resolved_url:
            wallet_guides_urls = merge_unique_limited(wallet_guides_urls, [resolved_url], limit=6)

    payment_protocol = payment.get("protocol") if isinstance(payment.get("protocol"), str) else None
    recommended_client = payment.get("recommended_client") if isinstance(payment.get("recommended_client"), str) else None
    auth_schemes = authentication.get("schemes") if isinstance(authentication.get("schemes"), list) else []
    if not auth_schemes and isinstance(payload.get("auth"), dict):
        auth_payload = payload.get("auth")
        auth_schemes = auth_payload.get("schemes") if isinstance(auth_payload.get("schemes"), list) else []
    payment_network_names: list[str] = []
    payment_chain_ids: list[str] = []
    payment_currency_codes: list[str] = []
    payment_assets: list[str] = []
    sample_actions: list[dict[str, Any]] = []
    probe_candidates: list[dict[str, Any]] = []
    priced_action_count = 0
    raw_networks = payment.get("networks") if isinstance(payment.get("networks"), list) else []
    for network in raw_networks:
        if not isinstance(network, dict):
            continue
        name = network.get("name")
        chain_id = network.get("chain_id")
        currency = network.get("currency")
        asset = network.get("asset")
        if isinstance(name, str) and name.strip():
            payment_network_names = merge_unique_limited(payment_network_names, [name.strip()], limit=12)
        if isinstance(chain_id, str) and chain_id.strip():
            payment_chain_ids = merge_unique_limited(payment_chain_ids, [chain_id.strip()], limit=12)
        if isinstance(currency, str) and currency.strip():
            payment_currency_codes = merge_unique_limited(payment_currency_codes, [currency.strip().upper()[:12]], limit=12)
        if isinstance(asset, str) and asset.strip():
            payment_assets = merge_unique_limited(payment_assets, [asset.strip()], limit=12)

    x402_network = x402.get("network") if isinstance(x402.get("network"), str) else None
    x402_asset = x402.get("asset") if isinstance(x402.get("asset"), str) else None
    x402_facilitator = x402.get("facilitator") if isinstance(x402.get("facilitator"), str) else None
    x402_endpoints = x402.get("endpoints") if isinstance(x402.get("endpoints"), dict) else {}
    x402_signaled = bool(
        (isinstance(payment_protocol, str) and payment_protocol.strip().lower() == "x402")
        or any(isinstance(scheme, str) and scheme.strip().lower() == "x402" for scheme in auth_schemes)
        or bool(x402)
    )
    if x402_network:
        payment_network_names = merge_unique_limited(payment_network_names, [x402_network], limit=12)
    if x402_asset:
        payment_currency_codes = merge_unique_limited(payment_currency_codes, [x402_asset.upper()[:12]], limit=12)
        payment_assets = merge_unique_limited(payment_assets, [x402_asset], limit=12)
    if x402_facilitator:
        facilitator_host = final_host(x402_facilitator)
        if facilitator_host:
            payment_assets = merge_unique_limited(payment_assets, [x402_asset] if x402_asset else [], limit=12)
    if x402_signaled:
        for base_candidate in (top_level_url, api_base_url):
            derived_x402_url = derive_x402_discovery_url(base_candidate)
            if derived_x402_url:
                x402_urls = merge_unique_limited(x402_urls, [derived_x402_url], limit=8)
    if not x402_url and x402_urls:
        x402_url = x402_urls[0]
    (
        x402_resource_urls,
        x402_sample_actions,
        x402_probe_candidates,
        x402_priced_action_count,
        x402_currencies,
    ) = extract_x402_actions(
        base_reference=base_reference,
        endpoints=x402_endpoints,
        source="agent_x402",
    )
    public_endpoint_urls = merge_unique_limited(public_endpoint_urls, x402_resource_urls, limit=12)
    sample_actions = merge_action_samples(sample_actions, x402_sample_actions)
    probe_candidates = merge_probe_candidates(probe_candidates, x402_probe_candidates)
    priced_action_count += x402_priced_action_count
    payment_currency_codes = merge_unique_limited(payment_currency_codes, x402_currencies, limit=12)

    extension_entries = capabilities.get("extensions") if isinstance(capabilities.get("extensions"), list) else []
    for extension in extension_entries:
        if not isinstance(extension, dict):
            continue
        params = extension.get("params") if isinstance(extension.get("params"), dict) else {}
        pricing = params.get("pricing") if isinstance(params.get("pricing"), dict) else {}
        endpoint_prices = pricing.get("endpoints") if isinstance(pricing.get("endpoints"), dict) else {}
        default_currency = params.get("currency") if isinstance(params.get("currency"), str) else None
        default_network = params.get("network") if isinstance(params.get("network"), str) else None
        for raw_path, endpoint_price in endpoint_prices.items():
            if not isinstance(raw_path, str) or not raw_path.strip():
                continue
            endpoint_url = resolve_openapi_path(api_base_url or top_level_url or base_reference, raw_path)
            endpoint_amount = endpoint_price.get("price") if isinstance(endpoint_price, dict) else endpoint_price
            endpoint_description = endpoint_price.get("description") if isinstance(endpoint_price, dict) else None
            endpoint_method = "POST" if "/batch" in raw_path.lower() else "GET"
            sample_actions = merge_action_samples(
                sample_actions,
                [
                    build_action_sample(
                        method=endpoint_method,
                        url=endpoint_url,
                        path=raw_path,
                        title=raw_path.strip().rsplit("/", 1)[-1],
                        description=endpoint_description if isinstance(endpoint_description, str) else None,
                        amount=endpoint_amount,
                        currency=default_currency,
                        network=default_network,
                        source="agent_extension",
                    )
                ],
            )
            if endpoint_amount is not None and str(endpoint_amount).strip():
                priced_action_count += 1
            if isinstance(default_currency, str) and default_currency.strip():
                payment_currency_codes = merge_unique_limited(payment_currency_codes, [default_currency.strip().upper()[:12]], limit=12)
            if isinstance(default_network, str) and default_network.strip():
                payment_network_names = merge_unique_limited(payment_network_names, [default_network.strip()], limit=12)
            candidate = build_probe_candidate(
                url=endpoint_url,
                method=endpoint_method,
                body=endpoint_method == "POST" and {} or None,
                content_type="application/json",
                source="agent_extension",
                title=endpoint_description if isinstance(endpoint_description, str) and endpoint_description.strip() else raw_path,
                amount=endpoint_amount,
                currency=default_currency,
            )
            if candidate:
                template_parameters = extract_template_parameters(raw_path)
                if template_parameters:
                    candidate["template_parameters"] = template_parameters
                probe_candidates = merge_probe_candidates(probe_candidates, [candidate])

    for skill in skills:
        if not isinstance(skill, dict):
            continue
        pricing = skill.get("pricing") if isinstance(skill.get("pricing"), dict) else {}
        amount = pricing.get("amount")
        currency = pricing.get("currency") if isinstance(pricing.get("currency"), str) else None
        if amount is not None and str(amount).strip():
            priced_action_count += 1
        if isinstance(currency, str) and currency.strip():
            payment_currency_codes = merge_unique_limited(payment_currency_codes, [currency.strip().upper()[:12]], limit=12)
        sample_actions = merge_action_samples(
            sample_actions,
            [
                build_action_sample(
                    title=skill.get("name") if isinstance(skill.get("name"), str) else skill.get("id"),
                    description=skill.get("description") if isinstance(skill.get("description"), str) else None,
                    amount=amount,
                    currency=currency,
                    source="agent_skill",
                )
            ],
        )

    payment_hints = collect_payment_hints(payload)

    return True, "Agent-like JSON detected", {
        "top_level_keys": sorted(str(key) for key in keys)[:12],
        "service_url": top_level_url,
        "api_base_url": api_base_url,
        "docs_url": docs_url,
        "openapi_url": openapi_url,
        "x402_url": x402_url,
        "x402_urls": x402_urls,
        "brand_facts_url": brand_facts_url,
        "llms_url": llms_url,
        "public_endpoint_urls": public_endpoint_urls,
        "public_endpoint_count": len(public_endpoint_urls),
        "product_urls": product_urls,
        "docs_urls": docs_urls or ([docs_url] if docs_url else []),
        "order_urls": order_urls,
        "register_urls": register_urls,
        "wallet_guides_urls": wallet_guides_urls,
        "payment_protocol": payment_protocol,
        "payment_network_names": payment_network_names,
        "payment_chain_ids": payment_chain_ids,
        "payment_currency_codes": payment_currency_codes,
        "payment_assets": payment_assets,
        "recommended_client": recommended_client,
        "sample_actions": sample_actions,
        "priced_action_count": priced_action_count,
        "payment_probe_candidates": probe_candidates,
        **observed_schema_facts,
        **payment_hints,
    }


def validate_agents(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    if fetch.truncated:
        return False, f"agents response truncated at {fetch.byte_count} bytes", {"truncated": True}

    try:
        payload = parse_json_body(fetch)
    except ValueError as error:
        return False, str(error), {}
    except json.JSONDecodeError as error:
        return False, f"invalid json: {error.msg}", {}
    observed_schema_facts = extract_observed_json_schema_facts(payload)

    if isinstance(payload, dict):
        keys = set(payload.keys())
        expected = {"agents", "workflows", "items", "results"}
        if not (keys & expected):
            return False, "agents JSON lacked agents/workflows style keys", {"top_level_keys": sorted(str(key) for key in keys)[:12]}
        count = 0
        for key in ("agents", "workflows", "items", "results"):
            value = payload.get(key)
            if isinstance(value, list):
                count += len(value)
        return True, "Agents/workflows JSON detected", {"entry_count": count, **observed_schema_facts}

    if isinstance(payload, list):
        return True, "Agents/workflows list detected", {"entry_count": len(payload), **observed_schema_facts}

    return False, "agents payload was not an object or list", {}
