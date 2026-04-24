from __future__ import annotations

import json
from typing import Any

from .helpers import (
    collect_payment_hints,
    decode_body,
    final_host,
    flatten_strings,
    has_placeholder_value,
    is_json_content_type,
    is_login_handoff_body,
    merge_unique_limited,
    normalize_status_value,
    parse_price,
    resolve_url,
)
from .http_client import parse_json_body
from .models import FetchResponse


def merge_action_samples(
    existing: list[dict[str, Any]],
    values: list[dict[str, Any]],
    *,
    limit: int = 6,
) -> list[dict[str, Any]]:
    merged = list(existing)
    seen = {
        (
            item.get("method"),
            item.get("url") or item.get("path"),
            item.get("title"),
        )
        for item in merged
        if isinstance(item, dict)
    }
    for value in values:
        if not isinstance(value, dict):
            continue
        key = (
            value.get("method"),
            value.get("url") or value.get("path"),
            value.get("title"),
        )
        if key in seen:
            continue
        merged.append(value)
        seen.add(key)
        if len(merged) >= limit:
            break
    return merged[:limit]


def merge_probe_candidates(
    existing: list[dict[str, Any]],
    values: list[dict[str, Any]],
    *,
    limit: int = 6,
) -> list[dict[str, Any]]:
    merged = list(existing)
    seen = {
        (
            item.get("method"),
            item.get("url"),
        )
        for item in merged
        if isinstance(item, dict)
    }
    for value in values:
        if not isinstance(value, dict):
            continue
        key = (
            value.get("method"),
            value.get("url"),
        )
        if key in seen:
            continue
        merged.append(value)
        seen.add(key)
        if len(merged) >= limit:
            break
    return merged[:limit]


def build_action_sample(
    *,
    method: str | None = None,
    url: str | None = None,
    path: str | None = None,
    title: str | None = None,
    description: str | None = None,
    amount: Any = None,
    currency: Any = None,
    network: Any = None,
    source: str | None = None,
) -> dict[str, Any]:
    sample: dict[str, Any] = {}
    if isinstance(method, str) and method.strip():
        sample["method"] = method.strip().upper()
    if isinstance(url, str) and url.strip():
        sample["url"] = url.strip()
    elif isinstance(path, str) and path.strip():
        sample["path"] = path.strip()
    if isinstance(title, str) and title.strip():
        sample["title"] = title.strip()[:120]
    if isinstance(description, str) and description.strip():
        sample["description"] = description.strip()[:240]
    if amount is not None and str(amount).strip():
        sample["amount"] = str(amount).strip()
    if isinstance(currency, str) and currency.strip():
        sample["currency"] = currency.strip().upper()[:16]
    if isinstance(network, str) and network.strip():
        sample["network"] = network.strip()[:48]
    if isinstance(source, str) and source.strip():
        sample["source"] = source.strip()
    return sample


def sample_value_for_field(field_name: str | None) -> Any:
    lowered = (field_name or "").lower()
    if lowered in {"q", "query", "search", "keyword"}:
        return "openai"
    if lowered in {"url", "uri", "link", "website"}:
        return "https://example.com"
    if lowered in {"prompt", "question", "message", "text"}:
        return "test"
    if lowered == "subject":
        return "Test subject"
    if lowered in {"replyto", "reply_to", "email"}:
        return "hello@example.com"
    if lowered in {"subdomain", "username", "handle", "slug"}:
        return "sample-name"
    if lowered in {"to", "emails", "recipients"}:
        return ["hello@example.com"]
    if lowered == "html":
        return "<p>Test message</p>"
    if lowered in {"jsonrpc"}:
        return "2.0"
    if lowered == "method":
        return "eth_blockNumber"
    if lowered == "id":
        return 1
    if lowered == "params":
        return []
    if lowered in {"maxpages", "max_results", "limit"}:
        return 1
    return None


def resolve_local_ref(payload: dict[str, Any], ref: str) -> Any:
    if not isinstance(ref, str) or not ref.startswith("#/"):
        return None
    current: Any = payload
    for raw_part in ref[2:].split("/"):
        part = raw_part.replace("~1", "/").replace("~0", "~")
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def sample_json_from_schema(
    schema: Any,
    root: dict[str, Any],
    *,
    field_name: str | None = None,
    depth: int = 0,
) -> Any:
    if depth > 6:
        return sample_value_for_field(field_name)
    if not isinstance(schema, dict):
        return sample_value_for_field(field_name)

    if "$ref" in schema:
        resolved = resolve_local_ref(root, schema["$ref"])
        if resolved is not None:
            return sample_json_from_schema(resolved, root, field_name=field_name, depth=depth + 1)

    for direct_key in ("example", "default", "const"):
        if direct_key in schema:
            return schema[direct_key]

    examples = schema.get("examples")
    if isinstance(examples, dict):
        for example in examples.values():
            if isinstance(example, dict) and "value" in example:
                return example["value"]
            if example is not None:
                return example
    if isinstance(examples, list) and examples:
        return examples[0]

    enum_values = schema.get("enum")
    if isinstance(enum_values, list) and enum_values:
        return enum_values[0]

    for keyword in ("oneOf", "anyOf", "allOf"):
        options = schema.get(keyword)
        if isinstance(options, list) and options:
            if keyword == "allOf":
                merged: dict[str, Any] = {}
                for option in options:
                    value = sample_json_from_schema(option, root, field_name=field_name, depth=depth + 1)
                    if isinstance(value, dict):
                        merged.update(value)
                if merged:
                    return merged
            return sample_json_from_schema(options[0], root, field_name=field_name, depth=depth + 1)

    schema_type = schema.get("type")
    if not schema_type:
        if "properties" in schema:
            schema_type = "object"
        elif "items" in schema:
            schema_type = "array"

    if schema_type == "object":
        properties = schema.get("properties") if isinstance(schema.get("properties"), dict) else {}
        required = schema.get("required") if isinstance(schema.get("required"), list) else []
        property_names = list(dict.fromkeys(required + list(properties.keys())))[:8]
        sample: dict[str, Any] = {}
        for property_name in property_names:
            property_schema = properties.get(property_name, {})
            value = sample_json_from_schema(property_schema, root, field_name=str(property_name), depth=depth + 1)
            if value is not None:
                sample[str(property_name)] = value
        fallback = sample_value_for_field(field_name)
        if not sample and isinstance(fallback, dict):
            return fallback
        return sample or {}

    if schema_type == "array":
        item_schema = schema.get("items")
        item_value = sample_json_from_schema(item_schema, root, field_name=field_name, depth=depth + 1)
        return [] if item_value is None else [item_value]

    if schema_type in {"integer", "number"}:
        return 1

    if schema_type == "boolean":
        return True

    fallback = sample_value_for_field(field_name)
    if fallback is not None:
        return fallback
    if schema.get("format") == "email":
        return "hello@example.com"
    if schema.get("format") in {"uri", "url"}:
        return "https://example.com"
    if schema.get("format") == "date-time":
        return "2026-01-01T00:00:00Z"
    return "string"


def sample_request_from_operation(operation: dict[str, Any], root: dict[str, Any]) -> tuple[str | None, Any]:
    request_body = operation.get("requestBody")
    if not isinstance(request_body, dict):
        return None, None

    if "$ref" in request_body:
        resolved = resolve_local_ref(root, request_body["$ref"])
        if isinstance(resolved, dict):
            request_body = resolved

    content = request_body.get("content") if isinstance(request_body.get("content"), dict) else {}
    preferred_media_type = None
    preferred_payload: dict[str, Any] | None = None
    for media_type, media_payload in content.items():
        if not isinstance(media_payload, dict):
            continue
        if preferred_payload is None:
            preferred_media_type = media_type
            preferred_payload = media_payload
        if media_type == "application/json":
            preferred_media_type = media_type
            preferred_payload = media_payload
            break

    if not preferred_payload or not isinstance(preferred_media_type, str):
        return None, None

    if "example" in preferred_payload:
        return preferred_media_type, preferred_payload["example"]

    examples = preferred_payload.get("examples")
    if isinstance(examples, dict):
        for example in examples.values():
            if isinstance(example, dict) and "value" in example:
                return preferred_media_type, example["value"]

    schema = preferred_payload.get("schema")
    return preferred_media_type, sample_json_from_schema(schema, root)


def heuristic_sample_body(candidate: str | None, method: str) -> Any:
    if method.upper() not in {"POST", "PUT", "PATCH"}:
        return None
    lowered = (candidate or "").lower()
    if "serp" in lowered or "/search" in lowered:
        return {"q": "openai"}
    if "scrape" in lowered:
        return {"url": "https://example.com"}
    if "crawl" in lowered:
        return {"url": "https://example.com", "maxPages": 1}
    if "report" in lowered or "analysis" in lowered or "chat" in lowered:
        return {"prompt": "test"}
    if "subdomain" in lowered and "buy" in lowered:
        return {"subdomain": "sample-name"}
    if lowered.endswith("/send") or "/send" in lowered:
        return {"to": ["hello@example.com"], "subject": "Test subject", "text": "Test message"}
    return {}


def build_probe_candidate(
    *,
    url: str | None,
    method: str | None,
    body: Any,
    content_type: str | None,
    source: str,
    title: str | None = None,
    amount: Any = None,
    currency: Any = None,
) -> dict[str, Any] | None:
    if not isinstance(url, str) or not url.strip():
        return None
    candidate: dict[str, Any] = {
        "url": url.strip(),
        "method": (method or "GET").strip().upper(),
        "source": source,
    }
    if body is not None:
        candidate["body"] = body
    if isinstance(content_type, str) and content_type.strip():
        candidate["content_type"] = content_type.strip()
    if isinstance(title, str) and title.strip():
        candidate["title"] = title.strip()[:120]
    if amount is not None and str(amount).strip():
        candidate["amount"] = str(amount).strip()
    if isinstance(currency, str) and currency.strip():
        candidate["currency"] = currency.strip().upper()[:16]
    return candidate


def extract_x402_actions(
    *,
    base_reference: str,
    services: list[Any] | None = None,
    endpoints: dict[str, Any] | None = None,
    source: str,
) -> tuple[list[str], list[dict[str, Any]], list[dict[str, Any]], int, list[str]]:
    resource_urls: list[str] = []
    sample_actions: list[dict[str, Any]] = []
    probe_candidates: list[dict[str, Any]] = []
    priced_action_count = 0
    currencies: list[str] = []

    for service in services or []:
        if not isinstance(service, dict):
            continue
        raw_url = service.get("url") or service.get("resource") or service.get("endpoint") or service.get("path")
        resolved_url = resolve_url(base_reference, raw_url)
        method = service.get("method") if isinstance(service.get("method"), str) else "POST"
        pricing = service.get("pricing") if isinstance(service.get("pricing"), dict) else {}
        amount = pricing.get("amount") or service.get("price")
        currency = pricing.get("asset") or pricing.get("currency") or service.get("currency")
        network = pricing.get("network") or service.get("network")
        title = service.get("name") if isinstance(service.get("name"), str) else raw_url
        description = service.get("description") if isinstance(service.get("description"), str) else None
        if resolved_url:
            resource_urls = merge_unique_limited(resource_urls, [resolved_url], limit=12)
        if amount is not None and str(amount).strip():
            priced_action_count += 1
        if isinstance(currency, str) and currency.strip():
            currencies = merge_unique_limited(currencies, [currency.strip().upper()[:16]], limit=12)
        sample_actions = merge_action_samples(
            sample_actions,
            [
                build_action_sample(
                    method=method,
                    url=resolved_url,
                    path=raw_url if isinstance(raw_url, str) else None,
                    title=title if isinstance(title, str) else None,
                    description=description,
                    amount=amount,
                    currency=currency if isinstance(currency, str) else None,
                    network=network if isinstance(network, str) else None,
                    source=source,
                )
            ],
        )
        candidate = build_probe_candidate(
            url=resolved_url,
            method=method,
            body=heuristic_sample_body(resolved_url or str(raw_url), method),
            content_type="application/json",
            source=source,
            title=title if isinstance(title, str) else None,
            amount=amount,
            currency=currency if isinstance(currency, str) else None,
        )
        if candidate:
            probe_candidates = merge_probe_candidates(probe_candidates, [candidate])

    for raw_path, endpoint in (endpoints or {}).items():
        if not isinstance(endpoint, dict):
            continue
        resolved_url = resolve_url(base_reference, raw_path)
        method = endpoint.get("method") if isinstance(endpoint.get("method"), str) else "POST"
        amount = endpoint.get("price") or endpoint.get("amount")
        currency = endpoint.get("currency") if isinstance(endpoint.get("currency"), str) else None
        description = endpoint.get("description") if isinstance(endpoint.get("description"), str) else None
        title = endpoint.get("name") if isinstance(endpoint.get("name"), str) else raw_path
        if resolved_url:
            resource_urls = merge_unique_limited(resource_urls, [resolved_url], limit=12)
        if amount is not None and str(amount).strip():
            priced_action_count += 1
        if isinstance(currency, str) and currency.strip():
            currencies = merge_unique_limited(currencies, [currency.strip().upper()[:16]], limit=12)
        sample_actions = merge_action_samples(
            sample_actions,
            [
                build_action_sample(
                    method=method,
                    url=resolved_url,
                    path=raw_path,
                    title=title if isinstance(title, str) else None,
                    description=description,
                    amount=amount,
                    currency=currency,
                    source=source,
                )
            ],
        )
        candidate = build_probe_candidate(
            url=resolved_url,
            method=method,
            body=heuristic_sample_body(resolved_url or raw_path, method),
            content_type="application/json",
            source=source,
            title=title if isinstance(title, str) else None,
            amount=amount,
            currency=currency,
        )
        if candidate:
            probe_candidates = merge_probe_candidates(probe_candidates, [candidate])

    return resource_urls, sample_actions, probe_candidates, priced_action_count, currencies


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
    api = payload.get("api") if isinstance(payload.get("api"), dict) else {}
    discovery = payload.get("discovery") if isinstance(payload.get("discovery"), dict) else {}
    endpoints = payload.get("endpoints") if isinstance(payload.get("endpoints"), dict) else {}
    payment = payload.get("payment") if isinstance(payload.get("payment"), dict) else {}
    x402 = payload.get("x402") if isinstance(payload.get("x402"), dict) else {}
    skills = payload.get("skills") if isinstance(payload.get("skills"), list) else []

    api_base_url = resolve_url(base_reference, api.get("base_url"))
    docs_url = resolve_url(base_reference, api.get("docs")) or resolve_url(base_reference, discovery.get("docs"))
    openapi_url = resolve_url(base_reference, discovery.get("openapi")) or resolve_url(base_reference, api.get("openapi"))
    x402_url = resolve_url(base_reference, discovery.get("x402"))
    brand_facts_url = resolve_url(base_reference, discovery.get("brand_facts"))
    llms_url = resolve_url(base_reference, discovery.get("llms_txt"))

    public_endpoint_urls: list[str] = []
    product_urls: list[str] = []
    docs_urls: list[str] = []
    order_urls: list[str] = []
    register_urls: list[str] = []
    wallet_guides_urls: list[str] = []

    for endpoint_name, endpoint in endpoints.items():
        if not isinstance(endpoint, dict):
            continue
        raw_path = endpoint.get("path")
        resolved_url = resolve_url(api_base_url or base_reference, raw_path)
        auth_required = endpoint.get("auth")
        if resolved_url and auth_required is False:
            public_endpoint_urls = merge_unique_limited(public_endpoint_urls, [resolved_url], limit=12)

        if endpoint_name == "products" and resolved_url:
            product_urls = merge_unique_limited(product_urls, [resolved_url], limit=6)
        elif endpoint_name == "docs" and resolved_url:
            docs_urls = merge_unique_limited(docs_urls, [resolved_url], limit=6)
        elif endpoint_name == "orders" and resolved_url:
            order_urls = merge_unique_limited(order_urls, [resolved_url], limit=6)
        elif endpoint_name == "register" and resolved_url:
            register_urls = merge_unique_limited(register_urls, [resolved_url], limit=6)
        elif endpoint_name == "wallet_guides" and resolved_url:
            wallet_guides_urls = merge_unique_limited(wallet_guides_urls, [resolved_url], limit=6)

    payment_protocol = payment.get("protocol") if isinstance(payment.get("protocol"), str) else None
    recommended_client = payment.get("recommended_client") if isinstance(payment.get("recommended_client"), str) else None
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
    if x402_network:
        payment_network_names = merge_unique_limited(payment_network_names, [x402_network], limit=12)
    if x402_asset:
        payment_currency_codes = merge_unique_limited(payment_currency_codes, [x402_asset.upper()[:12]], limit=12)
        payment_assets = merge_unique_limited(payment_assets, [x402_asset], limit=12)
    if x402_facilitator:
        facilitator_host = final_host(x402_facilitator)
        if facilitator_host:
            payment_assets = merge_unique_limited(payment_assets, [x402_asset] if x402_asset else [], limit=12)
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
        "api_base_url": api_base_url,
        "docs_url": docs_url,
        "openapi_url": openapi_url,
        "x402_url": x402_url,
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
        return True, "Agents/workflows JSON detected", {"entry_count": count}

    if isinstance(payload, list):
        return True, "Agents/workflows list detected", {"entry_count": len(payload)}

    return False, "agents payload was not an object or list", {}


def validate_openapi(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    if fetch.truncated:
        return False, f"OpenAPI response truncated at {fetch.byte_count} bytes", {"truncated": True}

    try:
        payload = parse_json_body(fetch)
    except ValueError as error:
        return False, str(error), {}
    except json.JSONDecodeError as error:
        return False, f"invalid json: {error.msg}", {}

    if not isinstance(payload, dict):
        return False, "OpenAPI payload was not an object", {}

    version = payload.get("openapi") or payload.get("swagger")
    paths = payload.get("paths")
    if not version or not isinstance(paths, dict):
        return False, "OpenAPI payload missing version or paths object", {}

    auth_schemes = []
    components = payload.get("components")
    if isinstance(components, dict):
        security_schemes = components.get("securitySchemes")
        if isinstance(security_schemes, dict):
            auth_schemes.extend(sorted(str(key) for key in security_schemes.keys()))

    security_definitions = payload.get("securityDefinitions")
    if isinstance(security_definitions, dict):
        auth_schemes.extend(sorted(str(key) for key in security_definitions.keys()))

    payment_required_operation_count = 0
    payment_signature_header_count = 0
    payment_required_paths: list[str] = []
    sample_actions: list[dict[str, Any]] = []
    probe_candidates: list[dict[str, Any]] = []
    commerce_path_count = 0
    servers = payload.get("servers") if isinstance(payload.get("servers"), list) else []
    base_reference = fetch.final_url or fetch.requested_url
    for server in servers:
        if isinstance(server, dict) and isinstance(server.get("url"), str) and server.get("url").strip():
            base_reference = server["url"].strip()
            break

    for path_name, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        path_like = str(path_name).lower()
        path_has_commerce_hint = any(token in path_like for token in ("commerce", "checkout", "purchase", "buy", "billing", "subscription"))
        if path_has_commerce_hint:
            commerce_path_count += 1

        path_level_parameters = path_item.get("parameters") if isinstance(path_item.get("parameters"), list) else []
        for method_name in ("get", "post", "put", "patch", "delete", "options", "head"):
            operation = path_item.get(method_name)
            if not isinstance(operation, dict):
                continue

            parameters = list(path_level_parameters)
            if isinstance(operation.get("parameters"), list):
                parameters.extend(operation["parameters"])

            for parameter in parameters:
                if not isinstance(parameter, dict):
                    continue
                name = parameter.get("name")
                location = parameter.get("in")
                if isinstance(name, str) and isinstance(location, str) and location.lower() == "header" and name.lower() == "payment-signature":
                    payment_signature_header_count += 1
                    break

            responses = operation.get("responses") if isinstance(operation.get("responses"), dict) else {}
            if "402" in responses or 402 in responses:
                payment_required_operation_count += 1
                payment_required_paths = merge_unique_limited(
                    payment_required_paths,
                    [f"{method_name.upper()} {path_name}"],
                    limit=6,
                )
                resolved_url = resolve_url(base_reference, path_name)
                summary = operation.get("summary") if isinstance(operation.get("summary"), str) else operation.get("operationId")
                sample_actions = merge_action_samples(
                    sample_actions,
                    [
                        build_action_sample(
                            method=method_name,
                            url=resolved_url,
                            path=path_name,
                            title=summary if isinstance(summary, str) else path_name,
                            description=operation.get("description") if isinstance(operation.get("description"), str) else None,
                            source="openapi",
                        )
                    ],
                )
                sample_content_type, sample_body = sample_request_from_operation(operation, payload)
                if sample_body is None:
                    sample_body = heuristic_sample_body(resolved_url or str(path_name), method_name.upper())
                candidate = build_probe_candidate(
                    url=resolved_url,
                    method=method_name,
                    body=sample_body,
                    content_type=sample_content_type or "application/json",
                    source="openapi",
                    title=summary if isinstance(summary, str) else path_name,
                )
                if candidate:
                    probe_candidates = merge_probe_candidates(probe_candidates, [candidate])

    return True, "OpenAPI document detected", {
        "openapi_version": version,
        "path_count": len(paths),
        "auth_schemes": sorted(set(auth_schemes))[:12],
        "commerce_path_count": commerce_path_count,
        "payment_required_operation_count": payment_required_operation_count,
        "payment_required_paths": payment_required_paths,
        "payment_signature_header_count": payment_signature_header_count,
        "sample_actions": sample_actions,
        "payment_probe_candidates": probe_candidates,
    }


def validate_x402(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    if fetch.truncated:
        return False, f"x402 response truncated at {fetch.byte_count} bytes", {"truncated": True}

    text = decode_body(fetch.body).strip()
    payment_required_header = fetch.headers.get("payment-required") or fetch.headers.get("x-payment-required")
    www_authenticate_header = fetch.headers.get("www-authenticate")
    if not text:
        if payment_required_header or www_authenticate_header:
            return True, "x402 payment challenge detected from headers", {
                "header_challenge_present": True,
                "payment_required_header_present": bool(payment_required_header),
                "www_authenticate_present": bool(www_authenticate_header),
            }
        return False, "x402 document was empty", {}

    lower_text = text.lower()
    if is_login_handoff_body(text):
        return False, "x402 document looked like a login handoff page", {}
    if is_json_content_type(fetch.content_type) or text[:1] in "[{":
        try:
            payload = parse_json_body(fetch)
        except ValueError as error:
            return False, str(error), {}
        except json.JSONDecodeError as error:
            return False, f"invalid json: {error.msg}", {}

        if isinstance(payload, dict):
            keys = set(str(key) for key in payload.keys())
            expected = {"x402", "accepts", "payment", "payment_required", "network", "resource", "price", "currencies"}
            if "x402" not in lower_text and not (keys & expected):
                return False, "x402 JSON lacked x402/payment-like keys", {"top_level_keys": sorted(keys)[:12]}
            accepts = payload.get("accepts") if isinstance(payload.get("accepts"), list) else []
            resources = payload.get("resources") if isinstance(payload.get("resources"), list) else []
            if isinstance(payload.get("resource"), dict):
                resources = [payload["resource"], *resources]
            note = payload.get("note") if isinstance(payload.get("note"), str) else ""
            lower_note = note.lower()
            probe_markers = (
                "does not accept",
                "canonical x402 probe",
                "always returns 402",
                "probe",
                "example",
                "inspect the payment-required envelope",
            )
            if not accepts and not resources and any(marker in lower_note for marker in probe_markers):
                return False, "x402 document looked like a probe/example, not a live payment surface", {
                    "top_level_keys": sorted(keys)[:12],
                    "probe_only": True,
                }
            base_reference = fetch.final_url or fetch.requested_url
            resource_urls, sample_actions, probe_candidates, priced_action_count, priced_action_currencies = extract_x402_actions(
                base_reference=base_reference,
                services=payload.get("services") if isinstance(payload.get("services"), list) else [],
                endpoints=payload.get("endpoints") if isinstance(payload.get("endpoints"), dict) else {},
                source="x402",
            )
            for resource in resources:
                if isinstance(resource, str) and resource.strip():
                    resource_urls = merge_unique_limited(resource_urls, [resource.strip()], limit=12)
                    sample_actions = merge_action_samples(
                        sample_actions,
                        [
                            build_action_sample(
                                method="POST",
                                url=resource.strip(),
                                title=resource.strip().rsplit("/", 1)[-1],
                                source="x402",
                            )
                        ],
                    )
                    candidate = build_probe_candidate(
                        url=resource.strip(),
                        method="POST",
                        body=heuristic_sample_body(resource.strip(), "POST"),
                        content_type="application/json",
                        source="x402",
                    )
                    if candidate:
                        probe_candidates = merge_probe_candidates(probe_candidates, [candidate])
                elif isinstance(resource, dict):
                    candidate_url = None
                    for candidate_key in ("url", "resource", "endpoint"):
                        candidate_value = resource.get(candidate_key)
                        if isinstance(candidate_value, str) and candidate_value.strip():
                            candidate_url = candidate_value.strip()
                            resource_urls = merge_unique_limited(resource_urls, [candidate_url], limit=12)
                            break
                    if candidate_url:
                        sample_actions = merge_action_samples(
                            sample_actions,
                            [
                                build_action_sample(
                                    method=resource.get("method") if isinstance(resource.get("method"), str) else "POST",
                                    url=candidate_url,
                                    title=resource.get("description") if isinstance(resource.get("description"), str) else candidate_url.rsplit("/", 1)[-1],
                                    description=resource.get("description") if isinstance(resource.get("description"), str) else None,
                                    source="x402",
                                )
                            ],
                        )
            accept_networks: list[str] = []
            accept_assets: list[str] = []
            accept_currencies: list[str] = []
            for accept in accepts:
                if not isinstance(accept, dict):
                    continue
                network = accept.get("network")
                asset = accept.get("asset")
                extra = accept.get("extra") if isinstance(accept.get("extra"), dict) else {}
                name = extra.get("name")
                if isinstance(network, str) and network.strip():
                    accept_networks = merge_unique_limited(accept_networks, [network.strip()], limit=12)
                if isinstance(asset, str) and asset.strip():
                    accept_assets = merge_unique_limited(accept_assets, [asset.strip()], limit=12)
                if isinstance(name, str) and name.strip():
                    accept_currencies = merge_unique_limited(accept_currencies, [name.strip().upper()[:16]], limit=12)
            payment_hints = collect_payment_hints(payload)
            return True, "x402-like JSON detected", {
                "top_level_keys": sorted(keys)[:12],
                "resource_urls": resource_urls,
                "resource_url_count": len(resource_urls),
                "accept_count": len(accepts),
                "accept_networks": accept_networks,
                "accept_assets": accept_assets,
                "accept_currencies": accept_currencies or priced_action_currencies,
                "instructions_char_count": len(payload.get("instructions", "")) if isinstance(payload.get("instructions"), str) else 0,
                "sample_actions": sample_actions,
                "priced_action_count": priced_action_count,
                "payment_probe_candidates": probe_candidates,
                "payment_required_header_present": bool(payment_required_header),
                "www_authenticate_present": bool(www_authenticate_header),
                **payment_hints,
            }

        if isinstance(payload, list) and payload:
            return True, "x402-like JSON list detected", {"item_count": len(payload)}

        return False, "x402 payload was empty or unsupported", {}

    if "<html" in lower_text or "<body" in lower_text or "<script" in lower_text:
        return False, "x402 document looked like HTML fallback", {}
    if "x402" in lower_text or "payment required" in lower_text:
        return True, "x402-like text detected", {
            "char_count": len(text),
            "payment_required_header_present": bool(payment_required_header),
            "www_authenticate_present": bool(www_authenticate_header),
        }
    return False, "x402 text lacked x402/payment language", {"char_count": len(text)}


def validate_payment_probe(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    payment_required_header = fetch.headers.get("payment-required") or fetch.headers.get("x-payment-required")
    www_authenticate_header = fetch.headers.get("www-authenticate")
    text = decode_body(fetch.body).strip() if fetch.body else ""
    facts: dict[str, Any] = {
        "probe_method": fetch.request_method,
        "probe_url": fetch.requested_url,
        "payment_required_header_present": bool(payment_required_header),
        "www_authenticate_present": bool(www_authenticate_header),
    }

    if fetch.status == 402:
        facts["probe_result"] = "payment_challenge"
        if text and (is_json_content_type(fetch.content_type) or text[:1] in "[{"):
            try:
                payload = parse_json_body(fetch)
            except (ValueError, json.JSONDecodeError):
                payload = None
            if isinstance(payload, dict):
                x402_valid, _, x402_facts = validate_x402(fetch)
                if x402_valid:
                    facts.update({
                        key: value
                        for key, value in x402_facts.items()
                        if key not in {"payment_probe_candidates"}
                    })
                facts["response_keys"] = sorted(str(key) for key in payload.keys())[:12]
        return True, "Payment challenge returned", facts

    if fetch.status == 200:
        facts["probe_result"] = "success_without_payment"
        if text:
            facts["response_char_count"] = len(text)
        return True, "Action request succeeded without payment challenge", facts

    if fetch.status in {400, 409, 422}:
        facts["probe_result"] = "input_validation"
        if text:
            facts["response_char_count"] = len(text)
        return True, "Action request reached the endpoint but failed input validation", facts

    if fetch.status in {401, 403, 405}:
        facts["probe_result"] = "auth_or_method_boundary"
        if text:
            facts["response_char_count"] = len(text)
        return True, "Action request reached the endpoint but hit an auth or method boundary", facts

    if fetch.status in {429, 503}:
        facts["probe_result"] = "temporary_failure"
        if text:
            facts["response_char_count"] = len(text)
        return True, "Action request hit a temporary service boundary", facts

    if fetch.status is None and fetch.error:
        facts["probe_result"] = "network_error"
        facts["error"] = fetch.error
        return False, fetch.error, facts

    if text:
        facts["response_char_count"] = len(text)
    return False, f"Unhandled action probe status {fetch.status}", facts


def validate_products(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    if fetch.truncated:
        return False, f"products catalog truncated at {fetch.byte_count} bytes", {"truncated": True}

    try:
        payload = parse_json_body(fetch)
    except ValueError as error:
        return False, str(error), {}
    except json.JSONDecodeError as error:
        return False, f"invalid json: {error.msg}", {}

    products: list[Any]
    if isinstance(payload, dict) and isinstance(payload.get("products"), list):
        products = payload["products"]
    elif (
        isinstance(payload, dict)
        and isinstance(payload.get("data"), dict)
        and isinstance(payload["data"].get("products"), list)
    ):
        products = payload["data"]["products"]
    elif isinstance(payload, list):
        products = payload
    else:
        return False, "products payload did not contain a products list", {}

    if not products:
        return False, "products list was empty", {"product_count": 0}

    product_count = 0
    variant_count = 0
    priced_variant_count = 0
    currencies: set[str] = set()
    min_price: float | None = None
    max_price: float | None = None
    sample_titles: list[str] = []
    sample_products: list[dict[str, Any]] = []
    seen_samples: set[tuple[str | None, str | None]] = set()
    preorder_product_count = 0
    stock_statuses: set[str] = set()

    for product in products:
        if not isinstance(product, dict):
            continue
        product_count += 1
        title = product.get("title") if isinstance(product.get("title"), str) else product.get("name")
        if isinstance(title, str) and title:
            sample_titles = merge_unique_limited(sample_titles, [title[:120]], limit=3)

        variants = product.get("variants")
        if not isinstance(variants, list):
            variants = product.get("variations")
        product_min_price: float | None = None
        product_max_price: float | None = None
        available_variant_count = 0
        requires_shipping = False
        sku = product.get("sku")
        slug = product.get("slug")
        json_ld = product.get("jsonLd") if isinstance(product.get("jsonLd"), dict) else {}
        json_ld_offers = json_ld.get("offers") if isinstance(json_ld.get("offers"), dict) else {}
        product_currency = product.get("currency") or json_ld_offers.get("priceCurrency")
        if isinstance(product_currency, str) and product_currency:
            currencies.add(product_currency.upper()[:8])
        product_price = parse_price(product.get("price") or json_ld_offers.get("price"))
        availability = json_ld_offers.get("availability")
        if isinstance(availability, str) and availability:
            availability_value = availability.rsplit("/", 1)[-1]
            stock_statuses.add(availability_value)
            if availability_value.lower() == "preorder":
                preorder_product_count += 1
        sample_key = (
            product.get("handle") if isinstance(product.get("handle"), str) else (
                slug if isinstance(slug, str) else None
            ),
            title if isinstance(title, str) else None,
        )
        if not isinstance(variants, list):
            if len(sample_products) < 3 and sample_key not in seen_samples:
                sample_products.append({
                    "title": title,
                    "handle": product.get("handle"),
                    "slug": slug,
                    "sku": sku,
                    "product_type": product.get("product_type"),
                    "vendor": product.get("vendor"),
                    "min_price": product_price,
                    "max_price": product_price,
                    "availability": availability.rsplit("/", 1)[-1] if isinstance(availability, str) and availability else None,
                })
                seen_samples.add(sample_key)
            if product_price is not None:
                priced_variant_count += 1
                min_price = product_price if min_price is None else min(min_price, product_price)
                max_price = product_price if max_price is None else max(max_price, product_price)
            continue

        for variant in variants:
            if not isinstance(variant, dict):
                continue
            variant_count += 1
            if variant.get("available") is True or normalize_status_value(variant.get("stock_status")) == "instock":
                available_variant_count += 1
            if isinstance(variant.get("stock_status"), str) and variant.get("stock_status").strip():
                stock_statuses.add(variant.get("stock_status").strip())
            if variant.get("requires_shipping") is True:
                requires_shipping = True
            price = parse_price(variant.get("price"))
            currency = variant.get("currency") or product_currency
            if isinstance(currency, str) and currency:
                currencies.add(currency.upper()[:8])
            if price is None:
                continue
            priced_variant_count += 1
            min_price = price if min_price is None else min(min_price, price)
            max_price = price if max_price is None else max(max_price, price)
            product_min_price = price if product_min_price is None else min(product_min_price, price)
            product_max_price = price if product_max_price is None else max(product_max_price, price)

        if product_min_price is None and product_price is not None:
            priced_variant_count += 1
            min_price = product_price if min_price is None else min(min_price, product_price)
            max_price = product_price if max_price is None else max(max_price, product_price)
            product_min_price = product_price
            product_max_price = product_price

        if len(sample_products) < 3 and sample_key not in seen_samples:
            sample_products.append({
                "title": title,
                "handle": product.get("handle"),
                "slug": slug,
                "sku": sku,
                "product_type": product.get("product_type"),
                "vendor": product.get("vendor"),
                "min_price": product_min_price if product_min_price is not None else product_price,
                "max_price": product_max_price if product_max_price is not None else product_price,
                "available_variant_count": available_variant_count,
                "variant_count": len(variants),
                "requires_shipping": requires_shipping,
                "availability": availability.rsplit("/", 1)[-1] if isinstance(availability, str) and availability else None,
            })
            seen_samples.add(sample_key)

    if product_count == 0:
        return False, "products list did not contain product objects", {}

    return True, "Usable products catalog detected", {
        "product_count": product_count,
        "variant_count": variant_count,
        "priced_variant_count": priced_variant_count,
        "currency_count": len(currencies),
        "currency_codes": sorted(currencies)[:12],
        "min_price": min_price,
        "max_price": max_price,
        "sample_titles": sample_titles,
        "sample_products": sample_products,
        "preorder_product_count": preorder_product_count,
        "stock_statuses": sorted(stock_statuses)[:12],
    }


def validate_cart(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    if fetch.truncated:
        return False, f"cart response truncated at {fetch.byte_count} bytes", {"truncated": True}

    try:
        payload = parse_json_body(fetch)
    except ValueError as error:
        return False, str(error), {}
    except json.JSONDecodeError as error:
        return False, f"invalid json: {error.msg}", {}

    if not isinstance(payload, dict):
        return False, "cart payload was not an object", {}

    currency = payload.get("currency")
    token = payload.get("token")
    if not isinstance(currency, str) or not currency:
        return False, "cart payload missing currency", {}

    facts = {
        "currency": currency,
        "item_count": payload.get("item_count"),
        "requires_shipping": payload.get("requires_shipping"),
    }
    if isinstance(token, str) and token:
        facts["has_token"] = True
    return True, "Public cart JSON detected", facts
