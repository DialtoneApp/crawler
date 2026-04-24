from __future__ import annotations

from typing import Any

from .helpers import merge_unique_limited, resolve_url, split_http_method_prefix


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
    if "/api/submit" in lowered or lowered.endswith("/submit"):
        return {
            "name": "Sample Service",
            "description": "A service that accepts x402 payments",
            "category": "api",
            "url": "https://example.com/api",
            "protocol": "x402",
        }
    if "serp" in lowered or "/search" in lowered:
        return {"q": "openai"}
    if "scrape" in lowered:
        return {"url": "https://example.com"}
    if "crawl" in lowered:
        return {"url": "https://example.com", "maxPages": 1}
    if "/inbox/" in lowered or lowered.endswith("/inbox"):
        return {"content": "Test message"}
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
    endpoints: Any = None,
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
        inferred_method, normalized_path = split_http_method_prefix(raw_url if isinstance(raw_url, str) else None)
        resolved_url = resolve_url(base_reference, normalized_path or raw_url)
        method = service.get("method") if isinstance(service.get("method"), str) else inferred_method or "POST"
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
                    path=normalized_path or raw_url if isinstance(raw_url, str) else None,
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
            body=heuristic_sample_body(resolved_url or str(normalized_path or raw_url), method),
            content_type="application/json",
            source=source,
            title=title if isinstance(title, str) else None,
            amount=amount,
            currency=currency if isinstance(currency, str) else None,
        )
        if candidate:
            probe_candidates = merge_probe_candidates(probe_candidates, [candidate])

    endpoint_items: list[tuple[str, dict[str, Any]]] = []
    if isinstance(endpoints, dict):
        for raw_path, endpoint in endpoints.items():
            if isinstance(endpoint, dict):
                endpoint_items.append((str(raw_path), endpoint))
    elif isinstance(endpoints, list):
        for endpoint in endpoints:
            if not isinstance(endpoint, dict):
                continue
            raw_path = endpoint.get("path") or endpoint.get("url") or endpoint.get("resource") or endpoint.get("endpoint")
            if isinstance(raw_path, str) and raw_path.strip():
                endpoint_items.append((raw_path.strip(), endpoint))

    for raw_path, endpoint in endpoint_items:
        if not isinstance(endpoint, dict):
            continue
        inferred_method, normalized_path = split_http_method_prefix(raw_path)
        resolved_url = resolve_url(base_reference, normalized_path or raw_path)
        method = endpoint.get("method") if isinstance(endpoint.get("method"), str) else inferred_method or "POST"
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
                    path=normalized_path or raw_path,
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
            body=heuristic_sample_body(resolved_url or normalized_path or raw_path, method),
            content_type="application/json",
            source=source,
            title=title if isinstance(title, str) else None,
            amount=amount,
            currency=currency,
        )
        if candidate:
            probe_candidates = merge_probe_candidates(probe_candidates, [candidate])

    return resource_urls, sample_actions, probe_candidates, priced_action_count, currencies
