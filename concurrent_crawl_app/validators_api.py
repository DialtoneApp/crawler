from __future__ import annotations

import json
from typing import Any

from .helpers import (
    collect_payment_hints,
    decode_json_like_header,
    decode_body,
    extract_template_parameters,
    extract_title,
    is_generic_error_fallback_body,
    is_html_content_type,
    is_json_content_type,
    is_login_handoff_body,
    looks_like_html_fragment,
    looks_like_markup_fragment,
    merge_unique_limited,
    resolve_openapi_path,
    resolve_url,
    split_http_method_prefix,
)
from .http_client import parse_json_body
from .models import FetchResponse
from .validators_support import (
    build_action_sample,
    build_probe_candidate,
    extract_x402_actions,
    heuristic_sample_body,
    merge_action_samples,
    merge_probe_candidates,
    sample_request_from_operation,
)


def summarize_x402_resource_title(payload: dict[str, Any], resource: dict[str, Any] | None, fallback_url: str | None) -> str | None:
    payload_name = payload.get("name")
    if isinstance(payload_name, str) and payload_name.strip():
        return payload_name.strip()[:120]
    if isinstance(resource, dict):
        resource_name = resource.get("name")
        if isinstance(resource_name, str) and resource_name.strip():
            return resource_name.strip()[:120]
        description = resource.get("description")
        if isinstance(description, str) and description.strip():
            first_sentence = description.strip().split(". ", 1)[0].strip()
            if first_sentence:
                return first_sentence[:120]
    if isinstance(fallback_url, str) and fallback_url.strip():
        return fallback_url.strip().rsplit("/", 1)[-1][:120]
    return None


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

    readable_paths = {
        str(path_name): path_item
        for path_name, path_item in paths.items()
        if isinstance(path_item, dict)
    }

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
                resolved_url = resolve_openapi_path(base_reference, path_name)
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
                if sample_body is None or sample_body == "string":
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
                    template_parameters = extract_template_parameters(path_name)
                    if template_parameters:
                        candidate["template_parameters"] = template_parameters
                        collection_path = path_name.split("{", 1)[0].rstrip("/")
                        collection_path_item = readable_paths.get(collection_path)
                        if isinstance(collection_path_item, dict) and isinstance(collection_path_item.get("get"), dict):
                            discovery_url = resolve_openapi_path(base_reference, collection_path)
                            if discovery_url:
                                candidate["discovery_url"] = discovery_url
                        if "discovery_url" not in candidate:
                            inferred_discovery_urls: list[str] = []
                            for candidate_path, candidate_path_item in readable_paths.items():
                                if candidate_path == path_name or extract_template_parameters(candidate_path):
                                    continue
                                candidate_get = candidate_path_item.get("get")
                                if not isinstance(candidate_get, dict):
                                    continue
                                candidate_text = " ".join(
                                    value.lower()
                                    for value in (
                                        candidate_path,
                                        candidate_get.get("summary"),
                                        candidate_get.get("operationId"),
                                        candidate_get.get("description"),
                                    )
                                    if isinstance(value, str) and value
                                )
                                score = 0
                                if candidate_path.startswith("/api/"):
                                    score += 5
                                if any(marker in candidate_text for marker in ("agents", "products", "assets", "resources", "servers", "catalog", "registry", "directory")):
                                    score += 40
                                if any(marker in candidate_text for marker in ("list", "browse", "search", "index")):
                                    score += 20
                                if score <= 0:
                                    continue
                                resolved_discovery_url = resolve_openapi_path(base_reference, candidate_path)
                                if isinstance(resolved_discovery_url, str) and resolved_discovery_url and resolved_discovery_url not in inferred_discovery_urls:
                                    inferred_discovery_urls.append(resolved_discovery_url)
                            if inferred_discovery_urls:
                                candidate["discovery_urls"] = inferred_discovery_urls[:6]
                        resource_path = path_name.rsplit("/", 1)[0] if "/" in path_name else path_name
                        for lookup_suffix in ("/price", "/pricing", "/quote"):
                            lookup_path = f"{resource_path}{lookup_suffix}"
                            lookup_item = readable_paths.get(lookup_path)
                            if isinstance(lookup_item, dict) and isinstance(lookup_item.get("get"), dict):
                                price_lookup_url = resolve_openapi_path(base_reference, lookup_path)
                                if price_lookup_url:
                                    candidate["price_lookup_url"] = price_lookup_url
                                break
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
    payment_required_payload = decode_json_like_header(payment_required_header)
    payment_required_hints = collect_payment_hints(payment_required_payload) if payment_required_payload is not None else {}
    if not text:
        if payment_required_header or www_authenticate_header:
            facts = {
                "header_challenge_present": True,
                "payment_required_header_present": bool(payment_required_header),
                "www_authenticate_present": bool(www_authenticate_header),
            }
            for key, value in payment_required_hints.items():
                if isinstance(value, list) and value:
                    facts[key] = value
                elif key == "payment_surface" and value:
                    facts[key] = value
                elif key == "crypto_only" and value:
                    facts[key] = value
            if isinstance(payment_required_payload, dict):
                facts["payment_required_header_keys"] = sorted(str(key) for key in payment_required_payload.keys())[:12]
            return True, "x402 payment challenge detected from headers", facts
        return False, "x402 document was empty", {}

    lower_text = text.lower()
    if is_login_handoff_body(text):
        return False, "x402 document looked like a login handoff page", {}
    if is_generic_error_fallback_body(text):
        return False, "x402 document looked like an error or block page", {}
    if is_json_content_type(fetch.content_type) or text[:1] in "[{":
        try:
            payload = parse_json_body(fetch)
        except ValueError as error:
            return False, str(error), {}
        except json.JSONDecodeError as error:
            return False, f"invalid json: {error.msg}", {}

        if isinstance(payload, dict):
            keys = set(str(key) for key in payload.keys())
            keys_lower = {key.strip().lower() for key in keys}
            challenge_context = fetch.status == 402 or bool(payment_required_header) or bool(www_authenticate_header)
            discovery_keys = {
                "accepts",
                "currencies",
                "endpoint",
                "endpoints",
                "extensions",
                "payment",
                "payment_required",
                "resource",
                "resources",
                "service",
                "services",
            }
            challenge_keys = discovery_keys | {
                "amount",
                "asset",
                "basepriceusd",
                "basetotalpriceusd",
                "beneficiary",
                "catalogdescription",
                "catalogtitle",
                "currency",
                "discountbps",
                "discountedpriceusd",
                "discountpercent",
                "externalurl",
                "price",
                "prices",
                "recipient",
            }
            has_structured_x402_keys = bool(
                "x402" in keys_lower
                or "x402version" in keys_lower
                or "x402_version" in keys_lower
                or (keys_lower & (challenge_keys if challenge_context else discovery_keys))
            )
            generic_error_keys = {
                "code",
                "contribution",
                "details",
                "error",
                "errors",
                "message",
                "path",
                "status",
                "statuscode",
                "timestamp",
                "type",
            }
            looks_like_generic_error = False
            if not has_structured_x402_keys and (keys_lower & {"error", "errors", "message", "status", "statuscode", "type", "code"}):
                status_code = payload.get("statusCode")
                status_value = payload.get("status")
                type_value = payload.get("type")
                message_value = payload.get("message")
                if isinstance(status_code, int) and status_code >= 400:
                    looks_like_generic_error = True
                elif isinstance(status_value, str):
                    normalized_status = status_value.strip().lower()
                    if normalized_status.isdigit() and int(normalized_status) >= 400:
                        looks_like_generic_error = True
                    elif normalized_status in {"error", "forbidden", "not found", "not_found", "not-found"}:
                        looks_like_generic_error = True
                if isinstance(type_value, str) and any(token in type_value.lower() for token in ("error", "forbidden", "not_found", "not-found")):
                    looks_like_generic_error = True
                if isinstance(message_value, str) and any(
                    token in message_value.lower()
                    for token in ("forbidden", "not found", "no route matches", "does not exist")
                ):
                    looks_like_generic_error = True
                if not looks_like_generic_error and not (keys_lower - generic_error_keys):
                    looks_like_generic_error = True
            if looks_like_generic_error:
                return False, "x402 JSON looked like a generic error response", {"top_level_keys": sorted(keys)[:12]}
            if not has_structured_x402_keys:
                return False, "x402 JSON lacked structured payment evidence", {"top_level_keys": sorted(keys)[:12]}
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
                endpoints=payload.get("endpoints"),
                source="x402",
            )
            primary_resource: dict[str, Any] | None = resources[0] if resources and isinstance(resources[0], dict) else None
            for resource in resources:
                if isinstance(resource, str) and resource.strip():
                    resource_method, resource_path = split_http_method_prefix(resource)
                    candidate_url = resolve_url(base_reference, resource_path) if isinstance(resource_path, str) else None
                    if candidate_url:
                        resource_urls = merge_unique_limited(resource_urls, [candidate_url], limit=12)
                    sample_actions = merge_action_samples(
                        sample_actions,
                        [
                            build_action_sample(
                                method=resource_method or "POST",
                                url=candidate_url,
                                path=resource_path,
                                title=(resource_path or resource.strip()).rsplit("/", 1)[-1],
                                source="x402",
                            )
                        ],
                    )
                    candidate = build_probe_candidate(
                        url=candidate_url,
                        method=resource_method or "POST",
                        body=heuristic_sample_body(candidate_url or resource_path or resource.strip(), resource_method or "POST"),
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
                        resource_title = summarize_x402_resource_title(payload, resource, candidate_url)
                        resource_description = resource.get("description") if isinstance(resource.get("description"), str) else None
                        sample_actions = merge_action_samples(
                            sample_actions,
                            [
                                build_action_sample(
                                    method=resource.get("method") if isinstance(resource.get("method"), str) else "POST",
                                    url=candidate_url,
                                    title=resource_title,
                                    description=resource_description,
                                    source="x402",
                                )
                            ],
                        )
            extensions = payload.get("extensions") if isinstance(payload.get("extensions"), dict) else {}
            bazaar = extensions.get("bazaar") if isinstance(extensions.get("bazaar"), dict) else {}
            bazaar_info = bazaar.get("info") if isinstance(bazaar.get("info"), dict) else {}
            bazaar_input = bazaar_info.get("input") if isinstance(bazaar_info.get("input"), dict) else {}
            if bazaar_input:
                candidate_url = None
                for candidate_key in ("url", "resource", "endpoint", "path"):
                    raw_value = bazaar_input.get(candidate_key)
                    if isinstance(raw_value, str) and raw_value.strip():
                        candidate_url = resolve_url(base_reference, raw_value)
                        if candidate_url:
                            break
                if candidate_url is None and isinstance(primary_resource, dict):
                    resource_url = primary_resource.get("url") or primary_resource.get("resource") or primary_resource.get("endpoint")
                    if isinstance(resource_url, str) and resource_url.strip():
                        candidate_url = resolve_url(base_reference, resource_url)
                if candidate_url is None:
                    candidate_url = fetch.final_url or fetch.requested_url

                candidate_method = bazaar_input.get("method") if isinstance(bazaar_input.get("method"), str) else "GET"
                candidate_body = bazaar_input.get("body")
                if candidate_body is None:
                    candidate_body = heuristic_sample_body(candidate_url, candidate_method)
                body_type = bazaar_input.get("bodyType") if isinstance(bazaar_input.get("bodyType"), str) else None
                content_type = "application/json" if body_type in {None, "json"} else None
                if body_type == "text":
                    content_type = "text/plain"
                title = summarize_x402_resource_title(payload, primary_resource, candidate_url)
                description = None
                if isinstance(primary_resource, dict) and isinstance(primary_resource.get("description"), str):
                    description = primary_resource.get("description")
                if candidate_url:
                    resource_urls = merge_unique_limited(resource_urls, [candidate_url], limit=12)
                sample_actions = merge_action_samples(
                    sample_actions,
                    [
                        build_action_sample(
                            method=candidate_method,
                            url=candidate_url,
                            title=title,
                            description=description,
                            source="x402",
                        )
                    ],
                )
                candidate = build_probe_candidate(
                    url=candidate_url,
                    method=candidate_method,
                    body=candidate_body,
                    content_type=content_type,
                    source="x402",
                    title=title,
                )
                if candidate:
                    probe_candidates = merge_probe_candidates(probe_candidates, [candidate])
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
            has_actionable_surface = bool(
                accepts
                or resources
                or sample_actions
                or probe_candidates
                or payment_required_header
                or www_authenticate_header
                or (challenge_context and keys_lower & challenge_keys)
            )
            if not has_actionable_surface:
                return False, "x402 JSON lacked actionable payment surface", {"top_level_keys": sorted(keys)[:12]}
            payment_hints = collect_payment_hints(payload)
            merged_provider_hints = merge_unique_limited(
                payment_hints.get("payment_provider_hints", []) if isinstance(payment_hints.get("payment_provider_hints"), list) else [],
                payment_required_hints.get("payment_provider_hints", []) if isinstance(payment_required_hints.get("payment_provider_hints"), list) else [],
                limit=12,
            )
            merged_rail_hints = merge_unique_limited(
                payment_hints.get("payment_rail_hints", []) if isinstance(payment_hints.get("payment_rail_hints"), list) else [],
                payment_required_hints.get("payment_rail_hints", []) if isinstance(payment_required_hints.get("payment_rail_hints"), list) else [],
                limit=12,
            )
            merged_endpoint_hosts = merge_unique_limited(
                payment_hints.get("payment_endpoint_hosts", []) if isinstance(payment_hints.get("payment_endpoint_hosts"), list) else [],
                payment_required_hints.get("payment_endpoint_hosts", []) if isinstance(payment_required_hints.get("payment_endpoint_hosts"), list) else [],
                limit=12,
            )
            merged_payment_surface = payment_hints.get("payment_surface") or payment_required_hints.get("payment_surface")
            merged_crypto_only = bool(payment_hints.get("crypto_only")) or bool(payment_required_hints.get("crypto_only"))
            facts = {
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
                "payment_provider_hints": merged_provider_hints,
                "payment_rail_hints": merged_rail_hints,
                "payment_endpoint_hosts": merged_endpoint_hosts,
                "payment_surface": merged_payment_surface,
                "crypto_only": merged_crypto_only,
            }
            if isinstance(payment_required_payload, dict):
                facts["payment_required_header_keys"] = sorted(str(key) for key in payment_required_payload.keys())[:12]
            return True, "x402-like JSON detected", facts

        if isinstance(payload, list) and payload:
            matching_item_count = 0
            for item in payload[:20]:
                if not isinstance(item, dict):
                    continue
                item_keys_lower = {str(key).strip().lower() for key in item.keys()}
                if (
                    "x402" in item_keys_lower
                    or "x402version" in item_keys_lower
                    or "x402_version" in item_keys_lower
                    or item_keys_lower & {"accepts", "resource", "resources", "services", "endpoints", "extensions", "payment", "payment_required"}
                ):
                    matching_item_count += 1
            if matching_item_count == 0:
                return False, "x402 list lacked structured payment objects", {"item_count": len(payload)}
            return True, "x402-like JSON list detected", {
                "item_count": len(payload),
                "matching_item_count": matching_item_count,
            }

        return False, "x402 payload was empty or unsupported", {}

    if is_html_content_type(fetch.content_type) or looks_like_markup_fragment(text):
        return False, "x402 document looked like HTML fallback", {}
    if payment_required_header or www_authenticate_header:
        facts = {
            "char_count": len(text),
            "payment_required_header_present": bool(payment_required_header),
            "www_authenticate_present": bool(www_authenticate_header),
        }
        for key, value in payment_required_hints.items():
            if isinstance(value, list) and value:
                facts[key] = value
            elif key == "payment_surface" and value:
                facts[key] = value
            elif key == "crypto_only" and value:
                facts[key] = value
        if isinstance(payment_required_payload, dict):
            facts["payment_required_header_keys"] = sorted(str(key) for key in payment_required_payload.keys())[:12]
        return True, "x402 payment challenge detected from headers", facts
    return False, "x402 text lacked structured machine-readable evidence", {"char_count": len(text)}


def validate_payment_probe(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    payment_required_header = fetch.headers.get("payment-required") or fetch.headers.get("x-payment-required")
    www_authenticate_header = fetch.headers.get("www-authenticate")
    text = decode_body(fetch.body).strip() if fetch.body else ""
    lower_text = text.lower()
    header_payment_hints = collect_payment_hints(www_authenticate_header) if isinstance(www_authenticate_header, str) and www_authenticate_header.strip() else {}
    payment_required_payload = decode_json_like_header(payment_required_header)
    payment_required_hints = collect_payment_hints(payment_required_payload) if payment_required_payload is not None else {}
    facts: dict[str, Any] = {
        "probe_method": fetch.request_method,
        "probe_url": fetch.requested_url,
        "payment_required_header_present": bool(payment_required_header),
        "www_authenticate_present": bool(www_authenticate_header),
    }
    merged_header_provider_hints = merge_unique_limited(
        header_payment_hints.get("payment_provider_hints", []) if isinstance(header_payment_hints.get("payment_provider_hints"), list) else [],
        payment_required_hints.get("payment_provider_hints", []) if isinstance(payment_required_hints.get("payment_provider_hints"), list) else [],
        limit=12,
    )
    merged_header_rail_hints = merge_unique_limited(
        header_payment_hints.get("payment_rail_hints", []) if isinstance(header_payment_hints.get("payment_rail_hints"), list) else [],
        payment_required_hints.get("payment_rail_hints", []) if isinstance(payment_required_hints.get("payment_rail_hints"), list) else [],
        limit=12,
    )
    merged_header_endpoint_hosts = merge_unique_limited(
        header_payment_hints.get("payment_endpoint_hosts", []) if isinstance(header_payment_hints.get("payment_endpoint_hosts"), list) else [],
        payment_required_hints.get("payment_endpoint_hosts", []) if isinstance(payment_required_hints.get("payment_endpoint_hosts"), list) else [],
        limit=12,
    )
    merged_header_surface = header_payment_hints.get("payment_surface") or payment_required_hints.get("payment_surface")
    merged_header_crypto_only = bool(header_payment_hints.get("crypto_only")) or bool(payment_required_hints.get("crypto_only"))
    if merged_header_provider_hints:
        facts["payment_provider_hints"] = merged_header_provider_hints
    if merged_header_rail_hints:
        facts["payment_rail_hints"] = merged_header_rail_hints
    if merged_header_endpoint_hosts:
        facts["payment_endpoint_hosts"] = merged_header_endpoint_hosts
    if merged_header_surface:
        facts["payment_surface"] = merged_header_surface
    if merged_header_crypto_only:
        facts["crypto_only"] = merged_header_crypto_only
    if isinstance(payment_required_payload, dict):
        facts["payment_required_header_keys"] = sorted(str(key) for key in payment_required_payload.keys())[:12]
    for key, value in header_payment_hints.items():
        if isinstance(value, list) and value:
            facts.setdefault(key, value)
        elif key == "payment_surface" and value:
            facts.setdefault(key, value)
        elif key == "crypto_only" and value:
            facts.setdefault(key, value)

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

    looks_like_html_document = (
        is_html_content_type(fetch.content_type)
        or looks_like_html_fragment(text)
        or "<!doctype html" in lower_text
        or "<html" in lower_text
        or "<head" in lower_text
        or "<body" in lower_text
        or "<title" in lower_text
        or "<meta " in lower_text
    )
    if fetch.status == 200:
        if looks_like_html_document:
            facts["probe_result"] = "html_landing_page"
            if text:
                facts["response_char_count"] = len(text)
                title = extract_title(text)
                if title:
                    facts["response_title"] = title
            detail = "Action request returned an HTML page instead of an API response"
            if is_generic_error_fallback_body(text):
                detail = "Action request returned an HTML error or block page"
            return False, detail, facts
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
