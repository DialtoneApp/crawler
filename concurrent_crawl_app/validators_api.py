from __future__ import annotations

import json
from typing import Any

from .helpers import (
    collect_payment_hints,
    decode_body,
    extract_template_parameters,
    is_json_content_type,
    is_login_handoff_body,
    merge_unique_limited,
    resolve_url,
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
                    template_parameters = extract_template_parameters(path_name)
                    if template_parameters:
                        candidate["template_parameters"] = template_parameters
                        collection_path = path_name.split("{", 1)[0].rstrip("/")
                        collection_path_item = readable_paths.get(collection_path)
                        if isinstance(collection_path_item, dict) and isinstance(collection_path_item.get("get"), dict):
                            discovery_url = resolve_url(base_reference, collection_path)
                            if discovery_url:
                                candidate["discovery_url"] = discovery_url
                        resource_path = path_name.rsplit("/", 1)[0] if "/" in path_name else path_name
                        for lookup_suffix in ("/price", "/pricing", "/quote"):
                            lookup_path = f"{resource_path}{lookup_suffix}"
                            lookup_item = readable_paths.get(lookup_path)
                            if isinstance(lookup_item, dict) and isinstance(lookup_item.get("get"), dict):
                                price_lookup_url = resolve_url(base_reference, lookup_path)
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
            primary_resource: dict[str, Any] | None = resources[0] if resources and isinstance(resources[0], dict) else None
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
    header_payment_hints = collect_payment_hints(www_authenticate_header) if isinstance(www_authenticate_header, str) and www_authenticate_header.strip() else {}
    facts: dict[str, Any] = {
        "probe_method": fetch.request_method,
        "probe_url": fetch.requested_url,
        "payment_required_header_present": bool(payment_required_header),
        "www_authenticate_present": bool(www_authenticate_header),
    }
    for key, value in header_payment_hints.items():
        if isinstance(value, list) and value:
            facts[key] = value
        elif key == "payment_surface" and value:
            facts[key] = value
        elif key == "crypto_only" and value:
            facts[key] = value

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
