from __future__ import annotations

import json
from typing import Any

from .helpers import (
    collect_payment_hints,
    decode_body,
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
    commerce_path_count = 0

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

    return True, "OpenAPI document detected", {
        "openapi_version": version,
        "path_count": len(paths),
        "auth_schemes": sorted(set(auth_schemes))[:12],
        "commerce_path_count": commerce_path_count,
        "payment_required_operation_count": payment_required_operation_count,
        "payment_required_paths": payment_required_paths,
        "payment_signature_header_count": payment_signature_header_count,
    }


def validate_x402(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    if fetch.truncated:
        return False, f"x402 response truncated at {fetch.byte_count} bytes", {"truncated": True}

    text = decode_body(fetch.body).strip()
    if not text:
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
            resource_urls: list[str] = []
            for resource in resources:
                if isinstance(resource, str) and resource.strip():
                    resource_urls = merge_unique_limited(resource_urls, [resource.strip()], limit=12)
                elif isinstance(resource, dict):
                    for candidate_key in ("url", "resource", "endpoint"):
                        candidate_value = resource.get(candidate_key)
                        if isinstance(candidate_value, str) and candidate_value.strip():
                            resource_urls = merge_unique_limited(resource_urls, [candidate_value.strip()], limit=12)
                            break
            payment_hints = collect_payment_hints(payload)
            return True, "x402-like JSON detected", {
                "top_level_keys": sorted(keys)[:12],
                "resource_urls": resource_urls,
                "resource_url_count": len(resource_urls),
                "accept_count": len(accepts),
                "instructions_char_count": len(payload.get("instructions", "")) if isinstance(payload.get("instructions"), str) else 0,
                **payment_hints,
            }

        if isinstance(payload, list) and payload:
            return True, "x402-like JSON list detected", {"item_count": len(payload)}

        return False, "x402 payload was empty or unsupported", {}

    if "<html" in lower_text or "<body" in lower_text or "<script" in lower_text:
        return False, "x402 document looked like HTML fallback", {}
    if "x402" in lower_text or "payment required" in lower_text:
        return True, "x402-like text detected", {"char_count": len(text)}
    return False, "x402 text lacked x402/payment language", {"char_count": len(text)}


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
