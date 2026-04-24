from __future__ import annotations

import json
from typing import Any

from .helpers import (
    collect_payment_hints,
    decode_body,
    derive_x402_discovery_url,
    extract_absolute_urls,
    extract_link_urls_by_rel,
    extract_title,
    final_host,
    flatten_strings,
    has_placeholder_value,
    infer_payment_surface_from_hints,
    is_active_offer_status,
    is_human_checkout_kind,
    is_html_content_type,
    is_json_content_type,
    is_live_status,
    is_login_handoff_body,
    is_prelaunch_status,
    is_text_content_type,
    is_xml_content_type,
    merge_unique_limited,
    normalize_status_value,
    sample_offer_from_payload,
)
from .http_client import parse_json_body
from .models import FetchResponse


def validate_homepage(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    if not fetch.body:
        return False, "Empty homepage response body", {}

    text = decode_body(fetch.body)
    lower_text = text.lower()
    title = extract_title(text)
    base_reference = fetch.final_url or fetch.requested_url
    shopify_hint = (
        "shopify" in lower_text
        or "myshopify.com" in lower_text
        or "cdn.shopify.com" in lower_text
    )
    service_desc_urls = extract_link_urls_by_rel(text, rel_token="service-desc", base_url=base_reference)
    service_meta_urls = extract_link_urls_by_rel(text, rel_token="service-meta", base_url=base_reference)
    api_catalog_urls = extract_link_urls_by_rel(text, rel_token="api-catalog", base_url=base_reference)
    facts = {"title": title, "shopify_hint": shopify_hint}
    if service_desc_urls:
        facts["service_desc_urls"] = service_desc_urls
    if service_meta_urls:
        facts["service_meta_urls"] = service_meta_urls
    if api_catalog_urls:
        facts["api_catalog_urls"] = api_catalog_urls

    if is_html_content_type(fetch.content_type) or "<html" in lower_text or title:
        return True, "Fetched homepage HTML", facts

    if is_text_content_type(fetch.content_type):
        return True, "Fetched non-HTML homepage text", facts

    return False, "Homepage did not look like HTML or text", facts


def validate_robots(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    text = decode_body(fetch.body).strip()
    if not text:
        return False, "robots.txt was empty", {}
    if "user-agent" not in text.lower():
        return False, "robots.txt did not contain a User-agent rule", {}
    return True, "Valid robots.txt pattern detected", {"line_count": len(text.splitlines())}


def validate_sitemap(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    text = decode_body(fetch.body).strip()
    lower_text = text.lower()
    if not text:
        return False, "sitemap.xml was empty", {}
    if not (is_xml_content_type(fetch.content_type) or lower_text.startswith("<?xml") or "<urlset" in lower_text or "<sitemapindex" in lower_text):
        return False, "sitemap.xml did not look like XML", {}
    if "<urlset" in lower_text:
        return True, "Valid sitemap urlset detected", {"sitemap_kind": "urlset"}
    if "<sitemapindex" in lower_text:
        return True, "Valid sitemap index detected", {"sitemap_kind": "sitemapindex"}
    if "<rss" in lower_text:
        return True, "RSS/Atom style sitemap detected", {"sitemap_kind": "rss"}
    return True, "XML sitemap-like document detected", {"sitemap_kind": "xml"}


def validate_llms(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    text = decode_body(fetch.body).strip()
    if not text:
        return False, "llms document was empty", {}
    lower_text = text.lower()
    if "<html" in lower_text or "<body" in lower_text or "<script" in lower_text:
        return False, "llms document looked like HTML fallback", {}
    if is_login_handoff_body(text):
        return False, "llms document looked like a login handoff page", {}
    discovered_urls = extract_absolute_urls(text)
    x402_urls: list[str] = []
    for url in discovered_urls:
        if "/.well-known/x402" in url.lower():
            x402_urls = merge_unique_limited(x402_urls, [url], limit=8)

    has_x402_markers = "x402" in lower_text or "payment required" in lower_text
    if has_x402_markers:
        for line in text.splitlines():
            line_urls = extract_absolute_urls(line)
            if not line_urls:
                continue
            lower_line = line.lower()
            if not any(marker in lower_line for marker in ("api base", "base url", "rest api base", "gateway", "base:")):
                continue
            for url in line_urls:
                derived_x402_url = derive_x402_discovery_url(url)
                if derived_x402_url:
                    x402_urls = merge_unique_limited(x402_urls, [derived_x402_url], limit=8)

    facts: dict[str, Any] = {"char_count": len(text)}
    if x402_urls:
        facts["x402_urls"] = x402_urls
    return True, "Non-empty llms text detected", facts


def validate_commerce(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    text = decode_body(fetch.body).strip()
    if not text:
        return False, "commerce document was empty", {}
    if is_login_handoff_body(text):
        return False, "commerce document looked like a login handoff page", {}

    if is_json_content_type(fetch.content_type) or text[:1] in "[{":
        try:
            payload = parse_json_body(fetch)
        except ValueError as error:
            return False, str(error), {}
        except json.JSONDecodeError as error:
            return False, f"invalid json: {error.msg}", {}

        if not isinstance(payload, (dict, list)):
            return False, "commerce payload was not an object or list", {}
        if isinstance(payload, dict):
            keys = sorted(str(key) for key in payload.keys())
            interesting = {
                "auth",
                "billing_provider",
                "checkout",
                "checkout_url",
                "commerce",
                "offer",
                "offerings",
                "offers",
                "payment",
                "paymentHandlers",
                "price",
                "pricing",
                "purchase",
                "x402",
            }
            if not any(key in interesting for key in payload.keys()):
                return False, "commerce JSON lacked price or purchase-like keys", {"top_level_keys": keys[:12]}
            offers = payload.get("offers") if isinstance(payload.get("offers"), list) else []
            if not offers and isinstance(payload.get("offerings"), list):
                offers = payload["offerings"]
            commerce_status = normalize_status_value(payload.get("status"))
            checkout_url = payload.get("checkout_url") if isinstance(payload.get("checkout_url"), str) and payload.get("checkout_url").strip() else None
            billing_provider = payload.get("billing_provider") if isinstance(payload.get("billing_provider"), dict) else {}
            billing_provider_name = billing_provider.get("name") if isinstance(billing_provider.get("name"), str) else None
            billing_provider_status = normalize_status_value(billing_provider.get("status"))
            offer_count = 0
            active_offer_count = 0
            priced_offer_count = 0
            sample_offers: list[dict[str, Any]] = []
            seen_offer_keys: set[tuple[str | None, str | None]] = set()
            purchase_intent_urls: list[str] = []
            offer_lookup_urls: list[str] = []

            for offer in offers:
                if not isinstance(offer, dict):
                    continue
                offer_count += 1
                sample_offer, has_price = sample_offer_from_payload(
                    offer,
                    default_status=commerce_status,
                    default_checkout_url=checkout_url,
                )
                if is_active_offer_status(sample_offer.get("status")) and not is_prelaunch_status(sample_offer.get("status")):
                    active_offer_count += 1
                if has_price:
                    priced_offer_count += 1
                sample_key = (
                    sample_offer.get("id") if isinstance(sample_offer.get("id"), str) else None,
                    sample_offer.get("title") if isinstance(sample_offer.get("title"), str) else None,
                )
                if len(sample_offers) < 3 and sample_key not in seen_offer_keys:
                    sample_offers.append(sample_offer)
                    seen_offer_keys.add(sample_key)
                purchase_intent_url = sample_offer.get("purchase_intent_url")
                if isinstance(purchase_intent_url, str):
                    purchase_intent_urls = merge_unique_limited(purchase_intent_urls, [purchase_intent_url], limit=8)
                offer_lookup_url = sample_offer.get("offer_lookup_url")
                if isinstance(offer_lookup_url, str):
                    offer_lookup_urls = merge_unique_limited(offer_lookup_urls, [offer_lookup_url], limit=8)

            auth = payload.get("auth") if isinstance(payload.get("auth"), dict) else {}
            checkout = payload.get("checkout") if isinstance(payload.get("checkout"), dict) else {}
            purchase_intent_auth = normalize_status_value(auth.get("purchase_intent")) or (
                auth.get("purchase_intent").strip() if isinstance(auth.get("purchase_intent"), str) else None
            )

            live_machine_payment_path_ids: list[str] = []
            live_machine_payment_path_labels: list[str] = []
            live_machine_payment_path_kinds: list[str] = []
            live_payment_provider_hints: list[str] = []
            live_payment_rail_hints: list[str] = []
            live_payment_endpoint_hosts: list[str] = []
            live_machine_payment_path_count = 0

            machine_payment_paths = payload.get("machine_payment_paths") if isinstance(payload.get("machine_payment_paths"), list) else []
            for path in machine_payment_paths:
                if not isinstance(path, dict) or not is_live_status(path.get("status")):
                    continue
                if is_human_checkout_kind(path.get("kind")):
                    continue
                live_machine_payment_path_count += 1
                path_id = path.get("id")
                path_label = path.get("label")
                path_kind = path.get("kind")
                if isinstance(path_id, str) and path_id.strip():
                    live_machine_payment_path_ids = merge_unique_limited(live_machine_payment_path_ids, [path_id.strip()], limit=12)
                if isinstance(path_label, str) and path_label.strip():
                    live_machine_payment_path_labels = merge_unique_limited(live_machine_payment_path_labels, [path_label.strip()], limit=12)
                if isinstance(path_kind, str) and path_kind.strip():
                    live_machine_payment_path_kinds = merge_unique_limited(live_machine_payment_path_kinds, [path_kind.strip()], limit=12)
                payment_hints = collect_payment_hints(path)
                live_payment_provider_hints = merge_unique_limited(
                    live_payment_provider_hints,
                    payment_hints.get("payment_provider_hints", []) if isinstance(payment_hints.get("payment_provider_hints"), list) else [],
                    limit=12,
                )
                live_payment_rail_hints = merge_unique_limited(
                    live_payment_rail_hints,
                    payment_hints.get("payment_rail_hints", []) if isinstance(payment_hints.get("payment_rail_hints"), list) else [],
                    limit=12,
                )
                live_payment_endpoint_hosts = merge_unique_limited(
                    live_payment_endpoint_hosts,
                    payment_hints.get("payment_endpoint_hosts", []) if isinstance(payment_hints.get("payment_endpoint_hosts"), list) else [],
                    limit=12,
                )

            for provider_key in ("dialtoneapp_network", "nevermined", "skyfire", "crossmint", "asterpay"):
                section = payload.get(provider_key)
                if not isinstance(section, dict) or not is_live_status(section.get("status")):
                    continue
                payment_hints = collect_payment_hints(section)
                live_payment_provider_hints = merge_unique_limited(
                    live_payment_provider_hints,
                    [provider_key] + (
                        payment_hints.get("payment_provider_hints", [])
                        if isinstance(payment_hints.get("payment_provider_hints"), list)
                        else []
                    ),
                    limit=12,
                )
                live_payment_rail_hints = merge_unique_limited(
                    live_payment_rail_hints,
                    payment_hints.get("payment_rail_hints", []) if isinstance(payment_hints.get("payment_rail_hints"), list) else [],
                    limit=12,
                )
                live_payment_endpoint_hosts = merge_unique_limited(
                    live_payment_endpoint_hosts,
                    payment_hints.get("payment_endpoint_hosts", []) if isinstance(payment_hints.get("payment_endpoint_hosts"), list) else [],
                    limit=12,
                )

            payment_handlers = payload.get("paymentHandlers") if isinstance(payload.get("paymentHandlers"), list) else []
            nonlive_payment_provider_hints = merge_unique_limited(
                [],
                [billing_provider_name] if isinstance(billing_provider_name, str) and billing_provider_name else [],
                limit=12,
            )
            nonlive_payment_rail_hints = merge_unique_limited(
                [],
                [str(handler).strip() for handler in payment_handlers if str(handler).strip()],
                limit=12,
            )
            nonlive_payment_endpoint_hosts = []
            if checkout_url:
                checkout_host = final_host(checkout_url)
                if checkout_host:
                    nonlive_payment_endpoint_hosts = merge_unique_limited(nonlive_payment_endpoint_hosts, [checkout_host], limit=12)

            payment_surface = infer_payment_surface_from_hints(live_payment_provider_hints, live_payment_rail_hints)
            if not payment_surface:
                payment_surface = infer_payment_surface_from_hints(nonlive_payment_provider_hints, nonlive_payment_rail_hints)
            facts = {
                "top_level_keys": keys[:12],
                "commerce_status": commerce_status,
                "offer_count": offer_count,
                "active_offer_count": active_offer_count,
                "priced_offer_count": priced_offer_count,
                "sample_offers": sample_offers,
                "checkout_url": checkout_url,
                "purchase_intent_urls": purchase_intent_urls,
                "purchase_intent_url_count": len(purchase_intent_urls),
                "offer_lookup_urls": offer_lookup_urls,
                "offer_lookup_url_count": len(offer_lookup_urls),
                "purchase_intent_auth": purchase_intent_auth,
                "checkout_state": checkout.get("current_state"),
                "billing_provider_name": billing_provider_name,
                "billing_provider_status": billing_provider_status,
                "live_machine_payment_path_ids": live_machine_payment_path_ids,
                "live_machine_payment_path_labels": live_machine_payment_path_labels,
                "live_machine_payment_path_kinds": live_machine_payment_path_kinds,
                "live_machine_payment_path_count": live_machine_payment_path_count,
                "payment_provider_hints": live_payment_provider_hints or nonlive_payment_provider_hints,
                "payment_rail_hints": live_payment_rail_hints or nonlive_payment_rail_hints,
                "payment_endpoint_hosts": live_payment_endpoint_hosts or nonlive_payment_endpoint_hosts,
                "payment_surface": payment_surface,
                "crypto_only": bool(
                    "crypto" in (live_payment_rail_hints or nonlive_payment_rail_hints)
                    and not ({"saved_card", "card", "digital_wallet", "x402"} & set(live_payment_rail_hints or nonlive_payment_rail_hints))
                ),
                "prelaunch": is_prelaunch_status(commerce_status) or is_prelaunch_status(billing_provider_status),
            }
            return True, "Structured commerce JSON detected", facts
        return True, "Structured commerce list detected", {"item_count": len(payload)}

    lower_text = text.lower()
    if "<html" in lower_text or "<body" in lower_text or "<script" in lower_text:
        return False, "commerce document looked like HTML fallback", {}
    keywords = ("price", "offer", "checkout", "payment", "purchase", "commerce")
    if any(keyword in lower_text for keyword in keywords):
        return True, "Commerce text with pricing or purchase language detected", {"char_count": len(text)}
    return False, "commerce text lacked pricing or purchase language", {"char_count": len(text)}


def validate_ucp(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    if fetch.truncated:
        return False, f"UCP response truncated at {fetch.byte_count} bytes", {"truncated": True}

    try:
        payload = parse_json_body(fetch)
    except ValueError as error:
        return False, str(error), {}
    except json.JSONDecodeError as error:
        return False, f"invalid json: {error.msg}", {}

    if not isinstance(payload, dict):
        return False, "UCP payload was not an object", {}

    ucp = payload.get("ucp")
    if not isinstance(ucp, dict):
        return False, "UCP payload missing top-level ucp object", {}

    raw_capabilities = ucp.get("capabilities")
    capability_count = 0
    capability_names: list[str] = []
    if isinstance(raw_capabilities, dict):
        capability_names = sorted(str(key) for key in raw_capabilities.keys())
        capability_count = len(capability_names)
    elif isinstance(raw_capabilities, list):
        capability_names = [
            str(item.get("name"))
            for item in raw_capabilities
            if isinstance(item, dict) and item.get("name")
        ]
        capability_count = len(capability_names)

    services = ucp.get("services") if isinstance(ucp.get("services"), dict) else {}
    version = ucp.get("version")
    supported_versions = ucp.get("supported_versions") if isinstance(ucp.get("supported_versions"), dict) else {}
    current_version_url = None
    if version:
        current_version_candidate = supported_versions.get(version)
        if isinstance(current_version_candidate, str) and current_version_candidate:
            current_version_url = current_version_candidate
    if not version and capability_count == 0 and not services:
        return False, "UCP payload lacked version, capabilities, and services", {}

    handler_names: list[str] = []
    handler_ids: list[str] = []
    payment_endpoints: list[str] = []
    shopping_mcp_endpoints: list[str] = []
    contains_placeholder = False

    shopping_services = services.get("dev.ucp.shopping")
    if isinstance(shopping_services, dict):
        shopping_services = [shopping_services]
    if isinstance(shopping_services, list):
        for service in shopping_services:
            if not isinstance(service, dict):
                continue
            transport = service.get("transport")
            endpoint = service.get("endpoint")
            if isinstance(endpoint, str) and endpoint:
                if transport == "mcp":
                    shopping_mcp_endpoints = merge_unique_limited(shopping_mcp_endpoints, [endpoint], limit=8)
                if has_placeholder_value(endpoint):
                    contains_placeholder = True

    payment = payload.get("payment") if isinstance(payload.get("payment"), dict) else {}
    handlers = payment.get("handlers") if isinstance(payment.get("handlers"), list) else []

    for handler in handlers:
        if not isinstance(handler, dict):
            continue
        handler_id = handler.get("id")
        handler_name = handler.get("name")
        if isinstance(handler_id, str) and handler_id:
            handler_ids.append(handler_id)
        if isinstance(handler_name, str) and handler_name:
            handler_names.append(handler_name)
        for value in flatten_strings(handler):
            if has_placeholder_value(value):
                contains_placeholder = True
            if value.startswith("http://") or value.startswith("https://"):
                payment_endpoints.append(value)

    ucp_payment_handlers = ucp.get("payment_handlers")
    if isinstance(ucp_payment_handlers, dict):
        for handler_name, entries in ucp_payment_handlers.items():
            if isinstance(handler_name, str) and handler_name:
                handler_names.append(handler_name)
            if isinstance(entries, dict):
                entries = [entries]
            if not isinstance(entries, list):
                continue
            for entry in entries:
                if not isinstance(entry, dict):
                    continue
                handler_id = entry.get("id")
                if isinstance(handler_id, str) and handler_id:
                    handler_ids.append(handler_id)
                for value in flatten_strings(entry):
                    if has_placeholder_value(value):
                        contains_placeholder = True
                    if value.startswith("http://") or value.startswith("https://"):
                        payment_endpoints.append(value)

    all_strings = flatten_strings(payload)
    if any(has_placeholder_value(value) for value in all_strings):
        contains_placeholder = True

    if contains_placeholder:
        return False, "UCP payload looked like a template or sandbox example", {
            "ucp_version": version,
            "capability_count": capability_count,
            "service_count": len(services),
            "payment_handler_count": len(handler_names),
            "payment_handler_names": sorted(set(handler_names))[:12],
        }

    payment_hints = collect_payment_hints(payload)

    return True, "Valid UCP document detected", {
        "ucp_version": version,
        "capability_count": capability_count,
        "capability_names": capability_names[:12],
        "service_count": len(services),
        "payment_handler_count": len(handler_names),
        "payment_handler_names": sorted(set(handler_names))[:12],
        "payment_handler_ids": sorted(set(handler_ids))[:12],
        "payment_endpoint_samples": sorted(set(payment_endpoints))[:6],
        "shopping_mcp_endpoints": shopping_mcp_endpoints,
        "current_version_url": current_version_url,
        **payment_hints,
    }
