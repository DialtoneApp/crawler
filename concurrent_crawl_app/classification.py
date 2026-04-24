from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .helpers import decode_body, merge_unique_limited, normalize_status_value
from .models import CrawlReceipt, DomainInput, ProbeOutcome

OBSERVED_SCHEMA_FACT_KEYS = (
    "observed_capability_names",
    "observed_payment_protocols",
    "observed_payment_methods",
    "observed_payment_providers",
    "observed_payment_handler_names",
    "observed_payment_assets",
    "observed_payment_networks",
    "observed_payment_flow_types",
    "observed_payment_requirement_keys",
    "observed_catalog_endpoints",
    "observed_quote_endpoints",
    "observed_checkout_endpoints",
    "observed_order_status_endpoints",
)


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
        merged.append(dict(value))
        seen.add(key)
        if len(merged) >= limit:
            break
    return merged[:limit]


def merge_observed_schema_facts(outcomes: dict[str, ProbeOutcome]) -> dict[str, list[str]]:
    merged: dict[str, list[str]] = {}
    for outcome in outcomes.values():
        if outcome.status != "valid":
            continue
        for fact_key in OBSERVED_SCHEMA_FACT_KEYS:
            values = outcome.facts.get(fact_key)
            if isinstance(values, list) and values:
                merged[fact_key] = merge_unique_limited(
                    merged.get(fact_key, []),
                    [value for value in values if isinstance(value, str)],
                    limit=12,
                )
    return merged


def rate_limited_outcomes(outcomes: dict[str, ProbeOutcome]) -> list[tuple[str, ProbeOutcome]]:
    return [
        (key, outcome)
        for key, outcome in outcomes.items()
        if outcome.status == "rate_limited"
    ]


def should_probe_products(homepage_fetch: Any, homepage_outcome: ProbeOutcome, outcomes: dict[str, ProbeOutcome]) -> bool:
    if outcomes.get("well_known_ucp", ProbeOutcome("", "", "", None, None)).status == "valid":
        return True

    facts = homepage_outcome.facts if homepage_outcome.status == "valid" else {}
    if facts.get("shopify_hint") is True:
        return True

    text = decode_body(homepage_fetch.body).lower() if homepage_fetch.body else ""
    shopify_markers = ("shopify", "myshopify.com", "cdn.shopify.com", "shopify.theme")
    return any(marker in text for marker in shopify_markers)


def best_valid_outcome(outcomes: dict[str, ProbeOutcome], keys: tuple[str, ...]) -> ProbeOutcome | None:
    for key in keys:
        outcome = outcomes.get(key)
        if outcome and outcome.status == "valid":
            return outcome
    return None


def infer_purchase_boundary(
    *,
    tags: list[str],
    products: ProbeOutcome | None,
    commerce: ProbeOutcome | None,
    payment_probe: ProbeOutcome | None,
) -> str:
    probe_result = payment_probe.facts.get("probe_result") if payment_probe and payment_probe.status == "valid" else None
    if probe_result == "payment_challenge":
        return "payment_challenge"
    if probe_result == "success_without_payment":
        return "optional_or_prepaid_payment"
    if probe_result == "auth_or_method_boundary":
        return "auth_boundary"
    if probe_result == "input_validation":
        return "input_validation_boundary"

    if commerce and commerce.status == "valid":
        checkout_url = commerce.facts.get("checkout_url")
        purchase_intent_url_count = int(commerce.facts.get("purchase_intent_url_count") or 0)
        priced_offer_count = int(commerce.facts.get("priced_offer_count") or 0)
        if isinstance(checkout_url, str) and checkout_url:
            return "checkout_redirect"
        if priced_offer_count > 0 and purchase_intent_url_count > 0:
            return "purchase_intent_documented"
        if priced_offer_count > 0:
            return "offer_documented"

    if products and products.status == "valid":
        return "catalog_only"
    if "ai_readable" in tags:
        return "read_only"
    if "crawl_basics" in tags:
        return "crawl_only"
    return "unknown"


def infer_control_boundary(
    *,
    payment_surface: str | None,
    crypto_only: bool,
    purchase_intent_auth: str | None,
    openapi_auth_schemes: list[str],
    payment_probe: ProbeOutcome | None,
    browser_checkout_surface: bool,
) -> str:
    probe_result = payment_probe.facts.get("probe_result") if payment_probe and payment_probe.status == "valid" else None

    auth_value = normalize_status_value(purchase_intent_auth)
    if auth_value in {"wallet", "siwx", "http_signatures"}:
        return "wallet_required"
    if auth_value in {"oauth2", "bearer", "token", "api_key", "apikey", "auth"}:
        return "token_required"

    if probe_result == "success_without_payment":
        return "none"
    if probe_result == "payment_challenge":
        if payment_surface == "saved_card_authority":
            return "owner_preauthorized_card"
        if crypto_only:
            return "wallet_required"
        return "payment_required"
    if probe_result == "auth_or_method_boundary":
        if crypto_only:
            return "wallet_required"
        if openapi_auth_schemes:
            return "token_required"
        return "auth_required"
    if probe_result == "input_validation":
        return "input_required"

    if payment_surface == "saved_card_authority":
        return "owner_preauthorized_card"
    if crypto_only:
        return "wallet_required"
    if openapi_auth_schemes:
        return "token_required"
    if browser_checkout_surface:
        return "human_checkout"
    return "unknown"


def merge_agent_facts(outcomes: dict[str, ProbeOutcome]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    list_keys = {
        "public_endpoint_urls",
        "product_urls",
        "docs_urls",
        "order_urls",
        "register_urls",
        "wallet_guides_urls",
        "x402_urls",
        "payment_network_names",
        "payment_chain_ids",
        "payment_currency_codes",
        "payment_assets",
        "payment_provider_hints",
        "payment_rail_hints",
        "payment_endpoint_hosts",
    }
    scalar_keys = {
        "service_url",
        "api_base_url",
        "docs_url",
        "openapi_url",
        "x402_url",
        "brand_facts_url",
        "llms_url",
        "payment_protocol",
        "recommended_client",
        "payment_surface",
        "crypto_only",
    }
    sample_actions: list[dict[str, Any]] = []
    priced_action_count = 0
    for key in ("well_known_agent_json", "root_agent_json", "well_known_agent_card"):
        outcome = outcomes.get(key)
        if not outcome or outcome.status != "valid":
            continue
        for fact_key in list_keys:
            if isinstance(outcome.facts.get(fact_key), list):
                merged[fact_key] = merge_unique_limited(
                    merged.get(fact_key, []) if isinstance(merged.get(fact_key), list) else [],
                    outcome.facts[fact_key],
                    limit=12,
                )
        for fact_key in scalar_keys:
            if fact_key not in merged and outcome.facts.get(fact_key):
                merged[fact_key] = outcome.facts[fact_key]
        if isinstance(outcome.facts.get("sample_actions"), list):
            sample_actions = merge_action_samples(sample_actions, outcome.facts["sample_actions"])
        if int(outcome.facts.get("priced_action_count") or 0) > priced_action_count:
            priced_action_count = int(outcome.facts.get("priced_action_count") or 0)
    if isinstance(merged.get("public_endpoint_urls"), list):
        merged["public_endpoint_count"] = len(merged["public_endpoint_urls"])
    if sample_actions:
        merged["sample_actions"] = sample_actions
    if priced_action_count > 0:
        merged["priced_action_count"] = priced_action_count
    return merged


def classify_receipt(
    domain_input: DomainInput,
    outcomes: dict[str, ProbeOutcome],
) -> CrawlReceipt:
    tags: list[str] = []
    aggregates: dict[str, Any] = {}
    limited_outcomes = rate_limited_outcomes(outcomes)

    homepage = outcomes.get("homepage")
    title = None
    if homepage and homepage.facts:
        title = homepage.facts.get("title")
        if homepage.facts.get("shopify_hint"):
            tags.append("shopify_hint")

    if limited_outcomes:
        tags.append("rate_limited")
        aggregates["rate_limited_probe_count"] = len(limited_outcomes)
        aggregates["rate_limited_probe_keys"] = [key for key, _ in limited_outcomes][:16]
        retry_after_seconds = sorted(
            {
                int(outcome.facts["retry_after_seconds"])
                for _, outcome in limited_outcomes
                if isinstance(outcome.facts.get("retry_after_seconds"), int)
            }
        )
        if retry_after_seconds:
            aggregates["rate_limited_retry_after_seconds"] = retry_after_seconds[0]
            aggregates["rate_limited_retry_after_values"] = retry_after_seconds[:8]

    valid_keys = {key for key, outcome in outcomes.items() if outcome.status == "valid"}
    payment_provider_hints: list[str] = []
    payment_rail_hints: list[str] = []
    payment_endpoint_hosts: list[str] = []
    payment_surface: str | None = None
    crypto_only = False
    verified_payment_surface = False
    commerce_docs_claim_machine_payable = False
    sample_actions: list[dict[str, Any]] = []
    priced_action_count = 0
    openapi_auth_schemes: list[str] = []
    purchase_intent_auth: str | None = None
    browser_checkout_surface = False

    if "llms_txt" in valid_keys or "llms_full_txt" in valid_keys:
        tags.append("ai_readable")
    if "well_known_ucp" in valid_keys or "products_json" in valid_keys or "api_products" in valid_keys:
        tags.append("catalog_surface")
    if {
        "openapi_json",
        "api_openapi_json",
        "well_known_commerce",
        "well_known_agent_json",
        "well_known_agents_json",
        "well_known_agent_card",
        "root_agent_json",
        "x402_json",
        "x402_well_known",
        "remote_x402",
        "payment_probe",
    } & valid_keys:
        tags.append("callable_surface")
    if {"robots_txt", "sitemap_xml"} <= valid_keys:
        tags.append("crawl_basics")

    products = best_valid_outcome(outcomes, ("products_json", "api_products"))
    if products and products.status == "valid":
        for key in (
            "product_count",
            "variant_count",
            "priced_variant_count",
            "currency_count",
            "currency_codes",
            "min_price",
            "max_price",
            "sample_titles",
            "preorder_product_count",
            "stock_statuses",
        ):
            if key in products.facts:
                aggregates[key] = products.facts[key]
        if "sample_products" in products.facts:
            sample_products: list[dict[str, Any]] = []
            for item in products.facts["sample_products"]:
                if not isinstance(item, dict):
                    continue
                sample_item = dict(item)
                handle = sample_item.get("handle")
                if isinstance(handle, str) and handle and not sample_item.get("product_url"):
                    sample_item["product_url"] = f"https://{domain_input.domain}/products/{handle}"
                sample_products.append(sample_item)
            aggregates["sample_products"] = sample_products
        if aggregates.get("sample_products") and isinstance(products.facts.get("currency_codes"), list) and products.facts["currency_codes"]:
            default_currency = next(
                (code for code in products.facts["currency_codes"] if isinstance(code, str) and code),
                None,
            )
            if default_currency:
                for item in aggregates["sample_products"]:
                    if isinstance(item, dict) and item.get("min_price") is not None and "currency" not in item:
                        item["currency"] = default_currency

    cart = outcomes.get("cart_json")
    if cart and cart.status == "valid":
        for key in ("currency", "item_count", "requires_shipping", "has_token"):
            if key in cart.facts:
                aggregates[f"cart_{key}"] = cart.facts[key]
        if aggregates.get("sample_products") and isinstance(cart.facts.get("currency"), str):
            for item in aggregates["sample_products"]:
                if isinstance(item, dict) and item.get("min_price") is not None and "currency" not in item:
                    item["currency"] = cart.facts["currency"]

    openapi = best_valid_outcome(outcomes, ("openapi_json", "api_openapi_json"))
    if openapi and openapi.status == "valid":
        openapi_auth_schemes = [
            value
            for value in openapi.facts.get("auth_schemes", [])
            if isinstance(value, str) and value
        ]
        for key in (
            "path_count",
            "auth_schemes",
            "commerce_path_count",
            "payment_required_operation_count",
            "payment_required_paths",
            "payment_signature_header_count",
        ):
            if key in openapi.facts:
                aggregates[f"openapi_{key}"] = openapi.facts[key]
        if isinstance(openapi.facts.get("sample_actions"), list):
            sample_actions = merge_action_samples(sample_actions, openapi.facts["sample_actions"])

    agent_facts = merge_agent_facts(outcomes)
    if agent_facts:
        for key in (
            "api_base_url",
            "service_url",
            "docs_url",
            "openapi_url",
            "x402_url",
            "x402_urls",
            "brand_facts_url",
            "llms_url",
            "public_endpoint_urls",
            "public_endpoint_count",
            "product_urls",
            "docs_urls",
            "order_urls",
            "register_urls",
            "wallet_guides_urls",
            "payment_protocol",
            "payment_network_names",
            "payment_chain_ids",
            "payment_currency_codes",
            "payment_assets",
            "recommended_client",
            "payment_provider_hints",
            "payment_rail_hints",
            "payment_endpoint_hosts",
            "payment_surface",
            "crypto_only",
            "sample_actions",
            "priced_action_count",
        ):
            if key in agent_facts:
                aggregates[f"agent_{key}"] = agent_facts[key]
        payment_provider_hints = merge_unique_limited(
            payment_provider_hints,
            agent_facts.get("payment_provider_hints", []) if isinstance(agent_facts.get("payment_provider_hints"), list) else [],
            limit=12,
        )
        payment_rail_hints = merge_unique_limited(
            payment_rail_hints,
            agent_facts.get("payment_rail_hints", []) if isinstance(agent_facts.get("payment_rail_hints"), list) else [],
            limit=12,
        )
        payment_endpoint_hosts = merge_unique_limited(
            payment_endpoint_hosts,
            agent_facts.get("payment_endpoint_hosts", []) if isinstance(agent_facts.get("payment_endpoint_hosts"), list) else [],
            limit=12,
        )
        if not payment_surface and isinstance(agent_facts.get("payment_surface"), str):
            payment_surface = agent_facts["payment_surface"]
        if bool(agent_facts.get("crypto_only")):
            crypto_only = True
        if isinstance(agent_facts.get("sample_actions"), list):
            sample_actions = merge_action_samples(sample_actions, agent_facts["sample_actions"])
        if int(agent_facts.get("priced_action_count") or 0) > priced_action_count:
            priced_action_count = int(agent_facts.get("priced_action_count") or 0)

    commerce = outcomes.get("well_known_commerce")
    if commerce and commerce.status == "valid":
        for key in (
            "commerce_status",
            "offer_count",
            "active_offer_count",
            "priced_offer_count",
            "sample_offers",
            "checkout_url",
            "purchase_intent_urls",
            "purchase_intent_url_count",
            "offer_lookup_urls",
            "offer_lookup_url_count",
            "purchase_intent_auth",
            "checkout_state",
            "billing_provider_name",
            "billing_provider_status",
            "live_machine_payment_path_ids",
            "live_machine_payment_path_labels",
            "live_machine_payment_path_kinds",
            "live_machine_payment_path_count",
            "payment_provider_hints",
            "payment_rail_hints",
            "payment_endpoint_hosts",
            "payment_surface",
            "crypto_only",
            "prelaunch",
        ):
            if key in commerce.facts:
                aggregates[f"commerce_{key}"] = commerce.facts[key]
        if "sample_offers" in commerce.facts:
            aggregates["sample_offers"] = commerce.facts["sample_offers"]
        if int(commerce.facts.get("priced_offer_count") or 0) > 0:
            tags.append("offer_surface")
        if bool(commerce.facts.get("prelaunch")):
            tags.append("prelaunch_offer_surface")
        if isinstance(commerce.facts.get("checkout_url"), str) and commerce.facts.get("checkout_url"):
            tags.append("browser_checkout_surface")
            browser_checkout_surface = True
        payment_provider_hints = merge_unique_limited(
            payment_provider_hints,
            commerce.facts.get("payment_provider_hints", []) if isinstance(commerce.facts.get("payment_provider_hints"), list) else [],
            limit=12,
        )
        payment_rail_hints = merge_unique_limited(
            payment_rail_hints,
            commerce.facts.get("payment_rail_hints", []) if isinstance(commerce.facts.get("payment_rail_hints"), list) else [],
            limit=12,
        )
        payment_endpoint_hosts = merge_unique_limited(
            payment_endpoint_hosts,
            commerce.facts.get("payment_endpoint_hosts", []) if isinstance(commerce.facts.get("payment_endpoint_hosts"), list) else [],
            limit=12,
        )
        if not payment_surface and isinstance(commerce.facts.get("payment_surface"), str):
            payment_surface = commerce.facts["payment_surface"]
        if bool(commerce.facts.get("crypto_only")):
            crypto_only = True

        priced_offer_count = int(commerce.facts.get("priced_offer_count") or 0)
        purchase_intent_url_count = int(commerce.facts.get("purchase_intent_url_count") or 0)
        live_machine_payment_path_count = int(commerce.facts.get("live_machine_payment_path_count") or 0)
        payment_required_operation_count = int(openapi.facts.get("payment_required_operation_count") or 0) if openapi and openapi.status == "valid" else 0
        payment_signature_header_count = int(openapi.facts.get("payment_signature_header_count") or 0) if openapi and openapi.status == "valid" else 0
        purchase_intent_auth = normalize_status_value(commerce.facts.get("purchase_intent_auth"))
        commerce_prelaunch = bool(commerce.facts.get("prelaunch"))
        has_machine_payment_contract = (
            live_machine_payment_path_count > 0
            or bool(commerce.facts.get("payment_surface"))
            or bool(commerce.facts.get("payment_provider_hints"))
            or purchase_intent_auth not in {None, "", "none"}
        )
        has_payment_challenge_api = payment_required_operation_count > 0 or payment_signature_header_count > 0 or purchase_intent_auth not in {None, "", "none"}
        if (
            priced_offer_count > 0
            and purchase_intent_url_count > 0
            and has_machine_payment_contract
            and has_payment_challenge_api
            and not commerce_prelaunch
        ):
            commerce_docs_claim_machine_payable = True

    ucp = outcomes.get("well_known_ucp")
    if ucp and ucp.status == "valid":
        for key in (
            "ucp_version",
            "capability_count",
            "capability_names",
            "service_count",
            "shopping_mcp_endpoints",
            "payment_handler_count",
            "payment_handler_names",
            "payment_handler_ids",
            "payment_endpoint_samples",
            "payment_provider_hints",
            "payment_rail_hints",
            "payment_endpoint_hosts",
            "payment_surface",
            "crypto_only",
        ):
            if key in ucp.facts:
                aggregate_key = key if key == "ucp_version" else f"ucp_{key}"
                aggregates[aggregate_key] = ucp.facts[key]
        payment_provider_hints = merge_unique_limited(
            payment_provider_hints,
            ucp.facts.get("payment_provider_hints", []) if isinstance(ucp.facts.get("payment_provider_hints"), list) else [],
            limit=12,
        )
        payment_rail_hints = merge_unique_limited(
            payment_rail_hints,
            ucp.facts.get("payment_rail_hints", []) if isinstance(ucp.facts.get("payment_rail_hints"), list) else [],
            limit=12,
        )
        payment_endpoint_hosts = merge_unique_limited(
            payment_endpoint_hosts,
            ucp.facts.get("payment_endpoint_hosts", []) if isinstance(ucp.facts.get("payment_endpoint_hosts"), list) else [],
            limit=12,
        )
        if not payment_surface and isinstance(ucp.facts.get("payment_surface"), str):
            payment_surface = ucp.facts["payment_surface"]
        if bool(ucp.facts.get("crypto_only")):
            crypto_only = True

    x402 = best_valid_outcome(outcomes, ("x402_json", "x402_well_known", "remote_x402"))
    x402_has_actionable_surface = False
    if x402 and x402.status == "valid":
        for key in (
            "resource_urls",
            "resource_url_count",
            "accept_count",
            "accept_networks",
            "accept_assets",
            "accept_currencies",
            "instructions_char_count",
            "payment_provider_hints",
            "payment_rail_hints",
            "payment_endpoint_hosts",
            "payment_surface",
            "crypto_only",
            "sample_actions",
            "priced_action_count",
        ):
            if key in x402.facts:
                aggregates[f"x402_{key}"] = x402.facts[key]
        payment_provider_hints = merge_unique_limited(
            payment_provider_hints,
            x402.facts.get("payment_provider_hints", []) if isinstance(x402.facts.get("payment_provider_hints"), list) else [],
            limit=12,
        )
        payment_rail_hints = merge_unique_limited(
            payment_rail_hints,
            x402.facts.get("payment_rail_hints", []) if isinstance(x402.facts.get("payment_rail_hints"), list) else [],
            limit=12,
        )
        payment_endpoint_hosts = merge_unique_limited(
            payment_endpoint_hosts,
            x402.facts.get("payment_endpoint_hosts", []) if isinstance(x402.facts.get("payment_endpoint_hosts"), list) else [],
            limit=12,
        )
        if not payment_surface and isinstance(x402.facts.get("payment_surface"), str):
            payment_surface = x402.facts["payment_surface"]
        if bool(x402.facts.get("crypto_only")):
            crypto_only = True
        if isinstance(x402.facts.get("sample_actions"), list):
            sample_actions = merge_action_samples(sample_actions, x402.facts["sample_actions"])
        if int(x402.facts.get("priced_action_count") or 0) > priced_action_count:
            priced_action_count = int(x402.facts.get("priced_action_count") or 0)
        x402_has_actionable_surface = bool(
            int(x402.facts.get("accept_count") or 0) > 0
            or int(x402.facts.get("resource_url_count") or 0) > 0
            or int(x402.facts.get("priced_action_count") or 0) > 0
            or (
                isinstance(x402.facts.get("sample_actions"), list)
                and len(x402.facts["sample_actions"]) > 0
            )
            or (
                isinstance(x402.facts.get("payment_probe_candidates"), list)
                and len(x402.facts["payment_probe_candidates"]) > 0
            )
            or bool(x402.facts.get("payment_required_header_present"))
            or bool(x402.facts.get("www_authenticate_present"))
        )

    if x402_has_actionable_surface:
        payment_provider_hints = merge_unique_limited(payment_provider_hints, ["x402"], limit=12)
        payment_rail_hints = merge_unique_limited(payment_rail_hints, ["x402"], limit=12)
        payment_surface = "x402"
        tags.append("machine_payable")

    payment_probe = outcomes.get("payment_probe")
    if payment_probe and payment_probe.status == "valid":
        for key in (
            "probe_method",
            "probe_url",
            "probe_result",
            "response_keys",
            "response_char_count",
            "payment_required_header_present",
            "www_authenticate_present",
            "candidate_source",
            "candidate_title",
            "candidate_sample_title",
            "candidate_amount",
            "candidate_currency",
            "candidate_body",
            "sample_actions",
            "resource_urls",
            "accept_count",
            "accept_networks",
            "accept_assets",
            "accept_currencies",
            "payment_provider_hints",
            "payment_rail_hints",
            "payment_endpoint_hosts",
            "payment_surface",
            "crypto_only",
        ):
            if key in payment_probe.facts:
                aggregates[f"payment_probe_{key}"] = payment_probe.facts[key]
        payment_provider_hints = merge_unique_limited(
            payment_provider_hints,
            payment_probe.facts.get("payment_provider_hints", []) if isinstance(payment_probe.facts.get("payment_provider_hints"), list) else [],
            limit=12,
        )
        payment_rail_hints = merge_unique_limited(
            payment_rail_hints,
            payment_probe.facts.get("payment_rail_hints", []) if isinstance(payment_probe.facts.get("payment_rail_hints"), list) else [],
            limit=12,
        )
        payment_endpoint_hosts = merge_unique_limited(
            payment_endpoint_hosts,
            payment_probe.facts.get("payment_endpoint_hosts", []) if isinstance(payment_probe.facts.get("payment_endpoint_hosts"), list) else [],
            limit=12,
        )
        if not payment_surface and isinstance(payment_probe.facts.get("payment_surface"), str):
            payment_surface = payment_probe.facts["payment_surface"]
        if bool(payment_probe.facts.get("crypto_only")):
            crypto_only = True
        if isinstance(payment_probe.facts.get("sample_actions"), list):
            sample_actions = merge_action_samples(sample_actions, payment_probe.facts["sample_actions"])
        elif payment_probe.facts.get("candidate_title") or payment_probe.facts.get("probe_url"):
            sample_actions = merge_action_samples(
                sample_actions,
                [
                    {
                        "method": payment_probe.facts.get("probe_method"),
                        "url": payment_probe.facts.get("probe_url"),
                        "title": payment_probe.facts.get("candidate_sample_title") or payment_probe.facts.get("candidate_title"),
                        "amount": payment_probe.facts.get("candidate_amount"),
                        "currency": payment_probe.facts.get("candidate_currency"),
                        "source": "payment_probe",
                    }
                ],
            )
        if payment_probe.facts.get("probe_result") in {"payment_challenge", "success_without_payment"}:
            tags.append("machine_payable")
            tags.append("verified_payment_surface")
            verified_payment_surface = True
        if payment_probe.facts.get("probe_result") == "success_without_payment":
            tags.append("free_tier_or_optional_payment")

    if commerce_docs_claim_machine_payable:
        tags.append("machine_payable")

    observed_schema_facts = merge_observed_schema_facts(outcomes)
    for fact_key, values in observed_schema_facts.items():
        if values:
            aggregates[fact_key] = values

    if sample_actions:
        aggregates["sample_actions"] = sample_actions
    if priced_action_count > 0:
        aggregates["priced_action_count"] = priced_action_count
    if payment_provider_hints:
        aggregates["payment_provider_hints"] = payment_provider_hints
    if payment_rail_hints:
        aggregates["payment_rail_hints"] = payment_rail_hints
    if payment_endpoint_hosts:
        aggregates["payment_endpoint_hosts"] = payment_endpoint_hosts
    if payment_surface:
        aggregates["payment_surface"] = payment_surface
    if payment_surface or payment_provider_hints or payment_rail_hints:
        aggregates["crypto_only"] = crypto_only
    purchase_boundary = infer_purchase_boundary(
        tags=tags,
        products=products,
        commerce=commerce,
        payment_probe=payment_probe,
    )
    control_boundary = infer_control_boundary(
        payment_surface=payment_surface,
        crypto_only=crypto_only,
        purchase_intent_auth=purchase_intent_auth,
        openapi_auth_schemes=openapi_auth_schemes,
        payment_probe=payment_probe,
        browser_checkout_surface=browser_checkout_surface,
    )
    aggregates["purchase_boundary"] = purchase_boundary
    aggregates["control_boundary"] = control_boundary
    aggregates["verified_payment_surface"] = verified_payment_surface

    if "machine_payable" in tags:
        label = "machine_payable"
    elif "offer_surface" in tags:
        label = "offer_surface"
    elif "callable_surface" in tags:
        label = "callable_surface"
    elif "catalog_surface" in tags:
        label = "catalog_surface"
    elif "ai_readable" in tags:
        label = "ai_readable"
    elif limited_outcomes:
        label = "rate_limited"
    elif homepage and homepage.status == "error":
        label = "unreachable"
    elif "crawl_basics" in tags:
        label = "crawl_basics_only"
    else:
        label = "no_clear_signal"

    return CrawlReceipt(
        domain=domain_input.domain,
        rank=domain_input.rank,
        row_index=domain_input.row_index,
        crawled_at=datetime.now(timezone.utc).isoformat(),
        label=label,
        tags=sorted(set(tags)),
        title=title,
        probes=outcomes,
        aggregates=aggregates,
    )
