from __future__ import annotations

import json
import re
from typing import Any

from .http_client import fetch_url, parse_json_body
from .models import FetchResponse, ProbeOutcome
from .validators_catalog import validate_products


SHOPIFY_UCP_AGENT_PROFILE = "https://shopify.dev/ucp/agent-profiles/2026-04-08/valid-with-capabilities.json"
TOKEN_RE = re.compile(r"[a-zA-Z]{4,}")

CATEGORY_QUERY_TOKENS = {
    "accessories",
    "anklets",
    "bag",
    "bags",
    "beauty",
    "boots",
    "bra",
    "bridal",
    "dress",
    "dresses",
    "earring",
    "earrings",
    "footwear",
    "gown",
    "gowns",
    "jacket",
    "jackets",
    "jewellery",
    "jewelry",
    "lingerie",
    "loafers",
    "necklace",
    "ring",
    "rings",
    "shoes",
    "skirt",
    "sneakers",
    "suit",
    "tuxedo",
    "veil",
    "wedding",
}

STOPWORD_QUERY_TOKENS = {
    "about",
    "bridesmaid",
    "bridesmaids",
    "bridalwear",
    "buy",
    "collection",
    "collections",
    "david",
    "davids",
    "free",
    "gift",
    "gifts",
    "home",
    "india",
    "kids",
    "life",
    "luxury",
    "mens",
    "online",
    "official",
    "sale",
    "shop",
    "shopping",
    "site",
    "store",
    "style",
    "styles",
    "wallace",
    "womens",
}


def tokenize_query_candidates(*values: str | None) -> list[str]:
    tokens: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value:
            continue
        for match in TOKEN_RE.finditer(value.lower()):
            token = match.group(0)
            if token not in tokens:
                tokens.append(token)
    return tokens


def build_ucp_catalog_queries(domain: str, homepage_title: str | None) -> list[str | None]:
    title_tokens = tokenize_query_candidates(homepage_title)
    domain_tokens = tokenize_query_candidates(domain.replace(".", " ").replace("-", " "))

    queries: list[str | None] = []
    for token in title_tokens + domain_tokens:
        if token in CATEGORY_QUERY_TOKENS and token not in queries:
            queries.append(token)
    for token in title_tokens + domain_tokens:
        if token in STOPWORD_QUERY_TOKENS:
            continue
        if token in queries:
            continue
        queries.append(token)
        if len(queries) >= 4:
            break
    if None not in queries:
        queries.append(None)
    return queries[:5]


def build_search_catalog_payload(query: str | None, *, country_code: str | None = None) -> bytes:
    catalog: dict[str, Any] = {
        "pagination": {"limit": 3},
        "filters": {"available": True},
        "context": {},
    }
    if isinstance(country_code, str) and country_code:
        catalog["context"]["address_country"] = country_code
    if isinstance(query, str) and query:
        catalog["query"] = query

    payload = {
        "jsonrpc": "2.0",
        "method": "tools/call",
        "id": 1,
        "params": {
            "name": "search_catalog",
            "arguments": {
                "meta": {"ucp-agent": {"profile": SHOPIFY_UCP_AGENT_PROFILE}},
                "catalog": catalog,
            },
        },
    }
    return json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")


def parse_ucp_tool_content(payload: Any) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None

    result = payload.get("result") if isinstance(payload.get("result"), dict) else {}
    structured_content = result.get("structuredContent")
    if isinstance(structured_content, dict):
        return structured_content

    content_items = result.get("content") if isinstance(result.get("content"), list) else []
    for item in content_items:
        if not isinstance(item, dict):
            continue
        text = item.get("text")
        if not isinstance(text, str) or not text.strip():
            continue
        try:
            candidate = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(candidate, dict):
            return candidate
    return None


def extract_ucp_products(fetch: FetchResponse) -> list[dict[str, Any]] | None:
    try:
        payload = parse_json_body(fetch)
    except (ValueError, json.JSONDecodeError):
        return None
    content_payload = parse_ucp_tool_content(payload)
    if not isinstance(content_payload, dict):
        return None
    products = content_payload.get("products")
    if not isinstance(products, list):
        return None
    return [product for product in products if isinstance(product, dict)]


def synthesize_products_fetch(products: list[dict[str, Any]], endpoint: str) -> FetchResponse:
    body = json.dumps({"products": products}, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    return FetchResponse(
        requested_url=endpoint,
        request_method="POST",
        request_content_type="application/json",
        final_url=endpoint,
        status=200,
        content_type="application/json",
        body=body,
    )


def score_product_facts(facts: dict[str, Any]) -> int:
    score = 0
    if (facts.get("max_price") or 0) not in {None, 0, 0.0}:
        score += 100
    if int(facts.get("priced_variant_count") or 0) > 0:
        score += 50
    if int(facts.get("product_count") or 0) > 0:
        score += 10
    if isinstance(facts.get("sample_products"), list):
        score += min(len(facts["sample_products"]), 3) * 5
    return score


def probe_ucp_catalog_products(
    *,
    domain: str,
    homepage_title: str | None,
    ucp_facts: dict[str, Any],
    timeout: float,
) -> ProbeOutcome | None:
    endpoints = ucp_facts.get("shopping_mcp_endpoints")
    if not isinstance(endpoints, list):
        return None
    capability_names = ucp_facts.get("capability_names")
    if not isinstance(capability_names, list) or "dev.ucp.shopping.catalog.search" not in capability_names:
        return None

    queries = build_ucp_catalog_queries(domain, homepage_title)
    country_code = "US"
    if domain.endswith(".co") or domain.endswith(".in"):
        country_code = "IN"

    best_outcome: ProbeOutcome | None = None
    best_score = -1

    for endpoint in endpoints:
        if not isinstance(endpoint, str) or not endpoint:
            continue
        for query in queries:
            fetch = fetch_url(
                endpoint,
                timeout=timeout,
                max_bytes=262_144,
                method="POST",
                body=build_search_catalog_payload(query, country_code=country_code),
                content_type="application/json",
            )
            if fetch.status != 200 or fetch.truncated:
                continue
            products = extract_ucp_products(fetch)
            if not products:
                continue
            synthetic_fetch = synthesize_products_fetch(products, endpoint)
            valid, detail, facts = validate_products(synthetic_fetch)
            if not valid:
                continue

            facts = dict(facts)
            facts["catalog_source"] = "ucp_search_catalog"
            facts["catalog_query"] = query
            facts["catalog_endpoint"] = endpoint

            outcome = ProbeOutcome(
                key="api_products",
                path=endpoint,
                status="valid",
                http_status=fetch.status,
                content_type=fetch.content_type,
                final_url=fetch.final_url,
                byte_count=fetch.byte_count,
                body_sha256=fetch.body_sha256,
                detail=f"{detail} via UCP search_catalog",
                facts=facts,
            )
            score = score_product_facts(facts)
            if score > best_score:
                best_outcome = outcome
                best_score = score
            if score >= 150:
                return outcome

    return best_outcome
