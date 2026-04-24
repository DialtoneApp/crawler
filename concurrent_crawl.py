#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import hashlib
import http.client
import json
import re
import ssl
import sys
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen


USER_AGENT = "dialtoneapp.com crawler v0.2.0 https://dialtoneapp.com/contact human@dialtoneapp.com"
DEFAULT_ACCEPT = "text/html, application/json, text/plain, application/xml, text/xml, */*;q=0.1"
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)

TEXTUAL_CONTENT_TYPES = {
    "application/ecmascript",
    "application/javascript",
    "application/json",
    "application/ld+json",
    "application/x-javascript",
    "application/x-json",
    "application/xml",
    "text/ecmascript",
    "text/html",
    "text/javascript",
    "text/json",
    "text/markdown",
    "text/plain",
    "text/xml",
}

INTERESTING_PROBE_KEYS = {
    "llms_txt",
    "llms_full_txt",
    "well_known_commerce",
    "well_known_ucp",
    "well_known_agent_json",
    "well_known_agents_json",
    "well_known_agent_card",
    "root_agent_json",
    "openapi_json",
    "x402_json",
    "x402_well_known",
    "products_json",
}

CONTROL_PATH_TEMPLATES = {
    "text": "/dialtoneapp-probe-miss-{token}.txt",
    "xml": "/dialtoneapp-probe-miss-{token}.xml",
    "json_root": "/dialtoneapp-probe-miss-{token}.json",
    "json_well_known": "/.well-known/dialtoneapp-probe-miss-{token}.json",
    "catalog": "/dialtoneapp-probe-products-miss-{token}.json",
}


@dataclass(frozen=True)
class DomainInput:
    row_index: int
    rank: int | None
    domain: str


@dataclass(frozen=True)
class ProbeSpec:
    key: str
    path: str
    validator: str
    max_bytes: int = 16_384
    control_group: str | None = None


@dataclass
class FetchResponse:
    requested_url: str
    final_url: str | None = None
    status: int | None = None
    content_type: str | None = None
    body: bytes = b""
    truncated: bool = False
    error: str | None = None

    @property
    def byte_count(self) -> int:
        return len(self.body)

    @property
    def body_sha256(self) -> str | None:
        if not self.body:
            return None
        return hashlib.sha256(self.body).hexdigest()


@dataclass(frozen=True)
class ProbeOutcome:
    key: str
    path: str
    status: str
    http_status: int | None
    content_type: str | None
    final_url: str | None = None
    byte_count: int = 0
    body_sha256: str | None = None
    detail: str | None = None
    facts: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class CrawlReceipt:
    domain: str
    rank: int | None
    row_index: int
    crawled_at: str
    label: str
    tags: list[str]
    title: str | None
    probes: dict[str, ProbeOutcome]
    aggregates: dict[str, Any] = field(default_factory=dict)


@dataclass
class ReceiptShardState:
    shard_index: int = 1
    record_count: int = 0
    byte_count: int = 0


BASE_PROBES = (
    ProbeSpec("homepage", "/", "homepage", max_bytes=32_768),
    ProbeSpec("robots_txt", "/robots.txt", "robots", control_group="text"),
    ProbeSpec("sitemap_xml", "/sitemap.xml", "sitemap", control_group="xml"),
    ProbeSpec("llms_txt", "/llms.txt", "llms", control_group="text"),
    ProbeSpec("llms_full_txt", "/llms-full.txt", "llms", control_group="text"),
    ProbeSpec("well_known_commerce", "/.well-known/commerce", "commerce", control_group="json_well_known"),
    ProbeSpec("well_known_ucp", "/.well-known/ucp", "ucp", control_group="json_well_known"),
    ProbeSpec("well_known_agent_json", "/.well-known/agent.json", "agent", control_group="json_well_known"),
    ProbeSpec("well_known_agents_json", "/.well-known/agents.json", "agents", control_group="json_well_known"),
    ProbeSpec("well_known_agent_card", "/.well-known/agent-card.json", "agent", control_group="json_well_known"),
    ProbeSpec("root_agent_json", "/agent.json", "agent", control_group="json_root"),
    ProbeSpec("openapi_json", "/openapi.json", "openapi", max_bytes=262_144, control_group="json_root"),
    ProbeSpec("x402_well_known", "/.well-known/x402", "x402", control_group="json_well_known"),
    ProbeSpec("x402_json", "/.well-known/x402.json", "x402", control_group="json_well_known"),
)

PRODUCTS_PROBE = ProbeSpec("products_json", "/products.json", "products", max_bytes=524_288, control_group="catalog")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe top domains and emit compact machine-surface receipts."
    )
    parser.add_argument("--csv", default="./top-1m.csv", help="CSV file containing domains.")
    parser.add_argument(
        "--results-dir",
        default="./results",
        help="Directory where receipt artifacts are written.",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=24,
        help="Maximum number of domains to probe concurrently.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        help="Per-request timeout in seconds.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional maximum number of domains to process.",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=250,
        help="Print progress every N completed domains. Use 0 to disable.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=100,
        help="Persist checkpoint metadata every N completed domains.",
    )
    parser.add_argument(
        "--receipt-shard-max-bytes",
        type=int,
        default=128 * 1024 * 1024,
        help="Rotate receipt NDJSON shards after this many bytes. Use 0 to disable byte-based rotation.",
    )
    parser.add_argument(
        "--receipt-shard-max-records",
        type=int,
        default=100_000,
        help="Rotate receipt NDJSON shards after this many records. Use 0 to disable record-based rotation.",
    )
    parser.add_argument(
        "--no-resume",
        action="store_true",
        help="Start at the beginning instead of resuming from checkpoint.json.",
    )
    return parser.parse_args()


def normalize_domain(value: str) -> str | None:
    domain = value.strip().lower().strip(".")
    if not domain or domain in {"domain", "root_domain", "host"}:
        return None

    if "://" in domain:
        parsed = urlsplit(domain)
        domain = parsed.netloc or parsed.path
    else:
        domain = domain.split("/", 1)[0]

    domain = domain.split("@")[-1].split(":", 1)[0].strip().strip(".")
    if not domain:
        return None

    try:
        return domain.encode("idna").decode("ascii")
    except UnicodeError:
        return None


def iter_domains(csv_path: Path) -> Iterator[DomainInput]:
    with csv_path.open(newline="", encoding="utf-8", errors="replace") as csv_file:
        reader = csv.reader(csv_file)
        for row_index, row in enumerate(reader):
            if not row:
                continue

            rank = None
            candidate = row[0]
            if len(row) > 1 and row[0].strip().isdigit():
                rank = int(row[0].strip())
                candidate = row[1]

            domain = normalize_domain(candidate)
            if domain:
                yield DomainInput(row_index=row_index, rank=rank, domain=domain)


def iter_domains_from_offset(csv_path: Path, start_row_index: int) -> Iterator[DomainInput]:
    for item in iter_domains(csv_path):
        if item.row_index >= start_row_index:
            yield item


def iter_limited(domains: Iterator[DomainInput], limit: int) -> Iterator[DomainInput]:
    if limit < 0:
        return

    for index, item in enumerate(domains):
        if index >= limit:
            break
        yield item


def normalize_content_type(content_type: str | None) -> str | None:
    if not content_type:
        return None
    return content_type.split(";", 1)[0].strip().lower() or None


def is_json_content_type(content_type: str | None) -> bool:
    media_type = normalize_content_type(content_type)
    if not media_type:
        return False
    return media_type == "application/json" or media_type.endswith("+json")


def is_xml_content_type(content_type: str | None) -> bool:
    media_type = normalize_content_type(content_type)
    if not media_type:
        return False
    return media_type in {"application/xml", "text/xml"} or media_type.endswith("+xml")


def is_text_content_type(content_type: str | None) -> bool:
    media_type = normalize_content_type(content_type)
    if not media_type:
        return False
    return media_type in TEXTUAL_CONTENT_TYPES or media_type.startswith("text/")


def is_html_content_type(content_type: str | None) -> bool:
    return normalize_content_type(content_type) == "text/html"


def decode_body(body: bytes) -> str:
    return body.decode("utf-8", errors="replace")


def extract_title(text: str) -> str | None:
    match = TITLE_RE.search(text)
    if not match:
        return None
    title = re.sub(r"\s+", " ", match.group(1)).strip()
    return title or None


def parse_price(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None

    cleaned = value.strip().replace(",", "")
    if not cleaned:
        return None
    try:
        return float(cleaned)
    except ValueError:
        return None


def parse_json_body(fetch: FetchResponse) -> Any:
    if not fetch.body:
        raise ValueError("empty body")

    text = decode_body(fetch.body).strip()
    if not text:
        raise ValueError("blank body")

    return json.loads(text)


def read_limited(response: Any, max_bytes: int) -> tuple[bytes, bool]:
    chunk = response.read(max_bytes + 1)
    if len(chunk) > max_bytes:
        return chunk[:max_bytes], True
    return chunk, False


def fetch_url(url: str, timeout: float, max_bytes: int) -> FetchResponse:
    request = Request(
        url,
        headers={
            "Accept": DEFAULT_ACCEPT,
            "Connection": "close",
            "User-Agent": USER_AGENT,
        },
        method="GET",
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            body, truncated = read_limited(response, max_bytes)
            return FetchResponse(
                requested_url=url,
                final_url=response.geturl(),
                status=response.status,
                content_type=response.headers.get("Content-Type"),
                body=body,
                truncated=truncated,
            )
    except HTTPError as error:
        body = b""
        truncated = False
        try:
            body, truncated = read_limited(error, max_bytes)
        except Exception:
            body = b""
            truncated = False
        return FetchResponse(
            requested_url=url,
            final_url=error.geturl(),
            status=error.code,
            content_type=error.headers.get("Content-Type"),
            body=body,
            truncated=truncated,
            error=f"http_{error.code}",
        )
    except TimeoutError:
        return FetchResponse(requested_url=url, error="timeout")
    except URLError as error:
        return FetchResponse(requested_url=url, error=f"url_error:{error.reason}")
    except (http.client.HTTPException, OSError, ssl.SSLError, UnicodeError) as error:
        return FetchResponse(requested_url=url, error=error.__class__.__name__)


def control_path_for_group(group: str, run_token: str) -> str:
    return CONTROL_PATH_TEMPLATES[group].format(token=run_token)


def responses_match(candidate: FetchResponse, control: FetchResponse) -> bool:
    if candidate.status != 200 or control.status != 200:
        return False
    if normalize_content_type(candidate.content_type) != normalize_content_type(control.content_type):
        return False
    if candidate.byte_count != control.byte_count:
        return False
    return candidate.body_sha256 == control.body_sha256


def validate_homepage(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    if not fetch.body:
        return False, "Empty homepage response body", {}

    text = decode_body(fetch.body)
    lower_text = text.lower()
    title = extract_title(text)
    shopify_hint = (
        "shopify" in lower_text
        or "myshopify.com" in lower_text
        or "cdn.shopify.com" in lower_text
    )

    if is_html_content_type(fetch.content_type) or "<html" in lower_text or title:
        return True, "Fetched homepage HTML", {"title": title, "shopify_hint": shopify_hint}

    if is_text_content_type(fetch.content_type):
        return True, "Fetched non-HTML homepage text", {"title": title, "shopify_hint": shopify_hint}

    return False, "Homepage did not look like HTML or text", {"title": title, "shopify_hint": shopify_hint}


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
    if "<html" in text.lower():
        return False, "llms document looked like HTML fallback", {}
    return True, "Non-empty llms text detected", {"char_count": len(text)}


def validate_commerce(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    text = decode_body(fetch.body).strip()
    if not text:
        return False, "commerce document was empty", {}

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
            interesting = {"offer", "offers", "price", "pricing", "payment", "purchase", "checkout", "commerce"}
            if not any(key in interesting for key in payload.keys()):
                return False, "commerce JSON lacked price or purchase-like keys", {"top_level_keys": keys[:12]}
            return True, "Structured commerce JSON detected", {"top_level_keys": keys[:12]}
        return True, "Structured commerce list detected", {"item_count": len(payload)}

    lower_text = text.lower()
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

    capabilities = ucp.get("capabilities") if isinstance(ucp.get("capabilities"), dict) else {}
    services = ucp.get("services") if isinstance(ucp.get("services"), dict) else {}
    version = ucp.get("version")
    if not version and not capabilities and not services:
        return False, "UCP payload lacked version, capabilities, and services", {}

    return True, "Valid UCP document detected", {
        "ucp_version": version,
        "capability_count": len(capabilities),
        "service_count": len(services),
    }


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

    return True, "Agent-like JSON detected", {"top_level_keys": sorted(str(key) for key in keys)[:12]}


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

    lower_text = decode_body(fetch.body).lower()
    return True, "OpenAPI document detected", {
        "openapi_version": version,
        "path_count": len(paths),
        "auth_schemes": sorted(set(auth_schemes))[:12],
        "mentions_402": "402" in lower_text or "payment required" in lower_text,
    }


def validate_x402(fetch: FetchResponse) -> tuple[bool, str, dict[str, Any]]:
    if fetch.truncated:
        return False, f"x402 response truncated at {fetch.byte_count} bytes", {"truncated": True}

    text = decode_body(fetch.body).strip()
    if not text:
        return False, "x402 document was empty", {}

    lower_text = text.lower()
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
            return True, "x402-like JSON detected", {"top_level_keys": sorted(keys)[:12]}

        if isinstance(payload, list) and payload:
            return True, "x402-like JSON list detected", {"item_count": len(payload)}

        return False, "x402 payload was empty or unsupported", {}

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

    for product in products:
        if not isinstance(product, dict):
            continue
        product_count += 1
        title = product.get("title")
        if isinstance(title, str) and title and len(sample_titles) < 3:
            sample_titles.append(title[:120])

        variants = product.get("variants")
        if not isinstance(variants, list):
            continue

        for variant in variants:
            if not isinstance(variant, dict):
                continue
            variant_count += 1
            price = parse_price(variant.get("price"))
            currency = variant.get("currency") or product.get("currency")
            if isinstance(currency, str) and currency:
                currencies.add(currency.upper()[:8])
            if price is None:
                continue
            priced_variant_count += 1
            min_price = price if min_price is None else min(min_price, price)
            max_price = price if max_price is None else max(max_price, price)

    if product_count == 0:
        return False, "products list did not contain product objects", {}

    return True, "Usable products catalog detected", {
        "product_count": product_count,
        "variant_count": variant_count,
        "priced_variant_count": priced_variant_count,
        "currency_count": len(currencies),
        "min_price": min_price,
        "max_price": max_price,
        "sample_titles": sample_titles,
    }


VALIDATORS = {
    "homepage": validate_homepage,
    "robots": validate_robots,
    "sitemap": validate_sitemap,
    "llms": validate_llms,
    "commerce": validate_commerce,
    "ucp": validate_ucp,
    "agent": validate_agent,
    "agents": validate_agents,
    "openapi": validate_openapi,
    "x402": validate_x402,
    "products": validate_products,
}


def build_outcome(
    spec: ProbeSpec,
    fetch: FetchResponse,
    control: FetchResponse | None = None,
) -> ProbeOutcome:
    if fetch.status == 404:
        return ProbeOutcome(
            key=spec.key,
            path=spec.path,
            status="missing",
            http_status=fetch.status,
            content_type=fetch.content_type,
            final_url=fetch.final_url,
            byte_count=fetch.byte_count,
            body_sha256=fetch.body_sha256,
            detail="404 not found",
        )

    if fetch.status is None and fetch.error:
        return ProbeOutcome(
            key=spec.key,
            path=spec.path,
            status="error",
            http_status=None,
            content_type=fetch.content_type,
            final_url=fetch.final_url,
            byte_count=fetch.byte_count,
            body_sha256=fetch.body_sha256,
            detail=fetch.error,
        )

    if fetch.status != 200:
        return ProbeOutcome(
            key=spec.key,
            path=spec.path,
            status="http_error",
            http_status=fetch.status,
            content_type=fetch.content_type,
            final_url=fetch.final_url,
            byte_count=fetch.byte_count,
            body_sha256=fetch.body_sha256,
            detail=f"HTTP {fetch.status}",
        )

    if spec.key != "homepage" and fetch.byte_count == 0:
        return ProbeOutcome(
            key=spec.key,
            path=spec.path,
            status="invalid",
            http_status=fetch.status,
            content_type=fetch.content_type,
            final_url=fetch.final_url,
            byte_count=fetch.byte_count,
            body_sha256=fetch.body_sha256,
            detail="Empty response body",
        )

    if control and responses_match(fetch, control):
        return ProbeOutcome(
            key=spec.key,
            path=spec.path,
            status="fallback",
            http_status=fetch.status,
            content_type=fetch.content_type,
            final_url=fetch.final_url,
            byte_count=fetch.byte_count,
            body_sha256=fetch.body_sha256,
            detail="Response matched control-path fallback",
        )

    validator = VALIDATORS[spec.validator]
    is_valid, detail, facts = validator(fetch)
    return ProbeOutcome(
        key=spec.key,
        path=spec.path,
        status="valid" if is_valid else "invalid",
        http_status=fetch.status,
        content_type=fetch.content_type,
        final_url=fetch.final_url,
        byte_count=fetch.byte_count,
        body_sha256=fetch.body_sha256,
        detail=detail,
        facts=facts,
    )


def serialize_probe_outcome(outcome: ProbeOutcome) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "path": outcome.path,
        "status": outcome.status,
        "http_status": outcome.http_status,
        "content_type": normalize_content_type(outcome.content_type),
        "final_url": outcome.final_url,
        "byte_count": outcome.byte_count,
        "body_sha256": outcome.body_sha256,
        "detail": outcome.detail,
    }
    if outcome.facts:
        payload["facts"] = outcome.facts
    return payload


def serialize_receipt(receipt: CrawlReceipt) -> dict[str, Any]:
    return {
        "domain": receipt.domain,
        "rank": receipt.rank,
        "row_index": receipt.row_index,
        "crawled_at": receipt.crawled_at,
        "label": receipt.label,
        "tags": receipt.tags,
        "title": receipt.title,
        "aggregates": receipt.aggregates,
        "probes": {key: serialize_probe_outcome(value) for key, value in receipt.probes.items()},
    }


def build_control_fetch(
    domain: str,
    control_group: str,
    run_token: str,
    timeout: float,
    cache: dict[str, FetchResponse],
) -> FetchResponse:
    if control_group not in cache:
        cache[control_group] = fetch_url(
            f"https://{domain}{control_path_for_group(control_group, run_token)}",
            timeout=timeout,
            max_bytes=4_096,
        )
    return cache[control_group]


def should_probe_products(homepage_fetch: FetchResponse, homepage_outcome: ProbeOutcome, outcomes: dict[str, ProbeOutcome]) -> bool:
    if outcomes.get("well_known_ucp", ProbeOutcome("", "", "", None, None)).status == "valid":
        return True

    facts = homepage_outcome.facts if homepage_outcome.status == "valid" else {}
    if facts.get("shopify_hint") is True:
        return True

    text = decode_body(homepage_fetch.body).lower() if homepage_fetch.body else ""
    shopify_markers = ("shopify", "myshopify.com", "cdn.shopify.com", "shopify.theme")
    return any(marker in text for marker in shopify_markers)


def classify_receipt(
    domain_input: DomainInput,
    outcomes: dict[str, ProbeOutcome],
) -> CrawlReceipt:
    tags: list[str] = []
    aggregates: dict[str, Any] = {}

    homepage = outcomes.get("homepage")
    title = None
    if homepage and homepage.facts:
        title = homepage.facts.get("title")
        if homepage.facts.get("shopify_hint"):
            tags.append("shopify_hint")

    valid_keys = {key for key, outcome in outcomes.items() if outcome.status == "valid"}

    if "llms_txt" in valid_keys or "llms_full_txt" in valid_keys:
        tags.append("ai_readable")
    if "well_known_ucp" in valid_keys or "products_json" in valid_keys:
        tags.append("catalog_surface")
    if {"openapi_json", "well_known_commerce", "well_known_agent_json", "well_known_agents_json", "root_agent_json"} & valid_keys:
        tags.append("callable_surface")
    if {"x402_json", "x402_well_known"} & valid_keys:
        tags.append("machine_payable")
    if {"robots_txt", "sitemap_xml"} <= valid_keys:
        tags.append("crawl_basics")

    products = outcomes.get("products_json")
    if products and products.status == "valid":
        for key in ("product_count", "variant_count", "priced_variant_count", "currency_count", "min_price", "max_price", "sample_titles"):
            if key in products.facts:
                aggregates[key] = products.facts[key]

    openapi = outcomes.get("openapi_json")
    if openapi and openapi.status == "valid":
        for key in ("path_count", "auth_schemes", "mentions_402"):
            if key in openapi.facts:
                aggregates[f"openapi_{key}"] = openapi.facts[key]

    ucp = outcomes.get("well_known_ucp")
    if ucp and ucp.status == "valid":
        for key in ("ucp_version", "capability_count", "service_count"):
            if key in ucp.facts:
                aggregate_key = key if key == "ucp_version" else f"ucp_{key}"
                aggregates[aggregate_key] = ucp.facts[key]

    if "machine_payable" in tags:
        label = "machine_payable"
    elif "callable_surface" in tags:
        label = "callable_surface"
    elif "catalog_surface" in tags:
        label = "catalog_surface"
    elif "ai_readable" in tags:
        label = "ai_readable"
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


def probe_domain(domain_input: DomainInput, timeout: float, run_token: str) -> CrawlReceipt:
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

    return classify_receipt(domain_input, outcomes)


def submit_next(
    executor: ThreadPoolExecutor,
    futures: dict[Future[CrawlReceipt], DomainInput],
    domains: Iterator[DomainInput],
    timeout: float,
    run_token: str,
) -> bool:
    try:
        domain_input = next(domains)
    except StopIteration:
        return False

    future = executor.submit(probe_domain, domain_input, timeout, run_token)
    futures[future] = domain_input
    return True


def load_checkpoint(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def write_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


class ReceiptShardWriter:
    def __init__(
        self,
        receipts_dir: Path,
        *,
        max_bytes: int,
        max_records: int,
        initial_state: ReceiptShardState | None = None,
    ) -> None:
        self.receipts_dir = receipts_dir
        self.max_bytes = max(0, max_bytes)
        self.max_records = max(0, max_records)
        self.state = initial_state or ReceiptShardState()
        self._file: Any | None = None

    def __enter__(self) -> "ReceiptShardWriter":
        self.receipts_dir.mkdir(parents=True, exist_ok=True)
        self._open_current_shard()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    @property
    def current_path(self) -> Path:
        return self.receipts_dir / f"receipt-{self.state.shard_index:06d}.ndjson"

    def _open_current_shard(self) -> None:
        if self._file is not None:
            self._file.close()
        self._file = self.current_path.open("a", encoding="utf-8")

        try:
            existing_size = self.current_path.stat().st_size
        except OSError:
            existing_size = 0

        if existing_size > self.state.byte_count:
            self.state.byte_count = existing_size

    def _rotate(self) -> None:
        if self._file is not None:
            self._file.close()
        self.state = ReceiptShardState(shard_index=self.state.shard_index + 1)
        self._open_current_shard()

    def _should_rotate(self, encoded_length: int) -> bool:
        if self.state.record_count == 0:
            return False
        if self.max_records and self.state.record_count >= self.max_records:
            return True
        if self.max_bytes and (self.state.byte_count + encoded_length) > self.max_bytes:
            return True
        return False

    def write_line(self, line: str) -> None:
        encoded_length = len((line + "\n").encode("utf-8"))
        if self._should_rotate(encoded_length):
            self._rotate()

        if self._file is None:
            self._open_current_shard()

        self._file.write(line)
        self._file.write("\n")
        self.state.record_count += 1
        self.state.byte_count += encoded_length

    def flush(self) -> None:
        if self._file is not None:
            self._file.flush()

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def snapshot(self) -> dict[str, int]:
        return {
            "shard_index": self.state.shard_index,
            "record_count": self.state.record_count,
            "byte_count": self.state.byte_count,
        }


def has_interesting_signal(receipt: CrawlReceipt) -> bool:
    return any(
        receipt.probes.get(key) and receipt.probes[key].status == "valid"
        for key in INTERESTING_PROBE_KEYS
    )


def build_checkpoint_payload(
    *,
    completed: int,
    found: int,
    next_row_index: int,
    started_at: float,
    label_counts: Counter[str],
    probe_status_counts: Counter[str],
    receipt_shard: dict[str, int],
) -> dict[str, Any]:
    return {
        "completed": completed,
        "found": found,
        "next_row_index": next_row_index,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(max(time.monotonic() - started_at, 0.0), 3),
        "label_counts": dict(sorted(label_counts.items())),
        "probe_status_counts": dict(sorted(probe_status_counts.items())),
        "receipt_shard": receipt_shard,
    }


def crawl(args: argparse.Namespace) -> int:
    csv_path = Path(args.csv)
    results_dir = Path(args.results_dir)
    receipts_dir = results_dir / "receipts"
    positives_dir = results_dir / "positives"
    checkpoint_path = results_dir / "checkpoint.json"

    if args.concurrency < 1:
        print("--concurrency must be at least 1", file=sys.stderr)
        return 2
    if args.receipt_shard_max_bytes < 0:
        print("--receipt-shard-max-bytes must be 0 or greater", file=sys.stderr)
        return 2
    if args.receipt_shard_max_records < 0:
        print("--receipt-shard-max-records must be 0 or greater", file=sys.stderr)
        return 2
    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}", file=sys.stderr)
        return 2

    results_dir.mkdir(parents=True, exist_ok=True)
    receipts_dir.mkdir(parents=True, exist_ok=True)
    positives_dir.mkdir(parents=True, exist_ok=True)

    checkpoint = None if args.no_resume else load_checkpoint(checkpoint_path)
    start_row_index = int(checkpoint.get("next_row_index", 0)) if checkpoint else 0
    completed = int(checkpoint.get("completed", 0)) if checkpoint else 0
    found = int(checkpoint.get("found", 0)) if checkpoint else 0
    label_counts: Counter[str] = Counter(checkpoint.get("label_counts", {})) if checkpoint else Counter()
    probe_status_counts: Counter[str] = Counter(checkpoint.get("probe_status_counts", {})) if checkpoint else Counter()
    checkpoint_receipt_shard = checkpoint.get("receipt_shard", {}) if checkpoint else {}
    receipt_shard_state = ReceiptShardState(
        shard_index=max(1, int(checkpoint_receipt_shard.get("shard_index", 1))),
        record_count=max(0, int(checkpoint_receipt_shard.get("record_count", 0))),
        byte_count=max(0, int(checkpoint_receipt_shard.get("byte_count", 0))),
    )

    if checkpoint and not args.no_resume:
        print(
            f"resuming from row_index={start_row_index} completed={completed} found={found} shard=receipt-{receipt_shard_state.shard_index:06d}",
            file=sys.stderr,
        )
    elif args.no_resume and any(receipts_dir.glob("receipt-*.ndjson")):
        print(
            f"--no-resume requested; appending fresh rows into {receipts_dir}",
            file=sys.stderr,
        )

    domains = iter_domains_from_offset(csv_path, start_row_index)
    if args.limit is not None:
        domains = iter_limited(domains, args.limit)

    started_at = time.monotonic()
    futures: dict[Future[CrawlReceipt], DomainInput] = {}
    run_token = hex(int(started_at * 1_000_000))[2:]
    last_completed_row_index = start_row_index

    with ReceiptShardWriter(
        receipts_dir,
        max_bytes=args.receipt_shard_max_bytes,
        max_records=args.receipt_shard_max_records,
        initial_state=receipt_shard_state,
    ) as receipt_writer:
        with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
            for _ in range(args.concurrency):
                if not submit_next(executor, futures, domains, args.timeout, run_token):
                    break

            while futures:
                done, _ = wait(futures, return_when=FIRST_COMPLETED)
                for future in done:
                    domain_input = futures.pop(future)

                    try:
                        receipt = future.result()
                    except Exception as error:
                        print(f"ERROR {domain_input.domain}: {error}", file=sys.stderr)
                        receipt = CrawlReceipt(
                            domain=domain_input.domain,
                            rank=domain_input.rank,
                            row_index=domain_input.row_index,
                            crawled_at=datetime.now(timezone.utc).isoformat(),
                            label="internal_error",
                            tags=[],
                            title=None,
                            probes={},
                            aggregates={},
                        )

                    completed += 1
                    last_completed_row_index = max(last_completed_row_index, domain_input.row_index + 1)
                    label_counts[receipt.label] += 1
                    for outcome in receipt.probes.values():
                        probe_status_counts[f"{outcome.key}:{outcome.status}"] += 1

                    serialized = serialize_receipt(receipt)
                    receipt_writer.write_line(json.dumps(serialized, separators=(",", ":"), sort_keys=True))

                    if has_interesting_signal(receipt):
                        found += 1
                        interesting_keys = sorted(
                            key for key in INTERESTING_PROBE_KEYS
                            if receipt.probes.get(key) and receipt.probes[key].status == "valid"
                        )
                        positive_path = positives_dir / f"{receipt.domain}.json"
                        positive_path.write_text(
                            json.dumps(serialized, indent=2, sort_keys=True),
                            encoding="utf-8",
                        )
                        print(f"FOUND {receipt.domain} {receipt.label} {','.join(interesting_keys)}")

                    if args.progress_every and completed % args.progress_every == 0:
                        elapsed = max(time.monotonic() - started_at, 0.001)
                        rate = completed / elapsed
                        print(
                            f"progress completed={completed} found={found} rate={rate:.1f}/s labels={dict(label_counts.most_common(5))}",
                            file=sys.stderr,
                        )

                    if args.checkpoint_every and completed % args.checkpoint_every == 0:
                        receipt_writer.flush()
                        write_checkpoint(
                            checkpoint_path,
                            build_checkpoint_payload(
                                completed=completed,
                                found=found,
                                next_row_index=last_completed_row_index,
                                started_at=started_at,
                                label_counts=label_counts,
                                probe_status_counts=probe_status_counts,
                                receipt_shard=receipt_writer.snapshot(),
                            ),
                        )

                    submit_next(executor, futures, domains, args.timeout, run_token)

    write_checkpoint(
        checkpoint_path,
        build_checkpoint_payload(
            completed=completed,
            found=found,
            next_row_index=last_completed_row_index,
            started_at=started_at,
            label_counts=label_counts,
            probe_status_counts=probe_status_counts,
            receipt_shard=receipt_writer.snapshot(),
        ),
    )
    print(
        f"done completed={completed} found={found} shard=receipt-{receipt_writer.state.shard_index:06d} labels={dict(label_counts.most_common())}",
        file=sys.stderr,
    )
    return 0


def main() -> int:
    return crawl(parse_args())


if __name__ == "__main__":
    raise SystemExit(main())
