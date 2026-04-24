from __future__ import annotations

import base64
import html
import json
import re
from typing import Any
from urllib.parse import urljoin, urlsplit

from .constants import PAYMENT_PROVIDER_MARKERS, PAYMENT_RAIL_MARKERS, TEXTUAL_CONTENT_TYPES, TITLE_RE
from .models import FetchResponse


TEMPLATE_PARAMETER_RE = re.compile(r"\{([^{}]+)\}")
COLON_PARAMETER_RE = re.compile(r"(?:(?<=/)|^):([a-zA-Z_][a-zA-Z0-9_]*)")
ABSOLUTE_URL_RE = re.compile(r"https?://[^\s<>()\"']+")
LINK_TAG_RE = re.compile(r"<link\b[^>]*>", re.IGNORECASE)
META_TAG_RE = re.compile(r"<meta\b[^>]*>", re.IGNORECASE)
HTML_ATTR_RE = re.compile(r'([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*=\s*("([^"]*)"|\'([^\']*)\'|([^\s>]+))')
HTTP_METHOD_PREFIX_RE = re.compile(r"^\s*([A-Z]+)\s+(/.*)$")


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
    title = " ".join(match.group(1).split())
    return title or None


def final_host(url: str | None) -> str | None:
    if not url:
        return None
    try:
        return (urlsplit(url).netloc or "").lower() or None
    except ValueError:
        return None


def resolve_url(base_url: str | None, candidate: Any) -> str | None:
    if not isinstance(candidate, str):
        return None
    candidate = candidate.strip()
    if not candidate:
        return None
    if candidate.startswith(("http://", "https://")):
        return candidate
    if not base_url:
        return None
    try:
        return urljoin(base_url, candidate)
    except ValueError:
        return None


def resolve_http_url(base_url: str | None, candidate: Any) -> str | None:
    resolved = resolve_url(base_url, candidate)
    if not isinstance(resolved, str) or not resolved:
        return None
    try:
        parsed = urlsplit(resolved)
    except ValueError:
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return resolved


def resolve_openapi_path(base_url: str | None, candidate: Any) -> str | None:
    if not isinstance(candidate, str):
        return None
    candidate = candidate.strip()
    if not candidate:
        return None
    if candidate.startswith(("http://", "https://")):
        return candidate
    if not base_url:
        return None
    try:
        parsed = urlsplit(base_url)
    except ValueError:
        return resolve_url(base_url, candidate)
    if not parsed.scheme or not parsed.netloc:
        return resolve_url(base_url, candidate)

    if not candidate.startswith("/"):
        return resolve_url(base_url, candidate)

    origin = f"{parsed.scheme}://{parsed.netloc}"
    base_path = parsed.path.rstrip("/")
    if base_path:
        last_segment = base_path.rsplit("/", 1)[-1]
        if "." in last_segment:
            base_path = base_path.rsplit("/", 1)[0] if "/" in base_path else ""
    target_path = f"{base_path}{candidate}" if base_path else candidate
    return resolve_url(origin, target_path)


def split_http_method_prefix(value: str | None) -> tuple[str | None, str | None]:
    if not isinstance(value, str):
        return None, None
    cleaned = value.strip()
    if not cleaned:
        return None, None
    match = HTTP_METHOD_PREFIX_RE.match(cleaned)
    if not match:
        return None, cleaned
    return match.group(1).strip().upper(), match.group(2).strip()


def extract_absolute_urls(text: str) -> list[str]:
    if not isinstance(text, str) or not text:
        return []
    results: list[str] = []
    for match in ABSOLUTE_URL_RE.findall(text):
        cleaned = match.rstrip("),.;:!?]}>\"'")
        if cleaned and cleaned not in results:
            results.append(cleaned)
    return results


def derive_well_known_url(base_url: str | None, well_known_path: str) -> str | None:
    if not isinstance(well_known_path, str) or not well_known_path.strip():
        return None
    if not isinstance(base_url, str) or not base_url.strip():
        return None
    try:
        parsed = urlsplit(base_url)
    except ValueError:
        return None
    if not parsed.scheme or not parsed.netloc:
        return None
    origin = f"{parsed.scheme}://{parsed.netloc}"
    normalized_path = well_known_path if well_known_path.startswith("/") else f"/{well_known_path}"
    return resolve_url(origin, normalized_path)


def derive_x402_discovery_url(base_url: str | None) -> str | None:
    return derive_well_known_url(base_url, "/.well-known/x402.json")


def decode_json_like_header(value: str | None) -> Any | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not text:
        return None
    if text[:1] in "[{":
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None

    compact = text.replace("\n", "").replace(" ", "")
    if not compact:
        return None
    padding = "=" * (-len(compact) % 4)
    try:
        decoded = base64.urlsafe_b64decode(f"{compact}{padding}".encode("ascii"))
    except (ValueError, UnicodeEncodeError):
        return None

    decoded_text = decoded.decode("utf-8", errors="replace").strip()
    if not decoded_text or decoded_text[:1] not in "[{":
        return None
    try:
        return json.loads(decoded_text)
    except json.JSONDecodeError:
        return None


def parse_html_attributes(tag_text: str) -> dict[str, str]:
    attributes: dict[str, str] = {}
    if not isinstance(tag_text, str) or not tag_text:
        return attributes
    for match in HTML_ATTR_RE.finditer(tag_text):
        key = (match.group(1) or "").strip().lower()
        value = match.group(3) or match.group(4) or match.group(5) or ""
        if key and key not in attributes:
            attributes[key] = html.unescape(value.strip())
    return attributes


def extract_link_urls_by_rel(text: str, *, rel_token: str, base_url: str | None = None) -> list[str]:
    if not isinstance(text, str) or not text:
        return []
    rel_lower = rel_token.strip().lower()
    if not rel_lower:
        return []
    urls: list[str] = []
    for match in LINK_TAG_RE.finditer(text):
        attrs = parse_html_attributes(match.group(0))
        rel_value = attrs.get("rel", "")
        href_value = attrs.get("href")
        if not href_value:
            continue
        rel_tokens = {part.strip().lower() for part in rel_value.split() if part.strip()}
        if rel_lower not in rel_tokens:
            continue
        resolved_url = resolve_http_url(base_url, href_value)
        if resolved_url and resolved_url not in urls:
            urls.append(resolved_url)
    return urls


def extract_meta_content(
    text: str,
    *,
    property_names: tuple[str, ...] = (),
    name_names: tuple[str, ...] = (),
    itemprop_names: tuple[str, ...] = (),
    base_url: str | None = None,
    resolve_as_url: bool = False,
) -> str | None:
    if not isinstance(text, str) or not text:
        return None

    property_candidates = tuple(
        value.strip().lower()
        for value in property_names
        if isinstance(value, str) and value.strip()
    )
    name_candidates = tuple(
        value.strip().lower()
        for value in name_names
        if isinstance(value, str) and value.strip()
    )
    itemprop_candidates = tuple(
        value.strip().lower()
        for value in itemprop_names
        if isinstance(value, str) and value.strip()
    )
    tags = [parse_html_attributes(match.group(0)) for match in META_TAG_RE.finditer(text)]
    if not tags:
        return None

    def read_value(raw_value: str | None) -> str | None:
        if not isinstance(raw_value, str):
            return None
        cleaned = html.unescape(raw_value.strip())
        if not cleaned:
            return None
        if resolve_as_url:
            return resolve_http_url(base_url, cleaned)
        return cleaned

    for property_name in property_candidates:
        for attrs in tags:
            if attrs.get("property", "").strip().lower() != property_name:
                continue
            value = read_value(attrs.get("content"))
            if value:
                return value

    for name_name in name_candidates:
        for attrs in tags:
            if attrs.get("name", "").strip().lower() != name_name:
                continue
            value = read_value(attrs.get("content"))
            if value:
                return value

    for itemprop_name in itemprop_candidates:
        for attrs in tags:
            if attrs.get("itemprop", "").strip().lower() != itemprop_name:
                continue
            value = read_value(attrs.get("content"))
            if value:
                return value

    return None


def extract_favicon_url(text: str, *, base_url: str | None = None) -> str | None:
    if not isinstance(text, str) or not text:
        return None

    tags = [parse_html_attributes(match.group(0)) for match in LINK_TAG_RE.finditer(text)]
    if not tags:
        return None

    relation_priorities = (
        ("shortcut", "icon"),
        ("icon",),
        ("apple-touch-icon-precomposed",),
        ("apple-touch-icon",),
        ("mask-icon",),
    )

    for required_tokens in relation_priorities:
        for attrs in tags:
            rel_tokens = {
                part.strip().lower()
                for part in attrs.get("rel", "").split()
                if part.strip()
            }
            if not rel_tokens or not all(token in rel_tokens for token in required_tokens):
                continue
            href_value = attrs.get("href")
            if not href_value:
                continue
            resolved = resolve_http_url(base_url, href_value)
            if resolved:
                return resolved

    return None


def extract_template_parameters(value: str | None) -> list[str]:
    if not isinstance(value, str) or not value:
        return []
    results: list[str] = []
    for match in TEMPLATE_PARAMETER_RE.finditer(value):
        candidate = match.group(1).strip()
        if candidate and candidate not in results:
            results.append(candidate)
    for match in COLON_PARAMETER_RE.finditer(value):
        candidate = match.group(1).strip()
        if candidate and candidate not in results:
            results.append(candidate)
    return results


def has_template_parameters(value: str | None) -> bool:
    return bool(extract_template_parameters(value))


def fill_template_parameters(value: str | None, replacements: dict[str, Any]) -> str | None:
    if not isinstance(value, str) or not value:
        return None

    def replace(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        replacement = replacements.get(key)
        if replacement is None:
            return match.group(0)
        cleaned = str(replacement).strip()
        return cleaned or match.group(0)

    replaced = TEMPLATE_PARAMETER_RE.sub(replace, value)

    def replace_colon(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        replacement = replacements.get(key)
        if replacement is None:
            return match.group(0)
        cleaned = str(replacement).strip()
        return cleaned or match.group(0)

    return COLON_PARAMETER_RE.sub(replace_colon, replaced)


def is_cross_host_redirect(fetch: FetchResponse) -> bool:
    requested_host = final_host(fetch.requested_url)
    final_redirect_host = final_host(fetch.final_url)
    return bool(requested_host and final_redirect_host and requested_host != final_redirect_host)


def is_login_like_host(host: str | None) -> bool:
    if not host:
        return False
    markers = (
        "login.",
        "signin.",
        "auth.",
        "sso.",
        "passport.",
        "accounts.",
        "id.",
    )
    return any(marker in host for marker in markers)


def is_login_handoff_body(text: str) -> bool:
    lower_text = text.lower()
    markers = (
        "retpath",
        "is_autologin",
        "passport",
        "signin",
        "login",
        "document.createelement('form')",
        "document.createelement(\"form\")",
    )
    matches = sum(1 for marker in markers if marker in lower_text)
    return matches >= 2


def looks_like_html_fragment(text: str) -> bool:
    if not isinstance(text, str) or "<" not in text or ">" not in text:
        return False
    lower_text = text.lower()
    markers = (
        "<!doctype html",
        "<html",
        "<head",
        "<body",
        "<title",
        "<meta ",
        "<script",
        "<iframe",
        "<frame",
        "<style",
        "<center",
        "<table",
        "<tr",
        "<td",
        "<form",
        "<div",
        "<span",
        "<p",
        "<a ",
        "<h1",
        "<h2",
        "<h3",
        "<h4",
        "<h5",
        "<h6",
    )
    return any(marker in lower_text for marker in markers)


def looks_like_markup_fragment(text: str) -> bool:
    if not isinstance(text, str):
        return False
    stripped = text.lstrip()
    if not stripped.startswith("<"):
        return False
    lower_text = stripped.lower()
    if looks_like_html_fragment(stripped):
        return True
    if stripped.startswith("<?xml"):
        return True
    if any(
        lower_text.startswith(marker)
        for marker in (
            "<error",
            "<response",
            "<message",
            "<status",
            "<statuscode",
            "<code",
            "<resource",
            "<urlset",
            "<sitemapindex",
            "<rss",
            "<feed",
        )
    ):
        return True
    return bool(re.match(r"<[a-zA-Z][\w:-]*(?:\s|>|/)", stripped))


def is_generic_error_fallback_body(text: str) -> bool:
    if not isinstance(text, str) or not text:
        return False
    lower_text = text.lower()
    if any(
        marker in lower_text
        for marker in (
            "web firewall security policies",
            "security policies have been blocked",
            "request / response that are contrary",
            "detect url",
            "common-error",
            "wh_errcode=404",
        )
    ):
        return True
    if "page not found" in lower_text and looks_like_html_fragment(text):
        return True
    if "not found" in lower_text and "redirecturl=" in lower_text:
        return True
    return False


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


def has_placeholder_value(value: Any) -> bool:
    if not isinstance(value, str):
        return False
    lower_value = value.lower()
    placeholders = (
        "your_",
        "example.com",
        "merchant.com",
        "shop name",
        "12345678901234567890",
        "your-forter",
        "your_forter",
        "sandbox.checkouttools.com",
    )
    return any(marker in lower_value for marker in placeholders)


def flatten_strings(value: Any) -> list[str]:
    results: list[str] = []
    if isinstance(value, str):
        results.append(value)
        return results
    if isinstance(value, dict):
        for item in value.values():
            results.extend(flatten_strings(item))
        return results
    if isinstance(value, list):
        for item in value:
            results.extend(flatten_strings(item))
    return results


def merge_unique_limited(existing: list[str], values: list[str], *, limit: int) -> list[str]:
    if limit <= 0:
        return []
    merged = list(existing)
    if len(merged) >= limit:
        return merged[:limit]
    seen = {value for value in merged if isinstance(value, str)}
    for value in values:
        if not isinstance(value, str):
            continue
        cleaned = value.strip()
        if not cleaned or cleaned in seen:
            continue
        merged.append(cleaned)
        seen.add(cleaned)
        if len(merged) >= limit:
            break
    return merged


def is_url_like_string(value: str) -> bool:
    return isinstance(value, str) and value.startswith(("http://", "https://"))


def marker_matches_text(text: str, marker: str) -> bool:
    lowered_text = text.lower()
    lowered_marker = marker.lower().strip()
    if not lowered_marker:
        return False
    if any(character in lowered_marker for character in (".", "/", ":", "_", "-", " ")):
        return lowered_marker in lowered_text
    return bool(re.search(rf"(?<![a-z0-9]){re.escape(lowered_marker)}(?![a-z0-9])", lowered_text))


def collect_payment_hints(value: Any) -> dict[str, Any]:
    strings = [item.strip() for item in flatten_strings(value) if isinstance(item, str)]
    text_strings = [item for item in strings if item and not is_url_like_string(item)]
    lower_text_strings = [item.lower() for item in text_strings]
    url_hosts: list[str] = []

    provider_hints: list[str] = []
    rail_hints: list[str] = []
    endpoint_hosts: list[str] = []

    for item in strings:
        if not is_url_like_string(item):
            continue
        host = final_host(item)
        if host:
            endpoint_hosts.append(host)
            url_hosts.append(host)

    for provider, markers in PAYMENT_PROVIDER_MARKERS.items():
        text_match = any(
            marker_matches_text(item, marker)
            for item in lower_text_strings
            for marker in markers
        )
        host_match = any(
            "." in marker and any(marker in host for host in url_hosts)
            for marker in markers
        )
        if text_match or host_match:
            provider_hints.append(provider)

    for rail, markers in PAYMENT_RAIL_MARKERS.items():
        if any(
            marker_matches_text(item, marker)
            for item in lower_text_strings
            for marker in markers
        ):
            rail_hints.append(rail)

    payment_surface = None
    if "x402" in rail_hints:
        payment_surface = "x402"
    elif "saved_card" in rail_hints or "dialtoneapp_network" in provider_hints:
        payment_surface = "saved_card_authority"
    elif {"card", "digital_wallet"} & set(rail_hints):
        payment_surface = "standard_checkout"
    elif "crypto" in rail_hints:
        payment_surface = "crypto"
    elif provider_hints:
        payment_surface = "provider_named"

    has_non_crypto_rail = bool({"card", "digital_wallet", "saved_card"} & set(rail_hints))
    has_crypto_markers = any(
        any(marker in item for marker in ("usdc", "usdt", "solana", "base", "ethereum", "wallet", "coinbase", "eip155"))
        for item in lower_text_strings
    )
    crypto_only = bool(
        ("crypto" in rail_hints or ("x402" in rail_hints and has_crypto_markers))
        and not has_non_crypto_rail
    )

    return {
        "payment_provider_hints": sorted(set(provider_hints))[:12],
        "payment_rail_hints": sorted(set(rail_hints))[:12],
        "payment_endpoint_hosts": sorted(set(endpoint_hosts))[:12],
        "payment_surface": payment_surface,
        "crypto_only": crypto_only,
    }


def extract_observed_json_schema_facts(payload: Any) -> dict[str, Any]:
    observed: dict[str, list[str]] = {
        "observed_capability_names": [],
        "observed_payment_protocols": [],
        "observed_payment_methods": [],
        "observed_payment_providers": [],
        "observed_payment_handler_names": [],
        "observed_payment_assets": [],
        "observed_payment_networks": [],
        "observed_payment_flow_types": [],
        "observed_payment_requirement_keys": [],
        "observed_catalog_endpoints": [],
        "observed_quote_endpoints": [],
        "observed_checkout_endpoints": [],
        "observed_order_status_endpoints": [],
    }

    def add_value(key: str, value: Any, *, upper: bool = False, limit: int = 12) -> None:
        if not isinstance(value, str):
            return
        cleaned = value.strip()
        if not cleaned:
            return
        if upper:
            cleaned = cleaned.upper()
        observed[key] = merge_unique_limited(observed.get(key, []), [cleaned], limit=limit)

    def add_many(key: str, values: Any, *, upper: bool = False, limit: int = 12) -> None:
        if isinstance(values, list):
            for item in values[:20]:
                if isinstance(item, str):
                    add_value(key, item, upper=upper, limit=limit)
                elif isinstance(item, dict):
                    for candidate_key in ("name", "id", "value", "method", "asset", "currency", "network"):
                        candidate_value = item.get(candidate_key)
                        if isinstance(candidate_value, str):
                            add_value(key, candidate_value, upper=upper, limit=limit)
        elif isinstance(values, str):
            add_value(key, values, upper=upper, limit=limit)

    def endpoint_value(candidate: Any) -> str | None:
        if isinstance(candidate, str):
            cleaned = candidate.strip()
            return cleaned or None
        if isinstance(candidate, dict):
            for endpoint_key in ("endpoint", "url", "href", "path"):
                endpoint_candidate = candidate.get(endpoint_key)
                if isinstance(endpoint_candidate, str) and endpoint_candidate.strip():
                    return endpoint_candidate.strip()
        return None

    def process_capabilities(value: Any) -> None:
        if isinstance(value, dict):
            for capability_name in list(value.keys())[:20]:
                if isinstance(capability_name, str):
                    add_value("observed_capability_names", capability_name)
            return
        if isinstance(value, list):
            for item in value[:30]:
                if isinstance(item, str):
                    add_value("observed_capability_names", item)
                elif isinstance(item, dict):
                    for candidate_key in ("name", "id"):
                        candidate_value = item.get(candidate_key)
                        if isinstance(candidate_value, str):
                            add_value("observed_capability_names", candidate_value)
                            break

    def process_provider_entries(value: Any, *, handler_mode: bool) -> None:
        if isinstance(value, dict):
            entries = []
            for entry_name, entry_value in list(value.items())[:20]:
                if isinstance(entry_value, dict):
                    entry_payload = dict(entry_value)
                    if "name" not in entry_payload and isinstance(entry_name, str):
                        entry_payload["name"] = entry_name
                    entries.append(entry_payload)
                elif isinstance(entry_name, str):
                    entries.append({"name": entry_name})
        elif isinstance(value, list):
            entries = value[:20]
        else:
            return

        for entry in entries:
            if isinstance(entry, str):
                target_key = "observed_payment_handler_names" if handler_mode else "observed_payment_providers"
                add_value(target_key, entry)
                continue
            if not isinstance(entry, dict):
                continue
            name_value = entry.get("name")
            if isinstance(name_value, str):
                target_key = "observed_payment_handler_names" if handler_mode else "observed_payment_providers"
                add_value(target_key, name_value)
            add_many("observed_payment_methods", entry.get("supported_methods"))
            add_many("observed_payment_methods", entry.get("payment_methods"))
            add_many("observed_payment_assets", entry.get("supported_assets"), upper=True)
            add_many("observed_payment_assets", entry.get("assets"), upper=True)
            add_many("observed_payment_assets", entry.get("currencies"), upper=True)
            add_many("observed_payment_networks", entry.get("networks"))
            protocol_value = entry.get("protocol")
            if isinstance(protocol_value, str):
                add_value("observed_payment_protocols", protocol_value.upper())

    def process_payment_block(value: Any) -> None:
        if not isinstance(value, dict):
            return
        protocol_value = value.get("protocol")
        if isinstance(protocol_value, str):
            add_value("observed_payment_protocols", protocol_value.upper())
        flow = value.get("flow")
        if isinstance(flow, dict):
            flow_type = flow.get("type")
            if isinstance(flow_type, str):
                add_value("observed_payment_flow_types", flow_type)
        requirements = value.get("requirements")
        if isinstance(requirements, dict):
            for requirement_key in list(requirements.keys())[:20]:
                if isinstance(requirement_key, str):
                    add_value("observed_payment_requirement_keys", requirement_key)
        add_many("observed_payment_methods", value.get("payment_methods"))
        add_many("observed_payment_methods", value.get("supported_methods"))
        add_many("observed_payment_assets", value.get("supported_assets"), upper=True)
        add_many("observed_payment_assets", value.get("assets"), upper=True)
        add_many("observed_payment_assets", value.get("currencies"), upper=True)
        add_many("observed_payment_networks", value.get("networks"))
        process_provider_entries(value.get("payment_providers"), handler_mode=False)
        process_provider_entries(value.get("providers"), handler_mode=False)
        process_provider_entries(value.get("payment_handlers"), handler_mode=True)
        process_provider_entries(value.get("handlers"), handler_mode=True)

    def process_endpoints(value: Any) -> None:
        if not isinstance(value, dict):
            return
        endpoint_roles = {
            "catalog": "observed_catalog_endpoints",
            "quote": "observed_quote_endpoints",
            "checkout": "observed_checkout_endpoints",
            "order_status": "observed_order_status_endpoints",
            "order-status": "observed_order_status_endpoints",
            "orderstatus": "observed_order_status_endpoints",
        }
        for raw_key, raw_value in list(value.items())[:30]:
            if not isinstance(raw_key, str):
                continue
            normalized_key = raw_key.strip().lower().replace(" ", "_")
            target_key = endpoint_roles.get(normalized_key)
            if not target_key:
                continue
            resolved_value = endpoint_value(raw_value)
            if resolved_value:
                add_value(target_key, resolved_value, limit=8)

    def walk(value: Any, *, path: tuple[str, ...] = (), depth: int = 0) -> None:
        if depth > 7:
            return
        if isinstance(value, dict):
            normalized_items: list[tuple[str, Any]] = []
            for raw_key, raw_value in list(value.items())[:40]:
                if isinstance(raw_key, str):
                    normalized_items.append((raw_key.strip().lower(), raw_value))

            if "capabilities" in {key for key, _ in normalized_items}:
                for key, item in normalized_items:
                    if key == "capabilities":
                        process_capabilities(item)
            if "payment" in {key for key, _ in normalized_items}:
                for key, item in normalized_items:
                    if key == "payment":
                        process_payment_block(item)
            if "endpoints" in {key for key, _ in normalized_items}:
                for key, item in normalized_items:
                    if key == "endpoints":
                        process_endpoints(item)

            path_set = set(path)
            for key, item in normalized_items:
                in_payment_context = bool(path_set & {"payment", "payments", "payment_provider", "payment_providers", "payment_handler", "payment_handlers"})
                if key == "protocol" and in_payment_context and isinstance(item, str):
                    add_value("observed_payment_protocols", item.upper())
                elif key in {"payment_methods", "supported_methods"}:
                    add_many("observed_payment_methods", item)
                elif key in {"supported_assets", "assets", "currencies"} and in_payment_context:
                    add_many("observed_payment_assets", item, upper=True)
                elif key in {"networks", "supported_networks"} and in_payment_context:
                    add_many("observed_payment_networks", item)
                elif key == "requirements" and in_payment_context and isinstance(item, dict):
                    for requirement_key in list(item.keys())[:20]:
                        if isinstance(requirement_key, str):
                            add_value("observed_payment_requirement_keys", requirement_key)
                elif key == "flow" and in_payment_context and isinstance(item, dict):
                    flow_type = item.get("type")
                    if isinstance(flow_type, str):
                        add_value("observed_payment_flow_types", flow_type)
                elif key in {"payment_providers", "providers"} and (in_payment_context or key == "payment_providers"):
                    process_provider_entries(item, handler_mode=False)
                elif key in {"payment_handlers", "handlers"} and (in_payment_context or key == "payment_handlers"):
                    process_provider_entries(item, handler_mode=True)
                walk(item, path=path + (key,), depth=depth + 1)
            return
        if isinstance(value, list):
            for item in value[:40]:
                walk(item, path=path, depth=depth + 1)

    walk(payload)
    return {key: value for key, value in observed.items() if value}


def normalize_status_value(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = value.strip().lower()
    return normalized or None


def is_live_status(value: Any) -> bool:
    return normalize_status_value(value) in {"active", "available", "enabled", "live"}


def is_active_offer_status(value: Any) -> bool:
    normalized = normalize_status_value(value)
    return normalized in {None, "active", "available", "enabled", "live"}


def is_human_checkout_kind(value: Any) -> bool:
    return normalize_status_value(value) == "human_browser_checkout"


def is_prelaunch_status(value: Any) -> bool:
    return normalize_status_value(value) in {
        "coming_soon",
        "pending",
        "pending_activation",
        "planned",
        "pre_launch",
        "preorder",
        "pre_order",
    }


def infer_payment_surface_from_hints(provider_hints: list[str], rail_hints: list[str]) -> str | None:
    rail_set = set(rail_hints)
    if "x402" in rail_set:
        return "x402"
    if "saved_card" in rail_set or "dialtoneapp_network" in provider_hints:
        return "saved_card_authority"
    if {"card", "digital_wallet"} & rail_set:
        return "standard_checkout"
    if "crypto" in rail_set:
        return "crypto"
    if provider_hints:
        return "provider_named"
    return None


def sample_offer_from_payload(
    offer: dict[str, Any],
    *,
    default_status: str | None = None,
    default_checkout_url: str | None = None,
) -> tuple[dict[str, Any], bool]:
    offer_id = offer.get("id") or offer.get("offer")
    title = offer.get("title") or offer.get("name")
    kind = offer.get("kind") or offer.get("type") or offer.get("tier")
    status = normalize_status_value(offer.get("status")) or default_status or "unknown"
    price = offer.get("price") if isinstance(offer.get("price"), dict) else {}
    amount = price.get("amount")
    currency = price.get("currency")
    interval = price.get("interval")
    if amount is None and offer.get("price") is not None and not isinstance(offer.get("price"), dict):
        amount = offer.get("price")
    if currency is None:
        currency = offer.get("priceCurrency") or offer.get("currency")
    if interval is None:
        interval = offer.get("unit") or offer.get("interval")
    api = offer.get("api") if isinstance(offer.get("api"), dict) else {}
    purchase_modes = [
        str(mode).strip()
        for mode in (offer.get("purchase_modes") if isinstance(offer.get("purchase_modes"), list) else [])
        if str(mode).strip()
    ]

    sample_offer: dict[str, Any] = {
        "id": offer_id,
        "title": title,
        "kind": kind,
        "status": status,
    }
    if isinstance(amount, (int, float, str)) and str(amount).strip():
        sample_offer["amount"] = str(amount).strip()
    if isinstance(currency, str) and currency.strip():
        sample_offer["currency"] = currency.strip().upper()[:8]
    if isinstance(interval, str) and interval.strip():
        sample_offer["interval"] = interval.strip().lower()
    if purchase_modes:
        sample_offer["purchase_modes"] = purchase_modes[:8]
    if isinstance(api.get("purchase_intent"), str) and api["purchase_intent"].strip():
        sample_offer["purchase_intent_url"] = api["purchase_intent"].strip()
    if isinstance(api.get("offer_lookup"), str) and api["offer_lookup"].strip():
        sample_offer["offer_lookup_url"] = api["offer_lookup"].strip()
    elif isinstance(offer.get("offer_lookup_url"), str) and offer["offer_lookup_url"].strip():
        sample_offer["offer_lookup_url"] = offer["offer_lookup_url"].strip()
    if "purchase_intent_url" not in sample_offer and isinstance(default_checkout_url, str) and default_checkout_url:
        sample_offer["checkout_url"] = default_checkout_url

    has_price = "amount" in sample_offer and "currency" in sample_offer
    return sample_offer, has_price
