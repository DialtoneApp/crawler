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
ABSOLUTE_URL_RE = re.compile(r"https?://[^\s<>()\"']+")
LINK_TAG_RE = re.compile(r"<link\b[^>]*>", re.IGNORECASE)
HTML_ATTR_RE = re.compile(r'([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*=\s*("([^"]*)"|\'([^\']*)\'|([^\s>]+))')


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
        resolved_url = resolve_url(base_url, href_value) or href_value.strip()
        if resolved_url and resolved_url not in urls:
            urls.append(resolved_url)
    return urls


def extract_template_parameters(value: str | None) -> list[str]:
    if not isinstance(value, str) or not value:
        return []
    results: list[str] = []
    for match in TEMPLATE_PARAMETER_RE.finditer(value):
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

    return TEMPLATE_PARAMETER_RE.sub(replace, value)


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


def collect_payment_hints(value: Any) -> dict[str, Any]:
    strings = [item.strip() for item in flatten_strings(value) if isinstance(item, str)]
    lower_strings = [item.lower() for item in strings if item]

    provider_hints: list[str] = []
    rail_hints: list[str] = []
    endpoint_hosts: list[str] = []

    for provider, markers in PAYMENT_PROVIDER_MARKERS.items():
        if any(any(marker in item for marker in markers) for item in lower_strings):
            provider_hints.append(provider)

    for rail, markers in PAYMENT_RAIL_MARKERS.items():
        if any(any(marker in item for marker in markers) for item in lower_strings):
            rail_hints.append(rail)

    for item in strings:
        if not (item.startswith("http://") or item.startswith("https://")):
            continue
        host = final_host(item)
        if host:
            endpoint_hosts.append(host)

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
        for item in lower_strings
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
