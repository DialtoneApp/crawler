from __future__ import annotations

import re

from .models import ProbeSpec


USER_AGENT = "dialtoneapp.com crawler v0.2.0 https://dialtoneapp.com/contact human@dialtoneapp.com"
DEFAULT_ACCEPT = "text/html, application/json, text/plain, application/xml, text/xml, */*;q=0.1"
TITLE_RE = re.compile(r"<title[^>]*>(.*?)</title>", re.IGNORECASE | re.DOTALL)

PAYMENT_PROVIDER_MARKERS = {
    "asterpay": ("asterpay", "asterpay.io"),
    "circle": ("circle", "circle.com", "circle gateway"),
    "coinbase": ("coinbase", "coinbase.com"),
    "crossmint": ("crossmint",),
    "dialtoneapp_network": ("dialtoneapp_network", "dialtoneapp network", "dtapp_network_v1"),
    "google_pay": ("com.google.pay", "gpay", "google pay", "pay.google.com"),
    "nevermined": ("nevermined", "nevermined.ai"),
    "paypal": ("paypal",),
    "polar": ("polar",),
    "shopify": ("shopify", "myshopify.com"),
    "stripe": ("stripe", "stripe-subscription", "stripe machine payments"),
    "skyfire": ("skyfire",),
    "tempo": ("tempo",),
    "x402": ("x402",),
}

PAYMENT_RAIL_MARKERS = {
    "card": ("card", "visa", "mastercard", "master", "american_express", "amex", "discover", "diners_club"),
    "digital_wallet": ("com.google.pay", "gpay", "google pay", "pay.google.com", "apple pay"),
    "crypto": ("usdc", "usdt", "bitcoin", "ethereum", "solana", "coinbase", "walletconnect", "nevermined", "asterpay"),
    "saved_card": ("saved card", "saved-card", "stripe-subscription", "card_network_machine_payments"),
    "x402": ("x402", "payment required"),
}

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
    "api_openapi_json",
    "payment_probe",
    "x402_json",
    "x402_well_known",
    "remote_x402",
    "products_json",
    "api_products",
}

CONTROL_PATH_TEMPLATES = {
    "text": "/dialtoneapp-probe-miss-{token}.txt",
    "xml": "/dialtoneapp-probe-miss-{token}.xml",
    "json_root": "/dialtoneapp-probe-miss-{token}.json",
    "json_well_known": "/.well-known/dialtoneapp-probe-miss-{token}.json",
    "catalog": "/dialtoneapp-probe-products-miss-{token}.json",
}

BASE_PROBES = (
    ProbeSpec("homepage", "/", "homepage", max_bytes=32_768),
    ProbeSpec("robots_txt", "/robots.txt", "robots", control_group="text"),
    ProbeSpec("sitemap_xml", "/sitemap.xml", "sitemap", control_group="xml"),
    ProbeSpec("llms_txt", "/llms.txt", "llms", control_group="text"),
    ProbeSpec("llms_full_txt", "/llms-full.txt", "llms", control_group="text"),
    ProbeSpec("well_known_commerce", "/.well-known/commerce", "commerce", max_bytes=131_072, control_group="json_well_known"),
    ProbeSpec("well_known_ucp", "/.well-known/ucp", "ucp", control_group="json_well_known"),
    ProbeSpec("well_known_agent_json", "/.well-known/agent.json", "agent", max_bytes=131_072, control_group="json_well_known"),
    ProbeSpec("well_known_agents_json", "/.well-known/agents.json", "agents", max_bytes=65_536, control_group="json_well_known"),
    ProbeSpec("well_known_agent_card", "/.well-known/agent-card.json", "agent", max_bytes=131_072, control_group="json_well_known"),
    ProbeSpec("root_agent_json", "/agent.json", "agent", max_bytes=131_072, control_group="json_root"),
    ProbeSpec("openapi_json", "/openapi.json", "openapi", max_bytes=262_144, control_group="json_root"),
    ProbeSpec("x402_well_known", "/.well-known/x402", "x402", max_bytes=524_288, control_group="json_well_known"),
    ProbeSpec("x402_json", "/.well-known/x402.json", "x402", max_bytes=524_288, control_group="json_well_known"),
)

PRODUCTS_PROBE = ProbeSpec("products_json", "/products.json", "products", max_bytes=524_288, control_group="catalog")
CART_PROBE = ProbeSpec("cart_json", "/cart.js", "cart", max_bytes=16_384, control_group="catalog")
