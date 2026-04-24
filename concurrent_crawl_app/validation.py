from __future__ import annotations

from .validators_content import (
    validate_commerce,
    validate_homepage,
    validate_llms,
    validate_robots,
    validate_sitemap,
    validate_ucp,
)
from .validators_surfaces import (
    validate_agent,
    validate_agents,
    validate_cart,
    validate_openapi,
    validate_payment_probe,
    validate_products,
    validate_x402,
)


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
    "payment_probe": validate_payment_probe,
    "x402": validate_x402,
    "products": validate_products,
    "cart": validate_cart,
}
