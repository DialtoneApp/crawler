from __future__ import annotations

import json
from typing import Any

from .helpers import merge_unique_limited, normalize_status_value, parse_price
from .http_client import parse_json_body
from .models import FetchResponse


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
