from __future__ import annotations

import json
from typing import Any

from .helpers import merge_unique_limited, normalize_status_value, parse_price
from .http_client import parse_json_body
from .models import FetchResponse


ZERO_DECIMAL_CURRENCIES = {
    "BIF",
    "CLP",
    "DJF",
    "GNF",
    "JPY",
    "KMF",
    "KRW",
    "MGA",
    "PYG",
    "RWF",
    "UGX",
    "VND",
    "VUV",
    "XAF",
    "XOF",
    "XPF",
}

THREE_DECIMAL_CURRENCIES = {
    "BHD",
    "IQD",
    "JOD",
    "KWD",
    "LYD",
    "OMR",
    "TND",
}


def normalize_minor_unit_price(value: Any, currency: Any) -> float | None:
    if not isinstance(value, (int, float)):
        return parse_price(value)
    if isinstance(currency, str) and currency.strip():
        currency_code = currency.strip().upper()[:8]
        if currency_code in ZERO_DECIMAL_CURRENCIES:
            return float(value)
        if currency_code in THREE_DECIMAL_CURRENCIES:
            return float(value) / 1000.0
    return float(value) / 100.0


def extract_amount_currency(value: Any) -> tuple[float | None, str | None]:
    if isinstance(value, dict):
        amount = value.get("amount")
        currency = value.get("currency") if isinstance(value.get("currency"), str) and value.get("currency").strip() else None
        return normalize_minor_unit_price(amount, currency), currency
    return parse_price(value), None


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
        product_url = product.get("url") if isinstance(product.get("url"), str) and product.get("url").strip() else None
        json_ld = product.get("jsonLd") if isinstance(product.get("jsonLd"), dict) else {}
        json_ld_offers = json_ld.get("offers") if isinstance(json_ld.get("offers"), dict) else {}
        price_range = product.get("price_range") if isinstance(product.get("price_range"), dict) else {}
        price_range_min = price_range.get("min")
        price_range_max = price_range.get("max")
        product_currency = product.get("currency") or json_ld_offers.get("priceCurrency")
        if not product_currency and isinstance(price_range_min, dict):
            product_currency = price_range_min.get("currency")
        if isinstance(product_currency, str) and product_currency:
            currencies.add(product_currency.upper()[:8])
        product_price, embedded_product_currency = extract_amount_currency(product.get("price") or json_ld_offers.get("price"))
        if product_price is None and isinstance(price_range_min, dict):
            product_price, embedded_product_currency = extract_amount_currency(price_range_min)
        if product_price is None and isinstance(price_range_max, dict):
            product_price, embedded_product_currency = extract_amount_currency(price_range_max)
        if embedded_product_currency and not product_currency:
            product_currency = embedded_product_currency
            currencies.add(embedded_product_currency.upper()[:8])
        availability = json_ld_offers.get("availability")
        if not isinstance(availability, str):
            availability_payload = product.get("availability") if isinstance(product.get("availability"), dict) else {}
            if availability_payload.get("available") is True:
                availability = "InStock"
            elif availability_payload.get("available") is False:
                availability = "OutOfStock"
        product_type = product.get("product_type")
        if not isinstance(product_type, str) or not product_type.strip():
            categories = product.get("categories") if isinstance(product.get("categories"), list) else []
            for category in categories:
                if not isinstance(category, dict):
                    continue
                value = category.get("value")
                if isinstance(value, str) and value.strip():
                    product_type = value.strip()[:120]
                    break
        vendor = product.get("vendor")
        if not isinstance(vendor, str) or not vendor.strip():
            seller = product.get("seller") if isinstance(product.get("seller"), dict) else {}
            vendor = seller.get("name") if isinstance(seller.get("name"), str) else vendor
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
                    "product_type": product_type,
                    "product_url": product_url,
                    "vendor": vendor,
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
            variant_is_available = (
                variant.get("available") is True
                or normalize_status_value(variant.get("stock_status")) == "instock"
            )
            availability_payload = variant.get("availability") if isinstance(variant.get("availability"), dict) else {}
            if availability_payload.get("available") is True:
                variant_is_available = True
            if variant_is_available:
                available_variant_count += 1
            if isinstance(variant.get("stock_status"), str) and variant.get("stock_status").strip():
                stock_statuses.add(variant.get("stock_status").strip())
            if variant.get("requires_shipping") is True:
                requires_shipping = True
            requires = variant.get("requires") if isinstance(variant.get("requires"), dict) else {}
            if requires.get("shipping") is True:
                requires_shipping = True
            price, embedded_currency = extract_amount_currency(variant.get("price"))
            currency = variant.get("currency") or embedded_currency or product_currency
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
                "product_type": product_type,
                "product_url": product_url,
                "vendor": vendor,
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
