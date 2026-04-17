from __future__ import annotations

import os


APP_API_BASE_URL_ENV_VAR = "DIALTONE_API_BASE_URL"
DEFAULT_APP_API_BASE_URL = "http://localhost:5173"
ALLOWED_APP_API_BASE_URLS = (
    "http://localhost:5173",
    "https://localhost:5173",
    "https://dialtoneapp.com",
)


def allowed_app_api_base_urls_text() -> str:
    return ", ".join(ALLOWED_APP_API_BASE_URLS)


def normalize_app_api_base_url(value: str) -> str:
    normalized = value.strip().rstrip("/")
    if normalized in ALLOWED_APP_API_BASE_URLS:
        return normalized

    raise ValueError(
        f"Invalid Dialtone API base URL {value!r}. "
        f"Use one of: {allowed_app_api_base_urls_text()}."
    )


def get_app_api_base_url() -> str:
    return normalize_app_api_base_url(
        os.getenv(APP_API_BASE_URL_ENV_VAR, DEFAULT_APP_API_BASE_URL)
    )


def build_app_api_url(api_base_url: str, path: str) -> str:
    if not path.startswith("/"):
        raise ValueError(f"Dialtone API path must start with '/': {path!r}")

    return f"{normalize_app_api_base_url(api_base_url)}{path}"
