from __future__ import annotations

import http.client
import json
import ssl
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .constants import CONTROL_PATH_TEMPLATES, DEFAULT_ACCEPT, USER_AGENT
from .helpers import normalize_content_type
from .models import FetchResponse


def parse_json_body(fetch: FetchResponse) -> Any:
    if not fetch.body:
        raise ValueError("empty body")

    text = fetch.body.decode("utf-8", errors="replace").strip()
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
