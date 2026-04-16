#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from html.parser import HTMLParser
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen
from urllib.robotparser import RobotFileParser

from db import connect_db, migrate_db


USER_AGENT = "dialtoneapp.com crawler v0.0.1"
HTML_CONTENT_TYPES = ("text/html", "application/xhtml+xml")
BINARY_SUFFIXES = {
    ".7z",
    ".avi",
    ".bin",
    ".css",
    ".csv",
    ".doc",
    ".docx",
    ".gif",
    ".gz",
    ".ico",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".mov",
    ".mp3",
    ".mp4",
    ".pdf",
    ".png",
    ".ppt",
    ".pptx",
    ".rar",
    ".rss",
    ".svg",
    ".tar",
    ".tgz",
    ".txt",
    ".webm",
    ".xml",
    ".zip",
}
SPECIAL_ASSETS = [
    ("llms_txt", "/llms.txt"),
    ("llm_txt", "/llm.txt"),
    ("agents_json", "/agents.json"),
    ("well_known_agents_json", "/.well-known/agents.json"),
]


@dataclass(frozen=True)
class FetchResult:
    requested_url: str
    final_url: str | None
    http_code: int | None
    response_bytes: int
    content_type: str | None
    x_robots_tag: str | None
    body: bytes
    error: str | None = None


@dataclass(frozen=True)
class AssetSummary:
    asset_type: str
    asset_url: str
    http_code: int | None
    response_bytes: int
    content_type: str | None
    is_present: bool
    parsed_ok: bool
    item_count: int | None


@dataclass(frozen=True)
class RobotsPolicy:
    asset: AssetSummary
    parser: RobotFileParser | None

    def can_fetch(self, url: str) -> bool:
        if self.asset.http_code in {401, 403}:
            return False

        if not self.asset.is_present or not self.asset.parsed_ok or self.parser is None:
            return True

        return self.parser.can_fetch(USER_AGENT, url)


@dataclass(frozen=True)
class PageSummary:
    url: str
    final_url: str | None
    referrer_url: str | None
    depth: int
    allowed_by_robots: bool
    http_code: int | None
    response_bytes: int
    content_type: str | None
    is_html: bool
    title: str | None
    meta_description_length: int | None
    canonical_url: str | None
    meta_robots: str | None
    x_robots_tag: str | None
    h1_count: int
    word_count: int
    internal_link_count: int
    external_link_count: int
    has_json_ld: bool
    has_open_graph: bool
    has_twitter_card: bool


@dataclass(frozen=True)
class Finding:
    category: str
    code: str
    severity: str
    page_url: str | None
    metric_value: int | None
    message: str


class PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hrefs: list[str] = []
        self.title_parts: list[str] = []
        self.text_parts: list[str] = []
        self.meta_description: str | None = None
        self.canonical_href: str | None = None
        self.meta_robots: str | None = None
        self.has_open_graph = False
        self.has_twitter_card = False
        self.has_json_ld = False
        self.h1_count = 0
        self._in_title = False
        self._ignored_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {key.lower(): value for key, value in attrs if key}
        lower_tag = tag.lower()

        if lower_tag == "title":
            self._in_title = True
        elif lower_tag in {"script", "style"}:
            self._ignored_depth += 1

        if lower_tag == "a":
            href = attr_map.get("href")
            if href:
                self.hrefs.append(href.strip())
        elif lower_tag == "h1":
            self.h1_count += 1
        elif lower_tag == "link":
            rel = {part.strip().lower() for part in (attr_map.get("rel") or "").split()}
            href = attr_map.get("href")
            if "canonical" in rel and href:
                self.canonical_href = href.strip()
        elif lower_tag == "meta":
            name = (attr_map.get("name") or "").strip().lower()
            prop = (attr_map.get("property") or "").strip().lower()
            content = (attr_map.get("content") or "").strip()

            if name == "description" and content:
                self.meta_description = content
            elif name == "robots" and content:
                self.meta_robots = content

            if prop.startswith("og:") and content:
                self.has_open_graph = True
            if name.startswith("twitter:") and content:
                self.has_twitter_card = True
        elif lower_tag == "script":
            script_type = (attr_map.get("type") or "").strip().lower()
            if "ld+json" in script_type:
                self.has_json_ld = True

    def handle_endtag(self, tag: str) -> None:
        lower_tag = tag.lower()
        if lower_tag == "title":
            self._in_title = False
        elif lower_tag in {"script", "style"} and self._ignored_depth > 0:
            self._ignored_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title_parts.append(data)
            return

        if self._ignored_depth == 0:
            text = data.strip()
            if text:
                self.text_parts.append(text)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Fetch homepage URLs from the repositories table, inspect crawler and "
            "LLM-facing site files, and store compact crawl summaries in SQLite."
        )
    )
    parser.add_argument(
        "--db",
        default="repos.db",
        help="SQLite database path. Default: repos.db.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        help="Maximum number of homepage URLs to crawl.",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        help="Per-request timeout in seconds. Default: 10.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=10,
        help="Maximum number of allowed pages to fetch per site. Default: 10.",
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=1,
        help="Maximum crawl depth from the homepage. Default: 1.",
    )
    return parser.parse_args()


def load_homepages(
    conn: sqlite3.Connection,
    limit: int | None,
) -> list[tuple[str, str]]:
    query = """
        SELECT full_name, homepage_url
        FROM repositories
        WHERE homepage_url > ''
        ORDER BY search_rank
    """
    params: tuple[int, ...] | tuple[()] = ()

    if limit is not None:
        query = f"{query} LIMIT ?"
        params = (limit,)

    return conn.execute(query, params).fetchall()


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    if not parsed.scheme:
        parsed = urlparse(f"https://{url}")

    if parsed.scheme not in {"http", "https"}:
        raise ValueError(f"unsupported URL scheme: {parsed.scheme}")

    if not parsed.netloc:
        raise ValueError("URL is missing a host")

    path = parsed.path or "/"
    return urlunparse(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            path,
            "",
            parsed.query,
            "",
        )
    )


def origin_from_url(url: str) -> str:
    parsed = urlparse(url)
    return urlunparse((parsed.scheme.lower(), parsed.netloc.lower(), "", "", "", ""))


def content_type_base(content_type: str | None) -> str | None:
    if not content_type:
        return None
    return content_type.split(";", 1)[0].strip().lower()


def decode_body(body: bytes, content_type: str | None) -> str:
    charset = "utf-8"
    if content_type:
        match = re.search(r"charset=([^\s;]+)", content_type, flags=re.IGNORECASE)
        if match:
            charset = match.group(1).strip("\"'")

    try:
        return body.decode(charset, errors="replace")
    except LookupError:
        return body.decode("utf-8", errors="replace")


def is_html_response(result: FetchResult) -> bool:
    base = content_type_base(result.content_type)
    return bool(base and base in HTML_CONTENT_TYPES)


def fetch_url(url: str, timeout: float) -> FetchResult:
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/json,text/plain,*/*",
        },
        method="GET",
    )

    try:
        with urlopen(request, timeout=timeout) as response:
            body = response.read()
            return FetchResult(
                requested_url=url,
                final_url=response.geturl(),
                http_code=response.status,
                response_bytes=len(body),
                content_type=response.headers.get("Content-Type"),
                x_robots_tag=response.headers.get("X-Robots-Tag"),
                body=body,
            )
    except HTTPError as exc:
        body = exc.read()
        return FetchResult(
            requested_url=url,
            final_url=exc.geturl(),
            http_code=exc.code,
            response_bytes=len(body),
            content_type=exc.headers.get("Content-Type"),
            x_robots_tag=exc.headers.get("X-Robots-Tag"),
            body=body,
        )
    except URLError as exc:
        return FetchResult(
            requested_url=url,
            final_url=None,
            http_code=None,
            response_bytes=0,
            content_type=None,
            x_robots_tag=None,
            body=b"",
            error=str(exc.reason),
        )


def parse_robots(origin: str, timeout: float) -> RobotsPolicy:
    robots_url = urljoin(origin + "/", "robots.txt")
    result = fetch_url(robots_url, timeout)
    parser: RobotFileParser | None = None
    parsed_ok = False
    item_count: int | None = None

    if result.http_code == 200 and result.body:
        text = decode_body(result.body, result.content_type)
        lines = text.splitlines()
        item_count = sum(
            1
            for line in lines
            if line.strip() and not line.lstrip().startswith("#")
        )
        parser = RobotFileParser()
        parser.set_url(robots_url)
        try:
            parser.parse(lines)
            parsed_ok = True
        except ValueError:
            parser = None
            parsed_ok = False

    asset = AssetSummary(
        asset_type="robots_txt",
        asset_url=robots_url,
        http_code=result.http_code,
        response_bytes=result.response_bytes,
        content_type=content_type_base(result.content_type),
        is_present=result.http_code == 200,
        parsed_ok=parsed_ok,
        item_count=item_count,
    )
    return RobotsPolicy(asset=asset, parser=parser)


def analyze_special_asset(
    asset_type: str,
    asset_url: str,
    timeout: float,
) -> AssetSummary:
    result = fetch_url(asset_url, timeout)
    parsed_ok = False
    item_count: int | None = None

    if result.http_code == 200:
        if asset_type in {"llms_txt", "llm_txt"}:
            text = decode_body(result.body, result.content_type)
            item_count = sum(
                1
                for line in text.splitlines()
                if line.strip() and not line.lstrip().startswith("#")
            )
            parsed_ok = True
        else:
            try:
                payload = json.loads(decode_body(result.body, result.content_type))
            except json.JSONDecodeError:
                parsed_ok = False
            else:
                parsed_ok = True
                if isinstance(payload, list):
                    item_count = len(payload)
                elif isinstance(payload, dict):
                    if isinstance(payload.get("agents"), list):
                        item_count = len(payload["agents"])
                    elif isinstance(payload.get("items"), list):
                        item_count = len(payload["items"])
                    else:
                        item_count = len(payload)
                else:
                    item_count = 1

    return AssetSummary(
        asset_type=asset_type,
        asset_url=asset_url,
        http_code=result.http_code,
        response_bytes=result.response_bytes,
        content_type=content_type_base(result.content_type),
        is_present=result.http_code == 200,
        parsed_ok=parsed_ok,
        item_count=item_count,
    )


def normalize_internal_url(base_url: str, href: str, site_netloc: str) -> str | None:
    candidate = href.strip()
    if not candidate:
        return None

    absolute = urljoin(base_url, candidate)
    parsed = urlparse(absolute)

    if parsed.scheme not in {"http", "https"}:
        return None

    if parsed.netloc.lower() != site_netloc.lower():
        return None

    lowered_path = parsed.path.lower()
    if any(lowered_path.endswith(suffix) for suffix in BINARY_SUFFIXES):
        return None

    path = parsed.path or "/"
    return urlunparse(
        (
            parsed.scheme.lower(),
            parsed.netloc.lower(),
            path,
            "",
            "",
            "",
        )
    )


def summarize_page(
    result: FetchResult,
    referrer_url: str | None,
    depth: int,
) -> tuple[PageSummary, list[str]]:
    final_url = result.final_url or result.requested_url

    if result.error:
        return (
            PageSummary(
                url=result.requested_url,
                final_url=None,
                referrer_url=referrer_url,
                depth=depth,
                allowed_by_robots=True,
                http_code=None,
                response_bytes=0,
                content_type=None,
                is_html=False,
                title=None,
                meta_description_length=None,
                canonical_url=None,
                meta_robots=None,
                x_robots_tag=None,
                h1_count=0,
                word_count=0,
                internal_link_count=0,
                external_link_count=0,
                has_json_ld=False,
                has_open_graph=False,
                has_twitter_card=False,
            ),
            [],
        )

    if not is_html_response(result):
        return (
            PageSummary(
                url=result.requested_url,
                final_url=final_url,
                referrer_url=referrer_url,
                depth=depth,
                allowed_by_robots=True,
                http_code=result.http_code,
                response_bytes=result.response_bytes,
                content_type=content_type_base(result.content_type),
                is_html=False,
                title=None,
                meta_description_length=None,
                canonical_url=None,
                meta_robots=None,
                x_robots_tag=result.x_robots_tag,
                h1_count=0,
                word_count=0,
                internal_link_count=0,
                external_link_count=0,
                has_json_ld=False,
                has_open_graph=False,
                has_twitter_card=False,
            ),
            [],
        )

    parser = PageParser()
    html = decode_body(result.body, result.content_type)
    parser.feed(html)
    parser.close()

    site_netloc = urlparse(final_url).netloc.lower()
    internal_links: set[str] = set()
    external_link_count = 0

    for href in parser.hrefs:
        normalized = normalize_internal_url(final_url, href, site_netloc)
        if normalized:
            internal_links.add(normalized)
        else:
            absolute = urljoin(final_url, href)
            parsed = urlparse(absolute)
            if parsed.scheme in {"http", "https"} and parsed.netloc.lower() != site_netloc:
                external_link_count += 1

    canonical_url = None
    if parser.canonical_href:
        canonical_url = urljoin(final_url, parser.canonical_href.strip())

    word_count = len(re.findall(r"\b[\w'-]+\b", " ".join(parser.text_parts)))
    title = " ".join(part.strip() for part in parser.title_parts if part.strip()) or None
    meta_description_length = None
    if parser.meta_description:
        meta_description_length = len(parser.meta_description)

    page = PageSummary(
        url=result.requested_url,
        final_url=final_url,
        referrer_url=referrer_url,
        depth=depth,
        allowed_by_robots=True,
        http_code=result.http_code,
        response_bytes=result.response_bytes,
        content_type=content_type_base(result.content_type),
        is_html=True,
        title=title,
        meta_description_length=meta_description_length,
        canonical_url=canonical_url,
        meta_robots=parser.meta_robots,
        x_robots_tag=result.x_robots_tag,
        h1_count=parser.h1_count,
        word_count=word_count,
        internal_link_count=len(internal_links),
        external_link_count=external_link_count,
        has_json_ld=parser.has_json_ld,
        has_open_graph=parser.has_open_graph,
        has_twitter_card=parser.has_twitter_card,
    )
    return page, sorted(internal_links)


def blocked_page(url: str, referrer_url: str | None, depth: int) -> PageSummary:
    return PageSummary(
        url=url,
        final_url=None,
        referrer_url=referrer_url,
        depth=depth,
        allowed_by_robots=False,
        http_code=None,
        response_bytes=0,
        content_type=None,
        is_html=False,
        title=None,
        meta_description_length=None,
        canonical_url=None,
        meta_robots=None,
        x_robots_tag=None,
        h1_count=0,
        word_count=0,
        internal_link_count=0,
        external_link_count=0,
        has_json_ld=False,
        has_open_graph=False,
        has_twitter_card=False,
    )


def create_crawl_run(
    conn: sqlite3.Connection,
    repository_full_name: str,
    homepage_url: str,
    site_origin: str,
) -> int:
    with conn:
        cursor = conn.execute(
            """
            INSERT INTO crawl_runs (
                repository_full_name,
                homepage_url,
                site_origin,
                user_agent,
                status,
                started_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                repository_full_name,
                homepage_url,
                site_origin,
                USER_AGENT,
                "running",
                utc_now(),
            ),
        )
    return int(cursor.lastrowid)


def finalize_crawl_run(
    conn: sqlite3.Connection,
    crawl_run_id: int,
    *,
    site_origin: str,
    homepage_final_url: str | None,
    homepage_http_code: int | None,
    homepage_response_bytes: int,
    robots_txt_present: bool,
    llms_txt_present: bool,
    llm_txt_present: bool,
    agents_json_present: bool,
    pages_discovered: int,
    pages_crawled: int,
    pages_blocked: int,
    html_pages: int,
    pages_with_title: int,
    pages_with_meta_description: int,
    pages_with_canonical: int,
    pages_with_json_ld: int,
    pages_with_open_graph: int,
    pages_with_twitter_card: int,
    findings_count: int,
    status: str = "completed",
    failure_reason: str | None = None,
) -> None:
    with conn:
        conn.execute(
            """
            UPDATE crawl_runs
            SET site_origin = ?,
                status = ?,
                failure_reason = ?,
                completed_at = ?,
                homepage_final_url = ?,
                homepage_http_code = ?,
                homepage_response_bytes = ?,
                robots_txt_present = ?,
                llms_txt_present = ?,
                llm_txt_present = ?,
                agents_json_present = ?,
                pages_discovered = ?,
                pages_crawled = ?,
                pages_blocked = ?,
                html_pages = ?,
                pages_with_title = ?,
                pages_with_meta_description = ?,
                pages_with_canonical = ?,
                pages_with_json_ld = ?,
                pages_with_open_graph = ?,
                pages_with_twitter_card = ?,
                findings_count = ?
            WHERE id = ?
            """,
            (
                site_origin,
                status,
                failure_reason,
                utc_now(),
                homepage_final_url,
                homepage_http_code,
                homepage_response_bytes,
                int(robots_txt_present),
                int(llms_txt_present),
                int(llm_txt_present),
                int(agents_json_present),
                pages_discovered,
                pages_crawled,
                pages_blocked,
                html_pages,
                pages_with_title,
                pages_with_meta_description,
                pages_with_canonical,
                pages_with_json_ld,
                pages_with_open_graph,
                pages_with_twitter_card,
                findings_count,
                crawl_run_id,
            ),
        )


def fail_crawl_run(
    conn: sqlite3.Connection,
    crawl_run_id: int,
    site_origin: str,
    failure_reason: str,
) -> None:
    finalize_crawl_run(
        conn,
        crawl_run_id,
        site_origin=site_origin,
        homepage_final_url=None,
        homepage_http_code=None,
        homepage_response_bytes=0,
        robots_txt_present=False,
        llms_txt_present=False,
        llm_txt_present=False,
        agents_json_present=False,
        pages_discovered=0,
        pages_crawled=0,
        pages_blocked=0,
        html_pages=0,
        pages_with_title=0,
        pages_with_meta_description=0,
        pages_with_canonical=0,
        pages_with_json_ld=0,
        pages_with_open_graph=0,
        pages_with_twitter_card=0,
        findings_count=0,
        status="failed",
        failure_reason=failure_reason[:500],
    )


def store_legacy_crawl(
    conn: sqlite3.Connection,
    homepage_url: str,
    http_code: int | None,
    response_bytes: int,
) -> None:
    with conn:
        conn.execute(
            """
            INSERT INTO crawls (homepage_url, http_code, response_bytes, crawled_at)
            VALUES (?, ?, ?, ?)
            """,
            (homepage_url, http_code, response_bytes, utc_now()),
        )


def store_asset(
    conn: sqlite3.Connection,
    crawl_run_id: int,
    asset: AssetSummary,
) -> None:
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO crawl_assets (
                crawl_run_id,
                asset_type,
                asset_url,
                http_code,
                response_bytes,
                content_type,
                is_present,
                parsed_ok,
                item_count,
                fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                crawl_run_id,
                asset.asset_type,
                asset.asset_url,
                asset.http_code,
                asset.response_bytes,
                asset.content_type,
                int(asset.is_present),
                int(asset.parsed_ok),
                asset.item_count,
                utc_now(),
            ),
        )


def store_page(
    conn: sqlite3.Connection,
    crawl_run_id: int,
    page: PageSummary,
) -> None:
    with conn:
        conn.execute(
            """
            INSERT OR REPLACE INTO crawl_pages (
                crawl_run_id,
                url,
                final_url,
                referrer_url,
                depth,
                allowed_by_robots,
                http_code,
                response_bytes,
                content_type,
                is_html,
                title,
                meta_description_length,
                canonical_url,
                meta_robots,
                x_robots_tag,
                h1_count,
                word_count,
                internal_link_count,
                external_link_count,
                has_json_ld,
                has_open_graph,
                has_twitter_card,
                fetched_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                crawl_run_id,
                page.url,
                page.final_url,
                page.referrer_url,
                page.depth,
                int(page.allowed_by_robots),
                page.http_code,
                page.response_bytes,
                page.content_type,
                int(page.is_html),
                page.title,
                page.meta_description_length,
                page.canonical_url,
                page.meta_robots,
                page.x_robots_tag,
                page.h1_count,
                page.word_count,
                page.internal_link_count,
                page.external_link_count,
                int(page.has_json_ld),
                int(page.has_open_graph),
                int(page.has_twitter_card),
                utc_now(),
            ),
        )


def store_findings(
    conn: sqlite3.Connection,
    crawl_run_id: int,
    findings: list[Finding],
) -> None:
    if not findings:
        return

    with conn:
        conn.executemany(
            """
            INSERT INTO crawl_findings (
                crawl_run_id,
                category,
                code,
                severity,
                page_url,
                metric_value,
                message,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    crawl_run_id,
                    finding.category,
                    finding.code,
                    finding.severity,
                    finding.page_url,
                    finding.metric_value,
                    finding.message,
                    utc_now(),
                )
                for finding in findings
            ],
        )


def build_findings(
    assets: list[AssetSummary],
    pages: list[PageSummary],
) -> list[Finding]:
    findings: list[Finding] = []
    html_pages = [page for page in pages if page.is_html]

    asset_present = {asset.asset_type for asset in assets if asset.is_present}
    asset_lookup = {asset.asset_type: asset for asset in assets}

    if "robots_txt" not in asset_present:
        findings.append(
            Finding(
                category="crawl",
                code="missing_robots_txt",
                severity="info",
                page_url=None,
                metric_value=1,
                message="No robots.txt file was found.",
            )
        )

    if "llms_txt" not in asset_present and "llm_txt" not in asset_present:
        findings.append(
            Finding(
                category="llm",
                code="missing_llms_txt",
                severity="info",
                page_url=None,
                metric_value=1,
                message="No llms.txt or llm.txt file was found.",
            )
        )

    agents_assets = [
        asset
        for asset in assets
        if asset.asset_type in {"agents_json", "well_known_agents_json"}
    ]
    if not any(asset.is_present for asset in agents_assets):
        findings.append(
            Finding(
                category="llm",
                code="missing_agents_json",
                severity="info",
                page_url=None,
                metric_value=1,
                message="No agents.json file was found at the root or .well-known path.",
            )
        )
    elif any(asset.is_present and not asset.parsed_ok for asset in agents_assets):
        broken = next(
            asset.asset_url for asset in agents_assets if asset.is_present and not asset.parsed_ok
        )
        findings.append(
            Finding(
                category="llm",
                code="invalid_agents_json",
                severity="warn",
                page_url=broken,
                metric_value=1,
                message="An agents.json file was found but could not be parsed as JSON.",
            )
        )

    if asset_lookup.get("robots_txt") and asset_lookup["robots_txt"].http_code in {401, 403}:
        findings.append(
            Finding(
                category="crawl",
                code="robots_denies_access",
                severity="warn",
                page_url=None,
                metric_value=1,
                message="robots.txt returned 401/403, which effectively blocks crawling.",
            )
        )

    def aggregate_page_issue(
        code: str,
        severity: str,
        message_template: str,
        predicate,
    ) -> None:
        matches = [page for page in html_pages if predicate(page)]
        if not matches:
            return
        findings.append(
            Finding(
                category="seo",
                code=code,
                severity=severity,
                page_url=matches[0].final_url or matches[0].url,
                metric_value=len(matches),
                message=message_template.format(count=len(matches), total=len(html_pages)),
            )
        )

    aggregate_page_issue(
        "missing_title",
        "warn",
        "{count} of {total} HTML pages are missing a <title> tag.",
        lambda page: not page.title,
    )
    aggregate_page_issue(
        "missing_meta_description",
        "info",
        "{count} of {total} HTML pages are missing a meta description.",
        lambda page: page.meta_description_length is None,
    )
    aggregate_page_issue(
        "missing_canonical",
        "info",
        "{count} of {total} HTML pages are missing a canonical link.",
        lambda page: not page.canonical_url,
    )
    aggregate_page_issue(
        "missing_h1",
        "warn",
        "{count} of {total} HTML pages are missing an H1 heading.",
        lambda page: page.h1_count == 0,
    )
    aggregate_page_issue(
        "missing_json_ld",
        "info",
        "{count} of {total} HTML pages are missing JSON-LD structured data.",
        lambda page: not page.has_json_ld,
    )
    aggregate_page_issue(
        "missing_open_graph",
        "info",
        "{count} of {total} HTML pages are missing Open Graph metadata.",
        lambda page: not page.has_open_graph,
    )
    aggregate_page_issue(
        "missing_twitter_card",
        "info",
        "{count} of {total} HTML pages are missing Twitter card metadata.",
        lambda page: not page.has_twitter_card,
    )
    aggregate_page_issue(
        "noindex_detected",
        "warn",
        "{count} of {total} HTML pages include a noindex directive.",
        lambda page: "noindex" in ((page.meta_robots or "") + " " + (page.x_robots_tag or "")).lower(),
    )
    return findings


def crawl_site(
    conn: sqlite3.Connection,
    repository_full_name: str,
    homepage_url: str,
    timeout: float,
    max_pages: int,
    max_depth: int,
) -> tuple[bool, str]:
    normalized_homepage = normalize_url(homepage_url)
    initial_origin = origin_from_url(normalized_homepage)
    crawl_run_id = create_crawl_run(
        conn,
        repository_full_name=repository_full_name,
        homepage_url=normalized_homepage,
        site_origin=initial_origin,
    )

    active_origin = initial_origin
    homepage_result: FetchResult | None = None
    pages: list[PageSummary] = []
    assets: list[AssetSummary] = []
    discovered_urls: set[str] = {normalized_homepage}

    try:
        robots = parse_robots(initial_origin, timeout)
        assets.append(robots.asset)
        store_asset(conn, crawl_run_id, robots.asset)

        if robots.can_fetch(normalized_homepage):
            homepage_result = fetch_url(normalized_homepage, timeout)
            store_legacy_crawl(
                conn,
                homepage_url=normalized_homepage,
                http_code=homepage_result.http_code,
                response_bytes=homepage_result.response_bytes,
            )
            homepage_page, homepage_links = summarize_page(homepage_result, None, 0)
            pages.append(homepage_page)
            store_page(conn, crawl_run_id, homepage_page)

            active_origin = origin_from_url(homepage_page.final_url or normalized_homepage)
            if active_origin != initial_origin:
                robots = parse_robots(active_origin, timeout)
                assets.append(robots.asset)
                store_asset(conn, crawl_run_id, robots.asset)
        else:
            blocked = blocked_page(normalized_homepage, None, 0)
            pages.append(blocked)
            store_page(conn, crawl_run_id, blocked)
            store_legacy_crawl(
                conn,
                homepage_url=normalized_homepage,
                http_code=None,
                response_bytes=0,
            )
            homepage_links = []

        for asset_type, asset_path in SPECIAL_ASSETS:
            asset = analyze_special_asset(asset_type, urljoin(active_origin + "/", asset_path.lstrip("/")), timeout)
            assets.append(asset)
            store_asset(conn, crawl_run_id, asset)

        queue: deque[tuple[str, str | None, int]] = deque()
        queued_urls: set[str] = set()
        fetched_pages = 1 if homepage_result or pages else 0

        if homepage_result and pages[0].is_html and max_depth > 0:
            for link in homepage_links:
                if link in discovered_urls:
                    continue
                discovered_urls.add(link)
                queue.append((link, pages[0].final_url or pages[0].url, 1))
                queued_urls.add(link)

        site_netloc = urlparse(active_origin).netloc.lower()

        while queue and fetched_pages < max_pages:
            url, referrer_url, depth = queue.popleft()
            queued_urls.discard(url)

            if depth > max_depth:
                continue

            if not robots.can_fetch(url):
                blocked = blocked_page(url, referrer_url, depth)
                pages.append(blocked)
                store_page(conn, crawl_run_id, blocked)
                continue

            result = fetch_url(url, timeout)
            page, links = summarize_page(result, referrer_url, depth)
            pages.append(page)
            store_page(conn, crawl_run_id, page)
            fetched_pages += 1

            if (
                page.is_html
                and depth < max_depth
                and page.final_url
                and urlparse(page.final_url).netloc.lower() == site_netloc
            ):
                for link in links:
                    if link in discovered_urls:
                        continue
                    discovered_urls.add(link)
                    queue.append((link, page.final_url, depth + 1))
                    queued_urls.add(link)

        findings = build_findings(assets, pages)
        store_findings(conn, crawl_run_id, findings)

        html_pages = [page for page in pages if page.is_html]
        agents_present = any(
            asset.is_present
            for asset in assets
            if asset.asset_type in {"agents_json", "well_known_agents_json"}
        )
        finalize_crawl_run(
            conn,
            crawl_run_id,
            site_origin=active_origin,
            homepage_final_url=pages[0].final_url if pages else None,
            homepage_http_code=pages[0].http_code if pages else None,
            homepage_response_bytes=pages[0].response_bytes if pages else 0,
            robots_txt_present=any(
                asset.is_present for asset in assets if asset.asset_type == "robots_txt"
            ),
            llms_txt_present=any(
                asset.is_present for asset in assets if asset.asset_type == "llms_txt"
            ),
            llm_txt_present=any(
                asset.is_present for asset in assets if asset.asset_type == "llm_txt"
            ),
            agents_json_present=agents_present,
            pages_discovered=len(discovered_urls),
            pages_crawled=sum(1 for page in pages if page.http_code is not None),
            pages_blocked=sum(1 for page in pages if not page.allowed_by_robots),
            html_pages=len(html_pages),
            pages_with_title=sum(1 for page in html_pages if page.title),
            pages_with_meta_description=sum(
                1 for page in html_pages if page.meta_description_length is not None
            ),
            pages_with_canonical=sum(1 for page in html_pages if page.canonical_url),
            pages_with_json_ld=sum(1 for page in html_pages if page.has_json_ld),
            pages_with_open_graph=sum(1 for page in html_pages if page.has_open_graph),
            pages_with_twitter_card=sum(1 for page in html_pages if page.has_twitter_card),
            findings_count=len(findings),
        )

        robots_flag = "yes" if any(
            asset.is_present for asset in assets if asset.asset_type == "robots_txt"
        ) else "no"
        llms_flag = "yes" if any(
            asset.is_present for asset in assets if asset.asset_type in {"llms_txt", "llm_txt"}
        ) else "no"
        agents_flag = "yes" if agents_present else "no"

        return (
            True,
            (
                f"{normalized_homepage} -> pages={sum(1 for page in pages if page.http_code is not None)} "
                f"html={len(html_pages)} robots={robots_flag} llms={llms_flag} "
                f"agents={agents_flag} findings={len(findings)}"
            ),
        )
    except Exception as exc:
        fail_crawl_run(conn, crawl_run_id, site_origin=active_origin, failure_reason=str(exc))
        raise


def main() -> int:
    args = parse_args()

    if args.limit is not None and args.limit <= 0:
        print("--limit must be > 0", file=sys.stderr)
        return 2

    if args.timeout <= 0:
        print("--timeout must be > 0", file=sys.stderr)
        return 2

    if args.max_pages <= 0:
        print("--max-pages must be > 0", file=sys.stderr)
        return 2

    if args.max_depth < 0:
        print("--max-depth must be >= 0", file=sys.stderr)
        return 2

    conn = connect_db(args.db)
    try:
        migrate_db(conn)
        rows = load_homepages(conn, args.limit)

        success_count = 0
        failure_count = 0

        for repository_full_name, homepage_url in rows:
            try:
                ok, message = crawl_site(
                    conn,
                    repository_full_name=repository_full_name,
                    homepage_url=homepage_url,
                    timeout=args.timeout,
                    max_pages=args.max_pages,
                    max_depth=args.max_depth,
                )
            except ValueError as exc:
                ok = False
                message = f"{homepage_url} - {exc}"
            except Exception as exc:
                ok = False
                message = f"{homepage_url} - crawl failed: {exc}"

            if ok:
                success_count += 1
                print(message, flush=True)
            else:
                failure_count += 1
                print(message, file=sys.stderr, flush=True)
    finally:
        conn.close()

    print(
        (
            f"Crawled {len(rows)} sites from {args.db} "
            f"({success_count} succeeded, {failure_count} failed)"
        ),
        file=sys.stderr,
    )
    return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
