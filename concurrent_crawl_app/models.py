from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Any


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
    request_method: str = "GET"
    request_content_type: str | None = None
    final_url: str | None = None
    status: int | None = None
    content_type: str | None = None
    body: bytes = b""
    truncated: bool = False
    error: str | None = None
    headers: dict[str, str] = field(default_factory=dict)

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
