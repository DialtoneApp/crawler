from __future__ import annotations

import json
import shutil
import sys
import time
from collections import Counter
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .constants import INTERESTING_PROBE_KEYS
from .inputs import iter_domains_from_offset, iter_limited, parse_args
from .models import CrawlReceipt, DomainInput, ReceiptShardState
from .outcomes import serialize_receipt
from .probe import probe_domain

ARTIFACT_FILE_NAMES = {
    "robots_txt": "robots.txt",
    "llms_txt": "llms.txt",
    "llms_full_txt": "llms-full.txt",
}


@dataclass(frozen=True)
class InFlightTask:
    domain_input: DomainInput
    submitted_at: float


def submit_next(
    executor: ThreadPoolExecutor,
    futures: dict[Future[CrawlReceipt], InFlightTask],
    domains: Any,
    timeout: float,
    run_token: str,
    wall_clock_limit: float,
    rate_limit_retry_passes: int,
    rate_limit_retry_delay: float,
) -> bool:
    try:
        domain_input = next(domains)
    except StopIteration:
        return False

    future = executor.submit(
        probe_domain,
        domain_input,
        timeout,
        run_token,
        wall_clock_limit,
        rate_limit_retry_passes,
        rate_limit_retry_delay,
    )
    futures[future] = InFlightTask(domain_input=domain_input, submitted_at=time.monotonic())
    return True


def load_checkpoint(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def write_checkpoint(path: Path, payload: dict[str, Any]) -> None:
    temp_path = path.with_suffix(path.suffix + ".tmp")
    temp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    temp_path.replace(path)


class ReceiptShardWriter:
    def __init__(
        self,
        receipts_dir: Path,
        *,
        max_bytes: int,
        max_records: int,
        initial_state: ReceiptShardState | None = None,
    ) -> None:
        self.receipts_dir = receipts_dir
        self.max_bytes = max(0, max_bytes)
        self.max_records = max(0, max_records)
        self.state = initial_state or ReceiptShardState()
        self._file: Any | None = None

    def __enter__(self) -> "ReceiptShardWriter":
        self.receipts_dir.mkdir(parents=True, exist_ok=True)
        self._open_current_shard()
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        self.close()

    @property
    def current_path(self) -> Path:
        return self.receipts_dir / f"receipt-{self.state.shard_index:06d}.ndjson"

    def _open_current_shard(self) -> None:
        if self._file is not None:
            self._file.close()
        self._file = self.current_path.open("a", encoding="utf-8")

        try:
            existing_size = self.current_path.stat().st_size
        except OSError:
            existing_size = 0

        if existing_size > self.state.byte_count:
            self.state.byte_count = existing_size

    def _rotate(self) -> None:
        if self._file is not None:
            self._file.close()
        self.state = ReceiptShardState(shard_index=self.state.shard_index + 1)
        self._open_current_shard()

    def _should_rotate(self, encoded_length: int) -> bool:
        if self.state.record_count == 0:
            return False
        if self.max_records and self.state.record_count >= self.max_records:
            return True
        if self.max_bytes and (self.state.byte_count + encoded_length) > self.max_bytes:
            return True
        return False

    def write_line(self, line: str) -> None:
        encoded_length = len((line + "\n").encode("utf-8"))
        if self._should_rotate(encoded_length):
            self._rotate()

        if self._file is None:
            self._open_current_shard()

        self._file.write(line)
        self._file.write("\n")
        self.state.record_count += 1
        self.state.byte_count += encoded_length

    def flush(self) -> None:
        if self._file is not None:
            self._file.flush()

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None

    def snapshot(self) -> dict[str, int]:
        return {
            "shard_index": self.state.shard_index,
            "record_count": self.state.record_count,
            "byte_count": self.state.byte_count,
        }


def has_interesting_signal(receipt: CrawlReceipt) -> bool:
    return any(
        receipt.probes.get(key) and receipt.probes[key].status == "valid"
        for key in INTERESTING_PROBE_KEYS
    )


def write_receipt_artifacts(evidence_dir: Path, receipt: CrawlReceipt) -> None:
    domain_dir = evidence_dir / receipt.domain
    if domain_dir.exists():
        try:
            shutil.rmtree(domain_dir)
        except OSError:
            pass
    if not receipt.artifacts:
        return
    domain_dir.mkdir(parents=True, exist_ok=True)
    for artifact_key, body in receipt.artifacts.items():
        file_name = ARTIFACT_FILE_NAMES.get(artifact_key)
        if not file_name or not isinstance(body, bytes) or not body:
            continue
        (domain_dir / file_name).write_bytes(body)


def remove_receipt_artifacts(evidence_dir: Path, domain: str) -> None:
    domain_dir = evidence_dir / domain
    if not domain_dir.exists():
        return
    try:
        shutil.rmtree(domain_dir)
    except OSError:
        pass


def build_checkpoint_payload(
    *,
    completed: int,
    found: int,
    next_row_index: int,
    started_at: float,
    label_counts: Counter[str],
    probe_status_counts: Counter[str],
    receipt_shard: dict[str, int],
) -> dict[str, Any]:
    return {
        "completed": completed,
        "found": found,
        "next_row_index": next_row_index,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_seconds": round(max(time.monotonic() - started_at, 0.0), 3),
        "label_counts": dict(sorted(label_counts.items())),
        "probe_status_counts": dict(sorted(probe_status_counts.items())),
        "receipt_shard": receipt_shard,
    }


def maybe_log_stalled_futures(
    futures: dict[Future[CrawlReceipt], InFlightTask],
    *,
    now: float,
    last_log_at: float,
    stalled_log_every: float,
) -> float:
    if stalled_log_every <= 0 or not futures:
        return last_log_at

    oldest_task = min(futures.values(), key=lambda task: task.submitted_at)
    oldest_age = max(now - oldest_task.submitted_at, 0.0)
    if last_log_at == 0.0:
        if oldest_age < stalled_log_every:
            return last_log_at
    elif (now - last_log_at) < stalled_log_every:
        return last_log_at

    print(
        f"waiting pending={len(futures)} oldest_domain={oldest_task.domain_input.domain} oldest_age={oldest_age:.1f}s",
        file=sys.stderr,
    )
    return now


def crawl(args) -> int:
    csv_path = Path(args.csv)
    results_dir = Path(args.results_dir)
    receipts_dir = results_dir / "receipts"
    positives_dir = results_dir / "positives"
    evidence_dir = results_dir / "evidence"
    checkpoint_path = results_dir / "checkpoint.json"

    if args.concurrency < 1:
        print("--concurrency must be at least 1", file=sys.stderr)
        return 2
    if args.receipt_shard_max_bytes < 0:
        print("--receipt-shard-max-bytes must be 0 or greater", file=sys.stderr)
        return 2
    if args.receipt_shard_max_records < 0:
        print("--receipt-shard-max-records must be 0 or greater", file=sys.stderr)
        return 2
    if args.stalled_log_every < 0:
        print("--stalled-log-every must be 0 or greater", file=sys.stderr)
        return 2
    if args.domain_wall_clock_limit < 0:
        print("--domain-wall-clock-limit must be 0 or greater", file=sys.stderr)
        return 2
    if args.rate_limit_retry_passes < 0:
        print("--rate-limit-retry-passes must be 0 or greater", file=sys.stderr)
        return 2
    if args.rate_limit_retry_delay < 0:
        print("--rate-limit-retry-delay must be 0 or greater", file=sys.stderr)
        return 2
    if not csv_path.exists():
        print(f"CSV file not found: {csv_path}", file=sys.stderr)
        return 2

    results_dir.mkdir(parents=True, exist_ok=True)
    receipts_dir.mkdir(parents=True, exist_ok=True)
    positives_dir.mkdir(parents=True, exist_ok=True)
    evidence_dir.mkdir(parents=True, exist_ok=True)

    checkpoint = None if args.no_resume else load_checkpoint(checkpoint_path)
    start_row_index = int(checkpoint.get("next_row_index", 0)) if checkpoint else 0
    completed = int(checkpoint.get("completed", 0)) if checkpoint else 0
    found = int(checkpoint.get("found", 0)) if checkpoint else 0
    label_counts: Counter[str] = Counter(checkpoint.get("label_counts", {})) if checkpoint else Counter()
    probe_status_counts: Counter[str] = Counter(checkpoint.get("probe_status_counts", {})) if checkpoint else Counter()
    checkpoint_receipt_shard = checkpoint.get("receipt_shard", {}) if checkpoint else {}
    receipt_shard_state = ReceiptShardState(
        shard_index=max(1, int(checkpoint_receipt_shard.get("shard_index", 1))),
        record_count=max(0, int(checkpoint_receipt_shard.get("record_count", 0))),
        byte_count=max(0, int(checkpoint_receipt_shard.get("byte_count", 0))),
    )

    if checkpoint and not args.no_resume:
        print(
            f"resuming from row_index={start_row_index} completed={completed} found={found} shard=receipt-{receipt_shard_state.shard_index:06d}",
            file=sys.stderr,
        )
    elif args.no_resume and any(receipts_dir.glob("receipt-*.ndjson")):
        print(
            f"--no-resume requested; appending fresh rows into {receipts_dir}",
            file=sys.stderr,
        )

    domains = iter_domains_from_offset(csv_path, start_row_index)
    if args.limit is not None:
        domains = iter_limited(domains, args.limit)

    started_at = time.monotonic()
    futures: dict[Future[CrawlReceipt], InFlightTask] = {}
    run_token = hex(int(started_at * 1_000_000))[2:]
    last_completed_row_index = start_row_index
    interrupted = False
    last_stalled_log_at = 0.0
    receipt_writer = ReceiptShardWriter(
        receipts_dir,
        max_bytes=args.receipt_shard_max_bytes,
        max_records=args.receipt_shard_max_records,
        initial_state=receipt_shard_state,
    )
    executor = ThreadPoolExecutor(max_workers=args.concurrency)

    try:
        with receipt_writer:
            for _ in range(args.concurrency):
                if not submit_next(
                    executor,
                    futures,
                    domains,
                    args.timeout,
                    run_token,
                    args.domain_wall_clock_limit,
                    args.rate_limit_retry_passes,
                    args.rate_limit_retry_delay,
                ):
                    break

            while futures:
                done, _ = wait(futures, timeout=1.0, return_when=FIRST_COMPLETED)
                if not done:
                    last_stalled_log_at = maybe_log_stalled_futures(
                        futures,
                        now=time.monotonic(),
                        last_log_at=last_stalled_log_at,
                        stalled_log_every=args.stalled_log_every,
                    )
                    continue

                last_stalled_log_at = 0.0
                for future in done:
                    task = futures.pop(future)
                    domain_input = task.domain_input

                    try:
                        receipt = future.result()
                    except Exception as error:
                        print(f"ERROR {domain_input.domain}: {error}", file=sys.stderr)
                        receipt = CrawlReceipt(
                            domain=domain_input.domain,
                            rank=domain_input.rank,
                            row_index=domain_input.row_index,
                            crawled_at=datetime.now(timezone.utc).isoformat(),
                            label="internal_error",
                            tags=[],
                            title=None,
                            probes={},
                            aggregates={},
                        )

                    completed += 1
                    last_completed_row_index = max(last_completed_row_index, domain_input.row_index + 1)
                    label_counts[receipt.label] += 1
                    for outcome in receipt.probes.values():
                        probe_status_counts[f"{outcome.key}:{outcome.status}"] += 1

                    serialized = serialize_receipt(receipt)
                    receipt_writer.write_line(json.dumps(serialized, separators=(",", ":"), sort_keys=True))
                    positive_path = positives_dir / f"{receipt.domain}.json"

                    if has_interesting_signal(receipt):
                        found += 1
                        interesting_keys = sorted(
                            key for key in INTERESTING_PROBE_KEYS
                            if receipt.probes.get(key) and receipt.probes[key].status == "valid"
                        )
                        positive_path.write_text(
                            json.dumps(serialized, indent=2, sort_keys=True),
                            encoding="utf-8",
                        )
                        write_receipt_artifacts(evidence_dir, receipt)
                        print(f"FOUND {receipt.domain} {receipt.label} {','.join(interesting_keys)}")
                    elif positive_path.exists():
                        try:
                            positive_path.unlink()
                        except OSError:
                            pass
                        remove_receipt_artifacts(evidence_dir, receipt.domain)
                    else:
                        remove_receipt_artifacts(evidence_dir, receipt.domain)

                    if args.progress_every and completed % args.progress_every == 0:
                        elapsed = max(time.monotonic() - started_at, 0.001)
                        rate = completed / elapsed
                        print(
                            f"progress completed={completed} found={found} rate={rate:.1f}/s labels={dict(label_counts.most_common(5))}",
                            file=sys.stderr,
                        )

                    if args.checkpoint_every and completed % args.checkpoint_every == 0:
                        receipt_writer.flush()
                        write_checkpoint(
                            checkpoint_path,
                            build_checkpoint_payload(
                                completed=completed,
                                found=found,
                                next_row_index=last_completed_row_index,
                                started_at=started_at,
                                label_counts=label_counts,
                                probe_status_counts=probe_status_counts,
                                receipt_shard=receipt_writer.snapshot(),
                            ),
                        )

                    submit_next(
                        executor,
                        futures,
                        domains,
                        args.timeout,
                        run_token,
                        args.domain_wall_clock_limit,
                        args.rate_limit_retry_passes,
                        args.rate_limit_retry_delay,
                    )
    except KeyboardInterrupt:
        interrupted = True
        print(
            f"interrupt received; writing checkpoint at row_index={last_completed_row_index} completed={completed} found={found}",
            file=sys.stderr,
        )
    finally:
        for future in futures:
            future.cancel()
        executor.shutdown(wait=False, cancel_futures=True)
        receipt_writer.flush()
        write_checkpoint(
            checkpoint_path,
            build_checkpoint_payload(
                completed=completed,
                found=found,
                next_row_index=last_completed_row_index,
                started_at=started_at,
                label_counts=label_counts,
                probe_status_counts=probe_status_counts,
                receipt_shard=receipt_writer.snapshot(),
            ),
        )

    print(
        f"{'interrupted' if interrupted else 'done'} completed={completed} found={found} shard=receipt-{receipt_writer.state.shard_index:06d} labels={dict(label_counts.most_common())}",
        file=sys.stderr,
    )
    return 130 if interrupted else 0


def main() -> int:
    return crawl(parse_args())
