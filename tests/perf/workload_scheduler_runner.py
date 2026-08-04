#!/usr/bin/env python3
# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Exercise GreptimeDB's experimental query/write workload scheduler.

Start a standalone server, then run this script against its HTTP port. The
script creates and seeds a real Mito table and executes concurrent request
phases. By default it also verifies the experimental workload scheduler's
Prometheus poll-admission counters; metrics can be disabled when benchmarking
against a scheduler-disabled baseline.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import json
import math
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

# Ensure the tests/perf directory is on the import path so report is importable
# when this module is loaded via importlib.util.spec_from_file_location.
_perf_dir = str(Path(__file__).resolve().parent)
if _perf_dir not in sys.path:
    sys.path.insert(0, _perf_dir)

from workload_scheduler_report import (
    SchedulerMetrics,
    parse_scheduler_metrics,
    summarize_scheduler_metrics,
)

# Default constants (overridden via run_phase parameters)
_DEFAULT_QUERY_TABLE = "catio_scheduler_query_load"
_DEFAULT_WRITE_TABLE = "catio_scheduler_write_load"
_DEFAULT_QUERY_SQL = (
    "SELECT host, count(*), sum(val), avg(val) FROM catio_scheduler_query_load "
    "GROUP BY host ORDER BY host"
)
_DEFAULT_SHARDS = 64
_DEFAULT_QUERY_PARTITIONS = 32
_DEFAULT_WRITE_PARTITIONS = 64
_DEFAULT_SEED_TIMESTAMP_MILLIS = 1_700_000_000_000
_DEFAULT_WRITE_SEQUENCE_START_MILLIS = 1_800_000_000_000


@dataclasses.dataclass(frozen=True)
class RequestEvent:
    """Immutable terminal event for one measured request."""

    token: int
    workload: str
    submission_offset: float  # seconds from measurement_start
    completion_offset: float | None  # seconds from measurement_start
    status: str  # "success" | "failure" | "timeout"
    latency_ms: float | None
    error: str | None


@dataclasses.dataclass(frozen=True)
class RequestStats:
    started: int
    completed: int
    completed_failures: int
    timeouts: int
    outstanding: int
    latencies_ms: tuple[float, ...]
    failure_samples: tuple[str, ...]
    events: tuple[RequestEvent, ...]

    @property
    def requests(self) -> int:
        return self.started

    @property
    def failures(self) -> int:
        return self.completed_failures + self.timeouts

    def summary(self, duration: float) -> dict[str, Any]:
        successful = self.completed - self.completed_failures
        latencies = sorted(self.latencies_ms)
        return {
            "started": self.started,
            "requests": self.requests,
            "completed": self.completed,
            "completed_failures": self.completed_failures,
            "timeouts": self.timeouts,
            "outstanding": self.outstanding,
            "failures": self.failures,
            "failure_samples": list(self.failure_samples),
            "successful_rps": successful / duration if duration > 0 else 0.0,
            "mean_ms": statistics.fmean(latencies) if latencies else None,
            "p50_ms": percentile(latencies, 0.50),
            "p95_ms": percentile(latencies, 0.95),
        }


@dataclasses.dataclass(frozen=True)
class ScrapeRecord:
    """One scheduled /metrics scrape result.

    ``start`` and ``completion`` are **offsets** (seconds) from
    ``measurement_start``, not absolute ``time.monotonic()`` values.
    ``completion`` is ``None`` for missed/error records.
    """

    offset: float  # scheduled offset from measurement_start
    start: float  # monotonic time when scrape began
    completion: float | None  # monotonic time when scrape ended
    status: str  # "success" | "error" | "missed"
    http_status: int | None
    error: str | None
    text: str | None
    path: str | None


@dataclasses.dataclass
class PhaseClock:
    warmup: float
    duration: float
    drain_timeout: float = 5.0
    measurement_start: float = 0.0
    deadline: float = 0.0

    def __post_init__(self) -> None:
        if self.drain_timeout <= 0:
            raise ValueError("drain_timeout must be greater than zero")

    @property
    def drain_deadline(self) -> float:
        return self.deadline + self.drain_timeout

    def start(self, started: float | None = None) -> None:
        if started is None:
            started = time.monotonic()
        self.measurement_start = started + self.warmup
        self.deadline = self.measurement_start + self.duration

    def request_timeout(self, started: float, fallback: float) -> float:
        return min(fallback, max(self.drain_deadline - started, 0.001))


class RequestWindow:
    """Thread-safe accounting for requests selected by their start timestamp.

    Provides condition-based waiting for efficient drain: callers can wait
    until all measured requests have completed without unconditionally sleeping
    the full drain timeout.
    """

    def __init__(self, clock: PhaseClock) -> None:
        self.clock = clock
        self.lock = threading.Lock()
        self._cond = threading.Condition(self.lock)
        self.next_token = 0
        self.started = 0
        self.completed = 0
        self.completed_failures = 0
        self.latencies_ms: list[float] = []
        self.failure_samples: list[str] = []
        self.frozen: RequestStats | None = None
        self._pending_submissions: dict[int, tuple[str, float]] = {}
        self._events: list[RequestEvent] = []

    def begin(self, started: float, workload: str = "") -> int | None:
        """Attempt to register a request with the given start time.

        Returns a token if the request falls within (measurement_start, deadline)
        and the window is not yet frozen. The token must be passed to complete().
        """
        if not self.clock.measurement_start <= started < self.clock.deadline:
            return None
        with self.lock:
            if self.frozen is not None:
                return None
            token = self.next_token
            self.next_token += 1
            self._pending_submissions[token] = (
                workload,
                started - self.clock.measurement_start,
            )
            self.started += 1
            return token

    def complete(
        self, token: int, completed: float, ok: bool, latency: float, body: Any
    ) -> None:
        """Record completion of a previously begun request.

        Only the first valid call per token takes effect. Completions after
        freeze are silently ignored. Completions with completed >= drain_deadline
        remain pending and become timeout events at freeze.
        """
        with self.lock:
            if self.frozen is not None:
                return
            if token not in self._pending_submissions:
                return
            if completed >= self.clock.drain_deadline:
                # >= drain_deadline means pending until freeze (timeout).
                return
            workload, sub_offset = self._pending_submissions.pop(token)
            completion_offset = completed - self.clock.measurement_start
            self.completed += 1
            if ok:
                self.latencies_ms.append(latency)
                event = RequestEvent(
                    token=token,
                    workload=workload,
                    submission_offset=sub_offset,
                    completion_offset=completion_offset,
                    status="success",
                    latency_ms=latency,
                    error=None,
                )
            else:
                self.completed_failures += 1
                error_text = _bounded_error(body)
                self._record_failure(error_text)
                event = RequestEvent(
                    token=token,
                    workload=workload,
                    submission_offset=sub_offset,
                    completion_offset=completion_offset,
                    status="failure",
                    latency_ms=latency,
                    error=error_text,
                )
            self._events.append(event)
            self._cond.notify_all()

    def freeze(self) -> RequestStats:
        """Seal the window and produce an immutable snapshot.

        Any pending (uncompleted) measured requests become timeout events.
        Idempotent: subsequent calls return the same snapshot.
        """
        with self.lock:
            if self.frozen is not None:
                return self.frozen
            outstanding = len(self._pending_submissions)
            for token, (workload, sub_offset) in self._pending_submissions.items():
                event = RequestEvent(
                    token=token,
                    workload=workload,
                    submission_offset=sub_offset,
                    completion_offset=None,
                    status="timeout",
                    latency_ms=None,
                    error=None,
                )
                self._events.append(event)
                self._record_failure(
                    "request did not complete before drain deadline"
                )
            self._pending_submissions.clear()
            self._events.sort(key=lambda e: e.token)
            self.frozen = RequestStats(
                started=self.started,
                completed=self.completed,
                completed_failures=self.completed_failures,
                timeouts=outstanding,
                outstanding=outstanding,
                latencies_ms=tuple(self.latencies_ms),
                failure_samples=tuple(self.failure_samples),
                events=tuple(self._events),
            )
            self._cond.notify_all()
            return self.frozen

    def wait_until_empty(self, deadline: float) -> None:
        """Block until no pending measured requests remain or *deadline* passes.

        Returns immediately if the window is already frozen or has no pending
        requests. Does not busy-wait.
        """
        with self.lock:
            while self._pending_submissions and not self.frozen:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                self._cond.wait(timeout=remaining)

    def _record_failure(self, sample: str) -> None:
        if len(self.failure_samples) < 3:
            self.failure_samples.append(sample[:500])


class MetricsScraper:
    """Absolute-schedule /metrics scraper for baseline and scheduled targets.

    Sampling points are absolute offsets ``0, interval, ..., duration`` from
    measurement_start, using absolute monotonic deadlines to avoid drift.
    Recorded ``start`` and ``completion`` are offsets (seconds) from
    ``measurement_start``, not absolute monotonic times.

    A missed, overlapping, or failed scrape is represented explicitly.
    If the prior scrape completes after the next absolute slot (no catch-up
    tolerance), every overlapped slot is emitted as ``missed`` instead of
    starting shifted/catch-up scrapes. The scraper is never invoked for a
    slot that has already been determined to be overlapped.

    The final offset is ``duration`` (same as ``deadline - measurement_start``).
    It is scheduled at an absolute deadline and attempted only if it is not
    already overlapped; if it is overlapped it becomes an explicit missed record.
    """

    def __init__(
        self,
        base_url: str,
        timeout: float,
        interval: float,
        duration: float,
        measurement_start: float,
        deadline: float,
        metrics_dir: str | Path | None = None,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout
        self._interval = interval
        self._duration = duration
        self._measurement_start = measurement_start
        self._deadline = deadline
        self._metrics_dir = Path(metrics_dir) if metrics_dir else None
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def run(self) -> list[ScrapeRecord]:
        """Execute the scrape schedule and return records.

        Each scrape has an absolute deadline offset. If the monotonic clock is
        already after a scheduled absolute slot, that slot is emitted as
        ``missed`` and the scraper is NOT invoked there. Equality
        (``now == target_time``) may attempt. The final offset (duration) is scheduled
        at its absolute deadline and attempted only if not already past due; if
        past due it becomes an explicit missed record. No catch-up tolerance is
        applied — every slot is evaluated independently against the monotonic
        clock with strict absolute deadlines."""

        offsets = self._schedule_offsets()
        records: list[ScrapeRecord] = []
        for idx, offset in enumerate(offsets):
            target_time = self._measurement_start + offset
            now = time.monotonic()
            # First-slot policy: offset 0 (baseline scrape at the phase
            # boundary) is attempted immediately even if now is slightly
            # past target_time due to barrier-return / call overhead.
            # A truly delayed scraper start is diagnosed via the actual
            # start recorded inside the ScrapeRecord, but offset 0 itself
            # is never silently skipped. Failure HTTP still invalid.
            if now > target_time and idx != 0:
                start_offset = now - self._measurement_start
                missed_record = ScrapeRecord(
                    offset=offset,
                    start=start_offset,
                    completion=None,
                    status="missed",
                    http_status=None,
                    error="monotonic clock passed scheduled absolute slot",
                    text=None,
                    path=None,
                )
                records.append(missed_record)
                continue
            if now > target_time and idx == 0:
                # Attempt immediately; record the actual start time.
                record = self._scrape_one(offset)
                records.append(record)
                continue
            if target_time > now:
                time.sleep(target_time - now)
            record = self._scrape_one(offset)
            records.append(record)
        return records

    def _schedule_offsets(self) -> list[float]:
        """Compute absolute scrape offsets.

        The final offset is always ``duration``, matching
        ``deadline - measurement_start``.
        """
        num_steps = int(self._duration / self._interval)
        offsets = [i * self._interval for i in range(num_steps + 1)]
        if not offsets or offsets[-1] != self._duration:
            offsets.append(self._duration)
        return offsets

    def _scrape_one(self, offset: float) -> ScrapeRecord:
        """Perform one /metrics scrape at the given *offset*, returning a record
        whose ``start`` and ``completion`` fields are **offsets** (seconds) from
        ``measurement_start``, not absolute ``time.monotonic()`` values."""

        start = time.monotonic() - self._measurement_start
        text: str | None = None
        http_status: int | None = None
        error: str | None = None
        completion: float | None = None
        status = "error"
        try:
            with self._opener.open(
                f"{self._base_url}/metrics", timeout=self._timeout
            ) as response:
                body = response.read()
                text = body.decode(errors="replace")
                http_status = response.status
            completion = time.monotonic() - self._measurement_start
            status = "success"
        except (OSError, urllib.error.URLError) as exc:
            error = str(exc)[:500]
            completion = time.monotonic() - self._measurement_start

        path: str | None = None
        if self._metrics_dir is not None and text is not None:
            scrape_dir = self._metrics_dir / "metrics"
            scrape_dir.mkdir(parents=True, exist_ok=True)
            index = int(offset / self._interval) if self._interval else 0
            prom_path = scrape_dir / f"scrape-{index:03d}.prom"
            prom_path.write_text(text)
            path = str(prom_path)

        return ScrapeRecord(
            offset=offset,
            start=start,
            completion=completion,
            status=status,
            http_status=http_status,
            error=error,
            text=text,
            path=path,
        )


def _bounded_error(body: Any) -> str:
    text = str(body)
    return text[:500]


def percentile(values: list[float], quantile: float) -> float | None:
    if not values:
        return None
    index = min(math.ceil(len(values) * quantile) - 1, len(values) - 1)
    return values[max(index, 0)]


class SqlClient:
    def __init__(self, base_url: str, database: str, timeout: float) -> None:
        self.base_url = base_url.rstrip("/")
        self.database = database
        self.timeout = timeout
        # Validation targets a local standalone process; inherited development
        # proxies can otherwise turn overload into unrelated HTTP 502 errors.
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def sql(self, sql: str, timeout: float | None = None) -> tuple[bool, float, Any]:
        data = urllib.parse.urlencode(
            {"sql": sql, "db": self.database, "format": "json"}
        ).encode()
        request = urllib.request.Request(
            f"{self.base_url}/v1/sql", data=data, method="POST"
        )
        started = time.monotonic()
        try:
            with self.opener.open(
                request, timeout=self.timeout if timeout is None else timeout
            ) as response:
                body = json.loads(response.read().decode())
            ok = response.status < 400 and not response_has_error(body)
            return ok, (time.monotonic() - started) * 1_000, body
        except urllib.error.HTTPError as error:
            body = error.read().decode(errors="replace")
            return (
                False,
                (time.monotonic() - started) * 1_000,
                f"HTTP {error.code}: {body}",
            )
        except (OSError, ValueError, urllib.error.URLError) as error:
            return False, (time.monotonic() - started) * 1_000, str(error)

    def scheduler_polls(self, required: bool = True) -> dict[str, int] | None:
        with self.opener.open(f"{self.base_url}/metrics", timeout=self.timeout) as response:
            text = response.read().decode()
        metrics = parse_scheduler_metrics(text)
        if metrics.polls.keys() != {"query", "write"}:
            if required:
                raise RuntimeError(
                    "scheduler metrics are missing; is "
                    "runtime.experimental_workload_scheduler.enable=true?"
                )
            return None
        return dict(metrics.polls)

    def scrape_metrics(self) -> tuple[str, int | None]:
        """Fetch raw /metrics text and return (text, http_status)."""

        with self.opener.open(f"{self.base_url}/metrics", timeout=self.timeout) as response:
            body = response.read()
            text = body.decode(errors="replace")
            return text, response.status


def response_has_error(body: Any) -> bool:
    if not isinstance(body, dict):
        return False
    if body.get("error") or body.get("err_msg") or body.get("error_msg"):
        return True
    code = str(body.get("code", "")).lower()
    return "output" not in body and code not in ("", "0", "success")


def setup_table(
    client: SqlClient,
    seed_rows: int,
    batch_size: int,
    shards: int = _DEFAULT_SHARDS,
    query_table: str = _DEFAULT_QUERY_TABLE,
    write_table: str = _DEFAULT_WRITE_TABLE,
    query_partitions: int = _DEFAULT_QUERY_PARTITIONS,
    write_partitions: int = _DEFAULT_WRITE_PARTITIONS,
    seed_timestamp_millis: int = _DEFAULT_SEED_TIMESTAMP_MILLIS,
) -> None:
    def create_table(table: str, partitions: int) -> str:
        partition_width = shards // partitions
        partition_predicates = [f"shard < {partition_width}"]
        partition_predicates.extend(
            f"shard >= {lower} AND shard < {lower + partition_width}"
            for lower in range(
                partition_width, shards - partition_width, partition_width
            )
        )
        partition_predicates.append(f"shard >= {shards - partition_width}")
        return (
            f"CREATE TABLE {table} ("
            "host STRING, shard INT, val DOUBLE, ts TIMESTAMP TIME INDEX, "
            "PRIMARY KEY(host, shard)) "
            f"PARTITION ON COLUMNS(shard) ({','.join(partition_predicates)}) "
            "ENGINE=mito"
        )

    statements = [
        f"DROP TABLE IF EXISTS {query_table}",
        f"DROP TABLE IF EXISTS {write_table}",
        create_table(query_table, query_partitions),
        create_table(write_table, write_partitions),
    ]
    for statement in statements:
        ok, _, body = client.sql(statement)
        if not ok:
            raise RuntimeError(f"setup failed for {statement!r}: {body}")

    for offset in range(0, seed_rows, batch_size):
        count = min(batch_size, seed_rows - offset)
        values = ",".join(
            f"('host-{(offset + row) % shards}',{(offset + row) % shards},"
            f"{offset + row},{seed_timestamp_millis + offset + row})"
            for row in range(count)
        )
        ok, _, body = client.sql(
            f"INSERT INTO {query_table} (host,shard,val,ts) VALUES {values}"
        )
        if not ok:
            raise RuntimeError(f"seed insert at row {offset} failed: {body}")


def query_worker(
    client: SqlClient,
    clock: PhaseClock,
    requests: RequestWindow,
    start: threading.Barrier,
    query_sql: str = _DEFAULT_QUERY_SQL,
) -> None:
    start.wait()
    while time.monotonic() < clock.deadline:
        submitted = time.monotonic()
        if submitted >= clock.deadline:
            break
        token = requests.begin(submitted, workload="query")
        ok, latency, body = client.sql(
            query_sql, timeout=clock.request_timeout(submitted, client.timeout)
        )
        if token is not None:
            requests.complete(token, time.monotonic(), ok, latency, body)


def write_worker(
    client: SqlClient,
    clock: PhaseClock,
    start: threading.Barrier,
    sequence: "Sequence",
    batch_size: int,
    delay: float,
    requests: RequestWindow,
    write_table: str = _DEFAULT_WRITE_TABLE,
    shards: int = _DEFAULT_SHARDS,
) -> None:
    start.wait()
    while time.monotonic() < clock.deadline:
        offset = sequence.take(batch_size)
        values = ",".join(
            f"('writer-{(offset + row) % shards}',{(offset + row) % shards},"
            f"{offset + row},{offset + row})"
            for row in range(batch_size)
        )
        sql = f"INSERT INTO {write_table} (host,shard,val,ts) VALUES {values}"
        # Capture submission timestamp after payload construction, before HTTP call
        submitted = time.monotonic()
        if submitted >= clock.deadline:
            break
        token = requests.begin(submitted, workload="write")
        ok, latency, body = client.sql(
            sql,
            timeout=clock.request_timeout(submitted, client.timeout),
        )
        if token is not None:
            requests.complete(token, time.monotonic(), ok, latency, body)
        if delay:
            remaining = clock.deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(delay, remaining))


class Sequence:
    def __init__(self, initial: int) -> None:
        self.value = initial
        self.lock = threading.Lock()

    def take(self, count: int) -> int:
        with self.lock:
            value = self.value
            self.value += count
            return value


def write_requests_jsonl(sample_dir: Path, query_stats: RequestStats, write_stats: RequestStats) -> Path:
    """Write token-sorted request events as JSONL with workload field."""
    all_events: list[RequestEvent] = []
    if query_stats.events:
        all_events.extend(query_stats.events)
    if write_stats.events:
        all_events.extend(write_stats.events)
    all_events.sort(key=lambda e: e.token)

    requests_jsonl = sample_dir / "requests.jsonl"
    with open(requests_jsonl, "w") as f:
        for event in all_events:
            record = {
                "token": event.token,
                "workload": event.workload,
                "submission_offset": event.submission_offset,
                "completion_offset": event.completion_offset,
                "status": event.status,
                "latency_ms": event.latency_ms,
                "error": event.error,
            }
            f.write(json.dumps(record, sort_keys=True) + "\n")
    return requests_jsonl


def write_scrapes_jsonl(sample_dir: Path, scrape_records: list[ScrapeRecord]) -> Path:
    """Write scrapes.jsonl with one record per scrape offset."""
    metrics_dir = sample_dir / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)
    scrapes_jsonl = sample_dir / "scrapes.jsonl"
    with open(scrapes_jsonl, "w") as f:
        for record in scrape_records:
            rec = {
                "offset": record.offset,
                "start": record.start,
                "completion": record.completion,
                "status": record.status,
                "http_status": record.http_status,
                "error": record.error,
                "text": record.text,
                "path": record.path,
            }
            f.write(json.dumps(rec, sort_keys=True) + "\n")
    return scrapes_jsonl


def write_sample_json(sample_dir: Path, result: dict[str, Any]) -> Path:
    """Write the complete phase result as sample.json."""
    sample_json = sample_dir / "sample.json"
    sample_json.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")
    return sample_json


def run_phase(
    client: SqlClient,
    name: str,
    duration: float,
    warmup: float,
    query_workers: int,
    write_workers: int,
    write_batch_size: int,
    write_delay: float,
    sequence: Sequence,
    scheduler_metrics: str,
    drain_timeout: float = 5.0,
    scrape_interval: float | None = None,
    metrics_dir: str | Path | None = None,
    query_sql: str = _DEFAULT_QUERY_SQL,
    write_table: str = _DEFAULT_WRITE_TABLE,
    shards: int = _DEFAULT_SHARDS,
) -> dict[str, Any]:
    worker_count = query_workers + write_workers
    clock = PhaseClock(warmup, duration, drain_timeout)
    start = threading.Barrier(worker_count + 1, action=clock.start)
    query_requests = RequestWindow(clock)
    write_requests = RequestWindow(clock)
    all_windows = [query_requests, write_requests]

    with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as executor:
        query_futures = [
            executor.submit(query_worker, client, clock, query_requests, start, query_sql)
            for _ in range(query_workers)
        ]
        write_futures = [
            executor.submit(
                write_worker,
                client,
                clock,
                start,
                sequence,
                write_batch_size,
                write_delay,
                write_requests,
                write_table,
                shards,
            )
            for _ in range(write_workers)
        ]
        start.wait()

        # Offset zero scheduling: instantiate scraper BEFORE measurement_start
        # and let it sleep to absolute offset 0. Remove the pre-sleep to
        # measurement_start in the scraper path.
        scrape_records: list[ScrapeRecord] = []
        scheduler_snapshots: list[SchedulerMetrics] = []
        if scrape_interval is not None and scrape_interval > 0:
            # Create scraper NOW, before measurement_start, so the offset-0
            # scrape sleeps to measurement_start replacing the generic pre-sleep.
            scraper = MetricsScraper(
                base_url=client.base_url,
                timeout=client.timeout,
                interval=scrape_interval,
                duration=duration,
                measurement_start=clock.measurement_start,
                deadline=clock.deadline,
                metrics_dir=metrics_dir,
            )
            scrape_records = scraper.run()
            # Parse each successful scrape into a SchedulerMetrics snapshot
            for record in scrape_records:
                if record.text is not None:
                    scheduler_snapshots.append(
                        parse_scheduler_metrics(record.text)
                    )
        else:
            # Legacy pre-sleep to measurement_start (no scraper)
            remaining = clock.measurement_start - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)

            # Legacy scrape at boundaries for backward compat
            before = (
                client.scheduler_polls(required=scheduler_metrics == "required")
                if scheduler_metrics != "disabled"
                else None
            )
            remaining = clock.deadline - time.monotonic()
            if remaining > 0:
                time.sleep(remaining)
            at_deadline = (
                client.scheduler_polls(required=True)
                if before is not None
                else None
            )

        # Efficient drain: wait until pending requests complete or drain_deadline
        for window in all_windows:
            window.wait_until_empty(clock.drain_deadline)

        query_stats = query_requests.freeze()
        write_stats = write_requests.freeze()

        # Join all workers
        for future in query_futures:
            future.result()
        for future in write_futures:
            future.result()

    # Build backward-compat polls/poll_share from scrape records
    if scrape_records and scheduler_snapshots:
        summary = summarize_scheduler_metrics(scheduler_snapshots)
        poll_delta = dict(summary.whole_window_polls) if summary.whole_window_polls else None
        total_polls = sum(poll_delta.values()) if poll_delta else 0
        shares = (
            {
                workload: (polls / total_polls if total_polls else 0.0)
                for workload, polls in poll_delta.items()
            }
            if poll_delta
            else None
        )
    else:
        poll_delta = (
            {
                workload: at_deadline[workload] - before[workload]
                for workload in before
            }
            if "before" in locals() and before is not None
            and "at_deadline" in locals() and at_deadline is not None
            else None
        )
        total_polls = sum(poll_delta.values()) if poll_delta is not None else 0
        shares = (
            {
                workload: (polls / total_polls if total_polls else 0.0)
                for workload, polls in poll_delta.items()
            }
            if poll_delta is not None
            else None
        )
        summary = None

    result: dict[str, Any] = {
        "name": name,
        "duration_s": duration,
        "warmup_s": warmup,
        "drain_timeout_s": drain_timeout,
        "workers": {"query": query_workers, "write": write_workers},
        "requests": {
            "query": query_stats.summary(duration),
            "write": write_stats.summary(duration),
        },
        "polls": poll_delta,
        "poll_share": shares,
        "scrape_records": [
            dataclasses.asdict(r) if isinstance(r, ScrapeRecord) else r
            for r in scrape_records
        ],
        "scheduler_snapshots": [
            dataclasses.asdict(s) if isinstance(s, SchedulerMetrics) else s
            for s in (scheduler_snapshots if scrape_records else [])
        ],
    }
    if summary is not None:
        result["scheduler_summary"] = dataclasses.asdict(summary)

    # Write durable artifacts atomically to sample_dir
    if metrics_dir is not None:
        sample_dir = Path(metrics_dir)
        write_requests_jsonl(sample_dir, query_stats, write_stats)
        write_scrapes_jsonl(sample_dir, scrape_records)
        write_sample_json(sample_dir, result)

    return result


def verify(phases: list[dict[str, Any]]) -> None:
    """Legacy verification — deprecated.

    This function previously asserted whole-window >= 80% poll shares. It is
    retained only to reject accidental use with a clear message. Use
    evaluate_scheduler_report() from workload_scheduler_report.py instead.
    """
    raise NotImplementedError(
        "Legacy verify() is removed. Use evaluate_scheduler_report() "
        "from workload_scheduler_report.py for scheduler gate verification."
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://127.0.0.1:4000")
    parser.add_argument("--database", default="public")
    parser.add_argument("--duration", type=float, default=10.0)
    parser.add_argument("--warmup", type=float, default=2.0)
    parser.add_argument("--drain-timeout", type=float, default=5.0)
    parser.add_argument("--query-workers", type=int, default=2)
    parser.add_argument("--write-workers", type=int, default=1152)
    parser.add_argument("--seed-rows", type=int, default=10_000)
    parser.add_argument("--seed-batch-size", type=int, default=500)
    parser.add_argument("--write-batch-size", type=int, default=32)
    parser.add_argument("--light-write-delay", type=float, default=0.1)
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument(
        "--phase",
        choices=("all", "query_only", "write_only", "light_write", "saturated"),
        default="all",
    )
    parser.add_argument(
        "--scheduler-metrics",
        choices=("required", "optional", "disabled"),
        default="required",
        help="whether scheduler Prometheus metrics must be collected",
    )
    parser.add_argument("--skip-setup", action="store_true")
    parser.add_argument("--no-verify", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.drain_timeout <= 0:
        raise ValueError("--drain-timeout must be greater than zero")
    client = SqlClient(args.url, args.database, args.timeout)
    if not args.skip_setup:
        setup_table(client, args.seed_rows, args.seed_batch_size)

    sequence = Sequence(_DEFAULT_WRITE_SEQUENCE_START_MILLIS)
    phase_options = {
        "query_only": (args.query_workers, 0, 0),
        "write_only": (0, args.write_workers, 0),
        "light_write": (args.query_workers, 1, args.light_write_delay),
        "saturated": (args.query_workers, args.write_workers, 0),
    }
    selected = (
        {
            name: phase_options[name]
            for name in ("query_only", "write_only", "light_write", "saturated")
        }
        if args.phase == "all"
        else {args.phase: phase_options[args.phase]}
    )
    phases = []
    for name, (query_workers, write_workers, write_delay) in selected.items():
        phases.append(
            run_phase(
                client,
                name,
                args.duration,
                args.warmup,
                query_workers,
                write_workers,
                args.write_batch_size,
                write_delay,
                sequence,
                args.scheduler_metrics,
                args.drain_timeout,
            )
        )
    if not args.no_verify:
        if args.phase != "all":
            raise ValueError("--phase requires --no-verify unless all phases are selected")
        if args.scheduler_metrics != "required":
            raise ValueError("verification requires --scheduler-metrics=required")
        verify(phases)

    result = {
        "verified": not args.no_verify,
        "mean_write_share_saturated": statistics.fmean(
            [
                phase["poll_share"]["write"]
                for phase in phases
                if phase["name"] == "saturated" and phase["poll_share"] is not None
            ]
        )
        if any(
            phase["name"] == "saturated" and phase["poll_share"] is not None
            for phase in phases
        )
        else None,
        "phases": phases,
    }
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
