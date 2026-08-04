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

"""Pure parsing and verification for workload-scheduler benchmark reports.

Every public function accepts plain dictionaries so callers can test with
synthetic data without file I/O.
"""

from __future__ import annotations

import dataclasses
import json
import math
import re
import statistics
from collections.abc import Iterable, Mapping, Sequence
from pathlib import Path


QUERY_WORKLOAD = "query"
WRITE_WORKLOAD = "write"
WORKLOADS = (QUERY_WORKLOAD, WRITE_WORKLOAD)
POLLS_METRIC = "greptime_workload_scheduler_polls"
QUEUED_METRIC = "greptime_workload_scheduler_queued_tasks"
ACTIVE_METRIC = "greptime_workload_scheduler_active_polls"
STATUS_EXIT_CODES = {"passed": 0, "failed": 1, "invalid": 2, "error": 3}

# Maximum acceptable lateness (seconds) for the offset-0 scrape attempt.
# Offset 0 is always attempted despite tiny barrier-return overhead, but
# the canonical loader rejects evidence where the actual start offset exceeds
# this bound. This is an execution-only integrity constant, not a policy or
# mechanism threshold.
MAX_INITIAL_SCRAPE_LATENESS = 0.25

# Maximum acceptable lateness (seconds) for scrape start relative to the
# planned offset for offsets > 0. The runner's strict no-catch-up semantics
# marks any slot where ``now > target_time`` as missed before invoking, so a
# successful start at offset > 0 should be at or very near the target subject
# only to OS-scheduling / call-measurement delay.  A bounded 250 ms integrity
# limit is acceptable for this purpose and prevents interval-scale drift.
MAX_SCRAPE_START_LATENESS = 0.25

# Maximum acceptable wall-clock duration (seconds) for a single /metrics
# scrape attempt.  A localhost ``/metrics`` endpoint should complete well
# within 250 ms; this bound ensures that a single scrape cannot materially
# extend into the next scheduled slot or into drain.  Both the runner's HTTP
# timeout for scrapes and the canonical timing validator enforce this bound.
MAX_SCRAPE_DURATION = 0.25

# Tolerance (milliseconds) between the client-reported latency_ms and the
# difference (completion_offset - submission_offset) * 1000. Both offsets
# and latency originate from the same monotonic clock in the runner, so they
# should match within sub-ms; the tolerance accommodates JSON serialization
# rounding and measurement-boundary jitter without overrejecting real evidence.
LATENCY_OFFSET_TOLERANCE_MS = 1.0
SAMPLE_RE = re.compile(
    r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*)\})?\s+(\S+)(?:\s+\d+)?$"
)
LABEL_RE = re.compile(r'\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"((?:\\.|[^"\\])*)"\s*(?:,|$)')


# ---------------------------------------------------------------------------
# Prometheus metric parsing
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class SchedulerMetrics:
    """One scheduler Prometheus scrape."""

    polls: Mapping[str, int]
    queued: Mapping[str, int]
    active: int | None

    def complete(self) -> bool:
        """Return whether all metrics required for scheduler verification exist."""

        return (
            all(
                workload in self.polls and workload in self.queued
                for workload in WORKLOADS
            )
            and self.active is not None
        )


@dataclasses.dataclass(frozen=True)
class SchedulerInterval:
    """The scheduler metrics change between two adjacent scrapes."""

    polls: Mapping[str, int] | None
    counter_reset: bool
    strict_dual_backlog: bool
    active_max: int | None


@dataclasses.dataclass(frozen=True)
class SchedulerSummary:
    """Whole-window and strict dual-backlog scheduler observations."""

    whole_window_polls: Mapping[str, int]
    whole_window_write_share: float | None
    dual_backlog_polls: Mapping[str, int]
    dual_backlog_write_share: float | None
    interval_count: int
    dual_backlog_interval_count: int
    dual_backlog_interval_fraction: float | None
    counter_reset_interval_count: int
    max_active_polls: int | None
    metrics_complete: bool


@dataclasses.dataclass(frozen=True)
class SchedulerGateThresholds:
    """Thresholds for pure scheduler mechanism verification.

    All fields map directly from Rust PerformanceGate / case.toml keys.
    """

    required_phases: tuple[str, ...] = (
        "query_only",
        "write_only",
        "light_write",
        "saturated",
    )
    write_share_min: float = 0.78
    write_share_max: float = 0.82
    min_dual_backlog_interval_fraction: float = 0.80
    min_dual_backlog_polls_per_class: int = 100
    single_class_purity_min_share: float = 0.99
    max_active_polls: int = 16
    # New fields mapped from Rust PerformanceGate
    max_failure_rate: float = 0.01
    max_outstanding_requests: int = 0
    min_light_write_query_share: float = 0.20
    active_within_scheduler_limit: bool = True
    max_capacity_normalized_regression_pct: float = 5.0


@dataclasses.dataclass(frozen=True)
class SchedulerEvaluation:
    """The status, exit code, and failed gates of scheduler verification."""

    status: str
    passed: bool
    exit_code: int
    failures: tuple[str, ...]


# ---------------------------------------------------------------------------
# Request validity helpers
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class RequestEvaluation:
    """Result of checking one workload's request stream."""

    status: str  # "passed", "failed", "invalid"
    failures: tuple[str, ...]


# ---------------------------------------------------------------------------
# Performance helpers
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class PerformanceEvaluation:
    """Per-iteration paired performance check."""

    phase: str
    iteration: int
    baseline_normalized: float
    scheduled_normalized: float
    pct_change: float | None
    passed: bool
    details: str | None


# ---------------------------------------------------------------------------
# Artifact validation
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class ArtifactValidation:
    """Result of validating persisted artifacts for one sample."""

    status: str  # "passed", "invalid"
    failures: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class PersistedSample:
    """Canonical reconstructed sample loaded solely from persisted files.

    Every field is derived from disk — never from in-memory sample dicts.
    ``validation`` carries the combined validation status; only when its
    status is ``"passed"`` are the remaining fields guaranteed consistent.
    """

    validation: ArtifactValidation
    metadata: dict[str, object]  # canonical metadata from sample.json
    request_events: list[dict[str, object]]  # parsed from requests.jsonl
    request_summaries: dict[str, dict[str, object]]  # per-workload summaries
    scrape_records: list[dict[str, object]]  # parsed from scrapes.jsonl
    scheduler_snapshots: list[dict[str, object]]  # parsed from .prom files
    artifact_dir: str


def _reconstruct_request_summaries(
    events: list[dict[str, object]],
) -> dict[str, dict[str, object]]:
    """Reconstruct per-workload summary counts from parsed request events.

    Uses (workload, token) identity, unique; validates terminal status and fields.
    Returns per-workload dicts with: started, requests (=started), completed,
    completed_failures, timeouts, outstanding, failures, plus successful-only
    latency diagnostics: mean_ms, p50_ms, p95_ms (computed from event latency_ms).
    """
    summaries: dict[str, dict[str, object]] = {}
    for wl in WORKLOADS:
        summaries[wl] = {
            "started": 0,
            "requests": 0,
            "completed": 0,
            "completed_failures": 0,
            "timeouts": 0,
            "outstanding": 0,
            "failures": 0,
        }

    successful_latencies: dict[str, list[float]] = {wl: [] for wl in WORKLOADS}

    for ev in events:
        wl = str(ev.get("workload", ""))
        if wl not in WORKLOADS:
            continue
        s = summaries[wl]
        s["started"] = int(s["started"]) + 1
        s["requests"] = int(s["requests"]) + 1
        status = str(ev.get("status", ""))
        if status == "success":
            s["completed"] = int(s["completed"]) + 1
            latency = ev.get("latency_ms")
            if isinstance(latency, (int, float)) and math.isfinite(latency) and latency >= 0:
                successful_latencies[wl].append(float(latency))
        elif status == "failure":
            s["completed"] = int(s["completed"]) + 1
            s["completed_failures"] = int(s["completed_failures"]) + 1
            s["failures"] = int(s["failures"]) + 1
        elif status == "timeout":
            s["timeouts"] = int(s["timeouts"]) + 1
            s["failures"] = int(s["failures"]) + 1
            s["outstanding"] = int(s["outstanding"]) + 1

    for wl in WORKLOADS:
        latencies = sorted(successful_latencies[wl])
        if latencies:
            summaries[wl]["mean_ms"] = statistics.fmean(latencies)
            summaries[wl]["p50_ms"] = percentile(latencies, 0.50)
            summaries[wl]["p95_ms"] = percentile(latencies, 0.95)
        else:
            summaries[wl]["mean_ms"] = None
            summaries[wl]["p50_ms"] = None
            summaries[wl]["p95_ms"] = None

    return summaries


def _check_submission_bound(
    sample_data: dict[str, object],
    sub_offset: float,
    ev: dict[str, object],
    failures: list[str],
) -> None:
    """Validate submission_offset is strictly within the measurement window [0, duration_s)."""
    duration_s = sample_data.get("duration_s")
    if isinstance(duration_s, (int, float)) and math.isfinite(duration_s) and duration_s > 0:
        if sub_offset >= duration_s:
            failures.append(
                f"requests.jsonl event token={ev.get('token')}: "
                f"submission_offset ({sub_offset}) >= duration_s ({duration_s})"
            )


def _check_completion_bound(
    sample_data: dict[str, object],
    completion_offset: float,
    ev: dict[str, object],
    failures: list[str],
) -> None:
    """Validate completion_offset is within [0, duration_s + drain_timeout_s).

    The half-open drain cutoff ensures completed events complete strictly
    before the drain deadline, consistent with timeout semantics (>= deadline).
    """
    duration_s = sample_data.get("duration_s")
    drain_timeout_s = sample_data.get("drain_timeout_s")
    if isinstance(duration_s, (int, float)) and math.isfinite(duration_s) and duration_s > 0:
        bound = duration_s
        if isinstance(drain_timeout_s, (int, float)) and math.isfinite(drain_timeout_s):
            bound = duration_s + drain_timeout_s
        if completion_offset >= bound:
            failures.append(
                f"requests.jsonl event token={ev.get('token')}: "
                f"completion_offset ({completion_offset}) >= "
                f"duration_s + drain_timeout_s ({bound})"
            )


def _check_latency_consistency(
    sub_offset: object,
    completion_offset: object,
    latency_ms: float,
    ev: dict[str, object],
    failures: list[str],
) -> None:
    """Cross-check latency_ms against completion-submission offset difference.

    Both offsets and latency originate from the same monotonic clock in the
    runner, so the client-reported latency should closely match the wall-clock
    difference. Uses an explicit small tolerance (LATENCY_OFFSET_TOLERANCE_MS)
    to accommodate JSON serialisation rounding without overrejecting real evidence.
    """
    if not (isinstance(sub_offset, (int, float)) and math.isfinite(sub_offset)
            and isinstance(completion_offset, (int, float)) and math.isfinite(completion_offset)
            and isinstance(latency_ms, (int, float)) and math.isfinite(latency_ms)):
        return  # One of the required values is unusable; basic validation already flagged.
    wall_diff_ms = (completion_offset - sub_offset) * 1000.0
    discrepancy_ms = abs(latency_ms - wall_diff_ms)
    if discrepancy_ms > LATENCY_OFFSET_TOLERANCE_MS:
        failures.append(
            f"requests.jsonl event token={ev.get('token')}: "
            f"latency_ms ({latency_ms}) differs from completion-submission "
            f"delta ({wall_diff_ms:.3f})ms by {discrepancy_ms:.3f}ms "
            f"(tolerance {LATENCY_OFFSET_TOLERANCE_MS}ms)"
        )


def _validate_scrape_timing(
    scrapes: list[dict[str, object]],
    sample_data: dict[str, object],
    failures: list[str],
) -> None:
    """Validate scrape record start/completion offset timing.

    Every persisted scrape record requires finite numeric ``start`` and (for
    success/error status) ``completion`` fields with ``completion >= start``.
    There is no legacy omission path — records without these fields are
    rejected.

    For ``success`` status:
    - The recorded ``start`` must not precede the planned ``offset`` by more
      than a tiny floating-clock precision tolerance (0.001 s).
    - Offset 0 (first scrape) must not exceed MAX_INITIAL_SCRAPE_LATENESS.
    - Offsets > 0 must not exceed ``offset + MAX_SCRAPE_START_LATENESS``.

    For ``missed`` / ``error`` status: finite ``start`` is always required.
    ``completion`` is required for ``error`` (the writer always sets it) and
    permitted to be ``None`` for ``missed`` (per writer format).
    """
    for rec in scrapes:
        offset = rec.get("offset")
        start = rec.get("start")
        completion = rec.get("completion")
        status = rec.get("status", "")

        # ---- Every record requires finite numeric start ----
        if start is None:
            failures.append(
                f"scrape offset {offset}: missing start"
            )
            continue
        if not (isinstance(start, (int, float)) and math.isfinite(start) and start >= 0):
            failures.append(
                f"scrape offset {offset}: start is non-finite or negative: {start}"
            )
            continue

        # ---- completion validation by status ----
        if completion is not None:
            if not (isinstance(completion, (int, float)) and math.isfinite(completion) and completion >= 0):
                failures.append(
                    f"scrape offset {offset}: completion is non-finite or negative: {completion}"
                )
                continue
            if completion < start:
                failures.append(
                    f"scrape offset {offset}: completion ({completion}) < start ({start})"
                )
                continue
        elif status in ("success", "error"):
            # Writer always sets completion for success and error
            failures.append(
                f"scrape offset {offset}: {status} record missing completion"
            )
            continue

        # ---- For success status, validate start against planned offset ----
        if status == "success":
            if isinstance(offset, (int, float)) and math.isfinite(offset):
                # Start must not precede planned offset beyond tiny clock precision
                if start < offset - 0.001:
                    failures.append(
                        f"scrape offset {offset}: start ({start}) precedes planned offset "
                        f"({offset}) by more than clock precision"
                    )

                # Offset 0 uses MAX_INITIAL_SCRAPE_LATENESS
                if offset == 0.0:
                    if start > MAX_INITIAL_SCRAPE_LATENESS:
                        failures.append(
                            f"scrape offset 0: start ({start}) exceeds "
                            f"MAX_INITIAL_SCRAPE_LATENESS ({MAX_INITIAL_SCRAPE_LATENESS})"
                        )
                else:
                    # Offsets > 0 use MAX_SCRAPE_START_LATENESS
                    if start > offset + MAX_SCRAPE_START_LATENESS:
                        failures.append(
                            f"scrape offset {offset}: start ({start}) exceeds "
                            f"offset ({offset}) + MAX_SCRAPE_START_LATENESS "
                            f"({MAX_SCRAPE_START_LATENESS})"
                        )

                # ---- Wall-clock duration must not exceed MAX_SCRAPE_DURATION ----
                if completion is not None:
                    duration = completion - start
                    if duration > MAX_SCRAPE_DURATION:
                        failures.append(
                            f"scrape offset {offset}: duration ({duration:.3f}s) exceeds "
                            f"MAX_SCRAPE_DURATION ({MAX_SCRAPE_DURATION})"
                        )

                    # Final offset completion must not materially extend beyond
                    # the planned offset + MAX_SCRAPE_DURATION + start lateness.
                    # This prevents the final scrape from extending into drain.
                    if isinstance(offset, (int, float)) and math.isfinite(offset):
                        max_acceptable_completion = offset + MAX_SCRAPE_DURATION + MAX_SCRAPE_START_LATENESS
                        if completion > max_acceptable_completion:
                            failures.append(
                                f"scrape offset {offset}: completion ({completion:.3f}s) "
                                f"exceeds offset + MAX_SCRAPE_DURATION + "
                                f"MAX_SCRAPE_START_LATENESS ({max_acceptable_completion:.3f}s)"
                            )


def _validate_timing_metadata(
    sample_data: dict[str, object],
    expected_metadata: dict[str, object] | None,
    failures: list[str],
) -> None:
    """Cross-check persisted timing metadata against plan values.

    duration_s, warmup_s, drain_timeout_s must be finite and exactly match
    the expected plan (normal float tolerance for representation).
    """
    timing_keys = ("duration_s", "warmup_s", "drain_timeout_s")
    if not expected_metadata:
        # Check that existing timing fields are at least well-formed
        for key in timing_keys:
            val = sample_data.get(key)
            if val is not None:
                if not (isinstance(val, (int, float)) and math.isfinite(val)):
                    failures.append(f"sample.json {key} is non-finite: {val}")
                elif isinstance(val, float) and val != val:
                    failures.append(f"sample.json {key} is NaN")
        return

    for key in timing_keys:
        expected = expected_metadata.get(key)
        if expected is None:
            continue
        if not (isinstance(expected, (int, float)) and math.isfinite(expected)):
            failures.append(
                f"expected_metadata {key} is non-finite: {expected}"
            )
            continue
        actual = sample_data.get(key)
        if actual is None:
            failures.append(f"sample.json missing {key} (expected {expected})")
            continue
        if not (isinstance(actual, (int, float)) and math.isfinite(actual)):
            failures.append(
                f"sample.json {key} is non-finite: {actual} (expected {expected})"
            )
            continue
        # Exact match with normal float tolerance for representation
        if abs(float(actual) - float(expected)) > 1e-9:
            failures.append(
                f"sample.json {key}: expected {expected!r}, got {actual!r}"
            )


def load_persisted_sample(
    sample_dir: str | Path,
    scenario_name: str | None = None,
    expected_metadata: dict[str, object] | None = None,
) -> PersistedSample:
    """Canonical single source of truth for one sample.

    Reads sample.json, requests.jsonl, scrapes.jsonl, and every scrape
    record's exact relative .prom path from *sample_dir*.  Rejects JSON
    parse errors, duplicate/missing request identities ``(workload, token)``,
    malformed fields, path escapes, missing/extra raw files, record/raw
    count mismatch, wrong metadata (iteration/phase/target/mode/artifact_dir),
    and cross-summary/count mismatch.

    Returns a ``PersistedSample`` whose ``.validation`` carries the combined
    artifact-validation status.  Only when ``.validation.status == "passed"``
    are the canonical reconstructed fields guaranteed consistent.  This
    function is the single source of truth — callers MUST NOT rely on
    in-memory ``scheduler_snapshots`` or original request summaries after
    this function returns.
    """
    failures: list[str] = []
    sample_dir = Path(sample_dir)
    canonical_metadata: dict[str, object] = {}
    canonical_events: list[dict[str, object]] = []
    canonical_scrapes: list[dict[str, object]] = []
    canonical_snapshots: list[dict[str, object]] = []
    canonical_request_summaries: dict[str, dict[str, object]] = {}

    # ------------------------------------------------------------------
    # 1. sample.json — metadata + request/scrape/snapshot summaries
    # ------------------------------------------------------------------
    sj_path = sample_dir / "sample.json"
    if not sj_path.exists():
        return PersistedSample(
            validation=ArtifactValidation(status="invalid", failures=(f"sample.json not found in {sample_dir}",)),
            metadata={},
            request_events=[],
            request_summaries={},
            scrape_records=[],
            scheduler_snapshots=[],
            artifact_dir=str(sample_dir),
        )

    try:
        sample_data: dict[str, object] = json.loads(sj_path.read_text())
    except (json.JSONDecodeError, OSError) as e:
        return PersistedSample(
            validation=ArtifactValidation(status="invalid", failures=(f"sample.json parse error: {e}",)),
            metadata={},
            request_events=[],
            request_summaries={},
            scrape_records=[],
            scheduler_snapshots=[],
            artifact_dir=str(sample_dir),
        )

    # Build canonical metadata from sample.json
    for key in ("mode", "iteration", "phase", "target", "artifact_dir", "name",
                "sample", "phase", "target_name", "scheduler_enabled", "workers",
                "duration_s", "warmup_s", "drain_timeout_s"):
        val = sample_data.get(key)
        if val is not None:
            canonical_metadata[key] = val

    # Validate required metadata keys
    meta_keys = ("mode", "iteration", "phase", "target", "artifact_dir")
    for key in meta_keys:
        val = sample_data.get(key)
        if val is None:
            failures.append(f"sample.json missing {key}")

    if expected_metadata:
        for key, expected in expected_metadata.items():
            actual = sample_data.get(key)
            # Normal float tolerance for representation (not policy defaults).
            if isinstance(expected, float) and isinstance(actual, (int, float)):
                if abs(float(actual) - expected) > 1e-9:
                    failures.append(
                        f"sample.json {key}: expected {expected!r}, got {actual!r}"
                    )
            else:
                if actual != expected:
                    failures.append(
                        f"sample.json {key}: expected {expected!r}, got {actual!r}"
                    )

    # ---- Timing metadata validation ----
    _validate_timing_metadata(sample_data, expected_metadata, failures)

    # artifact_dir must match sample_dir
    ad = sample_data.get("artifact_dir", "")
    if str(Path(str(ad)).resolve()) != str(sample_dir.resolve()):
        failures.append(f"sample.json artifact_dir mismatch: {ad} vs {sample_dir}")

    # ------------------------------------------------------------------
    # 2. requests.jsonl — validate identities, events, counts
    # ------------------------------------------------------------------
    rj_path = sample_dir / "requests.jsonl"
    if not rj_path.exists():
        failures.append(f"requests.jsonl not found in {sample_dir}")
    else:
        try:
            raw_events: list[dict[str, object]] = []
            for line in rj_path.read_text().strip().split("\n"):
                line = line.strip()
                if line:
                    try:
                        raw_events.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        failures.append(f"requests.jsonl parse error: {e}")
                        continue

            # Check for duplicate (workload, token) pairs
            seen_ids: set[tuple[str, int]] = set()
            for ev in raw_events:
                wl = str(ev.get("workload", ""))
                tok = ev.get("token")
                if not isinstance(tok, int) and not (isinstance(tok, float) and tok == int(tok)):
                    failures.append(f"requests.jsonl event has non-integer token: {tok}")
                    continue
                tok = int(tok)  # type: ignore[arg-type]
                pair = (wl, tok)
                if pair in seen_ids:
                    failures.append(f"requests.jsonl duplicate (workload={wl!r}, token={tok})")
                seen_ids.add(pair)

            # Validate token order is sorted
            tokens = [int(e.get("token", -1)) for e in raw_events if isinstance(e.get("token"), (int, float))]
            if tokens != sorted(tokens):
                failures.append("requests.jsonl tokens not in sorted order")

            # Validate event statuses and workload fields
            for ev in raw_events:
                status = ev.get("status", "")
                if status not in ("success", "failure", "timeout"):
                    failures.append(f"requests.jsonl event has invalid status: {status!r}")

                wl = ev.get("workload", "")
                if wl not in WORKLOADS:
                    failures.append(f"requests.jsonl event has invalid workload: {wl!r}")

                # Validate submission_offset: must be finite nonnegative
                # and within [0, duration_s) measurement window.
                sub_offset = ev.get("submission_offset")
                if sub_offset is not None:
                    if not (isinstance(sub_offset, (int, float)) and math.isfinite(sub_offset) and sub_offset >= 0):
                        failures.append(
                            f"requests.jsonl event token={ev.get('token')}: "
                            f"submission_offset is non-finite or negative: {sub_offset}"
                        )
                    else:
                        _check_submission_bound(sample_data, sub_offset, ev, failures)
                else:
                    failures.append(
                        f"requests.jsonl event token={ev.get('token')}: "
                        f"missing submission_offset"
                    )

                # Validate completion_offset and latency_ms based on status.
                # Timing boundaries use duration_s and drain_timeout_s.
                completion_offset = ev.get("completion_offset")
                latency_ms = ev.get("latency_ms")
                if status in ("success", "failure"):
                    if completion_offset is None:
                        failures.append(
                            f"requests.jsonl event token={ev.get('token')}: "
                            f"{status} event missing completion_offset"
                        )
                    elif not (isinstance(completion_offset, (int, float)) and
                              math.isfinite(completion_offset) and completion_offset >= 0):
                        failures.append(
                            f"requests.jsonl event token={ev.get('token')}: "
                            f"{status} event completion_offset is non-finite or negative: "
                            f"{completion_offset}"
                        )
                    else:
                        # completion >= submission
                        if isinstance(sub_offset, (int, float)) and math.isfinite(sub_offset):
                            if completion_offset < sub_offset:
                                failures.append(
                                    f"requests.jsonl event token={ev.get('token')}: "
                                    f"completion_offset ({completion_offset}) < "
                                    f"submission_offset ({sub_offset})"
                                )
                        # completion < duration_s + drain_timeout_s (half-open drain cutoff)
                        _check_completion_bound(sample_data, completion_offset, ev, failures)
                    if latency_ms is None:
                        failures.append(
                            f"requests.jsonl event token={ev.get('token')}: "
                            f"{status} event missing latency_ms"
                        )
                    elif not (isinstance(latency_ms, (int, float)) and
                              math.isfinite(latency_ms) and latency_ms >= 0):
                        failures.append(
                            f"requests.jsonl event token={ev.get('token')}: "
                            f"{status} event latency_ms is non-finite or negative: {latency_ms}"
                        )
                    else:
                        # Cross-check latency_ms against offset difference
                        _check_latency_consistency(sub_offset, completion_offset, latency_ms, ev, failures)
                elif status == "timeout":
                    if completion_offset is not None:
                        failures.append(
                            f"requests.jsonl event token={ev.get('token')}: "
                            f"timeout event has non-None completion_offset: {completion_offset}"
                        )
                    if latency_ms is not None:
                        failures.append(
                            f"requests.jsonl event token={ev.get('token')}: "
                            f"timeout event has non-None latency_ms: {latency_ms}"
                        )

            canonical_events = raw_events

        except OSError as e:
            failures.append(f"requests.jsonl read error: {e}")

    # Reconstruct per-workload summaries from events and cross-check against
    # persisted sample.json.requests if available.
    canonical_request_summaries = _reconstruct_request_summaries(canonical_events)
    persisted_reqs = sample_data.get("requests", {})
    if isinstance(persisted_reqs, dict) and not failures:
        for wl in WORKLOADS:
            pr = persisted_reqs.get(wl, {})
            if not isinstance(pr, dict) or not pr:
                continue
            cr = canonical_request_summaries.get(wl, {})
            # Cross-check counts
            for field in ("started", "requests", "completed", "completed_failures", "timeouts", "outstanding", "failures"):
                persisted_val = pr.get(field)
                if persisted_val is not None and isinstance(persisted_val, (int, float)):
                    if not (isinstance(persisted_val, (int, float)) and
                            persisted_val >= 0 and
                            (not isinstance(persisted_val, float) or persisted_val.is_integer())):
                        failures.append(
                            f"sample.json {wl}.{field} is non-integer or negative"
                        )
                        continue
                    persisted_int = int(persisted_val)
                    canonical_int = int(cr.get(field, 0))
                    if persisted_int != canonical_int:
                        failures.append(
                            f"sample.json {wl}.{field} ({persisted_int}) != "
                            f"events reconstructed ({canonical_int})"
                        )

            # Preserve/cross-check exact runner diagnostics:
            # successful_rps, mean_ms, p50_ms, p95_ms
            # Recompute successful_rps from events when duration_s is available.
            duration_s = sample_data.get("duration_s", None)
            if isinstance(duration_s, (int, float)) and math.isfinite(duration_s) and duration_s > 0:
                for wl in WORKLOADS:
                    cr = canonical_request_summaries.get(wl, {})
                    completed_count = int(cr.get("completed", 0))
                    completed_failures_count = int(cr.get("completed_failures", 0))
                    successful_count = completed_count - completed_failures_count
                    reconstructed_rps = successful_count / duration_s
                    pr = persisted_reqs.get(wl, {})
                    if isinstance(pr, dict):
                        persisted_rps = pr.get("successful_rps")
                        if persisted_rps is not None and isinstance(persisted_rps, (int, float)):
                            if not (math.isfinite(persisted_rps) and persisted_rps >= 0):
                                failures.append(
                                    f"sample.json {wl}.successful_rps is non-finite or negative: {persisted_rps}"
                                )
                            else:
                                # Cross-check: must be within 0.1% tolerance
                                if reconstructed_rps > 0:
                                    ratio = abs(float(persisted_rps) - reconstructed_rps) / reconstructed_rps
                                    if ratio > 0.001:
                                        failures.append(
                                            f"sample.json {wl}.successful_rps ({persisted_rps}) differs "
                                            f"from events-reconstructed ({reconstructed_rps:.4f}) by {ratio*100:.4f}%"
                                        )
                                elif float(persisted_rps) != 0.0:
                                    failures.append(
                                        f"sample.json {wl}.successful_rps ({persisted_rps}) != 0 "
                                        f"with zero successful events"
                                    )
                        # Set canonical successful_rps
                        cr["successful_rps"] = reconstructed_rps

            # Cross-check latency diagnostics: mean_ms, p50_ms, p95_ms
            for diag_field in ("mean_ms", "p50_ms", "p95_ms"):
                for wl in WORKLOADS:
                    cr = canonical_request_summaries.get(wl, {})
                    pr = persisted_reqs.get(wl, {})
                    if not isinstance(pr, dict):
                        continue
                    persisted_val = pr.get(diag_field)
                    if persisted_val is not None:
                        if not (isinstance(persisted_val, (int, float)) and math.isfinite(persisted_val) and persisted_val >= 0):
                            failures.append(
                                f"sample.json {wl}.{diag_field} is non-finite, negative, "
                                f"or NaN: {persisted_val}"
                            )
                        else:
                            canonical_val = cr.get(diag_field)
                            if canonical_val is not None and isinstance(canonical_val, (int, float)):
                                if abs(float(persisted_val) - float(canonical_val)) > 1.0:
                                    failures.append(
                                        f"sample.json {wl}.{diag_field} ({persisted_val}) differs "
                                        f"from events-reconstructed ({canonical_val}) by >1ms"
                                    )

    # ------------------------------------------------------------------
    # 3. scrapes.jsonl — records + scrapes
    # ------------------------------------------------------------------
    sjl_path = sample_dir / "scrapes.jsonl"
    if not sjl_path.exists():
        failures.append(f"scrapes.jsonl not found in {sample_dir}")
    else:
        try:
            for line in sjl_path.read_text().strip().split("\n"):
                line = line.strip()
                if line:
                    try:
                        canonical_scrapes.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        failures.append(f"scrapes.jsonl parse error: {e}")
        except OSError as e:
            failures.append(f"scrapes.jsonl read error: {e}")

    # ---- Validate scrape record timing (start/completion offsets) ----
    _validate_scrape_timing(canonical_scrapes, sample_data, failures)

    # ------------------------------------------------------------------
    # 4. Raw .prom files — one-to-one with scrape records, exact paths
    #    from manifest, no glob source for snapshots.
    #
    #    HIGH CONTAINMENT: The metrics/ directory must be a real directory
    #    inside sample_dir (not a symlink). Each raw path must resolve
    #    beneath this trusted metrics child. Symlinked raw files are also
    #    rejected for simpler artifact authority.
    # ------------------------------------------------------------------
    metrics_dir = sample_dir / "metrics"

    # Resolve sample_dir safely first
    try:
        sample_dir_resolved = sample_dir.resolve(strict=False)
    except (OSError, RuntimeError):
        sample_dir_resolved = None
        failures.append(f"cannot resolve sample_dir: {sample_dir}")

    # metrics_dir must be a real directory inside sample_dir, not a symlink
    metrics_dir_trusted: Path | None = None
    if sample_dir_resolved is not None:
        trusted_metrics = sample_dir_resolved / "metrics"
        try:
            if metrics_dir.is_symlink():
                failures.append(
                    f"metrics directory is a symlink, rejecting for containment safety: {metrics_dir}"
                )
            elif not metrics_dir.is_dir():
                failures.append(
                    f"metrics directory is absent or not a directory: {metrics_dir}"
                )
            else:
                metrics_dir_resolved = metrics_dir.resolve(strict=False)
                if metrics_dir_resolved != trusted_metrics:
                    failures.append(
                        f"metrics directory resolve mismatch: "
                        f"{metrics_dir_resolved} != {trusted_metrics}"
                    )
                else:
                    metrics_dir_trusted = metrics_dir_resolved
        except (OSError, RuntimeError) as e:
            failures.append(f"metrics directory resolve error: {e}")

    recorded_paths: set[str] = set()
    for rec in canonical_scrapes:
        raw_path = str(rec.get("path", ""))
        if raw_path:
            # Resolve to absolute path for comparison
            raw_full = Path(raw_path)
            if not raw_full.is_absolute():
                raw_full = (sample_dir / raw_full).resolve(strict=False)
            else:
                raw_full = raw_full.resolve(strict=False)
            recorded_paths.add(str(raw_full))
            # Reject raw file symlinks (simpler artifact authority)
            if raw_full.is_symlink():
                failures.append(
                    f"scrape record raw file is a symlink, rejecting: {raw_path} "
                    f"(resolved {raw_full})"
                )
            if not raw_full.exists():
                failures.append(f"scrape record references nonexistent raw file: {raw_path}")
            # Require resolved path to be beneath exactly metrics_dir, not merely
            # sample_dir. Rejects sibling-prefix (sample-evil), .., absolute
            # outside paths, and symlink targets escaping metrics dir.
            if metrics_dir_trusted is not None:
                try:
                    if not raw_full.is_relative_to(metrics_dir_trusted):
                        failures.append(
                            f"scrape record path escapes metrics directory: {raw_path} "
                            f"(resolved {raw_full} is not under {metrics_dir_trusted})"
                        )
                except (OSError, RuntimeError):
                    failures.append(f"scrape record path cannot be resolved: {raw_path}")
            else:
                failures.append(f"metrics directory cannot be resolved or is untrusted: {metrics_dir}")

            # Parse the .prom file for scheduler metrics
            if raw_full.exists():
                try:
                    prom_text = raw_full.read_text()
                    metrics = parse_scheduler_metrics(prom_text)
                    canonical_snapshots.append({
                        "polls": dict(metrics.polls),
                        "queued": dict(metrics.queued),
                        "active": metrics.active,
                    })
                except (OSError, ValueError) as e:
                    failures.append(f"failed to parse .prom file {raw_path}: {e}")

    # Check for unreferenced extra raw files (only if metrics dir is trusted)
    if metrics_dir_trusted is not None and metrics_dir_trusted.exists():
        for prom_path in metrics_dir_trusted.iterdir():
            if prom_path.suffix == ".prom":
                resolved_path = str(prom_path.resolve(strict=False))
                if resolved_path not in recorded_paths:
                    failures.append(f"unreferenced raw .prom file: {prom_path}")

    # Record/raw count mismatch
    if metrics_dir_trusted is not None and metrics_dir_trusted.exists():
        prom_files = list(metrics_dir_trusted.glob("*.prom"))
    else:
        prom_files = []
    actual_record_count = sum(1 for r in canonical_scrapes if r.get("status"))
    if actual_record_count != len(prom_files):
        failures.append(
            f"record/raw count mismatch: {actual_record_count} scrapes vs {len(prom_files)} .prom files"
        )

    status = "passed" if not failures else "invalid"
    return PersistedSample(
        validation=ArtifactValidation(status=status, failures=tuple(failures)),
        metadata=canonical_metadata,
        request_events=canonical_events,
        request_summaries=canonical_request_summaries,
        scrape_records=canonical_scrapes,
        scheduler_snapshots=canonical_snapshots,
        artifact_dir=str(sample_dir),
    )



def validate_sample_artifacts(
    sample: Mapping[str, object],
    work_dir: str | None = None,
    scheduler_metrics_required: bool = True,
) -> ArtifactValidation:
    """Cross-check sample summary, request JSONL, scrape JSONL, and raw file counts.

    The sample dict must have 'mode', 'name', 'sample' fields and may have
    'requests', 'scrape_records', 'scheduler_snapshots' summaries.
    If work_dir is provided, also validates that the persisted files exist.

    When *scheduler_metrics_required* is False (disabled target), scheduler
    metrics polls/queued/active are not required — their absence is valid
    artifact evidence.
    """
    failures: list[str] = []
    phase = str(sample.get("name", "unknown"))
    mode = str(sample.get("mode", "unknown"))
    iteration = sample.get("sample", 0)

    # Check sample.json was finalized with iteration/sample info
    if not iteration:
        failures.append(f"{phase}/{mode}: sample missing iteration number")

    # Check request summary matches persisted JSONL expectations
    reqs = sample.get("requests", {})
    if not isinstance(reqs, dict):
        failures.append(f"{phase}/{mode}: requests is not a dict")

    # Validate scrape records: expected count, unique offsets, status success
    scrape_records = sample.get("scrape_records", [])
    if isinstance(scrape_records, list):
        if not scrape_records:
            failures.append(f"{phase}/{mode}: no scrape records found")
        else:
            offsets = []
            for i, rec in enumerate(scrape_records):
                if not isinstance(rec, dict):
                    failures.append(f"{phase}/{mode}: scrape_records[{i}] is not a dict")
                    continue
                offset = rec.get("offset")
                if offset is None:
                    failures.append(f"{phase}/{mode}: scrape_records[{i}] missing offset")
                else:
                    offsets.append(offset)
                status = rec.get("status", "")
                if status == "missed":
                    failures.append(
                        f"{phase}/{mode}: scrape offset {offset} is missed"
                    )
                elif status == "error":
                    err_text = rec.get("error", "unknown")
                    failures.append(
                        f"{phase}/{mode}: scrape offset {offset} error: {err_text}"
                    )
                elif status != "success":
                    failures.append(
                        f"{phase}/{mode}: scrape offset {offset} unexpected status {status!r}"
                    )
                # Check HTTP status acceptable
                http_status = rec.get("http_status")
                if http_status is not None and http_status >= 400:
                    failures.append(
                        f"{phase}/{mode}: scrape offset {offset} HTTP {http_status}"
                    )
                # Check raw body/path present
                if status == "success":
                    text = rec.get("text")
                    if text is None:
                        failures.append(
                            f"{phase}/{mode}: scrape offset {offset} missing raw body text"
                        )
                    path = rec.get("path")
                    if path is None:
                        failures.append(
                            f"{phase}/{mode}: scrape offset {offset} missing .prom artifact path"
                        )
            # Validate unique ordered offsets
            sorted_offsets = sorted(offsets)
            if len(offsets) != len(set(offsets)):
                failures.append(f"{phase}/{mode}: scrape offsets are not unique")
            if offsets != sorted_offsets:
                failures.append(f"{phase}/{mode}: scrape offsets are not in order")

            # Validate scheduler snapshots only when required
            if scheduler_metrics_required:
                snapshots = sample.get("scheduler_snapshots", [])
                if isinstance(snapshots, list):
                    if not snapshots:
                        failures.append(f"{phase}/{mode}: no scheduler snapshots found but scheduler_metrics_required=True")
                    for i, snap in enumerate(snapshots):
                        if not isinstance(snap, dict):
                            failures.append(f"{phase}/{mode}: scheduler_snapshots[{i}] is not a dict")
                            continue
                        polls = snap.get("polls", {})
                        if not isinstance(polls, dict):
                            failures.append(f"{phase}/{mode}: scheduler_snapshots[{i}] polls not a dict")
                        for wl in WORKLOADS:
                            if wl not in polls:
                                failures.append(
                                    f"{phase}/{mode}: scheduler_snapshots[{i}] missing {wl} polls"
                                )
                        queued = snap.get("queued", {})
                        if not isinstance(queued, dict):
                            failures.append(f"{phase}/{mode}: scheduler_snapshots[{i}] queued not a dict")
                        for wl in WORKLOADS:
                            if wl not in queued:
                                failures.append(
                                    f"{phase}/{mode}: scheduler_snapshots[{i}] missing {wl} queued"
                                )
                        if snap.get("active") is None:
                            failures.append(
                                f"{phase}/{mode}: scheduler_snapshots[{i}] missing active"
                            )
                else:
                    failures.append(f"{phase}/{mode}: scheduler_snapshots is not a list")
    else:
        failures.append(f"{phase}/{mode}: scrape_records is not a list")

    status = "passed" if not failures else "invalid"
    return ArtifactValidation(status=status, failures=tuple(failures))


# ---------------------------------------------------------------------------
# Per-iteration scrape validation
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class IterationScrapeValidation:
    """Result of validating scrape completeness for one (iteration, phase)."""

    iteration: int
    phase: str
    target_mode: str  # "baseline" or "scheduled"
    expected_scrape_count: int
    actual_scrape_count: int
    scrape_offsets: tuple[float, ...]
    expected_offsets: tuple[float, ...]
    status: str  # "passed", "invalid", "error"
    failures: tuple[str, ...]


def validate_iteration_scrape(
    sample: Mapping[str, object],
    expected_scrape_count: int,
    scrape_interval: float = 1.0,
    duration: float = 60.0,
    scheduler_metrics_required: bool = True,
) -> IterationScrapeValidation:
    """Validate raw scrape records for one (iteration, phase, target).

    Checks:
    - exact expected_scrape_count
    - unique ordered indexes
    - exact planned offsets 0, interval, ..., duration
    - status success, no error, acceptable HTTP status
    - raw body/path present
    - scheduler metrics completeness only when *scheduler_metrics_required* is True

    When *scheduler_metrics_required* is False (baseline), scheduler metrics
    completeness/reset/mechanism fields are NOT required — their absence is
    valid artifact evidence.
    """
    phase = str(sample.get("name", "unknown"))
    mode = str(sample.get("mode", "unknown"))
    iteration = int(sample.get("sample", 0))
    failures: list[str] = []

    scrape_records = sample.get("scrape_records", [])
    if not isinstance(scrape_records, list):
        return IterationScrapeValidation(
            iteration=iteration,
            phase=phase,
            target_mode=mode,
            expected_scrape_count=expected_scrape_count,
            actual_scrape_count=0,
            scrape_offsets=(),
            expected_offsets=tuple(i * scrape_interval for i in range(expected_scrape_count)),
            status="invalid",
            failures=("scrape_records is not a list",),
        )

    actual = len(scrape_records)
    if actual != expected_scrape_count:
        failures.append(
            f"expected {expected_scrape_count} scrape records, got {actual}"
        )

    offsets = []
    planned_offsets = set()
    num_steps = int(duration / scrape_interval)
    for i in range(num_steps + 1):
        planned_offsets.add(i * scrape_interval)
    if 0 not in planned_offsets:
        planned_offsets.add(0.0)
    planned_offsets.add(duration)

    for i, rec in enumerate(scrape_records):
        if not isinstance(rec, dict):
            failures.append(f"scrape_records[{i}] is not a dict")
            continue
        offset = rec.get("offset")
        if offset is None:
            failures.append(f"scrape_records[{i}] missing offset")
            continue
        offsets.append(offset)
        if offset not in planned_offsets:
            failures.append(f"scrape offset {offset} not in planned offsets")
        status = rec.get("status", "")
        if status == "missed":
            failures.append(f"scrape offset {offset}: missed")
        elif status == "error":
            err_text = rec.get("error", "unknown")
            failures.append(f"scrape offset {offset}: error: {err_text}")
        elif status != "success":
            failures.append(f"scrape offset {offset}: unexpected status {status!r}")
        http_status = rec.get("http_status")
        if http_status is not None and http_status >= 400:
            failures.append(f"scrape offset {offset}: HTTP {http_status}")
        text = rec.get("text")
        path = rec.get("path")
        if status == "success":
            if text is None:
                failures.append(f"scrape offset {offset}: missing raw body text")
            if path is None:
                failures.append(f"scrape offset {offset}: missing .prom artifact path")

    # Check unique offsets
    if len(offsets) != len(set(offsets)):
        failures.append("scrape offsets are not unique")

    # ---- Scrape timing validation (start, completion, lateness bounds) ----
    _validate_scrape_timing(
        list(scrape_records),
        dict(sample),
        failures,
    )

    # Check scheduler metrics only when required (baseline may not have them)
    if scheduler_metrics_required:
        snapshots = sample.get("scheduler_snapshots", [])
        if isinstance(snapshots, list):
            if not snapshots:
                failures.append("no scheduler snapshots found but scheduler_metrics_required=True")
            for i, snap in enumerate(snapshots):
                if not isinstance(snap, dict):
                    failures.append(f"scheduler_snapshots[{i}] is not a dict")
                    continue
                for wl in WORKLOADS:
                    polls = snap.get("polls", {})
                    if not isinstance(polls, dict) or wl not in polls:
                        failures.append(
                            f"scheduler_snapshots[{i}] missing {wl} polls"
                        )
                    queued = snap.get("queued", {})
                    if not isinstance(queued, dict) or wl not in queued:
                        failures.append(
                            f"scheduler_snapshots[{i}] missing {wl} queued"
                        )
                if snap.get("active") is None:
                    failures.append(f"scheduler_snapshots[{i}] missing active")
        else:
            failures.append("scheduler_snapshots is not a list")

    status = "passed"
    if failures:
        status = "invalid"

    return IterationScrapeValidation(
        iteration=iteration,
        phase=phase,
        target_mode=mode,
        expected_scrape_count=expected_scrape_count,
        actual_scrape_count=actual,
        scrape_offsets=tuple(offsets),
        expected_offsets=tuple(sorted(planned_offsets)),
        status=status,
        failures=tuple(failures),
    )


# ---------------------------------------------------------------------------
# Parsing functions
# ---------------------------------------------------------------------------


def parse_scheduler_metrics(text: str) -> SchedulerMetrics:
    """Parse scheduler polls, queues, and active admissions from Prometheus text."""

    polls: dict[str, int] = {}
    queued: dict[str, int] = {}
    active: int | None = None
    for line in text.splitlines():
        match = SAMPLE_RE.match(line)
        if not match:
            continue
        metric, label_text, value_text = match.groups()
        if metric not in (POLLS_METRIC, QUEUED_METRIC, ACTIVE_METRIC):
            continue
        value = _integer_metric_value(value_text)
        if value is None:
            continue
        labels = _parse_labels(label_text)
        if labels is None:
            continue
        if metric == ACTIVE_METRIC:
            if not labels:
                active = value
            continue
        workload = labels.get("workload")
        if workload not in WORKLOADS:
            continue
        if metric == POLLS_METRIC:
            polls[workload] = value
        else:
            queued[workload] = value
    return SchedulerMetrics(polls=polls, queued=queued, active=active)


def interval_delta(
    before: SchedulerMetrics, after: SchedulerMetrics
) -> SchedulerInterval:
    """Compute one scrape interval, refusing poll deltas after counter resets."""

    polls: dict[str, int] | None = None
    counter_reset = False
    if all(
        workload in before.polls and workload in after.polls
        for workload in WORKLOADS
    ):
        counter_reset = any(
            after.polls[workload] < before.polls[workload]
            for workload in WORKLOADS
        )
        if not counter_reset:
            polls = {
                workload: after.polls[workload] - before.polls[workload]
                for workload in WORKLOADS
            }
    strict_dual_backlog = (
        polls is not None
        and all(
            before.queued.get(workload, 0) > 0 and after.queued.get(workload, 0) > 0
            for workload in WORKLOADS
        )
    )
    active_values = [
        value for value in (before.active, after.active) if value is not None
    ]
    return SchedulerInterval(
        polls=polls,
        counter_reset=counter_reset,
        strict_dual_backlog=strict_dual_backlog,
        active_max=max(active_values) if active_values else None,
    )


def summarize_scheduler_metrics(
    snapshots: Sequence[SchedulerMetrics],
) -> SchedulerSummary:
    """Summarize whole-window and strict dual-backlog poll shares separately."""

    intervals = [
        interval_delta(before, after)
        for before, after in zip(snapshots, snapshots[1:])
    ]
    whole_window_polls = _sum_polls(
        interval.polls for interval in intervals if interval.polls is not None
    )
    dual_backlog_intervals = [
        interval
        for interval in intervals
        if interval.strict_dual_backlog and interval.polls is not None
    ]
    dual_backlog_polls = _sum_polls(
        interval.polls for interval in dual_backlog_intervals
    )
    active_values = [
        snapshot.active for snapshot in snapshots if snapshot.active is not None
    ]
    return SchedulerSummary(
        whole_window_polls=whole_window_polls,
        whole_window_write_share=_share(whole_window_polls, WRITE_WORKLOAD),
        dual_backlog_polls=dual_backlog_polls,
        dual_backlog_write_share=_share(dual_backlog_polls, WRITE_WORKLOAD),
        interval_count=len(intervals),
        dual_backlog_interval_count=len(dual_backlog_intervals),
        dual_backlog_interval_fraction=(
            len(dual_backlog_intervals) / len(intervals) if intervals else None
        ),
        counter_reset_interval_count=sum(
            interval.counter_reset for interval in intervals
        ),
        max_active_polls=max(active_values) if active_values else None,
        metrics_complete=len(snapshots) >= 2
        and all(snapshot.complete() for snapshot in snapshots),
    )


# ---------------------------------------------------------------------------
# Evaluation functions
# ---------------------------------------------------------------------------


def evaluate_scheduler_report(
    phases: Mapping[str, SchedulerSummary | None],
    thresholds: SchedulerGateThresholds,
    *,
    errors: Sequence[str] = (),
) -> SchedulerEvaluation:
    """Evaluate scheduler gates without reading files or depending on benchmark state.

    All threshold values are taken from the thresholds object — no hardcoded
    0.20/0.80/[0.78,0.82] values remain in the evaluator logic. The hardcoded
    saturated dual-backlog checks that were present before have been removed in
    favor of threshold-driven logic.
    """

    if errors:
        return _evaluation("error", tuple(errors))
    threshold_error = _validate_thresholds(thresholds)
    if threshold_error:
        return _evaluation("error", (threshold_error,))

    missing_phases = [
        phase for phase in thresholds.required_phases if phase not in phases
    ]
    if missing_phases:
        return _evaluation(
            "invalid",
            (f"missing required phases: {', '.join(missing_phases)}",),
        )
    missing_metrics = []
    for phase in thresholds.required_phases:
        summary = phases[phase]
        if summary is None or not summary.metrics_complete:
            missing_metrics.append(phase)
    if missing_metrics:
        return _evaluation(
            "invalid",
            (
                f"missing required scheduler metrics: {', '.join(missing_metrics)}",
            ),
        )

    summaries: dict[str, SchedulerSummary] = {}
    for phase in thresholds.required_phases:
        summary = phases[phase]
        if summary is None:
            return _evaluation(
                "invalid", (f"missing required scheduler metrics: {phase}",)
            )
        summaries[phase] = summary
    counter_resets = [
        phase
        for phase, summary in summaries.items()
        if summary.counter_reset_interval_count > 0
    ]
    if counter_resets:
        return _evaluation(
            "invalid",
            (f"scheduler poll counter reset: {', '.join(counter_resets)}",),
        )

    failures: list[str] = []
    saturated = summaries["saturated"]
    write_share = saturated.dual_backlog_write_share

    # Use configured bounds from thresholds, not hardcoded [0.78, 0.82]
    if write_share is None or not (
        thresholds.write_share_min <= write_share <= thresholds.write_share_max
    ):
        failures.append(
            "dual-backlog write share must be within "
            f"[{thresholds.write_share_min:.3f}, {thresholds.write_share_max:.3f}]"
        )
    fraction = saturated.dual_backlog_interval_fraction
    if fraction is None or fraction < thresholds.min_dual_backlog_interval_fraction:
        failures.append(
            "dual-backlog interval fraction is below "
            f"{thresholds.min_dual_backlog_interval_fraction:.3f}"
        )

    # Saturated per-class dual-backlog minimum polls
    sat_db = saturated.dual_backlog_polls
    for w in WORKLOADS:
        if sat_db.get(w, 0) < thresholds.min_dual_backlog_polls_per_class:
            failures.append(
                f"saturated dual-backlog {w} polls ({sat_db.get(w, 0)}) below "
                f"minimum {thresholds.min_dual_backlog_polls_per_class}"
            )

    # Single-class purity: query_only query poll share, write_only write poll share
    for phase, workload in (
        ("query_only", QUERY_WORKLOAD),
        ("write_only", WRITE_WORKLOAD),
    ):
        share = _share(summaries[phase].whole_window_polls, workload)
        if share is None or share < thresholds.single_class_purity_min_share:
            _format_share = f"{share:.4f}" if share is not None else "None"
            failures.append(
                f"{phase} {workload} whole-window poll share ({_format_share}) is below "
                f"{thresholds.single_class_purity_min_share:.3f}"
            )

    # Light-write: both classes positive, query whole-window share STRICTLY > threshold
    lw = summaries["light_write"]
    lw_q_share = _share(lw.whole_window_polls, QUERY_WORKLOAD)
    lw_w_share = _share(lw.whole_window_polls, WRITE_WORKLOAD)

    # Both classes must have poll progress
    if lw_q_share is None or lw_w_share is None:
        failures.append(
            "light_write both classes must have poll progress "
            f"(query share: {lw_q_share}, write share: {lw_w_share})"
        )
    else:
        # Query share STRICTLY greater than configured threshold
        if lw_q_share <= thresholds.min_light_write_query_share:
            failures.append(
                f"light_write query whole-window share ({lw_q_share:.4f}) must be "
                f"strictly > {thresholds.min_light_write_query_share}"
            )
        # Write share must be positive
        if lw_w_share <= 0.0:
            _fw = f"{lw_w_share:.4f}" if lw_w_share is not None else "None"
            failures.append(
                f"light_write write whole-window share ({_fw}) must be positive"
            )

    # active_within_scheduler_limit: if true, every phase must respect limit
    if thresholds.active_within_scheduler_limit:
        for phase_name in thresholds.required_phases:
            summary = summaries[phase_name]
            if summary.max_active_polls is None:
                failures.append(
                    f"{phase_name} has no active admission data"
                )
            elif summary.max_active_polls > thresholds.max_active_polls:
                failures.append(
                    f"{phase_name} max active polls ({summary.max_active_polls}) "
                    f"exceeds limit ({thresholds.max_active_polls})"
                )
    else:
        # If false, active bound is diagnostic only — check but don't gate
        for phase_name in thresholds.required_phases:
            summary = summaries[phase_name]
            if summary.max_active_polls is not None and summary.max_active_polls > thresholds.max_active_polls:
                pass  # diagnostic only, not a gate failure

    return _evaluation("failed" if failures else "passed", tuple(failures))


def evaluate_request_validity(
    samples: Sequence[Mapping[str, object]],
    max_failure_rate: float = 0.01,
    max_outstanding_requests: int = 0,
) -> RequestEvaluation:
    """Check request validity for every active workload in every sample.

    Each sample dict must have:
      - name (phase name)
      - requests: dict with workload keys, each with started/completed/failures/etc.
      - mode: "baseline" or "scheduled"

    **Invalid vs failed split**:
    - Integrity / schema errors => ``invalid``:
      missing fields, non-finite numbers, non-integer counts, malformed dicts,
      inconsistent identities/counts/accounting (started vs requests count),
      completed_failures > completed, failures != completed_failures + timeouts,
      events count mismatch.
    - Well-formed observations that miss a threshold requirement => ``failed``:
      failure fraction too high, outstanding requests after drain.
    - ``--no-gate`` suppresses only ``failed`` status; ``invalid`` retains exit 2.
    """
    invalid_failures: list[str] = []
    gate_failures: list[str] = []
    for idx, sample in enumerate(samples):
        phase = str(sample.get("name", f"sample-{idx}"))
        mode = str(sample.get("mode", "unknown"))
        reqs = sample.get("requests", {})
        if not isinstance(reqs, dict):
            invalid_failures.append(f"{phase}/{mode}: requests is not a dict")
            continue
        for wl in WORKLOADS:
            # Skip workloads that have no workers assigned and no request data
            if not _has_workers(sample, wl):
                continue
            wl_dict = reqs.get(wl, {})
            if not isinstance(wl_dict, dict) or not wl_dict:
                invalid_failures.append(
                    f"{phase}/{mode}/{wl}: requests entry is missing or malformed"
                )
                continue

            # ---- schema / integrity checks => invalid ----
            # Validate schema and types — non-finite/non-integer => invalid
            started = wl_dict.get("started", 0)
            if not isinstance(started, (int, float)) or not math.isfinite(started) or started < 0 or (isinstance(started, float) and not started.is_integer()):
                invalid_failures.append(f"{phase}/{mode}/{wl}: invalid non-integer/finite started count")
                continue
            started = int(started)

            completed = wl_dict.get("completed", 0)
            if not isinstance(completed, (int, float)) or not math.isfinite(completed) or completed < 0 or (isinstance(completed, float) and not completed.is_integer()):
                invalid_failures.append(f"{phase}/{mode}/{wl}: invalid completed count")
                continue
            completed = int(completed)

            completed_failures = wl_dict.get("completed_failures", 0)
            if not isinstance(completed_failures, (int, float)) or not math.isfinite(completed_failures) or completed_failures < 0 or (isinstance(completed_failures, float) and not completed_failures.is_integer()):
                invalid_failures.append(f"{phase}/{mode}/{wl}: invalid completed_failures count")
                continue
            completed_failures = int(completed_failures)

            timeouts = wl_dict.get("timeouts", 0)
            if not isinstance(timeouts, (int, float)) or not math.isfinite(timeouts) or timeouts < 0 or (isinstance(timeouts, float) and not timeouts.is_integer()):
                invalid_failures.append(f"{phase}/{mode}/{wl}: invalid timeouts count")
                continue
            timeouts = int(timeouts)

            outstanding = wl_dict.get("outstanding", 0)
            if not isinstance(outstanding, (int, float)) or not math.isfinite(outstanding) or outstanding < 0 or (isinstance(outstanding, float) and not outstanding.is_integer()):
                invalid_failures.append(f"{phase}/{mode}/{wl}: invalid outstanding count")
                continue
            outstanding = int(outstanding)

            total_failures = wl_dict.get("failures", 0)
            if not isinstance(total_failures, (int, float)) or not math.isfinite(total_failures) or total_failures < 0 or (isinstance(total_failures, float) and not total_failures.is_integer()):
                invalid_failures.append(f"{phase}/{mode}/{wl}: invalid failures count")
                continue
            total_failures = int(total_failures)

            raw_requests = wl_dict.get("requests", 0)
            if not isinstance(raw_requests, (int, float)) or not math.isfinite(raw_requests) or raw_requests < 0 or (isinstance(raw_requests, float) and not raw_requests.is_integer()):
                invalid_failures.append(f"{phase}/{mode}/{wl}: invalid requests count")
                continue
            raw_requests = int(raw_requests)

            # Accounting validation — inconsistency => invalid
            if raw_requests != started:
                invalid_failures.append(
                    f"{phase}/{mode}/{wl}: requests ({raw_requests}) != started ({started})"
                )

            if completed_failures > completed:
                invalid_failures.append(
                    f"{phase}/{mode}/{wl}: completed_failures ({completed_failures}) > completed ({completed})"
                )

            expected_failures = completed_failures + timeouts
            if total_failures != expected_failures:
                invalid_failures.append(
                    f"{phase}/{mode}/{wl}: failures ({total_failures}) != completed_failures ({completed_failures}) + timeouts ({timeouts}) = {expected_failures}"
                )

            expected_completed_or_timeout = completed + timeouts
            if started > 0 and expected_completed_or_timeout != started:
                invalid_failures.append(
                    f"{phase}/{mode}/{wl}: completed ({completed}) + timeouts ({timeouts}) = {expected_completed_or_timeout} != started ({started})"
                )

            # At least one measured request
            if started == 0:
                invalid_failures.append(
                    f"{phase}/{mode}/{wl}: no requests started ({started})"
                )
                continue

            # ---- threshold gates => failed ----
            # Failure fraction strictly below max_failure_rate
            if started > 0:
                frac = total_failures / started
                if frac >= max_failure_rate:
                    gate_failures.append(
                        f"{phase}/{mode}/{wl}: failure fraction {frac:.4f} "
                        f">= {max_failure_rate}"
                    )

            # No outstanding after drain if required
            if max_outstanding_requests >= 0 and outstanding > max_outstanding_requests:
                gate_failures.append(
                    f"{phase}/{mode}/{wl}: {outstanding} outstanding requests after drain "
                    f"(max allowed: {max_outstanding_requests})"
                )

    # ---- Determine final status ----
    if not samples:
        return RequestEvaluation(status="invalid", failures=("no samples provided",))

    if invalid_failures:
        return RequestEvaluation(status="invalid", failures=tuple(invalid_failures))

    if gate_failures:
        return RequestEvaluation(status="failed", failures=tuple(gate_failures))

    return RequestEvaluation(status="passed", failures=())


def evaluate_performance(
    iterations: Sequence[tuple[int, Mapping[str, object]]],
    phases_order: Sequence[str],
    max_regression_pct: float = 5.0,
) -> list[PerformanceEvaluation]:
    """Evaluate performance per iteration.

    *iterations* is a list of (iteration_number, phase_data_dict) where
    phase_data_dict maps phase name -> {baseline, scheduled} each with
    {query_rps, write_rps} and baseline capacity references.

    Returns one PerformanceEvaluation per phase per iteration.
    Missing/malformed cell/capacity/change => invalid (never skipped).
    """
    results: list[PerformanceEvaluation] = []
    for iteration, phase_data in iterations:
        # Find capacities: baseline query_only query_rps and write_only write_rps
        baseline_query_capacity = None
        baseline_write_capacity = None
        qo = phase_data.get("query_only", {})
        if isinstance(qo, dict):
            bl = qo.get("baseline", {})
            if isinstance(bl, dict):
                baseline_query_capacity = bl.get("query_rps")
        wo = phase_data.get("write_only", {})
        if isinstance(wo, dict):
            bl = wo.get("baseline", {})
            if isinstance(bl, dict):
                baseline_write_capacity = bl.get("write_rps")

        if not baseline_query_capacity or not baseline_write_capacity:
            # Still produce evaluation for every phase, marking as invalid
            for phase in phases_order:
                results.append(
                    PerformanceEvaluation(
                        phase=phase,
                        iteration=iteration,
                        baseline_normalized=0.0,
                        scheduled_normalized=0.0,
                        pct_change=None,
                        passed=False,
                        details=f"missing baseline query_only or write_only capacity for iteration {iteration}",
                    )
                )
            continue

        for phase in phases_order:
            pd = phase_data.get(phase, {})
            if not isinstance(pd, dict):
                results.append(
                    PerformanceEvaluation(
                        phase=phase,
                        iteration=iteration,
                        baseline_normalized=0.0,
                        scheduled_normalized=0.0,
                        pct_change=None,
                        passed=False,
                        details=f"missing phase data for {phase} in iteration {iteration}",
                    )
                )
                continue
            bl = pd.get("baseline", {})
            sch = pd.get("scheduled", {})
            if not isinstance(bl, dict) or not isinstance(sch, dict):
                results.append(
                    PerformanceEvaluation(
                        phase=phase,
                        iteration=iteration,
                        baseline_normalized=0.0,
                        scheduled_normalized=0.0,
                        pct_change=None,
                        passed=False,
                        details=f"missing baseline or scheduled data for {phase} in iteration {iteration}",
                    )
                )
                continue

            bl_qrps = bl.get("query_rps")
            bl_wrps = bl.get("write_rps")
            sch_qrps = sch.get("query_rps")
            sch_wrps = sch.get("write_rps")

            # For single-class phases (query_only, write_only), produce entry
            # with pct_change=None and only the dominant RPS
            if phase in ("query_only", "write_only"):
                if bl_qrps is not None and isinstance(bl_qrps, (int, float)) and math.isfinite(bl_qrps) and bl_qrps > 0:
                    bl_norm = bl_qrps / baseline_query_capacity if baseline_query_capacity else 0.0
                    sch_norm = (sch_qrps / baseline_query_capacity if baseline_query_capacity else 0.0) if sch_qrps is not None else 0.0
                else:
                    bl_norm = 0.0
                    sch_norm = 0.0
                results.append(
                    PerformanceEvaluation(
                        phase=phase,
                        iteration=iteration,
                        baseline_normalized=bl_norm,
                        scheduled_normalized=sch_norm,
                        pct_change=None,
                        passed=True,
                        details=None,
                    )
                )
                continue

            # All four must be finite numeric
            missing = []
            for name, val in [("baseline query_rps", bl_qrps), ("baseline write_rps", bl_wrps),
                              ("scheduled query_rps", sch_qrps), ("scheduled write_rps", sch_wrps)]:
                if val is None:
                    missing.append(name)
                elif not (isinstance(val, (int, float)) and math.isfinite(val)):
                    missing.append(f"{name} (non-finite: {val})")

            if missing:
                results.append(
                    PerformanceEvaluation(
                        phase=phase,
                        iteration=iteration,
                        baseline_normalized=0.0,
                        scheduled_normalized=0.0,
                        pct_change=None,
                        passed=False,
                        details=f"missing or non-finite RPS values: {', '.join(missing)}",
                    )
                )
                continue

            bl_norm = bl_qrps / baseline_query_capacity + bl_wrps / baseline_write_capacity
            sch_norm = sch_qrps / baseline_query_capacity + sch_wrps / baseline_write_capacity

            # Guard: bl_norm must be finite and > 0, sch_norm finite and >= 0.
            # Zero/nonfinite baseline => invalid entry, never exception.
            if not (math.isfinite(bl_norm) and bl_norm > 0
                    and math.isfinite(sch_norm) and sch_norm >= 0):
                results.append(
                    PerformanceEvaluation(
                        phase=phase,
                        iteration=iteration,
                        baseline_normalized=bl_norm,
                        scheduled_normalized=sch_norm,
                        pct_change=None,
                        passed=False,
                        details=f"invalid: baseline normalized ({bl_norm}) must be finite positive, "
                                f"scheduled normalized ({sch_norm}) finite nonnegative",
                    )
                )
                continue

            pct = (sch_norm / bl_norm - 1.0) * 100.0

            passed = True
            details = None
            if pct is not None and pct < -max_regression_pct:
                passed = False
                details = (
                    f"scheduled vs baseline change {pct:.2f}% "
                    f"is below -{max_regression_pct:.1f}%"
                )

            results.append(
                PerformanceEvaluation(
                    phase=phase,
                    iteration=iteration,
                    baseline_normalized=bl_norm,
                    scheduled_normalized=sch_norm,
                    pct_change=pct,
                    passed=passed,
                    details=details,
                )
            )

    return results


# ---------------------------------------------------------------------------
# Status rank function
# ---------------------------------------------------------------------------


def _rank_status(status: str) -> int:
    """Rank status for precedence: passed=0 < failed=1 < invalid=2 < error=3."""
    return {"passed": 0, "failed": 1, "invalid": 2, "error": 3}.get(status, 3)


def combine_statuses(*statuses: str) -> str:
    """Combine multiple statuses using rank order.

    Returns the highest-rank (most severe) status.
    """
    ranked = [(s, _rank_status(s)) for s in statuses]
    ranked.sort(key=lambda x: x[1], reverse=True)
    return ranked[0][0]


# ---------------------------------------------------------------------------
# Public helpers for constructing unified reports
# ---------------------------------------------------------------------------


def make_performance_sample(
    phase: str,
    iteration: int,
    baseline_query_rps: float | None,
    baseline_write_rps: float | None,
    scheduled_query_rps: float | None,
    scheduled_write_rps: float | None,
    capacities: tuple[float, float] | None = None,
) -> dict[str, object]:
    """Build a performance sample dict for one phase/iteration pair.

    *capacities* is (query_capacity, write_capacity). When not provided they are
    inferred from the baseline query_only/write_only samples.
    """
    qc, wc = capacities or (0.0, 0.0)
    bl_qrps = baseline_query_rps or 0.0
    bl_wrps = baseline_write_rps or 0.0
    sch_qrps = scheduled_query_rps or 0.0
    sch_wrps = scheduled_write_rps or 0.0

    bl_norm = (bl_qrps / qc if qc else 0.0) + (bl_wrps / wc if wc else 0.0)
    sch_norm = (sch_qrps / qc if qc else 0.0) + (sch_wrps / wc if wc else 0.0)
    pct = None
    if math.isfinite(bl_norm) and bl_norm > 0 and math.isfinite(sch_norm) and sch_norm >= 0:
        pct = (sch_norm / bl_norm - 1.0) * 100.0

    return {
        "phase": phase,
        "iteration": iteration,
        "baseline": {"query_rps": bl_qrps, "write_rps": bl_wrps, "normalized": bl_norm},
        "scheduled": {"query_rps": sch_qrps, "write_rps": sch_wrps, "normalized": sch_norm},
        "pct_change": pct,
    }


def build_report(
    *,
    config: dict[str, object] | None = None,
    artifacts: dict[str, object] | None = None,
    samples: Sequence[Mapping[str, object]] | None = None,
    request_eval: RequestEvaluation | None = None,
    mechanism_eval: SchedulerEvaluation | None = None,
    performance_evals: Sequence[PerformanceEvaluation] | None = None,
    artifact_validation: ArtifactValidation | None = None,
    errors: Sequence[str] | None = None,
) -> dict[str, object]:
    """Build a unified benchmark report dictionary.

    Status precedence: error > invalid > failed > passed.

    In gated mode (when performance_evals is not None), absent request/
    mechanism/performance/artifact sections or expected checks => invalid.
    """
    errors = list(errors or [])
    statuses: list[str] = []

    if errors:
        statuses.append("error")

    # Check for missing evaluation sections in gated mode
    has_performance_evals = performance_evals is not None
    has_request_eval = request_eval is not None
    has_mechanism_eval = mechanism_eval is not None

    combined_failures: list[str] = list(errors)

    # In gated mode, all sections must be present
    if performance_evals is not None:  # gated mode
        missing_sections = []
        if not has_request_eval:
            missing_sections.append("request_evaluation")
        if not has_mechanism_eval:
            missing_sections.append("mechanism_evaluation")
        if missing_sections:
            statuses.append("invalid")
            combined_failures.append(
                f"missing sections in gated mode: {', '.join(missing_sections)}"
            )

    if request_eval is not None:
        statuses.append(request_eval.status)
        combined_failures.extend(request_eval.failures)

    if mechanism_eval is not None:
        statuses.append(mechanism_eval.status)
        combined_failures.extend(mechanism_eval.failures)

    if artifact_validation is not None:
        statuses.append(artifact_validation.status)
        combined_failures.extend(artifact_validation.failures)

    if performance_evals is not None:
        # Separate integrity failures (missing/nonfinite) from regression failures
        perf_integrity_failures = [
            e.details for e in performance_evals
            if not e.passed and e.details and e.details.startswith("invalid:")
        ]
        perf_regression_failures = [
            e.details for e in performance_evals
            if not e.passed and e.details and not e.details.startswith("invalid:")
        ]
        if perf_integrity_failures:
            statuses.append("invalid")
            combined_failures.extend(perf_integrity_failures)
        if perf_regression_failures:
            statuses.append("failed")
            combined_failures.extend(perf_regression_failures)

    # Combine statuses using rank precedence
    if not statuses:
        statuses.append("passed")
    status = combine_statuses(*statuses)
    exit_code = exit_code_for_status(status)

    report: dict[str, object] = {
        "status": status,
        "exit_code": exit_code,
        "failures": combined_failures,
    }
    if config is not None:
        report["config"] = config
    if artifacts is not None:
        report["artifacts"] = artifacts
    if samples is not None:
        report["samples"] = list(samples)
    if request_eval is not None:
        report["request_evaluation"] = {
            "status": request_eval.status,
            "failures": list(request_eval.failures),
        }
    if mechanism_eval is not None:
        report["mechanism_evaluation"] = {
            "status": mechanism_eval.status,
            "passed": mechanism_eval.passed,
            "exit_code": mechanism_eval.exit_code,
            "failures": list(mechanism_eval.failures),
        }
    if artifact_validation is not None:
        report["artifact_validation"] = {
            "status": artifact_validation.status,
            "failures": list(artifact_validation.failures),
        }
    if performance_evals is not None:
        report["performance_evaluations"] = [
            dataclasses.asdict(e) if isinstance(e, PerformanceEvaluation) else e
            for e in performance_evals
        ]
        # Compute median and worst pct_change
        valid_changes = [
            e.pct_change for e in performance_evals if e.pct_change is not None
        ]
        if valid_changes:
            report["performance_median_pct_change"] = sorted(valid_changes)[
                len(valid_changes) // 2
            ]
            report["performance_worst_pct_change"] = min(valid_changes)

    return report


def exit_code_for_status(status: str) -> int:
    """Map every defined verification status to a process exit code."""

    try:
        return STATUS_EXIT_CODES[status]
    except KeyError as error:
        raise ValueError(f"unknown scheduler verification status: {status}") from error


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def percentile(values: list[float], quantile: float) -> float | None:
    """Compute the given quantile (0-1) over sorted *values*.

    Returns None for empty inputs.
    """
    if not values:
        return None
    index = min(math.ceil(len(values) * quantile) - 1, len(values) - 1)
    return values[max(index, 0)]


def _has_workers(sample: Mapping[str, object], workload: str) -> bool:
    workers = sample.get("workers")
    if isinstance(workers, dict):
        return int(workers.get(workload, 0)) > 0
    return True


def _parse_labels(label_text: str | None) -> dict[str, str] | None:
    if label_text is None:
        return {}
    labels: dict[str, str] = {}
    offset = 0
    while offset < len(label_text):
        match = LABEL_RE.match(label_text, offset)
        if not match:
            return None
        key, value = match.groups()
        labels[key] = value
        offset = match.end()
    return labels


def _integer_metric_value(value_text: str) -> int | None:
    try:
        value = float(value_text)
    except ValueError:
        return None
    if not math.isfinite(value) or value < 0 or not value.is_integer():
        return None
    return int(value)


def _sum_polls(
    poll_sets: Iterable[Mapping[str, int] | None],
) -> dict[str, int]:
    totals = {workload: 0 for workload in WORKLOADS}
    for polls in poll_sets:
        if polls is not None:
            for workload in WORKLOADS:
                totals[workload] += polls[workload]
    return totals


def _share(
    polls: Mapping[str, int], workload: str
) -> float | None:
    total = sum(polls.values())
    return polls[workload] / total if total else None


def _validate_thresholds(
    thresholds: SchedulerGateThresholds,
) -> str | None:
    """Mirror Rust PerformanceGate validation exactly.

    Validated constraints:
    - max_failure_rate finite in [0, 1]
    - max_outstanding_requests nonnegative
    - write_share_min/write_share_max finite, ordered in [0,1], contain expected_write_share (0.8)
    - min_dual_backlog_interval_fraction finite in [0, 1]
    - min_dual_backlog_polls_per_class positive (> 0)
    - single_class_purity_min_share finite in [0, 1]
    - max_active_polls positive (> 0)
    - min_light_write_query_share finite in [0, 1]
    - max_capacity_normalized_regression_pct finite nonnegative
    """
    # max_failure_rate finite in [0, 1]
    if not math.isfinite(thresholds.max_failure_rate):
        return "gates.max_failure_rate must be finite"
    if not 0 <= thresholds.max_failure_rate <= 1:
        return "gates.max_failure_rate must be in [0, 1]"

    # max_outstanding_requests nonnegative
    if thresholds.max_outstanding_requests < 0:
        return "gates.max_outstanding_requests must be nonnegative"

    # write_share_min/write_share_max finite, ordered in [0,1]
    if not math.isfinite(thresholds.write_share_min):
        return "gates.dual_backlog_lower must be finite"
    if not 0 <= thresholds.write_share_min <= 1:
        return "gates.dual_backlog_lower must be in [0, 1]"
    if not math.isfinite(thresholds.write_share_max):
        return "gates.dual_backlog_upper must be finite"
    if not 0 <= thresholds.write_share_max <= 1:
        return "gates.dual_backlog_upper must be in [0, 1]"
    if not thresholds.write_share_min <= thresholds.write_share_max:
        return (
            "write-share thresholds must be ordered fractions between zero and one"
        )
    # Must contain derived expected_write_share (0.8)
    expected_write_share = 0.8
    if (thresholds.write_share_min > expected_write_share
            or thresholds.write_share_max < expected_write_share):
        return (
            f"gates dual_backlog bounds [{thresholds.write_share_min}, "
            f"{thresholds.write_share_max}] must contain derived "
            f"expected_write_share {expected_write_share}"
        )

    # min_dual_backlog_interval_fraction finite in [0, 1]
    if not math.isfinite(thresholds.min_dual_backlog_interval_fraction):
        return "gates.min_dual_backlog_interval_fraction must be finite"
    if not 0 <= thresholds.min_dual_backlog_interval_fraction <= 1:
        return "gates.min_dual_backlog_interval_fraction must be in [0, 1]"

    # min_dual_backlog_polls_per_class positive (> 0)
    if thresholds.min_dual_backlog_polls_per_class <= 0:
        return "gates.min_dual_backlog_polls_per_class must be positive"

    # single_class_purity_min_share finite in [0, 1]
    if not math.isfinite(thresholds.single_class_purity_min_share):
        return "gates.min_single_class_active_purity must be finite"
    if not 0 <= thresholds.single_class_purity_min_share <= 1:
        return "gates.min_single_class_active_purity must be in [0, 1]"

    # max_active_polls must be positive
    if thresholds.max_active_polls <= 0:
        return "maximum active polls must be a positive configured limit, not zero"

    # min_light_write_query_share finite in [0, 1]
    if not math.isfinite(thresholds.min_light_write_query_share):
        return "gates.min_light_write_query_share must be finite"
    if not 0 <= thresholds.min_light_write_query_share <= 1:
        return "gates.min_light_write_query_share must be in [0, 1]"

    # max_capacity_normalized_regression_pct finite nonnegative
    if not math.isfinite(thresholds.max_capacity_normalized_regression_pct):
        return "gates.max_capacity_normalized_regression_pct must be finite"
    if thresholds.max_capacity_normalized_regression_pct < 0:
        return "gates.max_capacity_normalized_regression_pct must be nonnegative"

    return None


def _evaluation(
    status: str, failures: tuple[str, ...]
) -> SchedulerEvaluation:
    return SchedulerEvaluation(
        status=status,
        passed=status == "passed",
        exit_code=exit_code_for_status(status),
        failures=failures,
    )
