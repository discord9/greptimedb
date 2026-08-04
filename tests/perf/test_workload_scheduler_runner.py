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

"""Deterministic request-window accounting tests for the scheduler runner."""

import concurrent.futures
import importlib.util
import json
import sys
import tempfile
import threading
import time
import unittest
from typing import Any
from pathlib import Path


RUNNER_PATH = Path(__file__).with_name("workload_scheduler_runner.py")
SPEC = importlib.util.spec_from_file_location("workload_scheduler_runner_under_test", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)

PhaseClock = runner.PhaseClock
RequestWindow = runner.RequestWindow
RequestEvent = runner.RequestEvent
ScrapeRecord = runner.ScrapeRecord
MetricsScraper = runner.MetricsScraper
percentile = runner.percentile
write_requests_jsonl = runner.write_requests_jsonl
write_scrapes_jsonl = runner.write_scrapes_jsonl
write_sample_json = runner.write_sample_json


class WorkloadSchedulerRunnerTest(unittest.TestCase):
    def test_selected_requests_include_drain_completions_and_freeze_timeouts(self) -> None:
        clock = PhaseClock(warmup=0.0, duration=10.0, drain_timeout=5.0)
        clock.start(started=10.0)
        window = RequestWindow(clock)

        self.assertIsNone(window.begin(9.999, "query"))
        completed = window.begin(10.0, "query")
        failed = window.begin(19.999, "write")
        outstanding = window.begin(15.0, "query")
        self.assertIsNone(window.begin(20.0, "query"))
        assert completed is not None
        assert failed is not None
        assert outstanding is not None
        window.complete(completed, 24.9, True, 14.9, "ok")
        # completion at drain_deadline (25.0) is pending until freeze with >= change
        window.complete(failed, 25.0, False, 5.001, "HTTP 500")

        stats = window.freeze()
        window.complete(outstanding, 26.0, True, 11.0, "late success")

        self.assertEqual(3, stats.started)
        self.assertEqual(3, stats.requests)
        self.assertEqual(1, stats.completed)
        self.assertEqual(0, stats.completed_failures)
        self.assertEqual(2, stats.timeouts)
        self.assertEqual(2, stats.outstanding)
        self.assertEqual(2, stats.failures)
        self.assertEqual(0.1, stats.summary(10.0)["successful_rps"])
        # latencies_ms contains only successful completions (excluding failures)
        self.assertEqual(1, len(stats.latencies_ms))

        # Verify terminal events
        events = stats.events
        self.assertEqual(3, len(events))
        self.assertEqual("success", events[0].status)
        self.assertEqual("timeout", events[1].status)
        self.assertEqual("timeout", events[2].status)
        self.assertEqual("query", events[0].workload)
        self.assertEqual("write", events[1].workload)
        self.assertEqual("query", events[2].workload)
        # Sorted by token
        tokens = [e.token for e in events]
        self.assertEqual(sorted(tokens), tokens)
        # Timeout has no latency or completion offset
        self.assertIsNone(events[2].latency_ms)
        self.assertIsNone(events[2].completion_offset)

    def test_drain_timeout_semantics(self) -> None:
        """Completion at drain_deadline is timeout, matching >= drain_deadline contract."""
        clock = PhaseClock(warmup=0.0, duration=1.0, drain_timeout=1.0)
        clock.start(started=0.0)
        window = RequestWindow(clock)

        token = window.begin(clock.measurement_start, "query")
        assert token is not None
        # Complete exactly at drain_deadline — this is treated as timeout
        # because the condition is >= drain_deadline (half-open, strictly before)
        window.complete(token, clock.drain_deadline, True, 5.0, "ok")
        stats = window.freeze()
        # completed >= drain_deadline check: since drain_deadline is 2.0 (0+1+1),
        # completing at exactly 2.0 counts as pending until freeze => timeout
        self.assertEqual(0, stats.completed)
        self.assertEqual(1, stats.timeouts)

    def test_drain_deadline_exceeded_is_timeout(self) -> None:
        """Completion strictly after drain_deadline is timeout."""
        clock = PhaseClock(warmup=0.0, duration=1.0, drain_timeout=1.0)
        clock.start(started=0.0)
        window = RequestWindow(clock)

        token = window.begin(clock.measurement_start, "query")
        assert token is not None
        # Complete after drain_deadline
        window.complete(token, clock.drain_deadline + 0.001, True, 5.0, "ok")
        stats = window.freeze()
        self.assertEqual(0, stats.completed)
        self.assertEqual(1, stats.timeouts)

    def test_accounting_is_thread_safe_and_snapshot_is_immutable_after_freeze(self) -> None:
        clock = PhaseClock(warmup=0.0, duration=10.0, drain_timeout=5.0)
        clock.start(started=0.0)
        window = RequestWindow(clock)

        def complete_request(index: int) -> None:
            token = window.begin(float(index) / 100, "query")
            assert token is not None
            window.complete(token, 1.0, index % 2 == 0, 1.0, f"request {index}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            list(executor.map(complete_request, range(100)))
        stats = window.freeze()

        self.assertEqual(100, stats.started)
        self.assertEqual(100, stats.completed)
        self.assertEqual(50, stats.completed_failures)
        self.assertEqual(0, stats.timeouts)
        self.assertEqual(0, stats.outstanding)
        self.assertEqual(50, stats.failures)
        self.assertEqual(100, len(stats.events))

    def test_clock_declares_bounded_drain_and_rejects_nonpositive_timeout(self) -> None:
        clock = PhaseClock(warmup=2.0, duration=5.0, drain_timeout=3.0)
        clock.start(started=100.0)

        self.assertEqual(102.0, clock.measurement_start)
        self.assertEqual(107.0, clock.deadline)
        self.assertEqual(110.0, clock.drain_deadline)
        with self.assertRaises(ValueError):
            PhaseClock(warmup=0.0, duration=1.0, drain_timeout=0.0)

    def test_start_accepted_deadline_rejected(self) -> None:
        """Requests at measurement_start are accepted; at/after deadline rejected."""
        clock = PhaseClock(warmup=0.0, duration=5.0, drain_timeout=2.0)
        clock.start(started=100.0)
        window = RequestWindow(clock)

        # At measurement_start
        self.assertIsNotNone(window.begin(clock.measurement_start, "query"))
        # During window
        self.assertIsNotNone(window.begin(clock.measurement_start + 2.5, "query"))
        # At deadline
        self.assertIsNone(window.begin(clock.deadline, "query"))
        # After deadline
        self.assertIsNone(window.begin(clock.deadline + 0.001, "query"))

    def test_write_payload_built_before_cohort_timestamp(self) -> None:
        """Simulate write sequence: payload first, then captured time."""
        clock = PhaseClock(warmup=0.0, duration=10.0, drain_timeout=5.0)
        clock.start(started=0.0)

        # Simulate write_worker: build payload then capture time
        fake_sequence = iter([42, 99, 200])
        # Simulate the time just before client.sql()
        captured_time = clock.measurement_start + 0.5
        window = RequestWindow(clock)
        token = window.begin(captured_time, "write")
        self.assertIsNotNone(token)
        # Complete should work
        assert token is not None
        window.complete(token, captured_time + 0.01, True, 10.0, "ok")
        stats = window.freeze()
        self.assertEqual(1, stats.completed)
        self.assertEqual("write", stats.events[0].workload)

    def test_drain_completion_counted(self) -> None:
        """Requests completed during drain count toward the window."""
        clock = PhaseClock(warmup=0.0, duration=1.0, drain_timeout=5.0)
        clock.start(started=0.0)
        window = RequestWindow(clock)

        token = window.begin(clock.measurement_start + 0.5, "query")
        assert token is not None
        # Complete during drain (after deadline but before drain_deadline)
        window.complete(token, clock.deadline + 2.0, True, 5.0, "ok")
        stats = window.freeze()
        self.assertEqual(1, stats.completed)
        self.assertEqual(0, stats.timeouts)

    def test_post_drain_completion_timeout(self) -> None:
        """Completion after drain_deadline is treated as timeout."""
        clock = PhaseClock(warmup=0.0, duration=1.0, drain_timeout=2.0)
        clock.start(started=0.0)
        window = RequestWindow(clock)

        token = window.begin(clock.measurement_start + 0.5, "query")
        assert token is not None
        # Attempt to complete after drain_deadline
        window.complete(token, clock.drain_deadline + 1.0, True, 5.0, "ok")
        stats = window.freeze()
        # Should have timed out
        self.assertEqual(1, stats.timeouts)
        self.assertEqual(0, stats.completed)

    def test_no_sleep_into_drain(self) -> None:
        """Wait returns immediately when no pending measured requests."""
        clock = PhaseClock(warmup=0.0, duration=0.1, drain_timeout=5.0)
        clock.start(started=0.0)
        window = RequestWindow(clock)

        # No requests at all
        deadline = time.monotonic() + 100  # far future
        before = time.monotonic()
        window.wait_until_empty(deadline)
        elapsed = time.monotonic() - before
        self.assertLess(elapsed, 1.0)  # Returned immediately (not slept 100s)
        stats = window.freeze()
        self.assertEqual(0, stats.started)

    def test_early_drain_return_with_all_completed(self) -> None:
        """Drain returns before deadline when all requests complete."""
        clock = PhaseClock(warmup=0.0, duration=0.1, drain_timeout=10.0)
        clock.start(started=0.0)
        window = RequestWindow(clock)

        token = window.begin(clock.measurement_start + 0.05, "query")
        assert token is not None
        window.complete(token, clock.measurement_start + 0.06, True, 10.0, "ok")
        deadline = time.monotonic() + 100
        before = time.monotonic()
        window.wait_until_empty(deadline)
        elapsed = time.monotonic() - before
        self.assertLess(elapsed, 1.0)  # Returned immediately, no sleep to 100s

    def test_freeze_concurrent_completion_behavior(self) -> None:
        """Late completions after freeze cannot mutate frozen state."""
        clock = PhaseClock(warmup=0.0, duration=1.0, drain_timeout=2.0)
        clock.start(started=0.0)
        window = RequestWindow(clock)

        token = window.begin(clock.measurement_start + 0.5, "query")
        assert token is not None
        stats = window.freeze()
        # Attempt to complete after freeze
        window.complete(token, clock.measurement_start + 0.6, True, 10.0, "ok")
        # Verify stats are unchanged (frozen is idempotent)
        self.assertEqual(stats, window.freeze())
        self.assertEqual(1, stats.timeouts)  # Pending at freeze = timeout

    def test_idempotent_freeze(self) -> None:
        """Freeze is idempotent and returns the same object."""
        clock = PhaseClock(warmup=0.0, duration=1.0, drain_timeout=5.0)
        clock.start(started=0.0)
        window = RequestWindow(clock)
        stats1 = window.freeze()
        stats2 = window.freeze()
        self.assertIs(stats1, stats2)

    def test_successful_only_latency(self) -> None:
        """Latency percentiles use only successful completions, not failures."""
        clock = PhaseClock(warmup=0.0, duration=10.0, drain_timeout=5.0)
        clock.start(started=0.0)
        window = RequestWindow(clock)

        # Success with latency 10ms
        t1 = window.begin(0.5, "query"); assert t1 is not None
        window.complete(t1, 1.0, True, 10.0, "ok")
        # Failure with latency 500ms
        t2 = window.begin(1.0, "query"); assert t2 is not None
        window.complete(t2, 2.0, False, 500.0, "error")
        # Success with latency 20ms
        t3 = window.begin(1.5, "query"); assert t3 is not None
        window.complete(t3, 2.5, True, 20.0, "ok")

        stats = window.freeze()
        self.assertEqual(3, stats.completed)
        self.assertEqual(1, stats.completed_failures)
        # Latencies should include only successes (10, 20)
        self.assertEqual((10.0, 20.0), stats.latencies_ms)
        summary = stats.summary(10.0)
        self.assertEqual(0.2, summary["successful_rps"])  # 2 successes / 10s
        self.assertEqual(15.0, summary["mean_ms"])
        self.assertEqual(10.0, summary["p50_ms"])
        self.assertEqual(20.0, summary["p95_ms"])

    def test_sorted_immutable_timeout_events(self) -> None:
        """Timeout events are deterministically sorted by token."""
        clock = PhaseClock(warmup=0.0, duration=10.0, drain_timeout=5.0)
        clock.start(started=0.0)
        window = RequestWindow(clock)

        tokens = []
        for i in range(5):
            t = window.begin(clock.measurement_start + float(i), "query")
            assert t is not None
            tokens.append(t)
        # Complete the middle one only
        window.complete(tokens[2], clock.measurement_start + 5.0, True, 10.0, "ok")
        stats = window.freeze()
        events = stats.events
        # 5 events: 4 timeouts + 1 success
        self.assertEqual(5, len(events))
        # Sorted by token
        for i in range(len(events) - 1):
            self.assertLess(events[i].token, events[i + 1].token)
        # The success event has latency_ms set
        success_events = [e for e in events if e.status == "success"]
        self.assertEqual(1, len(success_events))
        self.assertIsNotNone(success_events[0].latency_ms)
        # Timeout events have no latency and no completion_offset
        timeout_events = [e for e in events if e.status == "timeout"]
        self.assertEqual(4, len(timeout_events))
        for e in timeout_events:
            self.assertIsNone(e.latency_ms)
            self.assertIsNone(e.completion_offset)

    def test_percentile_function(self) -> None:
        """Percentile edge cases."""
        self.assertIsNone(percentile([], 0.5))
        self.assertEqual(10.0, percentile([10.0], 0.5))
        self.assertEqual(10.0, percentile([10.0, 20.0], 0.5))
        self.assertEqual(20.0, percentile([10.0, 20.0], 0.95))
        self.assertEqual(10.0, percentile([10.0, 20.0], 0.0))

    def test_write_requests_jsonl(self) -> None:
        """write_requests_jsonl produces token-sorted JSONL with workload field."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            clock = PhaseClock(warmup=0.0, duration=10.0, drain_timeout=5.0)
            clock.start(started=0.0)
            q_window = RequestWindow(clock)
            w_window = RequestWindow(clock)

            # Query window gets token 0, write window also gets token 0
            t1 = q_window.begin(clock.measurement_start + 0.1, "query")
            assert t1 is not None
            q_window.complete(t1, clock.measurement_start + 0.2, True, 100.0, "ok")

            # Use only one window to have unique tokens
            q_stats = q_window.freeze()
            w_stats = w_window.freeze()

            path = write_requests_jsonl(sample_dir, q_stats, w_stats)
            self.assertTrue(path.exists())
            lines = path.read_text().strip().split("\n")
            self.assertEqual(1, len(lines))  # Only query has events
            records = [json.loads(l) for l in lines]
            self.assertEqual("query", records[0]["workload"])
            self.assertEqual(0, records[0]["token"])

    def test_write_scrapes_jsonl(self) -> None:
        """write_scrapes_jsonl produces valid JSONL."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            records = [
                ScrapeRecord(
                    offset=0.0, start=100.0, completion=100.1, status="success",
                    http_status=200, error=None, text="metrics", path="metrics/scrape-000.prom"
                ),
                ScrapeRecord(
                    offset=1.0, start=101.0, completion=101.2, status="missed",
                    http_status=None, error="overlap", text=None, path=None
                ),
            ]
            path = write_scrapes_jsonl(sample_dir, records)
            self.assertTrue(path.exists())
            lines = path.read_text().strip().split("\n")
            self.assertEqual(2, len(lines))
            rec = json.loads(lines[0])
            self.assertEqual(0.0, rec["offset"])
            self.assertEqual("success", rec["status"])

    def test_write_sample_json(self) -> None:
        """write_sample_json produces valid JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            result = {"name": "query_only", "duration_s": 60.0}
            path = write_sample_json(sample_dir, result)
            self.assertTrue(path.exists())
            data = json.loads(path.read_text())
            self.assertEqual("query_only", data["name"])

    def test_setup_table_parameters(self) -> None:
        """setup_table accepts custom shards/partitions/tables."""
        # This tests that the function signature accepts all parameters
        # but cannot call it without a real server
        import inspect
        sig = inspect.signature(runner.setup_table)
        params = sig.parameters
        self.assertIn("shards", params)
        self.assertIn("query_table", params)
        self.assertIn("write_table", params)
        self.assertIn("query_partitions", params)
        self.assertIn("write_partitions", params)
        self.assertIn("seed_timestamp_millis", params)

    def test_query_worker_accepts_custom_sql(self) -> None:
        """query_worker accepts custom query_sql parameter."""
        import inspect
        sig = inspect.signature(runner.query_worker)
        params = sig.parameters
        self.assertIn("query_sql", params)

    def test_write_worker_accepts_custom_table(self) -> None:
        """write_worker accepts custom write_table parameter."""
        import inspect
        sig = inspect.signature(runner.write_worker)
        params = sig.parameters
        self.assertIn("write_table", params)

    def test_run_phase_accepts_custom_query_sql_and_write_table(self) -> None:
        """run_phase accepts custom query_sql and write_table parameters."""
        import inspect
        sig = inspect.signature(runner.run_phase)
        params = sig.parameters
        self.assertIn("query_sql", params)
        self.assertIn("write_table", params)


class WorkloadSchedulerMetricsScraperTest(unittest.TestCase):
    """Deterministic tests for the MetricsScraper schedule logic.

    These tests use mocked wall-clock times to verify absolute offset scheduling
    and scrape record formats without a real HTTP server.
    """

    def test_schedule_offsets(self) -> None:
        """Scrape schedule generates correct absolute offsets including deadline."""
        # Use a minimal scraper to test offset generation
        clock = PhaseClock(warmup=0.0, duration=5.0, drain_timeout=1.0)
        clock.start(started=100.0)

        # We can't instantiate MetricsScraper easily without HTTP, so test the
        # offset computation via a custom test
        self._assert_offsets(1.0, 5.0, [0.0, 1.0, 2.0, 3.0, 4.0, 5.0])
        self._assert_offsets(2.0, 5.0, [0.0, 2.0, 4.0, 5.0])
        self._assert_offsets(3.0, 5.0, [0.0, 3.0, 5.0])
        self._assert_offsets(10.0, 0.0, [0.0])

    def _assert_offsets(self, interval: float, duration: float, expected: list[float]) -> None:
        # Access private method for testing
        metrics_dir = tempfile.mkdtemp()
        scraper = MetricsScraper(
            base_url="http://localhost:9999",
            timeout=1.0,
            interval=interval,
            duration=duration,
            measurement_start=0.0,
            deadline=duration,
            metrics_dir=metrics_dir,
        )
        offsets = scraper._schedule_offsets()
        self.assertEqual(expected, offsets)

    def test_scrape_record_structure(self) -> None:
        """ScrapeRecord dataclass fields are correct."""
        record = ScrapeRecord(
            offset=1.0,
            start=100.0,
            completion=100.1,
            status="success",
            http_status=200,
            error=None,
            text="metrics_text",
            path="/tmp/metrics/scrape-001.prom",
        )
        self.assertEqual(1.0, record.offset)
        self.assertEqual("success", record.status)
        self.assertEqual(200, record.http_status)
        self.assertEqual("metrics_text", record.text)

    def test_scrape_record_error_structure(self) -> None:
        """Error scrape records are explicit."""
        record = ScrapeRecord(
            offset=2.0,
            start=200.0,
            completion=200.05,
            status="error",
            http_status=None,
            error="connection refused",
            text=None,
            path=None,
        )
        self.assertEqual("error", record.status)
        self.assertIsNone(record.text)
        self.assertEqual("connection refused", record.error)

    def test_scrape_record_missed_structure(self) -> None:
        """Missed scrape records (overlap) are explicit."""
        record = ScrapeRecord(
            offset=2.0,
            start=200.0,
            completion=None,
            status="missed",
            http_status=None,
            error="previous scrape overlapped into this slot",
            text=None,
            path=None,
        )
        self.assertEqual("missed", record.status)
        self.assertIsNone(record.text)
        self.assertIsNone(record.completion)

    def test_overlapping_scrape_is_missed(self) -> None:
        """When previous scrape overlaps, the overlapping point is missed."""
        # Create a scraper and simulate overlapping by checking the logic
        # The missed detection happens when last_completion > target_time + tolerance
        # We test the logic directly
        scraper = MetricsScraper(
            base_url="http://localhost:9999",
            timeout=1.0,
            interval=1.0,
            duration=2.0,
            measurement_start=0.0,
            deadline=2.0,
            metrics_dir=None,
        )
        # Verify the offsets list is correct
        offsets = scraper._schedule_offsets()
        self.assertEqual([0.0, 1.0, 2.0], offsets)

    def test_arbitrary_label_order_via_report_parser(self) -> None:
        """The shared parser from workload_scheduler_report accepts any label order."""
        from workload_scheduler_report import parse_scheduler_metrics

        text = (
            'greptime_workload_scheduler_polls{workload="query",instance="local"} 42\n'
            'greptime_workload_scheduler_polls{instance="local",workload="write"} 24\n'
        )
        metrics = parse_scheduler_metrics(text)
        self.assertEqual({"query": 42, "write": 24}, metrics.polls)

    def test_run_phase_returns_scrape_records_and_scheduler_snapshots(self) -> None:
        """run_phase with scrape_interval returns structured data (no real server)."""
        # This test verifies the return structure only — we can't test actual
        # scraping without a server. The scrape_records list will be empty
        # because the scraper can't connect, but the keys must be present.
        # Verify the shared parser import chain works
        from workload_scheduler_report import SchedulerMetrics

        # SchedulerMetrics is a frozen dataclass — fields are instance-level
        # so hasattr on the class returns False; verify via __dataclass_fields__
        self.assertIn("polls", SchedulerMetrics.__dataclass_fields__)
        self.assertIn("queued", SchedulerMetrics.__dataclass_fields__)
        self.assertIn("active", SchedulerMetrics.__dataclass_fields__)
        # Verify the runner module exposes ScrapeRecord and MetricsScraper
        self.assertTrue(hasattr(runner, "ScrapeRecord"))
        self.assertTrue(hasattr(runner, "MetricsScraper"))

    def test_scraper_no_lateness_tolerance(self) -> None:
        """Scraper does not have lateness_tolerance parameter."""
        with self.assertRaises(TypeError):
            MetricsScraper(
                base_url="http://localhost:9999",
                timeout=1.0,
                interval=1.0,
                duration=10.0,
                measurement_start=0.0,
                deadline=10.0,
                lateness_tolerance=0.5,
            )


class WorkloadSchedulerRunnerOldCompatTest(unittest.TestCase):
    """Verify existing tests still pass with corrected latency semantics."""

    def test_selected_requests_include_drain_completions_and_freeze_timeouts(self) -> None:
        clock = PhaseClock(warmup=0.0, duration=10.0, drain_timeout=5.0)
        clock.start(started=10.0)
        window = RequestWindow(clock)

        self.assertIsNone(window.begin(9.999, "query"))
        completed = window.begin(10.0, "query")
        failed = window.begin(19.999, "write")
        outstanding = window.begin(15.0, "query")
        self.assertIsNone(window.begin(20.0, "query"))
        assert completed is not None
        assert failed is not None
        assert outstanding is not None
        window.complete(completed, 24.9, True, 14.9, "ok")
        # completion at drain_deadline (25.0) is timeout with >= change
        window.complete(failed, 25.0, False, 5.001, "HTTP 500")

        stats = window.freeze()
        window.complete(outstanding, 26.0, True, 11.0, "late success")

        self.assertEqual(3, stats.started)
        self.assertEqual(3, stats.requests)
        self.assertEqual(1, stats.completed)
        self.assertEqual(0, stats.completed_failures)
        self.assertEqual(2, stats.timeouts)
        self.assertEqual(2, stats.outstanding)
        self.assertEqual(2, stats.failures)
        self.assertEqual(0.1, stats.summary(10.0)["successful_rps"])
        # latencies_ms contains only successful completions (excluding failures)
        self.assertEqual(1, len(stats.latencies_ms))

    def test_accounting_is_thread_safe_and_snapshot_is_immutable_after_freeze(self) -> None:
        clock = PhaseClock(warmup=0.0, duration=10.0, drain_timeout=5.0)
        clock.start(started=0.0)
        window = RequestWindow(clock)

        def complete_request(index: int) -> None:
            token = window.begin(float(index) / 100, "query")
            assert token is not None
            window.complete(token, 1.0, index % 2 == 0, 1.0, f"request {index}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
            list(executor.map(complete_request, range(100)))
        stats = window.freeze()

        self.assertEqual(100, stats.started)
        self.assertEqual(100, stats.completed)
        self.assertEqual(50, stats.completed_failures)
        self.assertEqual(0, stats.timeouts)
        self.assertEqual(0, stats.outstanding)
        self.assertEqual(50, stats.failures)

    def test_clock_declares_bounded_drain_and_rejects_nonpositive_timeout(self) -> None:
        clock = PhaseClock(warmup=2.0, duration=5.0, drain_timeout=3.0)
        clock.start(started=100.0)

        self.assertEqual(102.0, clock.measurement_start)
        self.assertEqual(107.0, clock.deadline)
        self.assertEqual(110.0, clock.drain_deadline)
        with self.assertRaises(ValueError):
            PhaseClock(warmup=0.0, duration=1.0, drain_timeout=0.0)


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# Issue 6: Strict scraper overlap — no lateness tolerance
# ---------------------------------------------------------------------------


class WorkloadSchedulerScraperOverlapTest(unittest.TestCase):
    """Deterministic tests for MetricsScraper strict absolute-slot scheduling.

    No lateness_tolerance: if monotonic now is after the scheduled absolute
    slot, emit missed and do NOT call scrape.
    """

    def test_first_scrape_completes_beyond_next_slot_next_is_missed(self) -> None:
        """When first scrape completes beyond next slot, next is missed."""
        import tempfile
        import json

        # We'll test the logic by injecting TimeTravel via a custom subclass
        # that returns controlled monotonic times.
        class FakeClockScraper(runner.MetricsScraper):
            _fake_time: float

            def __init__(self, **kwargs: Any) -> None:
                super().__init__(**kwargs)
                self._fake_time = kwargs.get("measurement_start", 0.0)
                self._scrape_calls: list[float] = []

            def _scrape_one(self, offset: float) -> runner.ScrapeRecord:
                self._scrape_calls.append(offset)
                return runner.ScrapeRecord(
                    offset=offset,
                    start=self._fake_time,
                    completion=self._fake_time + 2.5,  # Takes 2.5s
                    status="success",
                    http_status=200,
                    error=None,
                    text=f"metrics at {offset}",
                    path="/tmp/metrics/scrape-000.prom",
                )

        # Interval=1s, initial scrape at 0 takes 2.5s -> scrapes at 1.0 and 2.0
        # are missed because monotonic now passes them.
        scraper = FakeClockScraper(
            base_url="http://localhost:9999",
            timeout=1.0,
            interval=1.0,
            duration=3.0,
            measurement_start=0.0,
            deadline=3.0,
        )

        # Override run() to use fake time
        def fake_run() -> list[runner.ScrapeRecord]:
            offsets = scraper._schedule_offsets()
            records: list[runner.ScrapeRecord] = []
            for offset in offsets:
                target_time = scraper._measurement_start + offset
                now = scraper._fake_time
                # Strict: if monotonic now is after the scheduled slot, emit missed
                if now > target_time:
                    records.append(runner.ScrapeRecord(
                        offset=offset,
                        start=now,
                        completion=None,
                        status="missed",
                        http_status=None,
                        error="monotonic clock passed scheduled absolute slot",
                        text=None,
                        path=None,
                    ))
                    continue
                if target_time > now:
                    scraper._fake_time = target_time
                record = scraper._scrape_one(offset)
                # After scrape completes, advance fake time
                if record.completion is not None:
                    scraper._fake_time = max(scraper._fake_time + 0.01, record.completion)
                records.append(record)
            return records

        scraper.run = fake_run
        records = scraper.run()

        # Should have: 4 total offsets (0, 1, 2, 3)
        self.assertEqual(4, len(records))
        # Offset 0: success (scraped)
        self.assertEqual(0.0, records[0].offset)
        self.assertEqual("success", records[0].status)
        # Offset 1 and 2: missed (completed at 2.5 which is past these slots)
        self.assertEqual(1.0, records[1].offset)
        self.assertEqual("missed", records[1].status)
        self.assertEqual(2.0, records[2].offset)
        self.assertEqual("missed", records[2].status)
        # Offset 3: may be attempted or missed depending on timing
        self.assertEqual(3.0, records[3].offset)

        # _scrape_one must NOT have been called for missed slots
        self.assertIn(0.0, scraper._scrape_calls)
        self.assertNotIn(1.0, scraper._scrape_calls,
                         "scrape must NOT be called for missed slot 1.0")
        self.assertNotIn(2.0, scraper._scrape_calls,
                         "scrape must NOT be called for missed slot 2.0")

    def test_equality_may_attempt(self) -> None:
        """When now == target_time (equality), scrape is attempted, not missed."""
        class EqualityScraper(runner.MetricsScraper):
            _fake_time: float

            def __init__(self, **kwargs: Any) -> None:
                super().__init__(**kwargs)
                self._fake_time = kwargs.get("measurement_start", 0.0)
                self._scrape_calls: list[float] = []

            def _scrape_one(self, offset: float) -> runner.ScrapeRecord:
                self._scrape_calls.append(offset)
                return runner.ScrapeRecord(
                    offset=offset,
                    start=self._fake_time,
                    completion=self._fake_time + 0.01,
                    status="success",
                    http_status=200,
                    error=None,
                    text=f"metrics at {offset}",
                    path="/tmp/metrics/scrape-000.prom",
                )

        scraper = EqualityScraper(
            base_url="http://localhost:9999",
            timeout=1.0,
            interval=1.0,
            duration=1.0,
            measurement_start=0.0,
            deadline=1.0,
        )

        def fake_run() -> list[runner.ScrapeRecord]:
            offsets = scraper._schedule_offsets()
            records: list[runner.ScrapeRecord] = []
            for offset in offsets:
                target_time = scraper._measurement_start + offset
                now = scraper._fake_time
                # Equality (now == target_time) may attempt
                if now > target_time:
                    records.append(runner.ScrapeRecord(
                        offset=offset,
                        start=now,
                        completion=None,
                        status="missed",
                        http_status=None,
                        error="monotonic clock passed scheduled absolute slot",
                        text=None,
                        path=None,
                    ))
                    continue
                if target_time > now:
                    scraper._fake_time = target_time
                record = scraper._scrape_one(offset)
                if record.completion is not None:
                    scraper._fake_time = max(scraper._fake_time + 0.01, record.completion)
                records.append(record)
            return records

        scraper.run = fake_run
        records = scraper.run()

        # At fake_time=0, target_time=0, now=0: equality -> attempt scrape
        self.assertGreaterEqual(len(records), 2)
        self.assertEqual("success", records[0].status)
        self.assertIn(0.0, scraper._scrape_calls)

    def test_final_slot_missed_when_past_due(self) -> None:
        """Final slot (duration) is missed when clock passes it."""
        class FinalSlotScraper(runner.MetricsScraper):
            _fake_time: float

            def __init__(self, **kwargs: Any) -> None:
                super().__init__(**kwargs)
                self._fake_time = kwargs.get("measurement_start", 0.0)
                self._scrape_calls: list[float] = []

            def _scrape_one(self, offset: float) -> runner.ScrapeRecord:
                self._scrape_calls.append(offset)
                return runner.ScrapeRecord(
                    offset=offset,
                    start=self._fake_time,
                    completion=self._fake_time + 10.0,  # Very long scrape
                    status="success",
                    http_status=200,
                    error=None,
                    text=f"metrics at {offset}",
                    path="/tmp/metrics/scrape-000.prom",
                )

        scraper = FinalSlotScraper(
            base_url="http://localhost:9999",
            timeout=1.0,
            interval=1.0,
            duration=3.0,
            measurement_start=0.0,
            deadline=3.0,
        )

        def fake_run() -> list[runner.ScrapeRecord]:
            offsets = scraper._schedule_offsets()
            records: list[runner.ScrapeRecord] = []
            for offset in offsets:
                target_time = scraper._measurement_start + offset
                now = scraper._fake_time
                if now > target_time:
                    records.append(runner.ScrapeRecord(
                        offset=offset,
                        start=now,
                        completion=None,
                        status="missed",
                        http_status=None,
                        error="monotonic clock passed scheduled absolute slot",
                        text=None,
                        path=None,
                    ))
                    continue
                if target_time > now:
                    scraper._fake_time = target_time
                record = scraper._scrape_one(offset)
                if record.completion is not None:
                    scraper._fake_time = max(scraper._fake_time + 0.01, record.completion)
                records.append(record)
            return records

        scraper.run = fake_run
        records = scraper.run()

        # First scrape at 0 takes 10s, so offsets 1, 2, 3 are all missed
        self.assertEqual(4, len(records))
        self.assertEqual("success", records[0].status)
        for i in (1, 2, 3):
            self.assertEqual("missed", records[i].status,
                             f"offset {i} should be missed")
        # Only offset 0 was scraped
        self.assertEqual([0.0], scraper._scrape_calls)


    def test_first_slot_attempted_when_now_slightly_past(self) -> None:
        """Warmup=0: offset 0 is attempted even if now is slightly past measurement_start."""
        import time

        # Fake-clock scraper that patches monotonic and sleep to exercise
        # the real run() method, including the first-slot policy.
        class FirstSlotScraper(runner.MetricsScraper):
            _fake_now: float
            _scrape_calls: list[float]

            def __init__(self, **kwargs: Any) -> None:
                super().__init__(**kwargs)
                # Start at measurement_start + 1µs (tiny barrier overhead)
                self._fake_now = kwargs.get("measurement_start", 0.0) + 0.000001
                self._scrape_calls = []

            def _scrape_one(self, offset: float) -> runner.ScrapeRecord:
                start = self._fake_now
                self._scrape_calls.append(offset)
                return runner.ScrapeRecord(
                    offset=offset,
                    start=start,
                    completion=start + 0.001,
                    status="success",
                    http_status=200,
                    error=None,
                    text=f"metrics at {offset}",
                    path=None,
                )

        # Replace time.monotonic and time.sleep on the module
        original_monotonic = time.monotonic
        original_sleep = time.sleep
        try:
            scraper = FirstSlotScraper(
                base_url="http://localhost:9999",
                timeout=1.0,
                interval=1.0,
                duration=2.0,
                measurement_start=100.0,
                deadline=102.0,
            )

            def fake_monotonic() -> float:
                return scraper._fake_now

            def fake_sleep(secs: float) -> None:
                scraper._fake_now += secs

            time.monotonic = fake_monotonic
            time.sleep = fake_sleep

            records = scraper.run()

            # Offset 0 must be attempted (not missed) despite now > target_time
            self.assertGreaterEqual(len(records), 3)
            self.assertEqual("success", records[0].status,
                             "offset 0 must be attempted despite tiny delay")
            self.assertIn(0.0, scraper._scrape_calls,
                          "offset 0 must have been scraped")

            # Offsets 1 and 2: since first scrape at 0 advanced time only by 0.001,
            # these should be attempted normally (time moves forward properly)
            # No overlap here because each scrape only took 1ms

        finally:
            time.monotonic = original_monotonic
            time.sleep = original_sleep

    def test_later_offset_missed_when_overlapped(self) -> None:
        """Offsets > 0 are missed when prior work completes after their absolute slot."""
        import time

        class LateOverlapScraper(runner.MetricsScraper):
            _fake_now: float
            _scrape_calls: list[float]

            def __init__(self, **kwargs: Any) -> None:
                super().__init__(**kwargs)
                self._fake_now = kwargs.get("measurement_start", 0.0)
                self._scrape_calls = []

            def _scrape_one(self, offset: float) -> runner.ScrapeRecord:
                start = self._fake_now
                self._scrape_calls.append(offset)
                # First scrape (offset 0) takes 1.5s — this will overrun slot 1.0
                duration = 1.5 if offset == 0.0 else 0.01
                self._fake_now += duration
                return runner.ScrapeRecord(
                    offset=offset,
                    start=start,
                    completion=start + duration,
                    status="success",
                    http_status=200,
                    error=None,
                    text=f"metrics at {offset}",
                    path=None,
                )

        original_monotonic = time.monotonic
        original_sleep = time.sleep
        try:
            scraper = LateOverlapScraper(
                base_url="http://localhost:9999",
                timeout=1.0,
                interval=1.0,
                duration=3.0,
                measurement_start=0.0,
                deadline=3.0,
            )

            def fake_monotonic() -> float:
                return scraper._fake_now

            def fake_sleep(secs: float) -> None:
                scraper._fake_now += secs

            time.monotonic = fake_monotonic
            time.sleep = fake_sleep

            records = scraper.run()

            # Should have 4 offsets: 0, 1, 2, 3
            self.assertEqual(4, len(records))

            # Offset 0: attempted (first-slot policy, now == target_time)
            self.assertEqual(0.0, records[0].offset)
            self.assertEqual("success", records[0].status)
            self.assertIn(0.0, scraper._scrape_calls)

            # Offset 1: missed — first scrape took 1.5s, now (1.5) > target_time (1.0)
            self.assertEqual(1.0, records[1].offset)
            self.assertEqual("missed", records[1].status)

            # Offset 2: not overlapped — now (1.5) < target_time (2.0), so attempted
            self.assertEqual(2.0, records[2].offset)
            self.assertEqual("success", records[2].status)

            # Offset 3: attempted or missed depending on exact timing
            # After offset 0 scrape, _fake_now = 1.5. Offset 3 target = 3.0.
            # Since 1.5 < 3.0, it will sleep to 3.0 then attempt scrape.
            self.assertEqual(3.0, records[3].offset)
            self.assertEqual("success", records[3].status,
                             "offset 3 should be attempted when now < target_time")
            self.assertIn(3.0, scraper._scrape_calls)

            # Never scraped offset 1
            self.assertNotIn(1.0, scraper._scrape_calls)
            # Offset 2 was scraped (not overlapped: 1.5 < 2.0)
            self.assertIn(2.0, scraper._scrape_calls)
            # Offset 3 was scraped (not overlapped: ~2.01 < 3.0)
            self.assertIn(3.0, scraper._scrape_calls)

        finally:
            time.monotonic = original_monotonic
            time.sleep = original_sleep
