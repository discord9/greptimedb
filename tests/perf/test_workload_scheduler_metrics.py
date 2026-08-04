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

"""Deterministic tests for workload-scheduler Prometheus metrics."""

import importlib.util
import sys
import unittest
from pathlib import Path


REPORT_PATH = Path(__file__).with_name("workload_scheduler_report.py")
SPEC = importlib.util.spec_from_file_location(
    "workload_scheduler_report_metrics", REPORT_PATH
)
assert SPEC is not None and SPEC.loader is not None
report = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = report
SPEC.loader.exec_module(report)

SchedulerMetrics = report.SchedulerMetrics
interval_delta = report.interval_delta
parse_scheduler_metrics = report.parse_scheduler_metrics
summarize_scheduler_metrics = report.summarize_scheduler_metrics


class WorkloadSchedulerMetricsTest(unittest.TestCase):
    def test_parser_accepts_label_order_and_ignores_unrelated_metrics(self) -> None:
        metrics = parse_scheduler_metrics(
            """\
unrelated_metric{workload="write"} 999
greptime_workload_scheduler_polls{instance="local",workload="query"} 12
greptime_workload_scheduler_queued_tasks{workload="write",instance="local"} 4
greptime_workload_scheduler_active_polls 3
greptime_workload_scheduler_polls{workload="write",job="greptime"} 48
greptime_workload_scheduler_queued_tasks{job="greptime",workload="query"} 2
"""
        )

        self.assertEqual({"query": 12, "write": 48}, metrics.polls)
        self.assertEqual({"query": 2, "write": 4}, metrics.queued)
        self.assertEqual(3, metrics.active)

    def test_interval_delta_marks_counter_resets(self) -> None:
        before = SchedulerMetrics(
            polls={"query": 100, "write": 50},
            queued={"query": 1, "write": 1},
            active=3,
        )
        after = SchedulerMetrics(
            polls={"query": 2, "write": 60},
            queued={"query": 1, "write": 1},
            active=4,
        )

        interval = interval_delta(before, after)

        self.assertTrue(interval.counter_reset)
        self.assertIsNone(interval.polls)
        self.assertFalse(interval.strict_dual_backlog)

    def test_strict_dual_backlog_summary_stays_distinct_from_whole_window(
        self,
    ) -> None:
        snapshots = [
            SchedulerMetrics(
                {"query": 0, "write": 0}, {"query": 1, "write": 1}, 4
            ),
            SchedulerMetrics(
                {"query": 2, "write": 8}, {"query": 1, "write": 1}, 4
            ),
            SchedulerMetrics(
                {"query": 4, "write": 16}, {"query": 1, "write": 1}, 4
            ),
            SchedulerMetrics(
                {"query": 6, "write": 24}, {"query": 1, "write": 1}, 4
            ),
            SchedulerMetrics(
                {"query": 8, "write": 32}, {"query": 1, "write": 1}, 4
            ),
            SchedulerMetrics(
                {"query": 10, "write": 90}, {"query": 0, "write": 1}, 4
            ),
        ]

        summary = summarize_scheduler_metrics(snapshots)

        self.assertEqual(0.90, summary.whole_window_write_share)
        self.assertEqual(0.80, summary.dual_backlog_write_share)
        self.assertEqual(0.80, summary.dual_backlog_interval_fraction)
        self.assertEqual(4, summary.dual_backlog_interval_count)

    def test_integer_metric_rejects_non_integer_values(self) -> None:
        """Non-integer Prometheus metric values are rejected."""
        metrics = parse_scheduler_metrics(
            'greptime_workload_scheduler_polls{workload="query"} 12.5\n'
        )
        # Parsing silently fails because 12.5 is not an integer
        self.assertEqual({}, metrics.polls)

    def test_missing_active_metric(self) -> None:
        """Without active metric, active is None."""
        metrics = parse_scheduler_metrics(
            'greptime_workload_scheduler_polls{workload="query"} 12\n'
        )
        self.assertIsNone(metrics.active)

    def test_malformed_labels_rejected(self) -> None:
        """Malformed labels return empty or None."""
        metrics = parse_scheduler_metrics(
            'greptime_workload_scheduler_polls{workload="query",bad} 12\n'
        )
        # Bad label syntax means the line is rejected
        self.assertEqual({}, metrics.polls)

    def test_active_with_labels_ignored(self) -> None:
        """Active metric with labels is ignored (unlabeled one preferred)."""
        text = (
            'greptime_workload_scheduler_active_polls{instance="local"} 5\n'
            "greptime_workload_scheduler_active_polls 3\n"
        )
        metrics = parse_scheduler_metrics(text)
        self.assertEqual(3, metrics.active)

    def test_complete_method(self) -> None:
        """complete() returns True when all required fields present."""
        m = SchedulerMetrics(
            polls={"query": 1, "write": 2},
            queued={"query": 3, "write": 4},
            active=5,
        )
        self.assertTrue(m.complete())

        missing_polls = SchedulerMetrics(
            polls={"query": 1}, queued={"write": 4}, active=5
        )
        self.assertFalse(missing_polls.complete())

        missing_active = SchedulerMetrics(
            polls={"query": 1, "write": 2},
            queued={"query": 3, "write": 4},
            active=None,
        )
        self.assertFalse(missing_active.complete())


if __name__ == "__main__":
    unittest.main()
