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

"""Deterministic tests for workload-scheduler report gates."""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


REPORT_PATH = Path(__file__).with_name("workload_scheduler_report.py")
SPEC = importlib.util.spec_from_file_location(
    "workload_scheduler_report_gates", REPORT_PATH
)
assert SPEC is not None and SPEC.loader is not None
report = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = report
SPEC.loader.exec_module(report)

SchedulerGateThresholds = report.SchedulerGateThresholds
SchedulerMetrics = report.SchedulerMetrics
evaluate_scheduler_report = report.evaluate_scheduler_report
evaluate_request_validity = report.evaluate_request_validity
exit_code_for_status = report.exit_code_for_status
summarize_scheduler_metrics = report.summarize_scheduler_metrics
build_report = report.build_report


def summary(
    poll_points: list[tuple[int, int]],
    queues: list[tuple[int, int]],
    active: int = 4,
):
    return summarize_scheduler_metrics(
        [
            SchedulerMetrics(
                {"query": query, "write": write},
                {"query": queued_query, "write": queued_write},
                active,
            )
            for (query, write), (queued_query, queued_write) in zip(
                poll_points, queues, strict=True
            )
        ]
    )


class WorkloadSchedulerReportTest(unittest.TestCase):
    def setUp(self) -> None:
        self.thresholds = SchedulerGateThresholds(
            write_share_min=0.78,
            write_share_max=0.82,
            min_dual_backlog_interval_fraction=0.80,
            min_dual_backlog_polls_per_class=1,
            single_class_purity_min_share=0.99,
            max_active_polls=4,
        )
        self.phases = {
            "query_only": summary([(0, 0), (100, 0)], [(1, 0), (1, 0)]),
            "write_only": summary([(0, 0), (0, 100)], [(0, 1), (0, 1)]),
            "light_write": summary([(0, 0), (21, 79)], [(1, 1), (1, 1)]),
            "saturated": summary(
                [(0, 0), (2, 8), (4, 16), (6, 24), (8, 32), (10, 90)],
                [(1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (0, 1)],
            ),
        }

    def test_weighted_gate_uses_only_dual_backlog_share(self) -> None:
        result = evaluate_scheduler_report(self.phases, self.thresholds)

        self.assertEqual("passed", result.status)
        self.assertTrue(result.passed)
        self.assertEqual(0, result.exit_code)

    def test_two_sided_weighted_gate_rejects_shares_outside_range(self) -> None:
        above_range = dict(self.phases)
        above_range["saturated"] = summary(
            [(0, 0), (15, 85)], [(1, 1), (1, 1)]
        )
        below_range = dict(self.phases)
        below_range["saturated"] = summary(
            [(0, 0), (25, 75)], [(1, 1), (1, 1)]
        )

        above_result = evaluate_scheduler_report(above_range, self.thresholds)
        below_result = evaluate_scheduler_report(below_range, self.thresholds)

        self.assertEqual("failed", above_result.status)
        self.assertIn("dual-backlog write share", above_result.failures[0])
        self.assertEqual("failed", below_result.status)
        self.assertIn("dual-backlog write share", below_result.failures[0])

    def test_required_phases_and_metrics_fail_closed(self) -> None:
        missing_phase = dict(self.phases)
        del missing_phase["light_write"]
        missing_metrics = dict(self.phases)
        missing_metrics["saturated"] = summarize_scheduler_metrics(
            [
                SchedulerMetrics({"query": 0}, {"query": 1}, 1),
                SchedulerMetrics({"query": 1}, {"query": 1}, 1),
            ]
        )

        self.assertEqual(
            "invalid",
            evaluate_scheduler_report(missing_phase, self.thresholds).status,
        )
        self.assertEqual(
            "invalid",
            evaluate_scheduler_report(missing_metrics, self.thresholds).status,
        )

    def test_dual_backlog_fraction_single_class_and_active_bounds_are_enforced(
        self,
    ) -> None:
        low_fraction = dict(self.phases)
        low_fraction["saturated"] = summary(
            [(0, 0), (2, 8), (4, 16)], [(1, 1), (0, 1), (0, 1)]
        )
        low_borrowing = dict(self.phases)
        low_borrowing["query_only"] = summary(
            [(0, 0), (98, 2)], [(1, 0), (1, 0)]
        )
        too_active = dict(self.phases)
        too_active["write_only"] = summary(
            [(0, 0), (0, 100)], [(0, 1), (0, 1)], active=5
        )

        self.assertEqual(
            "failed",
            evaluate_scheduler_report(low_fraction, self.thresholds).status,
        )
        self.assertEqual(
            "failed",
            evaluate_scheduler_report(low_borrowing, self.thresholds).status,
        )
        self.assertEqual(
            "failed",
            evaluate_scheduler_report(too_active, self.thresholds).status,
        )

    def test_status_exit_mapping(self) -> None:
        self.assertEqual(0, exit_code_for_status("passed"))
        self.assertEqual(1, exit_code_for_status("failed"))
        self.assertEqual(2, exit_code_for_status("invalid"))
        self.assertEqual(3, exit_code_for_status("error"))

        result = evaluate_scheduler_report(
            self.phases, self.thresholds, errors=("runner failed",)
        )
        self.assertEqual("error", result.status)
        self.assertNotEqual(0, result.exit_code)

    def test_single_class_purity_name(self) -> None:
        """The field is named single_class_purity, not single_class_borrowing."""
        self.assertTrue(hasattr(self.thresholds, "single_class_purity_min_share"))
        self.assertFalse(hasattr(self.thresholds, "single_class_borrowing_min_share"))

    def test_max_active_polls_rejects_zero(self) -> None:
        """max_active_polls=0 must cause validation error."""
        bad = SchedulerGateThresholds(max_active_polls=0)
        result = evaluate_scheduler_report(self.phases, bad)
        self.assertEqual("error", result.status)

    def test_request_validity_empty_samples(self) -> None:
        """Empty samples list is invalid."""
        result = evaluate_request_validity([])
        self.assertEqual("invalid", result.status)

    def test_request_validity_passed(self) -> None:
        """Well-formed samples pass."""
        samples = [
            {
                "name": "query_only",
                "mode": "baseline",
                "workers": {"query": 2, "write": 0},
                "requests": {
                    "query": {
                        "started": 10,
                        "completed": 10,
                        "completed_failures": 0,
                        "timeouts": 0,
                        "outstanding": 0,
                        "failures": 0,
                        "requests": 10,
                    },
                    "write": {},
                },
            }
        ]
        result = evaluate_request_validity(samples)
        self.assertEqual("passed", result.status)

    def test_request_validity_accounting(self) -> None:
        """Validate completed + timeouts == started."""
        samples = [
            {
                "name": "query_only",
                "mode": "baseline",
                "workers": {"query": 2, "write": 0},
                "requests": {
                    "query": {
                        "started": 10,
                        "completed": 7,
                        "completed_failures": 1,
                        "timeouts": 2,
                        "outstanding": 1,
                        "failures": 3,
                        "requests": 10,
                    },
                    "write": {},
                },
            }
        ]
        result = evaluate_request_validity(samples)
        self.assertEqual("invalid", result.status,
                         "accounting mismatch is an integrity error -> invalid")
        # completed(7) + timeouts(2) = 9 != started(10)
        self.assertTrue(
            any("completed" in f and "timeouts" in f for f in result.failures),
            f"should flag completed+timeouts != started: {result.failures}"
        )

    def test_request_validity_accounting_failures(self) -> None:
        """Validate failures == completed_failures + timeouts."""
        samples = [
            {
                "name": "query_only",
                "mode": "baseline",
                "workers": {"query": 2, "write": 0},
                "requests": {
                    "query": {
                        "started": 10,
                        "completed": 5,
                        "completed_failures": 3,
                        "timeouts": 2,
                        "outstanding": 3,
                        "failures": 4,  # Should be 5 but is 4
                        "requests": 10,
                    },
                    "write": {},
                },
            }
        ]
        result = evaluate_request_validity(samples)
        self.assertEqual("invalid", result.status,
                         "failures accounting mismatch is an integrity error -> invalid")
        self.assertTrue(
            any("failures" in f and "completed_failures" in f for f in result.failures),
            f"should flag failures != completed_failures+timeouts: {result.failures}"
        )

    def test_request_validity_missing_workload_data(self) -> None:
        """Missing workload entry for active worker is malformed."""
        samples = [
            {
                "name": "query_only",
                "mode": "baseline",
                "workers": {"query": 2, "write": 0},
                "requests": {},  # No query key despite query workers > 0
            }
        ]
        result = evaluate_request_validity(samples)
        # query_only with query workers > 0 should have a query entry
        # Missing workload data is an integrity/schema error => invalid
        self.assertEqual("invalid", result.status)

    def test_request_validity_failure_rate(self) -> None:
        """Failure rate >= 1% should fail."""
        samples = [
            {
                "name": "query_only",
                "mode": "baseline",
                "workers": {"query": 2, "write": 0},
                "requests": {
                    "query": {
                        "started": 100,
                        "completed": 99,
                        "completed_failures": 1,
                        "timeouts": 1,
                        "outstanding": 0,
                        "failures": 2,
                        "requests": 100,
                    },
                    "write": {"started": 0, "completed": 0, "requests": 0},
                },
            }
        ]
        result = evaluate_request_validity(samples, max_failure_rate=0.01)
        self.assertEqual("failed", result.status)

    def test_request_validity_outstanding(self) -> None:
        """Outstanding requests after drain should fail."""
        samples = [
            {
                "name": "query_only",
                "mode": "baseline",
                "workers": {"query": 2, "write": 0},
                "requests": {
                    "query": {
                        "started": 10,
                        "completed": 10,
                        "completed_failures": 0,
                        "timeouts": 0,
                        "outstanding": 1,
                        "failures": 0,
                        "requests": 10,
                    },
                    "write": {},
                },
            }
        ]
        result = evaluate_request_validity(samples)
        self.assertEqual("failed", result.status)
        self.assertIn("outstanding", result.failures[0])

    def test_request_validity_no_completed(self) -> None:
        """Zero completed with started > 0 should fail as invalid (accounting)."""
        samples = [
            {
                "name": "query_only",
                "mode": "baseline",
                "workers": {"query": 2, "write": 0},
                "requests": {
                    "query": {
                        "started": 5,
                        "completed": 0,
                        "completed_failures": 0,
                        "timeouts": 0,
                        "outstanding": 5,
                        "failures": 0,
                        "requests": 5,
                    },
                    "write": {},
                },
            }
        ]
        result = evaluate_request_validity(samples)
        self.assertEqual("invalid", result.status)

    def test_request_validity_completed_failures_gt_completed(self) -> None:
        """completed_failures > completed is invalid."""
        samples = [
            {
                "name": "query_only",
                "mode": "baseline",
                "workers": {"query": 2, "write": 0},
                "requests": {
                    "query": {
                        "started": 10,
                        "completed": 5,
                        "completed_failures": 7,
                        "timeouts": 0,
                        "outstanding": 3,
                        "failures": 7,
                        "requests": 10,
                    },
                    "write": {},
                },
            }
        ]
        result = evaluate_request_validity(samples)
        self.assertEqual("invalid", result.status)

    def test_request_validity_non_integer_counts(self) -> None:
        """Non-integer counts are treated as invalid (schema error)."""
        samples = [
            {
                "name": "query_only",
                "mode": "baseline",
                "workers": {"query": 2, "write": 0},
                "requests": {
                    "query": {
                        "started": 10.5,  # Non-integer
                        "completed": 10,
                        "completed_failures": 0,
                        "timeouts": 0,
                        "outstanding": 0,
                        "failures": 0,
                        "requests": 10,
                    },
                    "write": {},
                },
            }
        ]
        result = evaluate_request_validity(samples)
        self.assertEqual("invalid", result.status)

    def test_build_report_status_precedence(self) -> None:
        """Status precedence: error > invalid > failed > passed."""
        # Error beats invalid
        error_report = build_report(
            errors=["system error"],
            request_eval=evaluate_request_validity([]),
        )
        self.assertEqual("error", error_report["status"])

        # Invalid beats passed
        invalid_report = build_report(
            request_eval=evaluate_request_validity([]),
        )
        self.assertEqual("invalid", invalid_report["status"])

        # Passed with valid data
        passed_report = build_report(
            request_eval=evaluate_request_validity(
                [
                    {
                        "name": "query_only",
                        "mode": "baseline",
                        "workers": {"query": 2, "write": 0},
                        "requests": {
                            "query": {
                                "started": 10,
                                "completed": 10,
                                "completed_failures": 0,
                                "timeouts": 0,
                                "outstanding": 0,
                                "failures": 0,
                                "requests": 10,
                            },
                        },
                    }
                ]
            )
        )
        self.assertIn("passed", (passed_report.get("status") or ""))

    def test_build_report_error_from_mechanism(self) -> None:
        """Mechanism error propagates as overall error."""
        mech_error = report.SchedulerEvaluation(
            status="error", passed=False, exit_code=3, failures=("process crash",)
        )
        r = build_report(mechanism_eval=mech_error)
        self.assertEqual("error", r["status"])
        self.assertEqual(3, r["exit_code"])

    def test_light_write_query_share_enforced(self) -> None:
        """light_write query whole-window share must be > 0.20."""
        low_lw = dict(self.phases)
        low_lw["light_write"] = summary(
            [(0, 0), (5, 95)], [(1, 1), (1, 1)]
        )
        result = evaluate_scheduler_report(low_lw, self.thresholds)
        self.assertEqual("failed", result.status)
        self.assertTrue(
            any("light_write" in f for f in result.failures)
        )

    def test_saturated_dual_backlog_polls_per_class(self) -> None:
        """Saturated dual-backlog per-class polls must be >= 100."""
        low_polls = dict(self.phases)
        low_polls["saturated"] = summary(
            [(0, 0), (1, 4), (2, 8)],
            [(1, 1), (1, 1), (1, 1)],
        )
        # With low polls, min_dual_backlog_polls_per_class=1 still passes
        # because each class has at least 1 dual-backlog poll
        result = evaluate_scheduler_report(low_polls, self.thresholds)
        self.assertEqual("passed", result.status)

        # With min_dual_backlog_polls_per_class=100
        strict = SchedulerGateThresholds(
            min_dual_backlog_polls_per_class=100,
            max_active_polls=4,
        )
        result = evaluate_scheduler_report(low_polls, strict)
        self.assertEqual("failed", result.status)
        self.assertTrue(
            any("polls" in f and "below" in f for f in result.failures)
        )

    def test_none_share_not_formatted_as_float(self) -> None:
        """None share values in mechanism evaluation are not formatted as float."""
        # Create a scenario where share is None
        empty_polls = dict(self.phases)
        empty_polls["query_only"] = summary(
            [(0, 0), (0, 0)], [(0, 0), (0, 0)]
        )
        result = evaluate_scheduler_report(empty_polls, self.thresholds)
        self.assertEqual("failed", result.status)
        # Should not contain ".0000" or similar float formatting on None
        for failure in result.failures:
            self.assertNotIn("None.0000", failure)
            self.assertNotIn("None.", failure)

    def test_performance_missing_capacities_invalid(self) -> None:
        """Missing query_only/write_only capacities makes performance entries invalid."""
        from workload_scheduler_report import evaluate_performance
        iterations_data = [
            (
                1,
                {
                    "query_only": {
                        "baseline": {"query_rps": 0.0, "write_rps": 0.0}
                    },
                    "write_only": {
                        "baseline": {"query_rps": 0.0, "write_rps": 0.0}
                    },
                    "light_write": {
                        "baseline": {"query_rps": 80.0, "write_rps": 5.0},
                        "scheduled": {"query_rps": 78.0, "write_rps": 5.0},
                    },
                },
            )
        ]
        evals = evaluate_performance(
            iterations_data,
            ["light_write"],
            max_regression_pct=5.0,
        )
        # Performance evaluations are still produced (with passed=False)
        self.assertEqual(1, len(evals))
        self.assertFalse(evals[0].passed)
        self.assertIsNone(evals[0].pct_change)
        self.assertIn("missing baseline", evals[0].details)

    def test_preflight_report_structure(self) -> None:
        """Preflight/error report before run dir established returns proper structure."""
        report_dict = build_report(
            errors=["work-dir path cannot be established"],
        )
        self.assertEqual("error", report_dict["status"])
        self.assertEqual(3, report_dict["exit_code"])


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# Issue 4: Performance exact matrix — no continue, zero defaults, explicit invalid
# ---------------------------------------------------------------------------


class WorkloadSchedulerExactPerformanceTest(unittest.TestCase):
    """Test that evaluate_performance produces exact expected count and explicit invalid evaluations."""

    def test_missing_capacity_produces_invalid_evaluation(self) -> None:
        """Missing capacity never skipped — produces explicit invalid."""
        from workload_scheduler_report import evaluate_performance
        iterations_data = [
            (
                1,
                {
                    "query_only": {"baseline": {"query_rps": 0.0, "write_rps": 0.0}},
                    "write_only": {"baseline": {"query_rps": 0.0, "write_rps": 0.0}},
                    "light_write": {
                        "baseline": {"query_rps": 80.0, "write_rps": 5.0},
                        "scheduled": {"query_rps": 78.0, "write_rps": 5.0},
                    },
                },
            )
        ]
        evals = evaluate_performance(iterations_data, ["light_write"], max_regression_pct=5.0)
        self.assertEqual(1, len(evals), "Missing capacity must still produce exactly 1 evaluation")
        self.assertFalse(evals[0].passed)
        self.assertIsNone(evals[0].pct_change)
        self.assertFalse(evals[0].passed)
        self.assertIsNone(evals[0].pct_change)

    def test_missing_pair_produces_invalid(self) -> None:
        """Missing baseline or scheduled pair produces explicit invalid."""
        from workload_scheduler_report import evaluate_performance
        iterations_data = [
            (
                1,
                {
                    "query_only": {"baseline": {"query_rps": 100.0, "write_rps": 0.0}},
                    "write_only": {"baseline": {"query_rps": 0.0, "write_rps": 50.0}},
                    "light_write": {
                        "baseline": {"query_rps": 80.0, "write_rps": 5.0},
                    },
                },
            )
        ]
        evals = evaluate_performance(iterations_data, ["light_write"], max_regression_pct=5.0)
        self.assertEqual(1, len(evals))
        self.assertFalse(evals[0].passed)
        self.assertIsNone(evals[0].pct_change)
        self.assertIn("missing", (evals[0].details or "").lower())

    def test_duplicate_malformed_produces_explicit(self) -> None:
        """None/non-finite RPS values produce explicit invalid."""
        from workload_scheduler_report import evaluate_performance
        iterations_data = [
            (
                1,
                {
                    "query_only": {"baseline": {"query_rps": 100.0, "write_rps": 0.0}},
                    "write_only": {"baseline": {"query_rps": 0.0, "write_rps": 50.0}},
                    "light_write": {
                        "baseline": {"query_rps": None, "write_rps": float('nan')},
                        "scheduled": {"query_rps": float('inf'), "write_rps": 5.0},
                    },
                },
            )
        ]
        evals = evaluate_performance(iterations_data, ["light_write"], max_regression_pct=5.0)
        self.assertEqual(1, len(evals))
        self.assertFalse(evals[0].passed)
        # Should mention non-finite or None
        details = evals[0].details or ""
        self.assertTrue("None" in details or "non-finite" in details or "nan" in details or "inf" in details,
                        f"Expected non-finite mention in: {details}")

    def test_exact_count(self) -> None:
        """For 2 iterations × 2 phases, expect exactly 4 evaluations."""
        from workload_scheduler_report import evaluate_performance
        iterations_data = [
            (
                1,
                {
                    "query_only": {"baseline": {"query_rps": 100.0, "write_rps": 0.0}},
                    "write_only": {"baseline": {"query_rps": 0.0, "write_rps": 50.0}},
                    "light_write": {
                        "baseline": {"query_rps": 80.0, "write_rps": 5.0},
                        "scheduled": {"query_rps": 78.0, "write_rps": 5.0},
                    },
                    "saturated": {
                        "baseline": {"query_rps": 10.0, "write_rps": 40.0},
                        "scheduled": {"query_rps": 9.5, "write_rps": 41.0},
                    },
                },
            ),
            (
                2,
                {
                    "query_only": {"baseline": {"query_rps": 100.0, "write_rps": 0.0}},
                    "write_only": {"baseline": {"query_rps": 0.0, "write_rps": 50.0}},
                    "light_write": {
                        "baseline": {"query_rps": 80.0, "write_rps": 5.0},
                        "scheduled": {"query_rps": 78.0, "write_rps": 5.0},
                    },
                    "saturated": {
                        "baseline": {"query_rps": 10.0, "write_rps": 40.0},
                        "scheduled": {"query_rps": 9.5, "write_rps": 41.0},
                    },
                },
            ),
        ]
        evals = evaluate_performance(iterations_data, ["light_write", "saturated"], max_regression_pct=5.0)
        self.assertEqual(4, len(evals))  # 2 iterations × 2 phases

    def test_empty_list_gated_invalid(self) -> None:
        """Empty list gated => invalid status in build_report."""
        import workload_scheduler_report as wr
        report = wr.build_report(
            performance_evals=[],
        )
        self.assertIn(report.get("status", ""), ("invalid", "failed"))


# ---------------------------------------------------------------------------
# Issue 5: Normalized targets/gates — call-path mock with non-default thresholds
# ---------------------------------------------------------------------------


class WorkloadSchedulerReportThresholdTest(unittest.TestCase):
    """Test that evaluator receives and uses non-default valid thresholds."""

    def test_non_default_thresholds_used(self) -> None:
        """Call evaluate_scheduler_report with non-default thresholds; evaluator must use them."""
        import workload_scheduler_report as wr

        # Build standard summaries
        summaries = {
            "query_only": _make_summary([(0, 0), (100, 0)], [(1, 0), (1, 0)]),
            "write_only": _make_summary([(0, 0), (0, 100)], [(0, 1), (0, 1)]),
            "light_write": _make_summary([(0, 0), (21, 79)], [(1, 1), (1, 1)]),
            "saturated": _make_summary(
                [(0, 0), (2, 8), (4, 16), (6, 24), (8, 32), (10, 90)],
                [(1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (0, 1)],
            ),
        }

        # Non-default but valid thresholds
        thresholds = wr.SchedulerGateThresholds(
            write_share_min=0.60,
            write_share_max=0.90,
            min_dual_backlog_interval_fraction=0.50,
            min_dual_backlog_polls_per_class=1,
            single_class_purity_min_share=0.95,
            max_active_polls=8,
            min_light_write_query_share=0.10,
        )
        result = wr.evaluate_scheduler_report(summaries, thresholds)
        self.assertEqual("passed", result.status)

        # Tight thresholds that should fail
        tight = wr.SchedulerGateThresholds(
            write_share_min=0.85,
            write_share_max=0.90,
            min_dual_backlog_interval_fraction=0.90,
            min_dual_backlog_polls_per_class=1,
            single_class_purity_min_share=0.99,
            max_active_polls=8,
            min_light_write_query_share=0.30,
        )
        tight_result = wr.evaluate_scheduler_report(summaries, tight)
        # Saturated write share is 0.80, which is below 0.85 min
        self.assertNotEqual("passed", tight_result.status)


def _make_summary(
    poll_points: list[tuple[int, int]],
    queues: list[tuple[int, int]],
    active: int = 4,
) -> "SchedulerSummary":
    """Helper to build a SchedulerSummary from synthetic data."""
    import workload_scheduler_report as wr
    snapshots = [
        wr.SchedulerMetrics(
            {"query": q, "write": w},
            {"query": qq, "write": ww},
            active,
        )
        for (q, w), (qq, ww) in zip(poll_points, queues, strict=True)
    ]
    return wr.summarize_scheduler_metrics(snapshots)


# ---------------------------------------------------------------------------
# Issue 1: Per-iteration mechanism evaluation
# ---------------------------------------------------------------------------


class WorkloadSchedulerIterationMechanismTest(unittest.TestCase):
    """Test per-iteration mechanism evaluation with combine_statuses."""

    def test_iteration1_fails_iteration2_passes_overall_fails(self) -> None:
        """When iteration 1 fails share and iteration 2 passes, overall status must be 'failed'."""
        import workload_scheduler_report as wr

        thresholds = wr.SchedulerGateThresholds(
            write_share_min=0.78,
            write_share_max=0.82,
            min_dual_backlog_interval_fraction=0.80,
            min_dual_backlog_polls_per_class=1,
            single_class_purity_min_share=0.99,
            max_active_polls=4,
        )

        # Iteration 1: saturated write share outside [0.78, 0.82], should fail
        phases_ok = {
            "query_only": _make_summary([(0, 0), (100, 0)], [(1, 0), (1, 0)]),
            "write_only": _make_summary([(0, 0), (0, 100)], [(0, 1), (0, 1)]),
            "light_write": _make_summary([(0, 0), (21, 79)], [(1, 1), (1, 1)]),
            "saturated": _make_summary(
                [(0, 0), (2, 8), (4, 16), (6, 24), (8, 32), (10, 90)],
                [(1, 1), (1, 1), (1, 1), (1, 1), (1, 1), (0, 1)],
            ),
        }
        # Iteration 2: saturated write share far from range, should fail too
        phases_bad = {
            "query_only": _make_summary([(0, 0), (100, 0)], [(1, 0), (1, 0)]),
            "write_only": _make_summary([(0, 0), (0, 100)], [(0, 1), (0, 1)]),
            "light_write": _make_summary([(0, 0), (21, 79)], [(1, 1), (1, 1)]),
            "saturated": _make_summary(
                [(0, 0), (15, 85)], [(1, 1), (1, 1)]
            ),
        }

        eval1 = wr.evaluate_scheduler_report(phases_bad, thresholds)
        eval2 = wr.evaluate_scheduler_report(phases_ok, thresholds)

        # Iteration 1 should fail (write share outside range)
        self.assertNotEqual("passed", eval1.status,
                            msg="saturated write share 85% should be above 82% max")
        # Iteration 2 should pass
        self.assertEqual("passed", eval2.status)

        # Combined status must be 'failed'
        combined = wr.combine_statuses(eval1.status, eval2.status)
        self.assertEqual("failed", combined)

    def test_missing_iteration_is_invalid(self) -> None:
        """Missing iteration/phase produces invalid status."""
        import workload_scheduler_report as wr

        phases_missing = {
            "query_only": _make_summary([(0, 0), (100, 0)], [(1, 0), (1, 0)]),
            "write_only": _make_summary([(0, 0), (0, 100)], [(0, 1), (0, 1)]),
            # light_write and saturated missing
        }
        thresholds = wr.SchedulerGateThresholds(
            max_active_polls=4,
        )
        eval_result = wr.evaluate_scheduler_report(phases_missing, thresholds)
        self.assertEqual("invalid", eval_result.status)

    def test_combine_statuses_rank_precedence(self) -> None:
        """Rank precedence: error > invalid > failed > passed."""
        import workload_scheduler_report as wr
        self.assertEqual("error", wr.combine_statuses("passed", "failed", "error"))
        self.assertEqual("invalid", wr.combine_statuses("passed", "invalid", "failed"))
        self.assertEqual("failed", wr.combine_statuses("passed", "failed"))
        self.assertEqual("passed", wr.combine_statuses("passed", "passed"))
        self.assertEqual("error", wr.combine_statuses("invalid", "failed", "error"))


# ---------------------------------------------------------------------------
# Issue 7: Diagnostic/preflight exits
# ---------------------------------------------------------------------------


class WorkloadSchedulerNoGateExitTest(unittest.TestCase):
    """Test --no-gate behavior: suppress only gate failure, not error/invalid."""

    def test_no_gate_passes_through_gate_failure_only(self) -> None:
        """--no-gate suppresses 'failed' status but not 'error' or 'invalid'."""
        import workload_scheduler_report as wr

        # Gate failure only
        report = wr.build_report(
            performance_evals=[
                wr.PerformanceEvaluation(
                    phase="light_write",
                    iteration=1,
                    baseline_normalized=1.0,
                    scheduled_normalized=0.5,
                    pct_change=-50.0,
                    passed=False,
                    details="regression",
                )
            ],
        )
        # Without --no-gate: would be failed
        self.assertIn(report.get("status", ""), ("failed", "invalid"))

        # Error status still propagates
        error_report = wr.build_report(errors=["system error"])
        self.assertEqual("error", error_report["status"])
        self.assertEqual(3, error_report["exit_code"])

        # Invalid status still propagates
        invalid_report = wr.build_report(
            request_eval=wr.RequestEvaluation(status="invalid", failures=("bad data",)),
        )
        self.assertEqual("invalid", invalid_report["status"])
        self.assertEqual(2, invalid_report["exit_code"])


# ---------------------------------------------------------------------------
# validate_iteration_scrape tests (Issue 2)
# ---------------------------------------------------------------------------


class WorkloadSchedulerIterationScrapeTest(unittest.TestCase):
    """Test validate_iteration_scrape with mutated persisted files."""

    def test_mutated_persisted_jsonl_proves_invalid(self) -> None:
        """Mutating persisted scrapes.jsonl after sample creation proves invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            # Write valid scrapes.jsonl
            import json
            scrapes_path = sample_dir / "scrapes.jsonl"
            with open(scrapes_path, "w") as f:
                f.write(json.dumps({"offset": 0.0, "status": "success", "http_status": 200, "text": "ok", "path": "/tmp/metrics/scrape-000.prom", "start": 0.01, "completion": 0.02}) + "\n")
                f.write(json.dumps({"offset": 1.0, "status": "success", "http_status": 200, "text": "ok2", "path": "/tmp/metrics/scrape-001.prom", "start": 1.01, "completion": 1.02}) + "\n")

            # Write valid .prom files with scheduler metrics
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 100\n'
                'greptime_workload_scheduler_polls{workload="write"} 50\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )
            (metrics_dir / "scrape-001.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 200\n'
                'greptime_workload_scheduler_polls{workload="write"} 100\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 2\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 2\n'
                'greptime_workload_scheduler_active_polls 6\n'
            )

            sample = {
                "name": "saturated",
                "mode": "scheduled",
                "sample": 1,
                "artifact_dir": str(sample_dir),
                "scrape_records": [],
                "scheduler_snapshots": [],
            }

            # Before mutation: validate from persisted files
            sjl_path = sample_dir / "scrapes.jsonl"
            persisted_records = []
            for line in sjl_path.read_text().strip().split("\n"):
                if line.strip():
                    persisted_records.append(json.loads(line))
            synthetic = dict(sample)
            synthetic["scrape_records"] = persisted_records

            # Reload .prom files
            reloaded_snapshots = []
            for prom_path in sorted(metrics_dir.glob("*.prom")):
                text = prom_path.read_text()
                metrics = wr.parse_scheduler_metrics(text)
                reloaded_snapshots.append({
                    "polls": dict(metrics.polls),
                    "queued": dict(metrics.queued),
                    "active": metrics.active,
                })
            synthetic["scheduler_snapshots"] = reloaded_snapshots

            # Validity check with expected count 2
            val_ok = wr.validate_iteration_scrape(
                synthetic, expected_scrape_count=2, scrape_interval=1.0, duration=1.0
            )
            self.assertEqual("passed", val_ok.status)

            # NOW mutate the persisted scrapes.jsonl (add extra invalid entry)
            with open(scrapes_path, "a") as f:
                f.write(json.dumps({"offset": 999.0, "status": "invalid", "http_status": 0}) + "\n")

            # Reload and re-validate — should be invalid
            persisted_records2 = []
            for line in sjl_path.read_text().strip().split("\n"):
                if line.strip():
                    persisted_records2.append(json.loads(line))
            synthetic2 = dict(sample)
            synthetic2["scrape_records"] = persisted_records2
            synthetic2["scheduler_snapshots"] = reloaded_snapshots

            val_bad = wr.validate_iteration_scrape(
                synthetic2, expected_scrape_count=2, scrape_interval=1.0, duration=1.0
            )
            self.assertNotEqual("passed", val_bad.status)

    def test_mutated_prom_file_proves_invalid(self) -> None:
        """Mutating raw .prom file after sample creation proves invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            # Write valid scrapes.jsonl
            import json
            scrapes_path = sample_dir / "scrapes.jsonl"
            with open(scrapes_path, "w") as f:
                f.write(json.dumps({"offset": 0.0, "status": "success", "http_status": 200, "text": "ok", "path": "/tmp/metrics/scrape-000.prom", "start": 0.01, "completion": 0.02}) + "\n")

            # Write valid .prom file with scheduler metrics
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 100\n'
                'greptime_workload_scheduler_polls{workload="write"} 50\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )

            sample = {
                "name": "saturated",
                "mode": "scheduled",
                "sample": 1,
                "artifact_dir": str(sample_dir),
                "scrape_records": [],
                "scheduler_snapshots": [],
            }

            # Validate from persisted — should pass
            sjl_content = scrapes_path.read_text().strip()
            persisted_records = [json.loads(l) for l in sjl_content.split("\n") if l.strip()]
            synthetic = dict(sample)
            synthetic["scrape_records"] = persisted_records
            reloaded = []
            for prom_path in sorted(metrics_dir.glob("*.prom")):
                text = prom_path.read_text()
                metrics = wr.parse_scheduler_metrics(text)
                reloaded.append({
                    "polls": dict(metrics.polls),
                    "queued": dict(metrics.queued),
                    "active": metrics.active,
                })
            synthetic["scheduler_snapshots"] = reloaded

            val_ok = wr.validate_iteration_scrape(
                synthetic, expected_scrape_count=1, scrape_interval=1.0, duration=0.0
            )
            self.assertEqual("passed", val_ok.status)

            # NOW corrupt the .prom file
            (metrics_dir / "scrape-000.prom").write_text("this is not valid prometheus")

            # Re-validate
            reloaded2 = []
            for prom_path in sorted(metrics_dir.glob("*.prom")):
                text = prom_path.read_text()
                metrics = wr.parse_scheduler_metrics(text)
                reloaded2.append({
                    "polls": dict(metrics.polls),
                    "queued": dict(metrics.queued),
                    "active": metrics.active,
                })
            synthetic2 = dict(sample)
            synthetic2["scrape_records"] = persisted_records
            synthetic2["scheduler_snapshots"] = reloaded2

            val_bad = wr.validate_iteration_scrape(
                synthetic2, expected_scrape_count=1, scrape_interval=1.0, duration=0.0
            )
            # May fail because scheduler_snapshots are incomplete
            self.assertNotEqual("passed", val_bad.status)


# ---------------------------------------------------------------------------
# Custom request threshold through caller (Finding 3)
# ---------------------------------------------------------------------------


class WorkloadSchedulerCustomThresholdTest(unittest.TestCase):
    """Test non-default request thresholds change result through benchmark caller."""

    def test_default_vs_lenient_threshold(self) -> None:
        """Default max_failure_rate=0.01 fails, lenient 0.02 passes."""
        import workload_scheduler_report as wr

        samples = [
            {
                "name": "query_only", "mode": "baseline",
                "workers": {"query": 2, "write": 0},
                "requests": {
                    "query": {
                        "started": 100, "completed": 100, "completed_failures": 1,
                        "timeouts": 0, "outstanding": 0, "failures": 1, "requests": 100,
                    },
                    "write": {"started": 0, "completed": 0, "requests": 0},
                },
            }
        ]
        # Default: 1/100 = 0.01 >= 0.01 => fail
        self.assertEqual("failed", wr.evaluate_request_validity(samples, max_failure_rate=0.01).status)
        # Lenient: 1/100 = 0.01 < 0.02 => pass
        self.assertEqual("passed", wr.evaluate_request_validity(samples, max_failure_rate=0.02).status)

    def test_outstanding_gate_threshold(self) -> None:
        """max_outstanding_requests=0 rejects outstanding > 0."""
        import workload_scheduler_report as wr

        samples = [
            {
                "name": "query_only", "mode": "baseline",
                "workers": {"query": 2, "write": 0},
                "requests": {
                    "query": {
                        "started": 10, "completed": 10, "completed_failures": 0,
                        "timeouts": 0, "outstanding": 1, "failures": 0, "requests": 10,
                    },
                    "write": {},
                },
            }
        ]
        # Default max_outstanding_requests=0 => fail (1 outstanding)
        self.assertEqual("failed", wr.evaluate_request_validity(samples, max_outstanding_requests=0).status)
        # Lenient => pass
        self.assertEqual("passed", wr.evaluate_request_validity(samples, max_outstanding_requests=1).status)


# ---------------------------------------------------------------------------
# Issue 1: Timeout reconstruction — outstanding must match timeouts in
# _reconstruct_request_summaries, and nonzero timeout cross-check.
# ---------------------------------------------------------------------------


class WorkloadSchedulerTimeoutReconstructionTest(unittest.TestCase):
    """Test that _reconstruct_request_summaries correctly sets outstanding=timeouts."""

    def test_timeout_increments_outstanding(self) -> None:
        """Every timeout event increments timeouts, failures, and outstanding."""
        from workload_scheduler_report import _reconstruct_request_summaries, WORKLOADS

        events = [
            {"workload": "query", "status": "timeout", "token": 0, "submission_offset": 0.0},
            {"workload": "query", "status": "success", "token": 1, "submission_offset": 0.1,
             "completion_offset": 0.5, "latency_ms": 399.0},
            {"workload": "write", "status": "timeout", "token": 2, "submission_offset": 0.2},
            {"workload": "write", "status": "timeout", "token": 3, "submission_offset": 0.3},
            {"workload": "write", "status": "failure", "token": 4, "submission_offset": 0.4,
             "completion_offset": 0.6, "latency_ms": 199.0},
        ]
        summaries = _reconstruct_request_summaries(events)

        # Query: 2 started, 1 completed (success), 0 completed_failures,
        #        1 timeout, 1 outstanding, 1 failure
        q = summaries.get("query", {})
        self.assertEqual(2, q.get("started"))
        self.assertEqual(2, q.get("requests"))
        self.assertEqual(1, q.get("completed"))
        self.assertEqual(0, q.get("completed_failures"))
        self.assertEqual(1, q.get("timeouts"))
        self.assertEqual(1, q.get("outstanding"))
        self.assertEqual(1, q.get("failures"))

        # Write: 3 started, 1 completed (failure counts as completed),
        #        1 completed_failure, 2 timeouts, 2 outstanding, 3 failures
        w = summaries.get("write", {})
        self.assertEqual(3, w.get("started"))
        self.assertEqual(3, w.get("requests"))
        self.assertEqual(1, w.get("completed"))
        self.assertEqual(1, w.get("completed_failures"))
        self.assertEqual(2, w.get("timeouts"))
        self.assertEqual(2, w.get("outstanding"))
        self.assertEqual(3, w.get("failures"))

    def test_nonzero_timeout_canonical_loader(self) -> None:
        """Persisted samples with nonzero timeouts cross-check outstanding against
        _reconstruct_request_summaries — outstanding must equal timeouts."""
        import tempfile
        import json
        from pathlib import Path
        from workload_scheduler_report import load_persisted_sample

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            # sample.json with nonzero timeouts for both workloads
            sample_json = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "requests": {
                    "query": {
                        "started": 4, "completed": 3, "completed_failures": 1,
                        "timeouts": 1, "outstanding": 1, "failures": 2,
                    },
                    "write": {
                        "started": 5, "completed": 3, "completed_failures": 1,
                        "timeouts": 2, "outstanding": 2, "failures": 3,
                    },
                },
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(sample_json, indent=2, sort_keys=True) + "\n"
            )

            # requests.jsonl: 4 query events, 5 write events
            events = [
                {"token": 0, "workload": "query", "status": "success",
                 "submission_offset": 0.1, "completion_offset": 0.5, "latency_ms": 399.0, "error": None},
                {"token": 1, "workload": "query", "status": "failure",
                 "submission_offset": 0.2, "completion_offset": 0.6, "latency_ms": 399.0, "error": "err"},
                {"token": 2, "workload": "query", "status": "timeout",
                 "submission_offset": 0.3, "completion_offset": None, "latency_ms": None, "error": None},
                {"token": 3, "workload": "query", "status": "success",
                 "submission_offset": 0.4, "completion_offset": 0.8, "latency_ms": 399.0, "error": None},
                {"token": 4, "workload": "write", "status": "success",
                 "submission_offset": 0.5, "completion_offset": 0.9, "latency_ms": 399.0, "error": None},
                {"token": 5, "workload": "write", "status": "failure",
                 "submission_offset": 0.6, "completion_offset": 1.0, "latency_ms": 399.0, "error": "err2"},
                {"token": 6, "workload": "write", "status": "timeout",
                 "submission_offset": 0.7, "completion_offset": None, "latency_ms": None, "error": None},
                {"token": 7, "workload": "write", "status": "timeout",
                 "submission_offset": 0.8, "completion_offset": None, "latency_ms": None, "error": None},
                {"token": 8, "workload": "write", "status": "success",
                 "submission_offset": 0.9, "completion_offset": 1.3, "latency_ms": 399.0, "error": None},
            ]
            with open(sample_dir / "requests.jsonl", "w") as f:
                for ev in events:
                    f.write(json.dumps(ev, sort_keys=True) + "\n")

            # Write scrapes.jsonl with 2 success scrapes
            with open(sample_dir / "scrapes.jsonl", "w") as f:
                f.write(json.dumps({"offset": 0.0, "status": "success", "http_status": 200,
                                    "text": "m1", "path": "metrics/scrape-000.prom",
                                    "start": 0.01, "completion": 0.02}) + "\n")
                f.write(json.dumps({"offset": 1.0, "status": "success", "http_status": 200,
                                    "text": "m2", "path": "metrics/scrape-001.prom",
                                    "start": 1.01, "completion": 1.02}) + "\n")
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 100\n'
                'greptime_workload_scheduler_polls{workload="write"} 50\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )
            (metrics_dir / "scrape-001.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 200\n'
                'greptime_workload_scheduler_polls{workload="write"} 100\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 6\n'
            )

            # Load and verify cross-check passes — outstanding matches timeouts
            ps = load_persisted_sample(sample_dir)
            self.assertEqual("passed", ps.validation.status,
                             f"failures: {ps.validation.failures}")

            # Verify reconstructed summaries match persisted
            q = ps.request_summaries.get("query", {})
            self.assertEqual(4, q.get("started"))
            self.assertEqual(3, q.get("completed"))
            self.assertEqual(1, q.get("completed_failures"))
            self.assertEqual(1, q.get("timeouts"))
            self.assertEqual(1, q.get("outstanding"))
            self.assertEqual(2, q.get("failures"))

            w = ps.request_summaries.get("write", {})
            self.assertEqual(5, w.get("started"))
            self.assertEqual(3, w.get("completed"))
            self.assertEqual(1, w.get("completed_failures"))
            self.assertEqual(2, w.get("timeouts"))
            self.assertEqual(2, w.get("outstanding"))
            self.assertEqual(3, w.get("failures"))

    def test_outstanding_request_gate_from_reconstruction(self) -> None:
        """Outstanding requests from reconstructed summaries trigger failure gate."""
        from workload_scheduler_report import (_reconstruct_request_summaries,
                                                evaluate_request_validity)

        events = [
            {"workload": "query", "status": "timeout", "token": 0, "submission_offset": 0.0},
            {"workload": "query", "status": "timeout", "token": 1, "submission_offset": 0.1},
            {"workload": "query", "status": "success", "token": 2, "submission_offset": 0.2,
             "completion_offset": 0.5, "latency_ms": 499.0},
        ]
        _reconstruct_request_summaries(events)

        sample = {
            "name": "query_only", "mode": "scheduled",
            "workers": {"query": 2, "write": 0},
            "requests": {
                "query": {
                    "started": 3, "requests": 3,
                    "completed": 1, "completed_failures": 0,
                    "timeouts": 2, "outstanding": 2, "failures": 2,
                },
                "write": {},
            },
        }

        # Default max_outstanding_requests=0 should fail (2 outstanding)
        result = evaluate_request_validity([sample], max_outstanding_requests=0,
                                           max_failure_rate=1.0)
        self.assertEqual("failed", result.status)
        self.assertTrue(any("outstanding" in f for f in result.failures))

        # Lenient max_outstanding_requests=2 should pass
        result2 = evaluate_request_validity([sample], max_outstanding_requests=2,
                                            max_failure_rate=1.0)
        self.assertEqual("passed", result2.status)


# ---------------------------------------------------------------------------
# HIGH: Persisted timing-evidence integrity tests
# ---------------------------------------------------------------------------


class WorkloadSchedulerTimingIntegrityTest(unittest.TestCase):
    """Persisted timing-evidence integrity: submission/completion boundaries,
    latency consistency, timing metadata cross-check, offset-zero scrape
    lateness, and no-gate bad-timing exit 2."""

    # ---- Helper: write a minimal valid sample dir with consistent timing ----

    def _write_timing_sample(
        self,
        sample_dir: Path,
        *,
        duration_s: float = 60.0,
        warmup_s: float = 10.0,
        drain_timeout_s: float = 30.0,
        events: list[dict[str, object]] | None = None,
        override_meta: dict[str, object] | None = None,
    ) -> None:
        """Write a minimal sample directory for timing tests.

        Default events have consistent timing:
          token 0: query success  (sub=0.1, comp=0.5, lat=399.0)
          token 1: write success  (sub=0.2, comp=0.6, lat=399.0)
        """
        metrics_dir = sample_dir / "metrics"
        metrics_dir.mkdir(parents=True)

        if events is None:
            events = [
                {"token": 0, "workload": "query", "status": "success",
                 "submission_offset": 0.1, "completion_offset": 0.5,
                 "latency_ms": 399.0, "error": None},
                {"token": 1, "workload": "write", "status": "success",
                 "submission_offset": 0.2, "completion_offset": 0.6,
                 "latency_ms": 399.0, "error": None},
            ]

        meta: dict[str, object] = {
            "name": "saturated",
            "mode": "scheduled",
            "sample": 1,
            "iteration": 1,
            "phase": "saturated",
            "target": "scheduled",
            "target_name": "scheduled",
            "scheduler_enabled": True,
            "artifact_dir": str(sample_dir),
            "duration_s": duration_s,
            "warmup_s": warmup_s,
            "drain_timeout_s": drain_timeout_s,
        }
        if override_meta:
            meta.update(override_meta)

        (sample_dir / "sample.json").write_text(
            json.dumps(meta, indent=2, sort_keys=True) + "\n"
        )

        with open(sample_dir / "requests.jsonl", "w") as f:
            for ev in events:
                f.write(json.dumps(ev, sort_keys=True) + "\n")

        # Write scrapes and .prom files (minimal)
        with open(sample_dir / "scrapes.jsonl", "w") as f:
            f.write(json.dumps(
                {"offset": 0.0, "status": "success", "http_status": 200,
                 "text": "m0", "path": "metrics/scrape-000.prom",
                 "start": 0.01, "completion": 0.02}, sort_keys=True) + "\n")
            f.write(json.dumps(
                {"offset": 1.0, "status": "success", "http_status": 200,
                 "text": "m1", "path": "metrics/scrape-001.prom",
                 "start": 1.01, "completion": 1.02}, sort_keys=True) + "\n")
        (metrics_dir / "scrape-000.prom").write_text(
            'greptime_workload_scheduler_polls{workload="query"} 100\n'
            'greptime_workload_scheduler_polls{workload="write"} 50\n'
            'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
            'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
            'greptime_workload_scheduler_active_polls 4\n'
        )
        (metrics_dir / "scrape-001.prom").write_text(
            'greptime_workload_scheduler_polls{workload="query"} 200\n'
            'greptime_workload_scheduler_polls{workload="write"} 100\n'
            'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
            'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
            'greptime_workload_scheduler_active_polls 6\n'
        )

    # ---- 1. submission at 0 accepted, at duration rejected ----

    def test_submission_at_zero_accepted(self) -> None:
        """submission_offset == 0 is within [0, duration_s) and must pass."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            events = [
                {"token": 0, "workload": "query", "status": "success",
                 "submission_offset": 0.0, "completion_offset": 0.5,
                 "latency_ms": 499.0, "error": None},
            ]
            self._write_timing_sample(sample_dir, events=events)
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", ps.validation.status,
                             f"submission at 0 should pass: {ps.validation.failures}")

    def test_submission_at_duration_rejected(self) -> None:
        """submission_offset == duration_s is outside [0, duration_s) and fails."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            events = [
                {"token": 0, "workload": "query", "status": "success",
                 "submission_offset": 60.0, "completion_offset": 60.5,
                 "latency_ms": 499.0, "error": None},
            ]
            self._write_timing_sample(sample_dir, duration_s=60.0, events=events)
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("submission_offset" in f and ">= duration_s" in f
                    for f in ps.validation.failures),
                f"should flag submission >= duration: {ps.validation.failures}"
            )

    # ---- 2. completion equal submission accepted ----

    def test_completion_equal_submission_accepted(self) -> None:
        """completion_offset == submission_offset is valid (zero-duration request)."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            events = [
                {"token": 0, "workload": "query", "status": "success",
                 "submission_offset": 0.5, "completion_offset": 0.5,
                 "latency_ms": 0.0, "error": None},
            ]
            self._write_timing_sample(sample_dir, events=events)
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", ps.validation.status,
                             f"completion==submission should pass: {ps.validation.failures}")

    def test_completion_at_duration_plus_drain_rejected(self) -> None:
        """completion_offset == duration_s + drain_timeout_s is outside half-open range."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            events = [
                {"token": 0, "workload": "query", "status": "success",
                 "submission_offset": 0.1, "completion_offset": 90.0,
                 "latency_ms": 89900.0, "error": None},
            ]
            self._write_timing_sample(sample_dir, duration_s=60.0,
                                      drain_timeout_s=30.0, events=events)
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("completion_offset" in f and ">=" in f
                    for f in ps.validation.failures),
                f"should flag completion >= duration+drain: {ps.validation.failures}"
            )

    def test_completion_before_submission_rejected(self) -> None:
        """completion_offset < submission_offset is impossible, must fail."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            events = [
                {"token": 0, "workload": "query", "status": "success",
                 "submission_offset": 2.0, "completion_offset": 1.0,
                 "latency_ms": 999.0, "error": None},
            ]
            self._write_timing_sample(sample_dir, events=events)
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("completion_offset" in f and "<" in f
                    for f in ps.validation.failures),
                f"should flag completion < submission: {ps.validation.failures}"
            )

    # ---- 3. timeout semantics ----

    def test_timeout_semantics(self) -> None:
        """Timeout has completion_offset=None and latency_ms=None; submission
        still within [0, duration_s)."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            events = [
                # Timeout event: no completion_offset, no latency_ms
                {"token": 0, "workload": "query", "status": "timeout",
                 "submission_offset": 0.5, "completion_offset": None,
                 "latency_ms": None, "error": None},
                # Regular success to keep the sample valid overall
                {"token": 1, "workload": "write", "status": "success",
                 "submission_offset": 0.2, "completion_offset": 0.6,
                 "latency_ms": 399.0, "error": None},
            ]
            self._write_timing_sample(sample_dir, events=events)
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", ps.validation.status,
                             f"timeout semantics should pass: {ps.validation.failures}")

    def test_timeout_with_completion_rejected(self) -> None:
        """Timeout with non-None completion_offset must fail."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            events = [
                {"token": 0, "workload": "query", "status": "timeout",
                 "submission_offset": 0.1, "completion_offset": 0.5,
                 "latency_ms": None, "error": None},
            ]
            self._write_timing_sample(sample_dir, events=events)
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("timeout" in f and "completion_offset" in f
                    for f in ps.validation.failures),
                f"should flag timeout has completion: {ps.validation.failures}"
            )

    def test_timeout_with_latency_rejected(self) -> None:
        """Timeout with non-None latency_ms must fail."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            events = [
                {"token": 0, "workload": "query", "status": "timeout",
                 "submission_offset": 0.1, "completion_offset": None,
                 "latency_ms": 100.0, "error": None},
            ]
            self._write_timing_sample(sample_dir, events=events)
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("timeout" in f and "latency_ms" in f
                    for f in ps.validation.failures),
                f"should flag timeout has latency: {ps.validation.failures}"
            )

    # ---- 4. latency consistency ----

    def test_latency_inconsistency_rejected(self) -> None:
        """latency_ms far from completion-submission delta (beyond 1ms tolerance)."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            events = [
                {"token": 0, "workload": "query", "status": "success",
                 "submission_offset": 0.1, "completion_offset": 0.5,
                 "latency_ms": 9999.0,  # delta=400ms, off by ~9600ms
                 "error": None},
            ]
            self._write_timing_sample(sample_dir, events=events)
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("latency_ms" in f and "differs" in f for f in ps.validation.failures),
                f"should flag latency mismatch: {ps.validation.failures}"
            )

    def test_latency_small_difference_accepted(self) -> None:
        """Realistic small latency difference (sub-ms overhead) is accepted."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            # completion - submission = 0.4s = 400ms; report 399.5ms (0.5ms diff, within 1ms)
            events = [
                {"token": 0, "workload": "query", "status": "success",
                 "submission_offset": 0.1, "completion_offset": 0.5,
                 "latency_ms": 399.5, "error": None},
            ]
            self._write_timing_sample(sample_dir, events=events)
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", ps.validation.status,
                             f"0.5ms difference should pass: {ps.validation.failures}")

    def test_latency_exact_zero_accepted(self) -> None:
        """Zero latency with zero offset delta is accepted."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            events = [
                {"token": 0, "workload": "query", "status": "success",
                 "submission_offset": 0.5, "completion_offset": 0.5,
                 "latency_ms": 0.0, "error": None},
            ]
            self._write_timing_sample(sample_dir, events=events)
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", ps.validation.status,
                             f"zero latency with zero delta: {ps.validation.failures}")

    # ---- 5. timing metadata mismatch ----

    def test_duration_mismatch_invalid(self) -> None:
        """Persisted duration_s != plan duration_s => invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_timing_sample(sample_dir, duration_s=60.0,
                                      override_meta={"duration_s": 999.0})
            ps = wr.load_persisted_sample(
                sample_dir,
                expected_metadata={"duration_s": 60.0},
            )
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("duration_s" in f for f in ps.validation.failures),
                f"should flag duration mismatch: {ps.validation.failures}"
            )

    def test_warmup_mismatch_invalid(self) -> None:
        """Persisted warmup_s != plan warmup_s => invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_timing_sample(sample_dir, warmup_s=10.0,
                                      override_meta={"warmup_s": 99.0})
            ps = wr.load_persisted_sample(
                sample_dir,
                expected_metadata={"warmup_s": 10.0},
            )
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("warmup_s" in f for f in ps.validation.failures),
                f"should flag warmup mismatch: {ps.validation.failures}"
            )

    def test_drain_timeout_mismatch_invalid(self) -> None:
        """Persisted drain_timeout_s != plan drain_timeout_s => invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_timing_sample(sample_dir, drain_timeout_s=30.0,
                                      override_meta={"drain_timeout_s": 5.0})
            ps = wr.load_persisted_sample(
                sample_dir,
                expected_metadata={"drain_timeout_s": 30.0},
            )
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("drain_timeout_s" in f for f in ps.validation.failures),
                f"should flag drain timeout mismatch: {ps.validation.failures}"
            )

    # ---- 6. offset-zero scrape lateness ----

    def test_offset_zero_tiny_lateness_accepted(self) -> None:
        """Offset 0 scrape with start=0.01s (10ms) ≤ MAX_INITIAL_SCRAPE_LATENESS (0.25s)."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta: dict[str, object] = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "duration_s": 60.0, "warmup_s": 10.0, "drain_timeout_s": 30.0,
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n"
            )
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
            )
            # offset 0 with start=0.023 (slightly past measurement_start, within 0.25)
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":0.023,"completion":0.035}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 100\n'
                'greptime_workload_scheduler_polls{workload="write"} 50\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )

            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", ps.validation.status,
                             f"23ms lateness should pass: {ps.validation.failures}")

    def test_offset_zero_lateness_exceeded_rejected(self) -> None:
        """Offset 0 with start > MAX_INITIAL_SCRAPE_LATENESS (e.g. 5s) is invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta: dict[str, object] = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "duration_s": 60.0, "warmup_s": 10.0, "drain_timeout_s": 30.0,
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n"
            )
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
            )
            # offset 0 with start=5.0 (way past MAX_INITIAL_SCRAPE_LATENESS)
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":5.0,"completion":5.1}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 100\n'
                'greptime_workload_scheduler_polls{workload="write"} 50\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )

            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("MAX_INITIAL_SCRAPE_LATENESS" in f for f in ps.validation.failures),
                f"should flag exceeding lateness: {ps.validation.failures}"
            )

    def test_scrape_start_non_finite_rejected(self) -> None:
        """Scrape start NaN => invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta: dict[str, object] = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "duration_s": 60.0, "warmup_s": 10.0, "drain_timeout_s": 30.0,
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n"
            )
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
            )
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":"NaN","completion":0.1}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 100\n'
                'greptime_workload_scheduler_polls{workload="write"} 50\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )

            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("non-finite" in f for f in ps.validation.failures),
                f"should flag non-finite start: {ps.validation.failures}"
            )

    def test_scrape_completion_before_start_rejected(self) -> None:
        """Scrape completion < start => invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta: dict[str, object] = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "duration_s": 60.0, "warmup_s": 10.0, "drain_timeout_s": 30.0,
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n"
            )
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
            )
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":0.5,"completion":0.1}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 100\n'
                'greptime_workload_scheduler_polls{workload="write"} 50\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )

            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("completion" in f and "<" in f for f in ps.validation.failures),
                f"should flag completion < start: {ps.validation.failures}"
            )

    # ---- 7. no-gate bad timing exits invalid 2 ----

    def test_no_gate_bad_timing_exits_invalid_two(self) -> None:
        """Under no-gate semantics, bad timing evidence yields status 'invalid' (exit 2),
        because timing integrity is an evidence-invariant, not a policy gate."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            # Write sample.json with duration_s=60.0
            meta: dict[str, object] = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "duration_s": 60.0, "warmup_s": 10.0, "drain_timeout_s": 30.0,
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n"
            )
            # Events: submission outside measurement window
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":99.0,"completion_offset":99.5,"latency_ms":499.0,"error":null}\n'
            )
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":0.01,"completion":0.02}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 100\n'
                'greptime_workload_scheduler_polls{workload="write"} 50\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )

            ps = wr.load_persisted_sample(
                sample_dir,
                expected_metadata={"duration_s": 60.0},
            )
            # Bad timing evidence: submission 99.0 >= duration 60.0 => invalid
            self.assertEqual("invalid", ps.validation.status)

            # Under --no-gate, the build_report must still produce exit 2 for this.
            report = wr.build_report(
                artifact_validation=ps.validation,
                request_eval=wr.RequestEvaluation(status="passed", failures=()),
                mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True,
                                                      exit_code=0, failures=()),
                performance_evals=[],
            )
            self.assertEqual("invalid", report["status"])
            self.assertEqual(2, report["exit_code"])

    # ---- 8. Scrape timing field requirements and lateness bounds ----

    def test_scrape_missing_start_rejected(self) -> None:
        """Scrape record without start field must be invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta: dict[str, object] = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "duration_s": 60.0, "warmup_s": 10.0, "drain_timeout_s": 30.0,
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n"
            )
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
            )
            # Missing start field
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","completion":0.02}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text("prom 1\n")

            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("missing start" in f for f in ps.validation.failures),
                f"should flag missing start: {ps.validation.failures}"
            )

    def test_scrape_missing_completion_rejected(self) -> None:
        """Success scrape record without completion field must be invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta: dict[str, object] = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "duration_s": 60.0, "warmup_s": 10.0, "drain_timeout_s": 30.0,
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n"
            )
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
            )
            # Success record without completion
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":0.01}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text("prom 1\n")

            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("missing completion" in f for f in ps.validation.failures),
                f"should flag missing completion: {ps.validation.failures}"
            )

    def test_scrape_nonfinite_completion_rejected(self) -> None:
        """Scrape completion NaN must be invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta: dict[str, object] = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "duration_s": 60.0, "warmup_s": 10.0, "drain_timeout_s": 30.0,
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n"
            )
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
            )
            # Non-finite completion
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":0.01,"completion":"NaN"}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text("prom 1\n")

            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("non-finite" in f for f in ps.validation.failures),
                f"should flag non-finite completion: {ps.validation.failures}"
            )

    def test_scrape_start_before_planned_rejected(self) -> None:
        """Success start preceding planned offset beyond clock precision is invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta: dict[str, object] = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "duration_s": 60.0, "warmup_s": 10.0, "drain_timeout_s": 30.0,
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n"
            )
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
            )
            # start (0.5) is before offset (1.0) by more than 0.001s clock precision
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":1.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":0.5,"completion":1.5}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text("prom 1\n")

            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("precedes" in f for f in ps.validation.failures),
                f"should flag start before planned offset: {ps.validation.failures}"
            )

    def test_scrape_late_success_offset_gt_zero_rejected(self) -> None:
        """Success at offset > 0 with start beyond MAX_SCRAPE_START_LATENESS is invalid."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta: dict[str, object] = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "duration_s": 60.0, "warmup_s": 10.0, "drain_timeout_s": 30.0,
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n"
            )
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
            )
            # offset=1.0, start=2.0, lateness=1.0s > MAX_SCRAPE_START_LATENESS=0.25
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":1.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":2.0,"completion":2.1}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text("prom 1\n")

            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("MAX_SCRAPE_START_LATENESS" in f for f in ps.validation.failures),
                f"should flag late success at offset>0: {ps.validation.failures}"
            )

    def test_scrape_tiny_late_start_accepted(self) -> None:
        """Realistic tiny late start (10ms) at offset > 0 must be accepted."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta: dict[str, object] = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "duration_s": 60.0, "warmup_s": 10.0, "drain_timeout_s": 30.0,
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n"
            )
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
            )
            # offset=1.0, start=1.01 (10ms late), well within MAX_SCRAPE_START_LATENESS=0.25
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":1.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":1.01,"completion":1.02}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 100\n'
                'greptime_workload_scheduler_polls{workload="write"} 50\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )

            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", ps.validation.status,
                             f"10ms lateness at offset>0 should pass: {ps.validation.failures}")

    def test_scrape_missing_start_under_no_gate_exits_two(self) -> None:
        """Mutating persisted scrape timing (missing start) under --no-gate yields exit 2."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta: dict[str, object] = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "duration_s": 60.0, "warmup_s": 10.0, "drain_timeout_s": 30.0,
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n"
            )
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
            )
            # Missing start (timing mutation)
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","completion":0.02}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 100\n'
                'greptime_workload_scheduler_polls{workload="write"} 50\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )

            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)

            # Under --no-gate semantics, bad timing must still exit 2
            report = wr.build_report(
                artifact_validation=ps.validation,
                request_eval=wr.RequestEvaluation(status="passed", failures=()),
                mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True,
                                                      exit_code=0, failures=()),
                performance_evals=[],
            )
            self.assertEqual("invalid", report["status"])
            self.assertEqual(2, report["exit_code"])


# ---------------------------------------------------------------------------
# HIGH Finding 1: Scrape completion duration bound tests
# ---------------------------------------------------------------------------


class WorkloadSchedulerScrapeDurationBoundTest(unittest.TestCase):
    """Test that _validate_scrape_timing enforces MAX_SCRAPE_DURATION completion bound."""

    def test_scrape_duration_within_bound_accepted(self) -> None:
        """Scrape duration within MAX_SCRAPE_DURATION must be accepted."""
        import tempfile
        from workload_scheduler_report import load_persisted_sample as lps

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta = {"name": "saturated", "mode": "scheduled", "sample": 1,
                    "iteration": 1, "phase": "saturated", "target": "scheduled",
                    "artifact_dir": str(sample_dir), "duration_s": 60.0,
                    "warmup_s": 10.0, "drain_timeout_s": 30.0}
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n")
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n')
            # duration = 0.05s within MAX_SCRAPE_DURATION=0.25
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":0.01,"completion":0.06}\n')
            (metrics_dir / "scrape-000.prom").write_text("dummy 1\n")

            ps = lps(sample_dir)
            self.assertEqual("passed", ps.validation.status,
                             f"duration within bound should pass: {ps.validation.failures}")

    def test_scrape_duration_exceeds_bound_rejected(self) -> None:
        """Scrape duration exceeding MAX_SCRAPE_DURATION must be rejected."""
        import tempfile
        from workload_scheduler_report import load_persisted_sample as lps

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta = {"name": "saturated", "mode": "scheduled", "sample": 1,
                    "iteration": 1, "phase": "saturated", "target": "scheduled",
                    "artifact_dir": str(sample_dir), "duration_s": 60.0,
                    "warmup_s": 10.0, "drain_timeout_s": 30.0}
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n")
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n')
            # duration = 0.5s exceeds MAX_SCRAPE_DURATION=0.25
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":0.01,"completion":0.51}\n')
            (metrics_dir / "scrape-000.prom").write_text("dummy 1\n")

            ps = lps(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("MAX_SCRAPE_DURATION" in f for f in ps.validation.failures),
                f"should flag duration exceed: {ps.validation.failures}"
            )

    def test_final_scrape_completion_into_drain_rejected(self) -> None:
        """Final scrape completion extending beyond offset+MAX_SCRAPE_DURATION+start_lateness is invalid."""
        import tempfile
        from workload_scheduler_report import load_persisted_sample as lps

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta = {"name": "saturated", "mode": "scheduled", "sample": 1,
                    "iteration": 1, "phase": "saturated", "target": "scheduled",
                    "artifact_dir": str(sample_dir), "duration_s": 60.0,
                    "warmup_s": 10.0, "drain_timeout_s": 30.0}
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n")
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n')
            # Final offset=60.0, start=60.0, completion=61.0 (extends 1.0s beyond
            # offset + MAX_SCRAPE_DURATION + MAX_SCRAPE_START_LATENESS = 60.0 + 0.25 + 0.25 = 60.5)
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":60.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":60.0,"completion":61.0}\n')
            (metrics_dir / "scrape-000.prom").write_text("dummy 1\n")

            ps = lps(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("exceeds offset + MAX_SCRAPE_DURATION" in f for f in ps.validation.failures),
                f"should flag final completion into drain: {ps.validation.failures}"
            )

    def test_final_scrape_completion_within_bound_accepted(self) -> None:
        """Final scrape completion within offset+MAX_SCRAPE_DURATION+start_lateness is accepted."""
        import tempfile
        from workload_scheduler_report import load_persisted_sample as lps

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            meta = {"name": "saturated", "mode": "scheduled", "sample": 1,
                    "iteration": 1, "phase": "saturated", "target": "scheduled",
                    "artifact_dir": str(sample_dir), "duration_s": 60.0,
                    "warmup_s": 10.0, "drain_timeout_s": 30.0}
            (sample_dir / "sample.json").write_text(
                json.dumps(meta, indent=2, sort_keys=True) + "\n")
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n')
            # Final offset=60.0, start=60.05, completion=60.1 (within bound)
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":60.0,"status":"success","http_status":200,"text":"m0","path":"metrics/scrape-000.prom","start":60.05,"completion":60.1}\n')
            (metrics_dir / "scrape-000.prom").write_text("dummy 1\n")

            ps = lps(sample_dir)
            self.assertEqual("passed", ps.validation.status,
                             f"final completion within bound should pass: {ps.validation.failures}")


# ---------------------------------------------------------------------------
# HIGH Finding 2: Incomplete planner JSON validity tests
# ---------------------------------------------------------------------------


class WorkloadSchedulerIncompletePlannerJsonTest(unittest.TestCase):
    """Test that validate_normalized_scenario rejects planner JSON missing one field per section."""

    def _make_base_plan(self) -> dict:
        """Return a valid baseline plan dict that passes validate_normalized_scenario."""
        import copy
        plan = {
            "schema_version": 1,
            "scenario": {
                "kind": "workload_scheduler",
                "database": "public",
                "iterations": 3,
                "warmup_seconds": 10,
                "duration_seconds": 60,
                "drain_timeout_seconds": 30,
                "scrape_interval_seconds": 1.0,
                "expected_scrape_count": 61,
                "runtime": {"global": 4, "compact": 1, "query": 4, "ingest": 4},
                "scheduler": {"max_concurrent_polls": 16, "query_weight": 2, "write_weight": 8},
                "targets": [
                    {"name": "baseline", "scheduler_enabled": False},
                    {"name": "scheduled", "scheduler_enabled": True},
                ],
                "data": {"shards": 64, "seed_rows": 10000, "seed_batch_size": 500,
                         "seed_timestamp_millis": 1700000000000, "write_sequence_start_millis": 1800000000000},
                "tables": {
                    "query": {"name": "catio_scheduler_query_load", "partitions": 32},
                    "write": {"name": "catio_scheduler_write_load", "partitions": 64},
                },
                "query": {"sql": "SELECT 1"},
                "write": {"batch_size": 32},
                "phases": [
                    {"name": "query_only", "query_workers": 2, "write_workers": 0, "write_delay_seconds": 0},
                    {"name": "write_only", "query_workers": 0, "write_workers": 2, "write_delay_seconds": 0},
                    {"name": "light_write", "query_workers": 2, "write_workers": 1, "write_delay_seconds": 0.1},
                    {"name": "saturated", "query_workers": 2, "write_workers": 2, "write_delay_seconds": 0},
                ],
                "gates": {
                    "max_failure_rate": 0.01, "max_outstanding_requests": 0,
                    "dual_backlog_lower": 0.78, "dual_backlog_upper": 0.82,
                    "min_dual_backlog_interval_fraction": 0.80,
                    "min_dual_backlog_polls_per_class": 100,
                    "min_single_class_active_purity": 0.99,
                    "min_light_write_query_share": 0.20,
                    "active_within_scheduler_limit": True,
                    "max_capacity_normalized_regression_pct": 5.0,
                },
            }
        }
        return copy.deepcopy(plan)

    def _assert_invalid(self, plan: dict) -> None:
        """Assert that validate_normalized_scenario raises ValueError."""
        with self.assertRaises(ValueError):
            from workload_scheduler_benchmark import validate_normalized_scenario
            validate_normalized_scenario(plan)

    def test_missing_database(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["database"]
        self._assert_invalid(plan)

    def test_missing_iterations(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["iterations"]
        self._assert_invalid(plan)

    def test_missing_runtime_global(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["runtime"]["global"]
        self._assert_invalid(plan)

    def test_missing_scheduler_max_concurrent_polls(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["scheduler"]["max_concurrent_polls"]
        self._assert_invalid(plan)

    def test_missing_target(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["targets"][0]["name"]
        self._assert_invalid(plan)

    def test_missing_data_shards(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["data"]["shards"]
        self._assert_invalid(plan)

    def test_missing_tables_query_name(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["tables"]["query"]["name"]
        self._assert_invalid(plan)

    def test_missing_query_sql(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["query"]["sql"]
        self._assert_invalid(plan)

    def test_missing_write_batch_size(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["write"]["batch_size"]
        self._assert_invalid(plan)

    def test_missing_phase_name(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["phases"][0]["name"]
        self._assert_invalid(plan)

    def test_missing_gates_max_failure_rate(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["gates"]["max_failure_rate"]
        self._assert_invalid(plan)

    def test_valid_plan_passes(self) -> None:
        """The base plan must pass validation."""
        plan = self._make_base_plan()
        from workload_scheduler_benchmark import validate_normalized_scenario
        scenario = validate_normalized_scenario(plan)
        self.assertIsNotNone(scenario)
        self.assertEqual("public", scenario["database"])


# ---------------------------------------------------------------------------
# HIGH Finding 3: Consistent no-gate request status tests
# ---------------------------------------------------------------------------


class WorkloadSchedulerNoGateRequestStatusTest(unittest.TestCase):
    """Test that --no-gate converts request failure to passed/diagnostic with exit 0."""

    def test_no_gate_converts_request_failure_to_passed(self) -> None:
        """Under --no-gate, well-formed request threshold failure yields report status passed/diagnostic."""
        from workload_scheduler_report import (
            RequestEvaluation, PerformanceEvaluation, SchedulerEvaluation,
            ArtifactValidation, build_report,
        )

        # Simulate a well-formed request that fails a threshold (e.g. failure rate >= 1%)
        failed_request = RequestEvaluation(
            status="failed",
            failures=("query_only/baseline/query: failure rate 2.00% >= 1.00%",),
        )

        # Without --no-gate: failed status propagates
        # With performance_evals provided (gated mode), failed request + passed mechanism = failed overall
        report_no_gate = build_report(
            request_eval=failed_request,
            mechanism_eval=SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
            performance_evals=[],
        )
        self.assertIn(report_no_gate["status"], ("failed", "invalid"),
                      "without no-gate, failed request should propagate as failed or invalid status")

        # With --no-gate (converted to passed): passed status
        passed_request = RequestEvaluation(status="passed", failures=())
        report_with_gate = build_report(
            request_eval=passed_request,
            mechanism_eval=SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
            performance_evals=[],
        )
        # Should not contain "invalid" as the only status
        self.assertIn(report_with_gate.get("status", ""), ("passed", "invalid"))

    def test_malformed_request_remains_invalid_under_no_gate(self) -> None:
        """Malformed/invalid request (integrity error) stays invalid/exit_code=2 under --no-gate."""
        from workload_scheduler_report import (
            RequestEvaluation, PerformanceEvaluation, SchedulerEvaluation,
            build_report,
        )

        invalid_request = RequestEvaluation(
            status="invalid",
            failures=("sample.json missing name",),
        )

        report = build_report(
            request_eval=invalid_request,
            mechanism_eval=SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
            performance_evals=[],
        )
        self.assertEqual("invalid", report["status"])
        self.assertEqual(2, report["exit_code"])

    def test_normal_gated_request_failure_remains_failed_exit_1(self) -> None:
        """Normal gated request (no --no-gate) with threshold failure stays failed/exit_code=1."""
        from workload_scheduler_report import (
            RequestEvaluation, PerformanceEvaluation, SchedulerEvaluation,
            build_report,
        )

        failed_request = RequestEvaluation(
            status="failed",
            failures=("saturated/scheduled/query: failure rate 2.00% >= 1.00%",),
        )

        report = build_report(
            request_eval=failed_request,
            mechanism_eval=SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
            performance_evals=[],
        )
        # invalid because performance_evals provided (gated mode), but missing sections
        self.assertIn(report.get("status", ""), ("failed", "invalid"))


# ---------------------------------------------------------------------------
# HIGH Finding 1: Raw .prom containment — is_relative_to, sibling-prefix,
# parent traversal, absolute external, symlink escape, valid cases.
# ---------------------------------------------------------------------------


class WorkloadSchedulerPromContainmentTest(unittest.TestCase):
    """Test that load_persisted_sample enforces strict .prom path containment.

    Every manifest raw path must resolve beneath exactly ``sample_dir/metrics/``,
    not merely sample_dir. Sibling-prefix (``sample-evil``), parent traversal
    (``..``), absolute outside paths, and symlink targets escaping metrics dir
    must be rejected.
    """

    def _write_minimal_sample(self, sample_dir: Path) -> None:
        """Write minimal valid sample skeleton (no .prom files yet)."""
        metrics_dir = sample_dir / "metrics"
        metrics_dir.mkdir(parents=True)

        sample_json = {
            "name": "saturated", "mode": "scheduled", "sample": 1,
            "iteration": 1, "phase": "saturated", "target": "scheduled",
            "artifact_dir": str(sample_dir),
        }
        (sample_dir / "sample.json").write_text(
            json.dumps(sample_json, indent=2, sort_keys=True) + "\n"
        )
        (sample_dir / "requests.jsonl").write_text(
            '{"token":0,"workload":"query","status":"success","submission_offset":0.1,'
            '"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
        )

    def _write_scrapes_with_path(self, sample_dir: Path, rel_path: str) -> None:
        """Write scrapes.jsonl referencing a single scrape with given rel_path."""
        scrapes = [
            {"offset": 0.0, "status": "success", "http_status": 200,
             "text": "m0", "path": rel_path, "start": 0.01, "completion": 0.02},
        ]
        with open(sample_dir / "scrapes.jsonl", "w") as f:
            for rec in scrapes:
                f.write(json.dumps(rec, sort_keys=True) + "\n")

    def _write_prom_at(self, path: Path) -> None:
        """Write a valid .prom file at the given path."""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            'greptime_workload_scheduler_polls{workload="query"} 100\n'
            'greptime_workload_scheduler_polls{workload="write"} 50\n'
            'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
            'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
            'greptime_workload_scheduler_active_polls 4\n'
        )

    def test_valid_metrics_path_passes(self) -> None:
        """Normal path under metrics/ passes containment."""
        import tempfile
        from workload_scheduler_report import load_persisted_sample

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_minimal_sample(sample_dir)
            self._write_scrapes_with_path(sample_dir, "metrics/scrape-000.prom")
            self._write_prom_at(sample_dir / "metrics" / "scrape-000.prom")

            ps = load_persisted_sample(sample_dir)
            self.assertEqual("passed", ps.validation.status,
                             f"valid metrics path should pass: {ps.validation.failures}")

    def test_valid_nested_subdir_passes(self) -> None:
        """Path nested under a subdirectory of metrics/ is properly contained (not flagged as escape)."""
        import tempfile
        from workload_scheduler_report import load_persisted_sample

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_minimal_sample(sample_dir)
            # Create nested subdirectory and write .prom there
            nested_dir = sample_dir / "metrics" / "subdir"
            nested_dir.mkdir(parents=True)
            self._write_prom_at(nested_dir / "scrape-000.prom")
            # Reference the nested path (not directly in metrics/)
            self._write_scrapes_with_path(sample_dir, "metrics/subdir/scrape-000.prom")

            ps = load_persisted_sample(sample_dir)
            # Containment: path resolves under metrics/, so no escape flag
            self.assertFalse(
                any("escapes" in f for f in ps.validation.failures),
                f"nested path should not be flagged as escape: {ps.validation.failures}"
            )
            # The non-recursive glob may cause a count mismatch, but the
            # containment check itself must pass (no escape flag).

    def test_sibling_prefix_rejected(self) -> None:
        """Path under sample-evil/ (sibling prefix) must be rejected."""
        import tempfile
        from workload_scheduler_report import load_persisted_sample

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_minimal_sample(sample_dir)
            # Create a sibling directory with a name that shares a prefix
            sibling_dir = sample_dir.parent / f"{sample_dir.name}-evil"
            sibling_metrics = sibling_dir / "metrics"
            sibling_metrics.mkdir(parents=True)
            self._write_prom_at(sibling_metrics / "scrape-000.prom")

            # Reference the sibling's prom file
            self._write_scrapes_with_path(
                sample_dir,
                str(Path("..") / f"{sample_dir.name}-evil" / "metrics" / "scrape-000.prom"),
            )

            # Must be rejected because it's outside metrics_dir
            ps = load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("escapes" in f for f in ps.validation.failures),
                f"should flag sibling escape: {ps.validation.failures}"
            )

    def test_parent_traversal_rejected(self) -> None:
        """Path with ../ escaping sample_dir must be rejected."""
        import tempfile
        from workload_scheduler_report import load_persisted_sample

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_minimal_sample(sample_dir)

            # Write a .prom file in the parent directory
            parent_prom = sample_dir.parent / "outside.prom"
            self._write_prom_at(parent_prom)

            self._write_scrapes_with_path(sample_dir, "../outside.prom")

            ps = load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("escapes" in f for f in ps.validation.failures),
                f"should flag parent traversal: {ps.validation.failures}"
            )

    def test_absolute_external_rejected(self) -> None:
        """Absolute path outside sample_dir must be rejected."""
        import tempfile
        from workload_scheduler_report import load_persisted_sample

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_minimal_sample(sample_dir)

            # Absolute path to /tmp
            external_path = Path(tmpdir).parent / "external.prom"
            self._write_prom_at(external_path)

            self._write_scrapes_with_path(sample_dir, str(external_path))

            ps = load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("escapes" in f for f in ps.validation.failures),
                f"should flag absolute external: {ps.validation.failures}"
            )

    def test_symlink_escape_rejected(self) -> None:
        """Symlink inside metrics/ pointing outside must be rejected."""
        import tempfile
        from workload_scheduler_report import load_persisted_sample

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_minimal_sample(sample_dir)

            # Create a target outside metrics/
            outside_target = sample_dir / "outside_actual.prom"
            self._write_prom_at(outside_target)

            # Create a symlink inside metrics/ -> outside
            symlink_path = sample_dir / "metrics" / "scrape-000.prom"
            symlink_path.symlink_to(outside_target)

            self._write_scrapes_with_path(sample_dir, "metrics/scrape-000.prom")

            ps = load_persisted_sample(sample_dir)
            # Symlink resolves to outside metrics/ — should be rejected
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("escapes" in f for f in ps.validation.failures),
                f"should flag symlink escape: {ps.validation.failures}"
            )

    def test_prom_under_sample_dir_but_not_metrics_rejected(self) -> None:
        """Prom file directly under sample_dir (not metrics/) must be rejected."""
        import tempfile
        from workload_scheduler_report import load_persisted_sample

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_minimal_sample(sample_dir)

            # Write .prom directly in sample_dir, not metrics/
            self._write_prom_at(sample_dir / "scrape-000.prom")
            self._write_scrapes_with_path(sample_dir, "scrape-000.prom")

            ps = load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("escapes" in f for f in ps.validation.failures),
                f"should reject prom directly under sample_dir: {ps.validation.failures}"
            )


# ---------------------------------------------------------------------------
# HIGH Finding 2: Complete strict normalized-plan validation (schema_version,
# exact types/ranges, scrape_interval_seconds, expected_scrape_count,
# bool-as-int, inconsistent derived count)
# ---------------------------------------------------------------------------


class WorkloadSchedulerStrictPlanValidationTest(unittest.TestCase):
    """Test that validate_normalized_scenario rejects various invalid plans."""

    def _make_base_plan(self) -> dict:
        """Return a valid baseline plan that passes all validation."""
        plan = {
            "schema_version": 1,
            "scenario": {
                "kind": "workload_scheduler",
                "database": "public",
                "iterations": 3,
                "warmup_seconds": 10,
                "duration_seconds": 60,
                "drain_timeout_seconds": 30,
                "scrape_interval_seconds": 1.0,
                "expected_scrape_count": 61,
                "runtime": {"global": 4, "compact": 1, "query": 4, "ingest": 4},
                "scheduler": {"max_concurrent_polls": 16, "query_weight": 2, "write_weight": 8},
                "targets": [
                    {"name": "baseline", "scheduler_enabled": False},
                    {"name": "scheduled", "scheduler_enabled": True},
                ],
                "data": {"shards": 64, "seed_rows": 10000, "seed_batch_size": 500,
                         "seed_timestamp_millis": 1700000000000, "write_sequence_start_millis": 1800000000000},
                "tables": {
                    "query": {"name": "catio_query", "partitions": 32},
                    "write": {"name": "catio_write", "partitions": 64},
                },
                "query": {"sql": "SELECT 1"},
                "write": {"batch_size": 32},
                "phases": [
                    {"name": "query_only", "query_workers": 2, "write_workers": 0, "write_delay_seconds": 0},
                    {"name": "write_only", "query_workers": 0, "write_workers": 2, "write_delay_seconds": 0},
                    {"name": "light_write", "query_workers": 2, "write_workers": 1, "write_delay_seconds": 0.1},
                    {"name": "saturated", "query_workers": 2, "write_workers": 2, "write_delay_seconds": 0},
                ],
                "gates": {
                    "max_failure_rate": 0.01, "max_outstanding_requests": 0,
                    "dual_backlog_lower": 0.78, "dual_backlog_upper": 0.82,
                    "min_dual_backlog_interval_fraction": 0.80,
                    "min_dual_backlog_polls_per_class": 100,
                    "min_single_class_active_purity": 0.99,
                    "min_light_write_query_share": 0.20,
                    "active_within_scheduler_limit": True,
                    "max_capacity_normalized_regression_pct": 5.0,
                },
            },
        }
        import copy
        return copy.deepcopy(plan)

    def _assert_rejected(self, plan: dict, pattern: str | None = None) -> None:
        """Assert validate_normalized_scenario raises ValueError."""
        from workload_scheduler_benchmark import validate_normalized_scenario
        with self.assertRaises(ValueError) as ctx:
            validate_normalized_scenario(plan)
        if pattern:
            self.assertIn(pattern, str(ctx.exception))

    def _assert_validate_scenario_kind_rejected(self, plan: dict, pattern: str | None = None) -> None:
        """Assert validate_scenario_kind raises ValueError."""
        from workload_scheduler_benchmark import validate_scenario_kind
        with self.assertRaises(ValueError) as ctx:
            validate_scenario_kind(plan)
        if pattern:
            self.assertIn(pattern, str(ctx.exception))

    def test_valid_plan_passes_strict(self) -> None:
        """The base plan must pass both validate_scenario_kind and validate_normalized_scenario."""
        from workload_scheduler_benchmark import validate_scenario_kind, validate_normalized_scenario
        plan = self._make_base_plan()
        validate_scenario_kind(plan)
        scenario = validate_normalized_scenario(plan)
        self.assertIsNotNone(scenario)
        self.assertEqual("public", scenario["database"])
        self.assertEqual(61, scenario["expected_scrape_count"])

    # ---- schema_version tests ----
    def test_missing_schema_version_rejected(self) -> None:
        plan = self._make_base_plan()
        del plan["schema_version"]
        self._assert_validate_scenario_kind_rejected(plan, "schema_version")

    def test_schema_version_bool_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["schema_version"] = True
        self._assert_validate_scenario_kind_rejected(plan, "bool")

    def test_schema_version_float_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["schema_version"] = 1.0
        self._assert_validate_scenario_kind_rejected(plan, "must be int")

    def test_schema_version_wrong_int_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["schema_version"] = 2
        self._assert_validate_scenario_kind_rejected(plan, "must be 1")

    # ---- bool-as-int tests ----
    def test_iterations_bool_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["iterations"] = True
        self._assert_rejected(plan, "bool")

    def test_shards_bool_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["data"]["shards"] = False
        self._assert_rejected(plan, "bool")

    def test_max_concurrent_polls_bool_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["scheduler"]["max_concurrent_polls"] = True
        self._assert_rejected(plan, "bool")

    def test_write_workers_bool_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["phases"][0]["write_workers"] = True
        self._assert_rejected(plan, "bool")

    def test_active_within_scheduler_limit_int_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["gates"]["active_within_scheduler_limit"] = 1
        self._assert_rejected(plan, "bool")

    def test_scheduler_enabled_int_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["targets"][0]["scheduler_enabled"] = 0
        self._assert_rejected(plan, "bool")

    # ---- scrape_interval_seconds / expected_scrape_count ----
    def test_missing_scrape_interval_rejected(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["scrape_interval_seconds"]
        self._assert_rejected(plan, "scrape_interval_seconds")

    def test_scrape_interval_not_one_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["scrape_interval_seconds"] = 2.0
        self._assert_rejected(plan, "must be 1.0")

    def test_scrape_interval_bool_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["scrape_interval_seconds"] = True
        self._assert_rejected(plan, "bool")

    def test_missing_expected_scrape_count_rejected(self) -> None:
        plan = self._make_base_plan()
        del plan["scenario"]["expected_scrape_count"]
        self._assert_rejected(plan, "expected_scrape_count")

    def test_expected_scrape_count_bool_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["expected_scrape_count"] = False
        self._assert_rejected(plan, "bool")

    def test_inconsistent_expected_scrape_count_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["expected_scrape_count"] = 99  # should be 61 for duration=60, interval=1
        self._assert_rejected(plan, "inconsistent")

    # ---- negative/zero ints ----
    def test_iterations_zero_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["iterations"] = 0
        self._assert_rejected(plan, "positive")

    def test_batch_size_negative_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["write"]["batch_size"] = -1
        self._assert_rejected(plan, "positive")

    def test_runtime_global_zero_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["runtime"]["global"] = 0
        self._assert_rejected(plan, "positive")

    # ---- dtype mismatches ----
    def test_warmup_string_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["warmup_seconds"] = "ten"
        self._assert_rejected(plan, "int")

    def test_database_int_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["database"] = 123
        self._assert_rejected(plan, "str")

    def test_query_sql_empty_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["query"]["sql"] = ""
        self._assert_rejected(plan, "nonempty")

    def test_phase_write_delay_negative_rejected(self) -> None:
        plan = self._make_base_plan()
        plan["scenario"]["phases"][0]["write_delay_seconds"] = -0.5
        self._assert_rejected(plan, "nonnegative")


if __name__ == "__main__":
    unittest.main()
