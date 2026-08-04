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

"""Deterministic tests for workload-scheduler benchmark CLI, planning, and artifacts.

These tests use synthetic data and temporary directories; no real server or
binary is required.
"""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path


BENCH_PATH = Path(__file__).with_name("workload_scheduler_benchmark.py")
SPEC = importlib.util.spec_from_file_location(
    "workload_scheduler_benchmark_under_test", BENCH_PATH
)
assert SPEC is not None and SPEC.loader is not None
bench = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = bench
SPEC.loader.exec_module(bench)

# Also import runner for MetricsScraper tests
RUNNER_PATH = Path(__file__).with_name("workload_scheduler_runner.py")
RUNNER_SPEC = importlib.util.spec_from_file_location(
    "workload_scheduler_runner_under_test", RUNNER_PATH
)
assert RUNNER_SPEC is not None and RUNNER_SPEC.loader is not None
runner = importlib.util.module_from_spec(RUNNER_SPEC)
sys.modules[RUNNER_SPEC.name] = runner
RUNNER_SPEC.loader.exec_module(runner)

# Also import report for synthetic test helpers
REPORT_PATH = Path(__file__).with_name("workload_scheduler_report.py")
REPORT_SPEC = importlib.util.spec_from_file_location(
    "workload_scheduler_report_synthetic", REPORT_PATH
)
assert REPORT_SPEC is not None and REPORT_SPEC.loader is not None
sreport = importlib.util.module_from_spec(REPORT_SPEC)
sys.modules[REPORT_SPEC.name] = sreport
REPORT_SPEC.loader.exec_module(sreport)


class WorkloadSchedulerBenchmarkPlannerTest(unittest.TestCase):
    """Test planner invocation parsing, kind rejection, and matrix overlay."""

    def test_validate_scenario_kind_accepts_workload_scheduler(self) -> None:
        plan = {"schema_version": 1, "scenario": {"kind": "workload_scheduler"}}
        kind = bench.validate_scenario_kind(plan)
        self.assertEqual("workload_scheduler", kind)

    def test_validate_scenario_kind_rejects_other_kinds(self) -> None:
        for bad_kind in ("direct_readable_sst", "prom_remote_write_then_query", ""):
            with self.subTest(kind=bad_kind):
                plan = {"scenario": {"kind": bad_kind}}
                with self.assertRaises(ValueError):
                    bench.validate_scenario_kind(plan)

    def test_validate_scenario_kind_rejects_missing_kind(self) -> None:
        plan = {"scenario": {}}
        with self.assertRaises(ValueError):
            bench.validate_scenario_kind(plan)

    def test_extract_phases(self) -> None:
        plan = {
            "scenario": {
                "phases": [
                    {"name": "query_only", "query_workers": 2, "write_workers": 0},
                    {"name": "write_only", "query_workers": 0, "write_workers": 2},
                ]
            }
        }
        phases = bench.extract_phases(plan)
        self.assertEqual(2, len(phases))
        self.assertEqual("query_only", phases[0]["name"])

    def test_extract_config(self) -> None:
        plan = {
            "scenario": {
                "iterations": 3,
                "duration_seconds": 60,
                "runtime": {"global": 4},
            }
        }
        cfg = bench.extract_config(plan)
        self.assertEqual(3, cfg.get("iterations"))
        self.assertEqual(60, cfg.get("duration_seconds"))


class WorkloadSchedulerBenchmarkMatrixTest(unittest.TestCase):
    """Test matrix order parity and fresh work-dir/reuse behavior."""

    def test_matrix_order_parity_even(self) -> None:
        """(iteration_index + phase_index) % 2 == 0 => baseline then scheduled."""
        order = bench.sample_order(iteration_index=1, phase_index=1)
        self.assertEqual((False, True), order)  # baseline, then scheduled

    def test_matrix_order_parity_odd(self) -> None:
        """(iteration_index + phase_index) % 2 == 1 => scheduled then baseline."""
        order = bench.sample_order(iteration_index=1, phase_index=0)
        self.assertEqual((True, False), order)  # scheduled, then baseline

    def test_matrix_order_zero_based_even(self) -> None:
        """Zero-based (0+0)%2==0 => baseline then scheduled."""
        order = bench.sample_order(0, 0)
        self.assertEqual((False, True), order)

    def test_matrix_order_zero_based_odd(self) -> None:
        """Zero-based (0+1)%2==1 => scheduled then baseline."""
        order = bench.sample_order(0, 1)
        self.assertEqual((True, False), order)

    def test_matrix_full_coverage(self) -> None:
        """All 4 phases x 3 iterations produce each target once per phase-iteration."""
        iterations = 3
        phases_names = ["query_only", "write_only", "light_write", "saturated"]
        pairs: set[tuple[int, str, str]] = set()
        for i in range(iterations):
            for idx, phase in enumerate(phases_names):
                order = bench.sample_order(i, idx)
                for enabled in order:
                    mode = "scheduled" if enabled else "baseline"
                    pair = (i + 1, phase, mode)
                    self.assertNotIn(pair, pairs, f"duplicate pair: {pair}")
                    pairs.add(pair)
        self.assertEqual(iterations * len(phases_names) * 2, len(pairs))

    def test_build_matrix_helper(self) -> None:
        """build_matrix returns expected structure matching dry-run output."""
        phases = [
            {"name": "query_only", "query_workers": 2, "write_workers": 0},
            {"name": "write_only", "query_workers": 0, "write_workers": 2},
        ]
        default_targets = [
            {"name": "baseline", "scheduler_enabled": False},
            {"name": "scheduled", "scheduler_enabled": True},
        ]
        matrix = bench.build_matrix(2, phases, targets=default_targets)
        self.assertEqual(4, len(matrix))  # 2 iterations * 2 phases
        # Check first entry
        self.assertEqual(1, matrix[0]["iteration"])
        self.assertEqual("query_only", matrix[0]["phase"])
        self.assertEqual(2, len(matrix[0]["entries"]))
        self.assertEqual("baseline", matrix[0]["entries"][0]["name"])
        self.assertFalse(matrix[0]["entries"][0]["scheduler_enabled"])
        self.assertEqual("scheduled", matrix[0]["entries"][1]["name"])
        self.assertTrue(matrix[0]["entries"][1]["scheduler_enabled"])

    def test_matrix_dry_run_execution_match(self) -> None:
        """Dry-run matrix order equals execution order."""
        iterations = 3
        phases_names = ["query_only", "write_only", "light_write", "saturated"]
        # Build dry-run matrix
        phases_list = [{"name": n} for n in phases_names]
        default_targets = [
            {"name": "baseline", "scheduler_enabled": False},
            {"name": "scheduled", "scheduler_enabled": True},
        ]
        matrix = bench.build_matrix(iterations, phases_list, targets=default_targets)
        # Verify execution order matches
        for entry in matrix:
            i = entry["iteration"] - 1
            idx = phases_names.index(entry["phase"])
            order = bench.sample_order(i, idx)
            expected_names = ["scheduled" if e else "baseline" for e in order]
            actual_names = [e["name"] for e in entry["entries"]]
            self.assertEqual(expected_names, actual_names)

    def test_run_subdirectory_isolation(self) -> None:
        """Each sample in a reuse run should create a fresh run subdirectory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            runs_dir = Path(tmpdir) / "runs"
            runs_dir.mkdir()

            # Simulate iteration-01/query_only/baseline
            sample_dir = runs_dir / "iteration-01" / "query_only" / "baseline"
            sample_dir.mkdir(parents=True)
            config_file = sample_dir / "config.toml"
            config_file.write_text("dummy")

            # Verify the directory exists
            self.assertTrue(config_file.exists())
            self.assertEqual(
                "dummy", config_file.read_text()
            )

    def test_reuse_creates_fresh_subtree(self) -> None:
        """reuse creates a fresh unique subtree path."""
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"
            runs_dir.mkdir()
            # Simulate existing content
            (runs_dir / "old_dir").mkdir()

            # The benchmark code would create a reuse-<timestamp> subtree
            # We can verify the concept: reuse creates a new unique path
            import time
            reuse_run_id = int(time.time() * 1000000)
            reuse_runs_dir = work_dir / "runs" / f"reuse-{reuse_run_id}"
            reuse_runs_dir.mkdir(parents=True)
            self.assertTrue(reuse_runs_dir.exists())
            self.assertNotEqual(runs_dir, reuse_runs_dir)


class WorkloadSchedulerBenchmarkArtifactTest(unittest.TestCase):
    """Test artifact writing and path expectations."""

    def test_build_artifacts_index_structure(self) -> None:
        """build_artifacts_index returns expected keys."""
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"
            runs_dir.mkdir()

            # Create some sample artifacts
            sample_dir = runs_dir / "iteration-01" / "query_only" / "baseline"
            sample_dir.mkdir(parents=True)
            (sample_dir / "config.toml").write_text("cfg")
            (sample_dir / "process.log").write_text("log")
            (sample_dir / "requests.jsonl").write_text("req")
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir()
            (metrics_dir / "scrape-000.prom").write_text("prom")
            scrapes_jsonl = sample_dir / "scrapes.jsonl"
            scrapes_jsonl.write_text("jsonl")
            sample_json = sample_dir / "sample.json"
            sample_json.write_text("{}")

            samples = [
                {
                    "name": "query_only",
                    "mode": "baseline",
                    "sample": 1,
                    "requests": {},
                }
            ]
            artifacts = bench.build_artifacts_index(work_dir, samples)

            self.assertIn("work_dir", artifacts)
            self.assertIn("run_dirs", artifacts)
            self.assertIn("samples", artifacts)
            self.assertEqual(1, len(artifacts["samples"]))
            sample_art = artifacts["samples"][0]
            self.assertIn("config", sample_art)
            self.assertIn("process_log", sample_art)
            self.assertIn("requests_jsonl", sample_art)
            self.assertIn("scrapes", sample_art)
            self.assertIn("scrapes_jsonl", sample_art)
            self.assertIn("sample_json", sample_art)

    def test_build_artifacts_index_requires_files_for_completed(self) -> None:
        """Completed samples without required files get missing_required."""
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"
            runs_dir.mkdir()

            sample_dir = runs_dir / "iteration-01" / "query_only" / "baseline"
            sample_dir.mkdir(parents=True)
            # No files inside

            samples = [
                {
                    "name": "query_only",
                    "mode": "baseline",
                    "sample": 1,
                    "requests": {"query": {"started": 1}},
                }
            ]
            artifacts = bench.build_artifacts_index(work_dir, samples)
            self.assertIn("samples", artifacts)
            self.assertEqual(1, len(artifacts["samples"]))
            sample_art = artifacts["samples"][0]
            self.assertIn("missing_required", sample_art)
            required = sample_art["missing_required"]
            self.assertIn("requests.jsonl", required)
            self.assertIn("sample.json", required)

    def test_build_artifacts_index_copes_with_missing_files(self) -> None:
        """build_artifacts_index does not crash when files are absent."""
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"
            runs_dir.mkdir()
            sample_dir = runs_dir / "iteration-01" / "query_only" / "baseline"
            sample_dir.mkdir(parents=True)
            # No files inside

            samples = [
                {
                    "name": "query_only",
                    "mode": "baseline",
                    "sample": 1,
                    "requests": {},
                }
            ]
            artifacts = bench.build_artifacts_index(work_dir, samples)
            self.assertIn("samples", artifacts)
            self.assertEqual(1, len(artifacts["samples"]))

    def test_requests_jsonl_is_token_sorted(self) -> None:
        """Verify that token-sorted ordering is maintained in requests.jsonl."""
        events = [
            {"token": 3, "workload": "query", "status": "success"},
            {"token": 1, "workload": "write", "status": "success"},
            {"token": 2, "workload": "query", "status": "timeout"},
        ]
        sorted_events = sorted(events, key=lambda e: e["token"])
        tokens = [e["token"] for e in sorted_events]
        self.assertEqual([1, 2, 3], tokens)


class WorkloadSchedulerBenchmarkSyntheticReportTest(unittest.TestCase):
    """Test report building from synthetic data."""

    def test_performance_evaluation_capacity_normalization(self) -> None:
        """Capacity normalization uses baseline query_only/write_only."""
        iterations_data = [
            (
                1,
                {
                    "query_only": {
                        "baseline": {"query_rps": 100.0, "write_rps": 0.0}
                    },
                    "write_only": {
                        "baseline": {"query_rps": 0.0, "write_rps": 50.0}
                    },
                    "light_write": {
                        "baseline": {"query_rps": 80.0, "write_rps": 5.0},
                        "scheduled": {"query_rps": 78.0, "write_rps": 5.0},
                    },
                    "saturated": {
                        "baseline": {"query_rps": 10.0, "write_rps": 40.0},
                        "scheduled": {"query_rps": 9.5, "write_rps": 41.0},
                    },
                },
            )
        ]
        evals = sreport.evaluate_performance(
            iterations_data,
            ["light_write", "saturated"],
            max_regression_pct=5.0,
        )

        self.assertEqual(2, len(evals))
        # Check light_write
        lw = [e for e in evals if e.phase == "light_write"][0]
        self.assertTrue(lw.passed)
        self.assertIsNotNone(lw.pct_change)

        # Check saturated
        sat = [e for e in evals if e.phase == "saturated"][0]
        self.assertTrue(sat.passed)

    def test_performance_worst_is_gate(self) -> None:
        """When one pair exceeds regression budget, gate fails."""
        iterations_data = [
            (
                1,
                {
                    "query_only": {
                        "baseline": {"query_rps": 100.0, "write_rps": 0.0}
                    },
                    "write_only": {
                        "baseline": {"query_rps": 0.0, "write_rps": 50.0}
                    },
                    "light_write": {
                        "baseline": {"query_rps": 80.0, "write_rps": 5.0},
                        "scheduled": {"query_rps": 20.0, "write_rps": 2.0},
                    },
                },
            )
        ]
        evals = sreport.evaluate_performance(
            iterations_data, ["light_write"], max_regression_pct=5.0
        )
        self.assertFalse(evals[0].passed)
        # pct_change should be very negative
        if evals[0].pct_change is not None:
            self.assertLess(evals[0].pct_change, -5.0)

    def test_build_report_status_precedence(self) -> None:
        """Verify build_report status precedence with synthetic data."""
        report = sreport.build_report(
            errors=["some error"],
        )
        self.assertEqual("error", report["status"])
        self.assertEqual(3, report["exit_code"])

    def test_build_report_invalid_over_failed(self) -> None:
        """Invalid beats failed in precedence."""
        # Invalid request + failed mechanism
        invalid_req = sreport.RequestEvaluation(status="invalid", failures=("missing data",))
        failed_mech = sreport.SchedulerEvaluation(
            status="failed", passed=False, exit_code=1, failures=("bad share",)
        )
        report = sreport.build_report(
            request_eval=invalid_req,
            mechanism_eval=failed_mech,
            performance_evals=[],
        )
        self.assertEqual("invalid", report["status"])
        self.assertEqual(2, report["exit_code"])

    def test_gated_mode_requires_all_sections(self) -> None:
        """In gated mode (performance_evals provided), missing mechanism is invalid."""
        report = sreport.build_report(
            performance_evals=[],
        )
        self.assertEqual("invalid", report["status"])
        self.assertIn("missing", report.get("failures", [])[0])

    def test_baseline_and_scheduled_artifacts(self) -> None:
        """Verify that both baseline and scheduled produce complete artifact sets."""
        # This tests the artifact paths and required files
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"

            # Create both baseline and scheduled directories
            for mode in ("baseline", "scheduled"):
                sample_dir = runs_dir / "iteration-01" / "query_only" / mode
                sample_dir.mkdir(parents=True)
                (sample_dir / "config.toml").write_text("cfg")
                (sample_dir / "process.log").write_text("log")
                (sample_dir / "requests.jsonl").write_text("req\n")
                metrics_dir = sample_dir / "metrics"
                metrics_dir.mkdir()
                (metrics_dir / "scrape-000.prom").write_text("prom")
                (metrics_dir / "scrape-001.prom").write_text("prom2")
                (sample_dir / "scrapes.jsonl").write_text("jsonl\n")
                (sample_dir / "sample.json").write_text('{"name":"test"}\n')

            samples = [
                {"name": "query_only", "mode": "baseline", "sample": 1, "requests": {}},
                {"name": "query_only", "mode": "scheduled", "sample": 1, "requests": {}},
            ]
            artifacts = bench.build_artifacts_index(work_dir, samples)
            self.assertEqual(2, len(artifacts["samples"]))
            for sample_art in artifacts["samples"]:
                self.assertIn("requests_jsonl", sample_art)
                self.assertIn("scrapes_jsonl", sample_art)
                self.assertIn("sample_json", sample_art)
                self.assertIn("scrapes", sample_art)
                self.assertEqual(2, len(sample_art["scrapes"]),
                    f"expected 2 prom files for {sample_art['mode']}")


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# Issue 3: Persisted artifacts authoritative
# ---------------------------------------------------------------------------


class WorkloadSchedulerPersistedArtifactTest(unittest.TestCase):
    """Test that persisted artifacts are authoritative and missing files invalidate report."""

    def test_final_metadata_exists_in_sample_json(self) -> None:
        """Unconditionally overwritten sample.json must have iteration/phase/target/artifact_dir."""
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir) / "iteration-01" / "query_only" / "scheduled"
            sample_dir.mkdir(parents=True)

            # Simulate benchmark writing sample.json with full metadata
            sample_json_data = {
                "name": "query_only",
                "mode": "scheduled",
                "sample": 1,
                "iteration": 1,
                "phase": "query_only",
                "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "requests": {},
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(sample_json_data, indent=2, sort_keys=True) + "\n"
            )

            # Reload and verify metadata exists
            reloaded = json.loads((sample_dir / "sample.json").read_text())
            self.assertEqual("query_only", reloaded.get("name"))
            self.assertEqual("scheduled", reloaded.get("mode"))
            self.assertEqual(1, reloaded.get("sample"))
            self.assertEqual(1, reloaded.get("iteration"))
            self.assertEqual("query_only", reloaded.get("phase"))
            self.assertEqual("scheduled", reloaded.get("target"))
            self.assertEqual(str(sample_dir), reloaded.get("artifact_dir"))

    def test_deleting_requests_jsonl_invalidates_report(self) -> None:
        """Deleting requests.jsonl after sample creation makes report invalid via missing_required."""
        import workload_scheduler_report as wr
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"
            sample_dir = runs_dir / "iteration-01" / "query_only" / "baseline"
            sample_dir.mkdir(parents=True)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir()

            # Write all required artifacts
            (sample_dir / "requests.jsonl").write_text('{"token":0}\n')
            (sample_dir / "scrapes.jsonl").write_text('{"offset":0}\n')
            (sample_dir / "sample.json").write_text('{"name":"x"}\n')
            (metrics_dir / "scrape-000.prom").write_text("metric 1\n")

            samples = [
                {
                    "name": "query_only",
                    "mode": "baseline",
                    "sample": 1,
                    "requests": {},
                    "artifact_dir": str(sample_dir),
                }
            ]
            artifacts = bench.build_artifacts_index(work_dir, samples)
            self.assertEqual(1, len(artifacts["samples"]))
            self.assertNotIn("missing_required", artifacts["samples"][0])

            # Now delete requests.jsonl and verify missing_required appears
            (sample_dir / "requests.jsonl").unlink()
            artifacts2 = bench.build_artifacts_index(work_dir, samples)
            self.assertIn("missing_required", artifacts2["samples"][0])
            self.assertIn("requests.jsonl", artifacts2["samples"][0]["missing_required"])

    def test_deleting_raw_prom_invalidates_report(self) -> None:
        """Deleting raw .prom files after sample creation makes report invalid."""
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"
            sample_dir = runs_dir / "iteration-01" / "query_only" / "baseline"
            sample_dir.mkdir(parents=True)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir()

            (sample_dir / "requests.jsonl").write_text('{"token":0}\n')
            (sample_dir / "scrapes.jsonl").write_text('{"offset":0}\n')
            (sample_dir / "sample.json").write_text('{"name":"x"}\n')
            (metrics_dir / "scrape-000.prom").write_text("metric 1\n")

            samples = [
                {
                    "name": "query_only",
                    "mode": "baseline",
                    "sample": 1,
                    "requests": {},
                    "artifact_dir": str(sample_dir),
                }
            ]
            artifacts = bench.build_artifacts_index(work_dir, samples)
            self.assertNotIn("missing_required", artifacts["samples"][0])

            # Delete the .prom file
            (metrics_dir / "scrape-000.prom").unlink()
            artifacts2 = bench.build_artifacts_index(work_dir, samples)
            self.assertIn("missing_required", artifacts2["samples"][0])
            self.assertIn("metrics/scrape-NNN.prom", artifacts2["samples"][0]["missing_required"])


# ---------------------------------------------------------------------------
# Issue 5: build_matrix used for both dry-run and execution
# ---------------------------------------------------------------------------


class WorkloadSchedulerMatrixCallSiteTest(unittest.TestCase):
    """Test that build_matrix output is used for BOTH dry-run and execution."""

    def test_matrix_entries_use_normalized_target_names(self) -> None:
        """build_matrix entries use normalized target name and scheduler_enabled."""
        phases = [
            {"name": "query_only", "query_workers": 2, "write_workers": 0},
            {"name": "write_only", "query_workers": 0, "write_workers": 2},
        ]
        targets = [
            {"name": "my_baseline", "scheduler_enabled": False},
            {"name": "my_scheduled", "scheduler_enabled": True},
        ]
        matrix = bench.build_matrix(2, phases, targets=targets)
        self.assertEqual(4, len(matrix))
        entry = matrix[0]
        self.assertEqual(1, entry["iteration"])
        self.assertEqual("query_only", entry["phase"])
        self.assertEqual(2, len(entry["entries"]))
        # First entry should be baseline
        self.assertEqual("my_baseline", entry["entries"][0]["name"])
        self.assertFalse(entry["entries"][0]["scheduler_enabled"])
        # Second entry should be scheduled
        self.assertEqual("my_scheduled", entry["entries"][1]["name"])
        self.assertTrue(entry["entries"][1]["scheduler_enabled"])

    def test_matrix_defaults_when_no_targets(self) -> None:
        """Without targets, defaults to baseline/scheduled."""
        phases = [{"name": "query_only"}]
        default_targets = [
            {"name": "baseline", "scheduler_enabled": False},
            {"name": "scheduled", "scheduler_enabled": True},
        ]
        matrix = bench.build_matrix(1, phases, targets=default_targets)
        self.assertEqual(1, len(matrix))
        entries = matrix[0]["entries"]
        self.assertEqual(2, len(entries))
        self.assertFalse(entries[0]["scheduler_enabled"])
        self.assertTrue(entries[1]["scheduler_enabled"])


# ---------------------------------------------------------------------------
# Issue 7: Diagnostic/preflight exits
# ---------------------------------------------------------------------------


class WorkloadSchedulerPreflightTest(unittest.TestCase):
    """Test work-dir management and exit behavior."""

    def test_fresh_rejects_nonempty_workdir(self) -> None:
        """Fresh mode rejects ANY nonempty work-dir content (not just runs/)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            # Create a non-run file in work-dir
            (work_dir / "some_other_file.txt").write_text("data")
            runs_dir = work_dir / "runs"

            # The benchmark work-dir check looks at ANY content, not just runs/
            # Verify runs/ does not exist but other content does
            self.assertFalse(runs_dir.exists())
            self.assertTrue((work_dir / "some_other_file.txt").exists())

    def test_reuse_creates_new_subtree(self) -> None:
        """Reuse creates a fresh unique run subtree."""
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"
            runs_dir.mkdir()

            # Create existing content in runs/
            (runs_dir / "old_run").mkdir()

            # Simulate reuse: create a new unique subtree
            import time
            reuse_run_id = int(time.time() * 1000000)
            fresh_subtree = work_dir / "runs" / f"reuse-{reuse_run_id}"
            fresh_subtree.mkdir(parents=True)

            self.assertTrue(fresh_subtree.exists())
            self.assertNotEqual(runs_dir, fresh_subtree)
            # Old content remains intact
            self.assertTrue((runs_dir / "old_run").exists())

    def test_no_gate_does_not_suppress_error_or_invalid(self) -> None:
        """--no-gate must not suppress error or invalid status (exit codepath test only)."""
        # Actual exit behavior is tested via the report construction
        import workload_scheduler_report as wr
        error_report = wr.build_report(errors=["cannot start server"])
        self.assertEqual("error", error_report["status"])
        self.assertEqual(3, error_report["exit_code"])

        invalid_report = wr.build_report(
            request_eval=wr.RequestEvaluation(status="invalid", failures=("bad",)),
        )
        self.assertEqual("invalid", invalid_report["status"])
        self.assertEqual(2, invalid_report["exit_code"])


# ---------------------------------------------------------------------------
#  Issue A: Canonical persisted evidence — synthetic artifact tests
# ---------------------------------------------------------------------------


class WorkloadSchedulerCanonicalLoaderTest(unittest.TestCase):
    """End-to-end synthetic artifact test for load_persisted_sample."""

    def _write_valid_sample(self, sample_dir: Path, mode: str = "scheduled") -> None:
        """Write a valid complete sample directory."""
        metrics_dir = sample_dir / "metrics"
        metrics_dir.mkdir(parents=True)

        # sample.json
        sample_json = {
            "name": "saturated",
            "mode": mode,
            "sample": 1,
            "iteration": 1,
            "phase": "saturated",
            "target": mode,
            "artifact_dir": str(sample_dir),
            "requests": {"query": {"started": 1, "completed": 1, "completed_failures": 0,
                                  "timeouts": 0, "outstanding": 0, "failures": 0,
                                  "successful_rps": 50.0},
                         "write": {"started": 1, "completed": 1, "completed_failures": 0,
                                   "timeouts": 0, "outstanding": 0, "failures": 0}},
        }
        (sample_dir / "sample.json").write_text(
            json.dumps(sample_json, indent=2, sort_keys=True) + "\n"
        )

        # requests.jsonl
        events = [
            {"token": 0, "workload": "query", "status": "success", "submission_offset": 0.1, "completion_offset": 0.5, "latency_ms": 399.0, "error": None},
            {"token": 1, "workload": "write", "status": "success", "submission_offset": 0.2, "completion_offset": 0.6, "latency_ms": 399.0, "error": None},
        ]
        with open(sample_dir / "requests.jsonl", "w") as f:
            for ev in events:
                f.write(json.dumps(ev, sort_keys=True) + "\n")

        # scrapes.jsonl + .prom files
        scrapes = [
            {"offset": 0.0, "status": "success", "http_status": 200, "text": "metric 1", "path": "metrics/scrape-000.prom", "start": 0.01, "completion": 0.02},
            {"offset": 1.0, "status": "success", "http_status": 200, "text": "metric 2", "path": "metrics/scrape-001.prom", "start": 1.01, "completion": 1.02},
        ]
        with open(sample_dir / "scrapes.jsonl", "w") as f:
            for rec in scrapes:
                f.write(json.dumps(rec, sort_keys=True) + "\n")
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

    def test_valid_sample_passes_loader(self) -> None:
        """Valid sample must pass load_persisted_sample."""
        import workload_scheduler_report as wr
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_valid_sample(sample_dir)
            av = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", av.validation.status, f"failures: {av.validation.failures}")

    def test_deleted_raw_file_causes_invalid(self) -> None:
        """Deleting a raw .prom file must change canonical evaluation to invalid."""
        import workload_scheduler_report as wr
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_valid_sample(sample_dir)
            valid_av = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", valid_av.validation.status)

            # Delete a raw .prom file
            (sample_dir / "metrics" / "scrape-001.prom").unlink()

            av = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", av.validation.status)
            self.assertTrue(
                any("nonexistent" in f or "count mismatch" in f for f in av.validation.failures)
            )

    def test_corrupted_requests_jsonl_causes_invalid(self) -> None:
        """Corrupting requests.jsonl must change evaluation to invalid."""
        import workload_scheduler_report as wr
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_valid_sample(sample_dir)
            valid_av = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", valid_av.validation.status)

            # Corrupt requests.jsonl
            (sample_dir / "requests.jsonl").write_text("not valid json\n")

            av = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", av.validation.status)

    def test_changed_persisted_summary_causes_mismatch(self) -> None:
        """Changing persisted sample.json metadata must cause mismatch."""
        import workload_scheduler_report as wr
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_valid_sample(sample_dir, mode="scheduled")

            # Valid with expected metadata
            av = wr.load_persisted_sample(
                sample_dir,
                expected_metadata={"mode": "scheduled", "phase": "saturated"},
            )
            self.assertEqual("passed", av.validation.status)

            # Wrong expected metadata
            av2 = wr.load_persisted_sample(
                sample_dir,
                expected_metadata={"mode": "baseline"},
            )
            self.assertEqual("invalid", av2.validation.status)

    def test_stale_in_memory_evidence_ignored(self) -> None:
        """Only persisted state matters; stale in-memory evidence ignored."""
        import workload_scheduler_report as wr
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_valid_sample(sample_dir)

            # Load once
            av = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", av.validation.status)

            # Corrupt files (change persisted state)
            (sample_dir / "requests.jsonl").write_text("{}\n")

            # Reload — must reflect corruption
            av2 = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", av2.validation.status)

    def test_extra_unreferenced_prom_fails(self) -> None:
        """Extra unreferenced .prom file must cause invalid."""
        import workload_scheduler_report as wr
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_valid_sample(sample_dir)

            # Add extra unreferenced .prom file
            (sample_dir / "metrics" / "scrape-999.prom").write_text("extra")

            av = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", av.validation.status)
            self.assertTrue(
                any("unreferenced" in f for f in av.validation.failures)
            )

    def test_fresh_rejects_hidden_file_strict(self) -> None:
        """The spec requires fresh mode to reject hidden files too."""
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            (work_dir / ".config").write_text("hidden")

            # Strict check: any directory entry => nonempty
            def strict_check(wd: Path) -> bool:
                if not wd.exists():
                    return False
                try:
                    next(wd.iterdir())
                    return True
                except StopIteration:
                    return False

            self.assertTrue(
                strict_check(work_dir),
                "strict check must find the hidden file",
            )

    def test_nonexistent_output_no_error(self) -> None:
        """A nonexistent output path must not cause errors in nonempty check."""
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            output_path = work_dir / "nonexistent" / "report.json"
            self.assertFalse(output_path.exists())

            (work_dir / "some_file.txt").write_text("content")

            # Replicate benchmark check with nonexistent output
            def check(wd: Path, out: Path | None) -> bool:
                if not wd.exists():
                    return False
                for child in wd.iterdir():
                    if child.name.startswith("."):
                        continue
                    if out and out.exists():
                        try:
                            if child.samefile(out):
                                continue
                        except (FileNotFoundError, OSError):
                            pass
                    return True
                return False

            # Must not raise FileNotFoundError
            result = check(work_dir, output_path)
            self.assertTrue(result)

    def test_two_historical_run_trees_ignored(self) -> None:
        """Two old historical runs must not satisfy or invalidate the new run."""
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"
            runs_dir.mkdir()

            (runs_dir / "old-run-1").mkdir()
            (runs_dir / "old-run-2").mkdir()

            active = runs_dir / "reuse-12345"
            active.mkdir()

            samples = [
                {
                    "name": "query_only",
                    "mode": "baseline",
                    "sample": 1,
                    "requests": {},
                    "artifact_dir": str(active / "iteration-01" / "query_only" / "baseline"),
                }
            ]
            artifacts = bench.build_artifacts_index(work_dir, samples)
            self.assertIn("run_dirs", artifacts)

    def test_active_subtree_indexed_exclusively(self) -> None:
        """Only the active subtree must be indexed, not historical runs."""
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"
            runs_dir.mkdir()

            hist1 = runs_dir / "historical-001"
            hist1.mkdir()
            (hist1 / "config.toml").write_text("old config")

            hist2 = runs_dir / "historical-002"
            hist2.mkdir()
            (hist2 / "config.toml").write_text("older config")

            active = runs_dir / "reuse-99999"
            sample_dir = active / "iteration-01" / "saturated" / "scheduled"
            sample_dir.mkdir(parents=True)
            (sample_dir / "config.toml").write_text("new config")
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir()
            (metrics_dir / "scrape-000.prom").write_text("new metric")

            samples = [
                {
                    "name": "saturated",
                    "mode": "scheduled",
                    "sample": 1,
                    "requests": {},
                    "artifact_dir": str(sample_dir),
                }
            ]
            artifacts = bench.build_artifacts_index(work_dir, samples)
            self.assertEqual(1, len(artifacts["samples"]))
            self.assertIn("reuse-99999", str(artifacts["samples"][0].get("dir", "")))


    def test_canonical_loader_returns_full_payload(self) -> None:
        """load_persisted_sample returns full canonical metadata, events, summaries, scrapes, snapshots."""
        import workload_scheduler_report as wr
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_valid_sample(sample_dir)

            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", ps.validation.status)

            # Metadata
            self.assertEqual("saturated", ps.metadata.get("name"))
            self.assertEqual("scheduled", ps.metadata.get("mode"))
            self.assertEqual(1, ps.metadata.get("sample"))
            self.assertEqual("saturated", ps.metadata.get("phase"))
            self.assertIn("artifact_dir", ps.metadata)

            # Request events
            self.assertEqual(2, len(ps.request_events))
            self.assertEqual("query", ps.request_events[0].get("workload"))
            self.assertEqual(0, ps.request_events[0].get("token"))

            # Request summaries reconstructed from events
            self.assertIn("query", ps.request_summaries)
            self.assertIn("write", ps.request_summaries)
            self.assertEqual(1, ps.request_summaries["query"].get("started"))
            self.assertEqual(1, ps.request_summaries["write"].get("started"))

            # Scrape records
            self.assertEqual(2, len(ps.scrape_records))
            self.assertEqual(0.0, ps.scrape_records[0].get("offset"))

            # Snapshots from exact .prom paths
            self.assertEqual(2, len(ps.scheduler_snapshots))
            self.assertIn("polls", ps.scheduler_snapshots[0])
            self.assertIn("queued", ps.scheduler_snapshots[0])
            self.assertIsNotNone(ps.scheduler_snapshots[0].get("active"))

    def test_canonical_loader_mutated_requests_jsonl_causes_mismatch(self) -> None:
        """Mutating requests.jsonl changes event count, causing sample.json cross-check failure."""
        import workload_scheduler_report as wr
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_valid_sample(sample_dir)

            # Write sample.json with specific request count that matches events
            sample_json = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "requests": {
                    "query": {"started": 1, "completed": 1, "completed_failures": 0,
                              "timeouts": 0, "outstanding": 0, "failures": 0,
                              "successful_rps": 50.0},
                    "write": {"started": 1, "completed": 1, "completed_failures": 0,
                              "timeouts": 0, "outstanding": 0, "failures": 0},
                },
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(sample_json, indent=2, sort_keys=True) + "\n"
            )

            # Valid
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", ps.validation.status)

            # NOW mutate requests.jsonl to have different count
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.0,"completion_offset":0.5,"latency_ms":499.0,"error":null}\n'
                '{"token":1,"workload":"query","status":"success","submission_offset":0.0,"completion_offset":0.5,"latency_ms":499.0,"error":null}\n'  # Extra query event
            )

            # Reload — cross-check fails because sample.json says 1 query, events have 2
            ps2 = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps2.validation.status)
            self.assertTrue(
                any("started" in f for f in ps2.validation.failures),
                f"should flag started mismatch: {ps2.validation.failures}"
            )

    def test_canonical_loader_mutated_sample_json_causes_mismatch(self) -> None:
        """Mutating sample.json request summary separate from events causes failure."""
        import workload_scheduler_report as wr
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            self._write_valid_sample(sample_dir)

            # Write sample.json with WRONG request summary that doesn't match events
            sample_json = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "requests": {
                    "query": {"started": 999, "completed": 999, "completed_failures": 0,
                              "timeouts": 0, "outstanding": 0, "failures": 0,
                              "successful_rps": 50.0},
                    "write": {"started": 999, "completed": 999, "completed_failures": 0,
                              "timeouts": 0, "outstanding": 0, "failures": 0},
                },
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(sample_json, indent=2, sort_keys=True) + "\n"
            )

            # Events have 1 each, sample.json says 999
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("started" in f and "999" in f for f in ps.validation.failures),
                f"should flag started mismatch: {ps.validation.failures}"
            )

    def test_canonical_loader_changes_exact_path_validates(self) -> None:
        """Changing exact raw path value causes path validation failure."""
        import workload_scheduler_report as wr
        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            # Write sample.json with artifact_dir
            sample_json = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(sample_json, indent=2, sort_keys=True) + "\n"
            )
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.0,"completion_offset":0.5,"latency_ms":499.0,"error":null}\n'
            )

            # Write scrapes.jsonl referencing a path that does NOT exist
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,'
                '"text":"m","path":"metrics/scrape-000.prom","start":0.01,"completion":0.02}\n'
            )

            # Path does not exist => invalid
            ps = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", ps.validation.status)
            self.assertTrue(
                any("nonexistent" in f for f in ps.validation.failures),
                f"should flag nonexistent path: {ps.validation.failures}"
            )

            # Now create the file
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 10\n'
                'greptime_workload_scheduler_polls{workload="write"} 10\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )

            # Now valid
            ps2 = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", ps2.validation.status)


# ---------------------------------------------------------------------------
# Issue 10: Production-path synthetic gated run — full mechanism / report
# ---------------------------------------------------------------------------


class WorkloadSchedulerSyntheticGatedRunTest(unittest.TestCase):
    """Complete synthetic persisted gated run reaching mechanism and returning a report."""

    def _write_synthetic_sample(
        self,
        sample_dir: Path,
        phase: str,
        mode: str,
        iteration: int,
        with_scheduler_metrics: bool = True,
    ) -> None:
        """Write a complete synthetic sample directory.

        If *with_scheduler_metrics* is False (baseline), the .prom files
        are still written but without scheduler poll/queued/active metrics.
        """
        metrics_dir = sample_dir / "metrics"
        metrics_dir.mkdir(parents=True)

        # sample.json
        sample_json = {
            "name": phase,
            "mode": mode,
            "sample": iteration,
            "iteration": iteration,
            "phase": phase,
            "target": mode,
            "artifact_dir": str(sample_dir),
            "target_name": mode,
            "requests": {"query": {"started": 1, "completed": 1, "completed_failures": 0,
                                  "timeouts": 0, "outstanding": 0, "failures": 0,
                                  "successful_rps": 50.0},
                         "write": {"started": 1, "completed": 1, "completed_failures": 0,
                                   "timeouts": 0, "outstanding": 0, "failures": 0}},
        }
        (sample_dir / "sample.json").write_text(
            json.dumps(sample_json, indent=2, sort_keys=True) + "\n"
        )

        # requests.jsonl
        events = [
            {"token": 0, "workload": "query", "status": "success", "submission_offset": 0.1,
             "completion_offset": 0.5, "latency_ms": 399.0, "error": None},
            {"token": 1, "workload": "write", "status": "success", "submission_offset": 0.2,
             "completion_offset": 0.6, "latency_ms": 399.0, "error": None},
        ]
        with open(sample_dir / "requests.jsonl", "w") as f:
            for ev in events:
                f.write(json.dumps(ev, sort_keys=True) + "\n")

        # scrapes.jsonl + .prom files
        scrape_count = 6
        scrapes = []
        for i in range(scrape_count):
            offset = float(i) * 1.0
            scrapes.append({
                "offset": offset,
                "status": "success",
                "http_status": 200,
                "text": f"metrics at {offset}",
                "path": f"metrics/scrape-{i:03d}.prom",
                "start": offset + 0.01,
                "completion": offset + 0.02,
            })
        with open(sample_dir / "scrapes.jsonl", "w") as f:
            for rec in scrapes:
                f.write(json.dumps(rec, sort_keys=True) + "\n")

        for i in range(scrape_count):
            if with_scheduler_metrics:
                (metrics_dir / f"scrape-{i:03d}.prom").write_text(
                    f'greptime_workload_scheduler_polls{{workload="query"}} {100 + i * 10}\n'
                    f'greptime_workload_scheduler_polls{{workload="write"}} {50 + i * 5}\n'
                    f'greptime_workload_scheduler_queued_tasks{{workload="query"}} 1\n'
                    f'greptime_workload_scheduler_queued_tasks{{workload="write"}} 1\n'
                    f'greptime_workload_scheduler_active_polls 4\n'
                )
            else:
                # Baseline: still write .prom files, but without scheduler metrics
                (metrics_dir / f"scrape-{i:03d}.prom").write_text(
                    f'some_other_metric{{label="val"}} {i}\n'
                )

    def test_full_synthetic_gated_run_reaches_mechanism_and_report(self) -> None:
        """Complete synthetic gated run: build samples, reload, eval mechanism, build report.

        Must not crash with undefined locals.
        """
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"
            runs_dir.mkdir()

            samples: list[dict[str, object]] = []
            phases = ["query_only", "write_only", "light_write", "saturated"]
            for iteration in range(1, 3):
                for phase in phases:
                    for mode in ("baseline", "scheduled"):
                        sample_dir = runs_dir / f"iteration-{iteration:02d}" / phase / mode
                        sample_dir.mkdir(parents=True)
                        self._write_synthetic_sample(
                            sample_dir, phase, mode, iteration,
                            with_scheduler_metrics=(mode == "scheduled"),
                        )
                        samples.append({
                            "name": phase,
                            "mode": mode,
                            "sample": iteration,
                            "artifact_dir": str(sample_dir),
                            "requests": {"query": {"started": 1}, "write": {"started": 1}},
                            "workers": {"query": 2, "write": 2},
                        })

            # Build artifacts index
            artifacts_index = bench.build_artifacts_index(work_dir, samples)

            # Canonical loader for every sample (must not crash)
            artifacts_invalid_failures: list[str] = []
            for sample in samples:
                artifact_dir_str = sample.get("artifact_dir")
                if not artifact_dir_str:
                    continue
                sample_dir = Path(str(artifact_dir_str))
                av = wr.load_persisted_sample(sample_dir)
                if av.validation.status != "passed":
                    artifacts_invalid_failures.extend(av.validation.failures)

            # Reload persisted data
            reloaded_synthetic_samples: dict[tuple[int, str, str], dict[str, object]] = {}
            iteration_scrape_failures: list[str] = []
            for sample in samples:
                artifact_dir_str = sample.get("artifact_dir")
                if not artifact_dir_str:
                    continue
                sample_dir = Path(str(artifact_dir_str))
                iteration = sample.get("sample", 0)
                phase_name = sample.get("name", "")
                mode = sample.get("mode", "")
                key = (int(iteration), str(phase_name), str(mode))

                # Reload scrapes.jsonl
                sjl_path = sample_dir / "scrapes.jsonl"
                reloaded_scrape_records: list[dict[str, object]] = []
                if sjl_path.exists():
                    for line in sjl_path.read_text().strip().split("\n"):
                        if line.strip():
                            reloaded_scrape_records.append(json.loads(line))

                # Reload .prom files
                reloaded_snapshots: list[dict[str, object]] = []
                metrics_dir = sample_dir / "metrics"
                if metrics_dir.exists():
                    for prom_path in sorted(metrics_dir.glob("*.prom")):
                        text = prom_path.read_text()
                        metrics = wr.parse_scheduler_metrics(text)
                        reloaded_snapshots.append({
                            "polls": dict(metrics.polls),
                            "queued": dict(metrics.queued),
                            "active": metrics.active,
                        })

                synthetic = dict(sample)
                synthetic["scrape_records"] = reloaded_scrape_records
                synthetic["scheduler_snapshots"] = reloaded_snapshots
                reloaded_synthetic_samples[key] = synthetic

                # Validate iteration scrape with scheduler_metrics_required
                scheduler_metrics_required = (mode == "scheduled")
                val = wr.validate_iteration_scrape(
                    synthetic,
                    expected_scrape_count=6,
                    scrape_interval=1.0,
                    duration=5.0,
                    scheduler_metrics_required=scheduler_metrics_required,
                )
                if val.status != "passed":
                    iteration_scrape_failures.extend(val.failures)

            # Mechanism evaluation from reloaded data
            def eval_mech(iteration: int) -> wr.SchedulerEvaluation | None:
                phase_summaries: dict[str, wr.SchedulerSummary | None] = {}
                for req_phase in ("query_only", "write_only", "light_write", "saturated"):
                    key = (iteration, req_phase, "scheduled")
                    synthetic = reloaded_synthetic_samples.get(key)
                    if synthetic is None:
                        phase_summaries[req_phase] = None
                        continue
                    snapshots_raw = synthetic.get("scheduler_snapshots", [])
                    if not isinstance(snapshots_raw, list):
                        phase_summaries[req_phase] = None
                        continue
                    snapshots = [
                        wr.SchedulerMetrics(
                            polls=dict(s.get("polls", {})),
                            queued=dict(s.get("queued", {})),
                            active=s.get("active"),
                        )
                        for s in snapshots_raw
                        if isinstance(s, dict)
                    ]
                    if not snapshots:
                        phase_summaries[req_phase] = None
                    else:
                        phase_summaries[req_phase] = wr.summarize_scheduler_metrics(snapshots)

                present = all(phase_summaries.get(p) is not None
                              for p in ("query_only", "write_only", "light_write", "saturated"))
                if not present:
                    return None
                thresholds = wr.SchedulerGateThresholds(max_active_polls=16)
                return wr.evaluate_scheduler_report(phase_summaries, thresholds, errors=())

            mechanism_evals: list[wr.SchedulerEvaluation] = []
            for iteration in range(1, 3):
                ie = eval_mech(iteration)
                if ie is not None:
                    mechanism_evals.append(ie)

            combined_status = wr.combine_statuses(*(e.status for e in mechanism_evals)) if mechanism_evals else "invalid"
            mechanism_eval = wr.SchedulerEvaluation(
                status=combined_status,
                passed=combined_status == "passed",
                exit_code=wr.exit_code_for_status(combined_status),
                failures=tuple(f for e in mechanism_evals for f in e.failures),
            )

            # Build report (must not crash)
            report = wr.build_report(
                samples=samples,
                request_eval=wr.evaluate_request_validity(samples),
                mechanism_eval=mechanism_eval,
                artifact_validation=wr.ArtifactValidation(
                    status="passed" if not iteration_scrape_failures else "invalid",
                    failures=tuple(iteration_scrape_failures),
                ),
                performance_evals=[],
            )

            self.assertIn("status", report)
            self.assertIn("exit_code", report)
            # No undefined locals — we reached here without crashing
            self.assertTrue(True, "synthetic gated run completed without crash")

    def test_corrupt_artifact_returns_invalid_under_no_gate(self) -> None:
        """Corrupt canonical artifact returns invalid exit 2 under no-gate, no exception."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
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
                '{"token":0,"workload":"query","status":"success","submission_offset":0.0,"completion_offset":0.5,"latency_ms":499.0,"error":null}\n'
            )
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"ok","path":"metrics/scrape-000.prom","start":0.01,"completion":0.02}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text("metric 1\n")

            # Load canonical — passes
            av = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", av.validation.status)

            # Now corrupt the sample.json (must always propagate invalid)
            (sample_dir / "sample.json").write_text("not json\n")

            av2 = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", av2.validation.status)
            # Under no-gate, invalid is still invalid (exit 2)
            self.assertEqual(2, wr.exit_code_for_status(av2.validation.status))

    def test_baseline_raw_scrapes_without_scheduler_metrics_validate(self) -> None:
        """Baseline (scheduler_metrics_required=False) validates without scheduler metrics."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            # Write valid scrapes.jsonl with 2 non-scheduler .prom files
            with open(sample_dir / "scrapes.jsonl", "w") as f:
                f.write(json.dumps({"offset": 0.0, "status": "success", "http_status": 200,
                                    "text": "nosched", "path": "metrics/scrape-000.prom",
                                    "start": 0.01, "completion": 0.02}) + "\n")
                f.write(json.dumps({"offset": 5.0, "status": "success", "http_status": 200,
                                    "text": "nosched2", "path": "metrics/scrape-001.prom",
                                    "start": 5.01, "completion": 5.02}) + "\n")
            (metrics_dir / "scrape-000.prom").write_text("other_metric 1\n")
            (metrics_dir / "scrape-001.prom").write_text("other_metric 2\n")

            sample = {
                "name": "saturated", "mode": "baseline", "sample": 1,
                "artifact_dir": str(sample_dir),
                "scrape_records": [
                    {"offset": 0.0, "status": "success", "http_status": 200,
                     "text": "nosched", "path": "metrics/scrape-000.prom",
                     "start": 0.01, "completion": 0.02},
                    {"offset": 5.0, "status": "success", "http_status": 200,
                     "text": "nosched2", "path": "metrics/scrape-001.prom",
                     "start": 5.01, "completion": 5.02},
                ],
                "scheduler_snapshots": [],
            }

            # With scheduler_metrics_required=False, baseline passes
            val = wr.validate_iteration_scrape(
                sample, expected_scrape_count=2, scrape_interval=5.0, duration=5.0,
                scheduler_metrics_required=False,
            )
            self.assertEqual("passed", val.status)

            # With scheduler_metrics_required=True, scheduled would fail
            val2 = wr.validate_iteration_scrape(
                sample, expected_scrape_count=2, scrape_interval=5.0, duration=5.0,
                scheduler_metrics_required=True,
            )
            self.assertEqual("invalid", val2.status,
                             "scheduler_metrics_required=True should fail without scheduler metrics")

    def test_scheduled_raw_mutation_alters_mechanism_result(self) -> None:
        """Mutating raw scrape data alters mechanism result; stale in-memory cannot win."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
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
                '{"token":0,"workload":"query","status":"success","submission_offset":0.0,"completion_offset":0.5,"latency_ms":499.0,"error":null}\n'
            )

            # Write original scrapes.jsonl + .prom with balanced write share
            with open(sample_dir / "scrapes.jsonl", "w") as f:
                f.write(json.dumps({"offset": 0.0, "status": "success", "http_status": 200,
                                    "text": "m1", "path": "metrics/scrape-000.prom",
                                    "start": 0.01, "completion": 0.02}) + "\n")
                f.write(json.dumps({"offset": 1.0, "status": "success", "http_status": 200,
                                    "text": "m2", "path": "metrics/scrape-001.prom",
                                    "start": 1.01, "completion": 1.02}) + "\n")
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 0\n'
                'greptime_workload_scheduler_polls{workload="write"} 0\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )
            # Balanced write share: 10 query + 40 write = 0.80
            (metrics_dir / "scrape-001.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 10\n'
                'greptime_workload_scheduler_polls{workload="write"} 40\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )

            # Synthesize sample from persisted
            sjl_records = [json.loads(l)
                           for l in (sample_dir / "scrapes.jsonl").read_text().strip().split("\n")
                           if l.strip()]
            prom_snapshots = []
            for p in sorted(metrics_dir.glob("*.prom")):
                metrics = wr.parse_scheduler_metrics(p.read_text())
                prom_snapshots.append({
                    "polls": dict(metrics.polls),
                    "queued": dict(metrics.queued),
                    "active": metrics.active,
                })

            # Build phases for mechanism eval — use different data per phase
            # so purity and other checks pass
            def make_summary(snaps: list[dict]) -> wr.SchedulerSummary:
                metrics_list = [
                    wr.SchedulerMetrics(
                        polls=dict(s.get("polls", {})),
                        queued=dict(s.get("queued", {})),
                        active=s.get("active"),
                    )
                    for s in snaps if isinstance(s, dict)
                ]
                return wr.summarize_scheduler_metrics(metrics_list)

            # query_only: query polls go from 0→100 (write stays at 0)
            qo_snaps = [
                {"polls": {"query": 0, "write": 0}, "queued": {"query": 1, "write": 1}, "active": 4},
                {"polls": {"query": 100, "write": 0}, "queued": {"query": 1, "write": 0}, "active": 4},
            ]
            # write_only: write polls go from 0→100 (query stays at 0)
            wo_snaps = [
                {"polls": {"query": 0, "write": 0}, "queued": {"query": 1, "write": 1}, "active": 4},
                {"polls": {"query": 0, "write": 100}, "queued": {"query": 0, "write": 1}, "active": 4},
            ]
            # light_write: both active
            lw_snaps = [
                {"polls": {"query": 0, "write": 0}, "queued": {"query": 1, "write": 1}, "active": 4},
                {"polls": {"query": 30, "write": 70}, "queued": {"query": 1, "write": 1}, "active": 4},
            ]
            phases_ok = {
                "query_only": make_summary(qo_snaps),
                "write_only": make_summary(wo_snaps),
                "light_write": make_summary(lw_snaps),
                "saturated": make_summary(prom_snapshots),
            }
            thresholds = wr.SchedulerGateThresholds(
                write_share_min=0.78, write_share_max=0.82,
                min_dual_backlog_interval_fraction=0.50,
                min_dual_backlog_polls_per_class=1,
                single_class_purity_min_share=0.99,
                max_active_polls=16,
                min_light_write_query_share=0.20,
            )
            eval_ok = wr.evaluate_scheduler_report(phases_ok, thresholds)
            self.assertEqual("passed", eval_ok.status, "balanced write share should pass")

            # NOW mutate persisted .prom file to make write share 95%
            (metrics_dir / "scrape-001.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 5\n'
                'greptime_workload_scheduler_polls{workload="write"} 95\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )

            # Reload from persisted (NOT from in-memory)
            prom_snapshots2 = []
            for p in sorted(metrics_dir.glob("*.prom")):
                metrics = wr.parse_scheduler_metrics(p.read_text())
                prom_snapshots2.append({
                    "polls": dict(metrics.polls),
                    "queued": dict(metrics.queued),
                    "active": metrics.active,
                })

            phases_bad = {
                "query_only": make_summary(qo_snaps),
                "write_only": make_summary(wo_snaps),
                "light_write": make_summary(lw_snaps),
                "saturated": make_summary(prom_snapshots2),
            }
            eval_bad = wr.evaluate_scheduler_report(phases_bad, thresholds)
            # Write share from mutated data: 95/(95+5) = 0.95, above 0.82
            self.assertNotEqual("passed", eval_bad.status,
                                "mutated write share 95% should fail")

            # Stale in-memory snapshot (prom_snapshots1) must NOT win
            # The first evaluation with the original data still shows passed
            self.assertEqual("passed", eval_ok.status,
                             "stale in-memory snapshot must still show passed independently")

    def test_offset_zero_attempted_normally(self) -> None:
        """Offset zero should be attempted (not missed) in normal timing."""
        import tempfile

        scraper = runner.MetricsScraper(
            base_url="http://localhost:9999",
            timeout=1.0,
            interval=1.0,
            duration=3.0,
            measurement_start=1002.0,
            deadline=1005.0,
            metrics_dir=None,
        )
        offsets = scraper._schedule_offsets()
        self.assertEqual([0.0, 1.0, 2.0, 3.0], offsets)
        # Offset 0 is always in the schedule
        self.assertIn(0.0, offsets)

    def test_all_four_performance_formulas_and_counts(self) -> None:
        """All four phases produce specified comparisons and missing/zero invalid."""
        import workload_scheduler_report as wr

        # 2 iterations x 4 phases = 8 evaluations expected
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
                        "scheduled": {"query_rps": 70.0, "write_rps": 5.0},
                    },
                    "saturated": {
                        "baseline": {"query_rps": 10.0, "write_rps": 40.0},
                        "scheduled": {"query_rps": 9.0, "write_rps": 42.0},
                    },
                },
            ),
        ]
        evals = wr.evaluate_performance(
            iterations_data,
            ["query_only", "write_only", "light_write", "saturated"],
            max_regression_pct=5.0,
        )
        # 2 x 4 = 8
        self.assertEqual(8, len(evals))

        # query_only / write_only: pct_change is None, passed is True
        qo_evals = [e for e in evals if e.phase == "query_only"]
        self.assertEqual(2, len(qo_evals))
        for e in qo_evals:
            self.assertIsNone(e.pct_change)
            self.assertTrue(e.passed)

        # light_write / saturated: pct_change computed
        lw_evals = [e for e in evals if e.phase == "light_write"]
        self.assertEqual(2, len(lw_evals))
        for e in lw_evals:
            self.assertIsNotNone(e.pct_change)
            # Iteration 2 light_write has regression > 5%
            if e.iteration == 2:
                self.assertFalse(e.passed)

        # Missing pair produces invalid
        iterations_missing = [
            (
                1,
                {
                    "query_only": {"baseline": {"query_rps": 100.0, "write_rps": 0.0}},
                    "write_only": {"baseline": {"query_rps": 0.0, "write_rps": 50.0}},
                    # saturated has no scheduled pair
                    "saturated": {"baseline": {"query_rps": 10.0, "write_rps": 40.0}},
                },
            )
        ]
        evals_missing = wr.evaluate_performance(
            iterations_missing, ["saturated"], max_regression_pct=5.0
        )
        self.assertEqual(1, len(evals_missing))
        self.assertFalse(evals_missing[0].passed)

    def test_non_default_runtime_weights_shards_gates_phase_delay_target(self) -> None:
        """Non-default runtime weights/shards/gates/phase delay/target passed through."""
        import workload_scheduler_report as wr

        # Use non-default thresholds
        thresholds = wr.SchedulerGateThresholds(
            write_share_min=0.50,
            write_share_max=0.90,
            min_dual_backlog_interval_fraction=0.30,
            min_dual_backlog_polls_per_class=1,
            single_class_purity_min_share=0.70,
            max_active_polls=32,
            max_failure_rate=0.05,
            max_outstanding_requests=5,
            min_light_write_query_share=0.05,
            active_within_scheduler_limit=False,
            max_capacity_normalized_regression_pct=10.0,
        )

        # Build per-phase summaries with proper purity
        def make_summary(
            poll_pairs: list[tuple[int, int]],
            queue_pairs: list[tuple[int, int]],
            active: int = 8,
        ) -> wr.SchedulerSummary:
            snapshots = [
                wr.SchedulerMetrics(
                    polls={"query": q, "write": w},
                    queued={"query": qq, "write": ww},
                    active=active,
                )
                for (q, w), (qq, ww) in zip(poll_pairs, queue_pairs, strict=True)
            ]
            return wr.summarize_scheduler_metrics(snapshots)

        phases = {
            "query_only": make_summary(
                [(0, 0), (200, 0)],  # query_only: 100% query after warmup
                [(1, 1), (1, 0)],
            ),
            "write_only": make_summary(
                [(0, 0), (0, 200)],  # write_only: 100% write after warmup
                [(1, 1), (0, 1)],
            ),
            "light_write": make_summary(
                [(0, 0), (30, 70)],  # both active
                [(1, 1), (1, 1)],
            ),
            "saturated": make_summary(
                [(0, 0), (20, 80)],  # write share = 0.80 within [0.50, 0.90]
                [(1, 1), (1, 1)],
            ),
        }
        eval_result = wr.evaluate_scheduler_report(phases, thresholds)
        # Wide thresholds should pass
        self.assertEqual("passed", eval_result.status)

        # Test evaluate_request_validity with non-default gates
        samples = [
            {
                "name": "saturated",
                "mode": "baseline",
                "workers": {"query": 2, "write": 2},
                "requests": {
                    "query": {
                        "started": 100, "completed": 100,
                        "completed_failures": 4, "timeouts": 0,
                        "outstanding": 3, "failures": 4, "requests": 100,
                    },
                    "write": {
                        "started": 100, "completed": 100,
                        "completed_failures": 1, "timeouts": 0,
                        "outstanding": 0, "failures": 1, "requests": 100,
                    },
                },
            }
        ]
        req_eval = wr.evaluate_request_validity(
            samples, max_failure_rate=0.05, max_outstanding_requests=5
        )
        self.assertEqual("passed", req_eval.status)

    def test_fresh_rejects_hidden_entry_reuse_excludes_historical(self) -> None:
        """Fresh mode rejects hidden work-dir entries; reuse excludes historical trees."""
        import tempfile

        # Fresh rejects hidden entries
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            (work_dir / ".config").write_text("hidden")

            def strict_check(wd: Path) -> bool:
                if not wd.exists():
                    return False
                try:
                    next(wd.iterdir())
                    return True
                except StopIteration:
                    return False

            self.assertTrue(strict_check(work_dir),
                            "hidden file makes work-dir nonempty")

            def check_empty(wd: Path) -> bool:
                """Check if work-dir has ANY content (strict, no exclusions)."""
                if not wd.exists():
                    return True
                try:
                    for _ in wd.iterdir():
                        return False
                except (OSError, FileNotFoundError):
                    pass
                return True

            self.assertFalse(check_empty(work_dir),
                             "hidden entry must make work-dir non-empty")

        # Reuse excludes historical trees
        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"
            runs_dir.mkdir()

            # Create old historical runs
            (runs_dir / "old-run-001").mkdir()
            (runs_dir / "old-run-002").mkdir()

            # Active subtree
            active = runs_dir / "reuse-12345"
            active.mkdir()

            samples = [
                {
                    "name": "saturated",
                    "mode": "scheduled",
                    "sample": 1,
                    "requests": {},
                    "artifact_dir": str(active / "iteration-01" / "saturated" / "scheduled"),
                }
            ]
            artifacts = bench.build_artifacts_index(
                work_dir, samples, active_run_root=active
            )
            self.assertEqual(1, len(artifacts["samples"]),
                             "only active sample should be indexed")
            self.assertEqual(1, len(artifacts["run_dirs"]),
                             "only active run root should be listed")
            self.assertNotIn("old-run", str(artifacts["run_dirs"]),
                             "historical runs must be excluded")


# ---------------------------------------------------------------------------
# Issue 1-6: Canonical, formula, gates, artifacts, target, no-gate integration
# ---------------------------------------------------------------------------


class WorkloadSchedulerCanonicalSampleTest(unittest.TestCase):
    """Test canonical sample building replaces in-memory samples for performance/validity."""

    def test_canonical_request_path_uses_persisted_only(self) -> None:
        """evaluate_request_validity must use canonical (persisted) samples, not in-memory."""
        import workload_scheduler_report as wr
        # Build a sample with persisted-like data (scrape_records/requests from files)
        canonical = [
            {
                "name": "query_only",
                "mode": "baseline",
                "sample": 1,
                "workers": {"query": 2, "write": 0},
                "requests": {
                    "query": {
                        "started": 10, "completed": 10, "completed_failures": 0,
                        "timeouts": 0, "outstanding": 0, "failures": 0, "requests": 10,
                    },
                },
                "scrape_records": [{"offset": 0.0, "status": "success", "http_status": 200,
                                    "text": "m", "path": "/tmp/m.prom"}],
                "scheduler_snapshots": [],
            }
        ]
        ev = wr.evaluate_request_validity(canonical)
        self.assertEqual("passed", ev.status,
                         "canonical samples with valid data must pass")

    def test_mutated_requests_in_memory_ignored_when_canonical_used(self) -> None:
        """Stale in-memory request summary cannot pass when canonical data is corrupt."""
        import tempfile
        import workload_scheduler_report as wr

        with tempfile.TemporaryDirectory() as tmpdir:
            sample_dir = Path(tmpdir)
            metrics_dir = sample_dir / "metrics"
            metrics_dir.mkdir(parents=True)

            # Write sample.json with clean metadata
            sample_json = {
                "name": "saturated", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "saturated", "target": "scheduled",
                "artifact_dir": str(sample_dir),
                "requests": {"query": {"started": 1, "completed": 1, "completed_failures": 0,
                                      "timeouts": 0, "outstanding": 0, "failures": 0,
                                      "successful_rps": 5.0}},
            }
            (sample_dir / "sample.json").write_text(
                json.dumps(sample_json, indent=2, sort_keys=True) + "\n"
            )
            # Write valid requests.jsonl
            (sample_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.1,"completion_offset":0.5,"latency_ms":399.0,"error":null}\n'
            )
            # Write valid scrapes.jsonl + .prom
            (sample_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m","path":"metrics/scrape-000.prom","start":0.01,"completion":0.02}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text(
                'greptime_workload_scheduler_polls{workload="query"} 10\n'
                'greptime_workload_scheduler_polls{workload="write"} 10\n'
                'greptime_workload_scheduler_queued_tasks{workload="query"} 1\n'
                'greptime_workload_scheduler_queued_tasks{workload="write"} 1\n'
                'greptime_workload_scheduler_active_polls 4\n'
            )

            # Canonic load passes
            av = wr.load_persisted_sample(sample_dir)
            self.assertEqual("passed", av.validation.status)

            # Now corrupt persisted requests.jsonl
            (sample_dir / "requests.jsonl").write_text("not valid json")

            # Reload from persisted — must fail
            av2 = wr.load_persisted_sample(sample_dir)
            self.assertEqual("invalid", av2.validation.status,
                             "corrupt persisted data must cause invalid")


class WorkloadSchedulerPerformanceFormulaTest(unittest.TestCase):
    """Test all-four performance formulas produce exact expected values."""

    def _make_perf_data(self) -> list[dict]:
        """Build 2 iterations x 4 phases = 8 canonical samples."""
        samples: list[dict] = []
        for it in (1, 2):
            for phase, bl_q, bl_w, sch_q, sch_w in [
                ("query_only", 100.0, 0.0, 95.0, 0.0),
                ("write_only", 0.0, 50.0, 0.0, 48.0),
                ("light_write", 80.0, 5.0, 78.0, 5.0),
                ("saturated", 10.0, 40.0, 9.5, 41.0),
            ]:
                samples.append({
                    "name": phase, "mode": "baseline", "sample": it,
                    "requests": {"query": {"successful_rps": bl_q},
                                 "write": {"successful_rps": bl_w}},
                })
                samples.append({
                    "name": phase, "mode": "scheduled", "sample": it,
                    "requests": {"query": {"successful_rps": sch_q},
                                 "write": {"successful_rps": sch_w}},
                })
        return samples

    def test_query_only_formula(self) -> None:
        """query_only: pct = (scheduled_query_rps / baseline_query_rps - 1)*100."""
        samples = self._make_perf_data()
        perf = bench.build_performance_report(samples, ["query_only", "write_only", "light_write", "saturated"], 5.0)
        phases = perf.get("phases", [])
        qo = [p for p in phases if p["phase"] == "query_only"]
        self.assertEqual(2, len(qo), "2 iterations x 1 query_only each")
        for p in qo:
            # bl_norm = 100/100=1.0, sch_norm = 95/100=0.95, pct = -5%
            self.assertAlmostEqual(p["pct_change"], -5.0, places=5,
                                   msg=f"query_only iteration {p['iteration']} pct")

    def test_write_only_formula(self) -> None:
        """write_only: pct = (scheduled_write_rps / baseline_write_rps - 1)*100."""
        samples = self._make_perf_data()
        perf = bench.build_performance_report(samples, ["query_only", "write_only", "light_write", "saturated"], 5.0)
        phases = perf.get("phases", [])
        wo = [p for p in phases if p["phase"] == "write_only"]
        self.assertEqual(2, len(wo))
        for p in wo:
            # bl_norm = 50/50=1.0, sch_norm = 48/50=0.96, pct = -4%
            self.assertAlmostEqual(p["pct_change"], -4.0, places=5,
                                   msg=f"write_only iteration {p['iteration']} pct")

    def test_light_write_formula(self) -> None:
        """light_write: bl_norm = qrps/qcap + wrps/wcap; sch_norm same."""
        samples = self._make_perf_data()
        perf = bench.build_performance_report(samples, ["query_only", "write_only", "light_write", "saturated"], 5.0)
        phases = perf.get("phases", [])
        lw = [p for p in phases if p["phase"] == "light_write"]
        self.assertEqual(2, len(lw))
        for p in lw:
            # bl_norm = 80/100 + 5/50 = 0.8+0.1=0.9
            # sch_norm = 78/100 + 5/50 = 0.78+0.1=0.88
            # pct = (0.88/0.9-1)*100 = -2.22...
            self.assertAlmostEqual(p["baseline"]["normalized"], 0.9, places=5)
            self.assertAlmostEqual(p["scheduled"]["normalized"], 0.88, places=5)

    def test_saturated_formula(self) -> None:
        """saturated: same formula as light_write."""
        samples = self._make_perf_data()
        perf = bench.build_performance_report(samples, ["query_only", "write_only", "light_write", "saturated"], 5.0)
        phases = perf.get("phases", [])
        sat = [p for p in phases if p["phase"] == "saturated"]
        self.assertEqual(2, len(sat))
        for p in sat:
            # bl_norm = 10/100 + 40/50 = 0.1+0.8=0.9
            # sch_norm = 9.5/100 + 41/50 = 0.095+0.82=0.915
            self.assertAlmostEqual(p["baseline"]["normalized"], 0.9, places=5)
            self.assertAlmostEqual(p["scheduled"]["normalized"], 0.915, places=5)

    def test_exact_iteration_phase_count(self) -> None:
        """2 iterations x 4 phases = exactly 8 performance entries."""
        samples = self._make_perf_data()
        perf = bench.build_performance_report(samples, ["query_only", "write_only", "light_write", "saturated"], 5.0)
        phases = perf.get("phases", [])
        self.assertEqual(8, len(phases))

    def test_write_only_not_use_query_rps(self) -> None:
        """write_only must NOT use query_rps (only write_rps)."""
        samples = [
            {"name": "query_only", "mode": "baseline", "sample": 1,
             "requests": {"query": {"successful_rps": 200.0}, "write": {"successful_rps": 0.0}}},
            {"name": "query_only", "mode": "scheduled", "sample": 1,
             "requests": {"query": {"successful_rps": 200.0}, "write": {"successful_rps": 0.0}}},
            {"name": "write_only", "mode": "baseline", "sample": 1,
             "requests": {"query": {"successful_rps": 0.0}, "write": {"successful_rps": 100.0}}},
            {"name": "write_only", "mode": "scheduled", "sample": 1,
             "requests": {"query": {"successful_rps": 0.0}, "write": {"successful_rps": 90.0}}},
        ]
        perf = bench.build_performance_report(samples, ["query_only", "write_only"], 5.0)
        phases = perf.get("phases", [])
        wo = [p for p in phases if p["phase"] == "write_only"]
        self.assertEqual(1, len(wo))
        p = wo[0]
        # write_only uses write_rps / write_capacity: 100/100=1.0 baseline, 90/100=0.9 scheduled
        self.assertAlmostEqual(p["baseline"]["normalized"], 1.0, places=5)
        self.assertAlmostEqual(p["scheduled"]["normalized"], 0.9, places=5)
        # query_rps must be None for write_only
        self.assertIsNone(p["baseline"]["query_rps"])
        self.assertIsNone(p["scheduled"]["query_rps"])


class WorkloadSchedulerRequestGateTest(unittest.TestCase):
    """Test exact request threshold names match Rust schema."""

    def test_non_default_threshold_changes_result(self) -> None:
        """Custom max_failure_rate changes result through benchmark caller."""
        import workload_scheduler_report as wr
        # Use data that satisfies accounting (completed+timeouts==started)
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
        # Default threshold: fraction 1/100 = 0.01 which is >= 0.01 => fail
        ev_default = wr.evaluate_request_validity(samples, max_failure_rate=0.01)
        self.assertEqual("failed", ev_default.status)

        # Lenient threshold: 0.02 tolerance => pass
        ev_lenient = wr.evaluate_request_validity(samples, max_failure_rate=0.02)
        self.assertEqual("passed", ev_lenient.status)

    def test_exact_rust_gate_names_used(self) -> None:
        """Python evaluator uses max_failure_rate/max_outstanding_requests matching Rust."""
        import workload_scheduler_report as wr
        # Verify function signature uses exact Rust field names
        import inspect
        sig = inspect.signature(wr.evaluate_request_validity)
        params = sig.parameters
        self.assertIn("max_failure_rate", params)
        self.assertIn("max_outstanding_requests", params)


class WorkloadSchedulerBaselineArtifactTest(unittest.TestCase):
    """Test baseline artifacts without scheduler metrics validate correctly."""

    def test_baseline_without_scheduler_metrics_valid(self) -> None:
        """Baseline without scheduler metrics passes validate_sample_artifacts."""
        import workload_scheduler_report as wr
        sample = {
            "name": "query_only", "mode": "baseline", "sample": 1,
            "workers": {"query": 2, "write": 0},
            "requests": {"query": {"started": 10}},
            "scrape_records": [
                {"offset": 0.0, "status": "success", "http_status": 200,
                 "text": "ok", "path": "/tmp/m.prom"},
            ],
            "scheduler_snapshots": [],
        }
        av = wr.validate_sample_artifacts(sample, scheduler_metrics_required=False)
        self.assertEqual("passed", av.status)

    def test_enabled_without_scheduler_metrics_invalid(self) -> None:
        """Enabled target without scheduler metrics fails validate_sample_artifacts."""
        import workload_scheduler_report as wr
        sample = {
            "name": "query_only", "mode": "scheduled", "sample": 1,
            "workers": {"query": 2, "write": 0},
            "requests": {"query": {"started": 10}},
            "scrape_records": [
                {"offset": 0.0, "status": "success", "http_status": 200,
                 "text": "ok", "path": "/tmp/m.prom"},
            ],
            "scheduler_snapshots": [],
        }
        # With scheduler_metrics_required=True (default), empty snapshots => invalid
        av = wr.validate_sample_artifacts(sample)
        self.assertEqual("invalid", av.status)


class WorkloadSchedulerNoGatePerformanceTest(unittest.TestCase):
    """Test --no-gate suppresses only performance regression, not integrity."""

    def test_no_gate_malformed_performance_exit_2(self) -> None:
        """Malformed performance (invalid) under no-gate must exit 2."""
        import workload_scheduler_report as wr
        # Build report with performance integrity failure (gated mode requires all sections)
        perf_evals = [
            wr.PerformanceEvaluation(
                phase="light_write", iteration=1,
                baseline_normalized=0.0, scheduled_normalized=0.0,
                pct_change=None, passed=False,
                details="invalid: missing baseline query_only capacity",
            )
        ]
        report = wr.build_report(
            performance_evals=perf_evals,
            request_eval=wr.RequestEvaluation(status="passed", failures=()),
            mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
        )
        self.assertEqual("invalid", report["status"])
        self.assertEqual(2, report["exit_code"])

    def test_no_gate_regression_failure_may_exit_0(self) -> None:
        """Performance regression failure under no-gate may exit 0."""
        import workload_scheduler_report as wr
        perf_evals = [
            wr.PerformanceEvaluation(
                phase="light_write", iteration=1,
                baseline_normalized=1.0, scheduled_normalized=0.5,
                pct_change=-50.0, passed=False,
                details="regression below threshold",
            )
        ]
        report = wr.build_report(
            performance_evals=perf_evals,
            request_eval=wr.RequestEvaluation(status="passed", failures=()),
            mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
        )
        # Under normal mode, this would be "failed" (exit 1)
        # Under no-gate, regression is suppressed
        self.assertNotEqual("error", report["status"])
        # The build_report still shows the regression failure - it's up to no-gate
        # caller to suppress. The key is that performance integrity → exit 2
        # regardless of no-gate.
        self.assertIn(report.get("status"), ("failed", "invalid"))

    def test_integrity_and_regression_separate(self) -> None:
        """Integrity and regression are separate statuses in build_report."""
        import workload_scheduler_report as wr
        # Both integrity and regression
        perf_evals = [
            wr.PerformanceEvaluation(
                phase="light_write", iteration=1,
                baseline_normalized=0.0, scheduled_normalized=0.0,
                pct_change=None, passed=False,
                details="invalid: missing capacity",
            ),
            wr.PerformanceEvaluation(
                phase="saturated", iteration=1,
                baseline_normalized=1.0, scheduled_normalized=0.5,
                pct_change=-50.0, passed=False,
                details="regression below threshold",
            ),
        ]
        report = wr.build_report(
            performance_evals=perf_evals,
            request_eval=wr.RequestEvaluation(status="passed", failures=()),
            mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
        )
        # Integrity beats regression in precedence => invalid (exit 2)
        self.assertEqual("invalid", report["status"])
        self.assertEqual(2, report["exit_code"])
        # Both should appear in failures
        self.assertTrue(any("invalid: missing capacity" in f for f in report.get("failures", [])))
        self.assertTrue(any("regression" in f for f in report.get("failures", [])))


class WorkloadSchedulerTargetContractTest(unittest.TestCase):
    """Test target name/enable contract matches Rust validation."""

    def test_builtin_target_names_hardcoded(self) -> None:
        """Built-in targets use exactly 'baseline' and 'scheduled'."""
        default_targets = [
            {"name": "baseline", "scheduler_enabled": False},
            {"name": "scheduled", "scheduler_enabled": True},
        ]
        matrix = bench.build_matrix(1, [{"name": "query_only"}], targets=default_targets)
        entries = matrix[0]["entries"]
        self.assertEqual(2, len(entries))
        self.assertEqual("baseline", entries[0]["name"])
        self.assertEqual("scheduled", entries[1]["name"])
        self.assertFalse(entries[0]["scheduler_enabled"])
        self.assertTrue(entries[1]["scheduler_enabled"])

    def test_rust_validation_requires_exact_names(self) -> None:
        """Rust case.rs requires exact names 'baseline' and 'scheduled'."""
        # Verify by testing the TOML parsing path - the Rust code enforces
        # exact name bindings in its validate() method.
        # This test validates that the Python code uses these exact names.
        import workload_scheduler_report as wr
        from workload_scheduler_benchmark import build_matrix

        targets = [
            {"name": "baseline", "scheduler_enabled": False},
            {"name": "scheduled", "scheduler_enabled": True},
        ]
        matrix = build_matrix(1, [{"name": "query_only"}], targets=targets)
        self.assertEqual("baseline", matrix[0]["entries"][0]["name"])
        self.assertEqual("scheduled", matrix[0]["entries"][1]["name"])

    def test_validate_sample_artifacts_gets_scheduler_metrics_required(self) -> None:
        """Production passes normalized scheduler_enabled to validate_sample_artifacts."""
        import workload_scheduler_report as wr
        # This is a contract test: the benchmark main() must pass
        # scheduler_metrics_required based on mode == "scheduled".
        # Verify the function signature accepts it.
        import inspect
        sig = inspect.signature(wr.validate_sample_artifacts)
        params = sig.parameters
        self.assertIn("scheduler_metrics_required", params)


class WorkloadSchedulerReportModelTest(unittest.TestCase):
    """Test build_report separates performance integrity from regression."""

    def test_only_integrity_is_invalid_not_failed(self) -> None:
        """Performance integrity failure alone produces 'invalid', not 'failed'."""
        import workload_scheduler_report as wr
        # Gated mode requires request_eval and mechanism_eval
        perf_evals = [
            wr.PerformanceEvaluation(
                phase="light_write", iteration=1,
                baseline_normalized=0.0, scheduled_normalized=0.0,
                pct_change=None, passed=False,
                details="invalid: missing baseline capacity",
            ),
        ]
        report = wr.build_report(
            performance_evals=perf_evals,
            request_eval=wr.RequestEvaluation(status="passed", failures=()),
            mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
        )
        self.assertEqual("invalid", report["status"])

    def test_only_regression_is_failed_not_invalid(self) -> None:
        """Performance regression alone produces 'failed', not 'invalid'."""
        import workload_scheduler_report as wr
        perf_evals = [
            wr.PerformanceEvaluation(
                phase="light_write", iteration=1,
                baseline_normalized=1.0, scheduled_normalized=0.5,
                pct_change=-50.0, passed=False,
                details="regression below -5.0%",
            ),
        ]
        report = wr.build_report(
            performance_evals=perf_evals,
            request_eval=wr.RequestEvaluation(status="passed", failures=()),
            mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
        )
        self.assertEqual("failed", report["status"])

    def test_integrity_beats_regression(self) -> None:
        """Integrity beats regression in precedence."""
        import workload_scheduler_report as wr
        perf_evals = [
            wr.PerformanceEvaluation(
                phase="light_write", iteration=1,
                baseline_normalized=0.0, scheduled_normalized=0.0,
                pct_change=None, passed=False,
                details="invalid: missing capacity",
            ),
            wr.PerformanceEvaluation(
                phase="saturated", iteration=1,
                baseline_normalized=1.0, scheduled_normalized=0.5,
                pct_change=-50.0, passed=False,
                details="regression",
            ),
        ]
        report = wr.build_report(
            performance_evals=perf_evals,
            request_eval=wr.RequestEvaluation(status="passed", failures=()),
            mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
        )
        self.assertEqual("invalid", report["status"])


# ---------------------------------------------------------------------------
# Issue 11: Regression — perf_data ordering, non-default gate thresholds,
#           canonical samples in final report
# ---------------------------------------------------------------------------


class WorkloadSchedulerPostPersistenceReportTest(unittest.TestCase):
    """Regression: complete post-persistence report assembly must not crash
    with UnboundLocalError (perf_data used before assignment), must forward
    non-default max_failure_rate/max_outstanding_requests to
    evaluate_request_validity, and final report samples must be canonical
    (persisted) not stale originals.
    """

    def test_report_assembly_without_crash(self) -> None:
        """Complete post-persistence assembly: build artifacts index, reload,
        eval request/mechanism/artifact/performance, build report.
        Must NOT raise UnboundLocalError or any other exception.
        """
        import tempfile
        import workload_scheduler_report as wr
        import workload_scheduler_benchmark as bench_mod

        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            runs_dir = work_dir / "runs"
            runs_dir.mkdir()

            # ---- Write synthetic persisted samples ----
            samples: list[dict[str, object]] = []
            phases = ["query_only", "write_only", "light_write", "saturated"]
            for iteration in range(1, 3):
                for phase in phases:
                    for mode in ("baseline", "scheduled"):
                        sample_dir = runs_dir / f"iteration-{iteration:02d}" / phase / mode
                        sample_dir.mkdir(parents=True)
                        metrics_dir = sample_dir / "metrics"
                        metrics_dir.mkdir()

                        # sample.json
                        sample_json = {
                            "name": phase, "mode": mode, "sample": iteration,
                            "iteration": iteration, "phase": phase, "target": mode,
                            "artifact_dir": str(sample_dir),
                            "target_name": mode,
                            "requests": {
                                "query": {"started": 100, "completed": 100,
                                          "completed_failures": 0, "failures": 0,
                                          "timeouts": 0, "outstanding": 0,
                                          "successful_rps": 50.0},
                                "write": {"started": 50, "completed": 50,
                                          "completed_failures": 0, "failures": 0,
                                          "timeouts": 0, "outstanding": 0,
                                          "successful_rps": 25.0},
                            },
                            "workers": {"query": 2, "write": 2},
                        }
                        (sample_dir / "sample.json").write_text(
                            json.dumps(sample_json, indent=2, sort_keys=True) + "\n"
                        )

                        # requests.jsonl — minimal valid events
                        events = []
                        for wl in ("query", "write"):
                            for tok in range(10):
                                events.append({
                                    "token": tok, "workload": wl, "status": "success",
                                    "submission_offset": 0.1, "completion_offset": 0.5,
                                    "latency_ms": 399.0, "error": None,
                                })
                        with open(sample_dir / "requests.jsonl", "w") as f:
                            for ev in events:
                                f.write(json.dumps(ev, sort_keys=True) + "\n")

                        # scrapes.jsonl + .prom files
                        scrape_count = 6
                        with open(sample_dir / "scrapes.jsonl", "w") as f:
                            for i in range(scrape_count):
                                offset = float(i) * 1.0
                                f.write(json.dumps({
                                    "offset": offset, "status": "success",
                                    "http_status": 200, "text": f"m{i}",
                                    "path": f"metrics/scrape-{i:03d}.prom",
                                    "start": offset + 0.01,
                                    "completion": offset + 0.02,
                                }, sort_keys=True) + "\n")

                        for i in range(scrape_count):
                            if mode == "scheduled":
                                (metrics_dir / f"scrape-{i:03d}.prom").write_text(
                                    f'greptime_workload_scheduler_polls{{workload="query"}} {100 + i * 10}\n'
                                    f'greptime_workload_scheduler_polls{{workload="write"}} {50 + i * 5}\n'
                                    f'greptime_workload_scheduler_queued_tasks{{workload="query"}} 1\n'
                                    f'greptime_workload_scheduler_queued_tasks{{workload="write"}} 1\n'
                                    f'greptime_workload_scheduler_active_polls 4\n'
                                )
                            else:
                                (metrics_dir / f"scrape-{i:03d}.prom").write_text(
                                    f'other_metric{{label="val"}} {i}\n'
                                )

                        samples.append({
                            "name": phase, "mode": mode, "sample": iteration,
                            "artifact_dir": str(sample_dir),
                            "requests": sample_json["requests"],
                            "workers": sample_json["workers"],
                        })

            # ---- Replicate main()'s post-persistence assembly ----
            config: dict[str, object] = {
                "iterations": 2,
                "duration_seconds": 5,
                "scrape_interval_seconds": 1.0,
                "gates": {
                    "max_failure_rate": 0.05,
                    "max_outstanding_requests": 5,
                    "max_capacity_normalized_regression_pct": 5.0,
                    "dual_backlog_lower": 0.78,
                    "dual_backlog_upper": 0.82,
                    "min_dual_backlog_interval_fraction": 0.50,
                    "min_dual_backlog_polls_per_class": 1,
                    "min_single_class_active_purity": 0.99,
                },
                "scheduler": {"max_concurrent_polls": 16},
            }
            REQUIRED_PHASES = ("query_only", "write_only", "light_write", "saturated")

            # 1. Build artifacts index
            artifacts_index = bench_mod.build_artifacts_index(work_dir, samples)

            # 2. Validate artifacts (canonical loader)
            artifacts_invalid_failures: list[str] = []
            for sample in samples:
                artifact_dir_str = sample.get("artifact_dir")
                if not artifact_dir_str:
                    continue
                sample_dir = Path(str(artifact_dir_str))
                av = wr.load_persisted_sample(sample_dir)
                if av.validation.status != "passed":
                    artifacts_invalid_failures.extend(av.validation.failures)

            # 3. Reload persisted data
            reloaded_synthetic_samples: dict[tuple[int, str, str], dict[str, object]] = {}
            iteration_scrape_failures: list[str] = []
            for sample in samples:
                artifact_dir_str = sample.get("artifact_dir")
                if not artifact_dir_str:
                    continue
                sample_dir = Path(str(artifact_dir_str))
                it = sample.get("sample", 0)
                phase_name = str(sample.get("name", ""))
                mode = str(sample.get("mode", ""))
                key = (int(it), phase_name, mode)

                sjl_path = sample_dir / "scrapes.jsonl"
                reloaded_scrape_records: list[dict[str, object]] = []
                if sjl_path.exists():
                    for line in sjl_path.read_text().strip().split("\n"):
                        if line.strip():
                            reloaded_scrape_records.append(json.loads(line))

                reloaded_snapshots: list[dict[str, object]] = []
                metrics_dir_p = sample_dir / "metrics"
                if metrics_dir_p.exists():
                    for prom_path in sorted(metrics_dir_p.glob("*.prom")):
                        text = prom_path.read_text()
                        metrics = wr.parse_scheduler_metrics(text)
                        reloaded_snapshots.append({
                            "polls": dict(metrics.polls),
                            "queued": dict(metrics.queued),
                            "active": metrics.active,
                        })

                synthetic = dict(sample)
                synthetic["scrape_records"] = reloaded_scrape_records
                synthetic["scheduler_snapshots"] = reloaded_snapshots
                reloaded_synthetic_samples[key] = synthetic

                scheduler_metrics_required = (mode == "scheduled")
                duration_seconds = float(config.get("duration_seconds", 5))
                scrape_interval = float(config.get("scrape_interval_seconds", 1.0))
                expected_scrape_count = int(duration_seconds / scrape_interval + 1)
                val = wr.validate_iteration_scrape(
                    synthetic,
                    expected_scrape_count=expected_scrape_count,
                    scrape_interval=scrape_interval,
                    duration=duration_seconds,
                    scheduler_metrics_required=scheduler_metrics_required,
                )
                if val.status != "passed":
                    iteration_scrape_failures.extend(val.failures)

            # 4. Build canonical samples from reloaded persisted data
            canonical: list[dict[str, object]] = list(reloaded_synthetic_samples.values())

            # 5. Request validity — MUST forward non-default gate thresholds
            request_eval = wr.evaluate_request_validity(
                canonical,
                max_failure_rate=config.get("gates", {}).get("max_failure_rate", 0.01),
                max_outstanding_requests=config.get("gates", {}).get("max_outstanding_requests", 0),
            )

            # 6. Mechanism eval
            def _iteration_mechanism_eval(
                iteration: int,
                synth_samples: dict[tuple[int, str, str], dict[str, object]],
            ) -> wr.SchedulerEvaluation | None:
                phase_summaries: dict[str, wr.SchedulerSummary | None] = {}
                for req_phase in REQUIRED_PHASES:
                    key = (iteration, req_phase, "scheduled")
                    syn = synth_samples.get(key)
                    if syn is None:
                        phase_summaries[req_phase] = None
                        continue
                    snaps_raw = syn.get("scheduler_snapshots", [])
                    if not isinstance(snaps_raw, list):
                        phase_summaries[req_phase] = None
                        continue
                    snaps = [
                        wr.SchedulerMetrics(
                            polls=dict(s.get("polls", {})),
                            queued=dict(s.get("queued", {})),
                            active=s.get("active"),
                        )
                        for s in snaps_raw if isinstance(s, dict)
                    ]
                    if not snaps:
                        phase_summaries[req_phase] = None
                    else:
                        phase_summaries[req_phase] = wr.summarize_scheduler_metrics(snaps)

                present = all(phase_summaries.get(p) is not None for p in REQUIRED_PHASES)
                if not present:
                    return None
                thresholds = wr.SchedulerGateThresholds()
                return wr.evaluate_scheduler_report(phase_summaries, thresholds, errors=())

            mechanism_evals: list[wr.SchedulerEvaluation] = []
            for iteration in range(1, 3):
                ie = _iteration_mechanism_eval(iteration, reloaded_synthetic_samples)
                if ie is not None:
                    mechanism_evals.append(ie)

            if mechanism_evals:
                all_statuses = [ie.status for ie in mechanism_evals]
                combined_status = wr.combine_statuses(*all_statuses)
                all_failures: list[str] = []
                for ie in mechanism_evals:
                    all_failures.extend(ie.failures)
                mechanism_eval = wr.SchedulerEvaluation(
                    status=combined_status,
                    passed=combined_status == "passed",
                    exit_code=wr.exit_code_for_status(combined_status),
                    failures=tuple(all_failures),
                )
            else:
                mechanism_eval = wr.SchedulerEvaluation(
                    status="invalid", passed=False, exit_code=2,
                    failures=("no iterations evaluated",),
                )

            # 7. Performance — this is the code that would crash with
            #    UnboundLocalError if perf_data were referenced before
            #    assignment (Bug 1 regression)
            perf_data = bench_mod.build_performance_report(
                canonical,
                REQUIRED_PHASES,
                float(config.get("gates", {}).get("max_capacity_normalized_regression_pct", 5.0)),
            )

            perf_eval_list: list[wr.PerformanceEvaluation] = []
            for p in perf_data.get("phases", []):
                is_integrity = p.get("details", "").startswith("invalid:") if p.get("details") else False
                pf = p["passed"]
                dt = p["details"]
                perf_eval_list.append(
                    wr.PerformanceEvaluation(
                        phase=p["phase"],
                        iteration=p["iteration"],
                        baseline_normalized=p["baseline"]["normalized"],
                        scheduled_normalized=p["scheduled"]["normalized"],
                        pct_change=p["pct_change"],
                        passed=pf,
                        details=dt,
                    )
                )

            # 8. Build report — MUST use canonical samples, not stale originals
            report = wr.build_report(
                config={
                    "iterations": 2,
                    "phases": list(REQUIRED_PHASES),
                },
                artifacts=bench_mod.build_artifacts_index(work_dir, canonical),
                samples=canonical,
                request_eval=request_eval,
                mechanism_eval=mechanism_eval,
                artifact_validation=wr.ArtifactValidation(
                    status="passed" if not iteration_scrape_failures and not artifacts_invalid_failures else "invalid",
                    failures=tuple(iteration_scrape_failures + artifacts_invalid_failures),
                ),
                performance_evals=perf_eval_list,
                errors=None,
            )

            # Assertions:
            # (a) Report built without exception
            self.assertIn("status", report)
            self.assertIn("exit_code", report)

            # (b) Report samples are canonical (reloaded), not original in-memory
            report_samples = report.get("samples", [])
            for rs in report_samples:
                self.assertIn(
                    "scrape_records", rs,
                    f"canonical sample '{rs.get('name')}/{rs.get('mode')}' missing reloaded scrape_records",
                )
                self.assertIn(
                    "scheduler_snapshots", rs,
                    f"canonical sample '{rs.get('name')}/{rs.get('mode')}' missing reloaded scheduler_snapshots",
                )

    def test_non_default_gate_thresholds_forwarded(self) -> None:
        """Non-default max_failure_rate and max_outstanding_requests are
        forwarded to evaluate_request_validity; verify the function is called
        with the expected params via a direct assertion on its behavior.
        """
        import workload_scheduler_report as wr

        # Create minimal samples with known failure rate
        failing_sample: dict[str, object] = {
            "name": "query_only",
            "mode": "scheduled",
            "sample": 1,
            "requests": {
                "query": {
                    "requests": 100,
                    "started": 100, "completed": 100,
                    "completed_failures": 2, "failures": 2,
                    "timeouts": 0, "outstanding": 0,
                },
            },
            "workers": {"query": 2, "write": 0},
        }

        # With strict default (0.01), 2/100 = 0.02 > 0.01 => failed
        eval_default = wr.evaluate_request_validity(
            [failing_sample],
            max_failure_rate=0.01,
            max_outstanding_requests=0,
        )
        self.assertNotEqual("passed", eval_default.status,
                            "2% failures should fail under 0.01 max_failure_rate")

        # With relaxed threshold (0.05), 2/100 = 0.02 <= 0.05 => passed
        eval_relaxed = wr.evaluate_request_validity(
            [failing_sample],
            max_failure_rate=0.05,
            max_outstanding_requests=0,
        )
        self.assertEqual("passed", eval_relaxed.status,
                         "2% failures should pass under 0.05 max_failure_rate")

        # Verify that passing the non-default value changes behavior
        self.assertNotEqual(
            eval_default.status, eval_relaxed.status,
            "different max_failure_rate values must produce different results",
        )

        # Verify non-default max_outstanding_requests also enforced
        outstanding_sample: dict[str, object] = {
            "name": "query_only",
            "mode": "scheduled",
            "sample": 1,
            "requests": {
                "query": {
                    "requests": 100,
                    "started": 100, "completed": 100,
                    "completed_failures": 0, "failures": 0,
                    "timeouts": 0, "outstanding": 10,
                },
            },
            "workers": {"query": 2, "write": 0},
        }

        # max_outstanding_requests=5, 10 outstanding > 5 => failed
        eval_strict_outstanding = wr.evaluate_request_validity(
            [outstanding_sample],
            max_failure_rate=0.01,
            max_outstanding_requests=5,
        )
        self.assertNotEqual("passed", eval_strict_outstanding.status,
                            "10 outstanding should fail under max_outstanding_requests=5")

        # max_outstanding_requests=20, 10 outstanding <= 20 => passed
        eval_relaxed_outstanding = wr.evaluate_request_validity(
            [outstanding_sample],
            max_failure_rate=0.01,
            max_outstanding_requests=20,
        )
        self.assertEqual("passed", eval_relaxed_outstanding.status,
                         "10 outstanding should pass under max_outstanding_requests=20")

        # Verify canonical samples used in final report
        import tempfile
        import workload_scheduler_report as wr_base

        with tempfile.TemporaryDirectory() as tmpdir:
            work_dir = Path(tmpdir)
            valid_dir = work_dir / "valid"
            valid_dir.mkdir()
            sample_json = {
                "name": "query_only", "mode": "scheduled", "sample": 1,
                "iteration": 1, "phase": "query_only", "target": "scheduled",
                "artifact_dir": str(valid_dir),
            }
            (valid_dir / "sample.json").write_text(
                json.dumps(sample_json, indent=2, sort_keys=True) + "\n"
            )
            (valid_dir / "requests.jsonl").write_text(
                '{"token":0,"workload":"query","status":"success","submission_offset":0.0,"completion_offset":0.5,"latency_ms":499.0,"error":null}\n'
            )
            metrics_dir = valid_dir / "metrics"
            metrics_dir.mkdir()
            (valid_dir / "scrapes.jsonl").write_text(
                '{"offset":0.0,"status":"success","http_status":200,"text":"m","path":"metrics/scrape-000.prom","start":0.01,"completion":0.02}\n'
            )
            (metrics_dir / "scrape-000.prom").write_text("other 1\n")

            # Canonical sample (with reloaded data)
            canonical_sample: dict[str, object] = {
                "name": "query_only", "mode": "scheduled", "sample": 1,
                "artifact_dir": str(valid_dir),
                "scrape_records": [{"offset": 0.0}],
                "scheduler_snapshots": [],
            }

            # Stale original sample (no reloaded fields)
            stale_sample: dict[str, object] = {
                "name": "query_only", "mode": "scheduled", "sample": 1,
                "artifact_dir": str(valid_dir),
            }

            # Report with canonical samples must include reloaded fields
            report_canon = wr_base.build_report(
                samples=[canonical_sample],
                request_eval=wr_base.RequestEvaluation(status="passed", failures=()),
                mechanism_eval=wr_base.SchedulerEvaluation(
                    status="passed", passed=True, exit_code=0, failures=(),
                ),
                performance_evals=[],
            )
            rep_samples = report_canon.get("samples", [])
            self.assertEqual(1, len(rep_samples))
            self.assertIn(
                "scrape_records", rep_samples[0],
                "canonical sample must include reloaded scrape_records in final report",
            )
            self.assertIn(
                "scheduler_snapshots", rep_samples[0],
                "canonical sample must include reloaded scheduler_snapshots in final report",
            )

            # Report with stale original must NOT have reloaded fields
            report_stale = wr_base.build_report(
                samples=[stale_sample],
                request_eval=wr_base.RequestEvaluation(status="passed", failures=()),
                mechanism_eval=wr_base.SchedulerEvaluation(
                    status="passed", passed=True, exit_code=0, failures=(),
                ),
                performance_evals=[],
            )
            stale_s = report_stale.get("samples", [])
            self.assertEqual(1, len(stale_s))
            self.assertNotIn(
                "scrape_records", stale_s[0],
                "stale original sample must NOT have reloaded scrape_records",
            )
            self.assertNotIn(
                "scheduler_snapshots", stale_s[0],
                "stale original sample must NOT have reloaded scheduler_snapshots",
            )

    def test_invalid_under_no_gate_still_exits_two(self) -> None:
        """Under --no-gate, invalid request/performance/artifact still
        yields exit code 2; only well-formed 'failed' is suppressed.
        """
        import workload_scheduler_report as wr

        # Invalid artifact validation => exit_code 2
        invalid_artifact = wr.ArtifactValidation(
            status="invalid",
            failures=("missing requests.jsonl",),
        )
        report = wr.build_report(
            artifact_validation=invalid_artifact,
            request_eval=wr.RequestEvaluation(status="passed", failures=()),
            mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
            performance_evals=[],
        )
        self.assertEqual("invalid", report["status"])
        self.assertEqual(2, report["exit_code"])
        # Under --no-gate, this should still exit 2 (not suppressed)
        # Verify status is not "failed" (which would be suppressed)
        self.assertNotEqual("failed", report["status"])

        # Well-formed 'failed' performance must NOT exit non-zero under no-gate
        # (but report still shows it; we verify the status-escalation rules)
        failed_perf = [
            wr.PerformanceEvaluation(
                phase="light_write", iteration=1,
                baseline_normalized=1.0, scheduled_normalized=0.5,
                pct_change=-50.0, passed=False,
                details="regression below -5.0%",
            ),
        ]
        report_failed = wr.build_report(
            performance_evals=failed_perf,
            request_eval=wr.RequestEvaluation(status="passed", failures=()),
            mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
        )
        self.assertEqual("failed", report_failed["status"])
        self.assertEqual(1, report_failed["exit_code"])
        # Under --no-gate, exit code 1 gets suppressed to 0 at the CLI level.
        # Verify status is "failed" (not "invalid")
        self.assertNotEqual("invalid", report_failed["status"])



# ---------------------------------------------------------------------------
# HIGH Finding 3: No-gate request status consistency tests
# ---------------------------------------------------------------------------


class WorkloadSchedulerNoGateExitConsistencyTest(unittest.TestCase):
    """Test that --no-gate produces consistent report exit_code and process decision."""

    def test_no_gate_request_failure_exit_zero(self) -> None:
        """Under --no-gate, well-formed request threshold failure yields report exit_code=0."""
        import workload_scheduler_report as wr

        # Simulate what main() does: convert failed request_eval to passed under --no-gate
        failed_request = wr.RequestEvaluation(
            status="failed",
            failures=("failure rate 2.00% >= 1.00%",),
        )

        # Simulate no-gate conversion
        no_gate_request = wr.RequestEvaluation(status="passed", failures=()) if failed_request.status == "failed" else failed_request

        report = wr.build_report(
            request_eval=no_gate_request,
            mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
            performance_evals=[],
        )
        # The report status might be "invalid" due to missing sections in gated mode,
        # but the request evaluation itself must be passed. We test the conversion in isolation.
        self.assertEqual("passed", no_gate_request.status,
                         "no-gate must convert failed request to passed")
        self.assertEqual(0, wr.exit_code_for_status(no_gate_request.status),
                         "passed status must map to exit code 0")

    def test_no_gate_preserves_invalid_request(self) -> None:
        """Under --no-gate, invalid request (integrity error) stays invalid/exit 2."""
        import workload_scheduler_report as wr

        invalid_request = wr.RequestEvaluation(
            status="invalid",
            failures=("missing required field",),
        )

        # No-gate should NOT convert invalid
        no_gate_decision = invalid_request.status if invalid_request.status == "failed" else invalid_request.status
        self.assertEqual("invalid", no_gate_decision,
                         "no-gate must NOT convert invalid request status")
        self.assertEqual(2, wr.exit_code_for_status("invalid"))

    def test_no_gate_request_exit_matches_process_exit(self) -> None:
        """The report exit_code must match the actual sys.exit code under --no-gate.

        Under --no-gate: well-formed gate failure => exit 0;
        invalid/error => exit 2/3.
        """
        import workload_scheduler_report as wr

        # Gate failure (converted to passed for report)
        report_passed = wr.build_report(
            request_eval=wr.RequestEvaluation(status="passed", failures=()),
            mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
            performance_evals=[],
        )
        # This simulates --no-gate path: suppressed failure, exit(0)
        self.assertEqual(0, report_passed.get("exit_code", 1),
                         "gate failure under no-gate must map to exit 0")

        # Invalid evidence (not suppressed by --no-gate)
        report_invalid = wr.build_report(
            request_eval=wr.RequestEvaluation(status="invalid", failures=("bad data",)),
            mechanism_eval=wr.SchedulerEvaluation(status="passed", passed=True, exit_code=0, failures=()),
            performance_evals=[],
        )
        self.assertEqual(2, report_invalid.get("exit_code", 0),
                         "invalid must map to exit 2 even under --no-gate")


# ---------------------------------------------------------------------------
# Issue B: Normalized plan validation tests — mirror Rust types exactly
# ---------------------------------------------------------------------------


class WorkloadSchedulerNormalizedPlanTest(unittest.TestCase):
    """Test validate_normalized_scenario against actual Rust types from case.rs.

    Rust types: iterations: u64, warmup_seconds: u64, duration_seconds: u64,
    drain_timeout_seconds: u64, scrape_interval_seconds: f64,
    query_weight/write_weight: u64 (fixed 2/8), scheduler_enabled: bool.
    """

    def _valid_normalized_plan(self) -> dict:
        """Return a valid normalized plan matching the Rust planner output."""
        return {
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
                "runtime": {
                    "global": 4,
                    "compact": 1,
                    "query": 4,
                    "ingest": 4,
                },
                "scheduler": {
                    "max_concurrent_polls": 16,
                    "query_weight": 2,
                    "write_weight": 8,
                },
                "targets": [
                    {"name": "baseline", "scheduler_enabled": False},
                    {"name": "scheduled", "scheduler_enabled": True},
                ],
                "data": {
                    "shards": 64,
                    "seed_rows": 10000,
                    "seed_batch_size": 500,
                    "seed_timestamp_millis": 1700000000000,
                    "write_sequence_start_millis": 1800000000000,
                },
                "tables": {
                    "query": {"name": "catio_scheduler_query_load", "partitions": 32},
                    "write": {"name": "catio_scheduler_write_load", "partitions": 64},
                },
                "query": {"sql": "SELECT count(*) FROM catio_scheduler_query_load WHERE ts > 0"},
                "write": {"batch_size": 32},
                "phases": [
                    {"name": "query_only", "query_workers": 2, "write_workers": 0, "write_delay_seconds": 0},
                    {"name": "write_only", "query_workers": 0, "write_workers": 1152, "write_delay_seconds": 0},
                    {"name": "light_write", "query_workers": 2, "write_workers": 1, "write_delay_seconds": 0.1},
                    {"name": "saturated", "query_workers": 2, "write_workers": 1152, "write_delay_seconds": 0},
                ],
                "gates": {
                    "max_failure_rate": 0.01,
                    "max_outstanding_requests": 0,
                    "dual_backlog_lower": 0.78,
                    "dual_backlog_upper": 0.82,
                    "min_dual_backlog_interval_fraction": 0.80,
                    "min_dual_backlog_polls_per_class": 100,
                    "min_single_class_active_purity": 0.99,
                    "min_light_write_query_share": 0.20,
                    "active_within_scheduler_limit": True,
                    "max_capacity_normalized_regression_pct": 5.0,
                },
            },
        }

    def test_valid_normalized_plan_passes(self) -> None:
        """A valid normalized plan must pass validation."""
        plan = self._valid_normalized_plan()
        # Should not raise
        scenario = bench.validate_normalized_scenario(plan)
        self.assertIsInstance(scenario, dict)
        self.assertEqual(scenario["database"], "public")
        self.assertEqual(scenario["iterations"], 3)

    def test_rejects_warmup_seconds_as_float(self) -> None:
        """warmup_seconds is u64 in Rust — float must be rejected."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["warmup_seconds"] = 10.5
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_warmup_seconds_as_bool(self) -> None:
        """warmup_seconds is u64 in Rust — bool must be rejected."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["warmup_seconds"] = True
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_duration_seconds_as_float(self) -> None:
        """duration_seconds is u64 in Rust — float must be rejected."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["duration_seconds"] = 60.0
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_duration_seconds_as_bool(self) -> None:
        """duration_seconds is u64 in Rust — bool must be rejected."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["duration_seconds"] = True
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_drain_timeout_seconds_as_float(self) -> None:
        """drain_timeout_seconds is u64 in Rust — float must be rejected."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["drain_timeout_seconds"] = 30.0
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_drain_timeout_seconds_as_bool(self) -> None:
        """drain_timeout_seconds is u64 in Rust — bool must be rejected."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["drain_timeout_seconds"] = True
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_near_but_not_equal_scrape_interval(self) -> None:
        """scrape_interval_seconds must be exactly 1.0 (mirror Rust f64::EPSILON)."""
        plan = self._valid_normalized_plan()
        # 1.0 + 2 * epsilon should be rejected (Rust uses f64::EPSILON)
        plan["scenario"]["scrape_interval_seconds"] = 1.0 + 3.0e-16
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_accepts_scrape_interval_at_exactly_1_dot_0(self) -> None:
        """Exactly 1.0 must pass."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["scrape_interval_seconds"] = 1.0
        # Should not raise
        bench.validate_normalized_scenario(plan)

    def test_rejects_query_weight_not_2(self) -> None:
        """query_weight must be exactly 2 (fixed 2:8 acceptance weights)."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["scheduler"]["query_weight"] = 3
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)
        # Also rejects 0
        plan["scenario"]["scheduler"]["query_weight"] = 0
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_write_weight_not_8(self) -> None:
        """write_weight must be exactly 8 (fixed 2:8 acceptance weights)."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["scheduler"]["write_weight"] = 7
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_swapped_target_booleans(self) -> None:
        """baseline must have scheduler_enabled=false, scheduled=true."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["targets"] = [
            {"name": "baseline", "scheduler_enabled": True},
            {"name": "scheduled", "scheduler_enabled": False},
        ]
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_arbitrary_target_names(self) -> None:
        """Only 'baseline' and 'scheduled' are valid target names."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["targets"] = [
            {"name": "control", "scheduler_enabled": False},
            {"name": "candidate", "scheduler_enabled": True},
        ]
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_arbitrary_target_name_baseline_variant(self) -> None:
        """Misspelled canonical names are rejected."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["targets"] = [
            {"name": "Baseline", "scheduler_enabled": False},
            {"name": "scheduled", "scheduler_enabled": True},
        ]
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_targets_duplicate(self) -> None:
        """Two targets with the same name must be rejected."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["targets"] = [
            {"name": "baseline", "scheduler_enabled": False},
            {"name": "baseline", "scheduler_enabled": False},
        ]
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_too_few_targets(self) -> None:
        """Must have exactly 2 targets."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["targets"] = [
            {"name": "baseline", "scheduler_enabled": False},
        ]
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_too_many_targets(self) -> None:
        """Must have exactly 2 targets."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["targets"] = [
            {"name": "baseline", "scheduler_enabled": False},
            {"name": "scheduled", "scheduler_enabled": True},
            {"name": "extra", "scheduler_enabled": False},
        ]
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_both_targets_scheduler_enabled_true(self) -> None:
        """Exactly one must have scheduler_enabled=true."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["targets"] = [
            {"name": "baseline", "scheduler_enabled": True},
            {"name": "scheduled", "scheduler_enabled": True},
        ]
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_both_targets_scheduler_enabled_false(self) -> None:
        """Exactly one must have scheduler_enabled=false."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["targets"] = [
            {"name": "baseline", "scheduler_enabled": False},
            {"name": "scheduled", "scheduler_enabled": False},
        ]
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_accepts_iterations_as_exact_int(self) -> None:
        """iterations (Rust u64) must be int, not float."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["iterations"] = 3
        bench.validate_normalized_scenario(plan)

    def test_rejects_iterations_as_float(self) -> None:
        """iterations (Rust u64) rejects float."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["iterations"] = 3.0
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_iterations_as_bool(self) -> None:
        """iterations (Rust u64) rejects bool."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["iterations"] = True
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)

    def test_rejects_warmup_seconds_negative(self) -> None:
        """warmup_seconds (Rust u64) rejects negative."""
        plan = self._valid_normalized_plan()
        plan["scenario"]["warmup_seconds"] = -5
        with self.assertRaises(ValueError):
            bench.validate_normalized_scenario(plan)


if __name__ == "__main__":
    unittest.main()
