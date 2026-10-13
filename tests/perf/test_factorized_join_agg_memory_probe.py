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

"""Focused tests for the experiment-only process memory probe."""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

RUNNER_PATH = Path(__file__).resolve().parents[2] / ".github/scripts/query-regression-run.py"
SPEC = importlib.util.spec_from_file_location("factorized_join_runner_under_test", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class FakeProc:
    def __init__(self, pid: int) -> None:
        self.pid = pid
        self.exited = False

    def poll(self):
        return 0 if self.exited else None


class FactorizedJoinMemoryProbeTest(unittest.TestCase):
    def setUp(self) -> None:
        self.args = type("Args", (), {"http_timeout": "1", "allow_large_fixture": "false"})()

    def execute(self, tmpdir: str, *, fail_reset: bool = False, exit_on_measure: bool = False, measure_status: int = 0):
        resets = []
        cleanup = []
        proc_map = {}

        def start(target, component, processes):
            self.procs = processes
            processes[(target.name, component)] = FakeProc(len(processes) + 100)

        def run(command, **_kwargs):
            phase = command[1]
            if phase == "prepare-direct":
                Path(command[command.index("--output") + 1]).write_text(json.dumps({"fixtures": [{"fixture_dir": "fixture"}]}))
                return type("Result", (), {"returncode": 0})()
            if phase == "measure":
                self.assertEqual(len([c for c in commands if c[1] == "measure"]), 1)
                if exit_on_measure:
                    self.procs[("candidate", "frontend")].exited = True
                return type("Result", (), {"returncode": measure_status})()
            return type("Result", (), {"returncode": 0})()

        commands = []
        self.commands = commands

        def record_run(command, **kwargs):
            commands.append(command)
            return run(command, **kwargs)

        def reset(pid):
            resets.append(pid)
            if fail_reset:
                raise PermissionError("clear_refs denied")
            return (10, 10)

        ports = list(range(10000, 10016))
        self.cleanup = cleanup

        def stop(target, component, _processes):
            cleanup.append((target.name, component))

        with (
            patch.object(runner, "allocate_ports", return_value=ports),
            patch.object(runner, "start_component", side_effect=start),
            patch.object(runner, "stop_component", side_effect=stop),
            patch.object(runner.subprocess, "run", side_effect=record_run),
            patch.object(runner, "_reset_and_read_memory", side_effect=reset),
            patch.object(runner, "_proc_memory_kb", return_value=(11, 12)),
        ):
            result = runner.run_direct_case(
                self.args, Path("case.toml"), Path(tmpdir), Path("base"), Path("candidate"), Path("fixture"), Path("runner")
            )
        return result, resets, cleanup, commands

    def test_success_configures_arms_resets_four_processes_once_and_writes_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            result, resets, _cleanup, commands = self.execute(tmpdir)
            self.assertEqual(result, 0)
            self.assertEqual(len(resets), 4)
            self.assertEqual(sum(command[1] == "measure" for command in commands), 1)
            for target, enabled in (("base", "false"), ("candidate", "true")):
                config = Path(tmpdir) / target / "frontend-factorized-join.toml"
                self.assertIn(f"experimental_enable_factorized_join_agg = {enabled}", config.read_text())
                record_path = Path(tmpdir) / target / "logs/factorized-join-memory.json"
                record = json.loads(record_path.read_text())
                self.assertEqual(record["measure_return_code"], 0)
                self.assertEqual(record["interval_scope"], "enclosing_capture_interval; component resets and reads are sequential")
                self.assertEqual(len(record["components"]), 2)
                self.assertTrue(all(component["status"] == "complete" for component in record["components"]))

    def test_reset_failure_fails_and_cleans_up_without_memory_artifacts(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaises(PermissionError):
                self.execute(tmpdir, fail_reset=True)
            self.assertEqual(
                set(self.cleanup),
                {(target, component) for target in ("base", "candidate") for component in ("frontend", "datanode", "metasrv")},
            )
            self.assertEqual(sum(command[1] == "measure" for command in self.commands), 0)
            self.assertFalse(list(Path(tmpdir).glob("*/logs/factorized-join-memory.json")))

    def test_process_exit_after_measure_fails_capture(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            with self.assertRaisesRegex(RuntimeError, "exited during measurement"):
                self.execute(tmpdir, exit_on_measure=True)
            self.assertFalse(list(Path(tmpdir).glob("*/logs/factorized-join-memory.json")))

    def test_nonzero_measure_is_not_retried(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            result, _resets, _starts, commands = self.execute(tmpdir, measure_status=7)
            self.assertEqual(result, 7)
            self.assertEqual(sum(command[1] == "measure" for command in commands), 1)
            self.assertEqual(json.loads((Path(tmpdir) / "base/logs/factorized-join-memory.json").read_text())["measure_return_code"], 7)

    def test_memory_parser_rejects_missing_and_malformed_fields(self) -> None:
        for contents in ("VmRSS: 5 kB\n", "VmRSS: invalid kB\nVmHWM: 5 kB\n", "VmRSS: 5 MB\nVmHWM: 5 kB\n"):
            with self.subTest(contents=contents), patch.object(Path, "read_text", return_value=contents):
                with self.assertRaises(RuntimeError):
                    runner._proc_memory_kb(123)


if __name__ == "__main__":
    unittest.main()
