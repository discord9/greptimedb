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

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

PATH = Path(__file__).with_name("query-regression-run.py")
SPEC = importlib.util.spec_from_file_location("query_regression_run", PATH)
assert SPEC and SPEC.loader
m = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = m
SPEC.loader.exec_module(m)


class DispatchTest(unittest.TestCase):
    def test_distributed_kind_uses_integrated_runner(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); case = root / "case.toml"; case.write_text("")
            fixture = root / "fixture"; fixture.write_text("")
            args = type("A", (), {"cargo_profile":"dev", "base_bin":root/"base", "candidate_bin":root/"candidate", "fixture_generator":fixture, "base_src":root, "candidate_src":root, "http_timeout":"1", "allow_large_fixture":"false", "kube_context":"ctx", "kubectl":"kubectl", "namespace_prefix":"prefix", "base_image":"image", "same_binary_ab":True, "keep_namespace_on_failure":False})()
            calls=[]
            def fake(cmd, **kwargs):
                calls.append(cmd)
                if cmd[1:3] == ["plan", "--case"]:
                    return type("R", (), {"returncode":0,"stdout":json.dumps({"scenario":{"kind":"workload_scheduler_distributed"}}),"stderr":""})()
                return type("R", (), {"returncode":0})()
            with patch.object(m.subprocess, "run", side_effect=fake):
                self.assertEqual(0, m.run_case(args, case, root / "work"))
            self.assertIn("workload_scheduler_distributed_runner.py", " ".join(map(str, calls[-1])))
            self.assertIn("--same-binary-ab", calls[-1])

if __name__ == "__main__": unittest.main()
