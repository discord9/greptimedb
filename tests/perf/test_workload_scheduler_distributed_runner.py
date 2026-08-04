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

"""No-cluster unit tests for distributed scheduler orchestration contracts."""
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
PATH = ROOT / "tests/perf/workload_scheduler_distributed_runner.py"
SPEC = importlib.util.spec_from_file_location("ws_distributed", PATH)
assert SPEC and SPEC.loader
mod = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = mod
SPEC.loader.exec_module(mod)


class DistributedContractsTest(unittest.TestCase):
    def plan(self):
        return {"schema_version": 1, "scenario": {"kind": "workload_scheduler_distributed", "topology": {r: {"replicas": n, "cpu": cpu, "memory": mem} for r, n, cpu, mem in (("frontend", 1, "1", "2Gi"), ("metasrv", 1, "1", "2Gi"), ("datanode", 3, "2", "4Gi"), ("loadgen", 1, "2", "4Gi"))}, "scheduler": {"query_weight": 2, "write_weight": 8}, "target_order": ["baseline", "scheduled", "scheduled", "baseline"]}}

    def test_exact_topology_and_order_are_required(self):
        self.assertEqual("workload_scheduler_distributed", mod.require_plan(self.plan())["kind"])
        bad = self.plan(); bad["scenario"]["topology"]["datanode"]["replicas"] = 2
        with self.assertRaises(ValueError): mod.require_plan(bad)
        bad = self.plan(); bad["scenario"]["target_order"] = ["baseline", "scheduled"]
        with self.assertRaises(ValueError): mod.require_plan(bad)

    def test_pods_are_limited_and_datanodes_are_pinnable(self):
        pod = mod.pod_manifest("datanode-0", "datanode-0", "cached", "node-a", mod.resources("2", "4Gi"))
        c = pod["spec"]["containers"][0]
        self.assertEqual("Never", c["imagePullPolicy"])
        self.assertEqual(c["resources"]["requests"], c["resources"]["limits"])
        self.assertEqual("node-a", pod["spec"]["nodeName"])
        self.assertEqual(3, len(pod["spec"]["volumes"]))

    def test_node_selection_rejects_insufficient_nodes(self):
        class Fake:
            def json(self, *_a, **_kw):
                return {"items": [{"metadata": {"name": "a"}, "spec": {}, "status": {"conditions": [{"type": "Ready", "status": "True"}]}}]}
        with self.assertRaises(ValueError): mod.select_nodes(Fake())

    def test_namespace_name_is_unique_safe_dns(self):
        self.assertEqual("scheduler-run", mod.safe_name("Scheduler Run!"))
        self.assertNotEqual(mod.uuid.uuid4().hex[:10], mod.uuid.uuid4().hex[:10])


if __name__ == "__main__":
    unittest.main()
