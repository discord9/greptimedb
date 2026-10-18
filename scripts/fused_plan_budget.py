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

"""Print one `TQL ANALYZE VERBOSE FORMAT JSON` plan as a per-operator budget.

Reads the JSON an `--analyze-only` run of `fused_series_profile.py` wrote and prints, for every
operator, its own `output_rows`, `output_batches` and its **own** named timers.

The point is to avoid the trap this project already hit twice:

* `elapsed_compute` is *not* additive down a tree. Nested operators each count their children and
  a partitioned plan counts once per partition, so summing it reports numbers larger than the
  query. This script never sums across operators; it prints each one's own counters and says so.
* Time-based operator metrics are **wall clock summed over the plan's partitions**, not CPU. Two
  operators' values are comparable to each other only when their partition counts are equal, and
  no value here is a share of the query's CPU. Use a CPU sampler for shares; use these for the
  split *within* one operator, which the sampler cannot resolve when it inlines.

    scripts/fused_plan_budget.py analyze-W-on.json [--json out.json]
"""

import argparse
import json

# The metrics that are this operator's own work. Anything else in `metrics` is either a child's
# contribution, a partition-local queue depth, or a wait.
INTERESTING = (
    "output_batches",
    "selectivity",
    "fused_input_chunks",
    "fused_series_completed",
    "fused_series_emitted",
    "fused_input_poll_time",
    "fused_evaluate_time",
    # `fused_evaluate_time` wraps `push`, which ingests, evaluates and builds columns, so it
    # says nothing about where inside the evaluator the time went. These three subdivide it per
    # batch of grid points -- the whole reason they exist is the split the sampler cannot make
    # when the kernel is inlined into `evaluate`.
    "fused_window_prep_time",
    "fused_kernel_time",
    "fused_columns_time",
    "fused_emit_time",
    "fused_flush_time",
    "aggregation_time",
    "emitting_time",
    "time_calculating_group_ids",
    "aggregate_arguments_time",
    "repartition_time",
    "send_time",
    "scan_cost",
    "convert_cost",
    "build_reader_cost",
    "dynamic_filter_cost",
    "yield_cost",
)

# `Time` metrics report nanoseconds (`Time::add_elapsed` accumulates `Duration::as_nanos`), which
# the JSON carries as a bare integer. Printed as milliseconds so they line up with the report.
# `time_calculating_group_ids` is a `Time` too, but its name does not end in `_time`.
TIME_KEYS = tuple(
    key for key in INTERESTING if key.endswith(("_time", "_cost", "_ids"))
)
NS_PER_MS = 1_000_000


def walk(node, depth, rows):
    metrics = node.get("metrics") or {}
    flat = {}
    for key, value in metrics.items():
        if isinstance(value, dict):
            # A partitioned operator nests its counters under `partition_N`: sum them, and say
            # how many partitions contributed, because the sum is wall time across partitions.
            for inner_key, inner_value in value.items():
                if isinstance(inner_value, (int, float)):
                    flat.setdefault(inner_key, 0)
                    flat[inner_key] += inner_value
        elif isinstance(value, (int, float)):
            flat[key] = value
    kept = {key: flat[key] for key in INTERESTING if key in flat}
    rows.append(
        {
            "name": node.get("name"),
            "param": node.get("param", ""),
            "depth": depth,
            "output_rows": node.get("output_rows"),
            "metrics": kept,
        }
    )
    for child in node.get("children", []):
        walk(child, depth + 1, rows)


def main():
    # argparse only for the usage text and the argument errors; this is a one-input printer.
    parser = argparse.ArgumentParser(
        description=__doc__.splitlines()[0],
        epilog="Every counter printed is the operator's own; nothing is summed across operators.",
    )
    parser.add_argument("analyze", help="the JSON an `--analyze-only` profile run wrote")
    parser.add_argument("--json", help="also write the parsed counters here")
    args = parser.parse_args()

    payload = json.load(open(args.analyze))
    blocks = payload["output"][0]["records"]["rows"]
    stages = []
    for stage, node, plan in blocks:
        if stage is None or not plan.lstrip().startswith("{"):
            continue
        rows = []
        walk(json.loads(plan), 0, rows)
        stages.append({"stage": stage, "node": node, "operators": rows})

    total = None
    for stage, node, plan in blocks:
        if stage is None and plan.startswith("Total rows:"):
            total = plan.strip()

    for stage in stages:
        print(f"=== stage {stage['stage']} node {stage['node']}")
        for row in stage["operators"]:
            print(f"{'  ' * row['depth']}{row['name']}")
            print(
                f"{'  ' * row['depth']}    output_rows={row['output_rows']}"
                + (
                    f"  output_batches={row['metrics']['output_batches']}"
                    if "output_batches" in row["metrics"]
                    else ""
                )
            )
            for key, value in row["metrics"].items():
                if key == "output_batches":
                    continue
                if key in TIME_KEYS:
                    print(f"{'  ' * row['depth']}    {key} = {value / NS_PER_MS:.3f} ms")
                else:
                    print(f"{'  ' * row['depth']}    {key} = {value}")
    if total:
        print(total)
    print()
    print(
        "Each operator's counters are its own; nothing here is summed across operators. A `_time`"
        " or `_cost` value is that block's wall clock, summed over the plan's partitions, so it"
        " tracks CPU only while the query is CPU-bound. `/proc/<pid>/stat` is the arbiter for"
        " the query's total."
    )
    if args.json:
        json.dump({"stages": stages, "total": total}, open(args.json, "w"), indent=2)
        print(f"wrote {args.json}")


if __name__ == "__main__":
    main()
