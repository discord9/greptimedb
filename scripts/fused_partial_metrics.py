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

"""Read one arm's fused-node cost evidence out of `TQL ANALYZE VERBOSE FORMAT JSON`.

Why `ANALYZE` and not `EXPLAIN`: in standalone, `TQL EXPLAIN` reports only
`CooperativeExec -> MergeScanExec` and never expands the datanode-side plan, so it shows
neither the aggregate nor the fused node. `ANALYZE` executes the query and reports one row per
stage whose `plan` column is a nested JSON string, with each operator's own registered metrics.

What this prints for the fused operator, per arm:

  - the input chunks it read, the series it completed, and -- in partial mode -- the updates,
    points, slots and state rows it produced, plus its two block-scoped timers;
  - the byte figures the operators report (`output_bytes`), which are cumulative allocations
    rather than a peak; the two are labelled apart because they answer different questions.

Anything the operator does not register is reported as `not measured`, never as zero.

    scripts/fused_partial_metrics.py --data-home DIR --bin PATH --arms off,on,sum
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fused_series_bench as bench
import fused_series_scale as scale

# `(report label, metric name, kind)`. `kind` decides how the value is read: `count` values are
# added across partitions, `time` values are nanoseconds and added, `bytes` likewise.
COUNT_METRICS = [
    ("input chunks", "fused_input_chunks"),
    ("series completed", "fused_series_completed"),
    ("series emitted", "fused_series_emitted"),
    ("partial updates", "fused_partial_updates"),
    ("partial points", "fused_partial_points"),
    ("partial slots", "fused_partial_slots"),
    ("state rows", "fused_partial_state_rows"),
]
TIME_METRICS = [
    ("evaluate", "fused_evaluate_time"),
    ("partial fold", "fused_partial_update_time"),
    ("partial state", "fused_partial_state_time"),
]


def stages_of(payload):
    """Every stage's parsed plan, from an `ANALYZE ... FORMAT JSON` response."""
    stages = []
    for block in payload.get("output", []):
        records = block.get("records") or {}
        columns = [c["name"] for c in records.get("schema", {}).get("column_schemas", [])]
        if "plan" not in columns:
            continue
        index = columns.index("plan")
        for row in records.get("rows", []):
            try:
                stages.append(json.loads(row[index]))
            except (TypeError, json.JSONDecodeError):
                continue
    return stages


def walk(node):
    """Every operator in a plan tree, depth first."""
    yield node
    for child in node.get("children") or []:
        yield from walk(child)


def fused_nodes(stages):
    return [
        node
        for stage in stages
        for node in walk(stage)
        if str(node.get("name", "")).startswith("FusedSeriesExec")
    ]


def metric_value(node, name):
    """A metric's value on one operator, or `None` when it does not report it."""
    value = (node.get("metrics") or {}).get(name)
    if isinstance(value, (int, float)):
        return value
    return None


def total(nodes, name):
    """Sum one metric across operators, or `None` when no operator reports it."""
    values = [metric_value(node, name) for node in nodes]
    values = [value for value in values if value is not None]
    return sum(values) if values else None


def report(arm, nodes, statement):
    print(f"=== arm {arm} ===")
    print(f"  statement: {statement}")
    if not nodes:
        print("  no fused operator ran")
        return
    # One line per fused operator, then the totals: a shape can fuse once per partition.
    for node in nodes:
        print(f"  {node['name']}: {node.get('param', '')}"[:160])
        print(
            f"    output rows {node.get('output_rows')}, "
            f"elapsed {node.get('elapsed_compute')} ns, "
            f"memory {node.get('memory_usage')} bytes"
        )
    print(f"  -- totals over {len(nodes)} fused operator(s)")
    for label, name in COUNT_METRICS:
        value = total(nodes, name)
        print(f"     {label:18} {value if value is not None else 'not measured'}")
    for label, name in TIME_METRICS:
        value = total(nodes, name)
        # The timers are nanoseconds of block time; printing them as such avoids implying that
        # they are CPU time or that they partition the query.
        print(
            f"     {label:18} "
            + (f"{value / 1e6:.3f} ms of block time" if value is not None else "not measured")
        )
    # Byte figures from the operators that report them, labelled as cumulative.
    for node in nodes:
        for key in ("output_bytes", "spill_count"):
            value = metric_value(node, key)
            if value is not None:
                print(f"     {key:18} {value} (cumulative, not a peak)")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-home", required=True)
    parser.add_argument("--bin", required=True)
    parser.add_argument("--arms", default="on,sum")
    parser.add_argument("--case", default="G4")
    parser.add_argument("--json", default=None, help="write the extracted figures here")
    parser.add_argument("--http", type=int, default=15090)
    parser.add_argument("--grpc", type=int, default=15092)
    parser.add_argument("--mysql", type=int, default=15094)
    parser.add_argument("--postgres", type=int, default=15096)
    parser.add_argument("--log", default="/tmp/fused-partial-metrics.log")
    args = parser.parse_args()

    cases = {case[0]: case for case in scale.CASES}
    if args.case not in cases:
        raise SystemExit(f"unknown case {args.case}; the cases are {', '.join(cases)}")
    _, inner, aggregation, _, _, _ = cases[args.case]
    statement = scale.tql(inner, aggregation)
    analyze = statement.replace("TQL EVAL ", "TQL ANALYZE VERBOSE FORMAT JSON ", 1)

    record = {"case": args.case, "statement": statement, "arms": {}}
    for arm in [a.strip() for a in args.arms.split(",") if a.strip()]:
        if arm not in bench.ARMS:
            raise SystemExit(f"unknown arm {arm!r}; the arms are {', '.join(bench.ARMS)}")
        native, overlay = bench.ARMS[arm]
        with bench.Server(
            args.bin,
            args.data_home,
            args.http,
            args.grpc,
            args.mysql,
            args.postgres,
            native,
            args.log,
            partial=overlay,
        ) as server:
            server.begin_query()
            payload, _ = server.sql(analyze)
            hits = server.hits_since()
            partials = server.partial_hits_since()
            peak = server.peak_memory_bytes()
        stages = stages_of(payload)
        nodes = fused_nodes(stages)
        report(arm, nodes, statement)
        print(f"     {'selections':18} {hits} fused, {partials} partial sum")
        print(
            f"     {'process peak RSS':18} "
            + (f"{peak / (1 << 20):.1f} MiB" if peak else "not measured")
        )
        print()
        record["arms"][arm] = {
            "fused_selections": hits,
            "partial_selections": partials,
            "peak_rss_bytes": peak,
            "operators": [
                {
                    "name": node["name"],
                    "param": node.get("param", ""),
                    "metrics": node.get("metrics") or {},
                }
                for node in nodes
            ],
            # Kept whole so a later question can be asked of the same run rather than re-run.
            "stages": stages,
        }

    if args.json:
        with open(args.json, "w") as handle:
            json.dump(record, handle, indent=2)
        print(f"wrote {args.json}")


if __name__ == "__main__":
    main()
