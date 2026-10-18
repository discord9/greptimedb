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

"""The fused node's three batch regions, measured from `TQL ANALYZE` records.

`fused_evaluate_time` wraps the whole of `RangeEvaluator::push` and cannot say where inside the
evaluator the time went; a CPU sampler cannot either, because the batch kernel is inlined into the
flush and the flush into the evaluation. The build that added `fused_window_prep_time`,
`fused_kernel_time` and `fused_columns_time` records those three regions once per batch of grid
points, and this script reads them out.

It reports **ratios within one ANALYZE record**, and that is the point. The absolute timings drift
25-35% between records of the same case on this machine, so a ratio of a value from one run to a
value from another is not a measurement of anything (see the report's section 4.8). A ratio of two
values from the same record is, which is why the per-record `prep/kernel` line is printed and the
medians are labelled as medians.

One server per invocation, warmed twice, then `--repeats` ANALYZE records.

    scripts/fused_series_regions.py BIN DATA_HOME W 9
"""

import argparse
import json
import statistics
import sys

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))

import fused_plan_budget as budget
import fused_series_bench as bench
import fused_series_profile as profile

# The counter itself first, then the three regions it is made of.
KEYS = (
    "fused_evaluate_time",
    "fused_window_prep_time",
    "fused_kernel_time",
    "fused_columns_time",
)


def regions(payload):
    """`{key: ns}` for the fused node, from the first stage whose plan holds it.

    `None` for a key the plan does not carry, so a binary built before a counter existed reports
    the absence instead of a zero that reads like "this region was free".
    """
    blocks = payload["output"][0]["records"]["rows"]
    for stage, _node, plan in blocks:
        if stage is None or not plan.lstrip().startswith("{"):
            continue
        rows = []
        budget.walk(json.loads(plan), 0, rows)
        for row in rows:
            if row["name"] == "FusedSeriesExec":
                return {key: row["metrics"].get(key) for key in KEYS}
    raise SystemExit("no FusedSeriesExec in the plan; did the case fuse?")


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("bin", help="a greptime binary built after the three regions were added")
    parser.add_argument("data_home", help="the seeded fixture directory")
    parser.add_argument("case", help="a case name from fused_series_bench.CASES, e.g. W")
    parser.add_argument("repeats", nargs="?", type=int, default=9, help="ANALYZE records")
    parser.add_argument("--http", type=int, default=15150)
    parser.add_argument("--grpc", type=int, default=15152)
    parser.add_argument("--mysql", type=int, default=15154)
    parser.add_argument("--postgres", type=int, default=15156)
    parser.add_argument("--log", default="/tmp/fused-regions-server.log")
    parser.add_argument("--json", help="also write the per-record values here")
    args = parser.parse_args()

    case = next((c for c in bench.CASES if c["name"] == args.case), None)
    if case is None:
        raise SystemExit(f"no case {args.case}; known: {[c['name'] for c in bench.CASES]}")
    statement = bench.tql(case)
    server = bench.Server(
        args.bin,
        args.data_home,
        args.http,
        args.grpc,
        args.mysql,
        args.postgres,
        True,
        args.log,
    )
    # The hit count needs the fused-selection line at debug; a full `debug` filter makes
    # datafusion format the physical plan per planning pass, which the profile measured at 4.6%.
    server.log_level = "info,query::datafusion::native_promql::fused=debug"

    records = []
    with server:
        for _ in range(2):
            server.sql(statement)
        for index in range(args.repeats):
            values = regions(profile.analyze(server, case))
            if any(values[key] is None for key in KEYS):
                missing = [key for key in KEYS if values[key] is None]
                raise SystemExit(
                    f"the plan does not carry {missing}; this binary predates those counters"
                )
            records.append(values)
            ms = {key: value / 1e6 for key, value in values.items()}
            total = sum(ms[key] for key in KEYS[1:])
            print(
                f"  #{index + 1}  evaluate {ms['fused_evaluate_time']:7.2f}  "
                f"prep {ms['fused_window_prep_time']:7.2f}  "
                f"kernel {ms['fused_kernel_time']:7.2f}  "
                f"columns {ms['fused_columns_time']:6.2f}  "
                f"regions/evaluate {100.0 * total / ms['fused_evaluate_time']:5.1f}%  "
                f"prep/kernel {ms['fused_window_prep_time'] / ms['fused_kernel_time']:.3f}",
                flush=True,
            )

    print()
    print(f"case {args.case}, {len(records)} ANALYZE records from one server")
    print(f"{'region':26} {'median ms':>10} {'min':>8} {'max':>8} {'of evaluate':>12}")
    evaluate = statistics.median(record["fused_evaluate_time"] / 1e6 for record in records)
    for key in KEYS:
        values = [record[key] / 1e6 for record in records]
        median = statistics.median(values)
        print(
            f"{key:26} {median:10.2f} {min(values):8.2f} {max(values):8.2f} "
            f"{100.0 * median / evaluate:11.1f}%"
        )
    ratios = [
        record["fused_window_prep_time"] / record["fused_kernel_time"] for record in records
    ]
    above = sum(1 for ratio in ratios if ratio > 1.0)
    offsets = [abs(ratio - 1.0) for ratio in ratios]
    print()
    print(f"prep/kernel per record: {[round(ratio, 3) for ratio in ratios]}")
    print(
        f"  median {statistics.median(ratios):.3f}, "
        f"range {min(ratios):.3f}-{max(ratios):.3f}, prep above kernel in {above}/{len(ratios)}"
    )
    print(
        f"  distance from parity: median {100 * statistics.median(offsets):.1f}%, "
        f"worst {100 * max(offsets):.1f}%"
    )
    if args.json:
        with open(args.json, "w") as handle:
            json.dump({"case": args.case, "records": records}, handle, indent=2)
        print(f"wrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
