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

"""Proves which of the three execution modes one live server actually ran, and that
the three agree on the answer.

The unit tests build their nodes by hand, so they cannot show that the plan the
production entry point produces is the one being measured. This script asks a real
standalone server, over the real seeded mito table, for the physical plan of one
aggregated PromQL query, under each of the three arms `fused_series_bench.ARMS`
describes, and checks the three facts that distinguish them:

* the ordinary arm has no `FusedSeriesExec` and two `AggregateExec` (partial + final);
* the evaluated arm has a `FusedSeriesExec` without a `partial=` marker and the same
  two aggregates;
* the partial arm has a `FusedSeriesExec` displaying `partial=sum`, and exactly one
  `AggregateExec`, in `FinalPartitioned` mode -- the partial one was replaced.

A plan is not a result, so each arm's query is also run and every arm's complete answer
is compared with the ordinary plan's through `fused_series_bench.compare_pair`: schema
names and types, label tuple, timestamps, row count, then every value at a relative
tolerance of `1e-9` with NaN and infinities handled apart.

    scripts/fused_series_partial_plan.py --data-home ~/.cache/fused-scale/data-seed \\
        --bin PATH --partial-sum-step env

`--partial-sum-step` names how the partial-SUM step is switched off in this build. It is
`env` while the feature is bench scaffolding read from `GREPTIME_FUSED_PARTIAL_SUM`, and
will be `planner` once the formal planner constructs the same configuration directly and
the arms differ by nothing but that decision -- at which point the two flag-on arms both
run the partial step and this script's `on`-vs-`sum` distinction stops existing.
"""

import argparse
import re
import sys

import fused_series_bench as bench

# One `AggregateExec` line per aggregate node, e.g.
# `AggregateExec: mode=FinalPartitioned, gby=[...], aggr=[sum(...)]`.
AGGREGATE = re.compile(r"AggregateExec: mode=(\w+)")
FUSED = "FusedSeriesExec"
PARTIAL = "partial=sum"


def physical_plan(server, case):
    """The `physical_plan` row `TQL EXPLAIN` answers with, as one string."""
    statement = bench.tql(case).replace("TQL EVAL ", "TQL EXPLAIN ", 1)
    payload, _ = server.sql(statement)
    for block in payload.get("output", []):
        records = block.get("records") or {}
        rows = records.get("rows") or []
        columns = [column["name"] for column in records.get("schema", {}).get("column_schemas", [])]
        if "plan_type" in columns and "plan" in columns:
            for row in rows:
                if row[columns.index("plan_type")] == "physical_plan":
                    return row[columns.index("plan")]
    raise SystemExit(f"no physical_plan row in the TQL EXPLAIN answer for {case['name']}")


def check_plan(case, arm, plan):
    """Fails unless `plan` is the plan `arm` is defined to run."""
    modes = AGGREGATE.findall(plan)
    fused = plan.count(FUSED)
    partial = plan.count(PARTIAL)
    expected_fused = 0 if arm == "off" else 1
    # `off` and `on` keep the partial and the final aggregate; `sum` replaced the partial
    # one, so only the final one may remain, and it must still be a final one.
    expected_modes = ["Partial", "FinalPartitioned"] if arm != "sum" else ["FinalPartitioned"]
    expected_partial = 1 if arm == "sum" else 0
    if fused != expected_fused:
        raise SystemExit(
            f"case {case['name']} arm {arm}: expected {expected_fused} {FUSED} in the plan, "
            f"found {fused}:\n{plan}"
        )
    if modes != expected_modes:
        raise SystemExit(
            f"case {case['name']} arm {arm}: expected aggregate modes {expected_modes}, "
            f"found {modes}:\n{plan}"
        )
    if partial != expected_partial:
        raise SystemExit(
            f"case {case['name']} arm {arm}: expected {expected_partial} `{PARTIAL}` "
            f"marker(s), found {partial}:\n{plan}"
        )
    return {"fused": fused, "modes": modes, "partial": partial}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-home", required=True)
    parser.add_argument("--bin", required=True)
    parser.add_argument("--case", default="C", help="a case name from fused_series_bench")
    parser.add_argument("--partial-sum-step", default="env", choices=["env", "planner"])
    parser.add_argument("--http", type=int, default=15240)
    parser.add_argument("--grpc", type=int, default=15242)
    parser.add_argument("--mysql", type=int, default=15244)
    parser.add_argument("--postgres", type=int, default=15246)
    parser.add_argument("--log", default="/tmp/fused-partial-plan-server.log")
    args = parser.parse_args()

    case = next((c for c in bench.CASES if c["name"] == args.case), None)
    if case is None:
        raise SystemExit(f"no case {args.case!r}; the cases are {[c['name'] for c in bench.CASES]}")
    if not case["fuses"]:
        raise SystemExit(f"case {args.case!r} does not fuse, so it cannot show the fused modes")
    arms = list(bench.ARMS)
    statement = bench.tql(case)

    plans = {}
    results = {}
    records = {}
    for arm in arms:
        native, overlay = bench.ARMS[arm]
        server = bench.Server(
            args.bin,
            args.data_home,
            args.http,
            args.grpc,
            args.mysql,
            args.postgres,
            native,
            args.log,
            partial=overlay,
        )
        with server:
            plans[arm] = physical_plan(server, case)
            records[arm] = check_plan(case, arm, plans[arm])
            # One ordinary execution, held to the same expectation the benchmark holds every
            # execution to: the arm must have taken the path its plan says it does.
            server.begin_query()
            payload, _ = server.sql(statement)
            hits, partials = server.hits_since(), server.partial_hits_since()
            expected = bench.expected_hits(case["fuses"], arm)
            expected_partial = bench.expected_partial_hits(case["fuses"], arm)
            if (hits, partials) != (expected, expected_partial):
                raise SystemExit(
                    f"case {case['name']} arm {arm}: expected {expected} selection(s) of which "
                    f"{expected_partial} partial, the server logged {hits} ({partials} partial)"
                )
            schema, rows = bench.flatten(payload)
            results[arm] = {
                "stem": f"{args.case}-{arm}",
                "case": args.case,
                "arm": arm,
                "schema": schema,
                "rows": rows,
            }
        print(
            f"{case['name']} {arm:4} fused={records[arm]['fused']} "
            f"aggregates={','.join(records[arm]['modes']) or '-':22} "
            f"partial={records[arm]['partial']} selections={hits}/{partials}",
            flush=True,
        )

    # Every arm must return the ordinary plan's answer, value by value.
    for arm in arms[1:]:
        bench.compare_pair(results[arms[0]], results[arm])
    rows = len(results[arms[0]]["rows"])
    print(
        f"\n{case['name']}: {rows} rows, the {len(arms)} arms agree on schema, labels, "
        f"timestamps and every value",
        flush=True,
    )
    if args.partial_sum_step == "planner":
        print("note: arms differ by the planner's own decision, not by an environment variable")


if __name__ == "__main__":
    sys.exit(main())
