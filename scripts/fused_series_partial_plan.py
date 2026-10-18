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
names and types, row count, the set of label tuples, then each series' own timestamps and
values at a relative tolerance of `1e-9` with NaN and infinities handled apart.

    scripts/fused_series_partial_plan.py --data-home ~/.cache/fused-scale/data-seed \\
        --bin PATH --partial-sum-step env

`--partial-sum-step` names how the partial-SUM step is switched off in this build. It is
`env` while the feature is bench scaffolding read from `GREPTIME_FUSED_PARTIAL_SUM`, and
will be `planner` once the formal planner constructs the same configuration directly and
the arms differ by nothing but that decision -- at which point the two flag-on arms both
run the partial step and this script's `on`-vs-`sum` distinction stops existing.
"""

import argparse
import json
import re
import sys

import fused_series_bench as bench

# One `AggregateExec` line per aggregate node, e.g.
# `AggregateExec: mode=FinalPartitioned, gby=[...], aggr=[sum(...)]`.
AGGREGATE = re.compile(r"AggregateExec: mode=(\w+)")
FUSED = "FusedSeriesExec"
PARTIAL = "partial=sum"


def executed_operators(server, case):
    """`[(name, detail)]` of every operator the query actually ran, from `TQL ANALYZE`.

    `TQL EXPLAIN` is not usable here: in standalone mode it prints
    `CooperativeExec -> MergeScanExec` and leaves the remote plan unexpanded, so no aggregate and
    no fused node appear in it at all. `TQL ANALYZE` runs the query and reports one entry per
    operator that executed, which is also the only form that can prove a node ran rather than
    merely appearing in a plan.
    """
    statement = bench.tql(case).replace("TQL EVAL ", "TQL ANALYZE VERBOSE FORMAT JSON ", 1)
    payload, _ = server.sql(statement)
    # One row per stage: `[stage, node, plan]`, the plan a JSON string. Standalone reports stage 0
    # as the datanode's `MergeScanExec` and every stage after it as the frontend's own plan, so the
    # aggregate and the fused node appear in stage 1 onwards.
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
                # A stage whose plan is not JSON (or is null) contributes no operators; the
                # emptiness check below is what turns a fully unparseable answer into an error.
                continue
    found = []

    # `ANALYZE ... FORMAT JSON` nests the plan inside a `plan` column as a JSON *string*, one row
    # per stage, so it has to be parsed before it can be walked. The operator's own configuration
    # -- `mode=FinalPartitioned`, `partial=sum` -- lives in `param`, not in `name`, so both are
    # kept: matching on `name` alone would see every `AggregateExec` and no partial marker.
    def walk(node):
        if isinstance(node, dict):
            name = node.get("name") or node.get("operator")
            if isinstance(name, str) and node.get("metrics") is not None:
                found.append(f"{name}: {node.get('param', '')}")
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    for stage in stages:
        walk(stage)
    if not found:
        raise SystemExit(
            f"case {case['name']}: the TQL ANALYZE answer holds no operator with metrics; "
            "the plan was not executed, so nothing can be proven about which path ran"
        )
    return found


def check_plan(case, arm, operators):
    """Fails unless the executed operators are the ones `arm` is defined to run."""
    names = list(operators)
    joined = "\n".join(names)
    modes = AGGREGATE.findall(joined)
    fused = sum(1 for line in names if line.startswith(FUSED))
    partial = sum(1 for line in names if PARTIAL in line)
    expected_fused = 0 if arm == "off" else 1
    # `off` and `on` keep the partial and the final aggregate; `sum` replaced the partial one,
    # so only the final one may remain, and it must still be a final one.
    expected_modes = ["Partial", "FinalPartitioned"] if arm != "sum" else ["FinalPartitioned"]
    expected_partial = 1 if arm == "sum" else 0
    if fused != expected_fused:
        raise SystemExit(
            f"case {case['name']} arm {arm}: expected {expected_fused} {FUSED} among the "
            f"executed operators, found {fused}: {names}"
        )
    if sorted(modes) != sorted(expected_modes):
        raise SystemExit(
            f"case {case['name']} arm {arm}: expected aggregate modes {expected_modes}, "
            f"found {modes}: {names}"
        )
    if partial != expected_partial:
        raise SystemExit(
            f"case {case['name']} arm {arm}: expected {expected_partial} operator(s) displaying "
            f"`{PARTIAL}`, found {partial}: {names}"
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
            plans[arm] = executed_operators(server, case)
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
        for line in plans[arm]:
            if line.startswith(("FusedSeriesExec", "AggregateExec", "RepartitionExec")):
                print(f"          {line[:150]}", flush=True)

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
