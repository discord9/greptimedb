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

`--timed N` adds N timed executions of the case's own statement (the `TQL EVAL` form, not the
ANALYZE one) between the warmups and the ANALYZE records, and prints the median wall and median
`/proc` CPU milliseconds beside them. It defaults to `0`, so the invocation above is unchanged.

`--compare-bin PATH` measures two binaries instead of one, `--rounds` rounds, alternating which
one goes first each round because this machine drifts 25-35% between sessions. Each round prints
both binaries' medians side by side and the ratio `compare-bin / bin`, and the summary over the
rounds is the median of those per-round ratios and their range. Every ratio divides two medians
from the same round; a ratio of a value from one round against a value from another round is the
mistake the report's section 4.8 corrects, so none is printed. The two binaries' query results are
also compared value by value through `fused_series_bench.compare_pair`, with `bin` as the
baseline.

`--self-test` checks the paired-ratio arithmetic with no server, for the same reason
`fused_profile_summarize.py` carries one.
"""

import argparse
import json
import statistics
import sys
import time

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

# The quantities a round reports, in the order every table prints them: the two whole-call
# figures first, then the evaluator and its three regions.
QUANTITIES = ("wall", "cpu", "evaluate", "prep", "kernel", "columns")

# Which ANALYZE counter each region quantity comes from.
REGION_QUANTITY = {
    "evaluate": "fused_evaluate_time",
    "prep": "fused_window_prep_time",
    "kernel": "fused_kernel_time",
    "columns": "fused_columns_time",
}

# The hit count needs the fused-selection line at debug; a full `debug` filter makes datafusion
# format the physical plan per planning pass, which the profile measured at 4.6%.
LOG_LEVEL = "info,query::datafusion::native_promql::fused=debug"


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


def medians(session):
    """`{quantity: median ms}` for one session, `None` where it did not measure that quantity.

    A session that ran no timed executions has no wall or CPU figure, and a binary predating a
    counter has no region; both come back as `None` so the caller prints `n/a` rather than a
    blank that reads like a measured zero.
    """
    measured = dict.fromkeys(QUANTITIES)
    if session["timed"]:
        measured["wall"] = statistics.median(s["wall_ms"] for s in session["timed"])
        measured["cpu"] = statistics.median(s["cpu_ms"] for s in session["timed"])
    if session["records"]:
        for quantity, key in REGION_QUANTITY.items():
            measured[quantity] = statistics.median(
                record[key] / 1e6 for record in session["records"]
            )
    return measured


def paired_ratios(baseline, other):
    """`{quantity: other / baseline}` from two medians of the **same** round.

    That is the whole point of the signature: it takes one round's `bin` medians and the same
    round's `compare-bin` medians and cannot see any other round, so it cannot divide a value
    from one round by a value from another. Any quantity either side did not measure maps to
    `None` but stays a key of the mapping, so the caller reports it as `n/a` instead of dropping
    the row: a missing measurement and a ratio of 1 must not look alike.
    """
    ratios = {}
    for quantity in QUANTITIES:
        left = baseline.get(quantity)
        right = other.get(quantity)
        ratios[quantity] = right / left if left and right is not None else None
    return ratios


def summarize_paired(per_round):
    """Per quantity: the median of the per-round ratios, their range, and how many there were.

    `rounds` and `missing` are reported beside the median because a summary computed from two of
    three rounds is not the same claim as one computed from three, and `n/a` on its own would
    not say which.
    """
    summary = {}
    for quantity in QUANTITIES:
        values = [r[quantity] for r in per_round if r[quantity] is not None]
        summary[quantity] = {
            "median": statistics.median(values) if values else None,
            "min": min(values) if values else None,
            "max": max(values) if values else None,
            "rounds": len(values),
            "missing": len(per_round) - len(values),
        }
    return summary


def cell_text(value):
    """A median cell, or `n/a` when the session did not measure that quantity."""
    return "n/a" if value is None else f"{value:.2f}"


def ratio_text(value):
    """A ratio cell, three decimals, or `n/a` when one of its two medians is missing."""
    return "n/a" if value is None else f"{value:.3f}"


def run_session(server, case, statement, repeats, timed, role=None, keep_result=False):
    """Warm the plan twice, time `timed` executions, then record `repeats` ANALYZE payloads.

    The timed executions use the case's own statement (`bench.tql`), not the ANALYZE form, and
    take wall and `/proc` CPU the way `fused_series_scale.run_arm` does: `cpu_seconds()` before
    and after, `perf_counter` around the call. The correctness query, when `keep_result` is set,
    is its own execution and never one of the timed ones, so no timing depends on the query that
    was compared.

    `role` only labels the per-record lines when two binaries run in one invocation; with
    `role=None` the printed text is exactly what a single-binary run has always printed.
    """
    tag = f"[{role}]" if role else ""
    correctness = None
    if keep_result:
        server.begin_query()
        payload, _ = server.sql(statement)
        schema, rows = bench.flatten(payload)
        if not rows:
            raise SystemExit(
                f"case {case['name']} ({role or 'bin'}) returned 0 rows in its correctness "
                "query; comparing nothing would pass"
            )
        # Shaped like `fused_series_scale.run_arm`'s result, because `bench.compare_pair` is the
        # comparator and names the case and the arm from these fields.
        correctness = {
            "case": case["name"],
            "arm": role or "bin",
            "stem": f"{case['name']}-{role or 'bin'}",
            "tql": statement,
            "schema": schema,
            "row_count": len(rows),
            "rows": rows,
        }

    for _ in range(2):
        server.sql(statement)

    samples = []
    for _ in range(timed):
        cpu_before = server.cpu_seconds()
        started = time.perf_counter()
        payload, size = server.sql(statement)
        wall_ms = (time.perf_counter() - started) * 1000.0
        cpu_ms = (server.cpu_seconds() - cpu_before) * 1000.0
        rows = sum(
            len(block["records"]["rows"])
            for block in payload.get("output", [])
            if block.get("records")
        )
        if not rows:
            # The same trap `fused_series_scale` refuses: a statement that selects nothing
            # returns a valid schema and a normal-looking time, so it times nothing.
            raise SystemExit(
                f"case {case['name']} ({role or 'bin'}) timed an execution that returned 0 rows"
            )
        samples.append({"wall_ms": wall_ms, "cpu_ms": cpu_ms, "rows": rows, "bytes": size})
    if samples:
        print(
            f"{tag}  timed {len(samples)}: "
            f"wall median {statistics.median(s['wall_ms'] for s in samples):.1f} ms, "
            f"CPU median {statistics.median(s['cpu_ms'] for s in samples):.1f} ms",
            flush=True,
        )

    records = []
    for index in range(repeats):
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
            f"{tag}  #{index + 1}  evaluate {ms['fused_evaluate_time']:7.2f}  "
            f"prep {ms['fused_window_prep_time']:7.2f}  "
            f"kernel {ms['fused_kernel_time']:7.2f}  "
            f"columns {ms['fused_columns_time']:6.2f}  "
            f"regions/evaluate {100.0 * total / ms['fused_evaluate_time']:5.1f}%  "
            f"prep/kernel {ms['fused_window_prep_time'] / ms['fused_kernel_time']:.3f}",
            flush=True,
        )
    return {"records": records, "timed": samples, "correctness": correctness}


def print_round(index, total, order, measured, ratios):
    """One round on one screen: both binaries' medians, and the ratio between them.

    The columns stay in a fixed role order whatever the execution order was, so two rounds are
    compared column for column and the ratio's direction cannot change under the rotation. The
    execution order is named in the line above the table.
    """
    print()
    print(f"round {index + 1} of {total}, execution order {' then '.join(order)}")
    print(f"  {'quantity':10} {'bin':>12} {'compare-bin':>12} {'cmp/bin':>8}")
    for quantity in QUANTITIES:
        print(
            f"  {quantity:10} {cell_text(measured['bin'][quantity]):>12} "
            f"{cell_text(measured['compare-bin'][quantity]):>12} "
            f"{ratio_text(ratios[quantity]):>8}"
        )
    print("  cmp/bin is compare-bin / bin: two medians from this round, and only this round")


def print_ratio_summary(per_round, summary):
    """The per-round ratios, then their median and range across rounds.

    Nothing here divides a number from one round by a number from another round: each `rN`
    column is that round's own `compare-bin / bin`, and `median` is the median of those ratios,
    not a ratio of two medians pooled over all rounds.
    """
    print()
    print("ratio compare-bin / bin: one per round, each from that round's two medians")
    header = " ".join(f"{'r' + str(index + 1):>7}" for index in range(len(per_round)))
    print(f"{'quantity':10} {header} {'median':>8} {'range':>13} {'rounds':>7}")
    for quantity in QUANTITIES:
        cells = " ".join(f"{ratio_text(entry[quantity]):>7}" for entry in per_round)
        row = summary[quantity]
        span = "n/a" if row["min"] is None else f"{row['min']:.3f}-{row['max']:.3f}"
        have = f"{row['rounds']}/{len(per_round)}"
        print(f"{quantity:10} {cells} {ratio_text(row['median']):>8} {span:>13} {have:>7}")


def run_compare(args, case, statement):
    """Both binaries over `--rounds` rounds, alternating which one runs first each round.

    A round is two sessions over the same data, one per binary; the ratio is `compare-bin / bin`
    and both terms are medians from that round, so no ratio ever divides a value from one round
    by a value from another. The rotation exists because this machine drifts 25-35% between
    sessions and a fixed order would put that drift into one side of every ratio.
    """
    paths = {"bin": args.bin, "compare-bin": args.compare_bin}
    results = {}
    rounds = []
    per_round = []
    for index in range(args.rounds):
        order = ["bin", "compare-bin"] if index % 2 == 0 else ["compare-bin", "bin"]
        sessions = {}
        for role in order:
            server = bench.Server(
                paths[role],
                args.data_home,
                args.http,
                args.grpc,
                args.mysql,
                args.postgres,
                True,
                args.log,
            )
            server.log_level = LOG_LEVEL
            with server:
                sessions[role] = run_session(
                    server,
                    case,
                    statement,
                    args.repeats,
                    args.timed,
                    role=role,
                    keep_result=True,
                )
            # The correctness query is deterministic, so it returns the same rows in every
            # round; only the last round's result is kept, which is the whole of the check.
            results[role] = sessions[role]["correctness"]
        measured = {role: medians(sessions[role]) for role in paths}
        ratios = paired_ratios(measured["bin"], measured["compare-bin"])
        per_round.append(ratios)
        rounds.append(
            {
                "round": index + 1,
                "order": list(order),
                "medians": measured,
                "ratios": ratios,
                "sessions": {
                    role: {
                        "records": sessions[role]["records"],
                        "timed": sessions[role]["timed"],
                    }
                    for role in order
                },
            }
        )
        print_round(index, args.rounds, order, measured, ratios)

    print()
    print("order per round: " + ", ".join("->".join(entry["order"]) for entry in rounds))
    print(
        f"medians per session: wall and CPU over {args.timed} timed execution(s), "
        f"regions over {args.repeats} ANALYZE record(s)"
    )
    summary = summarize_paired(per_round)
    print_ratio_summary(per_round, summary)

    # `compare_pair` stops with the first difference it finds, naming it; returning means the
    # two binaries agree on schema, labels, timestamps, row count and every value.
    labels, times, values = bench.compare_pair(results["bin"], results["compare-bin"])
    print()
    print(
        f"same rows and values on both binaries: case {args.case}, "
        f"{len(results['bin']['schema'])} columns "
        f"({len(labels)} label(s), {len(times)} timestamp(s), {len(values)} value(s)), "
        f"{results['bin']['row_count']} rows"
    )

    if args.json:
        with open(args.json, "w") as handle:
            json.dump(
                {
                    "case": args.case,
                    "bin": args.bin,
                    "compare_bin": args.compare_bin,
                    "rounds": rounds,
                    "paired_ratios": summary,
                },
                handle,
                indent=2,
            )
        print(f"wrote {args.json}")
    return 0


def self_test():
    """The paired-ratio arithmetic, with no server and no benchmark.

    The number this script exists to produce fails quietly in two ways: a ratio of one round's
    value against another round's value still looks like a measurement, and a quantity neither
    session measured disappears from the table as if it had been taken. Both are checked here
    against a fixture whose answer is known by hand, which is why
    `fused_profile_summarize.py` carries the same flag.
    """

    def session(ms):
        """One timed execution and one ANALYZE record per quantity, so each median is `ms`."""
        return {
            "timed": [{"wall_ms": ms["wall"], "cpu_ms": ms["cpu"]}],
            "records": [
                {
                    "fused_evaluate_time": ms["evaluate"] * 1e6,
                    "fused_window_prep_time": ms["prep"] * 1e6,
                    "fused_kernel_time": ms["kernel"] * 1e6,
                    "fused_columns_time": ms["columns"] * 1e6,
                }
            ],
        }

    failures = []

    # Round 1, chosen so every quantity has a different ratio: a mix-up between two of them
    # cannot pass, and the expected values are arithmetic done by hand.
    base = {
        "wall": 200.0,
        "cpu": 100.0,
        "evaluate": 400.0,
        "prep": 100.0,
        "kernel": 100.0,
        "columns": 50.0,
    }
    first = {
        "wall": 150.0,
        "cpu": 80.0,
        "evaluate": 300.0,
        "prep": 80.0,
        "kernel": 90.0,
        "columns": 25.0,
    }
    expected = {
        "wall": 0.75,
        "cpu": 0.8,
        "evaluate": 0.75,
        "prep": 0.8,
        "kernel": 0.9,
        "columns": 0.5,
    }
    round_one = paired_ratios(medians(session(base)), medians(session(first)))
    for quantity in QUANTITIES:
        got = round_one[quantity]
        if got is None or abs(got - expected[quantity]) > 1e-12:
            failures.append(f"{quantity} ratio is {got!r}, expected {expected[quantity]}")

    # Three rounds whose ratios are 0.5, 1.5 and 1.0 for every quantity: the median is 1.0 and
    # the range 0.5-1.5. A summary that pooled the two binaries' numbers into one figure, or
    # that paired a value from round 1 with a value from round 2, would not land there.
    per_round = []
    for factor in (0.5, 1.5, 1.0):
        other = {key: value * factor for key, value in base.items()}
        per_round.append(paired_ratios(medians(session(base)), medians(session(other))))
    summary = summarize_paired(per_round)
    for quantity in QUANTITIES:
        row = summary[quantity]
        if row["median"] is None or abs(row["median"] - 1.0) > 1e-12:
            failures.append(f"{quantity} median ratio is {row['median']!r}, expected 1.0")
        if row["min"] != 0.5 or row["max"] != 1.5:
            failures.append(
                f"{quantity} ratio range is {row['min']}-{row['max']}, expected 0.5-1.5"
            )
        if row["rounds"] != 3 or row["missing"] != 0:
            failures.append(f"{quantity} reports {row['rounds']} of 3 rounds, expected 3")

    # The rotation must not move the answer: the same rounds read in the other order summarize
    # to the same numbers, because the summary is order-insensitive by construction.
    reversed_summary = summarize_paired(list(reversed(per_round)))
    for quantity in QUANTITIES:
        if reversed_summary[quantity] != summary[quantity]:
            failures.append(
                f"{quantity} summary changed when the round order was reversed: "
                f"{reversed_summary[quantity]} against {summary[quantity]}"
            )

    # `--timed 0`: neither session has a wall or CPU figure. The ratio must be reported as
    # missing, not dropped and not read as a ratio of 1.
    untimed = {"timed": [], "records": []}
    nothing = paired_ratios(medians(untimed), medians(untimed))
    if set(nothing) != set(QUANTITIES):
        failures.append(f"a missing quantity was dropped from the mapping: {sorted(nothing)}")
    if nothing["cpu"] is not None:
        failures.append(f"an unmeasured CPU ratio came back as {nothing['cpu']!r}, not None")
    if ratio_text(nothing["wall"]) != "n/a":
        failures.append(f"an unmeasured wall ratio prints as {ratio_text(nothing['wall'])!r}")
    missing_summary = summarize_paired([nothing])
    if missing_summary["wall"]["rounds"] != 0 or missing_summary["wall"]["missing"] != 1:
        failures.append(
            f"an unmeasured wall is reported as {missing_summary['wall']}, expected 0 of 1"
        )
    if missing_summary["wall"]["median"] is not None:
        failures.append("an unmeasured wall median is a number, where `n/a` was required")

    # A zero baseline has no ratio; it must be reported rather than raise when divided.
    zero = paired_ratios({"wall": 0.0}, {"wall": 1.0})
    if zero["wall"] is not None:
        failures.append(f"a zero baseline gave the ratio {zero['wall']!r}, expected None")

    if failures:
        print("SELF-TEST FAILED", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        raise SystemExit(1)
    print(
        "self-test passed: round 1 ratios wall 0.750 cpu 0.800 evaluate 0.750 "
        "prep 0.800 kernel 0.900 columns 0.500"
    )
    print(
        "self-test passed: 3 rounds of 0.5/1.5/1.0 give median 1.000 and range 0.500-1.500, "
        "unchanged by a reversed round order"
    )
    print(
        "self-test passed: an unmeasured quantity prints as n/a and counts 0 of 1 rounds, and a "
        "zero baseline is reported rather than divided"
    )
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "bin",
        nargs="?",
        help="a greptime binary built after the three regions were added",
    )
    parser.add_argument("data_home", nargs="?", help="the seeded fixture directory")
    parser.add_argument(
        "case",
        nargs="?",
        help="a case name from fused_series_bench.CASES, e.g. W",
    )
    parser.add_argument("repeats", nargs="?", type=int, default=9, help="ANALYZE records")
    parser.add_argument("--http", type=int, default=15150)
    parser.add_argument("--grpc", type=int, default=15152)
    parser.add_argument("--mysql", type=int, default=15154)
    parser.add_argument("--postgres", type=int, default=15156)
    parser.add_argument("--log", default="/tmp/fused-regions-server.log")
    parser.add_argument("--json", help="also write the per-record values here")
    parser.add_argument(
        "--timed",
        type=int,
        default=0,
        metavar="N",
        help=(
            "also time N executions of the case's own statement before the ANALYZE records, "
            "and report the median wall and CPU milliseconds; 0 skips them"
        ),
    )
    parser.add_argument(
        "--compare-bin",
        metavar="PATH",
        help=(
            "a second binary to measure in the same rounds as `bin`; prints both binaries' "
            "medians and the per-round ratio compare-bin / bin"
        ),
    )
    parser.add_argument(
        "--rounds",
        type=int,
        default=3,
        help="rounds with --compare-bin; each round runs both binaries, alternating the order",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="check the paired-ratio arithmetic with no server and no benchmark",
    )
    args = parser.parse_args()

    if args.self_test:
        return self_test()
    absent = [name for name in ("bin", "data_home", "case") if getattr(args, name) is None]
    if absent:
        parser.error(f"{', '.join(absent)} required unless --self-test is given")
    if args.timed < 0:
        parser.error("--timed cannot be negative")
    if args.rounds < 1:
        parser.error("--rounds must be at least 1")

    case = next((c for c in bench.CASES if c["name"] == args.case), None)
    if case is None:
        raise SystemExit(f"no case {args.case}; known: {[c['name'] for c in bench.CASES]}")
    statement = bench.tql(case)
    if args.compare_bin:
        return run_compare(args, case, statement)

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
    server.log_level = LOG_LEVEL

    with server:
        session = run_session(server, case, statement, args.repeats, args.timed)
    records = session["records"]

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
        document = {"case": args.case, "records": records}
        if session["timed"]:
            document["timed"] = {
                "executions": session["timed"],
                "median_wall_ms": statistics.median(s["wall_ms"] for s in session["timed"]),
                "median_cpu_ms": statistics.median(s["cpu_ms"] for s in session["timed"]),
            }
        with open(args.json, "w") as handle:
            json.dump(document, handle, indent=2)
        print(f"wrote {args.json}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
