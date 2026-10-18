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

"""Vary input series count and post-aggregate group count over the existing fixture.

Both axes move in the *query*, not in the data, so this needs no re-seeding and no second
data home:

* **input series count** -- a label matcher narrows the scan. `host=~"h00[0-9][0-9]"`
  selects 100 of the 400 hosts, `host="h0007"` selects one. The fixture stores its host
  values zero-padded to four digits, so a bare `h7` matches nothing and returns an empty
  result with a normal-looking time; `run_arm` refuses a zero-row case for that reason.
* **post-aggregate group count** -- the aggregation changes. `sum by (instance)` keeps 4
  groups out of 400 series (100:1 reduction), `sum by (host)` keeps 400 (1:1, no reduction
  at all), and `sum by (job)` collapses to 1.

The interesting quantity is the *ratio* of intermediate rows to output rows, because that is
exactly the redundancy a partial aggregate inside the fused node would remove. It is reported
per row of the table below.

Everything else matches `fused_series_bench.py`: same server startup, same `/v1/sql`, same
`--arm off` / `--arm on` crossed order, same CPU accounting from `/proc/<pid>/stat`, and the
same check that a flag-off arm logs no fused selection while a flag-on arm logs one. Each arm
runs one dedicated correctness query before its timed iterations, and the two arms' results are
compared row by row and value by value through `fused_series_bench.compare_pair`: a case whose
arms disagree on schema, labels, timestamps, row count or any value stops the run, because that
is a correctness bug and not a measurement. Comparing row counts alone, as this script used to,
cannot see it -- a wrong value in the right number of rows passes.

    scripts/fused_series_scale.py --data-home DIR --bin PATH --out records/x.json \\
        --rounds 3 --warmup 2 --iterations 9

Each case runs an off arm and an on arm per round, with the arm order rotated every round, and
the reported ratio is the median over rounds. **The gate is whether the rounds agree**, not
whether a proxy case looks quiet: a second, differently-shaped query cannot say whether this
query's ratio repeats. Agreement is measured two-sided -- the worst round's distance from the
median in *either* direction, with that round and side named per case -- because the one-sided
`max/median - 1` used earlier was blind to a round below the median, and `G4`'s round 3 read
16% low while that formula scored it 4%. `control` is kept as a diagnostic and is deliberately
not the gate -- `max_over_time` does not fuse, so both of its arms run the ordinary plan, whose
`PromRangeManipulateExec` materialises the entire range matrix (176 MB per query at a 1 h
window, from `TQL ANALYZE`'s `output_bytes`). A query churning that much measures jemalloc's
variance rather than the machine's, and it read a 60% spread while a 2.3 s case read 11%.

CPU is reported two ways because `/proc/<pid>/stat` counts 10 ms ticks: the median of the
per-iteration figures (robust to one bad iteration, but 10% quantised when a case costs 2-3
ticks) and the total over all timed iterations divided by their count (fine resolution,
sensitive to that bad iteration). Their disagreement is recorded per case as
`median_vs_aggregate_gap`; a large gap means quantization or one interrupted iteration, and it
is reported rather than hidden by quoting whichever of the two looks better.
"""

import argparse
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fused_series_bench as bench

# 120 minutes of data at a 1 s step, so the grid is the same as case G/W and the numbers are
# comparable with section 4.6/4.7 of the report.
SPAN_MINUTES = 120
STEP = "1s"

# `(label, inner expression, aggregation, fuses)`. `{table}` is substituted. Each is wrapped
# in the case's own aggregation, so the label states both axes.
CASES = [
    # group count at a fixed input size: 400 series -> 4 / 400 / 1 groups
    ("G4", "rate({table}[5m])", "sum by (instance)", True),
    ("G400", "rate({table}[5m])", "sum by (host)", True),
    ("G1", "rate({table}[5m])", "sum by (job)", True),
    # input series count at a fixed group count: 100 / 1 series -> 4 / 1 groups.
    # The fixture's host values are zero-padded to four digits (`h0000`..`h0399`), which is what
    # is actually stored; matching `h7` selects nothing and silently yields zero rows.
    ("n100", 'rate({table}{{host=~"h00[0-9][0-9]"}}[5m])', "sum by (instance)", True),
    ("n1", 'rate({table}{{host="h0007"}}[5m])', "sum by (instance)", True),
    # no aggregation at all: the fused node's full output, nothing removed above it
    ("raw", "rate({table}[5m])", None, True),
    # Unchanged work in both arms: the same rows flow through the same plan either way, so the
    # ratio for this case is the drift of the whole call, not a measurement of anything.
    ("control", "max_over_time({table}[5m])", "sum by (instance)", False),
]


def tql(inner, aggregation):
    start = bench.QUERY_START_S
    end = start + SPAN_MINUTES * 60
    table = bench.TABLE
    expr = inner.format(table=table)
    if aggregation:
        expr = f"{aggregation} ({expr})"
    return f"TQL EVAL ({start}, {end}, '{STEP}') {expr}"


def aggregate_cpu(timed):
    """Total CPU over the timed iterations divided by their count.

    `/proc/<pid>/stat` counts 10 ms ticks (`SC_CLK_TCK` is 100 here), so one iteration of a
    short case is 2-3 ticks and its ratio carries 10% quantization -- more than any effect this
    script looks for. Summing first and dividing once amortises that: nine iterations of 30 ms
    are 27 ticks, not nine 3-tick readings averaged.

    The median is kept beside it because the two fail differently: the median rejects one
    iteration hit by background work, the aggregate lets it in as one term. When they disagree
    by more than a few percent, that pass is not reportable and the `run_arm` caller sees it.
    """
    return sum(sample["cpu_ms"] for sample in timed) / len(timed) if timed else 0.0


def ratio_deviation(ratios):
    """How far a case's worst round sits from its median ratio, as a fraction of that median.

    Returns `(deviation, worst_round, side)`, with `worst_round` 1-based and `side` one of
    `above`, `below`, `at`.

    Two-sided on purpose. The one-sided `max(ratios) / median - 1` this replaces could only
    see a round *above* the median: rounds reading 0.57 / 0.68 / 0.71 scored 0.71 / 0.68 - 1 =
    4%, so the case passed a 10% gate although 0.57 sits 16% below the median. That was `G4`'s
    round 3, and it is a property of the formula, not of that run -- a maximum is blind to the
    side it does not look at. The worst round is named with its side because a bare magnitude
    is what let the low round hide.
    """
    median = statistics.median(ratios)
    if not median:
        return 0.0, 1, "at"
    distances = [abs(ratio - median) / median for ratio in ratios]
    deviation = max(distances)
    worst_round = distances.index(deviation) + 1
    delta = ratios[worst_round - 1] - median
    side = "above" if delta > 0 else "below" if delta < 0 else "at"
    return deviation, worst_round, side


def run_arm(server, label, arm, statement, warmup, iterations, expect_rows=True):
    """One correctness pass, then the warmups, then one timing per iteration.

    The correctness pass is its own execution, never one of the timed ones: comparing a timed
    query's result would make the run's numbers depend on the query it timed. Its result and
    hit count are kept, and the two arms' results are compared through `bench.compare_pair`, so
    a flag-on arm that returns the wrong numbers stops the run instead of being timed.
    """
    server.begin_query()
    payload, _ = server.sql(statement)
    hits = server.hits_since()
    if hits != server.expected_hits:
        raise SystemExit(
            f"case {label} arm {arm} correctness: expected {server.expected_hits} fused "
            f"selection(s), the server logged {hits}"
        )
    correctness_hits = hits
    schema, corrected_rows = bench.flatten(payload)
    if expect_rows and not corrected_rows:
        # A label matcher that matches nothing returns an empty result with a valid schema and
        # a normal-looking time. Reporting that as a case would be a measurement of nothing.
        raise SystemExit(
            f"case {label} returned 0 rows; the statement selects nothing: {statement}"
        )
    # Shaped like the result files `bench.compare` reads, because `bench.compare_pair` is the
    # comparator and names the case and arm from these fields.
    correctness = {
        "case": label,
        "arm": arm,
        "stem": f"{label}-{arm}",
        "tql": statement,
        "schema": schema,
        "row_count": len(corrected_rows),
        "rows": corrected_rows,
    }
    for _ in range(warmup):
        server.begin_query()
        server.sql(statement)
        hits = server.hits_since()
        if hits != server.expected_hits:
            raise SystemExit(
                f"case {label} warmup: expected {server.expected_hits} fused selection(s), "
                f"the server logged {hits}"
            )
    results = []
    for _ in range(iterations):
        server.begin_query()
        cpu_before = server.cpu_seconds()
        started = time.perf_counter()
        payload, size = server.sql(statement)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        cpu_ms = (server.cpu_seconds() - cpu_before) * 1000.0
        hits = server.hits_since()
        if hits != server.expected_hits:
            raise SystemExit(
                f"case {label} iteration: expected {server.expected_hits} fused selection(s), "
                f"the server logged {hits}"
            )
        rows = sum(
            len(block["records"]["rows"])
            for block in payload.get("output", [])
            if block.get("records")
        )
        if expect_rows and rows == 0:
            # A label matcher that matches nothing returns an empty result with a valid schema and
            # a normal-looking time. Reporting that as a case would be a measurement of nothing.
            raise SystemExit(
                f"case {label} returned 0 rows; the statement selects nothing: {statement}"
            )
        results.append(
            {
                "ms": elapsed_ms,
                "cpu_ms": cpu_ms,
                "rows": rows,
                "bytes": size,
                "hits": hits,
                "load1": os.getloadavg()[0],
            }
        )
    return {"result": correctness, "correctness_hits": correctness_hits, "timed": results}


def check_arms(label, statement, arms):
    """Fails unless both arms returned rows and the same result, row for row."""
    counts = {arm: arms[arm]["result"]["row_count"] for arm in ("off", "on")}
    for arm, count in counts.items():
        if count == 0:
            raise SystemExit(
                f"case {label} arm {arm} returned 0 rows; a case that measures nothing is "
                f"not a measurement ({statement})"
            )
    if counts["off"] != counts["on"]:
        raise SystemExit(
            f"case {label}: the arms disagree on row count: "
            f"{counts['off']} (off) vs {counts['on']} (on)"
        )
    # Order-preserving, field by field: schema, labels, timestamps, row count, values.
    # Flag-off is the baseline because both arms must return the ordinary plan's answer.
    # A row count that matches says nothing about the numbers inside the rows, which is what
    # this script used to skip: `rows["off"] != rows["on"]` and nothing else.
    bench.compare_pair(arms["off"]["result"], arms["on"]["result"])


def quietness_gate(cases, gate):
    """The verdict: the worst two-sided ratio deviation over every case but `control`.

    `control` is excluded because it is a diagnostic, not the gate: it runs the same ordinary
    plan on both arms, so its ratio measures the drift of the whole call -- 60% on a query that
    materialises a 176 MB range matrix per call -- and would fail every pass for reasons that
    are not this script's subject. Its recorded numbers stay in the output for the reader.
    """
    worst = {"case": None, "deviation": 0.0, "round": None, "side": None}
    for row in cases:
        if row["label"] == "control":
            continue
        if row["cpu_ratio_deviation_over_median"] > worst["deviation"]:
            worst = {
                "case": row["label"],
                "deviation": row["cpu_ratio_deviation_over_median"],
                "round": row["cpu_ratio_worst_round"],
                "side": row["cpu_ratio_worst_side"],
            }
    return {
        "max_ratio_deviation_over_median": worst["deviation"],
        "worst_case": worst["case"],
        "worst_round": worst["round"],
        "worst_side": worst["side"],
        "gate": gate,
        "quiet": worst["deviation"] <= gate,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-home", required=True)
    parser.add_argument("--bin", required=True)
    parser.add_argument("--commit", default="unknown")
    parser.add_argument("--out", required=True)
    parser.add_argument("--results", default=None)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=9)
    parser.add_argument("--http", type=int, default=15140)
    parser.add_argument("--grpc", type=int, default=15142)
    parser.add_argument("--mysql", type=int, default=15144)
    parser.add_argument("--postgres", type=int, default=15146)
    parser.add_argument("--log", default="/tmp/fused-scale-server.log")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument(
        "--max-ratio-spread",
        type=float,
        default=0.10,
        help=(
            "how far a case's worst round may sit from its median on/off ratio, in either "
            "direction, for the pass to count as quiet. Two-sided because the one-sided "
            "`max/median - 1` could not see a round below the median. This is a test of the "
            "reported quantity's own repeatability, which is what a proxy query was trying "
            "and failing to provide."
        ),
    )
    args = parser.parse_args()

    record = {"commit": args.commit, "bin": args.bin, "rounds": [], "cases": []}
    per_case_rounds = {label: [] for label, _, _, _ in CASES}
    for round_index in range(args.rounds):
        # Rotate which arm goes first, so a drift across the round cannot favour one of them.
        order = ("off", "on") if round_index % 2 == 0 else ("on", "off")
        for label, inner, aggregation, fuses in CASES:
            statement = tql(inner, aggregation)
            arms = {}
            for arm in order:
                server = bench.Server(
                    args.bin,
                    args.data_home,
                    args.http,
                    args.grpc,
                    args.mysql,
                    args.postgres,
                    arm == "on",
                    args.log,
                )
                # A flag-off server must log nothing fused and a flag-on server exactly one; a
                # case that does not fuse must log none on either arm.
                server.expected_hits = 1 if (fuses and arm == "on") else 0
                with server:
                    arms[arm] = run_arm(
                        server, label, arm, statement, args.warmup, args.iterations
                    )
            # Schema, labels, timestamps, row count and every value: the two arms must return
            # the same answer, and the flag-on answer is not allowed to be merely similar.
            check_arms(label, statement, arms)
            rows = {a: arms[a]["result"]["row_count"] for a in ("off", "on")}
            # The row counts are `rows` above; what the correctness pass adds is the proof that
            # each arm took the path it was supposed to, so only the hit counts are kept.
            correctness_hits = {a: arms[a]["correctness_hits"] for a in ("off", "on")}
            cpu_median = {
                a: statistics.median([r["cpu_ms"] for r in arms[a]["timed"]])
                for a in ("off", "on")
            }
            cpu_total = {a: aggregate_cpu(arms[a]["timed"]) for a in ("off", "on")}
            wall = {
                a: statistics.median([r["ms"] for r in arms[a]["timed"]]) for a in ("off", "on")
            }
            entry = {
                "round": round_index + 1,
                "order": list(order),
                "label": label,
                "statement": statement,
                "fuses": fuses,
                "rows": rows["on"],
                "correctness_hits": correctness_hits,
                "cpu_median_ms": cpu_median,
                "cpu_aggregate_ms": cpu_total,
                "wall_median_ms": wall,
                "cpu_on_over_off": cpu_median["on"] / cpu_median["off"],
                "cpu_aggregate_on_over_off": cpu_total["on"] / cpu_total["off"],
                "wall_on_over_off": wall["on"] / wall["off"],
                "off": arms["off"]["timed"],
                "on": arms["on"]["timed"],
            }
            per_case_rounds[label].append(entry)
            print(
                f"r{round_index + 1} {label:8} rows={entry['rows']:>8}  "
                f"CPU med off {cpu_median['off']:7.0f} on {cpu_median['on']:7.0f} "
                f"on/off {entry['cpu_on_over_off']:.3f}   "
                f"agg on/off {entry['cpu_aggregate_on_over_off']:.3f}   "
                f"wall on/off {entry['wall_on_over_off']:.3f}",
                flush=True,
            )
        record["rounds"].append({"round": round_index + 1, "order": list(order)})

    # One summary row per case. The reported ratio is the median over rounds, and the gate is
    # whether the rounds agree with each other: repeating the quantity being reported is a more
    # direct test of its repeatability than any proxy query.
    for label, inner, aggregation, fuses in CASES:
        entries = per_case_rounds[label]
        ratios = [e["cpu_on_over_off"] for e in entries]
        agg_ratios = [e["cpu_aggregate_on_over_off"] for e in entries]
        median_ratio = statistics.median(ratios)
        deviation, worst_round, worst_side = ratio_deviation(ratios)
        record["cases"].append(
            {
                "label": label,
                "statement": entries[0]["statement"],
                "fuses": fuses,
                "span_minutes": SPAN_MINUTES,
                "step": STEP,
                "rows": entries[0]["rows"],
                "cpu_median_ms": {
                    a: statistics.median([e["cpu_median_ms"][a] for e in entries])
                    for a in ("off", "on")
                },
                "cpu_aggregate_ms": {
                    a: statistics.median([e["cpu_aggregate_ms"][a] for e in entries])
                    for a in ("off", "on")
                },
                "wall_median_ms": {
                    a: statistics.median([e["wall_median_ms"][a] for e in entries])
                    for a in ("off", "on")
                },
                "cpu_on_over_off": median_ratio,
                "cpu_aggregate_on_over_off": statistics.median(agg_ratios),
                "median_vs_aggregate_gap": abs(median_ratio - statistics.median(agg_ratios)),
                "cpu_per_round": ratios,
                "cpu_aggregate_per_round": agg_ratios,
                "cpu_ratio_deviation_over_median": deviation,
                "cpu_ratio_worst_round": worst_round,
                "cpu_ratio_worst_side": worst_side,
                "wall_on_over_off": statistics.median([e["wall_on_over_off"] for e in entries]),
                "rounds": entries,
            }
        )

    print()
    print(
        f"{'case':8} {'rows':>9} {'CPUoff':>8} {'CPUon':>8} {'on/off':>7} "
        f"{'per-round':>22} {'dev':>5} {'worst round':>11} {'wall':>7}"
    )
    for row in record["cases"]:
        c = row["cpu_median_ms"]
        per_round = " ".join(f"{r:.2f}" for r in row["cpu_per_round"])
        worst = f"r{row['cpu_ratio_worst_round']} {row['cpu_ratio_worst_side']}"
        print(
            f"{row['label']:8} {row['rows']:>9} {c['off']:>8.0f} {c['on']:>8.0f} "
            f"{row['cpu_on_over_off']:>7.3f} {per_round:>22} "
            f"{100 * row['cpu_ratio_deviation_over_median']:>4.0f}% {worst:>11} "
            f"{row['wall_on_over_off']:>7.3f}",
            flush=True,
        )

    # The verdict, over every case but `control` (see `quietness_gate` for why the control is a
    # diagnostic and not the gate). The deviation is two-sided: the `max / median - 1` this used
    # to compare against the gate could only see a round above the median, which is how `G4`'s
    # round 3 passed a 10% gate while reading 16% low.
    verdict = quietness_gate(record["cases"], args.max_ratio_spread)
    record["pass"] = verdict
    print()
    print(
        f"pass: worst two-sided per-round ratio deviation "
        f"{100 * verdict['max_ratio_deviation_over_median']:.0f}% "
        f"(case {verdict['worst_case']}, round {verdict['worst_round']} "
        f"{verdict['worst_side']} the median), gate {100 * args.max_ratio_spread:.0f}%"
    )
    if verdict["quiet"]:
        print("  quiet: the rounds agree, so each case's median ratio is repeatable.")
    else:
        print(
            "  NOT QUIET: that case's ratio does not repeat across rounds, so quote it only"
            "\n  with the per-round values beside it. Re-run when the machine is idle."
        )

    with open(args.out, "w") as handle:
        json.dump(record, handle, indent=2)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
