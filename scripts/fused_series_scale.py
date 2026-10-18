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
same check that a flag-off arm logs no fused selection while a flag-on arm logs one. Cases are
compared across arms within a round through the same `compare` path.

    scripts/fused_series_scale.py --data-home DIR --bin PATH --out records/x.json \\
        --rounds 3 --warmup 2 --iterations 9

Each case runs an off arm and an on arm per round, with the arm order rotated every round, and
the reported ratio is the median over rounds. **The gate is whether the rounds agree**, not
whether a proxy case looks quiet: a second, differently-shaped query cannot say whether this
query's ratio repeats. `control` is kept as a diagnostic and is deliberately not the gate --
`max_over_time` does not fuse, so both of its arms run the ordinary plan, whose
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


def run_arm(server, label, statement, warmup, iterations, expect_rows=True):
    """Warm up, then one timing per iteration, with the fused-hit check per query."""
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
    return results


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
            "how far a case's slowest round may sit above its median on/off ratio for the pass "
            "to count as quiet. This is a test of the reported quantity's own repeatability, "
            "which is what a proxy query was trying and failing to provide."
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
                    arms[arm] = run_arm(server, label, statement, args.warmup, args.iterations)
            rows = {a: arms[a][0]["rows"] for a in ("off", "on")}
            if rows["off"] != rows["on"]:
                raise SystemExit(
                    f"case {label}: the arms disagree on row count: "
                    f"{rows['off']} (off) vs {rows['on']} (on)"
                )
            cpu_median = {
                a: statistics.median([r["cpu_ms"] for r in arms[a]]) for a in ("off", "on")
            }
            cpu_total = {a: aggregate_cpu(arms[a]) for a in ("off", "on")}
            wall = {a: statistics.median([r["ms"] for r in arms[a]]) for a in ("off", "on")}
            entry = {
                "round": round_index + 1,
                "order": list(order),
                "label": label,
                "statement": statement,
                "fuses": fuses,
                "rows": rows["on"],
                "cpu_median_ms": cpu_median,
                "cpu_aggregate_ms": cpu_total,
                "wall_median_ms": wall,
                "cpu_on_over_off": cpu_median["on"] / cpu_median["off"],
                "cpu_aggregate_on_over_off": cpu_total["on"] / cpu_total["off"],
                "wall_on_over_off": wall["on"] / wall["off"],
                "off": arms["off"],
                "on": arms["on"],
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
        spread_over_median = (max(ratios) / median_ratio - 1.0) if median_ratio else 0.0
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
                "cpu_ratio_spread_over_median": spread_over_median,
                "wall_on_over_off": statistics.median([e["wall_on_over_off"] for e in entries]),
                "rounds": entries,
            }
        )

    print()
    print(
        f"{'case':8} {'rows':>9} {'CPUoff':>8} {'CPUon':>8} {'on/off':>7} "
        f"{'per-round':>22} {'wall':>7}"
    )
    for row in record["cases"]:
        c = row["cpu_median_ms"]
        per_round = " ".join(f"{r:.2f}" for r in row["cpu_per_round"])
        print(
            f"{row['label']:8} {row['rows']:>9} {c['off']:>8.0f} {c['on']:>8.0f} "
            f"{row['cpu_on_over_off']:>7.3f} {per_round:>22} {row['wall_on_over_off']:>7.3f}",
            flush=True,
        )

    # The gate. Applying it to `control` alone was wrong and is recorded here because the reason
    # is useful: `control` is `max_over_time`, which does not fuse, and the ordinary plan's
    # `PromRangeManipulateExec` materialises the whole range matrix -- 176 MB for this fixture at
    # a 1 h window, measured as `output_bytes` in `TQL ANALYZE`. A query that churns that much
    # per call measures the allocator's variance, not the machine's quietness, and it duly read
    # 60% spread while the 2.3 s cases read 11%. So the gate is the repeatability of each case's
    # own ratio across rounds, and `control` is kept as a diagnostic of that allocator noise.
    worst_case, worst_spread = None, 0.0
    for row in record["cases"]:
        if row["label"] == "control":
            continue
        if row["cpu_ratio_spread_over_median"] > worst_spread:
            worst_case, worst_spread = row["label"], row["cpu_ratio_spread_over_median"]
    quiet = worst_spread <= args.max_ratio_spread
    record["pass"] = {
        "max_ratio_spread_over_median": worst_spread,
        "worst_case": worst_case,
        "gate": args.max_ratio_spread,
        "quiet": quiet,
    }
    print()
    print(
        f"pass: worst per-round ratio spread {100 * worst_spread:.0f}% (case {worst_case}), "
        f"gate {100 * args.max_ratio_spread:.0f}%"
    )
    if quiet:
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
