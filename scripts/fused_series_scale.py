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
crossed arm order, same CPU accounting from `/proc/<pid>/stat`, and the same check that a
flag-off arm logs no fused selection while a flag-on arm logs one -- and, for the `sum` arm,
that the selection it logs is the `partial sum` one. Each arm runs one dedicated correctness
query before its timed iterations, and every pair of arms is compared series by series and
sample by sample through `fused_series_bench.compare_pair`: a case whose arms disagree on
schema, row count, the set of series, or any timestamp or value stops the run, because that is a
correctness bug and not a measurement. Comparing row counts alone, as this script used to,
cannot see it -- a wrong value in the right number of rows passes. Cross-series row order is not
compared, because a case with no aggregation above the fused node has no `Sort` in its plan and
neither arm's order is guaranteed.

    scripts/fused_series_scale.py --data-home DIR --bin PATH --out records/x.json \\
        --rounds 3 --warmup 2 --iterations 9 --arms off,on,sum

## The three arms, from one binary

`--arms` (default `off,on`, which is what every earlier run used) picks which arms a run
measures; each is one `bench.Server` start with a different config flag or environment:

| arm | `experimental_native_promql` | `GREPTIME_FUSED_PARTIAL_SUM` |
| --- | --- | --- |
| `off` | `false` | (irrelevant) |
| `on` | `true` | `0` |
| `sum` | `true` | `1` |

`sum / on` isolates the partial SUM and nothing else: both are the same binary with the same
config, both fuse the range subtree, and the one environment variable the server reads is the
only difference between them, so the ratio is that step's cost rather than a second reading of
fusion. `on / off` is the fusion gain section 4.9 archives and has to stay comparable with it;
`sum / off` is the two effects together, and `sum / on` is what a round that adds the partial
SUM is about, which is why it is printed first.

Each case runs every arm per round, with the arm order rotated by round, and the reported ratio
is the median over rounds. **The gate is whether the rounds agree**, not whether a proxy case
looks quiet: a second, differently-shaped query cannot say whether this query's ratio repeats.
Agreement is measured two-sided -- the worst round's distance from the median in *either*
direction, with that round and side named per case -- because the one-sided
`max/median - 1` used earlier was blind to a round below the median, and `G4`'s round 3 read
16% low while that formula scored it 4%. The gate is applied to `on / off` **and** to
`sum / on`, and a case that fails it is reported with its per-round ratios, never as a median
alone. `control` is kept as a diagnostic and is deliberately not the gate -- `max_over_time`
does not fuse, so all of its arms run the ordinary plan, whose `PromRangeManipulateExec`
materialises the entire range matrix (176 MB per query at a 1 h window, from `TQL ANALYZE`'s
`output_bytes`). A query churning that much measures jemalloc's variance rather than the
machine's, and it read a 60% spread while a 2.3 s case read 11%.

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

# `(label, inner expression, aggregation, fuses, input series, SQL predicate)`. `{table}` is
# substituted in the expression. Each is wrapped in the case's own aggregation, so the label
# states both axes. The SQL predicate restates the expression's own label selector, and is what
# the pre-flight counts; it is written out rather than translated from PromQL, because a
# translator that is wrong in the same way as the selector would agree with itself.
#
# `input series` is the number of distinct `host` values the inner expression's selector must
# match, and the driver counts them before it measures anything. Without that check a selector
# that matches nothing -- a padding or spelling difference from what `fused_series_bench.py`
# actually wrote -- returns an empty result with a valid schema, which reads as an
# unmeasurably fast query rather than as a broken one.
CASES = [
    # group count at a fixed input size: 400 series -> 4 / 400 / 1 groups
    ("G4", "rate({table}[5m])", "sum by (instance)", True, 400, ""),
    ("G400", "rate({table}[5m])", "sum by (host)", True, 400, ""),
    ("G1", "rate({table}[5m])", "sum by (job)", True, 400, ""),
    # input series count: 100 / 1 series. The fixture stores `h0000`..`h0399` with
    # `instance = i{host % 4}`, so the first hundred hosts are all `i0` and `n100` collapses
    # 100:1 -- the same ratio as `G4` -- over a quarter of the input.
    (
        "n100",
        'rate({table}{{host=~"h00[0-9][0-9]"}}[5m])',
        "sum by (instance)",
        True,
        100,
        " WHERE host ~ '^h00[0-9][0-9]$'",
    ),
    ("n1", 'rate({table}{{host="h0007"}}[5m])', "sum by (instance)", True, 1,
     " WHERE host = 'h0007'"),
    # no aggregation at all: the fused node's full output, nothing removed above it
    ("raw", "rate({table}[5m])", None, True, 400, ""),
    # Unchanged work in both arms: the same rows flow through the same plan either way, so the
    # ratio for this case is the drift of the whole call, not a measurement of anything.
    ("control", "max_over_time({table}[5m])", "sum by (instance)", False, 400, ""),
]

# The ratios reported per case, most important first: `sum / on` is what a round that adds the
# partial SUM is about, `on / off` is the fusion gain the archived section 4.9 quotes and must
# stay comparable with it, and `sum / off` is the two together. Each is
# `numerator / denominator` of the per-arm CPU medians, per round.
RATIOS = [
    ("sum_over_on", "sum", "on"),
    ("on_over_off", "on", "off"),
    ("sum_over_off", "sum", "off"),
]

RATIOS_BY_NAME = {name: (numerator, denominator) for name, numerator, denominator in RATIOS}

# How the ratios are named in the output, and the two the round-to-round gate is applied to.
# `sum_over_off` is a product of the other two, so gating it would report the same excursion
# twice under a third name.
RATIO_LABELS = {"sum_over_on": "sum/on", "on_over_off": "on/off", "sum_over_off": "sum/off"}
GATED_RATIOS = ("sum_over_on", "on_over_off")


def parse_arms(spec):
    """The arms `--arms` asks for, in the caller's order and checked against `bench.ARMS`.

    The order matters: it is the order the arms are started in the first round, and the order
    the ratios are listed in.
    """
    arms = []
    for name in spec.split(","):
        name = name.strip()
        if not name:
            continue
        if name not in bench.ARMS:
            raise SystemExit(
                f"unknown arm {name!r}; the arms are {', '.join(bench.ARMS)} "
                "(off = flag off, on = flag on, sum = flag on with the partial SUM)"
            )
        if name not in arms:
            arms.append(name)
    if len(arms) < 2:
        raise SystemExit(
            f"--arms needs at least two arms to compare; got {', '.join(arms) or 'none'}"
        )
    return arms


def arm_order(arms, round_index):
    """The arm order for one round: a cyclic rotation, so no arm is always first.

    With two arms this is the alternating `off,on` / `on,off` the earlier runs used; with three
    it is `off,on,sum` / `on,sum,off` / `sum,off,on`, so each arm starts, sits in the middle and
    goes last once every three rounds.
    """
    offset = round_index % len(arms)
    return arms[offset:] + arms[:offset]


def round_ratios(per_arm, name, label):
    """One per-round ratio, refused rather than divided by a zero CPU reading.

    `/proc/<pid>/stat` counts 10 ms ticks, so a sub-tick case reads 0 ms and a ratio over it is
    not a ratio this script can report.
    """
    numerator, denominator = RATIOS_BY_NAME[name]
    left, right = per_arm[numerator], per_arm[denominator]
    if not right:
        raise SystemExit(
            f"case {label}: {RATIO_LABELS[name]} would divide by 0 ms -- the {denominator} arm "
            "read as 0, which is below what 10 ms ticks can resolve"
        )
    return left / right


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


def check_expected(server, label, arm, phase, hits, partial_hits):
    """Fails unless one execution logged the arm's expected fused and partial-sum counts."""
    if hits != server.expected_hits or partial_hits != server.expected_partial_hits:
        raise SystemExit(
            f"case {label} arm {arm} {phase}: expected {server.expected_hits} fused "
            f"selection(s), {server.expected_partial_hits} of them partial sums; the server "
            f"logged {hits} ({partial_hits} partial sums)"
        )
    return hits


def run_arm(server, label, arm, statement, warmup, iterations, expect_rows=True):
    """One correctness pass, then the warmups, then one timing per iteration.

    The correctness pass is its own execution, never one of the timed ones: comparing a timed
    query's result would make the run's numbers depend on the query it timed. Its result and
    hit counts are kept, and every pair of arms' results is compared through
    `bench.compare_pair`, so a flag-on arm that returns the wrong numbers stops the run instead
    of being timed.

    Both hit counts are checked on every execution: `server.expected_hits` for the fused
    selections of either kind and `server.expected_partial_hits` for the `partial sum` ones. A
    `sum` arm that logged a plain selection would otherwise be the `on` arm timed twice.
    """
    server.begin_query()
    payload, _ = server.sql(statement)
    hits, partial_hits = server.hits_since(), server.partial_hits_since()
    check_expected(server, label, arm, "correctness", hits, partial_hits)
    correctness_hits = hits
    correctness_partial_hits = partial_hits
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
        check_expected(
            server,
            label,
            arm,
            "warmup",
            server.hits_since(),
            server.partial_hits_since(),
        )
    results = []
    for _ in range(iterations):
        server.begin_query()
        cpu_before = server.cpu_seconds()
        started = time.perf_counter()
        payload, size = server.sql(statement)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        cpu_ms = (server.cpu_seconds() - cpu_before) * 1000.0
        hits, partial_hits = server.hits_since(), server.partial_hits_since()
        check_expected(server, label, arm, "iteration", hits, partial_hits)
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
                "partial_hits": partial_hits,
                "load1": os.getloadavg()[0],
            }
        )
    return {
        "result": correctness,
        "correctness_hits": correctness_hits,
        "correctness_partial_hits": correctness_partial_hits,
        "timed": results,
        # `VmHWM` of the server process, which the kernel maintains as a high-water mark. A peak,
        # not an allocation total, and of the whole process rather than this query: it is only
        # comparable between arms measured the same way. `None` when the process is gone.
        "peak_rss_bytes": server.peak_memory_bytes(),
    }


def check_arms(label, statement, arms):
    """Fails unless every arm returned rows and the same result, row for row.

    Every pair, not only `off` against `on`: with three arms the two flag-on ones are a pair of
    their own, and `sum` disagreeing with `on` is exactly the bug a partial aggregate can
    introduce. The baseline of each pair is the earlier arm in `bench.ARMS`' order, so with
    `off` present it is always the ordinary plan's answer, whatever order the round ran them in.
    """
    canonical = [arm for arm in bench.ARMS if arm in arms]
    counts = {arm: arms[arm]["result"]["row_count"] for arm in canonical}
    for arm, count in counts.items():
        if count == 0:
            raise SystemExit(
                f"case {label} arm {arm} returned 0 rows; a case that measures nothing is "
                f"not a measurement ({statement})"
            )
    for base in canonical[1:]:
        if counts[base] != counts[canonical[0]]:
            raise SystemExit(
                f"case {label}: the arms disagree on row count: "
                + ", ".join(f"{counts[arm]} ({arm})" for arm in canonical)
            )
    # Sample by sample, series by series: schema, row count, the set of series, then every
    # timestamp and value of each. Flag-off is the baseline because every arm must return the
    # ordinary plan's answer. A row count that matches says nothing about the numbers inside the
    # rows, which is what this script used to skip: `rows["off"] != rows["on"]` and nothing else.
    for index, base in enumerate(canonical):
        for other in canonical[index + 1:]:
            bench.compare_pair(arms[base]["result"], arms[other]["result"])


def median_vs_aggregate(median_ratio, aggregate_ratio):
    """How far the two statistics are from each other, relative to the per-iteration median."""
    if not median_ratio:
        return 0.0
    return abs(aggregate_ratio - median_ratio) / median_ratio


def statistics_disagree(median_ratio, aggregate_ratio):
    """Whether the two statistics point at different answers, one below 1.00 and one above.

    Both are ratios of the same two arms over the same rounds, so one below 1.00 and one above
    means the pass supports neither a saving nor a regression from that pair alone -- which is
    exactly the case a single quoted number used to hide.
    """
    return (median_ratio - 1.0) * (aggregate_ratio - 1.0) < 0


def worst_gated_ratio(row, gated):
    """The gated ratio of `row` that sits furthest from its own median, with how far.

    The gate covers `on / off` and `sum / on`; a case is only quiet if both repeat, so this is
    the one figure per case that the pass/fail decision and the summary table both read.

    A row with no gated ratio is an error, not a quiet pass: `main` refuses an `--arms`
    combination that reports no gated ratio, so reaching this with nothing to check means the
    ratio names and the rows have gone out of step.
    """
    names = [name for name in gated if name in row["ratios"]]
    if not names:
        raise SystemExit(
            f"case {row['label']} reports no gated ratio "
            f"({', '.join(row['ratios'])}), so its quietness cannot be decided"
        )
    worst = None
    for name in names:
        summary = row["ratios"][name]
        # A ratio whose two statistics straddle 1.00 is not quiet however stable each one is:
        # the pass cannot say whether the arm is faster or slower. Reported as an infinite
        # deviation so it outranks any finite one, with the round it would have named.
        if statistics_disagree(summary["median"], summary["aggregate_median"]):
            deviation = float("inf")
        else:
            deviation = summary["deviation_over_median"]
        if worst is None or deviation > worst["deviation"]:
            worst = {
                "ratio": name,
                "deviation": deviation,
                "round": summary["worst_round"],
                "side": summary["worst_side"],
                "median": summary["median"],
                "aggregate_median": summary["aggregate_median"],
            }
    return worst


def quietness_gate(cases, gate):
    """The verdict: the worst two-sided ratio deviation over every case but `control`.

    `control` is excluded because it is a diagnostic, not the gate: it runs the same ordinary
    plan on every arm, so its ratio measures the drift of the whole call -- 60% on a query that
    materialises a 176 MB range matrix per call -- and would fail every pass for reasons that
    are not this script's subject. Its recorded numbers stay in the output for the reader.

    Both gated ratios are checked, and the worst of them decides: a `sum / on` that repeats
    while `on / off` does not is not a quiet pass either, because the two are quoted together.
    """
    worst = {"case": None, "ratio": None, "deviation": 0.0, "round": None, "side": None}
    for row in cases:
        if row["label"] == "control":
            continue
        candidate = worst_gated_ratio(row, GATED_RATIOS)
        # The first case is taken whatever it reads, so an all-zero pass still names the case and
        # ratio it read, instead of reporting `None` for a deviation of 0.
        if worst["case"] is None or candidate["deviation"] > worst["deviation"]:
            worst = {"case": row["label"], **candidate}
    return {
        "max_ratio_deviation_over_median": worst["deviation"],
        "worst_case": worst["case"],
        "worst_ratio": worst["ratio"],
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
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=9)
    parser.add_argument("--http", type=int, default=15140)
    parser.add_argument("--grpc", type=int, default=15142)
    parser.add_argument("--mysql", type=int, default=15144)
    parser.add_argument("--postgres", type=int, default=15146)
    parser.add_argument("--log", default="/tmp/fused-scale-server.log")
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument(
        "--arms",
        default="off,on",
        help=(
            "comma-separated arms to measure, in the order the first round starts them: `off` is "
            "the ordinary plan, `on` the fused one, `sum` the fused one with the partial SUM "
            "computed inside the fused node. The default is the two arms every earlier run used, "
            "so the record keeps the shape an archived comparison reads -- provided the binary "
            "that produced the archive defaulted `GREPTIME_FUSED_PARTIAL_SUM` to off, which this "
            "script cannot check and now states explicitly on the `on` arm instead of relying on "
            "it. A round that measures the partial SUM uses off,on,sum; a pair without a gated "
            "ratio (only off,sum) is refused"
        ),
    )
    parser.add_argument(
        "--max-ratio-spread",
        type=float,
        default=0.10,
        help=(
            "how far a case's worst round may sit from its median ratio, in either direction, "
            "for the pass to count as quiet. Applied to `on/off` and to `sum/on`, the two "
            "reported ratios that are not a product of the others. Two-sided because the "
            "one-sided `max/median - 1` could not see a round below the median. This is a test "
            "of the reported quantity's own repeatability, which is what a proxy query was "
            "trying and failing to provide."
        ),
    )
    args = parser.parse_args()

    arms = parse_arms(args.arms)
    # A ratio needs both of its arms in the run, so the reported set is a property of `--arms`:
    # `off,on` reports `on/off`, `off,on,sum` reports all three.
    ratio_names = [
        name
        for name, numerator, denominator in RATIOS
        if numerator in arms and denominator in arms
    ]
    gated = [name for name in GATED_RATIOS if name in ratio_names]
    # `off,sum` is the one legal pair that leaves no gated ratio: it reports only `sum/off`, the
    # product of the other two, which double-reports their excursions and so is deliberately not
    # gated. Falling back to gating it would let a run in which both arms took the same plan --
    # the environment variable unread, say -- read `sum/off` 1.00 with zero deviation and be
    # reported quiet. A run that can measure nothing is refused instead.
    if not gated:
        raise SystemExit(
            f"--arms {','.join(arms)} reports only "
            f"{', '.join(RATIO_LABELS[name] for name in ratio_names) or 'no ratio'}, and none "
            f"of those is gated ({', '.join(RATIO_LABELS[name] for name in GATED_RATIOS)}). "
            "A gate needs at least one of them: measure a run that includes both arms of one "
            "gated ratio, e.g. off,on (gates on/off) or on,sum (gates sum/on) or off,on,sum"
        )
    # `worst_gated_ratio` applies the same rule to the same set, so the table, the verdict and
    # the per-round report of a failing case can never disagree about what was gated.
    # The arm every case's row count is quoted from; every arm must agree on it anyway.
    baseline = [arm for arm in bench.ARMS if arm in arms][0]

    record = {
        "commit": args.commit,
        "bin": args.bin,
        "arms": list(arms),
        "ratios": ratio_names,
        "rounds": [],
        "cases": [],
    }
    per_case_rounds = {label: [] for label, _, _, _, _, _ in CASES}

    # The fixture's shape, checked once before anything is timed. A selector that matches a
    # different number of series than the case is named for measures a different query, and one
    # that matches none returns a valid empty result the timing loop would otherwise report.
    with bench.Server(
        args.bin,
        args.data_home,
        args.http,
        args.grpc,
        args.mysql,
        args.postgres,
        False,
        args.log,
    ) as server:
        total = server.rows(f'SELECT count(*) FROM "{bench.TABLE}"')[0][0]
        if total != bench.ROW_COUNT:
            raise SystemExit(
                f"the fixture holds {total} rows, not the {bench.ROW_COUNT} the selectors were "
                "written for; seed a fresh directory with fused_series_bench.py seed"
            )
        for label, _, _, _, input_series, predicate in CASES:
            found = server.rows(
                f'SELECT count(DISTINCT "host") FROM "{bench.TABLE}"{predicate}'
            )[0][0]
            if found != input_series:
                raise SystemExit(
                    f"case {label} expects {input_series} input series but its predicate "
                    f"{predicate or '(none)'} matches {found} hosts in the fixture"
                )
            print(f"fixture: case {label:7} selects {found} series", flush=True)

    for round_index in range(args.rounds):
        # Rotate which arm goes first, so a drift across the round cannot favour one of them and
        # no arm is always the one that follows another arm's server shutdown.
        order = arm_order(arms, round_index)
        for label, inner, aggregation, fuses, input_series, predicate in CASES:
            statement = tql(inner, aggregation)
            measured = {}
            for arm in order:
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
                # A flag-off server must log nothing fused and a flag-on server exactly one; the
                # `sum` arm must additionally log that one as a partial sum, or it is the `on`
                # arm timed twice. A case that does not fuse must log none on any arm.
                server.expected_hits = bench.expected_hits(fuses, arm)
                # A case with no aggregation fuses but pushes no partial SUM: `raw` measures the
                # fused node's whole output. Deriving this from `fuses` alone would demand a
                # partial-sum line that no aggregate exists to produce.
                server.expected_partial_hits = bench.expected_partial_hits(
                    fuses and aggregation is not None, arm
                )
                with server:
                    measured[arm] = run_arm(
                        server, label, arm, statement, args.warmup, args.iterations
                    )
            # Schema, labels, timestamps, row count and every value: every pair of arms must
            # return the same answer, and the flag-on answers are not allowed to be merely
            # similar.
            check_arms(label, statement, measured)
            rows = {a: measured[a]["result"]["row_count"] for a in arms}
            # The row counts are `rows` above; what the correctness pass adds is the proof that
            # each arm took the path it was supposed to, so only the hit counts are kept.
            cpu_median = {
                a: statistics.median([r["cpu_ms"] for r in measured[a]["timed"]]) for a in arms
            }
            cpu_total = {a: aggregate_cpu(measured[a]["timed"]) for a in arms}
            wall = {
                a: statistics.median([r["ms"] for r in measured[a]["timed"]]) for a in arms
            }
            entry = {
                "round": round_index + 1,
                "order": list(order),
                "label": label,
                "statement": statement,
                "fuses": fuses,
                "pushes_partial": fuses and aggregation is not None,
                "rows": rows[baseline],
                "correctness_hits": {a: measured[a]["correctness_hits"] for a in arms},
                "correctness_partial_hits": {
                    a: measured[a]["correctness_partial_hits"] for a in arms
                },
                "cpu_median_ms": cpu_median,
                "cpu_aggregate_ms": cpu_total,
                "wall_median_ms": wall,
                "peak_rss_bytes": {a: measured[a]["peak_rss_bytes"] for a in arms},
            }
            # Per-round ratios, under keys of the shape `cpu_on_over_off` that the archived
            # records already use, one family per reported ratio.
            for name in ratio_names:
                entry["cpu_" + name] = round_ratios(cpu_median, name, label)
                entry["cpu_aggregate_" + name] = round_ratios(cpu_total, name, label)
                entry["wall_" + name] = round_ratios(wall, name, label)
            for arm in [a for a in bench.ARMS if a in arms]:
                entry[arm] = measured[arm]["timed"]
            per_case_rounds[label].append(entry)
            print(
                f"case {label} round {round_index + 1} order={','.join(order)}  "
                + "  ".join(
                    f"{RATIO_LABELS[name]} {entry['cpu_' + name]:.2f}" for name in ratio_names
                ),
                flush=True,
            )
            cpu_text = " ".join(f"{a} {value:7.0f}" for a, value in cpu_median.items())
            detail = f"  r{round_index + 1} {label:8} rows={entry['rows']:>8}  CPU med {cpu_text}"
            if "on_over_off" in ratio_names:
                detail += (
                    f"  on/off {entry['cpu_on_over_off']:.3f}   "
                    f"agg on/off {entry['cpu_aggregate_on_over_off']:.3f}   "
                    f"wall on/off {entry['wall_on_over_off']:.3f}"
                )
            print(detail, flush=True)
        record["rounds"].append({"round": round_index + 1, "order": list(order)})

    # One summary row per case. The reported ratio is the median over rounds, and the gate is
    # whether the rounds agree with each other: repeating the quantity being reported is a more
    # direct test of its repeatability than any proxy query.
    for label, inner, aggregation, fuses, input_series, predicate in CASES:
        entries = per_case_rounds[label]
        ratio_rows = {}
        for name in ratio_names:
            per_round = [e["cpu_" + name] for e in entries]
            aggregate_per_round = [e["cpu_aggregate_" + name] for e in entries]
            wall_per_round = [e["wall_" + name] for e in entries]
            deviation, worst_round, worst_side = ratio_deviation(per_round)
            aggregate_deviation, _, _ = ratio_deviation(aggregate_per_round)
            ratio_rows[name] = {
                "label": RATIO_LABELS[name],
                "per_round": per_round,
                "median": statistics.median(per_round),
                "aggregate_per_round": aggregate_per_round,
                "aggregate_median": statistics.median(aggregate_per_round),
                "wall_per_round": wall_per_round,
                "wall_median": statistics.median(wall_per_round),
                "deviation_over_median": deviation,
                "worst_round": worst_round,
                "worst_side": worst_side,
                # The second statistic and its own repeatability. Both are reported for every
                # gated ratio because they can disagree, and when they do neither is the answer:
                # a per-iteration median resolves one iteration, a per-round total amortises the
                # 10 ms tick over nine of them. `statistics_disagree` is what makes a reader look
                # before quoting one of them.
                "aggregate_deviation_over_median": aggregate_deviation,
                "median_vs_aggregate": median_vs_aggregate(
                    statistics.median(per_round), statistics.median(aggregate_per_round)
                ),
                "gated": name in gated,
            }
        row = {
            "label": label,
            "statement": entries[0]["statement"],
            "fuses": fuses,
            "pushes_partial": fuses and aggregation is not None,
            "span_minutes": SPAN_MINUTES,
            "step": STEP,
            "rows": entries[0]["rows"],
            "arms": list(arms),
            "correctness_hits": {a: entries[0]["correctness_hits"][a] for a in arms},
            "correctness_partial_hits": {
                a: entries[0]["correctness_partial_hits"][a] for a in arms
            },
            "cpu_median_ms": {
                a: statistics.median([e["cpu_median_ms"][a] for e in entries]) for a in arms
            },
            "cpu_aggregate_ms": {
                a: statistics.median([e["cpu_aggregate_ms"][a] for e in entries]) for a in arms
            },
            "wall_median_ms": {
                a: statistics.median([e["wall_median_ms"][a] for e in entries]) for a in arms
            },
            # Per arm: the largest peak any round saw, since a peak that happened is what the
            # query needed, and rounds with a smaller peak cannot lower it.
            "peak_rss_bytes": {
                a: max(
                    (e["peak_rss_bytes"][a] for e in entries if e["peak_rss_bytes"][a]),
                    default=None,
                )
                for a in arms
            },
        }
        if "on_over_off" in ratio_rows:
            # The flat on/off figures section 4.9 was read from, under the names it read them by:
            # same numbers, same meaning, so an archived table can still be re-derived from a
            # record written by this version. `ratios` below holds the same numbers by ratio name
            # and is what the gate and the new columns read.
            on_over_off = ratio_rows["on_over_off"]
            row["cpu_on_over_off"] = on_over_off["median"]
            row["cpu_aggregate_on_over_off"] = on_over_off["aggregate_median"]
            row["median_vs_aggregate_gap"] = abs(
                on_over_off["median"] - on_over_off["aggregate_median"]
            )
            row["cpu_per_round"] = on_over_off["per_round"]
            row["cpu_aggregate_per_round"] = on_over_off["aggregate_per_round"]
            row["cpu_ratio_deviation_over_median"] = on_over_off["deviation_over_median"]
            row["cpu_ratio_worst_round"] = on_over_off["worst_round"]
            row["cpu_ratio_worst_side"] = on_over_off["worst_side"]
            row["wall_on_over_off"] = on_over_off["wall_median"]
        row["ratios"] = ratio_rows
        row["rounds"] = entries
        record["cases"].append(row)

    # The summary table. `sum/on` is reported first because it is what a round that adds the
    # partial SUM is about; `on/off` keeps the column the archived run reports.
    worst_width = 22 if len(gated) > 1 else 11
    header = [f"{'case':8}", f"{'rows':>9}"]
    header += [f"{('CPU' + arm):>8}" for arm in arms]
    if "sum_over_on" in ratio_names:
        header += [f"{'sum/on':>7}", f"{'s/o rounds':>22}"]
    if "on_over_off" in ratio_names:
        header += [f"{'on/off':>7}", f"{'per-round':>22}"]
    header += [f"{'dev':>5}", f"{'worst round':>{worst_width}}"]
    if "sum_over_off" in ratio_names:
        header += [f"{'sum/off':>8}"]
    header += [f"{'C/B agg':>8}", f"{'conflict':>9}"]
    if "on_over_off" in ratio_names:
        header += [f"{'wall':>7}"]
    print()
    print(" ".join(header))
    for row in record["cases"]:
        c = row["cpu_median_ms"]
        cells = [f"{row['label']:8}", f"{row['rows']:>9}"]
        cells += [f"{c[arm]:>8.0f}" for arm in arms]
        for name in ("sum_over_on", "on_over_off"):
            if name not in row["ratios"]:
                continue
            summary = row["ratios"][name]
            per_round = " ".join(f"{r:.2f}" for r in summary["per_round"])
            cells += [f"{summary['median']:>7.3f}", f"{per_round:>22}"]
        worst_ratio = worst_gated_ratio(row, gated)
        worst = f"r{worst_ratio['round']} {worst_ratio['side']}"
        if len(gated) > 1:
            worst += f" {RATIO_LABELS[worst_ratio['ratio']]}"
        cells += [f"{100 * worst_ratio['deviation']:>4.0f}%", f"{worst:>{worst_width}}"]
        if "sum_over_off" in row["ratios"]:
            cells += [f"{row['ratios']['sum_over_off']['median']:>8.3f}"]
        # The same ratio from the other statistic, and whether the two disagree about which arm
        # is faster. A case reading `0.900` by median and `1.034` by round total is neither a
        # saving nor a regression, and the `conflict` column is what says so.
        graded = row["ratios"].get("sum_over_on") or row["ratios"].get("on_over_off")
        cells += [f"{graded['aggregate_median']:>8.3f}"]
        cells += [
            "  YES" if statistics_disagree(graded["median"], graded["aggregate_median"]) else "   no"
        ]
        if "on_over_off" in row["ratios"]:
            cells += [f"{row['ratios']['on_over_off']['wall_median']:>7.3f}"]
        print(" ".join(cells), flush=True)

    # The verdict, over every case but `control` (see `quietness_gate` for why the control is a
    # diagnostic and not the gate). The deviation is two-sided: the `max / median - 1` this used
    # to compare against the gate could only see a round above the median, which is how `G4`'s
    # round 3 passed a 10% gate while reading 16% low. Both gated ratios count, and the worst of
    # them decides.
    verdict = quietness_gate(record["cases"], args.max_ratio_spread)
    record["pass"] = verdict
    print()
    print(
        f"pass: worst two-sided per-round ratio deviation "
        f"{100 * verdict['max_ratio_deviation_over_median']:.0f}% "
        f"(case {verdict['worst_case']}, "
        f"{RATIO_LABELS.get(verdict['worst_ratio'], '-')}, round {verdict['worst_round']} "
        f"{verdict['worst_side']} the median), gate {100 * args.max_ratio_spread:.0f}%"
    )
    # Cases whose two statistics straddle 1.00, listed regardless of the verdict: this is the
    # condition under which no ratio for that case should be quoted alone.
    conflicts = [
        (row["label"], name, row["ratios"][name])
        for row in record["cases"]
        for name in gated
        if name in row["ratios"]
        and statistics_disagree(
            row["ratios"][name]["median"], row["ratios"][name]["aggregate_median"]
        )
    ]
    if conflicts:
        print(
            "  the two CPU statistics disagree on the direction of: "
            + ", ".join(f"{label} ({RATIO_LABELS[name]})" for label, name, _ in conflicts)
        )
        for label, name, summary in conflicts:
            print(
                f"    {label:8} {RATIO_LABELS[name]:7} per-iteration median "
                f"{summary['median']:.3f}, per-round total {summary['aggregate_median']:.3f}"
                f"  ({' '.join(f'{v:.3f}' for v in summary['aggregate_per_round'])})"
            )
    if verdict["quiet"]:
        print("  quiet: the rounds agree, so each case's median ratio is repeatable.")
    else:
        print(
            "  NOT QUIET: that case's ratio does not repeat across rounds, so quote it only"
            "\n  with the per-round values beside it. Re-run when the machine is idle."
        )
    # A case outside the gate is reported per round, never as a median alone: the rounds are the
    # evidence, and they stay in the record whether or not the pass was quiet.
    failing = [
        (row["label"], name, row["ratios"][name])
        for row in record["cases"]
        if row["label"] != "control"
        for name in gated
        if name in row["ratios"]
        and row["ratios"][name]["deviation_over_median"] > args.max_ratio_spread
    ]
    if failing:
        print("  outside the gate, per round:")
        for label, name, summary in failing:
            per_round = " ".join(f"{r:.3f}" for r in summary["per_round"])
            print(
                f"    {label:8} {RATIO_LABELS[name]} {per_round}  "
                f"median {summary['median']:.3f}, round {summary['worst_round']} "
                f"{summary['worst_side']} by "
                f"{100 * summary['deviation_over_median']:.0f}%"
            )

    with open(args.out, "w") as handle:
        json.dump(record, handle, indent=2)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
