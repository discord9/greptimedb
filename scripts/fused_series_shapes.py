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

"""Probe three data shapes the strictly-rising `vmbench` fixture cannot express.

`fused_series_bench.py` seeds a counter that rises by a constant step and never resets, so
on that fixture `rate` needs no counter correction, no query window ever has to jump a gap,
and every window is fed by a handful of large, regularly sized on-disk chunks. This script
adds three small extra tables to the same data home -- 40 series x up to 1 440 samples, at
most 57 600 rows each, a twentieth of the main fixture -- and runs one `sum by (instance)`
query per shape against **both** arms: the ordinary plan (flag off) and the native PromQL
path (flag on), from the same script, through the same `/v1/sql` endpoint. Seeding is lazy
and idempotent, so the second and later runs add nothing.

* **`vmreset`** -- a counter that resets every 48 samples (12 min). `value = base + k * 15`
  where `k` counts samples since the last reset and `base = 100 * (host + 1)`. This is the
  shape that makes the batch kernel's counter-reset index path (`CounterReset`,
  `window_resets` in `src/promql/src/range_eval.rs`) actually fire; on `vmbench` it never
  does, so nothing there exercises the reset bookkeeping at all. The `rate[1h]` case is
  deliberately wide enough to straddle five resets in one window.
* **`vgaps`** -- the same rising values as a plain monotone counter, but rows are *absent*:
  a third of the series drop every fifth sample (periodic gaps), a third lose one contiguous
  30 min block (an outage), a third are complete. Window edges therefore fall inside gaps
  and a short window can hold one or two samples -- or none -- which a fixture with a
  complete 15 s grid never produces.
* **`vfrag`** -- *identical values and sample positions* to `vmreset`, written in 144
  statements per series (10 samples each, 5 760 in total) instead of one statement per
  series. The intent is many small on-disk chunks, so the scan emits many small batches.
  The values are identical on purpose, so `frag5m` and `reset5m` differ in nothing but the
  write pattern: a fused accumulation that loses state at a batch boundary shows up as an
  arm-versus-arm diff on `vfrag`, and that diff cannot be explained by different data.

What this script proves, and what it cannot:

* It proves **correctness and the hit path**. Every case runs on a flag-off and a flag-on
  server; every single execution (correctness pass, every warmup, every timed iteration) is
  held to an explicit fused-selection count -- one on the flag-on arm for a `rate` case,
  zero on flag-off, and zero for the deliberately non-fusing `max_over_time` control -- by
  counting the `Fused series selection succeeded` lines the server logged during that query
  alone. The two arms' correctness-pass results are then compared row by row, value by
  value, through `fused_series_bench.compare_pair`, and a shape whose arms disagree stops
  the run: that is a correctness bug, not a measurement. A case that returns no rows, or a
  different number of rows on the two arms, also stops the run -- an empty result has a
  valid schema and a normal-looking time and would otherwise be reported as a measurement
  of nothing.
* It cannot prove **performance** in general. Forty series say nothing about a 400-series
  workload, three tables are not a corpus, and one sample is not a distribution. The CPU
  and wall-clock numbers are recorded because they cost nothing, and they are comparable
  **only within one round** of this script: the arms are crossed (off then on, then on
  then off) precisely because a number from round 1 and a number from round 3 were taken
  under different machine conditions. A ratio printed here is a local observation, not a
  claim about the fused path's speed.

Everything else matches `fused_series_scale.py`: `bench.Server` over the same data home,
the same `/v1/sql` calls, server CPU from `/proc/<pid>/stat` around each timed iteration,
the same `--runs` / `--warmup` / `--iterations` flags (here `--runs` really does run that
many crossed rounds), and the same correctness-first-then-timed structure.

    scripts/fused_series_shapes.py --data-home DIR --bin PATH --out records/shapes.json \\
        --runs 3 --warmup 2 --iterations 9

The three tables live beside `vmbench` in the same data directory and are seeded on demand;
re-running the script skips a table whose row count already matches and refuses to add to
one that holds a different count, exactly as `bench.seed` does. Use a fresh data directory
if that happens.
"""

import argparse
import json
import os
import statistics
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fused_series_bench as bench

# 40 series at the fixture's own anchor and 15 s interval, so a sample sits where `vmbench`
# has one and the query range below is the same range cases G/W use.
SERIES = 40
SAMPLES = 1_440

# 120 minutes at a 1 s step: the same grid as cases G/W in `fused_series_bench.py`, so the
# row counts and the windows are comparable with section 4.6/4.7 of the report.
SPAN_MINUTES = 120
STEP = "1s"

# One reset every 48 samples at a 15 s step is one reset every 12 minutes; a 1 h window then
# straddles five of them.
RESET_PERIOD = 48

# Ports inside the 15000-15199 band these benchmark runs may bind. They are constants rather
# than flags because the script is meant to be run alone: a second server over the same data
# home would fight over `bench.Server`'s config file as well as over these ports.
HTTP_PORT = 15160
GRPC_PORT = 15162
MYSQL_PORT = 15164
POSTGRES_PORT = 15166


def series_name(host):
    return f"h{host:04d}"


def instance_name(host):
    return f"i{host % 4}"


def base_value(host):
    """A per-series base, so a series that is wrongly grouped cannot hide in a sum."""
    return 100 * (host + 1)


def sample_ts(index):
    return bench.FIRST_SAMPLE_MS + index * bench.SAMPLE_INTERVAL_MS


def reset_value(host, index):
    """`base + k * 15` with `k` samples since the last reset: rises, then drops."""
    return base_value(host) + (index % RESET_PERIOD) * bench.SAMPLE_INTERVAL_SECS


def gaps_value(host, index):
    """A plain monotone counter: identical to `reset_value` before its first reset."""
    return base_value(host) + index * bench.SAMPLE_INTERVAL_SECS


def keeps_all(host, index):
    return True


def gaps_keeps(host, index):
    """Periodic gaps, one outage block, or nothing, depending on the series."""
    if host % 3 == 0:
        return index % 5 != 0
    if host % 3 == 1:
        return not 600 <= index < 720
    return True


# The three shapes. `chunk` is how many samples go into one INSERT: a whole series for
# `vmreset`/`vgaps` (the fixture's own style) and 10 samples for `vfrag`.
SHAPES = [
    {
        "table": "vmreset",
        "value": reset_value,
        "keeps": keeps_all,
        "chunk": SAMPLES,
    },
    {
        "table": "vgaps",
        "value": gaps_value,
        "keeps": gaps_keeps,
        "chunk": SAMPLES,
    },
    {
        "table": "vfrag",
        # 10 samples per INSERT: 144 statements per series, 5 760 in total, so the table is
        # many small on-disk chunks and the scan emits many small batches instead of one
        # batch per series. The values and positions are identical to `vmreset` on purpose,
        # so an accumulation that loses state at a batch boundary shows up as a diff
        # between the two arms and cannot be blamed on different data.
        "value": reset_value,
        "keeps": keeps_all,
        "chunk": 10,
    },
]

CASES = [
    {
        "name": "reset5m",
        "table": "vmreset",
        "expr": "rate({table}[5m])",
        "span_minutes": SPAN_MINUTES,
        "step": STEP,
        "window_seconds": 300,
        "fuses": True,
    },
    {
        "name": "reset1h",
        "table": "vmreset",
        "expr": "rate({table}[1h])",
        "span_minutes": SPAN_MINUTES,
        "step": STEP,
        "window_seconds": 3_600,
        "fuses": True,
    },
    {
        "name": "gaps5m",
        "table": "vgaps",
        "expr": "rate({table}[5m])",
        "span_minutes": SPAN_MINUTES,
        "step": STEP,
        "window_seconds": 300,
        "fuses": True,
    },
    {
        "name": "frag5m",
        "table": "vfrag",
        "expr": "rate({table}[5m])",
        "span_minutes": SPAN_MINUTES,
        "step": STEP,
        "window_seconds": 300,
        "fuses": True,
    },
    {
        # The control: `max_over_time` never fuses, so both arms must log zero selections.
        # It is what proves the hit assertion above can fail, instead of passing because
        # every case happens to log nothing.
        "name": "control",
        "table": "vmreset",
        "expr": "max_over_time({table}[5m])",
        "span_minutes": SPAN_MINUTES,
        "step": STEP,
        "window_seconds": 300,
        "fuses": False,
    },
]


def log(message):
    print(message, flush=True)


def shape_totals(shape):
    """The `(rows, first_ts, last_ts)` this shape must end up with, from its own rules."""
    count = 0
    first_ts = None
    last_ts = None
    for host in range(SERIES):
        for index in range(SAMPLES):
            if not shape["keeps"](host, index):
                continue
            timestamp = sample_ts(index)
            count += 1
            first_ts = timestamp if first_ts is None else min(first_ts, timestamp)
            last_ts = timestamp if last_ts is None else max(last_ts, timestamp)
    return count, first_ts, last_ts


TOTALS = {shape["table"]: shape_totals(shape) for shape in SHAPES}


def check_ranges():
    """Fails unless every case's query range is covered by the seeded samples.

    `TQL EVAL` takes seconds while the rows sit at millisecond timestamps; a query range
    that runs past the last sample returns zero rows with a valid schema and a
    normal-looking time, which is the trap `fused_series_bench.py` documents. Checking it
    once here keeps the row-count guards from firing for the wrong reason.
    """
    last = sample_ts(SAMPLES - 1)
    for case in CASES:
        end_s = bench.QUERY_START_S + case["span_minutes"] * 60
        if end_s * 1000 > last:
            raise SystemExit(
                f"case {case['name']} ends at {end_s}s, after the last seeded sample "
                f"{last} ms; it would return nothing"
            )


def create_table(server, table):
    """The shape tables, in the fixture's layout, with the fixture's tags as primary key.

    `bench.create_table` hardcodes `vmbench`, so it cannot be used for the extra tables and
    is deliberately not changed: the fixture's table stays the one place its name is.
    """
    columns = ", ".join(f'"{tag}" STRING NULL' for tag in bench.TAG_COLUMNS)
    primary_key = ", ".join(f'"{tag}"' for tag in bench.TAG_COLUMNS)
    server.sql(
        f'CREATE TABLE IF NOT EXISTS "{table}" ('
        f"{columns}, "
        f'"{bench.FIELD}" DOUBLE NULL, '
        f'"{bench.TIME}" TIMESTAMP(3) NOT NULL, '
        f'TIME INDEX ("{bench.TIME}"), '
        f"PRIMARY KEY ({primary_key})"
        ") ENGINE=mito"
    )


def read_shape(server, table):
    """`(count, min ts, max ts)` straight from the server, never from the seed loop."""
    return server.rows(
        f'SELECT count(*), min("{bench.TIME}"), max("{bench.TIME}") FROM "{table}"'
    )[0]


def check_shape(table, where, count, first_ts, last_ts):
    """Fails unless the server's own count/min/max equal what the shape generator says."""
    expected_rows, expected_first, expected_last = TOTALS[table]
    if count != expected_rows:
        raise SystemExit(
            f"{where}: {table} holds {count} rows, but this shape seeds {expected_rows} "
            f"({SERIES} series x at most {SAMPLES} samples)"
        )
    if first_ts != expected_first or last_ts != expected_last:
        raise SystemExit(
            f"{where}: {table} spans {first_ts}..{last_ts} ms, but the shape's first and "
            f"last samples are {expected_first}..{expected_last} ms"
        )


def insert_shape(server, shape):
    """Writes one shape, one INSERT per `chunk` samples of a series."""
    table = shape["table"]
    statements = 0
    for host in range(SERIES):
        kept = [index for index in range(SAMPLES) if shape["keeps"](host, index)]
        for offset in range(0, len(kept), shape["chunk"]):
            values = ", ".join(
                f"('{series_name(host)}', '{instance_name(host)}', 'j', 'l', "
                f"{shape['value'](host, index)}, {sample_ts(index)})"
                for index in kept[offset : offset + shape["chunk"]]
            )
            server.sql(
                f'INSERT INTO "{table}" '
                f'("host", "instance", "job", "l1", "{bench.FIELD}", "{bench.TIME}") '
                f"VALUES " + values,
                timeout=3_600,
            )
            statements += 1
        if host % 10 == 0:
            log(f"  {table}: {host + 1}/{SERIES} series, {statements} statement(s)")
    return statements


def seed(data_home, binary):
    """Seeds every shape table once; safe to re-run because the row count is checked first."""
    check_ranges()
    os.makedirs(data_home, exist_ok=True)
    # Beside the data directory rather than inside it, like `bench.seed`'s log.
    log_path = os.path.join(data_home, "..", "fused-shapes-seed.log")
    with bench.Server(
        binary,
        data_home,
        HTTP_PORT,
        GRPC_PORT,
        MYSQL_PORT,
        POSTGRES_PORT,
        False,
        log_path,
    ) as server:
        for shape in SHAPES:
            table = shape["table"]
            create_table(server, table)
            expected_rows, _, _ = TOTALS[table]
            count, first_ts, last_ts = read_shape(server, table)
            if count == expected_rows:
                check_shape(table, "seed", count, first_ts, last_ts)
                log(f"{table} is already seeded: {count} rows at {first_ts}..{last_ts} ms")
                continue
            if count:
                raise SystemExit(
                    f"{table} holds {count} rows, not the {expected_rows} this shape seeds; "
                    "use a fresh data directory instead of mixing shapes"
                )
            log(f"seeding {expected_rows} rows into {table}")
            started = time.time()
            statements = insert_shape(server, shape)
            count, first_ts, last_ts = read_shape(server, table)
            check_shape(table, "seed", count, first_ts, last_ts)
            log(
                f"seeded {table}: {count} rows at {first_ts}..{last_ts} ms, "
                f"{statements} statement(s) in {time.time() - started:.1f}s"
            )


def tql(case):
    """`TQL EVAL` for one case, aggregated like `bench.tql` but over the case's own table."""
    expr = case["expr"].format(table=case["table"])
    start = bench.QUERY_START_S
    end = start + case["span_minutes"] * 60
    return f"TQL EVAL ({start}, {end}, '{case['step']}') sum by (instance) ({expr})"


def run_arm(server, case, arm, warmup, iterations):
    """One correctness pass, then the warmups, then one timing per iteration.

    Every execution is held to its expected fused-selection count, and the timed iterations
    must return as many rows as the correctness pass: a timed query that quietly returns a
    fragment would be timed for the wrong work.
    """
    statement = tql(case)
    server.begin_query()
    payload, _ = server.sql(statement)
    hits = server.hits_since()
    # This driver has no `sum` arm, so no execution of it may log a partial sum.
    bench.check_hits(case, arm, "correctness", hits, server.partial_hits_since())
    correctness_hits = hits
    schema, rows = bench.flatten(payload)
    if not rows:
        raise SystemExit(
            f"case {case['name']} arm {arm} returned 0 rows; {statement} selects nothing "
            f"from {case['table']}"
        )
    correctness = {
        "case": case["name"],
        "arm": arm,
        "stem": f"{case['name']}-{arm}",
        "tql": statement,
        "schema": schema,
        "row_count": len(rows),
        "rows": rows,
    }
    log(f"  {case['name']} arm {arm}: {len(rows)} correctness rows, {hits} fused hit(s)")

    warmup_hits = []
    for index in range(warmup):
        server.begin_query()
        server.sql(statement)
        hits = server.hits_since()
        bench.check_hits(
            case, arm, f"warmup {index + 1}/{warmup}", hits, server.partial_hits_since()
        )
        warmup_hits.append(hits)

    timed = []
    for index in range(iterations):
        server.begin_query()
        cpu_before = server.cpu_seconds()
        started = time.perf_counter()
        payload, size = server.sql(statement)
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        cpu_ms = (server.cpu_seconds() - cpu_before) * 1000.0
        hits = server.hits_since()
        bench.check_hits(
            case, arm, f"iteration {index + 1}/{iterations}", hits, server.partial_hits_since()
        )
        timed_rows = len(bench.flatten(payload)[1])
        if timed_rows != len(rows):
            raise SystemExit(
                f"case {case['name']} arm {arm} iteration {index + 1}: {timed_rows} rows, "
                f"not the {len(rows)} the correctness pass returned for {statement}"
            )
        timed.append(
            {
                "ms": elapsed_ms,
                "cpu_ms": cpu_ms,
                "rows": timed_rows,
                "bytes": size,
                "hits": hits,
                "load1": os.getloadavg()[0],
            }
        )
    return {
        "result": correctness,
        "correctness_hits": correctness_hits,
        "warmup_hits": warmup_hits,
        "timed": timed,
    }


def measure_case(case, arm, args):
    """Starts one server for one arm and runs the case against it."""
    server = bench.Server(
        args.bin,
        args.data_home,
        HTTP_PORT,
        GRPC_PORT,
        MYSQL_PORT,
        POSTGRES_PORT,
        arm == "on",
        args.log,
    )
    with server:
        return run_arm(server, case, arm, args.warmup, args.iterations)


def check_arms(case, results):
    """Fails unless both arms returned rows and the same result, row for row."""
    counts = {arm: results[arm]["result"]["row_count"] for arm in ("off", "on")}
    for arm, count in counts.items():
        if count == 0:
            raise SystemExit(
                f"case {case['name']} arm {arm} returned 0 rows; a case that measures "
                f"nothing is not a measurement ({tql(case)})"
            )
    if counts["off"] != counts["on"]:
        raise SystemExit(
            f"case {case['name']}: the arms disagree on row count: "
            f"{counts['off']} (off) vs {counts['on']} (on)"
        )
    # Order-preserving, field by field: schema, labels, timestamps, row count, values.
    # Flag-off is the baseline because both arms must return the ordinary plan's answer.
    bench.compare_pair(results["off"]["result"], results["on"]["result"])


def summarize(record):
    """Per-case summary across rounds, and the line each case prints at the end."""
    for case in CASES:
        name = case["name"]
        rounds = [
            entry
            for round_entry in record["rounds"]
            for entry in round_entry["cases"]
            if entry["name"] == name
        ]
        cpu = {
            arm: statistics.median(
                [sample["cpu_ms"] for entry in rounds for sample in entry[arm]]
            )
            for arm in ("off", "on")
        }
        wall = {
            arm: statistics.median(
                [sample["ms"] for entry in rounds for sample in entry[arm]]
            )
            for arm in ("off", "on")
        }
        hits = {
            arm: {
                kind: sorted(
                    {value for entry in rounds for value in entry["correctness"][arm][kind]}
                )
                for kind in ("correctness", "warmup", "timed")
            }
            for arm in ("off", "on")
        }
        entry = {
            "name": name,
            "table": case["table"],
            "expr": case["expr"],
            "step": case["step"],
            "span_minutes": case["span_minutes"],
            "window_seconds": case["window_seconds"],
            "fuses": case["fuses"],
            "tql": tql(case),
            "rows": rounds[0]["rows"],
            "hits": hits,
            "rounds": len(rounds),
            "cpu_median_ms": cpu,
            "wall_median_ms": wall,
            "cpu_on_over_off": cpu["on"] / cpu["off"],
            "wall_on_over_off": wall["on"] / wall["off"],
            "cpu_on_over_off_per_round": [
                round_entry["cpu_on_over_off"] for round_entry in rounds
            ],
            "wall_on_over_off_per_round": [
                round_entry["wall_on_over_off"] for round_entry in rounds
            ],
        }
        record["cases"].append(entry)
        # Timings are per round: this line is a summary for reading, not a result to cite.
        per_round = " ".join(
            f"{ratio:.3f}" for ratio in entry["cpu_on_over_off_per_round"]
        )
        print(
            f"{name:8} rows={entry['rows']:>7}  CPU off {cpu['off']:7.0f} ms "
            f"on {cpu['on']:7.0f} ms  on/off {entry['cpu_on_over_off']:.3f}   "
            f"wall off {wall['off']:8.1f} on {wall['on']:8.1f}  "
            f"on/off {entry['wall_on_over_off']:.3f}   "
            f"per-round CPU on/off: {per_round}   "
            f"hits off {hits['off']['timed']} on {hits['on']['timed']}",
            flush=True,
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data-home", required=True)
    parser.add_argument("--bin", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--warmup", type=int, default=2)
    parser.add_argument("--iterations", type=int, default=9)
    parser.add_argument("--log", default="/tmp/fused-shapes-server.log")
    args = parser.parse_args()
    if args.runs < 1 or args.iterations < 1:
        raise SystemExit("--runs and --iterations must both be at least 1")

    seed(args.data_home, args.bin)

    record = {
        "bin": args.bin,
        "data_home": os.path.abspath(args.data_home),
        "runs": args.runs,
        "warmup": args.warmup,
        "iterations": args.iterations,
        "shapes": {
            shape["table"]: {
                "series": SERIES,
                "samples_per_series": SAMPLES,
                "sample_interval_seconds": bench.SAMPLE_INTERVAL_SECS,
                "rows": TOTALS[shape["table"]][0],
                "first_sample_ms": TOTALS[shape["table"]][1],
                "last_sample_ms": TOTALS[shape["table"]][2],
                "statement_samples": shape["chunk"],
            }
            for shape in SHAPES
        },
        "rounds": [],
        "cases": [],
    }

    for round_index in range(args.runs):
        # Crossed order: a round that runs off then on is followed by one that runs on then
        # off, so a slow drift on the machine cannot favour the same arm in every round.
        order = ("off", "on") if round_index % 2 == 0 else ("on", "off")
        round_cases = []
        for case in CASES:
            results = {arm: measure_case(case, arm, args) for arm in order}
            check_arms(case, results)
            # The hit counts are recorded per arm and per phase, not summed: a total can
            # never be mistaken for the per-execution count `bench.check_hits` enforces.
            correctness = {
                arm: {
                    "rows": results[arm]["result"]["row_count"],
                    "correctness": [results[arm]["correctness_hits"]],
                    "warmup": results[arm]["warmup_hits"],
                    "timed": [sample["hits"] for sample in results[arm]["timed"]],
                }
                for arm in ("off", "on")
            }
            cpu = {
                arm: statistics.median([s["cpu_ms"] for s in results[arm]["timed"]])
                for arm in ("off", "on")
            }
            wall = {
                arm: statistics.median([s["ms"] for s in results[arm]["timed"]])
                for arm in ("off", "on")
            }
            entry = {
                "round": round_index + 1,
                "name": case["name"],
                "table": case["table"],
                "tql": tql(case),
                "rows": results["on"]["result"]["row_count"],
                "fuses": case["fuses"],
                "correctness": correctness,
                "cpu_median_ms": cpu,
                "wall_median_ms": wall,
                "cpu_on_over_off": cpu["on"] / cpu["off"],
                "wall_on_over_off": wall["on"] / wall["off"],
                "off": results["off"]["timed"],
                "on": results["on"]["timed"],
            }
            round_cases.append(entry)
            print(
                f"r{round_index + 1} {case['name']:8} rows={entry['rows']:>7}  "
                f"CPU off {cpu['off']:7.0f} ms on {cpu['on']:7.0f} ms  "
                f"on/off {entry['cpu_on_over_off']:.3f}   "
                f"wall off {wall['off']:8.1f} on {wall['on']:8.1f}  "
                f"on/off {entry['wall_on_over_off']:.3f}",
                flush=True,
            )
        record["rounds"].append(
            {"round": round_index + 1, "order": list(order), "cases": round_cases}
        )

    summarize(record)
    with open(args.out, "w") as handle:
        json.dump(record, handle, indent=2)
    print(f"wrote {args.out}")


if __name__ == "__main__":
    main()
