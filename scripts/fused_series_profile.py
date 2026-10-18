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

"""CPU-sample profile of one benchmark case, and the row counts its plan produced.

Two separate outputs, deliberately not mixed:

* **`pprof` samples** (`--profile-out`): the server's own sampling profiler, so the
  attribution is per function rather than per operator. Operator `elapsed_compute` is not
  used for CPU accounting: nested operators each count their children's time, and a
  partitioned plan runs the counter once per partition, so summing it over-states the work
  and cannot be compared with wall clock. With `--analyze-only` the sampling window is skipped
  entirely and only the analysis below is read, which is what the row and batch counts need.
  `TQL ANALYZE VERBOSE` is then queried, but only
  for `output_rows` / `output_batches`, which are counts rather than times.

* **`output_rows` / `output_batches`** (`--analyze-out`): what each operator in the
  physical plan actually produced. For a fused range query the interesting numbers are the
  intermediate row count the fused node hands up and the final row count the aggregate
  emits; their ratio is the redundancy a fused aggregate would remove.

The profiler samples every thread for `--seconds`, during which this script runs the case
query as many times as fit. Queries per sample window, their wall times and the server's CPU
consumption are reported so the reader can tell whether the window was dense enough and
whether the sampled work matches the measured work.

    scripts/fused_series_profile.py --data-home DIR --bin PATH --case W \\
        --seconds 30 --profile-out prof.txt --analyze-out analyze.json

Reuses `fused_series_bench.Server` for starting, health-checking and stopping the server, so
the profile is taken under the same conditions as the benchmark.
"""

import argparse
import json
import os
import re
import sys
import threading
import time
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import fused_profile_summarize as summarize
import fused_series_bench as bench


def fetch_profile(http, seconds, frequency, output):
    """Runs the server's CPU profiler for `seconds` and returns its body as text."""
    url = (
        # `/debug/prof/cpu`, verified against a running server: the debug router is nested under
        # `/debug`, and `/prof/cpu` returns 404.
        f"http://127.0.0.1:{http}/debug/prof/cpu"
        f"?seconds={seconds}&frequency={frequency}&output={output}"
    )
    request = urllib.request.Request(url, data=b"", method="POST")
    with urllib.request.urlopen(request, timeout=seconds + 120) as response:
        body = response.read()
    return body.decode("utf-8", errors="replace")


def analyze(server, case):
    """`TQL ANALYZE VERBOSE FORMAT JSON` for exactly the statement the benchmark runs.

    Two things here are easy to get wrong and both were, so they are spelled out.

    The expression is `bench.tql(case)`, not `case["expr"]`. The latter is only the inner
    `rate(...)`; `bench.tql` is what adds `sum by (instance)` (fused_series_bench.py:542). A plan
    of the inner expression has no aggregate in it and reports all 2 868 400 intermediate rows,
    which is a different query from the one being measured.

    The parameters are the three-argument form. `lookback` is `Option` in the parser
    (`src/sql/src/parsers/tql_parser.rs`), and the benchmark's `TQL EVAL (start, end, step)`
    passes only three, so the server uses its default lookback. Adding a fourth argument here
    would analyse a query with different staleness behaviour than the measured one.
    """
    statement = bench.tql(case).replace("TQL EVAL ", "TQL ANALYZE VERBOSE FORMAT JSON ", 1)
    payload, _ = server.sql(statement)
    return payload


OPERATOR_LINE = re.compile(r"(\w+(?:Exec|Scan|Source|SourceExec)?)\b.*?output_rows=(\d+)")


def operator_rows(payload):
    """`[(operator, output_rows, output_batches, elapsed_compute)]` from an ANALYZE plan.

    Walks the serialized plan looking for the per-operator metrics tuples rather than
    parsing the pretty-printed tree, because the tree's indentation is unstable. Only the
    counts are used downstream; `elapsed` is captured for the record and explicitly not
    summed.
    """
    found = []

    def walk(node):
        if isinstance(node, dict):
            metrics = node.get("metrics")
            name = node.get("name") or node.get("operator")
            if isinstance(metrics, dict) and name:
                found.append(
                    (
                        str(name),
                        int(metrics.get("output_rows", 0)),
                        int(metrics.get("output_batches", 0)),
                        metrics.get("elapsed_compute"),
                    )
                )
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for value in node:
                walk(value)

    walk(payload)
    return found


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-home", required=True)
    parser.add_argument("--bin", required=True)
    parser.add_argument("--case", required=True, help="a case name from fused_series_bench")
    parser.add_argument("--arm", default="on", choices=["on", "off"])
    parser.add_argument("--seconds", type=int, default=30)
    parser.add_argument("--frequency", type=int, default=99)
    parser.add_argument("--output", default="text", choices=["text", "flamegraph", "proto"])
    parser.add_argument("--profile-out")
    parser.add_argument("--analyze-out")
    parser.add_argument(
        "--analyze-only",
        action="store_true",
        help=(
            "skip the sampling window and only dump `TQL ANALYZE VERBOSE` for the case. The "
            "operator counters this reads (`output_rows`, `output_batches`, the fused node's "
            "own block timers) are sums over one operator each, so unlike nested "
            "`elapsed_compute` they do not count a child's work more than once."
        ),
    )
    parser.add_argument("--summary-out")
    parser.add_argument("--log", default="/tmp/fused-profile-server.log")
    parser.add_argument(
        "--log-level",
        default="info,query::datafusion::native_promql::fused=debug",
        help=(
            "tracing Targets filter for the server. The default keeps the fused-selection "
            "line at debug, which is what confirms the query fused, while everything else "
            "stays at info: a full `debug` filter makes datafusion format the physical plan "
            "for a log record on every planning pass, which was 4.6%% of the samples."
        ),
    )
    parser.add_argument("--http", type=int, default=15110)
    parser.add_argument("--grpc", type=int, default=15112)
    parser.add_argument("--mysql", type=int, default=15114)
    parser.add_argument("--postgres", type=int, default=15116)
    args = parser.parse_args()
    if not args.analyze_only and not args.profile_out:
        parser.error("--profile-out is required unless --analyze-only is given")
    if args.analyze_only and not args.analyze_out:
        parser.error("--analyze-only needs --analyze-out to write the plan to")

    case = next((c for c in bench.CASES if c["name"] == args.case), None)
    if case is None:
        raise SystemExit(f"no case {args.case}; known: {[c['name'] for c in bench.CASES]}")
    # `bench.tql` is the only thing that builds the statement the benchmark runs: it wraps the
    # case's inner expression in `sum by (instance)` (fused_series_bench.py:535). Using the raw
    # `expr` instead profiles `rate(...)` unaggregated, which returns one row per series per grid
    # point -- 2 868 400 rows for `W` -- and then measures JSON serialisation of all of them.
    statement = bench.tql(case)

    native = args.arm == "on"
    server = bench.Server(
        args.bin,
        args.data_home,
        args.http,
        args.grpc,
        args.mysql,
        args.postgres,
        native,
        args.log,
    )
    server.log_level = args.log_level
    with server:
        # warm the plan and the page cache, so the profile is of steady state
        for _ in range(2):
            server.sql(statement)
        if args.analyze_only:
            analysis = analyze(server, case)
            with open(args.analyze_out, "w") as handle:
                json.dump(analysis, handle, indent=2)
            for name, rows, batches, elapsed in operator_rows(analysis):
                print(f"  {name:28} output_rows={rows:>9} batches={batches:>6} elapsed={elapsed}")
            return
        cpu_before = server.cpu_seconds()
        started = time.perf_counter()

        profile = {}

        def run_profiler():
            try:
                profile["text"] = fetch_profile(
                    args.http, args.seconds, args.frequency, args.output
                )
            except Exception as error:  # reported below, not as a thread traceback
                profile["error"] = f"{type(error).__name__}: {error}"

        thread = threading.Thread(target=run_profiler, daemon=True)
        thread.start()
        # Give the profiler a moment to install its sampler before loading the CPU.
        time.sleep(1.0)

        queries = []
        while thread.is_alive():
            query_started = time.perf_counter()
            payload, size = server.sql(statement)
            queries.append(
                {
                    "ms": (time.perf_counter() - query_started) * 1000.0,
                    "bytes": size,
                    "output_rows": sum(
                        len(block["records"]["rows"])
                        for block in payload.get("output", [])
                        if block.get("records")
                    ),
                }
            )
        thread.join()

        cpu_ms = (server.cpu_seconds() - cpu_before) * 1000.0
        window_ms = (time.perf_counter() - started) * 1000.0

        if "text" not in profile:
            raise SystemExit(f"the profiler request failed: {profile.get('error')}")
        # `pprof` writes one line per *distinct* stack plus the number of samples that had it
        # (`{frames} {count}`, pprof-0.15.0/src/report.rs:150), so neither the line count nor a
        # per-frame tally over lines is a sample count. Everything below is weighted by that
        # trailing integer, through the same parser the summariser uses so the two cannot
        # drift apart.
        stacks = [line for line in profile["text"].splitlines() if line.strip()]
        weighted = []
        for stack in stacks:
            frames_text, weight, thread = summarize.parse_line(stack)
            weighted.append((summarize.frames_of(frames_text), weight, thread))
        sample_total = sum(weight for _, weight, _ in weighted)
        frames = {}
        for frame_list, weight, _ in weighted:
            for name in frame_list:
                frames[name] = frames.get(name, 0) + weight
        with open(args.profile_out, "w") as handle:
            handle.write(profile["text"])

        if args.analyze_out:
            analysis = analyze(server, case)
            with open(args.analyze_out, "w") as handle:
                json.dump(analysis, handle, indent=2)
            for name, rows, batches, elapsed in operator_rows(analysis):
                print(f"  {name:28} output_rows={rows:>9} batches={batches:>6} elapsed={elapsed}")

        markers = [
            "extrapolated_rate_batch",
            "<promql::range_eval::RangeEvaluator>::evaluate",
            "range_eval::flush_batch",
            "grid_series::",
            "fused_series::",
            "occupancy",
            "series_to_record_batch",
            "concat_batches",
            "AggregateExec",
            "grouped_hash",
            "HashAggregateStream",
            "serde_json",
            "merge_scan",
            "MergeScanExec",
            "PromSeries",
            "topk",
        ]
        # Weighted: a marker's count is the samples that contain it, not the lines.
        by_marker = {
            marker: sum(
                weight for frame_list, weight, _ in weighted if any(marker in f for f in frame_list)
            )
            for marker in markers
        }
        summary = {
            "case": args.case,
            "samples": sample_total,
            "distinct_stacks": len(stacks),
            "samples_with_marker": by_marker,
            "top_frames": sorted(frames.items(), key=lambda kv: -kv[1])[:25],
            "arm": args.arm,
            "expr": case["expr"],
            "step": case["step"],
            "span_minutes": case["span_minutes"],
            "window_seconds": case["window_seconds"],
            "sample_seconds": args.seconds,
            "frequency": args.frequency,
            "queries_in_window": len(queries),
            "window_ms": window_ms,
            "server_cpu_ms": cpu_ms,
            "busy_fraction": (cpu_ms / window_ms) if window_ms else None,
            "query_ms_median": sorted(q["ms"] for q in queries)[len(queries) // 2]
            if queries
            else None,
            "output_rows_per_query": sorted({q["output_rows"] for q in queries}),
            # The sampler's rate against the CPU actually burned in the same window. ITIMER_PROF
            # fires on process CPU, so `expected` is what a perfect sampler would have produced;
            # anything much below it means lost signals and a share that is not a share of CPU.
            "expected_samples_at_frequency": cpu_ms / 1000.0 * args.frequency,
            "captured_fraction": (
                sample_total / (cpu_ms / 1000.0 * args.frequency)
                if cpu_ms > 0 and args.frequency
                else None
            ),
            "queries": queries,
        }
        if args.summary_out:
            with open(args.summary_out, "w") as handle:
                json.dump(summary, handle, indent=2)
        expected = summary["expected_samples_at_frequency"]
        print(
            f"  {sample_total} samples, {len(stacks)} distinct stacks"
            + (
                f"; {cpu_ms / 1000.0:.1f} s of CPU at {args.frequency} Hz should give "
                f"~{expected:.0f}, so {100.0 * summary['captured_fraction']:.0f}% were captured"
                if expected
                else ""
            )
        )
        for marker, count in sorted(by_marker.items(), key=lambda kv: -kv[1]):
            if count:
                print(f"  samples containing {marker:52} {count:6}")
        print()
        for name, count in summary["top_frames"]:
            share = 100.0 * count / sample_total if sample_total else 0.0
            print(f"  {share:5.1f}%  {count:6}  {name[:110]}")
        print()
        print(
            f"case {args.case} arm {args.arm}: {len(queries)} queries in "
            f"{window_ms:.0f} ms, server CPU {cpu_ms:.0f} ms "
            f"({(cpu_ms / window_ms * 100.0) if window_ms else 0:.0f}% busy), "
            f"median query {summary['query_ms_median']:.1f} ms"
        )


if __name__ == "__main__":
    main()
