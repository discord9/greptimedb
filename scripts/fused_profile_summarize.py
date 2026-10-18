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

"""Turns a `fused_series_profile.py` text profile into a CPU attribution.

Two things this gets right that a naive frame count does not:

* **The stack is leaf-first.** `common_pprof`'s text output puts the sampled instruction
  first and ends at `start_thread`/`clone3`. Reading it in the other direction attributes
  every allocation inside the aggregate to the tokio runtime.

* **Ancestors are not owners.** The hash aggregate polls the fused node, so a sample taken
  inside `FusedSeriesStream` still holds the aggregate on its stack. Counting "samples
  containing the aggregate" would charge it for the fused node's work. The partition below
  assigns each sample to its *innermost* recognisable owner, so the buckets are disjoint and
  sum to the sample count.

Print the sample count with any percentage: the profiler's effective rate here was ~94
samples/s at a requested 199 Hz, and a few thousand samples puts about a point of noise on
each share.

    scripts/fused_profile_summarize.py PROFILE.txt [--top N]
"""

import argparse
import sys
from collections import Counter

# The same stack can name the same work at several levels; each tuple is one owner, checked
# innermost-first, and the first group that matches anywhere in the stack wins.
OWNERS = [
    ("fused: evaluator", ("promql::range_eval",)),
    ("fused: output build", ("promql::grid_series",)),
    ("fused: stream (emit/flush)", ("promql::extension_plan::fused_series",)),
    (
        "aggregate (sum by)",
        (
            "PartialHashAggregateStream",
            "GroupedHashAggregateStream",
            "GroupValuesColumn",
            "GroupValueBuilder",
            "group_values::multi_group_by",
            "groups_accumulator",
        ),
    ),
    ("sort (final order)", ("sorts::merge", "arrow_ord::sort", "compare_impl", "quicksort")),
    ("shuffle (repartition)", ("BatchPartitioner", "OutputChannel", "SharedCoalescer")),
    (
        "scan",
        (
            "SeriesDistributor",
            "SeriesScan",
            "scan_region",
            "ScanInput",
            "flat_dedup",
            "memtable",
            "part_reader",
            "Pruner",
        ),
    ),
    ("planning", ("create_physical_plan", "DefaultPhysicalPlanner")),
    ("logging / plan formatting", ("tracing_subscriber", "dispatch_record", "IndentVisitor")),
    (
        "http response",
        ("servers::http", "serde_json", "HttpRecords", "GreptimedbV1", "serde_core"),
    ),
]

RUNTIME = (
    "start_thread",
    "clone3",
    "std::sys::",
    "tokio::runtime",
    "core::future::future::Future>::poll",
    "core::pin::Pin",
    "alloc::boxed::Box",
    "futures_util::",
    "futures_core::",
    "tower::util",
    "axum::",
    "hyper",
    "tracing::instrument",
    "std::thread::lifecycle",
    "core::task::poll",
    "async_stream::",
    "core::result::Result",
    "core::ops::function",
    "std::backtrace",
)


def frames(line):
    return [
        frame.replace("FRAME: ", "", 1)
        for frame in line.rstrip("\n").split(" -> ")
        if not frame.startswith("THREAD:")
    ]


def owner(line):
    stack = frames(line)
    for name, needles in OWNERS:
        if any(needle in frame for frame in stack for needle in needles):
            return name
    # Nothing recognised: report the innermost frame that is not runtime scaffolding, so an
    # unclassified share is still diagnosable rather than a bare "other".
    for frame in stack:
        if not any(marker in frame for marker in RUNTIME):
            if "_rjem_" in frame or "alloc::alloc" in frame or "malloc" in frame:
                return "allocator"
            return f"other: {frame[:60]}"
    return "other: runtime only"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("profile")
    parser.add_argument("--top", type=int, default=14)
    args = parser.parse_args()

    with open(args.profile) as handle:
        lines = [line for line in handle if line.strip()]
    if not lines:
        raise SystemExit(f"{args.profile} holds no samples")

    counts = Counter(owner(line) for line in lines)
    total = len(lines)
    print(f"{args.profile}: {total} samples, {len(set(lines))} distinct stacks")
    print()
    print("CPU by innermost owner (disjoint; shares sum to 100%)")
    print(f"{'share':>7}  {'samples':>7}  owner")
    print("-" * 60)
    for name, count in counts.most_common():
        print(f"{100.0 * count / total:6.1f}%  {count:7}  {name}")

    print()
    print(f"top {args.top} innermost frames (leaf-first, unfiltered)")
    print(f"{'share':>7}  {'samples':>7}  frame")
    print("-" * 60)
    leaves = Counter(frames(line)[0] for line in lines)
    for name, count in leaves.most_common(args.top):
        print(f"{100.0 * count / total:6.1f}%  {count:7}  {name[:100]}")

    print()
    print("logging on the stack at all:")
    logging = sum(
        1 for line in lines if "tracing_subscriber" in line or "dispatch_record" in line
    )
    print(f"{100.0 * logging / total:6.1f}%  {logging:7}  (a `debug` filter makes this large)")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
