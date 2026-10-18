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

Three things a naive frame count gets wrong, each of which produced a wrong number here
before it was fixed:

**1. Each line carries the number of samples it stands for.** `pprof` 0.15's `Debug for
Report` writes `{frames:?} {count}` (`src/report.rs:150`), so one line is one *distinct*
stack and the trailing integer is how many samples had it. Counting lines instead of
summing that integer undercounts by the average multiplicity: run `W-on3` is 5 621 lines
and 26 529 samples, a factor of 4.7. Everything below is weighted, and `total` is the sum
of the trailing integers.

**2. The stack is leaf-first.** `common_pprof`'s text output puts the sampled instruction
first and ends at `start_thread`/`clone3`. Reading it in the other direction attributes
every allocation inside the aggregate to the tokio runtime.

**3. The innermost recognised owner wins, and the search follows the stack, not the
category list.** The previous version iterated owners in a fixed priority order and asked
whether *any* frame of the stack matched *that owner*; because `fused: stream` was checked
before `scan`, a sample taken inside a mito2 read called from `FusedSeriesStream` was
charged to the fused stream, not to the scan. The fix is to walk the frames from the leaf
outwards and stop at the first frame that matches any owner, so the frame nearest the
sampled instruction decides. Whatever matches nothing is reported as unrecognised with its
leaf frame, never folded into a named bucket.

    scripts/fused_profile_summarize.py PROFILE.txt [--top N] [--self-test]

`--self-test` checks the arithmetic against a two-record fixture whose answer is known.
"""

import argparse
import sys
from collections import Counter

# One owner per frame, checked against the frame itself. Order here is only a tie-break for
# frames that match two patterns; it is *not* a precedence over the stack.
OWNERS = [
    ("fused: evaluator", ("promql::range_eval",)),
    ("fused: output build", ("promql::grid_series",)),
    ("fused: stream (emit/flush)", ("promql::extension_plan::fused_series",)),
    (
        "aggregate",
        (
            "PartialHashAggregateStream",
            "GroupedHashAggregateStream",
            "GroupValuesColumn",
            "GroupValueBuilder",
            "group_values::multi_group_by",
            "groups_accumulator",
        ),
    ),
    ("sort", ("sorts::merge", "arrow_ord::sort", "compare_impl", "quicksort")),
    ("shuffle", ("BatchPartitioner", "OutputChannel", "SharedCoalescer")),
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
            "mito2::read",
            "mito2::sst",
            "mito_codec",
        ),
    ),
    ("planning", ("create_physical_plan", "DefaultPhysicalPlanner")),
    ("logging / plan formatting", ("tracing_subscriber", "dispatch_record", "IndentVisitor")),
    (
        "http response",
        ("servers::http", "serde_json", "HttpRecords", "GreptimedbV1", "serde_core"),
    ),
]


class ProfileError(SystemExit):
    """A profile line that cannot be parsed. Never silently defaulted."""


def parse_line(line):
    """`(frames, weight, thread)` from one line of pprof text output.

    The line is `FRAME: a -> FRAME: b -> THREAD: name <count>`. The thread name may contain
    spaces and digits (`global-worker 1`), so the count is the last whitespace-separated
    token and everything after the final `THREAD: ` belongs to the name. A line without a
    trailing integer is an error: guessing one would reintroduce the undercount this
    function exists to avoid.
    """
    line = line.rstrip("\n")
    if not line.strip():
        raise ProfileError("empty line in a profile; refusing to guess its weight")
    head, _, last = line.rpartition(" ")
    if not last.isdigit():
        raise ProfileError(
            f"last token of a profile line is not a sample count: {last!r}\n"
            f"  line: {line[:160]}\n"
            "pprof 0.15 writes `{frames} {count}`; a profile from another version or a "
            "truncated file must be re-taken rather than counted as one sample per line."
        )
    weight = int(last)
    # `head` still ends with the thread name, which is the last part of the frame listing.
    frames, _, thread = head.partition(" -> THREAD: ")
    return frames, weight, thread


def frames_of(frames_text):
    """The frames of one line, leaf first."""
    return [
        frame.replace("FRAME: ", "", 1)
        for frame in frames_text.split(" -> ")
        if frame and not frame.startswith("THREAD:")
    ]


def owner_of(frames):
    """The innermost recognised owner of one sample, or `None`.

    Walking from the leaf outwards is what makes this an attribution rather than a
    membership test: the first frame that names a known component is the component the
    sampled instruction was running in. A sample inside `mito2::read` that was reached
    through `FusedSeriesStream` is the scan's, because the scan's frame is nearer the leaf.
    """
    for frame in frames:
        for name, needles in OWNERS:
            if any(needle in frame for needle in needles):
                return name
    return None


def samples(lines):
    """`(frames, weight)` per distinct stack, validated once for every caller."""
    for line in lines:
        frames_text, weight, _thread = parse_line(line)
        frames = frames_of(frames_text)
        if not frames:
            raise ProfileError(f"no frames on a line: {line[:160]}")
        yield frames, weight


def summarize(lines):
    """`(total, Counter of owner -> weight, Counter of owner -> samples, unrecognised)`."""
    by_owner = Counter()
    distinct = Counter()
    total = 0
    for frames, weight in samples(lines):
        total += weight
        distinct[owner_of(frames) or "unrecognised"] += 1
        by_owner[owner_of(frames) or f"unrecognised: {frames[0][:60]}"] += weight
    return total, by_owner, distinct


def inside(lines, want):
    """`(weight, Counter of leaf frame -> weight)` for the samples attributed to `want`.

    The owner table answers "which module"; this answers "what inside that module", which the
    sampler can only do when the work is not inlined into one frame. The leaf is the innermost
    frame, i.e. the code that was actually running, so an inlined kernel that pprof resolved
    shows up under its own name rather than under its caller.
    """
    total = 0
    by_leaf = Counter()
    for frames, weight in samples(lines):
        owner = owner_of(frames)
        name = owner or "unrecognised"
        if name != want:
            continue
        total += weight
        by_leaf[frames[0][:100]] += weight
    return total, by_leaf


def merge_unrecognised(by_owner):
    """Fold the per-leaf unrecognised buckets into one, keeping the leaves as a breakdown."""
    merged = Counter()
    leaves = Counter()
    for name, weight in by_owner.items():
        if name.startswith("unrecognised"):
            merged["unrecognised"] += weight
            leaves[name.split(": ", 1)[1] if ": " in name else "?"] += weight
        else:
            merged[name] += weight
    return merged, leaves


SELF_TEST = [
    # Two records, 99 samples in the evaluator and 1 in the aggregate. A line count would
    # say 50%/50%; the answer is 99%/1% and a total of 100.
    "FRAME: <promql::range_eval::RangeEvaluator>::evaluate -> FRAME: start_thread "
    "-> THREAD: global-worker 1 99",
    "FRAME: <X as GroupValuesColumn<false>>::push -> FRAME: start_thread "
    "-> THREAD: global-worker 1 1",
]


def self_test():
    total, by_owner, distinct = summarize(SELF_TEST)
    merged, _leaves = merge_unrecognised(by_owner)
    failures = []
    if total != 100:
        failures.append(f"total is {total}, expected 100")
    got = 100.0 * merged["fused: evaluator"] / total if total else 0.0
    if abs(got - 99.0) > 1e-9:
        failures.append(f"evaluator share is {got:.4f}%, expected 99%")
    got_agg = 100.0 * merged["aggregate"] / total if total else 0.0
    if abs(got_agg - 1.0) > 1e-9:
        failures.append(f"aggregate share is {got_agg:.4f}%, expected 1%")
    if len(distinct) != 2:
        failures.append(f"two distinct owners expected, got {dict(distinct)}")

    # The second bug, in isolation: an inner scan frame under a fused-stream ancestor.
    inner = frames_of(
        "FRAME: mito2::read::scan_region -> FRAME: <promql::extension_plan::fused_series::"
        "FusedSeriesStream as Stream>::poll_next -> FRAME: start_thread"
    )
    if owner_of(inner) != "scan":
        failures.append(f"inner scan under FusedSeriesStream resolved to {owner_of(inner)!r}")

    if failures:
        print("SELF-TEST FAILED", file=sys.stderr)
        for failure in failures:
            print(f"  {failure}", file=sys.stderr)
        raise SystemExit(1)
    print("self-test passed: 2 records, total 100, evaluator 99.00%, aggregate 1.00%")
    print("self-test passed: a mito2 frame inside FusedSeriesStream resolves to `scan`")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("profile", nargs="?")
    parser.add_argument("--top", type=int, default=14)
    parser.add_argument("--self-test", action="store_true")
    parser.add_argument(
        "--cpu-seconds",
        type=float,
        help=(
            "the server's CPU seconds over the same sampling window, so the sample count can "
            "be checked against what the sampler should have produced. Compare it with the "
            "run's own figure, not another run's."
        ),
    )
    parser.add_argument("--frequency", type=int, default=199)
    parser.add_argument(
        "--inside",
        metavar="OWNER",
        help=(
            "also break one owner down by leaf frame, e.g. `--inside 'fused: evaluator'`; "
            "the table above answers which module, this answers what inside it"
        ),
    )
    args = parser.parse_args()

    if args.self_test:
        return self_test()
    if not args.profile:
        parser.error("a profile file is required unless --self-test is given")

    with open(args.profile) as handle:
        lines = [line for line in handle if line.strip()]
    if not lines:
        raise SystemExit(f"{args.profile} holds no samples")

    total, by_owner, distinct = summarize(lines)
    merged, leaves = merge_unrecognised(by_owner)

    print(f"{args.profile}: {len(lines)} distinct stacks, {total} samples")
    if args.cpu_seconds:
        expected = args.cpu_seconds * args.frequency
        print(
            f"  against {args.cpu_seconds:.1f} s of server CPU at {args.frequency} Hz the "
            f"sampler should have produced ~{expected:.0f} samples: captured "
            f"{100.0 * total / expected:.0f}%"
            if expected
            else "  no CPU figure to compare against"
        )
        print(
            f"  {total / args.cpu_seconds:.1f} samples per CPU second, "
            f"{total / args.cpu_seconds / args.frequency * 100:.0f}% of the requested rate"
        )
    print()
    print("CPU by innermost owner (disjoint, weighted; shares sum to 100%)")
    print(f"{'share':>7}  {'samples':>8}  {'stacks':>7}  owner")
    print("-" * 68)
    for name, weight in merged.most_common():
        print(
            f"{100.0 * weight / total:6.1f}%  {weight:8}  {distinct.get(name, 0):7}  {name}"
        )
    if leaves:
        print()
        print("  of the unrecognised samples, by leaf frame:")
        for leaf, weight in leaves.most_common(8):
            print(f"    {100.0 * weight / total:5.1f}%  {weight:7}  {leaf[:100]}")
    if args.inside:
        owner_weight, by_leaf = inside(lines, args.inside)
        if not owner_weight:
            raise SystemExit(f"no samples are attributed to {args.inside!r}")
        print()
        print(
            f"inside {args.inside!r}: {owner_weight} samples "
            f"({100.0 * owner_weight / total:.1f}% of the profile), by leaf frame"
        )
        print(f"{'share':>7}  {'samples':>8}  leaf")
        print("-" * 68)
        for leaf, weight in by_leaf.most_common(args.top):
            print(
                f"{100.0 * weight / owner_weight:7.2f}%  {weight:8}  {leaf}"
            )
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())
