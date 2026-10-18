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

"""Recompute this directory's tables from the archived `fused_series_regions.py` runs.

    python3 analyse.py W-8r.json W-self8.json G-8r.json G-self8.json

Each round contributes one paired difference per region: `compare-bin` minus `bin`, both medians
of the same round. That cancels nothing by itself — the two sessions of a round are 30 s apart on
a machine whose sessions differ by 6 ms on a 28 ms region — so the difference is additionally
controlled by `fused_kernel_time`, which the change under test does not touch and which therefore
measures the round's drift.

Both readings are printed: the *raw* difference and the *adjusted* one (`region - kernel`). The
adjustment is only trustworthy to the extent that an untouched region comes back at zero, which is
why the same arithmetic is run over the same-binary runs and printed beside it.
"""

import json
import statistics
import sys

REGIONS = ("evaluate", "prep", "kernel", "columns", "wall", "cpu")
CONTROLLED = ("evaluate", "prep", "columns")
POINTS = 400 * 7_171


def load(path):
    """`([raw differences], is_self_control, case)` for one run file."""
    data = json.load(open(path))
    rows = [
        {key: record["medians"]["compare-bin"][key] - record["medians"]["bin"][key]
         for key in REGIONS}
        for record in data["rounds"]
    ]
    return rows, data["bin"] == data["compare_bin"], data["case"]


def adjusted(rows, key):
    """`region - kernel` per round: the region's movement with the round's drift removed."""
    return [row[key] - row["kernel"] for row in rows]


def describe(label, values):
    median = statistics.median(values)
    error = statistics.stdev(values) / len(values) ** 0.5
    negative = sum(1 for value in values if value < 0)
    print(f"  {label:22} median {median:+7.2f} ms  SE {error:5.2f}  "
          f"negative {negative:2}/{len(values)}  range {min(values):+7.2f}..{max(values):+7.2f}")
    return median, error


def main():
    runs = {"ab": [], "control": []}
    for path in sys.argv[1:]:
        rows, is_control, case = load(path)
        runs["control" if is_control else "ab"].append((rows, case))
        print(f"{path:16} case {case:2} "
              f"{'same-binary control' if is_control else 'A/B'}  {len(rows)} rounds")

    for kind, label in (("ab", "A/B (old vs no-duplicate)"),
                        ("control", "same-binary control")):
        pooled = [row for rows, _ in runs[kind] for row in rows]
        if not pooled:
            continue
        print()
        print(f"{label}, {len(pooled)} rounds pooled")
        for key in CONTROLLED:
            describe(key, adjusted(pooled, key))

    # The effect is the difference between the two adjusted medians: the control says what the
    # adjustment itself does to an unchanged region, and subtracting it leaves the change.
    ab_pooled = [row for rows, _ in runs["ab"] for row in rows]
    ctrl_pooled = [row for rows, _ in runs["control"] for row in rows]
    if not (ab_pooled and ctrl_pooled):
        print()
        print("one of the two kinds is missing: no effect estimate")
        return 0

    print()
    print("effect, control-subtracted")
    for key in CONTROLLED:
        ab_values, ctrl_values = adjusted(ab_pooled, key), adjusted(ctrl_pooled, key)
        difference = statistics.median(ab_values) - statistics.median(ctrl_values)
        error = (statistics.stdev(ab_values) ** 2 / len(ab_values)
                 + statistics.stdev(ctrl_values) ** 2 / len(ctrl_values)) ** 0.5
        print(f"  {key:9} {difference:+7.2f} ms  ({difference / error:+.1f} SE, "
              f"{difference * 1e6 / POINTS:+.3f} ns per point)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
