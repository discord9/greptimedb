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

"""Summarises `fused_series_bench.py` records into the report tables.

Every record file is named `<case>-r<round>-<arm>.json` and holds one case, so the
file name is the index. Nothing is discarded: the table prints the median, minimum
and maximum over all rounds and all timed iterations, and the JSON records keep
every individual timing.

    scripts/fused_series_bench_summarize.py RECORD_DIR
"""

import glob
import json
import os
import statistics
import sys

ARMS = ["L", "N", "F"]
ARM_LABEL = {
    "L": "flag off",
    "N": "flag on, rescan",
    "F": "flag on, indexed",
}


def median(values):
    return statistics.median(values) if values else float("nan")


def load(directory):
    """`{case: {arm: [entry, ...]}}` over every record in `directory`."""
    collected = {}
    loads = []
    for path in sorted(glob.glob(os.path.join(directory, "*-r*-*.json"))):
        name = os.path.basename(path)[: -len(".json")]
        # The arm comes from the file name; the case names come from the record itself,
        # which is the only authority on them.
        _, rest = name.rsplit("-r", 1)
        _, arm = rest.split("-", 1)
        with open(path) as handle:
            record = json.load(handle)
        for case in sorted({entry["name"] for entry in record["cases"]}):
            entries = [entry for entry in record["cases"] if entry["name"] == case]
            collected.setdefault(case, {}).setdefault(arm, []).extend(entries)
        loads.extend(entry["load1"] for entry in entries if "load1" in entry)
    return collected, loads


def main():
    collected, loads = load(sys.argv[1])
    if not collected:
        raise SystemExit(f"no records in {sys.argv[1]}")

    print(f"{len(collected)} case(s): {', '.join(collected)}")
    if loads:
        print(f"host load average during the runs: min {min(loads):.1f}, "
              f"median {median(loads):.1f}, max {max(loads):.1f}")
    print()

    print("## Per-arm timings, all rounds and timed iterations\n")
    header = (f"{'case':<8}{'arm':<4}{'mode':<20}{'runs':>5}{'rows':>8}{'hits':>6}"
              f"{'median':>9}{'min':>8}{'max':>8}")
    print(header)
    print("-" * len(header))
    medians = {}
    for case, arms in collected.items():
        for arm in ARMS:
            entries = arms.get(arm, [])
            if not entries:
                continue
            times = [entry["ms"] for entry in entries]
            medians[(case, arm)] = median(times)
            print(
                f"{case:<8}{arm:<4}{ARM_LABEL[arm]:<20}{len(times):>5}"
                f"{'/'.join(map(str, sorted({e['rows'] for e in entries}))):>8}"
                f"{'/'.join(map(str, sorted({e['hits'] for e in entries}))):>6}"
                f"{median(times):>9.1f}{min(times):>8.1f}{max(times):>8.1f}"
            )
        print()

    print("## Ratios: ordinary time / fused time, so above 1 means fusion is faster\n")
    header = (f"{'case':<8}{'L off':>9}{'N rescan':>10}{'F indexed':>11}"
              f"{'L/N':>7}{'L/F':>7}{'N/F':>7}{'N->F':>8}")
    print(header)
    print("-" * len(header))
    for case in collected:
        left, rescan, fixed = (medians.get((case, arm)) for arm in ARMS)
        if None in (left, rescan, fixed):
            continue
        print(
            f"{case:<8}{left:>9.1f}{rescan:>10.1f}{fixed:>11.1f}"
            f"{left / rescan:>7.2f}{left / fixed:>7.2f}{rescan / fixed:>7.2f}"
            f"{100 * (rescan - fixed) / rescan:>7.0f}%"
        )


if __name__ == "__main__":
    main()
