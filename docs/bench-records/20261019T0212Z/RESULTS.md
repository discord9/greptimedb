# 2026-10-19 — section 4.11, the partial `SUM` inside the fused node

Binary: `83e366b33bd`, `cargo build --release --bin greptime` (`nightly` profile,
`lto = "thin"`), kept as `greptime-final-83e366b`. One binary; the three arms differ only in
`GREPTIME_FUSED_PARTIAL_SUM` (`off` = flag off; `on` = flag on, variable `0`; `sum` = flag on,
variable `1`).

## Which figure comes from which file

| figure | file |
| --- | --- |
| the seven cases' CPU medians and per-round ratios; the gate's not-quiet verdict | `scale-3arm.json` |
| the three plans, one per arm, for `sum by (instance) (rate(vmbench[5m]))` | `partial-plan-C.txt` |
| the 480-row agreement of the three arms on that query | `partial-plan-C.txt`, last line |

`scale-3arm.json` holds the per-iteration samples behind every median
(`cases[].rounds[].{off,on,sum}[].{ms,cpu_ms}`), so any median or ratio in the section can be
recomputed from it rather than taken on trust:

```python
import json, statistics
r = json.load(open("scale-3arm.json"))
for c in r["cases"]:
    for arm in ("off", "on", "sum"):
        vals = [rd["cpu_median_ms"][arm] for rd in c["rounds"]]
        print(f"{c['label']:8} {arm:4} " + " ".join(f"{v:6.0f}" for v in vals)
              + f"  median {statistics.median(vals):6.0f}")
```

## What was rejected, and why

- **The `--results` flag on this driver was dead** — it accepted the argument and wrote nothing.
  Removed rather than left looking like it archived something. There are therefore no per-arm
  result files here: the arms are compared in-process by `check_arms` on every case, and a
  disagreement stops the run before any record is written.
- **`TQL EXPLAIN` was tried first and is unusable for this.** In standalone it reports only
  `CooperativeExec -> MergeScanExec` and never expands the remote plan, so it shows neither the
  aggregate nor the fused node. `partial-plan-C.txt` comes from `TQL ANALYZE VERBOSE FORMAT
  JSON`, where an operator's configuration (`mode=...`, `partial=sum`) is in `param`, not in
  `name`.
- **An earlier run's round 1 is not quoted here.** That pass used a harness with two defects
  (see below) and is superseded by `scale-3arm.json`.

## The two harness defects this run exposed

Both were fixed before the archived run, and both would have reported a correct result as a
failure:

1. **Fusion was conflated with pushdown.** The `raw` case (`rate(...)` with no aggregation) does
   fuse, and has no aggregate to absorb, so it cannot log a partial sum. The harness demanded
   one anyway.
2. **The comparator demanded a row order the plan does not provide.** `raw` plans as
   `FilterExec -> FusedSeriesExec -> SeriesScanExec`, no `Sort`, twenty partitions merged, so
   neither arm's cross-series order is guaranteed. The arms were in fact identical: equal as
   multisets, and all 400 series with identical `(timestamp, value)` sequences. `compare_pair`
   now compares per series and reports the row order without requiring it, and
   `fused_series_bench.py selftest` asserts both halves — reordering tolerated, and a changed
   value, timestamp, dropped sample, dropped series, changed label or schema still caught.

## The mutation behind the slot-key claim

Keying a slot by the row offset instead of `start + offset`, in both the dense and the sparse
branch of `PartialSumStream::ingest`, fails exactly three tests and no others:

```
FAIL partial_sum_emits_absolute_grid_indices_and_nothing_for_a_missing_partition
FAIL partial_sum_keeps_the_duplicate_label_set_rules
FAIL partial_sum_keeps_nan_and_infinities
  left:  [(x, 300000), (x, 360000)]
  right: [(x, 300000), (x, 480000), (x, 540000)]
```

The source was restored afterwards and `git diff` on that file is empty.

## What this pass does not establish

The magnitude on `G4`/`G1`. The gate reports **not quiet** (worst two-sided deviation 60%,
`G4`'s `on/off`, round 2 above the median), and the instability is in the `on` arm, not the
`sum` arm: `on` spreads 50% on `G4` and 39% on `G1`, while `sum` spreads 14% on both. Those two
ratios are quoted with their per-round values and no single number is claimed from them.
