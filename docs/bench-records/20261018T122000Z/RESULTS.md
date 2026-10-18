# 2026-10-18T12:20Z — what the intermediate rows cost, and what the gain tracks

Binary: `a2d345cc76c` (`feat/narthex-integrated-chain`). One binary for every arm; the only
variable is `experimental_native_promql`.

Fixture: `~/.cache/fused-scale/data-seed`, 400 series x 2 880 samples at 15 s, table `vmbench`,
host values `h0000`..`h0399`.

## `scale.json` — section 4.9

`scripts/fused_series_scale.py --rounds 3 --warmup 2 --iterations 9`. Seven cases, off and on arm
per round, arm order rotated, CPU from `/proc/<pid>/stat`. `cases[].rounds[].off/on` hold every
per-iteration sample, so the medians and the per-round ratios can be recomputed. The commands in
this file run from this directory:

```bash
python3 - <<'PY'
import json, statistics
d = json.load(open("scale.json"))
for r in d["cases"]:
    print(r["label"], r["rows"], r["cpu_per_round"], r["cpu_ratio_spread_over_median"])
print(d["pass"])
PY
```

`cpu_ratio_spread_over_median` is the key this run's script wrote (`max(ratio) / median - 1`);
the current script writes `cpu_ratio_deviation_over_median`, so the archived JSON keeps the old
key and is left as recorded. The gate is now two-sided, and recomputing it from the per-round
ratios stored here changes which cases pass:

```bash
python3 - <<'EOF'
import json, statistics
d = json.load(open("scale.json"))
for c in d["cases"]:
    rs = c["cpu_per_round"]
    med = statistics.median(rs)
    dev = max(abs(r - med) / med for r in rs)
    worst = rs.index(min(rs, key=lambda r: -abs(r - med))) + 1
    print(f'{c["label"]:8} {[round(r, 3) for r in rs]} median {med:.3f} dev {100*dev:5.1f}%'
          f' worst round {worst}')
EOF
```

| case | per-round ratios | median | two-sided dev | worst round |
| --- | --- | ---: | ---: | ---: |
| `G4` | 0.714 / 0.682 / 0.567 | 0.682 | **16.9%** | 3 |
| `G400` | 0.970 / 0.974 / 0.966 | 0.970 | 0.5% | 3 |
| `G1` | 0.682 / 0.657 / 0.727 | 0.682 | 6.7% | 3 |
| `n100` | 0.909 / 0.818 / 0.818 | 0.818 | 11.1% | 1 |
| `n1` | 1.000 / 1.000 / 1.000 | 1.000 | 0.0% | 1 |
| `raw` | 0.967 / 0.967 / 0.952 | 0.967 | 1.6% | 3 |
| `control` | 0.829 / 1.127 / 1.023 | 1.023 | 19.0% | 1 |

The one-sided gate recorded this pass as `quiet: false` with `n100` worst at 11.1%, while `G4`
showed 4.8% and looked quiet. It was not: `G4`'s third round reads 0.567 against a 0.682 median,
a 17% excursion *below* the median, which `max(ratio) / median - 1` cannot see. Counting both
directions makes `G4` the worst case in the pass, ahead of `n100`, and section 4.9 quotes `G4`'s
ratio with its 0.567-0.714 range.

## `plan-delta.json` — section 4.8, the first and rejected attempt

**One** `TQL ANALYZE` per step. Section 4.8 quotes this only to say that it was not reportable:
it gave 22.1 / 17.4 ns per added row for the kernel and the aggregate, and replication moved
those to 20.1 / 13.5. Kept so the correction can be checked rather than taken on trust.

## `marginal-repeat.json` and `marginal-cpu.json` — section 4.8's numbers

The same statement at a 1 s and a 60 s step, repeated. `marginal-repeat.json` is 6
replications of the pair, alternating order, timers only: kernel larger than the aggregate in
6 of 6, at a median 17.9 against 12.3 ns per added row.

`marginal-cpu.json` is 4 further replications that take `/proc` CPU per query (40 queries per
step) **and** the timers in the same server and the same pass, so the operators' share is a
ratio of two quantities measured together. Medians over those 4, which are what section 4.8
quotes:

| quantity | median | range |
| --- | ---: | ---: |
| kernel ns/added row | 20.06 | 18.20-22.62 |
| aggregate partial ns/added row | 13.48 | 12.52-15.54 |
| five operators, ms | 99.3 | — |
| CPU delta, ms | 132.3 | 113.5-141.5 |
| five as a share of the delta | 75% | 68-83% per replication |

Every replication logged exactly one fused selection per query at both steps, so neither step
measured the ordinary plan.

## `step-sweep.json` — the CPU the grid costs

One server, three grid steps, CPU per query from `/proc` and the operators' timers from the same
process. This is the measurement that shows 81% of the query's CPU disappears when the grid
coarsens 60x, which the sampler's shares cannot cover.

## `eval-vs-series.json` — the kernel scales with series, not with a constant

`fused_evaluate_time` at 400 / 100 / 1 series on the same grid and window: 83.65 / 10.68 / 0.27
ms.

## `g-vs-w.json` — the window width, not the grid, is nearly free

`rate[5m]` against `rate[1h]` on the same 400 series and the same 120 min at a 1 s step: 211.2
against 221.3 ms CPU per query, 100 queries each. The 1 h window costs 10.2 ms more.

## `window-check.json` — the window width, measured inside one call

`rate[5m]` against `rate[1h]` on the same 400 series, grid and span, alternated four times in
one server, `TQL ANALYZE` each time. The first 1 h reading is a 2.5x outlier across every
operator; after it the two ranges overlap. The partial aggregate cannot see the window, and its
ratio to the evaluator stays at 0.62-0.70 through the outlier, with the evaluator correlating
+0.997 with it. That is drift, not a window effect.

## What is *not* here

- The raw pprof text of runs `W-on1`..`W-on3` (4-58 MB each) is not committed. Its two derived
  numbers that section 4.8 quotes — 5 621 samples over 215.0 s of CPU for `W-on3`, and the
  per-owner shares — are reproduced by `scripts/fused_profile_summarize.py`; the input is
  regenerated by `scripts/fused_series_profile.py --seconds 60`.
- Correctness results for the scales (each ~300 KB) are not committed; the row counts are in
  `scale.json` per arm and the script fails if the arms disagree.
