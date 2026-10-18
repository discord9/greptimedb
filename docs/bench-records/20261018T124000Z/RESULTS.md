# 2026-10-18T12:40Z — three data shapes the main fixture cannot express

Binary `a2d345cc76c`, one binary for both arms, native flag as the only variable.
`scripts/fused_series_shapes.py --runs 2 --warmup 2 --iterations 7`.

Three tables were seeded into the same data home as `vmbench`, 40 series x 1 440 samples at
15 s (57 600 rows) unless noted:

- `vmreset` — a counter that rises for 48 samples then drops: 24 resets per series, so the
  counter-reset index of `src/promql/src/range_eval.rs` actually holds entries. The main
  fixture is strictly increasing and never exercises it.
- `vgaps` — a plain rising counter with rows missing: `host % 3 == 0` drops every fifth sample,
  `host % 3 == 1` drops one contiguous 30 min block, `host % 3 == 2` keeps everything.
  **52 008** rows, not 57 600.
- `vfrag` — byte-identical values and timestamps to `vmreset`, written in 10-sample statements:
  5 760 statements instead of 40, so the scan sees many small chunks. Measured 5.5 s to seed.

## What this establishes

Correctness, in `shapes.json`: every case returned the same rows on both arms
(`check_arms` → `compare_pair`: schema, label tuples, timestamps, row count, values within
`1e-9 * max(1, |v|)`), the fused arm logged exactly one selection per execution and the
unfused arm none, and `control` (`max_over_time`) logged none on either arm.

The `vfrag`/`vmreset` pair is the sharp one, and it was checked separately because the script
only compares arms. Both hold the same numbers, so both must return the same answer:

| arm | reset rows | frag rows | fused hits (reset/frag) | differing cells | largest absolute difference |
| --- | ---: | ---: | --- | ---: | ---: |
| off | 28 684 | 28 684 | 0 / 0 | 0 | 1.4e-14 |
| on | 28 684 | 28 684 | 1 / 1 | **0** | **0.0** |

The ordinary path differs from itself in the last bits, because it accumulates in a different
order per query; the fused path reproduces itself exactly. A state-loss bug at a chunk or batch
boundary would show up here as a non-zero difference on the fused arm.

## What this does *not* establish

**Timings from this run are not usable and are not quoted.** At 40 series each query costs
70-160 ms of CPU on a 10 ms tick, and the two rounds moved in opposite directions
(`reset5m` 1.000 then 1.429; `gaps5m` 0.889 then 1.333). The `control` aggregate across the run's
timed iterations is exactly 1.000, which says the accounting works and the *queries* are too
small to resolve on this machine, not that a ratio is meaningful. Shape timings would need the
400-series fixture at each shape, which this does not seed.

`shapes.json` keeps every per-iteration `ms`, `cpu_ms`, `rows`, `bytes`, `hits` and `load1`.
