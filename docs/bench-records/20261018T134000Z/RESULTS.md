# 2026-10-18T13:40Z — the batch regions of the evaluator, measured

Binary `4293141770c` (`feat/narthex-integrated-chain`), the nightly profile, one standalone server
per case, `TQL ANALYZE VERBOSE FORMAT JSON` nine times per case. Every figure comes from one
ANALYZE record, so a ratio of two of them is a ratio of two measurements taken in the same
execution — the thing the scale and profile rounds could not offer, because their two terms came
from different runs.

Cases: `W` = `sum by (instance) (rate(vmbench[1h]))`, `G` = the same at `[5m]`, both 120 min at a
1 s step over 400 series. Same grid, same 2 868 400 intermediate rows; only the window differs, so
`W` holds 240 samples per window and `G` 20, a factor of 12.

`regions-W.txt` and `regions-G.txt` are the probe's own output.

## The three regions

| region | `W` median | of evaluator | `G` median | of evaluator |
| --- | ---: | ---: | ---: | ---: |
| `fused_evaluate_time` (whole `push`) | 60.69 ms | 100% | 60.50 ms | 100% |
| `fused_window_prep_time` | 28.05 ms | 46.2% | 27.55 ms | 45.5% |
| `fused_kernel_time` | 27.40 ms | 45.2% | 27.71 ms | 45.8% |
| `fused_columns_time` | 3.83 ms | 6.3% | 3.95 ms | 6.5% |
| three regions together | 59.30 ms | **97.7%** | 59.14 ms | **97.8%** |

Per `(series, grid point)`, i.e. over 400 x 7 171 = 2 868 400 points:

| | `W` | `G` |
| --- | ---: | ---: |
| whole `push` | 21.16 ns | 21.09 ns |
| window preparation | 9.78 ns | 9.60 ns |
| kernel | 9.55 ns | 9.66 ns |
| columns | 1.34 ns | 1.38 ns |

**The three regions cover 97.7-97.8% of the counter they subdivide** (median of the nine
per-record sums over the median `push`; every individual record is within 97.5-97.8%). That is
the check that the instrumentation is complete: the three regions cover essentially the whole
counter. The remaining 2.2-2.3% is **not** attributed. It contains `ingest`, the loop's own
bookkeeping and the flush's `clear_points`, and nothing here separates them; the profile's
0.5-0.6% for `ingest` is a differently-scoped number from a different instrument, so it is a
consistency hint and not an identification of the residual.

## prep/kernel is one, not 50/50-by-coincidence

Per record, not per median, because a ratio of two medians would hide which side moved:

| case | prep/kernel per record | median | range | prep above | distance from parity |
| --- | --- | ---: | ---: | ---: | ---: |
| `W` | 0.959 / 1.027 / 1.028 / 0.947 / 1.033 / 0.968 / 0.995 / 1.062 / 0.961 | 0.995 | 0.947-1.062 | 4/9 | median 3.3%, worst 6.2% |
| `G` | 1.075 / 0.981 / 1.109 / 0.997 / 1.082 / 0.976 / 1.004 / 0.977 / 0.975 | 0.997 | 0.975-1.109 | 4/9 | median 2.4%, worst 10.9% |

The absolute times drift 25% between records of the same case (evaluate 51.13-69.85 ms on the
first `W` run), and the ratio does not: its worst excursion is 10.9% against a 25% drift. So the
ratio is the quantity worth quoting and the absolute numbers are not comparable across runs.

**The profile's 50/50 was not an artifact of inlining.** The timers put the two halves at 45-46%
each, in two shapes, in 18 records, with prep above kernel in 8 of 18 — no side wins.

## What the window invariance says

`W` and `G` differ in samples per window by 12x and cost the same in every region: kernel 27.40
against 27.71 ms, preparation 28.05 against 27.55 ms, whole `push` 60.69 against 60.50 ms. So
neither region scales with the window's sample count. Both are **fixed cost per (series, grid
point)**: the kernel's per-window work is O(1) arithmetic over `first_t`/`last_t`/`eval_ts` plus
the reset fold, and preparation's scan is O(1) amortized per point by construction.

That is why the earlier round's `rate[1h]` = `rate[5m]` result (1.003x) holds. Averaged over this
fixture the evaluator costs ~21 ns per point whatever the window is; that is an amortized figure
for this shape only — 400 series of 15 s samples, 7 171 grid points at a 1 s step, wall-clock
timers on this machine — and not a rate that transfers to another series count, step or sample
density. 400 series x 7 171 points x 21 ns = 60 ms, which is the 33-38% of a 173 ms query the
profile attributes here.

## Consequence for the next change

Asking "window preparation or kernel" has no answer: they are the same size, to within the
measurement's own resolution. Picking one would be arbitrary, which is what the previous revision
did when it read a sampling split as an ordering.

What both halves share, and what the invariance points at, is the per-point machinery rather than
either half's arithmetic: preparation appends, for each grid point, a 16-byte window span and a
16-byte reset range **per field**, plus an 8-byte grid index and an 8-byte evaluation timestamp
shared across fields, all into four parallel `Vec`s that the kernel then reads back. On this
fixture — one value field — that is 48 bytes per point, so 2 868 400 points give **137.7 MB of
cumulative logical writes per query**. That is the sum of what is pushed into the four vectors, not
resident memory: they reallocate as they grow and the allocator's footprint is a different number
this file does not measure.

The actual `rate` result payload is **22.95 MB** — 2 868 400 `f64` at 8 bytes — so the per-point
window metadata is six times the size of the values it produces.

Two of the four are derivable, because a batch's grid indices are contiguous and ascending by the
flushing convention (the evaluator flushes before it skips grid points and before the tail
`advance`): `indices` is `first_index + k` and `eval_ts[k]` is `grid[first_index + k]`. Removing
both deletes 16 of those 48 bytes — **a third on a single-field series, and less as fields grow**,
since the span and the reset range scale with the field count while these two do not. The remaining
two thirds carry the window boundaries themselves.

That the deleted third is the *expensive* third is exactly what arithmetic cannot say. It says
nothing about how the write cost distributes across the four vectors, so the change is measured,
and the measurement is reported even if it shows nothing: fewer bytes written is not a speed
claim.

## Instrument overhead

15 flush calls per series (14 full batches of 512 plus a tail of 3), six clock reads per flush
(preparation start/stop, kernel start/stop, columns start/stop): 90 reads per series, one per 80
grid points. At 20 ns a read that is 0.72 ms per query, 0.42% of 173 ms — below this machine's
25-35% drift, so it is an estimate and not a measurement. An earlier note in the commit that added
the timers said "38 reads per series"; that counted only the preparation pair and is corrected
here.

## Reproduce

```bash
# from the repository root; needs the nightly build and one free data home
BIN=/mnt/nvme_rust/rust-targets/narthex-min-chain/fused-bench-bin/greptime-4293141770c
DATA=$HOME/.cache/fused-scale/data-seed
python3 scripts/fused_series_regions.py "$BIN" "$DATA" W 9
python3 scripts/fused_series_regions.py "$BIN" "$DATA" G 9
```

Each run starts one server, warms the plan twice, then issues nine `TQL ANALYZE VERBOSE` and
prints the fused node's four timers per record plus the summary above.
