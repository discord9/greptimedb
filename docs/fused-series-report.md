# FusedSeriesExec: implementation report and measured performance

Status of the `feat/narthex-integrated-chain` branch after removing the
per-grid-point counter-reset rescan from the native range evaluator. This
document records what was built, what was measured, and — importantly — what the
measurements do **not** show.

**Headline: the algorithm regression is gone, but that has not made the fused
path faster than the ordinary plan. `rate`/`increase` evaluation no longer
depends on the window width — a 1 h window costs the same as a 5 min one (1.003x,
where the previous code paid 3.38x) — and at the compute level `rate[1h]` is
4.24x faster. End to end that removed the wide-window regression (0.43x → 0.90x)
and leaves the fused path 0.88x-0.93x at fine steps with narrow windows, with
`max_over_time` as a 1% noise floor. The remaining gap is not the reset index and
is not the raw series scan, as an earlier revision of this report claimed: on
identical input the fused evaluator costs 2.83x the ordinary plan's range-function
work, which accounts for the whole end-to-end CPU gap. Splitting that ratio in a
single session puts the kernel at 0.89x the ordinary plan and the per-point
bookkeeping at 1.95x: the regression is entirely the bookkeeping that comes from
calling a scalar kernel once per grid point, where the ordinary plan assembles
every window in one columnar pass and vectorizes the kernel over the batch.
See sections 4.4 and 4.5. `experimental_native_promql` stays off.**

**This round takes that bookkeeping apart for `rate` and `increase`.** Those two
functions now take a batch of windows per call, with the function chosen outside the
loop, the window edges advanced as monotone cursors instead of searched per point, and
the results appended a column at a time. On a real server the fused query's CPU drops to
0.778 / 0.700 / 0.688 of the previous commit on `S` / `G` / `W` and stays at 1.000 on the
coarse-step `C`, which is the shape that has few grid points and so has nothing to
amortize; all 15 paired rounds returned identical results and both arms logged the
expected fused selections. Wall clock moves far less than CPU (4-8% against 22-31%),
because the query is not purely CPU-bound. Every other range function still runs one
boxed call per grid point and gained nothing. See sections 1.4 and 4.6.

Everything below was produced in this worktree on a 20-core machine with the
`nightly` Cargo profile (`lto = "thin"`, `strip = "debuginfo"`).

## 1. What was built

### 1.1 `FusedSeriesExec` — the node

`src/promql/src/extension_plan/fused_series.rs`

One physical node that chains **one** range evaluation and **0..N** `map_math`
kernels over typed `Series` values, so those two stages never cross a
`RecordBatch` boundary. It does not re-implement any arithmetic:

| Stage | Reused from | Owner |
| --- | --- | --- |
| range function (`rate`, `increase`, `delta`, …) | `RangeEvaluator` | #9673 / #9674 (merged) |
| float map kernels (`abs`, `sqrt`, `ceil`, `floor`, `round`) | `map_math` / `MathOp` | #9672 |
| primary-key label decoding | `materialize_raw_labels` | #9650 |

What this node adds on top of those is:

- `series_to_record_batch`: converting an evaluated `Series` straight into an
  output `RecordBatch` (this was the real gap — nothing in the tree did it).
- Prometheus' range-query duplicate-label-set rule: `rate` and the map kernels
  drop `__name__`, so two distinct raw series can reach the same final label
  set. Overlapping grid points are an error; disjoint ones stay legal and are
  emitted as separate rows rather than being merged or dropped.
- Row semantics identical to the ordinary plan: a grid point where every value
  column is NULL contributes no row, matching the `<value> IS NOT NULL` filter
  the ordinary plan always places above its projection.
- Output coalescing to `MAX_ROWS_PER_BATCH = 4_096` rows per batch per stream
  (`src/promql/src/extension_plan/fused_series.rs`), so a query over *n* series
  does not hand downstream *n* tiny batches.

Construction-time rejections (never a silent fallback): zero partitions, an
evaluation-grid input schema, `Interleaved` ordering, an illegal window or
function, an illegal frame schema, an illegal output schema.

### 1.2 Planner wiring — deliberate throwaway

`src/query/src/datafusion/native_promql/fused.rs`

> **Read this before extending it.** The current PromQL planner (PromQL AST →
> DataFusion `LogicalPlan` → physical plan) is temporary. The intended direction
> is to hand PromQL down directly, without DataFusion as an intermediate
> representation. When that lands, this whole module is deleted.

So the matcher is intentionally minimal: it locates the single
`Projection → [Filter] → Projection → RangeManipulate` subtree anywhere in the
plan (including under an `Aggregate`), identifies the corresponding physical
node **by schema and content** rather than by a logical child-index path, and
swaps it in place with `transform_up`. Any shape it cannot prove returns
`Ok(None)` and the ordinary plan runs. The node and its output contract are what
survive a planner replacement; the matcher is not.

It runs behind the existing `experimental_native_promql` flag
(`src/query/src/datafusion.rs`), and logs
`Fused series selection succeeded: <function> over <table>` at debug level when
it takes effect, so an operator can tell a fused run from an ordinary one
without reading plans.

### 1.3 The counter-reset index

`src/promql/src/range_eval.rs`, `src/promql/src/range_eval/function.rs`

**The defect.** `evaluate_row` hands the kernel the window as a slice pair — a
shared backing array with an offset and a length, *not* a copy — and `rate` then
walked that whole slice looking for drops:

```rust
let len = timestamps.partition_point(|&timestamp| timestamp <= window_end);
kernel(FloatWindow { timestamps: &timestamps[..len], values: ..., .. })
```

Sample *ingestion* was already linear (a `head` cursor advances), but kernel
*evaluation* was `O(grid points x samples per window)`. On the measured fixture,
`rate[1h]` at a 1 s step over 2 h visits 7 201 x 241 x 400 = 694 M samples.

**The fix.** Each field records the position of every sample that dropped below
its predecessor, at ingest time, when the comparison is free — the new sample is
already next to the previous one. A window then asks for the resets it contains
by binary search over that per-field index, and the kernel adds back the same
predecessors in the same order:

```rust
result = counter_resets
    .iter()
    .fold(result, |result, reset| result + reset.previous_value);
```

Properties that keep this safe rather than merely faster:

- **Same arithmetic, same order.** `last - first`, then each reset's preceding
  value in sample order, then the unchanged extrapolation. No prefix sums, no
  subtraction of partial sums.
- **Positions are absolute sequence numbers** (`base + index`), so dropping the
  samples behind the next window renumbers nothing and a reset keeps its
  position across chunk boundaries and drains.
- **Left-open window membership.** A reset at position `j` counts exactly when
  `L < j < R`, so a reset whose predecessor sits on the window's left edge — the
  sample the window excludes — does not count.
- **The index is released with the samples.** A reset is dropped together with
  the predecessor it reads, so the index cannot retain history no window can
  reach.
- **Per field, per series.** `WindowKernel` is shared across fields, so the index
  lives on `FieldSamples`, never in the kernel closure; `finish_series` clears it
  with the rest of the series' state.
- **Only `rate` and `increase` maintain it** (`RangeFunction::indexes_counter_resets`).
  Every other function reads an empty slice, so a function that never corrects
  counters pays nothing for the machinery: `max_over_time` measures within 0.4%
  of the previous code (section 4.2).

**This is not strict `O(1)` per window.** A window that contains resets still
walks them in order; the improvement is that it no longer walks the samples that
do *not* reset, which on real counters is the overwhelming majority.

### 1.4 The batch path — windows, kernel and output

`src/promql/src/range_eval.rs`, `src/promql/src/range_eval/function.rs`,
`src/promql/src/grid_series.rs`

The reset index removed the per-window scan of the samples but left the shape: one
boxed call per grid point per field, each rebuilding a `FloatWindow` and appending
one `Vec<Option<f64>>` row. At the benchmark shape that was 36.9 ns per point of
run-time bookkeeping against 16.8 ns of arithmetic, so the bookkeeping was most of
the path.

**The kernel now takes a batch.** The function is chosen once, outside the loop,
and the batch body is a plain loop over windows:

```rust
pub(crate) type BatchKernel =
    Box<dyn FnMut(&WindowBatch<'_>, &mut Vec<Option<f64>>) + Send>;

pub(crate) struct WindowBatch<'a> {
    timestamps: &'a [i64],
    values: &'a [f64],
    resets: &'a [CounterReset],
    windows: &'a [WindowSpan],          // WindowSpan { start, end }
    window_resets: &'a [Range<usize>],
    eval_ts: &'a [i64],
    range_ms: i64,
}
```

`WindowSpan` is an index pair into the retained arrays, not a slice: consecutive
windows overlap almost entirely, so a window is named rather than materialized.
That is possible because a counter function reads only a window's endpoints, its
sample count and its resets — which is why the batch input does not need each
window's samples to be contiguous.

`RangeFunction::batch_kernel()` returns `Some` for `rate` (`<true, true>`) and
`increase` (`<true, false>`) and `None` for everything else, which keeps every
other function on the per-point `WindowKernel`. The two forms share the arithmetic:
`extrapolated_rate_batch` reproduces `extrapolated_rate`'s operations in the same
order, including the saturating `i64` subtraction, so the batch form is not a
second implementation of the formula.

**Boundaries advance instead of being searched.** Consecutive grid points have
monotone window edges, so one forward pass per batch replaces the per-point
searches: the left edge is the field's `head`, the right edge a cursor
(`FieldSamples::right`) carried from the previous point, and the reset range comes
from two more cursors (`reset_start`, `reset_end`) that move only when the window
does. Nothing is re-searched per point; the retained-sample positions stay
continuous across chunks because `advance` shifts all four cursors by the number of
samples it drops.

**Output is appended by column.** `GridSeriesBuilder::push_columns(index, len,
&[&[Option<f64>]])` does the schema, length and grid-order checks once per batch and
then writes each field's column, a dense column through `extend` plus
`append_n_non_nulls`. The per-point `row` vector exists only on the compatibility
path now.

The batch is 512 grid points (`EVALUATION_BATCH`). It is flushed at that boundary,
at the end of the series, and — because a batch appends contiguous rows — whenever
the grid skips points ahead of the earliest retained sample. The real call path is
`FusedSeriesStream::poll_evaluated` → `RangeEvaluator::push` → `evaluate` → the
`batched` branch → `flush_batch` → `BatchKernel` → `push_columns`.

**On the output side** (`src/promql/src/extension_plan/fused_series.rs`), one
`occupancy` scan now yields both the valid-position runs that `claim_label_set`
needs for collision detection and the take-indices that `series_to_record_batch`
needs for construction; previously each scanned separately. A series whose only
field is a null-free dense float array is known dense from the array's null count
and takes a path with no per-row work at all. Tag columns are built at the kept
length, so a `take` no longer rebuilds a constant column.

## 2. Correctness

Five independent layers, all currently green.

1. **Node unit tests** — `src/promql/src/extension_plan/fused_series/tests.rs`
2. **Evaluator differential tests** — `src/promql/src/range_eval/tests.rs`. The
   reference side of every random test computes a window's resets by *scanning*
   it, and the probe kernel hashes them, so all 500 random grids × 3 series × 3
   fields also check the index. Verified by mutation: making `resets_in` return
   an empty slice fails `evaluator_matches_brute_force_reference`,
   `range_functions_match_prometheus` and `subquery_series_match_references`.
3. **Targeted state tests** — the reset that is the first sample of a later
   chunk; the reset whose predecessor lands exactly on a window's left edge and
   must not count; a reset entering a window whole; two fields with different
   NULL/reset layouts in the same window; the same evaluator serving a second
   series with no state leaking; and an index that stays bounded under
   continuous draining.
4. **Real storage scan** — `tests-integration/.../nested_merge_scan_capability_test.rs`,
   `test_fused_series_exec_over_real_storage_scan`.
5. **Real PromQL query through the planner** — see section 3.

`cargo nextest run -p promql` → 457 passed.
`cargo nextest run -p query` → 885 passed, 2 skipped.
`cargo clippy -p promql -p query --all-targets -- -D warnings` → clean.

### Window semantics

The fused path follows Prometheus' `(E - range, E]` — left-open, right-closed —
as documented and implemented in `src/promql/src/range_eval.rs`. The ordinary
fold's `scan_ranges` (`src/promql/src/extension_plan/range_manipulate.rs`) uses
the same `<= window_start` exclusion, so the two agree. Where the two
implementations differ is the last bits of a float sum, because they accumulate
in a different order; the comparison in section 4.1 confirms row identity under a
relative tolerance of 1e-9.

## 3. Bugs found by measuring, not by testing

These compiled, passed their unit tests, and silently disabled or masked the
behaviour under test. They are recorded because each one made an earlier
benchmark number meaningless.

1. **`sum by (...)` never fused.** The matcher walked a logical child-index path
   to find the physical node to swap. DataFusion plans one logical `Aggregate` as
   `AggregateExec(Partial) → RepartitionExec → AggregateExec(Final)`, so the
   lockstep walk desynchronised and returned `None`. Fixed by identifying the
   physical node by schema and content.
2. **A tag subset was rejected.** The builder required the fused output to carry
   *every* table tag. A `sum by (one_label)` projection is pruned to the labels
   the aggregate groups by, so on any table with more than one tag — that is,
   every real table — the gate rejected the most common PromQL query shape. The
   single-tag test table could not show it. Fixed by requiring one time column
   and one value column and letting the projection decide the tag columns.
3. **The wiring test could not see either bug.** It used a one-tag table, one
   partition, and compared the fused arm against the ordinary arm with a `0.25`
   tolerance. A tag-subset projection had nothing to prune, and the tolerance
   would have hidden a wrong value. It is now a four-tag table (`host`,
   `instance`, `job`, `l1`) with two storage partitions, one series per instance
   per partition, and `sum by (instance)`; the expected sums (3 and 7 per second)
   are computed by hand and cannot be reached by dropping a partition, losing the
   tag subset, or mixing the instances. Tolerance is now `1e-9` relative.
4. **The benchmark driver asked for the wrong time range.** `TQL EVAL (start,
   end, step)` takes seconds, while `INSERT` literals and returned timestamps are
   milliseconds. The first driver run passed milliseconds, so every query covered
   a range ~50 000 years in the future: zero rows, valid schema, `SELECT
   count(*)` still healthy. Caught only because the never-fused control took
   363 ms for zero rows.

## 4. Performance

### 4.1 The measurement setup

The fixture is one mito table, seeded once and reused by every arm:

| Property | Value |
| --- | --- |
| Tags (primary key, in order) | `host`, `instance`, `job`, `l1` |
| Field / time index | `metric_value` DOUBLE / `ts` TIMESTAMP(3) |
| Series | 400 (`host=h0..h399`, `instance=i0..i3`, `job=j`, `l1=l`) |
| Samples per series | 2 880 at 15 s |
| Span | `1700000015000`..`1700043200000` ms = 43 185 s |
| Rows | 1 152 000 |
| Counter resets / NULLs | **0 / 0** (values rise `15 × (host+1)` per sample) |
| Storage partitions | 20 |

A strictly rising value is deliberate: `rate` over it needs **no** counter
correction, so any cost it shows is the fixed per-window work plus whatever the
kernel spends looking for resets that are not there. That is exactly the cost
the reset-index round removed, and it means a regression cannot be blamed on reset
arithmetic. The index's correctness on real resets is covered by section 2, where
the expected values can be computed by hand.

Three arms, all over the same data directory, sequentially:

| Arm | Commit | Native flag | Range evaluation |
| --- | --- | --- | --- |
| **L** | `f387830a020` | off | ordinary materialising plan |
| **N** | `f387830a020` | on | fused, per-window rescan |
| **F** | `f387830a020` + this change | on | fused, per-field reset index |

Cases, all `sum by (instance) (<fn>(vmbench[window]))`:

| Name | Span | Step | Window | Grid points | Samples per window | Sample visits |
| --- | --- | --- | --- | ---: | ---: | ---: |
| `S` | 30 min | 1 s | 5 min | 1 801 | 21 | 15 M |
| `G` | 2 h | 1 s | 5 min | 7 201 | 21 | 61 M |
| `W` | 2 h | 1 s | 1 h | 7 201 | 241 | 694 M |
| `C` | 2 h | 1 min | 5 min | 121 | 21 | 1 M |
| `control` | 2 h | 1 s | 5 min | 7 201 | 21 | — (`max_over_time`, never fuses) |

`S` and `G` differ only in span, so the pair separates "more grid points" from
"more samples per window"; `G` and `W` differ only in window. `C` is the shape
the original query actually has.

Arms are interleaved **per case** — L, N, F, then N, F, L, then F, L, N — not run
as three whole sweeps. A whole-sweep order was tried first and produced a 3.4x
swing on `control`, which never fuses and therefore cannot differ between arms at
all. That swing was machine drift, and interleaving per case makes drift land on
every arm instead of one. The machine is shared, so this is not an idle host: the
host load average was 9.6-18.6 on 20 cores during the measured runs below (and
4.1-6.7 during the aborted whole-sweep attempt). That is why `control` carries
weight — it fixes the noise floor at 1% rather than leaving it to be assumed.
2 warmup iterations and 9 timed iterations per case per arm per round, 3 rounds,
so 27 timed samples per arm per case. Every individual timing, with the load
average beside it, is in the JSON records.

### 4.2 Compute: the evaluator alone

**These are the reset-index round's numbers.** The batch path of section 1.4 changed the
evaluator again; the `native_range_eval` group has not been re-run since, so the `after`
column below is the state before batching, not the current one. The group that *was* re-run
is `ingest` (section 4.5), and the end-to-end effect of batching is in section 4.6.

`src/promql/benches/bench_range_fn.rs`, group `native_range_eval`. One series of
2 880 samples at 15 s on a 7 201-point 1 s grid, one value field, evaluated by
`RangeEvaluator` directly. **Timed scope, identical in every case:**
`try_new` + one `push` + `finish`. The chunk, schema and grid are built once
before timing and borrowed by `push`, so no case pays for input construction or
adaptation inside the measurement.

Criterion, `--warm-up-time 2 --measurement-time 5`, two independent rounds. CIs
do not overlap between the two binaries in any row.

| case | values | window | before | after | speedup |
| --- | --- | --- | ---: | ---: | ---: |
| `rate` | rising | 5 min | 527.5 µs | 418.9 µs | **1.26x** |
| `rate` | rising | 1 h | 1 781.4 µs | 420.1 µs | **4.24x** |
| `rate` | resets every 240 samples | 5 min | 521.6 µs | 425.6 µs | 1.23x |
| `rate` | resets every 240 samples | 1 h | 1 781.1 µs | 431.1 µs | 4.13x |
| `max_over_time` | rising | 5 min | 348.6 µs | 349.4 µs | 1.00x |
| `max_over_time` | rising | 1 h | 2 286.9 µs | 2 268.6 µs | 1.01x |
| `max_over_time` | resets every 240 samples | 5 min | 348.9 µs | 350.4 µs | 1.00x |
| `max_over_time` | resets every 240 samples | 1 h | 2 287.1 µs | 2 265.2 µs | 1.01x |

Three things this table says:

1. **The window dependence is gone.** After the fix, `rate` over a 1 h window
   costs 1.003x what it costs over a 5 min window. Before, that ratio was 3.38x —
   close to the 11.5x ratio of samples per window, damped by the fixed
   per-window cost. This is the direct evidence that the rescan is gone, and it
   is stronger than any single speedup number, because it does not depend on
   where the fixed costs happen to sit.
2. **The narrow-window gain is real but modest** (1.26x): a 5 min window holds 21
   samples, so the rescan was never the dominant cost there.
3. **The reset work itself is small.** With resets present, the fixed algorithm
   costs 1.6-2.6% more than with a strictly rising counter, because it then walks
   the resets it reports. That is the honest price of the correction, not a
   regression.
4. **Functions that do not correct counters are untouched.** `max_over_time`
   moved from 348.6 to 349.4 µs at 5 min and from 2 286.9 to 2 268.6 µs at 1 h,
   both inside the run-to-run spread. An earlier draft read the index for every
   function and cost `max_over_time` a reproducible 4.9%; the `index_resets`
   guard in `evaluate_row` is what removed that, and this row is why the guard
   exists.

### 4.3 End to end

**This table is also from the reset-index round** (L = flag off, N = flag on with the
rescan, F = flag on with the index); section 4.6 has this round's pair.

Driver: `scripts/fused_series_bench.py` (seeding and one arm per invocation),
`scripts/fused_series_bench_crossed.sh` (the interleaved order above),
`scripts/fused_series_bench_summarize.py` (the tables below, from the raw JSON).
Every invocation checks that the flag-off arm logs zero fused selections and that
the flag-on arm logs exactly one per `rate` case and zero for `control`, so a
comparison can never silently measure the ordinary plan twice.

All 27 timed samples per arm per case; medians in ms.

| case | L (off) | N (rescan) | F (indexed) | L/N | L/F | N/F | N→F |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `S` | 18.5 | 20.3 | 19.9 | 0.91 | 0.93 | 1.02 | +2% |
| `G` | 51.2 | 61.8 | 58.1 | 0.83 | 0.88 | 1.06 | +6% |
| `W` | 48.2 | 111.8 | 53.7 | 0.43 | **0.90** | **2.08** | **+52%** |
| `C` | 25.4 | 24.3 | 23.8 | 1.05 | 1.07 | 1.02 | +2% |
| `control` | 53.6 | 53.6 | 52.8 | 1.00 | 1.00 | 1.00 | +1% |

Row counts were identical across arms in every case (7 084 / 28 684 / 28 684 /
480 / 28 744), so the timings compare the same work.

Where the ratios land:

- **`W` is the case the reset-index round was for.** The wide-window regression went from
  0.43x to 0.90x; between N and F it is a 2.08x improvement. That is the whole
  arithmetic half of the regression.
- **`S` and `G` do not improve** (0.93x and 0.88x against the ordinary plan),
  because their windows are narrow and the rescan was not what made them slow.
- **`C`, the original business shape**, is now slightly *faster* than the
  ordinary plan (1.07x) instead of level with it.
- **`control` is within 1% across all three arms** (L/N 1.00x, L/F 1.01x, N/F
  1.01x), which is the noise floor this measurement actually has. No ratio in
  this table smaller than ~1.03 should be read as an effect.

The earlier report quoted `S` at 1.38x. That number does not reproduce: `S`
measures 0.85x-0.93x in every run collected here, including the earlier
whole-sweep attempt (0.88x) that predates the reset work. The 1.38x came from a
whole-arm sequential sweep whose drift is visible as the 3.4x `control` swing in
4.1, and it is withdrawn rather than explained away. The correct reading is that
fusion has **not** been shown to beat the ordinary plan on any of these shapes
yet; what the reset-index round showed is that it no longer loses badly on the wide-window
one.

### 4.4 What the remaining gap is — an earlier claim, retracted

An earlier version of this section said the remaining gap was the raw series scan
(235 of 279 ms). **That claim is withdrawn; the table it came from does not measure
what it was used for.** It is kept here because the reason it fails is the useful part.

**Why the per-operator table cannot decide anything.** Measured in one server run, both
arms, same process:

| arm | `/proc` CPU per query | sum of `elapsed_compute` over plan nodes |
| --- | ---: | ---: |
| L | 231.5 ms | 613.5 ms |
| F | 360.0 ms | 295.0 ms |

On `L` the sum of the plan nodes is 2.6x the process's total CPU, which is impossible.
Three independent reasons, each enough on its own:

- **`elapsed_compute` includes `elapsed_await`.** `L`'s `SeriesScan`: compute 424 ms, of
  which `elapsed_await` 257 ms and `elapsed_poll` 0.96 ms. `F`'s `SeriesScanExec`:
  compute 255 ms, of which `elapsed_await` 213 ms. Nearly all of the "scan" cost is
  waiting for region I/O, not arithmetic.
- **The partitions run concurrently**, so summing per-partition time over 20 partitions
  counts the same wall-clock seconds many times.
- **`FusedSeriesExec` carried no metrics at all** — it printed `metrics=[]`. The node
  doing the dominant work of the fused plan was invisible in `ANALYZE VERBOSE`. This
  round added its metrics; section 4.5 reports what they show.

A fourth, smaller error in the same table: `F`'s `SeriesScanExec` reported
`output_rows: 424` where `L`'s `SeriesScan` reported 192 000. 424 is one partition's
last batch, not the scan's total.

### 4.5 Where the fused arm's extra CPU actually goes

Machine-wide process CPU counts, not per-operator self-reports. Two servers cannot share
a data home (raft-engine holds an exclusive lock), so the arms alternate across restarts
instead of running side by side; 2 warmup + 9 timed per arm.

| arm | wall/query | CPU/query | busy |
| --- | ---: | ---: | ---: |
| L | 51.6 / 57.2 / 60.7 ms | 240 / 260 / 260 ms | 425-465% |
| F | 72.0 / 80.6 / 94.3 ms | 370 / 390 / 390 ms | 413-514% |

`F` does about **1.5x the CPU** of `L` per query and spreads it over more threads. The
regression is real work, not latency or scheduling. Collapsing the pipeline to one
partition keeps the gap (1 partition: `L` 160 ms CPU / 143 ms wall, `F` 210 / 197;
20 partitions: `L` 250 / 59, `F` 350 / 61), so it is not a per-partition fixed cost.

**A marginal test separates per-row work from per-scan work.** `C` and `G` scan the
identical 192 000 raw samples and differ only in grid step, so 60x more output rows:

| arm | `C` (48 400 rows) | `G` (2 868 400 rows) | d CPU | per added row |
| --- | ---: | ---: | ---: | ---: |
| L | 70 ms | 250 ms | 180 ms | 64 ns |
| F | 40 ms | 330 ms | 290 ms | 103 ns |

`F` is *cheaper* than `L` on the coarse grid — it skips `ProjectionExec`,
`PromSeriesDivideExec` and `PromSeriesNormalizeExec` — and more expensive on the fine
grid. The extra cost tracks output rows, not the scan.

**The two implementations of the range function, on identical input.** One series of
2 880 samples at 15 s on the same 7 201-point 1 s grid, criterion. The scopes are
deliberately listed because comparing the wrong pair is how this section went wrong
twice:

| case | timed scope | 5 m window | 1 h window |
| --- | --- | ---: | ---: |
| `native/` | `RangeEvaluator::try_new` + `push` + `finish` | 386.3 us | 392.7 us |
| `legacy_udf_only/` | the UDF call, input prepared outside the loop | 93.1 us | 84.4 us |
| `legacy_full/` | build the sliding ranges, then call the UDF | 136.3 us | 126.2 us |

`native/` must be compared against `legacy_full/`, not `legacy_udf_only/`:
`RangeEvaluator::push` both ingests raw samples and assembles every window, which in the
ordinary plan is split between `PromRangeManipulate` and the UDF. Against the wrong
scope the ratio reads 4.1x; against the right one it is **2.83x**.

That gap accounts for the whole end-to-end regression: 400 series x (386.3 - 136.3) us =
100 ms, against the 128.5 ms of CPU `G` costs `F` over `L` (360 vs 231.5 ms), the
remainder being the emit and scan differences on that path. So the regression is in the
evaluator, not in the scan, the output or the flush.

**A caveat that applies to every number in this section.** Unchanged code drifts several
percent between criterion invocations on this host: `legacy_full` read 125.2 us in an
earlier invocation and 136.3 us in this one, and `present_over_time` read 244.4 us then
265.6 us. Ratios are therefore taken from a single invocation, and where a figure is
quoted across two, the drift is stated. An earlier revision of this section compared a
post-fix `native/` against a pre-fix `legacy_full/` across two invocations and reported
3.35x; that is withdrawn.

**Splitting the evaluator into bookkeeping and kernel.** `present_over_time`'s kernel is
`|_| Some(1.0)` — it never reads the window — so its time is the per-point bookkeeping and
nothing else. `rate` adds the extrapolation arithmetic to it:

| function | 5 m | 1 h | what it measures |
| --- | ---: | ---: | --- |
| `present_over_time` | 265.6 us | 253.4 us | bookkeeping only |
| `max_over_time` | 357.1 us | 2 321.5 us | bookkeeping + a kernel that reads the window |
| `rate` | 386.3 us | 392.7 us | bookkeeping + an O(1) kernel |

`rate` splits into **265.6 us bookkeeping (69%, 36.9 ns/point) and 120.8 us kernel (31%,
16.8 ns/point)**, and both halves are the same at 5 m and 1 h, which is what the
reset-index work predicted: neither the window assembly nor the kernel any longer depends
on the window width.

**Where the 2.83x actually goes, in one session.** The three numbers below come from a
single criterion invocation, because the same unchanged code drifts several percent
between invocations on this host and a ratio built across two of them is not trustworthy.
`present_over_time`'s kernel is `|_| Some(1.0)`, so its time is per-point bookkeeping and
nothing else.

| quantity | time | per grid point | vs `legacy_full` |
| --- | ---: | ---: | ---: |
| `legacy_full/rate/5m` | 136.30 us | 18.93 ns | 1.00x |
| `present_over_time/5m` (bookkeeping) | 265.56 us | 36.88 ns | **1.95x** |
| `native/rate/5m` | 386.34 us | 53.65 ns | **2.83x** |
| kernel = `native` - `present_over_time` | 120.78 us | 16.77 ns | **0.89x** |

So the fused evaluator splits into **bookkeeping at 1.95x the entire ordinary path** and a
**kernel that is already 11% cheaper than it**. The regression is the bookkeeping, and the
bookkeeping is the shape: two `partition_point` searches per point per field, a six-field
`FloatWindow` built per point, a `dyn` call per point, then `GridSeriesBuilder::push`.

That is the number section 1.4 was written against, and it named the work: the two searches
became monotone cursors, the per-point `dyn` call became one call per 512 points, and the
row append became a column append. The result is section 4.6's 0.700 / 0.688 CPU ratio on
`G` / `W` — from 1.95x bookkeeping to a shape that no longer dominates.

**What an earlier revision of this section claimed, and why it was wrong.** It said the
fused evaluator "with its bookkeeping reduced to zero would still cost 1.47x the ordinary
path, so no per-point tuning can cross that line". That number came from the pre-fix
kernel (183.6 us, 25.5 ns/point) compared against a `legacy_full` measured in a different
invocation. After the `i128 -> i64` fix the kernel reads 16.77 ns/point, which is *below*
`legacy_full`'s 18.93. The claim is withdrawn; the conclusion it supported — that
bookkeeping is the remaining cost — is what the table above now shows directly.

**What batching was estimated to buy, and what it then measured.** This section
originally carried this table as an estimate, made before the work was done:

| `B` | estimated | vs `legacy_full` |
| ---: | ---: | ---: |
| 1 (then) | 386.3 us | 2.83x |
| 8 | 154.0 us | 1.13x |
| 16 | 137.4 us | 1.01x |
| 32 | 129.1 us | 0.95x |
| unbounded | 120.8 us | 0.89x |

The estimate assumed that all of the bookkeeping amortizes, which only the searches can.
It has since been built (section 1.4) and measured end to end (section 4.6): the CPU of the
fused query falls to 0.778 / 0.700 / 0.688 of its parent on `S` / `G` / `W` and stays at
1.000 on the coarse-step `C`. The estimate and the measurement are not the same quantity --
evaluator-only at the benchmark shape against `legacy_full` versus a whole query against the
previous commit -- so they should not be compared row by row. What can be said is that the
predicted direction was right, that the win is real on grid-point-heavy shapes, and that on
a coarse-step shape it is zero, which is what removing per-point work should do.

**Two smaller costs, since addressed.** The fused node materializes all four tag columns
per row while `sum by (instance)` needs one, and `occupied_offsets` scanned every grid point
twice per series (`claim_label_set`, then `series_to_record_batch`). The double scan is gone
(section 1.4). The tag materialization is only partly addressed: the columns are now built
at the kept length, but the columns themselves are not trimmed, because trimming them means
changing the planner's output schema and the column references above it rather than
skipping a column the schema declares.

**Why this report described a scalar kernel called 7 201 times, and why memory was not
the reason.** That was the shape until section 1.4; `rate` and `increase` now take a batch,
and every other function still runs one call per point. The batching form already exists in
this repo: `RangeArray` (`src/promql/src/range_array.rs`)
is a dictionary array whose keys are packed `(offset, length)` pairs into one shared child
array, so 7 201 overlapping windows cost 7 201 keys rather than 7 201 windows.
`RangeManipulate` builds it once per batch and the UDF runs over the batch.

An earlier revision of this section said the fused evaluator avoids that "for memory
reasons". **That is wrong, and it conflates two separate decisions.** Memory rules out one
thing only: holding the *whole series* in a single array, i.e. O(series length) retention.
It does not rule out batching. Evaluating `B` consecutive grid points per call needs
samples covering `[T_first - range, T_last]`, which is O(range + B × step) — for `B = 32`
at a 1 s step that is 32 extra seconds of samples, about two samples at a 15 s interval.
So batching costs essentially nothing in memory, and memory is no defence of the per-point
shape.

What actually produced it is the shape of the two interfaces the evaluator was built
around:

- `WindowKernel` is `Box<dyn FnMut(FloatWindow<'_>) -> Option<f64>>` — one window in, one
  value out. Making 26 functions closures of that type is what keeps each of them short
  enough to check line by line against Prometheus, which is how their correctness was
  established.
- `GridSeriesBuilder::push(index, row)` appends a single row, and `Replay::Once` /
  `Replay::EveryPoint` replay one evaluated row at many indices.

Both are the simplest uniform thing for those goals, and neither is about memory. The
price was 36.9 ns per grid point of run-time bookkeeping, measured above; `rate` and
`increase` no longer pay it now that they take a batch, and the others still do.

**The cost that actually dominates at scale, which this report had been missing.** The
evaluator re-materializes every sample: `FieldSamples` holds growable `Vec<i64>` and
`Vec<f64>`, `push` appends each sample, and `advance` slides the window by `drain`ing the
front. Ingest is therefore O(samples in range) of copying, and it is paid for *every*
sample even though only grid points produce output. It used to be per-sample Rust work — a
bounds check, a checked add, two `Vec` pushes; bulk appending turned it into a memcpy
without removing it.

The `3-8 ns/sample` this section used to quote was an estimate, and it was wrong: the
measured rate was 15.2 ns/sample. Appending in bulk then brought it down to what copying
costs. Criterion group `ingest`, 200 000 samples, one invocation, with an untouched control:

| case | time | ns/sample |
| --- | ---: | ---: |
| `evaluator_push` (the current path) | 754.7 us | 3.77 |
| `bulk_copy` (two `memcpy`s plus a stale check) | 816.3 us | 4.08 |
| `read_only` (read every sample, copy nothing) | 87.0 us | 0.43 |

So ingest now costs a copy and nothing more, and what remains above the read-only floor is
that copy. For a series whose range holds 50 M samples, at the measured 3.77 ns/sample:

| series content in range | ingest, measured rate | point bookkeeping (12 h at 1 s) |
| --- | ---: | ---: |
| 2 880 samples (benchmark) | 0.01 ms | 1.6 ms |
| 50 M samples (800 MB) | **190 ms** | 1.6 ms |

The second row is arithmetic from the measured rate, not a measurement of a 50 M-sample
series.

The ordinary plan pays neither: the scan hands over Arrow arrays it does not copy, ranges
are index arithmetic over those arrays, and the UDF reads the shared child. So legacy is
cheaper on **both** axes — per output row and per input sample.

That is why the batching estimate above topped out at parity rather than a win: it removes
one of the two costs. **Not re-materializing the samples is still not done**, and it remains
the larger half at scale. It would mean keeping the incoming Arrow arrays and expressing
retained samples as fragments plus offsets, dropping whole fragments once no later window
reaches them — a redesign of `RangeEvaluator`'s storage, not of the kernel.

**Its ceiling has now been measured**, which is why it was not attempted in the same round.
`evaluator_push` already costs what `bulk_copy` costs (table above), so the entire saving
available is `bulk_copy - read_only`, about 3.6 ns/sample. Applied to the four measured
cases that is roughly 0.8% of `S`, 1.0% of `G`, 1.5% of `W` and 2.2% of `C` — every one of
them below the 5-9% run-to-run drift this host shows. Those percentages are arithmetic from
the measured per-sample rate and the per-case sample counts, not measurements. Against that
ceiling, the change would rewrite the `&[i64]` / `&[f64]` contract that the batch kernel and
the scalar fallback both depend on, and make a window span able to cross a fragment
boundary.

### 4.6 This round: batch windows against the previous commit

Run `records-20261018T073117Z`. **P** is `611917eb0b1`, the parent commit, evaluating one
grid point per call; **F** is `d42d14df06d`, the batch path of section 1.4. Both arms are
flag-on builds, so both fuse: this compares two kernels on the fused path, not fusion
against the ordinary plan. Crossed order per case, 3 rounds, 2 warmup + 9 timed per arm per
round, 27 timed samples per arm per case.

The two binaries are distinct: `extrapolated_rate_batch` appears 6 times in `F` and zero
times in `P`, `WindowSpan` 3 times and zero times.

| case | wall P → F (ms) | CPU P → F (ms) | wall F/P | CPU F/P | rows |
| --- | ---: | ---: | ---: | ---: | ---: |
| `S` | 20.8 → 19.1 | 90 → 70 | 0.918 | **0.778** | 7 084 |
| `G` | 57.9 → 55.5 | 300 → 210 | 0.958 | **0.700** | 28 684 |
| `W` | 58.5 → 54.1 | 320 → 220 | 0.924 | **0.688** | 28 684 |
| `C` | 25.6 → 25.7 | 50 → 50 | 1.003 | **1.000** | 480 |
| `control` | 53.8 → 56.0 | 340 → 330 | 1.040 | 0.971 | 28 744 |

Medians over all 27 timed samples per arm. `control` is `max_over_time`, which never fuses
and which this change does not touch; its 0.971 CPU / 1.040 wall is the drift band, and the
`S`/`G`/`W` CPU ratios sit well below it.

**Correctness.** All 15 paired rounds compared identical: schema column names and types,
every row's full label tuple, every timestamp, the row count, then every value. No round
differed.

**Fusion.** Both arms log exactly one fused selection per query on `S`/`G`/`W`/`C` and zero
on `control`, as required, so the comparison never silently measured the ordinary plan.

**`C` is the negative control this round needed.** It is the same 120-minute query as `G`
and `W` at a 1-minute step: 120 grid points per series instead of 7 171, so 48 000 kernel
evaluations instead of 2 868 400, against the same ~200 000 ingested samples. Its CPU ratio
is 1.000. The gain therefore tracks grid points, which is what removing per-point work
should look like — and it is direct evidence that the win is not an artefact of the
comparison.

**Two caveats, both material.**

1. **Wall clock is not the size of the effect.** CPU falls 22-31% on the grid-heavy shapes
   while wall falls only 4-8%: the query is not purely CPU-bound, so 90-100 ms of CPU
   surfaces as 2-4 ms of wall. Quoting the wall figure as "the speedup" would understate
   the change by roughly a factor of four. Wall also has real outliers here — `control`'s
   maximum is 232.5 ms against a 48.2 ms minimum in the same arm — which is why CPU is the
   primary statistic.
2. **The CPU resolution is 10 ms** (`SC_CLK_TCK` = 100). `S` is 7 ticks against 9, so its
   ratio is coarse. `G` (21 vs 30) and `W` (22 vs 32) carry the same ratio on three times
   the ticks and are the trustworthy rows.

**What this does not say.** It does not say the fused path now beats the ordinary plan:
the arms here are both fused. Sections 4.3 and 5 still hold — no comparison in this report
shows fusion ahead of the ordinary plan at fine steps with narrow windows.

## 5. What is not shown

- **No general speedup.** The algorithm regression is eliminated and one shape
  (`W`) improved 2.08x between N and F, but the fused path is still 0.88x-0.93x
  against the ordinary plan at fine steps with narrow windows. There is no
  evidence here to justify enabling `experimental_native_promql` by default; it
  remains off.
- **The per-point bookkeeping is gone for two functions only.** `rate` and `increase` take
  the batch path; `delta`, `irate`, `*_over_time` and the rest still call the boxed kernel
  once per grid point and gained nothing this round. Section 4.5's 36.9 ns-per-point figure
  was measured on the per-point form and has not been re-measured for the others.
- **The evaluator still pays per input sample.** Every sample is copied into growable
  `Vec`s so that the window can slide by `drain`ing the front, at a measured 3.77 ns/sample.
  That is now a memcpy and nothing more, but a memcpy is still O(samples in range) where the
  ordinary plan copies nothing and does index arithmetic over one shared array. The ceiling
  on removing it is measured (section 4.5): about 3.6 ns/sample, i.e. 0.8-2.2% on the four
  measured cases, all inside the drift band. So unlike batching, this one is *not* expected
  to show up end to end at these shapes, and at a range holding 50 M samples either it or
  the copy stays in place.
- **The output path is still not measured.** The node targets 4 096 rows per batch
  (`MAX_ROWS_PER_BATCH`); with 400 series over a 20-partition scan that is ~20
  series × 7 201 rows per partition ≈ 144 k rows ≈ 35 batches per partition.
  Those numbers are arithmetic from the configuration, not a measurement: no
  batch count, rows-per-batch, or emit/flush cost was collected, and no threshold
  was changed on the strength of them.
- **No large-window ceiling measured.** `W` is the widest window measured (1 h
  against a 2 h span). Nothing here says whether a still-wider window would now
  become *faster* than the ordinary plan rather than merely equal.
- **No CPU sampling profile.** Section 4.4 is operator-level `elapsed_compute`
  from `TQL ANALYZE VERBOSE`, not a sampled stack profile, so it attributes cost
  to operators rather than to functions within the scan.
- **No `EXPLAIN` visibility from the frontend.** In standalone, TQL is planned as
  a `MergeScan` pushed to the datanode, and fusion happens when the datanode
  builds its physical plan. `TQL EXPLAIN` at the frontend therefore never shows
  `FusedSeriesExec`; the debug log is the way to confirm a fused run.
- **No distributed or multi-region path.** Phase 1 is single-region,
  single-node. `RangeEvaluator` has no partial-aggregate contract, and one was
  deliberately not invented here.
- **No `PartialAggregate`, vector matching, or planner-level fusion.** Out of
  scope by design.
- **Nothing here covers a counter that resets inside every window at scale.**
  The unit and integration tests prove the index is correct with resets
  (including across chunk boundaries and window edges); the 1 152 000-row
  benchmark fixture has none, so no end-to-end timing includes real reset
  traffic.
- **The batch path's own unit of work was not measured directly.** Section 4.6
  measures whole queries before and after; there is no evaluator-only criterion number for
  the batch form, so the per-point bookkeeping figure (36.9 ns) still describes the form
  that `rate` and `increase` no longer use.
- **Nothing here shows fusion beating the ordinary plan.** Every comparison in section 4.6
  is between two fused builds. Sections 4.3 and 5's conclusion — 0.88x-0.93x against the
  ordinary plan at fine steps with narrow windows — is unchanged, because the change that
  would be needed to revisit it (not copying the input samples) is measured at 0.8-2.2% and
  was therefore not done.

## 6. Reproduction

Two commands per arm, over one shared data directory:

```bash
# Once: write the fixture (refuses to add to a directory of a different shape).
python3 scripts/fused_series_bench.py seed \
    --data-home ~/.cache/fused-scale/data-seed --bin <greptime>

# Per arm, per case: run and record. `--arm off` must log zero fused hits.
python3 scripts/fused_series_bench.py run \
    --data-home ~/.cache/fused-scale/data-seed --bin <greptime> \
    --arm on --commit <sha> --cases W \
    --out records/cW-r1-N.json --log records/cW-r1-N.log

# The interleaved L/N/F order, three rounds, all cases:
bash scripts/fused_series_bench_crossed.sh

# The tables above, from the raw records:
python3 scripts/fused_series_bench_summarize.py records
```

The compute benchmark is a normal criterion group in the promql package:
`cargo bench -p promql --profile nightly -- bench_main native_range_eval`.
Section 4.5's ingest table comes from the same binary's `ingest` group.

Section 4.6 compares two commits on the fused path rather than fusion against the ordinary
plan. Both arms are `--arm on`; the pair is driven interleaved with the order rotated per
round, and each round's two result sets are compared before the next round runs, so an arm
that disagrees stops the sweep. `--warmup 2 --iterations 9` per arm per round.

Two traps specific to that comparison:

- **Both arms must have the change verified, not assumed.** A rebuild that silently reused
  the previous binary would produce a clean "no difference". `strings <bin> | grep
  extrapolated_rate_batch` returning nothing on the parent commit and 6 hits on this one is
  what established that the two files differ.
- **The summariser had the ratio inverted on first use**: it computed `arms[1]/arms[0]` over
  a sorted arm list, i.e. P/F, while labelling the column "F / P". The numbers in section
  4.6 come from the corrected version, which names the two arms explicitly instead of
  relying on listing order. The raw records were unaffected either way.

Traps this driver encodes, each of which produced a wrong measurement first:

- `TQL EVAL (start, end, step)` takes **seconds**; `INSERT` literals and returned
  timestamps are **milliseconds**. A mismatch yields zero rows with a valid
  schema while `SELECT count(*)` looks healthy.
- `region`, `zone` and `mode` are SQL reserved words and cannot be column names.
- `POST /v1/sql` needs form encoding; a JSON body is rejected with 415.
- Two servers need disjoint port sets (`--http-addr`, `--grpc-bind-addr`,
  `--mysql-addr`, `--postgres-addr`), or they collide on the defaults.
- `pkill -f "greptime standalone"` matches the shell running it. Stop servers by
  PID.
