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
the results appended a column at a time. Against the previous commit the fused query's CPU
drops to 0.778 / 0.700 / 0.688 on `S` / `G` / `W` and stays at 1.000 on the coarse-step `C`,
which has few grid points and so has nothing to amortize; all 15 paired rounds returned
identical results and both arms logged the expected fused selections (section 4.6). Wall
clock moves 4-8% where CPU moves 22-31%; those measure different things and section 4.6 says
what each supports. Every other range function still runs one boxed call per grid point and
gained nothing.

**And that closes the gap with the ordinary plan.** On this build, one binary, flag off
against flag on, the fused arm costs **0.750 / 0.708 / 0.783 / 0.625** of the ordinary plan's
CPU on `S` / `G` / `W` / `C`, with all 12 paired rounds below 1.00 and a never-fused control
at 1.029 as the noise floor. Before this round it *cost* 8-14% more. Wall clock is 0.90-1.00,
so this is a CPU result, not yet a latency one. See section 4.7.

**And this round makes the fused node compute the partial aggregate itself.** For
`sum by (instance) (rate(metric[5m]))` the node now folds every evaluated series into the
replaced partial aggregate's own `GroupsAccumulator` and emits DataFusion's state directly, so
the 2 868 400 intermediate rows that aggregate used to consume are never materialised; the
`FinalPartitioned` aggregate and the repartition above it stay exactly as they were. Verified on
real mito storage through the real query entry point, and correct on seven shapes: all three arms
return the same answer, series by series and value by value, including the two 2 868 400-row
cases (section 4.11). The direction is unambiguous — `sum/on` is below 1.00 in **9 of 9 rounds**
of the three collapsing cases, and the partial arm is also the *most repeatable* of the three,
reading 130-150 ms across all six of its `G4`/`G1` rounds while the arm it is compared against
swings 170-270 ms. The **magnitude is not resolved** on the 100:1 shape a user writes (`G4`
0.70 / 0.48 / 0.88): the noise is in the denominator, and the gate reports the pass not quiet.
Where both arms repeat, it reads **0.900** on `n100` (three identical rounds) and **0.957** on
`G400`. Section 4.9's 40.7 ms bound is neither confirmed nor refuted — the rounds straddle it.
This is bench scaffolding behind an environment variable, and `experimental_native_promql` still
stays off.

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
- **Partial mode** (`PartialSum`, section 4.11): instead of converting each completed
  evaluated series into one output row per grid point, fold its points into the replaced
  partial aggregate's *own* `GroupsAccumulator` and emit that accumulator's `state()`
  once per partition. The state is DataFusion's, produced by DataFusion's accumulator, so
  the `FinalPartitioned` aggregate above consumes exactly what a `Partial` one would have
  written; the node reuses the aggregate rather than re-implementing `SUM`. Slots are keyed
  by `(label group, absolute grid index)` — `GridRange::start()` plus the row offset, never
  a local row number — because each series trims its own interval and a local index would
  alias two grid points. It is admitted only for a single-field shape: the occupancy set is
  the union over the series' fields, so with two fields a grid point can be occupied while
  the summed field is null, and the node would report a group the ordinary partial
  aggregate never makes.

Construction-time rejections (never a silent fallback): zero partitions, an
evaluation-grid input schema, `Interleaved` ordering, an illegal window or
function, an illegal frame schema, an illegal output schema, and — in partial
mode — a state layout that is not one `Float64` per group, a summed column that is
not the node's one series field, or a group column that is that field.

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

A `sum` above the fused subtree has a second, optional step (section 4.11).
DataFusion plans one logical aggregate as `Partial → Repartition → Final`, and the
partial one groups by the evaluation timestamp as well as the `by(...)` labels — PromQL
puts the evaluation timestamp in the grouping key — so each of its groups is exactly one
`(label set, grid index)`, which is the unit the node already produces. The hook finds
that partial aggregate and replaces *it*, handing the node the aggregate's own `SUM`.
The `RepartitionExec` and the `FinalPartitioned` aggregate above are rebuilt with their
own configuration and stay in place; this is a swap of the partial stage, not a
replacement of the aggregation.

It is admitted only when every gate holds, and each gate is a rejection rather than a
query error, because the ordinary plan is always a correct answer:

| what it requires | why the swap is not equivalent without it |
| --- | --- |
| the aggregate is `Partial`, has no grouping sets, exactly one `SUM` with no `DISTINCT`/ordering/filter | the node takes over one accumulator and replays no filter or rollup |
| the `SUM` argument and every group expression is a plain column | the node carries no expression state to re-evaluate |
| between them, either nothing or `<summed column> IS NOT NULL` | the node reproduces that filter by never emitting an all-null point; **only on the summed column** — dropping `IS NOT NULL` on a group column is *not* equivalent, because the ordinary partial aggregate never produces a null group key while the fused node would report that group |
| the aggregate's `input_schema()` is the node's output schema | otherwise its column indices no longer address the node's output positions |
| exactly one such aggregate above the subtree | phase 1 replaces one |

The step reads `GREPTIME_FUSED_PARTIAL_SUM`, which is bench scaffolding and not a user
option: it is not a `QueryOptions` field, not a `SET` parameter, not in any `config/*.toml`.
`0` keeps the evaluated-only fusion byte for byte, which is what the `on` arm measures.

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

**One regression this introduced, and its fix.** The first version decided "dense" only from
the single-null-free-field case, so a series with several fields where one of them covers
every row still built an identity index and copied the time column and every value column
through it. The previous code had skipped that copy whenever nothing was dropped
(`occupied.len() != rows`), regardless of field count, so this was a regression in exactly the
multi-field shape. It produced correct results, which is why no value comparison could see it;
the test that now guards it compares the time column's buffer address against the grid's at
the same element offset, because a copy cannot occupy the grid's buffer. The scan's result is
reused: when every row is occupied it has already produced a single run covering the range.

## 2. Correctness

Six independent layers, all currently green.

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
6. **Three data shapes the fixture cannot express**, through real storage and the real planner:
   a counter that resets every 48 samples (`vmreset`, 5 m and 1 h windows), a counter with
   periodic gaps plus a 30 min outage (`vgaps`, 52 008 of 57 600 samples present), and the same
   data as `vmreset` written in 5 760 ten-sample statements instead of 40 whole-series ones
   (`vfrag`). `scripts/fused_series_shapes.py` seeds all three, checks the hit count per
   execution, and compares the arms row for row: all five cases matched on both arms, the fused
   arm logging exactly one selection per query and the unfused arm none.

   The sharpest of those is `vfrag` against `vmreset`: **identical values at identical
   timestamps, different on-disk fragmentation**, so the two must return identical numbers. They
   do — `differing cells = 0`, largest absolute difference `0.0` on the fused path — while the
   ordinary path differs from itself by `1.4e-14` on the same comparison. That is the shape a
   lost-at-batch-boundary accumulator bug would fail, and an arm-versus-arm comparison could not
   catch it (both arms would be wrong in the same way).

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

**A cost estimate, which is why it was not attempted in the same round -- not a ceiling.**
`evaluator_push` already costs what `bulk_copy` costs (table above), so within that
microbenchmark the saving available from removing the copy is `bulk_copy - read_only`, about
3.6 ns/sample. Applied to the four measured cases that is roughly 0.8% of `S`, 1.0% of `G`,
1.5% of `W` and 2.2% of `C`.

**What that estimate is not.** The benchmark is one 200 000-sample chunk, one grid point, one
`PresentOverTime` field, so it omits what a real series would add: many chunks, the retained
window tail moving between them, `Vec` capacity growth, and resets. `read_only` adds up the
value column only -- it does not read the timestamps and does no stale or reset work -- so the
difference is not the whole cost of the storage change either. And these percentages are
per-sample work, not query latency: turning them into a wall-clock prediction needs the
per-partition split and the parallel assumptions, which are not measured here. So the honest
statement is that the saving looked small enough, under this benchmark, to prefer the batch
work first. It is not proof that the real saving is under 2%.

Taking it on would still rewrite the `&[i64]` / `&[f64]` contract that the batch kernel and
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

**Correctness.** Each round runs one dedicated correctness query per arm and compares those
through `compare`: schema column names and types, every row's full label tuple, every
timestamp, the row count, then every value against a tolerance of `1e-9 * max(1, |value|)`,
not bit equality. All 15 paired rounds passed. The timed queries are checked for row count
but are not the ones compared, so this says "identical within the driver's tolerance", which
is what a float pipeline can be held to and is stricter than the aggregation above it needs.

**Fusion.** Both arms log exactly one fused selection per query on `S`/`G`/`W`/`C` and zero
on `control`, as required, so the comparison never silently measured the ordinary plan.

**`C` is the negative control this round needed.** It is the same 120-minute query as `G`
and `W` at a 1-minute step: 121 grid points per series instead of 7 201, emitting 120 rows per
series instead of 7 171, so 48 000 rows leave the fused node instead of 2 868 400, against the
same ~200 000 ingested samples. Its CPU ratio is 1.000.

The two grid and row counts differ by the empty head and tail: `TQL EVAL` from 1700000000 to
1700007200 at 1 s is 7 201 points, the samples start 15 s in, so the first 15 points have no
window and the last 15 sit past the final sample. Those points are still evaluated -- the batch
kernel runs over all 7 201 -- but they produce no row, which is why a series emits 7 171 rows
and 400 series emit 2 868 400. At a 60 s step the same arithmetic gives 121 points and 120 rows.

The gain therefore tracks grid points, which is what removing per-point work should look like --
and it is direct evidence that the win is not an artefact of the comparison.

**The two statistics answer different questions, and both are real.** CPU falls 22-31% on
the grid-heavy shapes: that is resource consumption, the work the query costs the machine.
Wall falls 4-8%: that is the latency a caller waits for. They are not rival measurements of
one quantity and neither is the "real" one -- a query spread over many partitions accumulates
CPU in parallel, so its total CPU time and its wall time could not move in step by
construction. Nor do these two ratios, on their own, say where the savings go: they do not
separate I/O, scheduling or parallelism, and they do not establish any throughput claim.
Wall also has real outliers here -- `control`'s maximum is 232.5 ms against a 48.2 ms
minimum in the same arm -- which is why the median is quoted.

**One caveat on resolution.** The CPU counter ticks at 10 ms (`SC_CLK_TCK` = 100). `S` is 7
ticks against 9, so its ratio is coarse. `G` (21 vs 30) and `W` (22 vs 32) carry the same
ratio on three times the ticks and are the trustworthy rows.

**What this does not say.** It does not say the fused path now beats the ordinary plan:
the arms here are both fused. Sections 4.3 and 5 still hold — no comparison in this report
shows fusion ahead of the ordinary plan at fine steps with narrow windows.

### 4.7 This round against the ordinary plan

Section 4.3's comparison with the ordinary plan predates the batch path and can no longer
describe this build. This is that comparison, run on the current build with one binary:
`a2d345cc76c` with `experimental_native_promql = false` (the ordinary plan) against the same
binary with it `true` (fused). Run `records-20261018T091334Z`, crossed order, 3 rounds, 2
warmup + 9 timed per arm per round, 27 timed samples per arm per case.

| case | off CPU | on CPU | CPU on/off | wall on/off | per-round CPU on/off |
| --- | ---: | ---: | ---: | ---: | --- |
| `S` | 80 ms | 60 ms | **0.750** | 0.954 | 0.875 / 0.875 / 0.750 |
| `G` | 240 ms | 170 ms | **0.708** | 0.970 | 0.783 / 0.708 / 0.708 |
| `W` | 230 ms | 180 ms | **0.783** | 0.904 | 0.783 / 0.708 / 0.783 |
| `C` | 80 ms | 50 ms | **0.625** | 0.997 | 0.714 / 0.625 / 0.625 |
| `control` | 340 ms | 350 ms | 1.029 | 0.976 | 1.000 / 1.029 / 1.000 |

All 12 paired rounds on the four `rate` cases are below 1.00. The flag-off arms logged zero
fused selections and the flag-on arms exactly one, so neither arm measured the other's path.
Every paired round compared identical within the driver's tolerance.

**The control case is the noise ruler, and it says this comparison is sound.** `control` is
`max_over_time`: it never fuses, so both arms run the ordinary plan and the ratio must be 1.00.
It reads 1.029 on 34-against-35 ticks and 0.976 wall. That is the whole noise budget of the
run, and the four `rate` margins are far outside it.

**This inverts the pre-batch result.** Section 4.3 had the fused arm at `L/F = 0.88-0.93`
(`S` 0.93, `G` 0.88, `W` 0.90), i.e. *costing* 8-14% more than the ordinary plan. It now costs
25-37% less on CPU. Part of the difference is not the evaluator: the fused node also skips
`ProjectionExec`, `PromSeriesDivideExec` and `PromSeriesNormalizeExec`, which section 4.5
measured as the reason the fused arm was already cheaper on the coarse-step `C`. What this
round adds to that is that the grid-heavy shapes joined it.

**A second run of the same comparison, and why it is here.** It was first run while a
`cargo build --profile nightly` of the whole server was in progress
(`records-20261018T091013Z`). Load average was 26-37, wall clock inflated ~4x
(`S` read 89.0 ms where the clean run reads 21.9), and CPU inflated on both arms: `G` read 350
ticks for the ordinary plan against 240 on the clean run. The ratios came out compressed --
`G` 0.943 there against 0.708 here -- because contention does not inflate the two arms equally.
Both runs agree in direction and both have a control at 1.00-1.04, but only the clean run's
magnitudes should be quoted. This is the same trap as section 4.5's caveat, in a new form: it is
not enough for a ratio to come from one invocation if that invocation has a compiler fighting
it.

**What this establishes, and what it does not.** Fused is now cheaper than the ordinary plan on
CPU on all four measured shapes, and the coarse-step case is the largest margin. It does not
establish a wall-clock win: wall reads 0.90-1.00, and `C` is flat at 0.997. Nor does it
establish anything about other query shapes -- these are one table, four series, `rate` and
`max_over_time` only (section 5).

### 4.8 Where the CPU goes, by module — and four withdrawn conclusions

**Retracted first, because everything below replaces them.** An earlier revision of this
section turned a sampling profile into an attribution of CPU. Two defects in that pipeline, both
confirmed by independent review and both now fixed, invalidated four conclusions:

| withdrawn claim | what was wrong | corrected |
| --- | --- | --- |
| "the sampler recovered 13% of the CPU" | it counted profile *lines*, not the sample count each line carries | **62%** (run `W-on3`) and 72% (`W-on2`) |
| "the evaluator is 6.0% of the query" | shares were computed from those line counts, and each line was assigned by a fixed category priority that let an outer frame win over an inner one | **33.3%** (`W-on2`) / **37.6%** (`W-on3`) |
| "the kernel is the largest remaining cost; optimize it first" | it rested on the 6.0%, and the split inside the evaluator came from single unreplicated passes | inside the evaluator, kernel **45.2-45.8%** and window preparation **45.5-46.2%**: equal, and neither is the largest thing to attack |
| "a fused partial aggregate is bounded at 27% of the grid-dependent CPU" | derived from the same single pass | **22.5%** of the fused query's total CPU |

**Defect 1: the sample count is per line, not one per line.** `pprof` 0.15's
`Debug for Report` writes `{frames:?} {count}` (`pprof-0.15.0/src/report.rs:150`), so one line is
one *distinct stack* and the trailing integer is how many samples had it. `W-on3` is 5 621 lines
and **26 529 samples** — a factor of 4.7, and the deficit is not uniform across lines, so every
share computed from line counts was distorted rather than merely scaled. Both scripts now parse
the weight and accumulate everything — totals, per-owner shares, per-marker counts — weighted.
`fused_profile_summarize.py --self-test` checks it against a two-record fixture whose answer is
known: 99 samples in the evaluator and 1 in the aggregate must give 99.00% / 1.00% and a total of
100, and a line count of that fixture would say 50/50.

**Defect 2: owners were resolved by category priority, not by stack position.** The old code
iterated the owner list in order and asked whether *any* frame in the stack matched *that owner*.
Because `fused: stream` was checked before `scan`, a sample taken inside a mito2 read reached
through `FusedSeriesStream` was charged to the fused stream. The fix walks the frames from the
leaf outwards and stops at the first frame that names a known component, so the frame nearest the
sampled instruction decides. Unrecognised samples are reported as such with their leaf frame, at
2.1% of the total, never folded into a named bucket.

**The corrected profile.** Case `W` (`sum by (instance) (rate(vmbench[1h]))`, 120 min at a 1 s
step), fused, `POST /debug/prof/cpu`, `--frequency 199`. Two independent runs, because a single
profile from this machine has been wrong before:

| owner | `W-on2` (23 111 samples, 160.4 s CPU) | `W-on3` (26 529 samples, 215.0 s CPU) |
| --- | ---: | ---: |
| **fused: evaluator** (`promql::range_eval`) | **33.3%** | **37.6%** |
| **aggregate** (the `sum by` hash path) | **26.1%** | **26.3%** |
| scan (`mito2`, `SeriesScan`, region read) | 18.6% | 17.8% |
| fused: output build (`grid_series`) | 2.1% | 2.3% |
| fused: stream (emit/flush) | 2.8% | 2.6% |
| http response / JSON | 3.5% | 4.4% |
| sort | 2.3% | 2.8% |
| planning | 2.1% | 2.3% |
| shuffle | 1.4% | 1.3% |
| logging / plan formatting | 3.0% | 0.4% |
| unrecognised | 4.7% | 2.1% |

Two modules hold 60-64% of the query between them and the rest is a long tail. The two runs
agree to within 4 points on the top three, which is the noise this instrument has at these sample
counts.

**The capture rate, measured against the same window's CPU.** `ITIMER_PROF` fires on process CPU,
so a sampler asked for 199 Hz over a window in which the server burned 215.0 s of CPU should
produce ~42 800 samples. It produced 26 529: **62%**, or 123 samples per CPU second. `W-on2` is
72%. That is lost signals, not a wrong denominator, and it is the reason a single profile here
gets two runs and a cross-check. What it does *not* do is bias the shares systematically: a
uniformly lossy sampler keeps its proportions, which is why the corrected shares are usable even
at 62%.

**`fused_evaluate_time` is the whole evaluator, not the kernel.** The previous revision labelled
this counter "the batch kernel". It is not. It wraps one call to `RangeEvaluator::push`, which
contains **ingest** (`push` → `ingest`, ~0.5% of the evaluator by profile), **window
preparation** (advancing the four cursors and building each batch's spans, indices and reset
ranges), **the kernel** (`flush_batch`'s `kernel(&batch, …)`), and **result column construction**
(`flush_batch`'s `output.push_columns`). Everything in this section that used that counter as a
kernel figure is restated accordingly.

**Inside the evaluator, the profile resolves into two halves and stops there.** Restricting to
the samples attributed to `promql::range_eval` and looking at their leaf frames:

| leaf frame, within the evaluator | `W-on2` | `W-on3` | region |
| --- | ---: | ---: | --- |
| `extrapolated_rate_batch::<true, true>` | 50.5% | 49.4% | the kernel's arithmetic |
| `<RangeEvaluator>::evaluate` (own body) | 47.8% | 49.4% | window preparation, cursor advance |
| `<RangeEvaluator>::ingest` | 0.6% | 0.5% | copying samples in |
| `flush_batch` (own body) | — | 0.2% | batch bookkeeping |

**Window preparation costs as much as the kernel arithmetic.** That is the one substantive thing
this correction adds, and it is not what the previous revision said. Half of the largest region in
the query is spent deciding *which samples each window covers* — the forward cursors for the left
edge, the right edge and the two reset boundaries — and half is the `rate` arithmetic itself. The
profile puts them at 18.5% and 18.8% of the query, within 1-2 points of each other, and the batch
timers below confirm the tie with an instrument that does not depend on resolving inlined
frames.

**Which is why this could not be ordered from the profile, so it was measured directly.** The
two halves are adjacent code in one `#[inline]`-heavy function; a sampling attribution of inlined
frames cannot separate 18.5% from 18.8%, and the previous revision's error was exactly to treat a
sampling split as an ordering. Three **batch-level** intervals now time the evaluator instead —
window preparation, the batch kernel, and appending the flushed columns — once per batch of 512
grid points, so six clock reads per flush rather than two per point: 90 reads per series against
7 171 points, one per 80, worth an estimated 0.4% of the query.

**The timers say the two halves are equal, and that both are per-point cost.** Nine `TQL ANALYZE`
records per case, one server per case, every figure from a single record so each ratio is of two
measurements taken in one execution (`docs/bench-records/20261018T134000Z/`):

| region | `W` (`rate[1h]`) | of evaluator | `G` (`rate[5m]`) | of evaluator |
| --- | ---: | ---: | ---: | ---: |
| `fused_evaluate_time` (whole `push`) | 60.69 ms | 100% | 60.50 ms | 100% |
| `fused_window_prep_time` | 28.05 ms | 46.2% | 27.55 ms | 45.5% |
| `fused_kernel_time` | 27.40 ms | 45.2% | 27.71 ms | 45.8% |
| `fused_columns_time` | 3.83 ms | 6.3% | 3.95 ms | 6.5% |

The three regions sum to **97.7-97.8%** of the counter they subdivide. The remaining 2.2-2.3% is
not attributed: it contains `ingest`, the loop's own bookkeeping and the flush's `clear_points`,
and no measurement here separates those. The profile did put `ingest` at 0.5-0.6% of the
evaluator, but that is a separate instrument measuring a differently-scoped number, so it is a
consistency hint and not an identification of the residual. What the 97.7-97.8% does establish is
that the three regions cover essentially the whole counter, which is the check that the
instrumentation is complete.

Per record the `prep/kernel` ratio reads 0.995 (`W`) and 0.997 (`G`) by median, with preparation
above the kernel in 8 of 18 records and a worst excursion of 10.9% — against the 25% drift of the
absolute times between records of the same case. **There is no larger half to attack.** The
profile's 50/50 was not an artifact of inlining; it was the answer.

**Both halves are fixed cost per `(series, grid point)`, not per sample in the window.** `W` holds
240 samples per window and `G` 20 — a factor of 12 — over the same grid, and every region costs
the same in both: kernel 27.40 against 27.71 ms, preparation 28.05 against 27.55 ms, whole
`push` 60.69 against 60.50 ms. Averaged over this fixture that is 21.1 ns per point on either
case — an amortized figure for this shape only (400 series of 15 s samples over 7 171 grid points
at a 1 s step, wall-clock timers on this machine), not a rate that transfers to another series
count, step or sample density. It is the same property the earlier round found end to end
(`rate[1h]` = `rate[5m]`, 1.003x): 400 series x 7 171 points x 21 ns is the 60 ms here, and the
33-38% of a 173 ms query the profile attributes to this module.

**What that implies for the next change.** Asking "preparation or kernel" has no answer, and the
previous revision's mistake was to demand one. What both halves share is the per-point machinery
rather than either half's arithmetic: preparation appends, for each grid point, a 16-byte window
span and a 16-byte reset range **per field**, plus an 8-byte grid index and an 8-byte evaluation
timestamp that are shared across fields, all into four parallel `Vec`s that the kernel then reads
back. On this fixture — one value field — that is 48 bytes per point, so 2 868 400 points give
**137.7 MB of cumulative logical writes per query**. That is the sum of what is pushed into the
four vectors, not resident memory: they reallocate as they grow and the allocator's footprint is a
different number this section does not measure.

The actual `rate` result payload is **22.95 MB** — 2 868 400 `f64` at 8 bytes — so the
per-point window metadata is six times the size of the values it produces.

Two of the four are derivable, because a batch's grid indices are contiguous and ascending by the
flushing convention above: `indices` is `first_index + k` and `eval_ts[k]` is
`grid[first_index + k]`. Removing both deletes 16 of those 48 bytes — **a third on a single-field
series, and less as fields grow**, since the span and the reset range scale with the field count
while these two do not. The remaining two thirds carry the window boundaries themselves. That the
deleted third is the *expensive* third is exactly what arithmetic cannot answer: it says nothing
about how the write cost distributes across the four vectors, which is why the change is measured
rather than predicted.

**What the block timers say, and what they may not be compared to.** These are medians over four
replications of the same statement (`sum by (instance) (rate(vmbench[5m]))`, 120 min at 1 s, 400
series), each replication taking `/proc` CPU and the operator counters in one server:

| counter | median | share of the query's CPU |
| --- | ---: | ---: |
| `fused_evaluate_time` (the whole evaluator) | 58.40 ms | 34% |
| aggregate partial `aggregation_time` | 38.87 ms | 22% |
| aggregate final `aggregation_time` | 3.65 ms | 2% |
| `fused_emit_time` (`occupancy` + build output batch) | 1.84 ms | 1% |
| `fused_flush_time` (`concat_batches`) | 0.22 ms | — |
| `/proc` CPU per query | 173.0 ms | 100% |

The profile's independent estimate for the same two modules is 33.3-37.6% and 26.1-26.3%. A
sampling share and a block timer's share of `/proc` CPU are different instruments measuring the
same two regions, and they agree to within a few points: the evaluator is the largest, the
aggregate is second, and both are a fifth to a third of the query.

**One ratio this section will not quote.** An earlier revision wrote the evaluator as 28.2% of
the query by dividing a 65.45 ms block timer from one record by a 232.5 ms `/proc` figure from a
different record. Two numbers from different server runs are not a ratio: the pass-to-pass drift
on these timers is 40-50%, which is larger than the quantity being divided. Every percentage
above is either a share of one profile's own weighted total, or a ratio whose two terms were
measured in the same replication as the table says.

**The marginal test, replicated, for the cost of one intermediate row.** `G` and `C` scan the
same 192 000 raw samples with the same window and differ only in grid step, so the emitted rows
differ 60x — 2 868 400 against 48 000 — and differencing the same operator across the two gives
its cost per added row. A single pair gave 22.1 and 17.4 ns/row; that is not reportable, since a
27% gap on two unreplicated differences is inside this machine's drift. Six replications of the
pair put the kernel above the aggregate in 6 of 6 at 17.9 and 12.3 ns/row; four more, with
`/proc` CPU taken in the same pass, gave:

| operator | median ns/row, 4 reps | range | share of the CPU delta |
| --- | ---: | ---: | ---: |
| `fused_evaluate_time` (whole evaluator) | 20.06 | 18.20-22.62 | 43% |
| aggregate partial | 13.48 | 12.52-15.54 | 29% |
| aggregate final | 1.25 | 1.15-1.69 | 3% |
| `fused_emit_time` | 0.50 | 0.44-0.66 | 1% |
| `fused_flush_time` | -0.09 | -0.13--0.07 | 0% |

The five sum to 99.3 ms against a CPU delta of 132.3 ms — **75%**, per replication 68 / 81 / 81 /
83%, so the operators are a subset of the total as they must be. The other 25% is outside these
five and is not claimed here. These per-row figures are the *whole evaluator's* marginal cost,
not the kernel's: the batch-level timers of the next change are what will divide it.

**Two measurement artifacts this round had to remove.** Both would have inflated the fused
node's apparent cost:

1. **Profiling the wrong statement.** `fused_series_bench.py`'s `CASES[i]["expr"]` is the inner
   `rate(...)`; only `bench.tql(case)` wraps it in `sum by (instance)`. Profiling the raw
   expression returns all 2 868 400 rows and measures JSON serialisation of them: median 3 841.8
   ms, with `serde_json` and `HttpRecordsOutput::try_new` at the top. The same mistake was in
   `fused_series_profile.py`'s `ANALYZE` path, which built its statement from `case["expr"]`
   too; it now derives the analysed statement from `bench.tql` so the plan is the measured
   query's.
2. **A `debug` log filter.** The driver configures `level = "debug"` because the fused-selection
   line is logged at debug, and at that level datafusion formats the entire physical plan for a
   log record on every planning pass: 4.6% of samples in the first run, and the median query
   read 53.8 ms against 42.7 ms with the filter narrowed to
   `info,query::datafusion::native_promql::fused=debug`.

**What a fused partial aggregate could remove, at this measurement.** Of the 173.0 ms the fused
arm's query costs, the aggregate's generic hash path is 38.9 ms (22.5%) and `emit` 1.8 ms.
Collapsing the intermediate rows inside the fused node could therefore address at most 40.7 ms —
the fused arm would land near 132 ms. That is a **ceiling, not a prediction**: a fused accumulator
still adds every value into its group, which is far cheaper than the hash-table path but not free,
and it is only available when the group cardinality is known or bounded, which `sum by` over a
finite label set gives and a general aggregate does not. The `FilterExec` between the two nodes is
pure transport (`selectivity = 0`, every row passes) but is not added, because no `C`-side
measurement of it was taken.

### 4.9 Varying series count and group count

Section 4.7's cases all use one table shape: 400 series into 4 groups. Both axes move inside
the query, over the same fixture and without re-seeding: a label matcher selects the input
series, and the aggregation picks the group count.

`scripts/fused_series_scale.py`, 120 min at a 1 s step, one binary (`a2d345cc76c`), the native
flag as the only variable, 3 rounds with the arm order rotated, 2 warmup + 9 timed per arm per
round, and the same per-query fused-hit check as section 4.7. CPU is the median of the timed
iterations; the ratio is the median over rounds.

| case | intermediate rows | output rows | ratio | CPU off | CPU on | CPU on/off | per-round | wall on/off |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- | ---: |
| `G1` `sum by (job)` | 2 868 400 | 7 171 | 400:1 | 220 ms | 160 ms | **0.682** | 0.68 / 0.66 / 0.73 | 0.917 |
| `G4` `sum by (instance)` | 2 868 400 | 28 684 | 100:1 | 280 ms | 170 ms | **0.682** | 0.71 / 0.68 / 0.57 | 0.853 |
| `n100` (100 series, `sum by (instance)`) | 717 100 | 7 171 | 100:1 | 110 ms | 90 ms | 0.818 | 0.91 / 0.82 / 0.82 | 0.929 |
| `G400` `sum by (host)` | 2 868 400 | 2 868 400 | 1:1 | 2 340 ms | 2 280 ms | 0.970 | 0.97 / 0.97 / 0.97 | 1.005 |
| `raw` (no aggregation) | 2 868 400 | 2 868 400 | 1:1 | 1 230 ms | 1 190 ms | 0.967 | 0.97 / 0.97 / 0.95 | 1.026 |
| `n1` (1 series, `sum by (instance)`) | 7 171 | 7 171 | 1:1 | 30 ms | 30 ms | 1.000 | 1.00 / 1.00 / 1.00 | 0.854 |
| `control` (`max_over_time`, never fuses) | 2 868 400 | 28 744 | — | 430 ms | 440 ms | 1.023 | 0.83 / 1.13 / 1.02 | 0.889 |

**The gain tracks the reduction ratio, and vanishes at 1:1.** Where an aggregation collapses
100:1 or 400:1 the fused path costs 0.68 of the ordinary plan; where nothing is collapsed
(`G400`, `raw`) it costs 0.97, and with a single series it is 1.000. The three 1:1 cases are
also the most repeatable in the table — `G400` reads 0.97 in all three rounds, `n1` 1.00 in all
three, `raw` 0.97/0.97/0.95 — while the two cases with a group count worth collapsing carry
the most spread (`G4` 0.71/0.68/0.57, `n100` 0.91/0.82/0.82). The effect is largest where the
noise is, and absent where the noise is not; `G4`'s gain is 29-43% across its rounds, not the
32% its median alone suggests.

**Why the ratio matters more than the series count.** `n100` has the same 100:1 ratio as `G4`
but a quarter of the series, and reads 0.818 rather than 0.682. Both arms shrink with the series
count, but not equally: the fused arm's remaining cost is per-series fixed overhead plus
per-intermediate-row work, and at 100 series the fixed part is a larger share of a smaller
total. `n1` is the limit of that progression: one series, no aggregation to speak of, ratio
exactly 1.000.

**The gate on this pass.** It compares each case's per-round ratios against their median and
this pass reported **not quiet**: at a 10% gate, `G4` deviates **16.9%** (round 3, 0.567 below a
0.682 median), `n100` 11.1% (round 1, 0.909 above a 0.818 median), and `control` 19.0% (0.829,
1.127, 1.023). The conclusion above rests on `G1` (6.7%), `G400` (0.5%), `raw` (1.6%) and `n1`
(0.0%); `G4` and `n100` should be read with their rounds in view.

The gate used to measure `max(ratio) / median - 1`, which only sees a case drifting *up*. That
let `G4` pass at 4.8% in the same pass where its third round read 0.567 against a 0.682 median —
a 17% excursion on the low side, invisible to one-sided arithmetic, and larger than the 11.1%
that did trip the gate. The 4.8% figure was therefore not a quietness claim about `G4` and no
longer appears. Nothing below is derived from it: `G4`'s 0.682 is quoted with its 0.567-0.714
range wherever a ratio of ratios is used, and the marginal-cost measurements in section 4.8 come
from the `TQL ANALYZE` timers and `/proc` CPU of repeated identical statements, not from this
scale pass.

**What the two arms were compared on.** This pass compared them on **row count only**: the
script read `rows[off] != rows[on]` and stopped there, so a flag-on arm returning the right
number of wrong numbers would have been timed and reported. The script now runs one dedicated
correctness query per arm — its own execution, never a timed one — before the warmups, and
compares the two through `fused_series_bench.compare_pair`: schema names and types, every row's
label tuple, every timestamp, the row count, then every value at a relative tolerance of `1e-9`.
That comparator was exercised on the values it has to reject (a 4% value change, a changed label,
a moved timestamp, an extra row, a NULL against a number) and on the one it must accept (a
relative change of `1e-13`, and `NaN` against `NaN`). The archived pass predates the check, so
the equality evidence for it is its matching row counts plus section 4.7's value comparison on
the same binary, where the same 28 684-row `sum by (instance)` shape was compared value by value
and agreed in every round. The next scale run carries its own evidence.

**`control` is a poor gate and is kept only as a diagnostic.** It is `max_over_time`, which does
not fuse, so both arms run the ordinary plan and the ratio must be 1.00 — it reads 1.023, which
is fine. But it deviates 19% round to round because the ordinary plan's `PromRangeManipulateExec`
materialises the whole range matrix per query: `TQL ANALYZE` on `control`'s own statement reports
its `output_bytes` as **176 MB**, against 7 MB for the `SeriesScan` beneath it and 81 MB for the
`ProjectionExec` above it. That figure is identical for `max_over_time[5m]`, `rate[5m]` and
`rate[1h]` — 176 MB and 2 874 400 rows in all three — so it scales with the grid, not with the
window. A query allocating and freeing that every call measures jemalloc's variance, not the
machine's quietness, so the gate was moved to the repeatability of each case's own ratio. An earlier
version of the script gated on `control` and rejected a pass in which `G400`'s ratio was steady
to 0.5% — the wrong instrument, and the reason it was replaced is recorded here rather than only
in a commit message.

**What this implies for a fused partial aggregate.** Take `G4`, the shape PromQL users actually
write: the fused arm is 173.0 ms of CPU, of which the aggregate's generic hash path is 38.9 ms
(22.5%) and `emit` 1.8 ms. Collapsing the intermediate rows inside the fused node could
therefore address at most 40.7 ms of that 173.0 — the fused arm would land near 132 ms, not the
120 ms a single un-replicated pass implied. That is a real saving on top of the 32% fusion
already wins, but it is a fifth of the query rather than a third, and the module above it
(`fused_evaluate_time`, 58.4 ms, 34%) is larger as a whole.

Nothing *inside* that 34% is a better target than the aggregate, and that is now measured rather
than unknown: section 4.8's three batch regions put window preparation at 45.5-46.2% of the
evaluator and the kernel at 45.2-45.8%, so there is no larger half, and the remaining 6.3-6.5% is
the column append. The evaluator's cost is a fixed ~21 ns per `(series, grid point)` on this
fixture regardless of window width — `rate[1h]` and `rate[5m]` cost the same in every region —
so shrinking it means removing per-point machinery, not picking between two halves that measure the
same.

That is the comparison the next round should make: a fused partial aggregate is worth at most
40.7 ms of a 173.0 ms query and is well understood, while the evaluator's 60.7 ms is spread over
2.87 M points at a fixed cost each, on top of 137.7 MB of cumulative per-point window-metadata
writes that exceed the 22.95 MB of values the query returns. **Section 4.11 makes that
comparison.** Its `G4` rounds straddle the 40.7 ms bound — 0.70 / 0.48 / 0.88 against a
predicted floor of 0.765 — so the bound is neither confirmed nor refuted there, and the saving it
did not account for is the intermediate rows this estimate never charged to anything. The shapes where a partial aggregate
would do nothing are exactly the shapes where fusion already does nothing: at 1:1 there is no
redundancy to collapse, and `G400`'s 2.28 s is the cost of producing 2.87 M output rows that the
user asked for.

### 4.10 Removing two of the four accumulator vectors, and the drift that nearly faked it

Section 4.8 ends on a hypothesis it could not test: window preparation writes 48 bytes per point
per field into four parallel `Vec`s, two of which — the grid index and the evaluation
timestamp — are derivable, because the evaluator flushes the batch before it skips grid points
and before the tail `advance`, so a batch's indices are `first_index..first_index + len`. This
section is the controlled test of deleting them, on the experiment branch only.

**What changed.** `PendingBatch` holds `first_index: usize` and `len: usize` instead of
`indices: Vec<usize>` and `eval_ts: Vec<i64>`; `flush_batch` reconstructs the evaluation
timestamps by slicing the grid it is handed. `WindowBatch`, `BatchKernel`, the retained spans, the
reset ranges, the batch size and the flush placement are byte-for-byte unchanged. That makes the
kernel a **drift reference region** for the measurement below — not a negative control, see the
assumptions it rests on. Per point this removes two `Vec::push`es and adds one `usize`
add-and-store; a `debug_assert` pins the contiguity convention, and a deliberate off-by-one in it
fails 21 tests, including the batch-boundary and gap cases.

**The first answer was wrong, and the control said so.** Three rounds of the two binaries gave an
evaluator ratio of 0.897 — "10% faster". The same binary against itself, same script, same
arithmetic, gave 0.976: doing nothing was "2.4% faster". Two sessions 30 s apart on this machine
differ by 6.2-6.7 ms standard deviation on a 28 ms region, so a single paired difference cannot
resolve an effect of order 1 ms. That reversal is recorded here rather than quietly rerun.

**The measurement, using the kernel as a drift reference.** Eight paired rounds per case, arm
order alternating, plus eight rounds of the same binary against itself per case. Subtracting each
round's kernel difference from its `prep` difference, pooled over both cases (16 rounds each):

This is an estimate under two assumptions, and the change guarantees neither. First, that the
kernel's own cost is essentially unchanged: its arithmetic is unchanged, but `eval_ts` is no
longer a copied `Vec` — it is a slice borrowed from the grid, so the data source and the cache
behaviour of that access both change and the kernel's cost can move with them. Second, that drift
is roughly additive in milliseconds and shared across regions within a round. The same-binary
control checks the second as far as identical code can and cannot establish either one after a
code change, so it shows the adjustment is not grossly biased — not that it is unbiased.

| region | A/B adjusted | negative | self-control adjusted | negative | separates? |
| --- | ---: | ---: | ---: | ---: | --- |
| `prep` **(changed)** | **−1.77 ms** | **16/16** | +0.60 ms | 6/16 | **yes, −2.37 ms** |
| `evaluate` (contains prep) | −2.10 ms | 14/16 | −1.79 ms | 10/16 | no: control moves too |
| `columns` (untouched) | +0.82 ms | 6/16 | +1.76 ms | 6/16 | no: same in both |
| `kernel` (drift reference) | 0.00 by construction | — | 0.00 | — | the reference |

The two untouched regions are what make this usable: `columns` reads nearly the same in the A/B as
in the self-control, so the adjustment's offset is a property of the adjustment; `evaluate` reads
−1.79 in the control, so its own number is not evidence; and `prep`, the one region the change can
move, is negative in 16 of 16 rounds in the A/B against 6 of 16 in the control.

**Three estimates at three levels, which are not interchangeable.** The adjusted shift of `prep` is
−1.77 ms (16 of 16 rounds negative, standard error 0.25); the self-control's own adjusted shift is
+0.60 (6 of 16); their difference is −2.37 ms, about 5.6 standard errors. Over 2 868 400 points
that difference is **−0.83 ns per point**. Relative to `prep`'s 28.05 ms at this shape the first
estimate is **−6.3%** and the second **≈ −8.5%**, the gap being the size of the control's own
shift.

The −0.83 ns is a net differential estimate **for this path change**: two `Vec::push`es deleted,
`eval_ts` switched from a copied vector to a slice of the shared grid, one counter update added.
It is not the measured cost of a `Vec::push` instruction, and this measurement cannot separate
those three contributions from each other.

**What that establishes is narrow.** Under the assumptions above there is evidence of a small
improvement in `prep`. The **total evaluator and the end-to-end query were not resolved**: the
evaluator's own A/B reading (−2.10 ms) is indistinguishable from its control's (−1.79 ms), so no
lower bound is claimed for it — `prep` being a subset does not fix what the rest of the evaluator
did. The definite value of the change is that it deletes derivable duplicate data while leaving
the computation semantics unchanged.

**End to end, nothing is resolvable.** Wall clock moves −0.62 ms with a standard error of 1.39;
`/proc` CPU reads −0.00 with a standard error of 9.13, because its ticks are 10 ms against a
~150 ms query. No speed claim is made for the query; the region arithmetic that would put it near
1% is not a measurement of it, and is not quoted as one.

**This is experiment-branch only.** The split branch lets a batch span a skipped gap — its
`PendingBatch` says so in its own comment — so its `indices` is load-bearing and cannot become a
counter until that changes.

### 4.11 A partial `SUM` inside the fused node

Section 4.9 ends with a prediction: collapsing the intermediate rows could address "at most
40.7 ms" of `G4`'s 173.0 ms, "and is well understood". This section is the controlled test of
that, on the query PromQL users actually write — `sum by (instance) (rate(metric[5m]))`.

**What changed.** For that shape DataFusion plans `Aggregate(FinalPartitioned) →
Repartition → Aggregate(Partial) → Filter → Projection → RangeManipulate`, and because PromQL
puts the *evaluation timestamp* in the grouping key, each of the partial aggregate's groups is
exactly one `(label set, grid index)` — the unit the fused node already produces. The hook
therefore replaces the partial `AggregateExec` with the fused node carrying
`partial=sum` (`PartialSum`), which folds every evaluated series into the aggregate's own
`GroupsAccumulator` and emits that accumulator's `state()` once per partition. The state is
DataFusion's, so the `FinalPartitioned` aggregate and the `RepartitionExec` above it are
unchanged and consume it as they would have consumed the partial aggregate's own output.

Three arms, one binary, the environment variable as the only difference (`off`, `on`, `sum`),
120 min at a 1 s step on the 400-series fixture, 3 rounds with the arm order rotated,
2 warmup + 9 timed per arm per round. Binary built from `83e366b33bd`.

**The path is the intended one, on real storage.** `TQL ANALYZE VERBOSE FORMAT JSON` (not
`EXPLAIN`: in standalone it prints `CooperativeExec → MergeScanExec` and never expands the
remote plan, so an EXPLAIN-based check here would certify nothing) reports, for
`sum by (instance) (rate(vmbench[5m]))` at 20 partitions:

```
AggregateExec: mode=FinalPartitioned, gby=[instance@0, ts@1], aggr=[sum(prom_rate(...))]
  RepartitionExec: partitioning=Hash([instance@0, ts@1], 20), input_partitions=20
    FusedSeriesExec: eval=rate, range=300000ms, offset=0ms, maps=[], partial=sum
      SeriesScanExec: source=primary_key, fields=1, partitions=20
```

One aggregate, `FinalPartitioned`; the partial one is gone; the repartition is preserved.
Each arm logs what it should and only what it should: `off` 0 fused, `on` 1 fused with no
partial marker, `sum` 1 fused with `partial=sum`, and the never-fusing `control` 0 on all
three.

**Correctness: three arms, seven shapes, the same answer.** Every arm runs one dedicated
correctness query per case — its own execution, never a timed one — and every pair is compared
series by series and sample by sample: schema, row count, the set of label tuples with each
one's row count, then each series' own timestamp sequence and every value. All seven cases
agree, including the two 2 868 400-row ones.

| case | rows | fused / partial hits (off / on / sum) |
| --- | ---: | --- |
| `G4` `sum by (instance)` | 28 684 | 0/0, 1/0, **1/1** |
| `G400` `sum by (host)` | 2 868 400 | 0/0, 1/0, **1/1** |
| `G1` `sum by (job)` | 7 171 | 0/0, 1/0, **1/1** |
| `n100`, `n1` | 7 171 | 0/0, 1/0, **1/1** |
| `raw` (no aggregation) | 2 868 400 | 0/0, 1/0, **1/0** — nothing to push into |
| `control` (`max_over_time`) | 28 744 | 0/0, 0/0, 0/0 |

`raw`'s `1/0` is the load-bearing negative control *inside this build*: with no aggregation
above the fused node there is no partial aggregate to absorb, so the `sum` arm runs the
identical plan to `on` and its ratio is the harness's own floor.

**The result.**

| case | CPU off | CPU on | CPU sum | `sum/on` | per round | `sum/off` |
| --- | ---: | ---: | ---: | ---: | --- | ---: |
| `G4` `sum by (instance)` | 340 ms | 200 ms | 140 ms | **0.700** | 0.70 / 0.48 / 0.88 | 0.406 |
| `G1` `sum by (job)` | 300 ms | 180 ms | 140 ms | **0.667** | 0.87 / 0.61 / 0.67 | 0.400 |
| `n100` (100 series) | 110 ms | 100 ms | 90 ms | **0.900** | 0.90 / 0.90 / 0.90 | 0.818 |
| `G400` `sum by (host)` | 2 450 ms | 2 350 ms | 2 250 ms | **0.957** | 0.95 / 0.98 / 0.96 | 0.926 |
| `raw` (no aggregation) | 1 410 ms | 1 350 ms | 1 350 ms | 0.993 | 0.99 / 0.99 / 1.04 | 0.957 |
| `n1` (1 series) | 30 ms | 30 ms | 30 ms | 1.000 | 1.33 / 1.00 / 1.00 | 1.000 |
| `control` (`max_over_time`) | 470 ms | 470 ms | 500 ms | 1.298 | 1.30 / 0.70 / 1.39 | 1.064 |

**The direction is unambiguous; the magnitude is not, on the cases that matter most.** On the
three cases with something to collapse, `sum/on` is below 1.00 in **9 of 9 rounds** — `G4`
0.70/0.48/0.88, `G1` 0.87/0.61/0.67, `n100` 0.90/0.90/0.90. No round of any of them is above
the `on` arm. The magnitude is only resolvable where *both* arms repeat, and the gate reports
this pass **not quiet**: `G4`'s `on/off` deviates 60% and its `sum/on` 31%, `G1`'s 42% and 31%.
Those two ratios are therefore quoted with their per-round values and no single number is
claimed from them.

**Where the swing comes from, and it is not the partial arm.** Per arm, per round:

| case | `off` spread | `on` spread | `sum` spread |
| --- | ---: | ---: | ---: |
| `G4` | 18% | **50%** | **14%** |
| `G1` | 53% | **39%** | **14%** |
| `n100` | 9% | 0% | 0% |
| `G400` | 2% | 1% | 1% |
| `raw` | 1% | 4% | 1% |

The `sum` arm is the most repeatable of the three on every fusing case — it reads 130-150 ms
across all six `G4`/`G1` rounds — while the `on` arm swings 170-270 ms on `G4` alone. That is
consistent with what the two do: `on` materialises 2 868 400 intermediate rows per query and
pushes them through a generic `AggregateExec` hash table, `sum` never materialises them. So the
`sum/on` ratio's instability is inherited from its *denominator*, and the partial arm's cheapness
and its repeatability are the same property.

**Against section 4.9's bound.** 4.9 predicted `G4` would land "near 132 ms", i.e. `sum/on` no
lower than about **0.765**, from the aggregate's 38.9 ms share of a 173.0 ms fused arm. This run
reads 0.70 / 0.48 / 0.88: one round above that bound, two below. The bound falls inside the
observed spread, so this pass neither confirms nor refutes it, and no new bound is derived from
it. Two things also make the comparison inexact: the builds differ, and this machine was
noisier — `G4`'s `on` arm reads 200 ms here against 173.0 ms archived, so the fusion gain
itself moved from 0.682 to 0.526. The 4.9 accounting counted the aggregate's own hash path;
the partial step additionally avoids materialising the intermediate rows the aggregate
consumed, which that accounting did not charge to anything. That is a reason to expect the true
saving to exceed the bound, not evidence that it does.

**One small correction to 4.9's "nothing to collapse at 1:1".** `G400` is `sum by (host)`, 1:1,
and 4.9 predicted a partial aggregate would do nothing there. It reads 0.957 with all three
arms stable to 1-2% and all three rounds below 1.00, so the saving is small but real: at 1:1 the
output row count is unchanged, but the ordinary plan still builds a hash table over 2 868 400
rows and copies group columns, where the fused node writes the state directly. 4.3% is above
this shape's own floor (1-2%) and near `raw`'s (1-4% on a shorter query).

**The slot key is load-bearing, and that is verified rather than asserted.** A slot must be
keyed by the *absolute* grid index (`start + offset`), because each series trims its own
interval and a local row number would alias two different grid points. Changing that one
expression to the bare row offset — in both the dense and the sparse branch — makes exactly the
tests written for it fail: `partial_sum_emits_absolute_grid_indices_and_nothing_for_a_missing_partition`,
`partial_sum_keeps_nan_and_infinities` and `partial_sum_keeps_the_duplicate_label_set_rules`,
the last reporting `[(x, 300000), (x, 360000)]` where DataFusion's partial produced
`[(x, 300000), (x, 480000), (x, 540000)]` — two grid points collapsing onto one slot, which is
exactly the aliasing the absolute index prevents. One deliberate wrong expression, three
failures, no change to any other test — which is what makes the absolute-index claim a tested
property instead of a comment.

**What this does not show.** It does not resolve the partial step's magnitude on `G4`/`G1`, as
above. It also says nothing about latency, and the gap between the two is large enough to be
worth stating rather than eliding: wall clock for `sum/off` reads **0.722 / 0.793 / 0.940 /
0.984** on `G4`/`G1`/`n100`/`G400`, against CPU `sum/off` of 0.406 / 0.400 / 0.818 / 0.926.
Moving the aggregation into the scan halves the CPU in one case and removes a fifth of the wall
time, but the query still scans, evaluates and returns the same 2.87 M points' worth of series;
the removed work was never the majority of the elapsed time on a machine with idle cores. It is one fixture, one machine, one binary. And it is a
bench path: the step is selected by `GREPTIME_FUSED_PARTIAL_SUM` and deleted when the formal
planner constructs the same node configuration directly.

## 5. What is not shown

- **No general speedup.** On CPU the fused path now beats the ordinary plan on every `rate`
  shape measured (section 4.7: 0.625-0.783, all 12 paired rounds below 1.00) and ties it where
  there is nothing to collapse (section 4.9: 1:1 grouping and the un-aggregated statement).
  Wall clock moves far less (0.904-0.997): the fused path uses less CPU, not fewer round trips,
  and on a machine with idle cores the wall time was already dominated by other work. There is
  still no evidence here to justify enabling `experimental_native_promql` by default; it
  remains off.
- **The partial `SUM` (section 4.11) is not resolved on the shape it exists for.** Its direction
  is: `sum/on` is below 1.00 in all nine rounds of the three collapsing cases. Its magnitude is
  not: `G4` reads 0.70 / 0.48 / 0.88 and `G1` 0.87 / 0.61 / 0.67, and the gate reports the pass
  not quiet, mostly through the `on` arm's own 50% / 39% round-to-round spread. Only `n100`
  (0.900, three identical rounds) and `G400` (0.957, all arms within 2%) give a magnitude this
  run can support, and neither is the 100:1 shape a PromQL user writes. Section 4.9's 40.7 ms
  bound is neither confirmed nor refuted — the rounds straddle it.
- **The per-point bookkeeping is gone for two functions only.** `rate` and `increase` take
  the batch path; `delta`, `irate`, `*_over_time` and the rest still call the boxed kernel
  once per grid point and gained nothing this round. Section 4.5's 36.9 ns-per-point figure
  was measured on the per-point form and has not been re-measured for the others.
- **The evaluator still pays per input sample.** Every sample is copied into growable
  `Vec`s so that the window can slide by `drain`ing the front, at a measured 3.77 ns/sample.
  That is now a memcpy and nothing more, but a memcpy is still O(samples in range) where the
  ordinary plan copies nothing and does index arithmetic over one shared array. The saving
  from removing the copy is *estimated* (section 4.5) at about 3.6 ns/sample within a
  single-chunk microbenchmark, i.e. 0.8-2.2% on the four measured cases -- inside the drift
  band, so removing it is not expected to show up end to end at these shapes. That estimate
  omits many-chunk retention, capacity growth and reset work, so it is a reason for the
  ordering this round chose, not a bound on the real change.
- **The output path is measured only at these shapes.** Section 4.8 has the counters: case `G`
  emits 400 batches for 400 series (`MAX_ROWS_PER_BATCH` = 4 096 against 7 171 rows per
  series, so the buffer never coalesces two series), `emit` costs 0.5 ns per row and `flush`
  is below the resolution of its own timer. Case `C`, at 120 rows per series, coalesces: 20
  batches carry all 400 series. What is not measured is a shape where the batch budget is
  *small* relative to a series — a series emitting fewer rows than the budget but many series
  arriving interleaved — so no threshold was changed on the strength of these numbers.
- **No large-window ceiling measured.** `W` is the widest window measured (1 h
  against a 2 h span). Nothing here says whether a still-wider window would now
  become *faster* than the ordinary plan rather than merely equal.
- **The profile is usable but lossy, and it needed a timer to finish the job.** Section 4.8's
  corrected pipeline attributes 33-38% to the evaluator and 26% to the aggregate across two runs,
  and recovers 62-72% of the samples the requested rate implies. It could not separate the two
  halves *inside* the evaluator — window preparation and the kernel arithmetic, 47.8-50.5% and
  49.4% of it — because they are adjacent code in one inlined function. The three batch timers
  did separate them, and the answer is that they are equal (45.5-46.2% against 45.2-45.8%), which
  is why no single hotspot is named inside the evaluator. What the timers do **not** do is split
  window preparation itself: whether its 9.6-9.8 ns per point is metadata traffic or the cursor
  scans is arithmetic here, not measurement. Section 4.10 deletes two of the four metadata
  vectors and measures a net 0.83 ns per point for the whole path change, under that section's
  drift-reference assumptions. That is not a split of the traffic either: it bundles the two
  deleted appends with the switch to a borrowed grid and one added counter update, and it does not
  say how much of the 9.6-9.8 ns the remaining metadata accounts for.
- **Nothing here splits the scan into region read, memtable and merge.** The scan is 17.8-18.6%
  of the query and the profile attributes it to `mito2` as one region. Its sub-parts are not
  separated, and the block timers cannot either: `SeriesScanExec` reports one `scan_cost`.
- **No `EXPLAIN` visibility from the frontend.** In standalone, TQL is planned as
  a `MergeScan` pushed to the datanode, and fusion happens when the datanode
  builds its physical plan. `TQL EXPLAIN` at the frontend therefore never shows
  `FusedSeriesExec`; the debug log is the way to confirm a fused run.
- **No distributed or multi-region path.** Phase 1 is single-region,
  single-node. `RangeEvaluator` has no partial-aggregate contract, and one was
  deliberately not invented here.
- **No `PartialAggregate`, vector matching, or planner-level fusion.** Out of
  scope by design.
- **Resets are now covered for correctness, not for timing.** Section 2's sixth layer adds
  three shapes through real storage: a counter resetting every 48 samples (so 24 resets per
  series, at 5 m and 1 h windows), one with gaps and a 30 min outage, and fragmented chunks.
  All agree across arms, and `vfrag`/`vmreset` — identical values, different fragmentation —
  return bit-identical results on the fused path. What is still missing is a **timed**
  measurement at that shape: these tables carry 40 series and each query costs 70-160 ms of
  CPU on a 10 ms tick, so their ratios do not resolve, and no end-to-end *timing* here includes
  reset traffic. The 400-series fixture has no resets at all.
- **The batch path's own unit of work was not measured directly.** Section 4.6
  measures whole queries before and after; there is no evaluator-only criterion number for
  the batch form, so the per-point bookkeeping figure (36.9 ns) still describes the form
  that `rate` and `increase` no longer use.
- **No wall-clock win worth quoting.** Section 4.7 has the fused arm at 0.904-0.997 of the
  ordinary plan's wall time while its CPU is 0.625-0.783; section 4.9 reads 0.853-1.026 on
  wall against 0.682-1.000 on CPU, including two cases above 1.00. The query is not
  CPU-bound, so a third of the CPU buys at most a tenth of the latency, and at `raw` and
  `G400` it buys nothing measurable (1.026 and 1.005). Anyone who needs a latency result
  needs a different measurement.
- **The margins do not carry to other functions.** Sections 4.7 and 4.9 cover `rate` (and
  `max_over_time` as a control) on one machine. Every range function other than `rate` and
  `increase` still evaluates one boxed call per grid point, so nothing here says what `delta`,
  `irate`, `*_over_time` or the predict functions would do.

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

# Section 4.3: the interleaved L/N/F order, three rounds, all cases.
bash scripts/fused_series_bench_crossed.sh

# The tables of section 4.3, from the raw records:
python3 scripts/fused_series_bench_summarize.py records
```

Section 4.6 uses the two-arm companion, because both of its arms are flag-on:

```bash
# P and F are two commits; both run with --arm on. The arm order rotates per round.
BIN_P=<parent binary> COMMIT_P=<parent sha> \
BIN_F=<this binary>  COMMIT_F=<this sha> \
  bash scripts/fused_series_bench_pair.sh

# Its tables, from the raw records. The ratio is computed as F/P with both arms named,
# not from their position in a sorted list.
python3 scripts/fused_series_bench_pair_summarize.py <run directory>
```

Section 4.7 is the same driver with both arms pointing at one binary and the native flag as
the variable:

```bash
BIN_P=<binary> BIN_F=<same binary> \
COMMIT_P=<sha>-off COMMIT_F=<sha>-on ARM_P=off ARM_F=on \
  bash scripts/fused_series_bench_pair.sh
```

The driver refuses a flag-off arm that logs a fused selection, so an arm cannot silently
measure the other's path; that check is what makes this comparison safe.

The timing records of the run quoted in section 4.6 are committed under
`docs/bench-records/20261018T073117Z/`, so those medians and per-round ratios can be
recomputed rather than taken on trust:

```bash
python3 scripts/fused_series_bench_pair_summarize.py docs/bench-records/20261018T073117Z
```

Sections 4.8 and 4.9 come from a third set of scripts, all of which take the same
`--data-home`/`--bin` and write JSON:

```bash
# 4.9: every case, both arms, three rounds with the arm order rotated.
python3 scripts/fused_series_scale.py --data-home <dir> --bin <greptime> \
    --rounds 3 --warmup 2 --iterations 9 --out scale.json

# 4.8: the same statement at two grid densities, one ANALYZE each, then the per-operator
# budget. The two steps scan the same samples, so the difference is cost per added row.
python3 scripts/fused_series_profile.py --data-home <dir> --bin <greptime> --case G \
    --arm on --analyze-only --analyze-out analyze-G.json
python3 scripts/fused_series_profile.py --data-home <dir> --bin <greptime> --case C \
    --arm on --analyze-only --analyze-out analyze-C.json
python3 scripts/fused_plan_budget.py analyze-G.json --json budget-G.json
```

Section 4.10 compares two binaries and needs the same-binary control to mean anything:

```bash
# From the repository root. Each run writes its JSON into the archive directory.
ARCH=docs/bench-records/20261018T143000Z
for case in W G; do
  python3 scripts/fused_series_regions.py $OLD $DATA $case 9 --timed 5 \
      --compare-bin $NEW --rounds 8 --json $ARCH/$case-8r.json
  python3 scripts/fused_series_regions.py $OLD $DATA $case 9 --timed 5 \
      --compare-bin $OLD --rounds 8 --json $ARCH/$case-self8.json
done
# The tables of that section, from the four runs above.
python3 $ARCH/analyse.py $ARCH/W-8r.json $ARCH/W-self8.json \
    $ARCH/G-8r.json $ARCH/G-self8.json
```

Section 4.11 adds a third arm to the 4.9 driver, so it is the same command with
`--arms off,on,sum`; the three arms are one binary and one environment variable
(`GREPTIME_FUSED_PARTIAL_SUM`), which is what makes `sum/on` isolate the partial step alone:

```bash
python3 scripts/fused_series_scale.py --data-home <dir> --bin <greptime> \
    --commit <sha> --arms off,on,sum --rounds 3 --warmup 2 --iterations 9 \
    --out docs/bench-records/20261019T0212Z/scale-3arm.json

# The plan each arm actually runs, and that all three return the same answer. Needs
# TQL ANALYZE: in standalone TQL EXPLAIN reports only `CooperativeExec -> MergeScanExec`
# and never expands the remote plan, so it cannot show which operators ran.
python3 scripts/fused_series_partial_plan.py --data-home <dir> --bin <greptime> --case C \
    | tee docs/bench-records/20261019T0212Z/partial-plan-C.txt

# The comparator that run relies on, with no server: reordering tolerated, and a changed
# value, timestamp, dropped sample, dropped series, changed label or schema still caught.
python3 scripts/fused_series_bench.py selftest
```

The three data shapes of section 2 are seeded and measured by
`scripts/fused_series_shapes.py --data-home <dir> --bin <greptime> --out shapes.json`, which
refuses to seed into a directory already holding a different shape and refuses to report a case
that returned no rows.

`fused_plan_budget.py` prints each operator's *own* counters and refuses to sum them across the
tree, which is the mistake section 4.4 also documents. The CPU profile that section 4.8 rejects,
and the summary that attributes its stacks, are
`scripts/fused_series_profile.py --seconds 60` and `scripts/fused_profile_summarize.py`.

Section 4.11's three-arm pass is in `docs/bench-records/20261019T0212Z/`, with the per-iteration
samples behind every median. It writes no per-arm result files: the three arms are compared
in-process by `check_arms` on every case, and a disagreement stops the run before it records
anything, so a saved copy would only restate a check that already passed — and at 2 868 400 rows
per arm it would not be archivable anyway.

Sections 4.8 and 4.9's numbers are in `docs/bench-records/20261018T122000Z/`, with the
per-iteration samples behind every median; section 2's three data shapes and the
`vfrag`/`vmreset` comparison are in `docs/bench-records/20261018T124000Z/`; and the raw text
profiles behind section 4.8's corrected shares, gzipped exactly as the server wrote them, are in
`docs/bench-records/20261018T131500Z/`. Each has a `RESULTS.md` naming which figure comes from
which file, and where a measurement was rejected, why. The profile post-processing has a
self-test, so the arithmetic that was wrong twice can be checked in one command:

```bash
python3 scripts/fused_profile_summarize.py --self-test
gunzip -c docs/bench-records/20261018T131500Z/W-on3.txt.gz > /tmp/W-on3.txt
# the owner table, the capture rate, and the leaf-frame split inside the evaluator
python3 scripts/fused_profile_summarize.py /tmp/W-on3.txt --cpu-seconds 214.96 --frequency 199 \
    --inside 'fused: evaluator'
```

The correctness *results* of that run are ~300 KB each and are not committed;
`docs/bench-records/20261018T073117Z/RESULTS.md` lists each one's row count and sha256 so a
copy can be checked against what was actually compared.

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
