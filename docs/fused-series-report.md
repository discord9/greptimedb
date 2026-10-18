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
`max_over_time` as a 1% noise floor. The remaining gap is not range-function
arithmetic: it is the raw series scan, 235 of the 279 ms of operator compute left
in the fused plan. See section 4.4. `experimental_native_promql` stays off.**

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

### 1.3 The counter-reset index — this round's change

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
this round removes, and it means a regression cannot be blamed on reset
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

- **`W` is the case this round was for.** The wide-window regression went from
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
yet; what this round shows is that it no longer loses badly on the wide-window
one.

### 4.4 What the remaining gap is

`TQL ANALYZE VERBOSE` on `G`, median of the last 3 of 5 runs, per-operator
`elapsed_compute`. Only the operators that matter are listed.

| arm | operator | compute |
| --- | --- | ---: |
| L | `SeriesScan` | 378 ms |
| L | `ProjectionExec` | 51 ms |
| L | `AggregateExec` | 40 ms |
| L | `PromRangeManipulateExec` | 35 ms |
| L | `PromSeriesDivideExec` | 26 ms |
| L | `FilterExec` | 3 ms |
| | **sum** | **537 ms** |
| F | `SeriesScanExec` | 235 ms |
| F | `AggregateExec` | 41 ms |
| F | `SortPreservingMergeExec` | 2 ms |
| F | `FilterExec` | 0.4 ms |
| | **sum** | **279 ms** |

Two facts follow, and they answer "what is left":

1. **The fused scan is already cheaper than the ordinary one** — 235 ms against
   378 ms. The ordinary `SeriesScan` expands 192 000 raw rows into the columnar
   path (`output_rows: 192 000`); the fused `SeriesScanExec` hands over 424 raw
   chunks that never become columns. That is 143 ms the fused path does not
   spend, and it is why `W` reaches 0.90x at all.
2. **The fused plan has no range-function operator left to profile.** In the L
   plan, `PromRangeManipulate` (35 ms) + `ProjectionExec` (51 ms) + `FilterExec`
   (3 ms) + `PromSeriesDivide` (26 ms) is 115 ms of materialising work between
   the scan and the aggregate. In the F plan, that work happens inside
   `SeriesScanExec`, and the only node left above it is the aggregate, which the
   two arms share (41 ms vs 40 ms). So F's 279 ms is ~235 ms scan + ~41 ms
   aggregate, and the end-to-end 58.1 ms is that 279 ms of operator compute
   divided across 20 partitions, plus response serialization.

So the remaining gap in `G`/`S` is **not** in the range function. It is the raw
series scan — 235 of the 279 ms — which is the next distinct target, and it is
out of scope for this round.

## 5. What is not shown

- **No general speedup.** The algorithm regression is eliminated and one shape
  (`W`) improved 2.08x between N and F, but the fused path is still 0.88x-0.93x
  against the ordinary plan at fine steps with narrow windows. There is no
  evidence here to justify enabling `experimental_native_promql` by default; it
  remains off.
- **The remaining cost is located, not fixed.** Section 4.4 shows the raw series
  scan is 235 of the 279 ms of `G`'s operator compute. Making that cheaper is the
  next change, and it is a different one.
- **The output path was not measured.** The node targets 4 096 rows per batch
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
