# FusedSeriesExec: implementation report and measured performance

Status of the `feat/narthex-integrated-chain` branch after bringing up an
end-to-end fused range-function path. This document records what was built,
what was measured, and — importantly — what the measurements do **not** show.

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

## 2. Correctness

Four independent layers, all currently green.

1. **Node unit tests** — `src/promql/src/extension_plan/fused_series/tests.rs`
2. **Real storage scan** — `tests-integration/.../nested_merge_scan_capability_test.rs`,
   `test_fused_series_exec_over_real_storage_scan`: a real mito2 region whose
   labels are bound to the primary key. Asserts, against values derived from the
   inserted fixture rather than from re-running the implementation under test:
   `rate == 1.0` on five grid points, `host == "alpha"` (proving primary-key
   label decoding), and the exact timestamps `[300000, 360000, 420000, 480000,
   540000]`.
3. **Real PromQL query through the planner** — `a_real_rate_query_runs_through_the_fused_node`
   and `a_real_aggregated_rate_query_runs_through_the_fused_node`: build the plan
   through `PromPlanner`, plan physically with the engine's own planner (which is
   what installs `PromExtensionPlanner`), assert the result really contains a
   `FusedSeriesExec`, then execute both arms and compare rows.
4. **Standalone server A/B** — five queries, fused vs ordinary, row sets compared.
   All five `identical=True`.

`cargo nextest run -p promql` → 454 passed.
`cargo nextest run -p query` → 885 passed.
`cargo clippy -p promql -p query --all-targets -- -D warnings` → clean.

### Window semantics

The fused path follows Prometheus' `(E - range, E]` — left-open, right-closed —
as documented and implemented in `src/promql/src/range_eval.rs`. The ordinary
fold's `scan_ranges` (`src/promql/src/extension_plan/range_manipulate.rs`) uses
the same `<= window_start` exclusion, so the two agree. Where the two
implementations differ is the last bits of a float sum, because they accumulate
in a different order; the standalone A/B above confirms row identity under a
relative tolerance of 1e-9.

## 3. Bugs found by measuring, not by testing

Both of these compiled, passed every unit test, and silently disabled fusion for
the queries that matter. They are recorded because they are the reason the first
two benchmark runs produced meaningless numbers.

1. **`sum by (...)(rate(...))` never fused.** The matcher walked a logical
   child-index path to find the physical node to swap. DataFusion plans one
   logical `Aggregate` as `AggregateExec(Partial) → RepartitionExec →
   AggregateExec(Final)`, so the lockstep walk desynchronised and returned
   `None`. Fixed by identifying the physical node by schema and content.
2. **A tag subset was rejected.** The builder required the fused output to carry
   *every* table tag. A `sum by (one_label)` projection is pruned to the labels
   the aggregate groups by, so on any table with more than one tag — that is,
   every real table — the gate rejected the most common PromQL query shape. The
   single-tag test table could not show it, because there the aggregate happens
   to project all tags. Fixed by requiring one time column and one value column
   and letting the projection decide the tag columns; duplicate detection still
   compares each series' complete label set, so no identity is lost.

## 4. Performance

### 4.1 Method

Two standalone servers, one with `experimental_native_promql = true` and one with
`false`, over the same data written to disk, run **sequentially** (the data set
does not fit twice on this machine). Each query: 2 warmup iterations, 11 timed
iterations, wall clock over HTTP. A control query that phase 1 never fuses
(`max_over_time`, which is not a range fold over windowed arrays in the same
sense) runs the identical plan on both arms and therefore measures the noise
floor of the comparison.

Data: 400 series × 2880 samples at 15 s = 1,152,000 rows, one mito region.
Query span: 2 h or 12 h, 1-minute grid.

### 4.2 Results

| query | rows | fused hits | ordinary p50 | fused p50 | ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| `small-agg-rate-5m` | 480 | 14 | 26.1 ms | 24.4 ms | 1.07x |
| `small-bare-rate-5m` | 48 000 | 14 | 64.5 ms | 61.8 ms | 1.04x |
| `large-agg-rate-5m` | 2 880 | 14 | 79.9 ms | 78.5 ms | 1.02x |
| `large-agg-rate-1h` | 2 880 | 14 | 80.7 ms | 79.7 ms | 1.01x |
| **`control-max-over-time`** | 2 880 | 0 | 81.2 ms | 78.1 ms | **1.04x** |

`fused hits` is the count of `Fused series selection succeeded` lines emitted
during that query's timed iterations, so a row with 0 hits is not a measurement
of fusion at all. The control row has 0 by construction.

### 4.3 Reading

**The comparison does not show a benefit.** The control query — which runs the
identical plan on both arms — "improves" by 1.04x. Every fused query lands
between 1.01x and 1.07x, i.e. inside or barely outside that floor. Since the two
arms run sequentially, part of that 1.04x is warm-up drift: the ordinary arm
always runs second and is consistently the slower one across every query
including the control.

Two further observations point the same way:

- **The window width barely matters.** `rate[5m]` at 79.9 ms and `rate[1h]` at
  80.7 ms, despite a 12x larger window. Whatever dominates these queries is not
  the per-window work that fusion removes.
- **The removed work is small in absolute terms.** Per-operator figures from
  `TQL ANALYZE VERBOSE` on the ordinary plan at this scale:
  `PromSeriesDivideExec` 12 ms, `PromRangeManipulateExec` 1.0 ms, range-UDF
  `ProjectionExec` 0.98 ms, `FilterExec` 0.43 ms — against a scan spread over
  13 partitions. The fused path can eliminate roughly 15 ms of CPU that is
  already spread across partitions; it cannot touch the scan, the aggregation,
  or the JSON serialisation of the result, which dominate the wall clock.

### 4.4 What earlier, discarded runs showed

Kept for context, because the numbers moved a lot once the bugs above were
fixed — an illustration of why the hit counter is mandatory:

- Before the aggregate-matcher fix, `sum by (...)` did not fuse at all, so its
  row in the table compared the ordinary plan against itself.
- Before the tag-subset fix, on a 400-series multi-tag table, *no* `sum by`
  query fused. The measured ratios (0.97x–1.06x) and a `MISMATCHED RESULT SETS`
  correctness line were both artifacts of this.

## 5. What is not shown

- **No proven speedup.** In this data layout, at these query sizes, fusion
  measured neutral. There is no evidence here to justify enabling it by default,
  and `experimental_native_promql` remains off by default.
- **No test of a compute-bound regime.** Everything measured is scan- and
  serialisation-bound. A regime where window iteration dominates — all data in
  memory, or `[1d]`-class windows — was not measured. That would establish the
  ceiling of the approach, but it is not the o11ybench query shape.
- **No `EXPLAIN` visibility from the frontend.** In standalone, TQL is planned as
  a `MergeScan` pushed to the datanode, and fusion happens when the datanode
  builds its physical plan. `TQL EXPLAIN` at the frontend therefore never shows
  `FusedSeriesExec`; the debug log is the way to confirm a fused run. Surfacing
  this through `EXPLAIN ANALYZE` remote plans was not done.
- **No distributed or multi-region path.** Phase 1 is single-region,
  single-node. `RangeEvaluator` has no partial-aggregate contract, and one was
  deliberately not invented here.
- **No `PartialAggregate`, vector matching, or planner-level fusion.** Out of
  scope by design.

## 6. Reproduction

The benchmark driver is not committed (it is scaffolding, and it writes ~18 GB
of data). The shape of it is:

1. Seed one mito table with `host, instance, job, l1` tags, one value column, a
   time index, and `PRIMARY KEY(host, instance, job, l1)`, one row per series per
   15 s.
2. Write that data directory once; copy it is not needed if the arms run
   sequentially against it.
3. Start `greptime standalone start` twice, once per flag value, on disjoint
   port sets (`--http-addr`, `--grpc-bind-addr`, `--mysql-addr`,
   `--postgres-addr`; two instances otherwise collide on the defaults).
4. Run each query with `TQL EVAL (start, end, '1m') <expr>` over
   `POST /v1/sql` form-encoded, alternating or sequential, and count
   `Fused series selection succeeded` lines per query to prove which arm is
   which.
5. Compare row sets with a float tolerance before trusting any timing.

Two traps worth recording: TQL window arguments are **seconds** while `INSERT`
literals are **milliseconds** (a mismatch puts the data in 1970 and every query
returns zero rows while `SELECT count(*)` looks healthy), and `region`, `zone`
and `mode` are reserved words and cannot be used as column names unquoted.
