# FusedSeriesExec: implementation report and measured performance

Status of the `feat/narthex-integrated-chain` branch after bringing up an
end-to-end fused range-function path. This document records what was built,
what was measured, and — importantly — what the measurements do **not** show.

**Headline: the node is correct, and it is faster only in one narrow shape
(fine step, small window, 1.38x). At fine steps with a wider window or a longer
span it is a regression (0.42x-0.78x), because its kernel evaluation is
`O(grid points x samples per window)`. See section 4.3.** Do not enable it by
default on the strength of this work.

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

### 4.1 Where the work actually is

A per-operator profile of the ordinary plan (`TQL ANALYZE VERBOSE`, same data)
showed that the fusion target is only worth anything when the query is
compute-bound, and that **the window width is not what makes it compute-bound**:

| query | SeriesScan | all compute operators | compute share |
| --- | ---: | ---: | ---: |
| `rate[5m]`, step 1m | 243 ms | 38 ms | 13% |
| `rate[1h]`, step 1m | 363 ms | 37 ms | 9% |
| `rate[4h]`, step 1m | 206 ms | 33 ms | 14% |
| `rate[5m]`, step 1s | 89 ms | 61 ms | 41% |
| `rate[1h]`, step 1s | 70 ms | 50 ms | 42% |

Widening the window 48x (5m to 4h) leaves compute unchanged at 1-minute steps,
which is why the first benchmark's `rate[5m]` and `rate[1h]` rows were identical
to within noise. The lever is the **step**: a 1-second step multiplies grid
points by 15 and lifts the compute share to 41%. Any honest test of this node
must use a fine step, not a wide window.

### 4.2 Results, compute-bound regime

400 series x 2880 samples, `sum by (instance)`, two standalone servers run
sequentially over the same data directory. 2 warmup, 9 timed iterations. All
row sets verified identical (relative tolerance 1e-9) before timing was read.

| query | rows | fused hits | ordinary p50 | fused p50 | ratio |
| --- | ---: | ---: | ---: | ---: | ---: |
| step 1s, `rate[5m]`, 30 min span (1800 steps) | 7 204 | 12 | 28.3 ms | 20.4 ms | **1.38x** |
| step 1s, `rate[5m]`, 2 h span (7200 steps) | 28 804 | 12 | 46.8 ms | 60.1 ms | **0.78x** |
| step 1s, `rate[1h]`, 2 h span | 28 804 | 12 | 49.4 ms | 118.5 ms | **0.42x** |
| step 1m, `rate[5m]`, 2 h span | 484 | 12 | 24.9 ms | 27.0 ms | 0.92x |
| `control-max-over-time` (never fuses) | 28 804 | 0 | 61.3 ms | 57.1 ms | 1.07x |

`fused hits` counts `Fused series selection succeeded` lines emitted during that
query's iterations, so every fused row here really ran the fused node.

### 4.3 Reading: a win that turns into a regression as the grid grows

The small compute-bound case is the only one where fusion helps, and it helps by
a margin well above the 1.07x noise floor.

It then reverses. Going from 1 800 to 7 200 grid points (4x) raises the fused
time by 2.9x but the ordinary time by only 1.65x. Widening the window from 5m to
1h at 1-second steps **doubles** the fused time (60.1 to 118.5 ms) while the
ordinary plan barely moves (46.8 to 49.4 ms). The spread confirms this is not
noise: fused samples run 105.8-167.8 ms against ordinary 42.5-87.3 ms.

**Mechanism, confirmed in the code.** `RangeEvaluator::evaluate_row`
(`src/promql/src/range_eval.rs`) hands the kernel the whole window slice for
every grid point:

```rust
let len = timestamps.partition_point(|&timestamp| timestamp <= window_end);
kernel(FloatWindow { timestamps: &timestamps[..len], values: ..., ... })
```

The `rate` kernel then walks every value in that slice. The retained-sample
buffer is advanced by a cursor, so sample *ingestion* is linear, but kernel
*evaluation* is `O(grid points x samples per window)`:

- 1s step, 5m window, 30 min span: 1800 x 20 x 400 = 14 M visits -> 20 ms
- 1s step, 1h window, 2 h span: 7200 x 240 x 400 = 691 M visits -> 118 ms

The ratio of visits (49x) and the ratio of measured time (5.8x) are the same
order once fixed per-call overhead is accounted for. The ordinary path
materialises each window once in `RangeManipulate` and evaluates it in
`prom_rate`, and its measured cost is far less sensitive to the window, so the
per-grid-point rescan is the difference.

**Therefore: the node is correct but its evaluation strategy does not scale.**
It should either be restricted to shapes where `grid points x window samples`
stays small, or the kernel contract needs an incremental/aggregate form so that
a sliding window does not rescan its overlap. The second is the real fix and is
out of scope here.

### 4.4 Scan-bound shapes

At 1-minute steps the compute share is 9-14% and the node measures neutral
(0.92x), inside the noise floor. This is expected and is not a defect: it
removes work that was never the bottleneck.

### 4.4 What earlier, discarded runs showed

Kept for context, because the numbers moved a lot once the bugs above were
fixed — an illustration of why the hit counter is mandatory:

- Before the aggregate-matcher fix, `sum by (...)` did not fuse at all, so its
  row in the table compared the ordinary plan against itself.
- Before the tag-subset fix, on a 400-series multi-tag table, *no* `sum by`
  query fused. The measured ratios (0.97x–1.06x) and a `MISMATCHED RESULT SETS`
  correctness line were both artifacts of this.

## 5. What is not shown

- **No general speedup.** Fusion helps one narrow shape (fine step, small
  window) and regresses two larger ones. There is no evidence here to justify
  enabling it by default; `experimental_native_promql` remains off.
- **The scaling fix is not attempted.** Section 4.3 identifies
  `O(grid x window)` kernel evaluation as the cause. Making the kernel
  incremental — so a sliding window reuses the previous point's work instead of
  rescanning its overlap — is the change that would make fusion pay off at fine
  steps and wide windows. Not done.
- **No large-window ceiling measured.** Whether an incremental kernel would make
  wide windows *faster* than the ordinary materialising path, rather than merely
  no worse, is unmeasured.
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
4. Run each query with `TQL EVAL (start, end, '<step>') <expr>` over
   `POST /v1/sql` form-encoded, alternating or sequential, and count
   `Fused series selection succeeded` lines per query to prove which arm is
   which.
5. Compare row sets with a float tolerance before trusting any timing.

Two traps worth recording: TQL window arguments are **seconds** while `INSERT`
literals are **milliseconds** (a mismatch puts the data in 1970 and every query
returns zero rows while `SELECT count(*)` looks healthy), and `region`, `zone`
and `mode` are reserved words and cannot be used as column names unquoted.
