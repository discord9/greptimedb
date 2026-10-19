---
Feature Name: Fused Execution Model for Native PromQL
Tracking Issue: N/A
Date: 2026-10-19
Author: @discord9
---

# RFC: Fused Execution Model for Native PromQL

# Summary

PromQL range functions (`rate`, `increase`, `sum_over_time`, ...) are currently evaluated by a
chain of physical nodes that materialises intermediate row batches between every step. A native
path evaluates them directly over the typed series the storage layer produces.

This RFC proposes the execution model for that path. It covers what a fused evaluation may
cover, how batch data reaches it, where state lives, and what it emits. It deliberately proposes
no final Rust struct layout: the model has to be agreed before the types are worth fixing.

One property motivates the whole design. **Function selection and input binding belong in
physical compilation; the hot path must only feed batches to an already bound kernel.**

# Motivation

## What the native path replaces

For `rate(x[5m])` the ordinary plan builds, per grid point and per series, a window of samples
assembled from row batches. Every stage between the scan and the result is a `RecordBatch` with
its own allocation, projection and predicate.

The raw-series path can skip that: storage produces typed series chunks
(`RawSeriesChunk`, `datatypes/src/series/raw.rs:96`), and a range function over one series is a
windowed scan of two columns. Evaluated series (`Series`,
`datatypes/src/series/samples.rs:613`) carry a `TimeAxis::Grid`
(`datatypes/src/series/time.rs:172`) instead of per-row timestamps, so the grid is a property of
the schema rather than a column.

## What the current prototype gets wrong

An earlier prototype fused a range-function subtree by *recognising it after the physical plan
existed* and swapping a fused node into the located position. That has three problems:

1. **The opportunity is rediscovered from a physical tree shape.** Any change in how the
   optimizer arranges that subtree silently makes the fused path unreachable.
2. **The fused node is a fixed template.** It carries a function, an optional map chain and an
   optional partial aggregate as fields, so adding a shape widens the template rather than
   composing a computation.
3. **Output columns are classified at run time** by claiming "the next unclaimed `Float64`
   column" for the next field, which is only unambiguous while there is exactly one field.

This RFC keeps the prototype's *kernels* and drops its *structure*.

# Guide-level explanation

A query is compiled into one or more **FusedUnit**s. A unit is a linear chain:

```
raw scan
  → at most one window function      rate, increase, sum_over_time, ...
  → any number of point-wise maps    abs, ceil, floor, sqrt, round
  → at most one partial aggregate    sum by (...)
```

A unit is the smallest piece that pays for itself: everything inside it is evaluated over typed
series without materialising a row batch, and everything outside it is an ordinary physical node.

A unit's inputs are typed series, not rows. Its outputs are typed series, or — when it ends in a
partial aggregate — a batch with a fixed, compilation-time column layout.

# Reference-level explanation

## 1. Fusion scope and boundaries

### 1.1 What one unit may contain

A `FusedUnit` covers a linear chain over a **single input** that satisfies all of:

- **One input.** Both the window function and the map chain are single-input operations.
- **No partition exchange.** No shuffle may occur inside a unit.
- **No cross-series matching.** Vector-to-vector binary operators, `on()`/`ignoring()`
  matching, and set operators stay outside.
- **One evaluation grid.** Every stage shares one `GridSpec` (start, step, len).

A unit that cannot satisfy these is not fused; the ordinary physical nodes run instead.

### 1.2 Where the chain must be cut

| Reason to cut | Example | Handled by |
| --- | --- | --- |
| Partition exchange | `sum by (job) (...)` when `job` is not a partition key | existing `AggregateExec`, two-phase |
| Grid change | subquery `[1h:1m]` inner grid | inner unit + an explicit grid adaptation |
| Cross-series matching | `a + on(x) b` | existing join / vector-match node |
| Scalar broadcast | `... + 1` | existing `ProjectionExec` |
| Non-point-wise function | `label_replace`, `absent` | existing dedicated nodes |

### 1.3 Multiple inputs, branches, shared subexpressions

Not supported in the first phase, deliberately.

A multi-input unit needs a rule for which input's arrival advances the window, and a rule for
merging completion signals. `src/promql/src/source/merge.rs` already merges ordered inputs, but
it serves the row-batch pipeline; reusing it would reintroduce the coupling this design removes.

A subexpression referenced twice is recomputed in the first phase. No common-subexpression
elimination.

## 2. Batch data interface

### 2.1 Inputs

Two input kinds, reusing existing types:

- **Raw**: a stream of `RawSeriesChunk`
  (`datatypes/src/series/raw.rs:96`), which carries
  `primary_key: Buffer`, `metric: Option<MetricName>`, `time: RawTimestamps`,
  `fields: SeriesFields`, and `is_last: bool`.
- **Evaluated**: a stream of `Series` (`datatypes/src/series/samples.rs:613`), such as the inner
  result of a subquery, arriving one complete series at a time.

**`is_last` is the series-completion signal** (`datatypes/src/series/raw.rs:109`). No new signal
type is needed. Note its documented caveat: a merge step must recompute finalisation across its
inputs, which matters only once multi-input units exist.

**Validity** is not carried separately. Fields are Arrow arrays, so a null is an invalid sample.
Stale markers are kept as values on the evaluated-input path, which `push_series` already does.

**Grid position** is expressed by `SeriesSchema::eval_grid` plus `GridRange`, not as a per-row
column.

### 2.2 Window evaluation does not wait for a whole series

This is a property of the existing evaluator that the design must keep.

A window `(T - range_ms, T]` only needs samples later than `T - range_ms`. So on a chunk that is
not the last of its series, `RangeEvaluator::push` (`src/promql/src/range_eval.rs:227`) advances
the grid only as far as `last_ts + offset_ms` and returns `None`
(`src/promql/src/range_eval.rs:236-242`). Only the `is_last` chunk finalises the series.

Consequently a unit emits **several batches per series**, and a consumer must accept multiple
batches for one series. `SeriesOrdering::Contiguous`
(`src/datatypes/src/series.rs:40`) already guarantees each series is contiguous, which is exactly
the guarantee such a consumer needs.

### 2.3 Batch granularity

The hot path works on whole batches, never per sample and never per grid point:

- Input arrives as Arrow arrays inside `SeriesFields`; nothing is rebuilt sample by sample.
- A window function is evaluated over a range of grid positions at once.
- A map chain applies to a whole column.

## 3. State ownership

### 3.1 Compiled plan and run-time state are separate

```rust
// Illustrative, not a final layout.
struct FusedPlan {                 // immutable, shared by Arc across partitions
    kernel: KernelId,              // a selected function, not a captured closure
    window: RangeWindow,
    maps: Arc<[MathOp]>,
    grid: Arc<EvalGrid>,
    input_schema: Arc<SeriesSchema>,
    output_schema: Arc<SeriesSchema>,
    time_column: String,
    value_columns: Arc<[String]>,
    label_columns: Arc<[String]>,
    partial_agg: Option<PartialAggSpec>,
}

struct FusedState {                // created per execute(partition)
    evaluator: RangeEvaluator,
    agg: Option<PartialAggState>,
}
```

- `FusedPlan` is referenced by `Arc`, not copied per partition.
- `FusedState` is created inside `execute(partition)` and dropped when the stream ends.
- Per-series state (`SeriesState` in `src/promql/src/range_eval.rs:92`) is cleared on `is_last`,
  so nothing leaks across series.

`RangeEvaluator` already has this shape: `try_new` fixes the plan, `push` / `push_series` advance
it, `finish` validates the ending. It is the unit's kernel and does not need rewriting.

### 3.2 Partial aggregate state

A `PartialAggState` holds one accumulator per group key over the series of one partition:

- **Input**: evaluated `Series`.
- **State**: map from group key to an accumulator (`f64` plus validity).
- **Release**: emitted once, when the partition's stream ends.
- **Not shared across partitions.** Cross-partition merging belongs to the existing two-phase
  aggregation; a unit only produces partial state.

## 4. Output types and boundaries

| Unit boundary | Output | Type |
| --- | --- | --- |
| window function + map chain | native evaluated series | `Series` with `TimeAxis::Grid` |
| partial aggregate | partial aggregate state | batch with a fixed column layout |
| surrounding physical nodes | ordinary result rows | Arrow `RecordBatch` |

**Column binding is produced by physical compilation, not inferred at run time.**
`FusedPlan::time_column`, `value_columns` and `label_columns` are fixed when the unit is
compiled; nothing inspects a schema during execution to decide a layout. This is what removes the
prototype's `classify_output` heuristic.

# Query walkthroughs

## `abs(x)`

There is no window function, only a point-wise map. The first phase **does not fuse this**: the
gain of a fused unit comes from windowed evaluation, and `abs(x)` has none. It runs as the
existing selector plus projection.

The case is still worth stating because it fixes a boundary: maps alone are not a reason to fuse.

## `abs(rate(x[5m]))`

```
FusedUnit {
  input:   Raw(scan x)
  kernel:  Rate, window = { range: 5m, offset: 0 }
  maps:    [Abs]
  grid:    the statement's grid (start, step, len)
  output:  Series with TimeAxis::Grid
}
```

One unit, one input, no aggregate. State: the evaluator's window buffers and cursor, plus the
output builder. A surrounding projection turns the emitted `Series` into result rows.

## `sum by (job) (rate(x[5m]))`

```
FusedUnit {
  input:       Raw(scan x)
  kernel:      Rate, window = 5m
  maps:        []
  partial_agg: Sum { group_by: [job] }
  output:      partial state (job, accumulator)
}
  → existing AggregateExec (final)
```

Whether `job` is a partition key decides whether a shuffle is needed. Either way the unit produces
partial state and the existing aggregate finishes.

**The first phase should implement only "partial state + existing final aggregate"**, letting
DataFusion own the exchange. That keeps the design surface minimal.

## `sum by (job) (rate(x[5m])) + 1`

```
FusedUnit { kernel: Rate, partial_agg: Sum { group_by: [job] } }
  → existing AggregateExec (final: job, sum)
  → existing ProjectionExec (sum + 1)
```

**The `+ 1` acts on the final result rows as a scalar broadcast, not on partial state.** The
aggregate's output is the input to the projection.

The design consequence is the point of cutting here: a unit does not need to know `+ 1` exists.

## `sum_over_time(rate(x[5m])[1h:1m])`

Two grids, and they must be stated separately:

```
inner FusedUnit {
  input:  Raw(scan x)
  kernel: Rate, window = 5m
  grid:   INNER  grid = [start - 1h, end], step = 1m
  output: Series on the inner grid
}
  → grid adaptation
outer FusedUnit {
  input:  Evaluated(inner Series)
  kernel: SumOverTime, window = 1h
  grid:   OUTER  grid = [start, end], step = the statement step
  output: Series on the outer grid
}
```

`subquery_grid` (`src/promql/src/range_eval.rs:573`) already computes the inner grid.

The inner result feeds the outer unit through `push_series`
(`src/promql/src/range_eval.rs:249`), which requires the pushed series' grid to equal the input
grid and its field lengths to match. **Grid adaptation must therefore be explicit**; there is no
place for the run-time guess the prototype made.

The first phase does not implement this: it needs both unit chaining and grid adaptation.

## First-phase scope

| Shape | First phase | Why |
| --- | --- | --- |
| `rate(x[5m])` + map chain | yes | one unit; the existing evaluator carries it |
| `sum by (k) (rate(x[5m]))` | yes | adds a `PartialAggSpec`; exchange stays external |
| `sum by (k) (...) + 1` | yes, for free | the `+ 1` is outside the unit |
| `abs(x)` | no | no window, so no gain |
| nested subquery | no | needs unit chaining and grid adaptation |
| multi-input / cross-series | no | needs merged completion signals |

# Alternatives

## Keep the prototype's post-hoc subtree selection

Rejected. The opportunity would still be derived from a physical tree shape, the node would
still be a widened fixed template, and output columns would still be classified at run time.

## Fuse at the logical level only and let a single physical node do everything

Partially adopted: a logical node can express the evaluation, but deciding whether a *prepared*
scan can serve it depends on scanner state that exists only after physical planning. The
rejection mechanism therefore has to live after physical planning, whatever the node looks like.

## Give the fused node a general expression interpreter

Rejected for now. A general interpreter would replace compilation with run-time dispatch on the
hot path, which is the opposite of the stated goal. Kernels stay selected at compile time.

# Unresolved questions

- Should `FusedSeriesExec` hold an `Arc<FusedPlan>`, or should compilation expand the plan into
  the node's own fields? Holding the plan eases testing and `EXPLAIN`; expanding removes one
  indirection.
- Should a partial aggregate ship in the first phase at all? It clearly pays when the group key is
  a partition key; otherwise it only moves work earlier, and the gain needs measurement.
- `RawSeriesChunk::is_last` must be recomputed once inputs are merged
  (`datatypes/src/series/raw.rs:107-108`). Single-input units are unaffected, but the model needs
  a defined place for that recomputation before multi-input units are added.
