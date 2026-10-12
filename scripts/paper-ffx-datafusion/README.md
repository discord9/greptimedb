# Experimental DataFusion raw-factorization study

This standalone crate explores the explicit physical-plan boundary for a fixed,
bounded, one-partition join/aggregate fixture. `RawFactorExec` retains matched
fact values and dimension area/weight values once per non-null host; it does not
materialize the Cartesian join. `FactorAggregateExec` consumes those lists into
per-host/area pair count, valid product count, and score. Outside the guided
`--count` experiment described below, ordinary SQL planning is not modified and
these plans are never selected automatically.

This is not a production execution path: inputs are collected into memory and
there is no spill, distributed execution, memory reservation, or optimizer
integration. The producer accepts bounded, one-partition children only, and the
consumer accepts this producer's output only. Floating multiplication/addition
changes association; the score is only considered on controlled finite fixtures,
not generally equivalent for arbitrary floating-point inputs (cancellation can
differ). Non-finite payloads on non-null-host rows are rejected, but sums/products can still overflow
from finite inputs (for example fact values `1e308, 1e308` and weight `1e-308`
can overflow the fact sum although the joined products remain finite). The bounded
fixture implementation does not guarantee equivalence under such overflow or
floating-point cancellation.

Verification from this directory (with the pinned Rust toolchain available):

```sh
cargo fmt --check
cargo check --locked
cargo nextest run --locked
cargo run --locked
```

## `--count` automatic selection

`cargo run --locked -- --count` exercises the guided automatic-selection
experiment. `count_session` registers `facts`/`dims` as in-memory tables and
installs a `QueryPlanner` wrapper that recognizes exactly one shape — an inner
equi-join of those two registered providers on `host`, grouped by
`(host, area)` with a single plain `COUNT(*)`. The wrapper delegates to the
default planner first and splices `RawFactorExec` → `FactorAggregateExec` beneath
any ordinary sort/limit/projection ancestors; every other shape keeps the
delegated baseline plan unchanged, including shapes whose plan cannot be spliced.
There is no execution-time retry or replay.

The session uses the same configuration as the default harness (one target
partition, joins and aggregations not repartitioned). Setup runs a preflight on
the actual batches, never on caller-supplied counts: non-finite fact values or
dimension weights leave the session on the default planner,
`fact_rows * dim_rows > i64::MAX` refuses setup, and row totals above the Arrow
`List` offset capacity (`i32::MAX`) skip the selection. The factor path applies
only to bounded, one-partition `(Int64, Float64)` fact and
`(Int64, Int64, Float64)` dimension inputs; anything else falls back. `--count`
compares the complete grouped result with a plain session over the same batches
and prints both plan texts with `RawFactorExec` present or absent. The command
makes no performance claims.

## `--count --bench` end-to-end timing

`cargo run --release --locked -- --count --bench` (optionally
`--count --bench WARMUPS SAMPLES` for a shorter smoke run) times the *original*
COUNT SQL on the balanced/hot/low/hot2048 fixtures, where `hot2048` is the
one-hot-host 2048x2048 fanout shape of the COUNT sweep experiment. Every timed
iteration runs `ctx.sql(COUNT_QUERY).collect()` on a fresh plan for both arms —
the `count_session` arm, whose samples therefore include the SQL planning and
factorized-selection work of the automatic rewrite, and a plain same-config
session arm — alternating the arm order per iteration and reporting per-arm
sample vectors with medians after the warmups. Complete-result equality with the
plain baseline is re-checked once untimed before any timing and after every timed
iteration.

These are fixture-local numbers produced by this bounded in-memory harness on a
CI runner: they are evidence about this experiment and its fixtures only, not
claims about GreptimeDB product performance, and the timing includes full SQL
planning and selection overhead on both arms.
