# Experimental DataFusion raw-factorization study

This standalone crate explores the explicit physical-plan boundary for a fixed,
bounded, one-partition join/aggregate fixture. `RawFactorExec` retains matched
fact values and dimension area/weight values once per non-null host; it does not
materialize the Cartesian join. `FactorAggregateExec` consumes those lists into
per-host/area pair count, valid product count, and score. Ordinary SQL planning is
not modified and these plans are never selected automatically.

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
