# corrbound-stage0

Stage-0 probe for the CorrBound line of work: a **statistics-only
counterfactual** that shows whether losing the input-cardinality information of
a join input changes DataFusion's join decision on GreptimeDB's pinned
DataFusion fork.

## The claim this experiment supports

> On the fixture query from
> `tests/perf/query_cases/sql_join_filter_order/case.toml`, the pinned
> `datafusion::physical_optimizer::join_selection::JoinSelection` rule selects a
> **different physical join shape** depending only on the join inputs' `num_rows`
> statistic — it chooses `CollectLeft` when that statistic is the sound
> pre-query upper bound `|sigma_P(T)| <= |T| = 16,384`, and `Partitioned` when the
> same statistic is `Absent` (no input-cardinality information at all). Both plans
> return the same query results.

Measured on this revision (see `evidence/stage0-evidence.json`):

| replay | join inputs' `num_rows` | `JoinSelection` decision |
| --- | --- | --- |
| replay-1, the engine's own statistics | `Absent` on both sides | `HashJoinExec` mode=**Partitioned**, Inner, `left=sql_join_fact, right=sql_join_dim` |
| replay-2, only `num_rows` replaced | `Inexact(16384)` on both sides | `HashJoinExec` mode=**CollectLeft**, Inner, `left=sql_join_fact, right=sql_join_dim` |

Everything else is identical between the two replays: the same subtree (the
wrapper's inner plans are the very same `Arc`s, and the structural signature of
the plan is unchanged), the same `ConfigOptions`
(`hash_join_single_partition_threshold_rows = 131072 > 16384`,
`hash_join_single_partition_threshold = 1 MiB` with byte sizes `Absent`,
`join_reordering = true`, `use_statistics_registry = false`), and the same
`SessionState`.

The pair demonstrates the flip in both directions: **remove the number →
`Partitioned`; set it to the sound upper bound `|T|` → `CollectLeft`.** The
engine's own plan at this join is the `Absent` case, which is exactly the
"input-cardinality information lost" state; the probe supplies the bound and the
rule picks a different shape.

Note that this is a `PartitionMode` flip, not a side swap: with equal row counts
on both sides `should_swap_join_order` returns `false`, so the side assignment is
unchanged while the partition mode moves.

## What it actually does

1. Builds the `sql_join_filter_order` fixture with the **real engine**, in
   process: `tests_integration::standalone::GreptimeDbStandaloneBuilder`
   (standalone GreptimeDB: frontend + datanode + mito engine, file storage under
   a temp dir, no external cluster).
   * two append-mode Mito tables, one region each,
   * 32 `ADMIN FLUSH_TABLE` flushes of 512 rows per table = 16,384 rows/table,
   * 16 hosts / 64 instances / 4 zones, one row per second from
     `2024-01-01T00:00:00Z`.
2. Reads the **pre-query** cardinality from the engine's own region statistics
   (`MitoEngine::regions()` + `get_region_statistic()`); no query is executed to
   obtain the bound.
3. **Observation A (baseline):** runs `EXPLAIN` and `EXPLAIN VERBOSE` through the
   engine, and separately re-derives the baseline physical plan from the engine's
   own `SessionState` (analyzer -> logical optimizer -> physical planner,
   mirroring `src/query/src/datafusion.rs:485-501`).
4. **Observation B (counterfactual):** rebuilds the physical plan with the
   physical optimizer rule list from the engine's `SessionState` **truncated just
   before `join_selection`** (index 2 of 29 in this build), then invokes the
   pinned `JoinSelection::new().optimize(subtree, &config_options)` directly,
   twice:
   * **replay-1**: on the subtree exactly as planned (original statistics), and
   * **replay-2**: on a copy where the hash join's two inputs are wrapped in an
     experiment-local `StatisticsInjectExec` that reports
     `num_rows = Precision::Inexact(16_384)` and delegates every other statistic,
     plan property, schema and execution to the wrapped plan.
   The literal "remove the rule from the list" recipe (all rules except
   `join_selection`) is computed and reported too; both recipes reproduce the
   engine baseline. The truncation recipe is the one used for the counterfactual
   because it is exactly the subtree the rule sees in the real pipeline.
5. Continues the engine's rule list from `join_selection` onwards
   (`EnsureRequirements`, `OutputRequirements::remove_mode`, ...) so the replayed
   plan is executable, and asserts that this continuation does **not** revisit
   the join decision — and that applying it to the control plan reproduces the
   baseline physical plan text byte for byte.
6. Executes the fixture query through the engine and executes the replay-2 plan,
   then compares result rows with float tolerance and tolerance-bucketed tie
   handling.

## The soundness argument for `|sigma_P(T)| <= |T|`

`sigma_P(T)` is a subset of the rows of `T`, so for **any** predicate `P` the
filtered relation can never contain more rows than its input. `num_rows` is an
upper bound on the row count of the relation it describes, so a planner that has
lost the (post-filter, possibly estimated) cardinality of a join input can still
use the *pre-query* table cardinality `|T|` as a sound, non-null upper bound on
`|sigma_P(T)|`:

* it needs no predicate analysis and no execution,
* it is available pre-query from region statistics,
* it can only over-estimate, never under-estimate, the filtered input.

The probe therefore injects only `|T|` (16,384) — never a lower value — so
replay-2 is a plan a *sound* planner could have produced. It does **not** claim
that the resulting plan is better, cheaper, or even the plan a real algorithm
would pick.

## Exact invocation

```bash
# from the repository root
source scripts/corrbound-stage0/env.sh          # toolchain + cargo env (see below)
cargo run --manifest-path scripts/corrbound-stage0/Cargo.toml --locked
```

The probe prints a structured JSON summary to stdout and writes
`scripts/corrbound-stage0/evidence/stage0-evidence.json` (untracked; the
directory contains a `.gitignore`).

Exit codes: `0` = finished (including `NOT REPRODUCED`), `1` = an assertion
failed, `2` = fatal error.

`env.sh` deviates from the generic experiment environment in two documented
ways:

* `RUSTUP_TOOLCHAIN=nightly-2026-03-21` (the toolchain pinned by
  `rust-toolchain.toml`) instead of stable — the GreptimeDB crates this probe
  links (`query`, `table`, `sql`, `mito2`, ...) use `#![feature(...)]` gates and
  cannot be compiled by stable;
* `RUSTFLAGS=''` instead of `-C debuginfo=0`, so that prebuilt artifacts from an
  earlier build of this workspace (same toolchain, same empty RUSTFLAGS) stay
  reusable from the cargo cache.

## Assertions

All are printed as `PASS`/`FAIL`; any `FAIL` exits non-zero. The counterfactual
itself is **not** an assertion: if the two replays pick the same join decision the
probe prints `NOT REPRODUCED` and exits 0 with an honest report.

| # | Assertion | Meaning |
| --- | --- | --- |
| — | `pinned_datafusion_rev`, `pinned_datafusion_version` | the fork rev is present in the `Cargo.lock` consumed by the build, and the runtime `datafusion::DATAFUSION_VERSION` is `55.1.0` |
| 1 | `unfiltered_inputs_are_table_cardinality` | both tables hold 16,384 rows each, read from region statistics |
| 2 | `pre_query_upper_bound_source_exists` | the bound comes from fixture construction + region statistics, not from executing a query |
| 3 | `replay_original_statistics_reproduces_baseline` | replay-1 reproduces the engine baseline's join type/mode/sides exactly |
| 4 | `variants_differ_only_in_injected_num_rows` | the two replay inputs differ only by the injected `num_rows` (same structural signature, wrapper inner plans are the very same `Arc`s, byte size and column statistics preserved) |
| 5 | `counterfactual_changes_join_decision` | records the observed diff (informational; `NOT REPRODUCED` is reported, not hidden) |
| 6 | `post_selection_pipeline_preserves_join_decisions` | continuing the rule list after `join_selection` preserves both the control and the counterfactual decision |
| 7 | `control_replay_reproduces_baseline_plan_text` | control replay + continuation reproduces the engine's baseline physical plan text exactly |
| 8 | `results_identical_between_baseline_and_replanned_variant` | the re-planned variant returns the same rows as the engine baseline (abs tol `1e-9`, rel tol `1e-12`, tie-insensitive `ORDER BY ... LIMIT` comparison) |

Notes on two comparison rules:

* `JoinDecision` equality covers join type, partition mode, the null-aware flag
  and which fixture table sits on which side. It deliberately excludes the
  *names* of the join's direct children, because rules after `JoinSelection`
  (e.g. `EnsureRequirements`) insert `RepartitionExec` between the join and its
  inputs. Those names are still reported in the JSON evidence and in `diff()`.
* Result rows are grouped into tolerance buckets before comparison, so a
  `LIMIT 10` that returns two near-equal `score`s in either order still compares
  equal (in this fixture two rows differ by ~2e-13).

## Explicit non-claims

* No paper algorithm is implemented here. There is no bound-propagation pass, no
  sketch, no estimator: the probe only *injects one number* and observes the
  optimizer.
* No product integration. Nothing under `src/` is modified; the probe is a
  standalone `scripts/` workspace (it declares its own `[workspace]`, like
  `scripts/paper-ffx-datafusion`).
* No latency, memory, or cost conclusions. The probe executes the fixture query
  only to check that results are unchanged; it makes no performance claim.
* Local plan-shape evidence only: one DataFusion revision, one fixture, one
  query, one machine. `EXPLAIN` text, join mode, side assignment, and injected
  statistics are reported verbatim so the claim can be re-checked, not
  extrapolated.
* The probe does not claim that `CollectLeft` is *better* for this query, or that
  a real system should always use the raw table cardinality. It shows only that
  this statistic is consumed by `JoinSelection` and changes its output.
* The counterfactual is observed with the engine's rule list truncated at
  `join_selection`; the continuation after that point is replayed and asserted
  not to change the decision.

## Route

The probe uses the **real-engine route** (in-process standalone GreptimeDB via
`tests-integration`), not a hand-built `MemTable`/`MemSourceConfig` substitute:
the join inputs, their statistics, the physical extension planner
(`MergeScanExec`), the physical optimizer rule list, and the `ConfigOptions` all
come from the shipping engine.

## Wiring notes

Because it is a standalone workspace, the crate replicates what the root
`Cargo.toml` supplies:

* `[patch.crates-io]` entries for the DataFusion fork rev
  `2aa87d52cdce7006af492330064738f33ed294c1` (all crates) plus the other fork pins
  the dependency graph needs;
* an explicit git-source dependency on `datafusion-physical-optimizer` (and
  `datafusion-execution`/`-pruning`/`-session` as patch entries), because those
  crates are *not* in the root patch table and would otherwise be resolved from
  crates.io, where `datafusion-physical-optimizer 55.1.0` does not type-check
  against the fork's `datafusion-execution`;
* `Cargo.lock` seeded from the repository root so that the whole tree resolves to
  the same versions the workspace uses (notably
  `deadpool-postgres 0.14.1`/`deadpool 0.12.2`, whose newer crates.io releases do
  not satisfy `src/common/meta`'s `deadpool` bound);
* `meta-srv` with `features = ["mock"]`, which `tests-integration` requires.
