# Bounded COUNT join-aggregate experiment

This standalone prototype applies a DataFusion optimizer rule to the original
SQL shape, automatically replacing its join-then-aggregate with a fact pre-count,
a dimension pre-count grouped by host and area, an inner join, and count
multiplication. The harness checks its registered fixture cardinalities before
installing the rule. This is a harness-scoped trusted fixture bound, not a
runtime statistics proof; the experiment is not enabled by GreptimeDB or a
general FFX feature.

Only simple `fact` and `dim` scans, a single ordinary inner host equality, and
non-distinct built-in `COUNT(*)` with exactly the host/area grouping are
supported. `SUM`, `AVG`, outer or nested joins, residual join filters, and other
groupings are left unchanged. Accepted scan-level predicates remain in their
original child plans. Tests compare the automatically rewritten original SQL
against a separate DataFusion session without this rule and assert schema/results.

Commands (from the repository root):

```sh
cargo fmt --manifest-path scripts/paper-ffx-auto-rewrite/Cargo.toml -- --check
cargo check --locked --manifest-path scripts/paper-ffx-auto-rewrite/Cargo.toml
cargo nextest run --locked --manifest-path scripts/paper-ffx-auto-rewrite/Cargo.toml
cargo run --locked --manifest-path scripts/paper-ffx-auto-rewrite/Cargo.toml
cargo run --locked --release --manifest-path scripts/paper-ffx-auto-rewrite/Cargo.toml -- --bench
```

For the release-only COUNT fanout applicability sweep, run
`cargo run --release --locked --manifest-path scripts/paper-ffx-auto-rewrite/Cargo.toml -- --sweep`.
It retains the three `--bench` cases and adds hot (512/2048 rows, one host),
balanced (512/2048 rows, 16 hosts), and spread (512/2048 rows, one host
per row) fixtures. Each uses the original full-result SQL/oracle and the same
three warmups plus nine alternating timed samples. Sample arrays remain in
chronological order; medians are computed from a sorted copy. This is a bounded
DataFusion experiment, not GreptimeDB product performance or a runtime selector.
