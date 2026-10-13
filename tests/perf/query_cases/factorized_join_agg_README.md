# Factorized join aggregation experiment

This experiment changes all direct-readable-SST cases run by this script: base
uses `experimental_enable_factorized_join_agg=false` and candidate uses
`true`. Select only the four `tests/perf/query_cases/factorized_join_agg_*`
case paths explicitly when reproducing this experiment. The high case is a
single-key high-fanout join, not high-cardinality; low assigns a distinct host
to each of 2048 rows per side. Each case retains AVG or SUM output and a plain
EXPLAIN plan for review.

Expected results: AVG is 6 in every group. High SUM has four area groups, each
with 6,291,456. Low SUM has 2048 groups, each with 6. These are fixture/query
expectations, not performance results.

For each case, the runner mandatorily resets kernel HWM through `/proc/<pid>/clear_refs`
for frontend and datanode processes on both targets, then captures kernel-
reported approximate process RSS/HWM over an enclosing interval around the
complete combined base/candidate measure invocation. The interval includes
table validation, first query, warmups, repetitions, EXPLAIN, and background
activity; component resets and reads are sequential. This is not isolated
query/operator memory. Do not subtract measurements or sum component peaks or
thread RSS. A reset/capture failure fails the case; measurement is attempted
once with no automatic restart/retry. No performance claims are made without
release-CI results.
