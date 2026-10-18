# 2026-10-18T13:15Z — the corrected CPU profile, and the four claims it corrects

Binary `a2d345cc76c`, case `W` (`sum by (instance) (rate(vmbench[1h]))`, 120 min at a 1 s step),
fused, `POST /debug/prof/cpu?frequency=199&output=text` while the query repeats.

Two independent runs. `W-on3.txt.gz` and `W-on2.txt.gz` are the raw text profiles exactly as the
server produced them; the `-summary.json` beside each is what `fused_series_profile.py` recorded
for the same window (queries, CPU, median query time), and `-summarize.txt` is the corrected
analysis of the same raw text.

**Read `-summarize.txt`, not the `samples` field of `-summary.json`.** Those driver-written
summaries are kept as provenance and were written before the fix: their `samples` field is 5 661
and 5 621, which is the number of *distinct stacks*, not of samples. The weighted figure in each
run is 23 111 and 26 529. `-summarize.txt` is reproducible from the committed raw text with the
commands below.

## What was wrong with the previous numbers, and how to reproduce the fix

Both defects are in the post-processing, not the profiler:

1. **The sample count is per line.** `pprof` 0.15 writes `{frames:?} {count}`
   (`pprof-0.15.0/src/report.rs:150`), so one line is one distinct stack and the trailing integer
   is its sample count. `W-on3` is 5 621 lines and **26 529 samples**. Counting lines undercounted
   by 4.7x, unevenly across stacks.
2. **Owners were resolved by category priority, not stack position**, so a mito2 read reached
   through `FusedSeriesStream` was charged to the fused stream.

```bash
# The arithmetic, against a fixture whose answer is known.
python3 scripts/fused_profile_summarize.py --self-test

# The corrected shares, weighted, with the capture rate against the same window's CPU.
# Run from the repository root.
gunzip -c docs/bench-records/20261018T131500Z/W-on3.txt.gz > /tmp/W-on3.txt
gunzip -c docs/bench-records/20261018T131500Z/W-on2.txt.gz > /tmp/W-on2.txt
python3 scripts/fused_profile_summarize.py /tmp/W-on3.txt --cpu-seconds 214.96 --frequency 199 \
    --inside 'fused: evaluator'
python3 scripts/fused_profile_summarize.py /tmp/W-on2.txt --cpu-seconds 160.4 --frequency 199 \
    --inside 'fused: evaluator'
```

`--inside` is the second question the sampler can answer: the owner table says which module a
sample belongs to, and `--inside 'fused: evaluator'` says which frame inside it was running. The
division below is that output, and it is a 50/50 split in both runs.

## The corrected numbers

| owner | `W-on2` (23 111 samples) | `W-on3` (26 529 samples) |
| --- | ---: | ---: |
| fused: evaluator | 33.3% | 37.6% |
| aggregate | 26.1% | 26.3% |
| scan | 18.6% | 17.8% |
| fused: output build | 2.1% | 2.3% |
| fused: stream | 2.8% | 2.6% |
| http response | 3.5% | 4.4% |
| sort | 2.3% | 2.8% |
| planning | 2.1% | 2.3% |
| shuffle | 1.4% | 1.3% |
| logging | 3.0% | 0.4% |
| unrecognised | 4.7% | 2.1% |

Capture rate: 72% (`W-on2`, 160.4 s CPU) and 62% (`W-on3`, 215.0 s CPU) of the samples the
requested 199 Hz implies, i.e. 144 and 123 samples per CPU second.

Inside the evaluator, by leaf frame: the inlined kernel `extrapolated_rate_batch` 50.5% / 49.4%,
`evaluate`'s own body 47.8% / 49.4%, `ingest` 0.6% / 0.5%. The two halves are within 1-2 points,
which is why the next change adds batch-level timers rather than ordering them from this.

## Withdrawn

- "13% coverage" — it is 62-72%.
- "the evaluator is 6.0% of the query" — it is 33.3-37.6%.
- "the kernel is the largest remaining cost, optimize it first" — the evaluator is the largest
  *module*, but inside it window preparation and the kernel are 50/50 and this data cannot order
  them.
- "a fused partial aggregate is bounded at 27%" — 22.5% of the fused query's total CPU.
- "the evaluator is 28.2% of the query", computed as 65.45 ms ÷ 232.5 ms: those came from
  different server runs. It is 34% from one replication's own pair of measurements.
