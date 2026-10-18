# 2026-10-18T14:30Z — deleting two accumulator vectors, and the drift that nearly faked it

Two nightly binaries, one change between them, `W` and `G`, eight paired rounds each, plus a
same-binary control of eight rounds per case.

| binary | what it is |
| --- | --- |
| `greptime-4293141770c` | `PendingBatch` holds `indices: Vec<usize>` and `eval_ts: Vec<i64>` |
| `greptime-nodup` | `first_index: usize` + `len: usize`, and `flush_batch` slices the grid |

The change removes two `Vec::push`es per grid point and adds one `usize` add-and-store. Nothing
else moves: the retained spans, the reset ranges, `WindowBatch`, `BatchKernel`, the arithmetic,
the batch size and the flush placement are byte-for-byte the same (see
`docs/fused-series-report.md` section 4.10).

Every invocation:

```bash
python3 scripts/fused_series_regions.py BIN DATA_HOME W 9 --timed 5 \
    --compare-bin OTHER --rounds 8 --json W-8r.json
```

9 ANALYZE records and 5 timed executions per session, arm order alternating every round.

## The result that was wrong

The first A/B, three rounds, read `evaluate` at ratio 0.897 — "10% faster". It was not. The same
binary against itself, same script, same arithmetic, read 0.976: doing nothing was "2.4% faster".
The subtraction in `paired_ratios` cancels nothing when the two sessions are 30 s apart, because
the drift between them is not shared.

The same-binary control across 16 sessions puts the per-session noise where it matters: the two
sessions of one round differ by **sd 6.2-6.4 ms on prep and 6.2-6.7 ms on kernel**, with outliers
of 21 ms. Against an effect of order 1 ms, a single round says nothing. This is why the first
three-round answer (0.897) and the swapped three-round answer reversed each other.

## The control the change hands us

`fused_kernel_time` reads the retained spans, the reset ranges and the kernel arithmetic — none of
which the change touches. So the kernel's per-round difference **is** that round's drift, and two
uses of it are reported below.

**Method 1, discard.** Drop any round whose kernel difference exceeds three sd of the
same-binary control. Four of eight `W` rounds survive; prep is lower in 4/4, median −1.41 ms.

**Method 2, adjust.** Subtract the kernel difference from each other region's difference. This
assumes drift is roughly additive in milliseconds across regions, which it only approximately is —
and the assumption is testable, because `columns` is untouched too and can be run through the same
arithmetic on the same-binary data.

## Method 2 on the control, which is what makes it usable

Pooled over the two cases, 16 rounds each:

| region | A/B adjusted | negative | self-control adjusted | negative | separates? |
| --- | ---: | ---: | ---: | ---: | --- |
| `prep` **(changed)** | **−1.77 ms** | **16/16** | +0.60 ms | 6/16 | **yes, −2.37 ms** |
| `evaluate` (contains prep) | −2.10 ms | 14/16 | −1.79 ms | 10/16 | no: control moves too |
| `columns` (untouched) | +0.82 ms | 6/16 | +1.76 ms | 6/16 | no: same in both |
| `kernel` (the covariate) | 0.00 ms by construction | — | 0.00 ms | — | by construction |

Read the table for which regions separate, not for which are negative.

`columns` reads almost the same in the A/B as in the self-control (+0.82 against +1.76), which is
what an untouched region must do: whatever the adjustment does to it, it does in both, so the
difference is ~0 (−0.94 ms, −0.4 SE). `evaluate` does **not** separate either: its control reads
−1.79 ms against the A/B's −2.10, because `evaluate` is the whole `push` and inherits the
outlier rounds (+50 ms, −29 ms) that the kernel cannot track. The adjustment removes drift from
`prep` and `columns` but not from `evaluate`, so `evaluate`'s own number is not evidence of
anything.

`prep` is the one region that separates, and it separates cleanly:

- A/B: −1.77 ms, 16 of 16 rounds negative, standard error 0.25 ms.
- Control: +0.60 ms, 6 of 16 negative, standard error 0.34 ms.
- Difference: **−2.37 ms**, about 5.6 standard errors of the difference.

Per `(series, grid point)`, over 400 x 7 171 = 2 868 400 points: **−0.83 ns per point** for the
pair of `Vec::push`es that became one counter increment. That is −6.3% of the window-preparation
region, which is 28.05 ms at this shape.

What that means for the whole evaluator is a bound rather than a measurement: `prep` is a subset
of `fused_evaluate_time` and nothing else inside it changed, so the evaluator falls by **at least**
1.77 ms of 60.69 ms (2.9%), and no more than the unmeasured remainder allows one to claim. About
1% of the query's `/proc` CPU follows by arithmetic; it is not measured, see below.

Two `Vec::push`es cost ~0.8 ns — between two and three cycles at this machine's clock. That is
plausible rather than surprising, and it is the *only* number here that the byte count predicted:
removing 16 of 48 bytes per point did not predict the size of the effect, the measurement did.

## End to end, where nothing is resolvable

| quantity | A/B adjusted | self-control | verdict |
| --- | ---: | ---: | --- |
| wall | −0.62 ms, SE 1.39 | −0.20 ms, SE 1.85 | not resolvable |
| `/proc` CPU | −0.00 ms, SE 9.13 | +0.00 ms, SE 10.48 | not resolvable |

`/proc/<pid>/stat` ticks at 10 ms and a query is ~150 ms of CPU, so a 2 ms effect is a fifth of one
tick: the median lands on a tick boundary in both arms. Wall clock carries the same difficulty
plus the event loop's own variance. **No end-to-end speed claim is made, and none should be read
out of the 1% CPU figure above** — that is arithmetic on a region timer, not a measurement of the
query.

## Reproduce

```bash
BIN=/mnt/nvme_rust/rust-targets/narthex-min-chain/fused-bench-bin/greptime-4293141770c
NEW=/mnt/nvme_rust/rust-targets/narthex-min-chain/fused-bench-bin/greptime-nodup
DATA=$HOME/.cache/fused-scale/data-seed
for case in W G; do
  python3 scripts/fused_series_regions.py $BIN $DATA $case 9 --timed 5 \
      --compare-bin $NEW --rounds 8 --json $case-8r.json
  python3 scripts/fused_series_regions.py $BIN $DATA $case 9 --timed 5 \
      --compare-bin $BIN --rounds 8 --json $case-self8.json
done
```

`W-8r.json`, `G-8r.json`, `W-self8.json` and `G-self8.json` are the four runs above; each holds
every per-round median and every timed sample. `analyse.py` recomputes the tables from them.

## Files

| file | what |
| --- | --- |
| `W-8r.json`, `G-8r.json` | the A/B: `bin` = old, `compare-bin` = no-duplicate |
| `W-self8.json`, `G-self8.json` | the control: both slots the same binary |
| `W-8r.txt`, `G-8r.txt` | the console output of the two A/B runs |
| `analyse.py` | the per-round difference and the drift adjustment |
