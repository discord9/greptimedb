# How to test GC with huge per-region file counts

This document sketches a lab plan for testing GreptimeDB GC behavior when a
single region has an extreme number of files. The motivating customer-shaped
case is roughly:

- one region with O(2M) SST files;
- region metadata/manifest data reportedly over 10 GiB;
- limited storage in the office Kubernetes cluster, so each test object should
  be as small as possible.

The key point is that there is no single cheap test that covers all risk. We
need at least three different pressure tests:

1. **A: full-listing / list-filter memory pressure** with tiny dummy objects.
2. **B: real removed-file delete / report pressure** with real flush + compact.
3. **C: manifest / `FileMeta` metadata pressure** with generated or real
   manifest metadata.

The tests should run in an isolated office-cluster namespace, with a dedicated
GreptimeDB test cluster and an isolated object store bucket/root prefix.

## Why split the test?

GC uses different data sources and data structures at different phases:

- object-store listing returns object paths/entries and may materialize very
  large vectors;
- live region manifests contain `FileMeta` for current files;
- removed-file records drive actual deletion of old SSTs;
- `GcReport` returns the deleted `FileId`s through the datanode heartbeat path;
- manifest cleanup clears deleted removed-file records.

Two million tiny objects in object storage can exercise list/filter memory, but
it does **not** automatically exercise the manifest, `RemovedFile`, huge delete,
or huge report paths.

## Admin GC semantics and observability limits

`ADMIN GC_REGIONS(<region_id>[, true])` triggers GC for the specified regions:

- Region ids are variadic, so batch form is also valid:
  `ADMIN GC_REGIONS(<id1>, <id2>, ..., true)`.
- The optional second argument sets `full_file_listing` (default `false`).
  - `false`: fast mode uses `recently_removed_files` only;
  - `true`: full-listing mode lists the object store and filters results.
- **SQL returns only `processed_regions`** as a single `u64`
  (`src/common/function/src/admin/gc.rs:57`).
- **SQL default timeout is 60s** (`DEFAULT_GC_TIMEOUT`,
  `src/common/function/src/admin/gc.rs:33`). The `GcResponse` internally
  carries `need_retry_regions`, `deleted_files`, and `deleted_indexes`
  (`src/common/meta/src/rpc/procedure.rs:97-103`), but SQL does **not**
  expose them.

**Observability rule**: deleted counts, retries, per-region report details,
and second-GC no-op evidence must come from:
- Logs (metasrv logs the full `GcReport` at info level);
- Metrics (see [Metrics and logs to collect](#metrics-and-logs-to-collect));
- Object-store object counts before/after GC.
- Do **not** rely on SQL for deleted-file count or `need_retry_regions`.

Key config defaults for the test environment:

| Scope         | Config key                             | Default  |
|---------------|----------------------------------------|----------|
| mito/datanode | `gc.enable`                            | `false`  |
| mito/datanode | `gc.lingering_time`                    | `60s`    |
| mito/datanode | `gc.unknown_file_lingering_time`       | `3600s`  |
| mito/datanode | `gc.max_concurrent_lister_per_gc_job`  | `32`     |
| mito/datanode | `gc.max_concurrent_gc_job`             | `4`      |
| metasrv       | `enable`                              | `false`  |
| metasrv       | `max_concurrent_tables`               | `10`     |
| metasrv       | `region_gc_concurrency`                | `16`     |
| metasrv       | `max_retries_per_region`               | `3`      |
| metasrv       | `mailbox_timeout`                      | `60s`    |
| metasrv       | `full_file_listing_interval`           | `86400s` |

## Test A: tiny dummy objects for full-listing pressure

### Goal

Answer: can a datanode survive full-listing a single live region directory with
hundreds of thousands or millions of objects?

This test targets:

- object-store listing and pagination;
- path parsing for `.parquet` and `index/*.puffin` objects;
- in-memory `Vec<Entry>` / maps / sets created by full-listing GC;
- CPU/RSS pressure from filter logic.

### Method

1. Deploy a fresh GreptimeDB test cluster.
2. Create a real table and identify one real live region id and region object
   directory.
3. Write many tiny objects under that region directory:
   - `0B` or `1B` content;
   - legal UUID file names;
   - examples:
     - `<uuid>.parquet`
     - `index/<uuid>.puffin`
     - `index/<uuid>.<version>.puffin` if testing index-path listing pressure.
4. Trigger real admin GC:

   ```sql
   ADMIN GC_REGIONS(<region_id>, true);
   ```

5. Scale by file count:
   - 10k
   - 100k
   - 500k
   - 1M
   - 2M

### Why these dummy files usually won't be deleted

Active-region unknown `.parquet` / `.puffin` objects provide **listing/filter
pressure only**. The delete eligibility check is
`!in_manifest && !in_tmp_ref && (is_known ? eligible : is_region_dropped)`
(`src/mito2/src/gc.rs:62-80`, `:796-871`). For a **live active region**, dummy
objects that are:

- not in the live manifest;
- not in `RemovedFile` records;
- not referenced by temporary file refs;

are **not deleted**, even if they appear in a full listing. Do **not** describe
`unknown_file_lingering_time` as active-region orphan cleanup — it is a
retention gating mechanism, not an active-region reaper.

Test A is **not** a credible test for:
- huge delete batches;
- `GcReport.deleted_files` payload size;
- heartbeat/mailbox pressure;
- manifest `removed_files` cleanup.

### Concurrency caveat

Active-region full-listing concurrency uses `manifest.files.len()` as the
concurrency hint, **not** the dummy object count
(`src/mito2/src/gc.rs:411-417`, `:662-664`:
`min(max(file_cnt_hint / 1024, 1), max_concurrent_lister_per_gc_job)`). A
setup with few real `FileMeta` entries and many dummy objects may list with
low concurrency and does **not** model a 2M-real-`FileMeta` region.

### Object-store / path guardrails

- Region path construction and path types:
  `src/mito2/src/sst/location.rs:26-60`;
  `src/datanode/src/heartbeat/handler/gc_worker.rs:246-259`.
  The path shape is:
  - table dir: `data/<storage_path>/<table_id>/`;
  - region name: `<table_id>_<region_sequence:010>`;
  - base region dir: `<table_dir>/<region_name>/`;
  - `PathType::Bare`: use the base region dir directly;
  - `PathType::Data`: use `<base_region_dir>/data/`;
  - SST object: `<region_dir>/<uuid>.parquet`;
  - index object: `<region_dir>/index/<uuid>.puffin` or
    `<region_dir>/index/<uuid>.<version>.puffin`.
  Mito/Mito2 tables use `PathType::Bare`; metric-engine data regions use
  `PathType::Data`.
- **Fail closed before writing**: Phase 2 tooling must require either an
  explicitly supplied, human-verified prefix or independently verify the prefix
  against existing real region objects. If the prefix cannot be verified, do not
  write dummy objects.
- **Legal UUID filenames are required**: object partitioning relies on
  UUID-like distribution for path sharding
  (`src/mito2/src/gc.rs:983-989`). Use random/distributed UUID v4 names
  rather than sequential or fixed prefixes.
- **MinIO preflight**: before invoking GC, benchmark direct
  write/list/count against the target MinIO pod/pvc for the target object
  count. Confirm pvc iops and latency are safe before adding GC overhead.

If the region is a dropped/no-manifest region, full listing can have different
unknown-file semantics. Do not use `DROP TABLE` plus old region ids as the main
test path unless the metadata/routing semantics are explicitly verified; it can
be misleading.

### Pass/fail signals

Pass:

- datanode does not OOM/restart;
- metasrv procedure does not time out unexpectedly;
- later SQL reads/writes still work;
- RSS and list duration remain within planned limits.

Expected:

- dummy active-region objects may remain after GC.

Fail / stop:

- OOM kill or datanode restart;
- admin GC timeout at modest scale;
- object store list throttling dominates before GreptimeDB GC is exercised;
- foreground SQL becomes unavailable after the run.

## Test B: real removed-file delete/report pressure

### Goal

Answer: can GC delete and report a large number of real removed SST files
without blowing up delete batching, `GcReport`, heartbeat/mailbox, or manifest
cleanup?

This test targets:

- real `RemovedFile` records from flush + compaction;
- delete path and object-store delete batching;
- `GcReport.deleted_files` size;
- heartbeat/mailbox serialization and metasrv deserialization;
- manifest cleanup via clearing deleted removed-file records;
- second-GC idempotency.

### Method

1. Create a single-region append-mode table.
2. Generate many real SSTs by repeated small insert + manual flush.
3. Compact to create real removed files.
4. Configure GC deletion to be immediate for the test environment, e.g. set
   `lingering_time = 0s` where applicable.
5. Trigger real admin GC:

   ```sql
   ADMIN GC_REGIONS(<region_id>);
   ```

   Optionally also run the full-listing variant:

   ```sql
   ADMIN GC_REGIONS(<region_id>, true);
   ```

6. Scale conservatively:
   - 1k real removed SSTs
   - 5k
   - 10k
   - 50k
   - 100k if the environment remains healthy

### Why not start at 2M real removed SSTs?

Creating 2M real SSTs via SQL/flush is slow and expensive. It may first stress
write, flush, compaction, and manifest growth rather than the GC path we want to
observe. If the system is already unstable at 50k or 100k real removed files,
that is enough evidence to prioritize product fixes before trying 2M.

**No obvious delete-batch cap** exists in the delete path
(`src/mito2/src/gc.rs:522-567`). Scale conservatively and watch RSS and
object-store behavior at every step.

### Mailbox / `GcReport` payload risk

`GcReport` carries per-region vectors of deleted file/index ids
(`src/store-api/src/storage/file.rs:110-119`). Metasrv logs the **full
report at info level** (`src/meta-srv/src/gc/procedure.rs:985`).
Large Test B runs can stress:

- mailbox serialization/deserialization between datanode and metasrv;
- log ingestion on the metasrv side.

Add a **stop gate**: if a single `GcReport` is large enough to cause visible
log or mailbox latency, do not scale further — the product needs a fix first.

### Manifest cleanup semantics

- `clear_deleted_files` in the manifest manager operates **in-memory**
  and does not directly update the manifest version or checkpoint
  (`src/mito2/src/manifest/manager.rs:601-605`).
- Immediate second-GC in the same process may appear as a no-op because
  the in-memory removed-file set was already cleared.
- After a datanode restart, the manifest may be reloaded from the
  checkpoint, which may still contain removed-file records.
- **Do not claim** Test B fully validates persisted manifest cleanup
  unless a restart + post-restart GC cycle is explicitly tested.

### Pass/fail signals

Pass:

- deleted count matches object-store count reduction (verify with
  object-store list/count, **not** SQL);
- mailbox/procedure does not timeout;
- datanode/metasrv RSS stays within planned limits;
- delete latency per batch is bounded;
- retry/error metrics (`greptime_mito_gc_errors_total`) are near zero;
- second GC is effectively no-op (in-process);
- data remains queryable.

Expected:

- `need_retry_regions` and deleted counts are not visible via SQL;
  use metasrv `GcReport` logs or explicit procedure/report instrumentation for
  retry evidence, and use object-store deltas plus GC metric deltas for deleted
  count evidence.

Fail / stop:

- `GcReport` payload causes mailbox timeout or log-storm;
- delete batch fails or retries excessively;
- metasrv or datanode RSS approaches unsafe limits;
- second GC repeats massive work instead of becoming no-op;
- object-store delete latency spikes or throttling dominates;
- foreground SQL becomes unavailable.

## Test C: manifest / `FileMeta` metadata pressure

### Goal

Answer: can region open and GC survive a region whose manifest metadata is huge
(for example, millions of `FileMeta` entries and 10+ GiB metadata footprint)?

This is separate from object-count pressure. Test A can create 2M object names,
but it does not create 2M real `FileMeta` entries. Test B creates real metadata,
but only at smaller scales unless we are willing to spend a lot of time and
storage.

### Why this is hard

`FileMeta` is not just a file name. A realistic metadata entry includes region,
file id, time range, level, file size, row counts, sequence, optional partition
expression, index metadata, and manifest-version history. The manifest/checkpoint
format and replay logic must remain internally consistent, otherwise the test
only proves that a corrupted fixture cannot be opened.

Therefore, the metadata-pressure test likely needs one of these approaches:

### Option C1: real but smaller SQL-generated metadata

- Use real insert/flush to generate real `FileMeta` entries.
- Scale until time/storage cost becomes too high.
- Good for correctness and calibration.
- Bad for reaching millions of entries quickly.

### Option C2: lab-only manifest fixture generator

- Start from a real table/region created by SQL.
- Generate a large, internally consistent region manifest/checkpoint using
  GreptimeDB's own manifest and `FileMeta` types.
- Keep object bodies tiny or only materialize a selected subset.
- Restart/open the datanode and exercise real open + GC paths.

This is probably the only practical way to approximate a 10+ GiB metadata case
in an office cluster, but it is engineering work and must be clearly labeled as
lab-only.

### Option C3: hybrid fixture

- Real manifest contains many live `FileMeta` entries.
- Object store contains a smaller but large set of tiny dummy unknown objects.
- A separate real removed-file set tests delete/report at 1k-100k scale.

This combines all bottlenecks without requiring all of them to be at 2M in the
same first run.

### Observed C1 results

Target table: `gc_hf_test_c`, `table_id=1035`,
`region_id=4445291151360`, prefix
`gc-hf-lab/data/greptime/public/1035/1035_0000000000/`.

The C1 run used real SQL-generated active SSTs/FileMeta:

- table options: `append_mode=true`, `compaction.type=twcs`,
  `compaction.twcs.trigger_file_num=1000000000`;
- repeated `INSERT + ADMIN FLUSH_TABLE`;
- no `ADMIN COMPACT_TABLE`;
- fast/full GC probes expected and observed 0 deletes.

Scale results:

- 1k: `sst_num=1000`; fast GC `0.028s`; full GC `0.062s`; object count stayed
  `1013` total / `1000` parquet.
- 10k: `sst_num=10000`, `manifest_size=8,901,806`; fast GC `0.042s`; full GC
  `2.102s`; object count stayed `10013` total / `10000` parquet.
- 25k: `sst_num=25000`, `manifest_size=22,281,828`; fast GC `0.041s`; full GC
  `10.396s`; object count stayed `25013` total / `25000` parquet.
- 50k: `sst_num=50000`, `manifest_size=44,581,828`; fast GC `0.060s`; full GC
  `20.700s`; object count stayed `50013` total / `50000` parquet.
- 100k: `sst_num=100000`, `manifest_size=89,181,836`; fast GC `0.162s`; full
  GC `38.851s`; object count stayed `100013` total / `100000` parquet.

Restart/open probes:

- 50k: datanode pod Ready in `8.34s`; region manifest open cost `82.78ms`;
  region open elapsed `104.67ms`; file cache recovery `1.684s` for `50005`
  parquet entries.
- 100k: datanode pod Ready in `7.94s`; region manifest open cost `152.26ms`;
  region open elapsed `202.20ms`; file cache recovery `3.282s` for `100005`
  parquet entries.

Interpretation: C1 reached 100k active `FileMeta` without delete pollution,
restart/OOM, or slow manifest open in this lab. It remains far below a 10+ GiB
metadata footprint; use C2 for 500k-2M+ synthetic manifest/checkpoint pressure.

### Observed C2 checkpoint-only results

C2 uses a lab-only synthetic manifest/checkpoint fixture. The generator is the
Rust tool `gc_synthetic_manifest`; the Kubernetes/S3 harness is
`docs/how-to/gc-huge-file-region-scripts/scripts/run_synthetic_filemeta_manifest_probe.py`.

All C2a runs used fresh `gc_hf_test_c2_delta_*` tables, seed manifests with two
uncompressed delta JSON files and no `_last_checkpoint`, datanode-offline
checkpoint swap, checkpoint PUT before `_last_checkpoint`, no reads, and no
compaction. In every run, object counts stayed `5` total / `1` parquet / `4`
manifest after swap, fast GC, and full GC; GC reports had `0` deleted
`FileId(...)` entries; cluster phase stayed `Running`; all pods stayed
`Running`; restarts did not increase.

| Scale | Evidence | Table / region | Checkpoint bytes | Datanode Ready | Fast GC | Full GC |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 1k | `/tmp/opencode/gc-test-c2-delta-1k-r2/` | `table_id=1038`, `region_id=4458176053248` | `418,115` | `10.45s` | `0.029s` | `0.026s` |
| 10k | `/tmp/opencode/gc-test-c2-delta-10k/` | `table_id=1039`, `region_id=4462471020544` | `4,180,120` | `10.44s` | `0.036s` | `0.033s` |
| 100k | `/tmp/opencode/gc-test-c2-delta-100k/` | `table_id=1040`, `region_id=4466765987840` | `41,890,125` | `10.44s` | `0.079s` | `0.093s` |
| 500k | `/tmp/opencode/gc-test-c2-delta-500k/` | `table_id=1041`, `region_id=4471060955136` | `209,890,125` | `10.45s` | `0.402s` | `0.415s` |
| 1M | `/tmp/opencode/gc-test-c2-delta-1m/` | `table_id=1042`, `region_id=4475355922432` | `419,890,130` | `15.54s` | `1.10s` | `0.83s` |
| 2M | `/tmp/opencode/gc-test-c2-delta-2m/` | `table_id=1043`, `region_id=4479650889728` | `840,890,130` | `25.80s` | `1.63s` | `1.68s` |

Interpretation: C2a checkpoint-only validates manifest decode/load, synthetic
`FileMeta` in-memory state, region open, and GC set construction through 2M
active `FileMeta` entries with an `840.9MB` manifest checkpoint in this lab. It
does **not** exercise reads/compaction or full-listing over matching synthetic
object bodies; those remain separate C2b/hybrid concerns.

### Pass/fail signals

Pass:

- region opens without OOM;
- manifest replay/checkpoint load time is bounded;
- GC can build live/removed sets without OOM;
- normal reads still work.

Fail / stop:

- region open OOMs or takes unbounded time;
- manifest decode/replay dominates before GC starts;
- GC set construction multiplies memory by several times metadata size;
- manifest cleanup is O(N) enough to make GC unusable.

## Office Kubernetes environment

Use a dedicated namespace and object store for the experiment.

Recommended namespace pattern:

```text
gt-gc-hf-<owner>-<yyyymmdd>
```

Recommended initial layout:

- one namespace per experiment;
- single-pod MinIO with pinned image tag/digest;
- one GreptimeDB test cluster;
- dedicated bucket and root prefix;
- ResourceQuota, LimitRange, PVC size limit, and clear labels;
- no `latest` images;
- no shared MinIO for the first run.

Start with single-pod MinIO because it is easier to clean up and uses storage
more efficiently for tiny-object tests. If MinIO/PVC becomes the bottleneck
before GreptimeDB GC is meaningfully exercised, then run a second phase with a
dedicated distributed MinIO or an explicitly approved shared object store bucket.

## Metrics and logs to collect

Collect at least:

- datanode RSS and OOM/restart events;
- metasrv RSS and procedure errors;
- GC total/list/delete/update-manifest duration if metrics are available;
- deleted-file evidence from object-store count deltas and GC metric deltas;
- `need_retry_regions` evidence from metasrv `GcReport` logs or explicit
  procedure/report instrumentation, not SQL;
- object-store list/delete latency and throttling;
- heartbeat/mailbox timeout/error logs;
- foreground SQL health after each phase;
- MinIO pod RSS/CPU/PVC usage/object count.

Key Prometheus metric names (subject to exact naming in your build):

- `greptime_mito_gc_duration_seconds` — histogram by GC stage, such as `total`,
  `list_files`, `delete_files`, and `update_manifest`;
- `greptime_mito_gc_delete_file_count` — cumulative deleted-file counter;
- `greptime_mito_gc_files_deleted_total` — cumulative deleted-file counter
  labeled by file type;
- `greptime_mito_gc_runs_total` — GC invocations;
- `greptime_mito_gc_errors_total` — error counter, should be near zero.

Per-run values require before/after snapshots or Prometheus `increase()` over
the run window; do not interpret raw cumulative counters as one-run counts.

## Initial execution order and "do not scale" gates

1. Build isolated K8s environment.
2. Run Test A at 10k and 100k tiny objects to validate tooling.
3. **Do not scale** Test A until:
   - datanode RSS is well within limits at 100k;
   - full-listing duration is bounded;
   - MinIO pvc iops/latency confirm headroom;
   - foreground SQL is healthy post-GC.
4. Continue Test A toward 500k/1M/2M only if all 100k gates pass.
5. Run Test B at 1k/5k real removed SSTs.
6. **Do not scale** Test B until:
   - delete count matches object-store delta at 5k;
   - cumulative delete counters match the same run window via before/after
     snapshots or Prometheus `increase()`;
   - mailbox timeout is not triggered;
   - `greptime_mito_gc_errors_total` is near zero;
   - metasrv/datanode RSS is healthy;
   - second GC is no-op in-process.
7. Continue Test B toward 10k/50k/100k only if all 5k gates pass and
   `GcReport`/log payloads are safe.
8. Design Test C fixture generator after A/B results identify the actual
   bottleneck and safe resource envelope.

**Customer-shaped coverage** (O(2M) files, 10+ GiB metadata) is not achieved
until all three axes (A, B, C) have been run at scale or explicitly scoped out
with evidence.

## Lab results observed so far

These values are from the isolated office-cluster lab in namespace
`gc-stress-test`, using MinIO bucket `gc-stress-bucket` and root prefix
`gc-hf-lab`.

### Test A: full-listing / unknown-file pressure

- 1k, 10k, 100k, and 2M dummy-object runs completed without datanode OOM or new
  pod restarts.
- Active-region unknown dummy files remained after GC, as expected; cleanup was
  performed explicitly by manifest.
- The clean 2M run listed `2,000,001` files with one lister in about `466.7s`.
  The frontend SQL/admin call timed out at `60s`, but the datanode completed and
  later returned a successful `GcReport` too late for the procedure.
- Open-region dummy-object listing did not increase lister concurrency because
  the concurrency hint is based on `manifest.files.len()`, not unknown object
  count.

### Test B: real removed-file delete / report pressure

Target table: `gc_hf_test_b`, `table_id=1034`,
`region_id=4440996184064`, prefix
`gc-hf-lab/data/greptime/public/1034/1034_0000000000/`.

- 1k smoke:
  - after flush/compact: `1333` parquet objects;
  - after fast GC: `1` live parquet remained;
  - `1332` parquet files were deleted in about `0.36s`;
  - second GC was a no-op.
- 5k continuation:
  - baseline after 1k: `manifest=16`, `parquet=1`, `total=17`;
  - after 5000 more insert+flush operations: `manifest=22`, `parquet=7007`,
    `total=7029`;
  - fast GC deleted `7004` parquet files in about `1.76s` and left
    `manifest=22`, `parquet=3`, `total=25`;
  - second GC was a no-op;
  - cluster phase stayed `Running`, and pod restart counts did not change.
- 10k continuation:
  - baseline after 5k: `manifest=22`, `parquet=3`, `total=25`;
  - after 10000 more insert+flush operations: `manifest=17`, `parquet=14998`,
    `total=15015`;
  - fast GC deleted `14994` parquet files in about `3.91s` and left
    `manifest=17`, `parquet=4`, `total=21`;
  - second GC was a no-op;
  - cluster phase stayed `Running`, pod restart counts did not change, and
    post-run memory stayed modest: datanode `208Mi`, metasrv `59Mi`, MinIO
    `2261Mi`.
- 50k continuation:
  - after write/compact: `manifest=18`, `parquet=77062`, `total=77080`
    (includes a small partial-write retry from a harness failure);
  - fast GC deleted `77058` parquet files in about `19.67s` and left
    `manifest=18`, `parquet=4`, `total=22`;
  - second GC was a no-op;
  - cluster phase stayed `Running`, pod restart counts did not change, and
    post-run memory stayed modest: datanode `266Mi`, metasrv `80Mi`, MinIO
    `3279Mi`.
- 100k continuation:
  - after write/compact: `manifest=17`, `parquet=174823`, `total=174840`;
  - fast GC deleted `174819` parquet files in about `48.47s` and left
    `manifest=17`, `parquet=4`, `total=21`;
  - second GC was a no-op;
  - cluster phase stayed `Running`, pod restart counts did not change, and
    post-run memory stayed modest: datanode `473Mi`, metasrv `100Mi`, MinIO
    `4140Mi`.
- `GcReport` metasrv log size grows linearly with deleted file count:
  - 1k primary report: about `61.9 KiB` with `1332` file ids;
  - 5k primary report: about `322.8 KiB` with `7004` file ids;
  - 10k primary report: about `690.4 KiB` with `14994` file ids;
  - 50k primary report: about `3.55 MiB` with `77058` file ids.
- At 100k, the primary full report line was not retrievable from the original
  `kubectl logs` capture, but it was later recovered directly from the metasrv
  file appender at `/data/greptimedb/logs/greptimedb.2026-06-25-17` and saved as
  `/tmp/opencode/gc-test-b-100k/meta-gc-report-primary-filelog.jsonl`. It is
  about `8.04 MiB` and contains `174819` `FileId(...)`, matching the deleted
  file count. Treat this as log-retrieval evidence, not a GC logic issue.

Current implication: Test B reached the planned 100k scale and showed real
removed-file deletion throughput is acceptable in this lab. Further pressure
should target still-uncovered manifest/FileMeta dimensions rather than only more
slow repeated SQL flushes.

## Helper scripts

Phase 2 implementation: `docs/how-to/gc-huge-file-region-scripts/scripts/write_dummy_region_objects.py`

Test B scale harness:
`docs/how-to/gc-huge-file-region-scripts/scripts/run_removed_file_gc_scale.py`

Test C active FileMeta harness:
`docs/how-to/gc-huge-file-region-scripts/scripts/run_active_filemeta_gc_probe.py`

Test C synthetic manifest/FileMeta harness:
`docs/how-to/gc-huge-file-region-scripts/scripts/run_synthetic_filemeta_manifest_probe.py`

Test C synthetic checkpoint generator:
`src/cmd/src/bin/gc_synthetic_manifest.rs` (`cargo run -p cmd --bin gc_synthetic_manifest -- ...`)

Use the Test B harness for repeatable insert+flush+compact+GC runs. It captures
object counts, Prometheus before/after snapshots, pod snapshots, logs,
`GcReport` log-size summaries, and concise evidence files. It also supports
`--resume-after-compact` for recovering after a run has already generated and
compacted SSTs but failed before GC evidence collection.

Runnable via `uv run`. Uses Python stdlib only. `--help` for full CLI reference.
The default transport uses MinIO `mc`; use `--transport s3` when no local `mc`
binary is available.

**Quick start**:
```bash
# Dry-run (always safe):
uv run docs/how-to/gc-huge-file-region-scripts/scripts/write_dummy_region_objects.py \
  --region-prefix <mc-alias>/<bucket>/<namespace-prefix>/data/<table>/<region>/ \
  --count 1000 --manifest /tmp/gc_manifest.txt

# S3 transport dry-run (no local mc required; still does not write objects):
uv run docs/how-to/gc-huge-file-region-scripts/scripts/write_dummy_region_objects.py \
  --transport s3 \
  --s3-endpoint http://127.0.0.1:19000 \
  --s3-access-key <access-key> \
  --s3-secret-key <secret-key> \
  --region-prefix s3://<bucket>/<root-prefix>/data/<storage-path>/<table>/<region>/ \
  --safe-substring <root-prefix> \
  --count 1000 --manifest /tmp/gc_manifest.txt

# Re-run with existing manifest: add --overwrite-manifest
uv run docs/how-to/gc-huge-file-region-scripts/scripts/write_dummy_region_objects.py \
  --region-prefix <...> --count 1000 --manifest /tmp/gc_manifest.txt --overwrite-manifest

# Write (requires --i-verified-prefix or --verify-existing-region-object):
uv run docs/how-to/gc-huge-file-region-scripts/scripts/write_dummy_region_objects.py \
  --region-prefix <...> --count 1000 --manifest /tmp/gc_manifest.txt \
  --overwrite-manifest --verify-existing-region-object --execute

# Cleanup dry-run:
uv run docs/how-to/gc-huge-file-region-scripts/scripts/write_dummy_region_objects.py \
  --cleanup --manifest /tmp/gc_manifest.txt

# Cleanup execute:
uv run docs/how-to/gc-huge-file-region-scripts/scripts/write_dummy_region_objects.py \
  --cleanup --manifest /tmp/gc_manifest.txt \
  --i-verified-cleanup-manifest --execute
```

### Required inputs

- Object-store endpoint and access profile (MinIO `mc` alias or S3-compatible
  credentials);
- Bucket name and root prefix (must match the test cluster's config);
- Target table id and region id (from `INFORMATION_SCHEMA` or logs);
- Verified region prefix, or enough metadata for the tool to verify it:
  - table dir: `data/<storage_path>/<table_id>/`;
  - region name: `<table_id>_<region_sequence:010>`;
  - base region dir: `<table_dir>/<region_name>/`;
  - region path type from `src/mito2/src/sst/location.rs:26-60` and
    `src/datanode/src/heartbeat/handler/gc_worker.rs:246-259`;
  - `PathType::Bare` for Mito/Mito2 tables and `PathType::Data` for
    metric-engine data regions;
- Target object count per run.

### Safety and behavior contract

1. **Dry-run mode**: print the exact region prefix, object count, expected
   storage impact, and a sample of generated paths before writing anything.
2. **Namespace/bucket/prefix guard**: refuse to run unless the target matches
   an allowed prefix pattern (e.g. must contain `gt-gc-hf-`). Require an
   explicit `--allow-unsafe-prefix` flag to bypass. Cleanup mode also checks
   every manifest entry unless this flag is set.
3. **Manifest overwrite safety**: write mode refuses to overwrite an existing
   `--manifest` file unless `--overwrite-manifest` is given.
4. **Size cap**: default `--max-size-bytes 1024`; `--size-bytes` values above it
   require `--allow-large-objects` or an explicit `--max-size-bytes` override.
5. **Computed-prefix safety**: `--table-id` must be numeric; `--storage-path` is
   required unless `--allow-empty-storage-path` is given.
6. **UUID v4 filename generation**: all `.parquet` and `index/*.puffin` objects
   must use random UUID v4 names (or an equivalent distributed-naming scheme
   that respects `src/mito2/src/gc.rs:983-989` partitioning assumptions).
7. **Rate and concurrency controls**: limit write rate and parallel connections
   (`--concurrency`, `--sleep-ms`) to avoid saturating the MinIO pod/pvc.
8. **Object-count verification**: after writes, a before/after object listing
   delta must equal `--count` (`mc ls --recursive` in `mc` mode, S3
   `ListObjectsV2` in `s3` mode). Failure exits non-zero. Bypass only with
   `--skip-count-verification` (not recommended).
9. **Cleanup manifest validation**: rejects entries beginning with `-`, validates
   UUID `.parquet` / `index/*.puffin` path shape, guards against multiple
   distinct region prefixes unless `--allow-multiple-prefixes` is given.
10. **Background failure tracking**: every upload/delete operation is checked
    (`mc pipe`/`mc rm` in `mc` mode, S3 `PUT`/`DELETE` in `s3` mode); any
    failure causes the script to exit non-zero.
11. **S3 cleanup confirmation**: cleanup `--execute` requires either
    `--i-verified-prefix` or `--i-verified-cleanup-manifest`; S3 cleanup also
    fail-closes on bucket mismatch before issuing any delete requests.

### Warning

Dummy active-region objects injected for Test A are expected to **remain**
after GC. The cleanup script should distinguish these from real region data
and not assume GC will delete them.
