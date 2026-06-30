# GreptimeDB GC huge-file 压测问题清单

更新时间：2026-06-29

## 背景

当前压测在隔离 namespace `gc-stress-test` 中执行，使用 GreptimeDB `docker.io/greptime/greptimedb:v1.1.1`、chart `greptimedb-cluster-0.8.21`、MinIO S3 object store、`local-path`。

已完成两类主要压测：

- **Test A：unknown dummy files / full listing 压力**
  - 2M dummy `.parquet` object；对象数 `3 -> 2000003 -> 2000003 -> 3`。
  - `ADMIN GC_REGIONS(4398046511104, true)` 前端/SQL HTTP 500 timeout after `60005ms`。
  - datanode 实际完成 full listing：约 `2,000,001` files，`1` lister，list cost `466.725s`，OpenDAL S3 list delta `453.613s`。
- **Test B：real removed_files 删除/report 压力**
  - 100k insert+flush 后 `parquet=174823`，GC 删除 `174819` parquet，耗时 `48.472s`。
  - object count `174840 -> 21`，second GC no-op，cluster 保持 `Running`，restarts 未增加。
- **Test C：manifest / FileMeta 压力与 xref smoke**
  - C1 SQL+flush active `FileMeta` 到 100k，未触发 restart/OOM 或明显 open 慢问题。
  - C2a synthetic checkpoint-only 到 2M active `FileMeta`，840.9MB checkpoint，fast/full GC 均约 1-2s。
  - C2b matching placeholder active objects 通过 100/1k/10k/100k，验证 full-listing active-known 保护但不验证 readable SST。
  - cross-region same-table fast-GC smoke 10+10 通过：protected 10/10 保留，unprotected 10/10 删除。

## 1. SQL `ADMIN GC_REGIONS` 固定 60s timeout

- **状态**：已由另一个 agent 修复；此处保留为压测发现/历史记录。
- **影响**：full listing / 大规模 GC 可能超过 60s；SQL admin 调用会返回 timeout/HTTP 500，但 datanode 端 GC 仍可能继续并成功完成，用户侧容易误判为失败。
- **证据**：Test A 2M 中 `ADMIN GC_REGIONS(4398046511104, true)` 约 60s timeout；datanode 随后完成 full listing 和 GC worker。
- **代码线索**：
  - `src/common/function/src/admin/gc.rs`：SQL admin GC 使用 `DEFAULT_GC_TIMEOUT = Duration::from_secs(60)`。
  - `src/meta-srv/src/gc/options.rs`：background GC 有可配置 `GcSchedulerOptions.mailbox_timeout`。
  - `src/meta-srv/src/gc/scheduler.rs`：manual event 可带 timeout；缺省/0 时回退到 config。
  - `src/meta-srv/src/service/procedure.rs`：meta gRPC request 支持 `timeout_secs`。
- **修复状态**：用户确认另一个 agent 已修；后续只需在合并后用长耗时 GC case 验证 admin path 不再 60s timeout。
- **修复交接记录**：曾创建 worktree `/home/discord9/gc-admin-timeout-doc`，文档在 `/home/discord9/gc-admin-timeout-doc/docs/how-to/gc-admin-timeout-full-listing.md`。

## 2. `unknown_file_lingering_time` 当前基本没有实际作用

- **状态**：确认代码现状，需要产品/实现决策。
- **当前行为**：
  - `GcConfig.unknown_file_lingering_time` 有默认值 `1h`，也会从配置解析。
  - `list_to_be_deleted_files()` 里会计算 `unknown_file_may_linger_until = now - unknown_file_lingering_time`。
  - 但该值传入 `should_delete_file()` 后没有被使用：参数名是 `_unknown_file_may_linger_until`。
  - 因此它目前最多起到“配置解析 + duration range validation”的作用，不参与删除判断。
- **实际删除判断**：`src/mito2/src/gc.rs:62-80`
  - 已知 removed file：只有进入 `eligible_for_delete` 后才删；这由 `lingering_time` 控制。
  - live active region 的 unknown file：`is_known=false` 且 `is_region_dropped=false`，不会删。
  - dropped region 的 unknown file：`is_known=false` 且 `is_region_dropped=true`，会删；也不受 `unknown_file_lingering_time` 控制。
- **代码线索**：
  - `src/mito2/src/gc.rs:138-143`：配置字段注释称用于 unknown files。
  - `src/mito2/src/gc.rs:904-909`：计算 threshold。
  - `src/mito2/src/gc.rs:62-80`：`should_delete_file()` 未使用该 threshold。
  - `src/mito2/src/gc.rs:815-823`, `:844-852`, `:969-977`：threshold 仅被传递。
- **需要决策**：
  1. 如果设计目标是 active-region orphan cleanup，需要实现“unknown file 根据 object mtime / metadata age 超过 `unknown_file_lingering_time` 后删除”的逻辑，并明确安全边界。
  2. 如果设计目标是不清理 active-region unknown files，只清理 dropped-region unknown files，应删除/重命名该配置或更新文档，避免误导。
  3. 如果 dropped-region unknown files 也应有保留期，则当前实现也不符合配置语义。

## 3. 100k Test B primary `GcReport` 日志取证记录

- **状态**：非问题；primary 日志已从 metasrv 容器内 file appender 取回。保留为取证记录和 harness 解析修复记录。
- **已知事实**：
  - Test B 100k 已通过 object counts、metrics、datanode `LocalGcWorker` 日志确认删除成功。
  - `/tmp/opencode/gc-test-b-100k/meta-logs-since-4h.txt` 只捕获到 second-GC no-op 的 `GC report` 行，未捕获 primary full `GC report: GcReport { deleted_files: ... }`。
  - 5k/10k/50k 时可捕获 metasrv `GC report` 大行；50k 行约 `3.5MB`。
  - 已直接从 metasrv 容器内 `/data/greptimedb/logs/greptimedb.2026-06-25-17` 取回 primary 100k `GC report`，本地保存为 `/tmp/opencode/gc-test-b-100k/meta-gc-report-primary-filelog.jsonl`。
  - 该行大小 `8,042,302` bytes，包含 region `4440996184064(1034, 0)` 和 `174,819` 个 `FileId(...)`，与 Test B 100k 删除数一致。
- **配置确认**：
  - `docs/how-to/gc-huge-file-region-scripts/manifests/greptimedb-cluster-values.yaml` 开了 `monitoring.enabled` 和 `monitoring.vector`，但只指定了 Vector image/resources。
  - 当前实验 values 没有设置 `monitoring.logsCollection.pipeline.data`，也没有设置 `logging.*` / `meta.logging.*`。
  - chart `greptimedb-cluster-0.8.21` 只有在 `monitoring.logsCollection.pipeline.data` 非空时才渲染 logsCollection；因此本实验的 Vector 不是日志采集流水线。
  - 实际 metasrv config 中 `[logging] dir = "/data/greptimedb/logs"`、`log_format = "json"`；容器内文件名形如 `/data/greptimedb/logs/greptimedb.2026-06-25-17`。
- **取证说明**：
  - `src/meta-srv/src/gc/procedure.rs` 当前有 `info!("GC report: {:?}", report)`；`GcReport` 的 `Debug` 输出会把 `HashMap<RegionId, Vec<FileId>>` 展成单行，100k 规模可达数 MB。
  - `kubectl logs` 走容器运行时日志，超大单行可能被 CRI/containerd/kubelet 日志链路截断、轮转或返回窗口影响；本次直接查 file appender 证明日志本体仍在容器文件中。
  - 测试脚本 `run_removed_file_gc_scale.py` 的 `parse_gc_reports()` 查找完整子串 `"GC report:"`；实际 JSON 是 `"message":"GC report: GcReport ..."`，冒号后还有内容，不存在 `"GC report:"` 这个完整子串，因此解析会遗漏。
- **后续取证建议**：
  - 后续需要大日志证据时，优先直接检查 metasrv 容器内 `/data/greptimedb/logs/greptimedb.20*`，绕过 `kubectl logs`/CRI stdout 链路。
  - 测试脚本解析已修正为匹配 `GC report:`，而不是只匹配完整子串 `"GC report:"`。
  - 如确实需要日志后台采集，在实验 values 增加 `monitoring.logsCollection.pipeline.data`，tail `/data/greptimedb/logs/*.log`。

## 4. Test C：manifest / `FileMeta` metadata 压力

- **状态**：C1 真实 SQL/flush 路径已完成到 100k active `FileMeta`；C2a lab-only synthetic checkpoint fixture 已完成到 2M checkpoint-only；C2b matching active placeholder objects 已完成到 100k。
- **原因**：Test A dummy unknown files 主要压 object-store full listing/filter，不会制造真实 manifest `FileMeta` 压力，也不会放大 `manifest.files.len()`。
- **执行方式**：
  - 新表 `gc_hf_test_c`，`table_id=1035`，`region_id=4445291151360`。
  - `append_mode=true`，`compaction.type=twcs`，`compaction.twcs.trigger_file_num=1000000000`。
  - 重复 `INSERT + ADMIN FLUSH_TABLE`，不执行 `ADMIN COMPACT_TABLE`，让 SST 保持 active 并留在 `manifest.files` 中。
  - harness：`docs/how-to/gc-huge-file-region-scripts/scripts/run_active_filemeta_gc_probe.py`。
- **结果**：
  - 1k：`sst_num=1000`，fast GC `0.028s`，full GC `0.062s`，删除 0。
  - 10k：`sst_num=10000`，`manifest_size=8,901,806`，fast GC `0.042s`，full GC `2.102s`，删除 0。
  - 25k：`sst_num=25000`，`manifest_size=22,281,828`，fast GC `0.041s`，full GC `10.396s`，删除 0。
  - 50k：`sst_num=50000`，`manifest_size=44,581,828`，fast GC `0.060s`，full GC `20.700s`，删除 0；datanode RSS 约 `860Mi`。
  - 100k：`sst_num=100000`，`manifest_size=89,181,836`，fast GC `0.162s`，full GC `38.851s`，删除 0。
  - 50k restart/open：datanode pod Ready 约 `8.34s`；region manifest open cost `82.78ms`，region open elapsed `104.67ms`，file cache recover `1.684s`。
  - 100k restart/open：datanode pod Ready 约 `7.94s`；region manifest open cost `152.26ms`，region open elapsed `202.20ms`，file cache recover `3.282s`。
- **结论**：C1 到 100k active `FileMeta` 未触发删除污染、pod restart/OOM 或明显 open 慢问题；full-listing GC 主要耗时随 object-store list 增长。
- **C2 checkpoint-only ladder**：
  - harness：`docs/how-to/gc-huge-file-region-scripts/scripts/run_synthetic_filemeta_manifest_probe.py`；generator：`src/cmd/src/bin/gc_synthetic_manifest.rs`。
  - seed manifest 都只有两个 delta JSON、没有 `_last_checkpoint`；generator 重放 delta 后生成 exactly N 个 synthetic active `FileMeta` 的 checkpoint。
  - 每轮都在 datanode offline 时写 checkpoint，再写 `_last_checkpoint`；没有对 C1 表 `gc_hf_test_c` / `table_id=1035` 做任何写入。
  - 1k：`table_id=1038`，checkpoint `418,115` bytes，Ready `10.45s`，fast/full GC `0.029s`/`0.026s`。
  - 10k：`table_id=1039`，checkpoint `4,180,120` bytes，Ready `10.44s`，fast/full GC `0.036s`/`0.033s`。
  - 100k：`table_id=1040`，checkpoint `41,890,125` bytes，Ready `10.44s`，fast/full GC `0.079s`/`0.093s`。
  - 500k：`table_id=1041`，checkpoint `209,890,125` bytes，Ready `10.45s`，fast/full GC `0.402s`/`0.415s`。
  - 1M：`table_id=1042`，checkpoint `419,890,130` bytes，Ready `15.54s`，fast/full GC `1.10s`/`0.83s`。
  - 2M：`table_id=1043`，checkpoint `840,890,130` bytes，Ready `25.80s`，fast/full GC `1.63s`/`1.68s`。
  - 所有 C2a runs：`sst_num` 符合预期，GC report 删除 0，S3 counts 始终 `total=5/parquet=1/manifest=4`，cluster `Running` 且无 restart 增加。
- **C2b matching-object smoke**：
  - harness：同一个 `run_synthetic_filemeta_manifest_probe.py`，新增 `--materialize-active-objects`；generator `gc_synthetic_manifest` 输出 `files.jsonl`。
  - C2b 100：`table_id=1044`，`region_id=4483945857024`，materialized `100` placeholder parquet；fast/full GC 后 counts 保持 `total=105/parquet=101/manifest=4`，GC 删除 0。
  - C2b 1k：`table_id=1045`，`region_id=4488240824320`，materialized `1000` placeholder parquet；fast/full GC 后 counts 保持 `total=1005/parquet=1001/manifest=4`，GC 删除 0。
  - C2b 10k：`table_id=1049`，`region_id=4505420693504`，materialized `10000` placeholder parquet in `175.58s`；`sst_num=10000`，`manifest_size=4,180,132`；fast/full GC HTTP `200`，耗时 `0.041s`/`2.173s`；counts 保持 `total=10005/parquet=10001/manifest=4`，GC 删除 0。
  - C2b 100k：`table_id=1050`，`region_id=4509715660800`，materialized `100000` placeholder parquet in `1776.24s`；`sst_num=100000`，`manifest_size=41,890,137`；fast/full GC HTTP `200`，耗时 `0.072s`/`39.594s`；counts 保持 `total=100005/parquet=100001/manifest=4`，GC 删除 0。
  - 结论：full-listing active-known 保护在 matching placeholder object 下通过到 100k；placeholder 不是 readable SST，仍禁止 reads/compaction。
- **剩余未覆盖**：C2a/C2b 仍未覆盖 readable SST bodies、parquet footer recovery、reads/compaction、真实 repartition workload、index/puffin extra-file 压力。

## 5. Cross-region reference / repartition-like fast GC smoke

- **状态**：tiny same-table xref smoke 已通过；不是产品问题，目前作为覆盖证明和后续压力基线。
- **测试目标**：验证 source region A 中已进入 `removed_files` 的文件 X，如果仍被同表 related region B 作为 active `FileMeta { region_id: A, file_id: X }` 引用，fast GC A 时会通过 `FileRefsManifest` / `is_in_tmp_ref` 保护 X，不会把它当作 expired removed file 删除。
- **关键 fixture 形状**：
  - fresh 2-partition table：`PARTITION ON COLUMNS (host) (host < 'm', host >= 'm')`。
  - A checkpoint：`files = {}`，`removed_files = X ∪ Y`，timestamp 0。
  - B checkpoint：`files = X`，且每个 X 的 `FileMeta.region_id = A`。
  - X/Y placeholder `.parquet` 都写在 A prefix 下；datanode offline 时 PUT checkpoint，再 PUT `_last_checkpoint`。
  - 只跑 fast `ADMIN GC_REGIONS(A)`；不跑 reads/compaction。
- **成功结果**：
  - evidence：`/tmp/opencode/gc-xref-smoke-10-10-20260629c/`。
  - table `gc_hf_xref_smoke_20260629c`，`table_id=1048`；A `4501125726208`，B `4501125726209`。
  - checkpoint version `1000000`；datanode swap 后 Ready `20.55s`，cluster `Running`。
  - fast GC HTTP `200`，耗时 `0.0529s`，SQL `execution_time_ms=29`。
  - A counts `total=23/parquet=21/manifest=2 -> total=15/parquet=11/manifest=4`。
  - protected X：`10/10` present；unprotected Y：`10/10` missing。
- **修过的 harness 问题**：
  - 第一次尝试在 datanode scale-down 后用 jsonpath 查空 pod list 导致失败；已改为解析 pod-list JSON 并允许 expected-empty。
  - 第二次尝试因变量 shadowing 把 datanode label selector 覆盖成 `A`/`B`，Ready wait 查错 label；已改为 `datanode_label` / `region_label`。
- **代码线索**：
  - fast GC deletion guard：`src/mito2/src/gc.rs:62-80`、`:932-949`。
  - cross-region refs snapshot：`src/mito2/src/sst/file_ref.rs:get_snapshot_of_file_refs()`。
  - remap/repartition 保留 source `FileMeta.region_id`：`src/mito2/src/remap_manifest.rs`。
  - metasrv related-region/file-ref orchestration：`src/meta-srv/src/gc/procedure.rs`。

## 非问题 / 已澄清

- Test A 中 live active region 的 unknown dummy files 不被删除，是当前代码语义下的安全行为；Test A 只能作为 listing/filter 压力，不应当被描述为 active-region orphan cleanup 验证。
- 100k primary `GcReport` 最初未通过 `kubectl logs`/脚本拿到；后来已从 metasrv 文件日志取回。用户明确 `GcReport` 大 payload 本身不作为问题。
