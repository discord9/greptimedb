日期：2026-10-19 起，2026-10-19 收尾

目标是在现有 batching Flow 内跑通 sequence 驱动的局部重算：每轮找出受影响的 trace，只对这些 trace 执行原来的采样 SQL。保持批量实验已经验证的累计 upsert 行为，并测量重复计算和重复写入减少了多少。

## 结论摘要

**机制成立，收益来源与最初的预期不同。**

在 2 万条 trace、6 万个 span 的固定输入上，PoC 与全量方案在三个场景逐轮对照：

| 场景 | PoC 单轮 | 全量基准单轮 | 比值 | PoC 每轮提交行 | 基准每轮提交行 |
| - | - | - | - | - | - |
| 静止 | 85 ms | 2,200 ms | 0.04x | 0 | 1,200 |
| 稀疏（每轮 1% trace 追加） | 1,790 ms | 3,200 ms | 0.56x | 800 | 2,200–4,600 |
| 活跃（每轮 50% trace 追加） | 18,000 ms | 2,900 ms | 6.2x 慢 | 40,000–60,000 | 80,000–110,000 |

三条要点：

1. **收益主要来自停止重复写入，不是减少扫描。** 静止场景每轮写入从 1,200 行降到 0，耗时降到全量的 4%。这正是批量实验报告的痛点（3,925 行物理写入对应 1,211 个不同 span 的写放大）。
2. **受限查询本身并不便宜。** `trace_id` 的 skipping index 是 bloom filter，默认 granularity 10,240 行，在 row group 内按块裁剪，所以确实少读了块；但默认 row group 是 102,400 行[23]，本实验 6 万行整表落在单个 row group 内，row group 级的 `row_groups_by_minmax` 结构上无事可做。块级裁剪带来的是比例上的减少，不是量级上的。稀疏场景的收益来自"只写变化的行"，不是"少扫"。
3. **活跃场景明确退化。** 受影响 trace 达到 1 万条时，按每批 1,000 个 trace ID 拆成 10 条查询，每条各扫一遍窗口，扫描量放大 10 倍。这是分批阈值的代价而非方案固有上限，正确做法是按 `|D|` 占比退化成一次全量查询；本轮未实现该分支，故表中 6.2 倍是当前实现的真实开销。

正确性在所有场景成立：两种模式共用同一输入与 tick 序列，累积 sink 逐行一致。

**范围**：本轮只到开销对照为止。把它变成"能通过 DDL 声明的自动 Flow"需要企业版入口，原因与交接要点见第 6 节。

交付物是可重现的场景脚本与开销对照（`tests-integration/tests/flow_tail_sampling.rs`），加上本文。

# 1 已有实验提供的起点

用户提供的批量实验报告记录了以下结果。[1]

- 单个 batching Flow 已能完成错误 trace、慢 trace、等待后决策和迟到补写。

- 原始表使用 trace v1 的字段，append-only，TTL 为 1 小时；长期表为非 append 表，TTL 为 30 天。

- 长期表以 `(trace_id, span_id)` 加原始 `timestamp` 标识一行。重复输出会覆盖同一行，但仍产生物理写入。

- 背景规模约为 2 万条 trace、6 万个 span；报告记录单次全量查询约 40 ms。

- 另一次重复写入检查记录物理写入 3,925 行、不同 span 1,211 个。需要改进的是每 5 秒重算和重写整个近期结果的行为。

本 PoC 直接沿用实验中的规则、字段和 sink 写入语义。报告中的耗时是已有实验观察值，不作为新 PoC 的性能预期。报告提到的生成脚本如果可以取得就复用，否则按下面的场景重新生成。

# 2 固定本轮范围

| 项目 | PoC 选择 |
| - | - |
| 部署 | 一台 standalone，使用已有 exact sequence 读取能力的开发构建 |
| SQL 范围 | 只支持本文件附录中的固定采样模板 |
| 调度 | batching，每 5 秒执行 |
| 回看范围 | 10 分钟 |
| 等待条件 | 最早 span 开始时间满 30 秒 |
| 保留条件 | 任一 span 为 ERROR，或者最晚结束时间比最早开始时间晚超过 1 秒 |
| source | 新建 `raw_spans`，append-only，从第一笔写入前保留 row sequence |
| sink | `sampled_spans_poc`，普通覆盖写表，沿用显式列和稳定行标识 |
| 对照表 | `sampled_spans_baseline`，与 PoC sink 同结构 |
| 执行进度 | 内存中的各 region sequence 和上次成功评估时间 |
| 输出方式 | 受影响 trace 的当前查询结果整体 upsert，允许 trace 内重复写入 |

先做固定模板和显式查询调用。通用 SQL 改写器、逐 span 输出 diff、持久 checkpoint 和分布式恢复不列入本轮交付。

## 累计保留语义

两张 sink 都按下面的方式累计：

```text
baseline_sink = upsert(baseline_sink, 本轮原始全量 SQL 结果)
poc_sink      = upsert(poc_sink, 本轮受影响 trace 的 SQL 结果)
```

窗口过期或 trace 失选时，不删除长期表中已经保存的 span。继续按原 SQL 重新计算 `sample_reason`，不固定首次决策。

例如，旧 error span 退出窗口后，仍在窗口内的正常 span 可能从 `error` 更新为 `slow`；已经退出窗口的长期行保留上次写入的 reason。这与批量实验的行为一致，不需要另外修正。

# 3 准备表和运行环境

使用新的测试数据库或新表开始。原实验的 `v1.3.0-beta.2` 用于说明全量方案已经跑通，增量 PoC 应使用实际包含 sequence 能力的开发分支。

原始表的关键选项为：

```sql
WITH (
  'append_mode' = 'true',
  'preserve_row_sequence' = 'true',
  'ttl' = '1h'
)
```

这些选项必须在写入第一批数据之前生效。真实约束不是"只能建表时设置"——ALTER 也能打开 `preserve_row_sequence`[8]——而是精确读是 fail-closed 的：精确读要求所有参与的 SST 都带 preserved-sequence 标记，任何未标记且上界大于当前下界 C 的旧文件都会让整次读取报 `SequenceRangeUnsupported`，而不是静默少读[9]。因此本 PoC 在建表时就打开该选项、之后再写入，让下界 0 可用的范围最大。先用一个 region 即可，不复刻原实验的 K8s 部署和 16 分区配置。

原始表使用附录中的 span 字段，不含 `sample_reason`。两个 sink 使用同一组字段并增加 `sample_reason`，保持非 append 写入、`TIME INDEX (timestamp)` 和 `PRIMARY KEY (trace_id, span_id)`。始终输出原始 timestamp。

第一阶段可用 SQL 插入固定数据，便于安排写入顺序。真实 Flow 演示再使用 Python OpenTelemetry SDK 或已有 OTLP 生成脚本写入同样的场景。保留 `span_attributes.poc.case`，用于核对结果。

# 4 每轮执行过程

## 保存两个进度值

```rust
struct TailSamplingPocState {
    after_seqs: HashMap<RegionId, Sequence>,
    previous_eval_time: Timestamp,
}
```

每轮固定一个评估时间 `T`，所有查询使用同一个值。真实 Flow 使用该轮调度时间；场景测试直接注入时间，避免为 10 分钟窗口测试实际等待 10 分钟。

`C` 表示上次成功处理到的各 region sequence，`H` 表示本轮扫描返回的各 region 上界。它们都是 region 到 sequence 的映射。

## 首轮完整初始化

首次运行时，执行一次附录中的完整查询并写入 PoC sink。请求显式携带 `flow.return_region_seq=true`，完整消费结果并等待 terminal metrics，再保存 source watermark 和本轮评估时间。对于单 region PoC，缺少该 region 的 watermark 或存在 terminal error 时，本轮不能标记初始化完成。[7]

这一轮**不能注入 `incremental_mode`**，有两个原因：首轮还不知道 region id，而 `incremental_after_seqs` 缺少当前 region 时是硬错误、不会退化成全量读[10]；另外只有在请求不带上界时扫描才捕获 H，显式给上界会让 `snapshot_on_scan` 变假、不再产生新水位[11]。所以"先拿 region id 和 H"与"本轮就被 H 围栏"在同一次扫描里互斥，初始化必须分成这一步和后续查询。

初始化允许全量计算。后续轮次才使用下面的增量路径。进程重启时允许重新初始化，本轮不实现进度持久化。

**进度只能从查询一取，不能从采样查询本身取。** 采样查询对 `raw_spans` 有两次扫描（`recent` 和 `selected` 各一次），空输入时 JOIN 的一侧不产出数据、其扫描没有被执行到证明水位的程度，但它仍被算作参与分支；而水位合并是保守的——未证明的分支会否决已证明的分支，最终得到一个"有 region 参与但没有可用水位"的结果。查询一是单扫描形状，空表也能正常证明（实测空表上返回水位 0），所以它是唯一可靠的进度来源。

这不是写入语句的限制：同一形状的纯 `SELECT` 也同样拿不到水位，与是否 `INSERT` 无关。反过来，非空表上写入语句能正常返回水位。M4 接线时必须保持"查询一独立取水位"，否则空轮会让 `C` 停滞、每轮重复读同一段序列范围，而表现只是开销变大、不会报错。

因此首轮把两件事分开：先用普通 `SELECT` 取得 region 与 C，再执行初始快照写入。

## 查询一 找新增 span 涉及的 trace

```sql
SELECT DISTINCT trace_id
FROM raw_spans
WHERE `timestamp` >= :T - INTERVAL '10' MINUTE;
```

通过现有 Flow query extensions 将这个 source scan 限定到 `(C, H]`，使用 exact sequence 模式。流读完后取得 trace ID 集合 `D_new` 和 terminal watermark `H`。

查询一也显式要求返回 region watermark。即使返回零行，也取得并使用 terminal watermark；缺少 source region 的水位时保留旧进度并报错。进度不能来自结果行的 `MAX(sequence)`，也不用把内部 sequence 列暴露给 SQL。

这里有一条必须遵守的不变量：**查询一的时间谓词下界必须始终覆盖查询三要读的时间范围**。查询一把进度推进到 H 时，序列号小于 H 但事件时间落在查询一谓词之外的行不会再被查询一发现；如果查询三的回看范围比查询一更宽，那些迟到 span 会被永久漏掉，而且不会报错，只会静默少数据。本 PoC 两个范围都是 10 分钟，所以成立；改动其中任何一个都必须同时改另一个。

## 查询二 找时间推进涉及的 trace

```sql
SELECT trace_id
FROM raw_spans
WHERE `timestamp` >  :Tprev - INTERVAL '30' SECOND
  AND `timestamp` <= :T     - INTERVAL '30' SECOND

UNION

SELECT trace_id
FROM raw_spans
WHERE `timestamp` >= :Tprev - INTERVAL '10' MINUTE
  AND `timestamp` <  :T     - INTERVAL '10' MINUTE;
```

第一个区间找等待条件可能刚满足的 trace。第二个区间找有 span 刚退出回看范围的 trace。每 5 秒一轮时，两个区间各宽 5 秒。

此查询读取截至 `H` 的历史数据。对于本 PoC 新建且保留 row sequence 的 source，使用现有 exact `(0, H]` 读取：各 region 的下界为 0，上界为查询一返回的 `H`。这样 flush 后仍能读到本轮需要的历史行。[4]

得到 `D_time`，在 Rust 中用 `HashSet` 合并：

```text
D = D_new ∪ D_time
```

查询一没有新增行时，仍执行查询二。等待到期不依赖新 span 到达。

## 查询三 对受影响 trace 重跑原 SQL

若 `D` 为空，跳过本查询和 sink 写入。

否则，在附录的 `recent` 中增加 trace ID 条件：

```sql
WITH recent AS (
  SELECT *
  FROM raw_spans
  WHERE `timestamp` >= :T - INTERVAL '10' MINUTE
    AND trace_id IN (:trace_ids)
),
selected AS (
  -- 保留附录中的 GROUP BY、HAVING 和 sample_reason 表达式
)
-- 保留附录中的最终投影和 JOIN
```

这里的 `:trace_ids` 表示由查询一、二得到的实际 key 集合。实现时生成 DataFusion literal 表达式，或者为受控 fixture 生成正确转义的 SQL 常量列表；它不是新的数据库参数语法。

查询三同样读取 exact `(0, H]`，不能沿用查询一的下界 `C`。对每条受影响 trace，必须读齐它在当前 recent 内的新旧 span。

将返回的行按原列顺序写入 PoC sink。第一版允许这些 trace 的旧 span 再写一次。

为限制单条查询大小，可固定每批最多 1,000 个 trace ID。每条 trace 只进入一个批次，各批次共享 `T` 和 `H`，只按 trace ID 分批，不按 span 时间切分。

## 完成本轮

所有查询和 sink 写入成功后：

```text
C = H
Tprev = T
```

`D` 为空的成功轮次也推进这两个值。某轮失败时保留旧值，沿用已有错误日志，下轮重新执行即可。

整个 helper 可按下面的顺序实现：

```text
run_round(T):
    if 尚未初始化:
        执行一次原始全量查询并写入 sink
        从 terminal metrics 保存 C，保存 Tprev = T
        return

    D_new, H = query_new_trace_ids(C, T)
    D_time   = query_time_trace_ids(Tprev, T, H)
    D        = union_distinct(D_new, D_time)

    for trace_ids in chunks(D, 1000):
        rows = query_original_sql_for_traces(trace_ids, T, H)
        write_existing_sink(rows)

    C = H
    Tprev = T
```

选择三个显式查询，是为了让 PoC 能逐步打印和核对中间结果，并在空轮直接跳过历史重算。暂时保留 query 级 sequence hint，每条查询里的 source scan 使用相同范围。

# 5 最小代码接入

## 先实现一个可调用的执行函数

新增一个局部模块，例如：

```text
src/flow/src/batching_mode/tail_sampling_poc.rs
```

这个路径是建议的新文件，不代表已有实现。模块只负责固定模板、上述三次查询、内存进度和 sink 写入。测试 runner 与真实 Flow 共用同一个 `run_round`。

复用 `FrontendClient::query_with_terminal_metrics`。它已经接收 query extensions 和 `snapshot_seqs`，并返回带 terminal metrics 的结果。[2]

| 查询 | `incremental_after_seqs`（独占下界） | 上界来源 | 行级 `__sequence` 围栏 | 结果用途 |
| - | - | - | - | - |
| 首轮全量初始化 | 不注入 `incremental_mode` | 扫描捕获并返回 H | 否，全量快照 | 初始结果和进度 |
| 查询一 | C | 扫描时钉住已提交序列并返回 H | 是 | 新 trace ID 和本轮进度上界 |
| 查询二 | 各 region 为 0 | 显式 `snapshot_seqs` = H | 是 | 时间变化的 trace ID |
| 查询三 | 各 region 为 0 | 显式 `snapshot_seqs` = H | 是 | 完整 trace 重算结果 |
| 对照查询 | 各 region 为 0 | 显式 `snapshot_seqs` = H | 是 | 原始全量 SQL 结果 |

这里有两个独立旋钮，不要合并成一个"sequence range"概念：

- `incremental_after_seqs` 是各 region 的独占下界，只有配合 `incremental_mode=sequence_range` 才打开行级 `__sequence` 过滤。
- `snapshot_seqs` 只是上界，**不提供下界**。上界不进 SST 行过滤（`snapshot_sequence` 在扫描中仅用于内省[13]），它靠"memtable 过滤 + 已 flush 前沿围栏"保证不多读：比 H 新的行只要还在 memtable 里就会被过滤，而一旦 H 落后于已 flush 前沿，非精确模式会 fail-closed 报 `SnapshotFenceStale`，不会静默多读[17]。所以上界不是问题所在。
- **下界才是关键**：非精确模式下 SST 侧只有一个文件级剪枝提示 `sst_min_sequences`，它只跳过整体不大于 C 的文件，没有行级过滤，因此读到的是超集而不是 `(C, H]` 增量[18]。这与"查询一必须只看到新行"直接冲突，所以查询一也必须走精确模式；而 `sst_min_sequences` 与精确模式互斥，二者只能选一个。
- 精确模式对 memtable 和所有 SST 都做行级过滤，并且允许 H 落后于已 flush 前沿（围栏由行级过滤保证，不再比较前沿），这正是 §7"Flush 后回查"能成立的原因[9][12]。
- 查询一虽然不显式给上界，`snapshot_on_scan` 会让引擎在扫描时把上界钉到当时的已提交序列，因此查询一同样得到行级 `(C, H]`，返回的 watermark 就是这个被钉住的值、也就是过滤用的上界[19]。这意味着 `C = H` 不会漏行：报出去的 H 与用来过滤的 H 是同一个数。
- 查询二、查询三、对照查询要显式给 H：它们是后续轮次，不能重新钉一个新上界，否则与同轮对照表分叉。
- 精确模式下时间谓词可以叠加：按时间范围被排除的文件不可能含有落在 `(C, H]` 内的行，这是引擎里写明的论证，并有同时使用时间谓词与 exact 范围的现成测试[12][15]。查询二、查询三、对照查询都依赖这一点。
- 不要在任何一条查询里设置 `sst_min_sequences`：它和精确范围同时出现是硬错误[14]。

不要新增用户可见的内部 sequence 列。

## 再接入真实 Flow 调度

通过现有 `BatchingExecutionFactory` 创建 execution，在 `BatchingExecution::execute_once` 中调用同一个 `run_round`。该接口可以接收 task、query engine 和 frontend。[3]

standalone 侧的注册点目前是空实现：`src/plugins/src/flownode.rs` 的 `setup_flownode_plugins_post_build` 直接返回 `Ok(())`，`FlownodeBuilder::build` 只用 `Plugins::get::<Arc<dyn BatchingExecutionFactory>>()` 取值[5]。所以这是本轮要新增的接线，不是套用现成钩子：在该函数里插入 PoC factory，再沿用 `set_plugins` 接线。

只注册这一个 factory，不需要装饰默认 factory——OSS 目前没有任何默认 factory 注册，engine 在 `execution` 为 `None` 时走原有默认执行路径[16]。因此 PoC factory 必须在非本实验任务上返回 `None`，而不是自己实现默认行为。同时不要并行插入第二个同类型值：`Plugins::insert` 遇到同类型重复会 panic。

factory 按传入的 sink 元数据匹配 `sampled_spans_poc` 的完整名称，并核对 source 为本实验的 `raw_spans`，只为这个实验任务启用 PoC execution；其他任务返回 `None`。创建的 Flow 命名为 `trace_tail_sampling_poc`，但 factory 无需为匹配 Flow 名再增加查询。启动日志明确打印已启用的 PoC execution，不新增正式 DDL 参数。

复用已有执行 guard 和 sink 写入路径。直接调用 frontend 不会自动继承 task context；每个内部查询都显式转发 `task.query_context_snapshot()` 中的 `FLOW_SCHEDULED_TIME_MILLIS`，让所有 `now()` 使用本轮调度时间。场景 runner 使用同一扩展注入 T。[6]

保持 `EVAL INTERVAL '5s'` 的每轮调用，即使没有新写入也检查时间变化。不要把该 PoC 再送入现有的简单增量聚合 analyzer。

最终演示必须从 `CREATE FLOW` 开始，由 Flow 自动产生采样结果。手动调用 helper 只用于前面的定位与场景测试。

# 6 实施顺序

| 阶段 | 实现内容 | 当场能看到的结果 |
| - | - | - |
| M0 全量基准 | 建新表，复用原采样 SQL，生成少量正常、错误和慢 trace | baseline sink 内容符合规则 |
| M1 Sequence 读取 | 调用真实 FE 查询路径，连续两轮读取不同 sequence 范围 | 第二轮只返回新写入涉及的 trace，且能打印 C 和 H |
| M2 三查询执行 | 实现时间查询、key 合并、局部重算和内存进度 | 注入时间后可处理无写入到期与迟到补写 |
| M3 结果对照 | 同一输入、T、H 下运行全量与 PoC，比较累计 sink | 第 7 节的固定场景全部通过 |
| M4 Flow 接线 | 将同一 helper 接到 batching execution | **移出本轮范围**，见下 |
| M5 开销对照 | 运行约 2 万 trace 的静止、稀疏变化和活跃场景 | 产出扫描、查询耗时和 sink 提交行数对照 |

按这个顺序推进，不先建设通用改写器。M1 若没有真正读到跨 flush 的 sequence 范围，先修通该调用，后面的 SQL 改写暂不展开。

## M4 移出本轮范围的原因

让它在开源版成为"自动运行的 Flow"，需要用户能用 DDL 声明精确 sequence 范围模式。查询层接口本身已经在开源版可用（`flow.incremental_mode=sequence_range`、`flow.incremental_after_seqs`），但 `CREATE FLOW` 侧的哨兵 `__greptime_internal_exact_sequence_range` 在开源版被拒绝[21]。因此"机制可用"已经验证，"用户能通过 DDL 用上"不能在开源版内完成，属于企业版工作。

本轮 PoC 到 M5 的开销对照为止。后续在企业版接线时，下面这条实测结论必须带过去，否则会得到静默的错误行为：

**进度水位必须从单扫描查询取。** 采样查询对 `raw_spans` 扫了两次，没有执行的 JOIN 分支会被算作"参与了但未证明"，按保守合并规则（`merge_region_watermark_entries`[20]）否决另一分支已经证明的水位，返回 `Some(空 map)` 而不是 `None`。后果是空轮的水位 `C` 停滞、每轮重复全量重算，且不报错。这与 INSERT 语句、与空表都无关，只取决于查询是否单扫描。

# 7 固定验收场景

场景测试以“写入一批数据，再执行一个指定 T 的 tick”为基本步骤。全量对照查询和 PoC 使用同一个 H。场景结束后比较两个累计 sink 的稳定键和所有列值。

本轮局部查询结果还应等于“同一轮全量 SQL 结果中 trace_id 属于 D 的部分”。不要将累计 sink 直接与当前一轮 SELECT 结果比较。

| 场景 | 数据安排 | 通过条件 |
| - | - | - |
| 基础规则 | 正常 trace、子 span ERROR、1.5 秒慢 trace、跨分钟边界、20 分钟前的 trace | 与原实验预期一致；选中时保留当前 recent 中的整条 trace |
| 无写入等待到期 | 初次查询时最早 span 只有 20 秒，随后只推进 T | sequence delta 为空仍能选中；覆盖恰好 30 秒的边界 |
| 迟到错误首次入选 | 先写正常 span，之后补写相同业务时间范围的 ERROR span | 新旧 span 全部补回；不借助人为拉长 trace 时长触发慢规则 |
| 已选中 trace 补写 | 错误 trace 补一个正常 span；慢 trace 再补 ERROR | 新 span 出现；当前 recent 内的旧行按原 SQL 更新 reason |
| 旧事件时间的新输入 | 新 sequence 的 span 开始时间早于上轮 T，但仍在 10 分钟内 | 被查询一发现，不依赖事件时间递增 |
| 无写入窗口过期 | 推进 T 使唯一 ERROR 退出窗口，分别保留慢 span 和仅保留快 span | error 可变 slow；失选时不新增输出；原有长期行不删除 |
| 空结果推进 | 新写入的行全部不在 recent 内，之后再执行一个无写入 tick | 第一轮仍推进到 H，下一轮不重复读取同一 sequence 范围 |
| Flush 后回查 | 查询一取得 H 后主动 flush，再执行时间查询和历史重算 | exact (0,H] 仍能读到旧 span，结果与全量对照一致 |

窗口测试同时覆盖刚好等于下界与刚越过下界。已有行在 `timestamp = T - 10min` 时仍属于 recent。

场景数据的时间戳必须**相对 `T` 生成**，不能让 `T` 远离真实时钟。source 的 `ttl = '1h'` 按时间索引列计算，而不是按写入时间；一旦把 `T` 钉在过去（例如固定到某个历史时刻），数据会在写入后立即过期、查不到，场景会以"结果为空"而不是"规则不对"的方式失败。做法是先把 `T` 定在离当前时钟不到一分钟的位置，再由 `T` 加减偏移量生成所有 span 时间戳；`T` 通过 `flow.scheduled_time_millis` 注入，普通 SQL 的 `now()` 也会读取该扩展，因此不需要改写 SQL 文本。

真实 Flow 演示单独执行一个短场景：启动后写入正常、错误和慢 trace，持续观察等待到期，再补写迟到 span。保留 Flow 元数据、每轮日志和最终采样结果，证明 M4 使用的是自动调度。

# 8 开销对照

## 固定一组可复现输入

先用 20,000 条 trace、每条 3 个 span 作为约 6 万 span 的固定规模。用固定随机种子生成正常、错误和慢 trace，默认各 1% 错误和慢 trace，两个集合不重叠。新数据集不要求复刻原报告的随机计数。

对同一数据和 tick 序列分别运行全量基准与 PoC：

1. **静止场景。** 先确保所有 span 的 timestamp 都早于测量起点减 30 秒，并在起点完成一次成功 tick；再选择没有 span 跨过 10 分钟边界的时间段，连续执行 12 个 tick。PoC 的 D 应为空，查询三和 sink 提交行数应为零。年龄查询扫描每个 span，因此仅要求每条 trace 的最早 span 超过 30 秒还不够。

2. **稀疏变化场景。** 每轮只给固定的 1% trace 追加 span。观察历史重算是否集中到这些 trace 及时间范围涉及的 trace。

3. **活跃场景。** 每轮大部分 trace 都追加 span。记录方案退化时的实际开销，作为局部重算收益的边界。

正确性对照可串行执行并共享 T、H。性能对照分别运行，关闭额外的 oracle 查询，保持输入、构建和缓存准备方式一致。

## 输出一份 CSV 即可

每轮至少记录：

```text
mode,tick,eval_time,
new_trace_ids,time_trace_ids,affected_trace_ids,
q1_ms,q2_ms,q3_ms,total_ms,
sink_submitted_rows,sink_visible_rows
```

基准中不适用的分项留空。基准可见行数统计和 PoC 可见行数统计都放在计时范围之外。

已有执行指标能提供扫描行数、读取字节或聚合输入行数时，一并记录。拿不到的指标留空，不把查询返回的 trace ID 数冒充存储扫描行数，也不为本轮新做监控系统。

保存一次全量和局部查询的 `EXPLAIN` 或已有分析输出，检查 trace 条件是否在聚合前生效。若历史扫描仍读完整窗口，报告实际少做的聚合和写入，以及未减少的读取部分。先完成能运行、结果一致和空轮停止重写的闭环，不预设固定倍数的提速门槛。

## 实测结果

输入：20,000 条 trace、60,000 个 span，固定种子的生成器，1% 错误、1% 慢且两个集合不重叠。三场景各自串行运行 PoC 与全量基准，共用同一份数据与 tick 序列。表中是每个场景稳定后的单轮耗时（首轮含初始化，已排除）。

| 场景 | PoC 单轮 | 全量基准单轮 | 比值 | PoC 每轮提交行 | 基准每轮提交行 |
| - | - | - | - | - | - |
| 静止（12 tick 无写入） | 85 ms | 2,200 ms | **0.04x** | **0** | 1,200 |
| 稀疏（每轮 1% trace 追加） | 1,790 ms | 3,200 ms | 0.56x | 800 | 2,200–4,600 |
| 活跃（每轮 50% trace 追加） | 18,000 ms | 2,900 ms | **6.2x 慢** | 40,000–60,000 | 80,000–110,000 |

同样三个场景在 200 条 trace 上测过一遍，结论方向一致而幅度更小：静止 0.52x（50 ms 对 97 ms），稀疏约 1.35x 慢、活跃约 1.7x 慢。PoC 的固定开销基本不随数据量增长：静止场景从 200 到 20,000 条 trace 只从约 50 ms 涨到 85 ms，因为它只扫时间窗口内的时间列与 trace 列、不做聚合、不写表；而全量基准的成本随数据量线性增长（97 ms 到 2,200 ms）。

### 结论

**收益主要来自停止重复写入，而不是减少扫描。** 静止场景下受影响 trace 集合为空，局部重算被整体跳过，写入从每轮 1,200 行降到 0。这正好对应批量实验报告的痛点：物理写入 3,925 行、不同 span 1,211 个的写放大。稀疏场景也靠这一点取胜，但赢的幅度小得多，因为此时局部查询已经要处理 200 条 trace，而受限查询并没有比全量查询便宜多少。

两次 `EXPLAIN` 输出可以看清谓词落点：`trace_id IN (...)` 确实下推到了 `TableScan` 的 `partial_filters`，与时间谓词一起在扫表阶段生效，早于 `Aggregate` 和 `Inner Join`，说明"trace 条件在聚合前生效"这一条成立。但生效不等于能剪掉整段数据，这里有两层粒度：

- **row group 级**：默认 row group 是 100 × 1024 = 102,400 行，本实验的 6 万行整表落在单个 row group 内，`row_groups_by_minmax`[23] 在这一规模下结构上无事可做。
- **行组内块级**：`trace_id` 的 skipping index 是 bloom filter，默认 granularity 是 10,240 行，applier 在每个 row group 内按块把输出收窄成 `Vec<Range<usize>>`[22]。这一层即使在单个 row group 内也仍然生效。

两层叠加的实测表现是：受限查询确实少读了 bloom filter 判定不可能命中的块，但没有量级差别。稀疏场景 Q3 用约 1,500 ms 处理 200 条 trace，而全量查询用约 3,000 ms 处理同一窗口，省下的是块级裁剪和"只物化受影响 trace 的行"，不是整段跳过。所以"查询变快"不是这个方案的主要收益来源，收益主要来自"不写不变的输出"。

**活跃场景明确退化，而且退化原因可以定位。** 受影响 trace 达到 10,000 条时，按每批 1,000 个 trace ID 的限制会拆成 10 条查询，每条都要扫一遍窗口，于是扫描量放大 10 倍；而全量基准只扫一遍就得到全部结果。这不是方案的固有上限，而是分批阈值的代价：当 `|D|` 大到接近全量时，应当退化成一次不带 trace 过滤的全量查询，而不是继续分批。本轮没有实现该退化分支，因此上表记录的 6.2 倍是当前实现的真实开销。

这一场景还说明**写放大和扫描成本是两个独立的量，不能只优化其中一个**。活跃场景下 PoC 每轮提交 40,000–60,000 行，而全量基准提交 80,000–110,000 行，PoC 的写入其实少了一半以上；但多出的 10 倍扫描把这点收益完全吃掉，总耗时反而慢 6.2 倍。静止场景则是另一个极端：写入降到 0，扫描没有增加，于是净收益最大。两者的差别只在于 `|D|` 占窗口内 trace 总数的比例。

### 使用边界

- 静止与低变化率场景可以放心使用：收益随数据量放大，写入降到零。
- 高变化率场景不要使用当前实现：应先按 `|D|` 占窗口内 trace 总数的比例决定是否退化成全量查询。
- 所有场景的正确性都成立：每个场景两个模式跑完后逐行比对 sink，完全一致。

# 9 实现交付清单

本轮实际交付把上面设想的多文件拆分收敛到了单个测试文件，理由是所有步骤都要走真实 FE 查询路径、共享同一个 standalone 实例，拆成脚本反而需要额外维护连接与启动逻辑：

| 实际交付 | 内容 |
| - | - |
| `tests-integration/tests/flow_tail_sampling.rs` | 建表、数据生成、`run_round`、三条查询、全量基准、六个测试 |
| `tests-integration/tests/main.rs` | 注册该测试模块 |
| 本文 | 计划、实测结果与结论 |

文件内对应的部分：

| 部分 | 位置 |
| - | - |
| 建表语句 | `create_tables` |
| 采样 SQL（可带 trace 过滤） | `sampling_select` |
| 三条查询与内存进度 | `run_round`、`query_new_traces`、`query_time_traces`、`write_poc_sink` |
| 全量基准 | `run_baseline` |
| 固定场景与开销对照 | `test_cost_comparison` |
| 逐轮结果 CSV | 由 `POC_RESULTS_CSV` 指定，默认 `/tmp/flow-tail-sampling-results.csv` |

复现命令：

```bash
# 六个测试全部运行
cargo nextest run -p tests-integration --test main -E 'test(flow_tail_sampling::)'

# 只跑开销对照；规模、场景和 tick 数由环境变量控制
POC_TRACE_COUNT=20000 POC_SCENARIOS=static POC_TICKS=6 POC_RESULTS_CSV=/tmp/poc.csv \
  cargo nextest run -p tests-integration --test main \
  -E 'test(flow_tail_sampling::test_cost_comparison)' \
  --config 'profile.default.slow-timeout.period="900s"' \
  --config 'profile.default.slow-timeout.terminate-after=1'
```

`POC_SCENARIOS` 接受逗号分隔的 `static`、`sparse`、`active`，建议分别运行：2 万规模下单个场景就要分钟级，全部一起跑会触发超时。`POC_TICKS` 缺省时静止场景 12 轮、其余 6 轮。

完成标准的状态：固定场景全部通过；flush 后历史回查可用（`test_historical_spans_survive_flush`）；静止阶段停止重复写入（`test_cost_comparison` 中断言每轮提交为 0）；提供了一组可复现的全量与 PoC 开销对照。**"真实 Flow 自动连续执行"未完成，属于企业版范围**，原因见第 6 节。

# 附录 建表示例

下面的 DDL 用于新的 PoC 数据库。先建表，再运行数据生成器。

```sql
CREATE TABLE raw_spans (
  `timestamp` TIMESTAMP(9) NOT NULL,
  timestamp_end TIMESTAMP(9) NULL,
  duration_nano BIGINT UNSIGNED NULL,
  trace_id STRING NULL SKIPPING INDEX,
  span_id STRING NULL,
  parent_span_id STRING NULL,
  service_name STRING NULL,
  span_name STRING NULL,
  span_kind STRING NULL,
  span_status_code STRING NULL,
  span_status_message STRING NULL,
  `span_attributes.poc.case` STRING NULL,
  TIME INDEX (`timestamp`),
  PRIMARY KEY (trace_id, span_id)
) WITH (
  'append_mode' = 'true',
  'preserve_row_sequence' = 'true',
  'ttl' = '1h'
);

CREATE TABLE sampled_spans_poc (
  `timestamp` TIMESTAMP(9) NOT NULL,
  timestamp_end TIMESTAMP(9) NULL,
  duration_nano BIGINT UNSIGNED NULL,
  trace_id STRING NULL SKIPPING INDEX,
  span_id STRING NULL,
  parent_span_id STRING NULL,
  service_name STRING NULL,
  span_name STRING NULL,
  span_kind STRING NULL,
  span_status_code STRING NULL,
  span_status_message STRING NULL,
  `span_attributes.poc.case` STRING NULL,
  sample_reason STRING NULL,
  TIME INDEX (`timestamp`),
  PRIMARY KEY (trace_id, span_id)
) WITH ('ttl' = '30d');
```

创建 `sampled_spans_baseline` 时复用完全相同的 sink DDL，只替换表名。PoC 用这里的单 region source；原实验自动建表时的分区配置不影响本轮规则对照。

# 附录 原始采样查询

以下是对照模板。测试 runner 将 `now()` 绑定到指定 T；PoC 的查询三只在 `recent` 中增加受影响 trace ID 的过滤，其他表达式保持一致。

```sql
WITH recent AS (
  SELECT *
  FROM raw_spans
  WHERE `timestamp` >= now() - INTERVAL '10' MINUTE
),
selected AS (
  SELECT
    trace_id,
    CASE
      WHEN max(
        CASE WHEN span_status_code = 'STATUS_CODE_ERROR'
             THEN 1 ELSE 0 END
      ) = 1
      THEN 'error'
      ELSE 'slow'
    END AS sample_reason
  FROM recent
  GROUP BY trace_id
  HAVING min(`timestamp`) <= now() - INTERVAL '30' SECOND
     AND (
       max(
         CASE WHEN span_status_code = 'STATUS_CODE_ERROR'
              THEN 1 ELSE 0 END
       ) = 1
       OR max(timestamp_end)
          > min(`timestamp`) + INTERVAL '1' SECOND
     )
)
SELECT
  s.`timestamp`,
  s.timestamp_end,
  s.duration_nano,
  s.trace_id,
  s.span_id,
  s.parent_span_id,
  s.service_name,
  s.span_name,
  s.span_kind,
  s.span_status_code,
  s.span_status_message,
  s.`span_attributes.poc.case`,
  t.sample_reason
FROM recent s
JOIN selected t ON s.trace_id = t.trace_id;
```

# 参考

[1] 用户提供的《基于 GreptimeDB Flow 实现 Trace Tail Sampling》，更新于 2026-10-19。表设计与 SQL 见第 2 至 5 页，验证结果与写放大见第 6 至 7 页，限制见第 8 页。

[2] [FrontendClient 查询与 terminal metrics 接口](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/flow/src/batching_mode/frontend_client.rs#L549-L559)。

[3] [BatchingExecution 与 Factory 接口](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/flow/src/batching_mode/batching_execution.rs#L28-L59)。

[4] [新建 source 保留 row sequence 并在 flush 与 compaction 后按 exact 范围读取的已有测试 `test_exact_sequence_read_compacted_sst_with_preserve_row_sequence`](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/mito2/src/engine/scan_test.rs#L2167-L2269)。

[5] [standalone 注册 Flow plugin 并构建 flownode 的位置](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/cmd/src/standalone.rs#L633-L651)，以及 [builder 取得 BatchingExecutionFactory 的位置](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/flow/src/server.rs#L335-L344)。

[6] [Flow 为当前轮次设置调度时间的代码](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/flow/src/batching_mode/task.rs#L1925-L1943)。

[7] [sequence 范围参数与 watermark 收集配置](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/query/src/options.rs#L205-L273)。

[8] [`preserve_row_sequence` 可由 ALTER 打开，且要求 append-only；非 append 表会报 `preserve_row_sequence is only supported for append-only tables`](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/mito2/src/engine/alter_test.rs#L3060-L3080)。

[9] [精确读无法成立时 fail-closed，抛出 `SequenceRangeUnsupported` 而非静默返回范围外的行](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/mito2/src/engine.rs#L1295-L1315)。

[10] [`validate_for_scan`：缺少当前 region 的 `incremental_after_seqs` 是硬错误，不会退化为全量读](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/query/src/options.rs#L236-L266)。

[11] [`snapshot_on_scan` 仅在请求未带上界时才成立，显式上界会抑制水位捕获](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/query/src/dummy_catalog.rs#L425-L445)。

[12] [`exact_sequence_range` 的时间剪枝论证与实现：按时间范围被排除的文件不可能含有落在 `(C, H]` 内的行](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/mito2/src/read/scan_region.rs#L1850-L1885)。

[13] [`snapshot_sequence` 只用于内省，不参与 SST 行过滤；上界本身不构成围栏](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/mito2/src/read/scan_region.rs#L160-L172)。

[14] [`sst_min_sequence` 与精确范围同时出现是硬错误](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/mito2/src/read/scan_region.rs#L505-L515)。

[15] [时间谓词与 exact 范围同时使用的已有测试 `test_exact_sequence_range_ignores_time_pruned_unmarked_sst`：`ts >= 10s` 与 `(3, 6]` 叠加返回 3 行](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/mito2/src/engine/scan_test.rs#L2560-L2660)。

[16] [engine 在 `execution` 为 `None` 时走原有默认执行路径](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/flow/src/batching_mode/engine.rs#L750-L765)。

[17] [`validate_sequence_fences`：非精确模式下带上界且读 SST 时，H 落后于已 flush 前沿即报 `SnapshotFenceStale`](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/mito2/src/engine.rs#L1315-L1350)。

[18] [只有非精确模式才注入 `sst_min_sequence`；它是文件级剪枝提示，不是行级下界](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/query/src/dummy_catalog.rs#L447-L455)。

[19]
[20] [`merge_region_watermark_entries` 的保守合并规则：某个 sub-stage 未证明水位（`None`）会把同 region 已证明的水位降级为 `Unproved`，而不是取最大值](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/query/src/metrics.rs#L310-L345)。这是"进度必须从单扫描查询取"的代码依据。

[21] [开源版拒绝 `CREATE FLOW` 精确 sequence 范围哨兵 `__greptime_internal_exact_sequence_range`](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/common/meta/src/ddl_manager.rs#L1435)。

[22] [skipping index 默认 granularity 为 10,240 行](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/datatypes/src/schema/column_schema.rs#L74)，[bloom filter applier 在 row group 内按块收窄扫描范围](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/mito2/src/sst/index/bloom_filter/applier.rs#L420-L443)。

[23] [默认 row group 行数 `DEFAULT_ROW_GROUP_SIZE = 100 * 1024`](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/mito2/src/sst/parquet.rs#L78)，[row group 级 min-max 裁剪](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/mito2/src/sst/parquet/reader.rs#L1385-L1400)。
 [`snapshot_on_scan` 时引擎把 `memtable_max_sequence` 钉到当时的已提交序列，该值同时作为精确范围上界和返回的 watermark](https://github.com/GreptimeTeam/greptimedb/blob/52554db52fe36666240b9db1839057f6dfdc776d/src/mito2/src/engine.rs#L1066-L1080)。
