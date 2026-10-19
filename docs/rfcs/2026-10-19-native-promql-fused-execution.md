---
Feature Name: Native PromQL 的融合执行模型
Tracking Issue: N/A
Date: 2026-10-19
Author: "@discord9"
---

# RFC：Native PromQL 的融合执行模型

## 1. 目标与决策

本 RFC 定义 `FusedSeriesExec` 的可组合执行模型，以及它与 PromPlan、原生批次、内存管理和查询诊断的关系。它是实施设计，文中的新增接口不代表已经实现。

**第一期就采用可组合的 native 物理表达式树。** 已支持的运算可以按 PromQL 语义自由组合，不限制为一个窗口、若干 map 和一个末尾聚合。实现范围按算子及其数据类型划分，不按完整查询的固定形状划分。

本轮确定以下要求：

1. PromPlan 表达查询语义；物理编译生成已绑定输入、字段、网格和 kernel 的执行树。
2. Fused 内部节点通过函数调用直接传递 Rust struct；结构中复用 native 批次和 Arrow 缓冲，普通行转换只发生在明确的对外边界。
3. 窗口和即时选点支持分块输入、批量求值及分批输出。三项能力分别实现和验证。
4. 聚合是可组合节点。完整聚合结果可以继续进入 map、窗口或其他合法节点。
5. 运行态接入现有查询内存池，并继续使用现有存储扫描配额。
6. `EXPLAIN` 展示内部计算结构，`EXPLAIN ANALYZE` 展示对应 metrics；远端执行也能在前端观察。
7. 第一期不引入 JIT、通用 VM、公共子表达式缓存或独立调度框架。

本文核对的代码基线是 `upstream/narthex` 的 `a1052c0c0f7`。实验分支和待合 PR 中的批量 kernel 等优化按组件复用，不能当作该基线已经具备的能力。

### 1.1 要解决的问题

现有常规 PromQL 路径使用行批和区间数组组织求值。native 路径希望利用原始序列与规则求值网格，减少窗口构造、行列转换及重复绑定。

早期融合原型还有两个结构问题：从已生成的物理树重新识别查询形状，以及把 window、maps、partial aggregate 固定为顶层字段。原型的 `classify_output` 在构造期使用类型和位置启发式认领列，也缺少可靠的字段来源绑定；它不是逐批执行的热路径开销。

已有 kernel、语义测试和实验记录可以复用。固定模板和后置形状匹配不作为新执行模型的基础。是否获得端到端收益由新接线后的测量决定。

## 2. 从 PromPlan 到可组合执行树

### 2.1 三层职责

| 层 | 内容 | 职责 |
| --- | --- | --- |
| PromPlan | 查询运算、依赖、标签与时间语义 | 统一表达 PromQL，不携带某个物理节点的识别规则 |
| 已绑定 native 物理树 | 输入槽位、字段位置、kernel、网格、分组与匹配规则 | 决定如何在 native 数据上执行 |
| 每次执行的状态树 | 窗口保留、游标、输出块、聚合和匹配状态、reservation | 按批推进本次执行 |

本文的 PromPlan 指 planner 改造提供的统一语义计划。基线目前仍由 [PromPlanner](../../src/query/src/promql/planner.rs) 从 parser AST 生成 DataFusion LogicalPlan。此 RFC 不另造一套平行的语义 AST，也不宣称基线已有某个可直接调用的 `PromPlan` Rust 类型。

Fused 的逻辑承载节点引用 PromPlan 子图及输入边界。内部物理树是它的编译产物，不重新解释函数名字、默认参数、匹配修饰符或标签规则。

### 2.2 内部结构

Fused 内部的执行单位称为**计算节点（`NativeNode`）**。`kernel` 专指节点调用的数值计算函数；一个 Window 节点还拥有窗口推进、批次连接和状态管理职责。每次执行为每个计算节点建立独立的 `NodeMetrics`，不把所有同名函数合成一个诊断对象。

以下结构只说明组合方式，不规定最终 Rust 布局：

```rust
struct NativeNode {
    expr: NativeExpr,
    output: NativeType,
}

enum NativeExpr {
    Input(BoundInput),
    Scalar(BoundScalar),
    Instant { input: Box<NativeNode>, op: BoundInstant },
    Window { input: Box<NativeNode>, op: BoundWindow },
    Map { input: Box<NativeNode>, op: BoundMap },
    Aggregate { input: Box<NativeNode>, op: BoundAggregate },
    Binary {
        left: Box<NativeNode>,
        right: Box<NativeNode>,
        op: BoundBinary,
    },
}
```

每个节点有明确的输入和输出类型，以及已绑定的运算。`FusedSeriesExec` 持有这棵树、物理输入和出口绑定，不再持有唯一的 `window`、`maps` 或 `partial_agg` 槽位。

树的结构直接表达依赖，求值不需要额外的 `NodeId`。每个运行态直接持有自己的 `NodeMetrics`；EXPLAIN 和指标导出所需的定位由最终执行树的位置派生，见第 8 节，不进入 AST 的语义字段或批次数据。

一棵树即可表达本轮需要的组合。二元节点有两个子树；公共子表达式是否共享执行属于以后独立的优化。加入一个已支持运算的合法组合，应只改变编译出的树，不能要求增加一个完整形状的 matcher。

| 运算 | 输入与输出契约 |
| --- | --- |
| Instant | 原始样本到求值网格上的值，绑定 lookback、offset 等语义 |
| Window | 原始样本或已求值样本到本节点输出网格上的值 |
| Map / 标量广播 | 完整求值值到完整求值值，保持或显式更新字段、标签语义 |
| Aggregate | 按阶段接受值或聚合状态，输出完整值或聚合状态 |
| Binary / matching | 按已绑定规则对齐两个输入，执行基数、标签和时间语义检查 |

合法性来自这些类型及数据依赖。普通 map 不能消费尚未 finalize 的聚合状态；raw 样本也不能跳过 Instant 语义直接当查询求值点。两个窗口之间没有结构上的数量限制。

每个 Window 独立绑定输入网格和输出网格。Fused 不要求所有节点共用一个 `GridSpec`。Scalar 可以是常数，也可以是绑定了求值网格的计算值，不能把所有 scalar 都假设为时间不变。

### 2.3 物理编译与降级

物理编译按以下顺序进行：

1. 从 PromPlan 确定运算依赖、类型、所需时间范围和候选 native 区域。
2. 准备边界输入，检查 raw schema、标签绑定、ordering、分区分布和读取能力。
3. 为每个节点绑定字段、kernel、输入与输出网格、分组或匹配规则；为真实的数据交换建立边界。
4. 构造 native 执行树和明确的出口适配器，并生成对应的计划描述。
5. 某个算子或输入能力尚未接入时，在执行前为该语义片段选择普通 lowering，保留可继续使用的 native 区域。

准备扫描后才能得到的能力，在第 2 步检查。这个要求不意味着必须先构造完整普通物理树，再逆向恢复查询语义。[既有 extension planner 接口](../../src/promql/src/extension_plan/planner.rs) 可接收逻辑节点和已计划输入，具体接点与 PromPlan 接线统一。

降级理由进入计划描述。计划期不支持与查询本身非法分别处理。执行已经开始后遇到数据错误或内存不足，返回错误，不重放输入并切换到另一条路径。

### 2.4 融合边界

增加窗口、改变求值网格、添加标量广播、在完整聚合后继续计算，都不是天然的切分理由。

需要切分的是实际执行边界，例如跨执行分区的数据交换、尚无 native 实现的运算、或当前输入无法满足的分布要求。已共址的二元匹配可以在树内执行；需要重分布时由物理编译安排交换。跨序列运算不因此被永久排除在模型之外。

第一期允许合法节点阻塞式收集必要输入，例如在分区结束时完成聚合。可组合性不要求每个节点都立即产出结果；这些保留状态必须受内存池管理，计划也必须标明其输出时机。

## 3. 节点间的 Rust 数据结构

同一 Fused 内部通过函数调用、返回值或借用直接交接 Rust struct。按输入类型复用 `RawSeriesChunk`、`Series`，并补充 evaluated 分片和聚合状态所需的结构；不把每条内部连接包装成 DataFusion stream、frame 或序列化消息。

结构中的 Arrow 数组、buffer 和共享 schema 直接复用。同步消费可以借用；需要跨 poll 保留时持有结构或共享引用，并遵守第 7 节的内存责任。数据结构只包含计算需要的数据与批次边界，不携带节点定位信息。

### 3.1 Raw 输入与来源归并

复用 [RawSeriesChunk](../../src/datatypes/src/series/raw.rs)。它携带完整输入身份、时间戳、字段、标签绑定及 `is_last`。

`is_last` 对接收该流的算子表示该 series 在所有相关来源上的完成。即使 Fused 只有一个物理 child，该 child 也可能合并多个 region；来源归并必须在交付 Fused 前正确重算完成信号。

[merge_ordered_raw_series](../../src/promql/src/source/merge.rs) 已经是 typed raw stream 到 typed raw stream 的归并，不经过普通行批。满足它的 schema、绑定及 `KeyThenTime` 要求时可以复用。它处理物理来源归并，不代替表达式的二元 matching。

每个节点声明所需 ordering，编译器校验上游保证。第一版的逐序列窗口状态可要求 `Contiguous` 或更强的顺序；后续支持 `Interleaved` 时可更换状态组织。ordering、分区归属与完成信号是三个独立契约，不能互相替代。

### 3.2 Evaluated 批次

复用 `Series` 的字段和 `TimeAxis::Grid(GridRange)`，在流接口补上分片完成信息：

```rust
struct EvaluatedChunk {
    series: Series,
    is_last: bool,
}
```

输入 schema 固定在连接上。批次中的 `GridRange` 使用该连接网格上的绝对 `start/len`，不能使用批内行号代替绝对网格位置，也不能混用内层与外层网格索引。

非末片的区间表示这段网格已经完成求值，缺失位置使用现有 validity 表达。必要的全空区间也应推进下游，不只发送有值的点。计算进度与有效值覆盖范围不同，中间连接不能因裁剪 null 而丢失进度。区间单调且不重叠。

`is_last` 不依赖最后一批有没有有效样本。已经发出前缀，但最后没有新结果时，仍须发出带 series 身份、网格和 `is_last=true` 的空末片。现有 [GridRange](../../src/datatypes/src/series/time.rs) 允许空区间，可以复用这个表示。

每条已开始的 series 恰好关闭一次。map、过滤及出口适配不能吞掉内部连接的完成信息。分区 EOF 不代替尚未关闭的 series；正常 EOF 前必须完成各节点的 drain。

改变标签的节点，对同一输出 series 的数据片、全空片及末片使用一致的变换后身份；不能用输入身份关闭已经改变标签的输出 series。

null、computed stale 值、float/histogram 混合窗口以及标签删除的语义沿用现有实现。批量快路径不能绕过这些语义；按数据类型选择 kernel 或局部回落，不以错误结果换取整条链命中。

### 3.3 聚合状态与 Scalar

聚合中间状态通过已绑定类型的 Rust struct 直接传递，包含分组、求值时间和对应 accumulator state，不伪装成普通求值值。结构内部可复用 Arrow state 数组；列布局和 state schema 由编译器绑定。需要交给 DataFusion aggregate 或实际交换边界时，再适配为该边界要求的 `RecordBatch`。

常数 Scalar 可保留为一个值并在 kernel 中广播。随网格变化的 Scalar 明确携带相应网格和有效性。两者都不要求为了连接下一个 native 节点而先展开成普通结果行。

## 4. 批量执行与状态生命周期

### 4.1 按批推进状态树

不可变执行树可由 `Arc` 共享。每次 `execute(partition)` 创建自己的运行状态，使用实际的输入 partition，禁止固定消费 partition 0。跨 series 的聚合状态与当前 series 的窗口状态分别持有和释放。

采用递归 pull/poll 即可：

1. 当前节点有 ready 输出时，交付一个受行数或字节预算约束的批次。
2. 否则向 child 拉取输入，更新本节点状态。
3. child 为 Pending 时向上传递 Pending；同步工作按预算让出执行线程。
4. series 完成时分批 drain 剩余结果，再发出结束并清理该 series。
5. 分区结束时完成需要 EOF 的聚合或匹配，并排空输出。

节点之间不建立无界队列，也不各自启动任务。每个节点只保留当前处理块、待交付块及算法必需的状态。CPU 工作继续遵循现有查询执行与运行时约定，不新建 executor 或运行时。

AST 的分派发生在节点或批次边界。kernel 内部仍可遍历样本、网格或 reset；禁止的是每点重新解释 PromPlan、查找函数或恢复列绑定。表达式树节点不强制对应一次普通行物化。

metrics 对应编译后实际执行的计算节点。第一期保留各节点独立执行与计时，不跨 `NativeNode` 合并数值执行区间；直接传递 native 数据不要求每个节点都新建输出数组。若后续把相邻 map 合并成一个 kernel，应展示合并节点及其来源运算，并报告合并后的时间，不能为已经合并的各运算虚构独立耗时。

### 4.2 现有 evaluator 需要的适配

基线 [RangeEvaluator](../../src/promql/src/range_eval.rs) 在非末 raw chunk 上会提前计算，但返回 `None`，只在最后返回完整 `Series`。现有 `push_series` 每次也把输入视为完整 series。两者不等于已经支持分片输出和分片消费。

复用窗口算术及已有正确性语义，增加以下能力：

- 分开输入推进与提取 ready 输出，一次最多生成一批。
- 给 [GridSeriesBuilder](../../src/promql/src/grid_series.rs) 增加分段取出能力；取出后保留绝对 cursor、series 身份和未完成的窗口状态。
- 消费 `EvaluatedChunk`，在同一 series 的多个片段之间保留状态。
- 收到末片后分批 drain，最后一个输出之后才清理 series。
- 暴露内存增长与保留容量，供运行态管理 reservation。

现有 [select_instants](../../src/promql/src/instant_eval.rs) 接受完整输入轴；Instant 节点也需要保存候选样本、stale 状态和网格游标的增量适配。不能声称现成函数直接具备流式接口。

旧的完整 `Series` 接口可以作为便利包装保留。窗口、即时选点的第一期 native 连接应通过分片接口验收；聚合、matching 等按语义可保留需要完整范围的执行策略，并明确内存与输出时机。

## 5. 可组合的聚合

### 5.1 时间维度与执行阶段

状态索引是 `(group labels, absolute grid index)`，或等价的按网格寻址的每组累加器数组。[现有 planner](../../src/query/src/promql/planner.rs) 也把 timestamp 加入普通聚合的分组键。

`Aggregate` 在物理树上有三种执行阶段：

| 阶段 | 输入 | 输出 |
| --- | --- | --- |
| Complete | 本执行范围内完整的分组贡献 | 完整的 native evaluated 值 |
| Partial | 本分区的贡献 | 带求值时间的聚合 state |
| Final | 已收齐相关贡献的 partial state | 完整的 native evaluated 值 |

Complete 和 Final 输出的结果可以继续进入 Map、Window、Binary 或另一层 Aggregate。Partial state 只能交给接受该状态类型的运算。

复用既有 accumulator 的状态布局与数值语义。若与 DataFusion final aggregate 连接，必须精确匹配其 state schema；内部 native Final 可以复用 accumulator 的 merge/finalize 能力，将完整值构造成 evaluated 批次。

### 5.2 完整性与分区

`PerSeries` 只保证单条 series 完整，不保证一个聚合组完整。编译器必须证明每个 `(group, time)` 的全部贡献位于当前执行范围，才选择 Complete 或执行 Final。

例如按 `Hash(job, host)` 分区时，`sum by(job)` 的一个组仍可能跨分区。分区键与聚合键的名字有交集不是完整性证明。

没有更强的分组完成保证时，第一版在本分区 EOF 产出 partial state；完整输入范围上的聚合也可在 EOF finalize。需要交换时复用现有分布式执行边界，交换前后都保留已绑定的类型。

`sum by(job)(rate(x[5m])) + 1` 必须先得到每个时间点的完整 sum，再做 native 标量广播。若两个分区各先加 1 再合并，常数会被重复计入。

## 6. Native 连接与 DataFusion 出口

Fused 内部节点不必分别实现 DataFusion `ExecutionPlan`，也不在每条内部边上 pack/unpack frame。它们通过第 3 节的 Rust struct 直接连接，交接发生在普通的函数调用和状态树推进中。

`FusedSeriesExec` 的外壳负责 DataFusion 输入、执行上下文、metrics 以及输出协议。出口适配器可以是外壳的已绑定配置，也可以是明确的适配节点：

| 消费方 | 交付方式 |
| --- | --- |
| 同一 Fused 内的 native 节点 | Rust struct 直接传递，复用其中的原生数据及状态 |
| 普通 Projection / Aggregate / 查询结果出口 | 显式转换为所需的普通行或 aggregate state schema |
| 必须跨物理节点或传输边界的 native 数据 | 使用该边界明确支持的 carrier 与兼容性契约 |

现有 [series_frame](../../src/common/query/src/series_frame.rs) 定义 raw frame 边界，不能当作 evaluated 分片协议已经存在的证据。第一期优先把可共处的计算放在同一 native 树，避免为了串联而创建 `Series → rows → Series` 往返。

普通 `ProjectionExec` 不负责识别 typed series。字段、标签、时间列及 state 列的输出映射来自编译器；边界可验证 schema，但不按下一个 Float64 列猜测来源。

输入或输出适配也是计划中可见、可计时、可计费的步骤。它是否物化数组由实际消费者要求决定，不能把 typed 批次本身宣传为所有运算都零拷贝。

## 7. 接入现有内存管理

### 7.1 使用哪一层框架

Fused 每次执行的运行态通过 `MemoryConsumer` / `MemoryReservation` 注册到传入的 `TaskContext.runtime_env().memory_pool`。继承现有查询配置和 Greedy/Fair 等策略，不另建 Fused 专属内存池或重复的限额开关。

[查询 runtime](../../src/query/src/query_engine/runtime.rs) 在配置有界池时接入 [MetricsMemoryPool](../../src/query/src/query_engine/state.rs)，已有 usage、rejected 和 top-consumer 信息；未设限时继承原有无界行为。

存储扫描继续使用现有 `QueryMemoryTracker`、`MemoryTrackedStream` 和 mito 内部 scan pool。它们与查询算子 pool 不是一个共享余额，不能宣称全链已经统一计费，也不能把同一 Fused 状态额外向多个框架重复申请。

### 7.2 计费范围与责任

每个执行实例建立有名称的 consumer，名称可定位 Fused 与 partition。内部 node 记录自己的保留量，用于诊断；同一份状态不因树的父子关系再向池重复申请。

以下内存必须纳入实际保留量：

- 跨批保留的原始或已求值 Arrow 缓冲、当前块与尚未交付的输出。
- 窗口样本、reset 索引、游标辅助结构和 builder 容量。
- 标签、分组键、slot 表、accumulator、二元匹配及必要的大型临时缓冲。

`Vec` 按 capacity 而非 len 统计。清空逻辑长度但保留容量时不能释放对应额度。一个很小的 Arrow slice 可能保留整个底层分配；同一执行实例当前仍持有的批次和节点状态中，共享底层分配按局部共享关系统计，不能按视图或 chunk 数量虚增。释放后更新当前保留集合，不能用一个永不清理的历史地址集合来判断后续分配。

沿用 DataFusion 的保留者责任：同步流经的批次不因每次传递重复收费；producer 交付后不继续对已移交对象收费，下游跨 poll 或跨批保留时承担 reservation。Fused 内部移交可保持已有额度，生命周期拆分使用现有 reservation 能力。

这一约定不建立跨查询的全局 buffer 地址表，也不为每个批次引入新的额度 token。内存池统计运行态保留量，不等于进程所有 Arrow 分配的精确 heap 追踪。

### 7.3 增长、拒绝与释放

已知的分配或保留增长在发生前 `try_grow`。没有预留接口的 accumulator 按批及时核对 `size()`，连同外部分组表及索引容量调整 reservation；可预估的大增长先预留。

内存不足沿既有资源不足错误返回，停止拉取输入并释放运行态，不绕过限额继续分配。第一期未实现 spill 的节点按不可 spill consumer 注册；不能因为底层可用 FairSpillPool 就宣称本节点具备 spill。

正常结束、错误和取消均释放保留对象及 reservation。释放或移交对象后再 shrink/free，stream 被 drop 时由 RAII 收尾。已经交付的数组遵循下游所有权。

[NativeInstantStream](../../src/promql/src/extension_plan/instant_manipulate/native_exec.rs) 已有注册、try_grow 与失败释放示范。RangeEvaluator、GridSeriesBuilder 及直接使用的 accumulator 仍需接入，不能认为复用它们就自动完成计费。batch 行数或 payload 上限是工作预算，不是 resident memory 限额。

## 8. EXPLAIN、ANALYZE 与 metrics

这些是第一期交付条件。debug 日志仅用于辅助诊断，不能作为判断 native 是否执行的唯一途径。

### 8.1 计划展示

普通 `EXPLAIN` 展示：

- Fused 的物理输入、执行位置、partition 分布和出口模式。
- 内部树、节点运算和依赖，以及与节点指标一致的树位置。
- Window 的输入/输出网格、range、offset、anchor，以及字段绑定。
- 聚合阶段、分组时间维度和完整性依据；二元 matching 的关键规则。
- 源 ordering、完成信号的来源，以及普通路径的具体降级理由。

默认输出保持可读，`VERBOSE` 展开绑定细节。内部节点展示在 Fused 的描述中，不伪造为 `ExecutionPlan::children()` 中可独立执行的 child。

纯 `EXPLAIN` 不执行或 poll 数据扫描。若实际 Fused 只在 datanode 构造，需要在远端 query/describe 通道补充只准备并返回计划描述的能力，复用与执行相同的编译器。现有 describe 返回逻辑计划，不能直接当作实际远端物理计划。

前端预期计划和远端已准备计划明确标注，不能用前者声称已确认后者命中。若当前接线没有返回真实远端描述，应把这个缺口作为接线工作完成，不能以一条 debug 日志替代验收。

### 8.2 节点指标与现有采集接口

每个 `NativeNode` 的执行状态持有独立的 `NodeMetrics`。复用 DataFusion `ExecutionPlanMetricsSet` / `MetricBuilder`，并通过 Fused 的 `metrics()` 暴露查询局部指标。节点计算直接更新自己持有的指标句柄，不按 ID 或字符串查找指标。两个同为 `rate` 的节点分别统计，便于比较它们的输入规模、窗口成本与内存行为。

展示和指标注册使用最终执行树的结构位置：根为 `root`，一元输入为 `.input`，二元输入为 `.left` / `.right`，例如 `native.root.left.self_compute_time_ns` 和 `native.root.right.output_values`。输入与出口适配使用独立的保留前缀。这些名字在创建运行态、注册指标时生成，热路径只更新句柄。

树位置只是当前 Fused 计划内的观测定位，不要求跨查询、跨计划改写或跨版本稳定，也不需要 ID 分配器或节点注册表。`NativeNode` 不额外保存路径字符串；`DisplayAs` 与运行态构造在遍历同一最终树时临时携带位置，使用同一遍历规则。计划改写完成后才生成指标名字，不从子节点实际产出数据的先后顺序推导位置。

需要区分节点的原因是现有 [MetricCollector](../../src/common/recordbatch/src/adapter.rs) 会先执行 `aggregate_by_name()`。只使用 `rate.self_compute_time_ns` 或仅加 metric label，会把不同节点的同名指标合并。因此保留树位置前缀，但不据此要求执行模型具有 `NodeId`。

`DisplayAs` 输出带相同树位置的 native 树，TEXT/JSON 的现有 metrics 字段保存节点指标。JSON 的 `param` 包含树描述，metrics 字典通过树位置关联；现有 `children` 继续只表示真实物理 child。本轮不要求先扩展一个新的结构化诊断协议，也不让 common-recordbatch 反向依赖 promql。

| 指标 | 口径 |
| --- | --- |
| input/output batches | native 数据块数量，结束片可单独统计 |
| input samples | 实际摄入的原始或已求值时间位置数，字段值数量另计 |
| evaluated grid points | 按 series × 时间计的求值位置数，包含无有效结果的位置 |
| output values / completed series | 实际有效值数量及完成的 series 数 |
| self_compute_time | 本节点同步计算、状态维护及本地输出构造时间 |
| current/peak reserved bytes | 当前保留量与本次执行高水位 |
| groups / state entries | 聚合或匹配状态规模，按适用节点提供 |

`output_rows` 保持 Fused 对外物理输出的含义，不把内部 grid 点、字段值、frame 行与最终结果行混为一个计数。指标单位进入名称或展示说明。

### 8.3 计时与聚合规则

本节点 timer 不包住 child 的 poll 或递归求值。先取得输入，再计本节点的处理；返回 child 前停止计时。计时以批次或工作区间为粒度，不逐样本读取时钟。

Fused 标准 `elapsed_compute` 只计一次整个执行区域自身的工作，包括内部节点和适配器。内部 `native.*.self_compute_time` 作为明细，不再次进入总时间相加。若展示 inclusive 时间，必须明确标注，不能与 self time 求和。

这些 block timer 测量同步工作区间的经过时间，不直接宣称为进程 CPU 时间。并行 partition 的 compute 总量、查询 wall time、存储等待和远端时间分别展示；父子、远端阶段与本地等待不能重复累计。

Fused peak 按执行分区报告，使用该分区同一时刻的区域保留量，不能相加各节点分别发生的 peak。各 partition 的 peak 也不能相加后标作整个 Fused 的同时峰值。峰值指标名称必须保留 partition 身份，例如 `native.p2.root.left.peak_reserved_bytes`；Fused 分区高水位同样区分 partition。查询内存池原有指标继续沿用，节点诊断不再制造一份池额度。

### 8.4 远端 ANALYZE

复用现有链路：datanode 的实际 ExecutionPlan 指标经 `MetricCollector → RecordBatchMetrics → Flight metrics` 返回，`MergeScanExec` 保存 sub-stage metrics，前端 [DistAnalyzeExec](../../src/query/src/analyze.rs) 展示 TEXT/JSON。

新增 native 树描述和带树位置的指标必须经过同一条链路。保留 stage、远端节点、物理节点及 Fused 内部树位置的上下文，不能把不同远端实例的 `root.left` 混为同一个节点。远端实际执行树按它自己的结构位置关联，不以另一次前端编译的位置替代。

默认可按既有方式汇总各 partition 的 count、累计 self time 和有明确时刻口径的 current 值，peak 始终保留 partition 身份。`VERBOSE` 展示分布等细节；其他逐 partition 明细的身份也必须在序列化和按名聚合后保留。指标数随计划与执行分区规模增长，不把 series 标签、primary key 或时间点作为全局 Prometheus label。

## 9. 查询推演与第一期验收

下表是内部树的表达式记法，`Input` 已绑定扫描，`Window` 省略的网格由编译器明确补齐。

| 查询 | 必须能生成的 native 组合 |
| --- | --- |
| `abs(x)` | `Map(Abs, Instant(Input(x)))` |
| `abs(rate(x[5m]))` | `Map(Abs, Window(Rate, Input(x)))` |
| `sum by(job)(rate(x[5m]))` | `Aggregate(Sum, Window(Rate, Input(x)))` |
| `sum by(job)(rate(x[5m])) + 1` | 完整聚合或 Final 的 native 输出继续进入 `Map(AddScalar(1), ...)` |
| `sum_over_time(abs(rate(x[5m]))[1h:1m])` | `Window(SumOverTime, Map(Abs, Window(Rate, Input(x))))` |
| `rate(a[5m]) + on(job, instance) rate(b[5m])` | 两个 Window 子树进入已绑定 matching 的 Binary，按分布决定是否交换 |

子查询内网格由 [subquery_grid](../../src/promql/src/range_eval.rs) 根据对齐、窗口、offset 和 anchor 计算。外层 Window 绑定 `input_grid=inner` 和 `output_grid=outer`，直接消费内层结果；不先把内层结果重采样到外层网格。现有 [双网格测试](../../src/promql/src/range_eval/tests.rs) 已展示完整 Series 路径上的这种能力，分片连接仍需实现。

第一批实现至少覆盖上述组合需要的 Instant、已有 Range kernel、已有 Map、标量广播、Sum 的 Complete/Partial/Final，以及已有 native binary 算术/matching 的接入。其余算子按明确的能力清单接入；某个暂缺 kernel 不得成为退回固定形状模型的理由。

二元或全组算子可以先复用 [binary_vectors](../../src/promql/src/vector_match.rs) 等已有语义实现，并保留受限额管理的必要输入。不能把它接受完整输入集合的接口误写成现成的逐片算法。

验收覆盖以下几组真实风险：

1. **组合**：增加 map、在 map 后接窗口、在完整聚合后接运算、连接两个输入，只改变树和节点绑定，不增加完整形状识别代码。
2. **批次**：同一数据按不同 chunk 划分，覆盖空洞、全空中间块、空末片及 reset 跨边界，结果和完成次数一致；长 series 在末块前已有可观察的窗口输出。
3. **聚合与类型**：两个时间点不能合成一个值；跨分区的 `+1` 只在 Final 后执行；混合 histogram、stale、标签变化继续满足既有语义。
4. **内存**：小限额实际拒绝；执行中取消和错误后 reservation 回落；共享切片不按视图数虚增；已释放或移交的状态不继续计费。
5. **可观测性**：真实 server 的 EXPLAIN 显示绑定树，ANALYZE 的 TEXT/JSON 均有可关联的节点指标；远端计划和指标在前端可见；纯 EXPLAIN 不消费样本。
6. **性能**：新实现重新做端到端与节点区域对照，保留不变控制项和有效性门控。旧 POC 的比例不直接作为本实现收益；同时报告开关 metrics 的开销。

## 10. 实施顺序与复用清单

| 工作 | 复用内容 | 必须新增或适配 |
| --- | --- | --- |
| 语义与编译 | 正在推进的 PromPlan 接线、现有 PromPlanner 语义 | 子图到已绑定 native 树、类型与分布检查、计划期降级 |
| 原生输入 | RawSeriesChunk、来源绑定、typed raw merge、扫描配额 | 与实际物理输入能力对接，保留全局完成契约 |
| 批量执行 | Range kernel、已拆出的 reset/ingest/batch 优化、map | 可组合状态树、Instant/Range 分片接口、builder drain |
| 聚合与 matching | accumulator/state 语义、native binary kernels | 时间维度、Complete/Partial/Final、批次连接及受限状态 |
| 输出 | Arrow、Series/GridRange、已有 raw frame | 已绑定出口适配，不在内部连接反复行列转换 |
| 内存 | TaskContext pool、MemoryConsumer/Reservation、既有扫描框架 | 所有长期保留状态的容量和增长接入、取消释放 |
| 计划与诊断 | DisplayAs、metrics、RecordBatchMetrics、Flight、DistAnalyzeExec | 树描述与位置前缀、节点指标、远端只准备的计划可见性 |

先定编译树与批次接口，并一起定义内存和 metrics 的运行上下文。随后接入各类节点和出口，再完成实际 planner、远端展示与端到端验收。内存、EXPLAIN 和 ANALYZE 不作为功能上线后的补充项。

已独立拆出的 kernel 优化继续按各自范围评审。Fused、依赖其结构的输出优化和接线按本设计调整；固定 POC 不因单测通过或局部性能接近而成为合并依据。

