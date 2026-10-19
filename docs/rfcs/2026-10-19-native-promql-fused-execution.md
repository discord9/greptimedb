---
Feature Name: Native PromQL 的融合执行模型
Tracking Issue: N/A
Date: 2026-10-19
Author: "@discord9"
---

# RFC：Native PromQL 的融合执行模型

# 摘要

PromQL 的区间函数（`rate`、`increase`、`sum_over_time` 等）目前由一条物理节点链求值，每一步之间都要物化中间行批。native 路径则直接在存储层产出的带类型序列（typed series）上求值。

本 RFC 提出该路径的执行模型，涵盖：一次融合能覆盖什么、批量数据如何到达、状态存在哪里、以及输出是什么。本 RFC **刻意不提出最终的 Rust 结构体布局**——先把执行模型谈清楚，再定类型才有意义。

整个设计由一个性质驱动：**函数选择与输入绑定属于物理编译期；热路径只负责把批次喂给已绑定的 kernel。**

# 动机

## native 路径要替换掉什么

对于 `rate(x[5m])`，常规计划要为每个网格点、每条序列，从行批里拼出一个样本窗口。从扫描到结果之间的每一层都是 `RecordBatch`，各自带有分配、投影与谓词。

raw 序列路径可以跳过这一切：存储直接产出带类型的序列块（`RawSeriesChunk`，`src/datatypes/src/series/raw.rs:96`），而单条序列上的区间函数本质上就是两列上的窗口扫描。已求值序列（`Series`，`src/datatypes/src/series/samples.rs:613`）用 `TimeAxis::Grid`（`src/datatypes/src/series/time.rs:172`）替代逐行时间戳，于是网格成为 schema 的属性，而不是一列数据。

## 当前原型错在哪里

早期原型通过**在物理计划已经存在之后去识别一个区间函数子树**，再把融合节点换进定位到的位置。这带来三个问题：

1. **机会是从物理树形状里重新发现的。** 优化器只要改变该子树的排列方式，融合路径就会静默不可达。
2. **融合节点是一个固定模板。** 它把函数、可选 map 链、可选局部聚合作为字段携带，于是每增加一种形状都是在加宽模板，而不是组合出一个计算。
3. **输出列在运行期分类**，靠"下一个未被认领的 `Float64` 列"认领下一个 field——只有在恰好一个 field 时才没有歧义。

本 RFC 保留原型的 **kernel**，丢弃它的 **结构**。

# 概览

一个查询被编译成一个或多个 **FusedUnit**。一个单元是一条线性链：

```
raw scan
  → 至多一个窗口函数      rate、increase、sum_over_time ...
  → 任意多个逐点函数      abs、ceil、floor、sqrt、round
  → 至多一个局部聚合      sum by (...)
```

单元是"最划得来"的最小片段：单元内部全部在带类型序列上求值，不物化行批；单元外部全是常规物理节点。

单元的输入是带类型的序列，不是行。输出是带类型的序列；若单元以局部聚合结尾，则输出一个列布局在编译期就已固定的批次。

# 详细设计

## 1. 融合范围与边界

### 1.1 一个单元可以包含什么

一个 `FusedUnit` 覆盖**单输入**上的线性链，且必须同时满足：

- **单输入。** 窗口函数与逐点函数链都是单输入运算。
- **无分区交换。** 单元内部不得出现 shuffle。
- **无跨序列匹配。** 向量间二元运算、`on()`/`ignoring()` 匹配、集合运算一律在单元之外。
- **一个求值网格。** 所有阶段共享同一个 `GridSpec`（start、step、len）。

不满足这些条件的形状不融合，由常规物理节点执行。

### 1.2 必须在何处切分

| 切分原因 | 示例 | 由谁处理 |
| --- | --- | --- |
| 分区交换 | `sum by (job) (...)`，当 `job` 不是分区键 | 既有 `AggregateExec`，两阶段 |
| 网格改变 | 子查询 `[1h:1m]` 的内层网格 | 内层单元 + 一次显式的网格适配 |
| 跨序列匹配 | `a + on(x) b` | 既有 join / vector-match 节点 |
| 标量广播 | `... + 1` | 既有 `ProjectionExec` |
| 非逐点函数 | `label_replace`、`absent` | 既有专用节点 |

### 1.3 多输入、分支、共享子表达式

第一期**刻意不支持**。

多输入单元需要一条规则来决定"哪个输入的到达推进窗口"，还需要一条规则来合并完成信号。`src/promql/src/source/merge.rs` 已经能合并有序输入，但它服务的是行批管线；复用它会把本设计要消除的耦合重新引进来。

被两处引用的子表达式在第一期按重复计算处理，不做公共子表达式消除。

## 2. 批量数据接口

### 2.1 输入

两种输入，均复用既有类型：

- **Raw**：`RawSeriesChunk`（`src/datatypes/src/series/raw.rs:96`）构成的数据流，携带
  `primary_key: Buffer`、`metric: Option<MetricName>`、`time: RawTimestamps`、
  `fields: SeriesFields` 以及 `is_last: bool`。
- **Evaluated**：`Series`（`src/datatypes/src/series/samples.rs:613`）构成的数据流，例如子查询的内层结果，整条序列一次到达。

**`is_last` 就是序列完成信号**（`src/datatypes/src/series/raw.rs:109`），无需新造信号类型。注意其已注明的限制：合并步骤必须跨其所有输入重新计算收尾——这一点只有在存在多输入单元之后才会成为问题。

**有效性**不单独传递。字段是 Arrow 数组，null 即无效样本。在已求值输入路径上，stale marker 作为值保留，`push_series` 已是此语义。

**网格位置**由 `SeriesSchema::eval_grid` 加 `GridRange` 表达，不逐行传列。

### 2.2 窗口求值不等整条序列完成

这是既有求值器的一个性质，本设计必须保留。

窗口 `(T - range_ms, T]` 只需要晚于 `T - range_ms` 的样本。因此在一条序列的非末块上，`RangeEvaluator::push`（`src/promql/src/range_eval.rs:227`）只把网格推进到 `last_ts + offset_ms` 就返回 `None`（`src/promql/src/range_eval.rs:236-242`）；只有 `is_last` 块才收尾该序列。

因此一个单元对**每条序列会产出多个批次**，消费方必须能接受"同一条序列的多批"。`SeriesOrdering::Contiguous`（`src/datatypes/src/series.rs:40`）已经保证每条序列连续，这恰好是这种消费方需要的保证。

### 2.3 批量粒度

热路径以整批为单位工作，绝不逐样本、也绝不逐网格点：

- 输入以 Arrow 数组形式位于 `SeriesFields` 内，不逐样本重建。
- 窗口函数一次对一段网格位置求值。
- 逐点函数链作用于整列。

## 3. 状态归属

### 3.1 编译产物与运行态严格分离

```rust
// 示意，非最终布局。
struct FusedPlan {                 // 不可变，经 Arc 跨分区共享
    kernel: KernelId,              // 已选定的函数，而不是捕获的闭包
    window: RangeWindow,
    maps: Arc<[MathOp]>,
    grid: Arc<EvalGrid>,
    input_schema: Arc<SeriesSchema>,
    output_schema: Arc<SeriesSchema>,
    time_column: String,
    value_columns: Arc<[String]>,
    label_columns: Arc<[String]>,
    partial_agg: Option<PartialAggSpec>,
}

struct FusedState {                // 每次 execute(partition) 新建
    evaluator: RangeEvaluator,
    agg: Option<PartialAggState>,
}
```

- `FusedPlan` 以 `Arc` 引用，不按分区复制。
- `FusedState` 在 `execute(partition)` 内创建，流结束时丢弃。
- 按序列的状态（`src/promql/src/range_eval.rs:92` 的 `SeriesState`）在 `is_last` 时清空，不跨序列泄漏。

`RangeEvaluator` 已经是这个形状：`try_new` 固定计划，`push` / `push_series` 推进，`finish` 校验收尾。它就是一个单元的 kernel，不需要重写。

### 3.2 局部聚合状态

`PartialAggState` 在一个分区的序列范围内，按 group key 持有累加器：

- **输入**：已求值的 `Series`。
- **状态**：group key 到累加器（`f64` 加有效性位）的映射。
- **释放**：该分区数据流结束时一次性产出。
- **不跨分区共享。** 跨分区的合并属于既有两阶段聚合；单元只产出局部状态。

## 4. 输出类型与边界

| 单元边界 | 输出 | 类型 |
| --- | --- | --- |
| 窗口函数 + 逐点函数链 | 原生已求值序列 | `Series`，`TimeAxis::Grid` |
| 局部聚合 | 局部聚合状态 | 列布局固定的批次 |
| 外围物理节点 | 常规结果行 | Arrow `RecordBatch` |

**列绑定由物理编译产生，不在运行期推断。** `FusedPlan::time_column`、`value_columns`、`label_columns` 在单元编译时即固定；运行期不做任何 schema 检查来决定布局。这正是消除原型中 `classify_output` 启发式的地方。

# 查询推演

## `abs(x)`

没有窗口函数，只有逐点函数链。第一期**不融合**：融合的收益来自窗口求值，而 `abs(x)` 没有窗口。它由既有的 selector 加 projection 执行。

仍然值得一提，因为它确定了一条边界：仅凭逐点函数不构成融合的理由。

## `abs(rate(x[5m]))`

```
FusedUnit {
  input:    Raw(scan x)
  kernel:   Rate，window = { range: 5m, offset: 0 }
  maps:     [Abs]
  grid:     语句网格 (start, step, len)
  output:   Series，TimeAxis::Grid
}
```

一个单元，单输入，无聚合。状态：求值器的窗口缓冲与 cursor，加上输出构建器。外围 projection 把产出的 `Series` 转成结果行。

## `sum by (job) (rate(x[5m]))`

```
FusedUnit {
  input:       Raw(scan x)
  kernel:      Rate，window = 5m
  maps:        []
  partial_agg: Sum { group_by: [job] }
  output:      局部状态 (job, 累加器)
}
  → 既有 AggregateExec（final）
```

`job` 是否为分区键决定了要不要 shuffle，但两种情况下单元都只产出局部状态，由既有聚合收尾。

**第一期只实现"局部状态 + 既有 final 聚合"**，把交换完全交给 DataFusion。这样设计面最小。

## `sum by (job) (rate(x[5m])) + 1`

```
FusedUnit { kernel: Rate，partial_agg: Sum { group_by: [job] } }
  → 既有 AggregateExec（final: job, sum）
  → 既有 ProjectionExec（sum + 1）
```

**`+ 1` 作用于最终结果行，是标量广播，而不是作用于局部状态。** 聚合的输出就是该 projection 的输入。

设计上的结论正是"在此切开"的意义：**单元完全不需要知道 `+ 1` 的存在。**

## `sum_over_time(rate(x[5m])[1h:1m])`

两个网格，必须分别声明：

```
内层 FusedUnit {
  input:  Raw(scan x)
  kernel: Rate，window = 5m
  grid:   内层网格 = [start - 1h, end]，step = 1m
  output: 内层网格上的 Series
}
  → 网格适配
外层 FusedUnit {
  input:  Evaluated(内层 Series)
  kernel: SumOverTime，window = 1h
  grid:   外层网格 = [start, end]，step = 语句 step
  output: 外层网格上的 Series
}
```

内层网格的计算由 `subquery_grid`（`src/promql/src/range_eval.rs:573`）给出。

内层结果通过 `push_series`（`src/promql/src/range_eval.rs:249`）进入外层单元，而它要求被推入序列的网格等于输入网格、字段长度匹配。**因此网格适配必须显式进行**，不存在原型那种运行期猜测的位置。

第一期不实现这条：它同时需要单元串联与网格适配。

## 第一期实现范围

| 形状 | 第一期 | 理由 |
| --- | --- | --- |
| `rate(x[5m])` + map 链 | 是 | 单个单元；既有求值器即可承载 |
| `sum by (k) (rate(x[5m]))` | 是 | 增加 `PartialAggSpec`；交换留在外部 |
| `sum by (k) (...) + 1` | 是，自然成立 | `+ 1` 在单元之外 |
| `abs(x)` | 否 | 无窗口，收益不成立 |
| 嵌套子查询 | 否 | 需单元串联与网格适配 |
| 多输入 / 跨序列 | 否 | 需合并完成信号 |

# 备选方案

## 保留原型的后置子树选择

否决。机会仍将从物理树形状推导，节点仍是加宽的固定模板，输出列仍在运行期分类。

## 只在逻辑层融合，由一个物理节点承担全部

部分采纳：逻辑节点可以表达求值，但"一个**已准备**的扫描能否承载它"取决于只在物理规划之后才存在的 scanner 状态。因此无论节点长什么样，拒绝机制都必须位于物理规划之后。

## 让融合节点带一个通用表达式解释器

暂时否决。通用解释器会用运行期分派取代编译，而这与既定目标正好相反。kernel 保持在编译期选定。

# 开放问题

- `FusedSeriesExec` 应持有 `Arc<FusedPlan>`，还是让编译把计划展开为节点自身的字段？持有计划便于测试与 `EXPLAIN`；展开则少一层间接。
- 局部聚合是否应进入第一期？当 group key 就是分区键时收益明显；否则只是把计算前移，收益需要实测。
- 一旦输入需要合并，`RawSeriesChunk::is_last` 必须被重新计算（`src/datatypes/src/series/raw.rs:107-108`）。单输入单元不受影响，但在引入多输入单元之前，模型需要为这次重算指定一个确定的位置。
