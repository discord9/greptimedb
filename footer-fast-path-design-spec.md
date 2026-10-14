# Footer 快路径扩展设计方案（修订版）

## 目标

使 footer 快路径能安全处理 visibility/dedup，支持通用场景（多文件、多 series、有删除的表）。

## 核心设计决策

### 1. SeriesRun 扩展

```rust
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeriesRun {
    // 现有字段
    pub pk: String,                    // base64 编码的 primary key
    pub start: u64,                    // 行偏移
    pub count: u64,                    // 行数
    pub first_timestamp_ms: i64,       // 首行时间戳
    pub timestamp_step: Option<i128>,  // 时间戳步长
    pub value_column: Option<String>,  // 值列名
    pub value_first_bits: Option<u64>, // 值仿射参数
    pub value_step_bits: Option<u64>,

    // 新增字段
    pub all_put: bool,                 // 该 run 所有行都是 Put
}
```

**为什么需要 all_put**：
- mito2 是 append-only，但 compaction 会保留 Delete 行（`filter_deleted=false`）
- 写入时必须检查 op_type 列，全是 Put 才标记 `all_put: true`
- **all_put 是充分不必要条件**：普通路径能容忍 Delete 行（`maybe_filter_deleted`），footer 路径不能

**为什么不需要 sequence**：
- 要证明的性质是"每个 (series, ts) 在整个扫描集合里唯一"
- sequence min/max 无法证明唯一性（交错序列仍无解）
- 用"AllPut + 唯一性证明"代替 dedup

### 2. 写入侧改动

**位置**：`src/mito2/src/sst/parquet/writer.rs` 的 `append_series_runs` 函数

**改动**：
```rust
// 在现有 run 边界循环里计算 all_put
let op_type_idx = op_type_column_index(batch.num_columns()); // n - 1
let op_types = batch.column(op_type_idx)
    .as_any()
    .downcast_ref::<UInt8Array>()
    .expect("flat batch must end with UInt8 __op_type");

// 对每个 run：
let all_put = op_types.values()[run_start..run_end]
    .iter()
    .all(|&op| op == OpType::Put as u8);
```

**额外修复**：
- 非 ms 时间戳时**不写 KV**（当前会写空数组，导致数据丢失）
- `timestamp_step` 只在 `count == 1 || ts_step > 0` 时给 `Some`（防止 run 内重复时间戳）
- 序列化后 KV 大小上限（如 >64KB 就不写）

**不做的**：
- 不合并相邻同 key 的 run（浮点等价性无法保证）
- 不限制"只对小/已 compact 文件写"（无依据的限制）

### 3. 读取侧改动

**位置**：`src/mito2/src/read/research_parquet_series.rs` 的 `try_prepare_from_footer`

**验证顺序**（任一失败都 fallback）：

1. **入口守卫**：
   - `num_memtables() == 0`
   - `append_mode == false`
   - `compaction == false`
   - `sequence_range / snapshot_sequence` 为 None
   - `series_row_selector.is_none()`
   - sparse metadata
   - 单 partition

2. **文件级校验**：
   - `num_rows > 0` 时 KV 必须存在且 `runs` 非空
   - 覆盖性：按 `start` 排序后 `start_0==0`、`start_i==start_{i-1}+count_{i-1}`、`Σcount == num_rows()`
   - 每个 run `count >= 1`

3. **Assigned 过滤**：
   - 需要 `SeriesReader` 暴露 `series_filter()` 方法
   - **`decode_ids` 只用于 assigned membership**（返回 `(u32, u64)` reserved prefix）
   - assigned run 必须**完整解码 PK**（用 `decode_sparse`），解析失败 → fallback
   - **分组规则**：明确规范顺序、null/缺失标签规则及未知列处理，保留所有必要标签
   - 分组必须与普通路径的 series 身份语义一致；无法证明等价的编码情况直接 fallback
   - 不在 assigned 的 run 丢弃（唯一允许的丢弃，仅限 visibility/唯一性验证前）

4. **Visibility 校验**（只对 assigned 的 key）：
   - 每个 assigned run `all_put == true`
   - 如果不是 → fallback

5. **时间范围不重叠检查**（只对 assigned 的 key）：
   - 对每个 key，把其全部 run 按首时间排序
   - **run 内检查**：`count == 1` 允许 `step == 0`；`count > 1` 必须 `step > 0`（时间戳严格递增）
   - **run 间检查**：相邻 `prev_last < next_first`（时间范围严格不重叠）
   - checked 计算末时间戳，确认可表示为 `i64`
   - 有重叠或重复 → fallback

6. **时间范围裁剪**：
   - 用 `ScanInput.time_range` 裁剪每个 run
   - **半开区间**：`[start, end)`，边界可为空
   - 保留索引定义为 `[lo, hi)`，正 step 时上下界均以 ceil 计算，再 clamp 到 `[0,count]`
   - 单行 run、无界区间、空区间、负差值需特殊处理
   - 裁剪后若改为 `(first + step * lo) + step * j`，不能假设与 `first + step * (lo+j)` 的浮点结果相同
   - **裁剪时保留原始数值仿射参数和原始索引**，按原始公式生成保留样本

7. **谓词守卫**：
   - 只接受能被单个范围**精确**表达的时间谓词
   - 不能把宽化后的范围当成完整谓词（`GenericRange::or` 会将不连续范围扩为包络）
   - Field 语义谓词、dyn filter、region-partition 谓词不一致 → fallback

8. **完整重建校验**：
   - 单一受支持 Float64 field
   - `value_column` 匹配投影字段
   - 毫秒时间类型
   - 必要仿射参数齐全
   - footer 描述完整覆盖 SST
   - writer 保证描述逐位对应实际样本

9. **输出契约**：
   - 同一完整 PK 的所有块归入一个 `SeriesData`
   - 按时间排序输出
   - 满足 `VisibleSeriesBlock` 契约："checked timestamp bounds, row predicates and cross-source visibility"

10. **内存计算**：
    - 先 `try_grow` 再分配
    - 计入 keys/结构/block 容量/f64 载荷/临时 map 与排序
    - fallback 时 drop 独立 consumer
    - 内存预算必须覆盖各阶段，而非到最后才检查

### 4. 正确性条件

若满足以下条件，footer 重建的行集与普通路径的可见行集完全相同：
- 扫描集合正是某 pin 住版本、无 memtable 的文件集合
- assigned series 的所有相关来源均已检查且全为 Put
- 同一 (series, ts) 在整个集合中只出现一次（唯一性证明，包括 run 内和 run 间）
- 无 sequence/snapshot 过滤（调用方守卫）
- tag 谓词由 assigned 集合精确执行
- 时间谓词可被裁剪精确表示、无 field/dyn/partition 谓词
- footer 描述完整覆盖 SST 且逐位精确

### 5. 序列化格式

**选择：Postcard**

- 每个 run 约 30-50 字节（vs JSON 150-200 字节）
- 无 .proto 文件，无构建步骤
- serde derive 直接支持
- **无兼容性需求**（POC 分支，直接替换 JSON）

**注意**：Parquet metadata value 是字符串，Postcard 字节需要 Base64 编码（增加约 33% 大小）。

### 6. 测试要求

**正例测试**（明确断言 footer 命中）：
- 多文件 × 多 series，全部满足条件
- 只有未 assigned series 有删除（仍 fast path）
- 时间范围裁剪（样本恰等于 end、负时间戳、单行、无界）
- 带非零裁剪偏移的逐位对照测试
- 全部 run 未 assigned 的合法空结果
- 全部样本被时间裁剪掉的合法空结果

**反例测试**（明确断言 footer 拒绝且普通路径接管）：
- 有删除的 assigned series（fallback）
- run 内重复时间戳（`count>1, step=0`，fallback）
- 同 run、同文件跨 run、跨文件重复时间戳（fallback）
- 非仿射 run（fallback）
- 空/截断 run 列表（fallback）
- 错误 value_column（fallback）
- 非 ms 时间单位（fallback）
- 不连续时间 OR 谓词（fallback）
- field predicate（fallback）
- dyn filter（fallback）
- region-partition predicate（fallback）
- memtable 存在（fallback）
- append mode（fallback）
- compaction（fallback）
- sequence/snapshot 过滤（fallback）
- series_row_selector 非空（fallback）
- 内存池耗尽（fallback）
- fallback 后 reservation 释放

**对照测试**：
- 与普通路径的字节级一致性
- 相同 reserved prefix、不同完整标签的 series 区分
- 完整 PK 往返保真

**兼容性测试**：
- 旧 JSON footer 的 SST 经普通路径读取（不加速，但可读）
- 缺少 `all_put` 的旧 footer fallback
- 未知格式 fallback

## 实现检查清单

- [ ] `SeriesRun` 加 `all_put: bool` 字段
- [ ] 写入侧：计算 `all_put`，设置字段
- [ ] 写入侧：非 ms 时间戳时不写 KV
- [ ] 写入侧：`timestamp_step` 只在 `count == 1 || ts_step > 0` 时给 `Some`
- [ ] 写入侧：KV 大小上限
- [ ] 读取侧：入口守卫
- [ ] 读取侧：文件级校验（覆盖性）
- [ ] 读取侧：assigned 过滤（规范化编码 key 分组）
- [ ] 读取侧：visibility 校验（all_put，只对 assigned）
- [ ] 读取侧：唯一性证明（run 内 + run 间）
- [ ] 读取侧：时间范围裁剪（半开区间，保留原始参数）
- [ ] 读取侧：谓词守卫
- [ ] 读取侧：完整重建校验
- [ ] 读取侧：输出契约（SeriesData 分组排序）
- [ ] 读取侧：内存计算
- [ ] `SeriesReader` 暴露 `series_filter()` 方法
- [ ] Postcard 序列化替换 JSON
- [ ] 删除 `eprintln!` 调试代码
- [ ] 测试用例
