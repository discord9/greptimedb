## PR4 集成测试实现备注

### 集成测试期间的意外发现

- **SeqScan dyn_filters 只是剪枝提示**：`SeqScan` 行里的 `dyn_filters` 仅作为 `PruningPredicate` 统计剪枝提示传给 SST/memtable，不会在 scan 内部逐行执行 Bloom probe。因此不能通过 `dyn_filters` 里是否包含 `bloom_probe` 来判断 Bloom 是否真正过滤了行。
- **Bloom exact filter 必须留在 FilterExec**：`non_pushdown(...)` 包装确保 `DynamicFilterPhysicalExpr` 不会被 Mito scan 的 downcast 逻辑下推到 scan predicate，迫使 exact Bloom 在 `FilterExec` 行级求值。EXPLAIN VERBOSE 指标中的 `output_rows` 差值才是过滤效果的正确证明。
- **小 build-side 会被 InListExpr 短路**：DataFusion 的 `HashJoinExec` 在 build-side distinct key 数低于 `hash_join_inlist_pushdown_max_distinct_values`（默认 256）时，不会生成 `HashTableLookupExpr`，而是直接用 `InListExpr (IN (SET))`，导致 Bloom 编码全部跳过。因此 Bloom E2E 测试的 build-side 必须超过默认阈值；当前 fixture 用 302 个 key。
- **稀疏 key 避免 ArrayMap**：若 probe 和 build 的 join key 都是稠密整数（如 0..8192），DataFusion 可能用 `ArrayMap` 替代 `HashMap` join，同样不经过 `HashTableLookupExpr`。用稀疏的 `i * 1024` 风格 key 可以稳定走 `HashMap` 路径。
- **invalid update 不修改 runtime filter 状态**：oversized payload、解码失败、fingerprint 不匹配等场景，都不能修改现有的 pushdown/exact wrapper，也不能推进 generation 或标记 complete。否则会污染后续合法更新或提前关闭 filter。
- **指纹检测同版本跨平台**：`hash_compat_fingerprint` 只能在同 DataFusion commit 的不同平台间检测 hash 语义漂移；它不是跨版本稳定协议，无法在不同 DataFusion 版本间或不同 hash 算法实现间提供保证。
