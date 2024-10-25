(function() {
    var implementors = Object.fromEntries([["common_datasource",[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"common_datasource/compression/struct.CompressionTypeIter.html\" title=\"struct common_datasource::compression::CompressionTypeIter\">CompressionTypeIter</a>"]]],["common_error",[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"common_error/status_code/struct.StatusCodeIter.html\" title=\"struct common_error::status_code::StatusCodeIter\">StatusCodeIter</a>"]]],["common_recordbatch",[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"common_recordbatch/recordbatch/struct.RecordBatchRowIterator.html\" title=\"struct common_recordbatch::recordbatch::RecordBatchRowIterator\">RecordBatchRowIterator</a>&lt;'_&gt;"]]],["datatypes",[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"datatypes/vectors/decimal/struct.Decimal128Iter.html\" title=\"struct datatypes::vectors::decimal::Decimal128Iter\">Decimal128Iter</a>&lt;'_&gt;"],["impl&lt;'a&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"datatypes/vectors/list/struct.ListIter.html\" title=\"struct datatypes::vectors::list::ListIter\">ListIter</a>&lt;'a&gt;"],["impl&lt;T: <a class=\"trait\" href=\"datatypes/types/primitive_type/trait.LogicalPrimitiveType.html\" title=\"trait datatypes::types::primitive_type::LogicalPrimitiveType\">LogicalPrimitiveType</a>&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"datatypes/vectors/primitive/struct.PrimitiveIter.html\" title=\"struct datatypes::vectors::primitive::PrimitiveIter\">PrimitiveIter</a>&lt;'_, T&gt;"]]],["flow",[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"flow/expr/func/struct.BinaryFuncIter.html\" title=\"struct flow::expr::func::BinaryFuncIter\">BinaryFuncIter</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"flow/expr/relation/func/struct.AggregateFuncIter.html\" title=\"struct flow::expr::relation::func::AggregateFuncIter\">AggregateFuncIter</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"flow/expr/struct.VectorDiffIter.html\" title=\"struct flow::expr::VectorDiffIter\">VectorDiffIter</a>"]]],["log_store",[["impl&lt;'a, I: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a>&lt;Item = &amp;'a EntryId&gt;&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"log_store/kafka/util/range/struct.ConvertIndexToRange.html\" title=\"struct log_store::kafka::util::range::ConvertIndexToRange\">ConvertIndexToRange</a>&lt;'a, I&gt;"]]],["mito2",[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"mito2/memtable/partition_tree/tree/struct.TreeIter.html\" title=\"struct mito2::memtable::partition_tree::tree::TreeIter\">TreeIter</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"mito2/memtable/time_series/struct.Iter.html\" title=\"struct mito2::memtable::time_series::Iter\">Iter</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"mito2/sst/parquet/page_reader/struct.RowGroupCachedReader.html\" title=\"struct mito2::sst::parquet::page_reader::RowGroupCachedReader\">RowGroupCachedReader</a>"],["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"mito2/sst/parquet/row_group/struct.ColumnChunkIterator.html\" title=\"struct mito2::sst::parquet::row_group::ColumnChunkIterator\">ColumnChunkIterator</a>"],["impl&lt;I: <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a>&lt;Item = <a class=\"type\" href=\"mito2/error/type.Result.html\" title=\"type mito2::error::Result\">Result</a>&lt;<a class=\"struct\" href=\"mito2/read/struct.Batch.html\" title=\"struct mito2::read::Batch\">Batch</a>&gt;&gt;&gt; <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"mito2/read/dedup/struct.LastNonNullIter.html\" title=\"struct mito2::read::dedup::LastNonNullIter\">LastNonNullIter</a>&lt;I&gt;"]]],["servers",[["impl <a class=\"trait\" href=\"https://doc.rust-lang.org/nightly/core/iter/traits/iterator/trait.Iterator.html\" title=\"trait core::iter::traits::iterator::Iterator\">Iterator</a> for <a class=\"struct\" href=\"servers/postgres/types/error/struct.PgErrorCodeIter.html\" title=\"struct servers::postgres::types::error::PgErrorCodeIter\">PgErrorCodeIter</a>"]]]]);
    if (window.register_implementors) {
        window.register_implementors(implementors);
    } else {
        window.pending_implementors = implementors;
    }
})()
//{"start":57,"fragment_lengths":[396,367,419,1286,1015,618,2195,375]}