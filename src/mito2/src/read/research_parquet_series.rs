// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Query-local bridge from ordinary visible flat Parquet reads to series programs.

use std::sync::Arc;

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datatypes::arrow::array::{
    Array, BinaryArray, DictionaryArray, Float64Array, TimestampMillisecondArray,
};
use datatypes::arrow::datatypes::UInt32Type;
use futures::TryStreamExt;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec, SparsePrimaryKeyCodec};
use store_api::region_engine::{
    PreparedSeriesSource, SeriesBlockProcessor, VisibleSeriesBlock, VisibleSeriesValues,
};

use base64::Engine;

use crate::error::{Result, UnexpectedSnafu};
use crate::read::scan_region::StreamContext;
use crate::read::series_reader::SeriesReader;
use crate::sst::parquet::flat_format::{primary_key_column_index, time_index_column_index};
use crate::sst::parquet::metadata::{MetadataLoader, decode_series_runs};
use crate::sst::parquet::reader::MetadataCacheMetrics;

struct SeriesData {
    key: Vec<u8>,
    blocks: Vec<VisibleSeriesBlock>,
}

struct ParquetSeriesSource {
    context: Arc<StreamContext>,
    codec: SparsePrimaryKeyCodec,
    series: Vec<SeriesData>,
    _memory: MemoryReservation,
}

fn invalid(reason: impl Into<String>) -> crate::error::Error {
    UnexpectedSnafu {
        reason: reason.into(),
    }
    .build()
}

/// Builds series blocks from footer series-run descriptors when every run is
/// exactly affine in both timestamp and value. Returns `None` if any file
/// lacks footer metadata or contains a non-affine run, so the caller falls
/// back to ordinary column reads.
async fn try_prepare_from_footer(
    context: &Arc<StreamContext>,
    memory: &mut MemoryReservation,
) -> Result<Option<Vec<SeriesData>>> {
    let access_layer = context.input.access_layer();
    let table_dir = access_layer.table_dir();
    let path_type = access_layer.path_type();
    let object_store = access_layer.object_store().clone();
    let mut series: Vec<SeriesData> = Vec::new();
    for handle in &context.input.files {
        let file_path = handle.file_path(table_dir, path_type);
        let mut cache_metrics = MetadataCacheMetrics::default();
        let loader = MetadataLoader::new(object_store.clone(), &file_path, handle.size());
        let parquet_meta = loader.load(&mut cache_metrics).await?;
        let Some(runs) = decode_series_runs(&parquet_meta) else {
            return Ok(None);
        };
        if runs.is_empty() {
            continue;
        }
        // Every run must be affine in both timestamp and value.
        if runs.iter().any(|run| {
            run.timestamp_step.is_none()
                || run.value_first_bits.is_none()
                || run.value_step_bits.is_none()
        }) {
            return Ok(None);
        }
        for run in &runs {
            let key = base64::engine::general_purpose::STANDARD
                .decode(&run.pk)
                .map_err(|e| invalid(e.to_string()))?;
            let first_value = f64::from_bits(run.value_first_bits.unwrap());
            let step_value = f64::from_bits(run.value_step_bits.unwrap());
            let values: Vec<f64> = (0..run.count)
                .map(|i| first_value + step_value * i as f64)
                .collect();
            let values_array = Float64Array::from(values);
            let block = VisibleSeriesBlock {
                first_timestamp: run.first_timestamp_ms,
                timestamp_step: run.timestamp_step.unwrap(),
                values: VisibleSeriesValues::Dense(values_array),
            };
            let slot = if series.last().is_some_and(|item| item.key == key) {
                series.len() - 1
            } else {
                memory
                    .try_grow(key.len() + std::mem::size_of::<SeriesData>())
                    .map_err(|e| invalid(e.to_string()))?;
                series.push(SeriesData {
                    key,
                    blocks: Vec::new(),
                });
                series.len() - 1
            };
            memory
                .try_grow(std::mem::size_of::<VisibleSeriesBlock>())
                .map_err(|e| invalid(e.to_string()))?;
            series[slot].blocks.push(block);
        }
    }
    if series.is_empty() {
        return Ok(None);
    }
    common_telemetry::debug!(
        series = series.len(),
        files = context.input.files.len(),
        "Research footer series source prepared"
    );
    Ok(Some(series))
}

fn append_timestamp_runs(
    blocks: &mut Vec<VisibleSeriesBlock>,
    values: &Float64Array,
    times: &TimestampMillisecondArray,
    start: usize,
    end: usize,
    memory: &mut MemoryReservation,
) -> Result<usize> {
    let mut rows = 0;
    let mut start = start;
    while start < end {
        let step = if start + 1 < end {
            i128::from(times.value(start + 1)) - i128::from(times.value(start))
        } else {
            1
        };
        let mut run_end = (start + 2).min(end);
        while run_end < end
            && i128::from(times.value(run_end)) - i128::from(times.value(run_end - 1)) == step
        {
            run_end += 1;
        }
        let count = run_end - start;
        if blocks.len() == blocks.capacity() {
            memory
                .try_grow(std::mem::size_of::<VisibleSeriesBlock>())
                .map_err(|error| invalid(error.to_string()))?;
            blocks
                .try_reserve_exact(1)
                .map_err(|error| invalid(error.to_string()))?;
        }
        blocks.push(VisibleSeriesBlock {
            first_timestamp: times.value(start),
            timestamp_step: step,
            values: VisibleSeriesValues::Dense(values.slice(start, count)),
        });
        rows += count;
        start = run_end;
    }
    Ok(rows)
}

/// Prepare a retained, query-local source from the ordinary deduplicated flat reader.
pub(crate) async fn prepare(
    reader: SeriesReader,
    context: Arc<StreamContext>,
) -> Result<Option<Arc<dyn PreparedSeriesSource>>> {
    let mapper = &context.input.mapper;
    let field = mapper.field_column_start();
    let fields = mapper
        .batch_schema()
        .iter()
        .filter(|(id, _)| {
            mapper
                .metadata()
                .column_by_id(*id)
                .is_some_and(|column| column.semantic_type == api::v1::SemanticType::Field)
        })
        .collect::<Vec<_>>();
    if fields.len() != 1
        || fields[0].1 != datatypes::prelude::ConcreteDataType::float64_datatype()
        || field >= mapper.batch_schema().len()
    {
        return Ok(None);
    }
    let pool = context.input.scan_memory_pool.clone();
    let mut memory = MemoryConsumer::new("research-parquet-series").register(&pool);
    // Fast path: reconstruct series from footer run descriptors without
    // decoding data columns, when every run is exactly affine.
    if let Some(series) = try_prepare_from_footer(&context, &mut memory).await? {
        return Ok(Some(Arc::new(ParquetSeriesSource {
            codec: SparsePrimaryKeyCodec::new(context.input.region_metadata()),
            context,
            series,
            _memory: memory,
        })));
    }
    let mut stream = reader.build_stream().await?;
    let mut series: Vec<SeriesData> = Vec::new();
    let mut rows = 0usize;
    while let Some(batch) = stream.try_next().await? {
        let value_index = mapper.field_column_start();
        let time_index = time_index_column_index(batch.num_columns());
        let key_index = primary_key_column_index(batch.num_columns());
        let Some(values) = batch
            .column(value_index)
            .as_any()
            .downcast_ref::<Float64Array>()
        else {
            return Ok(None);
        };
        if values.null_count() != 0 {
            return Ok(None);
        }
        let Some(times) = batch
            .column(time_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
        else {
            return Ok(None);
        };
        if times.null_count() != 0 {
            return Ok(None);
        }
        let keys = batch.column(key_index);
        if !keys.as_any().is::<DictionaryArray<UInt32Type>>() && !keys.as_any().is::<BinaryArray>()
        {
            return Ok(None);
        }
        let key_at = |row: usize| -> Option<&[u8]> {
            if let Some(dict) = keys.as_any().downcast_ref::<DictionaryArray<UInt32Type>>() {
                let binary = dict.values().as_any().downcast_ref::<BinaryArray>()?;
                if dict.is_null(row) {
                    return None;
                }
                let index = dict.keys().value(row) as usize;
                (!binary.is_null(index)).then(|| binary.value(index))
            } else {
                let binary = keys.as_any().downcast_ref::<BinaryArray>()?;
                (!binary.is_null(row)).then(|| binary.value(row))
            }
        };
        memory
            .try_grow(values.get_buffer_memory_size())
            .map_err(|e| invalid(e.to_string()))?;
        let mut row = 0;
        while row < batch.num_rows() {
            let key = key_at(row).ok_or_else(|| invalid("Null flat primary key"))?;
            let mut end = row + 1;
            while end < batch.num_rows() && key_at(end) == Some(key) {
                end += 1;
            }
            for index in row + 1..end {
                if times.value(index - 1) >= times.value(index) {
                    return Err(invalid(
                        "Visible Parquet rows are not strictly time ordered",
                    ));
                }
            }
            let slot = if series.last().is_some_and(|item| item.key.as_slice() == key) {
                series.len() - 1
            } else {
                if series.last().is_some_and(|item| item.key.as_slice() > key) {
                    return Err(invalid("Visible Parquet primary keys are not ordered"));
                }
                let new_capacity = series.len() + 1;
                let growth = new_capacity
                    .saturating_sub(series.capacity())
                    .saturating_mul(std::mem::size_of::<SeriesData>());
                memory
                    .try_grow(key.len().saturating_add(growth))
                    .map_err(|e| invalid(e.to_string()))?;
                if new_capacity > series.capacity() {
                    series
                        .try_reserve_exact(1)
                        .map_err(|e| invalid(e.to_string()))?;
                }
                series.push(SeriesData {
                    key: key.to_vec(),
                    blocks: Vec::new(),
                });
                series.len() - 1
            };
            if let Some(previous) = series[slot].blocks.last() {
                if previous.first_timestamp as i128
                    + previous.timestamp_step * (previous.values.len() - 1) as i128
                    >= i128::from(times.value(row))
                {
                    return Err(invalid("Visible Parquet series order regressed"));
                }
            }
            rows += append_timestamp_runs(
                &mut series[slot].blocks,
                values,
                times,
                row,
                end,
                &mut memory,
            )?;
            row = end;
        }
    }
    common_telemetry::debug!(
        series = series.len(),
        rows,
        "Research Parquet series source prepared"
    );
    Ok(Some(Arc::new(ParquetSeriesSource {
        codec: SparsePrimaryKeyCodec::new(context.input.region_metadata()),
        context,
        series,
        _memory: memory,
    })))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, UnboundedMemoryPool};
    use datatypes::arrow::array::{Float64Array, TimestampMillisecondArray};

    use super::append_timestamp_runs;

    #[test]
    fn test_regular_series_and_singleton_runs_preserve_values() {
        let pool: Arc<dyn MemoryPool> = Arc::new(UnboundedMemoryPool::default());
        let mut memory = MemoryConsumer::new("parquet-series-test").register(&pool);
        let mut first_series = Vec::new();
        let values1 = Float64Array::from(vec![1.25, -0.0]);
        let times1 = TimestampMillisecondArray::from(vec![10, 20]);
        assert_eq!(
            2,
            append_timestamp_runs(&mut first_series, &values1, &times1, 0, 2, &mut memory).unwrap()
        );
        let values2 = Float64Array::from(vec![3.5, 4.5, 5.5, 6.5]);
        let times2 = TimestampMillisecondArray::from(vec![30, 40, 50, 55]);
        assert_eq!(
            4,
            append_timestamp_runs(&mut first_series, &values2, &times2, 0, 4, &mut memory).unwrap()
        );
        let samples = first_series
            .iter()
            .flat_map(|block| block.values.materialize().values().to_vec())
            .collect::<Vec<_>>();
        assert_eq!(
            samples
                .iter()
                .map(|value| value.to_bits())
                .collect::<Vec<_>>(),
            vec![
                1.25f64.to_bits(),
                (-0.0f64).to_bits(),
                3.5f64.to_bits(),
                4.5f64.to_bits(),
                5.5f64.to_bits(),
                6.5f64.to_bits()
            ]
        );
        assert_eq!(first_series[0].first_timestamp, 10);
        assert_eq!(first_series[0].timestamp_step, 10);
        assert_eq!(first_series[1].first_timestamp, 30);
        assert_eq!(first_series[1].timestamp_step, 10);
        assert_eq!(first_series[1].values.len(), 3);
        assert_eq!(first_series[2].first_timestamp, 55);
        assert_eq!(first_series[2].values.len(), 1);

        let mut second_series = Vec::new();
        let values = Float64Array::from(vec![7.0, 8.0]);
        let times = TimestampMillisecondArray::from(vec![5, 15]);
        append_timestamp_runs(&mut second_series, &values, &times, 0, 2, &mut memory).unwrap();
        assert_eq!(second_series.len(), 1);
        assert_eq!(second_series[0].values.materialize().values(), &[7.0, 8.0]);
    }
}

impl PreparedSeriesSource for ParquetSeriesSource {
    fn num_series(&self) -> usize {
        self.series.len()
    }

    fn read_series(
        &self,
        range: std::ops::Range<usize>,
        processor: &mut dyn SeriesBlockProcessor,
    ) -> std::result::Result<(), common_error::ext::BoxedError> {
        if range.start > range.end || range.end > self.series.len() {
            return Err(common_error::ext::BoxedError::new(invalid(
                "Series source range out of bounds",
            )));
        }
        for item in &self.series[range] {
            let decoded = self
                .codec
                .decode(&item.key)
                .map_err(|e| common_error::ext::BoxedError::new(invalid(e.to_string())))?;
            let CompositeValues::Sparse(values) = decoded else {
                return Err(common_error::ext::BoxedError::new(invalid(
                    "Expected sparse primary key",
                )));
            };
            let names = processor.tag_names();
            let tag_memory = self._memory.new_empty();
            let metadata = self.context.input.region_metadata();
            let tag_count = metadata
                .primary_key_columns()
                .filter(|column| {
                    names
                        .as_ref()
                        .is_none_or(|names| names.contains(&column.column_schema.name))
                })
                .count();
            let tag_bytes = metadata
                .primary_key_columns()
                .filter(|column| {
                    names
                        .as_ref()
                        .is_none_or(|names| names.contains(&column.column_schema.name))
                })
                .try_fold(
                    tag_count
                        .saturating_mul(std::mem::size_of::<(String, datatypes::value::Value)>()),
                    |bytes, column| {
                        let value = values.get_or_null(column.column_id);
                        bytes
                            .checked_add(column.column_schema.name.len())
                            .and_then(|total| total.checked_add(value.as_value_ref().data_size()))
                    },
                )
                .ok_or_else(|| {
                    common_error::ext::BoxedError::new(invalid("Tag memory size overflow"))
                })?;
            tag_memory
                .try_grow(tag_bytes)
                .map_err(|e| common_error::ext::BoxedError::new(invalid(e.to_string())))?;
            let mut tags = Vec::with_capacity(tag_count);
            tags.extend(
                metadata
                    .primary_key_columns()
                    .filter(|column| {
                        names
                            .as_ref()
                            .is_none_or(|names| names.contains(&column.column_schema.name))
                    })
                    .map(|column| {
                        (
                            column.column_schema.name.clone(),
                            values.get_or_null(column.column_id).clone(),
                        )
                    }),
            );
            let mut blocks = item.blocks.iter().map(|block| {
                Ok(VisibleSeriesBlock {
                    first_timestamp: block.first_timestamp,
                    timestamp_step: block.timestamp_step,
                    values: block.values.clone(),
                })
            });
            processor.process_series(&tags, &mut blocks)?;
            if blocks.next().is_some() {
                return Err(common_error::ext::BoxedError::new(invalid(
                    "Series processor left unread blocks",
                )));
            }
        }
        Ok(())
    }
}
