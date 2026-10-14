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

use std::result::Result as StdResult;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use base64::Engine;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use parquet::arrow::async_reader::MetadataFetch;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::statistics::Statistics;
use snafu::{IntoError as _, ResultExt};
use store_api::metadata::RegionMetadata;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use crate::error::{self, Result};
use crate::sst::parquet::SERIES_RUNS_METADATA_KEY;
use crate::sst::parquet::reader::MetadataCacheMetrics;
use crate::sst::parquet::writer::SeriesRun;

/// Decodes native per-series run descriptors from a Parquet footer.
pub(crate) fn decode_series_runs(metadata: &ParquetMetaData) -> Option<Vec<SeriesRun>> {
    let value = metadata
        .file_metadata()
        .key_value_metadata()?
        .iter()
        .find(|kv| kv.key == SERIES_RUNS_METADATA_KEY)?
        .value
        .as_deref()?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(value)
        .ok()?;
    postcard::from_bytes(&bytes).ok()
}

/// The estimated size of the footer and metadata need to read from the end of parquet file.
const DEFAULT_PREFETCH_SIZE: u64 = 64 * 1024;

pub struct MetadataLoader<'a> {
    // An object store that supports async read
    object_store: ObjectStore,
    // The path of parquet file
    file_path: &'a str,
    // The size of parquet file
    file_size: u64,
    page_index_policy: PageIndexPolicy,
}

impl<'a> MetadataLoader<'a> {
    /// Create a new parquet metadata loader.
    pub fn new(
        object_store: ObjectStore,
        file_path: &'a str,
        file_size: u64,
    ) -> MetadataLoader<'a> {
        Self {
            object_store,
            file_path,
            file_size,
            page_index_policy: Default::default(),
        }
    }

    pub(crate) fn with_page_index_policy(&mut self, page_index_policy: PageIndexPolicy) {
        self.page_index_policy = page_index_policy;
    }

    /// Get the size of parquet file. If file_size is 0, stat the object store to get the size.
    async fn get_file_size(&self) -> Result<u64> {
        let file_size = match self.file_size {
            0 => self
                .object_store
                .stat(self.file_path)
                .await
                .context(error::OpenDalSnafu)?
                .content_length(),
            other => other,
        };
        Ok(file_size)
    }

    pub async fn load(&self, cache_metrics: &mut MetadataCacheMetrics) -> Result<ParquetMetaData> {
        let path = self.file_path;
        let file_size = self.get_file_size().await?;
        let reader = ParquetMetaDataReader::new()
            .with_prefetch_hint(Some(DEFAULT_PREFETCH_SIZE as usize))
            // Mito uses offset indexes to translate row selections into byte ranges. It does not
            // consume Parquet column indexes, so decoding them only bloats the metadata cache.
            .with_column_index_policy(PageIndexPolicy::Skip)
            .with_offset_index_policy(self.page_index_policy);

        let num_reads = AtomicUsize::new(0);
        let bytes_read = AtomicU64::new(0);
        let fetch = ObjectStoreFetch {
            object_store: &self.object_store,
            file_path: self.file_path,
            num_reads: &num_reads,
            bytes_read: &bytes_read,
        };

        let metadata = reader
            .load_and_finish(fetch, file_size)
            .await
            .map_err(|e| match unbox_external_error(e) {
                Ok(os_err) => error::OpenDalSnafu {}.into_error(os_err),
                Err(parquet_err) => error::ReadParquetSnafu { path }.into_error(parquet_err),
            })?;

        cache_metrics.num_reads = num_reads.into_inner();
        cache_metrics.bytes_read = bytes_read.into_inner();

        Ok(metadata)
    }
}

/// Unpack ParquetError to get object_store::Error if possible.
fn unbox_external_error(e: ParquetError) -> StdResult<object_store::Error, ParquetError> {
    match e {
        ParquetError::External(boxed_err) => match boxed_err.downcast::<object_store::Error>() {
            Ok(os_err) => Ok(*os_err),
            Err(parquet_error) => Err(ParquetError::External(parquet_error)),
        },
        other => Err(other),
    }
}

pub(crate) fn extract_primary_key_range(
    parquet_meta: &ParquetMetaData,
    region_metadata: &RegionMetadata,
) -> Option<(Bytes, Bytes)> {
    if region_metadata.primary_key.is_empty() {
        return None;
    }

    let pk_column_idx = parquet_meta
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.name() == PRIMARY_KEY_COLUMN_NAME)?;

    let mut min: Option<Bytes> = None;
    let mut max: Option<Bytes> = None;

    for row_group in parquet_meta.row_groups() {
        let Statistics::ByteArray(stats) = row_group.column(pk_column_idx).statistics()? else {
            return None;
        };

        // Truncated bounds can end at a field boundary and masquerade as an
        // older Dense schema. Only complete endpoints can be schema-normalized.
        if !stats.min_is_exact() || !stats.max_is_exact() {
            return None;
        }
        let row_group_min = Bytes::copy_from_slice(stats.min_bytes_opt()?);
        let row_group_max = Bytes::copy_from_slice(stats.max_bytes_opt()?);
        min = Some(match min {
            Some(current) => current.min(row_group_min),
            None => row_group_min,
        });
        max = Some(match max {
            Some(current) => current.max(row_group_max),
            None => row_group_max,
        });
    }

    min.zip(max)
}

struct ObjectStoreFetch<'a> {
    object_store: &'a ObjectStore,
    file_path: &'a str,
    num_reads: &'a AtomicUsize,
    bytes_read: &'a AtomicU64,
}

impl MetadataFetch for ObjectStoreFetch<'_> {
    fn fetch(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let bytes_to_read = range.end - range.start;
        async move {
            let data = self
                .object_store
                .read_with(self.file_path)
                .range(range)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            self.num_reads.fetch_add(1, Ordering::Relaxed);
            self.bytes_read.fetch_add(bytes_to_read, Ordering::Relaxed);
            Ok(data.to_bytes())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{
        ArrayRef, BinaryArray, DictionaryArray, Int64Array, UInt32Array,
    };
    use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use object_store::services::Memory;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::metadata::{KeyValue, ParquetMetaData};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    use super::*;
    use crate::sst::parquet::{PARQUET_METADATA_KEY, SERIES_RUNS_METADATA_KEY};
    use crate::test_util::sst_util::sst_region_metadata;

    fn build_test_parquet_bytes(
        include_primary_key: bool,
        primary_keys: &[&[u8]],
        row_group_sizes: &[usize],
        stats_enabled: EnabledStatistics,
    ) -> Vec<u8> {
        let total_rows = row_group_sizes.iter().sum::<usize>();
        let mut fields = vec![Field::new("field", ArrowDataType::Int64, true)];
        let mut columns: Vec<ArrayRef> =
            vec![Arc::new(Int64Array::from_iter_values(0..total_rows as i64))];
        if include_primary_key {
            assert_eq!(total_rows, primary_keys.len());
            fields.push(Field::new(
                "__primary_key",
                ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::UInt32),
                    Box::new(ArrowDataType::Binary),
                ),
                false,
            ));
            let values = Arc::new(BinaryArray::from_iter_values(primary_keys.iter().copied()));
            let keys = UInt32Array::from_iter_values(0..primary_keys.len() as u32);
            columns.push(Arc::new(DictionaryArray::new(keys, values)));
        }

        let schema = Arc::new(Schema::new(fields));
        let region_metadata = Arc::new(sst_region_metadata());
        let key_value = KeyValue::new(
            PARQUET_METADATA_KEY.to_string(),
            region_metadata.to_json().unwrap(),
        );
        let runs = vec![
            SeriesRun {
                pk: "YQ==".to_string(),
                start: 0,
                count: 2,
                first_timestamp_ms: 10,
                timestamp_step: Some(5),
                value_column: Some("v".to_string()),
                value_first_bits: Some(1.0f64.to_bits()),
                value_step_bits: Some(2.0f64.to_bits()),
                all_put: true,
            },
            SeriesRun {
                pk: "Yg==".to_string(),
                start: 2,
                count: 2,
                first_timestamp_ms: 20,
                timestamp_step: Some(5),
                value_column: None,
                value_first_bits: None,
                value_step_bits: None,
                all_put: false,
            },
        ];
        let run_metadata = KeyValue::new(
            SERIES_RUNS_METADATA_KEY.to_string(),
            base64::engine::general_purpose::STANDARD.encode(postcard::to_allocvec(&runs).unwrap()),
        );
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![key_value, run_metadata]))
            .set_statistics_enabled(stats_enabled)
            .build();

        let mut parquet_bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut parquet_bytes, schema.clone(), Some(props)).unwrap();
        let mut offset = 0;
        for row_group_size in row_group_sizes {
            let batch = RecordBatch::try_new(
                schema.clone(),
                columns
                    .iter()
                    .map(|column| column.slice(offset, *row_group_size))
                    .collect(),
            )
            .unwrap();
            writer.write(&batch).unwrap();
            offset += row_group_size;
        }
        writer.close().unwrap();

        parquet_bytes
    }

    fn build_test_metadata(
        include_primary_key: bool,
        primary_keys: &[&[u8]],
        row_group_sizes: &[usize],
        stats_enabled: EnabledStatistics,
    ) -> ParquetMetaData {
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from(build_test_parquet_bytes(
            include_primary_key,
            primary_keys,
            row_group_sizes,
            stats_enabled,
        )))
        .unwrap()
        .metadata()
        .as_ref()
        .clone()
    }

    #[test]
    fn test_series_runs_metadata_round_trips_and_decodes() {
        let metadata = build_test_metadata(
            true,
            &[b"a", b"a", b"b", b"b"],
            &[2, 2],
            EnabledStatistics::Page,
        );
        let kv = metadata.file_metadata().key_value_metadata().unwrap();
        assert!(kv.iter().any(|entry| entry.key == SERIES_RUNS_METADATA_KEY));
        let runs = decode_series_runs(&metadata).unwrap();
        assert_eq!(2, runs.len());
        assert_eq!(
            ("YQ==", 0, 2, 10, Some(5)),
            (
                runs[0].pk.as_str(),
                runs[0].start,
                runs[0].count,
                runs[0].first_timestamp_ms,
                runs[0].timestamp_step
            )
        );
        assert_eq!(Some("v"), runs[0].value_column.as_deref());
        assert_eq!(Some(1.0f64.to_bits()), runs[0].value_first_bits);
        assert_eq!(Some(2.0f64.to_bits()), runs[0].value_step_bits);
        assert_eq!(
            ("Yg==", 2, 2, None, None),
            (
                runs[1].pk.as_str(),
                runs[1].start,
                runs[1].count,
                runs[1].value_column.as_deref(),
                runs[1].value_first_bits
            )
        );
    }

    #[tokio::test]
    async fn test_metadata_loader_only_loads_offset_indexes() {
        let parquet_bytes = build_test_parquet_bytes(false, &[], &[4], EnabledStatistics::Page);
        let file_size = parquet_bytes.len() as u64;
        let file_path = "test.parquet";
        let object_store = ObjectStore::new(Memory::default()).unwrap();
        object_store.write(file_path, parquet_bytes).await.unwrap();

        let mut loader = MetadataLoader::new(object_store, file_path, file_size);
        loader.with_page_index_policy(PageIndexPolicy::Required);
        let metadata = loader
            .load(&mut MetadataCacheMetrics::default())
            .await
            .unwrap();

        assert!(metadata.column_index().is_none());
        assert!(metadata.offset_index().is_some());
    }

    #[test]
    fn test_extract_primary_key_range_returns_none_when_column_absent() {
        let metadata = build_test_metadata(false, &[], &[1], EnabledStatistics::Page);
        let region_metadata = sst_region_metadata();

        assert_eq!(None, extract_primary_key_range(&metadata, &region_metadata));
    }

    #[test]
    fn test_extract_primary_key_range_folds_row_group_stats() {
        let metadata = build_test_metadata(
            true,
            &[b"bbb", b"ccc", b"aaa", b"zzz"],
            &[2, 2],
            EnabledStatistics::Page,
        );
        let region_metadata = sst_region_metadata();

        assert_eq!(
            Some((Bytes::from_static(b"aaa"), Bytes::from_static(b"zzz"))),
            extract_primary_key_range(&metadata, &region_metadata)
        );
    }

    #[test]
    fn test_extract_primary_key_range_rejects_truncated_statistics() {
        let key = vec![b'a'; 1024];
        let metadata = build_test_metadata(true, &[&key], &[1], EnabledStatistics::Page);
        assert_eq!(
            None,
            extract_primary_key_range(&metadata, &sst_region_metadata())
        );
    }

    #[test]
    fn test_extract_primary_key_range_returns_none_when_any_rg_stats_missing() {
        let metadata = build_test_metadata(
            true,
            &[b"bbb", b"ccc", b"aaa", b"zzz"],
            &[2, 2],
            EnabledStatistics::None,
        );
        let region_metadata = sst_region_metadata();

        assert_eq!(None, extract_primary_key_range(&metadata, &region_metadata));
    }
}
