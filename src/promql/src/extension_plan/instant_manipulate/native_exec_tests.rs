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

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use common_error::ext::{BoxedError, ErrorExt, PlainError};
use common_error::status_code::StatusCode;
use datafusion::arrow::array::{
    Array, ArrayRef, Float64Array, StructArray, TimestampMillisecondArray, UInt32Array,
    new_null_array,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::error::DataFusionError;
use datafusion::execution::memory_pool::{GreedyMemoryPool, MemoryConsumer, MemoryPool};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datatypes::arrow::buffer::ScalarBuffer;
use datatypes::series::histogram::public::{
    NATIVE_HISTOGRAM_FIELD_NAMES, PublicHistogramBatch, SUM_FIELD, native_histogram_arrow_type,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, PublicHistogramSamples, Samples, SeriesFields, SeriesSchema,
};
use datatypes::series::{MetricName, SeriesOrdering};
use futures::Stream;
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};

use super::*;

fn raw_schema() -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: Arc::from("value"),
        }]),
        eval_grid: None,
    })
}

fn output_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("value", DataType::Float64, true),
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]))
}

fn mapping_for(
    start: i64,
    end: i64,
    step: i64,
    lookback: i64,
) -> (Arc<NativeInstantMapping>, Arc<SeriesSchema>) {
    let logical = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    });
    let node = InstantManipulate::new(
        start,
        end,
        lookback,
        step,
        0,
        "time".into(),
        vec![],
        Some("value".into()),
        logical,
    );
    let series = raw_schema();
    let mapping = NativeInstantMapping::try_new(&node, output_schema(), series.clone(), &[])
        .unwrap()
        .unwrap();
    (Arc::new(mapping), series)
}

fn chunk(time: &[i64], values: &[f64], last: bool) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        &raw_schema(),
        datatypes::arrow::buffer::Buffer::from(vec![1, 2, 3]),
        Some(MetricName::try_new(None, vec![]).unwrap()),
        RawTimestamps::try_new(ScalarBuffer::from(time.to_vec())).unwrap(),
        SeriesFields::one(Samples::from_float(Float64Array::from(values.to_vec()))),
        last,
    )
    .unwrap()
}

struct RawMock {
    schema: Arc<SeriesSchema>,
    labels: RawLabelSource,
    items: VecDeque<Poll<Option<std::result::Result<RawSeriesChunk, BoxedError>>>>,
    polls: Arc<AtomicUsize>,
    drops: Arc<AtomicUsize>,
}
impl Stream for RawMock {
    type Item = std::result::Result<RawSeriesChunk, BoxedError>;
    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls.fetch_add(1, Ordering::SeqCst);
        self.items.pop_front().unwrap_or(Poll::Ready(None))
    }
}
impl Drop for RawMock {
    fn drop(&mut self) {
        self.drops.fetch_add(1, Ordering::SeqCst);
    }
}
impl RawSeriesStream for RawMock {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }
    fn label_source(&self) -> &RawLabelSource {
        &self.labels
    }
    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::Contiguous
    }
}

fn raw(
    items: impl IntoIterator<Item = Poll<Option<std::result::Result<RawSeriesChunk, BoxedError>>>>,
) -> (SendableRawSeriesStream, Arc<AtomicUsize>, Arc<AtomicUsize>) {
    let polls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    (
        Box::pin(RawMock {
            schema: raw_schema(),
            labels: RawLabelSource::Explicit,
            items: items.into_iter().collect(),
            polls: polls.clone(),
            drops: drops.clone(),
        }),
        polls,
        drops,
    )
}

fn stream(source: SendableRawSeriesStream, pool: &Arc<dyn MemoryPool>) -> NativeInstantStream {
    stream_for(source, pool, 10, 30, 10, 10)
}

fn stream_for(
    source: SendableRawSeriesStream,
    pool: &Arc<dyn MemoryPool>,
    start: i64,
    end: i64,
    step: i64,
    lookback: i64,
) -> NativeInstantStream {
    let (mapping, schema) = mapping_for(start, end, step, lookback);
    NativeInstantStream {
        source: Some(source),
        mapping: mapping.clone(),
        schema: mapping.schema(),
        series_schema: schema,
        pending: Vec::new(),
        reservation: MemoryConsumer::new("NativeInstantStreamTest").register(pool),
    }
}

fn poll(stream: &mut NativeInstantStream) -> Poll<Option<DfResult<RecordBatch>>> {
    let waker = futures::task::noop_waker();
    let mut cx = Context::from_waker(&waker);
    Pin::new(stream).poll_next(&mut cx)
}

#[test]
fn emits_complete_series_before_polling_later_pending_input() {
    let (input, polls, _) = raw([
        Poll::Ready(Some(Ok(chunk(&[10], &[1.0], false)))),
        Poll::Ready(Some(Ok(chunk(&[20], &[2.0], true)))),
        Poll::Pending,
    ]);
    let pool = Arc::new(GreedyMemoryPool::new(1_000_000)) as Arc<dyn MemoryPool>;
    let mut stream = stream(input, &pool);
    let Poll::Ready(Some(Ok(batch))) = poll(&mut stream) else {
        panic!("completed series should emit")
    };
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[1.0, 2.0]
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values(),
        &[10, 20]
    );
    assert!(matches!(poll(&mut stream), Poll::Pending));
    assert_eq!(polls.load(Ordering::SeqCst), 3);
}

#[test]
fn source_error_after_prefix_is_returned_once_and_clears_reservation_and_input() {
    let failure = BoxedError::new(PlainError::new(
        "mock source error".into(),
        StatusCode::Internal,
    ));
    let (input, polls, drops) = raw([
        Poll::Ready(Some(Ok(chunk(&[10], &[1.0], true)))),
        Poll::Ready(Some(Err(failure))),
    ]);
    let pool = Arc::new(GreedyMemoryPool::new(1_000_000)) as Arc<dyn MemoryPool>;
    let mut stream = stream(input, &pool);
    assert!(matches!(poll(&mut stream), Poll::Ready(Some(Ok(_)))));
    let Poll::Ready(Some(Err(error))) = poll(&mut stream) else {
        panic!("source error must be returned once")
    };
    assert_eq!(
        common_query::error::datafusion_status_code::<common_query::error::Error>(&error, None),
        StatusCode::Internal
    );
    let DataFusionError::External(source) = error else {
        panic!("source error must stay a typed DataFusionError::External payload")
    };
    let source = source
        .downcast_ref::<BoxedError>()
        .expect("source error must still be the original BoxedError");
    assert_eq!(source.status_code(), StatusCode::Internal);
    assert_eq!(source.to_string(), "mock source error");
    assert!(matches!(poll(&mut stream), Poll::Ready(None)));
    assert!(matches!(poll(&mut stream), Poll::Ready(None)));
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn unfinished_series_eof_is_error_once_and_drops_partial_chunks() {
    let (input, _, drops) = raw([Poll::Ready(Some(Ok(chunk(&[10], &[1.0], false))))]);
    let pool = Arc::new(GreedyMemoryPool::new(1_000_000)) as Arc<dyn MemoryPool>;
    let mut stream = stream(input, &pool);
    assert!(matches!(poll(&mut stream), Poll::Ready(Some(Err(_)))));
    assert_eq!(stream.pending.len(), 0);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(pool.reserved(), 0);
    assert!(matches!(poll(&mut stream), Poll::Ready(None)));
}

#[test]
fn dropping_pending_stream_releases_source_chunks_and_memory() {
    let array = Arc::new(Float64Array::from(vec![1.0]));
    let samples = Samples::from_float(array.as_ref().clone());
    drop(array);
    let pending_chunk = RawSeriesChunk::try_new(
        &raw_schema(),
        datatypes::arrow::buffer::Buffer::from(vec![1]),
        Some(MetricName::try_new(None, vec![]).unwrap()),
        RawTimestamps::try_new(ScalarBuffer::from(vec![10])).unwrap(),
        SeriesFields::one(samples),
        false,
    )
    .unwrap();
    let (input, _, drops) = raw([Poll::Ready(Some(Ok(pending_chunk))), Poll::Pending]);
    let pool = Arc::new(GreedyMemoryPool::new(1_000_000)) as Arc<dyn MemoryPool>;
    let mut stream = stream(input, &pool);
    assert!(matches!(poll(&mut stream), Poll::Pending));
    assert!(pool.reserved() > 0);
    drop(stream);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn reservation_failure_is_terminal_and_releases_input_and_partial_series() {
    let (input, _, drops) = raw([Poll::Ready(Some(Ok(chunk(&[10], &[1.0], false))))]);
    let pool = Arc::new(GreedyMemoryPool::new(1)) as Arc<dyn MemoryPool>;
    let mut stream = stream(input, &pool);
    assert!(matches!(
        poll(&mut stream),
        Poll::Ready(Some(Err(DataFusionError::ResourcesExhausted(_))))
    ));
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(stream.pending.len(), 0);
    assert_eq!(stream.pending.capacity(), 0);
    assert_eq!(pool.reserved(), 0);
    assert!(matches!(poll(&mut stream), Poll::Ready(None)));
}

#[test]
fn source_error_drops_unfinished_series_without_emitting_it() {
    let failure = BoxedError::new(PlainError::new(
        "mock source error".into(),
        StatusCode::Internal,
    ));
    let (input, polls, drops) = raw([
        Poll::Ready(Some(Ok(chunk(&[10], &[1.0], false)))),
        Poll::Ready(Some(Err(failure))),
    ]);
    let pool = Arc::new(GreedyMemoryPool::new(1_000_000)) as Arc<dyn MemoryPool>;
    let mut stream = stream(input, &pool);
    assert!(matches!(poll(&mut stream), Poll::Ready(Some(Err(_)))));
    assert!(stream.pending.is_empty());
    assert_eq!(stream.pending.capacity(), 0);
    assert!(matches!(poll(&mut stream), Poll::Ready(None)));
    assert_eq!(polls.load(Ordering::SeqCst), 2);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn descriptor_heavy_series_fails_reservation_before_accumulating_unbounded_chunks() {
    let items = (0..512)
        .map(|i| Poll::Ready(Some(Ok(chunk(&[i], &[i as f64], false)))))
        .chain([Poll::Pending]);
    let (input, polls, drops) = raw(items);
    let pool = Arc::new(GreedyMemoryPool::new(32 * 1024)) as Arc<dyn MemoryPool>;
    let mut stream = stream(input, &pool);
    assert!(matches!(
        poll(&mut stream),
        Poll::Ready(Some(Err(DataFusionError::ResourcesExhausted(_))))
    ));
    assert!(stream.pending.is_empty());
    assert_eq!(stream.pending.capacity(), 0);
    assert!(matches!(poll(&mut stream), Poll::Ready(None)));
    assert!(polls.load(Ordering::SeqCst) < 512);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn dense_large_chunk_uses_segment_count_not_row_count_for_descriptors() {
    let count = 100_000usize;
    let timestamps = (0..count).map(|i| i as i64).collect::<Vec<_>>();
    let values = Float64Array::from(vec![7.0; count]);
    let dense_chunk = RawSeriesChunk::try_new(
        &raw_schema(),
        datatypes::arrow::buffer::Buffer::from(vec![1]),
        Some(MetricName::try_new(None, vec![]).unwrap()),
        RawTimestamps::try_new(ScalarBuffer::from(timestamps)).unwrap(),
        SeriesFields::one(Samples::from_float(values)),
        true,
    )
    .unwrap();
    let (input, _, _) = raw([Poll::Ready(Some(Ok(dense_chunk)))]);
    // Input timestamp/field buffers and concatenation scratch fit; row-scaled Samples
    // descriptors would exceed this pool despite there being only one dense segment.
    let pool = Arc::new(GreedyMemoryPool::new(4 * 1024 * 1024)) as Arc<dyn MemoryPool>;
    let mut stream = stream_for(input, &pool, 10, 11, 1, 0);
    let Poll::Ready(Some(Ok(batch))) = poll(&mut stream) else {
        panic!("dense chunk should fit its bounded pool")
    };
    assert_eq!(batch.num_rows(), 2);
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[7.0, 7.0]
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values(),
        &[10, 11]
    );
    assert_eq!(pool.reserved(), 0);
}

#[test]
fn output_grid_scratch_is_reserved_before_large_evaluation() {
    let (input, polls, drops) = raw([Poll::Ready(Some(Ok(chunk(&[0], &[7.0], true))))]);
    let pool = Arc::new(GreedyMemoryPool::new(128 * 1024)) as Arc<dyn MemoryPool>;
    // One source point with a long lookback produces a large grid despite the tiny source.
    let mut stream = stream_for(input, &pool, 0, 900_000, 1, 900_001);
    assert!(matches!(
        poll(&mut stream),
        Poll::Ready(Some(Err(DataFusionError::ResourcesExhausted(_))))
    ));
    assert_eq!(polls.load(Ordering::SeqCst), 1);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    assert!(stream.pending.is_empty());
    assert_eq!(pool.reserved(), 0);
    assert!(matches!(poll(&mut stream), Poll::Ready(None)));
}

#[test]
fn larger_pool_completes_one_point_many_evaluation_grid() {
    let (input, _, _) = raw([Poll::Ready(Some(Ok(chunk(&[0], &[7.0], true))))]);
    let pool = Arc::new(GreedyMemoryPool::new(32 * 1024 * 1024)) as Arc<dyn MemoryPool>;
    let mut stream = stream_for(input, &pool, 0, 4, 1, 5);
    let Poll::Ready(Some(Ok(batch))) = poll(&mut stream) else {
        panic!("sufficient reservation should evaluate")
    };
    assert_eq!(
        batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .values(),
        &[7.0; 5]
    );
    assert_eq!(
        batch
            .column(1)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values(),
        &[0, 1, 2, 3, 4]
    );
    assert_eq!(pool.reserved(), 0);
}

/// The materialized metric of the public-histogram chunk fixture.
const PUBLIC_HISTOGRAM_METRIC: &str = "greptime_public_histogram_total";

/// Direct public-histogram sum fixture: ordered 18-field schema, not a canonical histogram.
fn public_histogram_struct(sums: &[Option<f64>]) -> StructArray {
    let DataType::Struct(fields) = native_histogram_arrow_type() else {
        panic!("the public native histogram type must be a struct");
    };
    assert_eq!(fields.len(), NATIVE_HISTOGRAM_FIELD_NAMES.len());
    let children = fields
        .iter()
        .map(|field| {
            if field.name() == SUM_FIELD {
                Arc::new(Float64Array::from(sums.to_vec())) as ArrayRef
            } else {
                new_null_array(field.data_type(), sums.len())
            }
        })
        .collect::<Vec<_>>();
    StructArray::new(fields, children, None)
}

/// A raw series chunk whose single field is a public-histogram carrier, carrying the materialized
/// metric of the series like the float `chunk` helper above.
fn public_histogram_chunk(
    time: &[i64],
    sums: &[Option<f64>],
    indices: Vec<Option<u32>>,
    floats: Option<Vec<Option<f64>>>,
) -> RawSeriesChunk {
    let batch =
        Arc::new(PublicHistogramBatch::try_from_arrow(public_histogram_struct(sums)).unwrap());
    let samples = PublicHistogramSamples::try_new(
        UInt32Array::from(indices),
        batch,
        floats.map(Float64Array::from),
    )
    .unwrap();
    RawSeriesChunk::try_new(
        &raw_schema(),
        datatypes::arrow::buffer::Buffer::from(vec![1, 2, 3]),
        Some(MetricName::try_new(Some(Arc::from(PUBLIC_HISTOGRAM_METRIC)), vec![]).unwrap()),
        RawTimestamps::try_new(ScalarBuffer::from(time.to_vec())).unwrap(),
        SeriesFields::one(Samples::from_public_histogram(samples)),
        true,
    )
    .unwrap()
}

#[test]
fn public_histogram_chunks_are_rejected_by_float_only_retained_accounting() {
    let cases = [
        (
            "pure",
            public_histogram_chunk(
                &[10, 20, 30],
                &[Some(1.0), None, Some(3.0)],
                vec![Some(0), Some(1), Some(2)],
                None,
            ),
        ),
        (
            "mixed with optional floats",
            public_histogram_chunk(
                &[10, 20],
                &[Some(1.0), Some(2.0)],
                vec![Some(0), None],
                Some(vec![None, Some(0.5)]),
            ),
        ),
    ];
    for (name, chunk) in cases {
        // The chunk really carries the public histogram field.
        let field = chunk.fields.get(0).unwrap();
        assert!(matches!(
            field.dense(),
            Some(DenseSamplesRef::PublicHistogram(_))
        ));
        // Float-only retained accounting must refuse the whole chunk instead of charging only its
        // timestamps and indices, so no part of the histogram payload is under-charged.
        assert_eq!(retained_chunk_bytes(&chunk), None, "{name}");
    }

    // The existing float lane keeps its non-zero estimate for the same chunk shape.
    let float_chunk = chunk(&[10, 20, 30], &[1.0, 2.0, 3.0], true);
    assert!(
        retained_chunk_bytes(&float_chunk).is_some_and(|bytes| bytes > 0),
        "float chunks stay accounted"
    );
}
