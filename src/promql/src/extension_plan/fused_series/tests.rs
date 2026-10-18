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

//! End-to-end checks of [`FusedSeriesExec`] against the same kernels driven directly.
//!
//! The reference path pushes the same chunks through a fresh [`RangeEvaluator`], applies the same
//! `map_math` chain and converts with the same private conversion, so a test fails when the
//! node's plumbing (finalization, ordering, labels, trimming) diverges, not when a kernel's
//! numerics change. Kernel numerics have their own tests.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::task::{Context, Poll};

use common_query::prometheus::PROMETHEUS_STALE_NAN_BITS;
use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, StringArray, TimestampMillisecondArray,
};
use datafusion::arrow::buffer::{Buffer, ScalarBuffer};
use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit, UInt32Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::test::TestMemoryExec;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, displayable,
};
use datatypes::series::frame::{
    RawLabelMode, RawSeriesChunkCursor, RawSeriesFrameBuilder, raw_series_frame_schema,
};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{FieldSpec, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};
use datatypes::series::{Label, MetricName, SeriesOrdering};
use futures::{Stream, StreamExt};
use store_api::region_engine::RawLabelSource;

use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};

use super::{
    FusedSeriesExec, MAX_CHUNKS_PER_POLL, MAX_ROWS_PER_BATCH, METRIC_EMIT, METRIC_EVALUATE,
    METRIC_FLUSH, METRIC_INPUT_CHUNKS, METRIC_INPUT_POLL, METRIC_SERIES_COMPLETED,
    METRIC_SERIES_EMITTED, apply_maps, classify_output, occupancy, series_to_record_batch,
};
use crate::map::MathOp;
use crate::range_eval::{RangeEvaluator, RangeFunction, RangeWindow};

const TIME: &str = "time";
const VALUE: &str = "value";
const VALUE2: &str = "value2";
const GRID_START: i64 = 300_000;
const GRID_STEP: i64 = 60_000;
const GRID_LEN: u64 = 5;

/// Source timestamp and `value` of one raw row; `None` is a missing value.
type Row = (i64, Option<f64>);

fn series_schema() -> Arc<SeriesSchema> {
    fields_schema(&[VALUE])
}

/// A raw series schema with one `Float64` field per name.
fn fields_schema(names: &[&str]) -> Arc<SeriesSchema> {
    Arc::new(SeriesSchema {
        fields: names
            .iter()
            .map(|name| FieldSpec {
                name: Arc::from(*name),
            })
            .collect(),
        eval_grid: None,
    })
}

/// The two-field schema of the multi-field case.
fn two_field_schema() -> Arc<SeriesSchema> {
    fields_schema(&[VALUE, VALUE2])
}

/// One `Float64` value column plus one nullable `Utf8` column per tag and a non-null timestamp.
fn output_schema(tags: &[&str]) -> SchemaRef {
    output_schema_of(&[VALUE], tags)
}

/// One `Float64` column per field name, one nullable `Utf8` column per tag, then the timestamp.
fn output_schema_of(fields: &[&str], tags: &[&str]) -> SchemaRef {
    let mut columns = fields
        .iter()
        .map(|name| Field::new(*name, DataType::Float64, true))
        .collect::<Vec<_>>();
    columns.extend(
        tags.iter()
            .map(|tag| Field::new(*tag, DataType::Utf8, true)),
    );
    columns.push(Field::new(
        TIME,
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    ));
    Arc::new(Schema::new(columns))
}

/// [`output_schema_of`] with every tag column `Dictionary(UInt32, Utf8)`, the encoding the real
/// PromQL plan reports for labels.
fn dictionary_output_schema_of(fields: &[&str], tags: &[&str]) -> SchemaRef {
    let mut columns = fields
        .iter()
        .map(|name| Field::new(*name, DataType::Float64, true))
        .collect::<Vec<_>>();
    columns.extend(tags.iter().map(|tag| {
        Field::new(
            *tag,
            DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8)),
            true,
        )
    }));
    columns.push(Field::new(
        TIME,
        DataType::Timestamp(TimeUnit::Millisecond, None),
        false,
    ));
    Arc::new(Schema::new(columns))
}

fn metric(host: &str) -> MetricName {
    named_metric(host, "http_requests_total")
}

fn named_metric(host: &str, name: &str) -> MetricName {
    MetricName::try_new(
        Some(Arc::from(name)),
        vec![Label {
            name: Arc::from("host"),
            value: Arc::from(host),
        }],
    )
    .unwrap()
}

fn chunk(key: u8, host: &str, rows: &[Row], is_last: bool) -> RawSeriesChunk {
    chunk_with(key, metric(host), rows, is_last)
}

/// [`chunk`] with an explicit metric name: `rate` drops `__name__`, so two differently named raw
/// series can still reach the same final label set.
fn named_chunk(name: &str, key: u8, host: &str, rows: &[Row], is_last: bool) -> RawSeriesChunk {
    chunk_with(key, named_metric(host, name), rows, is_last)
}

fn chunk_with(key: u8, metric: MetricName, rows: &[Row], is_last: bool) -> RawSeriesChunk {
    let timestamps = rows
        .iter()
        .map(|(timestamp, _)| *timestamp)
        .collect::<Vec<_>>();
    let values = rows.iter().map(|(_, value)| *value).collect::<Vec<_>>();
    raw_chunk(
        &series_schema(),
        key,
        metric,
        &timestamps,
        vec![Samples::from_float(Float64Array::from(values))],
        is_last,
    )
}

/// One two-field raw chunk, from `(timestamp, value, value2)` rows.
fn two_field_chunk(
    key: u8,
    rows: &[(i64, Option<f64>, Option<f64>)],
    is_last: bool,
) -> RawSeriesChunk {
    let timestamps = rows
        .iter()
        .map(|(timestamp, _, _)| *timestamp)
        .collect::<Vec<_>>();
    let values = rows.iter().map(|(_, value, _)| *value).collect::<Vec<_>>();
    let others = rows.iter().map(|(_, _, value)| *value).collect::<Vec<_>>();
    raw_chunk(
        &two_field_schema(),
        key,
        metric("a"),
        &timestamps,
        vec![
            Samples::from_float(Float64Array::from(values)),
            Samples::from_float(Float64Array::from(others)),
        ],
        is_last,
    )
}

/// Builds one raw chunk from explicit per-field samples.
fn raw_chunk(
    schema: &SeriesSchema,
    key: u8,
    metric: MetricName,
    timestamps: &[i64],
    fields: Vec<Samples>,
    is_last: bool,
) -> RawSeriesChunk {
    RawSeriesChunk::try_new(
        schema,
        Buffer::from(vec![key]),
        Some(metric),
        RawTimestamps::try_new(ScalarBuffer::from(timestamps.to_vec())).unwrap(),
        SeriesFields::try_from_vec(fields).unwrap(),
        is_last,
    )
    .unwrap()
}

fn frame_schema() -> SchemaRef {
    frame_schema_of(&series_schema())
}

fn frame_schema_of(schema: &SeriesSchema) -> SchemaRef {
    raw_series_frame_schema(schema, RawLabelMode::Explicit).unwrap()
}

/// Packs `chunks` into raw frame batches, buffering rows across chunks like a real producer.
fn frame_batches(chunks: &[RawSeriesChunk]) -> Vec<RecordBatch> {
    frame_batches_of(&series_schema(), chunks)
}

fn frame_batches_of(schema: &SeriesSchema, chunks: &[RawSeriesChunk]) -> Vec<RecordBatch> {
    let mut builder = RawSeriesFrameBuilder::try_new(
        schema,
        RawLabelMode::Explicit,
        RawSeriesFrameBuilder::DEFAULT_MAX_ROWS,
        RawSeriesFrameBuilder::DEFAULT_MAX_BYTES,
    )
    .unwrap();
    let mut batches = Vec::new();
    for chunk in chunks {
        let mut cursor =
            RawSeriesChunkCursor::try_new(schema, RawLabelMode::Explicit, chunk.clone()).unwrap();
        while let Some(batch) = cursor.append_to(&mut builder).unwrap() {
            batches.push(batch);
        }
    }
    if let Some(batch) = builder.flush().unwrap() {
        batches.push(batch);
    }
    batches
}

fn memory_child(chunks: &[RawSeriesChunk]) -> Arc<dyn ExecutionPlan> {
    memory_child_of(&series_schema(), chunks)
}

fn memory_child_of(schema: &SeriesSchema, chunks: &[RawSeriesChunk]) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        TestMemoryExec::try_new(
            &[frame_batches_of(schema, chunks)],
            frame_schema_of(schema),
            None,
        )
        .unwrap(),
    )
}

/// A child with one partition per element, each holding its own series: the shape a
/// `PerSeries`-distributed raw-series scan reports, where every partition owns whole series.
fn partitioned_child(partitions: &[&[RawSeriesChunk]]) -> Arc<dyn ExecutionPlan> {
    let batches = partitions
        .iter()
        .map(|chunks| frame_batches(chunks))
        .collect::<Vec<_>>();
    Arc::new(TestMemoryExec::try_new(&batches, frame_schema(), None).unwrap())
}

fn grid() -> Arc<EvalGrid> {
    Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, GRID_LEN).unwrap()).unwrap(),
    )
}

fn stale() -> f64 {
    f64::from_bits(PROMETHEUS_STALE_NAN_BITS)
}

/// Nine once-per-minute counter samples from 60s to 540s, rising `step` per sample, so that every
/// grid point from 300s on has a full five-sample window.
fn counter_rows(step: f64) -> Vec<Row> {
    (1..=9)
        .map(|minute| (minute * 60_000, Some(minute as f64 * step)))
        .collect()
}

/// Splits `rows` into chunks at the given row boundaries; the last chunk is the final one.
fn counter_chunks(key: u8, host: &str, rows: &[Row], bounds: &[usize]) -> Vec<RawSeriesChunk> {
    let mut chunks = Vec::new();
    let mut start = 0;
    for &end in bounds {
        chunks.push(chunk(key, host, &rows[start..end], end == rows.len()));
        start = end;
    }
    if start < rows.len() {
        chunks.push(chunk(key, host, &rows[start..], true));
    }
    chunks
}

/// One fused node configuration plus the reference path over the same configuration.
#[derive(Clone)]
struct Case {
    schema: Arc<SeriesSchema>,
    grid: Arc<EvalGrid>,
    window: RangeWindow,
    function: RangeFunction,
    maps: Vec<MathOp>,
    output_schema: SchemaRef,
    tags: Vec<String>,
}

impl Case {
    fn new(tags: &[&str], window: RangeWindow, function: RangeFunction, maps: Vec<MathOp>) -> Self {
        Self {
            schema: series_schema(),
            grid: grid(),
            window,
            function,
            maps,
            output_schema: output_schema(tags),
            tags: tags.iter().map(|tag| tag.to_string()).collect(),
        }
    }

    fn with_series_schema(&self, schema: Arc<SeriesSchema>) -> Self {
        Self {
            schema,
            ..self.clone()
        }
    }

    fn with_output_schema(&self, output_schema: SchemaRef) -> Self {
        Self {
            output_schema,
            ..self.clone()
        }
    }

    fn with_grid(&self, grid: Arc<EvalGrid>) -> Self {
        Self {
            grid,
            ..self.clone()
        }
    }

    fn fused(&self, child: Arc<dyn ExecutionPlan>) -> Result<FusedSeriesExec> {
        FusedSeriesExec::try_new(
            child,
            self.schema.clone(),
            RawLabelSource::Explicit,
            SeriesOrdering::Contiguous,
            self.grid.clone(),
            self.window,
            self.function,
            self.maps.clone(),
            self.output_schema.clone(),
            self.tags.clone(),
            TIME.to_string(),
        )
    }

    /// Evaluation, then the same map chain, then the same conversion, without the node.
    fn reference(&self, chunks: &[RawSeriesChunk]) -> Vec<(Series, RecordBatch)> {
        let schema = self.schema.clone();
        let mut evaluator =
            RangeEvaluator::try_new(&schema, self.grid.clone(), self.window, self.function)
                .unwrap();
        let classification =
            classify_output(&self.output_schema, &schema, &self.tags, TIME).unwrap();
        let mut out = Vec::new();
        for chunk in chunks {
            let Some(series) = evaluator.push(chunk).unwrap() else {
                continue;
            };
            let series = apply_maps(&evaluator, &self.maps, series).unwrap();
            let batch = {
                let occupancy = occupancy(&series).unwrap();
                series_to_record_batch(
                    &classification,
                    &self.tags,
                    &self.output_schema,
                    &series,
                    &occupancy,
                )
            }
            .unwrap();
            // A series without a value emits no batch, so it has no reference row either.
            if let Some(batch) = batch {
                out.push((series, batch));
            }
        }
        out
    }
}

async fn run(exec: FusedSeriesExec) -> Vec<Result<RecordBatch>> {
    run_partition(&exec, 0).await
}

/// Runs one partition of the node, so a test can execute a multi-partition node.
async fn run_partition(exec: &FusedSeriesExec, partition: usize) -> Vec<Result<RecordBatch>> {
    let mut stream = exec
        .execute(partition, Arc::new(TaskContext::default()))
        .unwrap();
    let mut items = Vec::new();
    while let Some(item) = stream.next().await {
        items.push(item);
    }
    items
}

fn collect(items: Vec<Result<RecordBatch>>) -> Vec<RecordBatch> {
    items
        .into_iter()
        .map(|item| item.expect("the fused stream must not fail"))
        .collect()
}

/// The rendered values of a plain string column.
fn string_values(array: &ArrayRef) -> Vec<Option<String>> {
    let array = array.as_any().downcast_ref::<StringArray>().unwrap();
    (0..array.len())
        .map(|row| array.is_valid(row).then(|| array.value(row).to_string()))
        .collect()
}

/// The rendered values of a `Dictionary(UInt32, Utf8)` column, decoded through its dictionary.
fn dictionary_values(array: &ArrayRef) -> Vec<Option<String>> {
    let array = array
        .as_any()
        .downcast_ref::<DictionaryArray<UInt32Type>>()
        .unwrap();
    let dictionary = array
        .values()
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..array.len())
        .map(|row| {
            array.is_valid(row).then(|| {
                dictionary
                    .value(array.keys().value(row) as usize)
                    .to_string()
            })
        })
        .collect()
}

fn float_column(batch: &RecordBatch, name: &str) -> Vec<Option<f64>> {
    let array = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    (0..array.len())
        .map(|row| array.is_valid(row).then(|| array.value(row)))
        .collect()
}

fn timestamp_column(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let array = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    assert!(array.null_count() == 0, "timestamp column has nulls");
    array.values().to_vec()
}

fn string_column(batch: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let array = batch
        .column_by_name(name)
        .unwrap_or_else(|| panic!("missing column {name}"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    (0..array.len())
        .map(|row| array.is_valid(row).then(|| array.value(row).to_string()))
        .collect()
}

fn grid_range(series: &Series) -> (usize, usize) {
    let TimeAxis::Grid(range) = &series.time else {
        panic!("expected a grid series, got {:?}", series.time);
    };
    (range.start(), range.len())
}

/// Asserts the node emitted exactly the reference rows, comparing values bit for bit.
///
/// The node coalesces completed series into batches of up to [`MAX_ROWS_PER_BATCH`] rows, so a
/// series no longer maps to one batch. Both sides are therefore concatenated first: the comparison
/// is over the rows the node produced, not over the batch boundaries it chose.
fn assert_matches_reference(
    actual: &[RecordBatch],
    reference: &[(Series, RecordBatch)],
    context: &str,
) {
    let expected = reference
        .iter()
        .map(|(_, batch)| batch.clone())
        .collect::<Vec<_>>();
    assert_same_rows(actual, &expected, context);
}

/// Asserts two batch streams hold the same rows, in the same order, under the same schema.
fn assert_same_rows(actual: &[RecordBatch], expected: &[RecordBatch], context: &str) {
    let Some(first) = actual.first().or_else(|| expected.first()) else {
        return;
    };
    let schema = first.schema();
    for batch in actual.iter().chain(expected.iter()) {
        assert_eq!(batch.schema(), schema, "{context}: schema");
    }
    let actual = concat_batches(&schema, actual).unwrap();
    let expected = concat_batches(&schema, expected).unwrap();
    assert_eq!(
        actual.num_rows(),
        expected.num_rows(),
        "{context}: row count"
    );
    for (actual, expected) in actual.columns().iter().zip(expected.columns()) {
        match expected.data_type() {
            DataType::Float64 => {
                let actual = actual.as_any().downcast_ref::<Float64Array>().unwrap();
                let expected = expected.as_any().downcast_ref::<Float64Array>().unwrap();
                let bits = |array: &Float64Array| {
                    (0..array.len())
                        .map(|row| array.is_valid(row).then(|| array.value(row).to_bits()))
                        .collect::<Vec<_>>()
                };
                assert_eq!(bits(actual), bits(expected), "{context}: value column");
            }
            DataType::Utf8 => {
                assert_eq!(
                    string_values(actual),
                    string_values(expected),
                    "{context}: string column"
                );
            }
            DataType::Dictionary(key, value)
                if **key == DataType::UInt32 && **value == DataType::Utf8 =>
            {
                assert_eq!(
                    dictionary_values(actual),
                    dictionary_values(expected),
                    "{context}: dictionary column"
                );
            }
            DataType::Timestamp(TimeUnit::Millisecond, None) => {
                let actual = actual
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                let expected = expected
                    .as_any()
                    .downcast_ref::<TimestampMillisecondArray>()
                    .unwrap();
                assert_eq!(
                    actual.values().to_vec(),
                    expected.values().to_vec(),
                    "{context}: timestamp column"
                );
            }
            other => panic!("{context}: unexpected column type {other:?}"),
        }
    }
}

/// Concatenates every message in an error's source chain.
fn error_chain(error: &DataFusionError) -> String {
    let mut message = error.to_string();
    let mut source = std::error::Error::source(error);
    while let Some(error) = source {
        message.push_str(": ");
        message.push_str(&error.to_string());
        source = error.source();
    }
    message
}

#[tokio::test]
async fn rate_over_three_chunks_matches_reference_and_prometheus() {
    let chunks = vec![
        chunk(
            1,
            "a",
            &[
                (60_000, Some(60.0)),
                (120_000, Some(120.0)),
                (180_000, Some(180.0)),
            ],
            false,
        ),
        chunk(
            1,
            "a",
            &[
                (240_000, Some(240.0)),
                (300_000, Some(300.0)),
                (360_000, Some(360.0)),
            ],
            false,
        ),
        chunk(
            1,
            "a",
            &[
                (420_000, Some(420.0)),
                (480_000, Some(480.0)),
                (540_000, Some(540.0)),
            ],
            true,
        ),
    ];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "rate");

    // A counter rising 60 per minute sampled once a minute extrapolates over a 5m window to
    // exactly one increment per second at every point.
    assert_eq!(
        float_column(&batches[0], VALUE),
        vec![Some(1.0); GRID_LEN as usize]
    );
    assert_eq!(
        timestamp_column(&batches[0], TIME),
        vec![300_000, 360_000, 420_000, 480_000, 540_000]
    );
    let (series, _) = &reference[0];
    assert_eq!(grid_range(series), (0, 5));
    assert_eq!(series.metric.get("host"), Some("a"));
    // `rate` drops the metric name on its own, so the reference keeps only the ordinary label.
    assert_eq!(series.metric.get("__name__"), None);
}

/// The node's own metrics must be observable: `ANALYZE VERBOSE` is the only place a reader can
/// see where a fused query spends its time, and before this the node printed `metrics=[]` while
/// doing the dominant work of the plan.
#[tokio::test]
async fn the_node_reports_its_own_timers_and_counters() {
    let chunks = counter_chunks(1, "a", &counter_rows(60.0), &[5]);
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let exec = case.fused(memory_child(&chunks)).unwrap();
    let batches = collect(run_partition(&exec, 0).await);
    assert!(!batches.is_empty(), "the fixture must produce rows");

    let metrics = exec.metrics().expect("the node must report metrics");
    // Each metric is registered per partition, so a name lookup must sum the partitions.
    let value = |name: &str| -> u64 {
        metrics
            .sum_by_name(name)
            .unwrap_or_else(|| panic!("metric {name} is missing: {metrics:?}"))
            .as_usize() as u64
    };

    // Counters are exact: one chunk in, one completed series, one batch out.
    assert_eq!(value(METRIC_INPUT_CHUNKS), chunks.len() as u64);
    assert_eq!(value(METRIC_SERIES_COMPLETED), 1);
    assert_eq!(value(METRIC_SERIES_EMITTED), 1);

    // `Time` records at least a nanosecond whenever the block ran, so a nonzero value is
    // evidence the block was entered rather than a precise measurement.
    for name in [
        METRIC_INPUT_POLL,
        METRIC_EVALUATE,
        METRIC_EMIT,
        METRIC_FLUSH,
    ] {
        assert!(
            value(name) > 0,
            "timer {name} must be recorded once the block has run: {metrics:?}"
        );
    }

    // `BaselineMetrics` supplies the standard counters, so the node is visible to any consumer
    // that reads them from a plan. They are read through their own accessors: `sum_by_name`
    // hard-codes `false` for `OutputRows` and `OutputBatches`, because their names are implied by
    // the variant rather than carried in it.
    assert_eq!(metrics.output_rows(), Some(GRID_LEN as usize));
    assert_eq!(baseline_output_batches(&metrics), 1);
}

/// `output_batches` summed over every partition.
///
/// `MetricsSet` exposes no accessor for it, and `sum_by_name` excludes the variant, so the
/// metric is read the only remaining way: by matching the variant directly.
fn baseline_output_batches(metrics: &MetricsSet) -> usize {
    metrics
        .iter()
        .filter_map(|metric| match metric.value() {
            MetricValue::OutputBatches(count) => Some(count.value()),
            _ => None,
        })
        .sum()
}

/// A timer must not charge time in which the child was pending: the input-poll timer is skipped
/// when the poll returns `Pending`, so a stream that wakes repeatedly does not accumulate.
#[tokio::test]
async fn the_input_timer_does_not_count_a_pending_poll() {
    let chunks = counter_chunks(1, "a", &counter_rows(60.0), &[5]);
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    // A child that answers `Pending` on every other poll and spends `PENDING_DELAY` doing it:
    // each `Pending` is time the timer must not charge, and each `Ready` is a chunk it must.
    const PENDING_DELAY: std::time::Duration = std::time::Duration::from_millis(8);
    let batches = chunks
        .iter()
        .flat_map(|chunk| frame_batches(std::slice::from_ref(chunk)))
        .collect::<Vec<_>>();
    let child = ProbeExec::new(
        batches,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(false)),
    )
    .pending_after(PENDING_DELAY);
    let exec = case.fused(Arc::new(child)).unwrap();
    let started = std::time::Instant::now();
    let batches = collect(run_partition(&exec, 0).await);
    let wall = started.elapsed();
    assert!(!batches.is_empty());

    let metrics = exec.metrics().expect("the node must report metrics");
    let chunks_seen = metrics.sum_by_name(METRIC_INPUT_CHUNKS).unwrap().as_usize();
    assert_eq!(
        chunks_seen,
        chunks.len(),
        "every chunk must still be consumed"
    );
    let polled = metrics.sum_by_name(METRIC_INPUT_POLL).unwrap().as_usize();
    // The comparison is against the wall time the run actually took, not a fixed budget: the
    // probe's pending polls account for a known share of that wall time, so a timer that
    // charged them reads a comparable fraction of it while a correct one reads a rounding
    // error. An absolute threshold would fail on a loaded machine for a reason that has nothing
    // to do with the property under test, which is how this test came to be flaky.
    assert!(
        wall > PENDING_DELAY * 2,
        "the run must span several pending polls, took {wall:?}"
    );
    assert!(
        (polled as f64) < wall.as_nanos() as f64 / 4.0,
        "the input timer must not accumulate wait time: {polled} ns charged out of {wall:?} wall"
    );
}

#[tokio::test]
async fn abs_of_rate_drops_the_metric_name_column() {
    let chunks = counter_chunks(1, "a", &counter_rows(60.0), &[5]);
    let case = Case::new(
        &["__name__", "host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![MathOp::Abs],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "abs(rate)");

    let batch = &batches[0];
    assert_eq!(
        string_column(batch, "__name__"),
        vec![None; batch.num_rows()],
        "the metric name is dropped, so a `__name__` column is null"
    );
    assert_eq!(
        string_column(batch, "host"),
        vec![Some("a".to_string()); batch.num_rows()]
    );
    assert_eq!(
        float_column(batch, VALUE),
        vec![Some(1.0); batch.num_rows()],
        "every five-minute window holds a full five-sample rate of one per second"
    );

    let mut expected = metric("a");
    expected.remove("__name__");
    let (series, _) = &reference[0];
    assert_eq!(series.metric, expected);
    assert!(
        series
            .metric
            .tags()
            .iter()
            .all(|tag| tag.name.as_ref() != "__name__")
    );
}

#[tokio::test]
async fn sqrt_of_abs_of_rate_matches_reference() {
    let chunks = counter_chunks(1, "a", &counter_rows(60.0), &[5]);
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![MathOp::Abs, MathOp::Sqrt],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "sqrt(abs(rate))");
    assert_eq!(
        float_column(&batches[0], VALUE),
        vec![Some(1.0); batches[0].num_rows()]
    );
}

#[tokio::test]
async fn two_series_in_one_frame_stay_separate() {
    let mut chunks = counter_chunks(1, "a", &counter_rows(60.0), &[5]);
    chunks.extend(counter_chunks(2, "b", &counter_rows(120.0), &[5]));
    // Both series fit in one frame batch, so the node splits them only by `is_last`.
    assert_eq!(frame_batches(&chunks).len(), 1);
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "two series");

    // Both series fit in one output batch, so it is cut by their rows instead.
    assert_eq!(batches.len(), 1, "{batches:?}");
    let hosts = string_column(&batches[0], "host");
    let values = float_column(&batches[0], VALUE);
    assert_eq!(hosts[..5].to_vec(), vec![Some("a".to_string()); 5]);
    assert_eq!(
        values[..5].to_vec(),
        vec![Some(1.0); 5],
        "series a must not see series b's counter"
    );
    assert_eq!(hosts[5..].to_vec(), vec![Some("b".to_string()); 5]);
    assert_eq!(
        values[5..].to_vec(),
        vec![Some(2.0); 5],
        "series b must not see series a's counter"
    );
}

#[tokio::test]
async fn duplicate_final_label_set_at_one_grid_point_is_an_error() {
    // `rate` drops `__name__`, so both raw series reach `{host="x"}` and both cover every grid
    // point: Prometheus rejects that at a shared evaluation timestamp.
    let rows = counter_rows(60.0);
    let chunks = vec![
        named_chunk("a_total", 1, "x", &rows, true),
        named_chunk("b_total", 2, "x", &rows, true),
    ];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let items = run(case.fused(memory_child(&chunks)).unwrap()).await;
    // The first series is buffered, not yet handed downstream, so the collision surfaces as the
    // only item: a failed stream yields its error and nothing else.
    assert_eq!(
        items.len(),
        1,
        "expected exactly the error item, got {items:?}"
    );
    let error = items.into_iter().next().unwrap().err().unwrap();
    let message = error_chain(&error);
    assert!(
        message.contains("vector cannot contain metrics with the same labelset"),
        "{message}"
    );
    assert!(
        message.contains("host=\"x\""),
        "the label set must be readable: {message}"
    );
}

#[tokio::test]
async fn duplicate_final_label_set_with_disjoint_grid_points_is_emitted() {
    // `a_total` only has a window result at 300s while `b_total` only has them at 480s and 540s,
    // so the two series share a final label set but no evaluation timestamp: Prometheus merges
    // them into one output series, this node emits both series as separate rows instead.
    let chunks = vec![
        named_chunk(
            "a_total",
            1,
            "x",
            &[(60_000, Some(60.0)), (120_000, Some(120.0))],
            true,
        ),
        named_chunk(
            "b_total",
            2,
            "x",
            &[(420_000, Some(420.0)), (480_000, Some(480.0))],
            true,
        ),
    ];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "disjoint label sets");

    // Both series are coalesced into one batch of three rows: the two label sets collide without
    // sharing a single grid point, so both survive as separate rows.
    assert_eq!(batches.len(), 1, "{batches:?}");
    assert_eq!(batches[0].num_rows(), 3, "no point may be lost");
    let hosts = string_column(&batches[0], "host");
    assert_eq!(
        hosts,
        vec![Some("x".to_string()); 3],
        "both series carry the same label set"
    );
    let timestamps = timestamp_column(&batches[0], TIME);
    assert_eq!(
        timestamps,
        vec![300_000, 480_000, 540_000],
        "each series keeps the points its own windows produced"
    );
    let mut distinct = timestamps.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        timestamps.len(),
        "no grid point may repeat: {timestamps:?}"
    );
}

#[tokio::test]
async fn empty_frame_emits_no_batch() {
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let items = run(case.fused(memory_child(&[])).unwrap()).await;
    assert!(items.is_empty(), "unexpected items: {items:?}");
}

#[tokio::test]
async fn short_lived_series_is_trimmed_to_its_absolute_interval() {
    let chunks = vec![chunk(
        1,
        "a",
        &[(420_000, Some(1.0)), (480_000, Some(2.0))],
        true,
    )];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "trimmed");

    let batch = &batches[0];
    // Only grid points 2 and 3 have a sample in their window.
    assert_eq!(timestamp_column(batch, TIME), vec![420_000, 480_000]);
    assert_eq!(
        float_column(batch, VALUE),
        vec![Some(1.0), Some(2.0)],
        "the trimmed interval keeps its absolute grid timestamps"
    );
    assert_eq!(grid_range(&reference[0].0), (2, 2));
}

#[tokio::test]
async fn nonzero_offset_is_passed_through_without_moving_the_grid() {
    let chunks = vec![chunk(
        1,
        "a",
        &[
            (420_000, Some(1.0)),
            (480_000, Some(2.0)),
            (540_000, Some(3.0)),
        ],
        true,
    )];
    let offset = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 60_000,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    );
    let plain = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    );
    let reference = offset.reference(&chunks);
    let batches = collect(run(offset.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "offset");

    let timestamps = timestamp_column(&batches[0], TIME);
    assert!(
        timestamps
            .iter()
            .all(|timestamp| (timestamp - GRID_START) % GRID_STEP == 0),
        "the node must not shift the grid timestamps: {timestamps:?}"
    );
    let plain_batches = collect(run(plain.fused(memory_child(&chunks)).unwrap()).await);
    assert_ne!(
        float_column(&plain_batches[0], VALUE),
        float_column(&batches[0], VALUE),
        "the offset must reach the evaluator instead of being ignored"
    );
}

#[tokio::test]
async fn two_fields_convert_both_columns_and_keep_a_half_missing_row() {
    let schema = two_field_schema();
    let chunks = vec![two_field_chunk(
        1,
        &[
            (420_000, Some(1.0), Some(10.0)),
            (480_000, None, Some(20.0)),
            (540_000, Some(3.0), Some(30.0)),
        ],
        true,
    )];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    )
    .with_series_schema(schema.clone())
    .with_output_schema(output_schema_of(&[VALUE, VALUE2], &["host"]));
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child_of(&schema, &chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "two fields");

    let batch = &batches[0];
    assert_eq!(
        timestamp_column(batch, TIME),
        vec![420_000, 480_000, 540_000]
    );
    assert_eq!(
        float_column(batch, VALUE),
        vec![Some(1.0), None, Some(3.0)],
        "only the missing field is null"
    );
    assert_eq!(
        float_column(batch, VALUE2),
        vec![Some(10.0), Some(20.0), Some(30.0)],
        "the row stays because this field has a value"
    );
}

#[tokio::test]
async fn missing_point_inside_the_trimmed_range_drops_its_row() {
    let chunks = vec![chunk(
        1,
        "a",
        &[(420_000, Some(1.0)), (540_000, Some(3.0))],
        true,
    )];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "missing point");

    // The empty middle window is not a point of the series, so it is omitted instead of being
    // emitted as a null row, and the remaining rows keep their own timestamps.
    let batch = &batches[0];
    assert_eq!(timestamp_column(batch, TIME), vec![420_000, 540_000]);
    assert_eq!(
        float_column(batch, VALUE),
        vec![Some(1.0), Some(3.0)],
        "the empty middle window is dropped, not shifted onto a neighbour"
    );
    assert_eq!(grid_range(&reference[0].0), (2, 3));
}

#[tokio::test]
async fn nan_stays_a_value_and_a_stale_marker_only_clears_its_window() {
    // (context, function, rows, expected timestamps, expected values)
    let cases = [
        (
            "ordinary NaN",
            RangeFunction::AvgOverTime,
            vec![(420_000, Some(f64::NAN)), (540_000, Some(f64::NAN))],
            vec![420_000, 540_000],
            vec![Some(f64::NAN), Some(f64::NAN)],
        ),
        (
            "stale marker",
            RangeFunction::SumOverTime,
            vec![
                (420_000, Some(stale())),
                (480_000, Some(1.0)),
                (540_000, Some(3.0)),
            ],
            vec![480_000, 540_000],
            vec![Some(1.0), Some(3.0)],
        ),
    ];
    let describe = |values: &[Option<f64>]| -> Vec<Option<u64>> {
        values.iter().map(|value| value.map(f64::to_bits)).collect()
    };
    for (context, function, rows, expected_timestamps, expected_values) in cases {
        let chunks = vec![chunk(1, "a", &rows, true)];
        let case = Case::new(
            &["host"],
            RangeWindow {
                range_ms: 60_000,
                offset_ms: 0,
                anchor_ms: None,
            },
            function,
            vec![MathOp::Abs],
        );
        let reference = case.reference(&chunks);
        let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
        assert_matches_reference(&batches, &reference, context);

        let batch = &batches[0];
        assert_eq!(
            timestamp_column(batch, TIME),
            expected_timestamps,
            "{context}: absolute grid timestamps"
        );
        let actual = float_column(batch, VALUE);
        assert_eq!(
            describe(&actual),
            describe(&expected_values),
            "{context}: expected {expected_values:?}, got {actual:?}"
        );
    }

    // An ordinary NaN is a value, never a null, so it is neither dropped nor turned into a
    // missing sample: only the empty middle window disappears.
    let nan_chunks = vec![chunk(
        1,
        "a",
        &[(420_000, Some(f64::NAN)), (540_000, Some(f64::NAN))],
        true,
    )];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::AvgOverTime,
        vec![MathOp::Abs],
    );
    let batch = &collect(run(case.fused(memory_child(&nan_chunks)).unwrap()).await)[0];
    let values = float_column(batch, VALUE);
    assert_eq!(batch.num_rows(), 2, "the empty middle window is dropped");
    assert!(values[0].is_some_and(f64::is_nan), "{values:?}");
    assert!(values[1].is_some_and(f64::is_nan), "{values:?}");
}

#[tokio::test]
async fn truncated_input_fails_at_eof_without_reexecuting_the_child() {
    let chunks = vec![chunk(
        1,
        "a",
        &[(60_000, Some(60.0)), (120_000, Some(120.0))],
        false,
    )];
    let executes = Arc::new(AtomicUsize::new(0));
    let plan = ProbeExec::new(
        frame_batches(&chunks),
        Arc::clone(&executes),
        Arc::new(AtomicBool::new(false)),
    );
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let items = run(case.fused(Arc::new(plan)).unwrap()).await;
    assert_eq!(items.len(), 1, "expected exactly one error item");
    let error = items.into_iter().next().unwrap().err().unwrap();
    assert!(
        error_chain(&error).contains("final"),
        "unexpected error: {error}"
    );
    assert_eq!(
        executes.load(Ordering::SeqCst),
        1,
        "the child must not be re-executed after an error"
    );
}

#[tokio::test]
async fn out_of_order_chunks_are_rejected_by_raw_frame_validation() {
    let chunks = vec![
        chunk(1, "a", &[(420_000, Some(1.0)), (540_000, Some(2.0))], false),
        chunk(1, "a", &[(300_000, Some(3.0))], true),
    ];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let items = run(case.fused(memory_child(&chunks)).unwrap()).await;
    assert_eq!(items.len(), 1, "expected exactly one error item");
    let error = items.into_iter().next().unwrap().err().unwrap();
    assert!(
        error_chain(&error).contains("not increasing"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn construction_rejects_unsupported_inputs() {
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );

    let no_partition: Arc<dyn ExecutionPlan> =
        Arc::new(TestMemoryExec::try_new(&[], frame_schema(), None).unwrap());
    let error = case.fused(no_partition).err().unwrap();
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    assert!(
        error.to_string().contains("at least one partition"),
        "{error}"
    );

    let mut fields = output_schema(&["host"])
        .fields()
        .iter()
        .map(|field| field.as_ref().clone())
        .collect::<Vec<_>>();
    fields.push(Field::new("bogus", DataType::Utf8, true));
    let unknown = Arc::new(Schema::new(fields));
    let error = case
        .with_output_schema(unknown)
        .fused(memory_child(&[]))
        .err()
        .unwrap();
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    assert!(error.to_string().contains("unknown column"), "{error}");

    let wrong_type = Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Int64, true),
        Field::new("host", DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let error = case
        .with_output_schema(wrong_type)
        .fused(memory_child(&[]))
        .err()
        .unwrap();
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    assert!(error.to_string().contains("must be Float64"), "{error}");
}

#[tokio::test]
async fn construction_rejects_a_non_nullable_field_column() {
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let not_nullable = Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, false),
        Field::new("host", DataType::Utf8, true),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let error = case
        .with_output_schema(not_nullable)
        .fused(memory_child(&[]))
        .err()
        .unwrap();
    assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
    let message = error.to_string();
    assert!(message.contains(VALUE), "{message}");
    assert!(message.contains("must be nullable"), "{message}");
}

#[tokio::test]
async fn a_non_nullable_tag_column_uses_the_empty_label_value() {
    // The PromQL plan reports tag columns as non-nullable, so the node must accept them and report
    // an absent label the way Prometheus reads it: as the empty label value.
    let not_nullable = Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, true),
        Field::new("host", DataType::Utf8, false),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ]));
    let present = counter_chunks(1, "a", &counter_rows(1.0), &[9]);
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    )
    .with_output_schema(not_nullable.clone());
    let batches = collect(run(case.fused(memory_child(&present)).unwrap()).await);
    assert_eq!(
        string_column(&batches[0], "host"),
        vec![Some("a".to_string()); batches[0].num_rows()],
        "a present label is emitted as itself"
    );

    // A tag the raw series does not carry becomes the empty label value, never a null.
    let case = Case::new(
        &["az"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    )
    .with_output_schema(Arc::new(Schema::new(vec![
        Field::new(VALUE, DataType::Float64, true),
        Field::new("az", DataType::Utf8, false),
        Field::new(
            TIME,
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
    ])));
    let batches = collect(run(case.fused(memory_child(&present)).unwrap()).await);
    let az = batches[0].column_by_name("az").unwrap();
    assert_eq!(az.null_count(), 0, "a non-nullable tag column has no nulls");
    assert_eq!(
        string_column(&batches[0], "az"),
        vec![Some(String::new()); batches[0].num_rows()]
    );
}

#[tokio::test]
async fn execute_rejects_a_partition_past_the_end() {
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    // A single-partition child yields a single-partition node, so partition 1 is out of range.
    let exec = case.fused(memory_child(&[])).unwrap();
    assert_eq!(exec.properties().output_partitioning().partition_count(), 1);
    let error = exec
        .execute(1, Arc::new(TaskContext::default()))
        .err()
        .unwrap();
    assert!(matches!(error, DataFusionError::Execution(_)), "{error}");
    assert!(error.to_string().contains("partitions"), "{error}");
}

#[tokio::test]
async fn explain_shows_the_function_and_the_map_chain() {
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![MathOp::Abs, MathOp::Sqrt],
    );
    let exec = case.fused(memory_child(&[])).unwrap();
    // `DisplayableExecutionPlan` has no `Display` impl in this fork; `indent` covers both
    // display formats.
    for text in [
        displayable(&exec).indent(false).to_string(),
        displayable(&exec).indent(true).to_string(),
    ] {
        assert!(text.starts_with("FusedSeriesExec:"), "{text}");
        assert!(text.contains("eval=rate"), "{text}");
        assert!(text.contains("range=300000ms"), "{text}");
        assert!(text.contains("offset=0ms"), "{text}");
        assert!(text.contains("maps=[abs, sqrt]"), "{text}");
        assert_eq!(text.matches("sqrt").count(), 1, "{text}");
    }
}

#[tokio::test]
async fn dropping_the_stream_releases_the_child() {
    // Enough rows for more than one output batch, so the first batch is handed downstream while
    // the child still has input left to give.
    const SERIES: u8 = 60;
    let grid = Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, 100).unwrap()).unwrap(),
    );
    let chunks = (0..SERIES)
        .map(|index| {
            let host = format!("h{index}");
            chunk(
                index + 1,
                &host,
                &long_counter_rows(160, f64::from(index) + 60.0),
                true,
            )
        })
        .collect::<Vec<_>>();
    let dropped = Arc::new(AtomicBool::new(false));
    let plan = ProbeExec::new(
        frame_batches(&chunks),
        Arc::new(AtomicUsize::new(0)),
        Arc::clone(&dropped),
    );
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    )
    .with_grid(grid);
    let exec = case.fused(Arc::new(plan)).unwrap();
    let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
    assert!(stream.next().await.unwrap().is_ok());
    assert!(
        !dropped.load(Ordering::SeqCst),
        "the child stream is still owned while it has input left"
    );
    drop(stream);
    assert!(
        dropped.load(Ordering::SeqCst),
        "dropping the fused stream must drop the child stream"
    );
}

#[tokio::test]
async fn a_poll_consumes_at_most_the_chunk_budget() {
    // Every series has a single sample, so `rate` has no window result and no batch comes out of
    // any of these chunks: without a budget one poll would drain the whole always-ready child.
    // The input size is fixed rather than derived from the constant, so changing the budget
    // changes one side of the comparison instead of rescaling both.
    const SERIES: u32 = 200;
    let chunks = (0..SERIES)
        .map(|index| {
            let key = u8::try_from(index).unwrap();
            let host = format!("h{index}");
            chunk(key, &host, &[(60_000, Some(1.0))], true)
        })
        .collect::<Vec<_>>();
    // One frame batch per chunk, so the child hands out exactly one chunk per poll.
    let batches = chunks
        .iter()
        .flat_map(|chunk| frame_batches(std::slice::from_ref(chunk)))
        .collect::<Vec<_>>();
    assert_eq!(batches.len(), chunks.len());
    let plan = ProbeExec::new(
        batches,
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicBool::new(false)),
    );
    let yields = Arc::clone(&plan.yields);
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let exec = case.fused(Arc::new(plan)).unwrap();
    let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();

    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    let polled = stream.as_mut().poll_next(&mut context);
    assert!(
        polled.is_pending(),
        "one poll must not drain the input: {polled:?}"
    );
    let consumed = yields.load(Ordering::SeqCst);
    assert!(consumed > 0, "the poll must make progress");
    assert!(
        consumed <= MAX_CHUNKS_PER_POLL,
        "consumed {consumed} chunks in one poll, budget is {MAX_CHUNKS_PER_POLL}"
    );
    assert!(
        (consumed as u32) < SERIES,
        "consumed {consumed} of {SERIES} chunks, the whole input was drained"
    );
}

/// Emits prepared raw frame batches and reports how often it ran and when its stream was dropped.
struct ProbeExec {
    batches: Vec<RecordBatch>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    executes: Arc<AtomicUsize>,
    dropped: Arc<AtomicBool>,
    /// Number of batches the child stream handed out, i.e. the fused node's chunk inputs.
    yields: Arc<AtomicUsize>,
    /// Answer `Pending` on every other poll, the way a child waiting on I/O would.
    pending_every_other: bool,
    /// Wall time each `Pending` poll takes, so the test can tell "charged the wait" from
    /// "did not" by the size of the difference instead of by machine speed.
    pending_delay: Option<std::time::Duration>,
}

impl ProbeExec {
    fn new(
        batches: Vec<RecordBatch>,
        executes: Arc<AtomicUsize>,
        dropped: Arc<AtomicBool>,
    ) -> Self {
        let schema = frame_schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            batches,
            schema,
            properties,
            executes,
            dropped,
            yields: Arc::new(AtomicUsize::new(0)),
            pending_every_other: false,
            pending_delay: None,
        }
    }

    /// Makes the child answer `Pending` on every other poll.
    ///
    /// The fused node must not charge that wait time to any of its block timers, which is the
    /// difference between reporting work and reporting latency.
    /// Makes every `Pending` poll take `delay` of wall time.
    ///
    /// Without this the probe returns `Pending` immediately, so a timer that wrongly spans the
    /// pending poll still reads near zero and the test can only fail when the machine happens to
    /// be slow -- the wrong reason. Charging the delay makes the two behaviours differ by the
    /// injected total, which no amount of load can hide.
    fn pending_after(mut self, delay: std::time::Duration) -> Self {
        self.pending_every_other = true;
        self.pending_delay = Some(delay);
        self
    }
}

impl std::fmt::Debug for ProbeExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeExec").finish()
    }
}

impl DisplayAs for ProbeExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ProbeExec")
    }
}

impl ExecutionPlan for ProbeExec {
    fn name(&self) -> &str {
        "ProbeExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.executes.fetch_add(1, Ordering::SeqCst);
        Ok(Box::pin(ProbeStream {
            batches: self.batches.clone().into_iter(),
            schema: self.schema.clone(),
            dropped: Arc::clone(&self.dropped),
            yields: Arc::clone(&self.yields),
            pending_every_other: self.pending_every_other,
            pending_delay: self.pending_delay,
            polls: 0,
        }))
    }
}

struct ProbeStream {
    batches: std::vec::IntoIter<RecordBatch>,
    schema: SchemaRef,
    dropped: Arc<AtomicBool>,
    yields: Arc<AtomicUsize>,
    pending_every_other: bool,
    pending_delay: Option<std::time::Duration>,
    polls: usize,
}

impl RecordBatchStream for ProbeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for ProbeStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.polls += 1;
        if self.pending_every_other && self.polls % 2 == 1 {
            if let Some(delay) = self.pending_delay {
                std::thread::sleep(delay);
            }
            // A real child wakes itself when the work completes; doing that here keeps the
            // stream progressing instead of stalling the test on a pending poll.
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }
        let batch = self.batches.next();
        if batch.is_some() {
            self.yields.fetch_add(1, Ordering::SeqCst);
        }
        Poll::Ready(batch.map(Ok))
    }
}

impl Drop for ProbeStream {
    fn drop(&mut self) {
        self.dropped.store(true, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn dictionary_tag_column_works_end_to_end() {
    let chunks = counter_chunks(1, "a", &counter_rows(1.0), &[3, 6, 9]);
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    )
    .with_output_schema(dictionary_output_schema_of(&[VALUE], &["host"]));
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    assert_matches_reference(&batches, &reference, "dictionary tag");

    let batch = &batches[0];
    let host = batch.column_by_name("host").unwrap();
    assert_eq!(host.data_type(), &dictionary_tag_type());
    assert_eq!(
        dictionary_values(host),
        vec![Some("a".to_string()); batch.num_rows()],
        "every row of one series carries the same label value"
    );
    // One series holds one label value, so the dictionary has one entry and the keys are a
    // constant buffer over it instead of one stored string per row.
    let host = host
        .as_any()
        .downcast_ref::<DictionaryArray<UInt32Type>>()
        .unwrap();
    assert_eq!(host.values().len(), 1);
    assert!(host.keys().values().iter().all(|key| *key == 0));
    assert_eq!(host.keys().null_count(), 0);
}

#[tokio::test]
async fn dictionary_tag_column_of_a_missing_tag_is_null() {
    // `rate` drops `__name__`, and the declared tag below is not part of this raw series, so the
    // tag column is entirely null while the row still exists.
    let chunks = counter_chunks(1, "a", &counter_rows(1.0), &[9]);
    let case = Case::new(
        &["host", "az"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    )
    .with_output_schema(dictionary_output_schema_of(&[VALUE], &["host", "az"]));
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    let az = batches[0].column_by_name("az").unwrap();
    assert_eq!(az.null_count(), batches[0].num_rows());
    assert_eq!(dictionary_values(az), vec![None; batches[0].num_rows()]);
}

#[tokio::test]
async fn construction_rejects_an_unsupported_tag_type() {
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    for tag_type in [
        DataType::Int32,
        DataType::LargeUtf8,
        DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
        DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::LargeUtf8)),
    ] {
        let schema = Arc::new(Schema::new(vec![
            Field::new(VALUE, DataType::Float64, true),
            Field::new("host", tag_type.clone(), true),
            Field::new(
                TIME,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
        ]));
        let error = case
            .with_output_schema(schema)
            .fused(memory_child(&[]))
            .err()
            .unwrap();
        assert!(matches!(error, DataFusionError::Plan(_)), "{error}");
        let message = error.to_string();
        assert!(message.contains("host"), "{tag_type:?}: {message}");
        assert!(
            message.contains("must be Utf8 or Dictionary(UInt32, Utf8)"),
            "{tag_type:?}: {message}"
        );
    }
}

/// The Arrow type of a dictionary-encoded tag column.
fn dictionary_tag_type() -> DataType {
    DataType::Dictionary(Box::new(DataType::UInt32), Box::new(DataType::Utf8))
}

#[tokio::test]
async fn a_gap_only_drops_the_null_rows() {
    // One sample lands between two grid points of a one-second window: no grid point of the
    // trimmed interval holds a value for it, while the two series below hold values everywhere.
    let chunks = vec![
        counter_chunks(1, "a", &counter_rows(1.0), &[9])[0].clone(),
        chunk(2, "b", &[(420_000, Some(1.0))], true),
        counter_chunks(3, "c", &counter_rows(2.0), &[9])[0].clone(),
    ];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    );
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);
    // The three series are coalesced into one batch, which is cut by their rows: `b` has a single
    // sample in range, so it contributes one row between the five rows of `a` and of `c`.
    assert_eq!(batches.len(), 1, "{batches:?}");
    let batch = &batches[0];
    let hosts = string_column(batch, "host");
    let mut distinct = hosts.clone();
    distinct.dedup();
    assert_eq!(
        distinct,
        vec![
            Some("a".to_string()),
            Some("b".to_string()),
            Some("c".to_string())
        ],
        "every series with a value keeps its own rows: {hosts:?}"
    );
    assert_eq!(
        hosts
            .iter()
            .filter(|host| **host == Some("b".to_string()))
            .count(),
        1,
        "the gapped series contributes only the grid point that holds a value: {hosts:?}"
    );
    let values = float_column(batch, VALUE);
    assert!(
        values.iter().all(|value| value.is_some()),
        "a null row survived: {values:?}"
    );
    assert_eq!(
        batch.num_rows(),
        values.len(),
        "the timestamp column must be cut like every other column"
    );
    assert_eq!(
        timestamp_column(batch, TIME).len(),
        values.len(),
        "row counts of all columns must agree"
    );
}

#[tokio::test]
async fn an_all_null_series_emits_no_batch() {
    // The evaluator itself never produces an all-null series (its trimmed interval starts and
    // ends with a value), so the conversion rule is exercised on the series value directly: a
    // non-empty grid whose every row is empty is not a batch of nulls but no batch at all, and so
    // contributes no row to whatever the stream coalesces.
    let grid = grid();
    // A grid series is validated against the schema's own grid, so the fixture declares it.
    let schema = Arc::new(SeriesSchema {
        fields: vec![FieldSpec {
            name: Arc::from(VALUE),
        }]
        .into(),
        eval_grid: Some(grid.clone()),
    });
    let range = GridRange::try_new(grid.clone(), 2, 3).unwrap();
    let fields = SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(vec![
        None::<f64>,
        None::<f64>,
        None::<f64>,
    ]))])
    .unwrap();
    let series = Series::try_new(&schema, metric("a"), TimeAxis::Grid(range), fields).unwrap();
    assert_eq!(grid_range(&series).1, 3, "the fixture has a non-empty grid");

    let output_schema = output_schema(&["host"]);
    let tags = ["host".to_string()];
    let classification = classify_output(&output_schema, &schema, &tags, TIME).unwrap();
    let batch = series_to_record_batch(
        &classification,
        &tags,
        &output_schema,
        &series,
        &occupancy(&series).unwrap(),
    )
    .unwrap();
    assert!(
        batch.is_none(),
        "a series without a value must not emit a batch, got {batch:?}"
    );

    // A series with one value in the same grid keeps its own rows, so the drop above is caused
    // by the missing values and not by the fixture.
    let grid_range_value = GridRange::try_new(grid, 2, 3).unwrap();
    let fields = SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(vec![
        Some(1.0),
        None,
        Some(3.0),
    ]))])
    .unwrap();
    let series = Series::try_new(
        &schema,
        metric("a"),
        TimeAxis::Grid(grid_range_value),
        fields,
    )
    .unwrap();
    let batch = series_to_record_batch(
        &classification,
        &tags,
        &output_schema,
        &series,
        &occupancy(&series).unwrap(),
    )
    .unwrap()
    .expect("a series with values must emit a batch");
    assert_eq!(batch.num_rows(), 2, "only the non-null rows are emitted");
}

/// A counter sampled once a minute for `count` minutes, rising `step` per sample.
fn long_counter_rows(count: i64, step: f64) -> Vec<Row> {
    (1..=count)
        .map(|minute| (minute * 60_000, Some(minute as f64 * step)))
        .collect()
}

#[tokio::test]
async fn each_input_partition_runs_on_its_own_stream() {
    // Two partitions, each owning whole series: the shape a `PerSeries`-distributed raw-series
    // scan reports, where no series is ever split across partitions.
    let left = counter_chunks(1, "a", &counter_rows(60.0), &[9]);
    let right = counter_chunks(2, "b", &counter_rows(120.0), &[9]);
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let exec = case
        .fused(partitioned_child(&[&left, &right]))
        .expect("a multi-partition child is legal");
    assert_eq!(
        exec.properties().output_partitioning().partition_count(),
        2,
        "the node must report one partition per input partition"
    );

    let first = collect(run_partition(&exec, 0).await);
    let second = collect(run_partition(&exec, 1).await);
    assert_eq!(first.len(), 1, "{first:?}");
    assert_eq!(second.len(), 1, "{second:?}");

    // Each partition emits exactly the series it owns, so neither sees the other's samples.
    let first_hosts = string_column(&first[0], "host");
    assert_eq!(first_hosts.len(), GRID_LEN as usize);
    assert!(
        first_hosts.iter().all(|host| host.as_deref() == Some("a")),
        "partition 0 must not hold the other partition's series: {first_hosts:?}"
    );
    assert_eq!(
        float_column(&first[0], VALUE),
        vec![Some(1.0); GRID_LEN as usize],
        "series a's counter rises 60 per minute"
    );
    let second_hosts = string_column(&second[0], "host");
    assert_eq!(second_hosts.len(), GRID_LEN as usize);
    assert!(
        second_hosts.iter().all(|host| host.as_deref() == Some("b")),
        "partition 1 must not hold the other partition's series: {second_hosts:?}"
    );
    assert_eq!(
        float_column(&second[0], VALUE),
        vec![Some(2.0); GRID_LEN as usize],
        "series b's counter rises 120 per minute"
    );

    // Both partitions together reproduce the single-partition reference over the same series.
    let reference = case.reference(&[left, right].concat());
    let mut both = first;
    both.extend(second);
    assert_matches_reference(&both, &reference, "two partitions");
}

#[tokio::test]
async fn completed_series_are_coalesced_into_few_batches() {
    // Far more rows than the row target: every series fills a long grid, so the uncoalesced shape
    // would hand downstream one batch per series.
    const SERIES: u8 = 50;
    let grid = Arc::new(
        EvalGrid::try_new(GridSpec::try_new(GRID_START, GRID_STEP, 100).unwrap()).unwrap(),
    );
    let chunks = (0..SERIES)
        .map(|index| {
            let host = format!("h{index}");
            chunk(index + 1, &host, &long_counter_rows(160, 60.0), true)
        })
        .collect::<Vec<_>>();
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    )
    .with_grid(grid);
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);

    let expected_rows = reference
        .iter()
        .map(|(_, batch)| batch.num_rows())
        .sum::<usize>();
    assert!(
        expected_rows > MAX_ROWS_PER_BATCH,
        "the fixture must cross the row target, got {expected_rows} rows"
    );
    let rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
    assert_eq!(rows, expected_rows, "the coalesced rows must be the same");
    // The reference path is the uncoalesced shape: one batch per completed series.
    assert_eq!(
        reference.len(),
        usize::from(SERIES),
        "the fixture must produce one batch per series without coalescing"
    );
    // Every batch but the last one was flushed at the row target, so the number of batches is
    // bounded by the row target and, above all, is far below the number of series.
    for batch in &batches[..batches.len() - 1] {
        assert!(
            batch.num_rows() >= MAX_ROWS_PER_BATCH,
            "a batch below the row target was flushed early: {} rows",
            batch.num_rows()
        );
    }
    assert!(
        batches.len() >= 2,
        "the row target must be reached and flushed before the input ends"
    );
    assert!(
        batches.len() <= expected_rows.div_ceil(MAX_ROWS_PER_BATCH),
        "{SERIES} series over {expected_rows} rows produced {} batches, expected at most one per \
         {MAX_ROWS_PER_BATCH} rows",
        batches.len()
    );
    assert!(
        batches.len() * 4 < usize::from(SERIES),
        "coalescing must produce far fewer batches than series: {} batches for {SERIES} series",
        batches.len()
    );
    // The rows themselves are unchanged by the batching: the same values, in the same order.
    assert_matches_reference(&batches, &reference, "coalesced");
}

#[tokio::test]
async fn the_last_batch_is_flushed_below_the_row_target() {
    // Total rows stay far below the target, so nothing but the end of the input flushes them.
    let chunks = counter_chunks(1, "a", &counter_rows(60.0), &[9]);
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 300_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::Rate,
        vec![],
    );
    let reference = case.reference(&chunks);
    let batches = collect(run(case.fused(memory_child(&chunks)).unwrap()).await);

    let expected_rows = reference
        .iter()
        .map(|(_, batch)| batch.num_rows())
        .sum::<usize>();
    assert!(
        expected_rows < MAX_ROWS_PER_BATCH,
        "the fixture must stay below the row target, got {expected_rows} rows"
    );
    assert_eq!(batches.len(), 1, "the pending rows must be flushed once");
    assert_eq!(batches[0].num_rows(), expected_rows);
    assert_matches_reference(&batches, &reference, "final flush");
}

/// A series whose rows are all kept must not be copied through an identity index.
///
/// `two_fields_convert_both_columns_and_keep_a_half_missing_row` is exactly this shape -- one
/// field has a hole, the other covers every row -- and its result is correct whether or not the
/// copy happens, so comparing values cannot see it. What separates the two is where the time
/// column's values live: built from the grid's shared buffer when nothing is dropped, freshly
/// allocated by `take` when something is. Comparing the pointer catches a regression that no
/// value comparison can.
#[tokio::test]
async fn a_series_that_keeps_every_row_shares_the_grid_instead_of_copying_it() {
    let schema = two_field_schema();
    let chunks = vec![two_field_chunk(
        1,
        &[
            (420_000, Some(1.0), Some(10.0)),
            (480_000, None, Some(20.0)),
            (540_000, Some(3.0), Some(30.0)),
        ],
        true,
    )];
    let case = Case::new(
        &["host"],
        RangeWindow {
            range_ms: 60_000,
            offset_ms: 0,
            anchor_ms: None,
        },
        RangeFunction::SumOverTime,
        vec![],
    )
    .with_series_schema(schema.clone())
    .with_output_schema(output_schema_of(&[VALUE, VALUE2], &["host"]));
    let batches = collect(run(case.fused(memory_child_of(&schema, &chunks)).unwrap()).await);
    // The grid spans 5 points and the first two hold no sample, so the trimmed range -- and the
    // output row count -- is 3. Both fields' rows are kept: the second covers all three.
    assert_eq!(
        batches[0].num_rows(),
        3,
        "the trim keeps 3 rows, and none is dropped"
    );

    let time = batches[0]
        .column_by_name(TIME)
        .expect("the output has a time column")
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .expect("the time column is a timestamp array");
    let grid = case.grid.timestamps();
    // The output starts at the first grid point holding a sample, so "shares the buffer" means
    // equal addresses at the same element offset, not at offset zero.
    let first = grid
        .iter()
        .position(|timestamp| *timestamp == timestamp_column(&batches[0], TIME)[0])
        .expect("the output starts on a grid point");
    // A `take` allocates, so no element offset could bring the two addresses together. A slice
    // of the grid's buffer lands exactly `first` elements in.
    assert_eq!(
        time.values().slice(0, 0).as_ptr(),
        grid.slice(first, 0).as_ptr(),
        "every row was kept, so the time column must be a slice of the grid's buffer; a \
         different address means it was copied through an identity index"
    );
}
