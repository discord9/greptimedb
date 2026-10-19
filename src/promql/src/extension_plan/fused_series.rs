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

//! One physical node that evaluates one range function and then 0..N float map kernels over raw
//! series, converting every completed evaluated series directly into output rows.
//!
//! There is no `SeriesFrame` pack/unpack and no intermediate `RecordBatch` between the evaluation
//! and the map kernels: both stages run over typed [`Series`] values inside this node.
//!
//! The node runs one evaluator per input partition: its partition count is its child's, and
//! `execute(partition, ..)` evaluates exactly the child stream of that partition with its own
//! label-set state. A `PerSeries`-distributed raw-series scan hands every partition whole series --
//! never one series split across partitions -- so evaluating each partition on its own is correct
//! and needs no final merge for the range function. Cross-partition state is deliberately absent
//! for the same reason: two partitions own disjoint series, so a final label set can only collide
//! with one inside a single partition.
//!
//! The completed series of one stream do not each become a batch: the node coalesces their rows
//! into output batches of up to `MAX_ROWS_PER_BATCH` rows, because one batch per series would hand
//! downstream hundreds of tiny batches per query.
//!
//! Phase 1 scope: the child produces raw frames with explicit labels that are not interleaved, and
//! the output schema must classify into exactly one timestamp column, one
//! `Float64` column per series field, and one string column per declared tag. A tag column is
//! either `Utf8` or `Dictionary(UInt32, Utf8)`, because the PromQL plan reports dictionary-encoded
//! labels; every row of one series carries the same single label value, so the dictionary holds
//! one entry and the keys are a constant buffer over it. Anything outside this scope is rejected
//! at construction instead of silently falling back. A field column must be declared nullable: a
//! grid point inside the trimmed interval can be missing. A tag column may be either, because the
//! range functions and the map kernels drop `__name__` and a tag can therefore be absent: a
//! nullable column reports an absent label as a NULL, and a non-nullable one as the empty label
//! value Prometheus reads for a label the series does not carry.
//!
//! Rows follow the ordinary PromQL plan's semantics: that plan keeps `<value> IS NOT NULL` above
//! its projection, so a grid point at which the series has no value never reaches the result. This
//! node therefore omits every row whose value columns are all null, and a series without any value
//! contributes no row at all instead of a batch of null rows.
//!
//! This node computes floats only, and admitting raw samples is the caller's responsibility:
//! validating the declared frame schema cannot prove that the samples are float, so a histogram
//! or mixed sample passes construction and surfaces at run time as [`RangeEvaluator`]'s (or a map
//! kernel's) `NotImplemented`. That runtime type check is kept rather than replaced by a schema
//! check that could not prove the same thing.
//!
//! Duplicate label sets follow Prometheus' range-query rule. `rate` and the map kernels drop
//! `__name__`, so two distinct raw series can reach the same final label set; if both have a
//! sample at the same evaluation timestamp that is an error (`vector cannot contain metrics with
//! the same labelset`), while series whose occupied grid points are disjoint are legal and
//! Prometheus merges them into one output series. This node emits such series as separate rows
//! and defers that merge to matrix assembly, so no sample is dropped. The occupancy state that
//! detects a collision belongs to one stream, so it is per partition.
//!
//! Memory: the state retained across polls that grows with the output is the per-stream label-set
//! occupancy map and the buffered rows of the batch being filled, and the bytes of both are
//! reserved on the task's memory pool. The reservation of the buffered rows is released when they
//! are handed downstream. The range-window samples [`RangeEvaluator`] retains across chunks and the
//! batches already handed downstream are deliberately not reserved here.
//!
//! Partial mode: with a [`PartialSum`] the node stops converting a completed series into rows. It
//! folds the series' points straight into the replaced partial aggregate's own `GroupsAccumulator`
//! and emits that accumulator's state once per partition, so the final aggregate above it consumes
//! exactly what DataFusion's partial `AggregateExec` would have written. The label-set collision
//! check still runs first and unchanged, on the full result label set: a `SUM` above the node must
//! not swallow the error a bare evaluated query reports. Slots are keyed by `(label group,
//! absolute grid index)` — `GridRange::start()` plus the row offset — never by a local row number,
//! because each series trims its own interval. What that state costs is reserved on the same pool
//! as it is taken: the slot column of each label group grows only to the highest absolute index the
//! group has occupied, the slot table and the label-group table are charged in fixed steps, and the
//! accumulator's own `size()` is charged as it grows. All of it is released together with the rest
//! of the stream's reservation.
//!
//! The child's `is_last` is the only completion signal. This node neither recomputes finality
//! across inputs nor emits a series it has not seen finalized, so a child that merges regions must
//! recompute `is_last` globally before handing its frames over; a region-local flag would finalize
//! a series early.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt::{self, Formatter};
use std::mem::size_of;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use common_query::series_frame::unpack_raw_series;
use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, Float64Array, StringArray, StringDictionaryBuilder,
    TimestampMillisecondArray, UInt32Array,
};
use datafusion::arrow::compute::{concat_batches, take};
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit, UInt32Type};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::logical_expr::{EmitTo, GroupsAccumulator};
use datafusion::physical_expr::aggregate::AggregateFunctionExpr;
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet, Time,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use datatypes::series::samples::{DenseSamplesRef, SampleRef, Samples, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, TimeAxis};
use datatypes::series::{MetricName, SeriesOrdering};
use futures::Stream;
use store_api::region_engine::{RawLabelSource, SendableRawSeriesStream};

use crate::map::{MathOp, map_math};
use crate::range_eval::{BatchTimers, RangeEvaluator, RangeFunction, RangeWindow};
use crate::source::materialize_raw_labels;

#[cfg(test)]
mod tests;

/// Where one output column of this node comes from.
#[derive(Clone, Copy, Debug)]
enum OutputColumn {
    /// The evaluation grid timestamps of the current interval.
    Time,
    /// Series field by index in the input series schema.
    Field(usize),
    /// Declared logical tag by index in the caller-supplied tag list, and its encoding.
    Tag(usize, TagEncoding),
}

/// How one output tag column encodes the single label value of a series.
#[derive(Clone, Copy, Debug)]
struct TagEncoding {
    dictionary: bool,
    /// A non-nullable column cannot carry an absent label as a NULL; it carries Prometheus'
    /// empty label value instead.
    nullable: bool,
}

/// One partial `SUM` computed over the series this node evaluated itself.
///
/// Created by the planner from the partial `AggregateExec` this node replaces, so the state this
/// node emits is DataFusion's own and the final aggregate above it consumes it unchanged. With a
/// `PartialSum` the node stops converting each completed series into output rows and instead folds
/// its points straight into the aggregate's own [`GroupsAccumulator`], then emits that
/// accumulator's state once per partition.
#[derive(Clone, Debug)]
pub struct PartialSum {
    /// Position of the value column in the node's evaluated classification; must be a `Field`.
    value_column: usize,
    /// Positions of the group-key columns in the evaluated classification, in the aggregate's own
    /// order. Each must be `Time` or `Tag`.
    group_columns: Vec<usize>,
    /// The `SUM` expression of the replaced partial aggregate.
    aggregate: Arc<AggregateFunctionExpr>,
    /// That aggregate's own output schema: group columns then state columns.
    output_schema: SchemaRef,
}

impl PartialSum {
    /// Creates the partial-aggregate binding, rejecting a state layout this node cannot emit.
    ///
    /// Everything that depends on the *evaluation* contract (which classification position is the
    /// summed field, which are time or tag columns) is checked by `FusedSeriesExec::try_new`,
    /// because only that type owns the classification.
    pub fn try_new(
        value_column: usize,
        group_columns: Vec<usize>,
        aggregate: Arc<AggregateFunctionExpr>,
        output_schema: SchemaRef,
    ) -> Result<Self> {
        // The node hands the accumulator one array: the summed field's column.
        if aggregate.expressions().len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec partial aggregate {} must aggregate exactly one column, got {}",
                aggregate.name(),
                aggregate.expressions().len()
            )));
        }
        let state_fields = aggregate.state_fields()?;
        let expected = group_columns.len() + state_fields.len();
        if output_schema.fields().len() != expected {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec partial output schema has {} columns, expected {} group columns \
                 plus {} state columns",
                output_schema.fields().len(),
                group_columns.len(),
                state_fields.len()
            )));
        }
        for (position, state) in state_fields.iter().enumerate() {
            let column = group_columns.len() + position;
            let field = output_schema.field(column);
            if field.name() != state.name()
                || field.data_type() != state.data_type()
                || field.is_nullable() != state.is_nullable()
            {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeriesExec partial output column {column} is {field:?}, but state field \
                     {position} of {} is {state:?}",
                    aggregate.name()
                )));
            }
            // The node computes floats only, so a partial aggregate whose state is not a float is
            // not a `SUM` over one evaluated field and cannot be collapsed into this node.
            if state.data_type() != &DataType::Float64 {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeriesExec partial aggregate {} must have Float64 state, got {:?}",
                    aggregate.name(),
                    state.data_type()
                )));
            }
        }
        Ok(Self {
            value_column,
            group_columns,
            aggregate,
            output_schema,
        })
    }
}

/// Time this node spends synchronously polling its input for one chunk.
///
/// The block ends before the poll returns `Pending`, so the value never includes time the
/// child spent waiting on I/O or on a wakeup. It is block elapsed time, not exclusive CPU.
const METRIC_INPUT_POLL: &str = "fused_input_poll_time";
/// Time spent in `RangeEvaluator::push` plus the map kernels it feeds.
const METRIC_EVALUATE: &str = "fused_evaluate_time";
/// Time spent turning one evaluated series into a `RecordBatch` (time, value and tag columns).
const METRIC_EMIT: &str = "fused_emit_time";
/// Time spent concatenating buffered batches for downstream.
const METRIC_FLUSH: &str = "fused_flush_time";
/// Time the evaluator spent filling a batch's windows, summed over every series.
const METRIC_WINDOW_PREP: &str = "fused_window_prep_time";
/// Time the evaluator spent in the range function's batch kernel.
const METRIC_KERNEL: &str = "fused_kernel_time";
/// Time the evaluator spent appending a flushed batch's columns to its output.
const METRIC_COLUMNS: &str = "fused_columns_time";
/// Raw chunks received from the child.
const METRIC_INPUT_CHUNKS: &str = "fused_input_chunks";
/// Series the evaluator completed.
const METRIC_SERIES_COMPLETED: &str = "fused_series_completed";
/// Series that produced at least one output: at least one kept row in evaluated mode, and at
/// least one point fed to the partial accumulator in partial mode.
const METRIC_SERIES_EMITTED: &str = "fused_series_emitted";
/// Time spent folding one completed series into the partial accumulator: resolving its label
/// group, allocating slots and building the group indices handed to `update_batch`.
const METRIC_PARTIAL_UPDATE: &str = "fused_partial_update_time";
/// Time spent reading the accumulator's state at end of partition and building the state batch.
const METRIC_PARTIAL_STATE: &str = "fused_partial_state_time";
/// `update_batch` calls, one per series that holds a point.
const METRIC_PARTIAL_UPDATES: &str = "fused_partial_updates";
/// Grid points fed to the accumulator.
const METRIC_PARTIAL_POINTS: &str = "fused_partial_points";
/// Accumulator slots at end of partition, i.e. distinct `(label group, grid index)` pairs.
const METRIC_PARTIAL_SLOTS: &str = "fused_partial_slots";
/// State rows emitted at end of partition.
const METRIC_PARTIAL_STATE_ROWS: &str = "fused_partial_state_rows";

/// Slot id of a `(label group, grid index)` pair that was never seen.
const NO_SLOT: u32 = u32::MAX;

/// Evaluates one range function followed by 0..N float map kernels per raw series.
pub struct FusedSeriesExec {
    input: Arc<dyn ExecutionPlan>,
    /// Raw (no evaluation grid) schema of the input and of the evaluated series.
    series_schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
    ordering: SeriesOrdering,
    grid: Arc<EvalGrid>,
    window: RangeWindow,
    function: RangeFunction,
    maps: Vec<MathOp>,
    output_schema: SchemaRef,
    tags: Vec<String>,
    time_column: String,
    /// One entry per output column, in output column order.
    classification: Arc<[OutputColumn]>,
    /// When set, the node folds its evaluated series into this partial aggregate instead of
    /// converting them into rows, and reports that aggregate's schema.
    partial: Option<PartialSum>,
    properties: Arc<PlanProperties>,
    metric: ExecutionPlanMetricsSet,
}

impl FusedSeriesExec {
    /// Creates the node, rejecting everything the phase 1 contract cannot execute.
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        series_schema: Arc<SeriesSchema>,
        label_source: RawLabelSource,
        ordering: SeriesOrdering,
        grid: Arc<EvalGrid>,
        window: RangeWindow,
        function: RangeFunction,
        maps: Vec<MathOp>,
        output_schema: SchemaRef,
        tags: Vec<String>,
        time_column: String,
        partial: Option<PartialSum>,
    ) -> Result<Self> {
        let partition_count = input.properties().partitioning.partition_count();
        if partition_count == 0 {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec requires an input with at least one partition".into(),
            ));
        }
        if series_schema.eval_grid.is_some() {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec input must be a raw series schema without an evaluation grid"
                    .into(),
            ));
        }
        // Both bindings are accepted: explicit chunks already carry complete labels, and
        // primary-key chunks are decoded through `materialize_raw_labels` in `execute`.
        if ordering == SeriesOrdering::Interleaved {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec does not support interleaved raw series ordering".into(),
            ));
        }
        // Surfaces invalid windows, smoothing/trend factors and predict_linear overflow here
        // rather than on the first chunk.
        drop(RangeEvaluator::try_new(
            &series_schema,
            grid.clone(),
            window,
            function,
        )?);
        validate_raw_frame_schema(&input, &series_schema, &label_source, ordering)?;
        let classification = classify_output(&output_schema, &series_schema, &tags, &time_column)?;
        if let Some(partial) = &partial {
            validate_partial(partial, &classification, &series_schema)?;
        }
        // In partial mode the node reports the replaced aggregate's own schema and finishes: one
        // state batch per partition, at its end. The evaluated properties are never reused,
        // because no evaluated row ever reaches the consumer.
        let reporting_schema = match &partial {
            Some(partial) => partial.output_schema.clone(),
            None => output_schema.clone(),
        };
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(reporting_schema),
            // One partition per input partition: `execute` runs the child stream of the partition
            // it is given, and each stream owns its own evaluator and label-set state.
            Partitioning::UnknownPartitioning(partition_count),
            match &partial {
                Some(_) => EmissionType::Final,
                None => EmissionType::Incremental,
            },
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            series_schema,
            label_source,
            ordering,
            grid,
            window,
            function,
            maps,
            output_schema,
            tags,
            time_column,
            classification: classification.into(),
            partial,
            properties,
            metric: ExecutionPlanMetricsSet::new(),
        })
    }

    /// The schema this node reports downstream: the partial aggregate's own schema in partial
    /// mode, the evaluated row schema otherwise.
    fn reporting_schema(&self) -> SchemaRef {
        match &self.partial {
            Some(partial) => partial.output_schema.clone(),
            None => self.output_schema.clone(),
        }
    }

    fn name() -> &'static str {
        "FusedSeriesExec"
    }
}

impl std::fmt::Debug for FusedSeriesExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("FusedSeriesExec").finish()
    }
}

impl DisplayAs for FusedSeriesExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter<'_>) -> fmt::Result {
        let maps: Vec<_> = self.maps.iter().map(|op| describe_map(*op)).collect();
        // The marker names the aggregate function, so `EXPLAIN` alone proves the mode without a
        // reader having to look for logs.
        let partial = match &self.partial {
            Some(partial) => format!(", partial={}", partial.aggregate.fun().name()),
            None => String::new(),
        };
        write!(
            f,
            "FusedSeriesExec: eval={}, range={}ms, offset={}ms, maps=[{}]{}",
            describe_function(self.function),
            self.window.range_ms,
            self.window.offset_ms,
            maps.join(", "),
            partial
        )
    }
}

impl ExecutionPlan for FusedSeriesExec {
    fn name(&self) -> &str {
        Self::name()
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metric.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [child] = <[Arc<dyn ExecutionPlan>; 1]>::try_from(children).map_err(|children| {
            DataFusionError::Plan(format!(
                "FusedSeriesExec requires exactly one child, got {}",
                children.len()
            ))
        })?;
        if child.schema() != self.input.schema() {
            return Err(DataFusionError::Plan(
                "FusedSeriesExec child must keep its declared raw series frame schema".into(),
            ));
        }
        let exec = Self::try_new(
            child,
            self.series_schema.clone(),
            self.label_source.clone(),
            self.ordering,
            self.grid.clone(),
            self.window,
            self.function,
            self.maps.clone(),
            self.output_schema.clone(),
            self.tags.clone(),
            self.time_column.clone(),
            self.partial.clone(),
        )?;
        Ok(Arc::new(exec))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let partition_count = self.properties.output_partitioning().partition_count();
        if partition >= partition_count {
            return Err(DataFusionError::Execution(format!(
                "FusedSeriesExec has {partition_count} partitions, got partition {partition}"
            )));
        }
        let memory_pool = Arc::clone(&context.runtime_env().memory_pool);
        let frame = self.input.execute(partition, context)?;
        let input = unpack_raw_series(
            frame,
            self.series_schema.clone(),
            self.label_source.clone(),
            self.ordering,
        )?;
        // `RangeEvaluator` needs complete labels. Explicit sources already supply them; a
        // scan-bound primary-key source is decoded here instead of in every consumer.
        let input = materialize_raw_labels(input).map_err(|error| {
            DataFusionError::Execution(format!("cannot materialize raw series labels: {error}"))
        })?;
        // The kernels are `FnMut` and not cloneable, so every stream needs its own evaluator.
        let evaluator = RangeEvaluator::try_new(
            &self.series_schema,
            self.grid.clone(),
            self.window,
            self.function,
        )?;
        // Same reason: the accumulator carries the running sums, so it too is per stream. State
        // never crosses a partition, because partitions own disjoint series.
        let partial = match &self.partial {
            Some(partial) => Some(PartialSumStream::try_new(
                partial.clone(),
                partial.aggregate.create_groups_accumulator()?,
                &self.classification,
                self.tags.clone().into(),
                self.grid.clone(),
            )?),
            None => None,
        };
        Ok(Box::pin(FusedSeriesStream {
            baseline: BaselineMetrics::new(&self.metric, partition),
            timers: FusedTimers::register(&self.metric, partition, self.partial.is_some()),
            input,
            evaluator: Some(evaluator),
            maps: self.maps.clone(),
            tags: self.tags.clone().into(),
            output_schema: self.reporting_schema(),
            classification: self.classification.clone(),
            partial,
            // Fresh per stream: two streams must never share duplicate-label-set state.
            seen: HashMap::new(),
            pending: Vec::new(),
            pending_rows: 0,
            pending_bytes: 0,
            reservation: MemoryConsumer::new("FusedSeriesStream").register(&memory_pool),
            finished: false,
        }))
    }
}

/// Checks the parts of the partial binding that depend on the evaluation contract.
///
/// The classification is the only place that knows which output column is the summed field and
/// which are the time and tag columns, so these rejections live here rather than in
/// [`PartialSum::try_new`].
fn validate_partial(
    partial: &PartialSum,
    classification: &[OutputColumn],
    series_schema: &SeriesSchema,
) -> Result<()> {
    let value_field = match classification.get(partial.value_column) {
        Some(OutputColumn::Field(index)) => *index,
        Some(other) => {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec partial sum must aggregate a series field, but output column {} \
                 is {other:?}",
                partial.value_column
            )));
        }
        None => {
            return Err(DataFusionError::Plan(format!(
                "FusedSeriesExec partial sum aggregates output column {}, but the evaluated \
                 schema has {} columns",
                partial.value_column,
                classification.len()
            )));
        }
    };
    // Every series field is a float field (`FieldSpec` carries a name only and the range
    // functions produce floats), so locating the summed field is the whole check; the state type
    // check in `PartialSum::try_new` proves the aggregate over it is a float `SUM`.
    if series_schema.fields.get(value_field).is_none() {
        return Err(DataFusionError::Plan(format!(
            "FusedSeriesExec partial sum aggregates field {value_field}, but the series schema has \
             {} fields",
            series_schema.fields.len()
        )));
    }
    for column in &partial.group_columns {
        match classification.get(*column) {
            Some(OutputColumn::Time) | Some(OutputColumn::Tag(_, _)) => {}
            Some(OutputColumn::Field(index)) => {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeriesExec partial sum group column {column} is the field \
                     {index:?}, but a group key must be the time column or a tag column"
                )));
            }
            None => {
                return Err(DataFusionError::Plan(format!(
                    "FusedSeriesExec partial sum groups by output column {column}, but the \
                     evaluated schema has {} columns",
                    classification.len()
                )));
            }
        }
    }
    Ok(())
}

/// Checks that the child's declared schema is a legal raw frame for this binding.
///
/// The check runs at construction time by feeding an empty stream with the declared schema
/// through the same decoder execution uses, and discarding the resulting stream.
fn validate_raw_frame_schema(
    input: &Arc<dyn ExecutionPlan>,
    series_schema: &Arc<SeriesSchema>,
    label_source: &RawLabelSource,
    ordering: SeriesOrdering,
) -> Result<()> {
    let empty = futures::stream::empty::<Result<RecordBatch>>();
    let frame = Box::pin(RecordBatchStreamAdapter::new(input.schema(), empty));
    drop(unpack_raw_series(
        frame,
        series_schema.clone(),
        label_source.clone(),
        ordering,
    )?);
    Ok(())
}

/// Classifies every output column, rejecting anything outside the phase 1 output contract.
fn classify_output(
    output_schema: &SchemaRef,
    series_schema: &SeriesSchema,
    tags: &[String],
    time_column: &str,
) -> Result<Vec<OutputColumn>> {
    for (index, tag) in tags.iter().enumerate() {
        if tags[..index].contains(tag) {
            return Err(DataFusionError::Plan(format!(
                "duplicate tag column {tag:?} in FusedSeriesExec output schema"
            )));
        }
        if tag == time_column {
            return Err(DataFusionError::Plan(format!(
                "tag column {tag:?} of FusedSeriesExec cannot be the time column"
            )));
        }
        if series_schema
            .fields
            .iter()
            .any(|field| field.name.as_ref() == tag)
        {
            return Err(DataFusionError::Plan(format!(
                "tag column {tag:?} of FusedSeriesExec clashes with a series field"
            )));
        }
    }

    let mut time_seen = false;
    let mut field_seen = vec![false; series_schema.fields.len()];
    let mut tag_seen = vec![false; tags.len()];
    let mut classification = Vec::with_capacity(output_schema.fields().len());
    for field in output_schema.fields() {
        let name = field.name();
        if name == time_column {
            if std::mem::replace(&mut time_seen, true) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate time column {name:?} in FusedSeriesExec output schema"
                )));
            }
            match field.data_type() {
                DataType::Timestamp(TimeUnit::Millisecond, None) => {}
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "time column {name:?} of FusedSeriesExec must be Timestamp(Millisecond, \
                         None), got {other:?}"
                    )));
                }
            }
            if field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "time column {name:?} of FusedSeriesExec must not be nullable"
                )));
            }
            classification.push(OutputColumn::Time);
            continue;
        }
        if let Some(index) = series_schema
            .fields
            .iter()
            .position(|field| field.name.as_ref() == name)
        {
            if std::mem::replace(&mut field_seen[index], true) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate field column {name:?} in FusedSeriesExec output schema"
                )));
            }
            if field.data_type() != &DataType::Float64 {
                return Err(DataFusionError::Plan(format!(
                    "field column {name:?} of FusedSeriesExec must be Float64, got {:?}",
                    field.data_type()
                )));
            }
            // A grid point inside the trimmed interval can have no value in this field.
            if !field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "field column {name:?} of FusedSeriesExec must be nullable, because a grid \
                     point can be missing"
                )));
            }
            classification.push(OutputColumn::Field(index));
            continue;
        }
        if let Some(index) = tags.iter().position(|tag| tag == name) {
            if std::mem::replace(&mut tag_seen[index], true) {
                return Err(DataFusionError::Plan(format!(
                    "duplicate tag column {name:?} in FusedSeriesExec output schema"
                )));
            }
            let dictionary = match field.data_type() {
                DataType::Utf8 => false,
                DataType::Dictionary(key, value)
                    if **key == DataType::UInt32 && **value == DataType::Utf8 =>
                {
                    true
                }
                other => {
                    return Err(DataFusionError::Plan(format!(
                        "tag column {name:?} of FusedSeriesExec must be Utf8 or \
                         Dictionary(UInt32, Utf8), got {other:?}"
                    )));
                }
            };
            // `rate` and the map kernels drop `__name__`, so a tag can be absent: a nullable
            // column says so with a NULL, and a non-nullable one with the empty label value
            // Prometheus reads for a label the series does not carry.
            classification.push(OutputColumn::Tag(
                index,
                TagEncoding {
                    dictionary,
                    nullable: field.is_nullable(),
                },
            ));
            continue;
        }
        // A PromQL plan names a range-function value column after the call that produced it
        // (`prom_rate(timestamp_range,field_0,timestamp,Int64(300000))`), not after the raw field
        // it reads, so a `Float64` column that names no field still carries exactly one field: the
        // next unclaimed one, in the order both were declared. Columns that name a field are
        // matched by that name above, so only genuinely unnamed value columns reach this branch.
        if field.data_type() == &DataType::Float64
            && let Some(index) = field_seen.iter().position(|seen| !seen)
        {
            // A grid point inside the trimmed interval can have no value in this field.
            if !field.is_nullable() {
                return Err(DataFusionError::Plan(format!(
                    "field column {name:?} of FusedSeriesExec must be nullable, because a grid \
                     point can be missing"
                )));
            }
            field_seen[index] = true;
            classification.push(OutputColumn::Field(index));
            continue;
        }
        return Err(DataFusionError::Plan(format!(
            "unknown column {name:?} in FusedSeriesExec output schema"
        )));
    }

    if !time_seen {
        return Err(DataFusionError::Plan(format!(
            "missing time column {time_column:?} in FusedSeriesExec output schema"
        )));
    }
    for (index, seen) in field_seen.iter().enumerate() {
        if !seen {
            return Err(DataFusionError::Plan(format!(
                "missing field column {:?} in FusedSeriesExec output schema",
                series_schema.fields[index].name
            )));
        }
    }
    for (index, seen) in tag_seen.iter().enumerate() {
        if !seen {
            return Err(DataFusionError::Plan(format!(
                "missing tag column {:?} in FusedSeriesExec output schema",
                tags[index]
            )));
        }
    }
    Ok(classification)
}

/// Converts one evaluated series into one batch, or into nothing at all.
///
/// The batch holds only this series' rows; the stream buffers it and coalesces several series into
/// one output batch. The time column is the trimmed absolute interval of the series, missing
/// samples are nulls and ordinary NaN stays a value. A row whose every value column is null is not
/// a point of the series, so it is omitted, exactly like the `<value> IS NOT NULL` filter the
/// ordinary PromQL plan keeps above its projection; a series without any value therefore
/// contributes no row at all.
fn series_to_record_batch(
    classification: &[OutputColumn],
    tags: &[String],
    output_schema: &SchemaRef,
    series: &Series,
    occupancy: &Occupancy,
) -> Result<Option<RecordBatch>> {
    let TimeAxis::Grid(range) = &series.time else {
        return Err(DataFusionError::Execution(
            "FusedSeriesExec requires evaluated series on a time grid".into(),
        ));
    };
    let rows = range.len();
    if occupancy.offsets.is_empty() && !occupancy.dense {
        return Ok(None);
    }
    // Every grid point holds a value in the common case, so the copy below is skipped entirely.
    let indices = occupancy.indices();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(classification.len());
    for (position, column) in classification.iter().enumerate() {
        let column_is_tag = matches!(column, OutputColumn::Tag(_, _));
        let values: ArrayRef = match column {
            OutputColumn::Time => {
                Arc::new(TimestampMillisecondArray::new(range.timestamps(), None))
            }
            OutputColumn::Field(index) => {
                let samples = series.fields.get(*index).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "evaluated series has no field at index {index}"
                    ))
                })?;
                if samples.len() != rows {
                    return Err(DataFusionError::Execution(format!(
                        "evaluated series field {index} has {} rows, time axis has {rows}",
                        samples.len()
                    )));
                }
                field_column(samples)?
            }
            OutputColumn::Tag(index, encoding) => {
                // A series' tag is the same on every row, so a `take` would only rebuild the same
                // constant at the shorter length. Building it once at the kept length skips both
                // the full-length allocation and the gather.
                let tag_rows = match &indices {
                    Some(indices) => indices.len(),
                    None => rows,
                };
                tag_column(series, tags, *index, *encoding, tag_rows, position)?
            }
        };
        columns.push(match (&indices, column_is_tag) {
            // Tags were already built at the kept length.
            (Some(_), true) => values,
            (Some(indices), false) => take(values.as_ref(), indices.as_ref(), None)?,
            (None, _) => values,
        });
    }
    Ok(Some(RecordBatch::try_new(output_schema.clone(), columns)?))
}

/// Converts one series' single label value into the constant tag column of every kept row.
///
/// `__name__` resolves to the metric name, every other tag through the label set, so a tag can be
/// absent; an absent tag is a null column, or the empty label value when the declared column is
/// not nullable. A dictionary column holds the one value in its dictionary and a constant key
/// buffer over it instead of one value per row.
fn tag_column(
    series: &Series,
    tags: &[String],
    index: usize,
    encoding: TagEncoding,
    rows: usize,
    position: usize,
) -> Result<ArrayRef> {
    let value = tag_value(series, tags, index, encoding, position)?;
    if !encoding.dictionary {
        // `Option<&str>` is `Copy`, so this repeats the reference: one allocation for the column,
        // none per row.
        return Ok(Arc::new(StringArray::from(
            std::iter::repeat_n(value, rows).collect::<Vec<_>>(),
        )));
    }
    let dictionary: ArrayRef = Arc::new(StringArray::from(vec![value.unwrap_or_default()]));
    let keys = match value {
        Some(_) => UInt32Array::from(vec![0_u32; rows]),
        None => UInt32Array::from(vec![None::<u32>; rows]),
    };
    Ok(Arc::new(DictionaryArray::<UInt32Type>::try_new(
        keys, dictionary,
    )?))
}

/// The single value one series' tag column carries, before it is repeated over the rows.
///
/// Borrowed from the series' own labels: an evaluated tag column repeats this value once per row,
/// so returning an owned `String` would make every row of every series allocate for a value that
/// never changes across them. Only a new label group in partial mode has to keep a copy.
///
/// Both the evaluated rows and the partial state's group columns print the same value, so this is
/// the one place the absent-label rule lives: a NULL where the column allows one, and the empty
/// label value where it does not, which is how Prometheus reads a label the series does not carry.
fn tag_value<'a>(
    series: &'a Series,
    tags: &[String],
    index: usize,
    encoding: TagEncoding,
    position: usize,
) -> Result<Option<&'a str>> {
    let name = tags.get(index).ok_or_else(|| {
        DataFusionError::Execution(format!(
            "output column {position} declares unavailable tag {index}"
        ))
    })?;
    Ok(match (series.metric.get(name), encoding.nullable) {
        (value, true) => value,
        (value, false) => Some(value.unwrap_or_default()),
    })
}

/// Converts one evaluated series field into one `Float64` column.
///
/// Dense float samples are reused as they are: the array already carries exactly the right nulls
/// and keeps NaN a value, so copying it through `Samples::iter()` would only cost an allocation
/// and a re-encode. Chunked samples go through the iterator, and histograms have no float column.
fn field_column(samples: &Samples) -> Result<ArrayRef> {
    match samples.dense() {
        Some(DenseSamplesRef::Float(values)) => Ok(Arc::new(values.clone())),
        Some(DenseSamplesRef::Histogram(_)) | Some(DenseSamplesRef::Mixed(_)) => {
            Err(DataFusionError::NotImplemented(
                "FusedSeriesExec cannot output histogram samples".into(),
            ))
        }
        None => {
            let values = samples
                .iter()
                .map(|sample| match sample {
                    SampleRef::Float(value) => Ok(Some(value)),
                    SampleRef::Missing => Ok(None),
                    SampleRef::Histogram(_) => Err(DataFusionError::NotImplemented(
                        "FusedSeriesExec cannot output histogram samples".into(),
                    )),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(Float64Array::from(values)))
        }
    }
}

/// The PromQL name of a range function, for `EXPLAIN`.
fn describe_function(function: RangeFunction) -> String {
    match function {
        RangeFunction::Rate => "rate".to_string(),
        RangeFunction::Increase => "increase".to_string(),
        RangeFunction::Delta => "delta".to_string(),
        RangeFunction::Irate => "irate".to_string(),
        RangeFunction::Idelta => "idelta".to_string(),
        RangeFunction::AvgOverTime => "avg_over_time".to_string(),
        RangeFunction::MinOverTime => "min_over_time".to_string(),
        RangeFunction::MaxOverTime => "max_over_time".to_string(),
        RangeFunction::SumOverTime => "sum_over_time".to_string(),
        RangeFunction::CountOverTime => "count_over_time".to_string(),
        RangeFunction::LastOverTime => "last_over_time".to_string(),
        RangeFunction::PresentOverTime => "present_over_time".to_string(),
        RangeFunction::StddevOverTime => "stddev_over_time".to_string(),
        RangeFunction::StdvarOverTime => "stdvar_over_time".to_string(),
        RangeFunction::QuantileOverTime(quantile) => {
            format!("quantile_over_time({quantile})")
        }
        RangeFunction::Resets => "resets".to_string(),
        RangeFunction::Changes => "changes".to_string(),
        RangeFunction::Deriv => "deriv".to_string(),
        RangeFunction::PredictLinear(duration) => format!("predict_linear({duration})"),
        RangeFunction::DoubleExponentialSmoothing { smoothing, trend } => {
            format!("double_exponential_smoothing({smoothing}, {trend})")
        }
        RangeFunction::FirstOverTime => "first_over_time".to_string(),
        RangeFunction::MadOverTime => "mad_over_time".to_string(),
        RangeFunction::TsOfFirstOverTime => "ts_of_first_over_time".to_string(),
        RangeFunction::TsOfLastOverTime => "ts_of_last_over_time".to_string(),
        RangeFunction::TsOfMinOverTime => "ts_of_min_over_time".to_string(),
        RangeFunction::TsOfMaxOverTime => "ts_of_max_over_time".to_string(),
    }
}

/// The PromQL name of a float map kernel, for `EXPLAIN`.
fn describe_map(op: MathOp) -> String {
    match op {
        MathOp::Abs => "abs".to_string(),
        MathOp::Ceil => "ceil".to_string(),
        MathOp::Floor => "floor".to_string(),
        MathOp::Sqrt => "sqrt".to_string(),
        MathOp::Round { nearest } => format!("round({nearest})"),
    }
}

/// Applies every map kernel in order and returns the final series.
///
/// The kernels work on typed [`Series`]; the duplicate-label-set check and the conversion into a
/// record batch both need the result of this stage, so no batch is built in between.
fn apply_maps(evaluator: &RangeEvaluator, maps: &[MathOp], series: Series) -> Result<Series> {
    let mut series = series;
    for op in maps {
        series = map_math(evaluator.output_schema(), &series, *op)?;
    }
    Ok(series)
}

/// Maximum number of raw chunks one `poll_next` call consumes before yielding.
///
/// A child that is always ready can deliver many short-lived series whose evaluation yields no
/// output at all, which would let a single poll monopolize the executor thread;
/// `unpack_raw_series`'s yield only covers consecutive empty record batches, not this case. The
/// budget is per poll, so a poll that returns a batch starts the next one fresh.
const MAX_CHUNKS_PER_POLL: usize = 64;

/// Rows the output batches of one stream are filled to before they are handed downstream.
///
/// One completed series produces at most one grid of rows, so without buffering a query over `n`
/// series hands downstream `n` tiny batches and pays the per-batch overhead `n` times. A few
/// thousand rows amortizes that overhead while keeping one buffered batch small enough to hold.
const MAX_ROWS_PER_BATCH: usize = 4_096;

/// A canonical, collision-free encoding of a final label set.
///
/// [`MetricName`] implements `PartialEq + Eq` but not `Hash`, and it is a dependency-owned type,
/// so the duplicate-label-set state keys on these bytes instead of on the label set itself. Every
/// component carries a length prefix: label names and values are arbitrary UTF-8, so a separator
/// byte could occur inside a component and let two different label sets encode to the same bytes.
/// [`MetricName`] keeps its tags sorted by name, so one label set has exactly one encoding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct LabelsKey(Vec<u8>);

impl LabelsKey {
    /// Encodes the metric name, marked for presence, and then every label, name before value.
    fn new(metric: &MetricName) -> Self {
        let mut bytes = Vec::new();
        match metric.name() {
            // The marker keeps an absent metric name distinct from an empty one.
            Some(name) => {
                bytes.push(1);
                push_component(&mut bytes, name);
            }
            None => bytes.push(0),
        }
        for tag in metric.tags() {
            push_component(&mut bytes, &tag.name);
            push_component(&mut bytes, &tag.value);
        }
        Self(bytes)
    }

    /// Number of bytes this key occupies, for the memory reservation.
    fn len(&self) -> usize {
        self.0.len()
    }
}

/// Appends one length-prefixed UTF-8 component.
fn push_component(bytes: &mut Vec<u8>, component: &str) {
    bytes.extend_from_slice(&component.len().to_le_bytes());
    bytes.extend_from_slice(component.as_bytes());
}

/// Renders a label set the way Prometheus prints one, for the duplicate-label-set error.
fn render_labels(metric: &MetricName) -> String {
    let mut parts = Vec::new();
    if let Some(name) = metric.name() {
        parts.push(format!("__name__={name:?}"));
    }
    parts.extend(
        metric
            .tags()
            .iter()
            .map(|tag| format!("{}={:?}", tag.name, tag.value)),
    );
    format!("{{{}}}", parts.join(", "))
}

/// Everything one output pass needs to know about which rows hold a value.
///
/// Computed once per series and shared by the label-set collision check and the batch
/// construction. Previously each scanned the series separately, so a series with values at every
/// grid point was walked twice per output.
struct Occupancy {
    /// The grid offsets that hold a value, in order.
    offsets: Vec<usize>,
    /// The same positions as merged runs of absolute grid indices.
    runs: Vec<(usize, usize)>,
    /// Whether every row holds a value. When set, no `take` is needed and `offsets` is the
    /// identity, so callers skip building it.
    dense: bool,
}

impl Occupancy {
    /// The `take` indices, or `None` when every row is kept.
    fn indices(&self) -> Option<ArrayRef> {
        (!self.dense).then(|| {
            Arc::new(UInt32Array::from(
                self.offsets
                    .iter()
                    .map(|offset| *offset as u32)
                    .collect::<Vec<_>>(),
            )) as ArrayRef
        })
    }
}

/// Scans the trimmed range once and reports its occupied offsets and runs.
///
/// A single dense, null-free float field makes every row occupied without touching them: the
/// array's own null count answers the question, so the common case does no per-row work at all.
/// Otherwise one pass over the rows computes the union of the fields' validity.
fn occupancy(series: &Series) -> Result<Occupancy> {
    let TimeAxis::Grid(range) = &series.time else {
        return Err(DataFusionError::Execution(
            "FusedSeriesExec requires evaluated series on a time grid".into(),
        ));
    };
    let rows = range.len();
    let start = range.start();
    if rows == 0 {
        return Ok(Occupancy {
            offsets: Vec::new(),
            runs: Vec::new(),
            dense: false,
        });
    }
    // The fast path: one dense float field with no nulls covers every row.
    let dense = series.fields.len() == 1
        && matches!(
            series.fields.iter().next().and_then(Samples::dense),
            Some(DenseSamplesRef::Float(values)) if values.null_count() == 0
        );
    if dense {
        return Ok(Occupancy {
            offsets: Vec::new(),
            runs: vec![(start, start + rows)],
            dense: true,
        });
    }

    let mut offsets = Vec::new();
    let mut runs: Vec<(usize, usize)> = Vec::new();
    for offset in 0..rows {
        if series
            .fields
            .iter()
            .any(|samples| has_value(samples, offset))
        {
            offsets.push(offset);
            let absolute = start + offset;
            match runs.last_mut() {
                Some(run) if run.1 == absolute => run.1 = absolute + 1,
                _ => runs.push((absolute, absolute + 1)),
            }
        }
    }
    // A field can cover every row while another has holes, and then nothing is dropped. Say so
    // instead of handing the caller an identity index: `series_to_record_batch` would copy the
    // time column and every value column through it for no change in the result. When every row
    // is occupied the scan has already produced exactly one run covering the whole range.
    if offsets.len() == rows {
        return Ok(Occupancy {
            offsets: Vec::new(),
            runs: vec![(start, start + rows)],
            dense: true,
        });
    }
    Ok(Occupancy {
        offsets,
        runs,
        dense: false,
    })
}

/// Whether one field holds a value, rather than a missing position, at `index`.
fn has_value(samples: &Samples, index: usize) -> bool {
    match samples.dense() {
        Some(DenseSamplesRef::Float(values)) => values.is_valid(index),
        Some(DenseSamplesRef::Histogram(values)) => values.indices().is_valid(index),
        Some(DenseSamplesRef::Mixed(values)) => {
            values.floats().is_valid(index) || values.histograms().indices().is_valid(index)
        }
        None => !matches!(samples.sample(index), SampleRef::Missing),
    }
}

/// Whether two sorted, disjoint run lists share at least one grid index.
fn runs_overlap(stored: &[(usize, usize)], new: &[(usize, usize)]) -> bool {
    let (mut left, mut right) = (0, 0);
    while left < stored.len() && right < new.len() {
        let (stored_start, stored_end) = stored[left];
        let (new_start, new_end) = new[right];
        if new_start < stored_end && stored_start < new_end {
            return true;
        }
        if stored_end <= new_start {
            left += 1;
        } else {
            right += 1;
        }
    }
    false
}

/// Adds sorted, disjoint `new` runs to the sorted, disjoint `stored` runs, coalescing neighbours.
fn merge_runs(stored: &mut Vec<(usize, usize)>, new: &[(usize, usize)]) {
    let mut merged: Vec<(usize, usize)> = Vec::with_capacity(stored.len() + new.len());
    let (mut left, mut right) = (0, 0);
    while left < stored.len() || right < new.len() {
        let run = match (stored.get(left), new.get(right)) {
            (Some(stored), Some(new)) if stored.0 <= new.0 => {
                left += 1;
                *stored
            }
            (Some(_), Some(new)) => {
                right += 1;
                *new
            }
            (Some(stored), None) => {
                left += 1;
                *stored
            }
            (None, Some(new)) => {
                right += 1;
                *new
            }
            (None, None) => break,
        };
        match merged.last_mut() {
            Some(last) if run.0 <= last.1 => last.1 = last.1.max(run.1),
            _ => merged.push(run),
        }
    }
    *stored = merged;
}

/// One partition's running partial aggregate.
///
/// Slots are keyed by `(label group, absolute grid index)`, never by a local row number: each
/// series trims its own interval, so only the absolute index names the DataFusion group the
/// ordinary partial aggregate would have produced for that point.
struct PartialSumStream {
    /// The node's binding: value column, group columns, output schema and accumulator factory.
    config: PartialSum,
    /// DataFusion's own accumulator, one per stream: its state *is* the emitted state.
    accumulator: Box<dyn GroupsAccumulator>,
    /// The series field the aggregate sums: the classification position of the aggregate's value
    /// column, resolved to the field it names.
    value_field: usize,
    /// The group columns resolved against the evaluated classification, in the aggregate's own
    /// order. Stored once so a batch never re-derives what a column is.
    group_columns: Vec<GroupColumn>,
    /// The node's evaluation grid: an absolute grid index names its timestamp here.
    grid: Arc<EvalGrid>,
    tags: Arc<[String]>,
    /// One entry per label group: absolute grid index -> slot id.
    slots: Vec<SlotColumn>,
    /// Label group key -> id in `slots`, `label_values` and `keys`.
    labels: HashMap<Vec<u8>, usize>,
    /// Per label group: the resolved value of each group *tag* column, in `group_columns` order.
    /// Filled from the same rule the evaluated tag columns use, so the emitted group columns print
    /// exactly the labels the evaluated rows would have carried.
    label_values: Vec<Vec<Option<String>>>,
    /// Slot -> `(label group, absolute grid index)`, in slot order: the order `state()` returns.
    keys: Vec<(u32, u32)>,
    /// Bytes of the persistent tables -- `slots` with their columns, `labels`, `label_values` and
    /// `keys` -- already charged on the pool. Measured from the containers' capacities (see
    /// [`Self::held_bytes`]) rather than by counting insertions, so a growth step the allocator
    /// took is charged even when no insertion announced it.
    persistent_bytes: usize,
    /// The accumulator's own `size()` at the last reservation, so only its growth is reserved.
    accumulator_bytes: usize,
}

/// One group column of the emitted state batch, resolved from the evaluated classification.
enum GroupColumn {
    /// The evaluation timestamp of the slot's absolute grid index.
    Time,
    /// A tag column: index in the node's tag list, its encoding, the classification position it
    /// came from (for error messages), and its ordinal among the group *tag* columns, which is
    /// where `label_values` keeps the value.
    Tag {
        tag: usize,
        encoding: TagEncoding,
        position: usize,
        ordinal: usize,
    },
}

/// The slot ids of one label group, over the interval of the grid that group actually covers.
///
/// `ids[offset]` is the slot for absolute grid index `base + offset`, so the column spans
/// `[base, base + ids.len())` and **nothing below `base` is ever initialized**. That is what keeps
/// a group whose series only cover the tail of a large grid cheap: a column indexed by the
/// absolute index would cost one `u32` per grid point per group whether or not the group has a
/// point there, so a 100 000-point grid with 1 000 groups holding one point each would pay about
/// 400 MB to represent 1 000 aggregating positions.
///
/// The span is a flat array rather than a chunked map because [`PartialSumStream::slot`] runs once
/// per grid point per series and must stay an array index. The price is that a group with points at
/// both ends of the grid pays for the span in between; that is the same shape the ordinary partial
/// aggregate pays for, since it holds a slot per group either way. These bytes are reserved on the
/// stream's memory pool together with the other persistent tables, by capacity.
#[derive(Default)]
struct SlotColumn {
    /// Absolute grid index of `ids[0]`, meaningless while `ids` is empty.
    base: usize,
    ids: Vec<u32>,
}

impl SlotColumn {
    /// The slot of `abs`, or `None` when this group has no point there yet.
    fn get(&self, abs: usize) -> Option<u32> {
        let offset = abs.checked_sub(self.base)?;
        match self.ids.get(offset) {
            Some(&slot) if slot != NO_SLOT => Some(slot),
            _ => None,
        }
    }

    /// Makes `abs` addressable, initializing only the indices this adds.
    ///
    /// Grows in steps, so a dense group pays one growth per step rather than one per point. A point
    /// below the current base means a later series covered an earlier interval: the existing ids
    /// shift up inside the same `Vec`, and only the gap down to the new lowest point is added.
    fn reserve(&mut self, abs: usize) {
        // Indices a single step adds: 256 bytes per group and step, at most one step of slack.
        const STEP: usize = 64;
        if self.ids.is_empty() {
            // The first point this group occupies defines the base, so a query over the tail of a
            // large grid never allocates the wasted prefix.
            self.base = abs;
            self.ids.resize(STEP, NO_SLOT);
            return;
        }
        if abs < self.base {
            let added = self.base - abs;
            let len = self.ids.len() + added;
            // The new indices land at the end, then rotate to the front: the old ids keep their
            // slots and end up at their new offsets, which are exactly `added` higher.
            self.ids.resize(len, NO_SLOT);
            self.ids.rotate_right(added);
            self.base = abs;
            return;
        }
        let offset = abs - self.base;
        if offset >= self.ids.len() {
            self.ids.resize(offset + STEP, NO_SLOT);
        }
    }

    fn set(&mut self, abs: usize, slot: u32) {
        self.ids[abs - self.base] = slot;
    }
}

impl PartialSumStream {
    /// Creates the accumulator state of one stream and resolves its group columns.
    fn try_new(
        config: PartialSum,
        accumulator: Box<dyn GroupsAccumulator>,
        classification: &[OutputColumn],
        tags: Arc<[String]>,
        grid: Arc<EvalGrid>,
    ) -> Result<Self> {
        let mut group_columns = Vec::with_capacity(config.group_columns.len());
        let mut ordinals = 0;
        for position in &config.group_columns {
            let column = match classification.get(*position) {
                Some(OutputColumn::Time) => GroupColumn::Time,
                Some(OutputColumn::Tag(tag, encoding)) => {
                    let column = GroupColumn::Tag {
                        tag: *tag,
                        encoding: *encoding,
                        position: *position,
                        ordinal: ordinals,
                    };
                    ordinals += 1;
                    column
                }
                other => {
                    return Err(DataFusionError::Execution(format!(
                        "partial sum group column {position} is {other:?}, but only the time \
                         column and tag columns can be group keys"
                    )));
                }
            };
            group_columns.push(column);
        }
        let value_field = match classification.get(config.value_column) {
            Some(OutputColumn::Field(index)) => *index,
            other => {
                return Err(DataFusionError::Execution(format!(
                    "partial sum value column {} is {other:?}, but it must be a series field",
                    config.value_column
                )));
            }
        };
        Ok(Self {
            config,
            accumulator,
            value_field,
            group_columns,
            grid,
            tags,
            slots: Vec::new(),
            labels: HashMap::new(),
            label_values: Vec::new(),
            keys: Vec::new(),
            persistent_bytes: 0,
            accumulator_bytes: 0,
        })
    }

    /// The bytes the persistent tables of this stream actually hold.
    ///
    /// Measured from each container's own `capacity()` and each stored `String`'s `capacity()`,
    /// not from the number of insertions that were made: a `Vec` that has grown to 128 slots holds
    /// them whether or not all are filled, and `Vec::push`/`HashMap::insert` pick their own growth
    /// steps, so a count-based figure drifts from what the allocator holds. The label map is
    /// charged its bucket count times the item size plus hashbrown's control byte.
    fn held_bytes(&self) -> usize {
        let mut bytes = self.slots.capacity() * size_of::<SlotColumn>()
            + self.labels.capacity() * (size_of::<(Vec<u8>, usize)>() + 1)
            + self.label_values.capacity() * size_of::<Vec<Option<String>>>()
            + self.keys.capacity() * size_of::<(u32, u32)>();
        for column in &self.slots {
            bytes += column.ids.capacity() * size_of::<u32>();
        }
        for key in self.labels.keys() {
            bytes += key.capacity();
        }
        for values in &self.label_values {
            // The `Option<String>` array, then each value's own buffer: the group columns are
            // printed once per emitted state row, so these values live as long as the stream.
            bytes += values.capacity() * size_of::<Option<String>>();
            for value in values {
                bytes += value.as_ref().map_or(0, String::capacity);
            }
        }
        bytes
    }

    /// Charges the growth of the persistent tables, and gives back what they released.
    ///
    /// Called once per ingested series rather than per point: a single series can add at most its
    /// own number of slots and at most one label group, so what is uncharged between two syncs is
    /// bounded by one series' coverage, while the per-point path stays an array index.
    fn sync_persistent(&mut self, reservation: &mut MemoryReservation) -> Result<()> {
        let held = self.held_bytes();
        if held > self.persistent_bytes {
            reservation.try_grow(held - self.persistent_bytes)?;
        } else if held < self.persistent_bytes {
            reservation.shrink(self.persistent_bytes - held);
        }
        self.persistent_bytes = held;
        Ok(())
    }

    /// Folds one completed evaluated series into the accumulator and reports the points it fed.
    ///
    /// Zero means the series held no point at all and contributed no group, so it has no output
    /// row and nothing to accumulate. The series is dropped by the caller right after this call:
    /// only accumulator state survives.
    fn ingest(
        &mut self,
        series: &Series,
        occupancy: &Occupancy,
        reservation: &mut MemoryReservation,
    ) -> Result<usize> {
        let TimeAxis::Grid(range) = &series.time else {
            return Err(DataFusionError::Execution(
                "FusedSeriesExec requires evaluated series on a time grid".into(),
            ));
        };
        let rows = range.len();
        // A dense occupancy is every row, so the common case allocates no offset list here.
        let points = if occupancy.dense {
            rows
        } else {
            occupancy.offsets.len()
        };
        if points == 0 {
            return Ok(0);
        }
        let start = range.start();

        // The label group is the aggregate's tag columns only: the timestamp is the other half of
        // the slot key, so it must not take part in the group's identity.
        let mut values = Vec::with_capacity(self.group_columns.len());
        for column in &self.group_columns {
            if let GroupColumn::Tag {
                tag,
                encoding,
                position,
                ..
            } = column
            {
                values.push(tag_value(series, &self.tags, *tag, *encoding, *position)?);
            }
        }
        let group = self.label_group(&values);

        // One `group_indices` entry per row of the value array below. A row with no value carries
        // an in-range index: SUM skips nulls, so it never contributes to that slot.
        let mut indices = vec![0usize; rows];
        if occupancy.dense {
            for (offset, index) in indices.iter_mut().enumerate() {
                *index = self.slot(group, start + offset)? as usize;
            }
        } else {
            for offset in &occupancy.offsets {
                indices[*offset] = self.slot(group, start + offset)? as usize;
            }
        }

        // The occupancy of a point is the union over the series' fields, so with more than one
        // field a point can be occupied while *this* value column is null: SUM skips it, and a
        // slot that receives no value at all emits a null state. The planner hook therefore only
        // binds a single-field shape, where "occupied" and "the summed value is non-null" are the
        // same set and the state rows match DataFusion's own partial groups exactly.
        let samples = series.fields.get(self.value_field).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "evaluated series has no field at index {}",
                self.value_field
            ))
        })?;
        let values = field_column(samples)?;
        if values.len() != rows {
            return Err(DataFusionError::Execution(format!(
                "evaluated series field {} has {} rows, time axis has {rows}",
                self.value_field,
                values.len()
            )));
        }
        // `total_num_groups` is the slot count *after* this series' slots exist, and slots are
        // handed out contiguously, so the accumulator can size itself once per batch.
        self.accumulator
            .update_batch(&[values], &indices, None, self.keys.len())?;
        // The series' slots and label group exist now, so charge the tables they grew. This runs
        // before the accumulator's own size is charged, so the pool sees the whole step at once.
        self.sync_persistent(reservation)?;
        let size = self.accumulator.size();
        if size > self.accumulator_bytes {
            reservation.try_grow(size - self.accumulator_bytes)?;
            self.accumulator_bytes = size;
        }
        Ok(points)
    }

    /// The label group of the given tag values, registering a new one on first use.
    ///
    /// The values are borrowed from the series, so a series that joins an existing group copies
    /// nothing; only a new group keeps its own copy, because it outlives the series.
    fn label_group(&mut self, values: &[Option<&str>]) -> usize {
        let mut key = Vec::new();
        for value in values {
            match value {
                Some(value) => {
                    // Length-prefixed, marked for presence: an absent label must not encode like
                    // the empty label value, and a value must not swallow its neighbour.
                    key.push(1);
                    push_component(&mut key, value);
                }
                None => key.push(0),
            }
        }
        if let Some(group) = self.labels.get(&key) {
            return *group;
        }
        let group = self.slots.len();
        self.labels.insert(key, group);
        self.slots.push(SlotColumn::default());
        self.label_values.push(
            values
                .iter()
                .map(|value| value.map(str::to_string))
                .collect(),
        );
        group
    }

    /// The slot of `(group, abs)`, allocated on first use.
    ///
    /// Nothing is reserved here: a series can add at most its own number of slots and at most one
    /// label group, so the growth is charged once per series by [`Self::sync_persistent`] instead
    /// of on the per-point path.
    fn slot(&mut self, group: usize, abs: usize) -> Result<u32> {
        if let Some(slot) = self.slots[group].get(abs) {
            return Ok(slot);
        }
        let slot = u32::try_from(self.keys.len()).map_err(|_| {
            DataFusionError::Execution(
                "FusedSeriesExec partial sum exceeds the supported number of groups".into(),
            )
        })?;
        self.slots[group].reserve(abs);
        self.slots[group].set(abs, slot);
        self.keys.push((
            u32::try_from(group).map_err(|_| {
                DataFusionError::Execution(
                    "FusedSeriesExec partial sum exceeds the supported number of label groups"
                        .into(),
                )
            })?,
            u32::try_from(abs).map_err(|_| {
                DataFusionError::Execution(
                    "FusedSeriesExec partial sum exceeds the supported grid length".into(),
                )
            })?,
        ));
        Ok(slot)
    }

    /// Reads the accumulator's state once and builds the batch this partition reports.
    ///
    /// `None` means the partition saw no point at all: an empty partition emits an empty stream
    /// instead of a fabricated zero row. `evaluate()` is never called; the state API is the
    /// contract, so the final aggregate above consumes exactly what DataFusion would have written.
    fn emit_state(&mut self) -> Result<Option<RecordBatch>> {
        if self.keys.is_empty() {
            return Ok(None);
        }
        let state = self.accumulator.state(EmitTo::All)?;
        let mut columns: Vec<ArrayRef> = Vec::with_capacity(self.group_columns.len() + state.len());
        for column in &self.group_columns {
            columns.push(match column {
                GroupColumn::Time => Arc::new(TimestampMillisecondArray::from(
                    self.keys
                        .iter()
                        .map(|(_, abs)| self.grid.timestamps()[*abs as usize])
                        .collect::<Vec<_>>(),
                )),
                GroupColumn::Tag {
                    ordinal, encoding, ..
                } => self.tag_group_column(*ordinal, *encoding)?,
            });
        }
        columns.extend(state);
        Ok(Some(RecordBatch::try_new(
            self.config.output_schema.clone(),
            columns,
        )?))
    }

    /// One group tag column of the state batch: the label value of each slot's group.
    fn tag_group_column(&self, ordinal: usize, encoding: TagEncoding) -> Result<ArrayRef> {
        let values = self
            .keys
            .iter()
            .map(|(group, _)| self.label_values[*group as usize][ordinal].as_deref());
        if !encoding.dictionary {
            return Ok(Arc::new(StringArray::from_iter(values)));
        }
        // Real keys through the builder, so the state batch's dictionary column is a legal
        // dictionary and not one entry with every row pointing at it.
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        for value in values {
            match value {
                Some(value) => builder.append_value(value),
                None => builder.append_null(),
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

/// Turns raw series chunks into output batches of coalesced completed series, or, in partial mode,
/// into one batch of partial-aggregate state per partition.
struct FusedSeriesStream {
    input: SendableRawSeriesStream,
    /// `None` once the child ended; `finish(self)` consumes the evaluator.
    evaluator: Option<RangeEvaluator>,
    maps: Vec<MathOp>,
    tags: Arc<[String]>,
    output_schema: SchemaRef,
    classification: Arc<[OutputColumn]>,
    /// The running partial aggregate of this partition; `None` in evaluated mode.
    partial: Option<PartialSumStream>,
    /// The grid positions each final label set already occupies, as merged, sorted runs.
    ///
    /// Per stream, hence per partition: each partition owns whole series of its own, so a label
    /// set only ever collides with one inside the same partition.
    seen: HashMap<LabelsKey, Vec<(usize, usize)>>,
    /// Batches of completed series not yet handed downstream, and the rows they hold.
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    /// Bytes of `pending`, reserved on `reservation` like the label-set state.
    pending_bytes: usize,
    /// Accounts for `seen`, `pending` and the partial aggregate's slot and accumulator state;
    /// batches already handed downstream are not reserved. Released at [`Self::abort`] on every
    /// error path, and by [`MemoryReservation`]'s own `Drop` otherwise.
    reservation: MemoryReservation,
    finished: bool,
    /// Standard `output_rows` / `output_batches` / `elapsed_compute`.
    baseline: BaselineMetrics,
    /// Handles to the counters registered on this partition's metrics set. They are `Arc`-backed
    /// clones, so recording on them updates what `metrics()` reports.
    timers: FusedTimers,
}

/// The four block-scoped timers, the three batch-region timers, the three counters of one
/// partition and, in partial mode, its two partial timers and four partial counters.
#[derive(Clone)]
struct FusedTimers {
    input_poll: Time,
    evaluate: Time,
    emit: Time,
    flush: Time,
    window_prep: Time,
    kernel: Time,
    columns: Time,
    input_chunks: Count,
    series_completed: Count,
    series_emitted: Count,
    /// Recorded but only *registered* in partial mode, so a node that never folds into an
    /// accumulator reports no empty partial metrics.
    partial_update: Time,
    partial_state: Time,
    partial_updates: Count,
    partial_points: Count,
    partial_slots: Count,
    partial_state_rows: Count,
}

impl FusedTimers {
    /// Registers the handles on `set` for `partition` and returns the clones the stream records on.
    fn register(set: &ExecutionPlanMetricsSet, partition: usize, partial: bool) -> Self {
        let build_time = |name: &'static str| {
            let time = Time::new();
            MetricBuilder::new(set)
                .with_partition(partition)
                .build(MetricValue::Time {
                    name: name.into(),
                    time: time.clone(),
                });
            time
        };
        let input_poll = build_time(METRIC_INPUT_POLL);
        let evaluate = build_time(METRIC_EVALUATE);
        let emit = build_time(METRIC_EMIT);
        let flush = build_time(METRIC_FLUSH);
        // The evaluator runs these three regions for every batch of every series; they are read
        // once, when the evaluator is done, so the per-batch cost stays inside the evaluator.
        let window_prep = build_time(METRIC_WINDOW_PREP);
        let kernel = build_time(METRIC_KERNEL);
        let columns = build_time(METRIC_COLUMNS);

        let build_count = |name: &'static str| {
            let count = Count::new();
            MetricBuilder::new(set)
                .with_partition(partition)
                .build(MetricValue::Count {
                    name: name.into(),
                    count: count.clone(),
                });
            count
        };
        let input_chunks = build_count(METRIC_INPUT_CHUNKS);
        let series_completed = build_count(METRIC_SERIES_COMPLETED);
        let series_emitted = build_count(METRIC_SERIES_EMITTED);

        // Partial metrics exist only on a node that actually folds into an accumulator; the
        // handles are still created so the recording sites need no branches, but an evaluated
        // node registers nothing and reports nothing.
        let partial_update = Time::new();
        let partial_state = Time::new();
        let partial_updates = Count::new();
        let partial_points = Count::new();
        let partial_slots = Count::new();
        let partial_state_rows = Count::new();
        if partial {
            for (name, time) in [
                (METRIC_PARTIAL_UPDATE, &partial_update),
                (METRIC_PARTIAL_STATE, &partial_state),
            ] {
                MetricBuilder::new(set)
                    .with_partition(partition)
                    .build(MetricValue::Time {
                        name: name.into(),
                        time: time.clone(),
                    });
            }
            for (name, count) in [
                (METRIC_PARTIAL_UPDATES, &partial_updates),
                (METRIC_PARTIAL_POINTS, &partial_points),
                (METRIC_PARTIAL_SLOTS, &partial_slots),
                (METRIC_PARTIAL_STATE_ROWS, &partial_state_rows),
            ] {
                MetricBuilder::new(set)
                    .with_partition(partition)
                    .build(MetricValue::Count {
                        name: name.into(),
                        count: count.clone(),
                    });
            }
        }

        Self {
            input_poll,
            evaluate,
            emit,
            flush,
            window_prep,
            kernel,
            columns,
            input_chunks,
            series_completed,
            series_emitted,
            partial_update,
            partial_state,
            partial_updates,
            partial_points,
            partial_slots,
            partial_state_rows,
        }
    }

    /// Adds one evaluator's batch regions to this partition's timers.
    ///
    /// Called exactly once per evaluator, right after it is taken out of the stream: a `take`
    /// leaves no evaluator behind, so the second path that could record has nothing to record and
    /// no flag is needed to guard it.
    fn record_batch_regions(&self, timers: BatchTimers) {
        // Every field is added even when it is zero: the timer floors a recorded zero at one
        // nanosecond, so a region that never ran reads as nothing instead of vanishing from the
        // report and sending a reader after a missing metric.
        self.window_prep.add_duration(timers.window_preparation);
        self.kernel.add_duration(timers.kernel);
        self.columns.add_duration(timers.columns);
    }
}

impl FusedSeriesStream {
    /// Marks the stream terminal and gives everything it still retained back to the pool.
    fn abort(&mut self) {
        self.finished = true;
        // The evaluator is dropped here, so its batch regions are read out first: a stream that
        // failed partway still knows where the time of the batches it did run went, and a region
        // it never reached records as nothing.
        if let Some(evaluator) = self.evaluator.take() {
            self.timers.record_batch_regions(evaluator.batch_timers());
        }
        self.release_partition_state();
        self.reservation.free();
    }

    /// Drops every structure charged on `reservation`.
    ///
    /// Both terminal paths call this *before* returning the bytes: `seen`, `pending` and the
    /// partial state are charged on the same reservation, so freeing it while they are still
    /// allocated would put bytes back that the allocator still holds, and the pool could hand them
    /// to another consumer. Used by the success path too, because a stream that has emitted its
    /// last batch is not necessarily dropped yet -- the caller keeps it until it polls `None` and
    /// may hold it longer.
    fn release_partition_state(&mut self) {
        self.seen = HashMap::new();
        self.pending = Vec::new();
        self.pending_rows = 0;
        self.pending_bytes = 0;
        self.partial = None;
    }

    /// Claims the occupied grid positions of one finished series for its final label set.
    ///
    /// A collision is an error only when both series hold a sample at the same evaluation
    /// timestamp, which is Prometheus' range-query rule. Series with disjoint points are legal:
    /// Prometheus merges them into one output series, while this node emits them as separate rows
    /// and defers that merge to matrix assembly, so every sample survives.
    fn claim_label_set(&mut self, series: &Series, runs: &[(usize, usize)]) -> Result<()> {
        let runs = runs.to_vec();
        let key = LabelsKey::new(&series.metric);
        match self.seen.entry(key) {
            Entry::Occupied(mut entry) => {
                let stored = entry.get_mut();
                if runs_overlap(stored, &runs) {
                    return Err(DataFusionError::Execution(format!(
                        "vector cannot contain metrics with the same labelset: {}",
                        render_labels(&series.metric)
                    )));
                }
                merge_runs(stored, &runs);
            }
            Entry::Vacant(entry) => {
                let bytes = entry.key().len() + runs.len() * 2 * size_of::<usize>();
                self.reservation.try_grow(bytes)?;
                entry.insert(runs);
            }
        }
        Ok(())
    }

    /// Registers the label set of one finished series and converts it into one output batch.
    ///
    /// `None` means the series holds no value at all, so it has no output row.
    fn emit(&mut self, series: &Series) -> Result<Option<RecordBatch>> {
        let started = Instant::now();
        // One scan serves both consumers: the collision check needs the runs, the batch needs the
        // offsets, and they are two views of the same validity.
        let occupancy = occupancy(series)?;
        self.claim_label_set(series, &occupancy.runs)?;
        let batch = series_to_record_batch(
            &self.classification,
            &self.tags,
            &self.output_schema,
            series,
            &occupancy,
        )?;
        self.timers.emit.add_elapsed(started);
        Ok(batch)
    }

    /// Folds one finished series into the partial aggregate and reports the points it fed.
    ///
    /// Zero means the series held no value at all, so it created no group and added no row. The
    /// duplicate-label-set check runs first and unchanged: a `SUM` above the node must not swallow
    /// the error a bare evaluated query reports.
    fn ingest_partial(&mut self, series: &Series) -> Result<usize> {
        let started = Instant::now();
        let occupancy = occupancy(series)?;
        self.claim_label_set(series, &occupancy.runs)?;
        let update_started = Instant::now();
        let points = match self.partial.as_mut() {
            Some(partial) => partial.ingest(series, &occupancy, &mut self.reservation)?,
            None => 0,
        };
        if points > 0 {
            self.timers.partial_update.add_elapsed(update_started);
            self.timers.partial_updates.add(1);
            self.timers.partial_points.add(points);
        }
        self.timers.emit.add_elapsed(started);
        Ok(points)
    }

    /// Emits the partial aggregate's state for this partition, once, at its end.
    ///
    /// `None` means the partition saw no point at all: it emits an empty stream rather than a
    /// fabricated zero row, so a `sum` over nothing stays empty.
    fn emit_partial_state(&mut self) -> Result<Option<RecordBatch>> {
        let Some(partial) = self.partial.as_mut() else {
            return Ok(None);
        };
        let started = Instant::now();
        let slots = partial.keys.len();
        let batch = partial.emit_state()?;
        if let Some(batch) = &batch {
            self.timers.partial_state.add_elapsed(started);
            self.timers.partial_slots.add(slots);
            self.timers.partial_state_rows.add(batch.num_rows());
        }
        Ok(batch)
    }

    /// Buffers one emitted batch and reserves its bytes on the memory pool.
    fn push_pending(&mut self, batch: RecordBatch) -> Result<()> {
        let bytes = batch.get_array_memory_size();
        self.reservation.try_grow(bytes)?;
        self.pending_bytes += bytes;
        self.pending_rows += batch.num_rows();
        self.pending.push(batch);
        Ok(())
    }

    /// Hands the buffered batches downstream as one concatenated batch.
    ///
    /// `None` means nothing was buffered. The reservation of the buffered rows is released here:
    /// the batch belongs to the caller afterwards, exactly like the batches the child hands this
    /// node. `concat_batches` accepts dictionary columns, so tag columns keep their encoding.
    fn flush(&mut self) -> Result<Option<RecordBatch>> {
        if self.pending.is_empty() {
            return Ok(None);
        }
        let started = Instant::now();
        let batches = std::mem::take(&mut self.pending);
        self.pending_rows = 0;
        // Concatenating allocates the result while the buffered batches are still alive, so the
        // buffered bytes stay reserved until it succeeded.
        let batch = concat_batches(&self.output_schema, &batches)?;
        let bytes = std::mem::take(&mut self.pending_bytes);
        self.reservation.try_shrink(bytes)?;
        self.timers.flush.add_elapsed(started);
        Ok(Some(batch))
    }
}

impl RecordBatchStream for FusedSeriesStream {
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl Stream for FusedSeriesStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.as_mut().get_mut();
        // `record_poll` owns the standard `output_rows`, `output_batches` and end timestamp;
        // everything else is recorded inside the loop where the block actually runs.
        let poll = this.poll_evaluated(cx);
        this.baseline.record_poll(poll)
    }
}

impl FusedSeriesStream {
    /// The stream's own poll loop, without the baseline bookkeeping around it.
    fn poll_evaluated(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        let this = self;
        if this.finished {
            return Poll::Ready(None);
        }
        // Every call starts a fresh budget, so returning a batch below resets it.
        let mut consumed = 0usize;
        loop {
            // A finished evaluator means the child already ended and its last series is emitted.
            // Evaluated mode then flushes the buffered rows before reporting the end, so the
            // stream never ends with rows still buffered. Partial mode has no buffered rows: it
            // emits the accumulator's state exactly once, and nothing after it.
            if this.evaluator.is_none() {
                if this.partial.is_some() {
                    let emitted = this.emit_partial_state();
                    // Set before reporting, so the next poll sees a terminal stream. A partition
                    // without a single point stays an empty stream, never a zero row.
                    this.finished = true;
                    return match emitted {
                        Ok(batch) => {
                            // Every structure charged on this reservation is dead weight from
                            // here on: the state batch is built, and the label-set table that
                            // guards the duplicate-label-set rule has already seen every series.
                            // Drop them *first* -- releasing a reservation while what it paid for
                            // is still allocated would let the pool hand those bytes to another
                            // consumer.
                            this.release_partition_state();
                            this.reservation.free();
                            match batch {
                                Some(batch) => Poll::Ready(Some(Ok(batch))),
                                None => Poll::Ready(None),
                            }
                        }
                        Err(error) => {
                            this.abort();
                            Poll::Ready(Some(Err(error)))
                        }
                    };
                }
                return match this.flush() {
                    Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                    Ok(None) => {
                        this.finished = true;
                        Poll::Ready(None)
                    }
                    Err(error) => {
                        this.abort();
                        Poll::Ready(Some(Err(error)))
                    }
                };
            }
            if consumed == MAX_CHUNKS_PER_POLL {
                // The whole budget went into chunks that filled no batch: hand the buffered rows
                // downstream if there are any, and otherwise give the executor thread back
                // instead of draining an always-ready child here.
                return match this.flush() {
                    Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                    Ok(None) => {
                        cx.waker().wake_by_ref();
                        Poll::Pending
                    }
                    Err(error) => {
                        this.abort();
                        Poll::Ready(Some(Err(error)))
                    }
                };
            }
            let poll_started = Instant::now();
            let polled = this.input.as_mut().poll_next(cx);
            if polled.is_pending() {
                // The block did not produce a chunk, so charging it would fold wait time into a
                // timer that claims to measure work.
                return Poll::Pending;
            }
            this.timers.input_poll.add_elapsed(poll_started);
            let chunk = match polled {
                Poll::Ready(None) => {
                    let Some(evaluator) = this.evaluator.take() else {
                        // Handled at the top of the loop; go back to it instead of assuming.
                        continue;
                    };
                    // Read out before `finish` consumes the evaluator. The `take` above is also
                    // what makes this the only place the regions can be recorded, so nothing
                    // has to guard against a double record: a second path finds no evaluator.
                    this.timers.record_batch_regions(evaluator.batch_timers());
                    if let Err(error) = evaluator.finish() {
                        this.abort();
                        return Poll::Ready(Some(Err(error)));
                    }
                    // The last series is emitted, so the loop flushes and reports the end.
                    continue;
                }
                Poll::Ready(Some(Err(error))) => {
                    this.abort();
                    return Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))));
                }
                Poll::Ready(Some(Ok(chunk))) => chunk,
                Poll::Pending => unreachable!("a pending poll returned above"),
            };
            consumed += 1;
            this.timers.input_chunks.add(1);
            let evaluate_started = Instant::now();
            let evaluated = {
                let maps = &this.maps;
                let Some(evaluator) = this.evaluator.as_mut() else {
                    // Handled at the top of the loop; go back to it instead of assuming.
                    continue;
                };
                evaluator.push(&chunk).and_then(|series| match series {
                    Some(series) => apply_maps(evaluator, maps, series).map(Some),
                    None => Ok(None),
                })
            };
            this.timers.evaluate.add_elapsed(evaluate_started);
            let series = match evaluated {
                Ok(series) => series,
                Err(error) => {
                    this.abort();
                    return Poll::Ready(Some(Err(error)));
                }
            };
            let Some(series) = series else {
                continue;
            };
            this.timers.series_completed.add(1);
            // Partial mode never builds evaluated rows: the series goes straight into the
            // accumulator, and the one state batch is built at the end of the partition.
            if this.partial.is_some() {
                match this.ingest_partial(&series) {
                    // A series without a single point contributes no group and no row.
                    Ok(0) => continue,
                    Ok(_) => {
                        this.timers.series_emitted.add(1);
                        continue;
                    }
                    Err(error) => {
                        this.abort();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
            }
            match this.emit(&series) {
                // A series without a single value has no row to contribute.
                Ok(None) => continue,
                Ok(Some(batch)) => {
                    this.timers.series_emitted.add(1);
                    if let Err(error) = this.push_pending(batch) {
                        this.abort();
                        return Poll::Ready(Some(Err(error)));
                    }
                }
                Err(error) => {
                    this.abort();
                    return Poll::Ready(Some(Err(error)));
                }
            }
            // Below the row target the batch stays buffered, so one series does not cost a batch.
            if this.pending_rows < MAX_ROWS_PER_BATCH {
                continue;
            }
            return match this.flush() {
                Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                Ok(None) => continue,
                Err(error) => {
                    this.abort();
                    Poll::Ready(Some(Err(error)))
                }
            };
        }
    }
}
