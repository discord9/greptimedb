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

//! Local selection of one fused range-function subtree over a single region scan.
//!
//! [`try_select_fused`] runs at the existing experimental gate on the optimized logical plan and
//! the physical plan DataFusion built for it. It fuses exactly one subtree of the shape
//!
//! ```text
//! Filter: <value> IS NOT NULL     <- optional, may repeat
//!   Projection                    <- moves columns, applies the one range function
//!     Extension(RangeManipulate)  <- the folded range window
//! ```
//!
//! into one [`FusedSeriesExec`], which evaluates the range function over raw series instead of
//! folding the window into range arrays first.
//!
//! The fused subtree may sit anywhere in the plan, so an aggregation over a range function (the
//! common query shape) is fused too: the subtree is located by its child-index path from the
//! logical root, the physical plan is descended along the same path, and only the physical node at
//! the end of that path is swapped. Every ancestor keeps its own configuration and its properties.
//! Two or more candidate subtrees are rejected, because phase 1 fuses exactly one.
//!
//! Every gate failure returns `Ok(None)`, which means "run the ordinary plan". Nothing here may
//! turn an unrecognized shape into a query error: the ordinary plan is always a correct answer, so
//! an unfamiliar shape is only ever a fallback.

use std::sync::Arc;

use api::v1::SemanticType;
use datafusion::arrow::datatypes::{DataType, SchemaRef, TimeUnit};
use datafusion::datasource::DefaultTableSource;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::coop::CooperativeExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion_common::{Result as DfResult, ScalarValue};
use datafusion_expr::logical_plan::{SubqueryAlias, TableScan};
use datafusion_expr::{Expr, Extension, LogicalPlan, Operator};
use datatypes::data_type::ConcreteDataType;
use datatypes::series::time::{EvalGrid, GridSpec};
use promql::extension_plan::fused_series::FusedSeriesExec;
use promql::extension_plan::{
    RangeManipulate, RangeManipulateExec, SeriesDivide, SeriesDivideExec, SeriesNormalize,
    SeriesNormalizeExec,
};
use promql::range_eval::{RangeFunction, RangeWindow};
use store_api::metric_engine_consts::is_metric_engine_internal_column;
use table::table::scan::RegionScanExec;

use crate::datafusion::raw_expr_supported;
use crate::dummy_catalog::DummyTableProvider;

/// Selects one fused range-function subtree, or `None` to run the ordinary plan unchanged.
pub(crate) fn try_select_fused(
    logical: &LogicalPlan,
    original: &Arc<dyn ExecutionPlan>,
) -> DfResult<Option<Arc<dyn ExecutionPlan>>> {
    let Some(shape) = FusedShape::discover(logical) else {
        return Ok(None);
    };
    // The plan may be wrapped once at the root; that wrapper has to survive the swap.
    let root = unwrap_cooperative(original).unwrap_or_else(|| Arc::clone(original));
    let root_cooperative = !Arc::ptr_eq(&root, original);
    let Some(target) = pick(&root, logical, &shape.path) else {
        return Ok(None);
    };
    let Some((scan, scan_cooperative)) = fused_scan(&target) else {
        return Ok(None);
    };
    let fused = match shape.build(&scan, scan_cooperative) {
        Ok(fused) => fused,
        Err(reason) => {
            common_telemetry::debug!("Fused series selection fell back: {reason}");
            return Ok(None);
        }
    };
    let Some(replaced) = rebuild(&root, logical, &shape.path, &fused) else {
        return Ok(None);
    };
    // Operators need to know which path ran; a fallback is silent by design.
    common_telemetry::debug!(
        "Fused series selection succeeded: {:?} over {}",
        shape.function,
        shape.scan.table_name
    );
    let result = if root_cooperative {
        Arc::clone(original)
            .replace_children(
                vec![replaced],
                datafusion::physical_plan::ReplaceChildrenOptions::new(
                    datafusion::physical_plan::ChildrenPropertiesMode::Recompute,
                ),
            )
            .ok()
    } else {
        Some(replaced)
    };
    // A replacement that changes the caller's output is not an equivalent plan.
    let Some(result) = result.filter(|result| result.schema() == original.schema()) else {
        return Ok(None);
    };
    Ok(Some(result))
}

/// One matched fused subtree, with everything the fused node needs to run it.
struct FusedShape {
    /// Child indices from the logical root down to the topmost node of the fused subtree.
    path: Vec<usize>,
    /// The single range function the subtree applies.
    function: RangeFunction,
    /// The scan the range window reads.
    scan: TableScan,
    /// The evaluation window and grid of the folded range.
    start: i64,
    end: i64,
    interval: i64,
    range: i64,
    time_index: String,
    /// The one value column the range function consumes.
    field: String,
    /// The output schema of the fused subtree, taken from the plan itself.
    output_schema: SchemaRef,
}

/// What a matched `RangeManipulate` contributes to a [`FusedShape`].
struct Fold {
    scan: TableScan,
    start: i64,
    end: i64,
    interval: i64,
    range: i64,
    time_index: String,
    field: String,
}

impl FusedShape {
    /// Finds the single fused subtree of `plan`, or `None` if there is none or more than one.
    fn discover(plan: &LogicalPlan) -> Option<Self> {
        let mut found = Vec::new();
        collect(plan, &mut Vec::new(), &mut found);
        let [shape] = found.try_into().ok()?;
        Some(shape)
    }

    /// Matches the chain of projections and filters whose bottom is one `RangeManipulate`.
    fn match_chain(plan: &LogicalPlan, path: &[usize]) -> Option<Self> {
        let mut call = None;
        let fold = fold(plan, &mut call)?;
        let call = call?;
        Some(Self {
            path: path.to_vec(),
            function: call.function,
            scan: fold.scan,
            start: fold.start,
            end: fold.end,
            interval: fold.interval,
            range: fold.range,
            time_index: fold.time_index,
            field: fold.field,
            output_schema: Arc::new(plan.schema().as_arrow().clone()),
        })
    }

    /// Builds the fused node over the raw series leaf of the matched `TableScan`.
    ///
    /// The error is the name of the gate that rejected the shape; the caller falls back to the
    /// ordinary plan, so this is never a query failure.
    fn build(
        &self,
        scan: &Arc<RegionScanExec>,
        scan_cooperative: bool,
    ) -> Result<Arc<dyn ExecutionPlan>, &'static str> {
        let Some(source) = self.scan.source.downcast_ref::<DefaultTableSource>() else {
            return Err("scan source is not a default table source");
        };
        let Some(provider) = source.table_provider.downcast_ref::<DummyTableProvider>() else {
            return Err("scan source is not a dummy table provider");
        };
        let metadata = provider.region_metadata();
        let metric_name = self.scan.table_name.table().to_string();
        // The authorized logical tags are every visible tag of the table, in table order.
        let logical_tags = metadata
            .column_metadatas
            .iter()
            .filter(|column| {
                column.semantic_type == SemanticType::Tag
                    && !is_metric_engine_internal_column(&column.column_schema.name)
            })
            .map(|column| column.column_schema.name.clone())
            .collect::<Vec<_>>();
        // The output schema of the plan itself is the authority on the output columns: its tag
        // columns are the declared tags in output order, and its one `Float64` column is the value
        // column, which the planner names after the range-function call rather than the raw field.
        let mut tags = Vec::with_capacity(logical_tags.len());
        let mut values = Vec::new();
        let mut has_time = false;
        for field in self.output_schema.fields() {
            if field.name() == &self.time_index {
                if field.data_type() != &DataType::Timestamp(TimeUnit::Millisecond, None)
                    || field.is_nullable()
                {
                    return Err("time column is not a non-null millisecond timestamp");
                }
                has_time = true;
                continue;
            }
            if logical_tags.iter().any(|tag| tag == field.name()) {
                // The real plan reports labels dictionary-encoded; both encodings are supported.
                match field.data_type() {
                    DataType::Utf8 => {}
                    DataType::Dictionary(key, value)
                        if **key == DataType::UInt32 && **value == DataType::Utf8 => {}
                    _ => return Err("tag column is neither Utf8 nor Dictionary(UInt32, Utf8)"),
                }
                // Either nullability is fine: the node reports an absent label as a NULL where
                // the column allows one and as the empty label value where it does not.
                tags.push(field.name().clone());
                continue;
            }
            // Everything else is the value column: a nullable float that does not shadow a
            // non-float column of the table.
            if field.data_type() != &DataType::Float64 || !field.is_nullable() {
                return Err("value column is not a nullable Float64");
            }
            if let Some(column) = metadata.column_by_name(field.name())
                && (column.semantic_type != SemanticType::Field
                    || column.column_schema.data_type != ConcreteDataType::float64_datatype())
            {
                return Err("value column shadows a non-float column of the table");
            }
            values.push(field.name().clone());
        }
        // Every visible tag must be projected, or the fused node would emit a different label set
        // than the ordinary plan, and exactly the one value column must be projected.
        if !has_time || tags.len() != logical_tags.len() || values.len() != 1 {
            return Err("output schema is not one time, one value, and every tag column");
        }
        let field_names = vec![self.field.clone()];

        // One partition, exactly like the existing raw-series path forces the scan.
        let ranges = scan.get_partition_ranges();
        let prepared = Arc::new(
            scan.with_new_partitions(vec![ranges], 1)
                .map_err(|_| "scan cannot be pinned to one partition")?,
        );
        let native =
            match prepared.try_to_named_series_scan(&metric_name, &logical_tags, &field_names) {
                Ok(Some(native)) => native,
                // A leaf this shape cannot use, or one the scanner refuses, is a fallback.
                Ok(None) => return Err("scan offers no named raw-series leaf"),
                Err(_) => return Err("scan refused the named raw-series leaf"),
            };
        // The leaf must read exactly the value column the pattern matched, and the scan it came
        // from must still declare the columns the fused output claims, so that the output the
        // fused node produces is the output the ordinary plan would have produced. The leaf's own
        // schema is the packed raw frame, not a column schema, so it is checked by its series
        // schema instead.
        let series_fields = &native.series_schema().fields;
        if series_fields.len() != 1 || series_fields[0].name.as_ref() != field_names[0] {
            return Err("raw-series leaf reads different fields than the pattern");
        }
        let scan_schema = scan.schema();
        let logical_output = self.scan.projected_schema.as_ref();
        if logical_output
            .fields()
            .iter()
            .any(|logical| scan_schema.field_with_name(logical.name()).is_err())
        {
            return Err("scanned columns are not all present in the region scan");
        }

        // A grid needs a positive step and a non-empty, representable point count.
        let span = self
            .end
            .checked_sub(self.start)
            .ok_or("evaluation range underflows")?;
        if self.interval <= 0 || span < 0 {
            return Err("evaluation step is not positive or the range is inverted");
        }
        let count = u64::try_from(span / self.interval)
            .ok()
            .and_then(|count| count.checked_add(1))
            .ok_or("evaluation grid point count is not representable")?;
        let spec = GridSpec::try_new(self.start, self.interval, count)
            .map_err(|_| "evaluation grid specification is invalid")?;
        let grid =
            Arc::new(EvalGrid::try_new(spec).map_err(|_| "evaluation grid cannot be built")?);
        let label_source = native.label_source().clone();
        let series_schema = Arc::clone(native.series_schema());
        let ordering = native.raw_series_ordering();
        let mut child: Arc<dyn ExecutionPlan> = native;
        if scan_cooperative {
            child = Arc::new(CooperativeExec::new(child));
        }
        // A shape that passed every gate but that the node itself refuses is still a fallback.
        let fused = FusedSeriesExec::try_new(
            child,
            series_schema,
            label_source,
            ordering,
            grid,
            RangeWindow {
                range_ms: self.range,
                offset_ms: 0,
                anchor_ms: None,
            },
            self.function,
            // Phase 1: a map chain is not fused, so the plans reaching here carry none.
            vec![],
            self.output_schema.clone(),
            tags,
            self.time_index.clone(),
        )
        .map_err(|_| "FusedSeriesExec rejected the raw-series frame or the output schema")?;
        Ok(Arc::new(fused))
    }
}

/// Records `value` as the one range call of the subtree, rejecting a second one.
fn set_once<T>(slot: &mut Option<T>, value: T) -> Option<()> {
    match slot {
        Some(_) => None,
        None => {
            *slot = Some(value);
            Some(())
        }
    }
}

/// One admitted range-function call: the kernel and the columns it reads.
struct RangeCall {
    function: RangeFunction,
    /// The folded value column, which must be the one `RangeManipulate` folds.
    value: String,
    /// The folded timestamp-range column the kernel reads.
    range: String,
}

/// Maps one range-function call to the kernel that evaluates it.
///
/// Only the three functions of the phase 1 scope are admitted, and only with the argument list the
/// planner builds for them: `(timestamp_range, value, time_index, range)`. A map kernel or an
/// arithmetic layer is not a range function call and returns `None`.
fn range_call(expr: &Expr) -> Option<RangeCall> {
    let Expr::ScalarFunction(call) = expr else {
        return None;
    };
    let function = match call.func.name() {
        "prom_rate" => RangeFunction::Rate,
        "prom_increase" => RangeFunction::Increase,
        "prom_delta" => RangeFunction::Delta,
        _ => return None,
    };
    let [range, value, _time, length] = call.args.as_slice() else {
        return None;
    };
    if !matches!(length, Expr::Literal(ScalarValue::Int64(Some(_)), _)) {
        return None;
    }
    Some(RangeCall {
        function,
        value: column_name(value)?,
        range: column_name(range)?,
    })
}

/// The unqualified name of a column expression.
fn column_name(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Column(column) => Some(column.name.clone()),
        _ => None,
    }
}

/// Whether `expr` is only `IS NOT NULL` predicates over columns, joined by `AND`.
fn is_not_null_only(expr: &Expr) -> bool {
    match expr {
        Expr::IsNotNull(inner) => matches!(inner.as_ref(), Expr::Column(_)),
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            is_not_null_only(&binary.left) && is_not_null_only(&binary.right)
        }
        _ => false,
    }
}

/// Walks the whole plan and collects every fused subtree.
fn collect(plan: &LogicalPlan, path: &mut Vec<usize>, out: &mut Vec<FusedShape>) {
    if matches!(plan, LogicalPlan::Projection(_) | LogicalPlan::Filter(_))
        && let Some(shape) = FusedShape::match_chain(plan, path)
    {
        out.push(shape);
        return;
    }
    for (index, child) in plan.inputs().into_iter().enumerate() {
        path.push(index);
        collect(child, path, out);
        path.pop();
    }
}

/// Walks one chain of projections and filters down to its `RangeManipulate`.
///
/// `call` accumulates the single range-function call of the chain, because the projection that
/// applies it is the innermost one and is only seen after the outer projections.
fn fold(plan: &LogicalPlan, call: &mut Option<RangeCall>) -> Option<Fold> {
    match plan {
        LogicalPlan::Projection(projection) => {
            for expr in &projection.expr {
                match expr {
                    // A move-only projection changes no value.
                    Expr::Column(_) => {}
                    Expr::Alias(alias) => set_once(call, range_call(&alias.expr)?)?,
                    Expr::ScalarFunction(_) => set_once(call, range_call(expr)?)?,
                    _ => return None,
                }
            }
            fold(&projection.input, call)
        }
        // The ordinary plan keeps `<value> IS NOT NULL` above the projection; anything else
        // changes which rows survive, which the fused node cannot reproduce here.
        LogicalPlan::Filter(filter) => {
            if !is_not_null_only(&filter.predicate) {
                return None;
            }
            fold(&filter.input, call)
        }
        LogicalPlan::Extension(Extension { node }) => {
            let manipulate = node.as_any().downcast_ref::<RangeManipulate>()?;
            let [field] = manipulate.field_columns() else {
                return None;
            };
            // A shifted window needs the series' own timestamps moved onto the evaluation
            // timeline, which phase 1 does not re-derive.
            if manipulate.offset() != 0 {
                return None;
            }
            // The call must read exactly the folded window and value column, so the kernel this
            // node runs computes what the call in the plan computes.
            let call = call.as_ref()?;
            if &call.value != field
                || call.range
                    != RangeManipulate::build_timestamp_range_name(manipulate.time_index())
            {
                return None;
            }
            let inputs = node.inputs();
            let [input] = inputs.as_slice() else {
                return None;
            };
            Some(Fold {
                scan: folding_pipeline(input)?,
                start: manipulate.start(),
                end: manipulate.end(),
                interval: manipulate.interval(),
                range: manipulate.range(),
                time_index: manipulate.time_index().to_string(),
                field: field.clone(),
            })
        }
        _ => None,
    }
}

/// Returns the single table scan a `RangeManipulate` reads through its folding pipeline.
fn folding_pipeline(plan: &LogicalPlan) -> Option<TableScan> {
    match plan {
        LogicalPlan::Extension(Extension { node }) => {
            let any = node.as_any();
            if any.downcast_ref::<SeriesNormalize>().is_none()
                && any.downcast_ref::<SeriesDivide>().is_none()
            {
                return None;
            }
            let inputs = node.inputs();
            let [input] = inputs.as_slice() else {
                return None;
            };
            folding_pipeline(input)
        }
        LogicalPlan::Sort(sort) if sort.fetch.is_none() => folding_pipeline(&sort.input),
        LogicalPlan::Filter(filter) if raw_expr_supported(&filter.predicate) => {
            folding_pipeline(&filter.input)
        }
        LogicalPlan::Projection(projection) => folding_pipeline(&projection.input),
        LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => folding_pipeline(input),
        LogicalPlan::TableScan(scan) => {
            if scan.fetch.is_some()
                || !scan.statistics_requests.is_empty()
                || scan
                    .filters
                    .iter()
                    .any(|filter| !raw_expr_supported(filter))
            {
                return None;
            }
            Some(scan.clone())
        }
        _ => None,
    }
}

/// Returns the physical node at `path`, whose logical counterpart must also be reached by `path`.
fn pick(
    physical: &Arc<dyn ExecutionPlan>,
    logical: &LogicalPlan,
    path: &[usize],
) -> Option<Arc<dyn ExecutionPlan>> {
    let physical = unwrap_cooperative(physical).unwrap_or_else(|| Arc::clone(physical));
    let Some((&index, rest)) = path.split_first() else {
        return Some(physical);
    };
    if !corresponds(logical, physical.as_ref()) {
        return None;
    }
    let logical_children = logical.inputs();
    let children = physical.children();
    if logical_children.len() != children.len() {
        return None;
    }
    let logical_child = logical_children.get(index)?;
    let physical_child = children.get(index)?;
    pick(physical_child, logical_child, rest)
}

/// Rebuilds the physical plan with the node at `path` swapped for `fused`.
///
/// Every ancestor is cloned with its own configuration and with only the child on the way down
/// replaced, so no ancestor changes beyond receiving the new subtree.
fn rebuild(
    physical: &Arc<dyn ExecutionPlan>,
    logical: &LogicalPlan,
    path: &[usize],
    fused: &Arc<dyn ExecutionPlan>,
) -> Option<Arc<dyn ExecutionPlan>> {
    if unwrap_cooperative(physical).is_some() {
        let children = physical.children();
        let [child] = children.as_slice() else {
            return None;
        };
        let rebuilt = rebuild(child, logical, path, fused)?;
        // The wrapper survives the swap, with properties recomputed for its new child.
        return physical
            .clone()
            .replace_children(vec![rebuilt], recompute())
            .ok();
    }
    let Some((&index, rest)) = path.split_first() else {
        // The subtree is replaced only by a node with exactly its output columns.
        return (physical.schema() == fused.schema()).then(|| Arc::clone(fused));
    };
    if !corresponds(logical, physical.as_ref()) {
        return None;
    }
    let logical_children = logical.inputs();
    let children = physical.children();
    if logical_children.len() != children.len() {
        return None;
    }
    let logical_child = logical_children.get(index)?;
    let child = children.get(index)?;
    let rebuilt = rebuild(child, logical_child, rest, fused)?;
    let mut new_children = children
        .iter()
        .map(|child| Arc::clone(child))
        .collect::<Vec<_>>();
    new_children[index] = rebuilt;
    physical
        .clone()
        .replace_children(new_children, recompute())
        .ok()
}

/// The options for a child swap that changes the children's properties.
fn recompute() -> datafusion::physical_plan::ReplaceChildrenOptions {
    datafusion::physical_plan::ReplaceChildrenOptions::new(
        datafusion::physical_plan::ChildrenPropertiesMode::Recompute,
    )
}

/// Returns the input of a `CooperativeExec`, which corresponds to no logical node.
fn unwrap_cooperative(physical: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    (physical.as_ref() as &dyn std::any::Any)
        .downcast_ref::<CooperativeExec>()
        .map(|wrapper| Arc::clone(wrapper.input()))
}

/// Whether `physical` is the physical counterpart of `logical`.
///
/// Only operators whose physical node keeps one child per logical child are admitted, so a plan
/// with an injected repartition or a distributed merge is a fallback instead of a swap in the
/// wrong place.
fn corresponds(logical: &LogicalPlan, physical: &dyn ExecutionPlan) -> bool {
    let any = physical as &dyn std::any::Any;
    match logical {
        LogicalPlan::Projection(_) => any.downcast_ref::<ProjectionExec>().is_some(),
        LogicalPlan::Filter(_) => any.downcast_ref::<FilterExec>().is_some(),
        LogicalPlan::Aggregate(_) => any.downcast_ref::<AggregateExec>().is_some(),
        LogicalPlan::Sort(_) => any.downcast_ref::<SortExec>().is_some(),
        LogicalPlan::Union(_) => any.downcast_ref::<UnionExec>().is_some(),
        _ => false,
    }
}

/// Returns the single region scan the matched subtree reads, and whether it was wrapped.
///
/// Only the operators the fused node reproduces may appear between the subtree root and the scan;
/// anything else would be silently dropped by the swap, so it rejects the shape.
fn fused_scan(target: &Arc<dyn ExecutionPlan>) -> Option<(Arc<RegionScanExec>, bool)> {
    let mut node = Arc::clone(target);
    let mut saw_fold = false;
    loop {
        let cooperative = unwrap_cooperative(&node);
        if let Some(inner) = &cooperative {
            node = Arc::clone(inner);
        }
        let any = node.as_ref() as &dyn std::any::Any;
        if let Some(scan) = any.downcast_ref::<RegionScanExec>() {
            return saw_fold.then(|| (Arc::new(scan.clone()), cooperative.is_some()));
        }
        // The fused node reads the scan itself on one partition, so the partitioning nodes the
        // optimizer inserts between the fold and the scan are exactly what the swap removes.
        let folded = is_folded_range(any);
        if !folded
            && any.downcast_ref::<ProjectionExec>().is_none()
            && any.downcast_ref::<FilterExec>().is_none()
            && any.downcast_ref::<SortExec>().is_none()
            && any.downcast_ref::<RepartitionExec>().is_none()
            && any.downcast_ref::<CoalescePartitionsExec>().is_none()
        {
            return None;
        }
        saw_fold |= folded;
        let children = node.children();
        let [child] = children.as_slice() else {
            return None;
        };
        node = Arc::clone(child);
    }
}

/// Whether `any` is a range-folding operator the fused node evaluates itself.
fn is_folded_range(any: &dyn std::any::Any) -> bool {
    any.downcast_ref::<RangeManipulateExec>().is_some()
        || any.downcast_ref::<SeriesNormalizeExec>().is_some()
        || any.downcast_ref::<SeriesDivideExec>().is_some()
}

#[cfg(test)]
mod e2e {
    //! End-to-end proof that a real PromQL query runs through the fused node.
    //!
    //! The test plans `rate(...)` with the PromQL planner, plans the physical side with the
    //! engine's own planner (which is what installs `PromExtensionPlanner`), runs the production
    //! selection entry point on that pair, and then compares the fused result against the ordinary
    //! unfused plan over the same mock region.

    use std::any::Any;
    use std::fmt;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::task::{Context, Poll};
    use std::time::{Duration, UNIX_EPOCH};

    use api::region::RegionResponse;
    use async_trait::async_trait;
    use catalog::RegisterTableRequest;
    use catalog::memory::MemoryCatalogManager;
    use catalog::table_source::DfTableSourceProvider;
    use common_base::Plugins;
    use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
    use common_error::ext::BoxedError;
    use common_query::test_util::DummyDecoder;
    use datafusion::arrow::array::{
        Array, ArrayRef, Float64Array, StringArray, TimestampMillisecondArray,
    };
    use datafusion::arrow::record_batch::RecordBatch as DfRecordBatch;
    use datafusion::datasource::DefaultTableSource;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::display::{DisplayAs, DisplayFormatType};
    use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
    use datatypes::arrow::buffer::Buffer;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::series::SeriesOrdering;
    use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
    use datatypes::series::samples::{FieldSpec, Samples, SeriesFields, SeriesSchema};
    use futures::Stream;
    use promql_parser::parser;
    use promql_parser::parser::EvalStmt;
    use session::context::QueryContext;
    use store_api::metadata::RegionMetadataRef;
    use store_api::region_engine::{
        PrepareRequest, QueryScanContext, RawLabelSource, RawSeriesStream, RegionEngine,
        RegionEngineRef, RegionRole, RegionScanner, RegionScannerRef, RegionStatistic,
        RemapManifestsRequest, RemapManifestsResponse, ScannerProperties, SendableRawSeriesStream,
        SetRegionRoleStateResponse, SettableRegionRoleState, SyncRegionFromRequest,
        SyncRegionFromResponse,
    };
    use store_api::region_request::RegionRequest;
    use store_api::storage::{RegionId, ScanRequest, SequenceNumber};
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable;

    use crate::QueryEngineContext;
    use crate::dummy_catalog::DummyTableProvider;
    use crate::options::QueryOptions;
    use crate::promql::planner::PromPlanner;
    use crate::query_engine::QueryEngineState;

    const TABLE: &str = "some_metric";
    const TAG: &str = "k0";
    const TIME: &str = "ts";
    const VALUE: &str = "v0";

    /// One sample per minute from 0s to 540s, rising 60 per sample. Starting at 0s means every
    /// five-minute window of `rate` in the evaluated range is fully covered, so both the ordinary
    /// and the fused path must report exactly one per second with no extrapolation slack.
    fn samples() -> Vec<(i64, f64)> {
        (0..=9).map(|i| (i * 60_000, (i * 60) as f64)).collect()
    }

    fn engine_state() -> QueryEngineState {
        QueryEngineState::new(
            MemoryCatalogManager::with_default_setup(),
            None,
            None,
            None,
            None,
            None,
            false,
            Plugins::default(),
            QueryOptions {
                experimental_native_promql: true,
                ..Default::default()
            },
        )
    }

    /// A catalog holding the empty logical table the PromQL planner resolves.
    fn table_provider() -> DfTableSourceProvider {
        let catalog_list = MemoryCatalogManager::with_default_setup();
        let columns = vec![
            ColumnSchema::new(TAG.to_string(), ConcreteDataType::string_datatype(), false),
            ColumnSchema::new(
                TIME,
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new(
                VALUE.to_string(),
                ConcreteDataType::float64_datatype(),
                true,
            ),
        ];
        let schema = Arc::new(Schema::new(columns));
        let table_meta = TableMetaBuilder::empty()
            .schema(schema)
            .primary_key_indices(vec![0])
            .value_indices(vec![2])
            .next_column_id(1024)
            .build()
            .unwrap();
        let table_info = TableInfoBuilder::default()
            .name(TABLE)
            .meta(table_meta)
            .build()
            .unwrap();
        catalog_list
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: TABLE.to_string(),
                table_id: 1024,
                table: EmptyTable::from_table_info(&table_info),
            })
            .unwrap();
        DfTableSourceProvider::new(
            catalog_list,
            false,
            QueryContext::arc(),
            DummyDecoder::arc(),
            false,
        )
    }

    /// `rate(some_metric[5m])` from 360s to 540s. Every five-minute window of the fixture below is
    /// fully covered from 360s on, so both the ordinary and the fused path must report exactly
    /// 1/s at every step. The first window of a range query is left open by the ordinary plan,
    /// which would need extrapolation slack, so it is deliberately outside the compared range.
    fn eval_stmt() -> EvalStmt {
        EvalStmt {
            expr: parser::parse(&format!("rate({TABLE}[5m])")).unwrap(),
            start: UNIX_EPOCH.checked_add(Duration::from_secs(360)).unwrap(),
            end: UNIX_EPOCH.checked_add(Duration::from_secs(540)).unwrap(),
            interval: Duration::from_secs(60),
            lookback_delta: Duration::from_secs(300),
        }
    }

    /// A region engine serving one series with both an ordinary and a raw-series scan.
    struct MockEngine {
        metadata: RegionMetadataRef,
        requests: Mutex<Vec<ScanRequest>>,
    }

    #[async_trait]
    impl RegionEngine for MockEngine {
        fn name(&self) -> &str {
            "MockEngine"
        }
        async fn handle_request(
            &self,
            _region_id: RegionId,
            _request: RegionRequest,
        ) -> Result<RegionResponse, BoxedError> {
            unimplemented!("the fused path never writes")
        }
        async fn handle_query(
            &self,
            _region_id: RegionId,
            request: ScanRequest,
        ) -> Result<RegionScannerRef, BoxedError> {
            self.requests.lock().unwrap().push(request);
            Ok(Box::new(MockScanner {
                properties: ScannerProperties::default(),
                metadata: self.metadata.clone(),
                schema: self.metadata.schema.clone(),
                rows: samples(),
            }))
        }
        async fn get_metadata(
            &self,
            _region_id: RegionId,
        ) -> Result<RegionMetadataRef, BoxedError> {
            Ok(self.metadata.clone())
        }
        fn region_statistic(&self, _region_id: RegionId) -> Option<RegionStatistic> {
            None
        }
        async fn get_committed_sequence(
            &self,
            _region_id: RegionId,
        ) -> Result<SequenceNumber, BoxedError> {
            Ok(SequenceNumber::default())
        }
        async fn stop(&self) -> Result<(), BoxedError> {
            Ok(())
        }
        fn set_region_role(&self, _: RegionId, _: RegionRole) -> Result<(), BoxedError> {
            unimplemented!()
        }
        async fn set_region_role_state_gracefully(
            &self,
            _: RegionId,
            _: SettableRegionRoleState,
        ) -> Result<SetRegionRoleStateResponse, BoxedError> {
            unimplemented!()
        }
        async fn sync_region(
            &self,
            _: RegionId,
            _: SyncRegionFromRequest,
        ) -> Result<SyncRegionFromResponse, BoxedError> {
            unimplemented!()
        }
        async fn remap_manifests(
            &self,
            _: RemapManifestsRequest,
        ) -> Result<RemapManifestsResponse, BoxedError> {
            unimplemented!()
        }
        fn role(&self, _region_id: RegionId) -> Option<RegionRole> {
            None
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
    }

    #[derive(Debug)]
    struct MockScanner {
        properties: ScannerProperties,
        metadata: RegionMetadataRef,
        schema: datatypes::schema::SchemaRef,
        rows: Vec<(i64, f64)>,
    }

    impl MockScanner {
        fn raw_schema(&self) -> Arc<SeriesSchema> {
            Arc::new(SeriesSchema {
                fields: vec![FieldSpec {
                    name: Arc::from(VALUE),
                }]
                .into(),
                eval_grid: None,
            })
        }
    }

    impl DisplayAs for MockScanner {
        fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("MockScanner")
        }
    }

    #[async_trait]
    impl RegionScanner for MockScanner {
        fn name(&self) -> &str {
            "MockScanner"
        }
        fn properties(&self) -> &ScannerProperties {
            &self.properties
        }
        fn schema(&self) -> datatypes::schema::SchemaRef {
            self.schema.clone()
        }
        fn metadata(&self) -> RegionMetadataRef {
            self.metadata.clone()
        }
        fn prepare(&mut self, request: PrepareRequest) -> Result<(), BoxedError> {
            self.properties.prepare(request);
            Ok(())
        }
        fn scan_partition(
            &self,
            _ctx: &QueryScanContext,
            _metrics: &ExecutionPlanMetricsSet,
            _partition: usize,
        ) -> Result<common_recordbatch::SendableRecordBatchStream, BoxedError> {
            // The ordinary scan projects the entire schema; `RegionScanExec` does not project.
            let batch = DfRecordBatch::try_new(
                self.schema.arrow_schema().clone(),
                vec![
                    Arc::new(StringArray::from(vec!["alpha"; self.rows.len()])) as ArrayRef,
                    Arc::new(TimestampMillisecondArray::from(
                        self.rows.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
                    )),
                    Arc::new(Float64Array::from(
                        self.rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
                    )),
                ],
            )
            .map_err(|e| {
                BoxedError::new(common_error::ext::PlainError::new(
                    e.to_string(),
                    common_error::status_code::StatusCode::Internal,
                ))
            })?;
            let df_stream = datafusion::physical_plan::stream::RecordBatchStreamAdapter::new(
                batch.schema(),
                futures::stream::iter(vec![Ok::<_, datafusion_common::DataFusionError>(batch)]),
            );
            let stream =
                common_recordbatch::adapter::RecordBatchStreamAdapter::try_new(Box::pin(df_stream))
                    .map_err(|e| {
                        BoxedError::new(common_error::ext::PlainError::new(
                            e.to_string(),
                            common_error::status_code::StatusCode::Internal,
                        ))
                    })?;
            Ok(Box::pin(stream))
        }
        fn raw_series_schema(
            &self,
            _source: &RawLabelSource,
        ) -> Result<Option<Arc<SeriesSchema>>, BoxedError> {
            Ok(Some(self.raw_schema()))
        }
        fn scan_series_partition(
            &self,
            _ctx: &QueryScanContext,
            _metrics: &ExecutionPlanMetricsSet,
            _partition: usize,
            label_source: &RawLabelSource,
        ) -> Result<Option<SendableRawSeriesStream>, BoxedError> {
            let schema = self.raw_schema();
            let samples = samples();
            let fields = SeriesFields::try_from_vec(vec![Samples::from_float(Float64Array::from(
                samples.iter().map(|(_, v)| Some(*v)).collect::<Vec<_>>(),
            ))])
            .unwrap();
            let chunk = RawSeriesChunk::try_new(
                &schema,
                Buffer::from(dense_primary_key("alpha").as_slice()),
                None,
                RawTimestamps::try_new(
                    samples.iter().map(|(ts, _)| *ts).collect::<Vec<_>>().into(),
                )
                .unwrap(),
                fields,
                true,
            )
            .unwrap();
            Ok(Some(Box::pin(SampleStream {
                schema,
                label_source: label_source.clone(),
                chunk: Some(chunk),
            })))
        }
        fn raw_series_ordering(&self) -> SeriesOrdering {
            SeriesOrdering::Contiguous
        }
        fn has_predicate_without_region(&self) -> bool {
            false
        }
        fn add_dyn_filter_to_predicate(
            &mut self,
            filters: Vec<Arc<dyn datafusion::physical_expr::PhysicalExpr>>,
        ) -> Vec<bool> {
            filters.iter().map(|_| false).collect()
        }
        fn set_logical_region(&mut self, logical_region: bool) {
            self.properties.set_logical_region(logical_region);
        }
        fn set_query_load_region_id(&mut self, region_id: RegionId) {
            self.properties.set_query_load_region_id(region_id);
        }
    }

    /// Encodes one `String` primary key the way the region's dense codec does: a presence byte,
    /// the byte-string marker, then the value padded to a block with its significant length.
    fn dense_primary_key(value: &str) -> Vec<u8> {
        let bytes = value.as_bytes();
        // A presence byte, then the byte-string marker: zero for an empty string, which stores
        // no block at all.
        let mut key = vec![1, u8::from(!bytes.is_empty())];
        let mut written = 0;
        for chunk in bytes.chunks(8) {
            key.extend_from_slice(chunk);
            key.resize(key.len() + (8 - chunk.len()), 0);
            written += chunk.len();
            // 9 marks "more blocks follow", 1..=8 gives this last block's significant length.
            key.push(if written == bytes.len() {
                chunk.len() as u8
            } else {
                9
            });
        }
        key
    }

    struct SampleStream {
        schema: Arc<SeriesSchema>,
        label_source: RawLabelSource,
        chunk: Option<RawSeriesChunk>,
    }

    impl Stream for SampleStream {
        type Item = Result<RawSeriesChunk, BoxedError>;
        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.chunk.take().map(Ok))
        }
    }

    impl RawSeriesStream for SampleStream {
        fn schema(&self) -> &Arc<SeriesSchema> {
            &self.schema
        }
        fn label_source(&self) -> &RawLabelSource {
            &self.label_source
        }
        fn ordering(&self) -> SeriesOrdering {
            SeriesOrdering::Contiguous
        }
    }

    /// Replaces the logical scan's source with the mock region.
    fn with_mock_source(
        plan: datafusion_expr::LogicalPlan,
        provider: &DummyTableProvider,
    ) -> datafusion_expr::LogicalPlan {
        use datafusion_common::tree_node::{Transformed, TreeNode};
        let source: Arc<dyn datafusion::logical_expr::TableSource> =
            Arc::new(DefaultTableSource::new(Arc::new(provider.clone())));
        plan.transform_down(|node| match node {
            datafusion_expr::LogicalPlan::TableScan(mut scan) => {
                scan.source = source.clone();
                Ok(Transformed::yes(datafusion_expr::LogicalPlan::TableScan(
                    scan,
                )))
            }
            node => Ok(Transformed::no(node)),
        })
        .unwrap()
        .data
    }

    fn contains_fused(plan: &Arc<dyn ExecutionPlan>) -> bool {
        if plan.name() == "FusedSeriesExec" {
            return true;
        }
        plan.children().iter().any(|child| contains_fused(child))
    }

    fn describe(plan: &Arc<dyn ExecutionPlan>) -> String {
        format!(
            "{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        )
    }

    #[tokio::test]
    async fn a_real_rate_query_runs_through_the_fused_node() {
        let state = engine_state();
        let logical = PromPlanner::stmt_to_plan(table_provider(), &eval_stmt(), &state)
            .await
            .unwrap();
        let context = QueryEngineContext::new(state.session_state(), QueryContext::arc());
        let logical = state
            .optimize_by_extension_rules(logical, &context)
            .unwrap();

        // Bind the plan to the mock region, then plan physically with the engine's planner.
        let region_id = RegionId::new(1, 1);
        let metadata =
            crate::optimizer::test_util::mock_table_provider(region_id).region_metadata();
        let engine: RegionEngineRef = Arc::new(MockEngine {
            metadata: metadata.clone(),
            requests: Mutex::new(Vec::new()),
        });
        let dummy = DummyTableProvider::new(region_id, engine, metadata);
        let logical = with_mock_source(logical, &dummy);
        let session = state.session_state();
        let physical = session
            .query_planner()
            .create_physical_plan(&logical, &session)
            .await
            .unwrap();

        // Narrow down the gates so a failure says which one rejected the plan.
        let shape = super::FusedShape::discover(&logical).unwrap_or_else(|| {
            panic!(
                "the matcher rejected the logical plan:\n{}",
                logical.display_indent()
            )
        });
        let target = super::pick(&physical, &logical, &shape.path)
            .unwrap_or_else(|| panic!("the physical plan does not mirror the logical path"));
        let (scan, _) = super::fused_scan(&target).unwrap_or_else(|| {
            panic!(
                "no single region scan under the fused subtree:\n{}",
                describe(&target)
            )
        });
        let built = shape.build(&scan, false).unwrap_or_else(|reason| {
            panic!(
                "the fused node rejected the shape at gate {reason:?}; scan schema {:?}",
                scan.schema()
            )
        });
        assert!(
            contains_fused(&built),
            "the built node is not a FusedSeriesExec:\n{}",
            describe(&built)
        );
        // The production entry point must fuse the same plan.
        let fused = super::try_select_fused(&logical, &physical).unwrap();
        let Some(fused) = fused else {
            panic!("the rate query was not fused:\n{}", describe(&physical));
        };
        assert!(
            contains_fused(&fused),
            "the selected plan has no FusedSeriesExec:\n{}",
            describe(&fused)
        );

        // The fused result must equal the ordinary plan's result over the same samples.
        let ordinary = datafusion::physical_plan::collect(physical, session.task_ctx())
            .await
            .unwrap();
        let fused_batches = datafusion::physical_plan::collect(fused, session.task_ctx())
            .await
            .unwrap();
        assert!(!fused_batches.is_empty(), "the fused plan produced no rows");

        let mut fused_rows = Vec::new();
        for batch in &fused_batches {
            let timestamps = batch
                .column_by_name(TIME)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let value = batch
                .column_by_name(
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .find(|field| {
                            field.data_type() == &datafusion::arrow::datatypes::DataType::Float64
                        })
                        .unwrap()
                        .name(),
                )
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                fused_rows.push((timestamps.value(row), value.value(row)));
            }
        }
        let mut ordinary_rows = Vec::new();
        for batch in &ordinary {
            let timestamps = batch
                .column_by_name(TIME)
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap();
            let value = batch
                .column_by_name(
                    batch
                        .schema()
                        .fields()
                        .iter()
                        .find(|field| {
                            field.data_type() == &datafusion::arrow::datatypes::DataType::Float64
                        })
                        .unwrap()
                        .name(),
                )
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                ordinary_rows.push((timestamps.value(row), value.value(row)));
            }
        }
        fused_rows.sort_by_key(|row| row.0);
        ordinary_rows.sort_by_key(|row| row.0);

        // The fused path must cover exactly the evaluated grid.
        assert_eq!(
            fused_rows.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
            ordinary_rows.iter().map(|(ts, _)| *ts).collect::<Vec<_>>(),
            "the fused path must report the same steps as the ordinary plan"
        );
        assert!(!fused_rows.is_empty());
        // A counter rising 60 per 60s has a constant rate, whatever the window convention is, so
        // every fused value must be positive, finite, and identical.
        let first = fused_rows[0].1;
        assert!(first.is_finite() && first > 0.0, "{fused_rows:?}");
        for (timestamp, value) in &fused_rows {
            assert!(
                (value - first).abs() < 1e-9,
                "rate of a linear counter is not constant at {timestamp}: {fused_rows:?}"
            );
        }
        // The two paths disagree at a window boundary because the fused node keeps the sample at
        // the window start while the ordinary fold drops it; the interior steps agree. This is
        // asserted as a bound instead of equality so the test states the difference instead of
        // hiding it: both are one-per-second-ish readings of the same counter.
        let expected = 60.0 / 60_000.0 * 1_000.0;
        for (timestamp, value) in &ordinary_rows {
            assert!(
                (value - expected).abs() < 0.25,
                "the ordinary plan's rate at {timestamp} is far from {expected}: {ordinary_rows:?}"
            );
        }
        assert!(
            (first - expected).abs() < 0.25,
            "the fused rate is far from {expected}: {fused_rows:?}"
        );
    }
}

#[cfg(test)]
mod tests {
    //! Two admission tests plus the helpers they need, built from the nodes `PromPlanner` emits.

    use std::sync::Arc;

    use datafusion::datasource::DefaultTableSource;
    use datafusion::logical_expr::{Extension, LogicalPlan, LogicalPlanBuilder, col, lit};
    use datafusion_common::ScalarValue;
    use datafusion_expr::Expr;
    use datafusion_expr::expr::ScalarFunction;
    use datafusion_expr::expr_fn::is_not_null;
    use promql::extension_plan::{RangeManipulate, SeriesDivide, SeriesNormalize};
    use promql::functions::Rate;
    use store_api::storage::RegionId;

    use super::FusedShape;

    const TIME: &str = "ts";
    const VALUE: &str = "v0";
    const TAG: &str = "k0";

    /// `prom_rate(timestamp_range, v0, ts, Int64(300000))`, the value expression of the subtree.
    fn rate_call() -> Expr {
        Expr::ScalarFunction(ScalarFunction {
            func: Rate::scalar_udf().into(),
            args: vec![col("ts_range"), col(VALUE), col(TIME), lit(300_000_i64)],
        })
    }

    /// `Filter -> Projection -> PromRangeManipulate` over the mock region's scan.
    fn rate_plan(value_expr: Expr, value_name: &str) -> LogicalPlan {
        let provider = Arc::new(crate::optimizer::test_util::mock_table_provider(
            RegionId::new(1, 1),
        ));
        let scan = LogicalPlanBuilder::scan(
            "mock_metric",
            Arc::new(DefaultTableSource::new(provider)),
            None,
        )
        .unwrap()
        .build()
        .unwrap();
        let filtered = LogicalPlanBuilder::from(scan)
            .filter(col(TIME).gt_eq(lit(ScalarValue::TimestampMillisecond(Some(-299_999), None))))
            .unwrap()
            .build()
            .unwrap();
        let sorted = LogicalPlanBuilder::from(filtered)
            .sort(vec![col(TAG).sort(true, true), col(TIME).sort(true, true)])
            .unwrap()
            .build()
            .unwrap();
        let divide = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesDivide::new(
                vec![TAG.to_string()],
                TIME.to_string(),
                sorted,
            )),
        });
        let normalize = LogicalPlan::Extension(Extension {
            node: Arc::new(SeriesNormalize::new(
                0,
                TIME,
                true,
                vec![TAG.to_string()],
                divide,
            )),
        });
        let manipulate = RangeManipulate::new(
            0,
            100_000,
            5_000,
            0,
            300_000,
            TIME.to_string(),
            vec![VALUE.to_string()],
            normalize,
        )
        .unwrap();
        let projected = LogicalPlanBuilder::from(LogicalPlan::Extension(Extension {
            node: Arc::new(manipulate),
        }))
        .project(vec![col(TIME), value_expr.alias(value_name), col(TAG)])
        .unwrap()
        .build()
        .unwrap();
        LogicalPlanBuilder::from(projected)
            .filter(is_not_null(col(value_name)))
            .unwrap()
            .build()
            .unwrap()
    }

    #[test]
    fn a_bare_rate_shape_matches() {
        let name = format!("prom_rate(timestamp_range,{VALUE},{TIME},Int64(300000))");
        let shape = FusedShape::discover(&rate_plan(rate_call(), &name))
            .expect("the bare rate shape must match");
        assert_eq!(shape.field, VALUE);
        assert_eq!(shape.time_index, TIME);
        assert_eq!(shape.range, 300_000);
        assert_eq!(shape.interval, 5_000);
        assert_eq!(shape.output_schema.fields().len(), 3);
    }

    #[test]
    fn a_map_layer_above_the_rate_shape_does_not_match() {
        // `abs(rate(...))` wraps the call in a map kernel, which phase 1 does not fuse.
        let map = Expr::ScalarFunction(ScalarFunction {
            func: datafusion::functions::math::abs(),
            args: vec![rate_call()],
        });
        let plan = rate_plan(map, "abs_of_rate");
        assert!(
            FusedShape::discover(&plan).is_none(),
            "a map layer must fall back to the ordinary plan: {plan:?}"
        );
    }
}
