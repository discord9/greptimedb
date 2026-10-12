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

//! Automatic selection of the raw-factorized `COUNT(*)` execution path.
//!
//! [`count_session`] registers two in-memory tables and, when the setup preflight
//! accepts the fixtures, installs a [`QueryPlanner`] wrapper that recognizes exactly
//! one bounded shape: an inner equi-join of the two registered providers on `host`,
//! grouped by `(host, area)`, with a single plain `COUNT(*)`. A matching plan is
//! spliced to [`RawFactorExec`] + [`FactorAggregateExec`] in place of the ordinary
//! join/aggregate subtree; every other shape delegates to the default planner and
//! returns its baseline plan unchanged. There is no execution-time retry or replay.

use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float64Array, RecordBatch};
use arrow_schema::{DataType, Schema};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Column, DFSchema, DataFusionError, Result};
use datafusion::datasource::{MemTable, source_as_provider};
use datafusion::execution::context::{QueryPlanner, SessionConfig, SessionContext};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::utils::COUNT_STAR_EXPANSION;
use datafusion::logical_expr::{Aggregate, Expr, Join, JoinConstraint, JoinType, LogicalPlan};
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ExecutionPlan, ReplaceChildrenOptions};

use crate::{FactorAggregateExec, RawFactorExec};

/// Session configuration shared by the factorized session and its plain baseline.
///
/// One target partition with repartitioning disabled keeps the experiment plans
/// deterministic and single-partition bounded.
pub fn count_config() -> SessionConfig {
    SessionConfig::new()
        .with_target_partitions(1)
        .with_repartition_joins(false)
        .with_repartition_aggregations(false)
}

/// Registers `facts`/`dims` and installs the factorized `COUNT(*)` selection when
/// the setup preflight accepts the fixtures.
///
/// The preflight runs on the actual batches, never on caller-supplied counts:
/// non-finite fact values or dimension weights, unexpected table shapes, or row
/// totals that cannot fit the Arrow `List` offset capacity simply leave the session
/// planning normally, while an input pair bound above `i64::MAX` refuses setup.
pub fn count_session(facts: RecordBatch, dims: RecordBatch) -> Result<SessionContext> {
    // Setup-time preflight on the actual batches: refuse setup when the pair
    // bound overflows, and otherwise install the selection only for eligible
    // shapes with finite payloads and row totals within the Arrow list capacity.
    let selected = select_factor_plan(
        facts.num_rows(),
        dims.num_rows(),
        payload_finite(&facts, &dims),
    )?;
    let selected =
        selected && fact_batch_ok(facts.schema().as_ref()) && dim_batch_ok(dims.schema().as_ref());

    let facts: Arc<dyn TableProvider> =
        Arc::new(MemTable::try_new(facts.schema(), vec![vec![facts]])?);
    let dims: Arc<dyn TableProvider> =
        Arc::new(MemTable::try_new(dims.schema(), vec![vec![dims]])?);
    let config = count_config();
    if !selected {
        let ctx = SessionContext::new_with_config(config);
        ctx.register_table("facts", facts)?;
        ctx.register_table("dims", dims)?;
        return Ok(ctx);
    }

    // Capture the default planner from a built state and wrap it, so unmatched
    // shapes keep the exact baseline planning path.
    let default_state = SessionStateBuilder::new()
        .with_config(config.clone())
        .with_default_features()
        .build();
    let inner = SessionState::query_planner(&default_state).clone();
    let planner: Arc<dyn QueryPlanner + Send + Sync> = Arc::new(CountQueryPlanner {
        inner,
        facts: Arc::clone(&facts),
        dims: Arc::clone(&dims),
    });
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_query_planner(planner)
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_table("facts", facts)?;
    ctx.register_table("dims", dims)?;
    Ok(ctx)
}

/// Refuses setup when the join can produce more pairs than an `i64` count holds.
pub fn checked_pair_bound(fact_rows: usize, dim_rows: usize) -> Result<i64> {
    let facts = i64::try_from(fact_rows).map_err(|_| pair_bound_error())?;
    let dims = i64::try_from(dim_rows).map_err(|_| pair_bound_error())?;
    facts.checked_mul(dims).ok_or_else(pair_bound_error)
}

fn pair_bound_error() -> DataFusionError {
    DataFusionError::Plan(
        "raw factorization requires fact_rows * dim_rows <= i64::MAX join pairs".into(),
    )
}

/// Conservative row bound: the factor payload keeps one row per fact and per
/// dimension row, and Arrow `List` offsets are `i32`.
pub fn payload_rows_fit(fact_rows: usize, dim_rows: usize) -> bool {
    fact_rows
        .checked_add(dim_rows)
        .is_some_and(|total| total <= i32::MAX as usize)
}

/// Setup-time decision for installing the factorized selection.
///
/// [`count_session`] calls this with row counts and finiteness derived from the
/// actual batches. Overflowing pair bounds refuse setup with an error; everything
/// else that is not provably usable leaves the session on the default planner.
pub fn select_factor_plan(fact_rows: usize, dim_rows: usize, finite_payload: bool) -> Result<bool> {
    checked_pair_bound(fact_rows, dim_rows)?;
    Ok(finite_payload && payload_rows_fit(fact_rows, dim_rows))
}

/// Planner that splices the factorized `COUNT(*)` subtree into the baseline plan.
#[derive(Debug)]
struct CountQueryPlanner {
    inner: Arc<dyn QueryPlanner + Send + Sync>,
    facts: Arc<dyn TableProvider>,
    dims: Arc<dyn TableProvider>,
}

#[async_trait]
impl QueryPlanner for CountQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The delegated plan is authoritative: it is returned unchanged whenever a
        // shape does not match, the factor plan cannot be built, or the splice fails.
        let baseline = self
            .inner
            .create_physical_plan(logical_plan, session)
            .await?;
        let Some(aggregate) = match_count_shape(logical_plan, &self.facts, &self.dims) else {
            return Ok(baseline);
        };
        let Ok(replacement) = build_factor_plan(session, &self.facts, &self.dims, aggregate).await
        else {
            return Ok(baseline);
        };
        Ok(splice_factor_plan(&baseline, &replacement, &aggregate.schema).unwrap_or(baseline))
    }
}

/// Locates the experiment aggregate below unary sort/limit/projection ancestors.
///
/// Those ancestors only reorder, limit, or rename the aggregate's complete output,
/// so they stay in place above the spliced factor plan. Anything else stops the
/// walk and falls back to the baseline plan.
fn match_count_shape<'a>(
    plan: &'a LogicalPlan,
    facts: &Arc<dyn TableProvider>,
    dims: &Arc<dyn TableProvider>,
) -> Option<&'a Aggregate> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => match_count_aggregate(aggregate, facts, dims),
        LogicalPlan::Sort(sort) => match_count_shape(&sort.input, facts, dims),
        LogicalPlan::Limit(limit) => match_count_shape(&limit.input, facts, dims),
        LogicalPlan::Projection(projection) => match_count_shape(&projection.input, facts, dims),
        _ => None,
    }
}

/// Matches `GROUP BY host, area ... COUNT(*)` over the registered equi-join.
fn match_count_aggregate<'a>(
    aggregate: &'a Aggregate,
    facts: &Arc<dyn TableProvider>,
    dims: &Arc<dyn TableProvider>,
) -> Option<&'a Aggregate> {
    if aggregate.group_expr.len() != 2 || aggregate.aggr_expr.len() != 1 {
        return None;
    }
    if !group_columns_match(&aggregate.group_expr, facts, dims) {
        return None;
    }
    if !is_plain_count_star(&aggregate.aggr_expr[0]) {
        return None;
    }
    match_join_input(&aggregate.input, facts, dims).map(|_| aggregate)
}

/// Finds the registered equi-join below the aggregate input.
///
/// The optimizer inserts a plain column-selection projection between the
/// aggregate and the join to drop unreferenced columns; such a projection keeps
/// every remaining column under its own name, so `host`/`area` resolve to the
/// same join columns. Any other input plan is refused.
fn match_join_input<'a>(
    plan: &'a LogicalPlan,
    facts: &Arc<dyn TableProvider>,
    dims: &Arc<dyn TableProvider>,
) -> Option<&'a Join> {
    match plan {
        LogicalPlan::Join(join) => join_matches(join, facts, dims).then_some(join),
        LogicalPlan::Projection(projection) if is_column_selection(projection) => {
            match_join_input(&projection.input, facts, dims)
        }
        _ => None,
    }
}

/// True for a projection that only re-selects columns under their own names.
fn is_column_selection(projection: &datafusion::logical_expr::Projection) -> bool {
    projection.expr.len() == projection.schema.fields().len()
        && projection
            .expr
            .iter()
            .zip(projection.schema.fields())
            .all(|(expr, field)| match expr {
                Expr::Column(column) => &column.name == field.name(),
                _ => false,
            })
}

/// Requires the two group keys to be the `host` and `area` columns of the two
/// registered providers. Both join sides carry `host`, and an inner equi-join on
/// `host` makes either side's value the same grouping key; `area` exists only in
/// the dimension provider because `count_session` pins both shapes.
fn group_columns_match(
    group_expr: &[Expr],
    facts: &Arc<dyn TableProvider>,
    dims: &Arc<dyn TableProvider>,
) -> bool {
    let [host, area] = group_expr else {
        return false;
    };
    let (Some(host), Some(area)) = (as_column(host), as_column(area)) else {
        return false;
    };
    host.name == "host"
        && area.name == "area"
        && dims.schema().index_of("host").is_ok()
        && facts.schema().index_of("host").is_ok()
        && dims.schema().index_of("area").is_ok()
}

fn as_column(expr: &Expr) -> Option<&Column> {
    match expr {
        Expr::Column(column) => Some(column),
        _ => None,
    }
}
/// Accepts only a plain `COUNT(*)`: no distinct, filter, ordering, or null
/// treatment, and the count-star argument the SQL planner produces.
fn is_plain_count_star(expr: &Expr) -> bool {
    let expr = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    let Expr::AggregateFunction(aggregate) = expr else {
        return false;
    };
    aggregate.func.name() == "count"
        && !aggregate.params.distinct
        && aggregate.params.filter.is_none()
        && aggregate.params.order_by.is_empty()
        && aggregate.params.null_treatment.is_none()
        && matches!(
            aggregate.params.args.as_slice(),
            [Expr::Literal(value, _)] if value == &COUNT_STAR_EXPANSION
        )
}

/// Requires an inner equi-join of the two registered providers on `host` without a
/// join filter or any scan-level filter/fetch that would carry semantics.
fn join_matches(
    join: &Join,
    facts: &Arc<dyn TableProvider>,
    dims: &Arc<dyn TableProvider>,
) -> bool {
    if join.join_type != JoinType::Inner
        || join.join_constraint != JoinConstraint::On
        || join.filter.is_some()
        || join.on.len() != 1
    {
        return false;
    }
    let (Some(left), Some(right)) = (as_column(&join.on[0].0), as_column(&join.on[0].1)) else {
        return false;
    };
    if left.name != "host" || right.name != "host" {
        return false;
    }
    let (Some(left), Some(right)) = (scan_provider(&join.left), scan_provider(&join.right)) else {
        return false;
    };
    (Arc::ptr_eq(&left, facts) && Arc::ptr_eq(&right, dims))
        || (Arc::ptr_eq(&left, dims) && Arc::ptr_eq(&right, facts))
}

/// Recovers the provider of a scan (optionally under a subquery alias).
///
/// Scans that already carry filters or a fetch limit are refused: the factor plan
/// reads full rows from the provider and would silently drop their semantics.
fn scan_provider(plan: &LogicalPlan) -> Option<Arc<dyn TableProvider>> {
    match plan {
        LogicalPlan::TableScan(scan) if scan.filters.is_empty() && scan.fetch.is_none() => {
            source_as_provider(&scan.source).ok()
        }
        LogicalPlan::SubqueryAlias(alias) => scan_provider(&alias.input),
        _ => None,
    }
}

/// Builds the factorized subtree for a matched logical aggregate.
///
/// The scans use the same providers with no projection, filters, or limit, and
/// [`RawFactorExec::try_new`] validates their single-partition bounded
/// `Int64`/`Float64` shapes. The final projection restores the aggregate's output
/// field names so ancestors can keep referencing it positionally.
async fn build_factor_plan(
    session: &dyn Session,
    facts: &Arc<dyn TableProvider>,
    dims: &Arc<dyn TableProvider>,
    aggregate: &Aggregate,
) -> Result<Arc<dyn ExecutionPlan>> {
    let fact_scan = facts.scan(session, None, &[], None).await?;
    let dim_scan = dims.scan(session, None, &[], None).await?;
    let raw = Arc::new(RawFactorExec::try_new(fact_scan, dim_scan)?);
    let factor: Arc<dyn ExecutionPlan> = Arc::new(FactorAggregateExec::try_new(raw)?);
    let projection = vec![
        (
            Arc::new(PhysicalColumn::new("host", 0)) as Arc<dyn PhysicalExpr>,
            aggregate.schema.field(0).name().to_string(),
        ),
        (
            Arc::new(PhysicalColumn::new("area", 1)),
            aggregate.schema.field(1).name().to_string(),
        ),
        (
            Arc::new(PhysicalColumn::new("pairs", 2)),
            aggregate.schema.field(2).name().to_string(),
        ),
    ];
    Ok(Arc::new(ProjectionExec::try_new(projection, factor)?))
}

/// Replaces the first physical aggregate whose schema matches the logical
/// aggregate, rebuilding unary ancestors such as sort/limit nodes around it.
///
/// Reaching a multi-child node, a leaf, or an aggregate with a different schema
/// reports no match, and the caller keeps the untouched baseline plan.
fn splice_factor_plan(
    baseline: &Arc<dyn ExecutionPlan>,
    replacement: &Arc<dyn ExecutionPlan>,
    target: &DFSchema,
) -> Option<Arc<dyn ExecutionPlan>> {
    if baseline.is::<AggregateExec>() {
        return schema_matches(baseline.schema().as_ref(), target).then(|| Arc::clone(replacement));
    }
    let children = baseline.children();
    if children.len() != 1 {
        return None;
    }
    let child = splice_factor_plan(children[0], replacement, target)?;
    baseline
        .clone()
        .replace_children(
            vec![child],
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
        .ok()
}

/// Compares physical and logical aggregate schemas by name, type, and nullability.
fn schema_matches(physical: &Schema, logical: &DFSchema) -> bool {
    physical.fields().len() == logical.fields().len()
        && physical
            .fields()
            .iter()
            .zip(logical.fields())
            .all(|(physical, logical)| {
                physical.name() == logical.name()
                    && physical.data_type() == logical.data_type()
                    && physical.is_nullable() == logical.is_nullable()
            })
}

fn fact_batch_ok(schema: &Schema) -> bool {
    schema.fields().len() == 2
        && schema.field(0).name() == "host"
        && schema.field(0).data_type() == &DataType::Int64
        && schema.field(1).name() == "value"
        && schema.field(1).data_type() == &DataType::Float64
}

fn dim_batch_ok(schema: &Schema) -> bool {
    schema.fields().len() == 3
        && schema.field(0).name() == "host"
        && schema.field(0).data_type() == &DataType::Int64
        && schema.field(1).name() == "area"
        && schema.field(1).data_type() == &DataType::Int64
        && schema.field(2).name() == "weight"
        && schema.field(2).data_type() == &DataType::Float64
}

/// True when every non-null fact value and dimension weight is finite.
fn payload_finite(facts: &RecordBatch, dims: &RecordBatch) -> bool {
    facts.num_columns() == 2
        && dims.num_columns() == 3
        && finite_column(facts.column(1))
        && finite_column(dims.column(2))
}

fn finite_column(column: &ArrayRef) -> bool {
    let Some(values) = column.as_any().downcast_ref::<Float64Array>() else {
        return false;
    };
    (0..values.len()).all(|index| values.is_null(index) || values.value(index).is_finite())
}
