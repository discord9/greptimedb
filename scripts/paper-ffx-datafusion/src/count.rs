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

//! Automatic selection of the raw-factorized `COUNT(*)` and exact-domain
//! `AVG(value * weight)` execution paths.
//!
//! [`count_session`] registers two in-memory tables and, when the setup preflight
//! accepts the fixtures, installs a [`QueryPlanner`] wrapper that recognizes two
//! bounded shapes over the two registered providers:
//!
//! * an inner equi-join on `host` grouped by `(host, area)` with a single plain
//!   `COUNT(*)`; and
//! * the same join and grouping with a single `AVG(facts.value * dims.weight)`,
//!   which is only admissible when [`exact_avg_domain`] proves the actual batches
//!   sit in the exact-equivalence domain (see that function for the conditions).
//!
//! A matching plan is spliced to [`RawFactorExec`] + [`FactorAggregateExec`] in
//! place of the ordinary join/aggregate subtree; every other shape — including a
//! matching `AVG` shape whose batches are outside the exact domain — delegates to
//! the default planner and returns its baseline plan unchanged. There is no
//! execution-time retry, replay, or approximation.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Schema};
use async_trait::async_trait;
use datafusion::catalog::{Session, TableProvider};
use datafusion::common::{Column, DFSchema, DataFusionError, Result};
use datafusion::datasource::{MemTable, source_as_provider};
use datafusion::execution::context::{QueryPlanner, SessionConfig, SessionContext};
use datafusion::execution::session_state::{SessionState, SessionStateBuilder};
use datafusion::logical_expr::utils::COUNT_STAR_EXPANSION;
use datafusion::logical_expr::{
    Aggregate, Expr, Join, JoinConstraint, JoinType, LogicalPlan, Operator,
};
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

/// Registers `facts`/`dims` and installs the factorized selection.
///
/// [`count_session`] enables every admissible shape: `COUNT(*)` whenever the
/// setup preflight accepts the fixtures, and `AVG(value * weight)` additionally
/// only when [`exact_avg_domain`] proves the actual batches exact.
///
/// The preflight runs on the actual batches, never on caller-supplied counts:
/// non-finite fact values or dimension weights, unexpected table shapes, or row
/// totals that cannot fit the Arrow `List` offset capacity simply leave the session
/// planning normally, while an input pair bound above `i64::MAX` refuses setup.
pub fn count_session(facts: RecordBatch, dims: RecordBatch) -> Result<SessionContext> {
    count_session_full(facts, dims, true)
}

/// [`count_session`] with an explicit switch for the `AVG(value * weight)` shape.
///
/// The `AVG` shape is served when `allow_avg` is true **and** [`exact_avg_domain`]
/// holds for the actual batches, so `allow_avg` can only *disable* the exact `AVG`
/// selection; `count_session_full(facts, dims, false)` is exactly the count-only
/// session and `count_session(facts, dims)` is
/// `count_session_full(facts, dims, true)`. `COUNT(*)` admission is unchanged and
/// never depends on `allow_avg`.
pub fn count_session_full(
    facts: RecordBatch,
    dims: RecordBatch,
    allow_avg: bool,
) -> Result<SessionContext> {
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
    // The exact-AVG admission is recorded here, from the actual batches, and can
    // only be narrowed by the caller's `allow_avg`.
    let allow_avg = selected && allow_avg && exact_avg_domain(&facts, &dims);

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
        allow_avg,
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

/// Planner that splices the factorized subtrees into the baseline plan.
///
/// `allow_avg` records whether the exact-domain `AVG(value * weight)` shape is
/// admissible for this session; the `COUNT(*)` shape is always admissible once
/// the wrapper is installed.
#[derive(Debug)]
struct CountQueryPlanner {
    inner: Arc<dyn QueryPlanner + Send + Sync>,
    facts: Arc<dyn TableProvider>,
    dims: Arc<dyn TableProvider>,
    allow_avg: bool,
}

#[async_trait]
impl QueryPlanner for CountQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // The delegated plan is authoritative: it is returned unchanged whenever a
        // shape does not match, its aggregate is not admissible for this session,
        // the factor plan cannot be built, or the splice fails.
        let baseline = self
            .inner
            .create_physical_plan(logical_plan, session)
            .await?;
        let Some((aggregate, kind)) = match_factor_shape(logical_plan, &self.facts, &self.dims)
        else {
            return Ok(baseline);
        };
        // A matching AVG shape outside the exact domain keeps the baseline plan:
        // no error and no approximate score.
        if kind == FactorAggregate::ExactAvg && !self.allow_avg {
            return Ok(baseline);
        }
        let Ok(replacement) =
            build_factor_plan(session, &self.facts, &self.dims, aggregate, kind).await
        else {
            return Ok(baseline);
        };
        Ok(splice_factor_plan(&baseline, &replacement, &aggregate.schema).unwrap_or(baseline))
    }
}

/// The aggregate shapes the factorized selection knows how to serve.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FactorAggregate {
    /// `COUNT(*)`: pair counts over the join, admissible once setup accepts the
    /// fixtures.
    Pairs,
    /// `AVG(value * weight)`: the factorized score, admissible only in the
    /// exact-equivalence domain of [`exact_avg_domain`].
    ExactAvg,
}

impl FactorAggregate {
    /// Columns of [`FactorAggregateExec`]'s `(host, area, pairs, valid, score)`
    /// output that carry this aggregate's value, paired with their positions.
    fn projection(self) -> [(&'static str, usize); 3] {
        match self {
            FactorAggregate::Pairs => [("host", 0), ("area", 1), ("pairs", 2)],
            FactorAggregate::ExactAvg => [("host", 0), ("area", 1), ("score", 4)],
        }
    }
}

/// Locates one of the experiment aggregates below unary sort/limit/projection
/// ancestors.
///
/// Those ancestors only reorder, limit, or rename the aggregate's complete output,
/// so they stay in place above the spliced factor plan. Anything else stops the
/// walk and falls back to the baseline plan.
fn match_factor_shape<'a>(
    plan: &'a LogicalPlan,
    facts: &Arc<dyn TableProvider>,
    dims: &Arc<dyn TableProvider>,
) -> Option<(&'a Aggregate, FactorAggregate)> {
    match plan {
        LogicalPlan::Aggregate(aggregate) => match_factor_aggregate(aggregate, facts, dims),
        LogicalPlan::Sort(sort) => match_factor_shape(&sort.input, facts, dims),
        LogicalPlan::Limit(limit) => match_factor_shape(&limit.input, facts, dims),
        LogicalPlan::Projection(projection) => match_factor_shape(&projection.input, facts, dims),
        _ => None,
    }
}

/// Matches `GROUP BY host, area` with either the plain `COUNT(*)` or the exact
/// `AVG(facts.value * dims.weight)` aggregate over the registered equi-join.
///
/// `COUNT(*)` matching is unchanged; the AVG arm is only recognized when the
/// argument is exactly the two-payload multiplication over the same join, and the
/// caller still decides whether the session admits it.
fn match_factor_aggregate<'a>(
    aggregate: &'a Aggregate,
    facts: &Arc<dyn TableProvider>,
    dims: &Arc<dyn TableProvider>,
) -> Option<(&'a Aggregate, FactorAggregate)> {
    if aggregate.group_expr.len() != 2 || aggregate.aggr_expr.len() != 1 {
        return None;
    }
    if !group_columns_match(&aggregate.group_expr, facts, dims) {
        return None;
    }
    let aggregate_expr = &aggregate.aggr_expr[0];
    if is_plain_count_star(aggregate_expr) {
        return match_join_input(&aggregate.input, facts, dims, false)
            .map(|_| (aggregate, FactorAggregate::Pairs));
    }
    if is_avg_product(aggregate_expr, &aggregate.input)
        && match_join_input(&aggregate.input, facts, dims, true).is_some()
    {
        return Some((aggregate, FactorAggregate::ExactAvg));
    }
    None
}

/// Finds the registered equi-join below the aggregate input.
///
/// The optimizer inserts a plain column-selection projection between the
/// aggregate and the join to drop unreferenced columns; such a projection keeps
/// every remaining column under its own name, so `host`/`area` resolve to the
/// same join columns. Any other input plan is refused.
///
/// When `allow_product` is set, a single boolean multiplication of a column of
/// each registered provider is also accepted: that is the projection the logical
/// `AVG(facts.value * dims.weight)` carries as its input, and the factor payload
/// already reads both payload columns, so the match does not change what is read.
fn match_join_input<'a>(
    plan: &'a LogicalPlan,
    facts: &Arc<dyn TableProvider>,
    dims: &Arc<dyn TableProvider>,
    allow_product: bool,
) -> Option<&'a Join> {
    match plan {
        LogicalPlan::Join(join) => join_matches(join, facts, dims).then_some(join),
        LogicalPlan::Projection(projection)
            if is_column_selection(projection)
                || (allow_product && is_payload_product(projection)) =>
        {
            match_join_input(&projection.input, facts, dims, false)
        }
        _ => None,
    }
}

/// True when the projection drops everything except one `facts.value *
/// dims.weight` multiplication, in either operand order.
fn is_payload_product(projection: &datafusion::logical_expr::Projection) -> bool {
    if projection.expr.len() != 1 || projection.schema.fields().len() != 1 {
        return false;
    }
    let Expr::Alias(alias) = &projection.expr[0] else {
        return false;
    };
    is_payload_product_expr(&alias.expr)
}

/// True for `facts.value * dims.weight` (either operand order) as a plain
/// multiplication of the two payload columns. Both registered schemas are pinned
/// by the setup preflight, so the column names identify the providers.
fn is_payload_product_expr(expr: &Expr) -> bool {
    let Expr::BinaryExpr(binary) = expr else {
        return false;
    };
    if binary.op != Operator::Multiply {
        return false;
    }
    let (Some(left), Some(right)) = (as_column(&binary.left), as_column(&binary.right)) else {
        return false;
    };
    (left.name == "value" && right.name == "weight")
        || (left.name == "weight" && right.name == "value")
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

/// Accepts only the plain `AVG(facts.value * dims.weight)` shape: no distinct,
/// filter, ordering, or null treatment, and one uncast `value * weight` product
/// of the two registered payload columns in either operand order.
///
/// This DataFusion keeps the multiplication inline in the aggregate argument. A
/// column that resolves — through the aggregate input projection — to exactly that
/// product is accepted too, so both planner layouts are recognized. Casts,
/// literals, additional arithmetic, and other payload columns keep the baseline
/// plan.
fn is_avg_product(expr: &Expr, input: &LogicalPlan) -> bool {
    let expr = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    let Expr::AggregateFunction(aggregate) = expr else {
        return false;
    };
    if aggregate.func.name() != "avg"
        || aggregate.params.distinct
        || aggregate.params.filter.is_some()
        || !aggregate.params.order_by.is_empty()
        || aggregate.params.null_treatment.is_some()
    {
        return false;
    }
    let [argument] = aggregate.params.args.as_slice() else {
        return false;
    };
    if is_payload_product_expr(argument) {
        return true;
    }
    let Some(column) = as_column(argument) else {
        return false;
    };
    let LogicalPlan::Projection(projection) = input else {
        return false;
    };
    let Some(index) = projection
        .schema
        .fields()
        .iter()
        .position(|field| field.name() == &column.name)
    else {
        return false;
    };
    let Expr::Alias(alias) = &projection.expr[index] else {
        return false;
    };
    is_payload_product_expr(&alias.expr)
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
/// `Int64`/`Float64` shapes. The final projection selects the aggregate's own
/// output columns — `(host, area, pairs)` for `COUNT(*)` and `(host, area, score)`
/// for the exact `AVG` — under the aggregate's field names so ancestors can keep
/// referencing them positionally.
async fn build_factor_plan(
    session: &dyn Session,
    facts: &Arc<dyn TableProvider>,
    dims: &Arc<dyn TableProvider>,
    aggregate: &Aggregate,
    kind: FactorAggregate,
) -> Result<Arc<dyn ExecutionPlan>> {
    let fact_scan = facts.scan(session, None, &[], None).await?;
    let dim_scan = dims.scan(session, None, &[], None).await?;
    let raw = Arc::new(RawFactorExec::try_new(fact_scan, dim_scan)?);
    let factor: Arc<dyn ExecutionPlan> = Arc::new(FactorAggregateExec::try_new(raw)?);
    let projection: Vec<(Arc<dyn PhysicalExpr>, String)> = kind
        .projection()
        .iter()
        .enumerate()
        .map(|(position, (name, index))| {
            (
                Arc::new(PhysicalColumn::new(name, *index)) as Arc<dyn PhysicalExpr>,
                aggregate.schema.field(position).name().to_string(),
            )
        })
        .collect();
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

/// Exact-equivalence classifier for `AVG(facts.value * dims.weight)`.
///
/// The factor path computes `sum(value) * sum(weight) / valid` per `(host, area)`
/// group, whereas the baseline averages every joined product in an order that
/// depends on the join. Both are the same single binary64 division only when every
/// intermediate is exact:
///
/// * every non-null fact value is an integer with `|v| <= 1_000_000`;
/// * every non-null weight is a multiple of `1/4` with `|w| <= 4`;
/// * each host group and each `(host, area)` group holds at least one non-null
///   payload row, so no group is empty and no score is NULL;
/// * a conservative pair bound over every group keeps the scaled sums below
///   `2^53`: with `max_n_f` the largest per-host fact count, `max_n_w` the largest
///   per-`(host, area)` weight count, `M_f = 1_000_000`, and `M_w = 4` (the value
///   bound, in quarters), `4 * max_n_f * M_f * max_n_w * M_w + HEADROOM` stays
///   below `2^53`.
///
/// Then each fact sum is at most `max_n_f * M_f < 2^53`, each weight sum scaled by
/// 4 is at most `max_n_w * M_w < 2^53`, their product fits in binary64, and the
/// factorized `sum * (4 * dim_sum) / 4 / valid` is a single exact division
/// producing exactly the baseline `AVG`. NULL `host`/`area` rows are fine because
/// they never match the join, and a group may mix NULL payloads with non-null ones
/// as long as it retains at least one valid entry: the factor path's `valid` count
/// is exactly the number of joined pairs whose product is non-null, so partition
/// and score both agree with the baseline. Unlike `COUNT(*)`, which keeps counting
/// NULL payloads as pairs, a group whose value or weight list is *entirely* NULL
/// is refused here because its score would be NULL instead of a division.
///
/// The grouping is computed from the actual batches in one pass each, so the
/// classifier is `O(rows)`; the bounds live in `u128`, and the final comparison is
/// checked, so the classifier itself cannot overflow. Any shape mismatch or
/// violated bound returns `false`.
pub(crate) fn exact_avg_domain(facts: &RecordBatch, dims: &RecordBatch) -> bool {
    if !fact_batch_ok(facts.schema().as_ref()) || !dim_batch_ok(dims.schema().as_ref()) {
        return false;
    }
    let (Some(fact_hosts), Some(fact_values), Some(dim_hosts), Some(dim_areas), Some(weights)) = (
        int64_column(facts, 0),
        float64_column(facts, 1),
        int64_column(dims, 0),
        int64_column(dims, 1),
        float64_column(dims, 2),
    ) else {
        return false;
    };

    // Per-host fact payloads: every row counts toward the size bound, and every
    // non-null value must be an exact integer inside the integer bound.
    let mut hosts: HashMap<i64, HostFacts> = HashMap::new();
    for index in 0..facts.num_rows() {
        let Some(host) = non_null_i64(fact_hosts, index) else {
            continue;
        };
        let entry = hosts.entry(host).or_default();
        entry.n += 1;
        if fact_values.is_null(index) {
            continue;
        }
        entry.valid += 1;
        let value = fact_values.value(index);
        if !value.is_finite() || value.fract() != 0.0 || value.abs() > MAX_FACT_MAGNITUDE {
            return false;
        }
    }

    // Per-(host, area) weight payloads: every row counts toward the size bound,
    // and every non-null weight must be a finite quarter inside the weight bound.
    let mut groups: HashMap<(i64, Option<i64>), DimWeights> = HashMap::new();
    for index in 0..dims.num_rows() {
        let Some(host) = non_null_i64(dim_hosts, index) else {
            continue;
        };
        let area = non_null_i64(dim_areas, index);
        let entry = groups.entry((host, area)).or_default();
        entry.n += 1;
        if weights.is_null(index) {
            continue;
        }
        entry.valid += 1;
        let weight = weights.value(index);
        if !weight.is_finite()
            || (weight * 4.0).fract() != 0.0
            || weight.abs() > MAX_WEIGHT_MAGNITUDE
        {
            return false;
        }
    }

    // Every host group and every (host, area) group must retain a non-null
    // payload; an all-NULL group would score NULL instead of dividing.
    if hosts.values().any(|facts| facts.valid == 0)
        || groups.values().any(|weights| weights.valid == 0)
    {
        return false;
    }

    // Conservative global bound over every group, so no lookup of a specific
    // host/area pairing is needed: 4 * max_n_f * M_f * max_n_w * M_w < 2^53.
    let max_n_f = hosts.values().map(|facts| facts.n).max().unwrap_or(0);
    let max_n_w = groups.values().map(|weights| weights.n).max().unwrap_or(0);
    (4 * max_n_f)
        .checked_mul(MAX_FACT_BOUND)
        .and_then(|scaled| scaled.checked_mul(max_n_w))
        .and_then(|scaled| scaled.checked_mul(MAX_QUARTER_BOUND))
        .is_some_and(|scaled| scaled + HEADROOM < (1u128 << 53))
}

/// Per-host fact payload sizes.
#[derive(Default)]
struct HostFacts {
    /// Rows, including NULL values.
    n: u128,
    /// Rows with a non-null value.
    valid: u128,
}

/// Per-`(host, area)` weight payload sizes.
#[derive(Default)]
struct DimWeights {
    /// Rows, including NULL weights.
    n: u128,
    /// Rows with a non-null weight.
    valid: u128,
}

/// Largest magnitude accepted for a non-null fact value (`M_f`).
const MAX_FACT_MAGNITUDE: f64 = 1_000_000.0;
/// [`MAX_FACT_MAGNITUDE`] as the integer used by the size bound.
const MAX_FACT_BOUND: u128 = 1_000_000;
/// Largest magnitude accepted for a non-null dimension weight.
const MAX_WEIGHT_MAGNITUDE: f64 = 4.0;
/// [`MAX_WEIGHT_MAGNITUDE`] in quarters, `M_w` of the size bound.
const MAX_QUARTER_BOUND: u128 = 4;
/// Distance kept below `2^53` so the additions forming each sum stay exact.
const HEADROOM: u128 = 1 << 20;

fn non_null_i64(values: &Int64Array, index: usize) -> Option<i64> {
    (!values.is_null(index)).then(|| values.value(index))
}

fn int64_column(batch: &RecordBatch, index: usize) -> Option<&Int64Array> {
    if batch.num_columns() <= index {
        return None;
    }
    batch.column(index).as_any().downcast_ref::<Int64Array>()
}

fn float64_column(batch: &RecordBatch, index: usize) -> Option<&Float64Array> {
    if batch.num_columns() <= index {
        return None;
    }
    batch.column(index).as_any().downcast_ref::<Float64Array>()
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

#[cfg(test)]
mod exact_avg_domain_tests {
    use super::*;
    use arrow_schema::Field;

    fn facts_batch(hosts: &[Option<i64>], values: &[Option<f64>]) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("host", DataType::Int64, true),
                Field::new("value", DataType::Float64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(hosts.to_vec())) as ArrayRef,
                Arc::new(Float64Array::from(values.to_vec())),
            ],
        )
        .unwrap()
    }

    fn dims_batch(
        hosts: &[Option<i64>],
        areas: &[Option<i64>],
        weights: &[Option<f64>],
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("host", DataType::Int64, true),
                Field::new("area", DataType::Int64, true),
                Field::new("weight", DataType::Float64, true),
            ])),
            vec![
                Arc::new(Int64Array::from(hosts.to_vec())) as ArrayRef,
                Arc::new(Int64Array::from(areas.to_vec())) as ArrayRef,
                Arc::new(Float64Array::from(weights.to_vec())),
            ],
        )
        .unwrap()
    }

    /// All-NULL host/area rows never match the join and must not disqualify.
    #[test]
    fn accepts_the_exact_domain_and_ignores_unmatched_null_keys() {
        let facts = facts_batch(
            &[Some(1), Some(1), None, Some(2)],
            &[Some(2.0), Some(-1_000_000.0), Some(1.5), Some(0.0)],
        );
        let dims = dims_batch(
            &[Some(1), Some(1), None, Some(2)],
            &[Some(7), Some(7), Some(3), None],
            &[Some(0.25), Some(-4.0), Some(0.3), Some(1.0)],
        );
        assert!(exact_avg_domain(&facts, &dims));
    }

    #[test]
    fn rejects_out_of_range_and_non_integral_payloads() {
        let cases: Vec<(&str, RecordBatch, RecordBatch)> = vec![
            (
                "non-integral value",
                facts_batch(&[Some(1)], &[Some(1.5)]),
                dims_batch(&[Some(1)], &[Some(0)], &[Some(0.25)]),
            ),
            (
                "value above the bound",
                facts_batch(&[Some(1)], &[Some(1_000_001.0)]),
                dims_batch(&[Some(1)], &[Some(0)], &[Some(0.25)]),
            ),
            (
                "value below the bound",
                facts_batch(&[Some(1)], &[Some(-1_000_001.0)]),
                dims_batch(&[Some(1)], &[Some(0)], &[Some(0.25)]),
            ),
            (
                "non-quarter weight",
                facts_batch(&[Some(1)], &[Some(2.0)]),
                dims_batch(&[Some(1)], &[Some(0)], &[Some(0.3)]),
            ),
            (
                "weight above the bound",
                facts_batch(&[Some(1)], &[Some(2.0)]),
                dims_batch(&[Some(1)], &[Some(0)], &[Some(4.25)]),
            ),
            (
                "weight below the bound",
                facts_batch(&[Some(1)], &[Some(2.0)]),
                dims_batch(&[Some(1)], &[Some(0)], &[Some(-4.25)]),
            ),
            (
                "NaN value",
                facts_batch(&[Some(1)], &[Some(f64::NAN)]),
                dims_batch(&[Some(1)], &[Some(0)], &[Some(0.25)]),
            ),
            (
                "infinite weight",
                facts_batch(&[Some(1)], &[Some(2.0)]),
                dims_batch(&[Some(1)], &[Some(0)], &[Some(f64::INFINITY)]),
            ),
        ];
        for (name, facts, dims) in cases {
            assert!(!exact_avg_domain(&facts, &dims), "{name} was accepted");
        }
    }

    /// A group whose payload is entirely NULL would score NULL, not a quotient.
    #[test]
    fn rejects_groups_without_a_valid_payload() {
        let facts = facts_batch(&[Some(1), Some(1), Some(2)], &[Some(2.0), Some(3.0), None]);
        let dims = dims_batch(
            &[Some(1), Some(1), Some(2)],
            &[Some(0), Some(1), Some(0)],
            &[Some(0.25), None, Some(0.5)],
        );
        // The host-2 fact group has only NULL values.
        assert!(!exact_avg_domain(&facts, &dims));
        // The host-1 area-1 dimension group has only a NULL weight.
        let dims = dims_batch(
            &[Some(1), Some(1), Some(2)],
            &[Some(0), Some(1), Some(0)],
            &[Some(0.25), Some(1.0), Some(0.5)],
        );
        let facts = facts_batch(&[Some(1), Some(1), Some(2)], &[Some(2.0), Some(3.0), None]);
        assert!(!exact_avg_domain(&facts, &dims));
        // A group mixing NULL and non-null payloads is still admissible.
        let facts = facts_batch(&[Some(1), None, Some(1)], &[Some(2.0), Some(3.0), None]);
        let dims = dims_batch(&[Some(1), Some(1)], &[Some(0), Some(0)], &[None, Some(0.5)]);
        assert!(exact_avg_domain(&facts, &dims));
    }

    /// The conservative pair bound is what keeps the scaled sums inside 2^53.
    #[test]
    fn size_bound_rejects_wide_groups_without_overflowing() {
        let facts = facts_batch(&[Some(1); 20_000], &[Some(1.0); 20_000]);
        let dims = dims_batch(
            &[Some(1); 20_000],
            &[Some(0); 20_000],
            &[Some(0.25); 20_000],
        );
        assert!(exact_avg_domain(&facts, &dims));
        let facts = facts_batch(&[Some(1); 24_000], &[Some(1.0); 24_000]);
        let dims = dims_batch(
            &[Some(1); 24_000],
            &[Some(0); 24_000],
            &[Some(0.25); 24_000],
        );
        assert!(!exact_avg_domain(&facts, &dims));
    }

    #[test]
    fn rejects_unexpected_batch_shapes_and_accepts_empty_inputs() {
        let facts = facts_batch(&[Some(1)], &[Some(2.0)]);
        let dims = dims_batch(&[Some(1)], &[Some(0)], &[Some(0.25)]);
        let swapped = dims_batch(&[Some(1)], &[Some(0)], &[Some(0.25)]);
        assert!(!exact_avg_domain(&swapped, &facts));
        assert!(exact_avg_domain(
            &RecordBatch::new_empty(facts.schema()),
            &RecordBatch::new_empty(dims.schema())
        ));
    }
}
