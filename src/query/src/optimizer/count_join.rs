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

//! Bounded `COUNT(*)` and certified-domain `AVG(value * weight)`
//! join-aggregation rewrites.
//!
//! Both rewrites turn `... JOIN ... GROUP BY host, area` with a single
//! aggregate into a join of two partial aggregates computed on one side of the
//! join each.
//!
//! `COUNT(*)`: the pair count is replaced by the product of the two branch row
//! counts, which is only sound while the join cannot produce more pairs than
//! `COUNT(*)` can hold in its `Int64` result. That bound is rule state: it is
//! supplied when the rule is constructed and the rule declines to rewrite when
//! its bound does not prove the pair count.
//!
//! `AVG(value * weight)`: the average of the pairwise products is replaced by
//! `SUM(value) * SUM(weight) / (COUNT(value) * COUNT(weight))`. Such a
//! factorization is exact only on a data domain where every intermediate value
//! is exactly representable and the cancellation of exact nonzero values cannot
//! leave a different signed zero than the pairwise sum does. Whether the data is
//! in that domain is not visible in a `LogicalPlan`, so the caller certifies it
//! with [`exact_avg_domain`] over the actual input batches and passes the
//! certificate to the rule at construction, exactly like the pair bound. The
//! rule rewrites `AVG` only when it was certified.
//!
//! The rule is intentionally narrow: it matches only the exact join/group/fact
//! and dim shapes described by `matching_shape` and `matching_avg_shape`. It is
//! not registered in any default rule list, so engine construction is
//! unaffected; the regression tests in `query_engine::state` install an
//! instance explicitly.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow::array::{Array, Float64Array, StringArray};
use arrow_schema::DataType;
use common_recordbatch::RecordBatch;
use datafusion::functions_aggregate::average::Avg;
use datafusion::functions_aggregate::count::{Count, count_udaf};
use datafusion::functions_aggregate::sum::sum_udaf;
use datafusion_common::tree_node::Transformed;
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_common::{Column, DFSchema, NullEquality, Result, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, BinaryExpr, Case, Cast};
use datafusion_expr::logical_plan::{Aggregate, Join, JoinConstraint, JoinType, Projection};
use datafusion_expr::{AggregateUDF, Expr, LogicalPlan, Operator};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

/// Rewrites the supported `COUNT(*)` join-aggregate shape into a join of
/// partial counts multiplied in a projection, and the certified-domain
/// `AVG(value * weight)` shape into a join of partial sums and counts.
#[derive(Debug)]
pub(crate) struct CountJoinRewrite {
    /// `left_rows * right_rows` when the bound proves that product is
    /// representable as the `Int64` value `COUNT(*)` produces, `None` when it
    /// does not. The bound belongs to the rule so it travels with the instance
    /// that applies the rewrite.
    pair_bound: Option<u64>,
    /// Whether the caller proved the exact-domain certificate of the `AVG`
    /// inputs with [`exact_avg_domain`]. Only a certified rule rewrites `AVG`.
    exact_avg: bool,
}

impl CountJoinRewrite {
    /// Creates the rule from the row-count bounds of the two joined inputs.
    ///
    /// `fact_rows_bound * dim_rows_bound` is stored when the product neither
    /// overflows `u64` nor exceeds `i64::MAX`; otherwise the rule records that
    /// no sound bound is available and declines every `COUNT(*)` rewrite.
    /// Declining keeps the query on the unrewritten plan instead of failing it.
    /// `AVG` is never rewritten by a count-only rule.
    pub(crate) fn new(fact_rows_bound: u64, dim_rows_bound: u64) -> Self {
        Self {
            pair_bound: pair_bound(fact_rows_bound, dim_rows_bound),
            exact_avg: false,
        }
    }

    /// Creates the rule from the row-count bounds and the exact-domain
    /// certificate of the two joined inputs.
    ///
    /// `exact_avg` must come from [`exact_avg_domain`] applied to the batches
    /// the two inputs will actually read; the rule trusts it and rewrites
    /// `AVG(value * weight)` accordingly. Without it the rule leaves every
    /// `AVG` on the unrewritten plan while still rewriting `COUNT(*)`.
    pub(crate) fn with_avg(fact_rows_bound: u64, dim_rows_bound: u64, exact_avg: bool) -> Self {
        Self {
            pair_bound: pair_bound(fact_rows_bound, dim_rows_bound),
            exact_avg,
        }
    }

    /// The pair count the rule is allowed to assume, if its bound proves one.
    pub(crate) fn pair_bound(&self) -> Option<u64> {
        self.pair_bound
    }

    /// Whether `AVG(value * weight)` may be rewritten with the certificate the
    /// rule was constructed with.
    pub(crate) fn exact_avg(&self) -> bool {
        self.exact_avg
    }
}

/// The product of the two input row-count bounds when it is representable as
/// the `Int64` value `COUNT(*)` produces.
fn pair_bound(fact_rows_bound: u64, dim_rows_bound: u64) -> Option<u64> {
    fact_rows_bound
        .checked_mul(dim_rows_bound)
        .filter(|pairs| *pairs <= i64::MAX as u64)
}

/// Certifies that `AVG(value * weight)` over the cross product of `fact` and
/// `dim` is exactly reproduced by
/// `SUM(value) * SUM(weight) / (COUNT(value) * COUNT(weight))`, bit for bit.
///
/// The certificate is a pure function of the data the two inputs will read,
/// which the rule itself cannot see:
///
/// - every non-null `fact.value` is finite, integral (`fract() == 0`) and
///   within `+-1_000_000`;
/// - every non-null `dim.weight` is finite, a quarter step
///   (`(weight * 4).fract() == 0`) and within `+-4`;
/// - every `host` group of `fact` and every `(host, area)` group of `dim`
///   keeps at least one non-null payload, and
/// - `4 * max_n_f * 1_000_000 * max_n_w * 16 + 2^20 < 2^53`, where `max_n_f` is
///   the largest non-null payload count of a `fact` group and `max_n_w` the
///   same for a `dim` group, computed in `u128`.
///
/// Integral values of magnitude at most `1_000_000` and quarter steps of
/// magnitude at most `4` multiply into exact multiples of `1/4` below `2^52`,
/// so both the pairwise sum the baseline computes and the factored product are
/// exact (`f64`-representable without rounding) and agree. The explicit
/// `2^20` of headroom keeps the bound strictly below the `2^53` integer-exact
/// limit rather than merely at it.
pub(crate) fn exact_avg_domain(fact: &RecordBatch, dim: &RecordBatch) -> bool {
    /// Largest magnitude `fact.value` may take.
    const MAX_VALUE_MAGNITUDE: f64 = 1_000_000.0;
    /// Largest magnitude `dim.weight` may take.
    const MAX_WEIGHT_MAGNITUDE: f64 = 4.0;
    /// `dim.weight` granularity: quarter steps.
    const QUARTER_STEPS: f64 = 4.0;

    let (Some(fact_host), Some(fact_value)) =
        (string_column(fact, "host"), float_column(fact, "value"))
    else {
        return false;
    };
    let (Some(dim_host), Some(dim_area), Some(dim_weight)) = (
        string_column(dim, "host"),
        string_column(dim, "area"),
        float_column(dim, "weight"),
    ) else {
        return false;
    };

    // Payload counts per group, including the groups the rewritten branches
    // would aggregate: `fact` by `host`, `dim` by `(host, area)`. A null join
    // key cannot match (`NullEqualsNothing`), but its group still exists in the
    // rewritten branch, so it is counted and checked like any other.
    let mut fact_payloads: BTreeMap<Option<&str>, u64> = BTreeMap::new();
    for row in 0..fact.num_rows() {
        let group = (!fact_host.is_null(row)).then(|| fact_host.value(row));
        let payloads = fact_payloads.entry(group).or_insert(0);
        if fact_value.is_null(row) {
            continue;
        }
        let value = fact_value.value(row);
        if !value.is_finite() || value.fract() != 0.0 || value.abs() > MAX_VALUE_MAGNITUDE {
            return false;
        }
        *payloads += 1;
    }

    let mut dim_payloads: BTreeMap<(Option<&str>, Option<&str>), u64> = BTreeMap::new();
    for row in 0..dim.num_rows() {
        let group = (
            (!dim_host.is_null(row)).then(|| dim_host.value(row)),
            (!dim_area.is_null(row)).then(|| dim_area.value(row)),
        );
        let payloads = dim_payloads.entry(group).or_insert(0);
        if dim_weight.is_null(row) {
            continue;
        }
        let weight = dim_weight.value(row);
        if !weight.is_finite()
            || (weight * QUARTER_STEPS).fract() != 0.0
            || weight.abs() > MAX_WEIGHT_MAGNITUDE
        {
            return false;
        }
        *payloads += 1;
    }

    // A group without a single non-null payload can only average to NULL, which
    // the certificate does not vouch for.
    if fact_payloads.values().any(|payloads| *payloads == 0)
        || dim_payloads.values().any(|payloads| *payloads == 0)
    {
        return false;
    }

    let max_fact_payloads = fact_payloads.values().copied().max().unwrap_or_default();
    let max_dim_payloads = dim_payloads.values().copied().max().unwrap_or_default();
    avg_domain_bound(max_fact_payloads, max_dim_payloads)
}

/// Whether the widest group pair count of the two inputs still bounds every
/// intermediate of the `AVG` factorization below the integer-exact limit.
///
/// `max_fact_payloads` is the largest payload count of a `fact` group and
/// `max_dim_payloads` the same for a `dim` group. The checked `u128` arithmetic
/// implements the conservative global bound
/// `4 * max_n_f * 1_000_000 * max_n_w * 16 + 2^20 < 2^53`: the leading `4` is
/// the weight magnitude, `1_000_000` the value magnitude, the trailing `16`
/// keeps a further margin over a pair's magnitude, and the `2^20` keeps the
/// result strictly below the limit instead of at it. An input with no rows has
/// no intermediate to bound and is accepted.
pub(crate) fn avg_domain_bound(max_fact_payloads: u64, max_dim_payloads: u64) -> bool {
    /// Largest magnitude `fact.value` may take.
    const MAX_VALUE_MAGNITUDE: u128 = 1_000_000;
    /// Largest magnitude `dim.weight` may take.
    const MAX_WEIGHT_MAGNITUDE: u128 = 4;
    /// Headroom over a pair's magnitude, keeping the bound conservative.
    const PRODUCT_HEADROOM: u128 = 16;
    /// Keeps the bound below the integer-exact limit instead of at it.
    const EXACT_HEADROOM: u128 = 1 << 20;
    /// Largest integer magnitude an `f64` represents exactly.
    const EXACT_LIMIT: u128 = 1 << 53;

    let bound = MAX_WEIGHT_MAGNITUDE
        .checked_mul(u128::from(max_fact_payloads))
        .and_then(|bound| bound.checked_mul(MAX_VALUE_MAGNITUDE))
        .and_then(|bound| bound.checked_mul(u128::from(max_dim_payloads)))
        .and_then(|bound| bound.checked_mul(PRODUCT_HEADROOM))
        .and_then(|bound| bound.checked_add(EXACT_HEADROOM));
    matches!(bound, Some(bound) if bound < EXACT_LIMIT)
}

fn float_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a Float64Array> {
    batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<Float64Array>()
}

fn string_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a StringArray> {
    batch
        .column_by_name(name)?
        .as_any()
        .downcast_ref::<StringArray>()
}

impl OptimizerRule for CountJoinRewrite {
    fn name(&self) -> &str {
        "bounded_count_join_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Aggregate(agg) = &plan else {
            return Ok(Transformed::no(plan));
        };
        // An unsatisfiable bound means the pair count cannot be proven to fit
        // `COUNT(*)`'s result, so the `COUNT(*)` rewrite must not be applied.
        if self.pair_bound.is_some()
            && let Some((join, count, left_key, right_key)) = matching_shape(agg)
        {
            let Expr::Column(right_group) = &agg.group_expr[1] else {
                return Ok(Transformed::no(plan));
            };

            let left_count_name = config.alias_generator().next("partial_count");
            let right_count_name = config.alias_generator().next("partial_count");
            let left = Aggregate::try_new(
                Arc::clone(&join.left),
                vec![Expr::Column(left_key)],
                vec![Expr::AggregateFunction(count.clone()).alias(&left_count_name)],
            )?;
            let right = Aggregate::try_new(
                Arc::clone(&join.right),
                vec![Expr::Column(right_key), Expr::Column(right_group.clone())],
                vec![Expr::AggregateFunction(count.clone()).alias(&right_count_name)],
            )?;

            let left = Arc::new(LogicalPlan::Aggregate(left));
            let right = Arc::new(LogicalPlan::Aggregate(right));
            let join_keys = (column_at(left.schema(), 0), column_at(right.schema(), 0));
            let rewritten_join = Join::try_new(
                left,
                right,
                vec![(Expr::Column(join_keys.0), Expr::Column(join_keys.1))],
                None,
                JoinType::Inner,
                JoinConstraint::On,
                NullEquality::NullEqualsNothing,
                false,
            )?;
            let product = Expr::Column(Column::new_unqualified(left_count_name))
                * Expr::Column(Column::new_unqualified(right_count_name));
            let output = vec![
                Expr::Column(column_at(&rewritten_join.schema, 0)),
                Expr::Column(column_at(&rewritten_join.schema, 3)),
                product.alias(agg.schema.field(2).name()),
            ];
            let projection =
                Projection::try_new(output, Arc::new(LogicalPlan::Join(rewritten_join)))?;
            return Ok(Transformed::yes(LogicalPlan::Projection(projection)));
        }
        // The `AVG` rewrite needs no pair bound: it never materializes the pair
        // count, and the certified numerator bound keeps `COUNT(value) *
        // COUNT(weight)` far below `Int64::MAX`. It does need the caller's
        // domain certificate, which a plan cannot show.
        if self.exact_avg
            && let Some(matched) = matching_avg_shape(agg)
        {
            return rewrite_avg(agg, &matched, config);
        }
        Ok(Transformed::no(plan))
    }
}

/// Rewrites the certified `AVG(value * weight)` aggregate into a join of two
/// partial aggregates whose projection reconstructs the average.
///
/// The projection computes, from the branch `nf`/`nv`/`sv` (fact) and
/// `nd`/`nw`/`sw` (dim) outputs:
///
/// ```text
/// valid = nv * nw
/// total = CASE WHEN sv * sw = 0.0 THEN 0.0 ELSE sv * sw END
/// AVG   = CASE WHEN valid = 0 THEN NULL ELSE total / CAST(valid AS Float64) END
/// ```
///
/// `total` normalizes a zero numerator to `+0.0` because the pairwise numerator
/// it must match bitwise can only cancel to `+0.0`: for facts `[1, -1]` with
/// weight `[-1]`, the baseline sums the products `-1` and `+1` into `+0.0`,
/// while `SUM(value)` is `+0.0`, `SUM(weight)` is `-1.0` and their naive product
/// is `-0.0`, which would divide into `-0.0` instead of the baseline's `+0.0`.
/// All sums are exact quarter-integers below `2^53` (see [`exact_avg_domain`]),
/// so no nonzero difference can be a rounding-order gap: the normalized zero is
/// the only place where the factorization could disagree with the baseline.
fn rewrite_avg(
    agg: &Aggregate,
    matched: &AvgMatch<'_>,
    config: &dyn OptimizerConfig,
) -> Result<Transformed<LogicalPlan>> {
    let AvgMatch {
        join,
        left_key,
        right_key,
        right_group,
        fact_payload,
        dim_payload,
    } = matched;

    let fact_rows_name = config.alias_generator().next("partial_nf");
    let fact_payloads_name = config.alias_generator().next("partial_nv");
    let fact_sum_name = config.alias_generator().next("partial_sv");
    let dim_rows_name = config.alias_generator().next("partial_nd");
    let dim_payloads_name = config.alias_generator().next("partial_nw");
    let dim_sum_name = config.alias_generator().next("partial_sw");

    let left = Aggregate::try_new(
        Arc::clone(&join.left),
        vec![Expr::Column(left_key.clone())],
        vec![
            partial_aggregate(count_udaf(), count_star(), &fact_rows_name),
            partial_aggregate(
                count_udaf(),
                vec![Expr::Column(fact_payload.clone())],
                &fact_payloads_name,
            ),
            partial_aggregate(
                sum_udaf(),
                vec![Expr::Column(fact_payload.clone())],
                &fact_sum_name,
            ),
        ],
    )?;
    let right = Aggregate::try_new(
        Arc::clone(&join.right),
        vec![
            Expr::Column(right_key.clone()),
            Expr::Column(right_group.clone()),
        ],
        vec![
            partial_aggregate(count_udaf(), count_star(), &dim_rows_name),
            partial_aggregate(
                count_udaf(),
                vec![Expr::Column(dim_payload.clone())],
                &dim_payloads_name,
            ),
            partial_aggregate(
                sum_udaf(),
                vec![Expr::Column(dim_payload.clone())],
                &dim_sum_name,
            ),
        ],
    )?;

    let left = Arc::new(LogicalPlan::Aggregate(left));
    let right = Arc::new(LogicalPlan::Aggregate(right));
    let right_area = left.schema().fields().len() + 1;
    let join_keys = (column_at(left.schema(), 0), column_at(right.schema(), 0));
    let rewritten_join = Join::try_new(
        left,
        right,
        vec![(Expr::Column(join_keys.0), Expr::Column(join_keys.1))],
        None,
        JoinType::Inner,
        JoinConstraint::On,
        NullEquality::NullEqualsNothing,
        false,
    )?;

    let partial = |name: &str| Expr::Column(Column::new_unqualified(name));
    let valid = partial(&fact_payloads_name) * partial(&dim_payloads_name);
    let product = partial(&fact_sum_name) * partial(&dim_sum_name);
    // `total`: the signed-zero normalization described on this function.
    let total = Expr::Case(Case::new(
        None,
        vec![(
            Box::new(is_zero(product.clone(), float_literal(0.0))),
            Box::new(float_literal(0.0)),
        )],
        Some(Box::new(product)),
    ));
    let average = Expr::Case(Case::new(
        None,
        vec![(
            Box::new(is_zero(valid.clone(), int_literal(0))),
            Box::new(Expr::Literal(ScalarValue::Float64(None), None)),
        )],
        Some(Box::new(Expr::BinaryExpr(BinaryExpr::new(
            Box::new(total),
            Operator::Divide,
            Box::new(Expr::Cast(Cast::new(Box::new(valid), DataType::Float64))),
        )))),
    ));
    let output = vec![
        Expr::Column(column_at(&rewritten_join.schema, 0)),
        Expr::Column(column_at(&rewritten_join.schema, right_area)),
        average.alias(agg.schema.field(2).name()),
    ];
    let projection = Projection::try_new(output, Arc::new(LogicalPlan::Join(rewritten_join)))?;
    Ok(Transformed::yes(LogicalPlan::Projection(projection)))
}

fn partial_aggregate(udaf: Arc<AggregateUDF>, args: Vec<Expr>, alias: &str) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        udaf,
        args,
        false,
        None,
        vec![],
        None,
    ))
    .alias(alias)
}

fn count_star() -> Vec<Expr> {
    vec![Expr::Literal(COUNT_STAR_EXPANSION, None)]
}

fn int_literal(value: i64) -> Expr {
    Expr::Literal(ScalarValue::Int64(Some(value)), None)
}

fn float_literal(value: f64) -> Expr {
    Expr::Literal(ScalarValue::Float64(Some(value)), None)
}

/// `<lhs> = <rhs>`, used to detect the zero and NULL cases of the `AVG`
/// reconstruction.
fn is_zero(lhs: Expr, rhs: Expr) -> Expr {
    Expr::BinaryExpr(BinaryExpr::new(Box::new(lhs), Operator::Eq, Box::new(rhs)))
}

fn column_at(schema: &DFSchema, index: usize) -> Column {
    let (qualifier, field) = schema.qualified_field(index);
    Column::new(qualifier.cloned(), field.name())
}

/// Matches the narrow supported shape and returns the join plus everything the
/// rewrite needs from it.
///
/// Required shape: an inner `ON` equi-join on the `host` columns, grouped by the
/// left `host` and the right `area`, aggregated by a single plain `COUNT(*)`
/// (i.e. `COUNT` over `COUNT_STAR_EXPANSION` with no modifier), with a `fact`
/// table or subquery alias on the left and a `dim` one on the right. The group
/// expressions may be separated from the join by a projection that repeats them.
fn matching_shape(agg: &Aggregate) -> Option<(&Join, &AggregateFunction, Column, Column)> {
    if agg.group_expr.len() != 2 || agg.aggr_expr.len() != 1 {
        return None;
    }
    let join = match agg.input.as_ref() {
        LogicalPlan::Join(join) => join,
        LogicalPlan::Projection(projection) if projection.expr == agg.group_expr => {
            let LogicalPlan::Join(join) = projection.input.as_ref() else {
                return None;
            };
            join
        }
        _ => return None,
    };
    if join.join_type != JoinType::Inner
        || join.join_constraint != JoinConstraint::On
        || join.null_equality != NullEquality::NullEqualsNothing
    {
        return None;
    }
    let (left_key, right_key) = equality_keys(join)?;
    if left_key.name != "host" || right_key.name != "host" {
        return None;
    }
    let Expr::AggregateFunction(count) = &agg.aggr_expr[0] else {
        return None;
    };
    let params = &count.params;
    if count.func.inner().downcast_ref::<Count>().is_none()
        || params.distinct
        || params.filter.is_some()
        || !params.order_by.is_empty()
        || params.null_treatment.is_some()
        || params.args != [Expr::Literal(COUNT_STAR_EXPANSION, None)]
    {
        return None;
    }
    let (Expr::Column(left_group), Expr::Column(right_group)) =
        (&agg.group_expr[0], &agg.group_expr[1])
    else {
        return None;
    };
    if left_group != left_key
        || !right_group.name.eq_ignore_ascii_case("area")
        || right_group.relation != right_key.relation
        || !fixture_scan(&join.left, "fact")
        || !fixture_scan(&join.right, "dim")
    {
        return None;
    }
    Some((join, count, left_key.clone(), right_key.clone()))
}

/// The pieces of the certified `AVG(<fact payload> * <dim payload>)` shape the
/// rewrite needs.
struct AvgMatch<'a> {
    join: &'a Join,
    left_key: Column,
    right_key: Column,
    right_group: Column,
    fact_payload: Column,
    dim_payload: Column,
}

/// Matches the narrow supported `AVG(payload * payload)` shape and returns the
/// join plus everything the rewrite needs from it.
///
/// Required shape: the same inner `ON` equi-join on `host` with a `fact` table
/// or subquery alias on the left and a `dim` one on the right (so the payload
/// columns are the `fact.value` and `dim.weight` columns [`exact_avg_domain`]
/// certified), grouped by the left `host` and the right `area`, aggregated by a
/// single `AVG` of the product of those two columns, with no `DISTINCT`,
/// `FILTER`, `ORDER BY`, null treatment or extra argument. The product operands
/// may be given in either order and may reach the aggregate through a
/// projection that repeats them.
fn matching_avg_shape(agg: &Aggregate) -> Option<AvgMatch<'_>> {
    if agg.group_expr.len() != 2 || agg.aggr_expr.len() != 1 {
        return None;
    }
    let (join, projection) = match agg.input.as_ref() {
        LogicalPlan::Join(join) => (join, None),
        LogicalPlan::Projection(projection) => match projection.input.as_ref() {
            LogicalPlan::Join(join) => (join, Some(projection)),
            _ => return None,
        },
        _ => return None,
    };
    if join.join_type != JoinType::Inner
        || join.join_constraint != JoinConstraint::On
        || join.null_equality != NullEquality::NullEqualsNothing
    {
        return None;
    }
    let (left_key, right_key) = equality_keys(join)?;
    if left_key.name != "host" || right_key.name != "host" {
        return None;
    }
    let function = match &agg.aggr_expr[0] {
        Expr::AggregateFunction(function) => function,
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::AggregateFunction(function) => function,
            _ => return None,
        },
        _ => return None,
    };
    let params = &function.params;
    if function.func.inner().downcast_ref::<Avg>().is_none()
        || params.distinct
        || params.filter.is_some()
        || !params.order_by.is_empty()
        || params.null_treatment.is_some()
        || params.args.len() != 1
    {
        return None;
    }
    let (Expr::Column(left_group), Expr::Column(right_group)) =
        (&agg.group_expr[0], &agg.group_expr[1])
    else {
        return None;
    };
    if left_group != left_key
        || !right_group.name.eq_ignore_ascii_case("area")
        || right_group.relation != right_key.relation
        || !fixture_scan(&join.left, "fact")
        || !fixture_scan(&join.right, "dim")
    {
        return None;
    }
    let (first, second) = product_operands(&params.args[0], projection)?;
    Some(AvgMatch {
        join,
        left_key: left_key.clone(),
        right_key: right_key.clone(),
        right_group: right_group.clone(),
        fact_payload: payload_column(&join.left, "value", first)?,
        dim_payload: payload_column(&join.right, "weight", second)?,
    })
}

/// The two operands of an `AVG` argument, which must be a multiplication of two
/// columns of the joined inputs, in either order.
///
/// The projection that may sit between the aggregate and the join either passes
/// the product's operands through under their original names (the argument is
/// then the multiplication itself) or computes the product on its own, in which
/// case the argument is a column of the projection and the multiplication is
/// looked up there.
fn product_operands<'a>(
    argument: &'a Expr,
    projection: Option<&'a Projection>,
) -> Option<(&'a Expr, &'a Expr)> {
    let entry = match argument {
        Expr::BinaryExpr(binary) => binary,
        Expr::Column(column) => {
            let projection = projection?;
            let index = projection
                .schema
                .maybe_index_of_column(column)
                .or_else(|| {
                    projection
                        .schema
                        .index_of_column_by_name(None, &column.name)
                })?;
            let entry = projection.expr.get(index)?;
            match entry {
                Expr::BinaryExpr(binary) => binary,
                Expr::Alias(alias) => match alias.expr.as_ref() {
                    Expr::BinaryExpr(binary) => binary,
                    _ => return None,
                },
                _ => return None,
            }
        }
        _ => return None,
    };
    if entry.op != Operator::Multiply {
        return None;
    }
    Some((entry.left.as_ref(), entry.right.as_ref()))
}

/// Resolves `operand` to the `name` column of the input it must come from, so a
/// payload from the wrong side of the join — or any expression other than a
/// plain column — cannot be rewritten.
fn payload_column(input: &LogicalPlan, name: &str, operand: &Expr) -> Option<Column> {
    let Expr::Column(column) = operand else {
        return None;
    };
    if !column.name.eq(name) || !input.schema().has_column(column) {
        return None;
    }
    Some(column.clone())
}

fn equality_keys(join: &Join) -> Option<(&Column, &Column)> {
    if join.on.len() != 1 || join.filter.is_some() {
        return None;
    }
    let (Expr::Column(left), Expr::Column(right)) = (&join.on[0].0, &join.on[0].1) else {
        return None;
    };
    Some((left, right))
}

fn fixture_scan(plan: &LogicalPlan, table: &str) -> bool {
    match plan {
        LogicalPlan::TableScan(scan) => scan.table_name.to_string().eq_ignore_ascii_case(table),
        LogicalPlan::SubqueryAlias(alias) => match alias.input.as_ref() {
            LogicalPlan::TableScan(scan) => scan.table_name.to_string().eq_ignore_ascii_case(table),
            _ => false,
        },
        _ => false,
    }
}
