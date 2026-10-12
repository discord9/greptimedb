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

//! Bounded `COUNT(*)` join-aggregation rewrite.
//!
//! The rule turns `... JOIN ... GROUP BY host, area` with a single plain
//! `COUNT(*)` into a join of two partial `COUNT(*)` aggregates whose results are
//! multiplied in a projection. That is only sound while the join cannot produce
//! more pairs than `COUNT(*)` can hold in its `Int64` result, so the bound that
//! proves it is rule state: it is supplied when the rule is constructed and the
//! rule declines to rewrite when its bound does not prove the pair count.
//!
//! The rule is intentionally narrow: it matches only the exact join/group/fact
//! and dim shape described by `matching_shape`. It is not registered in any
//! default rule list, so engine construction is unaffected; the regression tests
//! in `query_engine::state` install an instance explicitly.

use std::sync::Arc;

use datafusion::functions_aggregate::count::Count;
use datafusion_common::tree_node::Transformed;
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_common::{Column, DFSchema, NullEquality, Result};
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::logical_plan::{Aggregate, Join, JoinConstraint, JoinType, Projection};
use datafusion_expr::{Expr, LogicalPlan};
use datafusion_optimizer::optimizer::ApplyOrder;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule};

/// Rewrites the supported `COUNT(*)` join-aggregate shape into a join of
/// partial counts multiplied in a projection.
#[derive(Debug)]
pub(crate) struct CountJoinRewrite {
    /// `left_rows * right_rows` when the bound proves that product is
    /// representable as the `Int64` value `COUNT(*)` produces, `None` when it
    /// does not. The bound belongs to the rule so it travels with the instance
    /// that applies the rewrite.
    pair_bound: Option<u64>,
}

impl CountJoinRewrite {
    /// Creates the rule from the row-count bounds of the two joined inputs.
    ///
    /// `fact_rows_bound * dim_rows_bound` is stored when the product neither
    /// overflows `u64` nor exceeds `i64::MAX`; otherwise the rule records that
    /// no sound bound is available and declines every rewrite. Declining keeps
    /// the query on the unrewritten plan instead of failing it.
    pub(crate) fn new(fact_rows_bound: u64, dim_rows_bound: u64) -> Self {
        Self {
            pair_bound: fact_rows_bound
                .checked_mul(dim_rows_bound)
                .filter(|pairs| *pairs <= i64::MAX as u64),
        }
    }

    /// The pair count the rule is allowed to assume, if its bound proves one.
    pub(crate) fn pair_bound(&self) -> Option<u64> {
        self.pair_bound
    }
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
        // An unsatisfiable bound means the pair count cannot be proven to fit
        // `COUNT(*)`'s result, so the rewrite must not be applied.
        if self.pair_bound.is_none() {
            return Ok(Transformed::no(plan));
        }
        let LogicalPlan::Aggregate(agg) = &plan else {
            return Ok(Transformed::no(plan));
        };
        let Some((join, count, left_key, right_key)) = matching_shape(agg) else {
            return Ok(Transformed::no(plan));
        };
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
        let projection = Projection::try_new(output, Arc::new(LogicalPlan::Join(rewritten_join)))?;
        Ok(Transformed::yes(LogicalPlan::Projection(projection)))
    }
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
