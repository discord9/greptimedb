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

//! A bounded COUNT(*) join-aggregation rewrite experiment.

use std::sync::Arc;

use datafusion::execution::{SessionStateBuilder, context::SessionContext};
use datafusion::prelude::SessionConfig;
use datafusion_common::{
    Column, DataFusionError, NullEquality, Result, tree_node::Transformed,
    utils::expr::COUNT_STAR_EXPANSION,
};
use datafusion_expr::{
    Expr, LogicalPlan,
    logical_plan::{Aggregate, Join, JoinConstraint, JoinType},
};
use datafusion_functions_aggregate::count::Count;
use datafusion_optimizer::{OptimizerConfig, OptimizerRule, optimizer::ApplyOrder};

/// Rewrites only the fixture query shape supported by this experiment.
#[derive(Debug)]
struct CountJoinRewrite;

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
        let projection = datafusion_expr::logical_plan::Projection::try_new(
            output,
            Arc::new(LogicalPlan::Join(rewritten_join)),
        )?;
        Ok(Transformed::yes(LogicalPlan::Projection(projection)))
    }
}

fn column_at(schema: &datafusion_common::DFSchema, index: usize) -> Column {
    let (qualifier, field) = schema.qualified_field(index);
    Column::new(qualifier.cloned(), field.name())
}

fn matching_shape(
    agg: &Aggregate,
) -> Option<(
    &Join,
    &datafusion_expr::expr::AggregateFunction,
    Column,
    Column,
)> {
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

/// Creates a session after checking fixture pair counts fit COUNT's Int64 result.
pub fn experiment_session(left_rows: u64, right_rows: u64) -> Result<SessionContext> {
    let pairs = left_rows
        .checked_mul(right_rows)
        .filter(|pairs| *pairs <= i64::MAX as u64);
    if pairs.is_none() {
        return Err(DataFusionError::Plan(
            "fixture pair count exceeds COUNT(*) Int64 bound".into(),
        ));
    }
    let rule = CountJoinRewrite;
    let config = SessionConfig::new()
        .set_bool("datafusion.optimizer.skip_failed_rules", false)
        .set_usize("datafusion.execution.target_partitions", 1);
    let state = SessionStateBuilder::new_with_default_features()
        .with_config(config)
        .with_optimizer_rule(Arc::new(rule))
        .build();
    Ok(SessionContext::new_with_state(state))
}
