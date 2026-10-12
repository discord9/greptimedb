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

use std::sync::Arc;

use arrow::array::{Array, Float64Array, StringArray};
use arrow::util::display::array_value_to_string;
use catalog::RegisterTableRequest;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::OutputData;
use common_recordbatch::{RecordBatch, util};
use datafusion::functions_aggregate::count::Count;
use datafusion::functions_aggregate::sum::Sum;
use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysicalColumn};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::ScalarValue;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_expr::{LogicalPlan, Operator};
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
use session::context::QueryContext;
use table::test_util::MemTable;

use crate::optimizer::count_join::{
    AvgRewrite, CountJoinRewrite, PayloadFunction, approx_avg_domain, approx_avg_domain_bound,
    avg_domain_bound, avg_rewrite_for, exact_avg_domain,
};
use crate::options::QueryOptions as EngineOptions;
use crate::parser::QueryLanguageParser;
use crate::query_engine::QueryEngineFactory;

fn engine(
    with_rewrite: bool,
    fact: RecordBatch,
    dim: RecordBatch,
) -> crate::query_engine::QueryEngineRef {
    // The pair bound is rule state, so the rule is built from the real row
    // counts of the fixtures it will be asked to rewrite.
    let rule =
        with_rewrite.then(|| CountJoinRewrite::new(fact.num_rows() as u64, dim.num_rows() as u64));
    engine_with_rule(rule, fact, dim)
}

/// Builds a query engine and, when given, installs `rule` on it.
///
/// The rule is never part of the default engine construction: the caller hands
/// over the exact instance, and therefore the exact bound, to use.
fn engine_with_rule(
    rule: Option<CountJoinRewrite>,
    fact: RecordBatch,
    dim: RecordBatch,
) -> crate::query_engine::QueryEngineRef {
    let catalog = catalog::memory::new_memory_catalog_manager().unwrap();
    for (name, table_id, batch) in [("fact", 101, fact), ("dim", 102, dim)] {
        let table = MemTable::new_with_catalog(
            name,
            batch,
            table_id,
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_SCHEMA_NAME.to_string(),
        );
        catalog
            .register_table_sync(RegisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_SCHEMA_NAME.to_string(),
                table_name: name.to_string(),
                table_id,
                table,
            })
            .unwrap();
    }
    let engine = QueryEngineFactory::new(
        catalog,
        None,
        None,
        None,
        None,
        false,
        EngineOptions {
            parallelism: 1,
            ..Default::default()
        },
    )
    .query_engine();
    if let Some(rule) = rule {
        engine
            .engine_state()
            .df_context
            .add_optimizer_rule(Arc::new(rule));
    }
    engine
}

fn batches(
    facts: &[Option<&str>],
    values: &[Option<f64>],
    dims: &[(Option<&str>, Option<&str>)],
) -> (RecordBatch, RecordBatch) {
    let fact = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(
                (0..facts.len() as i64).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringVector::from(facts.to_vec())) as _,
            Arc::new(Float64Vector::from(values.to_vec())) as _,
        ],
    )
    .unwrap();
    let dim = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("area", ConcreteDataType::string_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(
                (0..dims.len() as i64).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringVector::from(
                dims.iter().map(|(host, _)| *host).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringVector::from(
                dims.iter().map(|(_, area)| *area).collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap();
    (fact, dim)
}

/// Builds a `fact`/`dim` fixture pair whose every row carries a nullable
/// payload: `fact.value` on the left, `dim.weight` on the right.
fn weighted_batches(
    facts: &[Option<&str>],
    values: &[Option<f64>],
    dims: &[(Option<&str>, Option<&str>, Option<f64>)],
) -> (RecordBatch, RecordBatch) {
    let fact = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("value", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(
                (0..facts.len() as i64).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringVector::from(facts.to_vec())) as _,
            Arc::new(Float64Vector::from(values.to_vec())) as _,
        ],
    )
    .unwrap();
    let dim = RecordBatch::new(
        Arc::new(Schema::new(vec![
            ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            )
            .with_time_index(true),
            ColumnSchema::new("host", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("area", ConcreteDataType::string_datatype(), true),
            ColumnSchema::new("weight", ConcreteDataType::float64_datatype(), true),
        ])),
        vec![
            Arc::new(TimestampMillisecondVector::from_slice(
                (0..dims.len() as i64).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringVector::from(
                dims.iter().map(|(host, _, _)| *host).collect::<Vec<_>>(),
            )) as _,
            Arc::new(StringVector::from(
                dims.iter().map(|(_, area, _)| *area).collect::<Vec<_>>(),
            )) as _,
            Arc::new(Float64Vector::from(
                dims.iter()
                    .map(|(_, _, weight)| *weight)
                    .collect::<Vec<_>>(),
            )) as _,
        ],
    )
    .unwrap();
    (fact, dim)
}

const COUNT_SQL: &str = "SELECT f.host, d.area, COUNT(*) AS n FROM fact f JOIN dim d ON f.host = d.host GROUP BY f.host, d.area ORDER BY n DESC, f.host NULLS LAST, d.area NULLS LAST";

#[tokio::test]
async fn original_count_sql_matches_independent_counts_and_physical_plan() {
    let (fact, dim) = batches(
        &[
            Some("a"),
            Some("a"),
            Some("a"),
            Some("b"),
            Some("b"),
            None,
            Some("missing"),
        ],
        &[None, None, Some(2.0), None, None, Some(4.0), Some(5.0)],
        &[
            (Some("a"), Some("west")),
            (Some("a"), Some("west")),
            (Some("a"), None),
            (Some("b"), Some("east")),
            (Some("b"), Some("east")),
            (Some("b"), Some("west")),
            (None, Some("null-key")),
            (Some("orphan"), None),
        ],
    );
    let fact_rows = fact.num_rows() as u64;
    let dim_rows = dim.num_rows() as u64;
    let rule = CountJoinRewrite::new(fact_rows, dim_rows);
    assert_eq!(
        Some(fact_rows * dim_rows),
        rule.pair_bound(),
        "the fixture pair bound must be provable by the rule"
    );
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = engine_with_rule(Some(rule), fact, dim);
    assert_rule_installation(&baseline, &candidate);

    let (base_schema, base_rows, _base_plan) = run(&baseline, COUNT_SQL).await;
    let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, COUNT_SQL).await;
    assert_eq!(expected_schema(), base_schema);
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(base_rows, candidate_rows);
    assert_eq!(
        vec!["a|west|6", "b|east|4", "a|NULL|3", "b|west|2"],
        candidate_rows
    );
    let base_logical = optimized_logical(&baseline, COUNT_SQL).await;
    let candidate_logical = optimized_logical(&candidate, COUNT_SQL).await;
    assert_rewritten_logical_shape(&candidate_logical);
    assert_baseline_logical_shape(&base_logical);
    assert_rewritten_physical_shape(candidate_plan.as_ref());

    // COUNT(nullable value) must stay on the unmodified aggregate path.
    let negative = "SELECT f.host, d.area, COUNT(f.value) AS n FROM fact f JOIN dim d ON f.host = d.host GROUP BY f.host, d.area ORDER BY f.host, d.area";
    let (base_schema, base_rows, _base_plan) = run(&baseline, negative).await;
    let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, negative).await;
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(base_rows, candidate_rows);
    assert_eq!(
        vec!["a|west|2", "a|NULL|1", "b|east|0", "b|west|0"],
        candidate_rows
    );
    let baseline_logical = optimized_logical(&baseline, negative).await;
    let candidate_logical = optimized_logical(&candidate, negative).await;
    assert_unrewritten_logical_shape(&candidate_logical);
    assert_baseline_logical_shape(&baseline_logical);
    assert_unrewritten_physical_shape(candidate_plan.as_ref());
}

#[tokio::test]
async fn original_count_sql_top_ten_preserves_group_ties_and_empty_schema() {
    let hosts = (0..32).map(|i| format!("h{i:02}")).collect::<Vec<_>>();
    let host_refs = hosts.iter().map(|s| Some(s.as_str())).collect::<Vec<_>>();
    let dim_rows = hosts
        .iter()
        .map(|s| (Some(s.as_str()), Some("area")))
        .collect::<Vec<_>>();
    let (fact, dim) = batches(&host_refs, &vec![None; 32], &dim_rows);
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = engine(true, fact, dim);
    assert_rule_installation(&baseline, &candidate);
    let sql = COUNT_SQL;
    let (base_schema, all_base, base_plan) = run(&baseline, sql).await;
    let (candidate_schema, all_candidate, candidate_plan) = run(&candidate, sql).await;
    let expected = hosts
        .iter()
        .map(|host| format!("{host}|area|1"))
        .collect::<Vec<_>>();
    assert_eq!(expected_schema(), base_schema);
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(expected, all_base);
    assert_eq!(all_base, all_candidate);
    let optimized = optimized_logical(&candidate, sql).await;
    assert_rewritten_logical_shape(&optimized);
    assert_rewritten_physical_shape(candidate_plan.as_ref());
    assert_baseline_logical_shape(&optimized_logical(&baseline, sql).await);
    assert_baseline_physical_shape(base_plan.as_ref());

    let top = format!("{sql} LIMIT 10");
    let (base_schema, top_base, _) = run(&baseline, &top).await;
    let (candidate_schema, top_candidate, _) = run(&candidate, &top).await;
    assert_eq!(expected_schema(), base_schema);
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(expected[..10], top_base);
    assert_eq!(top_base, top_candidate);
    assert_eq!(22, all_candidate.len() - top_candidate.len());

    let (empty_fact, dim) = batches(&[], &[], &[(Some("a"), Some("area"))]);
    let empty_baseline = engine(false, empty_fact.clone(), dim.clone());
    let empty_candidate = engine(true, empty_fact, dim);
    let (base_schema, base_rows, _) = run(&empty_baseline, sql).await;
    let (candidate_schema, candidate_rows, _) = run(&empty_candidate, sql).await;
    assert_eq!(expected_schema(), base_schema);
    assert_eq!(base_schema, candidate_schema);
    assert!(base_rows.is_empty());
    assert_eq!(base_rows, candidate_rows);
}

/// A pair bound that cannot prove the join stays inside `COUNT(*)`'s `Int64`
/// result must make the rule refuse to rewrite. The refused plan keeps the
/// unrewritten shape and still returns the same rows as the baseline.
#[tokio::test]
async fn unsatisfiable_pair_bound_refuses_rewrite_and_keeps_results() {
    // Exactly at the representable bound the rewrite is still allowed; one pair
    // more, a `u64` multiplication overflow, or a symmetric overflow are not.
    assert_eq!(
        Some(i64::MAX as u64),
        CountJoinRewrite::new(i64::MAX as u64, 1).pair_bound()
    );
    assert_eq!(
        None,
        CountJoinRewrite::new(i64::MAX as u64 + 1, 1).pair_bound()
    );
    assert_eq!(None, CountJoinRewrite::new(u64::MAX, 2).pair_bound());
    assert_eq!(None, CountJoinRewrite::new(2, u64::MAX).pair_bound());

    let (fact, dim) = batches(
        &[Some("a"), Some("a"), Some("b"), None],
        &[None, None, Some(1.0), Some(2.0)],
        &[
            (Some("a"), Some("west")),
            (Some("a"), Some("west")),
            (Some("b"), Some("east")),
            (Some("b"), Some("west")),
            (None, Some("null-key")),
            (Some("orphan"), None),
        ],
    );
    let refusing = CountJoinRewrite::new(u64::MAX, 2);
    assert_eq!(None, refusing.pair_bound());
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = engine_with_rule(Some(refusing), fact, dim);
    // The rule is installed, it simply declines to rewrite with this bound.
    assert_rule_installation(&baseline, &candidate);

    let (base_schema, base_rows, _base_plan) = run(&baseline, COUNT_SQL).await;
    let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, COUNT_SQL).await;
    assert_eq!(expected_schema(), base_schema);
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(base_rows, candidate_rows);
    assert_eq!(vec!["a|west|4", "b|east|1", "b|west|1"], candidate_rows);
    assert_baseline_logical_shape(&optimized_logical(&baseline, COUNT_SQL).await);
    assert_unrewritten_logical_shape(&optimized_logical(&candidate, COUNT_SQL).await);
    assert_unrewritten_physical_shape(candidate_plan.as_ref());
}

fn expected_schema() -> Vec<(String, String, bool)> {
    vec![
        ("host".into(), "Utf8".into(), true),
        ("area".into(), "Utf8".into(), true),
        ("n".into(), "Int64".into(), false),
    ]
}

fn assert_rule_installation(
    baseline: &crate::query_engine::QueryEngineRef,
    candidate: &crate::query_engine::QueryEngineRef,
) {
    assert!(
        !baseline
            .engine_state()
            .session_state()
            .optimizers()
            .iter()
            .any(|r| r.name() == "bounded_count_join_rewrite")
    );
    assert_eq!(
        1,
        candidate
            .engine_state()
            .session_state()
            .optimizers()
            .iter()
            .filter(|r| r.name() == "bounded_count_join_rewrite")
            .count()
    );
}

/// The rewritten plan must join exactly two directly aggregated inputs, keep no
/// aggregate above the join, and derive the output count as the product of the
/// two branch `COUNT(*)` outputs.
fn assert_rewritten_logical_shape(plan: &LogicalPlan) {
    let joins = find_logical_joins(plan);
    assert_eq!(1, joins.len(), "{plan:?}");
    let LogicalPlan::Join(join) = joins[0] else {
        unreachable!("find_logical_joins only returns joins");
    };
    let left_count = assert_branch_count(join.left.as_ref(), 1, "left", plan);
    let right_count = assert_branch_count(join.right.as_ref(), 2, "right", plan);
    assert!(
        !contains_aggregate_above_join(plan),
        "aggregate remains above the join: {plan:?}"
    );
    assert_count_product(plan, &left_count, &right_count);
}

/// The unrewritten plan keeps its aggregate above the join and scans both
/// sides without any pre-aggregation.
fn assert_baseline_logical_shape(plan: &LogicalPlan) {
    assert!(
        contains_aggregate_above_join(plan),
        "no aggregate above join: {plan:?}"
    );
    let joins = find_logical_joins(plan);
    assert_eq!(1, joins.len(), "{plan:?}");
    let LogicalPlan::Join(join) = joins[0] else {
        unreachable!("find_logical_joins only returns joins");
    };
    assert!(
        [&join.left, &join.right]
            .iter()
            .all(|input| !contains_logical_aggregate(input)),
        "join input is pre-aggregated: {plan:?}"
    );
}

fn assert_unrewritten_logical_shape(plan: &LogicalPlan) {
    assert_baseline_logical_shape(plan);
}

/// The physical plan must hash-join inputs that are aggregated on both
/// branches, keep no physical aggregate above the join, and project the output
/// count from the two partial-count columns of the join output.
fn assert_rewritten_physical_shape(plan: &dyn ExecutionPlan) {
    assert_eq!(1, count_hash_joins(plan), "{plan:?}");
    assert!(
        check_join_branches(plan, false),
        "no HashJoinExec with aggregated branches: {plan:?}"
    );
    assert!(
        count_product_projection(plan),
        "no projection multiplies the two partial counts: {plan:?}"
    );
}

fn assert_baseline_physical_shape(plan: &dyn ExecutionPlan) {
    assert_unrewritten_physical_shape(plan);
}

fn assert_unrewritten_physical_shape(plan: &dyn ExecutionPlan) {
    fn join_has_no_preaggregate(plan: &dyn ExecutionPlan) -> bool {
        if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
            return !contains_physical_aggregate(join.left().as_ref())
                && !contains_physical_aggregate(join.right().as_ref());
        }
        plan.children()
            .iter()
            .all(|child| join_has_no_preaggregate(child.as_ref()))
    }
    assert_eq!(1, count_hash_joins(plan), "{plan:?}");
    assert!(join_has_no_preaggregate(plan), "{plan:?}");
}

/// Returns the branch `COUNT(*)` output column name after checking the branch
/// is a direct aggregate of `COUNT(*)` over the expected number of group
/// columns.
fn assert_branch_count(
    input: &LogicalPlan,
    expected_groups: usize,
    side: &str,
    plan: &LogicalPlan,
) -> String {
    let LogicalPlan::Aggregate(aggregate) = input else {
        panic!("{side} join branch is not a direct aggregate: {plan:?}")
    };
    assert_eq!(
        expected_groups,
        aggregate.group_expr.len(),
        "{side} branch group columns: {plan:?}"
    );
    assert!(
        aggregate
            .group_expr
            .iter()
            .all(|expr| matches!(expr, datafusion_expr::Expr::Column(_))),
        "{side} branch groups by non-column expressions: {plan:?}"
    );
    assert_eq!(1, aggregate.aggr_expr.len(), "{plan:?}");
    // The optimizer keeps each branch count aliased (`COUNT(*) AS partial_count_n`).
    let function = match &aggregate.aggr_expr[0] {
        datafusion_expr::Expr::AggregateFunction(function) => function,
        datafusion_expr::Expr::Alias(alias) => match alias.expr.as_ref() {
            datafusion_expr::Expr::AggregateFunction(function) => function,
            _ => panic!("{side} branch alias does not wrap a function: {plan:?}"),
        },
        _ => panic!("{side} branch does not aggregate a function: {plan:?}"),
    };
    assert!(
        function.func.inner().downcast_ref::<Count>().is_some(),
        "{side} branch aggregate is not COUNT: {plan:?}"
    );
    let params = &function.params;
    assert!(
        !params.distinct
            && params.filter.is_none()
            && params.order_by.is_empty()
            && params.null_treatment.is_none()
            && params.args == [datafusion_expr::Expr::Literal(COUNT_STAR_EXPANSION, None)],
        "{side} branch aggregate is not plain COUNT(*): {plan:?}"
    );
    aggregate
        .schema
        .qualified_field(aggregate.schema.fields().len() - 1)
        .1
        .name()
        .to_string()
}

fn assert_count_product(plan: &LogicalPlan, left_count: &str, right_count: &str) {
    fn multiplies_counts(plan: &LogicalPlan, left_count: &str, right_count: &str) -> bool {
        if let LogicalPlan::Projection(projection) = plan {
            let product = projection.expr.iter().any(|expr| {
                // The product reaches the output through an alias (`... AS n`).
                let expr = match expr {
                    datafusion_expr::Expr::Alias(alias) => alias.expr.as_ref(),
                    expr => expr,
                };
                let datafusion_expr::Expr::BinaryExpr(binary) = expr else {
                    return false;
                };
                if binary.op != Operator::Multiply {
                    return false;
                }
                let (datafusion_expr::Expr::Column(left), datafusion_expr::Expr::Column(right)) =
                    (binary.left.as_ref(), binary.right.as_ref())
                else {
                    return false;
                };
                (left.name() == left_count && right.name() == right_count)
                    || (left.name() == right_count && right.name() == left_count)
            });
            if product {
                return true;
            }
        }
        plan.inputs()
            .iter()
            .any(|input| multiplies_counts(input, left_count, right_count))
    }
    assert!(
        multiplies_counts(plan, left_count, right_count),
        "no projection multiplies {left_count} and {right_count}: {plan:?}"
    );
}

fn count_hash_joins(plan: &dyn ExecutionPlan) -> usize {
    plan.downcast_ref::<HashJoinExec>().map_or(0, |_| 1)
        + plan
            .children()
            .iter()
            .map(|child| count_hash_joins(child.as_ref()))
            .sum::<usize>()
}

/// Visits the plan and, for every hash join, asserts no aggregate sits above
/// it and both of its branches contain aggregates.
fn check_join_branches(plan: &dyn ExecutionPlan, aggregate_above: bool) -> bool {
    let aggregate_above = aggregate_above || plan.downcast_ref::<AggregateExec>().is_some();
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        assert!(
            !aggregate_above,
            "aggregate remains above HashJoinExec: {plan:?}"
        );
        assert!(
            contains_physical_aggregate(join.left().as_ref()),
            "left HashJoinExec branch is not aggregated: {plan:?}"
        );
        assert!(
            contains_physical_aggregate(join.right().as_ref()),
            "right HashJoinExec branch is not aggregated: {plan:?}"
        );
        return true;
    }
    plan.children()
        .iter()
        .any(|child| check_join_branches(child.as_ref(), aggregate_above))
}

fn count_product_projection(plan: &dyn ExecutionPlan) -> bool {
    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let input_schema = plan.children()[0].schema();
        let product = projection.expr().iter().any(|expr| {
            let Some(binary) = expr.expr.downcast_ref::<BinaryExpr>() else {
                return false;
            };
            if binary.op() != &Operator::Multiply {
                return false;
            }
            let (Some(left), Some(right)) = (
                binary.left().downcast_ref::<PhysicalColumn>(),
                binary.right().downcast_ref::<PhysicalColumn>(),
            ) else {
                return false;
            };
            let references_partial_counts = [left.index(), right.index()].iter().all(|index| {
                input_schema
                    .fields()
                    .get(*index)
                    .is_some_and(|field| field.name().starts_with("partial_count"))
            });
            references_partial_counts && left.index() != right.index()
        });
        if product {
            return true;
        }
    }
    plan.children()
        .iter()
        .any(|child| count_product_projection(child.as_ref()))
}

fn contains_physical_aggregate(plan: &dyn ExecutionPlan) -> bool {
    plan.downcast_ref::<AggregateExec>().is_some()
        || plan
            .children()
            .iter()
            .any(|child| contains_physical_aggregate(child.as_ref()))
}

fn contains_logical_aggregate(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Aggregate(_))
        || plan
            .inputs()
            .iter()
            .any(|input| contains_logical_aggregate(input))
}

fn find_logical_joins(plan: &LogicalPlan) -> Vec<&LogicalPlan> {
    let mut joins = Vec::new();
    if matches!(plan, LogicalPlan::Join(_)) {
        joins.push(plan);
    }
    for input in plan.inputs() {
        joins.extend(find_logical_joins(input));
    }
    joins
}

fn contains_logical_join(plan: &LogicalPlan) -> bool {
    matches!(plan, LogicalPlan::Join(_))
        || plan
            .inputs()
            .iter()
            .any(|input| contains_logical_join(input))
}

/// Detects any aggregate that dominates a join through arbitrary
/// intermediate nodes, so projections between them cannot hide the shape.
fn contains_aggregate_above_join(plan: &LogicalPlan) -> bool {
    (matches!(plan, LogicalPlan::Aggregate(_))
        && plan
            .inputs()
            .iter()
            .any(|input| contains_logical_join(input)))
        || plan
            .inputs()
            .iter()
            .any(|input| contains_aggregate_above_join(input))
}

async fn optimized_logical(engine: &crate::query_engine::QueryEngineRef, sql: &str) -> LogicalPlan {
    let ctx = QueryContext::arc();
    let stmt = QueryLanguageParser::parse_sql(sql, &ctx).unwrap();
    let plan = engine.planner().plan(&stmt, ctx).await.unwrap();
    engine.engine_state().optimize_logical_plan(plan).unwrap()
}

async fn run(
    engine: &crate::query_engine::QueryEngineRef,
    sql: &str,
) -> (
    Vec<(String, String, bool)>,
    Vec<String>,
    Arc<dyn ExecutionPlan>,
) {
    let ctx = QueryContext::arc();
    let stmt = QueryLanguageParser::parse_sql(sql, &ctx).unwrap();
    let plan = engine.planner().plan(&stmt, ctx.clone()).await.unwrap();
    let output = engine.execute(plan, ctx).await.unwrap();
    let physical_plan = output
        .meta
        .plan
        .expect("query execution provides a physical plan");
    let OutputData::Stream(stream) = output.data else {
        panic!("expected query stream")
    };
    let schema = stream.schema();
    let signature = schema
        .column_schemas()
        .iter()
        .map(|field| {
            (
                field.name.to_string(),
                format!("{:?}", field.data_type.as_arrow_type()),
                field.is_nullable(),
            )
        })
        .collect();
    let rows = util::collect(stream)
        .await
        .unwrap()
        .iter()
        .flat_map(|batch| {
            (0..batch.num_rows()).map(|row| {
                (0..batch.num_columns())
                    .map(|col| {
                        if batch.column(col).is_null(row) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(batch.column(col).as_ref(), row).unwrap()
                        }
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            })
        })
        .collect();
    (signature, rows, physical_plan)
}

// ---------------------------------------------------------------------------
// Certified-domain `AVG(value * weight)` rewrite.
//
// The rule only ever sees a `LogicalPlan`, so the exact-domain certificate is
// computed here, over the real fixture batches, and handed to the rule at
// construction — exactly like the pair bound. Every engine below is therefore
// built the way a production caller would have to build it.
// ---------------------------------------------------------------------------

/// The supported `AVG(value * weight)` query, ordered the way the paper's
/// fixture orders its scores.
const AVG_SQL: &str = "SELECT f.host, d.area, AVG(f.value * d.weight) AS score FROM fact f JOIN dim d ON f.host = d.host GROUP BY f.host, d.area ORDER BY score DESC NULLS LAST, f.host NULLS LAST, d.area NULLS LAST";

/// Builds an engine whose rule carries the certificate
/// [`exact_avg_domain`] computes for these very batches.
fn avg_engine(fact: RecordBatch, dim: RecordBatch) -> crate::query_engine::QueryEngineRef {
    let rule = CountJoinRewrite::with_avg(
        fact.num_rows() as u64,
        dim.num_rows() as u64,
        exact_avg_domain(&fact, &dim),
    );
    engine_with_rule(Some(rule), fact, dim)
}

/// Builds an engine whose rule is told the domain is certified, whatever the
/// batches say. Only used to show that a shape — not the certificate — is what
/// keeps a query on the baseline plan.
fn forced_avg_engine(fact: RecordBatch, dim: RecordBatch) -> crate::query_engine::QueryEngineRef {
    let rule = CountJoinRewrite::with_avg(fact.num_rows() as u64, dim.num_rows() as u64, true);
    engine_with_rule(Some(rule), fact, dim)
}

/// The supported `SUM(value * weight)` query, ordered the way [`AVG_SQL`] orders
/// its scores.
const SUM_SQL: &str = "SELECT f.host, d.area, SUM(f.value * d.weight) AS score FROM fact f JOIN dim d ON f.host = d.host GROUP BY f.host, d.area ORDER BY score DESC NULLS LAST, f.host NULLS LAST, d.area NULLS LAST";

/// Builds an engine whose rule carries the approximate fuse
/// [`approx_avg_domain`] computes for these very batches. The exact certificate
/// is not consulted, exactly as the constructor documents.
fn approx_engine(fact: RecordBatch, dim: RecordBatch) -> crate::query_engine::QueryEngineRef {
    let rule = CountJoinRewrite::with_approx_avg(
        fact.num_rows() as u64,
        dim.num_rows() as u64,
        approx_avg_domain(&fact, &dim),
    );
    engine_with_rule(Some(rule), fact, dim)
}

/// Builds an engine whose rule carries both verdicts: the exact certificate
/// whenever it holds, the approximate fuse for the rest.
fn exact_or_approx_engine(
    fact: RecordBatch,
    dim: RecordBatch,
) -> crate::query_engine::QueryEngineRef {
    let rule = CountJoinRewrite::with_avg_and_approx(
        fact.num_rows() as u64,
        dim.num_rows() as u64,
        exact_avg_domain(&fact, &dim),
        approx_avg_domain(&fact, &dim),
    );
    engine_with_rule(Some(rule), fact, dim)
}

/// Facts and dims inside the certified domain: integer values, quarter weights,
/// duplicate `(host, area)` dim rows, a fact-only host and a dim-only host.
///
/// Hand-computed `AVG(value * weight)` per `(host, area)`:
///
/// - `(a, east)`: `(2, -1, 3) x (2.5)` sums to `10`, over 3 pairs -> `10/3`
/// - `(b, west)`: `(4, 0) x (1, -0.5)` sums to `2`, over 4 pairs -> `0.5`
/// - `(b, east)`: `(4, 0) x (0)` sums to `0`, over 2 pairs -> `+0.0`
/// - `(a, west)`: `(2, -1, 3) x (-1, 0.25)` sums to `-3`, over 6 pairs -> `-0.5`
/// - `(d, *)` and `(c, west)` never pair up, so they produce no row.
fn certified_batches() -> (RecordBatch, RecordBatch) {
    weighted_batches(
        &[
            Some("a"),
            Some("a"),
            Some("a"),
            Some("b"),
            Some("b"),
            Some("d"),
        ],
        &[
            Some(2.0),
            Some(-1.0),
            Some(3.0),
            Some(4.0),
            Some(0.0),
            Some(7.0),
        ],
        &[
            (Some("a"), Some("west"), Some(-1.0)),
            (Some("a"), Some("west"), Some(0.25)),
            (Some("a"), Some("east"), Some(2.5)),
            (Some("b"), Some("west"), Some(1.0)),
            (Some("b"), Some("west"), Some(-0.5)),
            (Some("b"), Some("east"), Some(0.0)),
            (Some("c"), Some("west"), Some(3.75)),
        ],
    )
}

/// The hand-computed rows of `AVG_SQL` over [`certified_batches`], in the
/// emitted order, with each score reduced to its bits.
fn certified_expected_scores() -> Vec<(String, String, Option<u64>)> {
    vec![
        ("a".into(), "east".into(), Some((10.0f64 / 3.0).to_bits())),
        ("b".into(), "west".into(), Some(0.5f64.to_bits())),
        ("b".into(), "east".into(), Some(0.0f64.to_bits())),
        ("a".into(), "west".into(), Some((-0.5f64).to_bits())),
    ]
}

/// Runs a query and returns `(host, area, score bits)` per row, keeping the
/// emitted order and distinguishing a `NULL` score from any bit pattern.
async fn score_rows(
    engine: &crate::query_engine::QueryEngineRef,
    sql: &str,
) -> Vec<(String, String, Option<u64>)> {
    let ctx = QueryContext::arc();
    let stmt = QueryLanguageParser::parse_sql(sql, &ctx).unwrap();
    let plan = engine.planner().plan(&stmt, ctx.clone()).await.unwrap();
    let output = engine.execute(plan, ctx).await.unwrap();
    let OutputData::Stream(stream) = output.data else {
        panic!("expected query stream")
    };
    let mut rows = Vec::new();
    for batch in util::collect(stream).await.unwrap() {
        let host = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let area = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let score = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for row in 0..batch.num_rows() {
            let text = |array: &StringArray, row: usize| {
                (!array.is_null(row)).then(|| array.value(row).to_string())
            };
            rows.push((
                text(host, row).unwrap_or_else(|| "NULL".to_string()),
                text(area, row).unwrap_or_else(|| "NULL".to_string()),
                (!score.is_null(row)).then(|| score.value(row).to_bits()),
            ));
        }
    }
    rows
}

/// The certified `AVG(value * weight)` query must leave the baseline plan, and
/// return the baseline's rows with every score bit-identical to it.
#[tokio::test]
async fn certified_avg_is_rewritten_and_bitwise_equal_to_the_baseline() {
    let (fact, dim) = certified_batches();
    assert!(exact_avg_domain(&fact, &dim));
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = avg_engine(fact, dim);
    assert_rule_installation(&baseline, &candidate);

    let (base_schema, base_rows, _) = run(&baseline, AVG_SQL).await;
    let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, AVG_SQL).await;
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(
        vec![
            ("host".to_string(), "Utf8".to_string(), true),
            ("area".to_string(), "Utf8".to_string(), true),
            ("score".to_string(), "Float64".to_string(), true),
        ],
        base_schema
    );
    assert_eq!(base_rows, candidate_rows);
    assert_eq!(
        vec![
            "a|east|3.3333333333333335",
            "b|west|0.5",
            "b|east|0.0",
            "a|west|-0.5",
        ],
        candidate_rows
    );

    // Every score is bit-identical to the baseline's, and to the hand-computed
    // row set.
    let base_scores = score_rows(&baseline, AVG_SQL).await;
    let candidate_scores = score_rows(&candidate, AVG_SQL).await;
    assert_eq!(base_scores, candidate_scores);
    assert_eq!(certified_expected_scores(), candidate_scores);

    // The emitted plan carries partial aggregates and reconstructs the average
    // instead of averaging over the join.
    let baseline_logical = optimized_logical(&baseline, AVG_SQL).await;
    let candidate_logical = optimized_logical(&candidate, AVG_SQL).await;
    assert_rewritten_avg_logical_shape(&candidate_logical);
    assert_baseline_logical_shape(&baseline_logical);
    assert_rewritten_avg_physical_shape(candidate_plan.as_ref());
}

/// The single case where a naive factorization differs from the pairwise sum:
/// the branch sums multiply into `-0.0`, while the baseline numerator cancels
/// to `+0.0`. The rewrite must return the baseline's `+0.0`, not `-0.0`.
#[tokio::test]
async fn certified_avg_normalizes_signed_zero_like_the_baseline() {
    let (fact, dim) = weighted_batches(
        &[Some("a"), Some("a")],
        &[Some(1.0), Some(-1.0)],
        &[(Some("a"), Some("west"), Some(-1.0))],
    );
    assert!(exact_avg_domain(&fact, &dim));
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = avg_engine(fact, dim);

    let base_scores = score_rows(&baseline, AVG_SQL).await;
    let candidate_scores = score_rows(&candidate, AVG_SQL).await;
    assert_eq!(
        vec![("a".to_string(), "west".to_string(), Some(0.0f64.to_bits()))],
        base_scores,
        "the pairwise numerator cancels to +0.0"
    );
    assert_eq!(base_scores, candidate_scores);
    assert_ne!(
        Some((-0.0f64).to_bits()),
        candidate_scores[0].2,
        "the rewrite must not emit a negative zero"
    );
    assert_rewritten_avg_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);
}

/// An ordered, limited certified query must emit the baseline's rows in the
/// baseline's order.
#[tokio::test]
async fn certified_avg_ordered_limit_emits_the_baseline_order() {
    let (fact, dim) = certified_batches();
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = avg_engine(fact, dim);

    let top = format!("{AVG_SQL} LIMIT 2");
    let (base_schema, top_base, _) = run(&baseline, &top).await;
    let (candidate_schema, top_candidate, _) = run(&candidate, &top).await;
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(top_base, top_candidate);
    assert_eq!(
        vec!["a|east|3.3333333333333335", "b|west|0.5"],
        top_candidate
    );

    let all_candidate = run(&candidate, AVG_SQL).await;
    assert_eq!(all_candidate.1, run(&baseline, AVG_SQL).await.1);
    assert_eq!(2, all_candidate.1.len() - top_candidate.len());
    assert_eq!(
        score_rows(&baseline, &top).await,
        score_rows(&candidate, &top).await
    );
}

/// Payloads outside the certified domain must leave the `AVG` on the baseline
/// plan and return the baseline's results. The shape itself still matches: the
/// same plan is rewritten when the certificate is forced, so it is the
/// certificate — not the shape — that declines here.
#[tokio::test]
async fn uncertified_avg_payloads_keep_the_baseline_plan_and_results() {
    type Dims = Vec<(Option<&'static str>, Option<&'static str>, Option<f64>)>;
    /// `(name, fact hosts, fact values, dim rows)` of one uncertified fixture.
    type Case = (
        &'static str,
        Vec<Option<&'static str>>,
        Vec<Option<f64>>,
        Dims,
    );
    let cases: Vec<Case> = vec![
        (
            "non-integer value",
            vec![Some("a"), Some("a")],
            vec![Some(1.5), Some(2.0)],
            vec![(Some("a"), Some("west"), Some(1.0))],
        ),
        (
            "non-quarter weight",
            vec![Some("a")],
            vec![Some(2.0)],
            vec![(Some("a"), Some("west"), Some(0.3))],
        ),
        (
            "weight above four",
            vec![Some("a")],
            vec![Some(2.0)],
            vec![(Some("a"), Some("west"), Some(4.25))],
        ),
        (
            "all-null value group",
            vec![Some("a"), Some("a"), Some("b")],
            vec![None, None, Some(1.0)],
            vec![
                (Some("a"), Some("west"), Some(1.0)),
                (Some("b"), Some("east"), Some(1.0)),
            ],
        ),
        (
            "all-null weight group",
            vec![Some("a"), Some("b")],
            vec![Some(1.0), Some(2.0)],
            vec![
                (Some("a"), Some("west"), None),
                (Some("b"), Some("east"), Some(1.0)),
            ],
        ),
        (
            "nan value",
            vec![Some("a")],
            vec![Some(f64::NAN)],
            vec![(Some("a"), Some("west"), Some(1.0))],
        ),
        (
            "infinite value",
            vec![Some("a")],
            vec![Some(f64::INFINITY)],
            vec![(Some("a"), Some("west"), Some(1.0))],
        ),
        (
            "nan weight",
            vec![Some("a")],
            vec![Some(2.0)],
            vec![(Some("a"), Some("west"), Some(f64::NAN))],
        ),
        (
            "infinite weight",
            vec![Some("a")],
            vec![Some(2.0)],
            vec![(Some("a"), Some("west"), Some(f64::NEG_INFINITY))],
        ),
    ];

    for (name, hosts, values, dims) in cases {
        let (fact, dim) = weighted_batches(&hosts, &values, &dims);
        assert!(
            !exact_avg_domain(&fact, &dim),
            "{name}: the certificate must reject the payloads"
        );
        let baseline = engine(false, fact.clone(), dim.clone());
        let candidate = avg_engine(fact.clone(), dim.clone());
        assert_rule_installation(&baseline, &candidate);

        let (base_schema, base_rows, _) = run(&baseline, AVG_SQL).await;
        let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, AVG_SQL).await;
        assert_eq!(base_schema, candidate_schema, "{name}");
        assert_eq!(base_rows, candidate_rows, "{name}");
        assert_eq!(
            score_rows(&baseline, AVG_SQL).await,
            score_rows(&candidate, AVG_SQL).await,
            "{name}"
        );
        assert_baseline_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);
        assert_unrewritten_physical_shape(candidate_plan.as_ref());

        // The same shape, with the certificate forced, is rewritten: the
        // certificate is what keeps this query on the baseline.
        let forced = forced_avg_engine(fact, dim);
        assert_rewritten_avg_logical_shape(&optimized_logical(&forced, AVG_SQL).await);

        // The rule instance stays live: `COUNT(*)` still rewrites.
        assert_rewritten_logical_shape(&optimized_logical(&candidate, COUNT_SQL).await);
    }
}

/// Shapes that are near the one supported `AVG`/`SUM` form must keep the
/// baseline plan, even though the payloads are certified. `SUM(value * weight)`
/// is deliberately among them for a certified batch: the exact path is
/// `AVG`-only, so only the approximate path (covered further down) rewrites it.
#[tokio::test]
async fn near_miss_avg_shapes_keep_the_baseline_plan() {
    let (fact, dim) = certified_batches();
    assert!(exact_avg_domain(&fact, &dim));
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = forced_avg_engine(fact, dim);
    assert_rewritten_avg_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);

    let select = |exprs: &str| {
        format!(
            "SELECT f.host, d.area, {exprs} FROM fact f JOIN dim d ON f.host = d.host \
             GROUP BY f.host, d.area ORDER BY score DESC NULLS LAST, f.host NULLS LAST, d.area NULLS LAST"
        )
    };
    let shapes = [
        ("avg of one payload", select("AVG(f.value) AS score")),
        (
            "sum of the product",
            select("SUM(f.value * d.weight) AS score"),
        ),
        ("sum of one payload", select("SUM(f.value) AS score")),
        (
            "sum of the product plus a constant",
            select("SUM(f.value * d.weight + 1) AS score"),
        ),
        (
            "distinct sum of the product",
            select("SUM(DISTINCT f.value * d.weight) AS score"),
        ),
        (
            "product plus a constant",
            select("AVG(f.value * d.weight + 1) AS score"),
        ),
        (
            "distinct product",
            select("AVG(DISTINCT f.value * d.weight) AS score"),
        ),
        (
            "filtered product",
            select("AVG(f.value * d.weight) FILTER (WHERE f.value > 0) AS score"),
        ),
        (
            "extra aggregate",
            select("AVG(f.value * d.weight) AS score, COUNT(*) AS n"),
        ),
        (
            "same-side product",
            select("AVG(f.value * f.value) AS score"),
        ),
    ];
    for (name, sql) in shapes {
        let (base_schema, base_rows, _) = run(&baseline, &sql).await;
        let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, &sql).await;
        assert_eq!(base_schema, candidate_schema, "{name}");
        assert_eq!(base_rows, candidate_rows, "{name}");
        assert_eq!(
            score_rows(&baseline, &sql).await,
            score_rows(&candidate, &sql).await,
            "{name}"
        );
        assert_baseline_logical_shape(&optimized_logical(&candidate, &sql).await);
        assert_unrewritten_physical_shape(candidate_plan.as_ref());
    }
}

/// A rule constructed without the certificate — `CountJoinRewrite::new` — must
/// rewrite `COUNT(*)` and leave `AVG` on the baseline, even for certified
/// payloads.
#[tokio::test]
async fn count_only_rule_rewrites_count_and_keeps_avg_on_the_baseline() {
    let (fact, dim) = certified_batches();
    assert!(
        exact_avg_domain(&fact, &dim),
        "the payloads are certified, so only the rule's construction withholds the AVG rewrite"
    );
    let rule = CountJoinRewrite::new(fact.num_rows() as u64, dim.num_rows() as u64);
    assert!(!rule.exact_avg());
    assert_eq!(
        Some(fact.num_rows() as u64 * dim.num_rows() as u64),
        rule.pair_bound()
    );
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = engine_with_rule(Some(rule), fact, dim);
    assert_rule_installation(&baseline, &candidate);

    let (base_schema, base_rows, _) = run(&baseline, COUNT_SQL).await;
    let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, COUNT_SQL).await;
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(base_rows, candidate_rows);
    assert_rewritten_logical_shape(&optimized_logical(&candidate, COUNT_SQL).await);
    assert_rewritten_physical_shape(candidate_plan.as_ref());

    let (base_schema, base_rows, _) = run(&baseline, AVG_SQL).await;
    let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, AVG_SQL).await;
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(base_rows, candidate_rows);
    assert_eq!(
        score_rows(&baseline, AVG_SQL).await,
        score_rows(&candidate, AVG_SQL).await
    );
    assert_baseline_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);
    assert_unrewritten_physical_shape(candidate_plan.as_ref());
}

/// The certificate's global bound keeps every intermediate below `2^53`.
#[test]
fn avg_domain_bound_edges_are_enforced() {
    // No payloads, nothing to bound.
    assert!(avg_domain_bound(0, 0));
    // 4 * 1 * 1_000_000 * 140_737_488 * 16 + 2^20 is the largest bound below
    // 2^53; one pair more is not.
    assert!(avg_domain_bound(1, 140_737_488));
    assert!(!avg_domain_bound(1, 140_737_489));
    // Overflow of the `u64` operands themselves must decline, not wrap.
    assert!(!avg_domain_bound(u64::MAX, 1));
    assert!(!avg_domain_bound(1, u64::MAX));
    assert!(!avg_domain_bound(u64::MAX, u64::MAX));
    assert!(avg_domain_bound(3, 2));
}

/// The rewritten `AVG` plan must join exactly two directly aggregated inputs,
/// keep no aggregate above the join, and reconstruct the average from the
/// branch statistics.
fn assert_rewritten_avg_logical_shape(plan: &LogicalPlan) {
    assert_rewritten_payload_logical_shape(plan, PayloadFunction::Avg);
}

/// The rewritten `SUM` plan must join exactly two directly aggregated inputs,
/// keep no aggregate above the join, and reconstruct the sum from the branch
/// statistics.
fn assert_rewritten_sum_logical_shape(plan: &LogicalPlan) {
    assert_rewritten_payload_logical_shape(plan, PayloadFunction::Sum);
}

/// Checks the rewritten payload plan shape: two directly aggregated join
/// branches, no aggregate above the join, and a reconstruction for `function`.
fn assert_rewritten_payload_logical_shape(plan: &LogicalPlan, function: PayloadFunction) {
    let joins = find_logical_joins(plan);
    assert_eq!(1, joins.len(), "{plan:?}");
    let LogicalPlan::Join(join) = joins[0] else {
        unreachable!("find_logical_joins only returns joins");
    };
    let left = assert_branch_partial_stats(join.left.as_ref(), 1, "left", plan);
    let right = assert_branch_partial_stats(join.right.as_ref(), 2, "right", plan);
    assert!(
        !contains_aggregate_above_join(plan),
        "aggregate remains above the join: {plan:?}"
    );
    assert_payload_reconstruction(plan, &left, &right, function);
}

/// Checks a join branch is a direct aggregate of the partial statistics the
/// rewrite needs, in order: an optional `COUNT(*)`, then `COUNT(payload)` and
/// `SUM(payload)`. The `COUNT(*)` partial is dropped again by the optimizer's
/// projection pruning whenever the reconstruction does not read the raw pair
/// count, so both lengths are accepted — but no other statistic is.
///
/// Returns the `[rows, payloads, sum]` output names, with the row count only
/// present when the branch still aggregates it.
fn assert_branch_partial_stats(
    input: &LogicalPlan,
    expected_groups: usize,
    side: &str,
    plan: &LogicalPlan,
) -> [Option<String>; 3] {
    let LogicalPlan::Aggregate(aggregate) = input else {
        panic!("{side} join branch is not a direct aggregate: {plan:?}")
    };
    assert_eq!(
        expected_groups,
        aggregate.group_expr.len(),
        "{side} branch group columns: {plan:?}"
    );
    assert!(
        aggregate
            .group_expr
            .iter()
            .all(|expr| matches!(expr, datafusion_expr::Expr::Column(_))),
        "{side} branch groups by non-column expressions: {plan:?}"
    );
    let fields = aggregate.schema.fields();
    let kinds = aggregate
        .aggr_expr
        .iter()
        .enumerate()
        .map(|(index, expr)| {
            let function = match expr {
                datafusion_expr::Expr::AggregateFunction(function) => function,
                datafusion_expr::Expr::Alias(alias) => match alias.expr.as_ref() {
                    datafusion_expr::Expr::AggregateFunction(function) => function,
                    _ => panic!("{side} branch alias does not wrap a function: {plan:?}"),
                },
                _ => panic!("{side} branch does not aggregate a function: {plan:?}"),
            };
            let params = &function.params;
            assert!(
                !params.distinct
                    && params.filter.is_none()
                    && params.order_by.is_empty()
                    && params.null_treatment.is_none(),
                "{side} branch aggregate carries a modifier: {plan:?}"
            );
            let name = fields[expected_groups + index].name().clone();
            let is_count = function.func.inner().downcast_ref::<Count>().is_some();
            let is_sum = function.func.inner().downcast_ref::<Sum>().is_some();
            assert!(
                is_count || is_sum,
                "{side} branch aggregate is neither COUNT nor SUM: {plan:?}"
            );
            if is_count
                && params.args == [datafusion_expr::Expr::Literal(COUNT_STAR_EXPANSION, None)]
            {
                return ("count(*)", name);
            }
            assert_eq!(1, params.args.len(), "{plan:?}");
            assert!(
                matches!(params.args[0], datafusion_expr::Expr::Column(_)),
                "{side} branch aggregate argument is not a column: {plan:?}"
            );
            if is_count {
                ("count(payload)", name)
            } else {
                ("sum(payload)", name)
            }
        })
        .collect::<Vec<_>>();
    let mut rows = None;
    let mut payloads = None;
    let mut sum = None;
    for (kind, name) in &kinds {
        let slot = match *kind {
            "count(*)" => &mut rows,
            "count(payload)" => &mut payloads,
            "sum(payload)" => &mut sum,
            _ => unreachable!(),
        };
        assert!(
            slot.replace(name.clone()).is_none(),
            "{side} branch repeats the {kind} statistic: {plan:?}"
        );
    }
    // Order check: every emitted statistic is the next one the rewrite emits,
    // so `COUNT(*)` may only be missing, never reordered or replaced.
    let expected_order = kinds.iter().map(|(kind, _)| *kind).collect::<Vec<_>>();
    assert!(
        matches!(
            expected_order.as_slice(),
            ["count(*)", "count(payload)", "sum(payload)"] | ["count(payload)", "sum(payload)"]
        ),
        "{side} branch partial statistics are out of order: {plan:?}"
    );
    assert!(
        payloads.is_some() && sum.is_some(),
        "{side} branch does not aggregate the payload statistics: {plan:?}"
    );
    [rows, payloads, sum]
}

/// Checks the projection above the rewritten join reconstructs the payload
/// function from the branch statistics: `AVG` divides the zero-normalized payload
/// sum product by the (cast) payload count product, `SUM` is that product
/// itself. Both guard an empty payload product with `NULL`.
///
/// The optimizer may hoist the shared payload-count product into its own
/// projection (`__common_expr_*`), so the referenced column names are collected
/// from every projection above the join while the shape checks run on the
/// reconstruction case expression itself.
fn assert_payload_reconstruction(
    plan: &LogicalPlan,
    left: &[Option<String>; 3],
    right: &[Option<String>; 3],
    function: PayloadFunction,
) {
    fn find_case(plan: &LogicalPlan) -> Option<datafusion_expr::Expr> {
        if let LogicalPlan::Projection(projection) = plan {
            for expr in &projection.expr {
                let candidate = match expr {
                    datafusion_expr::Expr::Alias(alias) => alias.expr.as_ref(),
                    expr => expr,
                };
                if matches!(candidate, datafusion_expr::Expr::Case(_)) {
                    return Some(candidate.clone());
                }
            }
        }
        plan.inputs().iter().find_map(|input| find_case(input))
    }

    /// Column names referenced by the expressions of every projection in the
    /// plan.
    fn projected_columns(plan: &LogicalPlan, columns: &mut Vec<String>) {
        if let LogicalPlan::Projection(projection) = plan {
            for expr in &projection.expr {
                expr.apply(|node| {
                    if let datafusion_expr::Expr::Column(column) = node {
                        columns.push(column.name.clone());
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .unwrap();
            }
        }
        for input in plan.inputs() {
            projected_columns(input, columns);
        }
    }

    let expr = find_case(plan).unwrap_or_else(|| panic!("no AVG reconstruction: {plan:?}"));
    let mut columns = Vec::new();
    projected_columns(plan, &mut columns);
    let mut divides = 0;
    let mut casts = 0;
    let mut zero_normalizations = 0;
    expr.apply(|node| {
        match node {
            datafusion_expr::Expr::BinaryExpr(binary) if binary.op == Operator::Divide => {
                divides += 1
            }
            datafusion_expr::Expr::Cast(_) => casts += 1,
            _ => {}
        }
        if let datafusion_expr::Expr::Case(case) = node
            && case.when_then_expr.iter().any(|(when, then)| {
                let against_zero = matches!(
                    when.as_ref(),
                    datafusion_expr::Expr::BinaryExpr(binary)
                        if binary.op == Operator::Eq
                            && matches!(binary.right.as_ref(), datafusion_expr::Expr::Literal(ScalarValue::Float64(Some(zero)), _) if *zero == 0.0)
                );
                let yields_zero = matches!(
                    then.as_ref(),
                    datafusion_expr::Expr::Literal(ScalarValue::Float64(Some(zero)), _) if *zero == 0.0
                );
                against_zero && yields_zero
            })
        {
            zero_normalizations += 1;
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    let (expected_divides, expected_casts) = match function {
        PayloadFunction::Avg => (1, 1),
        PayloadFunction::Sum => (0, 0),
    };
    assert_eq!(
        expected_divides, divides,
        "unexpected division in the {function:?} reconstruction {expr:?}"
    );
    assert_eq!(
        expected_casts, casts,
        "unexpected cast in the {function:?} reconstruction {expr:?}"
    );
    assert_eq!(
        1, zero_normalizations,
        "no signed-zero normalization in {expr:?}"
    );
    let payloads = [&left[1], &right[1]];
    let sums = [&left[2], &right[2]];
    for name in payloads.into_iter().chain(sums).flatten() {
        assert!(
            columns.contains(name),
            "reconstruction misses branch statistic {name}: {expr:?}"
        );
    }
    for name in [&left[0], &right[0]].into_iter().flatten() {
        assert!(
            !columns.contains(name),
            "reconstruction references the branch row count {name}: {expr:?}"
        );
    }
}

/// The rewritten `AVG` physical plan must hash-join aggregated branches with no
/// aggregate above the join.
fn assert_rewritten_avg_physical_shape(plan: &dyn ExecutionPlan) {
    assert_eq!(1, count_hash_joins(plan), "{plan:?}");
    assert!(
        check_join_branches(plan, false),
        "no HashJoinExec with aggregated branches: {plan:?}"
    );
}

// ---------------------------------------------------------------------------
// Approximate-mode `AVG(value * weight)` / `SUM(value * weight)` rewrite.
//
// The rewrite is opt-in rule state, exactly like the certificates above: the
// caller runs the anti-absurdity fuse (`approx_avg_domain`) over the real
// batches and hands the verdict to the rule at construction. Nothing here is
// part of any default rule list.
//
// The result is a bounded approximation, not a bitwise match, so the tests
// below compare general-float results within `APPROX_TOLERANCE` and leave the
// bit-for-bit comparisons to the certified-domain tests above. Everything the
// fuse declines — heavy cancellation, overflow-prone magnitudes, subnormal
// magnitudes, non-finite payloads — keeps the baseline plan and must still match
// the baseline exactly.
// ---------------------------------------------------------------------------

/// Standing tolerance of the approximate-mode comparisons: relative for scores
/// far from zero, absolute for the zero neighbourhood. It leaves several orders
/// of magnitude between the factorization's own rounding error and the
/// assertion.
const APPROX_TOLERANCE: f64 = 1e-9;

/// Facts and dims built from ordinary floats: non-integral values, non-quarter
/// weights, a fact-only host and a dim-only host. Every group is far from
/// cancellation and far from either end of the `f64` range, so the approximate
/// fuse accepts the pair even though the exact certificate rejects it.
fn general_float_batches() -> (RecordBatch, RecordBatch) {
    weighted_batches(
        &[
            Some("a"),
            Some("a"),
            Some("a"),
            Some("b"),
            Some("b"),
            Some("c"),
        ],
        &[
            Some(0.273),
            Some(1.8e-5),
            Some(-0.6),
            Some(2.5),
            Some(0.125),
            Some(7.0),
        ],
        &[
            (Some("a"), Some("west"), Some(0.3)),
            (Some("a"), Some("east"), Some(1.7)),
            (Some("a"), Some("east"), Some(-0.25)),
            (Some("b"), Some("west"), Some(-1.5)),
            (Some("orphan"), Some("north"), Some(3.0)),
        ],
    )
}

/// The hand-computed scores of [`general_float_batches`] per `(host, area)`:
/// `SUM` is the payload sum product, `AVG` that product over the pair count.
/// `(a, *)` sums to `-0.326982` over `3` fact rows, `(b, west)` to `2.625` over
/// `2`. The fact-only `(c, *)` and the dim-only `(orphan, north)` never pair up
/// and produce no row.
fn general_float_expected() -> Vec<(&'static str, &'static str, f64, f64)> {
    vec![
        ("a", "west", -0.326982 * 0.3, -0.326982 * 0.3 / 3.0),
        ("a", "east", -0.326982 * 1.45, -0.326982 * 1.45 / 6.0),
        ("b", "west", 2.625 * -1.5, 2.625 * -1.5 / 2.0),
    ]
}

/// Asserts `got` is within `tolerance` of `want`, relative to `want` for values
/// far from zero and absolute around it, so a tiny score and a cancelled one are
/// both compared meaningfully.
fn assert_close(got: f64, want: f64, tolerance: f64, what: &str) {
    let scale = want.abs().max(1.0);
    assert!(
        (got - want).abs() <= tolerance * scale,
        "{what}: {got} is not within {tolerance} of {want}"
    );
}

/// Asserts every `(host, area)` score is within `tolerance` of its expected
/// value, regardless of the emitted order.
fn assert_scores_near(
    rows: &[(String, String, Option<u64>)],
    expected: &[(&str, &str, f64)],
    tolerance: f64,
) {
    assert_eq!(expected.len(), rows.len(), "{rows:?}");
    for (host, area, want) in expected {
        let (_, _, bits) = rows
            .iter()
            .find(|(row_host, row_area, _)| row_host == host && row_area == area)
            .unwrap_or_else(|| panic!("no row for ({host}, {area}) in {rows:?}"));
        assert_close(
            f64::from_bits(bits.unwrap_or_else(|| panic!("({host}, {area}) scored NULL"))),
            *want,
            tolerance,
            &format!("({host}, {area})"),
        );
    }
}

/// Asserts the candidate's scores match the baseline's row for row, in the
/// emitted order, within `tolerance` — and that a `NULL` on one side is a `NULL`
/// on the other.
fn assert_scores_match(
    baseline: &[(String, String, Option<u64>)],
    candidate: &[(String, String, Option<u64>)],
    tolerance: f64,
) {
    assert_eq!(baseline.len(), candidate.len(), "{candidate:?}");
    for (base, scored) in baseline.iter().zip(candidate) {
        assert_eq!(
            (&base.0, &base.1),
            (&scored.0, &scored.1),
            "the rewritten plan changed the group rows"
        );
        assert_eq!(base.2.is_some(), scored.2.is_some(), "{scored:?}");
        if let (Some(base_bits), Some(scored_bits)) = (base.2, scored.2) {
            assert_close(
                f64::from_bits(scored_bits),
                f64::from_bits(base_bits),
                tolerance,
                &format!("({}, {})", base.0, base.1),
            );
        }
    }
}

/// The approximate switch rewrites general-float `AVG(value * weight)` that the
/// exact certificate rejects, and stays within a tolerance of the baseline.
#[tokio::test]
async fn approx_avg_rewrites_general_floats_and_stays_near_the_baseline() {
    let (fact, dim) = general_float_batches();
    assert!(
        !exact_avg_domain(&fact, &dim),
        "the exact certificate must reject non-integral values"
    );
    assert!(
        approx_avg_domain(&fact, &dim),
        "the fuse must accept well-conditioned general floats"
    );
    assert_eq!(AvgRewrite::Approximate, avg_rewrite_for(&fact, &dim, true));
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = approx_engine(fact.clone(), dim.clone());
    let both_verdicts = exact_or_approx_engine(fact, dim);
    assert_rule_installation(&baseline, &candidate);

    let (base_schema, _, _) = run(&baseline, AVG_SQL).await;
    let (candidate_schema, _, candidate_plan) = run(&candidate, AVG_SQL).await;
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(
        ("score".to_string(), "Float64".to_string(), true),
        candidate_schema[2]
    );

    let base_scores = score_rows(&baseline, AVG_SQL).await;
    let candidate_scores = score_rows(&candidate, AVG_SQL).await;
    assert_scores_match(&base_scores, &candidate_scores, APPROX_TOLERANCE);
    assert_scores_near(
        &candidate_scores,
        &general_float_expected()
            .into_iter()
            .map(|(host, area, _, avg)| (host, area, avg))
            .collect::<Vec<_>>(),
        APPROX_TOLERANCE,
    );

    // The approximate rewrite is still the factorized plan shape, and the same
    // rule instance still rewrites `COUNT(*)`.
    assert_rewritten_avg_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);
    assert_baseline_logical_shape(&optimized_logical(&baseline, AVG_SQL).await);
    assert_rewritten_avg_physical_shape(candidate_plan.as_ref());
    assert_rewritten_logical_shape(&optimized_logical(&candidate, COUNT_SQL).await);

    // A rule carrying both verdicts picks the same path on this batch: the exact
    // certificate declines it, so the approximate rewrite applies and the scores
    // are the same as the approximate-only rule's.
    assert_rewritten_avg_logical_shape(&optimized_logical(&both_verdicts, AVG_SQL).await);
    assert_eq!(
        score_rows(&candidate, AVG_SQL).await,
        score_rows(&both_verdicts, AVG_SQL).await,
        "the both-verdict rule falls through to the approximate path"
    );
}

/// The approximate switch rewrites general-float `SUM(value * weight)` too, with
/// the same tolerance against the baseline.
#[tokio::test]
async fn approx_sum_rewrites_general_floats_and_stays_near_the_baseline() {
    let (fact, dim) = general_float_batches();
    assert!(!exact_avg_domain(&fact, &dim));
    assert!(approx_avg_domain(&fact, &dim));
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = approx_engine(fact, dim);

    let (base_schema, _, _) = run(&baseline, SUM_SQL).await;
    let (candidate_schema, _, candidate_plan) = run(&candidate, SUM_SQL).await;
    assert_eq!(base_schema, candidate_schema);

    let base_scores = score_rows(&baseline, SUM_SQL).await;
    let candidate_scores = score_rows(&candidate, SUM_SQL).await;
    assert_scores_match(&base_scores, &candidate_scores, APPROX_TOLERANCE);
    assert_scores_near(
        &candidate_scores,
        &general_float_expected()
            .into_iter()
            .map(|(host, area, sum, _)| (host, area, sum))
            .collect::<Vec<_>>(),
        APPROX_TOLERANCE,
    );

    assert_rewritten_sum_logical_shape(&optimized_logical(&candidate, SUM_SQL).await);
    assert_baseline_logical_shape(&optimized_logical(&baseline, SUM_SQL).await);
    assert_rewritten_avg_physical_shape(candidate_plan.as_ref());
}

/// A batch inside the exact domain keeps the exact rewrite — and every bit of
/// the baseline — even when the approximate switch is on as well. The exact
/// certificate takes precedence; the approximate flag only covers inputs the
/// exact certificate declines.
#[tokio::test]
async fn exact_domain_takes_precedence_over_the_approximate_switch() {
    let (fact, dim) = certified_batches();
    assert!(exact_avg_domain(&fact, &dim));
    assert!(
        approx_avg_domain(&fact, &dim),
        "the fuse accepts the certified domain as well, so only the precedence keeps the exact path"
    );
    assert_eq!(AvgRewrite::Exact, avg_rewrite_for(&fact, &dim, true));
    assert_eq!(AvgRewrite::Exact, avg_rewrite_for(&fact, &dim, false));

    let rule = CountJoinRewrite::with_avg_and_approx(
        fact.num_rows() as u64,
        dim.num_rows() as u64,
        true,
        true,
    );
    assert!(rule.exact_avg() && rule.approx_avg());
    assert_eq!(AvgRewrite::Exact, rule.avg_rewrite());
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = engine_with_rule(Some(rule), fact, dim);

    let base_scores = score_rows(&baseline, AVG_SQL).await;
    let candidate_scores = score_rows(&candidate, AVG_SQL).await;
    assert_eq!(
        base_scores, candidate_scores,
        "the exact path keeps every bit even with the switch on"
    );
    assert_eq!(certified_expected_scores(), candidate_scores);
    assert_rewritten_avg_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);

    // The exact path is `AVG`-only, as it always was: it takes precedence for
    // `AVG` and leaves `SUM` on the baseline plan rather than extending the
    // certified rewrite to it.
    assert_eq!(
        score_rows(&baseline, SUM_SQL).await,
        score_rows(&candidate, SUM_SQL).await,
        "a certified batch keeps every bit of the baseline sum"
    );
    assert_baseline_logical_shape(&optimized_logical(&candidate, SUM_SQL).await);
}

/// A group whose payloads cancel heavily makes the factorization's relative
/// error unbounded, so the fuse must reject the whole batch: the query keeps the
/// baseline plan and the baseline's exact results.
#[tokio::test]
async fn cancellation_fuse_keeps_the_baseline_plan_and_results() {
    type Dims = Vec<(Option<&'static str>, Option<&'static str>, Option<f64>)>;
    /// `(name, fact hosts, fact values, dim rows)` of one cancelling fixture.
    type Case = (
        &'static str,
        Vec<Option<&'static str>>,
        Vec<Option<f64>>,
        Dims,
    );
    let cases: Vec<Case> = vec![
        (
            "fact cancellation",
            vec![Some("a"), Some("a"), Some("a")],
            vec![Some(1e16), Some(1.0), Some(-1e16)],
            vec![
                (Some("a"), Some("west"), Some(0.5)),
                (Some("a"), Some("west"), Some(1.5)),
            ],
        ),
        (
            "dim cancellation",
            vec![Some("a"), Some("a")],
            vec![Some(1e16), Some(1.0)],
            vec![
                (Some("a"), Some("west"), Some(1e16)),
                (Some("a"), Some("west"), Some(-1e16)),
            ],
        ),
    ];

    for (name, hosts, values, dims) in cases {
        let (fact, dim) = weighted_batches(&hosts, &values, &dims);
        assert!(
            !approx_avg_domain(&fact, &dim),
            "{name}: the fuse must reject heavy cancellation"
        );
        assert_eq!(
            AvgRewrite::Baseline,
            avg_rewrite_for(&fact, &dim, true),
            "{name}"
        );

        let baseline = engine(false, fact.clone(), dim.clone());
        let candidate = approx_engine(fact, dim);
        assert_rule_installation(&baseline, &candidate);
        let (base_schema, base_rows, _) = run(&baseline, AVG_SQL).await;
        let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, AVG_SQL).await;
        assert_eq!(base_schema, candidate_schema, "{name}");
        assert_eq!(base_rows, candidate_rows, "{name}");
        assert_eq!(
            score_rows(&baseline, AVG_SQL).await,
            score_rows(&candidate, AVG_SQL).await,
            "{name}"
        );
        // Neither side has a better answer to compare against: at these
        // magnitudes the *baseline's* own summation is already dominated by
        // rounding — the products are `1e32`-sized and their exact sum is `2.0`
        // — which is precisely why the relative error of factorizing them is
        // unbounded and why the query must keep the baseline's own answer
        // instead of a factorized guess.
        assert_baseline_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);
        assert_unrewritten_physical_shape(candidate_plan.as_ref());

        // The refused payload rewrite leaves `COUNT(*)` alone: the rule instance
        // stays live.
        assert_rewritten_logical_shape(&optimized_logical(&candidate, COUNT_SQL).await);
    }

    // The same magnitudes without the cancellation pass: the fuse keys on the
    // group sums, not on the payload magnitudes alone.
    let (fact, dim) = weighted_batches(
        &[Some("a"), Some("a"), Some("a")],
        &[Some(1e16), Some(1e16), Some(-1e16)],
        &[
            (Some("a"), Some("west"), Some(0.5)),
            (Some("a"), Some("west"), Some(1.5)),
        ],
    );
    assert!(approx_avg_domain(&fact, &dim));
}

/// The fuse rejects payloads whose scaled products and sums could leave the
/// `f64` range, so the query keeps the baseline plan. The bound is exercised
/// directly, including at the payload counts no small fixture could materialize.
#[tokio::test]
async fn overflow_fuse_keeps_the_baseline_plan_and_results() {
    // Zero magnitudes and zero payload counts bound nothing, and a `u64` count
    // can never scale a magnitude out of the allowed range on its own.
    assert!(approx_avg_domain_bound(0, 0.0, 0, 0.0));
    assert!(approx_avg_domain_bound(1, 1e300, 1, 1.0));
    assert!(approx_avg_domain_bound(1_000_000, 1.0, 1_000_000, 1.0));
    assert!(approx_avg_domain_bound(u64::MAX, 1.0, u64::MAX, 1.0));
    // A huge count times a huge magnitude does not, nor does their product.
    assert!(!approx_avg_domain_bound(1_000_000_000, 1e300, 1, 1.0));
    assert!(!approx_avg_domain_bound(1, 1e300, 1, 1e300));
    assert!(!approx_avg_domain_bound(4, 1e300, 4, 1e300));
    // A magnitude that is not a finite number is never boundable.
    assert!(!approx_avg_domain_bound(1, f64::INFINITY, 1, 1.0));
    assert!(!approx_avg_domain_bound(1, f64::NAN, 1, 1.0));
    assert!(!approx_avg_domain_bound(1, 1.0, 1, f64::NEG_INFINITY));

    // Each branch of this small fixture stays inside the bound on its own; it is
    // their product, the scaled numerator, that does not.
    let values = [Some(1e300); 4];
    let dims = [(Some("a"), Some("west"), Some(1e300)); 4];
    assert!(approx_avg_domain_bound(4, 1e300, 1, 1.0));
    assert!(approx_avg_domain_bound(1, 1.0, 4, 1e300));
    let (fact, dim) = weighted_batches(&[Some("a"); 4], &values, &dims);
    assert!(
        !approx_avg_domain(&fact, &dim),
        "the scaled numerator is past the bound"
    );

    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = approx_engine(fact, dim);
    let (base_schema, base_rows, _) = run(&baseline, AVG_SQL).await;
    let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, AVG_SQL).await;
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(base_rows, candidate_rows);
    assert_eq!(
        score_rows(&baseline, AVG_SQL).await,
        score_rows(&candidate, AVG_SQL).await
    );
    assert!(
        f64::from_bits(score_rows(&baseline, AVG_SQL).await[0].2.unwrap()).is_infinite(),
        "the fixture's pairwise products overflow, which is what the fuse refuses to factorize"
    );
    assert_baseline_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);
    assert_unrewritten_physical_shape(candidate_plan.as_ref());
}

/// Non-finite payloads have no factorization to reason about, so the approximate
/// fuse rejects them and the query keeps the baseline plan and its results.
#[tokio::test]
async fn non_finite_payloads_keep_the_baseline_plan_in_approximate_mode() {
    type Dims = Vec<(Option<&'static str>, Option<&'static str>, Option<f64>)>;
    /// `(name, fact hosts, fact values, dim rows)` of one non-finite fixture.
    type Case = (
        &'static str,
        Vec<Option<&'static str>>,
        Vec<Option<f64>>,
        Dims,
    );
    let cases: Vec<Case> = vec![
        (
            "nan value",
            vec![Some("a"), Some("a")],
            vec![Some(0.5), Some(f64::NAN)],
            vec![(Some("a"), Some("west"), Some(0.3))],
        ),
        (
            "infinite value",
            vec![Some("a")],
            vec![Some(f64::INFINITY)],
            vec![(Some("a"), Some("west"), Some(0.3))],
        ),
        (
            "nan weight",
            vec![Some("a")],
            vec![Some(0.5)],
            vec![(Some("a"), Some("west"), Some(f64::NAN))],
        ),
        (
            "infinite weight",
            vec![Some("a")],
            vec![Some(0.5)],
            vec![(Some("a"), Some("west"), Some(f64::NEG_INFINITY))],
        ),
    ];

    for (name, hosts, values, dims) in cases {
        let (fact, dim) = weighted_batches(&hosts, &values, &dims);
        assert!(!approx_avg_domain(&fact, &dim), "{name}");
        assert_eq!(
            AvgRewrite::Baseline,
            avg_rewrite_for(&fact, &dim, true),
            "{name}"
        );
        let baseline = engine(false, fact.clone(), dim.clone());
        let candidate = approx_engine(fact, dim);
        let (base_schema, base_rows, _) = run(&baseline, AVG_SQL).await;
        let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, AVG_SQL).await;
        assert_eq!(base_schema, candidate_schema, "{name}");
        assert_eq!(base_rows, candidate_rows, "{name}");
        assert_eq!(
            score_rows(&baseline, AVG_SQL).await,
            score_rows(&candidate, AVG_SQL).await,
            "{name}"
        );
        // The sum shape stays on the baseline too.
        assert_eq!(
            run(&baseline, SUM_SQL).await.1,
            run(&candidate, SUM_SQL).await.1,
            "{name}"
        );
        assert_baseline_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);
        assert_unrewritten_physical_shape(candidate_plan.as_ref());
    }
}

/// Payloads below `2^-1000` sit in the deep subnormal range, where products and
/// sums lose precision wholesale, so the fuse keeps them on the baseline.
#[tokio::test]
async fn subnormal_payloads_keep_the_baseline_plan_in_approximate_mode() {
    // The smallest subnormal and a still-subnormal payload, as a value and as a
    // weight alike.
    for tiny in [5e-324, 1e-310] {
        let (fact, dim) = weighted_batches(
            &[Some("a"), Some("a")],
            &[Some(tiny), Some(0.5)],
            &[(Some("a"), Some("west"), Some(0.3))],
        );
        assert!(
            !approx_avg_domain(&fact, &dim),
            "a subnormal value must keep the baseline: {tiny:e}"
        );
        let (fact, dim) = weighted_batches(
            &[Some("a"), Some("a")],
            &[Some(0.5), Some(1.0)],
            &[(Some("a"), Some("west"), Some(tiny))],
        );
        assert!(
            !approx_avg_domain(&fact, &dim),
            "a subnormal weight must keep the baseline: {tiny:e}"
        );
        assert!(!approx_avg_domain_bound(1, tiny, 1, 1.0));
    }
    // A tiny but normal magnitude, one power of two above the subnormal floor,
    // passes both the fuse and the bound.
    assert!(approx_avg_domain_bound(1, 1e-300, 1, 1.0));

    let (fact, dim) = weighted_batches(
        &[Some("a"), Some("a")],
        &[Some(5e-324), Some(0.5)],
        &[(Some("a"), Some("west"), Some(0.3))],
    );
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = approx_engine(fact, dim);
    let (base_schema, base_rows, _) = run(&baseline, AVG_SQL).await;
    let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, AVG_SQL).await;
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(base_rows, candidate_rows);
    assert_eq!(
        score_rows(&baseline, AVG_SQL).await,
        score_rows(&candidate, AVG_SQL).await
    );
    assert_baseline_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);
    assert_unrewritten_physical_shape(candidate_plan.as_ref());
}

/// With the approximate switch off — a count-only rule, an uncertified exact
/// rule, or a rule whose fuse verdict is `false` — the same general-float query
/// stays on the baseline plan.
#[tokio::test]
async fn approximate_switch_off_keeps_general_float_avg_on_the_baseline() {
    let (fact, dim) = general_float_batches();
    assert!(approx_avg_domain(&fact, &dim));
    assert!(!exact_avg_domain(&fact, &dim));
    let rows = (fact.num_rows() as u64, dim.num_rows() as u64);
    let count_only = CountJoinRewrite::new(rows.0, rows.1);
    let exact_only = CountJoinRewrite::with_avg(rows.0, rows.1, exact_avg_domain(&fact, &dim));
    let fuse_refused = CountJoinRewrite::with_approx_avg(rows.0, rows.1, false);
    for (name, rule) in [
        ("count only", count_only),
        ("exact only", exact_only),
        ("fuse refused", fuse_refused),
    ] {
        assert!(!rule.approx_avg(), "{name}");
        assert_eq!(AvgRewrite::Baseline, rule.avg_rewrite(), "{name}");
        let baseline = engine(false, fact.clone(), dim.clone());
        let candidate = engine_with_rule(Some(rule), fact.clone(), dim.clone());
        let (base_schema, base_rows, _) = run(&baseline, AVG_SQL).await;
        let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, AVG_SQL).await;
        assert_eq!(base_schema, candidate_schema, "{name}");
        assert_eq!(base_rows, candidate_rows, "{name}");
        assert_eq!(
            score_rows(&baseline, AVG_SQL).await,
            score_rows(&candidate, AVG_SQL).await,
            "{name}"
        );
        assert_baseline_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);
        assert_unrewritten_physical_shape(candidate_plan.as_ref());
        // The sum shape stays on the baseline as well.
        assert_eq!(
            run(&baseline, SUM_SQL).await.1,
            run(&candidate, SUM_SQL).await.1,
            "{name}"
        );
        assert_baseline_logical_shape(&optimized_logical(&candidate, SUM_SQL).await);
        // The rule instance still rewrites `COUNT(*)`.
        assert_rewritten_logical_shape(&optimized_logical(&candidate, COUNT_SQL).await);
    }
}

/// A group whose products are all genuinely zero is not a cancellation case: it
/// rewrites and returns `+0.0` for `AVG` and for `SUM`. A group without a single
/// non-null product returns `NULL` from both, exactly like the baseline.
#[tokio::test]
async fn approximate_zero_and_null_groups_match_the_baseline() {
    // `z`: every product is a genuine zero, so the score is `+0.0`, not a
    // cancellation and not `NULL`. `w`: the fact payloads are a signed zero and
    // a zero, the weight is negative, so the branch sums multiply into `-0.0`
    // while the pairwise sum is `+0.0` — the one place the free zero
    // normalization is load-bearing. `n`: no non-null payload at all, so the
    // score is `NULL` like the baseline's empty-pair-set `AVG`/`SUM`.
    let (fact, dim) = weighted_batches(
        &[
            Some("z"),
            Some("z"),
            Some("w"),
            Some("w"),
            Some("n"),
            Some("n"),
        ],
        &[Some(0.0), Some(0.0), Some(-0.0), Some(0.0), None, None],
        &[
            (Some("z"), Some("west"), Some(0.3)),
            (Some("w"), Some("west"), Some(-1.5)),
            (Some("n"), Some("west"), Some(0.3)),
        ],
    );
    assert!(
        approx_avg_domain(&fact, &dim),
        "all-zero payloads are not a cancellation"
    );
    assert!(!exact_avg_domain(&fact, &dim));
    let baseline = engine(false, fact.clone(), dim.clone());
    let candidate = approx_engine(fact, dim);

    // The `(host, area, score bits)` rows in the emitted order, with the
    // expected `host` order of the two `0.0` groups spelled out.
    let expected_rows = vec!["w|west|0.0", "z|west|0.0", "n|west|NULL"];
    let bits_of = |rows: &[(String, String, Option<u64>)], host: &str| {
        rows.iter()
            .find(|(row_host, _, _)| row_host == host)
            .unwrap_or_else(|| panic!("no row for {host} in {rows:?}"))
            .2
    };

    let (base_schema, base_rows, _) = run(&baseline, AVG_SQL).await;
    let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, AVG_SQL).await;
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(base_rows, candidate_rows);
    assert_eq!(
        expected_rows, candidate_rows,
        "an empty pair set averages to NULL in the baseline too"
    );
    let avg_scores = score_rows(&candidate, AVG_SQL).await;
    assert_eq!(
        Some(0.0f64.to_bits()),
        bits_of(&avg_scores, "z"),
        "the all-zero group is +0.0, not NULL"
    );
    assert_eq!(
        Some(0.0f64.to_bits()),
        bits_of(&avg_scores, "w"),
        "the signed-zero group is normalized to +0.0, not -0.0"
    );
    assert_eq!(None, bits_of(&avg_scores, "n"), "the empty group is NULL");
    assert_eq!(score_rows(&baseline, AVG_SQL).await, avg_scores);
    assert_rewritten_avg_logical_shape(&optimized_logical(&candidate, AVG_SQL).await);
    assert_rewritten_avg_physical_shape(candidate_plan.as_ref());

    let (base_schema, base_rows, _) = run(&baseline, SUM_SQL).await;
    let (candidate_schema, candidate_rows, candidate_plan) = run(&candidate, SUM_SQL).await;
    assert_eq!(base_schema, candidate_schema);
    assert_eq!(base_rows, candidate_rows);
    assert_eq!(
        expected_rows, candidate_rows,
        "SUM over no non-null product is NULL in the baseline too"
    );
    let sum_scores = score_rows(&candidate, SUM_SQL).await;
    assert_eq!(Some(0.0f64.to_bits()), bits_of(&sum_scores, "z"));
    assert_eq!(
        Some(0.0f64.to_bits()),
        bits_of(&sum_scores, "w"),
        "the sum is normalized to +0.0, not -0.0"
    );
    assert_eq!(None, bits_of(&sum_scores, "n"), "the empty group is NULL");
    assert_eq!(score_rows(&baseline, SUM_SQL).await, sum_scores);
    assert_rewritten_sum_logical_shape(&optimized_logical(&candidate, SUM_SQL).await);
    assert_rewritten_avg_physical_shape(candidate_plan.as_ref());
}
