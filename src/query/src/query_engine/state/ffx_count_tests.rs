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

use arrow::util::display::array_value_to_string;
use catalog::RegisterTableRequest;
use common_catalog::consts::{DEFAULT_CATALOG_NAME, DEFAULT_SCHEMA_NAME};
use common_query::OutputData;
use common_recordbatch::{RecordBatch, util};
use datafusion::functions_aggregate::count::Count;
use datafusion::physical_expr::expressions::{BinaryExpr, Column as PhysicalColumn};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::AggregateExec;
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
use datafusion_expr::{LogicalPlan, Operator};
use datatypes::prelude::*;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{Float64Vector, StringVector, TimestampMillisecondVector};
use session::context::QueryContext;
use table::test_util::MemTable;

use crate::optimizer::count_join::CountJoinRewrite;
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
