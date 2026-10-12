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

//! Automatic `COUNT(*)` selection tests for [`count_session`].
//!
//! Every positive case plans the SQL text through a session built by
//! `count_session` (never through the factorization constructors), executes the
//! resulting plan, and compares the complete result and schema with a plain
//! baseline session over the same batches. Negative shapes must come back with the
//! baseline topology, i.e. the wrapper leaves the delegated plan untouched.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use paper_ffx_datafusion::{
    checked_pair_bound, count_config, count_session, payload_rows_fit, select_factor_plan,
};

type CountRow = (Option<i64>, Option<i64>, i64);

const FULL_QUERY: &str = "SELECT f.host, d.area, COUNT(*) AS pairs FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area";
const ORDERED_QUERY: &str = "SELECT f.host, d.area, COUNT(*) AS pairs FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area ORDER BY pairs DESC NULLS LAST, f.host ASC NULLS LAST, d.area ASC NULLS LAST LIMIT 5";

fn facts_batch(hosts: Vec<Option<i64>>, values: Vec<Option<f64>>) -> RecordBatch {
    assert_eq!(hosts.len(), values.len());
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("host", DataType::Int64, true),
            Field::new("value", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(hosts)) as ArrayRef,
            Arc::new(Float64Array::from(values)),
        ],
    )
    .unwrap()
}

fn dims_batch(
    hosts: Vec<Option<i64>>,
    areas: Vec<Option<i64>>,
    weights: Vec<Option<f64>>,
) -> RecordBatch {
    assert_eq!(hosts.len(), areas.len());
    assert_eq!(hosts.len(), weights.len());
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("host", DataType::Int64, true),
            Field::new("area", DataType::Int64, true),
            Field::new("weight", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(hosts)) as ArrayRef,
            Arc::new(Int64Array::from(areas)) as ArrayRef,
            Arc::new(Float64Array::from(weights)),
        ],
    )
    .unwrap()
}

/// Balanced fixture: 16 hosts with 16 facts and 4 dimensions each (1024 pairs).
fn balanced() -> (RecordBatch, RecordBatch) {
    let facts = facts_batch(
        (0..16)
            .flat_map(|host| std::iter::repeat_n(Some(host), 16))
            .collect(),
        (0..256).map(|i| Some((i % 8) as f64 + 0.5)).collect(),
    );
    let dims = dims_batch(
        (0..16)
            .flat_map(|host| std::iter::repeat_n(Some(host), 4))
            .collect(),
        (0..64).map(|i| Some((i % 4) as i64)).collect(),
        (0..64).map(|i| Some(((i % 4) + 1) as f64 / 4.0)).collect(),
    );
    (facts, dims)
}

/// Fixture with null hosts, null values, null areas, and duplicates.
fn nullable_fixture() -> (RecordBatch, RecordBatch) {
    let facts = facts_batch(
        vec![Some(1), Some(1), Some(2), None, Some(3), Some(3)],
        vec![Some(1.0), None, Some(3.0), Some(9.0), None, Some(2.0)],
    );
    let dims = dims_batch(
        vec![Some(1), Some(1), Some(3), None, Some(3)],
        vec![None, Some(7), Some(7), Some(1), Some(7)],
        vec![Some(0.5), Some(1.0), Some(1.0), Some(1.0), None],
    );
    (facts, dims)
}

async fn baseline_context(facts: RecordBatch, dims: RecordBatch) -> SessionContext {
    let ctx = SessionContext::new_with_config(count_config());
    ctx.register_table(
        "facts",
        Arc::new(MemTable::try_new(facts.schema(), vec![vec![facts]]).unwrap()),
    )
    .unwrap();
    ctx.register_table(
        "dims",
        Arc::new(MemTable::try_new(dims.schema(), vec![vec![dims]]).unwrap()),
    )
    .unwrap();
    ctx
}

async fn plan_and_collect(
    ctx: &SessionContext,
    sql: &str,
) -> (Arc<dyn ExecutionPlan>, Vec<RecordBatch>) {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await.unwrap();
    (plan, batches)
}

fn plan_text(plan: &Arc<dyn ExecutionPlan>) -> String {
    format!("{}", displayable(plan.as_ref()).indent(true))
}

fn is_factorized(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let text = plan_text(plan);
    text.contains("RawFactorExec") && text.contains("FactorAggregateExec")
}

fn find_exec(plan: &Arc<dyn ExecutionPlan>, name: &str) -> Option<Arc<dyn ExecutionPlan>> {
    if plan.name() == name {
        return Some(Arc::clone(plan));
    }
    for child in plan.children() {
        if let Some(found) = find_exec(child, name) {
            return Some(found);
        }
    }
    None
}

/// Rows in plan output order (single partition makes that order deterministic).
fn counts_in_plan_order(batches: &[RecordBatch]) -> Vec<CountRow> {
    let mut rows = Vec::new();
    for batch in batches {
        let host = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let area = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let pairs = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                (!host.is_null(i)).then_some(host.value(i)),
                (!area.is_null(i)).then_some(area.value(i)),
                pairs.value(i),
            ));
        }
    }
    rows
}

/// Rows in a canonical order so complete results compare independent of layout.
fn counts(batches: &[RecordBatch]) -> Vec<CountRow> {
    let mut rows = counts_in_plan_order(batches);
    rows.sort();
    rows
}

fn string_rows(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    for batch in batches {
        for i in 0..batch.num_rows() {
            let row = (0..batch.num_columns())
                .map(|c| array_value_to_string(batch.column(c), i).unwrap())
                .collect::<Vec<_>>();
            rows.push(row);
        }
    }
    rows.sort();
    rows
}

/// Complete nested-loop oracle for `GROUP BY host, area COUNT(*)`: null hosts on
/// either side never match, and every matching (fact, dimension) row pair counts.
fn oracle(facts: &RecordBatch, dims: &RecordBatch) -> Vec<CountRow> {
    let fact_host = facts
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let dim_host = dims
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let dim_area = dims
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut groups = BTreeMap::<(Option<i64>, Option<i64>), i64>::new();
    for i in 0..facts.num_rows() {
        if fact_host.is_null(i) {
            continue;
        }
        for j in 0..dims.num_rows() {
            if dim_host.is_null(j) || fact_host.value(i) != dim_host.value(j) {
                continue;
            }
            let area = (!dim_area.is_null(j)).then_some(dim_area.value(j));
            *groups.entry((Some(fact_host.value(i)), area)).or_default() += 1;
        }
    }
    groups
        .into_iter()
        .map(|((host, area), pairs)| (host, area, pairs))
        .collect()
}

fn nulls_last(left: Option<i64>, right: Option<i64>) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn total_order(rows: &mut [CountRow]) {
    rows.sort_by(|left, right| {
        right
            .2
            .cmp(&left.2)
            .then_with(|| nulls_last(left.0, right.0))
            .then_with(|| nulls_last(left.1, right.1))
    });
}

#[tokio::test]
async fn count_sql_is_automatically_factorized_with_complete_results() {
    let (facts, dims) = balanced();
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (plan, batches) = plan_and_collect(&ctx, FULL_QUERY).await;
    assert!(is_factorized(&plan), "{}", plan_text(&plan));

    let expected = oracle(&facts, &dims);
    assert_eq!(counts(&batches), expected);
    assert_eq!(expected.len(), 64);

    // Control: the unmodified default planner picks the ordinary join/aggregate.
    let baseline = baseline_context(facts, dims).await;
    let (baseline_plan, baseline_batches) = plan_and_collect(&baseline, FULL_QUERY).await;
    assert!(!plan_text(&baseline_plan).contains("RawFactorExec"));
    assert!(plan_text(&baseline_plan).contains("HashJoinExec"));
    assert_eq!(counts(&baseline_batches), expected);
}

#[tokio::test]
async fn duplicate_rows_multiply_into_pair_counts() {
    // Three fact rows and two dimension rows for (host 1, area 7) must count six
    // pairs, while the other groups keep their own multiplicities.
    let facts = facts_batch(
        vec![Some(1), Some(1), Some(1), Some(2)],
        vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)],
    );
    let dims = dims_batch(
        vec![Some(1), Some(1), Some(1), Some(2), Some(2)],
        vec![Some(7), Some(7), Some(8), Some(7), Some(7)],
        vec![Some(1.0), Some(1.0), Some(1.0), Some(1.0), Some(1.0)],
    );
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (plan, batches) = plan_and_collect(&ctx, FULL_QUERY).await;
    assert!(is_factorized(&plan), "{}", plan_text(&plan));
    assert_eq!(
        counts(&batches),
        vec![
            (Some(1), Some(7), 6),
            (Some(1), Some(8), 3),
            (Some(2), Some(7), 2),
        ]
    );
    assert_eq!(counts(&batches), oracle(&facts, &dims));
}

#[tokio::test]
async fn null_hosts_never_match_while_null_values_and_areas_still_count() {
    let (facts, dims) = nullable_fixture();
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (plan, batches) = plan_and_collect(&ctx, FULL_QUERY).await;
    assert!(is_factorized(&plan), "{}", plan_text(&plan));

    let expected = oracle(&facts, &dims);
    // host 1 pairs with area NULL and area 7 (null fact value still counts);
    // host 3 has two facts and two area-7 dimensions, so four pairs; host 2 has
    // no dimensions.
    assert_eq!(
        expected,
        vec![
            (Some(1), None, 2),
            (Some(1), Some(7), 2),
            (Some(3), Some(7), 4),
        ]
    );
    assert_eq!(counts(&batches), expected);
    // Null hosts on either side never produce a group.
    assert!(counts(&batches).iter().all(|row| row.0.is_some()));

    let baseline = baseline_context(facts, dims).await;
    let (_, baseline_batches) = plan_and_collect(&baseline, FULL_QUERY).await;
    assert_eq!(counts(&baseline_batches), expected);
}

#[tokio::test]
async fn empty_inputs_produce_no_groups_and_keep_the_baseline_schema() {
    let (facts, dims) = balanced();
    let empty_facts = RecordBatch::new_empty(facts.schema());
    let empty_dims = RecordBatch::new_empty(dims.schema());
    for (facts, dims) in [
        (empty_facts.clone(), dims.clone()),
        (facts.clone(), empty_dims.clone()),
        (empty_facts, empty_dims),
    ] {
        let ctx = count_session(facts.clone(), dims.clone()).unwrap();
        let (plan, batches) = plan_and_collect(&ctx, FULL_QUERY).await;
        assert!(batches.iter().all(|batch| batch.num_rows() == 0));

        let baseline = baseline_context(facts, dims).await;
        let (baseline_plan, baseline_batches) = plan_and_collect(&baseline, FULL_QUERY).await;
        assert!(baseline_batches.iter().all(|batch| batch.num_rows() == 0));
        assert_eq!(plan.schema(), baseline_plan.schema());
    }
}

#[tokio::test]
async fn factorized_schema_matches_baseline_names_types_and_nullability() {
    let (facts, dims) = nullable_fixture();
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (plan, _) = plan_and_collect(&ctx, FULL_QUERY).await;
    let baseline = baseline_context(facts, dims).await;
    let (baseline_plan, _) = plan_and_collect(&baseline, FULL_QUERY).await;

    assert_eq!(plan.schema().fields().len(), 3);
    assert_eq!(plan.schema().field(0).name(), "host");
    assert_eq!(plan.schema().field(1).name(), "area");
    assert_eq!(plan.schema().field(2).name(), "pairs");
    assert_eq!(plan.schema().field(2).data_type(), &DataType::Int64);
    assert!(!plan.schema().field(2).is_nullable());
    assert_eq!(plan.schema(), baseline_plan.schema());
    for (actual, expected) in plan
        .schema()
        .fields()
        .iter()
        .zip(baseline_plan.schema().fields())
    {
        assert_eq!(actual.name(), expected.name());
        assert_eq!(actual.data_type(), expected.data_type());
        assert_eq!(actual.is_nullable(), expected.is_nullable());
    }
}

#[tokio::test]
async fn negative_shapes_fall_back_to_an_unchanged_baseline_plan() {
    let (facts, dims) = nullable_fixture();
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let baseline = baseline_context(facts, dims).await;
    let queries = [
        // COUNT over a nullable expression instead of COUNT(*).
        "SELECT f.host, d.area, COUNT(f.value) AS pairs FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area",
        // COUNT(DISTINCT ...) is not a plain pair count.
        "SELECT f.host, d.area, COUNT(DISTINCT f.host) AS pairs FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area",
        // Extra aggregates need the joined rows.
        "SELECT f.host, d.area, COUNT(*) AS pairs, SUM(f.value) AS total FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area",
        // Outer joins keep unmatched rows.
        "SELECT f.host, d.area, COUNT(*) AS pairs FROM facts f LEFT JOIN dims d ON f.host = d.host GROUP BY f.host, d.area",
        // Extra join predicate (non-equi condition).
        "SELECT f.host, d.area, COUNT(*) AS pairs FROM facts f JOIN dims d ON f.host = d.host AND f.value > d.weight GROUP BY f.host, d.area",
        // Second equi-join pair.
        "SELECT f.host, d.area, COUNT(*) AS pairs FROM facts f JOIN dims d ON f.host = d.host AND f.value = d.weight GROUP BY f.host, d.area",
        // WHERE filter above the join.
        "SELECT f.host, d.area, COUNT(*) AS pairs FROM facts f JOIN dims d ON f.host = d.host WHERE f.value > 0.0 GROUP BY f.host, d.area",
        // Altered grouping: single key.
        "SELECT f.host, COUNT(*) AS pairs FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host",
        // Altered grouping: keys swapped.
        "SELECT d.area, f.host, COUNT(*) AS pairs FROM facts f JOIN dims d ON f.host = d.host GROUP BY d.area, f.host",
        // FILTER clause on the aggregate.
        "SELECT f.host, d.area, COUNT(*) FILTER (WHERE f.value > 0.0) AS pairs FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area",
    ];
    for query in queries {
        let (plan, batches) = plan_and_collect(&ctx, query).await;
        assert!(
            !is_factorized(&plan),
            "unexpected selection for {query}:\n{}",
            plan_text(&plan)
        );
        let (baseline_plan, baseline_batches) = plan_and_collect(&baseline, query).await;
        assert_eq!(
            plan_text(&plan),
            plan_text(&baseline_plan),
            "topology changed for {query}"
        );
        assert_eq!(
            string_rows(&batches),
            string_rows(&baseline_batches),
            "results changed for {query}"
        );
    }
}

#[tokio::test]
async fn replaced_provider_falls_back_without_changing_results() {
    let (facts, dims) = nullable_fixture();
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    // Re-registering the same data through a different provider must not match the
    // stored handles.
    ctx.deregister_table("facts").unwrap();
    ctx.register_table(
        "facts",
        Arc::new(MemTable::try_new(facts.schema(), vec![vec![facts.clone()]]).unwrap()),
    )
    .unwrap();
    let (plan, batches) = plan_and_collect(&ctx, FULL_QUERY).await;
    assert!(!is_factorized(&plan), "{}", plan_text(&plan));

    let baseline = baseline_context(facts.clone(), dims.clone()).await;
    let (baseline_plan, baseline_batches) = plan_and_collect(&baseline, FULL_QUERY).await;
    assert_eq!(plan_text(&plan), plan_text(&baseline_plan));
    assert_eq!(counts(&batches), counts(&baseline_batches));
    assert_eq!(counts(&batches), oracle(&facts, &dims));
}

#[tokio::test]
async fn non_finite_payload_keeps_the_default_plan_and_an_exact_count() {
    // Reuses the historical 1e308/1e-308 averaging counterexample rows and adds
    // NaN/+Inf/-Inf payloads: setup must skip the selection, never error, and the
    // ordinary planner's COUNT(*) must stay exact.
    let facts = facts_batch(
        vec![Some(1), Some(1), Some(1), Some(2), Some(2)],
        vec![
            Some(1e308),
            Some(1e308),
            Some(f64::NAN),
            Some(1.0),
            Some(f64::NAN),
        ],
    );
    let dims = dims_batch(
        vec![Some(1), Some(1), Some(2)],
        vec![Some(7), Some(7), Some(7)],
        vec![Some(1e-308), Some(f64::INFINITY), Some(f64::NEG_INFINITY)],
    );
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (plan, batches) = plan_and_collect(&ctx, FULL_QUERY).await;
    assert!(!is_factorized(&plan), "{}", plan_text(&plan));
    let expected = vec![(Some(1), Some(7), 6), (Some(2), Some(7), 2)];
    assert_eq!(expected, oracle(&facts, &dims));
    assert_eq!(counts(&batches), expected);
    assert!(
        plan_text(&plan).contains("HashJoinExec"),
        "{}",
        plan_text(&plan)
    );

    let baseline = baseline_context(facts, dims).await;
    let (_, baseline_batches) = plan_and_collect(&baseline, FULL_QUERY).await;
    assert_eq!(counts(&baseline_batches), expected);
}

#[tokio::test]
async fn finite_extreme_values_still_count_exactly() {
    // The historical 1e308/1e-308 counterexample for the averaging path: the
    // values are finite, so the count selection is allowed, and counting never
    // multiplies the payloads.
    let facts = facts_batch(vec![Some(1), Some(1)], vec![Some(1e308), Some(1e308)]);
    let dims = dims_batch(vec![Some(1)], vec![Some(1)], vec![Some(1e-308)]);
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (plan, batches) = plan_and_collect(&ctx, FULL_QUERY).await;
    assert!(is_factorized(&plan), "{}", plan_text(&plan));
    assert_eq!(counts(&batches), vec![(Some(1), Some(1), 2)]);
    assert_eq!(counts(&batches), oracle(&facts, &dims));
}

#[tokio::test]
async fn setup_bounds_are_derived_from_arithmetic_not_allocations() {
    assert_eq!(checked_pair_bound(2, 3).unwrap(), 6);
    assert_eq!(checked_pair_bound(i64::MAX as usize, 1).unwrap(), i64::MAX);
    assert!(checked_pair_bound(i64::MAX as usize, 2).is_err());
    assert!(checked_pair_bound(usize::MAX, 1).is_err());
    assert!(select_factor_plan(2, 3, true).unwrap());
    assert!(!select_factor_plan(2, 3, false).unwrap());
    assert!(payload_rows_fit(i32::MAX as usize, 0));
    assert!(payload_rows_fit(0, i32::MAX as usize));
    // Oversized totals stay on the default plan instead of refusing setup.
    assert!(!select_factor_plan(i32::MAX as usize, 1, true).unwrap());
    assert!(select_factor_plan(usize::MAX, 1, true).is_err());
    assert!(select_factor_plan(usize::MAX, 2, true).is_err());
}

#[tokio::test]
async fn ordered_top_n_matches_the_full_result_prefix() {
    // Every group has the same count, so the host/area keys decide the order; the
    // NULL area group must sort last.
    let facts = facts_batch(
        (0..6)
            .flat_map(|host| std::iter::repeat_n(Some(host), 2))
            .collect(),
        (0..12).map(|i| Some(i as f64)).collect(),
    );
    let dims = dims_batch(
        vec![
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(2),
            Some(2),
            Some(3),
            Some(3),
            Some(4),
            Some(4),
            Some(5),
            Some(5),
            Some(0),
        ],
        vec![
            Some(0),
            Some(1),
            Some(0),
            Some(1),
            Some(0),
            Some(1),
            Some(0),
            Some(1),
            Some(0),
            Some(1),
            Some(0),
            Some(1),
            None,
        ],
        vec![Some(1.0); 13],
    );
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (plan, batches) = plan_and_collect(&ctx, ORDERED_QUERY).await;
    assert!(is_factorized(&plan), "{}", plan_text(&plan));

    let mut ordered = oracle(&facts, &dims);
    total_order(&mut ordered);
    let expected = ordered.iter().copied().take(5).collect::<Vec<_>>();
    // The plan emitted exactly this order: equal counts fall back to host then
    // area, and the NULL-area group of host 0 sorts last within its host.
    assert_eq!(counts_in_plan_order(&batches), expected);
    assert_eq!(
        expected,
        vec![
            (Some(0), Some(0), 2),
            (Some(0), Some(1), 2),
            (Some(0), None, 2),
            (Some(1), Some(0), 2),
            (Some(1), Some(1), 2),
        ]
    );

    let baseline = baseline_context(facts, dims).await;
    let (_, baseline_batches) = plan_and_collect(&baseline, ORDERED_QUERY).await;
    assert_eq!(
        counts_in_plan_order(&batches),
        counts_in_plan_order(&baseline_batches)
    );
}

#[tokio::test]
async fn factor_payload_grows_with_input_rows_not_pair_count() {
    let (facts, dims) = balanced();
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (plan, batches) = plan_and_collect(&ctx, FULL_QUERY).await;
    assert!(is_factorized(&plan), "{}", plan_text(&plan));

    let pairs = counts(&batches).iter().map(|row| row.2).sum::<i64>();
    assert_eq!(pairs, 1024);

    let raw = find_exec(&plan, "RawFactorExec").expect("factor producer in plan");
    let factor_batches = collect(raw, ctx.task_ctx()).await.unwrap();
    let mut payload_hosts = Vec::new();
    let mut max_values = 0;
    let mut value_elements = 0;
    for batch in &factor_batches {
        let hosts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        for i in 0..batch.num_rows() {
            payload_hosts.push(hosts.value(i));
            let length = values.value(i).len();
            max_values = max_values.max(length);
            value_elements += length;
        }
    }
    payload_hosts.sort_unstable();
    payload_hosts.dedup();
    assert_eq!(payload_hosts.len(), 16);
    assert_eq!(
        payload_hosts.len(),
        factor_batches
            .iter()
            .map(RecordBatch::num_rows)
            .sum::<usize>()
    );
    // 16 hosts, 16 facts and 4 dimensions each: payload lengths follow the input
    // rows (16 / 4), not the 1024 joined pairs.
    assert_eq!(max_values, 16);
    assert_eq!(value_elements, 256);
    assert!(payload_hosts.len() < pairs as usize);
    assert!(value_elements < pairs as usize);
}
