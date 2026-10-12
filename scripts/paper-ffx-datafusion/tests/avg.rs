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

//! Exact-domain `AVG(f.value * d.weight)` selection tests.
//!
//! Positive cases plan the SQL through a session built by `count_session`,
//! execute the plan, and compare the complete result *bitwise* with a plain
//! baseline session over the same batches. Cases outside the exact-equivalence
//! domain must keep the delegated baseline plan: the `AVG` shape never errors and
//! never returns an approximate score, while `COUNT(*)` admission is unchanged.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::arrow::util::display::array_value_to_string;
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use paper_ffx_datafusion::{count_config, count_session, count_session_full};

type AvgRow = (Option<i64>, Option<i64>, Option<f64>);

const FULL_AVG: &str = "SELECT f.host, d.area, AVG(f.value * d.weight) AS avg FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area";
const ORDERED_AVG: &str = "SELECT f.host, d.area, AVG(f.value * d.weight) AS avg FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area ORDER BY avg DESC NULLS LAST, f.host ASC NULLS LAST, d.area ASC NULLS LAST LIMIT 4";
const FULL_COUNT: &str = "SELECT f.host, d.area, COUNT(*) AS pairs FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area";

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

/// Exact-domain fixture: integer fact values, quarter weights, duplicate fact
/// rows, duplicate dimension rows, several hosts and areas, a negative fact, a
/// negative quarter weight, the magnitude bounds themselves, and a host whose
/// dimensions are all in one area.
fn exact_domain() -> (RecordBatch, RecordBatch) {
    let facts = facts_batch(
        vec![
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(1),
            Some(2),
        ],
        vec![
            Some(2.0),
            Some(5.0),
            Some(7.0),
            Some(11.0),
            Some(2.0),
            Some(-3.0),
            Some(8.0),
            Some(8.0),
            Some(1_000_000.0),
        ],
    );
    let dims = dims_batch(
        vec![
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(2),
            Some(2),
        ],
        vec![
            Some(0),
            Some(0),
            Some(1),
            Some(1),
            Some(1),
            Some(2),
            Some(2),
            Some(0),
            Some(0),
        ],
        vec![
            Some(0.25),
            Some(0.25),
            Some(1.0),
            Some(2.5),
            Some(4.0),
            Some(-0.25),
            Some(1.0),
            Some(0.5),
            Some(4.0),
        ],
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

/// True when the plan contains the ordinary (non-factorized) aggregate operator.
/// `FactorAggregateExec` contains the same substring, so match the operator line.
fn has_plain_aggregate(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan_text(plan)
        .lines()
        .any(|line| line.trim_start().starts_with("AggregateExec:"))
}

/// True when the plan keeps the ordinary join/aggregate topology.
fn is_baseline(plan: &Arc<dyn ExecutionPlan>) -> bool {
    let text = plan_text(plan);
    !is_factorized(plan) && text.contains("HashJoinExec") && has_plain_aggregate(plan)
}

/// Rows in plan output order (single partition makes that order deterministic).
fn avg_rows_in_plan_order(batches: &[RecordBatch]) -> Vec<AvgRow> {
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
        let avg = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            rows.push((
                (!host.is_null(i)).then_some(host.value(i)),
                (!area.is_null(i)).then_some(area.value(i)),
                (!avg.is_null(i)).then_some(avg.value(i)),
            ));
        }
    }
    rows
}

/// Rows in a canonical order so complete results compare independent of layout.
fn avg_rows(batches: &[RecordBatch]) -> Vec<AvgRow> {
    let mut rows = avg_rows_in_plan_order(batches);
    rows.sort_by(|left, right| {
        left.0
            .cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
            .then_with(|| {
                left.2
                    .unwrap_or(f64::NAN)
                    .total_cmp(&right.2.unwrap_or(f64::NAN))
            })
    });
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

/// Asserts that two complete grouped results hold the same rows with `to_bits`-equal
/// scores.
///
/// Scores compare with `to_bits`, so `+0.0` and `-0.0` are distinguished and no
/// rounding difference can hide behind `==`. Both sides are in the canonical
/// `(host, area)` order produced by [`avg_rows`], which fixes the row alignment.
fn assert_scores_bitwise(left: &[AvgRow], right: &[AvgRow], context: &str) {
    assert_eq!(
        left.len(),
        right.len(),
        "{context}: group count differs: {left:?} vs {right:?}"
    );
    for (left, right) in left.iter().zip(right) {
        assert_eq!(left.0, right.0, "{context}: host differs");
        assert_eq!(left.1, right.1, "{context}: grouping differs");
        let key = (left.0, left.1);
        match (left.2, right.2) {
            (Some(left), Some(right)) => assert_eq!(
                left.to_bits(),
                right.to_bits(),
                "{context}: score bits differ for {key:?}"
            ),
            (None, None) => {}
            (left, right) => panic!("{context}: NULL mismatch for {key:?}: {left:?} vs {right:?}"),
        }
    }
}

/// Exact rational oracle for the mean of the joined products.
///
/// Payload sums are accumulated independently per group in quarters (integers),
/// so the expected value is one correctly-rounded division of the exact rational
/// mean: `(4 * sum_v) * (4 * sum_w) / (16 * valid)`. It never replays the factorized
/// formula pairwise. NULL rows are skipped exactly as the baseline `AVG` skips NULL
/// products and `valid` counts the joined pairs with a non-null product, so fixtures
/// that mix NULL and non-null payloads are handled. Only admitted exact-domain
/// fixtures are fed to it: there every group keeps at least one valid entry, whereas
/// a group without one would score NULL and is omitted here. An exactly zero score
/// is `+0.0`, matching both plans, because the integer numerator is zero and the
/// divisor is positive.
fn oracle_avg(facts: &RecordBatch, dims: &RecordBatch) -> Vec<AvgRow> {
    let fact_host = facts
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let fact_value = facts
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
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
    let dim_weight = dims
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let mut fact_groups = BTreeMap::<i64, (i128, i128)>::new();
    for i in 0..facts.num_rows() {
        if fact_host.is_null(i) || fact_value.is_null(i) {
            continue;
        }
        let entry = fact_groups.entry(fact_host.value(i)).or_default();
        entry.0 += (fact_value.value(i) * 4.0) as i128;
        entry.1 += 1;
    }
    let mut groups = BTreeMap::<(i64, Option<i64>), (i128, i128)>::new();
    for j in 0..dims.num_rows() {
        if dim_host.is_null(j) || dim_weight.is_null(j) {
            continue;
        }
        let area = (!dim_area.is_null(j)).then_some(dim_area.value(j));
        let entry = groups.entry((dim_host.value(j), area)).or_default();
        entry.0 += (dim_weight.value(j) * 4.0) as i128;
        entry.1 += 1;
    }
    groups
        .into_iter()
        .filter_map(|((host, area), (weight_quarters, dim_rows))| {
            let (fact_quarters, fact_rows) = *fact_groups.get(&host)?;
            let numerator = fact_quarters * weight_quarters;
            Some((
                Some(host),
                area,
                Some(numerator as f64 / (fact_rows * dim_rows * 16) as f64),
            ))
        })
        .collect()
}

#[tokio::test]
async fn exact_domain_avg_is_factorized_and_bitwise_equal() {
    let (facts, dims) = exact_domain();
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (plan, batches) = plan_and_collect(&ctx, FULL_AVG).await;
    assert!(is_factorized(&plan), "{}", plan_text(&plan));
    // The spliced plan replaces the ordinary join/aggregate subtree.
    assert!(
        !plan_text(&plan).contains("HashJoinExec"),
        "{}",
        plan_text(&plan)
    );
    assert!(!has_plain_aggregate(&plan), "{}", plan_text(&plan));

    let baseline = baseline_context(facts.clone(), dims.clone()).await;
    let (baseline_plan, baseline_batches) = plan_and_collect(&baseline, FULL_AVG).await;
    assert!(is_baseline(&baseline_plan), "{}", plan_text(&baseline_plan));

    // Schema parity, including nullability, so ancestors can keep referencing the
    // aggregate positionally.
    assert_eq!(plan.schema(), baseline_plan.schema());
    assert_eq!(plan.schema().field(0).name(), "host");
    assert_eq!(plan.schema().field(1).name(), "area");
    assert_eq!(plan.schema().field(2).name(), "avg");
    assert_eq!(plan.schema().field(2).data_type(), &DataType::Float64);

    // Complete result equality, bitwise on every score: the factorized plan against
    // the authoritative baseline, and both against the exact rational oracle.
    let factorized = avg_rows(&batches);
    let expected = avg_rows(&baseline_batches);
    assert_scores_bitwise(&factorized, &expected, "baseline");
    assert_scores_bitwise(&factorized, &oracle_avg(&facts, &dims), "oracle");
    // The fixture's exact rational scores, computed by hand:
    //   host 0/area 0: (2+5+7+11+2)*0.5/10 = 1.35
    //   host 0/area 1: 27*7.5/15 = 13.5
    //   host 1/area 2: 13*0.75/6 = 1.625
    //   host 2/area 0: 1_000_000*4.5/2 = 2_250_000
    let expected_rows = vec![
        (Some(0), Some(0), Some(1.35)),
        (Some(0), Some(1), Some(13.5)),
        (Some(1), Some(2), Some(1.625)),
        (Some(2), Some(0), Some(2_250_000.0)),
    ];
    assert_scores_bitwise(&factorized, &expected_rows, "hand-computed scores");
}

#[tokio::test]
async fn ordered_avg_matches_the_baseline_order_and_prefix() {
    let (facts, dims) = exact_domain();
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (plan, batches) = plan_and_collect(&ctx, ORDERED_AVG).await;
    assert!(is_factorized(&plan), "{}", plan_text(&plan));
    assert!(
        !plan_text(&plan).contains("HashJoinExec"),
        "{}",
        plan_text(&plan)
    );
    assert!(!has_plain_aggregate(&plan), "{}", plan_text(&plan));

    let baseline = baseline_context(facts, dims).await;
    let (baseline_plan, baseline_batches) = plan_and_collect(&baseline, ORDERED_AVG).await;
    assert!(is_baseline(&baseline_plan), "{}", plan_text(&baseline_plan));
    // Sort and limit stay above the spliced factor plan; equal scores fall back
    // to host then area, so the emitted order is exactly the baseline's.
    assert_eq!(
        avg_rows_in_plan_order(&batches),
        avg_rows_in_plan_order(&baseline_batches)
    );
    assert_eq!(
        avg_rows_in_plan_order(&batches),
        vec![
            (Some(2), Some(0), Some(2_250_000.0)),
            (Some(0), Some(1), Some(13.5)),
            (Some(1), Some(2), Some(1.625)),
            (Some(0), Some(0), Some(1.35)),
        ]
    );
}

#[tokio::test]
async fn avg_selection_can_be_disabled_without_changing_count() {
    let (facts, dims) = exact_domain();
    // The exact-domain batches are admissible, but the caller disables the AVG
    // shape: the count-only session must keep COUNT(*) factorized and leave AVG
    // on the baseline plan.
    let ctx = count_session_full(facts.clone(), dims.clone(), false).unwrap();
    let (count_plan, count_batches) = plan_and_collect(&ctx, FULL_COUNT).await;
    assert!(is_factorized(&count_plan), "{}", plan_text(&count_plan));
    let (avg_plan, avg_batches) = plan_and_collect(&ctx, FULL_AVG).await;
    assert!(!is_factorized(&avg_plan), "{}", plan_text(&avg_plan));

    let baseline = baseline_context(facts, dims).await;
    let (baseline_count, baseline_count_batches) = plan_and_collect(&baseline, FULL_COUNT).await;
    assert_eq!(
        string_rows(&count_batches),
        string_rows(&baseline_count_batches)
    );
    let (baseline_avg, baseline_avg_batches) = plan_and_collect(&baseline, FULL_AVG).await;
    assert_eq!(plan_text(&avg_plan), plan_text(&baseline_avg));
    assert_eq!(avg_rows(&avg_batches), avg_rows(&baseline_avg_batches));
    assert!(is_baseline(&baseline_avg), "{}", plan_text(&baseline_avg));
    assert!(
        is_baseline(&baseline_count),
        "{}",
        plan_text(&baseline_count)
    );
}

#[tokio::test]
async fn non_exact_domain_batches_keep_baseline_avg_and_factorized_count() {
    let cases = [
        (
            "non-integer fact value",
            facts_batch(vec![Some(1), Some(1)], vec![Some(1.5), Some(2.0)]),
            dims_batch(vec![Some(1)], vec![Some(0)], vec![Some(0.5)]),
        ),
        (
            "non-quarter weight",
            facts_batch(vec![Some(1), Some(1)], vec![Some(2.0), Some(3.0)]),
            dims_batch(vec![Some(1)], vec![Some(0)], vec![Some(0.3)]),
        ),
        (
            "weight above the magnitude bound",
            facts_batch(vec![Some(1), Some(1)], vec![Some(2.0), Some(3.0)]),
            dims_batch(
                vec![Some(1), Some(1)],
                vec![Some(0), Some(0)],
                vec![Some(4.0), Some(4.25)],
            ),
        ),
        (
            "fact value above the magnitude bound",
            facts_batch(vec![Some(1)], vec![Some(1_000_001.0)]),
            dims_batch(vec![Some(1)], vec![Some(0)], vec![Some(0.25)]),
        ),
        (
            "a (host, area) group whose weights are all NULL",
            facts_batch(vec![Some(1), Some(1)], vec![Some(2.0), Some(3.0)]),
            dims_batch(
                vec![Some(1), Some(1)],
                vec![Some(0), Some(1)],
                vec![None, Some(0.5)],
            ),
        ),
        (
            "a fact group whose values are all NULL",
            facts_batch(vec![Some(1), Some(1)], vec![None, None]),
            dims_batch(vec![Some(1)], vec![Some(0)], vec![Some(0.5)]),
        ),
    ];
    for (name, facts, dims) in cases {
        let ctx = count_session(facts.clone(), dims.clone()).unwrap();
        let (avg_plan, avg_batches) = plan_and_collect(&ctx, FULL_AVG).await;
        assert!(
            !is_factorized(&avg_plan),
            "{name}: unexpected AVG selection:\n{}",
            plan_text(&avg_plan)
        );
        // COUNT(*) admission is unchanged: the same batches still factorize it.
        let (count_plan, count_batches) = plan_and_collect(&ctx, FULL_COUNT).await;
        assert!(
            is_factorized(&count_plan),
            "{name}: {}",
            plan_text(&count_plan)
        );

        let baseline = baseline_context(facts, dims).await;
        let (baseline_avg, baseline_avg_batches) = plan_and_collect(&baseline, FULL_AVG).await;
        assert_eq!(
            plan_text(&avg_plan),
            plan_text(&baseline_avg),
            "{name}: AVG topology changed"
        );
        assert_eq!(
            string_rows(&avg_batches),
            string_rows(&baseline_avg_batches),
            "{name}: AVG results changed"
        );
        let (_, baseline_count_batches) = plan_and_collect(&baseline, FULL_COUNT).await;
        assert_eq!(
            string_rows(&count_batches),
            string_rows(&baseline_count_batches),
            "{name}: COUNT results changed"
        );
    }
}

#[tokio::test]
async fn non_finite_payloads_skip_the_selection_entirely() {
    let facts = facts_batch(
        vec![Some(1), Some(1), Some(1)],
        vec![Some(2.0), Some(f64::NAN), Some(5.0)],
    );
    let dims = dims_batch(
        vec![Some(1), Some(1), Some(1)],
        vec![Some(0), Some(0), Some(0)],
        vec![Some(0.25), Some(f64::INFINITY), Some(0.5)],
    );
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    for query in [FULL_AVG, FULL_COUNT] {
        let (plan, batches) = plan_and_collect(&ctx, query).await;
        assert!(!is_factorized(&plan), "{}", plan_text(&plan));
        let baseline = baseline_context(facts.clone(), dims.clone()).await;
        let (baseline_plan, baseline_batches) = plan_and_collect(&baseline, query).await;
        assert_eq!(plan_text(&plan), plan_text(&baseline_plan));
        assert_eq!(string_rows(&batches), string_rows(&baseline_batches));
    }
}

#[tokio::test]
async fn near_miss_avg_shapes_keep_the_baseline_plan() {
    let (facts, dims) = exact_domain();
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let baseline = baseline_context(facts, dims).await;
    let queries = [
        // AVG of the fact payload alone.
        "SELECT f.host, d.area, AVG(f.value) AS avg FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area",
        // SUM of the same product.
        "SELECT f.host, d.area, SUM(f.value * d.weight) AS total FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area",
        // Extra arithmetic inside the aggregate argument.
        "SELECT f.host, d.area, AVG(f.value * d.weight + 1.0) AS avg FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area",
        // The product swapped with an unrelated payload column.
        "SELECT f.host, d.area, AVG(f.value * f.value) AS avg FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area",
        // DISTINCT changes the aggregate semantics.
        "SELECT f.host, d.area, AVG(DISTINCT f.value * d.weight) AS avg FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area",
        // FILTER clause on the aggregate.
        "SELECT f.host, d.area, AVG(f.value * d.weight) FILTER (WHERE f.value > 0.0) AS avg FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area",
        // Extra aggregates need the joined rows.
        "SELECT f.host, d.area, AVG(f.value * d.weight) AS avg, COUNT(*) AS pairs FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area",
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
async fn empty_inputs_produce_no_groups_with_the_factor_plan() {
    let (facts, dims) = exact_domain();
    for (facts, dims) in [
        (RecordBatch::new_empty(facts.schema()), dims.clone()),
        (facts.clone(), RecordBatch::new_empty(dims.schema())),
    ] {
        let ctx = count_session(facts.clone(), dims.clone()).unwrap();
        let (plan, batches) = plan_and_collect(&ctx, FULL_AVG).await;
        assert!(is_factorized(&plan), "{}", plan_text(&plan));

        let baseline = baseline_context(facts, dims).await;
        let (baseline_plan, baseline_batches) = plan_and_collect(&baseline, FULL_AVG).await;
        assert_eq!(plan.schema(), baseline_plan.schema());
        assert!(batches.iter().all(|batch| batch.num_rows() == 0));
        assert!(baseline_batches.iter().all(|batch| batch.num_rows() == 0));
    }
}

#[tokio::test]
async fn swapped_operand_order_is_still_the_exact_avg_shape() {
    let facts = facts_batch(vec![Some(1), Some(1)], vec![Some(2.0), Some(3.0)]);
    let dims = dims_batch(
        vec![Some(1), Some(1)],
        vec![Some(0), Some(0)],
        vec![Some(0.25), Some(1.0)],
    );
    let query = "SELECT f.host, d.area, AVG(d.weight * f.value) AS avg FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area";
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (plan, batches) = plan_and_collect(&ctx, query).await;
    assert!(is_factorized(&plan), "{}", plan_text(&plan));
    let baseline = baseline_context(facts, dims).await;
    let (_, baseline_batches) = plan_and_collect(&baseline, query).await;
    assert_eq!(avg_rows(&batches), avg_rows(&baseline_batches));
    assert_eq!(avg_rows(&batches), vec![(Some(1), Some(0), Some(1.5625))]);
}

/// Admitted composite fixture that still sits inside the exact domain.
///
/// * host 0/area 0 mixes NULL and non-null values *and* weights, so its factor sums
///   and its `valid` count both come from the non-null rows only (`valid = 4 > 0`);
/// * host 0's NULL-area dimension row is an ordinary `(0, NULL)` group of host 0's
///   facts, not a dropped row;
/// * host 1's values cancel exactly under a positive weight, so baseline, oracle,
///   and factor path all produce `+0.0`;
/// * host 2's value is a zero under a negative weight, so its only joined product is
///   `-0.0` in both arms while its grouped score is `+0.0`.
fn mixed_payload_fixture() -> (RecordBatch, RecordBatch) {
    let facts = facts_batch(
        vec![Some(0), Some(0), Some(0), Some(1), Some(1), Some(2)],
        vec![
            Some(4.0),
            None,
            Some(-1.0),
            Some(5.0),
            Some(-5.0),
            Some(0.0),
        ],
    );
    let dims = dims_batch(
        vec![Some(0), Some(0), Some(0), Some(0), Some(1), Some(2)],
        vec![Some(0), Some(0), Some(0), None, Some(0), Some(0)],
        vec![
            Some(0.5),
            None,
            Some(2.0),
            Some(-1.0),
            Some(1.0),
            Some(-0.5),
        ],
    );
    (facts, dims)
}

#[tokio::test]
async fn composite_fixture_with_nulls_and_zero_signs_matches_bitwise() {
    let (facts, dims) = mixed_payload_fixture();
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (plan, batches) = plan_and_collect(&ctx, FULL_AVG).await;
    assert!(is_factorized(&plan), "{}", plan_text(&plan));

    // hand-computed exact scores:
    //   host 0/1.875 = (4 + -1) * (0.5 + 2.0) / valid, valid = 2 * 2
    //   host 0 NULL area = (4 + -1) * -1.0 / 2
    //   host 1 = (5 + -5) * 1.0 / 2 = +0.0
    //   host 2 = 0.0 * -0.5 / 1 = +0.0 (the product is -0.0, the running sum is not)
    let expected = vec![
        (Some(0), None, Some(-1.5)),
        (Some(0), Some(0), Some(1.875)),
        (Some(1), Some(0), Some(0.0)),
        (Some(2), Some(0), Some(0.0)),
    ];
    let factorized = avg_rows(&batches);
    assert_scores_bitwise(&factorized, &expected, "hand-computed scores");
    // Both zero rows are positive zero: an exact cancellation and a single `-0.0`
    // product both enter a `+0.0` running sum, so signed zero never reaches the
    // result.
    assert_eq!(factorized[2].2.unwrap().to_bits(), 0.0f64.to_bits());
    assert_eq!(factorized[3].2.unwrap().to_bits(), 0.0f64.to_bits());
    // Complete result equality against the authoritative baseline, bitwise on every
    // score, including NULL payload rows, the NULL-area group, and the zero results.
    let baseline = baseline_context(facts.clone(), dims.clone()).await;
    let (_, baseline_batches) = plan_and_collect(&baseline, FULL_AVG).await;
    let baseline_rows = avg_rows(&baseline_batches);
    assert_scores_bitwise(&baseline_rows, &expected, "baseline vs hand-computed");
    assert_scores_bitwise(&factorized, &baseline_rows, "baseline");
    assert_scores_bitwise(&factorized, &oracle_avg(&facts, &dims), "oracle");
}

#[tokio::test]
async fn fixture_tables_refuse_mutations_and_keep_the_avg_admission() {
    let (facts, dims) = exact_domain();
    let ctx = count_session(facts.clone(), dims.clone()).unwrap();
    let (before_plan, _) = plan_and_collect(&ctx, FULL_AVG).await;
    let before_text = plan_text(&before_plan);

    // An out-of-domain payload must not be able to enter the admitted session.
    let insert = "INSERT INTO facts VALUES (CAST(0 AS BIGINT), CAST(1000001 AS DOUBLE))";
    let error = ctx
        .sql(insert)
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("the fixture table accepted an insert");
    assert!(error.to_string().contains("not implemented"), "{error}");

    // DataFusion plans UPDATE through a separate provider hook, which also refuses.
    let update = "UPDATE facts SET value = CAST(1000001 AS DOUBLE) WHERE host = CAST(0 AS BIGINT)";
    let error = ctx
        .sql(update)
        .await
        .unwrap()
        .collect()
        .await
        .expect_err("the fixture table accepted an update");
    assert!(error.to_string().contains("not supported"), "{error}");

    // The blocked mutations left the admission, the plan, and the results untouched.
    let (after_plan, after_batches) = plan_and_collect(&ctx, FULL_AVG).await;
    assert!(is_factorized(&after_plan), "{}", plan_text(&after_plan));
    assert_eq!(plan_text(&after_plan), before_text);
    let baseline = baseline_context(facts.clone(), dims.clone()).await;
    let (_, baseline_batches) = plan_and_collect(&baseline, FULL_AVG).await;
    assert_scores_bitwise(
        &avg_rows(&after_batches),
        &avg_rows(&baseline_batches),
        "after blocked mutations",
    );

    // Premise of the guard: the same SQL succeeds against an ordinary `MemTable`
    // session and changes its AVG result, so the refusal above is what keeps the
    // admission honest rather than a property of the statement.
    let mutable = baseline_context(facts, dims).await;
    let (_, mutable_before) = plan_and_collect(&mutable, FULL_AVG).await;
    mutable.sql(insert).await.unwrap().collect().await.unwrap();
    let (_, mutable_after) = plan_and_collect(&mutable, FULL_AVG).await;
    assert_ne!(
        avg_rows(&mutable_after),
        avg_rows(&mutable_before),
        "the insert did not reach the mutable baseline table"
    );
}
