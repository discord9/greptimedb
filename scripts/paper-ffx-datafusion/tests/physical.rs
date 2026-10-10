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

//! Physical execution plan regression tests.

use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::test::exec::MockExec;
use datafusion::physical_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use paper_ffx_datafusion::{FactorAggregateExec, RawFactorExec, factor_schema};

type AggregateRow = (Option<i64>, Option<i64>, i64, i64, Option<f64>);

#[test]
fn factor_schema_is_compact_and_fixed() {
    let schema = factor_schema();
    assert_eq!(schema.fields().len(), 4);
    assert_eq!(schema.field(0).name(), "host");
    assert_eq!(schema.field(1).name(), "values");
    assert_eq!(schema.field(2).name(), "areas");
    assert_eq!(schema.field(3).name(), "weights");
}

#[tokio::test]
async fn raw_payload_is_compact_and_executor_reexecutes() {
    let (facts, dims) = fixtures();
    let ctx = context(facts, dims).await;
    let fact_scan = ctx
        .table("facts")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let dim_scan = ctx
        .table("dims")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let raw: Arc<dyn ExecutionPlan> =
        Arc::new(RawFactorExec::try_new(fact_scan, dim_scan).unwrap());
    assert_eq!(raw.output_partitioning().partition_count(), 1);
    let task = ctx.task_ctx();
    let raw_batches = collect(raw.clone(), task.clone()).await.unwrap();
    assert_eq!(
        raw_batches.iter().map(RecordBatch::num_rows).sum::<usize>(),
        15
    );
    let logical_pairs = 26;
    let mut raw_hosts = Vec::new();
    let mut raw_payload_rows = 0;
    for batch in &raw_batches {
        let values = batch
            .column(1)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::ListArray>()
            .unwrap();
        let areas = batch
            .column(2)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::ListArray>()
            .unwrap();
        let weights = batch
            .column(3)
            .as_any()
            .downcast_ref::<datafusion::arrow::array::ListArray>()
            .unwrap();
        let hosts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            raw_payload_rows += 1;
            raw_hosts.push(hosts.value(i));
            assert_eq!(areas.value(i).len(), weights.value(i).len());
            if hosts.value(i) == 1 {
                assert_eq!(values.value(i).len(), 2);
                assert_eq!(areas.value(i).len(), 3);
            }
        }
    }
    raw_hosts.sort_unstable();
    raw_hosts.dedup();
    assert_eq!(raw_payload_rows, 15);
    assert_eq!(raw_hosts.len(), raw_payload_rows);
    assert!(logical_pairs > raw_payload_rows);
    let aggregate: Arc<dyn ExecutionPlan> =
        Arc::new(FactorAggregateExec::try_new(raw.clone()).unwrap());
    let first = collect(aggregate.clone(), task.clone()).await.unwrap();
    let second = collect(aggregate, task).await.unwrap();
    assert_eq!(rows(&first), rows(&second));
    assert!(raw.execute(1, ctx.task_ctx()).is_err());
    let consumer = FactorAggregateExec::try_new(raw).unwrap();
    assert!(consumer.execute(1, ctx.task_ctx()).is_err());
}

#[tokio::test]
async fn producer_propagates_child_stream_errors() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Int64, true),
        Field::new("value", DataType::Float64, true),
    ]));
    let error_exec: Arc<dyn ExecutionPlan> = Arc::new(
        MockExec::new(
            vec![Err(DataFusionError::Execution(
                "injected child error".into(),
            ))],
            schema.clone(),
        )
        .with_use_task(false)
        .with_unknown_statistics(),
    );
    let dims = empty(vec![
        Field::new("host", DataType::Int64, true),
        Field::new("area", DataType::Int64, true),
        Field::new("weight", DataType::Float64, true),
    ]);
    let raw: Arc<dyn ExecutionPlan> = Arc::new(RawFactorExec::try_new(error_exec, dims).unwrap());
    let ctx = SessionContext::new();
    let error = collect(raw, ctx.task_ctx()).await.unwrap_err();
    assert!(error.to_string().contains("injected child error"));
}

#[tokio::test]
async fn finite_input_cancellation_boundary_is_intentionally_not_equivalent() {
    let facts = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("host", DataType::Int64, true),
            Field::new("value", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(1)])) as ArrayRef,
            Arc::new(Float64Array::from(vec![Some(1e308), Some(1e308)])),
        ],
    )
    .unwrap();
    let dims = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("host", DataType::Int64, true),
            Field::new("area", DataType::Int64, true),
            Field::new("weight", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1)])),
            Arc::new(Float64Array::from(vec![Some(1e-308)])),
        ],
    )
    .unwrap();
    let (baseline, candidate, _, _, _) = baseline_and_candidate(facts, dims).await;
    let baseline_score = rows(&baseline)[0].4.unwrap();
    let factor_score = rows(&candidate)[0].4.unwrap();
    assert!(baseline_score.is_finite());
    assert!(!factor_score.is_finite());
}

fn empty(fields: Vec<Field>) -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(Arc::new(Schema::new(fields))))
}

#[test]
fn construction_and_child_replacement_validate_schemas_and_arity() {
    let fact = empty(vec![
        Field::new("host", DataType::Int64, true),
        Field::new("value", DataType::Float64, true),
    ]);
    let dim = empty(vec![
        Field::new("host", DataType::Int64, true),
        Field::new("area", DataType::Int64, true),
        Field::new("weight", DataType::Float64, true),
    ]);
    let producer = Arc::new(RawFactorExec::try_new(fact.clone(), dim.clone()).unwrap());
    let plan: Arc<dyn ExecutionPlan> = producer.clone();
    assert_eq!(plan.children().len(), 2);
    let options = ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute);
    assert!(
        plan.clone()
            .replace_children(vec![fact.clone()], options)
            .is_err()
    );
    assert!(
        plan.replace_children(vec![fact.clone(), dim.clone()], options)
            .is_ok()
    );
    assert!(FactorAggregateExec::try_new(fact).is_err());
    assert!(FactorAggregateExec::try_new(Arc::new(EmptyExec::new(factor_schema()))).is_err());
}

fn fixtures() -> (RecordBatch, RecordBatch) {
    let mut fact_hosts = vec![Some(1), Some(1), Some(2), Some(3), None, Some(4), Some(4)];
    fact_hosts.extend((5..=15).map(Some));
    let mut fact_values = vec![
        Some(2.0),
        Some(4.0),
        Some(8.0),
        None,
        Some(99.0),
        Some(3.0),
        None,
    ];
    fact_values.extend(std::iter::repeat_n(Some(1.0), 11));
    let facts = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("host", DataType::Int64, true),
            Field::new("value", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(fact_hosts)) as ArrayRef,
            Arc::new(Float64Array::from(fact_values)),
        ],
    )
    .unwrap();
    let mut dim_hosts = vec![
        Some(1),
        Some(1),
        Some(1),
        Some(2),
        Some(2),
        Some(3),
        Some(4),
        Some(4),
    ];
    dim_hosts.extend((5..=15).map(Some));
    let mut areas = vec![
        Some(7),
        Some(7),
        None,
        Some(8),
        Some(8),
        Some(9),
        Some(10),
        Some(10),
    ];
    areas.extend((11..=21).map(Some));
    let mut weights = vec![
        Some(0.5),
        Some(1.5),
        Some(2.0),
        Some(1.0),
        None,
        Some(1.0),
        Some(0.0),
        None,
    ];
    weights.extend(std::iter::repeat_n(Some(1.0), 11));
    let dims = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("host", DataType::Int64, true),
            Field::new("area", DataType::Int64, true),
            Field::new("weight", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(dim_hosts)) as ArrayRef,
            Arc::new(Int64Array::from(areas)) as ArrayRef,
            Arc::new(Float64Array::from(weights)),
        ],
    )
    .unwrap();
    (facts, dims)
}

async fn context(facts: RecordBatch, dims: RecordBatch) -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_repartition_joins(false)
        .with_repartition_aggregations(false);
    let ctx = SessionContext::new_with_config(config);
    let fact_split = facts.num_rows().min(1);
    let fact_batches = vec![
        facts.slice(0, fact_split),
        facts.slice(fact_split, facts.num_rows() - fact_split),
    ];
    let dim_batches = vec![
        dims.slice(0, dims.num_rows() / 2),
        dims.slice(dims.num_rows() / 2, dims.num_rows() - dims.num_rows() / 2),
    ];
    ctx.register_table(
        "facts",
        Arc::new(MemTable::try_new(facts.schema(), vec![fact_batches]).unwrap()),
    )
    .unwrap();
    ctx.register_table(
        "dims",
        Arc::new(MemTable::try_new(dims.schema(), vec![dim_batches]).unwrap()),
    )
    .unwrap();
    ctx
}

const QUERY: &str = "SELECT f.host, d.area, COUNT(*) AS pairs, COUNT(f.value * d.weight) AS valid, AVG(f.value * d.weight) AS score FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area ORDER BY score DESC NULLS LAST, f.host ASC NULLS LAST, d.area ASC NULLS LAST";
const LIMIT_QUERY: &str = "SELECT f.host, d.area, COUNT(*) AS pairs, COUNT(f.value * d.weight) AS valid, AVG(f.value * d.weight) AS score FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area ORDER BY score DESC NULLS LAST, f.host ASC NULLS LAST, d.area ASC NULLS LAST LIMIT 10";

async fn baseline_and_candidate(
    facts: RecordBatch,
    dims: RecordBatch,
) -> (
    Vec<RecordBatch>,
    Vec<RecordBatch>,
    datafusion::arrow::datatypes::SchemaRef,
    Vec<RecordBatch>,
    Vec<RecordBatch>,
) {
    let ctx = context(facts, dims).await;
    let baseline_plan = ctx
        .sql(QUERY)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let baseline_schema = baseline_plan.schema();
    let plan_text = format!(
        "{}",
        datafusion::physical_plan::displayable(baseline_plan.as_ref()).indent(true)
    );
    assert!(plan_text.contains("HashJoinExec"), "{plan_text}");
    assert!(plan_text.contains("AggregateExec"), "{plan_text}");
    let baseline = collect(baseline_plan, ctx.task_ctx()).await.unwrap();
    let facts = ctx
        .table("facts")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let dims = ctx
        .table("dims")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let producer = Arc::new(RawFactorExec::try_new(facts, dims).unwrap());
    let factor: Arc<dyn ExecutionPlan> = Arc::new(FactorAggregateExec::try_new(producer).unwrap());
    assert_eq!(baseline_schema, factor.schema());
    let ordering = LexOrdering::new(vec![
        PhysicalSortExpr::new_default(Arc::new(Column::new("score", 4)))
            .desc()
            .nulls_last(),
        PhysicalSortExpr::new_default(Arc::new(Column::new("host", 0)))
            .asc()
            .nulls_last(),
        PhysicalSortExpr::new_default(Arc::new(Column::new("area", 1)))
            .asc()
            .nulls_last(),
    ])
    .unwrap();
    let sorted = collect(
        Arc::new(SortExec::new(ordering.clone(), Arc::clone(&factor))),
        ctx.task_ctx(),
    )
    .await
    .unwrap();
    let topk = collect(
        Arc::new(SortExec::new(ordering, factor).with_fetch(Some(10))),
        ctx.task_ctx(),
    )
    .await
    .unwrap();
    let baseline_topk = ctx.sql(LIMIT_QUERY).await.unwrap().collect().await.unwrap();
    (baseline, sorted, baseline_schema, topk, baseline_topk)
}

fn oracle(facts: &RecordBatch, dims: &RecordBatch) -> Vec<AggregateRow> {
    let fh = facts
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let fv = facts
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let dh = dims
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let da = dims
        .column(1)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let dw = dims
        .column(2)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let mut groups = std::collections::BTreeMap::<(i64, Option<i64>), (i64, i64, f64)>::new();
    for i in 0..facts.num_rows() {
        if fh.is_null(i) {
            continue;
        }
        for j in 0..dims.num_rows() {
            if dh.is_null(j) || fh.value(i) != dh.value(j) {
                continue;
            }
            let entry = groups
                .entry((fh.value(i), (!da.is_null(j)).then_some(da.value(j))))
                .or_default();
            entry.0 += 1;
            if !fv.is_null(i) && !dw.is_null(j) {
                entry.1 += 1;
                entry.2 += fv.value(i) * dw.value(j);
            }
        }
    }
    groups
        .into_iter()
        .map(|((h, a), (pairs, valid, sum))| {
            (
                Some(h),
                a,
                pairs,
                valid,
                (valid > 0).then_some(sum / valid as f64),
            )
        })
        .collect()
}

fn rows(batches: &[RecordBatch]) -> Vec<AggregateRow> {
    let mut out = Vec::new();
    for batch in batches {
        let h = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let a = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let pairs = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let valid = batch
            .column(3)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let score = batch
            .column(4)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..batch.num_rows() {
            out.push((
                (!h.is_null(i)).then_some(h.value(i)),
                (!a.is_null(i)).then_some(a.value(i)),
                pairs.value(i),
                valid.value(i),
                (!score.is_null(i)).then_some(score.value(i)),
            ));
        }
    }
    out
}

#[tokio::test]
async fn physical_factor_pipeline_matches_real_sql_for_nulls_duplicates_and_empty_inputs() {
    let (facts, dims) = fixtures();
    let dims_for_empty = dims.clone();
    let (baseline, candidate, schema, topk, baseline_topk) =
        baseline_and_candidate(facts.clone(), dims.clone()).await;
    assert_eq!(baseline[0].schema(), schema);
    assert_eq!(candidate[0].schema(), schema);
    assert_eq!(topk[0].schema(), schema);
    assert_eq!(rows(&topk), rows(&baseline_topk));
    let mut expected = oracle(&facts, &dims);
    expected.sort_by(|a, b| {
        let scores = match (a.4, b.4) {
            (Some(a), Some(b)) => b.partial_cmp(&a).unwrap(),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => std::cmp::Ordering::Equal,
        };
        scores
            .then_with(|| a.0.cmp(&b.0))
            .then_with(|| a.1.cmp(&b.1))
    });
    assert_eq!(rows(&baseline), expected);
    let actual = rows(&candidate);
    assert_eq!(actual.len(), expected.len());
    assert_eq!(
        rows(&topk),
        expected.iter().take(10).cloned().collect::<Vec<_>>()
    );
    for (actual, expected) in actual.iter().zip(&expected) {
        assert_eq!(
            (actual.0, actual.1, actual.2, actual.3),
            (expected.0, expected.1, expected.2, expected.3)
        );
        match (actual.4, expected.4) {
            (Some(a), Some(e)) => assert!((a - e).abs() < 1e-12),
            (None, None) => {}
            _ => panic!("score null mismatch"),
        }
    }
    assert!(
        actual
            .iter()
            .any(|r| r.0 == Some(1) && r.1.is_none() && r.2 == 2)
    );
    assert!(actual.iter().any(|r| r.0 == Some(3)
        && r.1 == Some(9)
        && r.2 == 1
        && r.3 == 0
        && r.4.is_none()));
    let empty_facts = RecordBatch::new_empty(facts.schema());
    let (_, out, _, _, _) = baseline_and_candidate(empty_facts, dims_for_empty).await;
    assert!(rows(&out).is_empty());
    let empty_dims = RecordBatch::new_empty(dims.schema());
    let (_, out, _, _, _) = baseline_and_candidate(facts, empty_dims).await;
    assert!(rows(&out).is_empty());
}
