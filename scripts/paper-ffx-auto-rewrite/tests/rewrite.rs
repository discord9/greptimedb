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

use arrow::{
    array::{Array, Float64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::execution::context::SessionContext;
use paper_ffx_auto_rewrite::experiment_session;

const SQL: &str = "SELECT f.host,d.area,COUNT(*) AS pairs FROM fact f JOIN dim d ON f.host=d.host GROUP BY f.host,d.area ORDER BY pairs DESC NULLS LAST,f.host NULLS LAST,d.area NULLS LAST";
const SQL_LIMIT: &str = "SELECT f.host,d.area,COUNT(*) AS pairs FROM fact f JOIN dim d ON f.host=d.host GROUP BY f.host,d.area ORDER BY pairs DESC NULLS LAST,f.host NULLS LAST,d.area NULLS LAST LIMIT 10";

async fn register(ctx: &SessionContext) -> datafusion::error::Result<()> {
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));
    let fact = RecordBatch::try_new(
        fact_schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("a"),
                Some("b"),
                Some("b"),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                None,
                Some(3.0),
                Some(4.0),
                None,
                Some(5.0),
                Some(8.0),
            ])),
        ],
    )?;
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("area", DataType::Utf8, true),
    ]));
    let dim = RecordBatch::try_new(
        dim_schema,
        vec![
            Arc::new(StringArray::from(vec![
                Some("a"),
                Some("a"),
                Some("b"),
                Some("b"),
                Some("x"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("north"),
                None,
                Some("south"),
                Some("south"),
                Some("other"),
            ])),
        ],
    )?;
    ctx.register_batch("fact", fact)?;
    ctx.register_batch("dim", dim)?;
    Ok(())
}

fn has_partial_count(plan: &datafusion::logical_expr::LogicalPlan) -> bool {
    format!("{plan:#?}").contains("partial_count")
}

fn result_rows(batches: &[RecordBatch]) -> Vec<(String, Option<String>, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let hosts = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let areas = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let counts = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        for index in 0..batch.num_rows() {
            rows.push((
                hosts.value(index).to_owned(),
                (!areas.is_null(index)).then(|| areas.value(index).to_owned()),
                counts.value(index),
            ));
        }
    }
    rows
}

fn assert_rewrite(plan: &datafusion::logical_expr::LogicalPlan) {
    let text = format!("{plan:#?}");
    assert!(
        text.matches("partial_count").count() >= 2,
        "expected both pre-join partial counts and their product: {text}"
    );
}

#[tokio::test]
async fn original_sql_and_limit_activate_rule_and_preserve_results() -> datafusion::error::Result<()>
{
    let baseline = SessionContext::new();
    let candidate = experiment_session(6, 5)?;
    register(&baseline).await?;
    register(&candidate).await?;
    for sql in [SQL, SQL_LIMIT] {
        let baseline_frame = baseline.sql(sql).await?;
        let candidate_frame = candidate.sql(sql).await?;
        let baseline_plan = baseline.state().optimize(baseline_frame.logical_plan())?;
        let candidate_plan = candidate.state().optimize(candidate_frame.logical_plan())?;
        assert_rewrite(&candidate_plan);
        assert!(!format!("{baseline_plan:#?}").contains("partial_count"));
        let schema = candidate_plan.schema();
        assert_eq!(schema.field(0).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(1).data_type(), &DataType::Utf8);
        assert_eq!(schema.field(2).data_type(), &DataType::Int64);
        assert!(!schema.field(2).is_nullable());
        let baseline_rows = result_rows(&baseline_frame.clone().collect().await?);
        let candidate_rows = result_rows(&candidate_frame.clone().collect().await?);
        assert_eq!(baseline_rows, candidate_rows);
        if sql == SQL {
            assert_eq!(
                baseline_rows,
                vec![
                    ("b".to_owned(), Some("south".to_owned()), 4),
                    ("a".to_owned(), Some("north".to_owned()), 3),
                    ("a".to_owned(), None, 3),
                ]
            );
        }
    }
    Ok(())
}

#[tokio::test]
async fn limit_ten_orders_ties_and_excludes_remaining_groups() -> datafusion::error::Result<()> {
    let mut fact_hosts = Vec::new();
    let mut dim_hosts = Vec::new();
    let mut areas = Vec::new();
    let mut expected = Vec::new();
    for host in 0..16 {
        let name = format!("h{host:02}");
        fact_hosts.push(Some(name.clone()));
        for area in 0..2 {
            dim_hosts.push(Some(name.clone()));
            let area_name = format!("a{area}");
            areas.push(Some(area_name.clone()));
            expected.push((name.clone(), Some(area_name), 1));
        }
    }
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, false),
        Field::new("value", DataType::Float64, true),
    ]));
    let fact = RecordBatch::try_new(
        Arc::clone(&fact_schema),
        vec![
            Arc::new(StringArray::from(fact_hosts)),
            Arc::new(Float64Array::from(vec![None; 16])),
        ],
    )?;
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, false),
        Field::new("area", DataType::Utf8, false),
    ]));
    let dim = RecordBatch::try_new(
        Arc::clone(&dim_schema),
        vec![
            Arc::new(StringArray::from(dim_hosts)),
            Arc::new(StringArray::from(areas)),
        ],
    )?;
    let baseline = SessionContext::new();
    let candidate = experiment_session(16, 32)?;
    for ctx in [&baseline, &candidate] {
        ctx.register_batch("fact", fact.clone())?;
        ctx.register_batch("dim", dim.clone())?;
    }
    let baseline_frame = baseline.sql(SQL_LIMIT).await?;
    let candidate_frame = candidate.sql(SQL_LIMIT).await?;
    let optimized = candidate.state().optimize(candidate_frame.logical_plan())?;
    assert_rewrite(&optimized);
    expected.sort_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(&right.1)));
    expected.truncate(10);
    let baseline_rows = result_rows(&baseline_frame.collect().await?);
    let candidate_rows = result_rows(&candidate_frame.collect().await?);
    assert_eq!(baseline_rows, expected);
    assert_eq!(candidate_rows, expected);
    Ok(())
}

#[tokio::test]
async fn default_session_does_not_rewrite() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();
    register(&ctx).await?;
    let frame = ctx.sql(SQL).await?;
    let plan = ctx.state().optimize(frame.logical_plan())?;
    let text = format!("{plan:#?}");
    assert!(!text.contains("partial_count"));
    assert!(text.contains("Aggregate"));
    Ok(())
}

#[tokio::test]
async fn unsupported_counts_and_join_shapes_remain_unfactored() -> datafusion::error::Result<()> {
    let queries = [
        "SELECT f.host,d.area,COUNT(f.value) AS pairs FROM fact f JOIN dim d ON f.host=d.host GROUP BY f.host,d.area",
        "SELECT f.host,d.area,COUNT(DISTINCT f.value) AS pairs FROM fact f JOIN dim d ON f.host=d.host GROUP BY f.host,d.area",
        "SELECT f.host,d.area,COUNT(*) FILTER (WHERE f.value > 0) AS pairs FROM fact f JOIN dim d ON f.host=d.host GROUP BY f.host,d.area",
        "SELECT f.host,d.area,SUM(f.value) AS pairs FROM fact f JOIN dim d ON f.host=d.host GROUP BY f.host,d.area",
        "SELECT f.host,d.area,AVG(f.value) AS pairs FROM fact f JOIN dim d ON f.host=d.host GROUP BY f.host,d.area",
        "SELECT f.host,d.area,COUNT(*) AS pairs FROM fact f LEFT JOIN dim d ON f.host=d.host GROUP BY f.host,d.area",
        "SELECT f.host,d.area,COUNT(*) AS pairs FROM fact f JOIN dim d ON f.host=d.host AND f.value > 0 GROUP BY f.host,d.area",
        "SELECT f.host,d.area,f.value,COUNT(*) AS pairs FROM fact f JOIN dim d ON f.host=d.host GROUP BY f.host,d.area,f.value",
        "SELECT f.host,d.area,COUNT(*) AS pairs FROM fact f JOIN dim d ON f.host IS NOT DISTINCT FROM d.host GROUP BY f.host,d.area",
        "SELECT f.host,d.area,COUNT(*) AS pairs FROM fact f JOIN dim d ON f.host=d.host AND d.area IS NOT NULL GROUP BY f.host,d.area",
        "SELECT COUNT(*) AS pairs FROM fact f JOIN dim d ON f.host=d.host",
    ];
    let ctx = experiment_session(6, 5)?;
    register(&ctx).await?;
    for sql in queries {
        let frame = ctx.sql(sql).await?;
        let plan = ctx.state().optimize(frame.logical_plan())?;
        let text = format!("{plan:#?}");
        assert!(
            !text.contains("partial_count"),
            "unsupported shape was factored: {sql}: {text}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn empty_and_filtered_inputs_preserve_results() -> datafusion::error::Result<()> {
    let baseline = SessionContext::new();
    let candidate = experiment_session(0, 0)?;
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("area", DataType::Utf8, true),
    ]));
    for ctx in [&baseline, &candidate] {
        ctx.register_batch("fact", RecordBatch::new_empty(Arc::clone(&fact_schema)))?;
        ctx.register_batch("dim", RecordBatch::new_empty(Arc::clone(&dim_schema)))?;
    }
    let baseline_frame = baseline.sql(SQL).await?;
    let candidate_frame = candidate.sql(SQL).await?;
    let baseline_plan = baseline.state().optimize(baseline_frame.logical_plan())?;
    let candidate_plan = candidate.state().optimize(candidate_frame.logical_plan())?;
    assert!(!has_partial_count(&baseline_plan));
    assert!(has_partial_count(&candidate_plan));
    assert_eq!(baseline_frame.schema(), candidate_frame.schema());
    assert_eq!(
        baseline_frame.collect().await?,
        candidate_frame.collect().await?
    );

    let baseline = SessionContext::new();
    let candidate = experiment_session(6, 5)?;
    register(&baseline).await?;
    register(&candidate).await?;
    let sql = "SELECT f.host,d.area,COUNT(*) AS pairs FROM fact f JOIN dim d ON f.host=d.host WHERE f.value > 100 GROUP BY f.host,d.area";
    let baseline_frame = baseline.sql(sql).await?;
    let candidate_frame = candidate.sql(sql).await?;
    assert_eq!(
        result_rows(&baseline_frame.collect().await?),
        result_rows(&candidate_frame.collect().await?)
    );
    Ok(())
}

#[test]
fn experiment_session_rejects_pair_count_overflow() {
    assert!(experiment_session(u64::MAX, 2).is_err());
    assert!(experiment_session(i64::MAX as u64, 1).is_ok());
    assert!(experiment_session(i64::MAX as u64, 2).is_err());
    assert!(experiment_session(0, u64::MAX).is_ok());
}
