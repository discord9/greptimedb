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

use std::{collections::HashMap, sync::Arc, time::Instant};

use arrow::{
    array::{Array, Float64Array, StringArray},
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    dataframe::DataFrame, execution::context::SessionContext, prelude::SessionConfig,
};

const SQL: &str = "SELECT f.host,d.area,COUNT(*) AS pairs FROM fact f JOIN dim d ON f.host=d.host GROUP BY f.host,d.area ORDER BY pairs DESC NULLS LAST,f.host NULLS LAST,d.area NULLS LAST";
const SQL_LIMIT: &str = "SELECT f.host,d.area,COUNT(*) AS pairs FROM fact f JOIN dim d ON f.host=d.host GROUP BY f.host,d.area ORDER BY pairs DESC NULLS LAST,f.host NULLS LAST,d.area NULLS LAST LIMIT 10";

#[derive(Clone)]
struct Fixture {
    facts: Vec<(Option<String>, Option<f64>)>,
    dims: Vec<(Option<String>, Option<String>)>,
}

fn fixture(fact_rows: usize, dim_rows: usize, hosts: usize, areas: usize) -> Fixture {
    let facts = (0..fact_rows)
        .map(|i| {
            let host = if i % 19 == 0 {
                None
            } else {
                Some(format!("h{}", i % hosts))
            };
            (host, (i % 7 != 0).then_some(i as f64))
        })
        .collect();
    let dims = (0..dim_rows)
        .map(|i| {
            let host = Some(format!("h{}", i % hosts));
            let area = if i % 11 == 0 {
                None
            } else {
                Some(format!("a{}", (i / hosts) % areas))
            };
            (host, area)
        })
        .collect();
    Fixture { facts, dims }
}

async fn register(ctx: &SessionContext, fixture: &Fixture) -> datafusion::error::Result<()> {
    let fact_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("value", DataType::Float64, true),
    ]));
    let fact = RecordBatch::try_new(
        fact_schema,
        vec![
            Arc::new(StringArray::from(
                fixture
                    .facts
                    .iter()
                    .map(|(h, _)| h.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                fixture.facts.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
            )),
        ],
    )?;
    let dim_schema = Arc::new(Schema::new(vec![
        Field::new("host", DataType::Utf8, true),
        Field::new("area", DataType::Utf8, true),
    ]));
    let dim = RecordBatch::try_new(
        dim_schema,
        vec![
            Arc::new(StringArray::from(
                fixture
                    .dims
                    .iter()
                    .map(|(h, _)| h.as_deref())
                    .collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                fixture
                    .dims
                    .iter()
                    .map(|(_, a)| a.as_deref())
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    ctx.register_batch("fact", fact)?;
    ctx.register_batch("dim", dim)?;
    Ok(())
}

fn oracle(fixture: &Fixture) -> Vec<(String, Option<String>, i64)> {
    let mut groups = HashMap::<(String, Option<String>), i64>::new();
    for (fact_host, _) in &fixture.facts {
        let Some(host) = fact_host else { continue };
        for (dim_host, area) in &fixture.dims {
            if dim_host.as_ref() == Some(host) {
                *groups.entry((host.clone(), area.clone())).or_default() += 1;
            }
        }
    }
    let mut rows = groups
        .into_iter()
        .map(|((h, a), n)| (h, a, n))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        b.2.cmp(&a.2)
            .then_with(|| a.0.cmp(&b.0))
            .then_with(|| match (&a.1, &b.1) {
                (Some(left), Some(right)) => left.cmp(right),
                (Some(_), None) => std::cmp::Ordering::Less,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (None, None) => std::cmp::Ordering::Equal,
            })
    });
    rows
}

async fn collect(frame: &DataFrame) -> datafusion::error::Result<Vec<RecordBatch>> {
    frame.clone().collect().await
}

fn rows(batches: &[RecordBatch]) -> datafusion::error::Result<Vec<(String, Option<String>, i64)>> {
    let mut rows = Vec::new();
    for batch in batches {
        let Some(hosts) = batch.column(0).as_any().downcast_ref::<StringArray>() else {
            return Err(datafusion::error::DataFusionError::Execution(
                "host output is not Utf8".into(),
            ));
        };
        let Some(areas) = batch.column(1).as_any().downcast_ref::<StringArray>() else {
            return Err(datafusion::error::DataFusionError::Execution(
                "area output is not Utf8".into(),
            ));
        };
        let Some(counts) = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
        else {
            return Err(datafusion::error::DataFusionError::Execution(
                "COUNT output is not Int64".into(),
            ));
        };
        for i in 0..batch.num_rows() {
            rows.push((
                hosts.value(i).to_owned(),
                (!areas.is_null(i)).then(|| areas.value(i).to_owned()),
                counts.value(i),
            ));
        }
    }
    Ok(rows)
}

fn check_schema(frame: &DataFrame) -> datafusion::error::Result<()> {
    let schema = frame.schema();
    if schema.field(0).data_type() != &DataType::Utf8
        || schema.field(1).data_type() != &DataType::Utf8
        || schema.field(2).data_type() != &DataType::Int64
        || schema.field(2).is_nullable()
    {
        return Err(datafusion::error::DataFusionError::Plan(
            "query output schema differs from the expected host, area, COUNT(*) schema".into(),
        ));
    }
    Ok(())
}

async fn run_case(name: &str, fixture: Fixture, bench: bool) -> datafusion::error::Result<()> {
    let control = SessionContext::new_with_config(
        SessionConfig::new()
            .set_bool("datafusion.optimizer.skip_failed_rules", false)
            .set_usize("datafusion.execution.target_partitions", 1),
    );
    let candidate = paper_ffx_auto_rewrite::experiment_session(
        fixture.facts.len() as u64,
        fixture.dims.len() as u64,
    )?;
    register(&control, &fixture).await?;
    register(&candidate, &fixture).await?;
    let baseline = control.sql(SQL).await?;
    let optimized = candidate.sql(SQL).await?;
    let baseline_plan = control.state().optimize(baseline.logical_plan())?;
    let optimized_plan = candidate.state().optimize(optimized.logical_plan())?;
    let plan_text = format!("{optimized_plan:#?}");
    if plan_text.matches("partial_count").count() < 2 {
        return Err(datafusion::error::DataFusionError::Plan(format!(
            "automatic count-join rewrite did not activate: {plan_text}"
        )));
    }
    if format!("{baseline_plan:#?}").contains("partial_count") {
        return Err(datafusion::error::DataFusionError::Plan(
            "default optimizer unexpectedly contains experiment partial counts".into(),
        ));
    }
    check_schema(&optimized)?;
    check_schema(&baseline)?;
    let expected = oracle(&fixture);
    let baseline_rows = rows(&collect(&baseline).await?)?;
    let optimized_rows = rows(&collect(&optimized).await?)?;
    if baseline_rows != expected || optimized_rows != expected {
        return Err(datafusion::error::DataFusionError::Execution(format!(
            "baseline/candidate result differs from independent oracle: baseline={baseline_rows:?}, candidate={optimized_rows:?}, oracle={expected:?}"
        )));
    }

    let baseline_limit = control.sql(SQL_LIMIT).await?;
    let optimized_limit = candidate.sql(SQL_LIMIT).await?;
    check_schema(&optimized_limit)?;
    let expected_top = expected.iter().take(10).cloned().collect::<Vec<_>>();
    if rows(&collect(&baseline_limit).await?)? != expected_top
        || rows(&collect(&optimized_limit).await?)? != expected_top
    {
        return Err(datafusion::error::DataFusionError::Execution(
            "baseline or candidate LIMIT result differs from the ordered oracle".into(),
        ));
    }

    let baseline_physical = baseline.create_physical_plan().await?;
    let optimized_physical = optimized.create_physical_plan().await?;
    println!(
        "case={name} facts={} dims={} baseline_join_rows={} factorized_join_rows={} groups={}",
        fixture.facts.len(),
        fixture.dims.len(),
        expected
            .iter()
            .map(|(_, _, count)| *count as usize)
            .sum::<usize>(),
        expected.len(),
        expected.len()
    );
    println!(
        "baseline logical plan:\n{baseline_plan:#?}\noptimized logical plan:\n{optimized_plan:#?}\nbaseline physical plan:\n{baseline_physical:#?}\noptimized physical plan:\n{optimized_physical:#?}"
    );

    if bench {
        println!(
            "timing_scope=DataFrame collection, including per-query physical planning, execution, sorting, and complete result collection; fixture registration excluded"
        );
        let mut baseline_ns = Vec::new();
        let mut optimized_ns = Vec::new();
        for iteration in 0..12 {
            if iteration % 2 == 0 {
                baseline_ns.push(measure(&baseline, &expected).await?);
                optimized_ns.push(measure(&optimized, &expected).await?);
            } else {
                optimized_ns.push(measure(&optimized, &expected).await?);
                baseline_ns.push(measure(&baseline, &expected).await?);
            }
        }
        baseline_ns.drain(..3);
        optimized_ns.drain(..3);
        println!(
            "benchmark={name} baseline_samples_ns={baseline_ns:?} optimized_samples_ns={optimized_ns:?} baseline_median_ns={} optimized_median_ns={}",
            median(&mut baseline_ns),
            median(&mut optimized_ns)
        );
    }
    Ok(())
}

async fn measure(
    frame: &DataFrame,
    expected: &[(String, Option<String>, i64)],
) -> datafusion::error::Result<u128> {
    let start = Instant::now();
    let result = collect(frame).await?;
    let elapsed = start.elapsed().as_nanos();
    if rows(&result)? != expected {
        return Err(datafusion::error::DataFusionError::Execution(
            "timed query result differs from the independent full-result oracle".to_owned(),
        ));
    }
    Ok(elapsed)
}

fn median(samples: &mut [u128]) -> u128 {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let bench = std::env::args().any(|arg| arg == "--bench");
    if bench {
        run_case("balanced", fixture(256, 64, 16, 4), true).await?;
        run_case("hot", fixture(128, 128, 1, 4), true).await?;
        run_case("low-overlap", fixture(16, 16, 16, 1), true).await?;
    } else {
        run_case("proof", fixture(4, 4, 3, 2), false).await?;
    }
    Ok(())
}
