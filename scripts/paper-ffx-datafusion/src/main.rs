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

//! Local correctness and execution-timing harness; no product integration.

use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::cast::{as_float64_array, as_int64_array};
use datafusion::datasource::MemTable;
use datafusion::execution::context::{SessionConfig, SessionContext};
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr, expressions::Column};
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use paper_ffx_datafusion::{FactorAggregateExec, RawFactorExec, count_config, count_session};

const QUERY: &str = "SELECT f.host, d.area, COUNT(*) AS pairs, COUNT(f.value * d.weight) AS valid, AVG(f.value * d.weight) AS score FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area ORDER BY score DESC NULLS LAST, f.host ASC NULLS LAST, d.area ASC NULLS LAST LIMIT 10";
const COUNT_QUERY: &str = "SELECT f.host, d.area, COUNT(*) AS pairs FROM facts f JOIN dims d ON f.host = d.host GROUP BY f.host, d.area";
/// Fixtures timed by `--count --bench`. `hot2048` mirrors the one-hot-host fanout
/// shape of the COUNT fanout sweep experiment (2048 facts x 2048 dimensions, four
/// areas); the default `--count` correctness check keeps its original fixtures.
const COUNT_BENCH_CASES: [&str; 4] = ["balanced", "hot", "low", "hot2048"];
type Row = (Option<i64>, Option<i64>, i64, i64, Option<f64>);
type CountRow = (Option<i64>, Option<i64>, i64);

fn fixture(shape: &str) -> datafusion::common::Result<(RecordBatch, RecordBatch, usize)> {
    let (fact_hosts, dim_hosts): (Vec<i64>, Vec<i64>) = match shape {
        "balanced" => (
            (0..16).flat_map(|h| std::iter::repeat_n(h, 16)).collect(),
            (0..16).flat_map(|h| std::iter::repeat_n(h, 4)).collect(),
        ),
        "hot" => (
            std::iter::repeat_n(0, 128).collect(),
            std::iter::repeat_n(0, 128).collect(),
        ),
        "low" => ((0..16).collect(), (0..16).collect()),
        "hot2048" => (
            std::iter::repeat_n(0, 2048).collect(),
            std::iter::repeat_n(0, 2048).collect(),
        ),
        _ => {
            return Err(datafusion::common::DataFusionError::Plan(format!(
                "unknown fixture {shape}"
            )));
        }
    };
    let pairs = fact_hosts
        .iter()
        .map(|h| dim_hosts.iter().filter(|d| *d == h).count())
        .sum();
    let facts = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("host", DataType::Int64, true),
            Field::new("value", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                fact_hosts.iter().copied().map(Some).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float64Array::from(
                fact_hosts
                    .iter()
                    .enumerate()
                    .map(|(i, _)| Some(((i % 8) + 1) as f64 / 4.0))
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    let dims = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("host", DataType::Int64, true),
            Field::new("area", DataType::Int64, true),
            Field::new("weight", DataType::Float64, true),
        ])),
        vec![
            Arc::new(Int64Array::from(
                dim_hosts.iter().copied().map(Some).collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Int64Array::from(
                dim_hosts
                    .iter()
                    .enumerate()
                    .map(|(i, _)| Some((i % 4) as i64))
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float64Array::from(
                dim_hosts
                    .iter()
                    .enumerate()
                    .map(|(i, _)| Some(((i % 4) + 1) as f64 / 4.0))
                    .collect::<Vec<_>>(),
            )),
        ],
    )?;
    Ok((facts, dims, pairs))
}

fn rows(batches: &[RecordBatch]) -> datafusion::common::Result<Vec<Row>> {
    let mut rows = Vec::new();
    for batch in batches {
        let h = as_int64_array(batch.column(0).as_ref())?;
        let a = as_int64_array(batch.column(1).as_ref())?;
        let pairs = as_int64_array(batch.column(2).as_ref())?;
        let valid = as_int64_array(batch.column(3).as_ref())?;
        let score = as_float64_array(batch.column(4).as_ref())?;
        for i in 0..batch.num_rows() {
            rows.push((
                (!h.is_null(i)).then_some(h.value(i)),
                (!a.is_null(i)).then_some(a.value(i)),
                pairs.value(i),
                valid.value(i),
                (!score.is_null(i)).then_some(score.value(i)),
            ));
        }
    }
    Ok(rows)
}

fn ordered_oracle(facts: &RecordBatch, dims: &RecordBatch) -> datafusion::common::Result<Vec<Row>> {
    let fh = as_int64_array(facts.column(0).as_ref())?;
    let fv = as_float64_array(facts.column(1).as_ref())?;
    let dh = as_int64_array(dims.column(0).as_ref())?;
    let da = as_int64_array(dims.column(1).as_ref())?;
    let dw = as_float64_array(dims.column(2).as_ref())?;
    let mut groups = std::collections::BTreeMap::<(i64, Option<i64>), (i64, i64, f64)>::new();
    for i in 0..facts.num_rows() {
        for j in 0..dims.num_rows() {
            if fh.is_null(i) || dh.is_null(j) || fh.value(i) != dh.value(j) {
                continue;
            }
            let entry = groups
                .entry((fh.value(i), (!da.is_null(j)).then_some(da.value(j))))
                .or_default();
            entry.0 += 1;
            entry.1 += 1;
            entry.2 += fv.value(i) * dw.value(j);
        }
    }
    let mut rows = groups
        .into_iter()
        .map(|((h, a), (p, v, s))| (Some(h), a, p, v, Some(s / v as f64)))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| {
        b.4.unwrap_or(f64::NEG_INFINITY)
            .total_cmp(&a.4.unwrap_or(f64::NEG_INFINITY))
            .then_with(|| a.0.cmp(&b.0))
            .then_with(|| a.1.cmp(&b.1))
    });
    rows.truncate(10);
    Ok(rows)
}

async fn run_case(name: &str, warmups: usize, samples: usize) -> datafusion::common::Result<()> {
    let (facts, dims, logical_pairs) = fixture(name)?;
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_repartition_joins(false)
        .with_repartition_aggregations(false);
    let ctx = SessionContext::new_with_config(config);
    ctx.register_table(
        "facts",
        Arc::new(MemTable::try_new(
            facts.schema(),
            vec![vec![facts.clone()]],
        )?),
    )?;
    ctx.register_table(
        "dims",
        Arc::new(MemTable::try_new(dims.schema(), vec![vec![dims.clone()]])?),
    )?;
    let baseline = ctx.sql(QUERY).await?.create_physical_plan().await?;
    let fact_scan = ctx.table("facts").await?.create_physical_plan().await?;
    let dim_scan = ctx.table("dims").await?.create_physical_plan().await?;
    let raw = Arc::new(RawFactorExec::try_new(fact_scan, dim_scan)?);
    let raw_plan: Arc<dyn ExecutionPlan> = raw.clone();
    let factors = collect(raw_plan, ctx.task_ctx()).await?;
    let factor_rows = factors.iter().map(RecordBatch::num_rows).sum::<usize>();
    let aggregate: Arc<dyn ExecutionPlan> = Arc::new(FactorAggregateExec::try_new(raw)?);
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
    .ok_or_else(|| datafusion::common::DataFusionError::Plan("invalid ordering".into()))?;
    let candidate: Arc<dyn ExecutionPlan> = Arc::new(
        datafusion::physical_plan::sorts::sort::SortExec::new(ordering, aggregate)
            .with_fetch(Some(10)),
    );
    if baseline.schema() != candidate.schema() {
        return Err(datafusion::common::DataFusionError::Execution(
            "baseline and candidate schemas differ".into(),
        ));
    }
    let expected = ordered_oracle(&facts, &dims)?;
    let baseline_output = collect(Arc::clone(&baseline), ctx.task_ctx()).await?;
    let candidate_output = collect(Arc::clone(&candidate), ctx.task_ctx()).await?;
    if rows(&baseline_output)? != expected || rows(&candidate_output)? != expected {
        return Err(datafusion::common::DataFusionError::Execution(format!(
            "{name}: untimed outputs differ from nested-loop oracle"
        )));
    }
    println!(
        "case={name} fact_rows={} dim_rows={} logical_pairs={logical_pairs} factor_rows={factor_rows}\nbaseline plan:\n{}candidate plan:\n{}",
        facts.num_rows(),
        dims.num_rows(),
        displayable(baseline.as_ref()).indent(true),
        displayable(candidate.as_ref()).indent(true)
    );
    let mut base_ms = Vec::new();
    let mut candidate_ms = Vec::new();
    for i in 0..(warmups + samples) {
        // SortExec's dynamic TopK filter is execution-scoped, so build fresh
        // physical plans before each timed execution; keep planning out of time.
        let baseline = ctx.sql(QUERY).await?.create_physical_plan().await?;
        let fact_scan = ctx.table("facts").await?.create_physical_plan().await?;
        let dim_scan = ctx.table("dims").await?.create_physical_plan().await?;
        let raw = Arc::new(RawFactorExec::try_new(fact_scan, dim_scan)?);
        let aggregate: Arc<dyn ExecutionPlan> = Arc::new(FactorAggregateExec::try_new(raw)?);
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
        .ok_or_else(|| datafusion::common::DataFusionError::Plan("invalid ordering".into()))?;
        let candidate: Arc<dyn ExecutionPlan> = Arc::new(
            datafusion::physical_plan::sorts::sort::SortExec::new(ordering, aggregate)
                .with_fetch(Some(10)),
        );
        let mut order = [(&baseline, true), (&candidate, false)];
        if i % 2 == 1 {
            order.reverse();
        }
        for (plan, is_baseline) in order {
            let start = Instant::now();
            let output = collect(Arc::clone(plan), ctx.task_ctx()).await?;
            let elapsed = start.elapsed().as_secs_f64() * 1e3;
            if rows(&output)? != expected {
                return Err(datafusion::common::DataFusionError::Execution(format!(
                    "{name}: timed output differs from oracle"
                )));
            }
            if i >= warmups {
                if is_baseline {
                    base_ms.push(elapsed);
                } else {
                    candidate_ms.push(elapsed);
                }
            }
        }
    }
    let mut bmed = base_ms.clone();
    bmed.sort_by(f64::total_cmp);
    let mut cmed = candidate_ms.clone();
    cmed.sort_by(f64::total_cmp);
    println!(
        "case={name} warmups={warmups} alternating_samples={samples} baseline_full_ordered_TOP10_ms={base_ms:?} median_ms={} candidate_full_ordered_TOP10_ms={candidate_ms:?} median_ms={} (physical planning excluded)",
        bmed[bmed.len() / 2],
        cmed[cmed.len() / 2]
    );
    Ok(())
}

/// Complete grouped `COUNT(*)` rows ordered by (host, area).
fn count_rows(batches: &[RecordBatch]) -> datafusion::common::Result<Vec<CountRow>> {
    let mut rows = Vec::new();
    for batch in batches {
        let h = as_int64_array(batch.column(0).as_ref())?;
        let a = as_int64_array(batch.column(1).as_ref())?;
        let p = as_int64_array(batch.column(2).as_ref())?;
        for i in 0..batch.num_rows() {
            rows.push((
                (!h.is_null(i)).then_some(h.value(i)),
                (!a.is_null(i)).then_some(a.value(i)),
                p.value(i),
            ));
        }
    }
    rows.sort();
    Ok(rows)
}

/// Plain same-config session over the same batches, without the factorized
/// selection: the COUNT(*) baseline for the correctness and the bench modes.
fn plain_count_session(
    facts: &RecordBatch,
    dims: &RecordBatch,
) -> datafusion::common::Result<SessionContext> {
    let ctx = SessionContext::new_with_config(count_config());
    ctx.register_table(
        "facts",
        Arc::new(MemTable::try_new(
            facts.schema(),
            vec![vec![facts.clone()]],
        )?),
    )?;
    ctx.register_table(
        "dims",
        Arc::new(MemTable::try_new(dims.schema(), vec![vec![dims.clone()]])?),
    )?;
    Ok(ctx)
}

/// `--count --bench` fixture timing: alternating end-to-end runs of the original
/// COUNT SQL through `ctx.sql(..).collect()` on both arms.
///
/// Every timed iteration re-plans the SQL from scratch, so the `count_session`
/// numbers include the full SQL planning and factorized-selection cost of the
/// automatic rewrite and no physical plan is pre-built outside the timed region.
/// The other arm is the same SQL on a plain same-config session. Complete-result
/// equality with the plain baseline is re-checked once untimed before any timing
/// and again after every timed iteration.
async fn run_count_bench_case(
    name: &str,
    warmups: usize,
    samples: usize,
) -> datafusion::common::Result<()> {
    let (facts, dims, logical_pairs) = fixture(name)?;
    let fact_rows = facts.num_rows();
    let dim_rows = dims.num_rows();
    let candidate_ctx = count_session(facts.clone(), dims.clone())?;
    let baseline_ctx = plain_count_session(&facts, &dims)?;

    // Untimed pre-check: the SQL must select the factorized plan, and the
    // complete grouped result must match the plain baseline and the fixture.
    let candidate_plan = candidate_ctx
        .sql(COUNT_QUERY)
        .await?
        .create_physical_plan()
        .await?;
    let factor_text = displayable(candidate_plan.as_ref())
        .indent(true)
        .to_string();
    if !factor_text.contains("RawFactorExec") || !factor_text.contains("FactorAggregateExec") {
        return Err(datafusion::common::DataFusionError::Execution(format!(
            "{name}: COUNT(*) SQL did not select the factorized plan"
        )));
    }
    let candidate_rows = count_rows(&candidate_ctx.sql(COUNT_QUERY).await?.collect().await?)?;
    let baseline_rows = count_rows(&baseline_ctx.sql(COUNT_QUERY).await?.collect().await?)?;
    if candidate_rows != baseline_rows {
        return Err(datafusion::common::DataFusionError::Execution(format!(
            "{name}: COUNT(*) selection changed the complete grouped result"
        )));
    }
    let counted = candidate_rows.iter().map(|row| row.2).sum::<i64>();
    if counted != logical_pairs as i64 {
        return Err(datafusion::common::DataFusionError::Execution(format!(
            "{name}: counted pairs {counted} differ from fixture pairs {logical_pairs}"
        )));
    }

    let mut baseline_ms = Vec::new();
    let mut candidate_ms = Vec::new();
    for i in 0..(warmups + samples) {
        // Fresh planning per iteration for both arms: the selected plan is
        // execution-scoped, so nothing is reused across timed iterations.
        let mut order = [(&candidate_ctx, true), (&baseline_ctx, false)];
        if i % 2 == 1 {
            order.reverse();
        }
        for (ctx, is_candidate) in order {
            let start = Instant::now();
            let output = ctx.sql(COUNT_QUERY).await?.collect().await?;
            let elapsed = start.elapsed().as_secs_f64() * 1e3;
            if count_rows(&output)? != baseline_rows {
                return Err(datafusion::common::DataFusionError::Execution(format!(
                    "{name}: timed COUNT(*) result differs from the untimed baseline"
                )));
            }
            if i >= warmups {
                if is_candidate {
                    candidate_ms.push(elapsed);
                } else {
                    baseline_ms.push(elapsed);
                }
            }
        }
    }
    let mut bmed = baseline_ms.clone();
    bmed.sort_by(f64::total_cmp);
    let mut cmed = candidate_ms.clone();
    cmed.sort_by(f64::total_cmp);
    println!(
        "case={name} fact_rows={fact_rows} dim_rows={dim_rows} logical_pairs={logical_pairs} groups={} complete_result_equals_baseline=true warmups={warmups} samples={samples} baseline_ms={baseline_ms:?} median_ms={} candidate_ms={candidate_ms:?} median_ms={} note=timing-includes-full-SQL-planning-and-factorized-selection-on-fresh-plans-for-both-arms",
        baseline_rows.len(),
        bmed[bmed.len() / 2],
        cmed[cmed.len() / 2],
    );
    Ok(())
}

/// `--count` fixture check: plans the plain COUNT SQL through `ctx.sql` on a
/// session built by `count_session`, compares every group against a plain session
/// with the same configuration, and prints both plan texts. No timing claims.
async fn run_count_case(name: &str) -> datafusion::common::Result<()> {
    let (facts, dims, logical_pairs) = fixture(name)?;
    let fact_rows = facts.num_rows();
    let dim_rows = dims.num_rows();
    let ctx = count_session(facts.clone(), dims.clone())?;
    let plan = ctx.sql(COUNT_QUERY).await?.create_physical_plan().await?;
    let factor_batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;

    let plain = plain_count_session(&facts, &dims)?;
    let baseline = plain.sql(COUNT_QUERY).await?.create_physical_plan().await?;
    let baseline_batches = collect(Arc::clone(&baseline), plain.task_ctx()).await?;

    let factor_rows = count_rows(&factor_batches)?;
    let baseline_rows = count_rows(&baseline_batches)?;
    if factor_rows != baseline_rows {
        return Err(datafusion::common::DataFusionError::Execution(format!(
            "{name}: COUNT(*) selection changed the complete grouped result"
        )));
    }
    let counted = factor_rows.iter().map(|row| row.2).sum::<i64>();
    if counted != logical_pairs as i64 {
        return Err(datafusion::common::DataFusionError::Execution(format!(
            "{name}: counted pairs {counted} differ from fixture pairs {logical_pairs}"
        )));
    }
    let factor_text = displayable(plan.as_ref()).indent(true).to_string();
    let baseline_text = displayable(baseline.as_ref()).indent(true).to_string();
    if !factor_text.contains("RawFactorExec") || !factor_text.contains("FactorAggregateExec") {
        return Err(datafusion::common::DataFusionError::Execution(format!(
            "{name}: COUNT(*) SQL did not select the factorized plan"
        )));
    }
    println!(
        "case={name} fact_rows={fact_rows} dim_rows={dim_rows} logical_pairs={logical_pairs} groups={} complete_result_equals_baseline=true\nselected plan (RawFactorExec present: {}):\n{factor_text}baseline plan (RawFactorExec present: {}):\n{baseline_text}",
        factor_rows.len(),
        factor_text.contains("RawFactorExec"),
        baseline_text.contains("RawFactorExec"),
    );
    Ok(())
}

/// Shared usage error for every unsupported invocation.
fn usage_error() -> datafusion::common::DataFusionError {
    datafusion::common::DataFusionError::Plan(
        "usage: paper-ffx-datafusion [--bench | --count | --count --bench [WARMUPS SAMPLES]]"
            .into(),
    )
}

/// Explicit `--count --bench` sample count, for short smoke runs.
fn sample_count(value: &str) -> datafusion::common::Result<usize> {
    value.parse::<usize>().map_err(|_| {
        datafusion::common::DataFusionError::Plan(format!(
            "expected a non-negative integer sample count, got {value}"
        ))
    })
}

async fn run() -> datafusion::common::Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let flags = args.iter().map(String::as_str).collect::<Vec<_>>();
    if flags.as_slice() == ["--help"] || flags.as_slice() == ["-h"] {
        println!(
            "paper-ffx-datafusion [--bench | --count | --count --bench [WARMUPS SAMPLES]]\nDefault runs bounded correctness and timing fixtures. --bench enables 3 warmups and 9 alternating samples per fixture. --count runs the COUNT(*) auto-selection check on the balanced/hot/low fixtures with complete-result comparison and no timing output. --count --bench times the original COUNT(*) SQL end-to-end (SQL planning and factorized selection included) on the balanced/hot/low/hot2048 fixtures against a plain same-config session; optional counts override the 3 warmups and 9 samples for smoke runs."
        );
        return Ok(());
    }
    if flags.first() == Some(&"--count") {
        let (warmups, samples) = match flags.as_slice() {
            [_] => {
                for case in ["balanced", "hot", "low"] {
                    run_count_case(case).await?;
                }
                return Ok(());
            }
            [_, "--bench"] => (3, 9),
            [_, "--bench", warmups, samples] => (sample_count(warmups)?, sample_count(samples)?),
            _ => return Err(usage_error()),
        };
        if samples == 0 {
            return Err(usage_error());
        }
        for case in COUNT_BENCH_CASES {
            run_count_bench_case(case, warmups, samples).await?;
        }
        return Ok(());
    }
    let (warmups, samples) = if flags.is_empty() {
        (0, 1)
    } else if flags.as_slice() == ["--bench"] {
        (3, 9)
    } else {
        return Err(usage_error());
    };
    for case in ["balanced", "hot", "low"] {
        run_case(case, warmups, samples).await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("paper-ffx-datafusion failed: {error}");
        std::process::exit(1);
    }
}
