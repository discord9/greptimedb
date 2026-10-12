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

//! CorrBound stage-0 probe.
//!
//! Builds the `tests/perf/query_cases/sql_join_filter_order` fixture in an
//! in-process GreptimeDB standalone instance, plans the fixture query with the
//! real engine, and then replays the pinned DataFusion `JoinSelection` physical
//! optimizer rule on the pre-`JoinSelection` subtree:
//!
//! 1. with the subtree's original statistics (control), and
//! 2. with only the join inputs' `num_rows` replaced by the sound pre-query
//!    upper bound `|sigma_P(T)| <= |T| = 16,384`.
//!
//! If the two replays select different join modes or side assignments the probe
//! prints `REPRODUCED`; otherwise it prints `NOT REPRODUCED` and exits 0. A
//! failed assertion exits non-zero.
//!
//! The pre-`JoinSelection` subtree is obtained in two ways and both are
//! reported: the rule list with `join_selection` removed (the literal recipe),
//! and the rules that precede `join_selection` in the engine's list (the exact
//! input the rule sees at its position in the pipeline). The replay used for
//! the counterfactual is the first recipe that reproduces the engine baseline.

#![recursion_limit = "256"]

mod fixture;
mod inject;
mod plan_analysis;
mod rows;

use std::path::PathBuf;
use std::sync::Arc;
use std::{fmt, fs};

use common_query::{Output, OutputData};
use common_recordbatch::RecordBatch as GreptimeRecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::execution::context::SessionState;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::join_selection::JoinSelection;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::physical_plan::joins::HashJoinExec;
use datafusion_common::config::ConfigOptions;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::LogicalPlan;
use fixture::{DIM_TABLE, FACT_TABLE, TABLE_ROWS, build_fixture, table_statistic};
use frontend::instance::Instance;
use inject::StatisticsInjectExec;
use plan_analysis::{count_joins, join_observation, plan_text, statistics_json, statistics_of};
use query::QueryEngineContext;
use query::dist_plan::MergeScanLogicalPlan;
use query::parser::QueryStatement;
use serde_json::{Value, json};
use session::context::{QueryContext, QueryContextRef};
use sql::dialect::GreptimeDbDialect;
use sql::parser::{ParseOptions, ParserContext};
use sql::statements::statement::Statement;
use tests_integration::standalone::GreptimeDbStandaloneBuilder;

/// DataFusion fork revision this experiment is pinned to.
const PINNED_DATAFUSION_REV: &str = "2aa87d52cdce7006af492330064738f33ed294c1";
/// DataFusion crate version expected at that revision.
const PINNED_DATAFUSION_VERSION: &str = "55.1.0";
/// The physical optimizer rule that decides the join shape.
const JOIN_SELECTION_RULE: &str = "join_selection";

type RuleList = Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>;

/// The fixture query, verbatim from
/// `tests/perf/query_cases/sql_join_filter_order/case.toml`.
const FIXTURE_QUERY: &str = "SELECT f.host, d.zone, avg(f.value * d.weight) AS score \
     FROM sql_join_fact f JOIN sql_join_dim d ON f.host = d.host \
     WHERE f.ts >= '2024-01-01 00:00:00+0000'::TIMESTAMP \
     AND f.ts < '2024-01-01 04:00:00+0000'::TIMESTAMP \
     AND d.ts >= '2024-01-01 00:00:00+0000'::TIMESTAMP \
     AND d.ts < '2024-01-01 04:00:00+0000'::TIMESTAMP \
     AND d.zone IN ('zone1', 'zone2') \
     GROUP BY f.host, d.zone ORDER BY score DESC LIMIT 10";

/// Absolute/relative tolerance for `avg` comparisons.
const ABS_TOL: f64 = 1e-9;
const REL_TOL: f64 = 1e-12;

#[derive(Debug)]
pub enum ProbeError {
    Fatal(String),
}

impl fmt::Display for ProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProbeError::Fatal(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for ProbeError {}

type ProbeResult<T> = Result<T, ProbeError>;

impl From<datafusion_common::DataFusionError> for ProbeError {
    fn from(error: datafusion_common::DataFusionError) -> Self {
        ProbeError::Fatal(format!("datafusion: {error}"))
    }
}

#[derive(Debug, Clone)]
struct Check {
    name: &'static str,
    passed: bool,
    detail: String,
}

#[derive(Debug, Default)]
struct Checks {
    checks: Vec<Check>,
}

impl Checks {
    fn record(&mut self, name: &'static str, passed: bool, detail: impl Into<String>) {
        let detail = detail.into();
        println!(
            "[{}] {name}: {detail}",
            if passed { "PASS" } else { "FAIL" }
        );
        self.checks.push(Check {
            name,
            passed,
            detail,
        });
    }

    fn all_passed(&self) -> bool {
        self.checks.iter().all(|check| check.passed)
    }

    fn to_json(&self) -> Value {
        Value::Array(
            self.checks
                .iter()
                .map(|check| {
                    json!({
                        "name": check.name,
                        "passed": check.passed,
                        "detail": check.detail,
                    })
                })
                .collect(),
        )
    }
}

/// Parses a single SQL statement with the GreptimeDB dialect.
fn parse_statement(sql: &str) -> ProbeResult<Statement> {
    let dialect = GreptimeDbDialect {};
    let mut statements = ParserContext::create_with_dialect(sql, &dialect, ParseOptions::default())
        .map_err(|e| ProbeError::Fatal(format!("parse `{sql}`: {e}")))?;
    if statements.len() != 1 {
        return Err(ProbeError::Fatal(format!(
            "expected exactly one statement in `{sql}`"
        )));
    }
    Ok(statements.remove(0))
}

/// Engine path: analyzer -> logical optimizer -> physical planner. The physical
/// planner itself applies the session's physical optimizer rules, mirroring
/// `src/query/src/datafusion.rs:485-501`.
async fn plan_with_state(
    state: &SessionState,
    logical_plan: LogicalPlan,
) -> ProbeResult<Arc<dyn ExecutionPlan>> {
    let analyzed =
        state
            .analyzer()
            .execute_and_check(logical_plan, state.config_options(), |_, _| {})?;
    // The engine skips the logical optimizer for a plan the distributed planner
    // already rewrote into a `MergeScanLogicalPlan` extension node.
    let optimized = if let LogicalPlan::Extension(extension) = &analyzed
        && extension.node.name() == MergeScanLogicalPlan::name()
    {
        analyzed
    } else {
        state.optimizer().optimize(analyzed, state, |_, _| {})?
    };
    let physical = state
        .query_planner()
        .create_physical_plan(&optimized, state)
        .await?;
    Ok(physical)
}

/// A session state whose physical rule list is replaced by `rules`.
fn state_with_rules(state: &SessionState, rules: RuleList) -> SessionState {
    SessionStateBuilder::new_from_existing(state.clone())
        .with_physical_optimizer_rules(rules)
        .build()
}

fn join_selection_index(
    rules: &[Arc<dyn PhysicalOptimizerRule + Send + Sync>],
) -> ProbeResult<usize> {
    rules
        .iter()
        .position(|rule| rule.name() == JOIN_SELECTION_RULE)
        .ok_or_else(|| {
            ProbeError::Fatal(format!(
                "the engine's physical optimizer list has no `{JOIN_SELECTION_RULE}` rule"
            ))
        })
}

/// Applies physical optimizer rules in order, the way the planner's
/// `optimize_physical_plan` does.
fn apply_rules(
    mut plan: Arc<dyn ExecutionPlan>,
    rules: &[Arc<dyn PhysicalOptimizerRule + Send + Sync>],
    config: &ConfigOptions,
) -> ProbeResult<Arc<dyn ExecutionPlan>> {
    for rule in rules {
        plan = rule
            .optimize(plan, config)
            .map_err(|e| ProbeError::Fatal(format!("rule {} failed: {e}", rule.name())))?;
    }
    Ok(plan)
}

/// Runs the pinned `JoinSelection` rule once, exactly as the physical optimizer
/// would.
fn apply_join_selection(
    plan: Arc<dyn ExecutionPlan>,
    config: &ConfigOptions,
) -> ProbeResult<Arc<dyn ExecutionPlan>> {
    Ok(JoinSelection::new().optimize(plan, config)?)
}

struct InjectedVariant {
    plan: Arc<dyn ExecutionPlan>,
    wrapper_left: Arc<StatisticsInjectExec>,
    wrapper_right: Arc<StatisticsInjectExec>,
}

/// Rebuilds `plan` with the hash join's inputs wrapped in
/// [`StatisticsInjectExec`], which reports `Inexact(bound)` rows for them.
fn inject_into_join_inputs(
    plan: Arc<dyn ExecutionPlan>,
    bound: usize,
) -> ProbeResult<InjectedVariant> {
    let mut wrapper_left: Option<Arc<StatisticsInjectExec>> = None;
    let mut wrapper_right: Option<Arc<StatisticsInjectExec>> = None;
    let plan = plan
        .transform_down(
            |node: Arc<dyn ExecutionPlan>| -> datafusion_common::Result<
                Transformed<Arc<dyn ExecutionPlan>>,
            > {
                if wrapper_left.is_some() {
                    return Ok(Transformed::no(node));
                }
                if let Some(join) = node.downcast_ref::<HashJoinExec>() {
                    let left = Arc::new(StatisticsInjectExec::new(Arc::clone(join.left()), bound));
                    let right =
                        Arc::new(StatisticsInjectExec::new(Arc::clone(join.right()), bound));
                    let new_join = replace_children(
                        Arc::clone(&node),
                        vec![
                            Arc::clone(&left) as Arc<dyn ExecutionPlan>,
                            Arc::clone(&right) as Arc<dyn ExecutionPlan>,
                        ],
                    )?;
                    wrapper_left = Some(left);
                    wrapper_right = Some(right);
                    return Ok(Transformed::yes(new_join));
                }
                Ok(Transformed::no(node))
            },
        )
        .map_err(ProbeError::from)?
        .data;
    match (wrapper_left, wrapper_right) {
        (Some(wrapper_left), Some(wrapper_right)) => Ok(InjectedVariant {
            plan,
            wrapper_left,
            wrapper_right,
        }),
        _ => Err(ProbeError::Fatal(
            "no hash join found to inject statistics into".to_string(),
        )),
    }
}

/// Structural signature of a plan: node names plus output schema, with
/// [`StatisticsInjectExec`] treated as transparent.
fn plan_signature(plan: &Arc<dyn ExecutionPlan>) -> Vec<String> {
    let mut signature = Vec::new();
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if node.downcast_ref::<StatisticsInjectExec>().is_some() {
            for child in node.children() {
                stack.push(Arc::clone(child));
            }
            continue;
        }
        let fields = node
            .schema()
            .fields()
            .iter()
            .map(|field| format!("{}:{}", field.name(), field.data_type()))
            .collect::<Vec<_>>()
            .join(",");
        signature.push(format!("{}[{fields}]", node.name()));
        for child in node.children() {
            stack.push(Arc::clone(child));
        }
    }
    signature
}

/// Collects an [`Output`] into Arrow record batches.
async fn output_batches(output: Output) -> ProbeResult<Vec<arrow::record_batch::RecordBatch>> {
    match output.data {
        OutputData::RecordBatches(batches) => Ok(batches
            .iter()
            .map(|batch| batch.df_record_batch().clone())
            .collect()),
        OutputData::Stream(stream) => {
            let batches = common_recordbatch::util::collect(stream)
                .await
                .map_err(|e| ProbeError::Fatal(format!("collect stream: {e}")))?;
            Ok(batches
                .into_iter()
                .map(GreptimeRecordBatch::into_df_record_batch)
                .collect())
        }
        OutputData::AffectedRows(rows) => Err(ProbeError::Fatal(format!(
            "query returned affected rows ({rows}) instead of a result set"
        ))),
    }
}

/// Runs `sql` through the engine's normal statement path.
async fn run_sql(
    instance: &Instance,
    query_ctx: &QueryContextRef,
    sql: &str,
) -> ProbeResult<Output> {
    let statement = parse_statement(sql)?;
    instance
        .statement_executor()
        .execute_sql(statement, query_ctx.clone())
        .await
        .map_err(|e| ProbeError::Fatal(format!("execute `{sql}`: {e}")))
}

/// Runs `sql` and returns its pretty-printed textual output.
async fn run_sql_text(
    instance: &Instance,
    query_ctx: &QueryContextRef,
    sql: &str,
) -> ProbeResult<String> {
    let batches = output_batches(run_sql(instance, query_ctx, sql).await?).await?;
    arrow::util::pretty::pretty_format_batches(&batches)
        .map(|table| table.to_string())
        .map_err(|e| ProbeError::Fatal(format!("format `{sql}` output: {e}")))
}

fn config_snapshot(config: &ConfigOptions, state: &SessionState) -> Value {
    json!({
        "target_partitions": config.execution.target_partitions,
        "collect_statistics": config.execution.collect_statistics,
        "optimizer_join_reordering": config.optimizer.join_reordering,
        "optimizer_use_statistics_registry": config.optimizer.use_statistics_registry,
        "hash_join_single_partition_threshold": config.optimizer.hash_join_single_partition_threshold,
        "hash_join_single_partition_threshold_rows": config.optimizer.hash_join_single_partition_threshold_rows,
        "repartition_joins": config.optimizer.repartition_joins,
        "physical_optimizer_rules": state
            .physical_optimizers()
            .iter()
            .map(|rule| rule.name().to_string())
            .collect::<Vec<_>>(),
    })
}

/// Verifies the dependency pin against the lock file that produced this binary.
fn verify_pin(checks: &mut Checks) -> Value {
    let lock_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock");
    let lock = fs::read_to_string(&lock_path).unwrap_or_default();
    let rev_occurrences = lock.matches(PINNED_DATAFUSION_REV).count();
    checks.record(
        "pinned_datafusion_rev",
        rev_occurrences > 0,
        format!(
            "{rev_occurrences} occurrence(s) of rev {PINNED_DATAFUSION_REV} in {}",
            lock_path.display()
        ),
    );
    let version_ok = datafusion::DATAFUSION_VERSION == PINNED_DATAFUSION_VERSION;
    checks.record(
        "pinned_datafusion_version",
        version_ok,
        format!(
            "runtime datafusion::DATAFUSION_VERSION = {} (expected {})",
            datafusion::DATAFUSION_VERSION,
            PINNED_DATAFUSION_VERSION
        ),
    );
    json!({
        "expected_rev": PINNED_DATAFUSION_REV,
        "expected_version": PINNED_DATAFUSION_VERSION,
        "lockfile_rev_occurrences": rev_occurrences,
        "lockfile": lock_path.display().to_string(),
        "runtime_version": datafusion::DATAFUSION_VERSION,
        "git_rev_verifiable_at_runtime": false,
        "note": "the fork rev is asserted through the Cargo.lock consumed by the build; \
                 datafusion does not expose its git revision at runtime",
    })
}

/// One replay recipe: a session state whose physical rule list stops before (or
/// excludes) `join_selection`, plus the decisions the pinned rule produces from
/// the plan it yields.
struct Replay {
    label: &'static str,
    /// The pre-`JoinSelection` subtree.
    subtree: Arc<dyn ExecutionPlan>,
    /// The join inputs of `subtree`, as `JoinSelection` sees them.
    subtree_left: Arc<dyn ExecutionPlan>,
    subtree_right: Arc<dyn ExecutionPlan>,
    subtree_join: plan_analysis::JoinDecision,
    /// Plan produced by replaying `JoinSelection` with the original statistics.
    original_plan: Arc<dyn ExecutionPlan>,
    /// Decision after replaying `JoinSelection` with the original statistics.
    original: plan_analysis::JoinDecision,
    /// Plan produced by replaying `JoinSelection` with the injected bound.
    injected_plan: Arc<dyn ExecutionPlan>,
    /// Decision after replaying `JoinSelection` with the injected bound.
    injected: plan_analysis::JoinDecision,
    injected_variant: InjectedVariant,
    replays_baseline: bool,
    diff: Vec<String>,
}

impl Replay {
    fn to_json(&self) -> Value {
        json!({
            "recipe": self.label,
            "subtree_join": self.subtree_join.to_json(),
            "replay_original_statistics_join": self.original.to_json(),
            "replay_injected_statistics_join": self.injected.to_json(),
            "reproduces_baseline": self.replays_baseline,
            "baseline_diff": self.original.diff(&self.subtree_join),
            "counterfactual_diff": self.diff,
            "subtree_hash_joins": count_joins(&self.subtree),
            "subtree_join_child_nodes": self.subtree_join.child_nodes(),
            "subtree_join_input_statistics": {
                "left": statistics_json(&self.subtree_left),
                "right": statistics_json(&self.subtree_right),
            },
            "injected_bound": self.injected_variant.wrapper_left.injected_rows(),
            "subtree_plan": plan_text(&self.subtree),
        })
    }
}

async fn run_replay(
    label: &'static str,
    state: &SessionState,
    logical_plan: LogicalPlan,
    config: &ConfigOptions,
    baseline: &plan_analysis::JoinDecision,
    bound: usize,
) -> ProbeResult<Replay> {
    let subtree = plan_with_state(state, logical_plan).await?;
    let subtree_observation = join_observation(&subtree)
        .ok_or_else(|| ProbeError::Fatal(format!("recipe {label}: subtree has no hash join")))?;
    let subtree_join = subtree_observation.decision.clone();

    let original_plan = apply_join_selection(Arc::clone(&subtree), config)?;
    let original = join_observation(&original_plan)
        .ok_or_else(|| ProbeError::Fatal(format!("recipe {label}: replay-1 has no hash join")))?
        .decision;

    let injected_variant = inject_into_join_inputs(Arc::clone(&subtree), bound)?;
    let injected_plan = apply_join_selection(Arc::clone(&injected_variant.plan), config)?;
    let injected = join_observation(&injected_plan)
        .ok_or_else(|| ProbeError::Fatal(format!("recipe {label}: replay-2 has no hash join")))?
        .decision;

    println!(
        "---- recipe `{label}` pre-JoinSelection plan ----\n{}",
        plan_text(&subtree)
    );
    println!(
        "recipe `{label}` replay-1 (original statistics): {:?}",
        original
    );
    println!("recipe `{label}` replay-2 (num_rows=Inexact({bound})): {injected:?}");

    Ok(Replay {
        label,
        replays_baseline: original == *baseline,
        diff: original.diff(&injected),
        original_plan,
        injected_plan,
        subtree,
        subtree_left: subtree_observation.left,
        subtree_right: subtree_observation.right,
        subtree_join,
        original,
        injected,
        injected_variant,
    })
}

#[tokio::main(flavor = "multi_thread")]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("corrbound-stage0 fatal error: {error}");
        std::process::exit(2);
    }
}

async fn run() -> ProbeResult<()> {
    let mut checks = Checks::default();
    let pin = verify_pin(&mut checks);

    println!("== corrbound-stage0 probe ==");
    println!("fixture query: {FIXTURE_QUERY}");

    // ---------------------------------------------------------------- fixture
    let standalone = GreptimeDbStandaloneBuilder::new("corrbound_stage0")
        .build()
        .await;
    let instance: &Instance = standalone.fe_instance();
    let engine = &standalone.mito_engine;
    let query_ctx = QueryContext::arc();

    let fixture = build_fixture(instance, engine, &query_ctx).await?;
    println!(
        "fixture built: {} statements, fact rows={}, dim rows={}, regions={}/{}",
        fixture.statement_count,
        fixture.fact_statistic.num_rows,
        fixture.dim_statistic.num_rows,
        fixture.fact_statistic.regions,
        fixture.dim_statistic.regions
    );

    // Pre-query cardinality witnesses: construction counts and region
    // statistics, both obtained without executing any query.
    let fact_stats = table_statistic(instance, engine, &query_ctx, FACT_TABLE).await?;
    let dim_stats = table_statistic(instance, engine, &query_ctx, DIM_TABLE).await?;
    checks.record(
        "unfiltered_inputs_are_table_cardinality",
        fact_stats.num_rows == TABLE_ROWS as u64 && dim_stats.num_rows == TABLE_ROWS as u64,
        format!(
            "region statistics before any query: {FACT_TABLE} num_rows={} (regions={}, ssts={}), \
             {DIM_TABLE} num_rows={} (regions={}, ssts={}); expected {TABLE_ROWS} each",
            fact_stats.num_rows,
            fact_stats.regions,
            fact_stats.sst_num,
            dim_stats.num_rows,
            dim_stats.regions,
            dim_stats.sst_num
        ),
    );
    checks.record(
        "pre_query_upper_bound_source_exists",
        fixture.fact_constructed_rows == TABLE_ROWS
            && fixture.dim_constructed_rows == TABLE_ROWS
            && fact_stats.num_rows == TABLE_ROWS as u64
            && dim_stats.num_rows == TABLE_ROWS as u64,
        format!(
            "bound source = fixture construction ({}, {}) and mito region statistics ({}, {}); \
             |sigma_P(T)| <= |T| = {TABLE_ROWS}",
            fixture.fact_constructed_rows,
            fixture.dim_constructed_rows,
            fact_stats.num_rows,
            dim_stats.num_rows
        ),
    );

    // ------------------------------------------------------- observation A
    let engine_ctx: QueryEngineContext = instance.query_engine().engine_context(query_ctx.clone());
    let base_state = engine_ctx.state().clone();
    let config: ConfigOptions = (**base_state.config_options()).clone();

    let explain = run_sql_text(instance, &query_ctx, &format!("EXPLAIN {FIXTURE_QUERY}")).await?;
    // `EXPLAIN VERBOSE` is optional: if the engine rejects the syntax, record
    // the error text instead of failing the probe.
    let explain_verbose = match run_sql_text(
        instance,
        &query_ctx,
        &format!("EXPLAIN VERBOSE {FIXTURE_QUERY}"),
    )
    .await
    {
        Ok(text) => text,
        Err(error) => format!("EXPLAIN VERBOSE unavailable: {error}"),
    };
    println!("---- EXPLAIN ----\n{explain}\n---- EXPLAIN VERBOSE ----\n{explain_verbose}");

    let statement = QueryStatement::Sql(parse_statement(FIXTURE_QUERY)?);
    let logical_plan = instance
        .statement_executor()
        .plan(&statement, query_ctx.clone())
        .await
        .map_err(|e| ProbeError::Fatal(format!("plan fixture query: {e}")))?;
    println!("---- optimized logical plan ----\n{logical_plan}");

    let baseline_plan = plan_with_state(&base_state, logical_plan.clone()).await?;
    let baseline = join_observation(&baseline_plan)
        .ok_or_else(|| ProbeError::Fatal("baseline plan has no hash join".to_string()))?;
    println!(
        "---- baseline physical plan ----\n{}",
        plan_text(&baseline_plan)
    );
    println!("baseline join decision: {:?}", baseline.decision);

    // ------------------------------------------------------- observation B
    let rules = base_state.physical_optimizers().to_vec();
    let join_index = join_selection_index(&rules)?;
    println!(
        "rule list: {} rules, `{JOIN_SELECTION_RULE}` at index {join_index}",
        rules.len()
    );
    // Rules that run after `join_selection` in the engine's list. Replaying the
    // counterfactual means continuing this part of the pipeline from the plan
    // that the counterfactual `JoinSelection` produces.
    let post_selection_rules = rules[join_index + 1..].to_vec();

    // Recipe 1: the rules that precede `join_selection`; this is exactly what
    // the rule sees at its position in the pipeline.
    let prefix_state = state_with_rules(&base_state, rules[..join_index].to_vec());
    // Recipe 2: the whole list with `join_selection` removed (the literal
    // "remove the rule" recipe from the approved design).
    let suffix_state = state_with_rules(
        &base_state,
        rules
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != join_index)
            .map(|(_, rule)| Arc::clone(rule))
            .collect::<Vec<_>>(),
    );

    let mut replays = vec![
        run_replay(
            "rules_before_join_selection",
            &prefix_state,
            logical_plan.clone(),
            &config,
            &baseline.decision,
            TABLE_ROWS,
        )
        .await?,
        run_replay(
            "all_rules_minus_join_selection",
            &suffix_state,
            logical_plan.clone(),
            &config,
            &baseline.decision,
            TABLE_ROWS,
        )
        .await?,
    ];

    // ---------------------------------------------------- assertion (3)
    let reproducing = replays
        .iter()
        .filter(|replay| replay.replays_baseline)
        .count();
    checks.record(
        "replay_original_statistics_reproduces_baseline",
        reproducing > 0,
        format!(
            "baseline={:?}; recipes reproducing it: {} of {} ({})",
            baseline.decision,
            reproducing,
            replays.len(),
            replays
                .iter()
                .map(|replay| format!(
                    "{}={} (replay-1={:?})",
                    replay.label, replay.replays_baseline, replay.original
                ))
                .collect::<Vec<_>>()
                .join("; ")
        ),
    );

    // The replay used for the counterfactual: the exact pre-`JoinSelection`
    // subtree if it reproduces the baseline, otherwise the recipe that does.
    let chosen_index = replays
        .iter()
        .position(|replay| replay.replays_baseline)
        .unwrap_or(0);
    let chosen = replays.remove(chosen_index);
    let chosen_label = chosen.label;
    let chosen_subtree = Arc::clone(&chosen.subtree);
    let chosen_left = Arc::clone(&chosen.subtree_left);
    let chosen_right = Arc::clone(&chosen.subtree_right);

    // ---------------------------------------------------- assertion (4)
    let signature_before = plan_signature(&chosen_subtree);
    let signature_after = plan_signature(&chosen.injected_variant.plan);
    let same_inner_left = Arc::ptr_eq(chosen.injected_variant.wrapper_left.inner(), &chosen_left);
    let same_inner_right =
        Arc::ptr_eq(chosen.injected_variant.wrapper_right.inner(), &chosen_right);
    let left_before = statistics_of(chosen_left.as_ref())?;
    let left_after = statistics_of(chosen.injected_variant.wrapper_left.as_ref())?;
    let right_before = statistics_of(chosen_right.as_ref())?;
    let right_after = statistics_of(chosen.injected_variant.wrapper_right.as_ref())?;
    checks.record(
        "variants_differ_only_in_injected_num_rows",
        signature_before == signature_after
            && same_inner_left
            && same_inner_right
            && left_after.total_byte_size == left_before.total_byte_size
            && right_after.total_byte_size == right_before.total_byte_size
            && left_after.column_statistics == left_before.column_statistics
            && right_after.column_statistics == right_before.column_statistics
            && left_after.num_rows == Precision::Inexact(TABLE_ROWS)
            && right_after.num_rows == Precision::Inexact(TABLE_ROWS),
        format!(
            "recipe `{chosen_label}`: structural signatures equal ({} nodes), wrapper inner plans \
             are the same objects (left={same_inner_left}, right={same_inner_right}); left num_rows \
             {:?} -> {:?}, right num_rows {:?} -> {:?}; total_byte_size {:?}/{:?} -> {:?}/{:?}; \
             column statistics unchanged",
            signature_before.len(),
            left_before.num_rows,
            left_after.num_rows,
            right_before.num_rows,
            right_after.num_rows,
            left_before.total_byte_size,
            right_before.total_byte_size,
            left_after.total_byte_size,
            right_after.total_byte_size,
        ),
    );

    // ---------------------------------------------------- assertion (5)
    let diff = chosen.diff.clone();
    let reproduced = !diff.is_empty();
    checks.record(
        "counterfactual_changes_join_decision",
        true,
        if reproduced {
            format!(
                "recipe `{chosen_label}` decision changed: {}",
                diff.join("; ")
            )
        } else {
            format!(
                "recipe `{chosen_label}`: no change in join mode/side/type; reported as \
                 NOT REPRODUCED"
            )
        },
    );

    // ---------------------------------------------------- assertion (6)
    let baseline_rows = output_batches(run_sql(instance, &query_ctx, FIXTURE_QUERY).await?).await?;
    let baseline_rows = rows::extract_rows(&baseline_rows).map_err(ProbeError::Fatal)?;
    let task_ctx = engine_ctx.build_task_ctx();
    let replay2_plan = Arc::clone(&chosen.injected_plan);
    // The isolated `JoinSelection` output is not executable: it still carries the
    // pre-existing `OutputRequirementExec`, which only
    // `OutputRequirements::new_remove_mode()` (a rule that runs after
    // `join_selection`) removes, and it lacks the distribution enforcement that
    // `EnsureRequirements` adds. Continuing the engine's rule list from
    // `join_selection` onwards yields the plan the engine would have executed
    // for that join decision.
    let executable_plan = apply_rules(Arc::clone(&replay2_plan), &post_selection_rules, &config)?;
    let executed_join = join_observation(&executable_plan)
        .ok_or_else(|| ProbeError::Fatal("executable replay-2 plan has no hash join".to_string()))?
        .decision;
    // The same continuation applied to the control plan must reproduce the
    // engine's baseline plan exactly.
    let control_executable = apply_rules(
        Arc::clone(&chosen.original_plan),
        &post_selection_rules,
        &config,
    )?;
    let control_join = join_observation(&control_executable)
        .ok_or_else(|| ProbeError::Fatal("control plan has no hash join".to_string()))?
        .decision;
    let control_plan_text = plan_text(&control_executable);
    let baseline_plan_text = plan_text(&baseline_plan);
    checks.record(
        "post_selection_pipeline_preserves_join_decisions",
        executed_join == chosen.injected && control_join == baseline.decision,
        format!(
            "with the rules after `{JOIN_SELECTION_RULE}` replayed: control = {:?} (baseline {:?}), \
             counterfactual = {:?} (isolated `{JOIN_SELECTION_RULE}` produced {:?})",
            control_join, baseline.decision, executed_join, chosen.injected
        ),
    );
    checks.record(
        "control_replay_reproduces_baseline_plan_text",
        control_plan_text == baseline_plan_text,
        if control_plan_text == baseline_plan_text {
            "replaying the full engine rule list from the pre-JoinSelection subtree reproduces the \
             baseline physical plan text"
                .to_string()
        } else {
            format!(
                "baseline and control plan text differ;\n---- baseline ----\n{baseline_plan_text}\n\
                 ---- control ----\n{control_plan_text}"
            )
        },
    );
    let replay2_batches =
        datafusion::physical_plan::collect(Arc::clone(&executable_plan), Arc::clone(&task_ctx))
            .await
            .map_err(|e| ProbeError::Fatal(format!("execute replay-2 plan: {e}")))?;
    let executed_plan_note = "replay-2 plan (isolated JoinSelection output) plus the engine's rules from \
         `join_selection` onwards (EnsureRequirements, OutputRequirements::remove_mode, ...); \
         the join decision is unchanged by that continuation";
    println!(
        "---- executed replay-2 plan ----\n{}",
        plan_text(&executable_plan)
    );
    let replay2_rows = rows::extract_rows(&replay2_batches).map_err(ProbeError::Fatal)?;
    let (same_rows, detail) =
        rows::compare_results(&baseline_rows, &replay2_rows, ABS_TOL, REL_TOL);
    checks.record(
        "results_identical_between_baseline_and_replanned_variant",
        same_rows,
        format!(
            "baseline {} rows vs replay-2 plan {} rows ({executed_plan_note}): {detail}",
            baseline_rows.len(),
            replay2_rows.len()
        ),
    );

    // ------------------------------------------------------------- report
    let outcome = if reproduced {
        "REPRODUCED"
    } else {
        "NOT REPRODUCED"
    };
    let pre_stats = json!({
        "left": statistics_json(&chosen_left),
        "right": statistics_json(&chosen_right),
    });
    let report = json!({
        "experiment": "corrbound-stage0",
        "outcome": outcome,
        "fixture_query": FIXTURE_QUERY,
        "dependency_pin": pin,
        "config_snapshot": config_snapshot(&config, &base_state),
        "fixture": {
            "fact_table": FACT_TABLE,
            "dim_table": DIM_TABLE,
            "fact_constructed_rows": fixture.fact_constructed_rows,
            "dim_constructed_rows": fixture.dim_constructed_rows,
            "fact_region_statistics": {
                "table_id": fact_stats.table_id,
                "num_rows": fact_stats.num_rows,
                "regions": fact_stats.regions,
                "sst_num": fact_stats.sst_num,
                "sst_bytes": fact_stats.sst_bytes,
            },
            "dim_region_statistics": {
                "table_id": dim_stats.table_id,
                "num_rows": dim_stats.num_rows,
                "regions": dim_stats.regions,
                "sst_num": dim_stats.sst_num,
                "sst_bytes": dim_stats.sst_bytes,
            },
            "injected_num_rows": TABLE_ROWS,
            "statements_executed": fixture.statement_count,
        },
        "explain": explain,
        "explain_verbose": explain_verbose,
        "baseline_join": baseline.decision.to_json(),
        "baseline_physical_plan": plan_text(&baseline_plan),
        "join_selection_rule_index": join_index,
        "rule_count": rules.len(),
        "replay_recipe_used": chosen_label,
        "replays": replays.iter().map(Replay::to_json).collect::<Vec<_>>(),
        "chosen_recipe_replay": chosen.to_json(),
        "pre_selection_join_input_statistics": pre_stats,
        "counterfactual_diff": diff,
        "executed_replanned_variant": executed_plan_note,
        "executed_replanned_plan": plan_text(&executable_plan),
        "executed_replanned_join": executed_join.to_json(),
        "post_selection_rules": post_selection_rules
            .iter()
            .map(|rule| rule.name().to_string())
            .collect::<Vec<_>>(),
        "executed_replanned_plan": plan_text(&executable_plan),
        "executed_replanned_join": executed_join.to_json(),
        "baseline_result_rows": baseline_rows.iter().map(|row| row.raw.clone()).collect::<Vec<_>>(),
        "replay2_result_rows": replay2_rows.iter().map(|row| row.raw.clone()).collect::<Vec<_>>(),
        "baseline_result_row_count": baseline_rows.len(),
        "replay2_result_row_count": replay2_rows.len(),
        "checks": checks.to_json(),
    });

    let evidence_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("evidence");
    fs::create_dir_all(&evidence_dir)
        .map_err(|e| ProbeError::Fatal(format!("create {}: {e}", evidence_dir.display())))?;
    let evidence_path = evidence_dir.join("stage0-evidence.json");
    let report_json = serde_json::to_string_pretty(&report).unwrap_or_default();
    fs::write(&evidence_path, &report_json)
        .map_err(|e| ProbeError::Fatal(format!("write {}: {e}", evidence_path.display())))?;
    println!("evidence written to {}", evidence_path.display());
    println!("\n== structured summary (JSON) ==\n{report_json}");

    println!("\n== assertion summary ==");
    for check in &checks.checks {
        println!(
            "[{}] {}: {}",
            if check.passed { "PASS" } else { "FAIL" },
            check.name,
            check.detail
        );
    }
    println!("\nOUTCOME: {outcome}");
    if reproduced {
        println!(
            "counterfactual decision change (recipe `{chosen_label}`): {}",
            diff.join("; ")
        );
    } else {
        println!(
            "observed instead (recipe `{chosen_label}`): replay-1 = {:?}, replay-2 = {:?}",
            chosen.original, chosen.injected
        );
    }
    println!("baseline join decision: {:?}", baseline.decision);

    if !checks.all_passed() {
        eprintln!("\nsome assertions FAILED");
        std::process::exit(1);
    }
    Ok(())
}

/// Replaces a node's children with `ReplaceChildrenOptions::Recompute`, the
/// same path `ExecutionPlan::with_new_children` takes.
fn replace_children(
    plan: Arc<dyn ExecutionPlan>,
    children: Vec<Arc<dyn ExecutionPlan>>,
) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
    plan.replace_children(
        children,
        ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
    )
}
