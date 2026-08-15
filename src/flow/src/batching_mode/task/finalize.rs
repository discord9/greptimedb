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

//! Phase-2 unified catch-up finalize for a `BaseComplete` two-phase backfill
//! job.
//!
//! The finalize runs entirely inside `execution_lock` (the caller must hold
//! it) and performs ONE round of catch-up for one job:
//!
//! - **L** = `state.checkpoints()` (the durable frontier the active sink has
//!   consumed);
//! - **H** = this round's scan-open high watermark, captured once by a
//!   watermark-only probe and pre-bound into both branches as
//!   `snapshot_seqs`; and
//! - **F** = `job.frozen_watermark` (the scan-open watermark the Phase-1 Base
//!   aggregation was computed under).
//!
//! Invariants enforced by construction:
//!
//! - Base covers `seq <= F AND event_time in [start, end)`;
//! - Tail covers `seq in (F, H] AND event_time in [start, end)`;
//! - Base ∪ Tail is exactly `seq <= H AND event_time in [start, end)`; and
//! - the non-target delta covers `seq in (L, H] AND event_time NOT in
//!   [start, end)`.
//!
//! Only after BOTH branches succeed does the finalize commit: it writes the
//! singleton checkpoint row for `(epoch, H)`, advances the persisted epoch and
//! the in-memory checkpoints to H, and drops the staging table (finishing the
//! job). Any failure keeps the job `BaseComplete` for retry and never advances
//! the active checkpoint; both branch writes are idempotent primary-key
//! upserts, so a retry re-runs the same catch-up safely.
//!
//! This is an internal mechanism: the enterprise side wires the trigger
//! through its own fork; the OSS integration point only polls for a
//! `BaseComplete` job inside `execute_once_unlocked` (one job per round).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use api::v1::QueryRequest;
use common_error::ext::BoxedError;
use common_query::OutputData;
use common_query::logical_plan::breakup_insert_plan;
use common_recordbatch::util::collect_batches;
use common_telemetry::{debug, info, warn};
use common_time::Timestamp;
use datafusion::datasource::DefaultTableSource;
use datafusion_common::TableReference;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::dml::InsertOp;
use datafusion_expr::{DmlStatement, Expr, LogicalPlan, WriteOp, col, lit};
use query::QueryEngineRef;
use query::options::{
    FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE, FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE,
    FLOW_RETURN_REGION_SEQ, FLOW_SINK_TABLE_ID,
};
use query::query_engine::DefaultSerializer;
use snafu::{OptionExt, ResultExt, ensure};
use substrait::{DFLogicalSubstraitConvertor, SubstraitPlan};
use table::table::adapter::DfTableProviderAdapter;

use crate::Error;
use crate::batching_mode::frontend_client::FrontendClient;
use crate::batching_mode::state::{BackfillJob, BackfillJobStatus, to_df_literal};
use crate::batching_mode::task::{
    BatchingTask, encode_insert_plan_request, strip_internal_epoch_column,
    verify_backfill_watermark,
};
use crate::batching_mode::utils::{
    AddFilterRewriter, IncrementalMergeJoinKind, analyze_incremental_aggregate_plan,
    gen_plan_with_matching_schema, get_table_info_df_schema,
    rewrite_incremental_aggregate_with_sink_merge_kind, sql_to_df_plan,
};
use crate::df_optimizer::apply_df_optimizer;
use crate::error::{
    DatafusionSnafu, ExternalSnafu, InvalidQuerySnafu, SubstraitEncodeLogicalPlanSnafu,
    UnexpectedSnafu,
};

/// Consumes a query output so terminal metrics are finalized (streams only
/// mark their metrics ready after being fully drained).
async fn drain_output(output: OutputData) -> Result<(), Error> {
    match output {
        OutputData::AffectedRows(_) => {}
        OutputData::Stream(stream) => {
            let _ = collect_batches(stream)
                .await
                .map_err(BoxedError::new)
                .context(ExternalSnafu)?;
        }
        OutputData::RecordBatches(_) => {}
    }
    Ok(())
}

/// The probe's high watermark must cover every region that has a lower bound
/// (F or L) so both pre-bound branches can read a consistent `(., H]` range for
/// every region they scan, and H must never be older than F (sequences are
/// monotonic).
fn verify_finalize_high(
    frozen: &BTreeMap<u64, u64>,
    lower: &BTreeMap<u64, u64>,
    high: &BTreeMap<u64, u64>,
) -> Result<(), Error> {
    if high.is_empty() {
        return UnexpectedSnafu {
            reason: "backfill finalize captured an empty high watermark H".to_string(),
        }
        .fail();
    }
    for region_id in frozen.keys().chain(lower.keys()) {
        let high_seq = high.get(region_id).with_context(|| UnexpectedSnafu {
            reason: format!(
                "backfill finalize high watermark H {:?} is missing region {region_id} required by frozen watermark F {:?} or lower bound L {:?}",
                high, frozen, lower
            ),
        })?;
        if let Some(frozen_seq) = frozen.get(region_id) {
            ensure!(
                *high_seq >= *frozen_seq,
                UnexpectedSnafu {
                    reason: format!(
                        "backfill finalize high watermark {high_seq} for region {region_id} is older than the frozen watermark {frozen_seq}"
                    ),
                }
            );
        }
    }
    Ok(())
}

/// A finalize branch is pre-bound with `snapshot_seqs = H`; the storage must
/// prove it read exactly up to that pre-bound watermark for every region that
/// participated, mirroring the fenced-repair watermark proof.
fn finalize_branch_watermarks_match_high(
    participating_regions: &BTreeSet<u64>,
    watermark_map: &HashMap<u64, u64>,
    high: &BTreeMap<u64, u64>,
) -> bool {
    !participating_regions.is_empty()
        && participating_regions.len() == high.len()
        && watermark_map.len() == high.len()
        && participating_regions.iter().all(|region_id| {
            high.get(region_id)
                .zip(watermark_map.get(region_id))
                .is_some_and(|(high, watermark)| high == watermark)
        })
}

impl BatchingTask {
    /// Runs one unified catch-up finalize for a `BaseComplete` backfill job.
    ///
    /// Caller must hold `execution_lock` (see [`BatchingTask::execution_lock`]).
    /// The finalize is a two-branch replay followed by a single commit:
    ///
    /// 1. the non-target delta branch replays `(L, H]` rows whose event time is
    ///    outside `[start, end)` through the normal incremental sink merge and
    ///    writes them into the active sink;
    /// 2. the target final branch merges the staging Base with the Tail
    ///    `(F, H]` rows whose event time is inside `[start, end)` through a
    ///    FULL OUTER sink-merge rewrite whose base is the staging table, and
    ///    writes the merged result into the active sink;
    /// 3. only when both succeeded, the finalize commits the checkpoint
    ///    `(epoch, H)`, advances the persisted epoch and checkpoints, and drops
    ///    the staging table.
    ///
    /// Failure semantics: a failing branch, an unproved watermark, or a failed
    /// checkpoint-row write never advances the active checkpoint and never
    /// drops the staging table; the job stays `BaseComplete` and is retried by
    /// the next round.
    pub(crate) async fn run_backfill_finalize(
        &self,
        engine: &QueryEngineRef,
        frontend_client: &Arc<FrontendClient>,
        job_id: u64,
    ) -> Result<(), Error> {
        // 1. Take the job and its frozen watermark F under the state lock.
        let job = self.take_base_complete_job_for_finalize(job_id)?;
        let frozen = job
            .frozen_watermark
            .clone()
            .with_context(|| UnexpectedSnafu {
                reason: format!(
                    "backfill job {job_id} is BaseComplete without a frozen watermark F"
                ),
            })?;

        let lower = self.state.read().unwrap().checkpoints().clone();
        ensure!(
            !lower.is_empty(),
            UnexpectedSnafu {
                reason: "backfill finalize requires a non-empty durable checkpoint frontier L; the flow has not persisted any checkpoint yet"
                    .to_string(),
            }
        );

        // Checkpoint persistence is required to commit the catch-up: the
        // checkpoint row written by `write_checkpoint_row` is what makes the
        // advanced frontier durable across restarts.
        ensure!(
            self.state
                .read()
                .unwrap()
                .checkpoint_persistence()
                .is_some(),
            UnexpectedSnafu {
                reason: "backfill finalize requires checkpoint persistence; the flow has none"
                    .to_string(),
            }
        );

        // 2. Capture H once (watermark-only probe) and pre-bind both branches
        //    to the same H so the catch-up is consistent by construction.
        let high = self
            .capture_backfill_high_watermark(engine, frontend_client)
            .await?;
        verify_finalize_high(&frozen, &lower, &high)?;

        // 3. Non-target delta branch first, so the target branch's upsert of a
        //    group key wins for target windows (late rows with an aligned
        //    window inside the range never inflate the target value).
        let non_target_plan = self
            .build_backfill_non_target_delta_plan(engine.clone(), job.range)
            .await?;
        let (non_target_plan, epoch) = self.stamp_epoch_into_plan(non_target_plan).await?;
        let epoch = epoch.with_context(|| UnexpectedSnafu {
            reason: "backfill finalize stamped no epoch; checkpoint persistence is required"
                .to_string(),
        })?;
        self.execute_backfill_branch(frontend_client, non_target_plan, &lower, &high, &job)
            .await?;

        // 4. Target final branch: Tail (F, H] merged FULL OUTER with the
        //    staging Base, written into the active sink.
        let final_plan = self
            .build_backfill_final_delta_plan(engine.clone(), &job)
            .await?;
        let (final_plan, final_epoch) = self.stamp_epoch_into_plan(final_plan).await?;
        ensure!(
            final_epoch == Some(epoch),
            UnexpectedSnafu {
                reason: format!(
                    "backfill finalize branch epochs disagree ({epoch:?} vs {final_epoch:?}); refusing to commit"
                ),
            }
        );
        self.execute_backfill_branch(frontend_client, final_plan, &frozen, &high, &job)
            .await?;

        // 5. Commit — only after both branches proved their pre-bound H.
        if let Err(err) = self
            .write_checkpoint_row(frontend_client, epoch, &high)
            .await
        {
            warn!(
                "Flow {} backfill finalize for job {job_id} failed to persist the checkpoint row (epoch {}); falling back to full snapshot and keeping the job retryable: {:?}",
                self.config.flow_id, epoch, err
            );
            self.state.write().unwrap().mark_full_snapshot();
            return Err(err);
        }
        {
            let mut state = self.state.write().unwrap();
            state.advance_persisted_epoch(epoch);
            state.advance_checkpoints(high.iter().map(|(k, v)| (*k, *v)).collect());
        }
        self.finish_backfill_job(frontend_client, job_id).await?;
        info!(
            "Flow {} backfill job {job_id} finalized: checkpoint advanced to {} region(s) at epoch {}",
            self.config.flow_id,
            high.len(),
            epoch
        );
        Ok(())
    }

    /// Fetches the registered `BaseComplete` job for finalize. Only a
    /// `BaseComplete` job with a recorded frozen watermark F may finalize; any
    /// other state (or a missing F) fails closed.
    fn take_base_complete_job_for_finalize(&self, job_id: u64) -> Result<BackfillJob, Error> {
        let state = self.state.read().unwrap();
        let job = state
            .get_backfill_job(job_id)
            .with_context(|| UnexpectedSnafu {
                reason: format!("no registered backfill job {job_id} to finalize"),
            })?;
        ensure!(
            job.status == BackfillJobStatus::BaseComplete,
            UnexpectedSnafu {
                reason: format!(
                    "backfill job {job_id} cannot finalize: only a BaseComplete job may finalize, job is {:?}",
                    job.status
                ),
            }
        );
        ensure!(
            job.frozen_watermark
                .as_ref()
                .is_some_and(|frozen| !frozen.is_empty()),
            UnexpectedSnafu {
                reason: format!(
                    "backfill job {job_id} is BaseComplete but has no non-empty frozen watermark F; refusing to finalize"
                ),
            }
        );
        Ok(job.clone())
    }

    /// Captures this round's scan-open high watermark H with a watermark-only
    /// probe: the flow's own query executed as a plain read with
    /// `flow.return_region_seq=true` and NO preset snapshot bounds, so the
    /// storage captures the scan-open terminal watermark of every participating
    /// region. Both finalize branches are then pre-bound to this H.
    async fn capture_backfill_high_watermark(
        &self,
        engine: &QueryEngineRef,
        frontend_client: &Arc<FrontendClient>,
    ) -> Result<BTreeMap<u64, u64>, Error> {
        let query_ctx = self.state.read().unwrap().query_ctx.clone();
        let plan =
            sql_to_df_plan(query_ctx.clone(), engine.clone(), &self.config.query, false).await?;
        let (source_catalog, source_schema) = (
            query_ctx.current_catalog().to_string(),
            query_ctx.current_schema(),
        );
        // Fix all table refs to be fully qualified, same as the normal
        // execution path.
        let plan = plan
            .clone()
            .transform_down_with_subqueries(|p| {
                if let LogicalPlan::TableScan(mut table_scan) = p {
                    let resolved = table_scan
                        .table_name
                        .resolve(&source_catalog, &source_schema);
                    table_scan.table_name = resolved.into();
                    Ok(Transformed::yes(LogicalPlan::TableScan(table_scan)))
                } else {
                    Ok(Transformed::no(p))
                }
            })
            .with_context(|_| DatafusionSnafu {
                context: format!(
                    "Failed to fix table ref in backfill high-watermark probe plan:\n {}\n",
                    plan
                ),
            })?
            .data;

        let message = DFLogicalSubstraitConvertor {}
            .encode(&plan, DefaultSerializer)
            .context(SubstraitEncodeLogicalPlanSnafu)?;
        let req = QueryRequest {
            query: Some(api::v1::query_request::Query::LogicalPlan(message.to_vec())),
        };
        let extensions = [(FLOW_RETURN_REGION_SEQ, "true")];
        let extension_refs = extensions
            .iter()
            .map(|(key, value)| (*key, *value))
            .collect::<Vec<_>>();
        let mut peer_desc = None;
        let res = frontend_client
            .query_with_terminal_metrics(
                &self.config.sink_table_name[0],
                &self.config.sink_table_name[1],
                req,
                &extension_refs,
                &HashMap::new(),
                &mut peer_desc,
            )
            .await?;
        let client::OutputWithMetrics { output, metrics } = res;
        drain_output(output.data).await?;
        let watermark_map = metrics.region_watermark_map().unwrap_or_default();
        let participating_regions = metrics.participating_regions().unwrap_or_default();
        verify_backfill_watermark(&participating_regions, &watermark_map)?;
        debug!(
            "Flow {} backfill finalize captured high watermark H {:?} from the probe",
            self.config.flow_id, watermark_map
        );
        Ok(watermark_map.into_iter().collect())
    }

    /// Builds the non-target delta branch: the flow aggregate query matched to
    /// the active sink schema, with an event-time EXCLUDE filter
    /// `(win < start) OR (win >= end)` on the source scan, merged with the
    /// active sink state via the normal incremental LEFT join rewrite, wrapped
    /// in an INSERT into the active sink.
    ///
    /// The exclude filter keeps rows whose event time is inside `[start, end)`
    /// out of this branch so they are written exactly once by the target final
    /// branch.
    pub(crate) async fn build_backfill_non_target_delta_plan(
        &self,
        engine: QueryEngineRef,
        range: (Timestamp, Timestamp),
    ) -> Result<LogicalPlan, Error> {
        let col_name = self
            .config
            .time_window_expr
            .as_ref()
            .map(|expr| expr.column_name.clone())
            .with_context(|| UnexpectedSnafu {
                reason: "backfill finalize requires a time window expression".to_string(),
            })?;
        let lower = to_df_literal(range.0)?;
        let upper = to_df_literal(range.1)?;
        let exclude_filter = col(&col_name)
            .lt(lit(lower))
            .or(col(&col_name).gt_eq(lit(upper)));
        let dml = self
            .build_backfill_delta_dml(engine, exclude_filter)
            .await?;
        self.rewrite_backfill_delta_with_sink_merge(&dml, None)
            .await
    }

    /// Builds the target final branch: the flow aggregate query matched to the
    /// active sink schema, with the Tail event-time filter
    /// `win >= start AND win < end` on the source scan, merged FULL OUTER with
    /// the staging Base, wrapped in an INSERT into the active sink.
    ///
    /// The Tail filter is REQUIRED for correctness: late rows whose raw event
    /// time is outside `[start, end)` (but whose aligned window falls inside
    /// the range) would otherwise enter the Tail with the same group key as a
    /// Base group and be counted twice by the FULL OUTER merge.
    pub(crate) async fn build_backfill_final_delta_plan(
        &self,
        engine: QueryEngineRef,
        job: &BackfillJob,
    ) -> Result<LogicalPlan, Error> {
        let col_name = self
            .config
            .time_window_expr
            .as_ref()
            .map(|expr| expr.column_name.clone())
            .with_context(|| UnexpectedSnafu {
                reason: "backfill finalize requires a time window expression".to_string(),
            })?;
        let lower = to_df_literal(job.range.0)?;
        let upper = to_df_literal(job.range.1)?;
        let target_filter = col(&col_name)
            .gt_eq(lit(lower))
            .and(col(&col_name).lt(lit(upper)));
        let dml = self.build_backfill_delta_dml(engine, target_filter).await?;
        self.rewrite_backfill_delta_with_sink_merge(&dml, Some(job))
            .await
    }

    /// Builds the shared delta DML: the flow query matched against the active
    /// sink schema (epoch column stripped), a source-level event-time filter
    /// inserted below the aggregate via [`AddFilterRewriter`], the optimizer
    /// applied, and the result wrapped in an `INSERT` into the active sink.
    async fn build_backfill_delta_dml(
        &self,
        engine: QueryEngineRef,
        source_filter: Expr,
    ) -> Result<LogicalPlan, Error> {
        let (sink_table, _) = get_table_info_df_schema(
            self.config.catalog_manager.clone(),
            self.config.sink_table_name.clone(),
        )
        .await?;
        let table_meta = &sink_table.table_info().meta;
        let primary_key_indices = table_meta.primary_key_indices.clone();
        let (effective_schema, effective_pk_indices) =
            strip_internal_epoch_column(&table_meta.schema, &primary_key_indices);
        let query_ctx = self.state.read().unwrap().query_ctx.clone();

        let select_plan = gen_plan_with_matching_schema(
            &self.config.query,
            query_ctx.clone(),
            engine,
            Arc::new(effective_schema),
            &effective_pk_indices,
            true,
        )
        .await?;

        let mut add_filter = AddFilterRewriter::new(source_filter);
        let select_plan = select_plan
            .clone()
            .rewrite(&mut add_filter)
            .with_context(|_| DatafusionSnafu {
                context: format!(
                    "Failed to apply backfill delta source filter to plan:\n {}\n",
                    select_plan
                ),
            })?
            .data;
        let select_plan = apply_df_optimizer(select_plan, &query_ctx).await?;

        let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));
        let dml = LogicalPlan::Dml(DmlStatement::new(
            TableReference::Full {
                catalog: self.config.sink_table_name[0].clone().into(),
                schema: self.config.sink_table_name[1].clone().into(),
                table: self.config.sink_table_name[2].clone().into(),
            },
            table_source,
            WriteOp::Insert(InsertOp::Append),
            Arc::new(select_plan),
        ));
        Ok(dml)
    }

    /// Rewrites a backfill delta DML through the incremental aggregate
    /// sink-merge rewrite.
    ///
    /// With `job = None` the merge base is the active sink with a `Left` join
    /// (the normal incremental rewrite); with `Some(job)` the merge base is the
    /// job's staging table with a `FullOuter` join, so Base-only groups
    /// survive. A plain GROUP BY (no aggregate merge columns) merging with the
    /// active sink needs no explicit rewrite (sink primary-key upserts are
    /// idempotent), but the staging-backed FULL OUTER merge still needs the
    /// rewrite to preserve Base-only groups.
    async fn rewrite_backfill_delta_with_sink_merge(
        &self,
        dml: &LogicalPlan,
        job: Option<&BackfillJob>,
    ) -> Result<LogicalPlan, Error> {
        let LogicalPlan::Dml(dml) = dml else {
            return UnexpectedSnafu {
                reason: "backfill delta plan is not a DML insert".to_string(),
            }
            .fail();
        };
        let inner = dml.input.as_ref().clone();
        let Some(analysis) = analyze_incremental_aggregate_plan(&inner)? else {
            return UnexpectedSnafu {
                reason: format!(
                    "backfill finalize requires an aggregate flow query; the delta plan is not an incremental-mergeable aggregate:\n{}",
                    inner.display_indent()
                ),
            }
            .fail();
        };
        ensure!(
            analysis.unsupported_exprs.is_empty(),
            InvalidQuerySnafu {
                reason: format!(
                    "backfill finalize cannot merge unsupported incremental aggregate expressions: {:?}",
                    analysis.unsupported_exprs
                ),
            }
        );

        let rewritten_inner = if analysis.merge_columns.is_empty() && job.is_none() {
            inner
        } else {
            let (base_table, base_table_name, join_kind) = match job {
                Some(job) => {
                    let (staging_table, _) = get_table_info_df_schema(
                        self.config.catalog_manager.clone(),
                        job.staging_table_name.clone(),
                    )
                    .await?;
                    (
                        staging_table,
                        job.staging_table_name.clone(),
                        IncrementalMergeJoinKind::FullOuter,
                    )
                }
                None => {
                    let (sink_table, _) = get_table_info_df_schema(
                        self.config.catalog_manager.clone(),
                        self.config.sink_table_name.clone(),
                    )
                    .await?;
                    (
                        sink_table,
                        self.config.sink_table_name.clone(),
                        IncrementalMergeJoinKind::Left,
                    )
                }
            };
            rewrite_incremental_aggregate_with_sink_merge_kind(
                &inner,
                &analysis,
                base_table,
                &base_table_name,
                None,
                join_kind,
            )
            .await?
        };

        Ok(LogicalPlan::Dml(DmlStatement::new(
            dml.table_name.clone(),
            dml.target.clone(),
            dml.op.clone(),
            Arc::new(rewritten_inner),
        )))
    }

    /// Executes one pre-bound finalize branch through the frontend and proves
    /// the returned terminal watermarks match the pre-bound high watermark H.
    ///
    /// The branch runs with `FLOW_INCREMENTAL_MODE=sequence_range`,
    /// `FLOW_INCREMENTAL_AFTER_SEQS = after_seqs` (L for the non-target branch,
    /// F for the target branch) and `snapshot_seqs = H`, so the storage reads
    /// exactly `(after_seqs, H]` for every source region. The active sink and
    /// the staging table are excluded from incremental semantics via
    /// `FLOW_SINK_TABLE_ID` / `FLOW_INTERNAL_NON_SOURCE_TABLE_IDS` (plain
    /// reads).
    async fn execute_backfill_branch(
        &self,
        frontend_client: &Arc<FrontendClient>,
        plan: LogicalPlan,
        after_seqs: &BTreeMap<u64, u64>,
        high: &BTreeMap<u64, u64>,
        job: &BackfillJob,
    ) -> Result<(), Error> {
        let (source_catalog, source_schema) = {
            let ctx = self.state.read().unwrap().query_ctx.clone();
            (ctx.current_catalog().to_string(), ctx.current_schema())
        };
        // Fix all table refs to be fully qualified, same as the normal
        // execution path.
        let plan = plan
            .clone()
            .transform_down_with_subqueries(|p| {
                if let LogicalPlan::TableScan(mut table_scan) = p {
                    let resolved = table_scan
                        .table_name
                        .resolve(&source_catalog, &source_schema);
                    table_scan.table_name = resolved.into();
                    Ok(Transformed::yes(LogicalPlan::TableScan(table_scan)))
                } else {
                    Ok(Transformed::no(p))
                }
            })
            .with_context(|_| DatafusionSnafu {
                context: format!(
                    "Failed to fix table ref in backfill finalize branch plan:\n {}\n",
                    plan
                ),
            })?
            .data;

        let (insert_to, insert_input_plan) = breakup_insert_plan(
            &plan,
            &self.config.sink_table_name[0],
            &self.config.sink_table_name[1],
        )
        .with_context(|| UnexpectedSnafu {
            reason: "backfill finalize branch plan is not an INSERT into the active sink"
                .to_string(),
        })?;
        let req = encode_insert_plan_request(insert_to, &insert_input_plan)?;

        let mut extensions = self.backfill_query_extensions(job)?;
        let sink_table_id = self.sink_table_id().await?;
        extensions.push((FLOW_SINK_TABLE_ID, sink_table_id.to_string()));
        extensions.push((
            FLOW_INCREMENTAL_MODE,
            FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string(),
        ));
        extensions.push((
            FLOW_INCREMENTAL_AFTER_SEQS,
            serde_json::to_string(after_seqs).map_err(|err| {
                UnexpectedSnafu {
                    reason: format!("Failed to serialize backfill finalize after_seqs: {err}"),
                }
                .build()
            })?,
        ));
        let extension_refs = extensions
            .iter()
            .map(|(key, value)| (*key, value.as_str()))
            .collect::<Vec<_>>();

        let snapshot_seqs = high
            .iter()
            .map(|(k, v)| (*k, *v))
            .collect::<HashMap<_, _>>();
        let mut peer_desc = None;
        let res = frontend_client
            .query_with_terminal_metrics(
                &self.config.sink_table_name[0],
                &self.config.sink_table_name[1],
                req,
                &extension_refs,
                &snapshot_seqs,
                &mut peer_desc,
            )
            .await?;
        let client::OutputWithMetrics { output, metrics } = res;
        drain_output(output.data).await?;
        let watermark_map = metrics.region_watermark_map().unwrap_or_default();
        let participating_regions = metrics.participating_regions().unwrap_or_default();
        ensure!(
            finalize_branch_watermarks_match_high(&participating_regions, &watermark_map, high),
            UnexpectedSnafu {
                reason: format!(
                    "backfill finalize branch returned watermarks {:?} (participating {:?}) that do not match the pre-bound high watermark {:?}",
                    watermark_map, participating_regions, high
                ),
            }
        );
        debug!(
            "Flow {} backfill finalize branch executed with after_seqs {:?} and pre-bound high {:?}",
            self.config.flow_id, after_seqs, high
        );
        Ok(())
    }
}
