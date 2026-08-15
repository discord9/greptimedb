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

//! Batching mode task state, which changes frequently
//!

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use common_telemetry::debug;
use common_time::Timestamp;
use datatypes::value::Value;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use table::metadata::TableId;
use tokio::sync::oneshot;
use tokio::time::Instant;

use crate::batching_mode::task::BatchingTask;
use crate::batching_mode::time_window::TimeWindowExpr;
use crate::error::{DatatypesSnafu, InternalSnafu, TimeSnafu, UnexpectedSnafu};
use crate::metrics::{
    METRIC_FLOW_BATCHING_ENGINE_QUERY_WINDOW_CNT, METRIC_FLOW_BATCHING_ENGINE_QUERY_WINDOW_SIZE,
    METRIC_FLOW_BATCHING_ENGINE_STALLED_WINDOW_SIZE,
};
use crate::{Error, FlowId};

/// The state of the [`BatchingTask`].
#[derive(Debug)]
pub struct TaskState {
    /// Query context
    pub(crate) query_ctx: QueryContextRef,
    /// last query complete time
    last_update_time: Instant,
    /// last time query duration
    last_query_duration: Duration,
    /// Last successful execution time in unix timestamp milliseconds.
    last_exec_time_millis: Option<i64>,
    /// First execution time in unix timestamp milliseconds, set once.
    start_time_millis: Option<i64>,
    /// Dirty Time windows need to be updated
    /// mapping of `start -> end` and non-overlapping
    pub(crate) dirty_time_windows: DirtyTimeWindows,
    checkpoint_mode: CheckpointMode,
    pending_fenced_repair: Option<FencedRepair>,
    /// Region id -> last consumed watermark sequence. Incremental scans use
    /// this as the next lower sequence bound for each source region.
    checkpoints: BTreeMap<u64, u64>,
    /// Once set, the task will never attempt incremental mode again.
    /// Set when the flow's query shape is deterministically incompatible
    /// with incremental execution (e.g. unsupported aggregate expressions).
    incremental_disabled: bool,
    /// Checkpoint persistence layout, activated only when the batching mode is
    /// `SequenceRange` and the sink schema contains the reserved internal epoch
    /// column plus a unique BINARY state column. `None` keeps the task
    /// byte-for-byte identical to an ordinary flow.
    pub(crate) checkpoint_persistence: Option<CheckpointPersistence>,
    /// Epoch of the last durably persisted checkpoint record. Advanced only
    /// after the singleton checkpoint row write succeeds; rows stamped with a
    /// larger epoch invalidate the durable record on restart.
    persisted_epoch: u64,
    exec_state: ExecState,
    /// Registered Phase-1 two-phase backfill jobs. Normal incremental
    /// execution ignores these entirely: they are the staging-side bookkeeping
    /// for backfill (aligned range, staging table, frozen watermark F) and
    /// never block or alter live flow evaluation or checkpoint advancement.
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    backfill_jobs: Vec<BackfillJob>,
    /// Shutdown receiver
    pub(crate) shutdown_rx: oneshot::Receiver<()>,
    /// Task handle
    pub(crate) task_handle: Option<tokio::task::JoinHandle<()>>,
}
impl TaskState {
    pub fn new(query_ctx: QueryContextRef, shutdown_rx: oneshot::Receiver<()>) -> Self {
        Self::with_dirty_time_windows(query_ctx, shutdown_rx, DirtyTimeWindows::default())
    }

    pub fn with_dirty_time_windows(
        query_ctx: QueryContextRef,
        shutdown_rx: oneshot::Receiver<()>,
        dirty_time_windows: DirtyTimeWindows,
    ) -> Self {
        Self {
            query_ctx,
            last_update_time: Instant::now(),
            last_query_duration: Duration::from_secs(0),
            last_exec_time_millis: None,
            start_time_millis: None,
            dirty_time_windows,
            checkpoint_mode: CheckpointMode::FullSnapshot,
            pending_fenced_repair: None,
            checkpoints: Default::default(),
            incremental_disabled: false,
            checkpoint_persistence: None,
            persisted_epoch: 0,
            exec_state: ExecState::Idle,
            backfill_jobs: Vec::new(),
            shutdown_rx,
            task_handle: None,
        }
    }

    /// Record the first-execution start time. Call this once, just before
    /// the first frontend query is dispatched, not after it completes.
    pub fn record_start_time_if_first(&mut self) {
        if self.start_time_millis.is_none() {
            // start_time is recorded just before the first frontend query is dispatched
            // (pre-execution), so it may be marginally earlier than the streaming engine's
            // start_time which is set post-execution. Both are valid approximations of
            // "when this flow first ran".
            self.start_time_millis = Some(common_time::util::current_time_millis());
        }
    }

    pub fn after_query_exec(&mut self, elapsed: Duration, is_succ: bool) {
        self.exec_state = ExecState::Idle;
        self.last_query_duration = elapsed;
        self.last_update_time = Instant::now();
        if is_succ {
            self.last_exec_time_millis = Some(common_time::util::current_time_millis());
        }
    }

    pub fn last_execution_time_millis(&self) -> Option<i64> {
        self.last_exec_time_millis
    }

    /// First execution time in unix timestamp milliseconds, set once.
    pub fn start_time_millis(&self) -> Option<i64> {
        self.start_time_millis
    }

    pub fn checkpoint_mode(&self) -> CheckpointMode {
        self.checkpoint_mode
    }

    pub fn checkpoints(&self) -> &BTreeMap<u64, u64> {
        &self.checkpoints
    }

    /// Returns the resolved checkpoint persistence layout, if activated.
    pub fn checkpoint_persistence(&self) -> Option<&CheckpointPersistence> {
        self.checkpoint_persistence.as_ref()
    }

    /// Epoch of the last durably persisted checkpoint record (0 = none).
    pub fn persisted_epoch(&self) -> u64 {
        self.persisted_epoch
    }

    /// The epoch the current cycle must stamp onto emitted state rows and, on
    /// a successful checkpoint write, persist. One past the last durable epoch
    /// so rows from an unpersisted cycle always invalidate the older record.
    pub fn next_persist_epoch(&self) -> u64 {
        self.persisted_epoch.saturating_add(1)
    }

    /// Record a successfully persisted checkpoint epoch. Called only after the
    /// singleton checkpoint row write succeeds; never before it.
    pub fn advance_persisted_epoch(&mut self, epoch: u64) {
        self.persisted_epoch = self.persisted_epoch.max(epoch);
    }

    /// Activate or deactivate checkpoint persistence for this task.
    pub fn set_checkpoint_persistence(&mut self, persistence: Option<CheckpointPersistence>) {
        self.checkpoint_persistence = persistence;
    }

    /// Seed the task from a trusted restored checkpoint record: replace the
    /// in-memory checkpoint map, pin the durable epoch, and enter Incremental
    /// mode (unless incremental is permanently disabled).
    pub fn seed_checkpoints_from_record(&mut self, epoch: u64, checkpoints: BTreeMap<u64, u64>) {
        self.persisted_epoch = epoch;
        self.checkpoints = checkpoints;
        self.pending_fenced_repair = None;
        if !self.incremental_disabled {
            self.checkpoint_mode = CheckpointMode::Incremental;
        }
    }

    /// Returns the in-progress fenced repair, if the task is repairing dirty
    /// windows under a frozen full-snapshot high watermark.
    pub fn pending_fenced_repair(&self) -> Option<&FencedRepair> {
        self.pending_fenced_repair.as_ref()
    }

    /// Registers a Phase-1 backfill job under a strict lifecycle:
    /// `Preparing -> Prepared -> Running -> BaseComplete -> Finishing`.
    ///
    /// An exact duplicate (same `job_id` and identical immutable identity:
    /// aligned range, staging table name/id) is idempotent: the existing job
    /// is kept untouched, so a re-prepare never resets a recorded watermark F
    /// and never clears or rebuilds the staging table. Re-registering the same
    /// `job_id` with a different identity fails closed instead of silently
    /// replacing the registered job.
    ///
    /// This is the low-level identity-checking primitive; the atomic prepare
    /// reservation (including the `Preparing`-duplicate Busy rule) lives in
    /// [`Self::begin_backfill_prepare`].
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn register_backfill_job(&mut self, job: &BackfillJob) -> Result<(), Error> {
        if let Some(existing) = self
            .backfill_jobs
            .iter()
            .find(|existing| existing.job_id == job.job_id)
        {
            ensure!(
                existing.range == job.range
                    && existing.staging_table_name == job.staging_table_name
                    && existing.staging_table_id == job.staging_table_id,
                UnexpectedSnafu {
                    reason: format!(
                        "backfill job {} already registered with a different identity (range {:?}/{:?}, staging {:?}/{:?}, staging_table_id {:?}/{:?}); refusing to replace it",
                        job.job_id,
                        existing.range,
                        job.range,
                        existing.staging_table_name,
                        job.staging_table_name,
                        existing.staging_table_id,
                        job.staging_table_id
                    )
                }
            );
            return Ok(());
        }
        self.backfill_jobs.push(job.clone());
        Ok(())
    }

    /// Returns the registered backfill job with the given id, if any.
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn get_backfill_job(&self, job_id: u64) -> Option<&BackfillJob> {
        self.backfill_jobs.iter().find(|job| job.job_id == job_id)
    }

    /// Registered Phase-1 backfill jobs (never consulted by normal flow
    /// evaluation or checkpoint handling).
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn backfill_jobs(&self) -> &[BackfillJob] {
        &self.backfill_jobs
    }

    /// Atomically transitions a registered `Prepared` job to `Running`
    /// (short critical section, before the Base query starts). Returns the
    /// job so the caller works with the authoritative registered state.
    ///
    /// Re-entering a `Running` job or re-running a `BaseComplete` job is
    /// rejected: a concurrent or late run must never overwrite the frozen
    /// watermark F of a completed run. A `Preparing` (reservation not yet
    /// finished) or `Finishing` (cleanup in flight) job cannot start either.
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn begin_backfill_run(&mut self, job_id: u64) -> Result<BackfillJob, Error> {
        let job = self
            .backfill_jobs
            .iter_mut()
            .find(|job| job.job_id == job_id)
            .with_context(|| UnexpectedSnafu {
                reason: format!("no registered backfill job {job_id} to run"),
            })?;
        ensure!(
            job.status == BackfillJobStatus::Prepared,
            UnexpectedSnafu {
                reason: format!(
                    "backfill job {job_id} cannot start: already {:?}",
                    job.status
                )
            }
        );
        job.status = BackfillJobStatus::Running;
        Ok(job.clone())
    }

    /// Records the frozen scan-open watermark F and transitions a `Running`
    /// job to `BaseComplete`. Only a `Running` job may complete: a late
    /// completion from a stale run must not overwrite the F of a newer state.
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn complete_backfill_run(
        &mut self,
        job_id: u64,
        watermark: BTreeMap<u64, u64>,
    ) -> Result<(), Error> {
        let job = self
            .backfill_jobs
            .iter_mut()
            .find(|job| job.job_id == job_id)
            .with_context(|| UnexpectedSnafu {
                reason: format!("no registered backfill job {job_id} to complete"),
            })?;
        ensure!(
            job.status == BackfillJobStatus::Running,
            UnexpectedSnafu {
                reason: format!(
                    "backfill job {job_id} cannot complete: already {:?}",
                    job.status
                )
            }
        );
        job.status = BackfillJobStatus::BaseComplete;
        job.frozen_watermark = Some(watermark);
        Ok(())
    }

    /// Transitions a `Running` job back to `Prepared` after its Base query
    /// failed. The staging table and the registered job are kept so the caller
    /// can retry; only a `Running` job may fail back.
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn fail_backfill_run(&mut self, job_id: u64) -> Result<(), Error> {
        let job = self
            .backfill_jobs
            .iter_mut()
            .find(|job| job.job_id == job_id)
            .with_context(|| UnexpectedSnafu {
                reason: format!("no registered backfill job {job_id} to fail"),
            })?;
        ensure!(
            job.status == BackfillJobStatus::Running,
            UnexpectedSnafu {
                reason: format!(
                    "backfill job {job_id} cannot fail back: already {:?}",
                    job.status
                )
            }
        );
        job.status = BackfillJobStatus::Prepared;
        Ok(())
    }

    /// Removes and returns a registered backfill job. Used by finalize
    /// cleanup *after* the staging table drop succeeded (drop-before-remove);
    /// a failed drop keeps the job registered so cleanup can be retried.
    ///
    /// For drop-success removal with a generation check, prefer
    /// [`Self::remove_backfill_job_if_finishing`].
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn take_backfill_job(&mut self, job_id: u64) -> Option<BackfillJob> {
        let idx = self
            .backfill_jobs
            .iter()
            .position(|job| job.job_id == job_id)?;
        Some(self.backfill_jobs.remove(idx))
    }

    /// Atomically reserves a backfill prepare under the short state lock,
    /// BEFORE any async staging-table existence check / create / plan building.
    ///
    /// For a brand-new job, installs a `Preparing { staging_may_exist: false }`
    /// reservation so two concurrent prepares can never both observe the
    /// staging table as absent and independently create it. An exact-duplicate
    /// re-prepare of a `Prepared` / `BaseComplete` job returns
    /// [`PrepareReservation::Existing`] (idempotent; status and F untouched).
    /// A `Preparing { staging_may_exist: true }` job returns
    /// [`PrepareReservation::Resuming`] so the caller resumes the previous
    /// attempt. All other states reject the prepare:
    /// - `Preparing { staging_may_exist: false }` -> Busy (AlreadyPreparing);
    ///   the duplicate must not independently create the table.
    /// - `Running` -> Busy (a Base query is in flight).
    /// - `Finishing` -> Busy (cleanup is in flight).
    ///
    /// A different identity (aligned range or staging name) for the same
    /// `job_id` is rejected instead of replacing the registered job.
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn begin_backfill_prepare(
        &mut self,
        job_id: u64,
        range: (Timestamp, Timestamp),
        staging_table_name: [String; 3],
    ) -> Result<PrepareReservation, Error> {
        if let Some(existing) = self.backfill_jobs.iter().find(|job| job.job_id == job_id) {
            ensure!(
                existing.range == range && existing.staging_table_name == staging_table_name,
                UnexpectedSnafu {
                    reason: format!(
                        "backfill job {} already registered with a different identity (range {:?}/{:?}, staging {:?}/{:?}); refusing to replace it",
                        job_id,
                        existing.range,
                        range,
                        existing.staging_table_name,
                        staging_table_name
                    )
                }
            );
            return match existing.status {
                BackfillJobStatus::Preparing { staging_may_exist: false } => {
                    UnexpectedSnafu {
                        reason: format!(
                            "backfill job {job_id} is already being prepared (Preparing); a duplicate prepare must not independently create the staging table"
                        ),
                    }
                    .fail()
                }
                BackfillJobStatus::Preparing { staging_may_exist: true } => {
                    Ok(PrepareReservation::Resuming(existing.clone()))
                }
                BackfillJobStatus::Prepared | BackfillJobStatus::BaseComplete => {
                    Ok(PrepareReservation::Existing(existing.clone()))
                }
                BackfillJobStatus::Running => UnexpectedSnafu {
                    reason: format!(
                        "backfill job {job_id} cannot be prepared while a Base run is in flight (Running)"
                    ),
                }
                .fail(),
                BackfillJobStatus::Finishing => UnexpectedSnafu {
                    reason: format!(
                        "backfill job {job_id} cannot be prepared while cleanup is in flight (Finishing)"
                    ),
                }
                .fail(),
            };
        }
        let job = BackfillJob {
            job_id,
            range,
            staging_table_name,
            staging_table_id: None,
            frozen_watermark: None,
            status: BackfillJobStatus::Preparing {
                staging_may_exist: false,
            },
        };
        self.backfill_jobs.push(job.clone());
        Ok(PrepareReservation::Reserved(job))
    }

    /// Transitions a `Preparing` reservation to `Prepared` once the staging
    /// table create succeeded and its id is resolved. The job's immutable
    /// identity is unchanged; the recorded `staging_table_id` is filled in.
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn finish_backfill_prepare(
        &mut self,
        job_id: u64,
        staging_table_id: TableId,
    ) -> Result<BackfillJob, Error> {
        let job = self
            .backfill_jobs
            .iter_mut()
            .find(|job| job.job_id == job_id)
            .with_context(|| UnexpectedSnafu {
                reason: format!("no registered backfill job {job_id} to finish preparing"),
            })?;
        ensure!(
            matches!(job.status, BackfillJobStatus::Preparing { .. }),
            UnexpectedSnafu {
                reason: format!(
                    "backfill job {job_id} cannot finish preparing: already {:?}",
                    job.status
                )
            }
        );
        job.status = BackfillJobStatus::Prepared;
        job.staging_table_id = Some(staging_table_id);
        Ok(job.clone())
    }

    /// Marks a `Preparing` reservation as possibly having created its staging
    /// table (ambiguous create failure), keeping ownership of the job so the
    /// table is never anonymous. A re-prepare then resumes instead of erroring.
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn mark_staging_may_exist(&mut self, job_id: u64) -> Result<(), Error> {
        let job = self
            .backfill_jobs
            .iter_mut()
            .find(|job| job.job_id == job_id)
            .with_context(|| UnexpectedSnafu {
                reason: format!("no registered backfill job {job_id} to mark"),
            })?;
        ensure!(
            matches!(job.status, BackfillJobStatus::Preparing { .. }),
            UnexpectedSnafu {
                reason: format!(
                    "backfill job {job_id} cannot be marked: not Preparing ({:?})",
                    job.status
                )
            }
        );
        job.status = BackfillJobStatus::Preparing {
            staging_may_exist: true,
        };
        Ok(())
    }

    /// Cancels a `Preparing` reservation that failed before any create could
    /// have happened (fail-closed orphan detection): removes the job so the
    /// registry returns to the pre-prepare state and the caller can retry
    /// after explicit cleanup.
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn cancel_backfill_prepare(&mut self, job_id: u64) -> Result<(), Error> {
        let idx = self
            .backfill_jobs
            .iter()
            .position(|job| job.job_id == job_id)
            .with_context(|| UnexpectedSnafu {
                reason: format!("no registered backfill job {job_id} to cancel"),
            })?;
        ensure!(
            matches!(
                self.backfill_jobs[idx].status,
                BackfillJobStatus::Preparing { .. }
            ),
            UnexpectedSnafu {
                reason: format!(
                    "backfill job {job_id} cannot be cancelled: not Preparing ({:?})",
                    self.backfill_jobs[idx].status
                )
            }
        );
        self.backfill_jobs.remove(idx);
        Ok(())
    }

    /// Atomically transitions a `BaseComplete` job to `Finishing` — the only
    /// eligible stable state for Phase-1 cleanup. This prevents a cleanup from
    /// racing an active run (a `Running` job can never be cleaned up from
    /// under its Base query), a second concurrent finish (`Finishing` ->
    /// Busy), or a re-prepare / new generation while cleanup is in flight.
    /// `Prepared` / `Preparing` jobs are not cleanable in Phase 1 (aborting a
    /// prepared job is a separate transition, intentionally not exposed).
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn begin_backfill_finish(&mut self, job_id: u64) -> Result<BackfillJob, Error> {
        let job = self
            .backfill_jobs
            .iter_mut()
            .find(|job| job.job_id == job_id)
            .with_context(|| UnexpectedSnafu {
                reason: format!("no registered backfill job {job_id} to finish"),
            })?;
        ensure!(
            job.status == BackfillJobStatus::BaseComplete,
            UnexpectedSnafu {
                reason: format!(
                    "backfill job {job_id} cannot finish: only a BaseComplete job is cleanable in Phase 1, job is {:?}",
                    job.status
                )
            }
        );
        job.status = BackfillJobStatus::Finishing;
        Ok(job.clone())
    }

    /// Returns a `Finishing` job to `BaseComplete` after a failed staging DROP,
    /// preserving the job (and its staging table) for a cleanup retry. Only a
    /// `Finishing` job may restore.
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn restore_backfill_finish(&mut self, job_id: u64) -> Result<BackfillJob, Error> {
        let job = self
            .backfill_jobs
            .iter_mut()
            .find(|job| job.job_id == job_id)
            .with_context(|| UnexpectedSnafu {
                reason: format!("no registered backfill job {job_id} to restore"),
            })?;
        ensure!(
            job.status == BackfillJobStatus::Finishing,
            UnexpectedSnafu {
                reason: format!(
                    "backfill job {job_id} cannot restore: not Finishing ({:?})",
                    job.status
                )
            }
        );
        job.status = BackfillJobStatus::BaseComplete;
        Ok(job.clone())
    }

    /// CAS-removes the registered job only when it is still `Finishing` (the
    /// same cleanup generation the DROP was issued for). Returns `Ok(None)`
    /// when the job is already gone; errors if the job exists but is no longer
    /// `Finishing`, so a generation change can never remove a different job.
    #[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
    pub fn remove_backfill_job_if_finishing(
        &mut self,
        job_id: u64,
    ) -> Result<Option<BackfillJob>, Error> {
        let idx = match self
            .backfill_jobs
            .iter()
            .position(|job| job.job_id == job_id)
        {
            Some(idx) => idx,
            None => return Ok(None),
        };
        ensure!(
            self.backfill_jobs[idx].status == BackfillJobStatus::Finishing,
            UnexpectedSnafu {
                reason: format!(
                    "backfill job {job_id} changed state while finishing (now {:?}); refusing to remove",
                    self.backfill_jobs[idx].status
                )
            }
        );
        Ok(Some(self.backfill_jobs.remove(idx)))
    }

    pub fn is_incremental_disabled(&self) -> bool {
        self.incremental_disabled
    }

    /// Permanently disable incremental mode for this task and
    /// immediately fall back to full snapshot for the current cycle.
    pub fn disable_incremental(&mut self) {
        self.incremental_disabled = true;
        self.mark_full_snapshot();
    }

    /// Move back to top-level FullSnapshot mode. If a fenced repair is active,
    /// restore its not-yet-in-flight pending windows to the live dirty queue so
    /// the moved backlog is not lost.
    pub fn mark_full_snapshot(&mut self) {
        self.abandon_fenced_repair();
    }

    /// Replace full-snapshot checkpoints with a complete watermark proof.
    /// Clears fenced repair state and enters Incremental unless disabled.
    pub fn advance_checkpoints(&mut self, watermark_map: HashMap<u64, u64>) {
        self.checkpoints = watermark_map.into_iter().collect();
        self.pending_fenced_repair = None;
        if !self.incremental_disabled {
            self.checkpoint_mode = CheckpointMode::Incremental;
        }
    }

    /// Advance only the participating regions for an incremental delta query.
    /// This also clears any stale fenced repair sub-state.
    pub fn advance_incremental_checkpoints_with_participation(
        &mut self,
        participating_regions: &BTreeSet<u64>,
        watermark_map: HashMap<u64, u64>,
    ) {
        for region_id in participating_regions {
            if let Some(seq) = watermark_map.get(region_id) {
                self.checkpoints.insert(*region_id, *seq);
            }
        }
        if !self.incremental_disabled {
            self.checkpoint_mode = CheckpointMode::Incremental;
        }
        self.pending_fenced_repair = None;
    }

    /// Start repairing the current live dirty windows under a frozen high `H`.
    /// The current live backlog is moved into the fenced repair so successful
    /// chunks are consumed from that backlog. New post-`H` dirty signals can
    /// still arrive in the live queue while the fenced repair is active.
    pub fn start_fenced_repair(&mut self, high: BTreeMap<u64, u64>) -> Option<&FencedRepair> {
        if self.dirty_time_windows.is_empty() {
            self.pending_fenced_repair = None;
            return None;
        }

        let pending_windows = self.dirty_time_windows.clone();
        self.dirty_time_windows.clean();
        self.pending_fenced_repair = Some(FencedRepair {
            high,
            pending_windows,
        });
        self.checkpoint_mode = CheckpointMode::FullSnapshot;
        self.pending_fenced_repair.as_ref()
    }

    /// Finish the fenced repair and promote the frozen high watermark to the
    /// checkpoint map. Incremental-disabled flows stay in FullSnapshot mode.
    pub fn finish_fenced_repair(&mut self) -> Option<BTreeMap<u64, u64>> {
        let repair = self.pending_fenced_repair.take()?;
        self.checkpoints = repair.high;
        if !self.incremental_disabled {
            self.checkpoint_mode = CheckpointMode::Incremental;
        }
        Some(self.checkpoints.clone())
    }

    /// Abandon the current fenced repair and restore all not-yet-in-flight
    /// pending windows to the live dirty queue for a fresh scoped repair.
    pub fn abandon_fenced_repair(&mut self) -> bool {
        self.checkpoint_mode = CheckpointMode::FullSnapshot;
        let Some(repair) = self.pending_fenced_repair.take() else {
            return false;
        };

        self.dirty_time_windows
            .add_dirty_windows(&repair.pending_windows);
        true
    }

    /// Restore a scoped query's windows after a failed or unproven run. During
    /// an active fenced repair this requeues into `pending_windows`; otherwise
    /// it restores to the live dirty queue.
    pub fn restore_scoped_windows(&mut self, filter: &FilterExprInfo) {
        if let Some(repair) = self.pending_fenced_repair.as_mut() {
            repair
                .pending_windows
                .add_windows(filter.time_ranges.clone());
            return;
        }

        self.dirty_time_windows
            .add_windows(filter.time_ranges.clone());
    }

    /// Generate the next scoped filter from the fenced-repair queue when active;
    /// otherwise consume windows from the live dirty queue.
    pub fn gen_scoped_filter_exprs(
        &mut self,
        col_name: &str,
        expire_lower_bound: Option<Timestamp>,
        window_size: chrono::Duration,
        window_cnt: usize,
        flow_id: FlowId,
        task_ctx: Option<&BatchingTask>,
    ) -> Result<Option<FilterExprInfo>, Error> {
        if let Some(repair) = self.pending_fenced_repair.as_mut() {
            let expr = repair.pending_windows.gen_filter_exprs(
                col_name,
                expire_lower_bound,
                window_size,
                window_cnt,
                flow_id,
                task_ctx,
            )?;
            if expr.is_some() || !repair.pending_windows.is_empty() {
                return Ok(expr);
            }

            // All pending repair windows may have expired during merge. Clear
            // the empty repair so this call can fall back to live dirty windows
            // instead of routing future executions to an empty queue forever.
            self.pending_fenced_repair = None;
        }

        self.dirty_time_windows.gen_filter_exprs(
            col_name,
            expire_lower_bound,
            window_size,
            window_cnt,
            flow_id,
            task_ctx,
        )
    }

    /// Returns true only when the query result's participating regions and
    /// terminal watermarks exactly match the fenced repair's frozen high `H`.
    pub fn fenced_repair_watermarks_match_high(
        &self,
        participating_regions: &BTreeSet<u64>,
        watermark_map: &HashMap<u64, u64>,
    ) -> bool {
        let Some(repair) = self.pending_fenced_repair.as_ref() else {
            return false;
        };

        !participating_regions.is_empty()
            && participating_regions.len() == repair.high.len()
            && watermark_map.len() == repair.high.len()
            && participating_regions.iter().all(|region_id| {
                repair
                    .high
                    .get(region_id)
                    .zip(watermark_map.get(region_id))
                    .is_some_and(|(high, watermark)| high == watermark)
            })
    }

    /// Whether the active fenced repair has drained all pending windows.
    pub fn fenced_repair_pending_is_empty(&self) -> bool {
        self.pending_fenced_repair
            .as_ref()
            .is_some_and(|repair| repair.pending_windows.is_empty())
    }

    /// Full-snapshot checkpoint advances require a watermark for every region
    /// that participated in the query.
    pub fn can_advance_full_snapshot_checkpoints(
        &self,
        participating_regions: &BTreeSet<u64>,
        watermark_map: &HashMap<u64, u64>,
    ) -> bool {
        !participating_regions.is_empty()
            && participating_regions.len() == watermark_map.len()
            && participating_regions
                .iter()
                .all(|region_id| watermark_map.contains_key(region_id))
    }

    /// Incremental advances are limited to participating regions whose returned
    /// watermark is not older than the stored checkpoint.
    pub fn can_advance_incremental_checkpoints_with_participation(
        &self,
        participating_regions: &BTreeSet<u64>,
        watermark_map: &HashMap<u64, u64>,
    ) -> bool {
        !self.incremental_disabled
            && !self.checkpoints.is_empty()
            && !participating_regions.is_empty()
            && participating_regions.len() == watermark_map.len()
            && participating_regions
                .iter()
                .all(|region_id| self.checkpoints.contains_key(region_id))
            && participating_regions.iter().all(|region_id| {
                let checkpoint = self.checkpoints.get(region_id);
                watermark_map
                    .get(region_id)
                    .zip(checkpoint)
                    .is_some_and(|(seq, checkpoint)| seq >= checkpoint)
            })
    }

    /// Compute the next query delay based on the time window size or the last query duration.
    /// Aiming to avoid too frequent queries. But also not too long delay.
    ///
    /// next wait time is calculated as:
    /// last query duration, capped by [max(min_run_interval, time_window_size), max_timeout],
    /// note at most wait for `max_timeout`.
    ///
    /// if current the dirty time range is longer than one query can handle,
    /// execute immediately to faster clean up dirty time windows.
    /// Active fenced repairs also execute immediately while pending windows
    /// remain: the current backlog has moved out of live dirty windows and into
    /// `pending_fenced_repair.pending_windows`.
    ///
    /// If `prefer_short_incremental_cadence` is true, run incremental queries
    /// more often when there is no large dirty backlog. This only reduces the
    /// chance of hitting a stale cursor after flush; it is not required for
    /// correctness.
    pub fn get_next_start_query_time(
        &self,
        flow_id: FlowId,
        time_window_size: &Option<Duration>,
        min_refresh_duration: Duration,
        max_timeout: Option<Duration>,
        max_filter_num_per_query: usize,
        prefer_short_incremental_cadence: bool,
    ) -> Instant {
        // = last query duration, capped by [max(min_run_interval, time_window_size), max_timeout], note at most `max_timeout`
        let lower = time_window_size.unwrap_or(min_refresh_duration);
        let next_duration = self.last_query_duration.max(lower);
        let next_duration = if let Some(max_timeout) = max_timeout {
            next_duration.min(max_timeout)
        } else {
            next_duration
        };

        if self
            .pending_fenced_repair
            .as_ref()
            .is_some_and(|repair| !repair.pending_windows().is_empty())
        {
            debug!(
                "Flow id = {}, active fenced repair still has pending windows, execute immediately",
                flow_id,
            );
            return Instant::now();
        }

        let cur_dirty_window_size = self.dirty_time_windows.window_size();
        // compute how much time range can be handled in one query
        let max_query_update_range = (*time_window_size)
            .unwrap_or_default()
            .mul_f64(max_filter_num_per_query as f64);
        // if dirty time range is more than one query can handle, execute immediately
        // to faster clean up dirty time windows
        if cur_dirty_window_size < max_query_update_range {
            if prefer_short_incremental_cadence {
                // Run incremental queries sooner than the normal time-window
                // cadence, while still backing off by at least the previous
                // query duration and respecting the max-timeout cap.
                let next_duration = self.last_query_duration.max(min_refresh_duration);
                let next_duration = if let Some(max_timeout) = max_timeout {
                    next_duration.min(max_timeout)
                } else {
                    next_duration
                };
                self.last_update_time + next_duration
            } else {
                self.last_update_time + next_duration
            }
        } else {
            // if dirty time windows can't be clean up in one query, execute immediately to faster
            // clean up dirty time windows
            debug!(
                "Flow id = {}, still have too many {} dirty time window({:?}), execute immediately",
                flow_id,
                self.dirty_time_windows.windows.len(),
                self.dirty_time_windows.windows
            );
            Instant::now()
        }
    }
}

/// For keep recording of dirty time windows, which is time window that have new data inserted
/// since last query.
#[derive(Debug, Clone)]
pub struct DirtyTimeWindows {
    /// windows's `start -> end` and non-overlapping
    /// `end` is exclusive(and optional)
    windows: BTreeMap<Timestamp, Option<Timestamp>>,
    /// Maximum number of filters allowed in a single query
    max_filter_num_per_query: usize,
    /// Time window merge distance
    ///
    time_window_merge_threshold: usize,
}

impl DirtyTimeWindows {
    pub fn new(max_filter_num_per_query: usize, time_window_merge_threshold: usize) -> Self {
        Self {
            windows: BTreeMap::new(),
            max_filter_num_per_query,
            time_window_merge_threshold,
        }
    }

    #[cfg(test)]
    pub(crate) fn max_filter_num_per_query(&self) -> usize {
        self.max_filter_num_per_query
    }

    #[cfg(test)]
    pub(crate) fn time_window_merge_threshold(&self) -> usize {
        self.time_window_merge_threshold
    }
}

impl Default for DirtyTimeWindows {
    fn default() -> Self {
        Self {
            windows: BTreeMap::new(),
            max_filter_num_per_query: 20,
            time_window_merge_threshold: 3,
        }
    }
}

impl DirtyTimeWindows {
    /// Time window merge distance
    ///
    /// TODO(discord9): make those configurable
    pub const MERGE_DIST: i32 = 3;

    /// Add lower bounds to the dirty time windows. Upper bounds are ignored.
    ///
    /// # Arguments
    ///
    /// * `lower_bounds` - An iterator of lower bounds to be added.
    pub fn add_lower_bounds(&mut self, lower_bounds: impl Iterator<Item = Timestamp>) {
        for lower_bound in lower_bounds {
            let entry = self.windows.entry(lower_bound);
            entry.or_insert(None);
        }
    }

    pub fn window_size(&self) -> Duration {
        let mut ret = Duration::from_secs(0);
        for (start, end) in &self.windows {
            if let Some(end) = end
                && let Some(duration) = end.sub(start)
            {
                ret += duration.to_std().unwrap_or_default();
            }
        }
        ret
    }

    pub fn add_window(&mut self, start: Timestamp, end: Option<Timestamp>) {
        self.add_or_merge_window(start, end);
    }

    pub fn add_windows(&mut self, time_ranges: Vec<(Timestamp, Timestamp)>) {
        for (start, end) in time_ranges {
            self.add_or_merge_window(start, Some(end));
        }
    }

    /// Add all dirty markers from another dirty-window set.
    pub fn add_dirty_windows(&mut self, dirty_windows: &DirtyTimeWindows) {
        for (start, end) in &dirty_windows.windows {
            self.add_or_merge_window(*start, *end);
        }
    }

    fn add_or_merge_window(&mut self, start: Timestamp, end: Option<Timestamp>) {
        self.windows
            .entry(start)
            .and_modify(|current_end| {
                *current_end = Self::union_window_end(*current_end, end);
            })
            .or_insert(end);
    }

    fn union_window_end(
        current_end: Option<Timestamp>,
        incoming_end: Option<Timestamp>,
    ) -> Option<Timestamp> {
        match (current_end, incoming_end) {
            (Some(current), Some(incoming)) => Some(current.max(incoming)),
            // `None` is a dirty marker without a known upper bound.  When one
            // side has a concrete end, keep it so merging a restored snapshot
            // never shrinks an already-known dirty range with the same start.
            (Some(end), None) | (None, Some(end)) => Some(end),
            (None, None) => None,
        }
    }

    /// Clean all dirty time windows, useful when can't found time window expr
    pub fn clean(&mut self) {
        self.windows.clear();
    }

    /// Set windows to be dirty, only useful for full aggr without time window
    /// to mark some new data is inserted
    pub fn set_dirty(&mut self) {
        self.add_or_merge_window(Timestamp::new_second(0), None);
    }

    /// Number of dirty windows.
    pub fn len(&self) -> usize {
        self.windows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.windows.is_empty()
    }

    /// Get the effective count of time windows, which is the number of time windows that can be
    /// used for query, compute from total time window range divided by `window_size`.
    pub fn effective_count(&self, window_size: &Duration) -> usize {
        if self.windows.is_empty() {
            return 0;
        }
        let window_size =
            chrono::Duration::from_std(*window_size).unwrap_or(chrono::Duration::zero());
        let total_window_time_range =
            self.windows
                .iter()
                .fold(chrono::Duration::zero(), |acc, (start, end)| {
                    if let Some(end) = end {
                        acc + end.sub(start).unwrap_or(chrono::Duration::zero())
                    } else {
                        acc + window_size
                    }
                });

        // not sure window_size is zero have any meaning, but just in case
        if window_size.num_seconds() == 0 {
            0
        } else {
            (total_window_time_range.num_seconds() / window_size.num_seconds()) as usize
        }
    }

    /// Generate all filter expressions consuming all time windows
    ///
    /// there is two limits:
    /// - shouldn't return a too long time range(<=`window_size * window_cnt`), so that the query can be executed in a reasonable time
    /// - shouldn't return too many time range exprs, so that the query can be parsed properly instead of causing parser to overflow
    pub fn gen_filter_exprs(
        &mut self,
        col_name: &str,
        expire_lower_bound: Option<Timestamp>,
        window_size: chrono::Duration,
        window_cnt: usize,
        flow_id: FlowId,
        task_ctx: Option<&BatchingTask>,
    ) -> Result<Option<FilterExprInfo>, Error> {
        ensure!(
            window_size.num_seconds() > 0,
            UnexpectedSnafu {
                reason: "window_size is zero, can't generate filter exprs",
            }
        );

        debug!(
            "expire_lower_bound: {:?}, window_size: {:?}",
            expire_lower_bound.map(|t| t.to_iso8601_string()),
            window_size
        );
        self.merge_dirty_time_windows(window_size, expire_lower_bound)?;

        if self.windows.len() > window_cnt {
            let first_time_window = self.windows.first_key_value();
            let last_time_window = self.windows.last_key_value();

            if let Some(task_ctx) = task_ctx {
                debug!(
                    "Flow id = {:?}, too many time windows: {}, only the first {} are taken for this query, the group by expression might be wrong. Time window expr={:?}, expire_after={:?}, first_time_window={:?}, last_time_window={:?}, the original query: {:?}",
                    task_ctx.config.flow_id,
                    self.windows.len(),
                    window_cnt,
                    task_ctx.config.time_window_expr,
                    task_ctx.config.expire_after,
                    first_time_window,
                    last_time_window,
                    task_ctx.config.query
                );
            } else {
                debug!(
                    "Flow id = {:?}, too many time windows: {}, only the first {} are taken for this query, the group by expression might be wrong. first_time_window={:?}, last_time_window={:?}",
                    flow_id,
                    self.windows.len(),
                    window_cnt,
                    first_time_window,
                    last_time_window
                )
            }
        }

        // get the first `window_cnt` time windows
        let max_time_range = window_size * window_cnt as i32;

        let mut to_be_query = BTreeMap::new();
        let mut new_windows = self.windows.clone();
        let mut cur_time_range = chrono::Duration::zero();
        for (idx, (start, end)) in self.windows.iter().enumerate() {
            let first_end = start
                .add_duration(window_size.to_std().unwrap())
                .context(TimeSnafu)?;
            let end = end.unwrap_or(first_end);

            // if time range is too long, stop
            if cur_time_range >= max_time_range {
                break;
            }

            // if we have enough time windows, stop
            if idx >= window_cnt {
                break;
            }

            let Some(x) = end.sub(start) else {
                continue;
            };
            if cur_time_range + x <= max_time_range {
                to_be_query.insert(*start, Some(end));
                new_windows.remove(start);
                cur_time_range += x;
            } else {
                // too large a window, split it
                // split at window_size * times
                let surplus = max_time_range - cur_time_range;
                if surplus.num_seconds() <= window_size.num_seconds() {
                    // Skip splitting if surplus is smaller than window_size
                    break;
                }
                let times = surplus.num_seconds() / window_size.num_seconds();

                let split_offset = window_size * times as i32;
                let split_at = start
                    .add_duration(split_offset.to_std().unwrap())
                    .context(TimeSnafu)?;
                to_be_query.insert(*start, Some(split_at));

                // remove the original window
                new_windows.remove(start);
                new_windows.insert(split_at, Some(end));
                cur_time_range += split_offset;
                break;
            }
        }

        self.windows = new_windows;

        METRIC_FLOW_BATCHING_ENGINE_QUERY_WINDOW_CNT
            .with_label_values(&[flow_id.to_string().as_str()])
            .observe(to_be_query.len() as f64);

        let full_time_range = to_be_query
            .iter()
            .fold(chrono::Duration::zero(), |acc, (start, end)| {
                if let Some(end) = end {
                    acc + end.sub(start).unwrap_or(chrono::Duration::zero())
                } else {
                    acc + window_size
                }
            })
            .num_seconds() as f64;
        METRIC_FLOW_BATCHING_ENGINE_QUERY_WINDOW_SIZE
            .with_label_values(&[flow_id.to_string().as_str()])
            .observe(full_time_range);

        let stalled_time_range =
            self.windows
                .iter()
                .fold(chrono::Duration::zero(), |acc, (start, end)| {
                    if let Some(end) = end {
                        acc + end.sub(start).unwrap_or(chrono::Duration::zero())
                    } else {
                        acc + window_size
                    }
                });

        METRIC_FLOW_BATCHING_ENGINE_STALLED_WINDOW_SIZE
            .with_label_values(&[flow_id.to_string().as_str()])
            .observe(stalled_time_range.num_seconds() as f64);

        let std_window_size = window_size.to_std().map_err(|e| {
            InternalSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;

        let mut expr_lst = vec![];
        let mut time_ranges = vec![];
        for (start, end) in to_be_query.into_iter() {
            // align using time window exprs
            let (start, end) = if let Some(ctx) = task_ctx {
                let Some(time_window_expr) = &ctx.config.time_window_expr else {
                    UnexpectedSnafu {
                        reason: "time_window_expr is not set",
                    }
                    .fail()?
                };
                Self::align_time_window(start, end, time_window_expr)?
            } else {
                (start, end)
            };
            let end = end.unwrap_or(start.add_duration(std_window_size).context(TimeSnafu)?);
            time_ranges.push((start, end));

            debug!(
                "Time window start: {:?}, end: {:?}",
                start.to_iso8601_string(),
                end.to_iso8601_string()
            );

            use datafusion_expr::{col, lit};
            let lower = to_df_literal(start)?;
            let upper = to_df_literal(end)?;
            let expr = col(col_name)
                .gt_eq(lit(lower))
                .and(col(col_name).lt(lit(upper)));
            expr_lst.push(expr);
        }
        let expr = expr_lst.into_iter().reduce(|a, b| a.or(b));
        let ret = expr.map(|expr| FilterExprInfo {
            expr,
            col_name: col_name.to_string(),
            time_ranges,
            window_size,
        });
        Ok(ret)
    }

    /// Align a time range `[start, end)` (end is optional and exclusive) to
    /// time window boundaries defined by the time window expr.
    pub(crate) fn align_time_window(
        start: Timestamp,
        end: Option<Timestamp>,
        time_window_expr: &TimeWindowExpr,
    ) -> Result<(Timestamp, Option<Timestamp>), Error> {
        let align_start = time_window_expr.eval(start)?.0.context(UnexpectedSnafu {
            reason: format!(
                "Failed to align start time {:?} with time window expr {:?}",
                start, time_window_expr
            ),
        })?;
        let align_end = end
            .and_then(|end| {
                time_window_expr
                    .eval(end)
                    // if after aligned, end is the same, then use end(because it's already aligned) else use aligned end
                    .map(|r| if r.0 == Some(end) { r.0 } else { r.1 })
                    .transpose()
            })
            .transpose()?;
        Ok((align_start, align_end))
    }

    /// Merge time windows that overlaps or get too close
    ///
    /// TODO(discord9): not merge and prefer to send smaller time windows? how?
    pub fn merge_dirty_time_windows(
        &mut self,
        window_size: chrono::Duration,
        expire_lower_bound: Option<Timestamp>,
    ) -> Result<(), Error> {
        if self.windows.is_empty() {
            return Ok(());
        }

        let mut new_windows = BTreeMap::new();

        let std_window_size = window_size.to_std().map_err(|e| {
            InternalSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;

        // previous time window
        let mut prev_tw = None;
        for (mut lower_bound, upper_bound) in std::mem::take(&mut self.windows) {
            // filter out expired time window
            if let Some(expire_lower_bound) = expire_lower_bound {
                match upper_bound {
                    // A bounded range ending at or before the expire bound is
                    // fully expired, drop it.
                    Some(upper_bound) if upper_bound <= expire_lower_bound => continue,
                    // A bounded range crossing the expire bound keeps its
                    // still-live suffix. The expire bound is aligned to the
                    // time window boundary by the caller, so the clipped start
                    // stays aligned.
                    Some(_) if lower_bound < expire_lower_bound => {
                        lower_bound = expire_lower_bound;
                    }
                    // Unbounded windows keep the start-based behavior.
                    None if lower_bound < expire_lower_bound => continue,
                    _ => {}
                }
            }

            let Some(prev_tw) = &mut prev_tw else {
                prev_tw = Some((lower_bound, upper_bound));
                continue;
            };

            // if cur.lower - prev.upper <= window_size * MERGE_DIST, merge
            // this also deal with overlap windows because cur.lower > prev.lower is always true
            let prev_upper = prev_tw
                .1
                .unwrap_or(prev_tw.0.add_duration(std_window_size).context(TimeSnafu)?);
            prev_tw.1 = Some(prev_upper);

            let cur_upper = upper_bound.unwrap_or(
                lower_bound
                    .add_duration(std_window_size)
                    .context(TimeSnafu)?,
            );

            if lower_bound
                .sub(&prev_upper)
                .map(|dist| dist <= window_size * self.time_window_merge_threshold as i32)
                .unwrap_or(false)
            {
                // Union the two windows: the current window may be contained
                // in the previous one, so keep the larger upper bound.
                prev_tw.1 = Some(prev_upper.max(cur_upper));
            } else {
                new_windows.insert(prev_tw.0, prev_tw.1);
                *prev_tw = (lower_bound, Some(cur_upper));
            }
        }

        if let Some(prev_tw) = prev_tw {
            new_windows.insert(prev_tw.0, prev_tw.1);
        }

        self.windows = new_windows;

        Ok(())
    }
}

pub(crate) fn to_df_literal(value: Timestamp) -> Result<datafusion_common::ScalarValue, Error> {
    let value = Value::from(value);
    let value = value
        .try_to_scalar_value(&value.data_type())
        .with_context(|_| DatatypesSnafu {
            extra: format!("Failed to convert to scalar value: {}", value),
        })?;
    Ok(value)
}

#[derive(Debug, Clone)]
enum ExecState {
    Idle,
    Executing,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointMode {
    FullSnapshot,
    Incremental,
}

/// Column layout of the sink table required for checkpoint persistence.
///
/// Resolved once at task creation when the batching mode is `SequenceRange`
/// and the sink schema satisfies the persistence contract (see
/// `BatchingTask::detect_checkpoint_persistence`): the reserved internal epoch
/// column, an explicitly identified BINARY state column, and a timestamp
/// time-index window column. The window column's sentinel value marks the
/// singleton checkpoint row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CheckpointPersistence {
    /// Name of the reserved internal epoch column
    /// ([`crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME`]).
    pub epoch_col_name: String,
    /// Name of the sink BINARY column storing the encoded checkpoint record.
    pub state_col_name: String,
    /// Name of the sink window/time-index column used for the sentinel row.
    pub window_col_name: String,
    /// Sink primary-key (dimension) column names. The singleton checkpoint
    /// row's logical key is `(typed NULL for every entry, sentinel window)`;
    /// the writer explicitly projects each entry as a typed NULL rather than
    /// relying on omitted columns/defaults. Empty when the sink has no
    /// explicit primary-key columns (time-index-only key).
    pub primary_key_columns: Vec<String>,
}

/// Dirty windows that must be repaired under a frozen full-snapshot watermark.
/// This is a FullSnapshot sub-state, not a separate checkpoint mode.
#[derive(Debug, Clone)]
pub struct FencedRepair {
    high: BTreeMap<u64, u64>,
    pending_windows: DirtyTimeWindows,
}

impl FencedRepair {
    /// Frozen high watermark `H` used as the snapshot upper bound for chunks.
    pub fn high(&self) -> &BTreeMap<u64, u64> {
        &self.high
    }

    /// Dirty windows still waiting to be repaired under `high`.
    pub fn pending_windows(&self) -> &DirtyTimeWindows {
        &self.pending_windows
    }
}

/// Lifecycle status of a registered backfill job.
///
/// ```text
/// Preparing -> Prepared -> Running -> BaseComplete(F) -> Finishing
/// ```
///
/// `Preparing` is the atomic prepare reservation: it is installed under the
/// short state lock *before* any async staging-table existence check / create
/// / plan building, so two concurrent prepares can never both observe an
/// absent table and independently create it. `Preparing { staging_may_exist:
/// false }` means a prepare is in flight (an exact-duplicate prepare returns
/// Busy instead of independently creating); `Preparing { staging_may_exist:
/// true }` means a previous attempt may have created the table (ambiguous
/// create failure) and the job keeps ownership — the table is never anonymous
/// and a re-prepare resumes the attempt instead of erroring.
///
/// `Prepared` jobs may start (or re-start after a failed run); a `Running`
/// job is executing its Base query and may not be entered again; `BaseComplete`
/// carries the frozen scan-open watermark F and is final — a late or
/// concurrent run must never overwrite it. `Finishing` is the atomic cleanup
/// transition (staging DROP in flight) that only a `BaseComplete` job may
/// enter; from `Finishing` a job may only return to `BaseComplete` (drop
/// failure, retryable) or be removed (drop success). Runs and prepares are
/// rejected while `Finishing`, and a second finish is rejected.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
pub enum BackfillJobStatus {
    /// Atomic prepare reservation installed under the state lock before any
    /// async staging-table work. `staging_may_exist` records whether a
    /// previous attempt may have created the table (ambiguous create failure);
    /// when `false` the prepare is in flight and duplicates return Busy.
    Preparing { staging_may_exist: bool },
    /// Registered and prepared; the staging table exists and the Base query
    /// may be (re)started.
    Prepared,
    /// A Base query is currently executing; no other run may start.
    Running,
    /// The Base query succeeded; the frozen scan-open watermark F is recorded
    /// and the job is final.
    BaseComplete,
    /// Cleanup (staging DROP) is in flight; the job may only return to
    /// `BaseComplete` on drop failure or be removed on drop success.
    Finishing,
}

/// A registered Phase-1 two-phase backfill job.
///
/// Holds everything the staging side of a backfill needs: the aligned
/// event-time range, the staging table it writes into, and the frozen
/// scan-open watermark `F` captured by
/// [`crate::batching_mode::task::BatchingTask::run_backfill_base`].
///
/// The identity (`job_id`, `range`, `staging_table_name`, `staging_table_id`)
/// is immutable once registered; re-registering the same `job_id` with a
/// different identity fails closed.
///
/// This is deliberately NOT a checkpoint sub-state: it never affects
/// `checkpoint_mode`, the active sink, or normal incremental execution.
#[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
#[derive(Debug, Clone)]
pub struct BackfillJob {
    pub job_id: u64,
    /// Aligned event-time range `[start, end)` covered by this job.
    pub range: (Timestamp, Timestamp),
    /// Staging table `greptime_private.__flow_backfill_<flow_id>_<job_id>`.
    pub staging_table_name: [String; 3],
    /// Resolved id of the staging table, used to exclude it from source
    /// incremental bounds via `flow.internal_non_source_table_ids` when a
    /// later phase scans it.
    pub staging_table_id: Option<TableId>,
    /// Frozen scan-open terminal watermark F captured by `run_backfill_base`.
    /// `None` until the Base query succeeds; a failed Phase 1 keeps the
    /// staging table and the registered job so the caller may retry or clean
    /// up explicitly.
    pub frozen_watermark: Option<BTreeMap<u64, u64>>,
    /// Lifecycle status; see [`BackfillJobStatus`].
    pub status: BackfillJobStatus,
}

/// Outcome of [`TaskState::begin_backfill_prepare`].
#[derive(Debug, Clone)]
#[allow(dead_code)] // Phase 2 wires the caller; Phase 1 ships the primitive + tests.
pub enum PrepareReservation {
    /// A fresh `Preparing { staging_may_exist: false }` reservation was
    /// installed for a brand-new job; the caller owns the identity and must
    /// drive the async staging-table existence check / create / plan build,
    /// then call [`TaskState::finish_backfill_prepare`] (or
    /// [`TaskState::mark_staging_may_exist`] /
    /// [`TaskState::cancel_backfill_prepare`] on failure).
    Reserved(BackfillJob),
    /// An exact-duplicate re-prepare of a `Prepared` or `BaseComplete` job:
    /// the registered job is returned untouched (status and frozen watermark F
    /// preserved) and only the plan is rebuilt. No reservation is held.
    Existing(BackfillJob),
    /// A previous prepare attempt left the job in
    /// `Preparing { staging_may_exist: true }`: the reservation is still held
    /// and the caller may resume the attempt (re-check existence, then create
    /// or adopt its own table) under the already-installed reservation.
    Resuming(BackfillJob),
}

/// Filter Expression's information
#[derive(Debug, Clone)]
pub struct FilterExprInfo {
    pub expr: datafusion_expr::Expr,
    pub col_name: String,
    pub time_ranges: Vec<(Timestamp, Timestamp)>,
    pub window_size: chrono::Duration,
}

impl FilterExprInfo {
    pub fn total_window_length(&self) -> chrono::Duration {
        self.time_ranges
            .iter()
            .fold(chrono::Duration::zero(), |acc, (start, end)| {
                acc + end.sub(start).unwrap_or(chrono::Duration::zero())
            })
    }

    pub fn predicate_for_col(
        &self,
        col_name: &str,
    ) -> Result<Option<datafusion_expr::Expr>, Error> {
        use datafusion_common::Column;
        use datafusion_expr::{Expr, lit};

        let mut expr_lst = Vec::with_capacity(self.time_ranges.len());
        for (start, end) in &self.time_ranges {
            let lower = to_df_literal(*start)?;
            let upper = to_df_literal(*end)?;
            let filter_col = || Expr::Column(Column::new_unqualified(col_name));
            expr_lst.push(
                filter_col()
                    .gt_eq(lit(lower))
                    .and(filter_col().lt(lit(upper))),
            );
        }

        Ok(expr_lst.into_iter().reduce(|a, b| a.or(b)))
    }
}

#[cfg(test)]
mod test {
    use pretty_assertions::assert_eq;
    use session::context::QueryContext;

    use super::*;
    use crate::batching_mode::time_window::find_time_window_expr;
    use crate::batching_mode::utils::sql_to_df_plan;
    use crate::test_utils::create_test_query_engine;

    #[test]
    fn test_task_state_records_last_execution_time() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);

        assert_eq!(None, state.last_execution_time_millis());
        state.after_query_exec(std::time::Duration::from_millis(1), false);
        assert_eq!(None, state.last_execution_time_millis());

        state.after_query_exec(std::time::Duration::from_millis(1), true);
        assert!(state.last_execution_time_millis().is_some());
    }

    #[test]
    fn test_backfill_job_lifecycle_register_begin_complete_fail_take() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);
        let job = BackfillJob {
            job_id: 7,
            range: (Timestamp::new_second(100), Timestamp::new_second(200)),
            staging_table_name: [
                "greptime".to_string(),
                "greptime_private".to_string(),
                "__flow_backfill_1_7".to_string(),
            ],
            staging_table_id: Some(2048),
            frozen_watermark: None,
            status: BackfillJobStatus::Prepared,
        };
        state.register_backfill_job(&job).unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert!(state.backfill_jobs()[0].frozen_watermark.is_none());
        assert_eq!(state.backfill_jobs()[0].status, BackfillJobStatus::Prepared);

        // Exact duplicate register is idempotent and preserves state.
        state.register_backfill_job(&job).unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);

        // Same job_id with a different identity fails closed.
        let mut mismatched = job.clone();
        mismatched.range = (Timestamp::new_second(0), Timestamp::new_second(100));
        let err = state.register_backfill_job(&mismatched).unwrap_err();
        assert!(format!("{err:?}").contains("different identity"));

        // begin: Prepared -> Running.
        let running = state.begin_backfill_run(7).unwrap();
        assert_eq!(running.job_id, 7);
        assert_eq!(state.backfill_jobs()[0].status, BackfillJobStatus::Running);

        // A second concurrent run is rejected.
        let err = state.begin_backfill_run(7).unwrap_err();
        assert!(format!("{err:?}").contains("already Running"));

        // complete: Running -> BaseComplete(F).
        state
            .complete_backfill_run(7, BTreeMap::from([(1, 10), (2, 20)]))
            .unwrap();
        assert_eq!(
            state.backfill_jobs()[0].frozen_watermark,
            Some(BTreeMap::from([(1, 10), (2, 20)]))
        );
        assert_eq!(
            state.backfill_jobs()[0].status,
            BackfillJobStatus::BaseComplete
        );

        // A completed job cannot be re-run: the recorded F is final.
        let err = state.begin_backfill_run(7).unwrap_err();
        assert!(format!("{err:?}").contains("already BaseComplete"));
        // A late completion from a stale run cannot overwrite F either.
        let err = state
            .complete_backfill_run(7, BTreeMap::from([(9, 99)]))
            .unwrap_err();
        assert!(format!("{err:?}").contains("cannot complete"));
        assert_eq!(
            state.backfill_jobs()[0].frozen_watermark,
            Some(BTreeMap::from([(1, 10), (2, 20)]))
        );

        // Unknown ids fail closed on every transition.
        assert!(state.begin_backfill_run(99).is_err());
        assert!(state.complete_backfill_run(99, BTreeMap::new()).is_err());
        assert!(state.fail_backfill_run(99).is_err());

        // take removes the job and returns it.
        let taken = state.take_backfill_job(7).unwrap();
        assert_eq!(taken.job_id, 7);
        assert!(state.backfill_jobs().is_empty());
        assert!(state.take_backfill_job(7).is_none());
    }

    #[test]
    fn test_backfill_job_failed_run_returns_to_prepared_for_retry() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);
        let job = BackfillJob {
            job_id: 8,
            range: (Timestamp::new_second(0), Timestamp::new_second(100)),
            staging_table_name: [
                "greptime".to_string(),
                "greptime_private".to_string(),
                "__flow_backfill_1_8".to_string(),
            ],
            staging_table_id: None,
            frozen_watermark: None,
            status: BackfillJobStatus::Prepared,
        };
        state.register_backfill_job(&job).unwrap();

        state.begin_backfill_run(8).unwrap();
        assert_eq!(state.backfill_jobs()[0].status, BackfillJobStatus::Running);

        // A failed Base query returns the job to Prepared (staging kept) so
        // the caller can retry.
        state.fail_backfill_run(8).unwrap();
        assert_eq!(state.backfill_jobs()[0].status, BackfillJobStatus::Prepared);
        assert!(state.backfill_jobs()[0].frozen_watermark.is_none());

        // Retry: begin again works, and this time the run completes.
        state.begin_backfill_run(8).unwrap();
        state
            .complete_backfill_run(8, BTreeMap::from([(1, 10)]))
            .unwrap();
        assert_eq!(
            state.backfill_jobs()[0].status,
            BackfillJobStatus::BaseComplete
        );

        // Failing a non-Running job fails closed.
        let err = state.fail_backfill_run(8).unwrap_err();
        assert!(format!("{err:?}").contains("cannot fail back"));
    }

    fn backfill_job_prepared(job_id: u64) -> BackfillJob {
        BackfillJob {
            job_id,
            range: (Timestamp::new_second(0), Timestamp::new_second(100)),
            staging_table_name: [
                "greptime".to_string(),
                "greptime_private".to_string(),
                format!("__flow_backfill_1_{job_id}"),
            ],
            staging_table_id: Some(2048),
            frozen_watermark: None,
            status: BackfillJobStatus::Prepared,
        }
    }

    #[test]
    fn test_backfill_prepare_reservation_lifecycle() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);
        let name = [
            "greptime".to_string(),
            "greptime_private".to_string(),
            "__flow_backfill_1_9".to_string(),
        ];

        // Fresh job: a Preparing reservation is installed.
        let reservation = state
            .begin_backfill_prepare(
                9,
                (Timestamp::new_second(0), Timestamp::new_second(100)),
                name.clone(),
            )
            .unwrap();
        let PrepareReservation::Reserved(reserved) = reservation else {
            panic!("expected Reserved reservation, got {reservation:?}");
        };
        assert_eq!(reserved.job_id, 9);
        assert_eq!(
            reserved.status,
            BackfillJobStatus::Preparing {
                staging_may_exist: false
            }
        );
        assert_eq!(reserved.staging_table_id, None);
        assert_eq!(state.backfill_jobs().len(), 1);

        // An exact-duplicate prepare while the reservation is in flight is
        // Busy: it must not independently create the table.
        let err = state
            .begin_backfill_prepare(
                9,
                (Timestamp::new_second(0), Timestamp::new_second(100)),
                name.clone(),
            )
            .unwrap_err();
        assert!(format!("{err:?}").contains("already being prepared"));

        // A different range for the same job fails closed instead of replacing
        // the reservation.
        let err = state
            .begin_backfill_prepare(
                9,
                (Timestamp::new_second(0), Timestamp::new_second(200)),
                name.clone(),
            )
            .unwrap_err();
        assert!(format!("{err:?}").contains("different identity"));

        // A failed create keeps ownership: Preparing{staging_may_exist:true}.
        state.mark_staging_may_exist(9).unwrap();
        assert_eq!(
            state.backfill_jobs()[0].status,
            BackfillJobStatus::Preparing {
                staging_may_exist: true
            }
        );

        // A re-prepare now resumes (same reservation held) instead of erroring.
        let reservation = state
            .begin_backfill_prepare(
                9,
                (Timestamp::new_second(0), Timestamp::new_second(100)),
                name.clone(),
            )
            .unwrap();
        assert!(matches!(reservation, PrepareReservation::Resuming(_)));

        // Once the create is confirmed, the reservation moves to Prepared.
        let job = state.finish_backfill_prepare(9, 4096).unwrap();
        assert_eq!(job.status, BackfillJobStatus::Prepared);
        assert_eq!(job.staging_table_id, Some(4096));
        assert_eq!(state.backfill_jobs()[0].status, BackfillJobStatus::Prepared);

        // An exact-duplicate prepare of a Prepared job is idempotent.
        let reservation = state
            .begin_backfill_prepare(
                9,
                (Timestamp::new_second(0), Timestamp::new_second(100)),
                name.clone(),
            )
            .unwrap();
        assert!(matches!(reservation, PrepareReservation::Existing(_)));

        // A second finish_prepare on a non-Preparing job fails closed.
        let err = state.finish_backfill_prepare(9, 9999).unwrap_err();
        assert!(format!("{err:?}").contains("cannot finish preparing"));
    }

    #[test]
    fn test_backfill_cancel_prepare_removes_only_preparing_jobs() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);
        let name = [
            "greptime".to_string(),
            "greptime_private".to_string(),
            "__flow_backfill_1_10".to_string(),
        ];
        state
            .begin_backfill_prepare(
                10,
                (Timestamp::new_second(0), Timestamp::new_second(100)),
                name,
            )
            .unwrap();
        state
            .register_backfill_job(&backfill_job_prepared(11))
            .unwrap();

        // Cancelling the Preparing job removes exactly it.
        state.cancel_backfill_prepare(10).unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert_eq!(state.backfill_jobs()[0].job_id, 11);

        // Cancelling a non-Preparing (or unknown) job fails closed.
        let err = state.cancel_backfill_prepare(11).unwrap_err();
        assert!(format!("{err:?}").contains("not Preparing"));
        let err = state.cancel_backfill_prepare(999).unwrap_err();
        assert!(format!("{err:?}").contains("no registered backfill job"));
    }

    #[test]
    fn test_backfill_finish_lifecycle_base_complete_only() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);

        // Prepared jobs are NOT cleanable in Phase 1 (abort is separate).
        state
            .register_backfill_job(&backfill_job_prepared(12))
            .unwrap();
        let err = state.begin_backfill_finish(12).unwrap_err();
        assert!(
            format!("{err:?}").contains("only a BaseComplete job is cleanable"),
            "expected BaseComplete-only error, got {err:?}"
        );

        // Drive to BaseComplete via the run transitions.
        state.begin_backfill_run(12).unwrap();
        assert_eq!(state.backfill_jobs()[0].status, BackfillJobStatus::Running);
        // A finish while Running is rejected: cleanup must never race a run.
        let err = state.begin_backfill_finish(12).unwrap_err();
        assert!(format!("{err:?}").contains("only a BaseComplete job is cleanable"));
        state
            .complete_backfill_run(12, BTreeMap::from([(1, 99)]))
            .unwrap();

        // BaseComplete -> Finishing.
        let finishing = state.begin_backfill_finish(12).unwrap();
        assert_eq!(finishing.status, BackfillJobStatus::Finishing);
        assert_eq!(
            state.backfill_jobs()[0].status,
            BackfillJobStatus::Finishing
        );

        // While Finishing: a second finish, a run, and a re-prepare are all
        // rejected (no new generation can replace the job mid-cleanup).
        let err = state.begin_backfill_finish(12).unwrap_err();
        assert!(format!("{err:?}").contains("cannot finish"));
        let err = state.begin_backfill_run(12).unwrap_err();
        assert!(format!("{err:?}").contains("cannot start"));
        let err = state
            .begin_backfill_prepare(
                12,
                (Timestamp::new_second(0), Timestamp::new_second(100)),
                [
                    "greptime".to_string(),
                    "greptime_private".to_string(),
                    "__flow_backfill_1_12".to_string(),
                ],
            )
            .unwrap_err();
        assert!(format!("{err:?}").contains("cleanup is in flight"));

        // Drop failure: Finishing -> BaseComplete restore keeps the job.
        let restored = state.restore_backfill_finish(12).unwrap();
        assert_eq!(restored.status, BackfillJobStatus::BaseComplete);
        assert_eq!(
            state.backfill_jobs()[0].frozen_watermark,
            Some(BTreeMap::from([(1, 99)]))
        );

        // A second finish after restore is legitimate (retry), and the
        // successful drop CAS-removes exactly the Finishing job.
        state.begin_backfill_finish(12).unwrap();
        let removed = state.remove_backfill_job_if_finishing(12).unwrap();
        assert_eq!(removed.unwrap().job_id, 12);
        assert!(state.backfill_jobs().is_empty());
        // Removing an already-removed job is a no-op (Ok(None)).
        assert!(
            state
                .remove_backfill_job_if_finishing(12)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_backfill_finish_remove_refuses_non_finishing_job() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);
        state
            .register_backfill_job(&backfill_job_prepared(13))
            .unwrap();

        // CAS-remove only removes a Finishing job; a state change (here the
        // job is still Prepared) must refuse instead of removing it.
        let err = state.remove_backfill_job_if_finishing(13).unwrap_err();
        assert!(format!("{err:?}").contains("changed state while finishing"));
        assert_eq!(state.backfill_jobs().len(), 1);
    }

    #[test]
    fn test_backfill_prepare_rejects_running_and_finishing_jobs() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);
        let name = |id: u64| {
            [
                "greptime".to_string(),
                "greptime_private".to_string(),
                format!("__flow_backfill_1_{id}"),
            ]
        };

        // Running job: prepare is Busy.
        state
            .register_backfill_job(&backfill_job_prepared(14))
            .unwrap();
        state.begin_backfill_run(14).unwrap();
        let err = state
            .begin_backfill_prepare(
                14,
                (Timestamp::new_second(0), Timestamp::new_second(100)),
                name(14),
            )
            .unwrap_err();
        assert!(format!("{err:?}").contains("Base run is in flight"));
        state.fail_backfill_run(14).unwrap();
        state.begin_backfill_run(14).unwrap();
        state
            .complete_backfill_run(14, BTreeMap::from([(1, 5)]))
            .unwrap();

        // Finishing job: prepare is Busy.
        state.begin_backfill_finish(14).unwrap();
        let err = state
            .begin_backfill_prepare(
                14,
                (Timestamp::new_second(0), Timestamp::new_second(100)),
                name(14),
            )
            .unwrap_err();
        assert!(format!("{err:?}").contains("cleanup is in flight"));
        // Restore and clean up for a clean end state.
        state.restore_backfill_finish(14).unwrap();
        state.begin_backfill_finish(14).unwrap();
        state.remove_backfill_job_if_finishing(14).unwrap();
        assert!(state.backfill_jobs().is_empty());
    }

    #[test]
    fn test_merge_dirty_time_windows() {
        let merge_dist = DirtyTimeWindows::default().time_window_merge_threshold;
        let testcases = vec![
            // just enough to merge
            (
                vec![
                    Timestamp::new_second(0),
                    Timestamp::new_second((1 + merge_dist as i64) * 5 * 60),
                ],
                (chrono::Duration::seconds(5 * 60), None),
                BTreeMap::from([(
                    Timestamp::new_second(0),
                    Some(Timestamp::new_second((2 + merge_dist as i64) * 5 * 60)),
                )]),
                Some(
                    "((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:25:00' AS TIMESTAMP)))",
                ),
            ),
            // separate time window
            (
                vec![
                    Timestamp::new_second(0),
                    Timestamp::new_second((2 + merge_dist as i64) * 5 * 60),
                ],
                (chrono::Duration::seconds(5 * 60), None),
                BTreeMap::from([
                    (
                        Timestamp::new_second(0),
                        Some(Timestamp::new_second(5 * 60)),
                    ),
                    (
                        Timestamp::new_second((2 + merge_dist as i64) * 5 * 60),
                        Some(Timestamp::new_second((3 + merge_dist as i64) * 5 * 60)),
                    ),
                ]),
                Some(
                    "(((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:05:00' AS TIMESTAMP))) OR ((ts >= CAST('1970-01-01 00:25:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:30:00' AS TIMESTAMP))))",
                ),
            ),
            // overlapping
            (
                vec![
                    Timestamp::new_second(0),
                    Timestamp::new_second((merge_dist as i64) * 5 * 60),
                ],
                (chrono::Duration::seconds(5 * 60), None),
                BTreeMap::from([(
                    Timestamp::new_second(0),
                    Some(Timestamp::new_second((1 + merge_dist as i64) * 5 * 60)),
                )]),
                Some(
                    "((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:20:00' AS TIMESTAMP)))",
                ),
            ),
            // complex overlapping
            (
                vec![
                    Timestamp::new_second(0),
                    Timestamp::new_second((merge_dist as i64) * 3),
                    Timestamp::new_second((merge_dist as i64) * 3 * 2),
                ],
                (chrono::Duration::seconds(3), None),
                BTreeMap::from([(
                    Timestamp::new_second(0),
                    Some(Timestamp::new_second((merge_dist as i64) * 7)),
                )]),
                Some(
                    "((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:00:21' AS TIMESTAMP)))",
                ),
            ),
            // split range
            (
                Vec::from_iter((0..20).map(|i| Timestamp::new_second(i * 3)).chain(
                    std::iter::once(Timestamp::new_second(
                        60 + 3 * (DirtyTimeWindows::MERGE_DIST as i64 + 1),
                    )),
                )),
                (chrono::Duration::seconds(3), None),
                BTreeMap::from([
                    (Timestamp::new_second(0), Some(Timestamp::new_second(60))),
                    (
                        Timestamp::new_second(60 + 3 * (DirtyTimeWindows::MERGE_DIST as i64 + 1)),
                        Some(Timestamp::new_second(
                            60 + 3 * (DirtyTimeWindows::MERGE_DIST as i64 + 1) + 3,
                        )),
                    ),
                ]),
                Some(
                    "((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:01:00' AS TIMESTAMP)))",
                ),
            ),
            // split 2 min into 1 min
            (
                Vec::from_iter((0..40).map(|i| Timestamp::new_second(i * 3))),
                (chrono::Duration::seconds(3), None),
                BTreeMap::from([(
                    Timestamp::new_second(0),
                    Some(Timestamp::new_second(40 * 3)),
                )]),
                Some(
                    "((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:01:00' AS TIMESTAMP)))",
                ),
            ),
            // split 3s + 1min into 3s + 57s
            (
                Vec::from_iter(
                    std::iter::once(Timestamp::new_second(0))
                        .chain((0..40).map(|i| Timestamp::new_second(20 + i * 3))),
                ),
                (chrono::Duration::seconds(3), None),
                BTreeMap::from([
                    (Timestamp::new_second(0), Some(Timestamp::new_second(3))),
                    (Timestamp::new_second(20), Some(Timestamp::new_second(140))),
                ]),
                Some(
                    "(((ts >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:00:03' AS TIMESTAMP))) OR ((ts >= CAST('1970-01-01 00:00:20' AS TIMESTAMP)) AND (ts < CAST('1970-01-01 00:01:17' AS TIMESTAMP))))",
                ),
            ),
            // expired
            (
                vec![
                    Timestamp::new_second(0),
                    Timestamp::new_second((merge_dist as i64) * 5 * 60),
                ],
                (
                    chrono::Duration::seconds(5 * 60),
                    Some(Timestamp::new_second((merge_dist as i64) * 6 * 60)),
                ),
                BTreeMap::from([]),
                None,
            ),
        ];
        // let len = testcases.len();
        // let testcases = testcases[(len - 2)..(len - 1)].to_vec();
        for (lower_bounds, (window_size, expire_lower_bound), expected, expected_filter_expr) in
            testcases
        {
            let mut dirty = DirtyTimeWindows::default();
            dirty.add_lower_bounds(lower_bounds.into_iter());
            dirty
                .merge_dirty_time_windows(window_size, expire_lower_bound)
                .unwrap();
            assert_eq!(expected, dirty.windows);
            let filter_expr = dirty
                .gen_filter_exprs(
                    "ts",
                    expire_lower_bound,
                    window_size,
                    dirty.max_filter_num_per_query,
                    0,
                    None,
                )
                .unwrap()
                .map(|e| e.expr);

            let unparser = datafusion::sql::unparser::Unparser::default();
            let to_sql = filter_expr
                .as_ref()
                .map(|e| unparser.expr_to_sql(e).unwrap().to_string());
            assert_eq!(expected_filter_expr, to_sql.as_deref());
        }
    }

    #[test]
    fn test_merge_dirty_time_windows_with_bounded_ranges() {
        let window_size = chrono::Duration::seconds(5);
        let testcases = vec![
            // A contained bounded range must not shrink the containing window:
            // [0s, 15s) merged with nested [5s, 10s) stays [0s, 15s).
            (
                vec![
                    (Timestamp::new_second(0), Some(Timestamp::new_second(15))),
                    (Timestamp::new_second(5), Some(Timestamp::new_second(10))),
                ],
                BTreeMap::from([(Timestamp::new_second(0), Some(Timestamp::new_second(15)))]),
            ),
            // An unbounded dirty window nested in a bounded range must not
            // shrink the range either: [0s, 15s) merged with 3s (window end
            // 8s) stays [0s, 15s).
            (
                vec![
                    (Timestamp::new_second(0), Some(Timestamp::new_second(15))),
                    (Timestamp::new_second(3), None),
                ],
                BTreeMap::from([(Timestamp::new_second(0), Some(Timestamp::new_second(15)))]),
            ),
            // Disjoint bounded ranges far apart are kept separate.
            (
                vec![
                    (Timestamp::new_second(0), Some(Timestamp::new_second(5))),
                    (Timestamp::new_second(100), Some(Timestamp::new_second(110))),
                ],
                BTreeMap::from([
                    (Timestamp::new_second(0), Some(Timestamp::new_second(5))),
                    (Timestamp::new_second(100), Some(Timestamp::new_second(110))),
                ]),
            ),
            // Overlapping bounded ranges are unioned: [0s, 10s) and [5s, 20s)
            // become [0s, 20s).
            (
                vec![
                    (Timestamp::new_second(0), Some(Timestamp::new_second(10))),
                    (Timestamp::new_second(5), Some(Timestamp::new_second(20))),
                ],
                BTreeMap::from([(Timestamp::new_second(0), Some(Timestamp::new_second(20)))]),
            ),
        ];

        for (windows, expected) in testcases {
            let mut dirty = DirtyTimeWindows::default();
            for (start, end) in windows {
                dirty.add_window(start, end);
            }
            dirty.merge_dirty_time_windows(window_size, None).unwrap();
            assert_eq!(expected, dirty.windows);
        }

        // Expire bound handling for bounded ranges vs unbounded windows.
        let expire_testcases = vec![
            // A bounded range ending at the expire bound is fully expired.
            (
                vec![(Timestamp::new_second(0), Some(Timestamp::new_second(10)))],
                BTreeMap::from([]),
            ),
            // A bounded range ending before the expire bound is fully expired.
            (
                vec![(Timestamp::new_second(0), Some(Timestamp::new_second(5)))],
                BTreeMap::from([]),
            ),
            // A bounded range crossing the expire bound keeps its live
            // suffix: [0s, 15s) with expire 10s becomes [10s, 15s).
            (
                vec![(Timestamp::new_second(0), Some(Timestamp::new_second(15)))],
                BTreeMap::from([(Timestamp::new_second(10), Some(Timestamp::new_second(15)))]),
            ),
            // A bounded range starting at the expire bound is kept intact.
            (
                vec![(Timestamp::new_second(10), Some(Timestamp::new_second(15)))],
                BTreeMap::from([(Timestamp::new_second(10), Some(Timestamp::new_second(15)))]),
            ),
            // An unbounded window starting before the expire bound is
            // dropped, preserving the existing start-based behavior.
            (vec![(Timestamp::new_second(5), None)], BTreeMap::from([])),
            // An unbounded window starting at the expire bound is kept.
            (
                vec![(Timestamp::new_second(10), None)],
                BTreeMap::from([(Timestamp::new_second(10), None)]),
            ),
        ];

        for (windows, expected) in expire_testcases {
            let mut dirty = DirtyTimeWindows::default();
            for (start, end) in windows {
                dirty.add_window(start, end);
            }
            dirty
                .merge_dirty_time_windows(window_size, Some(Timestamp::new_second(10)))
                .unwrap();
            assert_eq!(expected, dirty.windows);
        }
    }

    #[tokio::test]
    async fn test_align_time_window() {
        type TimeWindow = (Timestamp, Option<Timestamp>);
        struct TestCase {
            sql: String,
            aligns: Vec<(TimeWindow, TimeWindow)>,
        }
        let testcases: Vec<TestCase> = vec![TestCase{
            sql: "SELECT date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window;".to_string(),
            aligns: vec![
                ((Timestamp::new_second(3), None), (Timestamp::new_second(0), None)),
                ((Timestamp::new_second(8), None), (Timestamp::new_second(5), None)),
                ((Timestamp::new_second(8), Some(Timestamp::new_second(10))), (Timestamp::new_second(5), Some(Timestamp::new_second(10)))),
                ((Timestamp::new_second(8), Some(Timestamp::new_second(9))), (Timestamp::new_second(5), Some(Timestamp::new_second(10)))),
            ],
        }];

        let query_engine = create_test_query_engine();
        let ctx = QueryContext::arc();
        for TestCase { sql, aligns } in testcases {
            let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), &sql, true)
                .await
                .unwrap();

            let (column_name, time_window_expr, _, df_schema) = find_time_window_expr(
                &plan,
                query_engine.engine_state().catalog_manager().clone(),
                ctx.clone(),
            )
            .await
            .unwrap();

            let time_window_expr = time_window_expr
                .map(|expr| {
                    TimeWindowExpr::from_expr(
                        &expr,
                        &column_name,
                        &df_schema,
                        &query_engine.engine_state().session_state(),
                    )
                })
                .transpose()
                .unwrap()
                .unwrap();

            for (before_align, expected_after_align) in aligns {
                let after_align = DirtyTimeWindows::align_time_window(
                    before_align.0,
                    before_align.1,
                    &time_window_expr,
                )
                .unwrap();
                assert_eq!(expected_after_align, after_align);
            }
        }
    }

    #[test]
    fn test_task_state_checkpoint_mode_and_advancement() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);

        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert!(state.checkpoints().is_empty());

        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));
        assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
        assert_eq!(
            state.checkpoints(),
            &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
        );

        state.mark_full_snapshot();
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert_eq!(
            state.checkpoints(),
            &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
        );
    }

    #[test]
    fn test_mark_full_snapshot_restores_pending_fenced_repair_windows() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));

        state
            .start_fenced_repair(BTreeMap::from([(1_u64, 10_u64)]))
            .unwrap();
        assert!(state.dirty_time_windows.is_empty());
        assert_eq!(
            state
                .pending_fenced_repair()
                .unwrap()
                .pending_windows()
                .len(),
            2
        );

        state.mark_full_snapshot();

        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert!(state.pending_fenced_repair().is_none());
        assert_eq!(state.dirty_time_windows.len(), 2);
    }

    #[test]
    fn test_disable_incremental_persists_full_snapshot_mode() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);

        assert!(!state.is_incremental_disabled());

        // After disable, mode becomes FullSnapshot and flag is set.
        state.disable_incremental();
        assert!(state.is_incremental_disabled());
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);

        // `advance_checkpoints` will NOT transition to Incremental when disabled.
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert_eq!(
            state.checkpoints(),
            &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
        );

        // `mark_full_snapshot` does not re-enable incremental.
        state.mark_full_snapshot();
        assert!(state.is_incremental_disabled());
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    }

    #[test]
    fn test_full_snapshot_checkpoint_advancement_requires_participating_regions() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let state = TaskState::new(query_ctx, rx);

        assert!(!state.can_advance_full_snapshot_checkpoints(&BTreeSet::new(), &HashMap::new()));
        assert!(!state.can_advance_full_snapshot_checkpoints(
            &BTreeSet::from([1_u64, 2_u64]),
            &HashMap::from([(1_u64, 10_u64)]),
        ));
        assert!(state.can_advance_full_snapshot_checkpoints(
            &BTreeSet::from([1_u64, 2_u64]),
            &HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]),
        ));
    }

    #[test]
    fn test_incremental_checkpoint_advancement_requires_participation_alignment() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));

        assert!(
            state.can_advance_incremental_checkpoints_with_participation(
                &BTreeSet::from([1_u64]),
                &HashMap::from([(1_u64, 11_u64)]),
            )
        );
        assert!(
            !state.can_advance_incremental_checkpoints_with_participation(
                &BTreeSet::from([1_u64, 2_u64]),
                &HashMap::from([(1_u64, 11_u64)]),
            )
        );
        assert!(
            !state.can_advance_incremental_checkpoints_with_participation(
                &BTreeSet::from([3_u64]),
                &HashMap::from([(3_u64, 11_u64)]),
            )
        );
        assert!(
            !state.can_advance_incremental_checkpoints_with_participation(
                &BTreeSet::from([1_u64]),
                &HashMap::from([(1_u64, 9_u64)]),
            )
        );
        assert!(
            state.can_advance_incremental_checkpoints_with_participation(
                &BTreeSet::from([1_u64, 2_u64]),
                &HashMap::from([(1_u64, 11_u64), (2_u64, 21_u64)]),
            )
        );

        state.disable_incremental();
        assert!(
            !state.can_advance_incremental_checkpoints_with_participation(
                &BTreeSet::from([1_u64, 2_u64]),
                &HashMap::from([(1_u64, 12_u64), (2_u64, 22_u64)]),
            )
        );
    }

    #[test]
    fn test_incremental_checkpoint_advancement_merges_participating_subset() {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);
        state.advance_checkpoints(HashMap::from([
            (1_u64, 10_u64),
            (2_u64, 20_u64),
            (3_u64, 30_u64),
        ]));

        state.advance_incremental_checkpoints_with_participation(
            &BTreeSet::from([1_u64, 3_u64]),
            HashMap::from([(1_u64, 12_u64), (3_u64, 35_u64)]),
        );

        assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
        assert_eq!(
            state.checkpoints(),
            &BTreeMap::from([(1_u64, 12_u64), (2_u64, 20_u64), (3_u64, 35_u64)])
        );
    }

    #[test]
    fn test_filter_expr_info_predicate_for_col_empty_ranges() {
        let filter = FilterExprInfo {
            expr: datafusion_expr::col("ts"),
            col_name: "ts".to_string(),
            time_ranges: vec![],
            window_size: chrono::Duration::seconds(1),
        };

        assert!(filter.predicate_for_col("time_window").unwrap().is_none());
    }

    #[test]
    fn test_filter_expr_info_predicate_for_col_single_range() {
        let filter = FilterExprInfo {
            expr: datafusion_expr::col("ts"),
            col_name: "ts".to_string(),
            time_ranges: vec![(Timestamp::new_second(0), Timestamp::new_second(1))],
            window_size: chrono::Duration::seconds(1),
        };

        let predicate = filter.predicate_for_col("time_window").unwrap().unwrap();
        let unparser = datafusion::sql::unparser::Unparser::default();
        assert_eq!(
            "((time_window >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (time_window < CAST('1970-01-01 00:00:01' AS TIMESTAMP)))",
            unparser.expr_to_sql(&predicate).unwrap().to_string()
        );
    }

    #[test]
    fn test_filter_expr_info_predicate_for_col_multiple_ranges() {
        let filter = FilterExprInfo {
            expr: datafusion_expr::col("ts"),
            col_name: "ts".to_string(),
            time_ranges: vec![
                (Timestamp::new_second(0), Timestamp::new_second(1)),
                (Timestamp::new_second(10), Timestamp::new_second(11)),
            ],
            window_size: chrono::Duration::seconds(1),
        };

        let predicate = filter.predicate_for_col("time_window").unwrap().unwrap();
        let unparser = datafusion::sql::unparser::Unparser::default();
        assert_eq!(
            "(((time_window >= CAST('1970-01-01 00:00:00' AS TIMESTAMP)) AND (time_window < CAST('1970-01-01 00:00:01' AS TIMESTAMP))) OR ((time_window >= CAST('1970-01-01 00:00:10' AS TIMESTAMP)) AND (time_window < CAST('1970-01-01 00:00:11' AS TIMESTAMP))))",
            unparser.expr_to_sql(&predicate).unwrap().to_string()
        );
    }

    /// Helper: create a `TaskState` whose `last_update_time` is a known duration in the past.
    fn state_with_past_update(age: Duration) -> TaskState {
        let query_ctx = QueryContext::arc();
        let (_tx, rx) = tokio::sync::oneshot::channel();
        let mut state = TaskState::new(query_ctx, rx);
        state.last_update_time = Instant::now() - age;
        state
    }

    #[test]
    fn test_short_incremental_cadence_uses_min_refresh() {
        // When prefer_short_incremental_cadence is true and dirty backlog is manageable,
        // the next start time should be last_update_time + min_refresh (short cadence),
        // ignoring the longer time_window_size.
        let state = state_with_past_update(Duration::from_secs(10));

        let time_window_size = Some(Duration::from_secs(60)); // large window
        let min_refresh = Duration::from_secs(5);
        let flow_id = 1;

        let result = state.get_next_start_query_time(
            flow_id,
            &time_window_size,
            min_refresh,
            None,
            20,
            true, // prefer_short_incremental_cadence
        );

        // With short cadence, result should be last_update_time + min_refresh.
        let expected = state.last_update_time + min_refresh;
        assert_eq!(result, expected);
    }

    #[test]
    fn test_short_incremental_cadence_respects_last_query_duration() {
        let mut state = state_with_past_update(Duration::from_secs(10));
        state.last_query_duration = Duration::from_secs(20);

        let time_window_size = Some(Duration::from_secs(60));
        let min_refresh = Duration::from_secs(5);
        let flow_id = 1;

        let result = state.get_next_start_query_time(
            flow_id,
            &time_window_size,
            min_refresh,
            None,
            20,
            true,
        );

        assert_eq!(result, state.last_update_time + state.last_query_duration);
    }

    #[test]
    fn test_short_incremental_cadence_respects_max_timeout() {
        let mut state = state_with_past_update(Duration::from_secs(10));
        state.last_query_duration = Duration::from_secs(20);

        let time_window_size = Some(Duration::from_secs(60));
        let min_refresh = Duration::from_secs(30);
        let max_timeout = Duration::from_secs(5);
        let flow_id = 1;

        let result = state.get_next_start_query_time(
            flow_id,
            &time_window_size,
            min_refresh,
            Some(max_timeout),
            20,
            true,
        );

        assert_eq!(result, state.last_update_time + max_timeout);
    }

    #[test]
    fn test_full_snapshot_ignores_short_cadence() {
        // When prefer_short_incremental_cadence is false (full snapshot mode),
        // the normal long-cadence based on time_window_size applies.
        let mut state = state_with_past_update(Duration::from_secs(10));
        // Make last_query_duration small so the lower bound (time_window_size) dominates.
        state.last_query_duration = Duration::from_secs(1);

        let time_window_size = Some(Duration::from_secs(60)); // large window
        let min_refresh = Duration::from_secs(5);
        let flow_id = 1;

        let result = state.get_next_start_query_time(
            flow_id,
            &time_window_size,
            min_refresh,
            None,
            20,
            false, // prefer_short_incremental_cadence = false
        );

        // With normal cadence, result should be last_update_time + time_window_size
        // (since last_query_duration < time_window_size).
        let expected = state.last_update_time + Duration::from_secs(60);
        assert_eq!(result, expected);
    }

    #[test]
    fn test_dirty_window_overflow_schedules_immediately_even_with_short_cadence() {
        // Dirty-window overflow must always schedule immediately,
        // regardless of prefer_short_incremental_cadence.
        let mut state = state_with_past_update(Duration::from_secs(10));
        // Create a very large dirty backlog.
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(3600)));

        let time_window_size = Some(Duration::from_secs(1)); // tiny window => overflow
        let min_refresh = Duration::from_secs(5);
        let flow_id = 1;

        // With short cadence flag.
        let result = state.get_next_start_query_time(
            flow_id,
            &time_window_size,
            min_refresh,
            None,
            1, // max 1 filter => tiny capacity
            true,
        );
        assert!(
            result <= Instant::now(),
            "dirty overflow should schedule immediately"
        );

        // Without short cadence flag — same behavior.
        let result2 = state.get_next_start_query_time(
            flow_id,
            &time_window_size,
            min_refresh,
            None,
            1,
            false,
        );
        assert!(
            result2 <= Instant::now(),
            "dirty overflow should schedule immediately"
        );
    }

    #[test]
    fn test_pending_fenced_repair_schedules_immediately() {
        let mut state = state_with_past_update(Duration::from_secs(10));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
        state
            .start_fenced_repair(BTreeMap::from([(1_u64, 10_u64)]))
            .unwrap();
        assert!(state.dirty_time_windows.is_empty());
        assert!(!state.fenced_repair_pending_is_empty());

        let result = state.get_next_start_query_time(
            1,
            &Some(Duration::from_secs(60)),
            Duration::from_secs(5),
            None,
            20,
            false,
        );

        assert!(
            result <= Instant::now(),
            "pending fenced repair backlog should schedule immediately"
        );
    }

    #[test]
    fn test_incremental_disabled_ignores_short_cadence() {
        // When prefer_short_incremental_cadence is true but the dirty backlog is
        // manageable, the short cadence is applied. This test verifies that the
        // caller-side guard (checkpoint_mode + !is_incremental_disabled) controls
        // whether short cadence is requested at all — when incremental is disabled,
        // the flag is false, and the long cadence applies.
        //
        // This simulates the case where the caller computed
        // prefer_short_incremental_cadence = false (e.g. incremental disabled
        // or FullSnapshot mode), so the long cadence is used.
        let mut state = state_with_past_update(Duration::from_secs(10));
        state.last_query_duration = Duration::from_secs(1);

        let time_window_size = Some(Duration::from_secs(60));
        let min_refresh = Duration::from_secs(5);
        let flow_id = 1;

        let result = state.get_next_start_query_time(
            flow_id,
            &time_window_size,
            min_refresh,
            None,
            20,
            false, // prefer_short_incremental_cadence = false
        );

        // With normal cadence, result should be last_update_time + time_window_size.
        let expected = state.last_update_time + Duration::from_secs(60);
        assert_eq!(result, expected);
    }
}
