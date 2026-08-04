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

use std::collections::HashMap;
use std::num::NonZeroUsize;

use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct CaseFile {
    pub(super) scenario: Scenario,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind")]
pub(super) enum Scenario {
    #[serde(rename = "direct_readable_sst")]
    DirectReadableSst(DirectReadableSstScenario),
    #[serde(rename = "prom_remote_write_then_query")]
    PromRemoteWriteThenQuery(PromRemoteWriteThenQueryScenario),
    #[serde(rename = "workload_scheduler")]
    WorkloadScheduler(WorkloadSchedulerScenario),
    #[serde(rename = "workload_scheduler_distributed")]
    WorkloadSchedulerDistributed(DistributedWorkloadSchedulerScenario),
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct DirectReadableSstScenario {
    #[serde(default)]
    pub(super) seed: Option<u64>,
    pub(super) tables: Vec<TableConfig>,
    pub(super) layout: LayoutConfig,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct PromRemoteWriteThenQueryScenario {
    #[serde(default)]
    pub(super) queries: Vec<serde_json::Value>,
    pub(super) remote_write: PromRemoteWritePlan,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct PromRemoteWritePlan {
    #[serde(default = "default_database")]
    pub(super) database: String,
    #[serde(alias = "metric_name")]
    pub(super) metric: String,
    #[serde(default = "default_physical_table")]
    pub(super) physical_table: String,
    #[serde(default = "default_series_count")]
    pub(super) series_count: u64,
    #[serde(default = "default_samples_per_series")]
    pub(super) samples_per_series: u64,
    #[serde(default = "default_start_unix_millis")]
    pub(super) start_unix_millis: i64,
    #[serde(default = "default_step_millis")]
    pub(super) step_millis: i64,
    #[serde(default = "default_chunk_series_count", alias = "batch_size")]
    pub(super) chunk_series_count: u64,
    #[serde(default = "default_timeout_seconds")]
    pub(super) timeout_seconds: u64,
    #[serde(default)]
    pub(super) sample_chunk_size: Option<u64>,
    #[serde(default = "default_flush_every_sample_chunks")]
    pub(super) flush_every_sample_chunks: u64,
    #[serde(default = "default_visibility_timeout_seconds")]
    pub(super) visibility_timeout_seconds: u64,
    #[serde(default)]
    pub(super) prom_store: PromStoreConfig,
    #[serde(default)]
    pub(super) value: ValueConfig,
    #[serde(default)]
    pub(super) storage: Option<StorageConfig>,
    #[serde(default)]
    pub(super) read_bench: Option<ReadBenchConfig>,
}

pub(super) fn default_database() -> String {
    "public".to_string()
}
pub(super) fn default_physical_table() -> String {
    "greptime_physical_table".to_string()
}
pub(super) fn default_series_count() -> u64 {
    8
}
pub(super) fn default_samples_per_series() -> u64 {
    30
}
pub(super) fn default_start_unix_millis() -> i64 {
    1_704_067_200_000
}
pub(super) fn default_step_millis() -> i64 {
    15_000
}
pub(super) fn default_chunk_series_count() -> u64 {
    8
}
pub(super) fn default_timeout_seconds() -> u64 {
    60
}
pub(super) fn default_flush_every_sample_chunks() -> u64 {
    1
}
pub(super) fn default_visibility_timeout_seconds() -> u64 {
    30
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct PromStoreConfig {
    #[serde(default = "default_pending_rows_flush_interval")]
    pub(super) pending_rows_flush_interval: String,
    #[serde(default = "default_max_batch_rows")]
    pub(super) max_batch_rows: u64,
    #[serde(default = "default_max_concurrent_flushes")]
    pub(super) max_concurrent_flushes: u64,
    #[serde(default = "default_worker_channel_capacity")]
    pub(super) worker_channel_capacity: u64,
    #[serde(default = "default_max_inflight_requests")]
    pub(super) max_inflight_requests: u64,
}

impl Default for PromStoreConfig {
    fn default() -> Self {
        Self {
            pending_rows_flush_interval: default_pending_rows_flush_interval(),
            max_batch_rows: default_max_batch_rows(),
            max_concurrent_flushes: default_max_concurrent_flushes(),
            worker_channel_capacity: default_worker_channel_capacity(),
            max_inflight_requests: default_max_inflight_requests(),
        }
    }
}

pub(super) fn default_pending_rows_flush_interval() -> String {
    "1s".to_string()
}
pub(super) fn default_max_batch_rows() -> u64 {
    100000
}
pub(super) fn default_max_concurrent_flushes() -> u64 {
    256
}
pub(super) fn default_worker_channel_capacity() -> u64 {
    65526
}
pub(super) fn default_max_inflight_requests() -> u64 {
    3000
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, ValueEnum, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "snake_case")]
pub(super) enum ValuePattern {
    Linear,
    Constant,
    Modulo,
    Unique,
    SeededRandom,
    RunLength,
    QuantizedSignal,
    SignalWithSporadicStalls,
    MixedSignalRepeated,
}

impl std::fmt::Display for ValuePattern {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            serde_json::to_value(self).unwrap().as_str().unwrap()
        )
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct ValueConfig {
    #[serde(default = "default_value_pattern")]
    pub(super) pattern: ValuePattern,
    #[serde(default)]
    pub(super) base: f64,
    #[serde(default = "default_value_step")]
    pub(super) step: f64,
    #[serde(default = "default_value_cardinality")]
    pub(super) cardinality: u64,
    #[serde(default)]
    pub(super) seed: u64,
    #[serde(default = "default_value_run_length")]
    pub(super) run_length: u64,
    #[serde(default = "default_value_stall_every")]
    pub(super) stall_every: u64,
    #[serde(default = "default_value_stall_length")]
    pub(super) stall_length: u64,
    #[serde(default = "default_value_mixed_every")]
    pub(super) mixed_every: u64,
}
impl Default for ValueConfig {
    fn default() -> Self {
        Self {
            pattern: default_value_pattern(),
            base: 0.0,
            step: default_value_step(),
            cardinality: default_value_cardinality(),
            seed: 0,
            run_length: default_value_run_length(),
            stall_every: default_value_stall_every(),
            stall_length: default_value_stall_length(),
            mixed_every: default_value_mixed_every(),
        }
    }
}
pub(super) fn default_value_pattern() -> ValuePattern {
    ValuePattern::Linear
}
pub(super) fn default_value_step() -> f64 {
    0.125
}
pub(super) fn default_value_cardinality() -> u64 {
    97
}
pub(super) fn default_value_run_length() -> u64 {
    8
}
pub(super) fn default_value_stall_every() -> u64 {
    100
}
pub(super) fn default_value_stall_length() -> u64 {
    16
}
pub(super) fn default_value_mixed_every() -> u64 {
    5
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct StorageConfig {
    #[serde(default = "default_true")]
    pub(super) inspect: bool,
    #[serde(default = "default_storage_column")]
    pub(super) column: String,
    #[serde(default)]
    pub(super) root_suffix: Option<String>,
    #[serde(default)]
    pub(super) include_metadata_files: bool,
    #[serde(default = "default_min_files")]
    pub(super) min_files: u64,
    #[serde(default = "default_min_files")]
    pub(super) min_files_with_column: u64,
    #[serde(default)]
    pub(super) require_encodings: Vec<String>,
    #[serde(default)]
    pub(super) forbid_encodings: Vec<String>,
    #[serde(default)]
    pub(super) max_total_file_size_bytes: Option<u64>,
    #[serde(default)]
    pub(super) max_column_compressed_size_bytes: Option<u64>,
    #[serde(default)]
    pub(super) max_column_uncompressed_size_bytes: Option<u64>,
    #[serde(default)]
    pub(super) max_candidate_total_file_size_regression_pct: Option<f64>,
    #[serde(default)]
    pub(super) max_candidate_column_compressed_size_regression_pct: Option<f64>,
    #[serde(default)]
    pub(super) max_candidate_column_uncompressed_size_regression_pct: Option<f64>,
    #[serde(skip_deserializing, default)]
    pub(super) planned_thresholds: Vec<StorageThresholdPlan>,
}

#[derive(Debug, Serialize)]
pub(super) struct StorageThresholdPlan {
    pub(super) threshold: String,
    pub(super) status: String,
    pub(super) value: serde_json::Value,
}

impl StorageConfig {
    pub(super) fn populate_planned_thresholds(&mut self) {
        let mut planned = Vec::new();
        planned.push(StorageThresholdPlan::new("min_files", self.min_files));
        planned.push(StorageThresholdPlan::new(
            "min_files_with_column",
            self.min_files_with_column,
        ));
        if !self.require_encodings.is_empty() {
            planned.push(StorageThresholdPlan::new(
                "require_encodings",
                &self.require_encodings,
            ));
        }
        if !self.forbid_encodings.is_empty() {
            planned.push(StorageThresholdPlan::new(
                "forbid_encodings",
                &self.forbid_encodings,
            ));
        }
        macro_rules! push_optional {
            ($name:literal, $value:expr) => {
                if let Some(value) = $value {
                    planned.push(StorageThresholdPlan::new($name, value));
                }
            };
        }
        push_optional!("max_total_file_size_bytes", self.max_total_file_size_bytes);
        push_optional!(
            "max_column_compressed_size_bytes",
            self.max_column_compressed_size_bytes
        );
        push_optional!(
            "max_column_uncompressed_size_bytes",
            self.max_column_uncompressed_size_bytes
        );
        push_optional!(
            "max_candidate_total_file_size_regression_pct",
            self.max_candidate_total_file_size_regression_pct
        );
        push_optional!(
            "max_candidate_column_compressed_size_regression_pct",
            self.max_candidate_column_compressed_size_regression_pct
        );
        push_optional!(
            "max_candidate_column_uncompressed_size_regression_pct",
            self.max_candidate_column_uncompressed_size_regression_pct
        );
        self.planned_thresholds = planned;
    }
}

impl StorageThresholdPlan {
    fn new<T: Serialize>(threshold: &str, value: T) -> Self {
        Self {
            threshold: threshold.to_string(),
            status: "planned".to_string(),
            value: serde_json::to_value(value).expect("storage threshold value must serialize"),
        }
    }
}
pub(super) fn default_true() -> bool {
    true
}
pub(super) fn default_storage_column() -> String {
    "greptime_value".to_string()
}
pub(super) fn default_min_files() -> u64 {
    1
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub(super) struct ReadBenchConfig {
    #[serde(default = "default_true")]
    pub(super) enabled: bool,
    #[serde(default = "default_true")]
    pub(super) parquetbench: bool,
    #[serde(default = "default_true")]
    pub(super) scanbench: bool,
    #[serde(default = "default_iterations")]
    pub(super) iterations: u64,
    #[serde(default)]
    pub(super) projection: Vec<String>,
    #[serde(default = "default_parquet_reader")]
    pub(super) parquet_reader: String,
    #[serde(default = "default_scan_scanner")]
    pub(super) scan_scanner: String,
    #[serde(default = "default_parallelism")]
    pub(super) parallelism: u64,
    #[serde(default)]
    pub(super) max_files: Option<usize>,
    #[serde(flatten)]
    pub(super) thresholds: HashMap<String, serde_json::Value>,
}

impl Default for ReadBenchConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            parquetbench: true,
            scanbench: true,
            iterations: default_iterations(),
            projection: vec![],
            parquet_reader: default_parquet_reader(),
            scan_scanner: default_scan_scanner(),
            parallelism: default_parallelism(),
            max_files: None,
            thresholds: HashMap::new(),
        }
    }
}

pub(super) fn default_iterations() -> u64 {
    7
}
pub(super) fn default_parquet_reader() -> String {
    "direct".to_string()
}
pub(super) fn default_scan_scanner() -> String {
    "seq".to_string()
}
pub(super) fn default_parallelism() -> u64 {
    1
}

impl Scenario {
    pub(super) fn kind(&self) -> &'static str {
        match self {
            Scenario::DirectReadableSst(_) => "direct_readable_sst",
            Scenario::PromRemoteWriteThenQuery(_) => "prom_remote_write_then_query",
            Scenario::WorkloadScheduler(_) => "workload_scheduler",
            Scenario::WorkloadSchedulerDistributed(_) => "workload_scheduler_distributed",
        }
    }

    pub(super) fn direct_readable_sst(&self) -> &DirectReadableSstScenario {
        match self {
            Scenario::DirectReadableSst(scenario) => scenario,
            _ => panic!("scenario is not direct_readable_sst"),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct TableConfig {
    pub(super) database: String,
    pub(super) name: String,
    pub(super) engine: String,
    #[serde(default)]
    pub(super) append_mode: Option<bool>,
    #[serde(default)]
    pub(super) sst_format: Option<String>,
    pub(super) primary_key: Vec<String>,
    pub(super) time_index: String,
    pub(super) columns: Vec<ColumnConfig>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct ColumnConfig {
    pub(super) name: String,
    #[serde(rename = "type")]
    pub(super) ty: String,
    pub(super) semantic: String,
    pub(super) distribution: Option<Distribution>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind")]
pub(super) enum Distribution {
    #[serde(rename = "cardinality")]
    Cardinality {
        values: NonZeroUsize,
        prefix: String,
    },
    #[serde(rename = "deterministic_wave")]
    DeterministicWave { min: f64, max: f64 },
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct LayoutConfig {
    pub(super) regions: usize,
    pub(super) sst_count: usize,
    pub(super) rows_per_sst: usize,
    pub(super) row_group_size: usize,
    pub(super) series_count: NonZeroUsize,
    pub(super) start_unix_nanos: i64,
    pub(super) step_nanos: i64,
    pub(super) time_range_layout: String,
    pub(super) series_layout: String,
}

// ---------------------------------------------------------------------------
// Workload Scheduler scenario
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WorkloadSchedulerScenario {
    #[serde(default = "default_database")]
    pub(super) database: String,
    pub(super) iterations: u64,
    #[serde(default)]
    pub(super) warmup_seconds: u64,
    pub(super) duration_seconds: u64,
    pub(super) drain_timeout_seconds: u64,
    #[serde(default = "default_scrape_interval_seconds")]
    pub(super) scrape_interval_seconds: f64,
    pub(super) runtime: RuntimeConfig,
    pub(super) scheduler: SchedulerConfig,
    pub(super) targets: Vec<TargetConfig>,
    pub(super) data: DataConfig,
    pub(super) tables: TablesConfig,
    pub(super) query: QueryConfig,
    pub(super) write: WriteConfig,
    pub(super) phases: Vec<PhaseConfig>,
    pub(super) gates: PerformanceGate,
    // Derived read-only fields — populated during validation, not accepted from TOML.
    #[serde(skip_deserializing, default)]
    pub(super) expected_write_share: f64,
    #[serde(skip_deserializing, default)]
    pub(super) expected_scrape_count: u64,
    #[serde(skip_deserializing, default)]
    pub(super) effective_max_active_polls: u64,
}

pub(super) fn default_scrape_interval_seconds() -> f64 {
    1.0
}

/// The Kubernetes distributed E2E scheduler scenario.  The flattened common
/// scheduler fields deliberately retain the local fixture vocabulary; topology and
/// scheduling ownership are normalized here rather than guessed by Python.
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DistributedWorkloadSchedulerScenario {
    #[serde(flatten)]
    pub(super) common: WorkloadSchedulerScenario,
    pub(super) topology: DistributedTopology,
    #[serde(default = "default_max_polls")]
    pub(super) max_polls: u64,
    #[serde(skip_deserializing, default)]
    pub(super) target_order: Vec<String>,
}

fn default_max_polls() -> u64 {
    16
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DistributedTopology {
    pub(super) frontend: RoleResources,
    pub(super) metasrv: RoleResources,
    pub(super) datanode: RoleResources,
    pub(super) loadgen: RoleResources,
    #[serde(default)]
    pub(super) node_selection: NodeSelection,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RoleResources {
    pub(super) replicas: u64,
    pub(super) cpu: String,
    pub(super) memory: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NodeSelection {
    #[serde(default = "default_true")]
    pub(super) distinct_datanodes: bool,
    #[serde(default = "default_true")]
    pub(super) prefer_loadgen_distinct: bool,
}

impl Default for NodeSelection {
    fn default() -> Self {
        Self {
            distinct_datanodes: true,
            prefer_loadgen_distinct: true,
        }
    }
}

impl DistributedWorkloadSchedulerScenario {
    pub(super) fn validate(&mut self) -> Result<(), String> {
        self.common.validate()?;
        let mut errs = Vec::new();
        if self.topology.frontend.replicas != 1 {
            errs.push("topology.frontend.replicas must be exactly 1".to_string());
        }
        if self.topology.metasrv.replicas != 1 {
            errs.push("topology.metasrv.replicas must be exactly 1".to_string());
        }
        if self.topology.datanode.replicas != 3 {
            errs.push("topology.datanode.replicas must be exactly 3".to_string());
        }
        if self.topology.loadgen.replicas != 1 {
            errs.push("topology.loadgen.replicas must be exactly 1".to_string());
        }
        for (name, role, cpu, memory) in [
            ("frontend", &self.topology.frontend, "1", "2Gi"),
            ("metasrv", &self.topology.metasrv, "1", "2Gi"),
            ("datanode", &self.topology.datanode, "2", "4Gi"),
            ("loadgen", &self.topology.loadgen, "2", "4Gi"),
        ] {
            if role.cpu != cpu || role.memory != memory {
                errs.push(format!(
                    "topology.{} resources must be cpu={} and memory={}",
                    name, cpu, memory
                ));
            }
        }
        if !self.topology.node_selection.distinct_datanodes {
            errs.push("topology.node_selection.distinct_datanodes must be true".to_string());
        }
        if self.max_polls == 0 {
            errs.push("max_polls must be positive".to_string());
        }
        if self.max_polls != self.common.scheduler.max_concurrent_polls {
            errs.push("max_polls must equal scheduler.max_concurrent_polls".to_string());
        }
        // Explicit sequential A/B/B/A order is emitted for every pair of
        // iterations.  The runner reverses it on odd iterations.
        self.target_order = vec![
            "baseline".to_string(),
            "scheduled".to_string(),
            "scheduled".to_string(),
            "baseline".to_string(),
        ];
        if errs.is_empty() {
            Ok(())
        } else {
            Err(errs.join("\n  - "))
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct RuntimeConfig {
    pub(super) global: u64,
    pub(super) compact: u64,
    pub(super) query: u64,
    pub(super) ingest: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SchedulerConfig {
    #[serde(default = "default_max_concurrent_polls")]
    pub(super) max_concurrent_polls: u64,
    pub(super) query_weight: u64,
    pub(super) write_weight: u64,
}

pub(super) fn default_max_concurrent_polls() -> u64 {
    16
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TargetConfig {
    pub(super) name: String,
    pub(super) scheduler_enabled: bool,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct DataConfig {
    pub(super) shards: u64,
    pub(super) seed_rows: u64,
    pub(super) seed_batch_size: u64,
    pub(super) seed_timestamp_millis: i64,
    pub(super) write_sequence_start_millis: i64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TablesConfig {
    pub(super) query: TableRole,
    pub(super) write: TableRole,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TableRole {
    pub(super) name: String,
    pub(super) partitions: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct QueryConfig {
    pub(super) sql: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct WriteConfig {
    pub(super) batch_size: u64,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PhaseConfig {
    pub(super) name: String,
    pub(super) query_workers: u64,
    pub(super) write_workers: u64,
    /// Per-phase write pacing delay in seconds. The built-in case uses
    /// 0 for query_only/write_only/saturated and 0.1 for light_write.
    /// Python runner reads this directly — never inferred from phase name.
    #[serde(default)]
    pub(super) write_delay_seconds: f64,
    // NOTE: query_share_fraction was removed as redundant — explicit
    // per-phase write_delay_seconds (set to 0.1 for light_write) already
    // owns pacing. The Rust planner does not use this field.
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PerformanceGate {
    pub(super) max_failure_rate: f64,
    pub(super) max_outstanding_requests: u64,
    pub(super) dual_backlog_lower: f64,
    pub(super) dual_backlog_upper: f64,
    pub(super) min_dual_backlog_interval_fraction: f64,
    pub(super) min_dual_backlog_polls_per_class: u64,
    pub(super) min_single_class_active_purity: f64,
    pub(super) min_light_write_query_share: f64,
    pub(super) active_within_scheduler_limit: bool,
    pub(super) max_capacity_normalized_regression_pct: f64,
}

// ---------------------------------------------------------------------------
// Validation helpers
// ---------------------------------------------------------------------------

/// Returns true if `s` is a nonempty ASCII alphanumeric-or-underscore identifier.
fn is_valid_ascii_identifier(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

impl WorkloadSchedulerScenario {
    /// Validates all constraints and populates derived read-only fields.
    /// Returns `Err` with a joined description of all problems.
    pub(super) fn validate(&mut self) -> Result<(), String> {
        let mut errs: Vec<String> = Vec::new();

        // ---- database ----
        if !is_valid_ascii_identifier(&self.database) {
            errs.push("database must be a nonempty ASCII identifier".to_string());
        }

        // ---- timing ----
        if self.iterations == 0 {
            errs.push("iterations must be positive".to_string());
        }
        // warmup nonnegative (allow zero)
        if self.duration_seconds == 0 {
            errs.push("duration_seconds must be positive".to_string());
        }
        if self.drain_timeout_seconds == 0 {
            errs.push("drain_timeout_seconds must be positive".to_string());
        }
        // warmup nonnegative (allow zero) — u64 is always finite
        // scrape interval must be finite and exactly 1.0
        if !self.scrape_interval_seconds.is_finite() {
            errs.push("scrape_interval_seconds must be finite".to_string());
        } else if (self.scrape_interval_seconds - 1.0).abs() > f64::EPSILON {
            errs.push("scrape_interval_seconds must be exactly 1.0".to_string());
        }
        // duration_seconds is u64, so it is always integral in scrape intervals

        // ---- runtime ----
        if self.runtime.global == 0 {
            errs.push("runtime.global must be positive".to_string());
        }
        if self.runtime.compact == 0 {
            errs.push("runtime.compact must be positive".to_string());
        }
        if self.runtime.query == 0 {
            errs.push("runtime.query must be positive".to_string());
        }
        if self.runtime.ingest == 0 {
            errs.push("runtime.ingest must be positive".to_string());
        }

        // ---- scheduler ----
        if self.scheduler.max_concurrent_polls == 0 {
            errs.push("scheduler.max_concurrent_polls must be positive".to_string());
        }
        if self.scheduler.query_weight == 0 {
            errs.push("scheduler.query_weight must be positive".to_string());
        }
        if self.scheduler.write_weight == 0 {
            errs.push("scheduler.write_weight must be positive".to_string());
        }
        // fixed 2:8 acceptance weights
        if self.scheduler.query_weight != 2 {
            errs.push(
                "scheduler.query_weight must be 2 (fixed 2:8 acceptance weights)".to_string(),
            );
        }
        if self.scheduler.write_weight != 8 {
            errs.push(
                "scheduler.write_weight must be 8 (fixed 2:8 acceptance weights)".to_string(),
            );
        }

        // ---- targets: exactly two, unique names, canonical names "baseline"/"scheduled"
        // with baseline=false/scheduled=true contract. Reject arbitrary names, missing
        // canonical names, duplicates with descriptive errors.
        if self.targets.len() != 2 {
            errs.push("must have exactly 2 targets".to_string());
        }
        let mut target_names = std::collections::HashSet::new();
        let mut enabled_true_count = 0;
        let mut enabled_false_count = 0;
        for (i, t) in self.targets.iter().enumerate() {
            if t.name.is_empty() {
                errs.push(format!("targets[{}].name must be nonempty", i));
            }
            if !target_names.insert(&t.name) {
                errs.push(format!("target name \"{}\" is duplicated", t.name));
            }
            if t.scheduler_enabled {
                enabled_true_count += 1;
            } else {
                enabled_false_count += 1;
            }
            // Canonical names must be exactly "baseline" or "scheduled".
            // Arbitrary names like "control"/"candidate" are rejected.
            match (t.name.as_str(), t.scheduler_enabled) {
                ("baseline", false) => {} // correct
                ("scheduled", true) => {} // correct
                ("baseline", true) => {
                    errs.push(format!(
                        "target \"baseline\" must have scheduler_enabled=false"
                    ));
                }
                ("scheduled", false) => {
                    errs.push(format!(
                        "target \"scheduled\" must have scheduler_enabled=true"
                    ));
                }
                (other, _) => {
                    errs.push(format!(
                        "unrecognized target name \"{}\"; only \"baseline\" and \"scheduled\" are allowed",
                        other
                    ));
                }
            }
        }
        // Require exactly one baseline (false) and one scheduled (true)
        if enabled_true_count != 1 {
            errs.push(format!(
                "must have exactly 1 target with scheduler_enabled=true, found {}",
                enabled_true_count
            ));
        }
        if enabled_false_count != 1 {
            errs.push(format!(
                "must have exactly 1 target with scheduler_enabled=false, found {}",
                enabled_false_count
            ));
        }

        // ---- data ----
        if self.data.shards == 0 {
            errs.push("data.shards must be positive".to_string());
        }
        if self.data.seed_rows == 0 {
            errs.push("data.seed_rows must be positive".to_string());
        }
        if self.data.seed_batch_size == 0 {
            errs.push("data.seed_batch_size must be positive".to_string());
        }

        // ---- tables ----
        if !is_valid_ascii_identifier(&self.tables.query.name) {
            errs.push("tables.query.name must be a nonempty ASCII identifier".to_string());
        }
        if !is_valid_ascii_identifier(&self.tables.write.name) {
            errs.push("tables.write.name must be a nonempty ASCII identifier".to_string());
        }
        if self.tables.query.name == self.tables.write.name {
            errs.push("table names must be unique between query and write roles".to_string());
        }
        if self.tables.query.partitions == 0 {
            errs.push("tables.query.partitions must be positive".to_string());
        } else if self.data.shards % self.tables.query.partitions != 0 {
            errs.push(format!(
                "data.shards ({}) must be divisible by tables.query.partitions ({})",
                self.data.shards, self.tables.query.partitions
            ));
        }
        if self.tables.write.partitions == 0 {
            errs.push("tables.write.partitions must be positive".to_string());
        } else if self.data.shards % self.tables.write.partitions != 0 {
            errs.push(format!(
                "data.shards ({}) must be divisible by tables.write.partitions ({})",
                self.data.shards, self.tables.write.partitions
            ));
        }

        // ---- query ----
        if self.query.sql.trim().is_empty() {
            errs.push("query.sql must be nonempty".to_string());
        }

        // ---- write ----
        if self.write.batch_size == 0 {
            errs.push("write.batch_size must be positive".to_string());
        }

        // ---- phases ----
        {
            let required_phase_names: [&str; 4] =
                ["query_only", "write_only", "light_write", "saturated"];

            if self.phases.len() != 4 {
                errs.push(format!(
                    "must have exactly 4 phases, found {}",
                    self.phases.len()
                ));
            }

            let mut phase_names = std::collections::HashSet::new();
            for (i, p) in self.phases.iter().enumerate() {
                if p.name.is_empty() {
                    errs.push(format!("phases[{}].name must be nonempty", i));
                }
                if !phase_names.insert(&p.name) {
                    errs.push(format!("phase name \"{}\" is duplicated", p.name));
                }
            }

            // Check that the required phase names are present (case sensitive)
            // and each has the correct semantics.
            let mut found_required = std::collections::HashSet::new();
            for p in &self.phases {
                if required_phase_names.contains(&p.name.as_str()) {
                    found_required.insert(p.name.as_str());
                }
                // validate phase workers
                if p.query_workers == 0 && p.write_workers == 0 {
                    errs.push(format!(
                        "phase \"{}\" must have at least one worker (query_workers or write_workers > 0)",
                        p.name
                    ));
                }
                // query_share_fraction was removed — no validation needed.
            }

            for required in &required_phase_names {
                if !found_required.contains(required) {
                    errs.push(format!(
                        "phase \"{}\" is required for a gated case",
                        required
                    ));
                }
            }

            // Specific semantic checks for required phases
            for p in &self.phases {
                match p.name.as_str() {
                    "query_only" => {
                        if p.write_workers != 0 {
                            errs.push(
                                "phase \"query_only\" must have write_workers == 0".to_string(),
                            );
                        }
                        if p.query_workers == 0 {
                            errs.push(
                                "phase \"query_only\" must have query_workers > 0".to_string(),
                            );
                        }
                    }
                    "write_only" => {
                        if p.query_workers != 0 {
                            errs.push(
                                "phase \"write_only\" must have query_workers == 0".to_string(),
                            );
                        }
                        if p.write_workers == 0 {
                            errs.push(
                                "phase \"write_only\" must have write_workers > 0".to_string(),
                            );
                        }
                    }
                    "light_write" => {
                        if p.query_workers == 0 || p.write_workers == 0 {
                            errs.push(
                                "phase \"light_write\" must have both workers > 0".to_string(),
                            );
                        }
                    }
                    "saturated" => {
                        if p.query_workers == 0 || p.write_workers == 0 {
                            errs.push("phase \"saturated\" must have both workers > 0".to_string());
                        }
                    }
                    _ => {}
                }
            }
        }

        // ---- gates ----
        {
            let w = &self.gates;

            // max_failure_rate in [0, 1], finite
            if !w.max_failure_rate.is_finite() {
                errs.push("gates.max_failure_rate must be finite".to_string());
            } else if !(0.0..=1.0).contains(&w.max_failure_rate) {
                errs.push("gates.max_failure_rate must be in [0, 1]".to_string());
            }
            // share bounds finite, ordered/in [0, 1] and containing derived 0.8
            if !w.dual_backlog_lower.is_finite() {
                errs.push("gates.dual_backlog_lower must be finite".to_string());
            } else if !(0.0..=1.0).contains(&w.dual_backlog_lower) {
                errs.push("gates.dual_backlog_lower must be in [0, 1]".to_string());
            }
            if !w.dual_backlog_upper.is_finite() {
                errs.push("gates.dual_backlog_upper must be finite".to_string());
            } else if !(0.0..=1.0).contains(&w.dual_backlog_upper) {
                errs.push("gates.dual_backlog_upper must be in [0, 1]".to_string());
            }
            if w.dual_backlog_lower > w.dual_backlog_upper {
                errs.push(format!(
                    "gates.dual_backlog_lower ({}) must be <= dual_backlog_upper ({})",
                    w.dual_backlog_lower, w.dual_backlog_upper
                ));
            }
            // Must contain derived 0.8
            let expected_ws = 0.8_f64;
            if w.dual_backlog_lower > expected_ws || w.dual_backlog_upper < expected_ws {
                errs.push(format!(
                    "gates dual_backlog bounds [{}, {}] must contain derived expected_write_share {}",
                    w.dual_backlog_lower, w.dual_backlog_upper, expected_ws
                ));
            }

            // min_dual_backlog_interval_fraction in [0, 1], finite
            if !w.min_dual_backlog_interval_fraction.is_finite() {
                errs.push("gates.min_dual_backlog_interval_fraction must be finite".to_string());
            } else if !(0.0..=1.0).contains(&w.min_dual_backlog_interval_fraction) {
                errs.push("gates.min_dual_backlog_interval_fraction must be in [0, 1]".to_string());
            }
            if w.min_dual_backlog_polls_per_class == 0 {
                errs.push("gates.min_dual_backlog_polls_per_class must be positive".to_string());
            }
            // min_single_class_active_purity in [0, 1], finite
            if !w.min_single_class_active_purity.is_finite() {
                errs.push("gates.min_single_class_active_purity must be finite".to_string());
            } else if !(0.0..=1.0).contains(&w.min_single_class_active_purity) {
                errs.push("gates.min_single_class_active_purity must be in [0, 1]".to_string());
            }
            // min_light_write_query_share in [0, 1], finite
            if !w.min_light_write_query_share.is_finite() {
                errs.push("gates.min_light_write_query_share must be finite".to_string());
            } else if !(0.0..=1.0).contains(&w.min_light_write_query_share) {
                errs.push("gates.min_light_write_query_share must be in [0, 1]".to_string());
            }
            // nonnegative finite regression budget
            if !w.max_capacity_normalized_regression_pct.is_finite() {
                errs.push(
                    "gates.max_capacity_normalized_regression_pct must be finite".to_string(),
                );
            } else if w.max_capacity_normalized_regression_pct < 0.0 {
                errs.push(
                    "gates.max_capacity_normalized_regression_pct must be nonnegative".to_string(),
                );
            }
        }

        if !errs.is_empty() {
            return Err(errs.join("\n  - "));
        }

        // ---- populate derived fields ----
        self.expected_write_share = 0.8;
        self.expected_scrape_count =
            (self.duration_seconds as f64 / self.scrape_interval_seconds + 1.0) as u64;
        self.effective_max_active_polls = self.scheduler.max_concurrent_polls;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// Verifies that the built-in TOML case parses and validates correctly.
    #[test]
    fn test_distributed_workload_scheduler_requires_exact_topology() {
        let toml_str = r#"
[scenario]
kind = "workload_scheduler_distributed"
iterations = 4
duration_seconds = 1
drain_timeout_seconds = 1
max_polls = 16
[scenario.topology.frontend]
replicas = 1
cpu = "1"
memory = "2Gi"
[scenario.topology.metasrv]
replicas = 1
cpu = "1"
memory = "2Gi"
[scenario.topology.datanode]
replicas = 3
cpu = "2"
memory = "4Gi"
[scenario.topology.loadgen]
replicas = 1
cpu = "2"
memory = "4Gi"
[scenario.runtime]
global = 1
compact = 1
query = 1
ingest = 1
[scenario.scheduler]
max_concurrent_polls = 16
query_weight = 2
write_weight = 8
[[scenario.targets]]
name = "baseline"
scheduler_enabled = false
[[scenario.targets]]
name = "scheduled"
scheduler_enabled = true
[scenario.data]
shards = 64
seed_rows = 1
seed_batch_size = 1
seed_timestamp_millis = 0
write_sequence_start_millis = 1
[scenario.tables.query]
name = "query_table"
partitions = 32
[scenario.tables.write]
name = "write_table"
partitions = 64
[scenario.query]
sql = "SELECT 1"
[scenario.write]
batch_size = 1
[[scenario.phases]]
name = "query_only"
query_workers = 1
write_workers = 0
[[scenario.phases]]
name = "write_only"
query_workers = 0
write_workers = 1
[[scenario.phases]]
name = "light_write"
query_workers = 1
write_workers = 1
[[scenario.phases]]
name = "saturated"
query_workers = 1
write_workers = 1
[scenario.gates]
max_failure_rate = 0.01
max_outstanding_requests = 0
dual_backlog_lower = 0.78
dual_backlog_upper = 0.82
min_dual_backlog_interval_fraction = 0.8
min_dual_backlog_polls_per_class = 1
min_single_class_active_purity = 0.9
min_light_write_query_share = 0.2
active_within_scheduler_limit = true
max_capacity_normalized_regression_pct = 5.0
"#;
        let mut case: CaseFile = toml::from_str(toml_str).unwrap();
        let Scenario::WorkloadSchedulerDistributed(scenario) = &mut case.scenario else {
            panic!("expected distributed scenario");
        };
        scenario.validate().unwrap();
        assert_eq!(
            scenario.target_order,
            ["baseline", "scheduled", "scheduled", "baseline"]
        );
        scenario.topology.datanode.replicas = 2;
        assert!(scenario.validate().unwrap_err().contains("exactly 3"));
    }

    #[test]
    fn test_workload_scheduler_case_toml_parses() {
        let toml_str = r#"
[scenario]
kind = "workload_scheduler"
iterations = 3
warmup_seconds = 10
duration_seconds = 60
drain_timeout_seconds = 30

[scenario.runtime]
global = 4
compact = 1
query = 4
ingest = 4

[scenario.scheduler]
max_concurrent_polls = 16
query_weight = 2
write_weight = 8

[[scenario.targets]]
name = "baseline"
scheduler_enabled = false

[[scenario.targets]]
name = "scheduled"
scheduler_enabled = true

[scenario.data]
shards = 64
seed_rows = 10000
seed_batch_size = 500
seed_timestamp_millis = 1700000000000
write_sequence_start_millis = 1800000000000

[scenario.tables.query]
name = "catio_scheduler_query_load"
partitions = 32

[scenario.tables.write]
name = "catio_scheduler_write_load"
partitions = 64

[scenario.query]
sql = "SELECT count(*) FROM catio_scheduler_query_load WHERE ts > 0"

[scenario.write]
batch_size = 32

[[scenario.phases]]
name = "query_only"
query_workers = 2
write_workers = 0
write_delay_seconds = 0

[[scenario.phases]]
name = "write_only"
query_workers = 0
write_workers = 1152
write_delay_seconds = 0

[[scenario.phases]]
name = "light_write"
query_workers = 2
write_workers = 1
write_delay_seconds = 0.1

[[scenario.phases]]
name = "saturated"
query_workers = 2
write_workers = 1152
write_delay_seconds = 0

[scenario.gates]
max_failure_rate = 0.01
max_outstanding_requests = 0
dual_backlog_lower = 0.78
dual_backlog_upper = 0.82
min_dual_backlog_interval_fraction = 0.80
min_dual_backlog_polls_per_class = 100
min_single_class_active_purity = 0.99
min_light_write_query_share = 0.20
active_within_scheduler_limit = true
max_capacity_normalized_regression_pct = 5.0
"#;

        let mut case: CaseFile = toml::from_str(toml_str).expect("TOML must parse");
        let scenario = match &mut case.scenario {
            Scenario::WorkloadScheduler(s) => s,
            _ => panic!("expected WorkloadScheduler scenario"),
        };

        scenario.validate().expect("validation must succeed");

        // check derived fields
        assert!((scenario.expected_write_share - 0.8).abs() < f64::EPSILON);
        assert_eq!(scenario.expected_scrape_count, 61); // 60/1 + 1
        assert_eq!(scenario.effective_max_active_polls, 16);
        // check write_delay_seconds on phases
        for p in &scenario.phases {
            match p.name.as_str() {
                "light_write" => assert!((p.write_delay_seconds - 0.1).abs() < f64::EPSILON),
                _ => assert!((p.write_delay_seconds - 0.0).abs() < f64::EPSILON),
            }
        }
    }

    #[test]
    fn test_workload_scheduler_rejects_unknown_fields() {
        let toml_str = r#"
[scenario]
kind = "workload_scheduler"
iterations = 3
warmup_seconds = 10
duration_seconds = 60
drain_timeout_seconds = 30
unknown_field = "bad"

[scenario.runtime]
global = 4
compact = 1
query = 4
ingest = 4

[scenario.scheduler]
max_concurrent_polls = 16
query_weight = 2
write_weight = 8

[[scenario.targets]]
name = "baseline"
scheduler_enabled = false

[[scenario.targets]]
name = "scheduled"
scheduler_enabled = true

[scenario.data]
shards = 64
seed_rows = 10000
seed_batch_size = 500
seed_timestamp_millis = 1700000000000
write_sequence_start_millis = 1800000000000

[scenario.tables.query]
name = "catio_scheduler_query_load"
partitions = 32

[scenario.tables.write]
name = "catio_scheduler_write_load"
partitions = 64

[scenario.query]
sql = "SELECT 1"

[scenario.write]
batch_size = 32

[[scenario.phases]]
name = "query_only"
query_workers = 2
write_workers = 0
write_delay_seconds = 0

[[scenario.phases]]
name = "write_only"
query_workers = 0
write_workers = 1152
write_delay_seconds = 0

[[scenario.phases]]
name = "light_write"
query_workers = 2
write_workers = 1
write_delay_seconds = 0.1

[[scenario.phases]]
name = "saturated"
query_workers = 2
write_workers = 1152
write_delay_seconds = 0

[scenario.gates]
max_failure_rate = 0.01
max_outstanding_requests = 0
dual_backlog_lower = 0.78
dual_backlog_upper = 0.82
min_dual_backlog_interval_fraction = 0.80
min_dual_backlog_polls_per_class = 100
min_single_class_active_purity = 0.99
min_light_write_query_share = 0.20
active_within_scheduler_limit = true
max_capacity_normalized_regression_pct = 5.0
"#;
        let result: Result<CaseFile, _> = toml::from_str(toml_str);
        assert!(result.is_err(), "unknown field should be rejected");
    }

    #[test]
    fn test_workload_scheduler_rejects_derived_fields_in_toml() {
        let toml_str = r#"
[scenario]
kind = "workload_scheduler"
iterations = 3
warmup_seconds = 10
duration_seconds = 60
drain_timeout_seconds = 30
expected_write_share = 0.9

[scenario.runtime]
global = 4
compact = 1
query = 4
ingest = 4

[scenario.scheduler]
max_concurrent_polls = 16
query_weight = 2
write_weight = 8

[[scenario.targets]]
name = "baseline"
scheduler_enabled = false

[[scenario.targets]]
name = "scheduled"
scheduler_enabled = true

[scenario.data]
shards = 64
seed_rows = 10000
seed_batch_size = 500
seed_timestamp_millis = 1700000000000
write_sequence_start_millis = 1800000000000

[scenario.tables.query]
name = "catio_scheduler_query_load"
partitions = 32

[scenario.tables.write]
name = "catio_scheduler_write_load"
partitions = 64

[scenario.query]
sql = "SELECT 1"

[scenario.write]
batch_size = 32

[[scenario.phases]]
name = "query_only"
query_workers = 2
write_workers = 0
write_delay_seconds = 0

[[scenario.phases]]
name = "write_only"
query_workers = 0
write_workers = 1152
write_delay_seconds = 0

[[scenario.phases]]
name = "light_write"
query_workers = 2
write_workers = 1
write_delay_seconds = 0.1

[[scenario.phases]]
name = "saturated"
query_workers = 2
write_workers = 1152
write_delay_seconds = 0

[scenario.gates]
max_failure_rate = 0.01
max_outstanding_requests = 0
dual_backlog_lower = 0.78
dual_backlog_upper = 0.82
min_dual_backlog_interval_fraction = 0.80
min_dual_backlog_polls_per_class = 100
min_single_class_active_purity = 0.99
min_light_write_query_share = 0.20
active_within_scheduler_limit = true
max_capacity_normalized_regression_pct = 5.0
"#;
        // expected_write_share is skip_deserializing, so TOML will fail with unknown field
        let result: Result<CaseFile, _> = toml::from_str(toml_str);
        assert!(result.is_err(), "derived field in TOML should be rejected");
    }

    #[test]
    fn test_workload_scheduler_validation_errors() {
        // missing required fields (no phases, no gates, etc.)
        let toml_str = r#"
[scenario]
kind = "workload_scheduler"
iterations = 0
duration_seconds = 0
drain_timeout_seconds = 0
scrape_interval_seconds = 0.5

[scenario.runtime]
global = 0
compact = 0
query = 0
ingest = 0

[scenario.scheduler]
max_concurrent_polls = 0
query_weight = 1
write_weight = 1

[[scenario.targets]]
name = "baseline"
scheduler_enabled = false

[scenario.data]
shards = 0
seed_rows = 0
seed_batch_size = 0
seed_timestamp_millis = 1700000000000
write_sequence_start_millis = 1800000000000

[scenario.tables.query]
name = ""
partitions = 0

[scenario.tables.write]
name = "catio_scheduler_write_load"
partitions = 0

[scenario.query]
sql = ""

[scenario.write]
batch_size = 0

[[scenario.phases]]
name = "query_only"
query_workers = 0
write_workers = 0

[scenario.gates]
max_failure_rate = -0.01
max_outstanding_requests = 0
dual_backlog_lower = 0.9
dual_backlog_upper = 0.7
min_dual_backlog_interval_fraction = 1.5
min_dual_backlog_polls_per_class = 0
min_single_class_active_purity = 1.5
min_light_write_query_share = -0.1
active_within_scheduler_limit = true
max_capacity_normalized_regression_pct = -1.0
"#;
        let mut case: CaseFile = toml::from_str(toml_str).expect("TOML must parse");
        let scenario = match &mut case.scenario {
            Scenario::WorkloadScheduler(s) => s,
            _ => panic!("expected WorkloadScheduler"),
        };
        let result = scenario.validate();
        assert!(result.is_err(), "validation must fail");
        let err = result.unwrap_err();
        // Should contain many distinct error messages
        assert!(err.contains("iterations must be positive"));
        assert!(err.contains("duration_seconds"));
        assert!(err.contains("scrape_interval_seconds"));
        assert!(err.contains("runtime.global"));
        assert!(err.contains("scheduler.max_concurrent_polls"));
        assert!(err.contains("query_weight must be 2"));
        assert!(err.contains("write_weight must be 8"));
        assert!(err.contains("must have exactly 2 targets"));
        assert!(err.contains("data.shards"));
        assert!(err.contains("tables.query.name"));
        assert!(err.contains("query.sql"));
        assert!(err.contains("write.batch_size"));
        assert!(err.contains("phase"));
        assert!(err.contains("gates.max_failure_rate"));
        assert!(err.contains("gates.dual_backlog_lower"));
        assert!(
            err.contains("tables.query.partitions must be positive"),
            "zero partitions must produce error without panic: {}",
            err
        );
        assert!(
            err.contains("tables.write.partitions must be positive"),
            "zero partitions must produce error without panic: {}",
            err
        );
    }

    #[test]
    fn test_workload_scheduler_rejects_invalid_target_booleans() {
        // Two targets both with scheduler_enabled=false
        let toml_str = r#"
[scenario]
kind = "workload_scheduler"
iterations = 3
warmup_seconds = 10
duration_seconds = 60
drain_timeout_seconds = 30

[scenario.runtime]
global = 4
compact = 1
query = 4
ingest = 4

[scenario.scheduler]
max_concurrent_polls = 16
query_weight = 2
write_weight = 8

[[scenario.targets]]
name = "baseline"
scheduler_enabled = false

[[scenario.targets]]
name = "scheduled"
scheduler_enabled = false

[scenario.data]
shards = 64
seed_rows = 10000
seed_batch_size = 500
seed_timestamp_millis = 1700000000000
write_sequence_start_millis = 1800000000000

[scenario.tables.query]
name = "catio_scheduler_query_load"
partitions = 32

[scenario.tables.write]
name = "catio_scheduler_write_load"
partitions = 64

[scenario.query]
sql = "SELECT 1"

[scenario.write]
batch_size = 32

[[scenario.phases]]
name = "query_only"
query_workers = 2
write_workers = 0
write_delay_seconds = 0

[[scenario.phases]]
name = "write_only"
query_workers = 0
write_workers = 1152
write_delay_seconds = 0

[[scenario.phases]]
name = "light_write"
query_workers = 2
write_workers = 1
write_delay_seconds = 0.1

[[scenario.phases]]
name = "saturated"
query_workers = 2
write_workers = 1152
write_delay_seconds = 0

[scenario.gates]
max_failure_rate = 0.01
max_outstanding_requests = 0
dual_backlog_lower = 0.78
dual_backlog_upper = 0.82
min_dual_backlog_interval_fraction = 0.80
min_dual_backlog_polls_per_class = 100
min_single_class_active_purity = 0.99
min_light_write_query_share = 0.20
active_within_scheduler_limit = true
max_capacity_normalized_regression_pct = 5.0
"#;
        let mut case: CaseFile = toml::from_str(toml_str).expect("TOML must parse");
        let scenario = match &mut case.scenario {
            Scenario::WorkloadScheduler(s) => s,
            _ => panic!("expected WorkloadScheduler"),
        };
        let result = scenario.validate();
        assert!(result.is_err(), "two false targets must be rejected");
        let err = result.unwrap_err();
        assert!(
            err.contains("scheduler_enabled=true"),
            "must complain about missing true target: {}",
            err
        );
    }

    #[test]
    fn test_workload_scheduler_rejects_swapped_target_meanings() {
        // Two targets: baseline=true (wrong), scheduled=false (wrong)
        let toml_str = r#"
[scenario]
kind = "workload_scheduler"
iterations = 3
warmup_seconds = 10
duration_seconds = 60
drain_timeout_seconds = 30

[scenario.runtime]
global = 4
compact = 1
query = 4
ingest = 4

[scenario.scheduler]
max_concurrent_polls = 16
query_weight = 2
write_weight = 8

[[scenario.targets]]
name = "baseline"
scheduler_enabled = true

[[scenario.targets]]
name = "scheduled"
scheduler_enabled = false

[scenario.data]
shards = 64
seed_rows = 10000
seed_batch_size = 500
seed_timestamp_millis = 1700000000000
write_sequence_start_millis = 1800000000000

[scenario.tables.query]
name = "catio_scheduler_query_load"
partitions = 32

[scenario.tables.write]
name = "catio_scheduler_write_load"
partitions = 64

[scenario.query]
sql = "SELECT 1"

[scenario.write]
batch_size = 32

[[scenario.phases]]
name = "query_only"
query_workers = 2
write_workers = 0
write_delay_seconds = 0

[[scenario.phases]]
name = "write_only"
query_workers = 0
write_workers = 1152
write_delay_seconds = 0

[[scenario.phases]]
name = "light_write"
query_workers = 2
write_workers = 1
write_delay_seconds = 0.1

[[scenario.phases]]
name = "saturated"
query_workers = 2
write_workers = 1152
write_delay_seconds = 0

[scenario.gates]
max_failure_rate = 0.01
max_outstanding_requests = 0
dual_backlog_lower = 0.78
dual_backlog_upper = 0.82
min_dual_backlog_interval_fraction = 0.80
min_dual_backlog_polls_per_class = 100
min_single_class_active_purity = 0.99
min_light_write_query_share = 0.20
active_within_scheduler_limit = true
max_capacity_normalized_regression_pct = 5.0
"#;
        let mut case: CaseFile = toml::from_str(toml_str).expect("TOML must parse");
        let scenario = match &mut case.scenario {
            Scenario::WorkloadScheduler(s) => s,
            _ => panic!("expected WorkloadScheduler"),
        };
        let result = scenario.validate();
        assert!(result.is_err(), "swapped target booleans must be rejected");
        let err = result.unwrap_err();
        assert!(
            err.contains("target \"baseline\" must have scheduler_enabled=false")
                && err.contains("target \"scheduled\" must have scheduler_enabled=true"),
            "must bind scheduler enablement to canonical target names: {}",
            err
        );
    }

    #[test]
    fn test_workload_scheduler_rejects_arbitrary_target_names() {
        // Using "control"/"candidate" instead of "baseline"/"scheduled"
        let toml_str = r#"
[scenario]
kind = "workload_scheduler"
iterations = 3
warmup_seconds = 10
duration_seconds = 60
drain_timeout_seconds = 30

[scenario.runtime]
global = 4
compact = 1
query = 4
ingest = 4

[scenario.scheduler]
max_concurrent_polls = 16
query_weight = 2
write_weight = 8

[[scenario.targets]]
name = "control"
scheduler_enabled = false

[[scenario.targets]]
name = "candidate"
scheduler_enabled = true

[scenario.data]
shards = 64
seed_rows = 10000
seed_batch_size = 500
seed_timestamp_millis = 1700000000000
write_sequence_start_millis = 1800000000000

[scenario.tables.query]
name = "catio_scheduler_query_load"
partitions = 32

[scenario.tables.write]
name = "catio_scheduler_write_load"
partitions = 64

[scenario.query]
sql = "SELECT 1"

[scenario.write]
batch_size = 32

[[scenario.phases]]
name = "query_only"
query_workers = 2
write_workers = 0
write_delay_seconds = 0

[[scenario.phases]]
name = "write_only"
query_workers = 0
write_workers = 1152
write_delay_seconds = 0

[[scenario.phases]]
name = "light_write"
query_workers = 2
write_workers = 1
write_delay_seconds = 0.1

[[scenario.phases]]
name = "saturated"
query_workers = 2
write_workers = 1152
write_delay_seconds = 0

[scenario.gates]
max_failure_rate = 0.01
max_outstanding_requests = 0
dual_backlog_lower = 0.78
dual_backlog_upper = 0.82
min_dual_backlog_interval_fraction = 0.80
min_dual_backlog_polls_per_class = 100
min_single_class_active_purity = 0.99
min_light_write_query_share = 0.20
active_within_scheduler_limit = true
max_capacity_normalized_regression_pct = 5.0
"#;
        let mut case: CaseFile = toml::from_str(toml_str).expect("TOML must parse");
        let scenario = match &mut case.scenario {
            Scenario::WorkloadScheduler(s) => s,
            _ => panic!("expected WorkloadScheduler"),
        };
        let result = scenario.validate();
        assert!(result.is_err(), "arbitrary target names must be rejected");
        let err = result.unwrap_err();
        assert!(
            err.contains("unrecognized target name"),
            "must complain about unrecognized names: {}",
            err
        );
        assert!(
            err.contains("control"),
            "must mention the specific unrecognized name: {}",
            err
        );
    }

    #[test]
    fn test_workload_scheduler_serialization() {
        let toml_str = r#"
[scenario]
kind = "workload_scheduler"
iterations = 3
warmup_seconds = 10
duration_seconds = 60
drain_timeout_seconds = 30

[scenario.runtime]
global = 4
compact = 1
query = 4
ingest = 4

[scenario.scheduler]
max_concurrent_polls = 16
query_weight = 2
write_weight = 8

[[scenario.targets]]
name = "baseline"
scheduler_enabled = false

[[scenario.targets]]
name = "scheduled"
scheduler_enabled = true

[scenario.data]
shards = 64
seed_rows = 10000
seed_batch_size = 500
seed_timestamp_millis = 1700000000000
write_sequence_start_millis = 1800000000000

[scenario.tables.query]
name = "catio_scheduler_query_load"
partitions = 32

[scenario.tables.write]
name = "catio_scheduler_write_load"
partitions = 64

[scenario.query]
sql = "SELECT count(*) FROM catio_scheduler_query_load WHERE ts > 0"

[scenario.write]
batch_size = 32

[[scenario.phases]]
name = "query_only"
query_workers = 2
write_workers = 0
write_delay_seconds = 0

[[scenario.phases]]
name = "write_only"
query_workers = 0
write_workers = 1152
write_delay_seconds = 0

[[scenario.phases]]
name = "light_write"
query_workers = 2
write_workers = 1
write_delay_seconds = 0.1

[[scenario.phases]]
name = "saturated"
query_workers = 2
write_workers = 1152
write_delay_seconds = 0

[scenario.gates]
max_failure_rate = 0.01
max_outstanding_requests = 0
dual_backlog_lower = 0.78
dual_backlog_upper = 0.82
min_dual_backlog_interval_fraction = 0.80
min_dual_backlog_polls_per_class = 100
min_single_class_active_purity = 0.99
min_light_write_query_share = 0.20
active_within_scheduler_limit = true
max_capacity_normalized_regression_pct = 5.0
"#;

        let mut case: CaseFile = toml::from_str(toml_str).expect("TOML must parse");
        let scenario = match &mut case.scenario {
            Scenario::WorkloadScheduler(s) => s,
            _ => panic!("expected WorkloadScheduler"),
        };
        scenario.validate().expect("validation must succeed");

        let json_str =
            serde_json::to_string_pretty(&case.scenario).expect("serialization must succeed");

        // The JSON output must contain derived fields – parse structurally instead
        // of matching on formatting.
        let json_val: serde_json::Value = serde_json::from_str(&json_str).expect("JSON must parse");
        assert!(
            (json_val["expected_write_share"].as_f64().unwrap() - 0.8).abs() < f64::EPSILON,
            "expected_write_share must be 0.8"
        );
        assert_eq!(json_val["expected_scrape_count"], 61);
        assert_eq!(json_val["effective_max_active_polls"], 16);

        // Must contain the scenario kind
        assert_eq!(json_val["kind"], "workload_scheduler");
    }

    #[test]
    fn test_workload_scheduler_rejects_nan_scrape_interval() {
        // TOML cannot represent NaN directly, but we can test that our finite
        // check exists. Use a known-bad float — serde_json NaN works via serde
        // untagged. Here we test via JSON round-trip of the struct.*/
        // Actually test via a TOML with the sentinel "nan" string as a float
        let toml_str = r#"
[scenario]
kind = "workload_scheduler"
iterations = 3
warmup_seconds = 10
duration_seconds = 60
drain_timeout_seconds = 30
scrape_interval_seconds = 1.0

[scenario.runtime]
global = 4
compact = 1
query = 4
ingest = 4

[scenario.scheduler]
max_concurrent_polls = 16
query_weight = 2
write_weight = 8

[[scenario.targets]]
name = "baseline"
scheduler_enabled = false

[[scenario.targets]]
name = "scheduled"
scheduler_enabled = true

[scenario.data]
shards = 64
seed_rows = 10000
seed_batch_size = 500
seed_timestamp_millis = 1700000000000
write_sequence_start_millis = 1800000000000

[scenario.tables.query]
name = "catio_scheduler_query_load"
partitions = 32

[scenario.tables.write]
name = "catio_scheduler_write_load"
partitions = 64

[scenario.query]
sql = "SELECT 1"

[scenario.write]
batch_size = 32

[[scenario.phases]]
name = "query_only"
query_workers = 2
write_workers = 0

[[scenario.phases]]
name = "write_only"
query_workers = 0
write_workers = 1152

[[scenario.phases]]
name = "light_write"
query_workers = 2
write_workers = 1

[[scenario.phases]]
name = "saturated"
query_workers = 2
write_workers = 1152

[scenario.gates]
max_failure_rate = 0.01
max_outstanding_requests = 0
dual_backlog_lower = 0.78
dual_backlog_upper = 0.82
min_dual_backlog_interval_fraction = 0.80
min_dual_backlog_polls_per_class = 100
min_single_class_active_purity = 0.99
min_light_write_query_share = 0.20
active_within_scheduler_limit = true
max_capacity_normalized_regression_pct = 5.0
"#;
        let mut case: CaseFile = toml::from_str(toml_str).expect("TOML must parse");
        let scenario = match &mut case.scenario {
            Scenario::WorkloadScheduler(s) => s,
            _ => panic!("expected WorkloadScheduler"),
        };
        // Directly set to NaN (simulates what would happen if TOML/JSON produced NaN)
        scenario.scrape_interval_seconds = f64::NAN;
        let result = scenario.validate();
        assert!(result.is_err(), "NaN scrape_interval should be rejected");
        assert!(
            result
                .unwrap_err()
                .contains("scrape_interval_seconds must be finite"),
            "NaN must produce finite error"
        );
    }
}
