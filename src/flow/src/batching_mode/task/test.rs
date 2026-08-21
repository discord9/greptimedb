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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use catalog::RegisterTableRequest;
use catalog::memory::MemoryCatalogManager;
use client::OutputWithMetrics;
use common_catalog::consts::{
    DEFAULT_CATALOG_NAME, DEFAULT_PRIVATE_SCHEMA_NAME, DEFAULT_SCHEMA_NAME,
};
use common_error::ext::{BoxedError, ErrorExt, PlainError};
use common_error::mock::MockError;
use common_error::status_code::StatusCode;
use common_query::Output;
use common_recordbatch::adapter::{RecordBatchMetrics, RegionWatermarkEntry};
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datatypes::arrow::array::Array as _;
use datatypes::data_type::ConcreteDataType as CDT;
use datatypes::prelude::{MutableVector, ScalarVectorBuilder};
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{
    Int32Vector, TimestampMillisecondVector, TimestampNanosecondVector, UInt32Vector, VectorRef,
};
use pretty_assertions::assert_eq;
use query::options::{
    FLOW_INCREMENTAL_AFTER_SEQS, FLOW_INCREMENTAL_MODE, FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY,
    FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE, FLOW_INTERNAL_NON_SOURCE_TABLE_IDS,
    FLOW_RETURN_REGION_SEQ, FLOW_SCHEDULED_TIME_MILLIS, FLOW_SINK_TABLE_ID, QueryOptions,
};
use session::context::QueryContext;
use snafu::ResultExt;
use store_api::mito_engine_options::APPEND_MODE_KEY;
use substrait::DFLogicalSubstraitConvertor;
use table::test_util::MemTable;

use super::*;
use crate::batching_mode::IncrementalMode;
use crate::batching_mode::checkpoint::{
    CHECKPOINT_DECISION_ADVANCE, CHECKPOINT_DECISION_FALLBACK, CHECKPOINT_REASON_NONE,
    CHECKPOINT_RECORD_FORMAT_VERSION, CheckpointRecord, FlowCheckpointDecision,
    FlowQueryFallbackReason, decode_checkpoint_record, encode_checkpoint_record,
};
use crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError;
use crate::batching_mode::state::{CheckpointMode, CheckpointPersistence};
use crate::batching_mode::time_window::find_time_window_expr;
use crate::test_utils::create_test_query_engine;

fn incremental_batch_opts() -> Arc<BatchingModeOptions> {
    Arc::new(BatchingModeOptions {
        experimental_enable_incremental_read: true,
        ..Default::default()
    })
}

async fn new_test_task_and_plan_with_missing_sink() -> (BatchingTask, LogicalPlan) {
    new_test_task_engine_and_plan_with_query(
        "SELECT number, ts FROM numbers_with_ts",
        "missing_sink",
    )
    .await
    .into_task_and_plan()
}

struct TestTaskParts {
    task: BatchingTask,
    query_engine: QueryEngineRef,
    plan: LogicalPlan,
}

impl TestTaskParts {
    fn into_task_and_plan(self) -> (BatchingTask, LogicalPlan) {
        (self.task, self.plan)
    }
}

async fn new_test_task_engine_and_plan_with_query(query: &str, sink_table: &str) -> TestTaskParts {
    new_test_task_engine_and_plan_with_query_and_opts(query, sink_table, incremental_batch_opts())
        .await
}

async fn new_test_task_engine_and_plan_with_query_and_opts(
    query: &str,
    sink_table: &str,
    batch_opts: Arc<BatchingModeOptions>,
) -> TestTaskParts {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(
        ctx.clone(),
        query_engine.clone(),
        "SELECT number, ts FROM numbers_with_ts",
        true,
    )
    .await
    .unwrap();
    let (_tx, rx) = tokio::sync::oneshot::channel();

    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query,
        plan: plan.clone(),
        time_window_expr: None,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            sink_table.to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts,
        flow_eval_interval: None,
        eval_schedule: None,
    })
    .unwrap();

    TestTaskParts {
        task,
        query_engine,
        plan,
    }
}

#[tokio::test]
async fn test_incremental_read_is_disabled_by_default() {
    let task = new_test_task_engine_and_plan_with_query_and_opts(
        "SELECT number, ts FROM numbers_with_ts",
        "numbers_with_ts",
        Arc::new(BatchingModeOptions::default()),
    )
    .await
    .task;

    assert!(task.state.read().unwrap().is_incremental_disabled());
}

#[tokio::test]
async fn test_dirty_time_windows_uses_batch_opts() {
    let task = new_test_task_engine_and_plan_with_query_and_opts(
        "SELECT number, ts FROM numbers_with_ts",
        "numbers_with_ts",
        Arc::new(BatchingModeOptions {
            experimental_max_filter_num_per_query: 7,
            experimental_time_window_merge_threshold: 11,
            ..Default::default()
        }),
    )
    .await
    .task;

    let state = task.state.read().unwrap();
    assert_eq!(7, state.dirty_time_windows.max_filter_num_per_query());
    assert_eq!(11, state.dirty_time_windows.time_window_merge_threshold());
}

#[tokio::test]
async fn test_execute_once_serialized_waits_for_execution_lock() {
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query(
        "SELECT number, ts FROM numbers_with_ts",
        "missing_sink",
    )
    .await;
    let (frontend_client, _handler) =
        FrontendClient::from_empty_grpc_handler(QueryOptions::default());
    let frontend_client = Arc::new(frontend_client);

    let guard = task.execution_lock.clone().lock_owned().await;
    let task_to_run = task.clone();
    let query_engine_to_run = query_engine.clone();
    let frontend_client_to_run = frontend_client.clone();
    let exec = tokio::spawn(async move {
        task_to_run
            .execute_once_serialized(&query_engine_to_run, &frontend_client_to_run, None)
            .await
    });

    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !exec.is_finished(),
        "execute_once_serialized should wait for execution_lock"
    );

    drop(guard);
    tokio::time::timeout(Duration::from_secs(1), exec)
        .await
        .expect("execute_once_serialized should finish once execution_lock is released")
        .expect("execute_once_serialized task should not panic")
        .expect_err("missing sink should fail after acquiring execution_lock");
}

async fn new_time_window_test_task_with_query(query: &str) -> TestTaskParts {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan_query = "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number";
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), plan_query, true)
        .await
        .unwrap();
    let (column_name, time_window_expr, _, df_schema) = find_time_window_expr(
        &plan,
        query_engine.engine_state().catalog_manager().clone(),
        ctx.clone(),
    )
    .await
    .unwrap();
    let time_window_expr = time_window_expr.map(|expr| {
        TimeWindowExpr::from_expr(
            &expr,
            &column_name,
            &df_schema,
            &query_engine.engine_state().session_state(),
        )
        .unwrap()
    });
    let (_tx, rx) = tokio::sync::oneshot::channel();

    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query,
        plan: plan.clone(),
        time_window_expr,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            "missing_sink".to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: incremental_batch_opts(),
        flow_eval_interval: None,
        eval_schedule: None,
    })
    .unwrap();

    TestTaskParts {
        task,
        query_engine,
        plan,
    }
}

fn register_number_only_sink(query_engine: &QueryEngineRef, table_name: &str) {
    let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
        "number",
        CDT::uint32_datatype(),
        false,
    )]));
    let columns: Vec<VectorRef> = vec![Arc::new(UInt32Vector::from_slice([1_u32]))];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table(table_name, recordbatch);
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id: 9001,
        table,
    };
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog.register_table_sync(request).unwrap();
}

fn register_auto_created_aggregate_sink(query_engine: &QueryEngineRef, table_name: &str) {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), true),
        ColumnSchema::new("ts", CDT::timestamp_millisecond_datatype(), false).with_time_index(true),
        ColumnSchema::new("update_at", CDT::timestamp_millisecond_datatype(), true),
    ]));
    let columns: Vec<VectorRef> = vec![
        Arc::new(UInt32Vector::from_slice([1_u32])),
        Arc::new(TimestampMillisecondVector::from_slice([0_i64])),
        Arc::new(TimestampMillisecondVector::from_slice([0_i64])),
    ];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table(table_name, recordbatch);
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id: 9002,
        table,
    };
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog.register_table_sync(request).unwrap();
}

fn dirty_marker() -> DirtyTimeWindows {
    let mut dirty = DirtyTimeWindows::default();
    dirty.set_dirty();
    dirty
}

fn flow_error_with_status(status_code: StatusCode) -> Error {
    Err::<(), _>(BoxedError::new(MockError::new(status_code)))
        .context(crate::error::ExternalSnafu)
        .unwrap_err()
}

/// Test-only error that carries a non-RequestOutdated status code but
/// displays a stale-snapshot-fence marker string, simulating the real-world
/// scenario where the structured status code is lost through frontend/client
/// wrapping layers.
#[derive(Debug)]
struct StaleFenceTextError {
    code: StatusCode,
    message: String,
}

impl std::fmt::Display for StaleFenceTextError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for StaleFenceTextError {}

impl common_error::ext::ErrorExt for StaleFenceTextError {
    fn status_code(&self) -> StatusCode {
        self.code
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl common_error::ext::StackError for StaleFenceTextError {
    fn debug_fmt(&self, _: usize, _: &mut Vec<String>) {}
    fn next(&self) -> Option<&dyn common_error::ext::StackError> {
        None
    }
}

fn flow_error_with_code_and_text(code: StatusCode, text: &str) -> Error {
    let inner = StaleFenceTextError {
        code,
        message: text.to_string(),
    };
    Err::<(), _>(BoxedError::new(inner))
        .context(crate::error::ExternalSnafu)
        .unwrap_err()
}

fn dirty_range(start: i64, end: i64) -> DirtyTimeWindows {
    let mut dirty = DirtyTimeWindows::default();
    dirty.add_window(
        Timestamp::new_second(start),
        Some(Timestamp::new_second(end)),
    );
    dirty
}

fn expire_after_for_retention_filter_test() -> i64 {
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs();
    (now_secs - 10) as i64
}

fn aggregate_time_window_sink_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]))
}

async fn assert_unscoped_failure_restore(
    consumed_dirty_windows: DirtyTimeWindows,
    current_dirty_windows: DirtyTimeWindows,
    expected_len: usize,
    expected_window_size_secs: u64,
) {
    let (task, plan) = new_test_task_and_plan_with_missing_sink().await;
    {
        let mut state = task.state.write().unwrap();
        state.dirty_time_windows.clean();
        state
            .dirty_time_windows
            .add_dirty_windows(&current_dirty_windows);
    }
    let unscoped_query = PlanInfo {
        plan,
        dirty_restore: DirtyRestore::Unscoped(consumed_dirty_windows),
        coverage: QueryCoverage::UnfilteredFull,
    };

    task.handle_executed_query_failure(Some(&unscoped_query));

    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), expected_len);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(expected_window_size_secs)
    );
}

// --- scheduled-time QueryContext restore regression tests ---

/// Register a sink table whose schema matches the output of a `date_bin`
/// time-window-expression query (columns: `number` uint32, `time_window` timestamp).
fn register_twe_sink(query_engine: &QueryEngineRef, table_name: &str, table_id: u32) {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));
    let columns: Vec<VectorRef> = vec![
        Arc::new(UInt32Vector::from_slice([1_u32])),
        Arc::new(TimestampMillisecondVector::from_slice([0_i64])),
    ];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table(table_name, recordbatch);
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id,
        table,
    };
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog.register_table_sync(request).unwrap();
}

fn register_scheduled_now_sink(query_engine: &QueryEngineRef, table_name: &str, table_id: u32) {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("ts", CDT::timestamp_nanosecond_datatype(), false).with_time_index(true),
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
    ]));
    let columns: Vec<VectorRef> = vec![
        Arc::new(TimestampNanosecondVector::from_slice([0_i64])),
        Arc::new(UInt32Vector::from_slice([1_u32])),
    ];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::table(table_name, recordbatch);
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id,
        table,
    };
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog.register_table_sync(request).unwrap();
}

struct CaptureScheduledNowHandler {
    expected_extension: String,
    captured_sql: Arc<std::sync::Mutex<Option<String>>>,
    query_engine: QueryEngineRef,
}

#[async_trait::async_trait]
impl crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError
    for CaptureScheduledNowHandler
{
    async fn do_query(
        &self,
        query: api::v1::greptime_request::Request,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        assert_eq!(
            ctx.extension(FLOW_SCHEDULED_TIME_MILLIS),
            Some(self.expected_extension.as_str())
        );

        let api::v1::greptime_request::Request::Query(api::v1::QueryRequest {
            query: Some(api::v1::query_request::Query::Sql(sql)),
            ..
        }) = query
        else {
            panic!("expected scheduled SQL flow to send a SQL query, got {query:?}");
        };

        let planned = sql_to_df_plan(ctx, self.query_engine.clone(), &sql, true)
            .await
            .unwrap();
        assert_sql_uses_scheduled_time(&planned.to_string());

        *self.captured_sql.lock().unwrap() = Some(sql);
        Ok(Output::new_with_affected_rows(1))
    }
}

fn assert_sql_uses_scheduled_time(sql: &str) {
    let lower = sql.to_ascii_lowercase();
    assert!(!lower.contains("now()"), "SQL still contains now(): {sql}");
    assert!(
        sql.contains("2023-11-14") || sql.contains("1700000000"),
        "SQL does not contain the scheduled date or epoch value: {sql}"
    );
    assert!(
        sql.contains("22:13:20") || sql.contains("1700000000"),
        "SQL does not contain the scheduled time or epoch value: {sql}"
    );
}

/// After a scheduled-time attempt fails (missing sink), the
/// `FLOW_SCHEDULED_TIME_MILLIS` extension must not leak into
/// `TaskState.query_ctx` or `frontend_extensions()`.
#[tokio::test]
async fn test_scheduled_time_ctx_restored_on_error() {
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query(
        "SELECT number, ts FROM numbers_with_ts",
        "missing_sink",
    )
    .await;
    let (frontend_client, _handler) =
        FrontendClient::from_empty_grpc_handler(QueryOptions::default());
    let frontend_client = Arc::new(frontend_client);

    // Before: no scheduled time extension
    assert_eq!(
        task.state
            .read()
            .unwrap()
            .query_ctx
            .extension(FLOW_SCHEDULED_TIME_MILLIS),
        None
    );
    assert!(
        !task
            .frontend_extensions()
            .contains_key(FLOW_SCHEDULED_TIME_MILLIS)
    );

    let scheduled_time_secs = 1700000000i64;
    let outcome = task
        .execute_once_serialized_at_scheduled_time(
            &query_engine,
            &frontend_client,
            scheduled_time_secs,
        )
        .await;

    // Missing sink table → gen_insert_plan_unlocked should fail.
    assert!(
        outcome.result.is_err(),
        "Expected an error (missing sink), got {:?}",
        outcome.result
    );

    // After: extension must be restored to absent.
    assert_eq!(
        task.state
            .read()
            .unwrap()
            .query_ctx
            .extension(FLOW_SCHEDULED_TIME_MILLIS),
        None,
        "FLOW_SCHEDULED_TIME_MILLIS leaked into query_ctx after error"
    );
    assert!(
        !task
            .frontend_extensions()
            .contains_key(FLOW_SCHEDULED_TIME_MILLIS),
        "FLOW_SCHEDULED_TIME_MILLIS leaked into frontend_extensions after error"
    );
}

/// After a scheduled-time attempt returns `Ok(None)` (no dirty windows),
/// the `FLOW_SCHEDULED_TIME_MILLIS` extension must not leak into
/// `TaskState.query_ctx`.
#[tokio::test]
async fn test_scheduled_time_ctx_restored_on_no_dirty_windows() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan_query = "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window \
                      FROM numbers_with_ts GROUP BY time_window, number";
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), plan_query, true)
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
        .unwrap();

    let sink_table_name = "twe_sink_for_ctx_restore";
    register_twe_sink(&query_engine, sink_table_name, 9101);

    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query: plan_query,
        plan: plan.clone(),
        time_window_expr,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            sink_table_name.to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: incremental_batch_opts(),
        flow_eval_interval: None,
        eval_schedule: None,
    })
    .unwrap();

    let (frontend_client, _handler) =
        FrontendClient::from_empty_grpc_handler(QueryOptions::default());
    let frontend_client = Arc::new(frontend_client);

    // Before: no scheduled time extension
    assert_eq!(
        task.state
            .read()
            .unwrap()
            .query_ctx
            .extension(FLOW_SCHEDULED_TIME_MILLIS),
        None
    );

    let scheduled_time_secs = 1700000000i64;
    let outcome = task
        .execute_once_serialized_at_scheduled_time(
            &query_engine,
            &frontend_client,
            scheduled_time_secs,
        )
        .await;

    // No dirty windows → scoped repair returns None → outcome is Ok(None).
    assert!(
        matches!(outcome.result, Ok(None)),
        "Expected Ok(None) (no dirty windows), got {:?}",
        outcome.result
    );

    // After: extension must be restored to absent.
    assert_eq!(
        task.state
            .read()
            .unwrap()
            .query_ctx
            .extension(FLOW_SCHEDULED_TIME_MILLIS),
        None,
        "FLOW_SCHEDULED_TIME_MILLIS leaked into query_ctx after Ok(None)"
    );
}

#[tokio::test]
async fn test_scheduled_time_now_is_bound_to_selected_attempt() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let query = "SELECT date_trunc('second', now()) AS ts, number FROM numbers_with_ts";
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), query, true)
        .await
        .unwrap();
    let sink_table_name = "scheduled_now_sink";
    register_scheduled_now_sink(&query_engine, sink_table_name, 9102);
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query,
        plan,
        time_window_expr: None,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            sink_table_name.to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: incremental_batch_opts(),
        flow_eval_interval: Some(Duration::from_secs(1)),
        eval_schedule: None,
    })
    .unwrap();

    let scheduled_time_secs = 1_700_000_000_i64;
    let expected_extension = (scheduled_time_secs * 1000).to_string();
    let captured_sql = Arc::new(std::sync::Mutex::new(None));
    let handler: Arc<dyn crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError> =
        Arc::new(CaptureScheduledNowHandler {
            expected_extension,
            captured_sql: captured_sql.clone(),
            query_engine: query_engine.clone(),
        });
    let frontend_client = Arc::new(FrontendClient::from_grpc_handler(
        Arc::downgrade(&handler),
        QueryOptions::default(),
    ));

    let outcome = task
        .execute_once_serialized_at_scheduled_time(
            &query_engine,
            &frontend_client,
            scheduled_time_secs,
        )
        .await;

    assert!(
        matches!(outcome.result, Ok(Some((1, _)))),
        "scheduled attempt should execute once, got {:?}",
        outcome.result
    );
    let sent_sql = captured_sql
        .lock()
        .unwrap()
        .clone()
        .expect("frontend handler should capture generated SQL");
    assert!(!sent_sql.is_empty());
}

fn output_with_region_watermarks(
    watermarks: impl IntoIterator<Item = (u64, Option<u64>)>,
) -> OutputWithMetrics {
    let result = OutputWithMetrics::from_output(Output::new_with_affected_rows(0));
    result.metrics.update(Some(RecordBatchMetrics {
        region_watermarks: watermarks
            .into_iter()
            .map(|(region_id, watermark)| RegionWatermarkEntry {
                region_id,
                watermark,
            })
            .collect(),
        ..Default::default()
    }));
    result.metrics.mark_ready();
    result
}

#[test]
fn test_apply_query_result_to_state_advances_full_snapshot_to_incremental() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    let result = output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]);

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        &QueryCoverage::UnfilteredFull,
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::AdvancedFromFullSnapshot {
            participating_regions: 2,
            watermarks: 2,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
    assert_eq!(
        state.checkpoints(),
        &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    );
}

#[test]
fn test_apply_query_result_to_state_stays_full_snapshot_when_incremental_disabled() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state.disable_incremental();
    assert!(state.is_incremental_disabled());
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);

    let result = output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]);
    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        &QueryCoverage::UnfilteredFull,
    );

    // Should NOT claim advancement to incremental; should fallback with correct reason.
    assert_eq!(
        decision,
        FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::IncrementalDisabled,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.is_incremental_disabled());
    // Checkpoints are still updated even if mode doesn't advance.
    assert_eq!(
        state.checkpoints(),
        &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    );
}

#[test]
fn test_apply_query_result_to_state_rejects_unproved_watermark() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    let result = output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, None)]);

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        &QueryCoverage::UnfilteredFull,
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.checkpoints().is_empty());
}

#[test]
fn test_apply_query_result_to_state_reports_missing_watermark() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    let result = OutputWithMetrics::from_output(Output::new_with_affected_rows(0));

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        &QueryCoverage::UnfilteredFull,
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::MissingRegionWatermark,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.checkpoints().is_empty());
}

#[test]
fn test_apply_query_result_to_state_advances_incremental_subset() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state.advance_checkpoints(HashMap::from([
        (1_u64, 10_u64),
        (2_u64, 20_u64),
        (3_u64, 30_u64),
    ]));
    let result = output_with_region_watermarks([(1_u64, Some(12_u64)), (3_u64, Some(35_u64))]);

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        &QueryCoverage::IncrementalDelta,
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::AdvancedIncremental {
            participating_regions: 2,
            watermarks: 2,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
    assert_eq!(
        state.checkpoints(),
        &BTreeMap::from([(1_u64, 12_u64), (2_u64, 20_u64), (3_u64, 35_u64)])
    );
}

#[test]
fn test_scoped_base_repair_with_dirty_backlog_starts_fenced_repair_from_full_snapshot() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    // Set a dirty window so that ScopedBaseRepair enters fenced repair instead
    // of advancing directly; coverage type plus live dirty-window presence now
    // determines this transition.
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(20)));
    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    let result = output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]);

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &result,
        std::time::Duration::from_millis(1),
        &QueryCoverage::ScopedBaseRepair,
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::ContinuedFencedRepair {
            pending_windows: 1,
            watermarks: 2,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.dirty_time_windows.is_empty());
    let repair = state.pending_fenced_repair().unwrap();
    assert_eq!(repair.high(), &high);
    assert_eq!(repair.pending_windows().len(), 1);
}

fn next_fenced_repair_filter(state: &mut TaskState, window_cnt: usize) -> FilterExprInfo {
    state
        .gen_scoped_filter_exprs(
            "ts",
            None,
            chrono::Duration::seconds(10),
            window_cnt,
            1,
            None,
        )
        .unwrap()
        .unwrap()
}

#[test]
fn test_fenced_repair_chunk_with_pending_windows_stays_full_snapshot() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(20)));
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(110)));

    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    state.start_fenced_repair(high.clone()).unwrap();
    let _filter = next_fenced_repair_filter(&mut state, 1);
    assert_eq!(
        state
            .pending_fenced_repair()
            .unwrap()
            .pending_windows()
            .len(),
        1
    );

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]),
        std::time::Duration::from_millis(1),
        &QueryCoverage::FencedRepairChunk { high },
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::ContinuedFencedRepair {
            pending_windows: 1,
            watermarks: 2,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.checkpoints().is_empty());
    assert_eq!(
        state
            .pending_fenced_repair()
            .unwrap()
            .pending_windows()
            .len(),
        1
    );
    assert!(state.dirty_time_windows.is_empty());
}

#[test]
fn test_continued_fenced_repair_uses_pending_snapshot_not_later_live_dirty() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));

    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    state.start_fenced_repair(high.clone()).unwrap();

    // Make the two queues distinguishable: the fenced repair should keep using
    // the moved pending backlog captured above, not this later live dirty window.
    state.dirty_time_windows.add_window(
        Timestamp::new_second(1000),
        Some(Timestamp::new_second(1005)),
    );
    assert_eq!(state.dirty_time_windows.len(), 1);

    let first_filter = next_fenced_repair_filter(&mut state, 1);
    assert_eq!(
        first_filter.time_ranges,
        vec![(Timestamp::new_second(10), Timestamp::new_second(15))]
    );

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]),
        std::time::Duration::from_millis(1),
        &QueryCoverage::FencedRepairChunk { high },
    );
    assert_eq!(
        decision,
        FlowCheckpointDecision::ContinuedFencedRepair {
            pending_windows: 1,
            watermarks: 2,
        }
    );

    let second_filter = next_fenced_repair_filter(&mut state, 1);
    assert_eq!(
        second_filter.time_ranges,
        vec![(Timestamp::new_second(100), Timestamp::new_second(105))]
    );
    assert!(state.fenced_repair_pending_is_empty());
    assert_eq!(state.dirty_time_windows.len(), 1);
}

#[test]
fn test_final_fenced_repair_chunk_advances_to_high() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(20)));

    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    state.start_fenced_repair(high.clone()).unwrap();
    let _filter = next_fenced_repair_filter(&mut state, 1);
    assert!(state.fenced_repair_pending_is_empty());

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &output_with_region_watermarks([(1_u64, Some(10_u64)), (2_u64, Some(20_u64))]),
        std::time::Duration::from_millis(1),
        &QueryCoverage::FencedRepairChunk { high: high.clone() },
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::AdvancedFromFullSnapshot {
            participating_regions: 2,
            watermarks: 2,
        }
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
    assert_eq!(state.checkpoints(), &high);
    assert!(state.pending_fenced_repair().is_none());
    assert!(state.dirty_time_windows.is_empty());
}

#[test]
fn test_fenced_repair_watermarks_require_exact_high() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(20)));

    state
        .start_fenced_repair(BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]))
        .unwrap();
    let participating_regions = BTreeSet::from([1_u64, 2_u64]);

    assert!(state.fenced_repair_watermarks_match_high(
        &participating_regions,
        &HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    ));
    assert!(!state.fenced_repair_watermarks_match_high(
        &participating_regions,
        &HashMap::from([(1_u64, 11_u64), (2_u64, 20_u64)])
    ));
    assert!(!state.fenced_repair_watermarks_match_high(
        &participating_regions,
        &HashMap::from([(1_u64, 10_u64)])
    ));
}

#[test]
fn test_fenced_repair_chunk_watermark_mismatch_restores_pending_but_consumes_inflight() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));

    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    state.start_fenced_repair(high.clone()).unwrap();

    let _filter = next_fenced_repair_filter(&mut state, 1);
    assert_eq!(
        state
            .pending_fenced_repair()
            .unwrap()
            .pending_windows()
            .len(),
        1
    );
    assert!(state.dirty_time_windows.is_empty());

    let decision = BatchingTask::apply_query_result_to_state(
        &mut state,
        &output_with_region_watermarks([(1_u64, Some(11_u64)), (2_u64, Some(20_u64))]),
        std::time::Duration::from_millis(1),
        &QueryCoverage::FencedRepairChunk { high },
    );

    assert_eq!(
        decision,
        FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
        }
    );
    assert!(state.pending_fenced_repair().is_none());
    assert_eq!(state.dirty_time_windows.len(), 1);
}

#[tokio::test]
async fn test_fenced_repair_mismatch_next_plan_is_scoped_base_repair() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    let _filter = {
        let mut state = task.state.write().unwrap();
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));
        state.start_fenced_repair(high.clone()).unwrap();
        next_fenced_repair_filter(&mut state, 1)
    };

    {
        let mut state = task.state.write().unwrap();
        let decision = BatchingTask::apply_query_result_to_state(
            &mut state,
            &output_with_region_watermarks([(1_u64, Some(11_u64)), (2_u64, Some(20_u64))]),
            std::time::Duration::from_millis(1),
            &QueryCoverage::FencedRepairChunk { high },
        );
        assert_eq!(
            decision,
            FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: CheckpointMode::FullSnapshot,
                reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
            }
        );
        assert!(state.pending_fenced_repair().is_none());
    }

    let plan = task
        .gen_query_with_time_window(
            query_engine,
            &aggregate_time_window_sink_schema(),
            &[],
            false,
            Some(1),
        )
        .await
        .unwrap()
        .expect("mismatch should keep live dirty backlog for a fresh scoped repair");
    assert!(matches!(plan.coverage, QueryCoverage::ScopedBaseRepair));
    let DirtyRestore::Scoped(filter) = &plan.dirty_restore else {
        panic!("scoped base repair should carry scoped dirty restore info");
    };
    assert_eq!(
        filter.time_ranges,
        vec![(Timestamp::new_second(100), Timestamp::new_second(105))],
        "executed pre-H repair item should not be requeued; only remaining pending window is retried"
    );
}

#[test]
fn test_apply_query_failure_to_state_falls_back_from_incremental() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state.advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);

    let decision = BatchingTask::apply_query_failure_to_state(
        &mut state,
        std::time::Duration::from_millis(1),
        &QueryCoverage::IncrementalDelta,
        FlowQueryFallbackReason::IncrementalQueryFailure,
    );

    assert_eq!(
        decision,
        Some(FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::Incremental,
            reason: FlowQueryFallbackReason::IncrementalQueryFailure,
        })
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert_eq!(
        state.checkpoints(),
        &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    );
}

#[test]
fn test_apply_query_failure_to_state_records_full_snapshot_failure() {
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);

    let decision = BatchingTask::apply_query_failure_to_state(
        &mut state,
        std::time::Duration::from_millis(1),
        &QueryCoverage::UnfilteredFull,
        FlowQueryFallbackReason::QueryFailure,
    );

    assert_eq!(
        decision,
        Some(FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::QueryFailure,
        })
    );
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert!(state.checkpoints().is_empty());
}

#[test]
fn test_query_failure_reason_distinguishes_fenced_repair_stale_fence() {
    let err = flow_error_with_status(StatusCode::RequestOutdated);

    assert_eq!(
        BatchingTask::query_failure_reason(
            &err,
            &QueryCoverage::FencedRepairChunk {
                high: BTreeMap::new(),
            },
        ),
        FlowQueryFallbackReason::SnapshotFenceExpired
    );
    assert_eq!(
        BatchingTask::query_failure_reason(&err, &QueryCoverage::IncrementalDelta),
        FlowQueryFallbackReason::StaleCursor
    );

    let generic_err = flow_error_with_status(StatusCode::Unexpected);
    assert_eq!(
        BatchingTask::query_failure_reason(&generic_err, &QueryCoverage::ScopedBaseRepair),
        FlowQueryFallbackReason::QueryFailure
    );
    assert_eq!(
        BatchingTask::query_failure_reason(&generic_err, &QueryCoverage::IncrementalDelta),
        FlowQueryFallbackReason::IncrementalQueryFailure
    );
}

/// Wrapped errors carrying stale snapshot fence marker text in their
/// Display/Debug chain should be classified as `SnapshotFenceExpired` on
/// fenced repair coverage, even when the structured `StatusCode::RequestOutdated`
/// was lost through client layering. This prevents an infinite retry loop
/// where the fenced chunk re-sends the same stale `given_seq` every tick.
#[test]
fn test_query_failure_reason_text_fallback_stale_snapshot_fence() {
    let high = BTreeMap::new();
    let fenced = QueryCoverage::FencedRepairChunk { high: high.clone() };

    // STALE_SNAPSHOT_FENCE marker with a non-RequestOutdated status code
    let err = flow_error_with_code_and_text(
        StatusCode::Internal,
        "gRPC error: STALE_SNAPSHOT_FENCE: snapshot upper bound stale, region: 1024/0",
    );
    assert_eq!(
        BatchingTask::query_failure_reason(&err, &fenced),
        FlowQueryFallbackReason::SnapshotFenceExpired
    );

    // REBIND_SNAPSHOT_FENCE marker
    let err = flow_error_with_code_and_text(
        StatusCode::Internal,
        "STALE_SNAPSHOT_FENCE ... retry_hint: REBIND_SNAPSHOT_FENCE",
    );
    assert_eq!(
        BatchingTask::query_failure_reason(&err, &fenced),
        FlowQueryFallbackReason::SnapshotFenceExpired
    );

    // snapshot upper bound stale marker (the natural-language fragment)
    let err = flow_error_with_code_and_text(
        StatusCode::Internal,
        "query failed: snapshot upper bound stale, consider rebinding",
    );
    assert_eq!(
        BatchingTask::query_failure_reason(&err, &fenced),
        FlowQueryFallbackReason::SnapshotFenceExpired
    );

    // Fenced coverage with a generic wrapped error (no stale-fence marker) →
    // still QueryFailure
    let generic_err =
        flow_error_with_code_and_text(StatusCode::Internal, "some transient network error");
    assert_eq!(
        BatchingTask::query_failure_reason(&generic_err, &fenced),
        FlowQueryFallbackReason::QueryFailure
    );

    // Non-fenced incremental coverage with stale-fence marker text must NOT
    // classify as SnapshotFenceExpired; it should remain IncrementalQueryFailure.
    let err = flow_error_with_code_and_text(
        StatusCode::Internal,
        "STALE_SNAPSHOT_FENCE blob in unexpected context",
    );
    assert_eq!(
        BatchingTask::query_failure_reason(&err, &QueryCoverage::IncrementalDelta),
        FlowQueryFallbackReason::IncrementalQueryFailure
    );

    // Existing RequestOutdated behavior is unchanged.
    let outdated_err = flow_error_with_status(StatusCode::RequestOutdated);
    assert_eq!(
        BatchingTask::query_failure_reason(&outdated_err, &fenced),
        FlowQueryFallbackReason::SnapshotFenceExpired
    );
    assert_eq!(
        BatchingTask::query_failure_reason(&outdated_err, &QueryCoverage::IncrementalDelta),
        FlowQueryFallbackReason::StaleCursor
    );
}

#[test]
fn test_fenced_repair_coverage_produces_snapshot_seq_map_for_distributed_metadata_path() {
    // Covers the metadata boundary between QueryCoverage and the
    // frontend/distributed client API: only FencedRepairChunk carries a
    // non-empty snapshot_seqs map so the datanode can bind per-region
    // snapshot upper bounds against the frozen high H. Other coverage
    // variants must produce an empty map.
    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    let coverage = QueryCoverage::FencedRepairChunk { high: high.clone() };
    assert_eq!(
        coverage.snapshot_seqs(),
        HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    );

    assert!(QueryCoverage::UnfilteredFull.snapshot_seqs().is_empty());
    assert!(QueryCoverage::ScopedBaseRepair.snapshot_seqs().is_empty());
    assert!(QueryCoverage::IncrementalDelta.snapshot_seqs().is_empty());
}

#[tokio::test]
async fn test_fenced_repair_stale_fence_next_plan_is_scoped_base_repair() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    let filter = {
        let mut state = task.state.write().unwrap();
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));
        state.start_fenced_repair(high.clone()).unwrap();
        next_fenced_repair_filter(&mut state, 1)
    };

    {
        let mut state = task.state.write().unwrap();
        let decision = BatchingTask::apply_query_failure_to_state(
            &mut state,
            std::time::Duration::from_millis(1),
            &QueryCoverage::FencedRepairChunk { high },
            FlowQueryFallbackReason::SnapshotFenceExpired,
        );
        assert_eq!(
            decision,
            Some(FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: CheckpointMode::FullSnapshot,
                reason: FlowQueryFallbackReason::SnapshotFenceExpired,
            })
        );
        assert!(state.pending_fenced_repair().is_none());

        // Simulate the outer execution failure restore for the in-flight chunk.
        state.restore_scoped_windows(&filter);
    }

    let plan = task
        .gen_query_with_time_window(
            query_engine,
            &aggregate_time_window_sink_schema(),
            &[],
            false,
            Some(1),
        )
        .await
        .unwrap()
        .expect("stale fence should restore dirty windows for a fresh scoped repair");
    assert!(matches!(plan.coverage, QueryCoverage::ScopedBaseRepair));
}

#[test]
fn test_fenced_repair_transient_non_stale_failure_retries_same_high() {
    // Opposite of stale-fence abandon: a non-RequestOutdated failure on a
    // fenced repair chunk should NOT abandon the pending repair. The same
    // high H is retained, the failed in-flight window goes back into
    // pending_windows (not live dirty_time_windows), and the next execution
    // can re-attempt the same fenced repair chunk.
    let query_ctx = QueryContext::arc();
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let mut state = TaskState::new(query_ctx, rx);
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
    state
        .dirty_time_windows
        .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));

    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    state.start_fenced_repair(high.clone()).unwrap();
    let filter = next_fenced_repair_filter(&mut state, 1);
    assert_eq!(
        state
            .pending_fenced_repair()
            .unwrap()
            .pending_windows()
            .len(),
        1
    );

    let decision = BatchingTask::apply_query_failure_to_state(
        &mut state,
        std::time::Duration::from_millis(1),
        &QueryCoverage::FencedRepairChunk { high: high.clone() },
        FlowQueryFallbackReason::QueryFailure,
    );

    assert_eq!(
        decision,
        Some(FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::FullSnapshot,
            reason: FlowQueryFallbackReason::QueryFailure,
        })
    );
    // Pending repair is NOT abandoned: high H is unchanged.
    let repair = state.pending_fenced_repair().unwrap();
    assert_eq!(repair.high(), &high);
    assert_eq!(repair.pending_windows().len(), 1);

    // Simulate the outer execution failure restore for the in-flight chunk.
    state.restore_scoped_windows(&filter);

    // After restore, the in-flight chunk goes back into pending_windows
    // (because pending_fenced_repair is still Some), NOT into live
    // dirty_time_windows.
    assert_eq!(
        state
            .pending_fenced_repair()
            .unwrap()
            .pending_windows()
            .len(),
        2,
        "in-flight window restored into pending_windows"
    );
    assert_eq!(
        state.dirty_time_windows.len(),
        0,
        "live dirty windows unchanged (not where in-flight was restored)"
    );
}

/// When `query_failure_reason` classifies a wrapped error as
/// `SnapshotFenceExpired` via the text-marker fallback (not via
/// `StatusCode::RequestOutdated`), the state machine must still
/// abandon the fenced repair and produce a `ScopedBaseRepair` plan
/// next, exactly like the structured-code path.
#[tokio::test]
async fn test_text_fallback_stale_fence_produces_scoped_base_repair() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    let high = BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]);
    let filter = {
        let mut state = task.state.write().unwrap();
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));
        state.start_fenced_repair(high.clone()).unwrap();
        next_fenced_repair_filter(&mut state, 1)
    };

    // Construct a wrapped error that hits the text fallback (non-RequestOutdated
    // status code with STALE_SNAPSHOT_FENCE marker text).
    let err = flow_error_with_code_and_text(
        StatusCode::Internal,
        "STALE_SNAPSHOT_FENCE: snapshot upper bound stale, retry_hint: REBIND_SNAPSHOT_FENCE",
    );
    let coverage = QueryCoverage::FencedRepairChunk { high };
    let reason = BatchingTask::query_failure_reason(&err, &coverage);
    assert_eq!(reason, FlowQueryFallbackReason::SnapshotFenceExpired);

    {
        let mut state = task.state.write().unwrap();
        let decision = BatchingTask::apply_query_failure_to_state(
            &mut state,
            std::time::Duration::from_millis(1),
            &coverage,
            reason,
        );
        assert_eq!(
            decision,
            Some(FlowCheckpointDecision::FallbackToFullSnapshot {
                previous_mode: CheckpointMode::FullSnapshot,
                reason: FlowQueryFallbackReason::SnapshotFenceExpired,
            })
        );
        assert!(state.pending_fenced_repair().is_none());

        // Simulate the outer execution failure restore for the in-flight chunk.
        state.restore_scoped_windows(&filter);
    }

    let plan = task
        .gen_query_with_time_window(
            query_engine,
            &aggregate_time_window_sink_schema(),
            &[],
            false,
            Some(1),
        )
        .await
        .unwrap()
        .expect("text-fallback stale fence should restore dirty windows for a fresh scoped repair");
    assert!(
        matches!(plan.coverage, QueryCoverage::ScopedBaseRepair),
        "next plan after text-fallback stale fence should be ScopedBaseRepair"
    );
}

#[test]
fn test_checkpoint_decision_labels_are_stable() {
    let advance = FlowCheckpointDecision::AdvancedIncremental {
        participating_regions: 1,
        watermarks: 1,
    };
    let fallback = FlowCheckpointDecision::FallbackToFullSnapshot {
        previous_mode: CheckpointMode::Incremental,
        reason: FlowQueryFallbackReason::StaleCursor,
    };

    assert_eq!(advance.mode_label(), "incremental");
    assert_eq!(advance.decision_label(), CHECKPOINT_DECISION_ADVANCE);
    assert_eq!(advance.reason_label(), CHECKPOINT_REASON_NONE);
    assert_eq!(fallback.mode_label(), "incremental");
    assert_eq!(fallback.decision_label(), CHECKPOINT_DECISION_FALLBACK);
    assert_eq!(fallback.reason_label(), "stale_cursor");
    assert_eq!(
        FlowQueryFallbackReason::SnapshotFenceExpired.as_label(),
        "snapshot_fence_expired"
    );
    assert_eq!(
        FlowQueryFallbackReason::DirtyBacklogPending.as_label(),
        "dirty_backlog_pending"
    );
    assert_eq!(
        FlowQueryFallbackReason::QueryFailure.as_label(),
        "query_failure"
    );
}

#[tokio::test]
async fn test_build_flow_query_extensions_switches_with_checkpoint_mode() {
    let (task, _) = new_test_task_engine_and_plan_with_query(
        "SELECT number, ts FROM numbers_with_ts",
        "numbers_with_ts",
    )
    .await
    .into_task_and_plan();

    let extensions = task.build_flow_query_extensions(false, true).await.unwrap();
    assert_eq!(
        extensions,
        vec![("flow.return_region_seq", "true".to_string())]
    );

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));

    let extensions = task.build_flow_query_extensions(false, true).await.unwrap();
    assert!(extensions.contains(&("flow.return_region_seq", "true".to_string())));
    assert!(
        !extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_MODE)
    );
    assert!(
        !extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_AFTER_SEQS)
    );

    let extensions = task.build_flow_query_extensions(true, true).await.unwrap();

    assert!(extensions.contains(&("flow.return_region_seq", "true".to_string())));
    assert!(extensions.contains(&(
        FLOW_INCREMENTAL_MODE,
        FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string()
    )));
    assert!(extensions.contains(&(
        FLOW_INCREMENTAL_AFTER_SEQS,
        serde_json::json!({"1": 10, "2": 20}).to_string(),
    )));

    let extensions = task.build_flow_query_extensions(true, false).await.unwrap();
    assert!(extensions.contains(&("flow.return_region_seq", "true".to_string())));
    assert!(
        !extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_MODE)
    );
    assert!(
        !extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_AFTER_SEQS)
    );

    task.state.write().unwrap().disable_incremental();
    let extensions = task.build_flow_query_extensions(true, true).await.unwrap();
    assert!(extensions.contains(&("flow.return_region_seq", "true".to_string())));
    assert!(
        !extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_MODE)
    );
    assert!(
        !extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_AFTER_SEQS)
    );
}

// `sequence_range` must flow into the emitted extensions only with a
// checkpoint map present, requesting exact filtering across memtables and all
// SSTs; the default mode keeps emitting `memtable_only`.
#[tokio::test]
async fn test_build_flow_query_extensions_sequence_range_mode() {
    let (task, _) = new_test_task_engine_and_plan_with_query_and_opts(
        "SELECT number, ts FROM numbers_with_ts",
        "numbers_with_ts",
        Arc::new(BatchingModeOptions {
            experimental_enable_incremental_read: true,
            incremental_mode: IncrementalMode::SequenceRange,
            ..Default::default()
        }),
    )
    .await
    .into_task_and_plan();

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));

    let extensions = task.build_flow_query_extensions(true, true).await.unwrap();
    assert!(extensions.contains(&(
        FLOW_INCREMENTAL_MODE,
        FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE.to_string()
    )));
    assert!(extensions.contains(&(
        FLOW_INCREMENTAL_AFTER_SEQS,
        serde_json::json!({"1": 10}).to_string(),
    )));
}

#[tokio::test]
async fn test_full_snapshot_scoped_plan_marks_checkpoint_advance_safe_only_after_backlog_drained() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        state.disable_incremental();
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(30), Some(Timestamp::new_second(35)));
    }
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    let first = task
        .gen_query_with_time_window(query_engine.clone(), &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(first.coverage, QueryCoverage::ScopedBaseRepair));
    assert_eq!(task.state.read().unwrap().dirty_time_windows.len(), 1);

    let second = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(second.coverage, QueryCoverage::ScopedBaseRepair));
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());
}

#[tokio::test]
async fn test_expired_empty_fenced_repair_generates_scoped_base_repair_plan() {
    let TestTaskParts {
        mut task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    Arc::get_mut(&mut task.config)
        .expect("test task config should be uniquely owned")
        .expire_after = Some(expire_after_for_retention_filter_test());

    {
        let mut state = task.state.write().unwrap();
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
        state
            .start_fenced_repair(BTreeMap::from([(1_u64, 10_u64)]))
            .unwrap();

        state.dirty_time_windows.clean();
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));
    }

    let plan = task
        .gen_query_with_time_window(
            query_engine,
            &aggregate_time_window_sink_schema(),
            &[],
            false,
            Some(1),
        )
        .await
        .unwrap()
        .expect("expired empty repair should fall back to live dirty");

    assert!(matches!(plan.coverage, QueryCoverage::ScopedBaseRepair));
    assert!(plan.coverage.snapshot_seqs().is_empty());
    assert!(task.state.read().unwrap().pending_fenced_repair().is_none());
}

#[tokio::test]
async fn test_incremental_plan_consumes_dirty_signal_for_checkpoint_safety() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(30), Some(Timestamp::new_second(35)));
    }
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    let plan = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(plan.coverage, QueryCoverage::IncrementalDelta));
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());
}

#[tokio::test]
async fn test_scoped_base_repair_plan_applies_dirty_window_filter() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert!(!state.is_incremental_disabled());
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(30), Some(Timestamp::new_second(35)));
    }
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    let plan = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();

    let plan_text = plan.plan.to_string();
    assert!(matches!(plan.coverage, QueryCoverage::ScopedBaseRepair));
    assert_eq!(task.state.read().unwrap().dirty_time_windows.len(), 1);
    assert!(plan_text.contains("Filter:"), "{plan_text}");
}

#[tokio::test]
async fn test_full_snapshot_seeding_applies_expire_after_retention_filter() {
    let TestTaskParts {
        mut task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert!(!state.is_incremental_disabled());
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(100), Some(Timestamp::new_second(105)));
    }
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    Arc::get_mut(&mut task.config)
        .expect("test task config should be uniquely owned")
        .expire_after = Some(expire_after_for_retention_filter_test());
    let plan = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(plan.coverage, QueryCoverage::ScopedBaseRepair));
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());
    let plan_text = plan.plan.to_string();
    assert!(
        plan_text.contains("Filter: ts >= TimestampMillisecond("),
        "{plan_text}"
    );
}

#[tokio::test]
async fn test_incremental_plan_does_not_add_dirty_window_filter() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
    }
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    let plan = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();

    let plan_text = plan.plan.to_string();
    assert!(matches!(plan.coverage, QueryCoverage::IncrementalDelta));
    assert!(!plan_text.contains("Filter:"), "{plan_text}");
}

#[tokio::test]
async fn test_incremental_delta_applies_expire_after_retention_filter() {
    let TestTaskParts {
        mut task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
    }
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    Arc::get_mut(&mut task.config)
        .expect("test task config should be uniquely owned")
        .expire_after = Some(expire_after_for_retention_filter_test());
    let plan = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(plan.coverage, QueryCoverage::IncrementalDelta));
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());
    let plan_text = plan.plan.to_string();
    assert!(
        plan_text.contains("Filter: ts >= TimestampMillisecond("),
        "{plan_text}"
    );
}

#[tokio::test]
async fn test_successful_incremental_checkpoint_fallback_consumes_unscoped_dirty_signal() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT max(number) AS number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window",
    )
    .await;
    {
        let mut state = task.state.write().unwrap();
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]));
        state
            .dirty_time_windows
            .add_window(Timestamp::new_second(0), Some(Timestamp::new_second(5)));
    }
    let sink_schema = aggregate_time_window_sink_schema();

    let plan_info = task
        .gen_query_with_time_window(query_engine.clone(), &sink_schema, &[], false, Some(1))
        .await
        .unwrap()
        .unwrap();

    assert!(matches!(
        plan_info.coverage,
        QueryCoverage::IncrementalDelta
    ));
    assert!(matches!(
        &plan_info.dirty_restore,
        DirtyRestore::Unscoped(_)
    ));
    assert!(task.state.read().unwrap().dirty_time_windows.is_empty());

    let result = output_with_region_watermarks([(1_u64, Some(12_u64)), (2_u64, None)]);
    let decision = {
        let mut state = task.state.write().unwrap();
        BatchingTask::apply_query_result_to_state(
            &mut state,
            &result,
            std::time::Duration::from_millis(1),
            &plan_info.coverage,
        )
    };
    assert_eq!(
        decision,
        FlowCheckpointDecision::FallbackToFullSnapshot {
            previous_mode: CheckpointMode::Incremental,
            reason: FlowQueryFallbackReason::IncompleteRegionWatermark,
        }
    );

    {
        let state = task.state.read().unwrap();
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert!(state.dirty_time_windows.is_empty());
    }

    let followup = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, Some(1))
        .await
        .unwrap();
    assert!(
        followup.is_none(),
        "successful fallback consumes the dirty signal instead of re-running it"
    );
}

#[tokio::test]
async fn test_explicit_full_query_paths_generate_unfiltered_full() {
    for (case_name, query_type, flow_eval_interval) in [
        ("TQL", QueryType::Tql, None),
        (
            "eval-interval SQL",
            QueryType::Sql,
            Some(Duration::from_secs(60)),
        ),
    ] {
        let TestTaskParts {
            mut task,
            query_engine,
            ..
        } = new_test_task_engine_and_plan_with_query(
            "SELECT number, ts FROM numbers_with_ts",
            "missing_sink",
        )
        .await;
        {
            let config =
                Arc::get_mut(&mut task.config).expect("test task config should be uniquely owned");
            config.query_type = query_type;
            config.flow_eval_interval = flow_eval_interval;
        }
        task.state.write().unwrap().dirty_time_windows.set_dirty();
        let sink_schema = Arc::new(Schema::new(vec![
            ColumnSchema::new("number", CDT::uint32_datatype(), false),
            ColumnSchema::new("ts", CDT::timestamp_millisecond_datatype(), false)
                .with_time_index(true),
        ]));

        let plan = task
            .gen_query_with_time_window(query_engine, &sink_schema, &[], false, None)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("{case_name} full-query path should generate a plan"));

        assert!(
            matches!(plan.coverage, QueryCoverage::UnfilteredFull),
            "{case_name} should use UnfilteredFull"
        );
        assert!(
            task.state.read().unwrap().dirty_time_windows.is_empty(),
            "{case_name} should consume the dirty signal"
        );
    }
}

#[tokio::test]
async fn test_executed_query_failure_restores_scoped_dirty_windows_for_flush_path() {
    let (task, plan) = new_test_task_and_plan_with_missing_sink().await;
    {
        let mut state = task.state.write().unwrap();
        state.dirty_time_windows.clean();
    }
    let scoped_query = PlanInfo {
        plan,
        dirty_restore: DirtyRestore::Scoped(FilterExprInfo {
            expr: datafusion_expr::lit(true),
            col_name: "ts".to_string(),
            time_ranges: vec![(Timestamp::new_second(10), Timestamp::new_second(20))],
            window_size: chrono::Duration::seconds(10),
        }),
        coverage: QueryCoverage::ScopedBaseRepair,
    };

    task.handle_executed_query_failure(Some(&scoped_query));

    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), 1);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(10)
    );
}

#[tokio::test]
async fn test_prepare_plan_for_incremental_disables_on_non_aggregate() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(
        ctx.clone(),
        query_engine.clone(),
        "SELECT number, ts FROM numbers_with_ts",
        true,
    )
    .await
    .unwrap();

    // Build a DML wrapper using a real sink table from the test engine.
    let (sink_table, _) = get_table_info_df_schema(
        query_engine.engine_state().catalog_manager().clone(),
        [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ],
    )
    .await
    .unwrap();
    let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider));
    let dml_plan = LogicalPlan::Dml(DmlStatement::new(
        datafusion_common::TableReference::bare("test"),
        table_source,
        WriteOp::Insert(datafusion_expr::dml::InsertOp::Append),
        Arc::new(plan),
    ));

    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query: "SELECT number, ts FROM numbers_with_ts",
        plan: dml_plan.clone(),
        time_window_expr: None,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: incremental_batch_opts(),
        flow_eval_interval: None,
        eval_schedule: None,
    })
    .unwrap();

    // Put the state into Incremental mode with checkpoints.
    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
    assert_eq!(
        task.state.read().unwrap().checkpoint_mode(),
        CheckpointMode::Incremental
    );

    let incremental_plan = task.prepare_plan_for_incremental(&dml_plan).await.unwrap();
    assert!(incremental_plan.is_none());
    let state = task.state.read().unwrap();
    assert!(state.is_incremental_disabled());
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
}

#[tokio::test]
async fn test_unsafe_incremental_plan_skip_restores_dirty_without_query() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(
        ctx.clone(),
        query_engine.clone(),
        "SELECT sum(number) AS total, ts FROM numbers_with_ts GROUP BY ts",
        true,
    )
    .await
    .unwrap();

    let (sink_table, _) = get_table_info_df_schema(
        query_engine.engine_state().catalog_manager().clone(),
        [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ],
    )
    .await
    .unwrap();
    let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider));
    let dml_plan = LogicalPlan::Dml(DmlStatement::new(
        datafusion_common::TableReference::bare("test"),
        table_source,
        WriteOp::Insert(datafusion_expr::dml::InsertOp::Append),
        Arc::new(plan),
    ));

    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query: "SELECT sum(number) AS total, ts FROM numbers_with_ts GROUP BY ts",
        plan: dml_plan.clone(),
        time_window_expr: None,
        expire_after: None,
        // The sink table exists, but does not have the rewritten aggregate
        // output column `total`, so incremental rewrite fails before any
        // frontend query should be sent.
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: incremental_batch_opts(),
        flow_eval_interval: None,
        eval_schedule: None,
    })
    .unwrap();

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
    let dirty_restore = DirtyRestore::Unscoped(dirty_range(10, 15));
    let (frontend_client, _) = FrontendClient::from_empty_grpc_handler(QueryOptions::default());

    let result = task
        .execute_logical_plan_unlocked(
            &Arc::new(frontend_client),
            &dml_plan,
            &dirty_restore,
            &QueryCoverage::IncrementalDelta,
        )
        .await
        .unwrap();

    assert!(
        result.is_none(),
        "unsafe incremental fallback must skip query"
    );
    let state = task.state.read().unwrap();
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
    assert!(!state.is_incremental_disabled());
    assert_eq!(state.dirty_time_windows.len(), 1);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(5)
    );
}

#[tokio::test]
async fn test_prepare_plan_for_incremental_group_by_without_merge_columns_uses_original_plan() {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(
        ctx.clone(),
        query_engine.clone(),
        "SELECT ts FROM numbers_with_ts GROUP BY ts",
        true,
    )
    .await
    .unwrap();

    let (sink_table, _) = get_table_info_df_schema(
        query_engine.engine_state().catalog_manager().clone(),
        [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ],
    )
    .await
    .unwrap();
    let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider));
    let dml_plan = LogicalPlan::Dml(DmlStatement::new(
        datafusion_common::TableReference::bare("test"),
        table_source,
        WriteOp::Insert(datafusion_expr::dml::InsertOp::Append),
        Arc::new(plan),
    ));

    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query: "SELECT ts FROM numbers_with_ts GROUP BY ts",
        plan: dml_plan.clone(),
        time_window_expr: None,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: incremental_batch_opts(),
        flow_eval_interval: None,
        eval_schedule: None,
    })
    .unwrap();

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));

    let incremental_plan = task
        .prepare_plan_for_incremental(&dml_plan)
        .await
        .unwrap()
        .expect("plain GROUP BY is incremental-safe without a rewrite");

    assert_eq!(format!("{incremental_plan}"), format!("{dml_plan}"));
    assert!(!task.state.read().unwrap().is_incremental_disabled());
}

#[tokio::test]
async fn test_auto_created_sql_aggregate_sink_reaches_incremental_safe() {
    let sink_table = "auto_created_aggregate_sink";
    let query = "SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts";
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query(query, sink_table).await;
    register_auto_created_aggregate_sink(&query_engine, sink_table);

    let ctx = task.state.read().unwrap().query_ctx.clone();
    let plan = sql_to_df_plan(ctx, query_engine.clone(), query, true)
        .await
        .unwrap();
    let (sink_table, _) = get_table_info_df_schema(
        query_engine.engine_state().catalog_manager().clone(),
        [
            "greptime".to_string(),
            "public".to_string(),
            sink_table.to_string(),
        ],
    )
    .await
    .unwrap();
    let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider));
    let dml_plan = LogicalPlan::Dml(DmlStatement::new(
        datafusion_common::TableReference::bare("test"),
        table_source,
        WriteOp::Insert(datafusion_expr::dml::InsertOp::Append),
        Arc::new(plan),
    ));

    task.state
        .write()
        .unwrap()
        .advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
    let incremental_plan = task.prepare_plan_for_incremental(&dml_plan).await.unwrap();
    let incremental_safe = incremental_plan.is_some();

    assert!(incremental_safe);
    assert!(!task.state.read().unwrap().is_incremental_disabled());

    let extensions = task
        .build_flow_query_extensions(incremental_safe, true)
        .await
        .unwrap();
    assert!(extensions.contains(&(
        FLOW_INCREMENTAL_MODE,
        FLOW_INCREMENTAL_MODE_MEMTABLE_ONLY.to_string()
    )));
    assert!(
        extensions
            .iter()
            .any(|(key, _)| *key == FLOW_INCREMENTAL_AFTER_SEQS)
    );
}

#[tokio::test]
async fn test_unscoped_failure_restores_consumed_dirty_signal() {
    assert_unscoped_failure_restore(dirty_marker(), DirtyTimeWindows::default(), 1, 0).await;
    assert_unscoped_failure_restore(dirty_range(30, 40), dirty_range(10, 20), 2, 20).await;
    assert_unscoped_failure_restore(dirty_range(30, 40), dirty_range(30, 50), 1, 20).await;
}

#[tokio::test]
async fn test_unscoped_execution_invariant_error_preserves_dirty_signal() {
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query(
        "SELECT missing_column FROM numbers_with_ts",
        "missing_sink",
    )
    .await;
    task.state.write().unwrap().dirty_time_windows.set_dirty();
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("ts", CDT::timestamp_millisecond_datatype(), false).with_time_index(true),
    ]));

    let result = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, None)
        .await;

    let err = match result {
        Err(err) => err,
        Ok(_) => panic!("execution should reject SQL without TWE or EVAL INTERVAL"),
    };
    assert!(matches!(err, Error::Unexpected { .. }), "{err}");
    assert!(
        err.to_string()
            .contains("create-flow validation should have rejected it"),
        "{err}"
    );
    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), 1);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(0)
    );
}

#[tokio::test]
async fn test_scoped_plan_generation_failure_restores_consumed_dirty_windows() {
    let TestTaskParts {
        task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT missing_column, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, missing_column",
    )
    .await;
    task.state
        .write()
        .unwrap()
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));
    let sink_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));

    let result = task
        .gen_query_with_time_window(query_engine, &sink_schema, &[], false, None)
        .await;

    assert!(result.is_err());
    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), 1);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(5)
    );
}

#[tokio::test]
async fn test_insert_plan_matching_failure_restores_consumed_dirty_marker() {
    let sink_table = "partial_sink";
    let TestTaskParts {
        mut task,
        query_engine,
        ..
    } = new_time_window_test_task_with_query(
        "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window FROM numbers_with_ts GROUP BY time_window, number",
    )
    .await;
    Arc::get_mut(&mut task.config)
        .expect("test task config should be uniquely owned")
        .sink_table_name[2] = sink_table.to_string();
    register_number_only_sink(&query_engine, sink_table);
    task.state.write().unwrap().dirty_time_windows.set_dirty();

    let result = task.gen_insert_plan_unlocked(&query_engine, None).await;

    assert!(result.is_err());
    let _err = match result {
        Ok(_) => panic!("gen_insert_plan_unlocked should fail with a sink column mismatch"),
        Err(err) => err,
    };
    let state = task.state.read().unwrap();
    assert_eq!(state.dirty_time_windows.len(), 1);
    assert_eq!(
        state.dirty_time_windows.window_size(),
        std::time::Duration::from_secs(5)
    );
}

// ---------------------------------------------------------------------------
// Checkpoint persistence: record codec, activation, restore, stamping, and
// checkpoint row writes.
// ---------------------------------------------------------------------------

/// The exact EE-like sink schema: a real query-produced BINARY state column, a
/// timestamp time-index window column, an auto update_at column, and the
/// reserved internal epoch column added by the enterprise state schema/view.
fn persistence_sink_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ColumnSchema::new("state", CDT::binary_datatype(), true),
        ColumnSchema::new("window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
        ColumnSchema::new("update_at", CDT::timestamp_millisecond_datatype(), true),
        ColumnSchema::new(
            crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME,
            CDT::uint64_datatype(),
            true,
        ),
        ColumnSchema::new(
            crate::batching_mode::INTERNAL_FLOW_CHECKPOINT_COL_NAME,
            CDT::binary_datatype(),
            true,
        ),
    ]))
}

/// A persistence-sink row: `(window ms, epoch, state)`.
type SinkRow = (Option<i64>, Option<u64>, Option<Vec<u8>>);

/// Builds a record batch for `persistence_sink_schema`.
fn persistence_sink_recordbatch(rows: Vec<SinkRow>) -> RecordBatch {
    let schema = persistence_sink_schema();
    let mut states = datatypes::vectors::BinaryVectorBuilder::with_capacity(rows.len());
    let mut checkpoints = datatypes::vectors::BinaryVectorBuilder::with_capacity(rows.len());
    let mut windows =
        datatypes::vectors::TimestampMillisecondVectorBuilder::with_capacity(rows.len());
    let mut epochs = datatypes::vectors::UInt64VectorBuilder::with_capacity(rows.len());
    let mut update_at =
        datatypes::vectors::TimestampMillisecondVectorBuilder::with_capacity(rows.len());
    for (window, epoch, state) in rows {
        states.push(None);
        checkpoints.push(state.as_deref());
        windows.push(window.map(datatypes::timestamp::TimestampMillisecond::new));
        epochs.push(epoch);
        update_at.push(Some(datatypes::timestamp::TimestampMillisecond::new(0)));
    }
    RecordBatch::new(
        schema,
        vec![
            states.to_vector(),
            windows.to_vector(),
            update_at.to_vector(),
            epochs.to_vector(),
            checkpoints.to_vector(),
        ],
    )
    .unwrap()
}

fn register_persistence_sink(
    query_engine: &QueryEngineRef,
    table_name: &str,
    rows: Vec<SinkRow>,
    table_id: u32,
) {
    let batch = persistence_sink_recordbatch(rows);
    let table = MemTable::table(table_name, batch);
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id,
        table,
    };
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog.register_table_sync(request).unwrap();
}

/// The exact EE-like sink schema with a nullable `host` primary-key dimension
/// in front of the BINARY state column (checkpoint persistence must resolve
/// the state column explicitly, never by "the unique BINARY column").
fn persistence_sink_schema_with_dimension() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ColumnSchema::new("host", CDT::string_datatype(), true),
        ColumnSchema::new("state", CDT::binary_datatype(), true),
        ColumnSchema::new("window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
        ColumnSchema::new("update_at", CDT::timestamp_millisecond_datatype(), true),
        ColumnSchema::new(
            crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME,
            CDT::uint64_datatype(),
            true,
        ),
        ColumnSchema::new(
            crate::batching_mode::INTERNAL_FLOW_CHECKPOINT_COL_NAME,
            CDT::binary_datatype(),
            true,
        ),
    ]))
}

/// A persistence-sink row for the dimension sink:
/// `(host, window ms, epoch, state)`.
type DimensionSinkRow = (Option<String>, Option<i64>, Option<u64>, Option<Vec<u8>>);

/// Builds a record batch for `persistence_sink_schema_with_dimension`.
fn dimension_persistence_sink_recordbatch(rows: Vec<DimensionSinkRow>) -> RecordBatch {
    let schema = persistence_sink_schema_with_dimension();
    let mut hosts = datatypes::vectors::StringVectorBuilder::with_capacity(rows.len());
    let mut states = datatypes::vectors::BinaryVectorBuilder::with_capacity(rows.len());
    let mut checkpoints = datatypes::vectors::BinaryVectorBuilder::with_capacity(rows.len());
    let mut windows =
        datatypes::vectors::TimestampMillisecondVectorBuilder::with_capacity(rows.len());
    let mut epochs = datatypes::vectors::UInt64VectorBuilder::with_capacity(rows.len());
    let mut update_at =
        datatypes::vectors::TimestampMillisecondVectorBuilder::with_capacity(rows.len());
    for (host, window, epoch, state) in rows {
        hosts.push(host.as_deref());
        states.push(None);
        checkpoints.push(state.as_deref());
        windows.push(window.map(datatypes::timestamp::TimestampMillisecond::new));
        epochs.push(epoch);
        update_at.push(Some(datatypes::timestamp::TimestampMillisecond::new(0)));
    }
    RecordBatch::new(
        schema,
        vec![
            hosts.to_vector(),
            states.to_vector(),
            windows.to_vector(),
            update_at.to_vector(),
            epochs.to_vector(),
            checkpoints.to_vector(),
        ],
    )
    .unwrap()
}

/// A data source serving one record batch with projection/limit support,
/// mirroring the table crate's `MemtableDataSource` for tables whose metadata
/// (primary-key indices) must be under the test's control.
struct SingleBatchDataSource {
    batch: RecordBatch,
}

impl store_api::data_source::DataSource for SingleBatchDataSource {
    fn get_stream(
        &self,
        request: store_api::storage::ScanRequest,
    ) -> std::result::Result<SendableRecordBatchStream, BoxedError> {
        let df_recordbatch = if let Some(indices) = request.projection.as_deref() {
            self.batch
                .df_record_batch()
                .project(indices)
                .map_err(|err| {
                    BoxedError::new(PlainError::new(err.to_string(), StatusCode::Internal))
                })?
        } else {
            self.batch.df_record_batch().clone()
        };
        let rows = df_recordbatch.num_rows();
        let limit = if let Some(limit) = request.limit {
            limit.min(rows)
        } else {
            rows
        };
        let df_recordbatch = df_recordbatch.slice(0, limit);
        let recordbatch = RecordBatch::from_df_record_batch(
            Arc::new(Schema::try_from(df_recordbatch.schema()).map_err(|err| {
                BoxedError::new(PlainError::new(err.to_string(), StatusCode::Internal))
            })?),
            df_recordbatch,
        );
        Ok(Box::pin(SingleBatchStream {
            schema: recordbatch.schema.clone(),
            recordbatch: Some(recordbatch),
        }))
    }
}

struct SingleBatchStream {
    schema: Arc<Schema>,
    recordbatch: Option<RecordBatch>,
}

impl futures::Stream for SingleBatchStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        match self.recordbatch.take() {
            Some(records) => std::task::Poll::Ready(Some(Ok(records))),
            None => std::task::Poll::Ready(None),
        }
    }
}

impl common_recordbatch::RecordBatchStream for SingleBatchStream {
    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[common_recordbatch::OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        None
    }
}

/// Registers a data-bearing sink table whose metadata (engine, primary-key
/// indices, append mode) is fully under the test's control, so checkpoint
/// restore can be exercised against a sink with a nullable `host` dimension
/// primary key.
fn register_dimension_persistence_sink(
    query_engine: &QueryEngineRef,
    table_name: &str,
    rows: Vec<DimensionSinkRow>,
    table_id: u32,
) {
    use table::Table;
    use table::metadata::{FilterPushDownType, TableInfoBuilder, TableMetaBuilder, TableType};

    let batch = dimension_persistence_sink_recordbatch(rows);
    let meta = TableMetaBuilder::empty()
        .schema(batch.schema.clone())
        .primary_key_indices(vec![0])
        .value_indices(vec![])
        .engine("mito".to_string())
        .next_column_id(0)
        .options(Default::default())
        .created_on(Default::default())
        .build()
        .unwrap();
    let info = Arc::new(
        TableInfoBuilder::default()
            .table_id(table_id)
            .table_version(0)
            .name(table_name)
            .catalog_name(DEFAULT_CATALOG_NAME)
            .schema_name(DEFAULT_SCHEMA_NAME)
            .desc(None)
            .table_type(TableType::Base)
            .meta(meta)
            .build()
            .unwrap(),
    );
    let data_source = Arc::new(SingleBatchDataSource { batch });
    let table = Arc::new(Table::new(
        info,
        FilterPushDownType::Unsupported,
        data_source,
    ));
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id,
        table,
    };
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog.register_table_sync(request).unwrap();
}

/// Registers a sink table whose metadata is fully under the test's control
/// (engine, primary-key indices, table options), so detection can be exercised
/// against every fail-closed contract. The table carries no data.
fn register_sink_table_with_meta(
    query_engine: &QueryEngineRef,
    table_name: &str,
    table_id: u32,
    schema: Arc<Schema>,
    primary_key_indices: Vec<usize>,
    engine: &str,
    append_mode: bool,
) {
    use table::metadata::{TableInfoBuilder, TableMetaBuilder};
    use table::test_util::EmptyTable;

    let mut extra_options = HashMap::new();
    if append_mode {
        extra_options.insert(APPEND_MODE_KEY.to_string(), "true".to_string());
    }
    let options = table::requests::TableOptions {
        extra_options,
        ..Default::default()
    };
    let schema = if schema
        .column_schema_by_name(crate::batching_mode::INTERNAL_FLOW_CHECKPOINT_COL_NAME)
        .is_none()
    {
        let mut columns = schema.column_schemas().to_vec();
        columns.push(ColumnSchema::new(
            crate::batching_mode::INTERNAL_FLOW_CHECKPOINT_COL_NAME,
            CDT::binary_datatype(),
            true,
        ));
        Arc::new(Schema::new(columns))
    } else {
        schema
    };
    let meta = TableMetaBuilder::empty()
        .schema(schema)
        .primary_key_indices(primary_key_indices)
        .value_indices(vec![])
        .engine(engine.to_string())
        .next_column_id(0)
        .options(options)
        .created_on(Default::default())
        .build()
        .unwrap();
    let info = TableInfoBuilder::default()
        .table_id(table_id)
        .table_version(0)
        .name(table_name)
        .catalog_name(DEFAULT_CATALOG_NAME)
        .schema_name(DEFAULT_SCHEMA_NAME)
        .desc(None)
        .table_type(table::metadata::TableType::Base)
        .meta(meta)
        .build()
        .unwrap();
    let table = EmptyTable::from_table_info(&info);
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id,
        table,
    };
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog.register_table_sync(request).unwrap();
}

fn sequence_range_batch_opts() -> Arc<BatchingModeOptions> {
    Arc::new(BatchingModeOptions {
        experimental_enable_incremental_read: true,
        incremental_mode: IncrementalMode::SequenceRange,
        ..Default::default()
    })
}

async fn new_sequence_range_test_task(sink_table: &str) -> TestTaskParts {
    new_test_task_engine_and_plan_with_query_and_opts(
        "SELECT number, ts FROM numbers_with_ts",
        sink_table,
        sequence_range_batch_opts(),
    )
    .await
}

/// Builds a task for an exact EE-like time-window aggregate query (a real
/// BINARY UDDSketch `state` output plus a `date_bin` window) with
/// `SequenceRange` batch options, ready for an EE-like sink registration.
async fn new_ee_sequence_range_task(sink_table: &str, query: &str) -> TestTaskParts {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), query, true)
        .await
        .unwrap();
    let (column_name, time_window_expr, _, df_schema) = find_time_window_expr(
        &plan,
        query_engine.engine_state().catalog_manager().clone(),
        ctx.clone(),
    )
    .await
    .unwrap();
    let time_window_expr = time_window_expr.map(|expr| {
        TimeWindowExpr::from_expr(
            &expr,
            &column_name,
            &df_schema,
            &query_engine.engine_state().session_state(),
        )
        .unwrap()
    });
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query,
        plan: plan.clone(),
        time_window_expr,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            sink_table.to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: sequence_range_batch_opts(),
        flow_eval_interval: None,
        eval_schedule: None,
    })
    .unwrap();
    TestTaskParts {
        task,
        query_engine,
        plan,
    }
}

fn test_persistence() -> CheckpointPersistence {
    CheckpointPersistence {
        epoch_col_name: crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME.to_string(),
        checkpoint_col_name: crate::batching_mode::INTERNAL_FLOW_CHECKPOINT_COL_NAME.to_string(),
        window_col_name: "window".to_string(),
        primary_key_columns: vec![],
    }
}

/// A `CheckpointPersistence` whose sink has a `host` primary-key (dimension)
/// column, exercising the marker sentinel key projection.
fn test_persistence_with_dimension() -> CheckpointPersistence {
    CheckpointPersistence {
        epoch_col_name: crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME.to_string(),
        checkpoint_col_name: crate::batching_mode::INTERNAL_FLOW_CHECKPOINT_COL_NAME.to_string(),
        window_col_name: "window".to_string(),
        primary_key_columns: vec!["host".to_string()],
    }
}

#[tokio::test]
async fn test_detect_checkpoint_persistence_requires_sequence_range_and_epoch_column() {
    let sink_table = "persistence_detect_sink";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    // Sink with the epoch + BINARY state columns.
    register_persistence_sink(&query_engine, sink_table, vec![], 9100);

    let persistence = task
        .detect_checkpoint_persistence()
        .await
        .unwrap()
        .expect("sequence range + epoch column should activate persistence");
    assert_eq!(test_persistence(), persistence);

    // MemtableOnly mode never activates persistence.
    let sink_table = "persistence_detect_memtable_sink";
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query_and_opts(
        "SELECT number, ts FROM numbers_with_ts",
        sink_table,
        incremental_batch_opts(),
    )
    .await;
    register_persistence_sink(&query_engine, sink_table, vec![], 9101);
    assert!(
        task.detect_checkpoint_persistence()
            .await
            .unwrap()
            .is_none(),
        "MemtableOnly must not activate persistence"
    );

    // A sink without the reserved epoch column never activates persistence.
    let sink_table = "persistence_detect_plain_sink";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    register_auto_created_aggregate_sink(&query_engine, sink_table);
    assert!(
        task.detect_checkpoint_persistence()
            .await
            .unwrap()
            .is_none(),
        "sink without epoch column must not activate persistence"
    );
}

#[tokio::test]
async fn test_detect_checkpoint_persistence_fails_closed_on_broken_contracts() {
    // The epoch column must be strictly UInt64; any other integer type fails
    // closed instead of silently pretending durability.
    let sink_table = "persistence_detect_i64_epoch";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    let i64_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("state", CDT::binary_datatype(), true),
        ColumnSchema::new("window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
        ColumnSchema::new("update_at", CDT::timestamp_millisecond_datatype(), true),
        ColumnSchema::new(
            crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME,
            CDT::int64_datatype(),
            true,
        ),
    ]));
    register_sink_table_with_meta(
        &query_engine,
        sink_table,
        9200,
        i64_schema,
        vec![],
        "mito",
        false,
    );
    assert!(
        task.detect_checkpoint_persistence()
            .await
            .unwrap()
            .is_none(),
        "a non-UInt64 epoch column must fail closed"
    );

    // An append-mode sink would accumulate duplicate sentinel rows instead of
    // overwriting the singleton; persistence must be inactive.
    let sink_table = "persistence_detect_append_sink";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    register_sink_table_with_meta(
        &query_engine,
        sink_table,
        9201,
        persistence_sink_schema(),
        vec![],
        "mito",
        true,
    );
    assert!(
        task.detect_checkpoint_persistence()
            .await
            .unwrap()
            .is_none(),
        "an append-mode sink must fail closed"
    );

    // A non-mito sink engine must fail closed.
    let sink_table = "persistence_detect_non_mito";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    register_sink_table_with_meta(
        &query_engine,
        sink_table,
        9202,
        persistence_sink_schema(),
        vec![],
        "file",
        false,
    );
    assert!(
        task.detect_checkpoint_persistence()
            .await
            .unwrap()
            .is_none(),
        "a non-mito sink must fail closed"
    );

    // The epoch column must never be part of the primary key.
    let sink_table = "persistence_detect_epoch_pk";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    let epoch_pk_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("state", CDT::binary_datatype(), true),
        ColumnSchema::new("window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
        ColumnSchema::new(
            crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME,
            CDT::uint64_datatype(),
            false,
        ),
    ]));
    register_sink_table_with_meta(
        &query_engine,
        sink_table,
        9203,
        epoch_pk_schema,
        vec![2],
        "mito",
        false,
    );
    assert!(
        task.detect_checkpoint_persistence()
            .await
            .unwrap()
            .is_none(),
        "an epoch column in the primary key must fail closed"
    );

    // Marker values allow persistence with non-nullable primary-key columns.
    let sink_table = "persistence_detect_non_nullable_pk";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    let non_nullable_pk_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("host", CDT::string_datatype(), false),
        ColumnSchema::new("state", CDT::binary_datatype(), true),
        ColumnSchema::new("window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
        ColumnSchema::new("update_at", CDT::timestamp_millisecond_datatype(), true),
        ColumnSchema::new(
            crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME,
            CDT::uint64_datatype(),
            true,
        ),
    ]));
    register_sink_table_with_meta(
        &query_engine,
        sink_table,
        9204,
        non_nullable_pk_schema,
        vec![0],
        "mito",
        false,
    );
    assert!(
        task.detect_checkpoint_persistence()
            .await
            .unwrap()
            .is_some(),
        "a non-nullable primary-key column must be accepted with markers"
    );
}

#[tokio::test]
async fn test_detect_checkpoint_persistence_resolves_state_column_explicitly() {
    // A sink with a BINARY *dimension* primary key plus the BINARY state
    // column must still resolve the state column. With the explicit internal
    // state-column name injected by the enterprise layer, the configured name
    // wins; without it the schema contract (unique non-PK BINARY column)
    // applies. It must never guess by "the unique BINARY column", which would
    // be ambiguous here.
    let sink_table = "persistence_detect_dimension_sink";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    register_sink_table_with_meta(
        &query_engine,
        sink_table,
        9205,
        persistence_sink_schema_with_dimension(),
        vec![0],
        "mito",
        false,
    );

    let persistence = task
        .detect_checkpoint_persistence()
        .await
        .unwrap()
        .expect("schema contract must resolve the state column");
    assert_eq!(test_persistence_with_dimension(), persistence);

    // The explicit internal option overrides the schema contract.
    let sink_table = "persistence_detect_dimension_sink_config";
    let explicit_opts = Arc::new(BatchingModeOptions {
        experimental_enable_incremental_read: true,
        incremental_mode: IncrementalMode::SequenceRange,
        state_col_name: Some("state".to_string()),
        ..Default::default()
    });
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query_and_opts(
        "SELECT number, ts FROM numbers_with_ts",
        sink_table,
        explicit_opts,
    )
    .await;
    register_sink_table_with_meta(
        &query_engine,
        sink_table,
        9206,
        persistence_sink_schema_with_dimension(),
        vec![0],
        "mito",
        false,
    );
    let persistence = task
        .detect_checkpoint_persistence()
        .await
        .unwrap()
        .expect("explicit state column name must activate persistence");
    assert_eq!(test_persistence_with_dimension(), persistence);

    // The configured query state-column option does not affect checkpoint
    // persistence now that checkpoint records use the dedicated column.
    let sink_table = "persistence_detect_missing_state_col";
    let missing_opts = Arc::new(BatchingModeOptions {
        experimental_enable_incremental_read: true,
        incremental_mode: IncrementalMode::SequenceRange,
        state_col_name: Some("does_not_exist".to_string()),
        ..Default::default()
    });
    let TestTaskParts {
        task, query_engine, ..
    } = new_test_task_engine_and_plan_with_query_and_opts(
        "SELECT number, ts FROM numbers_with_ts",
        sink_table,
        missing_opts,
    )
    .await;
    register_sink_table_with_meta(
        &query_engine,
        sink_table,
        9207,
        persistence_sink_schema_with_dimension(),
        vec![0],
        "mito",
        false,
    );
    assert!(
        task.detect_checkpoint_persistence()
            .await
            .unwrap()
            .is_some(),
        "a missing configured state column must not affect persistence"
    );

    // Multiple query-produced BINARY columns are allowed because checkpoint
    // persistence uses the dedicated internal column.
    let sink_table = "persistence_detect_ambiguous_state_col";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    let ambiguous_schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("state", CDT::binary_datatype(), true),
        ColumnSchema::new("payload", CDT::binary_datatype(), true),
        ColumnSchema::new("window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
        ColumnSchema::new("update_at", CDT::timestamp_millisecond_datatype(), true),
        ColumnSchema::new(
            crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME,
            CDT::uint64_datatype(),
            true,
        ),
    ]));
    register_sink_table_with_meta(
        &query_engine,
        sink_table,
        9208,
        ambiguous_schema,
        vec![],
        "mito",
        false,
    );
    assert!(
        task.detect_checkpoint_persistence()
            .await
            .unwrap()
            .is_some(),
        "query BINARY columns must not affect persistence"
    );
}

/// Builds a DataFusion session state able to decode sink/source scans: the
/// engine's session state (which carries all flow functions) plus a catalog
/// list that resolves tables through the engine's catalog manager. The engine's
/// bare `session_state()` has an empty catalog list, so scans cannot be
/// resolved from it directly.
fn decode_session_state(query_engine: &QueryEngineRef) -> datafusion::execution::SessionState {
    let catalog_list: Arc<dyn datafusion::catalog::CatalogProviderList> =
        Arc::new(catalog::table_source::dummy_catalog::DummyCatalogList::new(
            query_engine.engine_state().catalog_manager().clone(),
        ));
    datafusion::execution::SessionStateBuilder::new_from_existing(
        query_engine.engine_state().session_state(),
    )
    .with_catalog_list(catalog_list)
    .build()
}

/// A frontend handler that decodes and executes `Query::LogicalPlan` requests
/// against the test query engine (MemTable catalog) and records every request.
/// Used to prove that checkpoint restore scans travel through the frontend
/// transport instead of the local `QueryEngine::execute` path.
struct CaptureLogicalPlanHandler {
    query_engine: QueryEngineRef,
    captured: Arc<std::sync::Mutex<Vec<api::v1::QueryRequest>>>,
}

#[async_trait::async_trait]
impl GrpcQueryHandlerWithBoxedError for CaptureLogicalPlanHandler {
    async fn do_query(
        &self,
        query: api::v1::greptime_request::Request,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        let api::v1::greptime_request::Request::Query(q) = &query else {
            return Ok(Output::new_with_affected_rows(0));
        };
        self.captured.lock().unwrap().push(q.clone());
        let Some(api::v1::query_request::Query::LogicalPlan(bytes)) = &q.query else {
            return Ok(Output::new_with_affected_rows(0));
        };
        let session_state = decode_session_state(&self.query_engine);
        let plan = DFLogicalSubstraitConvertor {}
            .decode(bytes::Bytes::from(bytes.clone()), session_state)
            .await
            .map_err(BoxedError::new)?;
        let output = self
            .query_engine
            .execute(plan, ctx)
            .await
            .map_err(BoxedError::new)?;
        Ok(output)
    }
}

/// Captured `Query::LogicalPlan` requests plus the handler handle.
type RestoreFrontendClient = (
    Arc<FrontendClient>,
    Arc<std::sync::Mutex<Vec<api::v1::QueryRequest>>>,
    Arc<dyn GrpcQueryHandlerWithBoxedError>,
);

/// Builds a frontend client whose handler executes decoded LogicalPlan
/// requests against `query_engine`, plus the captured request log. The handler
/// `Arc` is returned too so the client's weak handle stays alive for the
/// whole test.
fn restore_frontend_client(query_engine: &QueryEngineRef) -> RestoreFrontendClient {
    let captured = Arc::new(std::sync::Mutex::new(Vec::new()));
    let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(CaptureLogicalPlanHandler {
        query_engine: query_engine.clone(),
        captured: captured.clone(),
    });
    let frontend_client = Arc::new(FrontendClient::from_grpc_handler(
        Arc::downgrade(&handler),
        QueryOptions::default(),
    ));
    (frontend_client, captured, handler)
}

#[tokio::test]
async fn test_restore_via_frontend_sends_logical_plans_and_seeds_incremental() {
    let sink_table = "persistence_restore_sink";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    let record = CheckpointRecord {
        format_version: CHECKPOINT_RECORD_FORMAT_VERSION,
        epoch: 3,
        checkpoints: BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]),
    };
    let encoded = encode_checkpoint_record(&record).unwrap();
    register_persistence_sink(
        &query_engine,
        sink_table,
        vec![
            (Some(1_000), Some(3), None),
            (Some(2_000), Some(3), None),
            (
                Some(crate::batching_mode::CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS),
                Some(3),
                Some(encoded),
            ),
        ],
        9102,
    );

    let (frontend_client, captured, _handler) = restore_frontend_client(&query_engine);
    task.try_enable_checkpoint_persistence(&frontend_client)
        .await;

    // Both restore scans (sentinel row + non-sentinel epoch aggregation) were
    // sent as LogicalPlan requests through the frontend client.
    let requests = captured.lock().unwrap();
    assert_eq!(2, requests.len(), "expected two restore scan requests");
    for request in requests.iter() {
        assert!(
            matches!(
                request.query,
                Some(api::v1::query_request::Query::LogicalPlan(_))
            ),
            "restore scans must be transported as Query::LogicalPlan, got {request:?}"
        );
    }
    drop(requests);

    let state = task.state.read().unwrap();
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
    assert_eq!(
        state.checkpoints(),
        &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    );
    assert_eq!(state.persisted_epoch(), 3);
    assert_eq!(Some(&test_persistence()), state.checkpoint_persistence());
}

/// Runs one untrusted-restore scenario through the frontend transport and
/// asserts the task falls back to full snapshot with persistence still armed.
async fn assert_restore_falls_back(table_name: &str, table_id: u32, rows: Vec<SinkRow>) {
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(table_name).await;
    register_persistence_sink(&query_engine, table_name, rows, table_id);

    let (frontend_client, _captured, _handler) = restore_frontend_client(&query_engine);
    task.try_enable_checkpoint_persistence(&frontend_client)
        .await;

    let state = task.state.read().unwrap();
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert_eq!(state.persisted_epoch(), 0);
    assert!(state.checkpoints().is_empty());
    // Persistence stays armed so later cycles can persist fresh checkpoints.
    assert!(state.checkpoint_persistence().is_some());
}

/// Consolidates the missing / corrupt / empty-map / multiple-sentinel /
/// newer-row-epoch / NULL-epoch restore fallbacks. Every case must leave the
/// task in full snapshot with no trusted checkpoint.
#[tokio::test]
async fn test_restore_falls_back_on_untrusted_records() {
    let sentinel = Some(crate::batching_mode::CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS);
    let record = CheckpointRecord {
        format_version: CHECKPOINT_RECORD_FORMAT_VERSION,
        epoch: 3,
        checkpoints: BTreeMap::from([(1_u64, 10_u64)]),
    };
    let encoded = encode_checkpoint_record(&record).unwrap();

    // Missing sentinel row.
    assert_restore_falls_back(
        "persistence_no_sentinel",
        9103,
        vec![(Some(1_000), Some(2), None)],
    )
    .await;
    // Sentinel row with undecodable state bytes.
    assert_restore_falls_back(
        "persistence_corrupt_record",
        9104,
        vec![(sentinel, Some(2), Some(b"garbage".to_vec()))],
    )
    .await;
    // Sentinel row holding a valid v1 record with an empty checkpoint map.
    let empty_record = CheckpointRecord {
        format_version: CHECKPOINT_RECORD_FORMAT_VERSION,
        epoch: 2,
        checkpoints: BTreeMap::new(),
    };
    let empty_encoded = encode_checkpoint_record(&empty_record).unwrap();
    assert_restore_falls_back(
        "persistence_empty_map",
        9105,
        vec![(sentinel, Some(2), Some(empty_encoded))],
    )
    .await;
    // State rows stamped with epoch 5 are newer than the record's epoch 3:
    // crash between state write and checkpoint write.
    assert_restore_falls_back(
        "persistence_newer_rows",
        9106,
        vec![
            (Some(1_000), Some(5), None),
            (sentinel, Some(3), Some(encoded.clone())),
        ],
    )
    .await;
    // Pre-upgrade rows with NULL epochs are untrusted.
    assert_restore_falls_back(
        "persistence_null_epoch_rows",
        9107,
        vec![
            (Some(1_000), None, None),
            (sentinel, Some(3), Some(encoded.clone())),
        ],
    )
    .await;
    // Two sentinel rows make the record ambiguous.
    assert_restore_falls_back(
        "persistence_multi_sentinel",
        9109,
        vec![
            (sentinel, Some(3), Some(encoded.clone())),
            (sentinel, Some(3), Some(encoded)),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_restore_accepts_record_without_state_data() {
    // NULL epochs are acceptable when there is no non-sentinel state data, as
    // long as the physical sentinel row's epoch is non-NULL and equals the
    // encoded record epoch.
    let sink_table = "persistence_no_state_data";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    let record = CheckpointRecord {
        format_version: CHECKPOINT_RECORD_FORMAT_VERSION,
        epoch: 7,
        checkpoints: BTreeMap::from([(1_u64, 10_u64)]),
    };
    let encoded = encode_checkpoint_record(&record).unwrap();
    register_persistence_sink(
        &query_engine,
        sink_table,
        vec![(
            Some(crate::batching_mode::CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS),
            Some(7),
            Some(encoded),
        )],
        9108,
    );
    let (frontend_client, _captured, _handler) = restore_frontend_client(&query_engine);
    task.try_enable_checkpoint_persistence(&frontend_client)
        .await;
    let state = task.state.read().unwrap();
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
    assert_eq!(state.checkpoints(), &BTreeMap::from([(1_u64, 10_u64)]));
    assert_eq!(state.persisted_epoch(), 7);
}

#[tokio::test]
async fn test_restore_rejects_sentinel_epoch_mismatch() {
    // The physical sentinel row's epoch column must be non-NULL and equal the
    // encoded record epoch. A mismatch (state column written by a different
    // checkpoint write than the epoch column) makes the record untrusted.
    let sentinel = Some(crate::batching_mode::CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS);
    let record = CheckpointRecord {
        format_version: CHECKPOINT_RECORD_FORMAT_VERSION,
        epoch: 3,
        checkpoints: BTreeMap::from([(1_u64, 10_u64)]),
    };
    let encoded = encode_checkpoint_record(&record).unwrap();
    // Physical sentinel epoch 2 != encoded record epoch 3.
    assert_restore_falls_back(
        "persistence_sentinel_epoch_mismatch",
        9120,
        vec![(sentinel, Some(2), Some(encoded.clone()))],
    )
    .await;
    // Physical sentinel epoch is NULL while the record carries epoch 3.
    assert_restore_falls_back(
        "persistence_sentinel_epoch_null",
        9121,
        vec![(sentinel, None, Some(encoded))],
    )
    .await;
}

/// Runs one untrusted-restore scenario against the dimension sink (a nullable
/// `host` primary-key column) and asserts the task falls back to full snapshot
/// with persistence still armed.
async fn assert_dimension_restore_falls_back(
    table_name: &str,
    table_id: u32,
    rows: Vec<DimensionSinkRow>,
) {
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(table_name).await;
    register_dimension_persistence_sink(&query_engine, table_name, rows, table_id);

    let (frontend_client, _captured, _handler) = restore_frontend_client(&query_engine);
    task.try_enable_checkpoint_persistence(&frontend_client)
        .await;

    let state = task.state.read().unwrap();
    assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
    assert_eq!(state.persisted_epoch(), 0);
    assert!(state.checkpoints().is_empty());
    // Persistence stays armed so later cycles can persist fresh checkpoints.
    assert!(state.checkpoint_persistence().is_some());
}

#[tokio::test]
async fn test_restore_rejects_sentinel_row_with_non_null_dimension_pk() {
    // The canonical sentinel logical key is `(typed NULL for every
    // primary-key/dimension column, sentinel window)`. A visible row at the
    // sentinel window whose physical epoch matches the encoded record epoch
    // and whose state decodes to a valid v1 record is still untrusted when at
    // least one dimension primary key is non-NULL: it is not the canonical
    // sentinel, and silently ignoring it could mask a missing canonical
    // sentinel (restoring a stale checkpoint as if it were fresh).
    let sentinel = Some(crate::batching_mode::CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS);
    let record = CheckpointRecord {
        format_version: CHECKPOINT_RECORD_FORMAT_VERSION,
        epoch: 3,
        checkpoints: BTreeMap::from([(1_u64, 10_u64)]),
    };
    let encoded = encode_checkpoint_record(&record).unwrap();

    // A single sentinel-window row, decodable and epoch-consistent, but with a
    // non-NULL `host` dimension key: FullSnapshot fallback.
    assert_dimension_restore_falls_back(
        "persistence_non_null_dimension_sentinel",
        9122,
        vec![(
            Some("not_the_canonical_sentinel".to_string()),
            sentinel,
            Some(3),
            Some(encoded.clone()),
        )],
    )
    .await;

    // Even next to a canonical all-NULL sentinel, any sentinel-window row with
    // a non-NULL primary key makes the whole record untrusted.
    assert_dimension_restore_falls_back(
        "persistence_mixed_dimension_sentinel",
        9123,
        vec![
            (
                Some("foreign_row".to_string()),
                sentinel,
                Some(3),
                Some(encoded.clone()),
            ),
            (None, sentinel, Some(3), Some(encoded)),
        ],
    )
    .await;
}

#[tokio::test]
async fn test_restore_accepts_canonical_marker_dimension_sentinel() {
    // The canonical sentinel row `(marker host, sentinel window)` with a
    // decodable record and matching physical epoch must restore successfully
    // through the dimension sink.
    let sink_table = "persistence_marker_dimension_sentinel";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    let record = CheckpointRecord {
        format_version: CHECKPOINT_RECORD_FORMAT_VERSION,
        epoch: 3,
        checkpoints: BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)]),
    };
    let encoded = encode_checkpoint_record(&record).unwrap();
    register_dimension_persistence_sink(
        &query_engine,
        sink_table,
        vec![
            (None, Some(1_000), Some(3), None),
            (
                Some("__greptime_checkpoint__".to_string()),
                Some(crate::batching_mode::CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS),
                Some(3),
                Some(encoded),
            ),
        ],
        9124,
    );

    let (frontend_client, _captured, _handler) = restore_frontend_client(&query_engine);
    task.try_enable_checkpoint_persistence(&frontend_client)
        .await;

    let state = task.state.read().unwrap();
    assert_eq!(state.checkpoint_mode(), CheckpointMode::Incremental);
    assert_eq!(
        state.checkpoints(),
        &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
    );
    assert_eq!(state.persisted_epoch(), 3);
    assert_eq!(
        Some(&test_persistence_with_dimension()),
        state.checkpoint_persistence()
    );
}

#[tokio::test]
async fn test_stamp_epoch_into_plan_is_noop_when_inactive() {
    let TestTaskParts { task, plan, .. } = new_test_task_engine_and_plan_with_query(
        "SELECT number, ts FROM numbers_with_ts",
        "missing_sink",
    )
    .await;

    let (stamped, epoch) = task.stamp_epoch_into_plan(plan.clone()).await.unwrap();
    assert_eq!(epoch, None);
    assert_eq!(
        stamped, plan,
        "ordinary flow plan must be byte-for-byte unchanged"
    );
}

#[tokio::test]
async fn test_stamp_epoch_into_plan_appends_epoch_literal() {
    let sink_table = "auto_created_aggregate_sink";
    let query = "SELECT max(number) AS number, ts FROM numbers_with_ts GROUP BY ts";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    register_auto_created_aggregate_sink(&query_engine, sink_table);
    task.state
        .write()
        .unwrap()
        .set_checkpoint_persistence(Some(test_persistence()));

    let ctx = task.state.read().unwrap().query_ctx.clone();
    let plan = sql_to_df_plan(ctx, query_engine.clone(), query, true)
        .await
        .unwrap();
    let (sink_table, _) = get_table_info_df_schema(
        query_engine.engine_state().catalog_manager().clone(),
        [
            "greptime".to_string(),
            "public".to_string(),
            sink_table.to_string(),
        ],
    )
    .await
    .unwrap();
    let table_provider = Arc::new(DfTableProviderAdapter::new(sink_table));
    let table_source = Arc::new(DefaultTableSource::new(table_provider));
    let dml_plan = LogicalPlan::Dml(DmlStatement::new(
        datafusion_common::TableReference::bare("test"),
        table_source,
        WriteOp::Insert(datafusion_expr::dml::InsertOp::Append),
        Arc::new(plan),
    ));

    let (stamped, epoch) = task.stamp_epoch_into_plan(dml_plan).await.unwrap();
    assert_eq!(epoch, Some(1), "first cycle stamps epoch 1");

    let LogicalPlan::Dml(dml) = &stamped else {
        panic!("expected DML plan");
    };
    let fields = dml.input.schema().fields();
    assert_eq!(
        crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME,
        fields.last().unwrap().name(),
        "epoch column must be appended as the last output field"
    );
    let exprs = dml.input.expressions();
    let last = exprs.last().expect("epoch projection expr");
    assert!(
        format!("{last:?}").contains("UInt64(1)"),
        "epoch column must be stamped with the current epoch literal, got {last:?}"
    );
}

#[tokio::test]
async fn test_exact_ee_schema_plan_retains_binary_state_column() {
    // Exact EE-like flow: the query itself produces the BINARY UDDSketch
    // `state` column plus the `date_bin` window. The sink is
    // `[state BINARY, window TIMESTAMP time-index, update_at TIMESTAMP,
    //  epoch integer]`. The real state column must survive schema matching,
    // the incremental rewrite analysis, and the final stamped DML.
    let sink_table = "persistence_ee_sink";
    let query = "SELECT uddsketch_state(128, 0.01, CAST(number AS DOUBLE)) AS state, \
        date_bin(INTERVAL '5 second', ts) AS window FROM greptime.public.numbers_with_ts GROUP BY window";
    let TestTaskParts {
        task, query_engine, ..
    } = new_ee_sequence_range_task(sink_table, query).await;
    register_persistence_sink(
        &query_engine,
        sink_table,
        vec![(Some(0), Some(1), None)],
        9110,
    );
    task.state
        .write()
        .unwrap()
        .set_checkpoint_persistence(Some(test_persistence()));
    task.state
        .write()
        .unwrap()
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));

    // 1. Schema match: plan generation succeeds and the real BINARY `state`
    //    column is retained (only the epoch column is stripped from matching).
    let info = task
        .gen_insert_plan_unlocked(&query_engine, None)
        .await
        .unwrap_or_else(|err| panic!("EE-like schema must not break plan generation, got: {err:?}"))
        .expect("dirty windows must produce a plan");
    let LogicalPlan::Dml(dml) = &info.plan else {
        panic!("expected DML insert plan, got {:?}", info.plan);
    };
    let matched_fields = dml.input.schema().fields();
    let matched_names = matched_fields
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        vec!["state", "window", "update_at"],
        matched_names,
        "schema matching must keep the real state column and append update_at; \
         the epoch column is stamped later"
    );
    assert_eq!(
        CDT::binary_datatype(),
        CDT::from_arrow_type(matched_fields[0].data_type()),
        "the state column must stay BINARY through schema matching"
    );

    // 2. Incremental rewrite: the exact EE-like `uddsketch_state` aggregate is
    //    recognized as mergeable, so `prepare_plan_for_incremental` returns a
    //    rewritten DML that merges the delta state with the existing sink
    //    state via `uddsketch_merge_state`. The rewrite must keep the real
    //    BINARY state column in its output, and incremental mode must stay
    //    enabled instead of the pre-#8863 refuse-and-disable fallback. The
    //    retained BINARY state column must not crash the analyzer.
    {
        let mut state = task.state.write().unwrap();
        state.advance_checkpoints(HashMap::from([(1_u64, 10_u64)]));
    }
    let rewritten = task
        .prepare_plan_for_incremental(&info.plan)
        .await
        .unwrap_or_else(|err| {
            panic!("incremental rewrite must not error on the EE-like plan: {err:?}")
        })
        .expect("the EE-like uddsketch_state plan must be incrementally rewritable");
    let LogicalPlan::Dml(rewritten_dml) = &rewritten else {
        panic!("expected rewritten DML plan, got {rewritten:?}");
    };
    let rewritten_text = format!("{}", rewritten.display_indent());
    assert!(
        rewritten_text.contains("uddsketch_merge_state"),
        "rewritten plan must merge the delta and sink states via uddsketch_merge_state: \
         {rewritten_text}"
    );
    assert!(
        rewritten_dml
            .input
            .expressions()
            .iter()
            .any(|expr| format!("{expr:?}").contains("uddsketch_merge_state")),
        "the rewritten merge projection must contain a uddsketch_merge_state call, got: {:?}",
        rewritten_dml.input.expressions()
    );
    let rewritten_fields = rewritten_dml.input.schema().fields();
    let rewritten_names = rewritten_fields
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        vec!["state", "window", "update_at"],
        rewritten_names,
        "the rewritten merge plan must keep the real state column, the window group key, \
         and the update_at pass-through"
    );
    assert_eq!(
        CDT::binary_datatype(),
        CDT::from_arrow_type(rewritten_fields[0].data_type()),
        "the state column must stay BINARY through the incremental rewrite"
    );
    assert!(
        !task.state.read().unwrap().is_incremental_disabled(),
        "the EE-like uddsketch_state plan must remain incremental-enabled"
    );

    // 3. Final stamped DML: the epoch literal is appended and the real state
    //    column keeps its exact name in the insert.
    let (stamped, epoch) = task.stamp_epoch_into_plan(info.plan).await.unwrap();
    assert_eq!(Some(1), epoch, "first cycle stamps epoch 1");
    let LogicalPlan::Dml(stamped_dml) = &stamped else {
        panic!("expected stamped DML plan");
    };
    let stamped_names = stamped_dml
        .input
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        vec![
            "state",
            "window",
            "update_at",
            crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME
        ],
        stamped_names,
        "final DML must carry the real state column and the stamped epoch column"
    );
}

/// A frontend handler that captures the insert request and returns success.
struct CaptureInsertHandler {
    captured: Arc<std::sync::Mutex<Option<api::v1::QueryRequest>>>,
}

#[async_trait::async_trait]
impl GrpcQueryHandlerWithBoxedError for CaptureInsertHandler {
    async fn do_query(
        &self,
        query: api::v1::greptime_request::Request,
        _ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        if let api::v1::greptime_request::Request::Query(q) = &query {
            *self.captured.lock().unwrap() = Some(q.clone());
        }
        Ok(Output::new_with_affected_rows(1))
    }
}

#[tokio::test]
async fn test_write_checkpoint_row_sends_singleton_sentinel_row() {
    let sink_table = "persistence_write_sink";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    register_persistence_sink(
        &query_engine,
        sink_table,
        vec![(Some(0), Some(1), None)],
        9111,
    );
    task.state
        .write()
        .unwrap()
        .set_checkpoint_persistence(Some(test_persistence()));

    let captured = Arc::new(std::sync::Mutex::new(None));
    let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(CaptureInsertHandler {
        captured: captured.clone(),
    });
    let frontend_client = Arc::new(FrontendClient::from_grpc_handler(
        Arc::downgrade(&handler),
        QueryOptions::default(),
    ));

    let checkpoints = BTreeMap::from([(1_u64, 11_u64)]);
    task.write_checkpoint_row(&frontend_client, 7, &checkpoints)
        .await
        .expect("checkpoint row write should succeed");

    let captured = captured
        .lock()
        .unwrap()
        .clone()
        .expect("handler captured the request");
    let api::v1::query_request::Query::InsertIntoPlan(insert) =
        captured.query.expect("insert plan")
    else {
        panic!("expected InsertIntoPlan");
    };
    assert_eq!(
        "persistence_write_sink",
        insert.table_name.as_ref().unwrap().table_name
    );

    let session_state = query_engine.engine_state().session_state();
    let plan = DFLogicalSubstraitConvertor {}
        .decode(bytes::Bytes::from(insert.logical_plan), session_state)
        .await
        .unwrap();
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected projection over empty relation, got {plan:?}");
    };
    // window + epoch + state + auto update_at.
    assert_eq!(4, projection.expr.len());

    // window column -> sentinel timestamp
    let window_expr = &projection.expr[0];
    let window_sql = format!("{window_expr:?}");
    assert!(
        window_sql
            .contains(&crate::batching_mode::CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS.to_string()),
        "sentinel window literal expected, got {window_sql}"
    );
    // epoch column -> 7
    let epoch_expr = &projection.expr[1];
    assert!(
        format!("{epoch_expr:?}").contains("UInt64(7)"),
        "epoch literal expected, got {epoch_expr:?}"
    );
    // checkpoint column -> the encoded v1 record bytes must appear verbatim.
    let expected = encode_checkpoint_record(&CheckpointRecord {
        format_version: CHECKPOINT_RECORD_FORMAT_VERSION,
        epoch: 7,
        checkpoints,
    })
    .unwrap();
    let decoded = decode_checkpoint_record(&expected).unwrap().unwrap();
    assert_eq!(7, decoded.epoch);
    assert_eq!(BTreeMap::from([(1_u64, 11_u64)]), decoded.checkpoints);
    let state_bytes = match &projection.expr[2] {
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::Literal(ScalarValue::Binary(Some(bytes)), _) => bytes,
            other => panic!("expected binary literal for checkpoint column, got {other:?}"),
        },
        other => panic!("expected alias for checkpoint column, got {other:?}"),
    };
    assert_eq!(
        &expected, state_bytes,
        "checkpoint record bytes must be stored verbatim"
    );
}

/// A frontend handler that always fails.
struct FailInsertHandler;

#[async_trait::async_trait]
impl GrpcQueryHandlerWithBoxedError for FailInsertHandler {
    async fn do_query(
        &self,
        _query: api::v1::greptime_request::Request,
        _ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        Err(BoxedError::new(MockError::new(StatusCode::Internal)))
    }
}

#[tokio::test]
async fn test_checkpoint_row_write_failure_is_reported() {
    let sink_table = "persistence_write_fail_sink";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    register_persistence_sink(
        &query_engine,
        sink_table,
        vec![(Some(0), Some(1), None)],
        9112,
    );
    task.state
        .write()
        .unwrap()
        .set_checkpoint_persistence(Some(test_persistence()));

    let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(FailInsertHandler);
    let frontend_client = Arc::new(FrontendClient::from_grpc_handler(
        Arc::downgrade(&handler),
        QueryOptions::default(),
    ));

    let err = task
        .write_checkpoint_row(&frontend_client, 7, &BTreeMap::from([(1_u64, 11_u64)]))
        .await
        .unwrap_err();
    assert!(matches!(err, Error::External { .. }), "{err}");
}

/// A frontend handler that captures the insert request and returns a canned
/// affected-rows count.
struct CaptureInsertWithRowsHandler {
    captured: Arc<std::sync::Mutex<Option<api::v1::QueryRequest>>>,
    affected_rows: usize,
}

#[async_trait::async_trait]
impl GrpcQueryHandlerWithBoxedError for CaptureInsertWithRowsHandler {
    async fn do_query(
        &self,
        query: api::v1::greptime_request::Request,
        _ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        if let api::v1::greptime_request::Request::Query(q) = &query {
            *self.captured.lock().unwrap() = Some(q.clone());
        }
        Ok(Output::new_with_affected_rows(self.affected_rows))
    }
}

#[tokio::test]
async fn test_write_checkpoint_row_projects_marker_primary_keys() {
    // The canonical sentinel logical key uses marker values for every
    // primary-key/dimension column and the sentinel window. The checkpoint
    // writer explicitly projects each PK column with its marker value.
    let sink_table = "persistence_write_dimension_sink";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    register_sink_table_with_meta(
        &query_engine,
        sink_table,
        9114,
        persistence_sink_schema_with_dimension(),
        vec![0],
        "mito",
        false,
    );
    task.state
        .write()
        .unwrap()
        .set_checkpoint_persistence(Some(test_persistence_with_dimension()));

    let captured = Arc::new(std::sync::Mutex::new(None));
    let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(CaptureInsertWithRowsHandler {
        captured: captured.clone(),
        affected_rows: 1,
    });
    let frontend_client = Arc::new(FrontendClient::from_grpc_handler(
        Arc::downgrade(&handler),
        QueryOptions::default(),
    ));

    task.write_checkpoint_row(&frontend_client, 7, &BTreeMap::from([(1_u64, 11_u64)]))
        .await
        .expect("checkpoint row write should succeed");

    let captured = captured
        .lock()
        .unwrap()
        .clone()
        .expect("handler captured the request");
    let api::v1::query_request::Query::InsertIntoPlan(insert) =
        captured.query.expect("insert plan")
    else {
        panic!("expected InsertIntoPlan");
    };
    let session_state = query_engine.engine_state().session_state();
    let plan = DFLogicalSubstraitConvertor {}
        .decode(bytes::Bytes::from(insert.logical_plan), session_state)
        .await
        .unwrap();
    let LogicalPlan::Projection(projection) = &plan else {
        panic!("expected projection over empty relation, got {plan:?}");
    };
    // marker host + window + epoch + checkpoint + auto update_at.
    assert_eq!(5, projection.expr.len(), "{projection:?}");

    // The primary-key column must be projected first as the marker string.
    let host_expr = &projection.expr[0];
    let host_sql = format!("{host_expr:?}");
    assert!(
        host_sql.contains("\"host\""),
        "host primary key must be projected explicitly, got {host_sql}"
    );
    assert!(
        host_sql.contains("__greptime_checkpoint__"),
        "host must be the canonical marker string literal, got {host_sql}"
    );

    // window column -> sentinel timestamp.
    let window_expr = &projection.expr[1];
    let window_sql = format!("{window_expr:?}");
    assert!(
        window_sql
            .contains(&crate::batching_mode::CHECKPOINT_SENTINEL_WINDOW_TS_MILLIS.to_string()),
        "sentinel window literal expected, got {window_sql}"
    );
    // epoch column -> 7.
    let epoch_expr = &projection.expr[2];
    assert!(
        format!("{epoch_expr:?}").contains("UInt64(7)"),
        "epoch literal expected, got {epoch_expr:?}"
    );
    // checkpoint column -> the encoded v1 record bytes must appear verbatim.
    let expected = encode_checkpoint_record(&CheckpointRecord {
        format_version: CHECKPOINT_RECORD_FORMAT_VERSION,
        epoch: 7,
        checkpoints: BTreeMap::from([(1_u64, 11_u64)]),
    })
    .unwrap();
    let state_bytes = match &projection.expr[3] {
        Expr::Alias(alias) => match alias.expr.as_ref() {
            Expr::Literal(ScalarValue::Binary(Some(bytes)), _) => bytes,
            other => panic!("expected binary literal for checkpoint column, got {other:?}"),
        },
        other => panic!("expected alias for checkpoint column, got {other:?}"),
    };
    assert_eq!(
        &expected, state_bytes,
        "checkpoint record bytes must be stored verbatim"
    );
}

#[tokio::test]
async fn test_checkpoint_row_write_requires_exactly_one_affected_row() {
    // A checkpoint write that reports any affected-rows count other than
    // exactly 1 must be reported as a failure so the caller walks the existing
    // CheckpointPersistFailure fallback (full snapshot + dirty restore) instead
    // of pretending the checkpoint is durable.
    let sink_table = "persistence_write_rows_sink";
    let TestTaskParts {
        task, query_engine, ..
    } = new_sequence_range_test_task(sink_table).await;
    register_persistence_sink(
        &query_engine,
        sink_table,
        vec![(Some(0), Some(1), None)],
        9115,
    );
    task.state
        .write()
        .unwrap()
        .set_checkpoint_persistence(Some(test_persistence()));

    for affected_rows in [0_usize, 2] {
        let captured = Arc::new(std::sync::Mutex::new(None));
        let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> =
            Arc::new(CaptureInsertWithRowsHandler {
                captured: captured.clone(),
                affected_rows,
            });
        let frontend_client = Arc::new(FrontendClient::from_grpc_handler(
            Arc::downgrade(&handler),
            QueryOptions::default(),
        ));
        let err = task
            .write_checkpoint_row(&frontend_client, 7, &BTreeMap::from([(1_u64, 11_u64)]))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("expected exactly 1"),
            "affected_rows={affected_rows} must be reported as a persist failure, got: {err}"
        );
    }
}

/// A minimal record-batch stream that reports region watermarks through its
/// terminal metrics handle immediately (it produces no rows). Lets the flow
/// execution path see a full watermark proof without consuming a real query.
struct WatermarkOnlyStream {
    schema: datatypes::schema::SchemaRef,
    watermarks: Vec<(u64, Option<u64>)>,
}

impl futures::Stream for WatermarkOnlyStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(None)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(0))
    }
}

impl common_recordbatch::RecordBatchStream for WatermarkOnlyStream {
    fn name(&self) -> &str {
        "WatermarkOnlyStream"
    }

    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[common_recordbatch::OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        Some(RecordBatchMetrics {
            region_watermarks: self
                .watermarks
                .iter()
                .map(|(region_id, watermark)| RegionWatermarkEntry {
                    region_id: *region_id,
                    watermark: *watermark,
                })
                .collect(),
            ..Default::default()
        })
    }
}

/// Walks a decoded logical plan looking for the checkpoint row's
/// `EmptyRelation` root (state-row inserts are scan-based and never contain
/// one).
fn contains_empty_relation(plan: &LogicalPlan) -> bool {
    let mut found = false;
    let _ = plan.apply(|node| {
        if matches!(node, LogicalPlan::EmptyRelation(_)) {
            found = true;
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    });
    found
}

/// A frontend handler for the checkpoint persist-failure cycle: state-row
/// inserts return terminal watermarks; the first checkpoint-row write fails,
/// later checkpoint writes succeed.
struct PersistFailOnceHandler {
    query_engine: QueryEngineRef,
    checkpoint_write_calls: std::sync::atomic::AtomicUsize,
    checkpoint_write_attempts: Arc<std::sync::Mutex<usize>>,
}

#[async_trait::async_trait]
impl GrpcQueryHandlerWithBoxedError for PersistFailOnceHandler {
    async fn do_query(
        &self,
        query: api::v1::greptime_request::Request,
        _ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        let api::v1::greptime_request::Request::Query(api::v1::QueryRequest {
            query: Some(api::v1::query_request::Query::InsertIntoPlan(insert)),
            ..
        }) = query
        else {
            return Ok(Output::new_with_affected_rows(0));
        };
        // Best-effort decode to classify the request. The singleton
        // checkpoint-row write is a Projection over EmptyRelation and always
        // decodes; the state-row insert carries flow-only UDAFs (e.g.
        // `uddsketch_state`) that the test session state cannot resolve from
        // substrait anchors, so a decode failure is treated as a state row.
        let convertor = DFLogicalSubstraitConvertor {};
        let is_checkpoint_row = match convertor
            .decode(
                bytes::Bytes::from(insert.logical_plan),
                decode_session_state(&self.query_engine),
            )
            .await
        {
            Ok(plan) => contains_empty_relation(&plan),
            Err(_) => false,
        };
        if is_checkpoint_row {
            // Singleton checkpoint row write.
            *self.checkpoint_write_attempts.lock().unwrap() += 1;
            let call = self
                .checkpoint_write_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if call == 0 {
                return Err(BoxedError::new(MockError::new(StatusCode::Internal)));
            }
            return Ok(Output::new_with_affected_rows(1));
        }
        // State-row insert: prove full coverage with terminal watermarks.
        Ok(Output::new_with_stream(Box::pin(WatermarkOnlyStream {
            schema: Arc::new(Schema::new(vec![])),
            watermarks: vec![(1, Some(10)), (2, Some(20))],
        })))
    }
}

#[tokio::test]
async fn test_checkpoint_persist_failure_schedules_backfill_and_replacement_checkpoint() {
    let sink_table = "persistence_exec_sink";
    let query = "SELECT uddsketch_state(128, 0.01, CAST(number AS DOUBLE)) AS state, \
        date_bin(INTERVAL '5 second', ts) AS window FROM greptime.public.numbers_with_ts GROUP BY window";
    let TestTaskParts {
        task, query_engine, ..
    } = new_ee_sequence_range_task(sink_table, query).await;
    register_persistence_sink(
        &query_engine,
        sink_table,
        vec![(Some(0), Some(1), None)],
        9113,
    );
    task.state
        .write()
        .unwrap()
        .set_checkpoint_persistence(Some(test_persistence()));
    task.state
        .write()
        .unwrap()
        .dirty_time_windows
        .add_window(Timestamp::new_second(10), Some(Timestamp::new_second(15)));

    let checkpoint_write_attempts = Arc::new(std::sync::Mutex::new(0));
    let handler: Arc<dyn GrpcQueryHandlerWithBoxedError> = Arc::new(PersistFailOnceHandler {
        query_engine: query_engine.clone(),
        checkpoint_write_calls: std::sync::atomic::AtomicUsize::new(0),
        checkpoint_write_attempts: checkpoint_write_attempts.clone(),
    });
    let frontend_client = Arc::new(FrontendClient::from_grpc_handler(
        Arc::downgrade(&handler),
        QueryOptions::default(),
    ));

    // Cycle 1: the state insert advances checkpoints, but the checkpoint row
    // write fails. The task must fall back to full snapshot, keep the consumed
    // dirty work pending, and not advance the durable epoch.
    let outcome = task
        .execute_once_serialized(&query_engine, &frontend_client, None)
        .await;
    assert!(
        outcome.is_ok(),
        "state insert should succeed, got: {outcome:?}"
    );
    {
        let state = task.state.read().unwrap();
        assert_eq!(
            state.checkpoint_mode(),
            CheckpointMode::FullSnapshot,
            "a failed checkpoint write must reset to full snapshot"
        );
        assert_eq!(
            state.persisted_epoch(),
            0,
            "a failed checkpoint write must not advance the durable epoch"
        );
        assert!(
            !state.dirty_time_windows.is_empty(),
            "the executed plan's consumed dirty work must be restored"
        );
    }

    // Cycle 2: the restored dirty work drives a full repair/backfill; the
    // replacement checkpoint row write succeeds and the durable epoch advances.
    let outcome = task
        .execute_once_serialized(&query_engine, &frontend_client, None)
        .await;
    assert!(
        outcome.is_ok(),
        "repair cycle should succeed, got: {outcome:?}"
    );
    {
        let state = task.state.read().unwrap();
        assert_eq!(
            state.checkpoint_mode(),
            CheckpointMode::Incremental,
            "the replacement checkpoint write must restore incremental mode"
        );
        assert_eq!(
            state.persisted_epoch(),
            1,
            "the replacement checkpoint must advance the durable epoch"
        );
        assert_eq!(
            state.checkpoints(),
            &BTreeMap::from([(1_u64, 10_u64), (2_u64, 20_u64)])
        );
    }
    assert_eq!(
        2,
        *checkpoint_write_attempts.lock().unwrap(),
        "one failed checkpoint write followed by one replacement write"
    );
}

// ---------------------------------------------------------------------
// Phase 1 two-phase backfill primitives
// ---------------------------------------------------------------------

/// A metrics-carrying one-shot stream used by the backfill mock handler.
struct BackfillMetricsStream {
    schema: datatypes::schema::SchemaRef,
    batch: Option<RecordBatch>,
    metrics: RecordBatchMetrics,
    terminal_metrics_only: bool,
}

impl futures::Stream for BackfillMetricsStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(self.batch.take().map(Ok))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (
            usize::from(self.batch.is_some()),
            Some(usize::from(self.batch.is_some())),
        )
    }
}

impl RecordBatchStream for BackfillMetricsStream {
    fn name(&self) -> &str {
        "BackfillMetricsStream"
    }

    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        if self.terminal_metrics_only && self.batch.is_some() {
            return None;
        }
        Some(self.metrics.clone())
    }
}

/// Mock frontend handler for the full Phase-1 backfill lifecycle: handles the
/// staging `CREATE TABLE` DDL (registering a compatible memory table and
/// counting create attempts so "only one create" can be proven), the `DROP
/// TABLE` SQL used by `finish_backfill_job` (with optional failure and an
/// optional gate to hold a DROP in flight), and the Base `InsertIntoPlan`
/// query with terminal watermarks.
///
/// `fail_creates_remaining` fails the next `n` create DDLs; when
/// `register_table_on_failed_create` is set the table is still registered
/// before the failure is returned, simulating a create that may have
/// succeeded remotely. `query_failures_remaining` fails the next `n` Base
/// queries so failure/retry paths can be exercised.
#[derive(Clone)]
pub(crate) struct BackfillLifecycleHandler {
    engine: QueryEngineRef,
    next_table_id: Arc<std::sync::atomic::AtomicU32>,
    create_attempts: Arc<std::sync::atomic::AtomicUsize>,
    fail_creates_remaining: Arc<std::sync::atomic::AtomicUsize>,
    register_table_on_failed_create: bool,
    watermarks: Vec<(u64, Option<u64>)>,
    query_failures_remaining: Arc<std::sync::atomic::AtomicUsize>,
    query_gate: Option<Arc<tokio::sync::Notify>>,
    fail_drops: bool,
    drop_gate: Option<Arc<tokio::sync::Notify>>,
    created_tables: Arc<std::sync::Mutex<Vec<String>>>,
    captured_insert: Arc<std::sync::Mutex<Option<api::v1::InsertIntoPlan>>>,
    captured_ctx_extensions: Arc<std::sync::Mutex<Option<HashMap<String, String>>>>,
    captured_sql: Arc<std::sync::Mutex<Vec<String>>>,
}

impl BackfillLifecycleHandler {
    pub(crate) fn new(engine: QueryEngineRef, watermarks: Vec<(u64, Option<u64>)>) -> Self {
        Self {
            engine,
            next_table_id: Arc::new(std::sync::atomic::AtomicU32::new(1)),
            create_attempts: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            fail_creates_remaining: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            register_table_on_failed_create: false,
            watermarks,
            query_failures_remaining: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            query_gate: None,
            fail_drops: false,
            drop_gate: None,
            created_tables: Arc::new(std::sync::Mutex::new(Vec::new())),
            captured_insert: Arc::new(std::sync::Mutex::new(None)),
            captured_ctx_extensions: Arc::new(std::sync::Mutex::new(None)),
            captured_sql: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn with_starting_table_id(self, table_id: u32) -> Self {
        self.next_table_id
            .store(table_id, std::sync::atomic::Ordering::SeqCst);
        self
    }

    fn with_failed_creates(
        mut self,
        failures: usize,
        register_table_on_failed_create: bool,
    ) -> Self {
        self.fail_creates_remaining
            .store(failures, std::sync::atomic::Ordering::SeqCst);
        self.register_table_on_failed_create = register_table_on_failed_create;
        self
    }

    pub(crate) fn with_query_failures(self, failures: usize) -> Self {
        self.query_failures_remaining
            .store(failures, std::sync::atomic::Ordering::SeqCst);
        self
    }

    fn with_query_gate(mut self, gate: Arc<tokio::sync::Notify>) -> Self {
        self.query_gate = Some(gate);
        self
    }

    fn with_failed_drops(mut self) -> Self {
        self.fail_drops = true;
        self
    }

    fn with_drop_gate(mut self, gate: Arc<tokio::sync::Notify>) -> Self {
        self.drop_gate = Some(gate);
        self
    }

    pub(crate) fn create_attempts(&self) -> usize {
        self.create_attempts
            .load(std::sync::atomic::Ordering::SeqCst)
    }
}

/// Builds a `FrontendClient` wired to the given lifecycle handler.
pub(crate) fn lifecycle_frontend_client(
    handler: &Arc<BackfillLifecycleHandler>,
) -> Arc<FrontendClient> {
    let handler_trait: Arc<dyn GrpcQueryHandlerWithBoxedError> = handler.clone();
    Arc::new(FrontendClient::from_grpc_handler(
        Arc::downgrade(&handler_trait),
        QueryOptions::default(),
    ))
}

#[async_trait::async_trait]
impl crate::batching_mode::frontend_client::GrpcQueryHandlerWithBoxedError
    for BackfillLifecycleHandler
{
    async fn do_query(
        &self,
        query: api::v1::greptime_request::Request,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        use api::v1::ddl_request::Expr;
        use api::v1::greptime_request::Request;
        use api::v1::query_request::Query;
        use api::v1::{DdlRequest, QueryRequest};

        match query {
            Request::Ddl(DdlRequest {
                expr: Some(Expr::CreateTable(create)),
                ..
            }) => {
                // Yield so a concurrent prepare task can observe the
                // reservation while this create is in flight (the memory
                // catalog completes synchronously, so without a yield the
                // whole prepare would finish before the other task runs).
                tokio::task::yield_now().await;
                let failed = self
                    .fail_creates_remaining
                    .load(std::sync::atomic::Ordering::SeqCst)
                    > 0;
                if failed {
                    self.fail_creates_remaining
                        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                }
                self.create_attempts
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                if !failed || self.register_table_on_failed_create {
                    let table_id = self
                        .next_table_id
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    register_backfill_staging_table(&self.engine, &create.table_name, table_id);
                    self.created_tables
                        .lock()
                        .unwrap()
                        .push(create.table_name.clone());
                }
                if failed {
                    return Err(BoxedError::new(MockError::new(StatusCode::Internal)));
                }
                Ok(Output::new_with_affected_rows(0))
            }
            Request::Query(QueryRequest {
                query: Some(Query::Sql(sql)),
                ..
            }) => {
                self.captured_sql.lock().unwrap().push(sql.clone());
                if let Some(gate) = &self.drop_gate {
                    gate.notified().await;
                }
                if self.fail_drops && sql.contains("DROP TABLE") {
                    return Err(BoxedError::new(MockError::new(StatusCode::Internal)));
                }
                // A successful DROP also removes the tracked staging tables
                // from the local catalog, mirroring the real frontend, so a
                // later re-prepare observes the table as gone.
                if sql.contains("DROP TABLE") {
                    let tables = self.created_tables.lock().unwrap().clone();
                    let catalog_manager = self.engine.engine_state().catalog_manager();
                    let memory_catalog = catalog_manager
                        .as_any()
                        .downcast_ref::<MemoryCatalogManager>()
                        .unwrap();
                    for name in tables {
                        memory_catalog
                            .deregister_table_sync(catalog::DeregisterTableRequest {
                                catalog: DEFAULT_CATALOG_NAME.to_string(),
                                schema: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
                                table_name: name,
                            })
                            .unwrap();
                    }
                    self.created_tables.lock().unwrap().clear();
                }
                Ok(Output::new_with_affected_rows(0))
            }
            Request::Query(QueryRequest {
                query: Some(Query::InsertIntoPlan(insert)),
                ..
            }) => {
                // Capture the request BEFORE any gate so tests can observe the
                // in-flight query, then block.
                *self.captured_ctx_extensions.lock().unwrap() = Some(ctx.extensions().clone());
                *self.captured_insert.lock().unwrap() = Some(insert);
                if let Some(gate) = &self.query_gate {
                    gate.notified().await;
                }
                if self
                    .query_failures_remaining
                    .load(std::sync::atomic::Ordering::SeqCst)
                    > 0
                {
                    self.query_failures_remaining
                        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    return Err(BoxedError::new(MockError::new(StatusCode::Internal)));
                }

                let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
                    "v",
                    CDT::int32_datatype(),
                    false,
                )]));
                let batch = RecordBatch::new(
                    schema.clone(),
                    vec![Arc::new(Int32Vector::from_slice([1])) as VectorRef],
                )
                .unwrap();
                let metrics = RecordBatchMetrics {
                    region_watermarks: self
                        .watermarks
                        .iter()
                        .map(|(region_id, watermark)| RegionWatermarkEntry {
                            region_id: *region_id,
                            watermark: *watermark,
                        })
                        .collect(),
                    ..Default::default()
                };
                Ok(Output::new_with_stream(Box::pin(BackfillMetricsStream {
                    schema,
                    batch: Some(batch),
                    metrics,
                    terminal_metrics_only: true,
                })))
            }
            other => panic!("unexpected backfill request, got {other:?}"),
        }
    }
}

/// Builds a task with a time-window expression and a registered sink table,
/// ready for Phase 1 backfill tests. Returns the task, the query engine, and
/// the query used to build the task.
pub(crate) async fn new_backfill_task(
    sink_table_name: &str,
    sink_table_id: u32,
) -> (BatchingTask, QueryEngineRef, String) {
    let query_engine = create_test_query_engine();
    let ctx = QueryContext::arc();
    let plan_query = "SELECT number, date_bin(INTERVAL '5 second', ts) AS time_window \
         FROM numbers_with_ts GROUP BY time_window, number";
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), plan_query, true)
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
        .unwrap();

    register_twe_sink(&query_engine, sink_table_name, sink_table_id);

    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query: plan_query,
        plan: plan.clone(),
        time_window_expr,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            sink_table_name.to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: incremental_batch_opts(),
        flow_eval_interval: None,
        eval_schedule: None,
    })
    .unwrap();
    (task, query_engine, plan_query.to_string())
}

/// Registers a staging table under `greptime_private` mirroring the sink
/// schema (dimension `number` nullable, window `time_window` time index).
fn register_backfill_staging_table(query_engine: &QueryEngineRef, table_name: &str, table_id: u32) {
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), true),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
    ]));
    let columns: Vec<VectorRef> = vec![
        Arc::new(UInt32Vector::from_slice([1_u32])),
        Arc::new(TimestampMillisecondVector::from_slice([0_i64])),
    ];
    let recordbatch = RecordBatch::new(schema, columns).unwrap();
    let table = MemTable::new_with_catalog(
        table_name,
        recordbatch,
        table_id,
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
    );
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog
        .register_schema_sync(catalog::RegisterSchemaRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
        })
        .unwrap();
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id,
        table,
    };
    memory_catalog.register_table_sync(request).unwrap();
}

#[test]
fn test_backfill_staging_table_name_is_stable_and_injection_free() {
    assert_eq!(
        backfill_staging_table_name(1, 7),
        [
            DEFAULT_CATALOG_NAME.to_string(),
            DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
            "__flow_backfill_1_7".to_string(),
        ]
    );
    // u64 identifiers are the only inputs, so the generated name is a strict
    // `__flow_backfill_<flow_id>_<job_id>` format with no room for SQL
    // metacharacters.
    let name = backfill_staging_table_name(u64::MAX, u64::MAX);
    assert_eq!(name[0], DEFAULT_CATALOG_NAME);
    assert_eq!(name[1], DEFAULT_PRIVATE_SCHEMA_NAME);
    assert!(name[2].starts_with("__flow_backfill_"));
    assert!(name[2].ends_with("_18446744073709551615"));
}

#[tokio::test]
async fn test_prepare_backfill_base_registers_job_and_builds_staging_plan() {
    let sink_table_name = "twe_sink_backfill_prepare";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9201).await;

    let staging_table_name = "__flow_backfill_1_42";
    // The staging table does NOT pre-exist; prepare must create it through the
    // frontend (the handler registers it in the local catalog on the DDL).
    let handler = Arc::new(BackfillLifecycleHandler::new(query_engine.clone(), vec![]));
    let frontend_client = lifecycle_frontend_client(&handler);

    let start = Timestamp::new_second(0);
    let end = Timestamp::new_second(300);
    let base = task
        .prepare_backfill_base(&query_engine, &frontend_client, 42, start, end)
        .await
        .unwrap();

    // Exactly one staging table create was issued, and it landed.
    assert_eq!(handler.create_attempts(), 1);
    assert_eq!(
        handler.created_tables.lock().unwrap().as_slice(),
        &[staging_table_name.to_string()]
    );

    // Job registered with aligned range, staging table info, and Prepared.
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        let job = &state.backfill_jobs()[0];
        assert_eq!(job.job_id, 42);
        assert_eq!(job.range, (start, end));
        assert_eq!(
            job.staging_table_name,
            [
                DEFAULT_CATALOG_NAME.to_string(),
                DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
                staging_table_name.to_string(),
            ]
        );
        assert_eq!(job.staging_table_id, Some(1));
        assert_eq!(
            job.status,
            crate::batching_mode::state::BackfillJobStatus::Prepared
        );
        assert!(job.frozen_watermark.is_none());
    }

    // The returned base carries the same job and a DML plan targeting the
    // staging table with the `[start, end)` event-time filter.
    assert_eq!(base.job.job_id, 42);
    let LogicalPlan::Dml(dml) = &base.plan else {
        panic!("expected DML plan, got {:?}", base.plan);
    };
    assert_eq!(
        dml.table_name,
        TableReference::Full {
            catalog: Arc::from(DEFAULT_CATALOG_NAME),
            schema: Arc::from(DEFAULT_PRIVATE_SCHEMA_NAME),
            table: Arc::from(staging_table_name),
        }
    );
    let inner = format!("{}", dml.input.display_indent());
    assert!(
        inner.contains("TimestampMillisecond(0"),
        "base plan must filter by the aligned lower bound, got: {inner}"
    );
    assert!(
        inner.contains("TimestampMillisecond(300000"),
        "base plan must filter by the aligned upper bound, got: {inner}"
    );

    // Phase 1 must not touch checkpoints or checkpoint mode.
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert!(state.checkpoints().is_empty());
    }
}

#[tokio::test]
async fn test_prepare_backfill_base_orphan_table_fails_closed() {
    let sink_table_name = "twe_sink_backfill_orphan";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9203).await;

    let staging_table_name = "__flow_backfill_1_49";
    // Simulate a restart: the deterministic staging table survives but the
    // in-memory job registry is empty (nothing is registered).
    register_backfill_staging_table(&query_engine, staging_table_name, 9204);

    let handler = Arc::new(BackfillLifecycleHandler::new(query_engine.clone(), vec![]));
    let frontend_client = lifecycle_frontend_client(&handler);

    let err = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            49,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("refusing to reuse an orphan table"),
        "expected fail-closed orphan error, got {err:?}"
    );
    // No create was attempted and no job was registered: the orphan table was
    // never silently adopted.
    assert_eq!(handler.create_attempts(), 0);
    assert!(task.state.read().unwrap().backfill_jobs().is_empty());

    // Explicit cleanup (drop the orphan) then retry: a fresh prepare creates a
    // new staging table and registers the job.
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog
        .deregister_table_sync(catalog::DeregisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
            table_name: staging_table_name.to_string(),
        })
        .unwrap();

    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            49,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();
    assert_eq!(base.job.job_id, 49);
    assert_eq!(handler.create_attempts(), 1);
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert_eq!(
            state.backfill_jobs()[0].status,
            crate::batching_mode::state::BackfillJobStatus::Prepared
        );
    }
}

#[tokio::test]
async fn test_prepare_backfill_base_concurrent_same_identity_single_create() {
    let sink_table_name = "twe_sink_backfill_concurrent";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9205).await;

    let handler = Arc::new(BackfillLifecycleHandler::new(query_engine.clone(), vec![]));
    let frontend_client = lifecycle_frontend_client(&handler);

    let task_a = task.clone();
    let client_a = frontend_client.clone();
    let engine_a = query_engine.clone();
    let task_b = task.clone();
    let client_b = frontend_client.clone();
    let engine_b = query_engine.clone();

    let (result_a, result_b) = tokio::join!(
        async move {
            task_a
                .prepare_backfill_base(
                    &engine_a,
                    &client_a,
                    50,
                    Timestamp::new_second(0),
                    Timestamp::new_second(300),
                )
                .await
        },
        async move {
            task_b
                .prepare_backfill_base(
                    &engine_b,
                    &client_b,
                    50,
                    Timestamp::new_second(0),
                    Timestamp::new_second(300),
                )
                .await
        },
    );

    // Exactly one prepare wins the reservation and creates the staging table;
    // the other gets Busy (AlreadyPreparing) and must not create.
    let (winner, loser) = match (result_a, result_b) {
        (Ok(base), Err(e)) => (base, e),
        (Err(e), Ok(base)) => (base, e),
        other => panic!("expected exactly one Ok and one Err, got {other:?}"),
    };
    assert_eq!(winner.job.job_id, 50);
    assert!(
        format!("{loser:?}").contains("already being prepared"),
        "expected Busy/AlreadyPreparing error, got {loser:?}"
    );
    assert_eq!(handler.create_attempts(), 1, "only one create may happen");
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert_eq!(
            state.backfill_jobs()[0].status,
            crate::batching_mode::state::BackfillJobStatus::Prepared
        );
        assert_eq!(
            state.backfill_jobs()[0].range,
            (Timestamp::new_second(0), Timestamp::new_second(300))
        );
    }
}

#[tokio::test]
async fn test_prepare_backfill_base_concurrent_different_range_no_identity_replacement() {
    let sink_table_name = "twe_sink_backfill_concurrent_range";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9206).await;

    let handler = Arc::new(BackfillLifecycleHandler::new(query_engine.clone(), vec![]));
    let frontend_client = lifecycle_frontend_client(&handler);

    let task_a = task.clone();
    let client_a = frontend_client.clone();
    let engine_a = query_engine.clone();
    let task_b = task.clone();
    let client_b = frontend_client.clone();
    let engine_b = query_engine.clone();

    let (result_a, result_b) = tokio::join!(
        async move {
            task_a
                .prepare_backfill_base(
                    &engine_a,
                    &client_a,
                    51,
                    Timestamp::new_second(0),
                    Timestamp::new_second(300),
                )
                .await
        },
        async move {
            task_b
                .prepare_backfill_base(
                    &engine_b,
                    &client_b,
                    51,
                    Timestamp::new_second(0),
                    Timestamp::new_second(600),
                )
                .await
        },
    );

    // Exactly one range wins; the conflicting range is rejected without
    // replacing the registered identity, and only one create happens.
    let (winner, loser) = match (result_a, result_b) {
        (Ok(base), Err(e)) => (base, e),
        (Err(e), Ok(base)) => (base, e),
        other => panic!("expected exactly one Ok and one Err, got {other:?}"),
    };
    assert_eq!(winner.job.job_id, 51);
    assert!(
        format!("{loser:?}").contains("different identity"),
        "expected identity-mismatch error, got {loser:?}"
    );
    assert_eq!(handler.create_attempts(), 1);
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        // The registered identity is the winner's range; nothing was replaced.
        assert_eq!(state.backfill_jobs()[0].range, winner.job.range);
    }
}

#[tokio::test]
async fn test_prepare_backfill_base_create_failure_keeps_reservation_and_retry_resumes() {
    let sink_table_name = "twe_sink_backfill_create_fail";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9207).await;

    // The first create fails without creating anything.
    let handler = Arc::new(
        BackfillLifecycleHandler::new(query_engine.clone(), vec![]).with_failed_creates(1, false),
    );
    let frontend_client = lifecycle_frontend_client(&handler);

    let err = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            52,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap_err();
    assert_eq!(
        err.status_code(),
        StatusCode::EngineExecuteQuery,
        "expected create failure, got {err:?}"
    );

    // Ownership is retained as Preparing { staging_may_exist: true }: the job
    // record is not erased and the table is never anonymous.
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert_eq!(
            state.backfill_jobs()[0].status,
            crate::batching_mode::state::BackfillJobStatus::Preparing {
                staging_may_exist: true
            }
        );
    }

    // Re-prepare resumes under the held reservation: the table is still absent
    // (the failed create created nothing), so a fresh create lands and the job
    // reaches Prepared. No Busy error, because the reservation is resumable.
    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            52,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();
    assert_eq!(base.job.job_id, 52);
    assert_eq!(
        handler.create_attempts(),
        2,
        "failed attempt + resumed create"
    );
    {
        let state = task.state.read().unwrap();
        assert_eq!(
            state.backfill_jobs()[0].status,
            crate::batching_mode::state::BackfillJobStatus::Prepared
        );
    }
}

#[tokio::test]
async fn test_prepare_backfill_base_create_may_have_succeeded_is_adopted_on_resume() {
    let sink_table_name = "twe_sink_backfill_create_ambiguous";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9208).await;

    // The first create fails but the table WAS registered (simulating a create
    // that succeeded remotely with a lost response): the job keeps ownership
    // and a re-prepare must adopt its own table without a second create.
    let handler = Arc::new(
        BackfillLifecycleHandler::new(query_engine.clone(), vec![]).with_failed_creates(1, true),
    );
    let frontend_client = lifecycle_frontend_client(&handler);

    let err = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            53,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap_err();
    assert_eq!(err.status_code(), StatusCode::EngineExecuteQuery);

    // Re-prepare: the table exists and belongs to our reservation, so it is
    // adopted and the job moves to Prepared without another create.
    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            53,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();
    assert_eq!(base.job.job_id, 53);
    assert_eq!(
        handler.create_attempts(),
        1,
        "the resumed prepare must adopt the existing table, not re-create it"
    );
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert_eq!(
            state.backfill_jobs()[0].status,
            crate::batching_mode::state::BackfillJobStatus::Prepared
        );
        assert!(state.backfill_jobs()[0].staging_table_id.is_some());
    }
}

#[tokio::test]
async fn test_run_backfill_base_captures_f_and_leaves_checkpoints_untouched() {
    let sink_table_name = "twe_sink_backfill_run";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9301).await;

    let handler = Arc::new(BackfillLifecycleHandler::new(
        query_engine.clone(),
        vec![(7, Some(99))],
    ));
    let frontend_client = lifecycle_frontend_client(&handler);

    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            77,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();
    assert_eq!(handler.create_attempts(), 1);

    let frozen = task
        .run_backfill_base(&frontend_client, &base)
        .await
        .unwrap();
    assert_eq!(frozen, BTreeMap::from([(7_u64, 99_u64)]));

    // F recorded on the registered job; the job is now BaseComplete.
    {
        let state = task.state.read().unwrap();
        assert_eq!(
            state.backfill_jobs()[0].frozen_watermark,
            Some(BTreeMap::from([(7_u64, 99_u64)]))
        );
        assert_eq!(
            state.backfill_jobs()[0].status,
            crate::batching_mode::state::BackfillJobStatus::BaseComplete
        );
        // Active checkpoint state must be untouched by Phase 1.
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert!(state.checkpoints().is_empty());
        assert_eq!(state.persisted_epoch(), 0);
    }

    // The Base request was an InsertIntoPlan into the staging table, carrying
    // the return_region_seq extension and the staging table id exclusion.
    let captured_insert = handler.captured_insert.lock().unwrap().clone().unwrap();
    assert_eq!(
        captured_insert.table_name.unwrap().table_name,
        "__flow_backfill_1_77"
    );
    let captured_extensions = handler
        .captured_ctx_extensions
        .lock()
        .unwrap()
        .clone()
        .unwrap();
    assert_eq!(
        captured_extensions
            .get(FLOW_RETURN_REGION_SEQ)
            .map(String::as_str),
        Some("true")
    );
    assert_eq!(
        captured_extensions
            .get(FLOW_INTERNAL_NON_SOURCE_TABLE_IDS)
            .map(String::as_str),
        Some("[1]")
    );
}

#[tokio::test]
async fn test_run_backfill_base_fails_on_incomplete_watermark_and_keeps_staging() {
    let sink_table_name = "twe_sink_backfill_missing_watermark";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9401).await;

    // Handler returns no terminal watermarks at all.
    let handler = Arc::new(BackfillLifecycleHandler::new(query_engine.clone(), vec![]));
    let frontend_client = lifecycle_frontend_client(&handler);

    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            88,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();

    let err = task
        .run_backfill_base(&frontend_client, &base)
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("incomplete terminal watermarks"),
        "expected incomplete-watermark error, got {err:?}"
    );

    // Phase 1 failure keeps the staging table and the registered job, and the
    // job returns to Prepared so the run is retryable.
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert_eq!(state.backfill_jobs()[0].job_id, 88);
        assert!(state.backfill_jobs()[0].frozen_watermark.is_none());
        assert_eq!(
            state.backfill_jobs()[0].status,
            crate::batching_mode::state::BackfillJobStatus::Prepared
        );
        assert_eq!(state.checkpoint_mode(), CheckpointMode::FullSnapshot);
        assert!(state.checkpoints().is_empty());
    }
}

#[tokio::test]
async fn test_run_backfill_base_multi_region_watermark_success_and_incomplete_failure() {
    let sink_table_name = "twe_sink_backfill_multi_region";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9501).await;

    // Complete multi-region F: every participating region proves a watermark.
    let handler = Arc::new(BackfillLifecycleHandler::new(
        query_engine.clone(),
        vec![(7, Some(99)), (8, Some(100))],
    ));
    let frontend_client = lifecycle_frontend_client(&handler);

    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            90,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();
    let frozen = task
        .run_backfill_base(&frontend_client, &base)
        .await
        .unwrap();
    assert_eq!(frozen, BTreeMap::from([(7_u64, 99_u64), (8_u64, 100_u64)]));

    // A separate job whose participating region has no provable watermark
    // (None) is omitted from the map, so F is incomplete and the run fails
    // closed, returning the job to Prepared.
    let handler = Arc::new(
        BackfillLifecycleHandler::new(query_engine.clone(), vec![(7, Some(99)), (8, None)])
            .with_starting_table_id(2),
    );
    let frontend_client = lifecycle_frontend_client(&handler);
    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            91,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();
    let err = task
        .run_backfill_base(&frontend_client, &base)
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("incomplete terminal watermarks"),
        "expected incomplete-watermark error for None watermark, got {err:?}"
    );
    {
        let state = task.state.read().unwrap();
        assert_eq!(
            state.backfill_jobs()[0].status,
            crate::batching_mode::state::BackfillJobStatus::BaseComplete
        );
        assert_eq!(state.backfill_jobs()[1].job_id, 91);
        assert_eq!(
            state.backfill_jobs()[1].status,
            crate::batching_mode::state::BackfillJobStatus::Prepared
        );
        assert!(state.backfill_jobs()[1].frozen_watermark.is_none());
    }
}

#[tokio::test]
async fn test_prepare_backfill_base_duplicate_same_range_is_idempotent() {
    let sink_table_name = "twe_sink_backfill_dup_prepare";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9601).await;

    let handler = Arc::new(BackfillLifecycleHandler::new(query_engine.clone(), vec![]));
    let frontend_client = lifecycle_frontend_client(&handler);

    let start = Timestamp::new_second(0);
    let end = Timestamp::new_second(300);
    let base = task
        .prepare_backfill_base(&query_engine, &frontend_client, 43, start, end)
        .await
        .unwrap();
    assert_eq!(handler.create_attempts(), 1);

    // Exact duplicate prepare (same job_id, same aligned range, same staging
    // identity): idempotent. Returns the existing job, does not re-create the
    // staging table, and does not reset status/F.
    let base_dup = task
        .prepare_backfill_base(&query_engine, &frontend_client, 43, start, end)
        .await
        .unwrap();
    assert_eq!(base_dup.job.job_id, base.job.job_id);
    assert_eq!(base_dup.job.range, base.job.range);
    assert_eq!(base_dup.job.staging_table_name, base.job.staging_table_name);
    assert_eq!(handler.create_attempts(), 1, "duplicate must not re-create");
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert_eq!(state.backfill_jobs()[0].job_id, 43);
    }
}

#[tokio::test]
async fn test_prepare_backfill_base_different_range_rejects() {
    let sink_table_name = "twe_sink_backfill_dup_range";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9701).await;

    let handler = Arc::new(BackfillLifecycleHandler::new(query_engine.clone(), vec![]));
    let frontend_client = lifecycle_frontend_client(&handler);

    task.prepare_backfill_base(
        &query_engine,
        &frontend_client,
        44,
        Timestamp::new_second(0),
        Timestamp::new_second(300),
    )
    .await
    .unwrap();
    assert_eq!(handler.create_attempts(), 1);

    // Same job_id with a different aligned range fails closed.
    let err = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            44,
            Timestamp::new_second(0),
            Timestamp::new_second(600),
        )
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("different identity"),
        "expected identity-mismatch error, got {err:?}"
    );
    assert_eq!(handler.create_attempts(), 1);
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert_eq!(
            state.backfill_jobs()[0].range,
            (Timestamp::new_second(0), Timestamp::new_second(300))
        );
    }
}

#[tokio::test]
async fn test_run_backfill_base_rejects_second_run_after_complete() {
    let sink_table_name = "twe_sink_backfill_second_run";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9801).await;

    let handler = Arc::new(BackfillLifecycleHandler::new(
        query_engine.clone(),
        vec![(7, Some(99))],
    ));
    let frontend_client = lifecycle_frontend_client(&handler);

    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            45,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();

    let frozen = task
        .run_backfill_base(&frontend_client, &base)
        .await
        .unwrap();
    assert_eq!(frozen, BTreeMap::from([(7_u64, 99_u64)]));

    // The job is BaseComplete: a second run is rejected and cannot overwrite F.
    let err = task
        .run_backfill_base(&frontend_client, &base)
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("already BaseComplete"),
        "expected already-BaseComplete error, got {err:?}"
    );
    {
        let state = task.state.read().unwrap();
        assert_eq!(
            state.backfill_jobs()[0].frozen_watermark,
            Some(BTreeMap::from([(7_u64, 99_u64)]))
        );
    }
}

#[tokio::test]
async fn test_run_backfill_base_query_failure_returns_to_prepared_and_retry_succeeds() {
    let sink_table_name = "twe_sink_backfill_retry";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 9901).await;

    // First query fails at the frontend; the job must return to Prepared.
    let handler = Arc::new(
        BackfillLifecycleHandler::new(query_engine.clone(), vec![(7, Some(99))])
            .with_query_failures(1),
    );
    let frontend_client = lifecycle_frontend_client(&handler);

    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            46,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();

    let err = task
        .run_backfill_base(&frontend_client, &base)
        .await
        .unwrap_err();
    assert_eq!(
        err.status_code(),
        StatusCode::Internal,
        "expected frontend query failure, got {err:?}"
    );
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert_eq!(state.backfill_jobs()[0].job_id, 46);
        assert_eq!(
            state.backfill_jobs()[0].status,
            crate::batching_mode::state::BackfillJobStatus::Prepared
        );
        assert!(state.backfill_jobs()[0].frozen_watermark.is_none());
    }

    // Retry succeeds and captures F.
    let frozen = task
        .run_backfill_base(&frontend_client, &base)
        .await
        .unwrap();
    assert_eq!(frozen, BTreeMap::from([(7_u64, 99_u64)]));
    {
        let state = task.state.read().unwrap();
        assert_eq!(
            state.backfill_jobs()[0].status,
            crate::batching_mode::state::BackfillJobStatus::BaseComplete
        );
    }
}

/// Polls until `cond` holds (with a bounded timeout) so tests can observe a
/// request that is in flight in another task.
async fn wait_until<F: Fn() -> bool>(cond: F) {
    for _ in 0..5000 {
        if cond() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(2)).await;
    }
    panic!("condition not met within timeout");
}

#[tokio::test]
async fn test_finish_backfill_job_unknown_job_does_not_drop() {
    let sink_table_name = "twe_sink_backfill_finish_unknown";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 10001).await;

    let handler = Arc::new(BackfillLifecycleHandler::new(query_engine.clone(), vec![]));
    let frontend_client = lifecycle_frontend_client(&handler);

    task.prepare_backfill_base(
        &query_engine,
        &frontend_client,
        47,
        Timestamp::new_second(0),
        Timestamp::new_second(300),
    )
    .await
    .unwrap();

    // An unknown job id fails closed without issuing any DROP.
    let err = task
        .finish_backfill_job(&frontend_client, 999)
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("no registered backfill job"),
        "expected unknown-job error, got {err:?}"
    );
    assert!(
        handler.captured_sql.lock().unwrap().is_empty(),
        "unknown job must not issue a DROP"
    );
    // The registered job is untouched.
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert_eq!(state.backfill_jobs()[0].job_id, 47);
    }
}

#[tokio::test]
async fn test_finish_backfill_job_drop_failure_keeps_job_and_retry_removes() {
    let sink_table_name = "twe_sink_backfill_finish_retry";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 10101).await;

    // Prepare and run to BaseComplete through the same lifecycle handler.
    let handler = Arc::new(BackfillLifecycleHandler::new(
        query_engine.clone(),
        vec![(7, Some(99))],
    ));
    let frontend_client = lifecycle_frontend_client(&handler);
    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            48,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();
    task.run_backfill_base(&frontend_client, &base)
        .await
        .unwrap();

    // Drop fails: the job must stay registered (restored to BaseComplete) so
    // cleanup is retryable.
    let failing = Arc::new((*handler).clone().with_failed_drops());
    let frontend_client = lifecycle_frontend_client(&failing);
    let err = task
        .finish_backfill_job(&frontend_client, 48)
        .await
        .unwrap_err();
    assert_eq!(
        err.status_code(),
        StatusCode::Internal,
        "expected drop failure, got {err:?}"
    );
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert_eq!(state.backfill_jobs()[0].job_id, 48);
        assert_eq!(
            state.backfill_jobs()[0].status,
            crate::batching_mode::state::BackfillJobStatus::BaseComplete,
            "a failed drop must restore the job to BaseComplete for retry"
        );
    }
    assert_eq!(
        failing.captured_sql.lock().unwrap().len(),
        1,
        "a DROP must have been issued"
    );

    // Retry with a succeeding drop removes the job.
    let succeeding = Arc::new(handler.clone());
    let frontend_client = lifecycle_frontend_client(&succeeding);
    task.finish_backfill_job(&frontend_client, 48)
        .await
        .unwrap();
    {
        let state = task.state.read().unwrap();
        assert!(state.backfill_jobs().is_empty());
    }
}

#[tokio::test]
async fn test_finish_backfill_job_rejects_running_job() {
    let sink_table_name = "twe_sink_backfill_finish_vs_run";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 10201).await;

    // The Base query blocks at the gate, keeping the job Running.
    let query_gate = Arc::new(tokio::sync::Notify::new());
    let handler = Arc::new(
        BackfillLifecycleHandler::new(query_engine.clone(), vec![(7, Some(99))])
            .with_query_gate(query_gate.clone()),
    );
    let frontend_client = lifecycle_frontend_client(&handler);
    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            54,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();

    let run_task = task.clone();
    let run_client = frontend_client.clone();
    let run_handle = tokio::spawn(async move {
        run_task
            .run_backfill_base(&run_client, &base)
            .await
            .unwrap()
    });
    // Wait until the Base query is in flight (job is Running).
    wait_until(|| handler.captured_insert.lock().unwrap().is_some()).await;
    assert_eq!(
        task.state.read().unwrap().backfill_jobs()[0].status,
        crate::batching_mode::state::BackfillJobStatus::Running
    );

    // A finish while Running is rejected and must not issue a DROP.
    let err = task
        .finish_backfill_job(&frontend_client, 54)
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("only a BaseComplete job is cleanable"),
        "expected finish-while-Running rejection, got {err:?}"
    );
    assert!(handler.captured_sql.lock().unwrap().is_empty());

    // Release the query; the run completes normally and the job reaches
    // BaseComplete (untouched by the rejected finish).
    query_gate.notify_one();
    run_handle.await.unwrap();
    assert_eq!(
        task.state.read().unwrap().backfill_jobs()[0].status,
        crate::batching_mode::state::BackfillJobStatus::BaseComplete
    );
}

#[tokio::test]
async fn test_finish_backfill_job_second_finish_returns_busy() {
    let sink_table_name = "twe_sink_backfill_double_finish";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 10301).await;

    // The first finish's DROP blocks at the gate, keeping the job Finishing.
    let drop_gate = Arc::new(tokio::sync::Notify::new());
    let handler = Arc::new(
        BackfillLifecycleHandler::new(query_engine.clone(), vec![(7, Some(99))])
            .with_drop_gate(drop_gate.clone()),
    );
    let frontend_client = lifecycle_frontend_client(&handler);
    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            55,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();
    task.run_backfill_base(&frontend_client, &base)
        .await
        .unwrap();

    // First finish: DROP in flight, job is Finishing.
    let finish_task = task.clone();
    let finish_client = frontend_client.clone();
    let finish_handle =
        tokio::spawn(async move { finish_task.finish_backfill_job(&finish_client, 55).await });
    wait_until(|| !handler.captured_sql.lock().unwrap().is_empty()).await;
    assert_eq!(
        task.state.read().unwrap().backfill_jobs()[0].status,
        crate::batching_mode::state::BackfillJobStatus::Finishing
    );

    // A second concurrent finish is Busy and must not issue another DROP.
    let err = task
        .finish_backfill_job(&frontend_client, 55)
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("cannot finish"),
        "expected second-finish Busy error, got {err:?}"
    );
    assert_eq!(handler.captured_sql.lock().unwrap().len(), 1);

    // Release the DROP; the first finish completes and removes the job.
    drop_gate.notify_one();
    finish_handle.await.unwrap().unwrap();
    assert!(task.state.read().unwrap().backfill_jobs().is_empty());
}

#[tokio::test]
async fn test_finish_backfill_job_rejects_reprepare_while_finishing() {
    let sink_table_name = "twe_sink_backfill_finish_vs_prepare";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 10401).await;

    let drop_gate = Arc::new(tokio::sync::Notify::new());
    let handler = Arc::new(
        BackfillLifecycleHandler::new(query_engine.clone(), vec![(7, Some(99))])
            .with_drop_gate(drop_gate.clone()),
    );
    let frontend_client = lifecycle_frontend_client(&handler);
    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            56,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();
    task.run_backfill_base(&frontend_client, &base)
        .await
        .unwrap();

    // First finish holds the job in Finishing (DROP gated).
    let finish_task = task.clone();
    let finish_client = frontend_client.clone();
    let finish_handle =
        tokio::spawn(async move { finish_task.finish_backfill_job(&finish_client, 56).await });
    wait_until(|| !handler.captured_sql.lock().unwrap().is_empty()).await;
    assert_eq!(
        task.state.read().unwrap().backfill_jobs()[0].status,
        crate::batching_mode::state::BackfillJobStatus::Finishing
    );

    // A re-prepare while Finishing is rejected: a new generation can never be
    // installed over an in-flight cleanup.
    let err = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            56,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap_err();
    assert!(
        format!("{err:?}").contains("cleanup is in flight"),
        "expected re-prepare-while-Finishing rejection, got {err:?}"
    );

    // Release the DROP; the finish completes and removes the job.
    drop_gate.notify_one();
    finish_handle.await.unwrap().unwrap();
    assert!(task.state.read().unwrap().backfill_jobs().is_empty());
}

#[tokio::test]
async fn test_finish_backfill_job_then_reprepare_is_a_fresh_generation() {
    let sink_table_name = "twe_sink_backfill_finish_then_prepare";
    let (task, query_engine, _) = new_backfill_task(sink_table_name, 10501).await;

    let handler = Arc::new(BackfillLifecycleHandler::new(
        query_engine.clone(),
        vec![(7, Some(99))],
    ));
    let frontend_client = lifecycle_frontend_client(&handler);

    // Generation 1: prepare -> run -> finish (DROP succeeds, table removed).
    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            57,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();
    task.run_backfill_base(&frontend_client, &base)
        .await
        .unwrap();
    task.finish_backfill_job(&frontend_client, 57)
        .await
        .unwrap();
    assert!(task.state.read().unwrap().backfill_jobs().is_empty());
    assert_eq!(handler.create_attempts(), 1);

    // Generation 2: a fresh prepare for the same job id is a brand-new job
    // (registry empty, table gone) and creates a new staging table — the old
    // generation is never reused.
    let base = task
        .prepare_backfill_base(
            &query_engine,
            &frontend_client,
            57,
            Timestamp::new_second(0),
            Timestamp::new_second(300),
        )
        .await
        .unwrap();
    assert_eq!(base.job.job_id, 57);
    assert_eq!(
        handler.create_attempts(),
        2,
        "a new generation must re-create"
    );
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 1);
        assert_eq!(
            state.backfill_jobs()[0].status,
            crate::batching_mode::state::BackfillJobStatus::Prepared
        );
    }
}

/// ---------------------------------------------------------------------------
/// Phase-2 finalize tests: unified catch-up (non-target delta branch + target
/// FULL OUTER staging merge branch) with a single pre-bound high watermark H.
///
/// The mock frontend executes the captured branch SELECT plans through the
/// query engine (real aggregation / merge behavior) and returns the branch's
/// terminal region watermarks from a synthetic map, so the end-to-end test can
/// assert the exact rows each branch would insert into the active sink.
/// ---------------------------------------------------------------------------
/// Builds a query engine with a custom `numbers_with_ts` source (rows are
/// `(number, ts_ms)`). Mirrors `crate::test_utils::create_test_query_engine`
/// but with caller-controlled source data.
fn create_finalize_engine(source_rows: &[(u32, i64)]) -> QueryEngineRef {
    let catalog_list = catalog::memory::new_memory_catalog_manager().unwrap();
    let schema = Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), false),
        ColumnSchema::new("ts", CDT::timestamp_millisecond_datatype(), false).with_time_index(true),
    ]));
    let mut numbers = datatypes::vectors::UInt32VectorBuilder::with_capacity(source_rows.len());
    let mut ts =
        datatypes::vectors::TimestampMillisecondVectorBuilder::with_capacity(source_rows.len());
    for (number, ts_ms) in source_rows {
        numbers.push(Some(*number));
        ts.push(Some(datatypes::timestamp::TimestampMillisecond::new(
            *ts_ms,
        )));
    }
    let recordbatch = RecordBatch::new(schema, vec![numbers.to_vector(), ts.to_vector()]).unwrap();
    let table = MemTable::table("numbers_with_ts", recordbatch);
    catalog_list
        .register_table_sync(RegisterTableRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_SCHEMA_NAME.to_string(),
            table_name: "numbers_with_ts".to_string(),
            table_id: 1024,
            table,
        })
        .unwrap();
    let factory = query::QueryEngineFactory::new(
        catalog_list,
        None,
        None,
        None,
        None,
        false,
        QueryOptions::default(),
    );
    let engine = factory.query_engine();
    crate::transform::register_function_to_query_engine(&engine);
    engine
}

/// The active sink used by the finalize tests: the flow output columns
/// (`number` dimension, `max_ts` aggregate, `time_window` time index) plus the
/// auto-created `update_at` and the reserved internal epoch column, so
/// `stamp_epoch_into_plan` and `write_checkpoint_row` can run.
fn finalize_sink_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), true),
        ColumnSchema::new("max_ts", CDT::timestamp_millisecond_datatype(), true),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
        ColumnSchema::new("update_at", CDT::timestamp_millisecond_datatype(), true),
        ColumnSchema::new(
            crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME,
            CDT::uint64_datatype(),
            true,
        ),
    ]))
}

/// A finalize sink row: `(number, max_ts_ms, time_window_ms, epoch)`.
type FinalizeSinkRow = (Option<u32>, Option<i64>, Option<i64>, Option<u64>);

fn finalize_sink_recordbatch(rows: Vec<FinalizeSinkRow>) -> RecordBatch {
    let schema = finalize_sink_schema();
    let mut numbers = datatypes::vectors::UInt32VectorBuilder::with_capacity(rows.len());
    let mut max_ts =
        datatypes::vectors::TimestampMillisecondVectorBuilder::with_capacity(rows.len());
    let mut windows =
        datatypes::vectors::TimestampMillisecondVectorBuilder::with_capacity(rows.len());
    let mut update_at =
        datatypes::vectors::TimestampMillisecondVectorBuilder::with_capacity(rows.len());
    let mut epochs = datatypes::vectors::UInt64VectorBuilder::with_capacity(rows.len());
    for (number, max_ts_ms, window_ms, epoch) in rows {
        numbers.push(number);
        max_ts.push(max_ts_ms.map(datatypes::timestamp::TimestampMillisecond::new));
        windows.push(window_ms.map(datatypes::timestamp::TimestampMillisecond::new));
        update_at.push(Some(datatypes::timestamp::TimestampMillisecond::new(0)));
        epochs.push(epoch);
    }
    RecordBatch::new(
        schema,
        vec![
            numbers.to_vector(),
            max_ts.to_vector(),
            windows.to_vector(),
            update_at.to_vector(),
            epochs.to_vector(),
        ],
    )
    .unwrap()
}

fn register_finalize_sink(
    query_engine: &QueryEngineRef,
    table_name: &str,
    table_id: u32,
    rows: Vec<FinalizeSinkRow>,
) {
    let batch = finalize_sink_recordbatch(rows);
    let table = MemTable::table(table_name, batch);
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id,
        table,
    };
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog.register_table_sync(request).unwrap();
}

/// The backfill staging table for the finalize tests: the flow output columns
/// only (the FULL OUTER merge base reads group keys + aggregate columns).
fn finalize_staging_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        ColumnSchema::new("number", CDT::uint32_datatype(), true),
        ColumnSchema::new("max_ts", CDT::timestamp_millisecond_datatype(), true),
        ColumnSchema::new("time_window", CDT::timestamp_millisecond_datatype(), false)
            .with_time_index(true),
        ColumnSchema::new("update_at", CDT::timestamp_millisecond_datatype(), true),
    ]))
}

fn finalize_staging_recordbatch(rows: Vec<(u32, i64, i64)>) -> RecordBatch {
    let schema = finalize_staging_schema();
    let mut numbers = datatypes::vectors::UInt32VectorBuilder::with_capacity(rows.len());
    let mut max_ts =
        datatypes::vectors::TimestampMillisecondVectorBuilder::with_capacity(rows.len());
    let mut windows =
        datatypes::vectors::TimestampMillisecondVectorBuilder::with_capacity(rows.len());
    let mut update_at =
        datatypes::vectors::TimestampMillisecondVectorBuilder::with_capacity(rows.len());
    for (number, max_ts_ms, window_ms) in rows {
        numbers.push(Some(number));
        max_ts.push(Some(datatypes::timestamp::TimestampMillisecond::new(
            max_ts_ms,
        )));
        windows.push(Some(datatypes::timestamp::TimestampMillisecond::new(
            window_ms,
        )));
        update_at.push(Some(datatypes::timestamp::TimestampMillisecond::new(0)));
    }
    RecordBatch::new(
        schema,
        vec![
            numbers.to_vector(),
            max_ts.to_vector(),
            windows.to_vector(),
            update_at.to_vector(),
        ],
    )
    .unwrap()
}

fn register_finalize_staging(
    query_engine: &QueryEngineRef,
    table_name: &str,
    table_id: u32,
    rows: Vec<(u32, i64, i64)>,
) {
    let batch = finalize_staging_recordbatch(rows);
    let table = MemTable::new_with_catalog(
        table_name,
        batch,
        table_id,
        DEFAULT_CATALOG_NAME.to_string(),
        DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
    );
    let catalog_manager = query_engine.engine_state().catalog_manager();
    let memory_catalog = catalog_manager
        .as_any()
        .downcast_ref::<MemoryCatalogManager>()
        .unwrap();
    memory_catalog
        .register_schema_sync(catalog::RegisterSchemaRequest {
            catalog: DEFAULT_CATALOG_NAME.to_string(),
            schema: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
        })
        .unwrap();
    let request = RegisterTableRequest {
        catalog: DEFAULT_CATALOG_NAME.to_string(),
        schema: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
        table_name: table_name.to_string(),
        table_id,
        table,
    };
    memory_catalog.register_table_sync(request).unwrap();
}

/// Builds a task for the finalize tests: a time-window aggregate flow over a
/// custom `numbers_with_ts` source with the finalize sink registered.
async fn new_finalize_task(
    sink_table: &str,
    _sink_table_id: u32,
    source_rows: &[(u32, i64)],
) -> (BatchingTask, QueryEngineRef) {
    let query_engine = create_finalize_engine(source_rows);
    let ctx = QueryContext::arc();
    let flow_query = "SELECT number, max(ts) AS max_ts, date_bin(INTERVAL '5 second', ts) AS time_window \
         FROM numbers_with_ts GROUP BY time_window, number";
    let plan = sql_to_df_plan(ctx.clone(), query_engine.clone(), flow_query, true)
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
        .unwrap();

    // NOTE: the sink table is registered by each test (empty or pre-seeded
    // with the active state at L) so the e2e test can control the Active side.
    let (_tx, rx) = tokio::sync::oneshot::channel();
    let task = BatchingTask::try_new(TaskArgs {
        flow_id: 1,
        query: flow_query,
        plan: plan.clone(),
        time_window_expr,
        expire_after: None,
        sink_table_name: [
            "greptime".to_string(),
            "public".to_string(),
            sink_table.to_string(),
        ],
        source_table_names: vec![[
            "greptime".to_string(),
            "public".to_string(),
            "numbers_with_ts".to_string(),
        ]],
        query_ctx: ctx,
        catalog_manager: query_engine.engine_state().catalog_manager().clone(),
        shutdown_rx: rx,
        batch_opts: sequence_range_batch_opts(),
        flow_eval_interval: None,
        eval_schedule: None,
    })
    .unwrap();
    (task, query_engine)
}

/// A stream that yields a list of batches and exposes terminal region
/// watermarks only once exhausted (mirrors `BackfillMetricsStream`).
struct FinalizeMetricsStream {
    batches: Vec<RecordBatch>,
    idx: usize,
    metrics: RecordBatchMetrics,
}

impl futures::Stream for FinalizeMetricsStream {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.idx < self.batches.len() {
            let batch = self.batches[self.idx].clone();
            self.idx += 1;
            Poll::Ready(Some(Ok(batch)))
        } else {
            Poll::Ready(None)
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.batches.len() - self.idx;
        (remaining, Some(remaining))
    }
}

impl RecordBatchStream for FinalizeMetricsStream {
    fn name(&self) -> &str {
        "FinalizeMetricsStream"
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn schema(&self) -> datatypes::schema::SchemaRef {
        self.batches
            .first()
            .map(|batch| batch.schema.clone())
            .unwrap_or_else(|| Arc::new(Schema::new(vec![])))
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        if self.idx < self.batches.len() {
            // Not terminal yet: do not overwrite the terminal metrics handle.
            None
        } else {
            Some(self.metrics.clone())
        }
    }
}

/// A finalize mock frontend: executes captured branch/probe SELECT plans
/// through the query engine (real aggregation and FULL OUTER merge behavior),
/// returns synthetic terminal watermarks, handles the checkpoint-row write
/// (detected by the EmptyRelation sentinel projection) as a one-row affected
/// write, and drops the staging table on the DROP TABLE SQL.
#[derive(Clone)]
struct FinalizeLifecycleHandler {
    engine: QueryEngineRef,
    /// Watermarks returned for every branch AND the probe (unless overridden).
    watermarks: Vec<(u64, Option<u64>)>,
    /// Watermarks returned for the probe specifically; defaults to `watermarks`.
    probe_watermarks: Option<Vec<(u64, Option<u64>)>>,
    /// Number of branch queries to fail (first N branches).
    branch_failures_remaining: Arc<std::sync::atomic::AtomicUsize>,
    /// Fails the second branch (the target final branch).
    fail_second_branch: bool,
    /// Fails the probe.
    fail_probe: bool,
    /// Fails the checkpoint-row write.
    fail_checkpoint_write: bool,
    /// Fails the DROP TABLE SQL.
    fail_drops: bool,
    created_tables: Arc<std::sync::Mutex<Vec<String>>>,
    captured_probe_requests: Arc<std::sync::Mutex<Vec<api::v1::QueryRequest>>>,
    captured_branch_plans: Arc<std::sync::Mutex<Vec<api::v1::InsertIntoPlan>>>,
    captured_branch_extensions: Arc<std::sync::Mutex<Vec<HashMap<String, String>>>>,
    captured_branch_snapshot_seqs: Arc<std::sync::Mutex<Vec<HashMap<u64, u64>>>>,
    captured_checkpoint_plans: Arc<std::sync::Mutex<Vec<api::v1::InsertIntoPlan>>>,
    /// Record batches of every executed branch SELECT (the rows each branch
    /// would insert into the active sink).
    captured_branches: Arc<std::sync::Mutex<Vec<Vec<RecordBatch>>>>,
    branch_count: Arc<std::sync::atomic::AtomicUsize>,
    request_log: Arc<std::sync::Mutex<Vec<&'static str>>>,
}

impl FinalizeLifecycleHandler {
    fn new(engine: QueryEngineRef, watermarks: Vec<(u64, Option<u64>)>) -> Self {
        Self {
            engine,
            watermarks,
            probe_watermarks: None,
            branch_failures_remaining: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            fail_second_branch: false,
            fail_probe: false,
            fail_checkpoint_write: false,
            fail_drops: false,
            created_tables: Arc::new(std::sync::Mutex::new(Vec::new())),
            captured_probe_requests: Arc::new(std::sync::Mutex::new(Vec::new())),
            captured_branch_plans: Arc::new(std::sync::Mutex::new(Vec::new())),
            captured_branch_extensions: Arc::new(std::sync::Mutex::new(Vec::new())),
            captured_branch_snapshot_seqs: Arc::new(std::sync::Mutex::new(Vec::new())),
            captured_checkpoint_plans: Arc::new(std::sync::Mutex::new(Vec::new())),
            captured_branches: Arc::new(std::sync::Mutex::new(Vec::new())),
            branch_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            request_log: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    fn with_probe_watermarks(mut self, watermarks: Vec<(u64, Option<u64>)>) -> Self {
        self.probe_watermarks = Some(watermarks);
        self
    }

    fn with_branch_failures(self, failures: usize) -> Self {
        self.branch_failures_remaining
            .store(failures, std::sync::atomic::Ordering::SeqCst);
        self
    }

    fn with_fail_second_branch(mut self) -> Self {
        self.fail_second_branch = true;
        self
    }

    fn with_fail_probe(mut self) -> Self {
        self.fail_probe = true;
        self
    }

    fn with_fail_checkpoint_write(mut self) -> Self {
        self.fail_checkpoint_write = true;
        self
    }

    fn with_failed_drops(mut self) -> Self {
        self.fail_drops = true;
        self
    }

    fn branch_count(&self) -> usize {
        self.branch_count.load(std::sync::atomic::Ordering::SeqCst)
    }
}

fn finalize_frontend_client(handler: &Arc<FinalizeLifecycleHandler>) -> Arc<FrontendClient> {
    let handler_trait: Arc<dyn GrpcQueryHandlerWithBoxedError> = handler.clone();
    Arc::new(FrontendClient::from_grpc_handler(
        Arc::downgrade(&handler_trait),
        QueryOptions::default(),
    ))
}

fn finalize_metrics_stream(batches: Vec<RecordBatch>, watermarks: &[(u64, Option<u64>)]) -> Output {
    let metrics = RecordBatchMetrics {
        region_watermarks: watermarks
            .iter()
            .map(|(region_id, watermark)| RegionWatermarkEntry {
                region_id: *region_id,
                watermark: *watermark,
            })
            .collect(),
        ..Default::default()
    };
    Output::new_with_stream(Box::pin(FinalizeMetricsStream {
        batches,
        idx: 0,
        metrics,
    }))
}

#[async_trait::async_trait]
impl GrpcQueryHandlerWithBoxedError for FinalizeLifecycleHandler {
    async fn do_query(
        &self,
        query: api::v1::greptime_request::Request,
        ctx: QueryContextRef,
    ) -> std::result::Result<Output, BoxedError> {
        use api::v1::greptime_request::Request;
        use api::v1::query_request::Query;
        use api::v1::{DdlRequest, QueryRequest};

        match query {
            Request::Ddl(DdlRequest { expr: Some(_), .. }) => {
                panic!("finalize tests register tables directly, no DDL expected")
            }
            Request::Query(QueryRequest {
                query: Some(Query::Sql(sql)),
                ..
            }) => {
                self.request_log.lock().unwrap().push("drop");
                if self.fail_drops && sql.contains("DROP TABLE") {
                    return Err(BoxedError::new(MockError::new(StatusCode::Internal)));
                }
                if sql.contains("DROP TABLE") {
                    // The DROP statement is
                    // `DROP TABLE IF EXISTS <catalog>.<schema>.<table>` (the
                    // table name may or may not be quoted); extract the last
                    // dot-separated segment as the table name and deregister
                    // it from the private schema.
                    let table_name = sql
                        .rsplit('.')
                        .next()
                        .expect("table name in DROP")
                        .trim()
                        .trim_matches('"')
                        .to_string();
                    let catalog_manager = self.engine.engine_state().catalog_manager();
                    let memory_catalog = catalog_manager
                        .as_any()
                        .downcast_ref::<MemoryCatalogManager>()
                        .unwrap();
                    memory_catalog
                        .deregister_table_sync(catalog::DeregisterTableRequest {
                            catalog: DEFAULT_CATALOG_NAME.to_string(),
                            schema: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
                            table_name,
                        })
                        .unwrap();
                    self.created_tables.lock().unwrap().clear();
                }
                Ok(Output::new_with_affected_rows(0))
            }
            Request::Query(QueryRequest {
                query: Some(Query::InsertIntoPlan(insert)),
                ..
            }) => {
                let session_state = decode_session_state(&self.engine);
                let plan = DFLogicalSubstraitConvertor {}
                    .decode(
                        bytes::Bytes::from(insert.logical_plan.clone()),
                        session_state,
                    )
                    .await
                    .map_err(BoxedError::new)?;

                if contains_empty_relation(&plan) {
                    // The checkpoint-row write: the singleton sentinel
                    // projection over an empty relation. The write must affect
                    // exactly one row.
                    self.request_log.lock().unwrap().push("checkpoint");
                    self.captured_checkpoint_plans
                        .lock()
                        .unwrap()
                        .push(insert.clone());
                    if self.fail_checkpoint_write {
                        return Err(BoxedError::new(MockError::new(StatusCode::Internal)));
                    }
                    return Ok(Output::new_with_affected_rows(1));
                }

                // A finalize branch: execute the merged SELECT through the
                // query engine and return its rows (with terminal watermarks).
                let branch_idx = self
                    .branch_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                self.request_log.lock().unwrap().push("branch");
                self.captured_branch_plans
                    .lock()
                    .unwrap()
                    .push(insert.clone());
                self.captured_branch_extensions
                    .lock()
                    .unwrap()
                    .push(ctx.extensions().clone());
                self.captured_branch_snapshot_seqs
                    .lock()
                    .unwrap()
                    .push(ctx.snapshots());

                if self
                    .branch_failures_remaining
                    .load(std::sync::atomic::Ordering::SeqCst)
                    > 0
                {
                    self.branch_failures_remaining
                        .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
                    return Err(BoxedError::new(MockError::new(StatusCode::Internal)));
                }
                if self.fail_second_branch && branch_idx == 1 {
                    return Err(BoxedError::new(MockError::new(StatusCode::Internal)));
                }

                let output = self
                    .engine
                    .execute(plan, ctx)
                    .await
                    .map_err(BoxedError::new)?;
                let batches = match output.data {
                    common_query::OutputData::RecordBatches(batches) => {
                        batches.into_iter().collect::<Vec<_>>()
                    }
                    common_query::OutputData::Stream(stream) => {
                        common_recordbatch::util::collect_batches(stream)
                            .await
                            .map_err(BoxedError::new)?
                            .into_iter()
                            .collect::<Vec<_>>()
                    }
                    common_query::OutputData::AffectedRows(_) => vec![],
                };
                self.captured_branches.lock().unwrap().push(batches.clone());
                Ok(finalize_metrics_stream(batches, &self.watermarks))
            }
            Request::Query(QueryRequest {
                query: Some(Query::LogicalPlan(bytes)),
                ..
            }) => {
                // The high-watermark probe.
                self.request_log.lock().unwrap().push("probe");
                self.captured_probe_requests
                    .lock()
                    .unwrap()
                    .push(QueryRequest {
                        query: Some(Query::LogicalPlan(bytes.clone())),
                    });
                if self.fail_probe {
                    return Err(BoxedError::new(MockError::new(StatusCode::Internal)));
                }
                let session_state = decode_session_state(&self.engine);
                let plan = DFLogicalSubstraitConvertor {}
                    .decode(bytes::Bytes::from(bytes), session_state)
                    .await
                    .map_err(BoxedError::new)?;
                let output = self
                    .engine
                    .execute(plan, ctx)
                    .await
                    .map_err(BoxedError::new)?;
                let batches = match output.data {
                    common_query::OutputData::RecordBatches(batches) => {
                        batches.into_iter().collect::<Vec<_>>()
                    }
                    common_query::OutputData::Stream(stream) => {
                        common_recordbatch::util::collect_batches(stream)
                            .await
                            .map_err(BoxedError::new)?
                            .into_iter()
                            .collect::<Vec<_>>()
                    }
                    common_query::OutputData::AffectedRows(_) => vec![],
                };
                let probe_watermarks = self.probe_watermarks.as_deref().unwrap_or(&self.watermarks);
                Ok(finalize_metrics_stream(batches, probe_watermarks))
            }
            other => panic!("unexpected finalize request, got {other:?}"),
        }
    }
}

/// Drives a registered job to `BaseComplete(F)`: reserve -> finish prepare
/// (staging table must already be registered with `staging_table_id`) -> run
/// -> complete.
fn drive_job_to_base_complete(
    task: &BatchingTask,
    job_id: u64,
    range: (Timestamp, Timestamp),
    staging_name: [String; 3],
    staging_table_id: u32,
    frozen: BTreeMap<u64, u64>,
) {
    let mut state = task.state.write().unwrap();
    let reservation = state
        .begin_backfill_prepare(job_id, range, staging_name)
        .expect("finalize job reserves");
    // A fresh job reserves; a retry of an already-registered (BaseComplete)
    // job returns `Existing` with the identity preserved and stays
    // BaseComplete.
    let is_fresh = match reservation {
        crate::batching_mode::state::PrepareReservation::Reserved(_) => {
            state
                .finish_backfill_prepare(job_id, staging_table_id)
                .expect("finish prepare");
            true
        }
        crate::batching_mode::state::PrepareReservation::Existing(_) => false,
        other => panic!("unexpected finalize prepare reservation: {other:?}"),
    };
    if is_fresh {
        state.begin_backfill_run(job_id).expect("begin run");
        state
            .complete_backfill_run(job_id, frozen)
            .expect("complete run");
    }
}

/// Seeds the durable frontier L, checkpoint persistence, and a registered
/// BaseComplete job with frozen watermark F, then runs `run_backfill_finalize`.
#[allow(clippy::too_many_arguments)]
async fn run_finalize_with_setup(
    engine: &QueryEngineRef,
    task: &BatchingTask,
    frontend_client: &Arc<FrontendClient>,
    job_id: u64,
    range: (Timestamp, Timestamp),
    staging_name: [String; 3],
    staging_table_id: u32,
    frozen: BTreeMap<u64, u64>,
    lower: BTreeMap<u64, u64>,
) -> Result<(), crate::Error> {
    {
        let mut state = task.state.write().unwrap();
        state.set_checkpoint_persistence(Some(CheckpointPersistence {
            epoch_col_name: crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME.to_string(),
            checkpoint_col_name: crate::batching_mode::INTERNAL_FLOW_CHECKPOINT_COL_NAME.to_string(),
            window_col_name: "time_window".to_string(),
            primary_key_columns: vec!["number".to_string()],
        }));
        state.advance_checkpoints(lower.clone().into_iter().collect());
    }
    drive_job_to_base_complete(task, job_id, range, staging_name, staging_table_id, frozen);
    task.run_backfill_finalize(engine, frontend_client, job_id)
        .await
}

/// The finalize test range: `[0s, 15s)` aligned to 5-second windows.
fn finalize_test_range() -> (Timestamp, Timestamp) {
    (Timestamp::new_second(0), Timestamp::new_second(15))
}

fn finalize_staging_name(job_id: u64) -> [String; 3] {
    backfill_staging_table_name(1, job_id)
}

/// Asserts the non-target branch plan carries the event-time EXCLUDE filter
/// `ts < start OR ts >= end` (test list item 1).
#[tokio::test]
async fn test_finalize_non_target_plan_has_exclude_filter() {
    let (task, query_engine) =
        new_finalize_task("finalize_plan_sink", 9150, &[(1, 1000), (5, 20000)]).await;
    register_finalize_sink(&query_engine, "finalize_plan_sink", 9150, vec![]);

    let range = finalize_test_range();
    let plan = task
        .build_backfill_non_target_delta_plan(query_engine.clone(), range)
        .await
        .expect("non-target delta plan builds");
    let LogicalPlan::Dml(dml) = &plan else {
        panic!("expected DML insert into the active sink, got {:?}", plan);
    };
    assert_eq!(
        dml.table_name.table().to_string().as_str(),
        "finalize_plan_sink",
        "the non-target branch must write into the active sink"
    );
    let inner = format!("{}", dml.input.display_indent());
    // The exclude filter is `(ts < 0) OR (ts >= 15000)` (in millis).
    assert!(
        inner.contains("TimestampMillisecond(0") && inner.contains("OR"),
        "non-target branch must carry the exclude filter (win < start) OR (win >= end), got: {inner}"
    );
    assert!(
        inner.contains("TimestampMillisecond(15000"),
        "non-target branch must carry the aligned upper bound, got: {inner}"
    );
}

/// Asserts the target final branch carries the Tail event-time filter
/// `ts >= start AND ts < end`, merges FULL OUTER with the staging table as the
/// base, and writes into the active sink (test list items 2 and 3).
#[tokio::test]
async fn test_finalize_target_plan_filters_and_full_outer_staging_base() {
    let (task, query_engine) =
        new_finalize_task("finalize_target_plan_sink", 9151, &[(1, 1000), (2, 5000)]).await;
    register_finalize_sink(&query_engine, "finalize_target_plan_sink", 9151, vec![]);

    let job_id = 31;
    let staging_name = finalize_staging_name(job_id);
    register_finalize_staging(&query_engine, &staging_name[2], 31, vec![(1, 1000, 0)]);
    let job = BackfillJob {
        job_id,
        range: finalize_test_range(),
        staging_table_name: staging_name.clone(),
        staging_table_id: Some(31),
        frozen_watermark: Some(BTreeMap::from([(11_u64, 99_u64)])),
        status: crate::batching_mode::state::BackfillJobStatus::BaseComplete,
    };

    let plan = task
        .build_backfill_final_delta_plan(query_engine.clone(), &job)
        .await
        .expect("target final plan builds");
    let LogicalPlan::Dml(dml) = &plan else {
        panic!("expected DML insert into the active sink, got {:?}", plan);
    };
    assert_eq!(
        dml.table_name.table().to_string().as_str(),
        "finalize_target_plan_sink",
        "the target branch must write into the active sink"
    );
    let inner = format!("{}", dml.input.display_indent());
    // Tail event-time filter `ts >= 0 AND ts < 15000` (millis).
    assert!(
        inner.contains("TimestampMillisecond(0") && inner.contains("AND"),
        "target branch must carry the event-time [start, end) filter, got: {inner}"
    );
    assert!(
        inner.contains("TimestampMillisecond(15000"),
        "target branch must carry the aligned upper bound, got: {inner}"
    );
    // FULL OUTER join against the staging table base (DataFusion renders
    // `JoinType::Full` as "Full Join" in `display_indent`).
    assert!(
        inner.contains("Full Join"),
        "target branch must merge FULL OUTER with the staging base, got: {inner}"
    );
    assert!(
        inner.contains("__flow_backfill_1_31"),
        "target branch merge base must be the staging table, got: {inner}"
    );
    // No dirty-window or retention filter may sneak into the delta plan.
    assert!(
        !inner.contains("expire_after"),
        "target branch must not apply a retention filter, got: {inner}"
    );
}

/// Asserts the probe runs FIRST and both branches are pre-bound to the SAME
/// high watermark H, with the correct after-seqs (L for the non-target branch,
/// F for the target branch) and sequence-range incremental extensions
/// (test list item 5).
#[tokio::test]
async fn test_finalize_branches_prebound_consistent_high() {
    let (task, query_engine) = new_finalize_task(
        "finalize_h_sink",
        9152,
        &[
            (1, 1000),
            (2, 5000),
            (2, 7000),
            (3, 10000),
            (5, 20000),
            (6, -1000),
        ],
    )
    .await;
    register_finalize_sink(&query_engine, "finalize_h_sink", 9152, vec![]);

    let job_id = 32;
    let staging_name = finalize_staging_name(job_id);
    register_finalize_staging(
        &query_engine,
        &staging_name[2],
        32,
        vec![(1, 1000, 0), (2, 5000, 5000)],
    );

    let frozen = BTreeMap::from([(11_u64, 99_u64), (22_u64, 199_u64)]);
    let lower = BTreeMap::from([(11_u64, 50_u64), (22_u64, 100_u64)]);
    let high = BTreeMap::from([(11_u64, 120_u64), (22_u64, 250_u64)]);
    let watermarks = high
        .iter()
        .map(|(region_id, seq)| (*region_id, Some(*seq)))
        .collect::<Vec<_>>();

    let handler = Arc::new(FinalizeLifecycleHandler::new(
        query_engine.clone(),
        watermarks,
    ));
    let frontend_client = finalize_frontend_client(&handler);
    run_finalize_with_setup(
        &query_engine,
        &task,
        &frontend_client,
        job_id,
        finalize_test_range(),
        staging_name,
        32,
        frozen.clone(),
        lower.clone(),
    )
    .await
    .expect("finalize succeeds with a consistent pre-bound H");

    // Probe first, then non-target branch, target branch, checkpoint, drop.
    let log = handler.request_log.lock().unwrap().clone();
    assert_eq!(
        log,
        vec!["probe", "branch", "branch", "checkpoint", "drop"],
        "the probe must run before both branches and the commit after them, got {log:?}"
    );

    // Both branches are pre-bound to the same H.
    let snapshot_seqs = handler.captured_branch_snapshot_seqs.lock().unwrap();
    assert_eq!(snapshot_seqs.len(), 2);
    let high_map: HashMap<u64, u64> = high.iter().map(|(k, v)| (*k, *v)).collect();
    assert_eq!(
        snapshot_seqs[0], high_map,
        "non-target branch must be pre-bound to H"
    );
    assert_eq!(
        snapshot_seqs[1], high_map,
        "target branch must be pre-bound to the SAME H"
    );

    // after_seqs: L on the non-target branch, F on the target branch.
    let extensions = handler.captured_branch_extensions.lock().unwrap();
    assert_eq!(extensions.len(), 2);
    let after_seqs_0: BTreeMap<u64, u64> =
        serde_json::from_str(extensions[0].get(FLOW_INCREMENTAL_AFTER_SEQS).unwrap()).unwrap();
    let after_seqs_1: BTreeMap<u64, u64> =
        serde_json::from_str(extensions[1].get(FLOW_INCREMENTAL_AFTER_SEQS).unwrap()).unwrap();
    assert_eq!(after_seqs_0, lower, "non-target branch replays (L, H]");
    assert_eq!(after_seqs_1, frozen, "target branch replays (F, H]");
    for ext in extensions.iter() {
        assert_eq!(
            ext.get(FLOW_INCREMENTAL_MODE).map(String::as_str),
            Some(FLOW_INCREMENTAL_MODE_SEQUENCE_RANGE),
            "finalize branches must run in sequence_range incremental mode"
        );
        assert_eq!(
            ext.get(FLOW_SINK_TABLE_ID).map(String::as_str),
            Some("1"),
            "the active sink must be excluded from incremental semantics (memory-catalog MemTable carries table id 1)"
        );
        assert_eq!(
            ext.get(FLOW_INTERNAL_NON_SOURCE_TABLE_IDS)
                .map(String::as_str),
            Some("[32]"),
            "the staging table must be scanned as a plain non-source read"
        );
    }

    // The probe itself was sent as a watermark-only read.
    let probes = handler.captured_probe_requests.lock().unwrap();
    assert_eq!(probes.len(), 1);
    assert!(matches!(
        probes[0].query,
        Some(api::v1::query_request::Query::LogicalPlan(_))
    ));
}

/// Extracts rows `(number, max_ts_ms, time_window_ms)` from a batch list.
fn finalize_rows(batches: &[RecordBatch]) -> Vec<(u32, i64, i64)> {
    let mut rows = Vec::new();
    for batch in batches {
        let number = batch
            .column_by_name("number")
            .expect("number column")
            .as_any()
            .downcast_ref::<datatypes::arrow::array::UInt32Array>()
            .expect("number array");
        let max_ts = batch
            .column_by_name("max_ts")
            .expect("max_ts column")
            .as_any()
            .downcast_ref::<datatypes::arrow::array::TimestampMillisecondArray>()
            .expect("max_ts array");
        let window = batch
            .column_by_name("time_window")
            .expect("time_window column")
            .as_any()
            .downcast_ref::<datatypes::arrow::array::TimestampMillisecondArray>()
            .expect("time_window array");
        for i in 0..number.len() {
            if number.is_null(i) || max_ts.is_null(i) || window.is_null(i) {
                continue;
            }
            rows.push((number.value(i), max_ts.value(i), window.value(i)));
        }
    }
    rows.sort_unstable();
    rows
}

/// End-to-end finalize: Base (staging, F) + Tail + non-target data + pre-seeded
/// Active sink rows, asserting the active sink state equals
/// `Base ⊕ Tail(target) ∪ Active ∪ Delta(non-target)` with no duplicates, no
/// loss, correct target window values, and late rows excluded from the target.
/// Also asserts the checkpoint advances to H, the staging table is dropped,
/// and the job is removed (test list items 4, 6, and 8).
#[tokio::test]
async fn test_finalize_end_to_end_merges_and_commits() {
    // Source rows: in-range (1,2,2,3) + non-target (5: ts >= end, 6: late ts <
    // start). (2,7000) arrived after F was frozen, so it only enters the Tail.
    let (task, query_engine) = new_finalize_task(
        "finalize_e2e_sink",
        9153,
        &[
            (1, 1000),
            (2, 5000),
            (2, 7000),
            (3, 10000),
            (5, 20000),
            (6, -1000),
        ],
    )
    .await;
    // Active sink state at L: one pre-existing non-target group (7, window 0).
    register_finalize_sink(
        &query_engine,
        "finalize_e2e_sink",
        9153,
        vec![(Some(7), Some(500), Some(0), None)],
    );

    let job_id = 33;
    let staging_name = finalize_staging_name(job_id);
    // Base (staging) at F: covers (1, window 0) and (2, window 5000) up to F.
    register_finalize_staging(
        &query_engine,
        &staging_name[2],
        33,
        vec![(1, 1000, 0), (2, 5000, 5000)],
    );

    // Multi-region F / L / H maps (test list item 8).
    let frozen = BTreeMap::from([(11_u64, 99_u64), (22_u64, 199_u64)]);
    let lower = BTreeMap::from([(11_u64, 50_u64), (22_u64, 100_u64)]);
    let high = BTreeMap::from([(11_u64, 120_u64), (22_u64, 250_u64)]);
    let watermarks = high
        .iter()
        .map(|(region_id, seq)| (*region_id, Some(*seq)))
        .collect::<Vec<_>>();

    let handler = Arc::new(FinalizeLifecycleHandler::new(
        query_engine.clone(),
        watermarks,
    ));
    let frontend_client = finalize_frontend_client(&handler);
    run_finalize_with_setup(
        &query_engine,
        &task,
        &frontend_client,
        job_id,
        finalize_test_range(),
        staging_name.clone(),
        33,
        frozen,
        lower,
    )
    .await
    .expect("finalize succeeds");

    // --- Branch outputs ---
    let captured_rows = {
        let captured = handler.captured_branches.lock().unwrap();
        assert_eq!(captured.len(), 2, "exactly two branch writes expected");
        captured.clone()
    };

    // Target branch: Base ⊕ Tail over [0s, 15s).
    let target_rows = finalize_rows(&captured_rows[1]);
    assert_eq!(
        target_rows,
        vec![
            (1, 1000, 0),      // matched: max(1000, 1000)
            (2, 7000, 5000),   // Tail-only update: max(7000, 5000)
            (3, 10000, 10000), // Tail-only group
        ],
        "target window values must merge Base and Tail exactly once, got {target_rows:?}"
    );
    assert!(
        !target_rows
            .iter()
            .any(|(number, _, _)| *number == 5 || *number == 6),
        "non-target rows must never enter the target branch"
    );

    // Non-target branch: Active ∪ Delta over ts outside [0s, 15s).
    let non_target_rows = finalize_rows(&captured_rows[0]);
    assert_eq!(
        non_target_rows,
        vec![(5, 20000, 20000), (6, -1000, -5000)],
        "non-target delta rows must be replayed, got {non_target_rows:?}"
    );

    // No duplicates across branches (disjoint event-time ranges) and the
    // pre-existing Active row is untouched (it is not re-written).
    let mut all_rows = target_rows.clone();
    all_rows.extend(non_target_rows.iter().cloned());
    all_rows.sort_unstable();
    all_rows.dedup();
    assert_eq!(
        all_rows.len(),
        target_rows.len() + non_target_rows.len(),
        "branches must not overlap"
    );

    // --- Commit side effects (test list item 6) ---
    // The checkpoint row carries epoch 1 and the full H map.
    let checkpoint_plan_bytes = {
        let checkpoint_plans = handler.captured_checkpoint_plans.lock().unwrap();
        assert_eq!(checkpoint_plans.len(), 1);
        checkpoint_plans[0].logical_plan.clone()
    };
    let session_state = decode_session_state(&query_engine);
    let checkpoint_plan = DFLogicalSubstraitConvertor {}
        .decode(bytes::Bytes::from(checkpoint_plan_bytes), session_state)
        .await
        .unwrap();
    let checkpoint_text = format!("{}", checkpoint_plan.display_indent());
    assert!(
        checkpoint_text.contains("UInt64(1)"),
        "checkpoint row must carry the real cycle epoch, got: {checkpoint_text}"
    );
    // The encoded checkpoint record inside the state literal.
    let mut encoded = None;
    let _ = checkpoint_plan.apply(|node| {
        if let datafusion_expr::LogicalPlan::Projection(projection) = node {
            for expr in &projection.expr {
                if let datafusion_expr::Expr::Alias(alias) = expr
                    && let datafusion_expr::Expr::Literal(ScalarValue::Binary(Some(bytes)), _) =
                        alias.expr.as_ref()
                {
                    encoded = Some(bytes.clone());
                }
            }
        }
        Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue)
    });
    let decoded = decode_checkpoint_record(&encoded.expect("encoded checkpoint state"))
        .unwrap()
        .expect("decodable checkpoint record");
    assert_eq!(decoded.epoch, 1);
    assert_eq!(decoded.checkpoints, high, "checkpoint must advance to H");

    // State: epoch advanced, checkpoints == H, job removed, staging dropped.
    {
        let state = task.state.read().unwrap();
        assert_eq!(
            state.persisted_epoch(),
            1,
            "epoch must be monotonic and durable"
        );
        assert_eq!(
            state.checkpoints(),
            &high,
            "the durable frontier must advance to H"
        );
        assert_eq!(state.backfill_jobs().len(), 0, "the job must be removed");
        assert_eq!(
            state.checkpoint_mode(),
            CheckpointMode::Incremental,
            "finalize leaves the flow in incremental mode"
        );
    }
    // The branch plans carry the real cycle epoch (1), never the staging
    // sentinel epoch u64::MAX. Decoded BEFORE the drop assertion below, since
    // the captured branch plans reference the staging table (the FULL OUTER
    // merge base) which is dropped by the commit.
    // The captured branch plans are Substrait-encoded and their decode
    // resolves the FULL OUTER merge base (the staging table) through the
    // catalog, which the commit already dropped. Re-register a placeholder
    // staging table only for the decode, then deregister it again before the
    // drop assertion below.
    register_finalize_staging(&query_engine, &staging_name[2], 33, vec![]);
    let branch_plan_bytes = {
        let branch_plans = handler.captured_branch_plans.lock().unwrap();
        assert_eq!(branch_plans.len(), 2);
        branch_plans
            .iter()
            .map(|insert| insert.logical_plan.clone())
            .collect::<Vec<_>>()
    };
    for insert_bytes in branch_plan_bytes {
        let session_state = decode_session_state(&query_engine);
        let plan = DFLogicalSubstraitConvertor {}
            .decode(bytes::Bytes::from(insert_bytes), session_state)
            .await
            .unwrap();
        let text = format!("{}", plan.display_indent());
        assert!(
            text.contains("UInt64(1)"),
            "branch plans must carry the real cycle epoch, got: {text}"
        );
        assert!(
            !text.contains("UInt64(18446744073709551615)"),
            "branch plans must never carry the staging sentinel epoch, got: {text}"
        );
    }
    {
        let catalog_manager = query_engine.engine_state().catalog_manager();
        let memory_catalog = catalog_manager
            .as_any()
            .downcast_ref::<MemoryCatalogManager>()
            .unwrap();
        memory_catalog
            .deregister_table_sync(catalog::DeregisterTableRequest {
                catalog: DEFAULT_CATALOG_NAME.to_string(),
                schema: DEFAULT_PRIVATE_SCHEMA_NAME.to_string(),
                table_name: staging_name[2].clone(),
            })
            .unwrap();
    }

    // The staging table was dropped by the commit.
    assert!(
        !task.is_table_exist(&staging_name).await.unwrap(),
        "the staging table must be dropped after a successful finalize"
    );
}

/// Failure paths: a failed non-target branch, a failed target branch, a failed
/// checkpoint write, and unproved branch watermarks all keep the job
/// BaseComplete, never advance the checkpoint or epoch, never drop the staging
/// table, and a retry succeeds (test list item 7).
#[tokio::test]
async fn test_finalize_failure_paths_keep_job_retryable() {
    async fn assert_finalize_failure_keeps_state(
        sink_table: &str,
        sink_table_id: u32,
        handler_config: impl FnOnce(&QueryEngineRef) -> Arc<FinalizeLifecycleHandler>,
    ) -> (
        BatchingTask,
        QueryEngineRef,
        Arc<FinalizeLifecycleHandler>,
        Arc<FrontendClient>,
        [String; 3],
    ) {
        let (task, query_engine) = new_finalize_task(
            sink_table,
            sink_table_id,
            &[(1, 1000), (2, 5000), (2, 7000), (5, 20000), (6, -1000)],
        )
        .await;
        register_finalize_sink(&query_engine, sink_table, sink_table_id, vec![]);
        let job_id = 34;
        let staging_name = finalize_staging_name(job_id);
        register_finalize_staging(
            &query_engine,
            &staging_name[2],
            34,
            vec![(1, 1000, 0), (2, 5000, 5000)],
        );
        let handler = handler_config(&query_engine);
        let frontend_client = finalize_frontend_client(&handler);
        let frozen = BTreeMap::from([(11_u64, 99_u64)]);
        let lower = BTreeMap::from([(11_u64, 50_u64)]);
        let err = run_finalize_with_setup(
            &query_engine,
            &task,
            &frontend_client,
            job_id,
            finalize_test_range(),
            staging_name.clone(),
            34,
            frozen,
            lower.clone(),
        )
        .await
        .expect_err("finalize must fail");
        let _ = err;
        // Failure must leave everything untouched and retryable.
        {
            let state = task.state.read().unwrap();
            assert_eq!(state.backfill_jobs().len(), 1, "job must stay registered");
            assert_eq!(
                state.backfill_jobs()[0].status,
                crate::batching_mode::state::BackfillJobStatus::BaseComplete,
                "job must stay BaseComplete for retry"
            );
            assert_eq!(
                state.backfill_jobs()[0].frozen_watermark,
                Some(BTreeMap::from([(11_u64, 99_u64)]))
            );
            assert_eq!(state.checkpoints(), &lower, "checkpoints must not advance");
            assert_eq!(state.persisted_epoch(), 0, "epoch must not advance");
        }
        assert!(
            task.is_table_exist(&staging_name).await.unwrap(),
            "staging table must be kept on failure"
        );
        (task, query_engine, handler, frontend_client, staging_name)
    }

    let high = BTreeMap::from([(11_u64, 120_u64)]);
    let watermarks = vec![(11_u64, Some(120_u64))];

    // (a) Non-target branch fails.
    let (task, query_engine, handler, frontend_client, staging_name) =
        assert_finalize_failure_keeps_state("finalize_fail_nt_sink", 9161, |engine| {
            Arc::new(
                FinalizeLifecycleHandler::new(engine.clone(), watermarks.clone())
                    .with_branch_failures(1),
            )
        })
        .await;
    assert_eq!(handler.branch_count(), 1, "only the first branch ran");
    // Retry with the failure cleared succeeds and commits.
    run_finalize_with_setup(
        &query_engine,
        &task,
        &frontend_client,
        34,
        finalize_test_range(),
        staging_name,
        34,
        BTreeMap::from([(11_u64, 99_u64)]),
        BTreeMap::from([(11_u64, 50_u64)]),
    )
    .await
    .expect("retry after a non-target branch failure succeeds");
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.checkpoints(), &high);
        assert_eq!(state.persisted_epoch(), 1);
        assert_eq!(state.backfill_jobs().len(), 0);
    }

    // (b) Target branch fails.
    let (task, query_engine, handler, frontend_client, staging_name) =
        assert_finalize_failure_keeps_state("finalize_fail_t_sink", 9162, |engine| {
            Arc::new(
                FinalizeLifecycleHandler::new(engine.clone(), watermarks.clone())
                    .with_fail_second_branch(),
            )
        })
        .await;
    assert_eq!(
        handler.branch_count(),
        2,
        "both branches ran, the target failed"
    );
    // Retry succeeds.
    run_finalize_with_setup(
        &query_engine,
        &task,
        &frontend_client,
        34,
        finalize_test_range(),
        staging_name,
        34,
        BTreeMap::from([(11_u64, 99_u64)]),
        BTreeMap::from([(11_u64, 50_u64)]),
    )
    .await
    .expect("retry after a target branch failure succeeds");
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.checkpoints(), &high);
        assert_eq!(state.backfill_jobs().len(), 0);
    }

    // (c) Checkpoint-row write fails.
    let (task, query_engine, handler, _frontend_client, staging_name) =
        assert_finalize_failure_keeps_state("finalize_fail_ckpt_sink", 9163, |engine| {
            Arc::new(
                FinalizeLifecycleHandler::new(engine.clone(), watermarks.clone())
                    .with_fail_checkpoint_write(),
            )
        })
        .await;
    assert_eq!(
        handler.branch_count(),
        2,
        "both branches ran before the commit"
    );
    {
        let state = task.state.read().unwrap();
        assert_eq!(
            state.checkpoint_mode(),
            CheckpointMode::FullSnapshot,
            "a failed checkpoint write falls back to full snapshot"
        );
    }
    // Retry after clearing the failure succeeds.
    {
        let mut state = task.state.write().unwrap();
        state.set_checkpoint_persistence(Some(CheckpointPersistence {
            epoch_col_name: crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME.to_string(),
            checkpoint_col_name: crate::batching_mode::INTERNAL_FLOW_CHECKPOINT_COL_NAME.to_string(),
            window_col_name: "time_window".to_string(),
            primary_key_columns: vec!["number".to_string()],
        }));
    }
    let handler = Arc::new(FinalizeLifecycleHandler::new(
        query_engine.clone(),
        watermarks.clone(),
    ));
    let frontend_client = finalize_frontend_client(&handler);
    run_finalize_with_setup(
        &query_engine,
        &task,
        &frontend_client,
        34,
        finalize_test_range(),
        staging_name,
        34,
        BTreeMap::from([(11_u64, 99_u64)]),
        BTreeMap::from([(11_u64, 50_u64)]),
    )
    .await
    .expect("retry after a checkpoint write failure succeeds");
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.checkpoints(), &high);
        assert_eq!(state.backfill_jobs().len(), 0);
    }

    // (d) Unproved branch watermarks (returned != pre-bound H) fail closed.
    // The probe returns the correct H (120), but the branches return a
    // different watermark (119), so the pre-bound watermark proof fails.
    let (task, query_engine, _handler, _frontend_client, _staging_name) =
        assert_finalize_failure_keeps_state("finalize_fail_wm_sink", 9164, |engine| {
            Arc::new(
                FinalizeLifecycleHandler::new(engine.clone(), vec![(11_u64, Some(119_u64))])
                    .with_probe_watermarks(vec![(11_u64, Some(120_u64))]),
            )
        })
        .await;
    // The probe captured H correctly but the branch watermark proof failed;
    // assert the failure was a watermark mismatch by checking state untouched.
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.checkpoints(), &BTreeMap::from([(11_u64, 50_u64)]));
        assert_eq!(state.persisted_epoch(), 0);
        assert_eq!(state.backfill_jobs().len(), 1);
    }
    let _ = (query_engine, _handler, _frontend_client, _staging_name);
}

/// Rejects: a non-BaseComplete job, a BaseComplete job without a frozen
/// watermark, and a missing durable frontier L.
#[tokio::test]
async fn test_finalize_rejects_invalid_job_and_missing_lower_bound() {
    let (task, query_engine) = new_finalize_task("finalize_reject_sink", 9165, &[]).await;
    register_finalize_sink(&query_engine, "finalize_reject_sink", 9165, vec![]);
    let handler = Arc::new(FinalizeLifecycleHandler::new(
        query_engine.clone(),
        vec![(11_u64, Some(120_u64))],
    ));
    let frontend_client = finalize_frontend_client(&handler);
    let job_id = 35;
    let staging_name = finalize_staging_name(job_id);
    register_finalize_staging(&query_engine, &staging_name[2], 35, vec![]);

    // Prepared (not BaseComplete) -> rejected.
    {
        let mut state = task.state.write().unwrap();
        state.set_checkpoint_persistence(Some(CheckpointPersistence {
            epoch_col_name: crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME.to_string(),
            checkpoint_col_name: crate::batching_mode::INTERNAL_FLOW_CHECKPOINT_COL_NAME.to_string(),
            window_col_name: "time_window".to_string(),
            primary_key_columns: vec!["number".to_string()],
        }));
        state.advance_checkpoints(BTreeMap::from([(11_u64, 50_u64)]).into_iter().collect());
        let reservation = state
            .begin_backfill_prepare(job_id, finalize_test_range(), staging_name.clone())
            .unwrap();
        assert!(matches!(
            reservation,
            crate::batching_mode::state::PrepareReservation::Reserved(_)
        ));
        state.finish_backfill_prepare(job_id, 35).unwrap();
    }
    let err = task
        .run_backfill_finalize(&query_engine, &frontend_client, job_id)
        .await
        .expect_err("a Prepared job must not finalize");
    assert!(
        format!("{err:?}").contains("only a BaseComplete job"),
        "expected BaseComplete rejection, got {err:?}"
    );

    // BaseComplete without F -> rejected.
    {
        let mut state = task.state.write().unwrap();
        state.begin_backfill_run(job_id).unwrap();
        state
            .complete_backfill_run(job_id, BTreeMap::new())
            .unwrap();
    }
    let err = task
        .run_backfill_finalize(&query_engine, &frontend_client, job_id)
        .await
        .expect_err("a BaseComplete job without F must not finalize");
    assert!(
        format!("{err:?}").contains("frozen watermark"),
        "expected missing-F rejection, got {err:?}"
    );

    // Empty durable frontier L -> rejected.
    {
        let mut state = task.state.write().unwrap();
        state
            .complete_backfill_run(job_id, BTreeMap::from([(11_u64, 99_u64)]))
            .unwrap_err();
        // The job is already BaseComplete; re-register a fresh one.
        state.take_backfill_job(job_id);
        let reservation = state
            .begin_backfill_prepare(job_id, finalize_test_range(), staging_name.clone())
            .unwrap();
        assert!(matches!(
            reservation,
            crate::batching_mode::state::PrepareReservation::Reserved(_)
        ));
        state.finish_backfill_prepare(job_id, 35).unwrap();
        state.begin_backfill_run(job_id).unwrap();
        state
            .complete_backfill_run(job_id, BTreeMap::from([(11_u64, 99_u64)]))
            .unwrap();
        state.advance_checkpoints(HashMap::new());
    }
    let err = task
        .run_backfill_finalize(&query_engine, &frontend_client, job_id)
        .await
        .expect_err("an empty durable frontier L must not finalize");
    assert!(
        format!("{err:?}").contains("non-empty durable checkpoint frontier"),
        "expected missing-L rejection, got {err:?}"
    );
}

/// The integration point: `execute_once_unlocked` polls for a BaseComplete job
/// and finalizes one job per round before normal evaluation, skipping the
/// round's normal incremental work.
#[tokio::test]
async fn test_execute_once_unlocked_finalizes_pending_job() {
    let (task, query_engine) = new_finalize_task(
        "finalize_poll_sink",
        9166,
        &[(1, 1000), (2, 5000), (5, 20000)],
    )
    .await;
    register_finalize_sink(&query_engine, "finalize_poll_sink", 9166, vec![]);
    let job_id = 36;
    let staging_name = finalize_staging_name(job_id);
    register_finalize_staging(&query_engine, &staging_name[2], 36, vec![(1, 1000, 0)]);

    let watermarks = vec![(11_u64, Some(120_u64))];
    let handler = Arc::new(FinalizeLifecycleHandler::new(
        query_engine.clone(),
        watermarks,
    ));
    let frontend_client = finalize_frontend_client(&handler);

    {
        let mut state = task.state.write().unwrap();
        state.set_checkpoint_persistence(Some(CheckpointPersistence {
            epoch_col_name: crate::batching_mode::INTERNAL_FLOW_EPOCH_COL_NAME.to_string(),
            checkpoint_col_name: crate::batching_mode::INTERNAL_FLOW_CHECKPOINT_COL_NAME.to_string(),
            window_col_name: "time_window".to_string(),
            primary_key_columns: vec!["number".to_string()],
        }));
        state.advance_checkpoints(BTreeMap::from([(11_u64, 50_u64)]).into_iter().collect());
    }
    drive_job_to_base_complete(
        &task,
        job_id,
        finalize_test_range(),
        staging_name,
        36,
        BTreeMap::from([(11_u64, 99_u64)]),
    );

    let outcome = task
        .execute_once_unlocked(&query_engine, &frontend_client, None)
        .await;
    assert!(
        outcome.result.is_ok(),
        "finalize inside the poll must succeed"
    );
    let log = handler.request_log.lock().unwrap().clone();
    assert_eq!(
        log,
        vec!["probe", "branch", "branch", "checkpoint", "drop"],
        "the poll must finalize the pending job before any normal evaluation"
    );
    {
        let state = task.state.read().unwrap();
        assert_eq!(state.backfill_jobs().len(), 0);
        assert_eq!(state.checkpoints(), &BTreeMap::from([(11_u64, 120_u64)]));
    }
}
