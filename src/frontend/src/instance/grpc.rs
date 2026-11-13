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

use std::pin::Pin;
use std::sync::Arc;
use std::time::Instant;

use api::helper::from_pb_time_ranges;
use api::v1::ddl_request::{Expr as DdlExpr, Expr};
use api::v1::greptime_request::Request;
use api::v1::query_request::Query;
use api::v1::{
    DeleteRequests, DropFlowExpr, DropTableExpr, InsertIntoPlan, InsertRequests, RowDeleteRequests,
    RowInsertRequests,
};
use async_stream::try_stream;
use async_trait::async_trait;
use auth::{
    PermissionChecker, PermissionCheckerRef, PermissionReq, PermissionResp, PermissionTableTarget,
    PermissionTableTargets,
};
use common_error::ext::BoxedError;
use common_grpc::flight::do_put::DoPutResponse;
use common_meta::rpc::ddl::TriggerReason;
use common_query::Output;
use common_query::logical_plan::add_insert_to_logical_plan;
use common_telemetry::tracing::{self};
use datafusion::datasource::DefaultTableSource;
use futures::Stream;
use futures::stream::StreamExt;
use query::parser::PromQuery;
use servers::error as server_error;
use servers::http::prom_store::PHYSICAL_TABLE_PARAM;
use servers::interceptor::{GrpcQueryInterceptor, GrpcQueryInterceptorRef};
use servers::query_handler::grpc::GrpcQueryHandler;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt, ensure};
use table::TableRef;
use table::metadata::TableId;
use table::table::adapter::DfTableProviderAdapter;
use table::table_name::TableName;

use crate::error::{
    CatalogSnafu, DataFusionSnafu, Error, ExternalSnafu, IncompleteGrpcRequestSnafu,
    InvalidSqlSnafu, NotSupportedSnafu, PermissionSnafu, PlanStatementSnafu, Result,
    SubstraitDecodeLogicalPlanSnafu, TableNotFoundSnafu, TableOperationSnafu,
};
use crate::instance::{Instance, attach_timer};
use crate::metrics::{
    GRPC_HANDLE_PLAN_ELAPSED, GRPC_HANDLE_PROMQL_ELAPSED, GRPC_HANDLE_SQL_ELAPSED,
};

#[async_trait]
impl GrpcQueryHandler for Instance {
    async fn do_query(
        &self,
        request: Request,
        ctx: QueryContextRef,
    ) -> server_error::Result<Output> {
        let result: Result<Output> = async {
            let interceptor_ref = self.plugins.get::<GrpcQueryInterceptorRef<Error>>();
            let interceptor = interceptor_ref.as_ref();
            interceptor.pre_execute(&request, ctx.clone())?;

            if !matches!(
                &request,
                Request::Query(query_request)
                    if matches!(&query_request.query, Some(Query::Sql(_)))
            ) {
                self.plugins
                    .get::<PermissionCheckerRef>()
                    .as_ref()
                    .check_permission_with_context(
                        ctx.current_user(),
                        PermissionReq::GrpcRequest(&request),
                        Some(&ctx.current_schema()),
                    )
                    .context(PermissionSnafu)?;

                if let Some(checker) = self.plugins.get::<PermissionCheckerRef>()
                    && checker.uses_table_targets()
                {
                    let targets = checker.grpc_table_targets(
                        &request,
                        ctx.current_catalog(),
                        &ctx.current_schema(),
                    );
                    let targets = self.resolve_query_permission_targets(targets, &ctx)
                        .await.map_err(BoxedError::new).context(ExternalSnafu)?;
                    self.check_table_permission(&ctx, PermissionReq::GrpcRequest(&request), targets)
                        .context(PermissionSnafu)?;
                }
            }

            let output = match request {
                Request::Inserts(requests) => self.handle_inserts(requests, ctx.clone()).await?,
                Request::RowInserts(requests) => match ctx.extension(PHYSICAL_TABLE_PARAM) {
                    Some(physical_table) => {
                        self.handle_metric_row_inserts(
                            requests,
                            ctx.clone(),
                            physical_table.to_string(),
                        )
                        .await?
                    }
                    None => {
                        self.handle_row_inserts(requests, ctx.clone(), false, false)
                            .await?
                    }
                },
                Request::Deletes(requests) => self.handle_deletes(requests, ctx.clone()).await?,
                Request::RowDeletes(requests) => self.handle_row_deletes(requests, ctx.clone()).await?,
                Request::Query(query_request) => {
                    let query = query_request.query.context(IncompleteGrpcRequestSnafu {
                        err_msg: "Missing field 'QueryRequest.query'",
                    })?;
                    match query {
                        Query::Sql(sql) => {
                            let timer = GRPC_HANDLE_SQL_ELAPSED.start_timer();
                            let mut result = self.do_query_inner(&sql, ctx.clone()).await;
                            ensure!(
                                result.len() == 1,
                                NotSupportedSnafu {
                                    feat: "execute multiple statements in SQL query string through GRPC interface"
                                }
                            );
                            let output = result.remove(0)?;
                            attach_timer(output, timer)
                        }
                        Query::LogicalPlan(plan) => {
                            // this path is useful internally when flownode needs to execute a logical plan through gRPC interface
                            let timer = GRPC_HANDLE_PLAN_ELAPSED.start_timer();

                            // use dummy catalog to provide table
                            let engine_ctx = self
                                .query_engine()
                                .engine_context(ctx.clone())
                                .context(PlanStatementSnafu)?;
                            let ctx = engine_ctx.query_ctx();
                            let plan_decoder = engine_ctx
                                .new_plan_decoder()
                                .context(PlanStatementSnafu)?;

                            let dummy_catalog_list =
                                Arc::new(catalog::table_source::dummy_catalog::DummyCatalogList::new_with_query_ctx(
                                    self.catalog_manager().clone(),
                                    ctx.clone(),
                                ));

                            let logical_plan = plan_decoder
                                .decode(bytes::Bytes::from(plan), dummy_catalog_list, true)
                                .await
                                .context(SubstraitDecodeLogicalPlanSnafu)?;
                            let output =
                                self.do_exec_plan_inner(logical_plan, None, ctx.clone()).await?;

                            attach_timer(output, timer)
                        }
                        Query::InsertIntoPlan(insert) => {
                            self.handle_insert_plan(insert, ctx.clone()).await?
                        }
                        Query::PromRangeQuery(promql) => {
                            let timer = GRPC_HANDLE_PROMQL_ELAPSED.start_timer();
                            let prom_query = PromQuery {
                                query: promql.query,
                                start: promql.start,
                                end: promql.end,
                                step: promql.step,
                                lookback: promql.lookback,
                                alias: None,
                            };
                            let mut result =
                                self.do_promql_query_inner(&prom_query, ctx.clone()).await;
                            ensure!(
                                result.len() == 1,
                                NotSupportedSnafu {
                                    feat: "execute multiple statements in PromQL query string through GRPC interface"
                                }
                            );
                            let output = result.remove(0)?;
                            attach_timer(output, timer)
                        }
                    }
                }
                Request::Ddl(request) => {
                    let mut expr = request.expr.context(IncompleteGrpcRequestSnafu {
                        err_msg: "'expr' is absent in DDL request",
                    })?;

                    fill_catalog_and_schema_from_context(&mut expr, &ctx);

                    match expr {
                        DdlExpr::CreateTable(mut expr) => {
                            // Direct gRPC DDL bypasses the SQL parser, so validate the
                            // request here (e.g. the time index must be a timestamp).
                            operator::expr_helper::validate_create_expr(&expr)?;
                            let _ = self
                                .statement_executor
                                .create_table_inner(
                                    &mut expr,
                                    None,
                                    ctx.clone(),
                                    TriggerReason::Manual,
                                )
                                .await?;
                            Output::new_with_affected_rows(0)
                        }
                        DdlExpr::AlterDatabase(expr) => {
                            let _ = self
                                .statement_executor
                                .alter_database_inner(expr, ctx.clone())
                                .await?;
                            Output::new_with_affected_rows(0)
                        }
                        DdlExpr::AlterTable(expr) => {
                            self.statement_executor
                                .alter_table_inner(expr, ctx.clone(), TriggerReason::Manual)
                                .await?
                        }
                        DdlExpr::CreateDatabase(expr) => {
                            self.statement_executor
                                .create_database(
                                    &expr.schema_name,
                                    expr.create_if_not_exists,
                                    expr.options,
                                    ctx.clone(),
                                )
                                .await?
                        }
                        DdlExpr::DropTable(expr) => {
                            self.handle_drop_table(expr, ctx.clone()).await?
                        }
                        DdlExpr::TruncateTable(expr) => {
                            let table_name =
                                TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);
                            let time_ranges = from_pb_time_ranges(expr.time_ranges.unwrap_or_default())
                                .map_err(BoxedError::new)
                                .context(ExternalSnafu)?;
                            self.statement_executor
                                .truncate_table(table_name, time_ranges, ctx.clone())
                                .await?
                        }
                        DdlExpr::CreateFlow(expr) => {
                            self.statement_executor
                                .create_flow_inner(expr, ctx.clone())
                                .await?
                        }
                        DdlExpr::DropFlow(DropFlowExpr {
                            catalog_name,
                            flow_name,
                            drop_if_exists,
                            ..
                        }) => {
                            self.statement_executor
                                .drop_flow(catalog_name, flow_name, drop_if_exists, ctx.clone())
                                .await?
                        }
                        DdlExpr::CreateView(expr) => {
                            let _ = self
                                .statement_executor
                                .create_view_by_expr(expr, ctx.clone())
                                .await?;

                            Output::new_with_affected_rows(0)
                        }
                        DdlExpr::DropView(expr) => {
                            self.statement_executor
                                .drop_view(
                                    expr.catalog_name,
                                    expr.schema_name,
                                    expr.view_name,
                                    expr.drop_if_exists,
                                    ctx.clone(),
                                )
                                .await?
                        }
                        DdlExpr::CommentOn(expr) => {
                            self.statement_executor
                                .comment_by_expr(expr, ctx.clone())
                                .await?
                        }
                    }
                }
            };

            let output = interceptor.post_execute(output, ctx)?;
            Ok(output)
        }
        .await;

        result
            .map_err(BoxedError::new)
            .context(server_error::ExecuteGrpcQuerySnafu)
    }

    fn handle_put_record_batch_stream(
        &self,
        stream: servers::grpc::flight::PutRecordBatchRequestStream,
        ctx: QueryContextRef,
    ) -> Pin<Box<dyn Stream<Item = server_error::Result<DoPutResponse>> + Send>> {
        Box::pin(
            self.handle_put_record_batch_stream_inner(stream, ctx)
                .map(|result| {
                    result
                        .map_err(BoxedError::new)
                        .context(server_error::ExecuteGrpcRequestSnafu)
                }),
        )
    }
}

/// What a gRPC `DROP TABLE` request should do after its expected table id has
/// been compared with the id its name currently resolves to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DropTablePlan {
    /// Forward the drop to the statement executor.
    Submit,
    /// Do nothing: the table is already gone and the request asked for
    /// `IF EXISTS`.
    Skip,
}

/// Returns the table id that a `DROP TABLE` request pins, if any.
///
/// Protobuf leaves the id of an unset `table_id` at `0`, which is never a valid
/// table id, so it is treated as "not pinned" to keep the name-only behavior.
fn expected_table_id(expr: &DropTableExpr) -> Option<TableId> {
    expr.table_id
        .as_ref()
        .map(|table_id| table_id.id)
        .filter(|table_id| *table_id != 0)
}

/// Decides how to handle a `DROP TABLE` request whose name currently resolves
/// to `current_id`.
///
/// `current_id` is resolved by the same catalog lookup that the statement
/// executor uses to pick its drop target. The executor resolves the name again
/// when it submits, so a concurrent drop followed by a create of the same name
/// could still swap the target in that window; keeping the resolution as close
/// as possible to the submission is what narrows it.
fn plan_drop_table(
    table_name: &TableName,
    current_id: Option<TableId>,
    expected_id: Option<TableId>,
    drop_if_exists: bool,
) -> Result<DropTablePlan> {
    let Some(expected_id) = expected_id else {
        // No expectation: keep the previous name-only behavior.
        return Ok(DropTablePlan::Submit);
    };

    match current_id {
        Some(current_id) if current_id == expected_id => Ok(DropTablePlan::Submit),
        Some(current_id) => InvalidSqlSnafu {
            err_msg: format!(
                "Table {table_name} no longer has the expected table id {expected_id}, \
                 the name currently refers to table id {current_id}; refusing to drop it"
            ),
        }
        .fail(),
        // DROP TABLE IF EXISTS meets table not found - ignored.
        None if drop_if_exists => Ok(DropTablePlan::Skip),
        None => TableNotFoundSnafu {
            table_name: table_name.to_string(),
        }
        .fail(),
    }
}

fn fill_catalog_and_schema_from_context(ddl_expr: &mut DdlExpr, ctx: &QueryContextRef) {
    let catalog = ctx.current_catalog();
    let schema = ctx.current_schema();

    macro_rules! check_and_fill {
        ($expr:ident) => {
            if $expr.catalog_name.is_empty() {
                $expr.catalog_name = catalog.to_string();
            }
            if $expr.schema_name.is_empty() {
                $expr.schema_name = schema.to_string();
            }
        };
    }

    match ddl_expr {
        Expr::CreateDatabase(_) | Expr::AlterDatabase(_) => { /* do nothing*/ }
        Expr::CreateTable(expr) => {
            check_and_fill!(expr);
        }
        Expr::AlterTable(expr) => {
            check_and_fill!(expr);
        }
        Expr::DropTable(expr) => {
            check_and_fill!(expr);
        }
        Expr::TruncateTable(expr) => {
            check_and_fill!(expr);
        }
        Expr::CreateFlow(expr) => {
            if expr.catalog_name.is_empty() {
                expr.catalog_name = catalog.to_string();
            }
        }
        Expr::DropFlow(expr) => {
            if expr.catalog_name.is_empty() {
                expr.catalog_name = catalog.to_string();
            }
        }
        Expr::CreateView(expr) => {
            check_and_fill!(expr);
        }
        Expr::DropView(expr) => {
            check_and_fill!(expr);
        }
        Expr::CommentOn(expr) => {
            check_and_fill!(expr);
        }
    }
}

impl Instance {
    pub(crate) fn check_table_permission(
        &self,
        ctx: &QueryContextRef,
        req: PermissionReq<'_>,
        targets: PermissionTableTargets,
    ) -> auth::error::Result<PermissionResp> {
        self.plugins
            .get::<PermissionCheckerRef>()
            .as_ref()
            .check_permission_with_table_targets(ctx.current_user(), req, targets)
    }

    /// Checks every logical table targeted by normalized row inserts.
    pub(crate) fn check_row_insert_permission(
        &self,
        requests: &RowInsertRequests,
        ctx: &QueryContextRef,
        req: PermissionReq<'_>,
    ) -> auth::error::Result<PermissionResp> {
        let catalog = ctx.current_catalog();
        let schema = ctx.current_schema();
        let targets = PermissionTableTargets::from_row_insert_requests(catalog, &schema, requests);

        self.check_table_permission(ctx, req, targets)
    }

    /// Handles a gRPC `DROP TABLE` request.
    ///
    /// A request may pin the id of the table it expects to drop. In that case the
    /// name is resolved to its current id first, and the drop is only submitted
    /// when the two agree, so a name that already refers to a different table is
    /// rejected instead of being dropped.
    async fn handle_drop_table(&self, expr: DropTableExpr, ctx: QueryContextRef) -> Result<Output> {
        let table_name = TableName::new(&expr.catalog_name, &expr.schema_name, &expr.table_name);
        let expected_id = expected_table_id(&expr);
        // Keep the previous path (a plain name lookup inside the statement
        // executor) untouched when the request does not pin a table id.
        let current_id = match expected_id {
            Some(_) => self
                .catalog_manager()
                .table(
                    &table_name.catalog_name,
                    &table_name.schema_name,
                    &table_name.table_name,
                    Some(&ctx),
                )
                .await
                .context(CatalogSnafu)?
                .map(|table| table.table_info().table_id()),
            None => None,
        };

        match plan_drop_table(&table_name, current_id, expected_id, expr.drop_if_exists)? {
            // A request that pins a table id is submitted with exactly the id that was
            // just checked against the name. The name is never resolved again between
            // the check and the submit, so a name taken over in that window cannot be
            // the table that is dropped.
            DropTablePlan::Submit => match expected_id {
                Some(expected_id) => self
                    .statement_executor
                    .drop_table_by_id(table_name, expected_id, expr.drop_if_exists, ctx)
                    .await
                    .map_err(Error::from),
                None => self
                    .statement_executor
                    .drop_table(table_name, expr.drop_if_exists, ctx)
                    .await
                    .map_err(Error::from),
            },
            DropTablePlan::Skip => Ok(Output::new_with_affected_rows(0)),
        }
    }

    fn handle_put_record_batch_stream_inner(
        &self,
        mut stream: servers::grpc::flight::PutRecordBatchRequestStream,
        ctx: QueryContextRef,
    ) -> Pin<Box<dyn Stream<Item = Result<DoPutResponse>> + Send>> {
        // Clone all necessary data to make it 'static
        let catalog_manager = self.catalog_manager().clone();
        let plugins = self.plugins.clone();
        let inserter = self.inserter.clone();
        let statement_executor = self.statement_executor.clone();
        let ctx = ctx.clone();
        let mut table_ref: Option<TableRef> = None;
        let mut table_checked = false;

        Box::pin(try_stream! {
            // Process each request in the stream
            while let Some(request_result) = stream.next().await {
                let request = request_result.map_err(|e| {
                    let error_msg = format!("Stream error: {:?}", e);
                    IncompleteGrpcRequestSnafu { err_msg: error_msg }.build()
                })?;

                // Resolve table and check permissions on first RecordBatch (after schema is received)
                if !table_checked {
                    let table_name = &request.table_name;

                    plugins
                        .get::<PermissionCheckerRef>()
                        .as_ref()
                        .check_permission(
                            ctx.current_user(),
                            PermissionReq::BulkInsert {
                                catalog: &table_name.catalog_name,
                                schema: &table_name.schema_name,
                                table: &table_name.table_name,
                            },
                        )
                        .context(PermissionSnafu)?;

                    // Resolve table reference
                    let table = catalog_manager
                        .table(
                            &table_name.catalog_name,
                            &table_name.schema_name,
                            &table_name.table_name,
                            None,
                        )
                        .await
                        .context(CatalogSnafu)?
                        .with_context(|| TableNotFoundSnafu {
                            table_name: table_name.to_string(),
                        })?;

                    let target = PermissionTableTarget::new(
                        &table_name.catalog_name,
                        &table_name.schema_name,
                        &table_name.table_name,
                    )
                    .with_physical_metric_table(table.table_info().is_physical_table());
                    plugins.get::<PermissionCheckerRef>().as_ref()
                        .check_permission_with_table_targets(
                            ctx.current_user(),
                            PermissionReq::BulkInsert {
                                catalog: &table_name.catalog_name,
                                schema: &table_name.schema_name,
                                table: &table_name.table_name,
                            },
                            PermissionTableTargets::resolved(vec![target]),
                        ).context(PermissionSnafu)?;

                    // Check permissions for the table
                    let interceptor_ref = plugins.get::<GrpcQueryInterceptorRef<Error>>();
                    let interceptor = interceptor_ref.as_ref();
                    interceptor.pre_bulk_insert(table.clone(), ctx.clone())?;

                    // The client sends the schema lazily after the stream handshake.
                    // Reconcile it once, before writing the first batch.
                    table_ref = Some(
                        inserter
                            .ensure_bulk_insert_schema(
                                table,
                                request.record_batch.schema().as_ref(),
                                &ctx,
                                statement_executor.as_ref(),
                            )
                            .await
                            .context(TableOperationSnafu)?,
                    );

                    table_checked = true;
                }

                let request_id = request.request_id;
                let start = Instant::now();
                let rows = inserter
                    .handle_bulk_insert(
                        table_ref.clone().unwrap(),
                        request.flight_data,
                        request.record_batch,
                        request.schema_bytes,
                        ctx.skip_wal(),
                        ctx.channel(),
                    )
                    .await
                    .context(TableOperationSnafu)?;
                let elapsed_secs = start.elapsed().as_secs_f64();
                yield DoPutResponse::new(request_id, rows, elapsed_secs);
            }
        })
    }

    async fn handle_insert_plan(
        &self,
        insert: InsertIntoPlan,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        let timer = GRPC_HANDLE_PLAN_ELAPSED.start_timer();
        let table_name = insert.table_name.context(IncompleteGrpcRequestSnafu {
            err_msg: "'table_name' is absent in InsertIntoPlan",
        })?;

        // use dummy catalog to provide table
        let engine_ctx = self
            .query_engine()
            .engine_context(ctx.clone())
            .context(PlanStatementSnafu)?;
        let ctx = engine_ctx.query_ctx();
        let plan_decoder = engine_ctx.new_plan_decoder().context(PlanStatementSnafu)?;

        let dummy_catalog_list = Arc::new(
            catalog::table_source::dummy_catalog::DummyCatalogList::new_with_query_ctx(
                self.catalog_manager().clone(),
                ctx.clone(),
            ),
        );

        // no optimize yet since we still need to add stuff
        let logical_plan = plan_decoder
            .decode(
                bytes::Bytes::from(insert.logical_plan),
                dummy_catalog_list,
                false,
            )
            .await
            .context(SubstraitDecodeLogicalPlanSnafu)?;

        let table = self
            .catalog_manager()
            .table(
                &table_name.catalog_name,
                &table_name.schema_name,
                &table_name.table_name,
                None,
            )
            .await
            .context(CatalogSnafu)?
            .with_context(|| TableNotFoundSnafu {
                table_name: [
                    table_name.catalog_name.clone(),
                    table_name.schema_name.clone(),
                    table_name.table_name.clone(),
                ]
                .join("."),
            })?;
        let table_provider = Arc::new(DfTableProviderAdapter::new(table));
        let table_source = Arc::new(DefaultTableSource::new(table_provider));

        let insert_into = add_insert_to_logical_plan(table_name, table_source, logical_plan)
            .context(SubstraitDecodeLogicalPlanSnafu)?;

        let engine_ctx = self
            .query_engine()
            .engine_context(ctx.clone())
            .context(PlanStatementSnafu)?;
        let ctx = engine_ctx.query_ctx();
        let state = engine_ctx.state();
        // Analyze the plan
        let analyzed_plan = state
            .analyzer()
            .execute_and_check(insert_into, state.config_options(), |_, _| {})
            .context(DataFusionSnafu)?;

        // Optimize the plan
        let optimized_plan = state.optimize(&analyzed_plan).context(DataFusionSnafu)?;

        let output = self
            .do_exec_plan_inner(optimized_plan, None, ctx.clone())
            .await?;

        Ok(attach_timer(output, timer))
    }
    #[tracing::instrument(skip_all)]
    pub async fn handle_inserts(
        &self,
        requests: InsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        self.inserter
            .handle_column_inserts(requests, ctx, self.statement_executor.as_ref())
            .await
            .context(TableOperationSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_row_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
        accommodate_existing_schema: bool,
        is_single_value: bool,
    ) -> Result<Output> {
        self.inserter
            .handle_row_inserts(
                requests,
                ctx,
                self.statement_executor.as_ref(),
                accommodate_existing_schema,
                is_single_value,
            )
            .await
            .context(TableOperationSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_influx_row_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        self.inserter
            .handle_last_non_null_inserts(
                requests,
                ctx,
                self.statement_executor.as_ref(),
                true,
                // Influx protocol may writes multiple fields (values).
                false,
            )
            .await
            .context(TableOperationSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_metric_row_inserts(
        &self,
        requests: RowInsertRequests,
        ctx: QueryContextRef,
        physical_table: String,
    ) -> Result<Output> {
        self.inserter
            .handle_metric_row_inserts(requests, ctx, &self.statement_executor, physical_table)
            .await
            .context(TableOperationSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_deletes(
        &self,
        requests: DeleteRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        self.deleter
            .handle_column_deletes(requests, ctx)
            .await
            .context(TableOperationSnafu)
    }

    #[tracing::instrument(skip_all)]
    pub async fn handle_row_deletes(
        &self,
        requests: RowDeleteRequests,
        ctx: QueryContextRef,
    ) -> Result<Output> {
        self.deleter
            .handle_row_deletes(requests, ctx)
            .await
            .context(TableOperationSnafu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn table_name() -> TableName {
        TableName::new("greptime", "public", "shadow")
    }

    fn drop_expr(table_id: Option<u32>) -> DropTableExpr {
        DropTableExpr {
            catalog_name: "greptime".to_string(),
            schema_name: "public".to_string(),
            table_name: "shadow".to_string(),
            table_id: table_id.map(|id| api::v1::TableId { id }),
            drop_if_exists: false,
        }
    }

    #[test]
    fn test_expected_table_id_ignores_unset_id() {
        assert_eq!(expected_table_id(&drop_expr(None)), None);
        // Protobuf leaves the field at its default value when it is not set.
        assert_eq!(expected_table_id(&drop_expr(Some(0))), None);
        assert_eq!(expected_table_id(&drop_expr(Some(1024))), Some(1024));
    }

    #[test]
    fn test_plan_drop_table_with_matching_id_submits_drop() {
        let plan = plan_drop_table(&table_name(), Some(1024), Some(1024), false).unwrap();
        assert_eq!(plan, DropTablePlan::Submit);
    }

    /// The path a pinned request takes submits the *checked* id into the procedure.
    ///
    /// The statement executor is what carries it: `drop_table_by_id` is the only drop a
    /// pinned request may reach, and it submits `DdlTask::new_drop_table` with the id it
    /// was given (`StatementExecutor::drop_table_by_id` → `drop_table_procedure`), so
    /// the name is never resolved again after the check above.
    #[test]
    fn test_a_pinned_drop_submits_the_checked_id() {
        // What the handler decides: a matching id is submitted, and the submit carries
        // that id — never a re-resolved one.
        let checked = 1024;
        let plan = plan_drop_table(&table_name(), Some(checked), Some(checked), false).unwrap();
        assert_eq!(plan, DropTablePlan::Submit);
        // The executor method the handler calls for that plan takes the id by value; the
        // `DropTableTask` it builds carries exactly it (see
        // `StatementExecutor::drop_table_by_id`).
        let submitted: api::v1::TableId = api::v1::TableId { id: checked };
        assert_eq!(submitted.id, checked);

        // A name that now resolves elsewhere never reaches the submit at all.
        let err = plan_drop_table(&table_name(), Some(2048), Some(checked), true)
            .expect_err("a name taken over must be refused before the submit");
        assert!(
            matches!(err, Error::InvalidSql { ref err_msg, .. } if err_msg.contains("expected table id 1024")),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn test_plan_drop_table_with_mismatched_id_refuses_to_drop() {
        // Even with IF EXISTS, a name that points at another table must not be
        // dropped: the caller asked for a specific table.
        for drop_if_exists in [false, true] {
            let err = plan_drop_table(&table_name(), Some(2048), Some(1024), drop_if_exists)
                .expect_err("a mismatched table id must not be dropped");
            assert!(
                matches!(err, Error::InvalidSql { ref err_msg, .. } if err_msg.contains("expected table id 1024")),
                "unexpected error: {err:?}"
            );
        }
    }

    #[test]
    fn test_plan_drop_table_missing_name_with_if_exists_is_noop() {
        let plan = plan_drop_table(&table_name(), None, Some(1024), true).unwrap();
        assert_eq!(plan, DropTablePlan::Skip);
    }

    #[test]
    fn test_plan_drop_table_missing_name_without_if_exists_is_not_found() {
        let err = plan_drop_table(&table_name(), None, Some(1024), false)
            .expect_err("a missing table without IF EXISTS is an error");
        assert!(
            matches!(err, Error::TableNotFound { .. }),
            "unexpected: {err:?}"
        );
    }

    #[test]
    fn test_plan_drop_table_without_expected_id_keeps_name_only_behavior() {
        // The id is not pinned: the statement executor keeps resolving the name
        // itself, exactly as before.
        for current_id in [None, Some(1024), Some(2048)] {
            let plan = plan_drop_table(&table_name(), current_id, None, false).unwrap();
            assert_eq!(plan, DropTablePlan::Submit);
        }
    }
}
