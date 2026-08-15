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

use api::v1::flow::FlowRequestHeader;
use async_trait::async_trait;
use common_error::ext::BoxedError;
use common_function::handlers::FlowServiceHandler;
use common_meta::key::flow::FlowMetadataManagerRef;
use common_meta::node_manager::NodeManagerRef;
use common_query::error::Result;
use common_telemetry::tracing_context::TracingContext;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use session::context::QueryContextRef;
use snafu::{OptionExt, ResultExt};

use crate::utils::to_meta_query_context;

/// The operator for flow service which implements [`FlowServiceHandler`].
pub struct FlowServiceOperator {
    flow_metadata_manager: FlowMetadataManagerRef,
    node_manager: NodeManagerRef,
}

impl FlowServiceOperator {
    pub fn new(
        flow_metadata_manager: FlowMetadataManagerRef,
        node_manager: NodeManagerRef,
    ) -> Self {
        Self {
            flow_metadata_manager,
            node_manager,
        }
    }

    pub fn flow_metadata_manager(&self) -> FlowMetadataManagerRef {
        self.flow_metadata_manager.clone()
    }
}

#[async_trait]
impl FlowServiceHandler for FlowServiceOperator {
    async fn flush(
        &self,
        catalog: &str,
        flow: &str,
        ctx: QueryContextRef,
    ) -> Result<api::v1::flow::FlowResponse> {
        self.flush_inner(catalog, flow, ctx).await
    }

    async fn backfill(
        &self,
        catalog: &str,
        flow: &str,
        job_id: u64,
        start: i64,
        end: i64,
        ctx: QueryContextRef,
    ) -> Result<api::v1::flow::FlowResponse> {
        self.backfill_inner(catalog, flow, job_id, start, end, ctx)
            .await
    }

    async fn backfill_status(
        &self,
        catalog: &str,
        flow: &str,
        job_id: u64,
        ctx: QueryContextRef,
    ) -> Result<api::v1::flow::FlowResponse> {
        self.backfill_status_inner(catalog, flow, job_id, ctx).await
    }
}

impl FlowServiceOperator {
    /// Resolves the flow name to its flow id.
    async fn resolve_flow_id(&self, catalog: &str, flow: &str) -> Result<u32> {
        Ok(self
            .flow_metadata_manager
            .flow_name_manager()
            .get(catalog, flow)
            .await
            .map_err(BoxedError::new)
            .context(common_query::error::ExecuteSnafu)?
            .context(common_meta::error::FlowNotFoundSnafu {
                flow_name: format!("{}.{}", catalog, flow),
            })
            .map_err(BoxedError::new)
            .context(common_query::error::ExecuteSnafu)?
            .flow_id())
    }

    /// Resolves all flownodes that host the flow, in arbitrary order.
    async fn resolve_flownodes(
        &self,
        flow_id: u32,
    ) -> Result<Vec<common_meta::node_manager::FlownodeRef>> {
        let all_flownode_peers = self
            .flow_metadata_manager
            .flow_route_manager()
            .routes(flow_id)
            .await
            .map_err(BoxedError::new)
            .context(common_query::error::ExecuteSnafu)?;

        let all_flow_nodes = FuturesUnordered::from_iter(
            all_flownode_peers
                .iter()
                .map(|(_key, peer)| self.node_manager.flownode(peer.peer())),
        )
        .collect::<Vec<_>>()
        .await;

        Ok(all_flow_nodes)
    }

    /// Merge the responses of all flownodes, keeping the first response's
    /// header and accumulating rows/flows/extensions.
    fn merge_flownode_responses(
        final_result: &mut Option<api::v1::flow::FlowResponse>,
        res: api::v1::flow::FlowResponse,
    ) {
        if let Some(prev) = final_result {
            prev.affected_rows = res.affected_rows;
            prev.affected_flows.extend(res.affected_flows);
            prev.extensions.extend(res.extensions);
        } else {
            *final_result = Some(res);
        }
    }

    /// Dispatch a backfill request to all flownodes hosting the flow.
    async fn backfill_inner(
        &self,
        catalog: &str,
        flow: &str,
        job_id: u64,
        start: i64,
        end: i64,
        ctx: QueryContextRef,
    ) -> Result<api::v1::flow::FlowResponse> {
        let flow_id = self.resolve_flow_id(catalog, flow).await?;
        let all_flow_nodes = self.resolve_flownodes(flow_id).await?;

        let mut final_result: Option<api::v1::flow::FlowResponse> = None;
        for node in all_flow_nodes {
            let res = {
                use api::v1::flow::{BackfillFlow, FlowRequest, flow_request};
                let backfill_req = FlowRequest {
                    header: Some(FlowRequestHeader {
                        tracing_context: TracingContext::from_current_span().to_w3c(),
                        query_context: Some(to_meta_query_context(ctx.clone()).into()),
                    }),
                    body: Some(flow_request::Body::Backfill(BackfillFlow {
                        flow_id: Some(api::v1::FlowId { id: flow_id }),
                        job_id,
                        start,
                        end,
                    })),
                };
                node.handle(backfill_req)
                    .await
                    .map_err(BoxedError::new)
                    .context(common_query::error::ExecuteSnafu)?
            };

            Self::merge_flownode_responses(&mut final_result, res);
        }

        final_result.context(common_query::error::FlownodeNotFoundSnafu)
    }

    /// Dispatch a backfill status request to all flownodes hosting the flow
    /// and merge their status extensions into the response.
    async fn backfill_status_inner(
        &self,
        catalog: &str,
        flow: &str,
        job_id: u64,
        ctx: QueryContextRef,
    ) -> Result<api::v1::flow::FlowResponse> {
        let flow_id = self.resolve_flow_id(catalog, flow).await?;
        let all_flow_nodes = self.resolve_flownodes(flow_id).await?;

        let mut final_result: Option<api::v1::flow::FlowResponse> = None;
        for node in all_flow_nodes {
            let res = {
                use api::v1::flow::{BackfillStatusFlow, FlowRequest, flow_request};
                let status_req = FlowRequest {
                    header: Some(FlowRequestHeader {
                        tracing_context: TracingContext::from_current_span().to_w3c(),
                        query_context: Some(to_meta_query_context(ctx.clone()).into()),
                    }),
                    body: Some(flow_request::Body::BackfillStatus(BackfillStatusFlow {
                        flow_id: Some(api::v1::FlowId { id: flow_id }),
                        job_id,
                    })),
                };
                node.handle(status_req)
                    .await
                    .map_err(BoxedError::new)
                    .context(common_query::error::ExecuteSnafu)?
            };

            Self::merge_flownode_responses(&mut final_result, res);
        }

        final_result.context(common_query::error::FlownodeNotFoundSnafu)
    }

    /// Flush the flownodes according to the flow id.
    async fn flush_inner(
        &self,
        catalog: &str,
        flow: &str,
        ctx: QueryContextRef,
    ) -> Result<api::v1::flow::FlowResponse> {
        let flow_id = self.resolve_flow_id(catalog, flow).await?;
        let all_flow_nodes = self.resolve_flownodes(flow_id).await?;

        let mut final_result: Option<api::v1::flow::FlowResponse> = None;
        for node in all_flow_nodes {
            let res = {
                use api::v1::flow::{FlowRequest, FlushFlow, flow_request};
                let flush_req = FlowRequest {
                    header: Some(FlowRequestHeader {
                        tracing_context: TracingContext::from_current_span().to_w3c(),
                        query_context: Some(to_meta_query_context(ctx.clone()).into()),
                    }),
                    body: Some(flow_request::Body::Flush(FlushFlow {
                        flow_id: Some(api::v1::FlowId { id: flow_id }),
                    })),
                };
                node.handle(flush_req)
                    .await
                    .map_err(BoxedError::new)
                    .context(common_query::error::ExecuteSnafu)?
            };

            Self::merge_flownode_responses(&mut final_result, res);
        }

        final_result.context(common_query::error::FlownodeNotFoundSnafu)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use api::v1::flow::{
        BackfillFlow, BackfillStatusFlow, FlowRequest, FlowResponse, flow_request,
    };
    use api::v1::meta::Peer;
    use async_trait::async_trait;
    use common_meta::error::Result as MetaResult;
    use common_meta::key::flow::FlowMetadataManager;
    use common_meta::key::flow::flow_info::FlowInfoValue;
    use common_meta::key::flow::flow_route::FlowRouteValue;
    use common_meta::kv_backend::memory::MemoryKvBackend;
    use common_meta::node_manager::NodeManagerRef;
    use common_meta::test_util::{MockFlownodeHandler, MockFlownodeManager};
    use session::context::QueryContext;
    use table::table_name::TableName;

    use super::*;

    const CATALOG: &str = "my_catalog";
    const FLOW: &str = "my_flow";
    const FLOW_ID: u32 = 1;
    const JOB_ID: u64 = 7;

    /// A [`MockFlownodeHandler`] that records every received [`FlowRequest`]
    /// and answers with a fixed [`FlowResponse`].
    #[derive(Clone)]
    struct RecordingFlownodeHandler {
        requests: Arc<Mutex<Vec<FlowRequest>>>,
        response: FlowResponse,
    }

    #[async_trait]
    impl MockFlownodeHandler for RecordingFlownodeHandler {
        async fn handle(&self, _peer: &Peer, request: FlowRequest) -> MetaResult<FlowResponse> {
            self.requests.lock().unwrap().push(request);
            Ok(self.response.clone())
        }
    }

    /// Builds a [`FlowServiceOperator`] whose metadata contains
    /// `CATALOG.FLOW -> FLOW_ID` routed to `flownode_count` mock flownodes.
    async fn build_operator(
        handler: RecordingFlownodeHandler,
        flownode_count: u32,
    ) -> FlowServiceOperator {
        let kv = Arc::new(MemoryKvBackend::new());
        let flow_meta = Arc::new(FlowMetadataManager::new(kv));

        let flow_info = FlowInfoValue {
            source_table_ids: vec![1],
            all_source_table_names: vec![],
            unresolved_source_table_names: vec![],
            sink_table_name: TableName {
                catalog_name: CATALOG.to_string(),
                schema_name: "greptime".to_string(),
                table_name: "sink".to_string(),
            },
            flownode_ids: (0..flownode_count).map(|i| (i, i as u64 + 1)).collect(),
            catalog_name: CATALOG.to_string(),
            query_context: None,
            flow_name: FLOW.to_string(),
            raw_sql: "SELECT * FROM source".to_string(),
            expire_after: None,
            eval_interval_secs: None,
            comment: String::new(),
            options: HashMap::new(),
            status: Default::default(),
            created_time: chrono::Utc::now(),
            updated_time: chrono::Utc::now(),
            eval_schedule: None,
        };
        let routes = (0..flownode_count)
            .map(|i| {
                (
                    i,
                    FlowRouteValue::from(Peer::new(
                        i as u64 + 1,
                        format!("127.0.0.1:{}", 4000 + i),
                    )),
                )
            })
            .collect();
        flow_meta
            .create_flow_metadata(FLOW_ID, flow_info, routes)
            .await
            .unwrap();

        let node_manager: NodeManagerRef = Arc::new(MockFlownodeManager::new(handler));
        FlowServiceOperator::new(flow_meta, node_manager)
    }

    #[tokio::test]
    async fn test_backfill_dispatches_backfill_request() {
        let handler = RecordingFlownodeHandler {
            requests: Arc::new(Mutex::new(vec![])),
            response: FlowResponse {
                affected_flows: vec![api::v1::FlowId { id: FLOW_ID }],
                ..Default::default()
            },
        };
        let op = build_operator(handler.clone(), 1).await;

        let resp = op
            .backfill(CATALOG, FLOW, JOB_ID, 1000, 2000, QueryContext::arc())
            .await
            .unwrap();

        assert_eq!(resp.affected_flows, vec![api::v1::FlowId { id: FLOW_ID }]);

        let requests = handler.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        match requests[0].body.clone().unwrap() {
            flow_request::Body::Backfill(BackfillFlow {
                flow_id,
                job_id,
                start,
                end,
            }) => {
                assert_eq!(flow_id.unwrap().id, FLOW_ID);
                assert_eq!(job_id, JOB_ID);
                assert_eq!(start, 1000);
                assert_eq!(end, 2000);
            }
            other => panic!("unexpected flow request body: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_backfill_status_dispatches_status_request_and_merges_extensions() {
        let handler = RecordingFlownodeHandler {
            requests: Arc::new(Mutex::new(vec![])),
            response: FlowResponse {
                affected_flows: vec![api::v1::FlowId { id: FLOW_ID }],
                extensions: HashMap::from([("backfill_status".to_string(), b"Running".to_vec())]),
                ..Default::default()
            },
        };
        let op = build_operator(handler.clone(), 1).await;

        let resp = op
            .backfill_status(CATALOG, FLOW, JOB_ID, QueryContext::arc())
            .await
            .unwrap();

        assert_eq!(resp.extensions.get("backfill_status").unwrap(), b"Running");

        let requests = handler.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        match requests[0].body.clone().unwrap() {
            flow_request::Body::BackfillStatus(BackfillStatusFlow { flow_id, job_id }) => {
                assert_eq!(flow_id.unwrap().id, FLOW_ID);
                assert_eq!(job_id, JOB_ID);
            }
            other => panic!("unexpected flow request body: {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_backfill_merges_multiple_flownodes() {
        let handler = RecordingFlownodeHandler {
            requests: Arc::new(Mutex::new(vec![])),
            response: FlowResponse {
                extensions: HashMap::from([("node".to_string(), b"1".to_vec())]),
                ..Default::default()
            },
        };
        let op = build_operator(handler.clone(), 2).await;

        let resp = op
            .backfill(CATALOG, FLOW, JOB_ID, 1000, 2000, QueryContext::arc())
            .await
            .unwrap();

        assert_eq!(handler.requests.lock().unwrap().len(), 2);
        // two nodes return the same "node" extension, merged into one entry
        assert_eq!(resp.extensions.get("node").unwrap(), b"1");
    }

    #[tokio::test]
    async fn test_backfill_flow_not_found() {
        let handler = RecordingFlownodeHandler {
            requests: Arc::new(Mutex::new(vec![])),
            response: FlowResponse::default(),
        };
        let op = build_operator(handler.clone(), 1).await;

        let err = op
            .backfill(
                CATALOG,
                "no_such_flow",
                JOB_ID,
                1000,
                2000,
                QueryContext::arc(),
            )
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("Flow not found"),
            "unexpected error: {err}"
        );
        assert!(handler.requests.lock().unwrap().is_empty());
    }
}
