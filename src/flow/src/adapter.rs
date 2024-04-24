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

//! for getting data from source and sending results to sink
//! and communicating with other parts of the database

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use common_meta::key::table_info::TableInfoManager;
use common_meta::key::table_name::TableNameManager;
use hydroflow::scheduled::graph::Hydroflow;
use query::QueryEngine;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use table::metadata::TableId;
use tokio::sync::broadcast;
use tokio::task::LocalSet;

use crate::compute::DataflowState;
use crate::expr::GlobalId;
use crate::plan::Plan;
use crate::repr::DiffRow;
use crate::transform::{sql_to_flow_plan, FlowNodeContext};

pub(crate) mod error;
use error::Error;

// TODO: refactor common types for flow to a separate module
pub type TaskId = u64;
pub type TableName = Vec<String>;

/// FlowNodeManager manages the state of all tasks in the flow node, which should be run on the same thread
///
/// The choice of timestamp is just using current system timestamp for now
pub struct FlowNodeManager<'subgraph> {
    pub task_states: BTreeMap<TaskId, ActiveDataflowState<'subgraph>>,
    pub local_set: LocalSet,
    /// broadcast sender for source table, any incoming write request will be sent to the source table's corresponding sender
    ///
    /// Note that we are getting insert requests with table id, so we should use table id as the key
    pub source_sender: BTreeMap<TableId, broadcast::Sender<DiffRow>>,
    /// broadcast receiver for sink table, there should only be one receiver, and it will receive all the data from the sink table
    ///
    /// and send it back to the client, since we are mocking the sink table as a client, we should use table name as the key
    pub sink_receiver: BTreeMap<TableName, broadcast::Receiver<DiffRow>>,
    // TODO: catalog/tableinfo manager for query schema and translate sql to plan
    query_engine: Arc<dyn QueryEngine>,
    /// contains mapping from table name to global id, and table schema
    flownode_context: FlowNodeContext,
}

/// mapping of table name <-> table id should be query from tableinfo manager
struct TableNameIdMapping {
    // for query `TableId -> TableName` mapping
    table_info_manager: TableInfoManager,
    // for query `TableName -> TableId` mapping
    table_name_manager: TableNameManager,
    // a in memory cache, will be invalid if necessary
}

impl TableNameIdMapping {
    pub async fn get_table_id(&self, table_name: TableName) -> Result<TableId, Error> {
        todo!()
    }

    pub async fn get_table_name(&self, table_id: TableId) -> Result<TableName, Error> {
        todo!()
    }
}

/// ActiveDataflowState is a wrapper around `Hydroflow` and `DataflowState`
pub(crate) struct ActiveDataflowState<'subgraph> {
    df: Hydroflow<'subgraph>,
    state: DataflowState,
}

impl<'s> FlowNodeManager<'s> {
    /// Return task id if a new task is created, otherwise return None
    ///
    /// steps to create task:
    /// 1. parse query into typed plan(and optional parse expire_when expr)
    /// 2. render source/sink with output table id and used input table id
    ///
    /// TODO(discord9): use greptime-proto type to create task instead
    #[allow(clippy::too_many_arguments)]
    pub async fn create_task(
        &mut self,
        task_id: TaskId,
        output_table_id: TableId,
        create_if_not_exist: bool,
        expire_when: Option<String>,
        comment: Option<String>,
        sql: String,
        task_options: HashMap<String, String>,
    ) -> Result<Option<TaskId>, Error> {
        if create_if_not_exist {
            // check if the task already exists
            if self.task_states.contains_key(&task_id) {
                return Ok(None);
            }
        }

        // construct a active dataflow state with it
        let flow_plan =
            sql_to_flow_plan(&mut self.flownode_context, &self.query_engine, &sql).await?;
        let used = flow_plan.plan.find_used_collection();

        todo!()
    }

    pub async fn get_table_id(&self, table_name: TableName) -> Result<TableId, Error> {
        todo!()
    }
}
