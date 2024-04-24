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

use hydroflow::scheduled::graph::Hydroflow;
use query::QueryEngine;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use table::metadata::TableId;
use tokio::sync::broadcast;
use tokio::task::LocalSet;

use crate::compute::DataflowState;
use crate::repr::DiffRow;
use crate::transform::{sql_to_flow_plan, DataflowContext};

pub(crate) mod error;
use error::Error;

// TODO: refactor common types for flow to a separate module
pub type TaskId = u64;

/// FlowNodeManager manages the state of all tasks in the flow node, which should be run on the same thread
///
/// The choice of timestamp is just using current system timestamp for now
pub struct FlowNodeManager<'subgraph> {
    pub task_states: BTreeMap<TaskId, ActiveDataflowState<'subgraph>>,
    pub local_set: LocalSet,
    /// broadcast sender for source table, any incoming write request will be sent to the source table's corresponding sender
    pub source_sender: BTreeMap<TableId, broadcast::Sender<DiffRow>>,
    /// broadcast receiver for sink table, there should only be one receiver, and it will receive all the data from the sink table
    ///
    /// and send it back to the client
    pub sink_receiver: BTreeMap<TableId, broadcast::Receiver<DiffRow>>,
    // catalog/tableinfo manager for query schema and translate sql to plan
    query_engine: Arc<dyn QueryEngine>,
}

/// ActiveDataflowState is a wrapper around `Hydroflow` and `DataflowState`
pub struct ActiveDataflowState<'subgraph> {
    df: Hydroflow<'subgraph>,
    state: DataflowState,
}

/// POD derive
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFullName {
    pub catalog: String,
    pub task_name: String,
}

impl<'s> FlowNodeManager<'s> {
    pub async fn create_task(
        &mut self,
        task_id: TaskId,
        create_if_not_exist: bool,
        expire_when: String,
        comment: String,
        sql: String,
        task_options: HashMap<String, String>,
    ) -> Result<(), Error> {
        let mut ctx = self.get_ctx().await?;
        let flow_plan = sql_to_flow_plan(&mut ctx, &self.query_engine, &sql).await?;
        todo!()
    }

    /// Get the newest Dataflow context which including table schema and mapping from table name to dataflow id
    pub async fn get_ctx(&mut self) -> Result<DataflowContext, Error> {
        todo!()
    }
}
