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

use std::collections::BTreeMap;

use hydroflow::scheduled::graph::Hydroflow;
use tokio::task::LocalSet;

use crate::compute::DataflowState;

pub(crate) mod error;

pub type TaskId = u64;

/// FlowNodeManager manages the state of all tasks in the flow node, which should be run on the same thread
pub struct FlowNodeManager<'subgraph> {
    pub task_states: BTreeMap<TaskId, ActiveDataflowState<'subgraph>>,
    pub local_set: LocalSet,
}

/// ActiveDataflowState is a wrapper around `Hydroflow` and `DataflowState`
pub struct ActiveDataflowState<'subgraph> {
    df: Hydroflow<'subgraph>,
    state: DataflowState,
}
