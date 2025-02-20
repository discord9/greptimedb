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

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use common_time::Timestamp;
use datafusion_common::tree_node::TreeNode;
use datatypes::value::Value;
use query::QueryEngineRef;
use session::context::QueryContextRef;
use snafu::ResultExt;

use super::frontend_client::FrontendClient;
use super::{df_plan_to_sql, AddFilterRewriter};
use crate::adapter::FlowId;
use crate::error::DatafusionSnafu;
use crate::recording_rules::{find_plan_time_window_lower_bound, sql_to_df_plan};
use crate::Error;

/// TODO(discord9): determine how to configure refresh rate
#[derive(Clone)]
pub struct RecordingRuleEngine {
    rules: BTreeMap<FlowId, RecordingRuleTask>,
    states: BTreeMap<FlowId, RecordingRuleState>,
    frontend_client: Arc<FrontendClient>,
    engine: QueryEngineRef,
}

impl RecordingRuleEngine {}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct RecordingRuleTask {
    query: String,
    /// in millisecond
    expire_after: Option<u64>,
    sink_table_name: [String; 3],
}

impl RecordingRuleTask {
    async fn gen_query_with_time_window(
        &self,
        engine: QueryEngineRef,
        query_ctx: QueryContextRef,
    ) -> Result<String, Error> {
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");
        let low_bound = self
            .expire_after
            .map(|e| since_the_epoch.as_millis() - e as u128);

        let Some(low_bound) = low_bound else {
            return Ok(self.query.clone());
        };

        let low_bound = Timestamp::new_millisecond(low_bound as i64);

        let plan = sql_to_df_plan(query_ctx.clone(), engine.clone(), &self.query, true).await?;

        let Some((col_name, value)) =
            find_plan_time_window_lower_bound(&plan, low_bound, query_ctx.clone(), engine.clone())
                .await?
        else {
            return Ok(self.query.clone());
        };

        let new_sql = {
            let value = Value::from(value);
            let value = value.try_to_scalar_value(&value.data_type()).unwrap();
            let expr = {
                use datafusion_expr::{col, lit};
                col(col_name).gt(lit(value))
            };

            let mut add_filter = AddFilterRewriter::new(expr);
            // make a not optimized plan for clearer unparse
            let plan =
                sql_to_df_plan(query_ctx.clone(), engine.clone(), &self.query, false).await?;
            let plan = plan
                .clone()
                .rewrite(&mut add_filter)
                .with_context(|_| DatafusionSnafu {
                    context: format!("Failed to rewrite plan {plan:?}"),
                })?
                .data;
            df_plan_to_sql(&plan)?
        };

        Ok(new_sql)
    }
}

#[derive(Debug, Clone)]
pub struct RecordingRuleState {
    query_ctx: QueryContextRef,
    last_update_time: Timestamp,
}
