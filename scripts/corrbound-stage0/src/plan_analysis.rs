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

//! Physical-plan inspection helpers: locate the pinned
//! `HashJoinExec` node, read its join decision, and read the statistics that
//! `datafusion::physical_optimizer::join_selection::JoinSelection` consumes.

use std::sync::Arc;

use datafusion::physical_plan::joins::HashJoinExec;
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs, StatisticsContext, displayable};
use datafusion_common::{Result as DfResult, Statistics};
use serde_json::{Value, json};

use crate::fixture::{DIM_TABLE, FACT_TABLE};

/// Field names that identify which fixture table a join input reads. The plan
/// projects `f.host, f.value` on the fact side and `d.host, d.zone, d.weight` on
/// the dim side, so these markers identify the side even after the join inputs
/// have been swapped.
const FACT_MARKERS: [&str; 1] = ["value"];
const DIM_MARKERS: [&str; 2] = ["zone", "weight"];

/// The join decision that the probe compares across replays.
///
/// Equality covers the decision itself — join type, partition mode, null-aware
/// flag and which fixture table sits on which side. It deliberately excludes
/// the *names* of the join's direct child nodes: rules that run after
/// `JoinSelection` (e.g. `EnsureRequirements`) insert `RepartitionExec` between
/// the join and its inputs, so those names are pipeline artifacts rather than
/// part of the join decision. The full node names are still reported in the
/// JSON evidence and in the `diff()` text when nothing else differs.
#[derive(Debug, Clone)]
pub struct JoinDecision {
    pub node: String,
    pub join_type: String,
    pub partition_mode: String,
    pub null_aware: bool,
    pub left_side: String,
    pub right_side: String,
    pub left_node: String,
    pub right_node: String,
}

impl PartialEq for JoinDecision {
    fn eq(&self, other: &Self) -> bool {
        self.join_type == other.join_type
            && self.partition_mode == other.partition_mode
            && self.null_aware == other.null_aware
            && self.side_assignment() == other.side_assignment()
    }
}

impl Eq for JoinDecision {}

impl JoinDecision {
    pub fn side_assignment(&self) -> String {
        format!("left={},right={}", self.left_side, self.right_side)
    }

    /// Fields that differ between two decisions, used for the counterfactual
    /// report.
    pub fn diff(&self, other: &JoinDecision) -> Vec<String> {
        let mut changed = Vec::new();
        if self.partition_mode != other.partition_mode {
            changed.push(format!(
                "partition_mode: {} -> {}",
                self.partition_mode, other.partition_mode
            ));
        }
        if self.side_assignment() != other.side_assignment() {
            changed.push(format!(
                "side_assignment: {} -> {}",
                self.side_assignment(),
                other.side_assignment()
            ));
        }
        if self.join_type != other.join_type {
            changed.push(format!(
                "join_type: {} -> {}",
                self.join_type, other.join_type
            ));
        }
        if changed.is_empty() && self.node != other.node {
            changed.push(format!("join node name: {} -> {}", self.node, other.node));
        }
        changed
    }

    /// Child node names, reported alongside the decision but excluded from
    /// equality (see the type-level docs).
    pub fn child_nodes(&self) -> String {
        format!("left={},right={}", self.left_node, self.right_node)
    }

    pub fn to_json(&self) -> Value {
        json!({
            "node": self.node,
            "join_type": self.join_type,
            "partition_mode": self.partition_mode,
            "null_aware": self.null_aware,
            "left_side": self.left_side,
            "right_side": self.right_side,
            "left_node": self.left_node,
            "right_node": self.right_node,
            "side_assignment": self.side_assignment(),
        })
    }
}

/// A join node plus the decision it currently encodes.
pub struct JoinObservation {
    pub decision: JoinDecision,
    pub left: Arc<dyn ExecutionPlan>,
    pub right: Arc<dyn ExecutionPlan>,
}

/// Depth-first search for the first `HashJoinExec` in the plan.
pub fn find_join(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if node.downcast_ref::<HashJoinExec>().is_some() {
            return Some(node);
        }
        for child in node.children() {
            stack.push(Arc::clone(child));
        }
    }
    None
}

pub fn count_joins(plan: &Arc<dyn ExecutionPlan>) -> usize {
    let mut count = 0;
    let mut stack = vec![Arc::clone(plan)];
    while let Some(node) = stack.pop() {
        if node.downcast_ref::<HashJoinExec>().is_some() {
            count += 1;
        }
        for child in node.children() {
            stack.push(Arc::clone(child));
        }
    }
    count
}

/// Reads the join decision of the (single) hash join in `plan`.
pub fn join_observation(plan: &Arc<dyn ExecutionPlan>) -> Option<JoinObservation> {
    let join = find_join(plan)?;
    let hash_join = join.downcast_ref::<HashJoinExec>()?;
    let left = Arc::clone(hash_join.left());
    let right = Arc::clone(hash_join.right());
    let decision = JoinDecision {
        node: join.name().to_string(),
        join_type: format!("{}", hash_join.join_type()),
        partition_mode: format!("{:?}", hash_join.partition_mode()),
        null_aware: hash_join.null_aware,
        left_side: side_label(&left),
        right_side: side_label(&right),
        left_node: left.name().to_string(),
        right_node: right.name().to_string(),
    };
    Some(JoinObservation {
        decision,
        left,
        right,
    })
}

/// Classifies a join input subtree as the fact table, the dim table, or
/// `unknown(<fields>)`.
pub fn side_label(subtree: &Arc<dyn ExecutionPlan>) -> String {
    let schema = subtree.schema();
    let names: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
    let has = |marker: &str| names.iter().any(|name| name == marker);
    if FACT_MARKERS.iter().all(|m| has(m)) {
        FACT_TABLE.to_string()
    } else if DIM_MARKERS.iter().all(|m| has(m)) {
        DIM_TABLE.to_string()
    } else {
        format!("unknown({})", names.join(","))
    }
}

/// Indented physical plan text.
pub fn plan_text(plan: &Arc<dyn ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(true).to_string()
}

/// Statistics of a plan node in the same way `JoinSelection` reads them.
pub fn statistics_of(node: &dyn ExecutionPlan) -> DfResult<Arc<Statistics>> {
    StatisticsContext::new().compute(node, &StatisticsArgs::new())
}

/// JSON summary of the fields `JoinSelection` looks at.
pub fn statistics_json(node: &Arc<dyn ExecutionPlan>) -> Value {
    match statistics_of(node.as_ref()) {
        Ok(stats) => {
            let columns_with_stats = stats
                .column_statistics
                .iter()
                .filter(|c| {
                    format!("{:?}", c.distinct_count) != "Absent"
                        || format!("{:?}", c.null_count) != "Absent"
                })
                .count();
            json!({
                "node": node.name(),
                "side": side_label(node),
                "num_rows": format!("{:?}", stats.num_rows),
                "total_byte_size": format!("{:?}", stats.total_byte_size),
                "columns": stats.column_statistics.len(),
                "columns_with_statistics": columns_with_stats,
            })
        }
        Err(err) => json!({
            "node": node.name(),
            "side": side_label(node),
            "error": format!("{err}"),
        }),
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;

    #[test]
    fn side_labels_come_from_projected_columns() {
        let fact_schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("instance", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
            Field::new(
                "ts",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Nanosecond, None),
                true,
            ),
        ]));
        let fact: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(fact_schema));
        assert_eq!(side_label(&fact), FACT_TABLE);

        let dim_schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, true),
            Field::new("zone", DataType::Utf8, true),
            Field::new("weight", DataType::Float64, true),
        ]));
        let dim: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(dim_schema));
        assert_eq!(side_label(&dim), DIM_TABLE);
    }

    fn sample_decision() -> JoinDecision {
        JoinDecision {
            node: "HashJoinExec".to_string(),
            join_type: "Inner".to_string(),
            partition_mode: "Partitioned".to_string(),
            null_aware: false,
            left_side: FACT_TABLE.to_string(),
            right_side: DIM_TABLE.to_string(),
            left_node: "a".to_string(),
            right_node: "b".to_string(),
        }
    }

    #[test]
    fn decision_diff_reports_mode_and_side_changes() {
        let base = sample_decision();
        assert!(base.diff(&base).is_empty());
        assert_eq!(base, base);

        let mut other = base.clone();
        other.partition_mode = "CollectLeft".to_string();
        assert_ne!(base, other);
        assert_eq!(base.diff(&other).len(), 1);

        let mut swapped = base.clone();
        swapped.left_side = DIM_TABLE.to_string();
        swapped.right_side = FACT_TABLE.to_string();
        assert_ne!(base, swapped);
        assert_eq!(base.diff(&swapped).len(), 1);
    }

    #[test]
    fn decision_equality_ignores_repartitioning_artifacts() {
        // `EnsureRequirements` inserts RepartitionExec between the join and its
        // inputs; the same join decision must still compare equal.
        let base = sample_decision();
        let mut repartitioned = base.clone();
        repartitioned.left_node = "RepartitionExec".to_string();
        repartitioned.right_node = "RepartitionExec".to_string();
        assert_eq!(base, repartitioned);
        assert!(base.diff(&repartitioned).is_empty());
        // ... but the reported child nodes still show the difference.
        assert_ne!(base.child_nodes(), repartitioned.child_nodes());
    }
}
