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

//! Experiment-local statistics injection.
//!
//! [`StatisticsInjectExec`] is a pass-through [`ExecutionPlan`] that reports the
//! statistics of its input with exactly one field replaced: `num_rows` becomes
//! `Precision::Inexact(bound)`. This models a planner that has lost the
//! filtered-cardinality information of a join input while still knowing the
//! sound pre-query upper bound `|sigma_P(T)| <= |T| = bound`.
//!
//! Everything else (byte size, column statistics, partitioning, execution) is
//! delegated to the wrapped plan, so a counterfactual plan built on top of the
//! wrapper still produces the same rows.

use std::fmt;
use std::sync::Arc;

use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{ChildrenPropertiesMode, ReplaceChildrenOptions};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
    StatisticsArgs, StatisticsContext,
};
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result as DfResult, Statistics};

/// Returns `stats` with `num_rows` replaced by `Inexact(bound)`.
///
/// All other fields are preserved verbatim.
pub fn inject_num_rows(stats: &Statistics, bound: usize) -> Statistics {
    let mut injected = stats.clone();
    injected.num_rows = Precision::Inexact(bound);
    injected
}

/// Pass-through plan that reports an injected `num_rows` upper bound.
#[derive(Debug)]
pub struct StatisticsInjectExec {
    inner: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    injected_rows: usize,
}

impl StatisticsInjectExec {
    pub fn new(inner: Arc<dyn ExecutionPlan>, injected_rows: usize) -> Self {
        let properties = Arc::new(compute_properties(&inner));
        Self {
            inner,
            properties,
            injected_rows,
        }
    }

    /// The wrapped plan. The probe uses this to prove that the injected variant
    /// differs from the original one only by the wrapper.
    pub fn inner(&self) -> &Arc<dyn ExecutionPlan> {
        &self.inner
    }

    pub fn injected_rows(&self) -> usize {
        self.injected_rows
    }
}

fn compute_properties(inner: &Arc<dyn ExecutionPlan>) -> PlanProperties {
    let inner_properties = inner.properties();
    PlanProperties::new(
        EquivalenceProperties::new(inner.schema()),
        inner_properties.partitioning.clone(),
        inner_properties.emission_type,
        inner_properties.boundedness,
    )
}

impl DisplayAs for StatisticsInjectExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => write!(
                f,
                "StatisticsInjectExec: num_rows=Inexact({})",
                self.injected_rows
            ),
            DisplayFormatType::TreeRender => {
                write!(f, "num_rows=Inexact({})", self.injected_rows)
            }
        }
    }
}

impl ExecutionPlan for StatisticsInjectExec {
    fn name(&self) -> &str {
        "StatisticsInjectExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        let child = if children.len() == 1 {
            children.pop().expect("length checked above")
        } else {
            Arc::clone(&self.inner)
        };
        Ok(Arc::new(StatisticsInjectExec::new(
            child,
            self.injected_rows,
        )))
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DfResult<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DfResult<SendableRecordBatchStream> {
        self.inner.execute(partition, context)
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_expr::PhysicalExpr>,
        ) -> DfResult<TreeNodeRecursion>,
    ) -> DfResult<TreeNodeRecursion> {
        self.inner.apply_expressions(f)
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        args: &StatisticsArgs,
    ) -> DfResult<Arc<Statistics>> {
        // The bottom-up walk supplies the wrapped plan's statistics through
        // `child_stats_requests`; only `num_rows` is replaced.
        let base = match input_stats.first() {
            Some(stats) => Arc::clone(stats),
            None => StatisticsContext::new().compute(self.inner.as_ref(), args)?,
        };
        Ok(Arc::new(inject_num_rows(base.as_ref(), self.injected_rows)))
    }

    fn child_stats_requests(
        &self,
        partition: Option<usize>,
    ) -> Vec<datafusion::physical_plan::ChildStats> {
        vec![datafusion::physical_plan::ChildStats::At(partition)]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        self.inner.maintains_input_order()
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        self.inner.benefits_from_input_partitioning()
    }
}

#[cfg(test)]
mod tests {
    use arrow::datatypes::{Field, Schema};
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::physical_plan::empty::EmptyExec;

    use super::*;

    #[test]
    fn injection_replaces_only_num_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            arrow::datatypes::DataType::Int32,
            true,
        )]));
        let mut stats = Statistics::new_unknown(&schema);
        stats.num_rows = Precision::Exact(42);
        stats.total_byte_size = Precision::Exact(1024);
        let injected = inject_num_rows(&stats, 16_384);
        assert_eq!(injected.num_rows, Precision::Inexact(16_384));
        assert_eq!(injected.total_byte_size, stats.total_byte_size);
        assert_eq!(
            injected.column_statistics.len(),
            stats.column_statistics.len()
        );
        assert_eq!(stats.num_rows, Precision::Exact(42), "input is not mutated");
    }

    #[tokio::test]
    async fn wrapper_preserves_schema_and_reports_injected_rows() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "a",
            arrow::datatypes::DataType::Int32,
            true,
        )]));
        let inner: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(Arc::clone(&schema)));
        let wrapper = StatisticsInjectExec::new(Arc::clone(&inner), 16_384);
        assert_eq!(
            wrapper.schema().fields().len(),
            inner.schema().fields().len()
        );
        assert!(Arc::ptr_eq(wrapper.inner(), &inner));
        // Statistics are taken from the child statistics supplied by the walk.
        let mut child_stats = Statistics::new_unknown(&schema);
        child_stats.num_rows = Precision::Exact(7);
        let stats = wrapper
            .statistics_from_inputs(&[Arc::new(child_stats)], &StatisticsArgs::new())
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Inexact(16_384));
        // ... and `child_stats_requests` asks the walk for the child's own stats.
        assert_eq!(wrapper.child_stats_requests(None).len(), 1);
    }
}
