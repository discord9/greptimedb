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

//! Experimental raw-factorized join/aggregate physical plans for DataFusion.
//!
//! These plans retain per-host input payloads once. They intentionally target bounded,
//! one-partition fixtures and are not a general-purpose optimizer rewrite.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use arrow_array::builder::{Float64Builder, Int64Builder, ListBuilder};
use arrow_array::{Array, ArrayRef, Float64Array, Int64Array, ListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::context::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::ExecutionPlanProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream,
};
use futures::TryStreamExt;

type DimensionRows = HashMap<i64, Vec<(Option<i64>, Option<f64>)>>;

/// Fixed raw-factor schema: host, fact values and dimension area/weight lists.
pub fn factor_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("host", DataType::Int64, false),
        Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            false,
        ),
        Field::new(
            "areas",
            DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
            false,
        ),
        Field::new(
            "weights",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            false,
        ),
    ]))
}

/// Builds a compact per-host payload from ordinary fact and dimension scans.
#[derive(Debug)]
pub struct RawFactorExec {
    facts: Arc<dyn ExecutionPlan>,
    dims: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl RawFactorExec {
    /// Creates a producer after validating single-partition input schemas.
    pub fn try_new(facts: Arc<dyn ExecutionPlan>, dims: Arc<dyn ExecutionPlan>) -> Result<Self> {
        let fact = facts.schema();
        let dim = dims.schema();
        if fact.fields().len() != 2
            || fact.field(0).data_type() != &DataType::Int64
            || fact.field(1).data_type() != &DataType::Float64
            || dim.fields().len() != 3
            || dim.field(0).data_type() != &DataType::Int64
            || dim.field(1).data_type() != &DataType::Int64
            || dim.field(2).data_type() != &DataType::Float64
            || facts.output_partitioning().partition_count() != 1
            || dims.output_partitioning().partition_count() != 1
            || facts.boundedness() != Boundedness::Bounded
            || dims.boundedness() != Boundedness::Bounded
        {
            return Err(DataFusionError::Plan("RawFactorExec requires one-partition (host,value) and (host,area,weight) Int64/Float64 inputs".into()));
        }
        let schema = factor_schema();
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            facts,
            dims,
            schema,
            properties,
        })
    }
}
impl DisplayAs for RawFactorExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RawFactorExec")
    }
}
impl ExecutionPlan for RawFactorExec {
    fn name(&self) -> &'static str {
        "RawFactorExec"
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.facts, &self.dims]
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 2 {
            return Err(DataFusionError::Plan(
                "RawFactorExec requires exactly two children".into(),
            ));
        }
        Ok(Arc::new(Self::try_new(
            children[0].clone(),
            children[1].clone(),
        )?))
    }
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "invalid RawFactorExec partition {partition}"
            )));
        }
        let facts = self.facts.execute(0, context.clone())?;
        let dims = self.dims.execute(0, context)?;
        let schema = self.schema.clone();
        let output_schema = self.schema.clone();
        let fut = async move {
            let mut fact_rows: HashMap<i64, Vec<Option<f64>>> = HashMap::new();
            let batches = facts.try_collect::<Vec<_>>().await?;
            for b in batches {
                let h = as_i64(b.column(0))?;
                let v = as_f64(b.column(1))?;
                for i in 0..b.num_rows() {
                    if !h.is_null(i) {
                        if !v.is_null(i) && !v.value(i).is_finite() {
                            return Err(DataFusionError::Execution(
                                "raw factorization requires finite fact values".into(),
                            ));
                        }
                        fact_rows
                            .entry(h.value(i))
                            .or_default()
                            .push(if v.is_null(i) { None } else { Some(v.value(i)) });
                    }
                }
            }
            let mut dim_rows = DimensionRows::new();
            let batches = dims.try_collect::<Vec<_>>().await?;
            for b in batches {
                let h = as_i64(b.column(0))?;
                let a = as_i64(b.column(1))?;
                let w = as_f64(b.column(2))?;
                for i in 0..b.num_rows() {
                    if !h.is_null(i) {
                        if !w.is_null(i) && !w.value(i).is_finite() {
                            return Err(DataFusionError::Execution(
                                "raw factorization requires finite dimension weights".into(),
                            ));
                        }
                        dim_rows.entry(h.value(i)).or_default().push((
                            if a.is_null(i) { None } else { Some(a.value(i)) },
                            if w.is_null(i) { None } else { Some(w.value(i)) },
                        ));
                    }
                }
            }
            build_factor_batch(schema, fact_rows, dim_rows)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            futures::stream::once(fut),
        )))
    }
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition, Distribution::SinglePartition]
    }
}

fn check_list_child_length(lengths: impl IntoIterator<Item = usize>) -> Result<()> {
    let total = lengths.into_iter().try_fold(0usize, |total, len| {
        total
            .checked_add(len)
            .filter(|sum| *sum <= i32::MAX as usize)
    });
    if total.is_none() {
        return Err(DataFusionError::Execution(
            "factor list child length exceeds Arrow List offset limit".into(),
        ));
    }
    Ok(())
}

fn as_i64(a: &ArrayRef) -> Result<&Int64Array> {
    a.as_any()
        .downcast_ref()
        .ok_or_else(|| DataFusionError::Execution("expected Int64 array".into()))
}
fn as_f64(a: &ArrayRef) -> Result<&Float64Array> {
    a.as_any()
        .downcast_ref()
        .ok_or_else(|| DataFusionError::Execution("expected Float64 array".into()))
}
fn build_factor_batch(
    schema: SchemaRef,
    facts: HashMap<i64, Vec<Option<f64>>>,
    dims: DimensionRows,
) -> Result<RecordBatch> {
    let mut hosts: Vec<_> = facts
        .keys()
        .filter(|h| dims.contains_key(h))
        .copied()
        .collect();
    hosts.sort_unstable();
    check_list_child_length(hosts.iter().map(|host| facts[host].len()))?;
    check_list_child_length(hosts.iter().map(|host| dims[host].len()))?;
    let mut vb = ListBuilder::new(Float64Builder::new());
    let mut ab = ListBuilder::new(Int64Builder::new());
    let mut wb = ListBuilder::new(Float64Builder::new());
    for h in &hosts {
        for v in &facts[h] {
            vb.values().append_option(*v);
        }
        vb.append(true);
        for (a, w) in &dims[h] {
            ab.values().append_option(*a);
            wb.values().append_option(*w);
        }
        ab.append(true);
        wb.append(true);
    }
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(hosts)),
            Arc::new(vb.finish()),
            Arc::new(ab.finish()),
            Arc::new(wb.finish()),
        ],
    )
    .map_err(Into::into)
}

/// Consumes raw host factors into pair count, valid product count, and average by area.
#[derive(Debug)]
pub struct FactorAggregateExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}
impl FactorAggregateExec {
    /// Creates a one-partition consumer for the fixed factor schema.
    pub fn try_new(input: Arc<dyn ExecutionPlan>) -> Result<Self> {
        if !input.is::<RawFactorExec>()
            || input.schema().as_ref() != factor_schema().as_ref()
            || input.output_partitioning().partition_count() != 1
            || input.boundedness() != Boundedness::Bounded
        {
            return Err(DataFusionError::Plan(
                "FactorAggregateExec requires the fixed factor schema and one partition".into(),
            ));
        }
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Int64, true),
            Field::new("area", DataType::Int64, true),
            Field::new("pairs", DataType::Int64, false),
            Field::new("valid", DataType::Int64, false),
            Field::new("score", DataType::Float64, true),
        ]));
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            schema,
            properties,
        })
    }
}
impl DisplayAs for FactorAggregateExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FactorAggregateExec")
    }
}
impl ExecutionPlan for FactorAggregateExec {
    fn name(&self) -> &'static str {
        "FactorAggregateExec"
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(
            &Arc<dyn datafusion::physical_plan::PhysicalExpr>,
        ) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }
    fn with_new_children(
        self: Arc<Self>,
        c: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if c.len() != 1 {
            return Err(DataFusionError::Plan(
                "FactorAggregateExec requires exactly one child".into(),
            ));
        }
        Ok(Arc::new(Self::try_new(c[0].clone())?))
    }
    fn execute(&self, p: usize, c: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
        if p != 0 {
            return Err(DataFusionError::Execution(format!(
                "invalid FactorAggregateExec partition {p}"
            )));
        }
        let input = self.input.execute(0, c)?;
        let schema = self.schema.clone();
        let fut = async move {
            let bs = input.try_collect::<Vec<_>>().await?;
            consume(schema, bs)
        };
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            futures::stream::once(fut),
        )))
    }
    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }
}
fn consume(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<RecordBatch> {
    let mut out: HashMap<(i64, Option<i64>), (i64, i64, f64)> = HashMap::new();
    for b in batches {
        let hs = as_i64(b.column(0))?;
        let vs = b
            .column(1)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("expected value list".into()))?;
        let ars = b
            .column(2)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("expected area list".into()))?;
        let ws = b
            .column(3)
            .as_any()
            .downcast_ref::<ListArray>()
            .ok_or_else(|| DataFusionError::Execution("expected weight list".into()))?;
        for i in 0..b.num_rows() {
            let v = vs.value(i);
            let v = as_f64(&v)?;
            let a = ars.value(i);
            let a = as_i64(&a)?;
            let wv = ws.value(i);
            let w = as_f64(&wv)?;
            if a.len() != w.len() {
                return Err(DataFusionError::Execution(
                    "area/weight list lengths differ".into(),
                ));
            }
            let fact_count = i64::try_from(v.len())
                .map_err(|_| DataFusionError::Execution("fact count overflow".into()))?;
            let fact_valid_count =
                i64::try_from((0..v.len()).filter(|x| !v.is_null(*x)).count())
                    .map_err(|_| DataFusionError::Execution("fact count overflow".into()))?;
            let fact_sum: f64 = (0..v.len())
                .filter(|k| !v.is_null(*k))
                .map(|k| v.value(k))
                .sum();
            let mut dims_by_area: HashMap<Option<i64>, (i64, i64, f64)> = HashMap::new();
            for j in 0..a.len() {
                let area = if a.is_null(j) { None } else { Some(a.value(j)) };
                let e = dims_by_area.entry(area).or_insert((0, 0, 0.0));
                e.0 = e
                    .0
                    .checked_add(1)
                    .ok_or_else(|| DataFusionError::Execution("dimension count overflow".into()))?;
                if !w.is_null(j) {
                    e.1 = e.1.checked_add(1).ok_or_else(|| {
                        DataFusionError::Execution("dimension count overflow".into())
                    })?;
                    e.2 += w.value(j);
                }
            }
            for (area, (dim_count, dim_valid_count, dim_sum)) in dims_by_area {
                let pairs = fact_count
                    .checked_mul(dim_count)
                    .ok_or_else(|| DataFusionError::Execution("pair count overflow".into()))?;
                let valid = fact_valid_count
                    .checked_mul(dim_valid_count)
                    .ok_or_else(|| DataFusionError::Execution("valid count overflow".into()))?;
                let total = fact_sum * dim_sum;
                let e = out.entry((hs.value(i), area)).or_insert((0, 0, 0.0));
                e.0 =
                    e.0.checked_add(pairs)
                        .ok_or_else(|| DataFusionError::Execution("pair count overflow".into()))?;
                e.1 =
                    e.1.checked_add(valid)
                        .ok_or_else(|| DataFusionError::Execution("valid count overflow".into()))?;
                e.2 += total;
            }
        }
    }
    let mut rows: Vec<_> = out.into_iter().collect();
    rows.sort_by_key(|((h, a), _)| (*h, *a));
    let hs = rows.iter().map(|((h, _), _)| *h).collect::<Vec<_>>();
    let areas = rows.iter().map(|((_, a), _)| *a).collect::<Vec<_>>();
    let pairs = rows.iter().map(|(_, v)| v.0).collect::<Vec<_>>();
    let valid = rows.iter().map(|(_, v)| v.1).collect::<Vec<_>>();
    let score = rows
        .iter()
        .map(|(_, v)| {
            if v.1 == 0 {
                None
            } else {
                Some(v.2 / v.1 as f64)
            }
        })
        .collect::<Vec<_>>();
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(hs)),
            Arc::new(Int64Array::from(areas)),
            Arc::new(Int64Array::from(pairs)),
            Arc::new(Int64Array::from(valid)),
            Arc::new(Float64Array::from(score)),
        ],
    )
    .map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::check_list_child_length;

    #[test]
    fn list_child_offset_limit_checks_total_without_allocating_payloads() {
        let max = i32::MAX as usize;
        assert!(check_list_child_length([0, 0]).is_ok());
        assert!(check_list_child_length([max]).is_ok());
        assert!(check_list_child_length([max, 0]).is_ok());
        assert!(check_list_child_length([max, 1]).is_err());
        assert!(check_list_child_length([usize::MAX, 1]).is_err());
    }
}
