use serde::{Deserialize, Serialize};

use crate::expr::{AggregateExpr, Id, LocalId, MapFilterProject, SafeMfpPlan, ScalarExpr};

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct KeyValPlan {
    pub key_plan: SafeMfpPlan,
    pub val_plan: SafeMfpPlan,
}

/// TODO(discord9): Hier Plan for min/max that's not montonic
#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub enum ReducePlan {
    /// Plan for not computing any aggregations, just determining the set of
    /// distinct keys.
    Distinct,
    /// Plan for computing only accumulable aggregations.
    Accumulable(AccumulablePlan),
}

/// TODO: support distinct aggrs& state config
#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
pub struct AccumulablePlan {
    /// All of the aggregations we were asked to compute, stored
    /// in order.
    pub full_aggrs: Vec<AggregateExpr>,
    /// All of the non-distinct accumulable aggregates.
    /// Each element represents:
    /// (index of aggr output, index of value among inputs, aggr expr)
    /// These will all be rendered together in one dataflow fragment.
    pub simple_aggrs: Vec<(usize, usize, AggregateExpr)>,
    /// Same as above but for all of the `DISTINCT` accumulable aggregations.
    pub distinct_aggrs: Vec<(usize, usize, AggregateExpr)>,
}
