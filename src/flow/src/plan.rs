//! This module contain basic definition for DAG style plan that can be translate to hydro dataflow

mod reduce;

use serde::{Deserialize, Serialize};

pub(crate) use self::reduce::{AccumulablePlan, KeyValPlan, ReducePlan};
use crate::expr::{AggregateExpr, Id, LocalId, MapFilterProject, SafeMfpPlan, ScalarExpr};
use crate::repr::RelationType;

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub struct TypedPlan {
    /// output type of the relation
    pub typ: RelationType,
    pub plan: Plan,
}

#[derive(Debug, Clone, Eq, PartialEq, Deserialize, Serialize)]
pub enum Plan {
    /// Get CDC data from source
    Get { id: Id },
    /// Create a temporary collection from given `value``, and make this bind only available
    /// in scope of `body`
    Let {
        id: LocalId,
        value: Box<Plan>,
        body: Box<Plan>,
    },
    /// Map, Filter, and Project operators.
    Mfp {
        /// The input collection.
        input: Box<Plan>,
        /// Linear operator to apply to each record.
        mfp: MapFilterProject,
    },
    Reduce {
        /// The input collection.
        input: Box<Plan>,
        /// A plan for changing input records into key, value pairs.
        key_val_plan: KeyValPlan,
        /// A plan for performing the reduce.
        ///
        /// The implementation of reduction has several different strategies based
        /// on the properties of the reduction, and the input itself.
        reduce_plan: ReducePlan,
    },
}
