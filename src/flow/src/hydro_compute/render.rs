use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;

use hydroflow::scheduled::graph::Hydroflow;

use crate::expr::{GlobalId, LocalId};
use crate::hydro_compute::render::state::ComputeState;
use crate::hydro_compute::types::{DataflowDescription, RecvPort, SendPort};
use crate::repr;

mod state;

/// A Thread Local Manager to manage multiple dataflow
pub struct HydroManager {
    /// map from task name to dataflow
    pub dataflows: BTreeMap<String, Hydroflow>,
    /// map from id to input/output
    pub compute_state: ComputeState,
}

/// Build a dataflow from description and connect it with input/output by fetching it
/// from `compute_state`
/// return the `Hydroflow` being built
/// TODO: add compute state for this
pub fn build_compute_dataflow(
    dataflow: DataflowDescription,
    compute_state: &mut ComputeState,
) -> Hydroflow {
    todo!()
}

/// The Context for build a Operator with id of `GlobalId`
pub struct Context<'a> {
    pub id: GlobalId,
    pub df: &'a mut Hydroflow,
    pub compute_state: &'a mut ComputeState,
    /// multiple ports if this operator is used by dst operator multiple times
    /// key being None means this operator is sink
    send_ports: BTreeMap<Option<GlobalId>, Vec<SendPort>>,
    /// multiple ports if this operator use source operator multiple times
    recv_ports: BTreeMap<GlobalId, Vec<RecvPort>>,
    /// for each local scope created from `Let`, map from local id to global id
    /// each `RecvPort` port is just the same port depulicated and should be take out when use
    local_scope: Vec<HashMap<LocalId, Vec<RecvPort>>>,
    /// The write frontier, this operator can't receive records older than this time
    /// and can only send records with this time if not otherwise specified
    /// TODO(discord9): use it as current time in temporal filter to get current correct result
    as_of: Rc<RefCell<repr::Timestamp>>,
}
