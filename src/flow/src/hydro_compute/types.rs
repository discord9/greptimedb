use hydroflow::scheduled::handoff::VecHandoff;
use hydroflow::scheduled::port::{Port, RECV, SEND};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::expr::GlobalId;
use crate::plan::TypedPlan;
use crate::repr::{self, Row};

/// (Data, Tick, Diff)
pub type Delta<T> = (T, repr::Timestamp, repr::Diff);
pub type DiffRow = Delta<Row>;
pub type Hoff = VecHandoff<DiffRow>;

pub type RawRecv = UnboundedReceiverStream<DiffRow>;

pub type RawSend = UnboundedSender<DiffRow>;

/// Send Port for both (ok, err) using `T` as Handoff to store
pub type SendPort<T = DiffRow> = Port<SEND, VecHandoff<T>>;
/// Recv Port for both (ok, err) using `T` as Handoff to store
pub type RecvPort<T = DiffRow> = Port<RECV, VecHandoff<T>>;

/// An association of a global identifier to an expression.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BuildDesc<P = TypedPlan> {
    pub id: GlobalId,
    pub plan: P,
}

#[derive(Default, Debug)]
pub struct DataflowDescription {
    pub objects_to_build: Vec<BuildDesc>,
    pub inputs: Vec<GlobalId>,
    pub outputs: Vec<GlobalId>,
    /// name of the dataflow, assigned by user using `CREATE TASK <name>`
    pub name: String,
}

impl DataflowDescription {
    pub fn new(name: String) -> Self {
        Self {
            objects_to_build: Vec::new(),
            inputs: Vec::new(),
            outputs: Vec::new(),
            name,
        }
    }
}
