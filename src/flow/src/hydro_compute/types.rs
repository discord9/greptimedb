use hydroflow::scheduled::handoff::VecHandoff;
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
