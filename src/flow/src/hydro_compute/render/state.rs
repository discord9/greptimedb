//! various states using in streaming operator
//!

use std::cell::RefCell;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;

use common_time::time::Time;
use datatypes::data_type::ConcreteDataType;
use datatypes::value::Value;
use hydroflow::scheduled::SubgraphId;

use crate::expr::error::{EvalError, LateDataDiscardedSnafu};
use crate::expr::{GlobalId, ScalarExpr};
use crate::hydro_compute::types::{Delta, DiffRow, RawRecv, RawSend};
use crate::hydro_compute::utils::DiffMap;
use crate::repr::{self, value_to_internal_ts, Diff, Row, Timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StateId(usize);

/// Worker-local state that is maintained across dataflows.
/// input/output of a dataflow
/// One `ComputeState` manage the input/output/schedule of one `Hydroflow`
/// TODO: use broadcast channel recv for input instead
#[derive(Default)]
pub struct ComputeState {
    /// vec in case of muiltple dataflow needed to be construct at once
    pub input_recv: BTreeMap<GlobalId, Vec<RawRecv>>,
    /// vec in case of muiltple dataflow needed to be construct at once
    pub output_send: BTreeMap<GlobalId, Vec<RawSend>>,
    /// current time, updated before run tick to progress dataflow
    pub current_time: Rc<RefCell<repr::Timestamp>>,
    pub state_to_subgraph: BTreeMap<StateId, Option<SubgraphId>>,
    pub scheduled_actions: BTreeMap<repr::Timestamp, BTreeSet<SubgraphId>>,
}

impl ComputeState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn alloc_next_state_id(&mut self) -> StateId {
        let mut id = StateId(self.state_to_subgraph.len());
        while self.state_to_subgraph.contains_key(&id) {
            id.0 += 1;
        }
        self.state_to_subgraph.insert(id, None);
        id
    }

    pub fn register_state(&mut self, id: StateId, subgraph_id: SubgraphId) -> Option<SubgraphId> {
        self.state_to_subgraph
            .insert(id, Some(subgraph_id))
            .and_then(|v| v)
    }

    pub fn add_input(&mut self, id: GlobalId) -> RawSend {
        let (input_ok, ok_recv) = hydroflow::util::unbounded_channel::<DiffRow>();

        let recv = ok_recv;
        if let Some(recv_list) = self.input_recv.get_mut(&id) {
            recv_list.push(recv);
        } else if let Some(v) = self.input_recv.insert(id, vec![recv]) {
            panic!("Can't add input more than once for each id {id:?}")
        }

        input_ok
    }

    pub fn add_output(&mut self, id: GlobalId) -> RawRecv {
        let (send_ok, mut output_ok) = hydroflow::util::unbounded_channel::<DiffRow>();

        let send = send_ok;
        if let Some(send_list) = self.output_send.get_mut(&id) {
            send_list.push(send);
        } else if let Some(v) = self.output_send.insert(id, vec![send]) {
            panic!("Can't add output more than once for each id {id:?}")
        }
        output_ok
    }
}

/// State need to be schedule after certain time
pub trait ScheduledAction {
    /// Schedule next run at given time
    fn schd_at(&self, now: repr::Timestamp) -> Option<repr::Timestamp>;
}

/// including all the future action to insert/remove rows(because current rows are send forward therefore no need to store)
///
/// Only store row since EvalError should be sent immediately
#[derive(Debug, Default)]
pub struct TemporalFilterState {
    pub spine: BTreeMap<Timestamp, BTreeMap<Row, Diff>>,
}

impl ScheduledAction for TemporalFilterState {
    fn schd_at(&self, now: repr::Timestamp) -> Option<repr::Timestamp> {
        self.spine.iter().next().map(|e| *e.0)
    }
}

impl TemporalFilterState {
    pub fn append_delta_row(&mut self, rows: impl IntoIterator<Item = (Row, Timestamp, Diff)>) {
        for (row, time, diff) in rows {
            let this_time = self.spine.entry(time).or_default();
            let mut row_entry = this_time.entry(row);
            // remove row from spine
            if let Entry::Occupied(mut o) = row_entry {
                *o.get_mut() += diff;
                if *o.get() == 0 {
                    o.remove_entry();
                }
            } else {
                row_entry.or_insert(diff);
            }
        }
    }

    /// trunc all the rows before(including) given time, and send them back
    pub fn trunc_until(&mut self, time: Timestamp) -> Vec<(Row, Timestamp, Diff)> {
        let mut ret = Vec::new();
        // drain all keys that are <= time
        let mut gt_time = self.spine.split_off(&(time + 1));
        // swap(both are basically raw ptr so should be fast)
        std::mem::swap(&mut self.spine, &mut gt_time);
        let lte_time = gt_time;

        for (t, rows) in lte_time {
            for (row, diff) in rows {
                ret.push((row, t, diff));
            }
        }
        ret
    }
}

/// A KV reduce state with timestamp and expire time
///
/// Any keys that are not updated for a long time will be removed from the state
/// and not sending any delta to downstream, since they are simple not used anymore
#[derive(Debug)]
pub struct ExpiringKeyValueStore {
    inner: DiffMap<Row, Row>,
    time2key: BTreeMap<Timestamp, BTreeSet<Row>>,
    /// duration after which a key is considered expired, and will be removed from state
    key_expiration_duration: Option<Timestamp>,
    /// using this to get timestamp from key row
    event_timestamp_from_row: ScalarExpr,
}

impl Default for ExpiringKeyValueStore {
    fn default() -> Self {
        Self {
            inner: DiffMap::default(),
            time2key: BTreeMap::new(),
            key_expiration_duration: None,
            event_timestamp_from_row: ScalarExpr::literal(
                Value::from(0i64),
                ConcreteDataType::int64_datatype(),
            ),
        }
    }
}

impl ScheduledAction for ExpiringKeyValueStore {
    fn schd_at(&self, now: repr::Timestamp) -> Option<repr::Timestamp> {
        self.time2key
            .iter()
            .next()
            .and_then(|kv| self.key_expiration_duration.map(|v| v + *kv.0))
    }
}

impl ExpiringKeyValueStore {
    pub fn extract_event_ts(&self, row: &Row) -> Result<Timestamp, EvalError> {
        let ts = value_to_internal_ts(self.event_timestamp_from_row.eval(&row.inner)?)?;
        Ok(ts)
    }
    pub fn get_expire_time(&self, current: Timestamp) -> Option<Timestamp> {
        self.key_expiration_duration.map(|d| current - d)
    }
    pub fn trunc_expired(&mut self, cur_time: Timestamp) {
        let expire_time = if let Some(t) = self.get_expire_time(cur_time) {
            t
        } else {
            return;
        };
        // TODO(discord9): determine if include/exclude expire_time itself
        let mut after = self.time2key.split_off(&expire_time);
        // swap
        std::mem::swap(&mut self.time2key, &mut after);
        let before = after;
        for (_, keys) in before.into_iter() {
            for key in keys.into_iter() {
                // should silently remove from inner
                // w/out producing new delta row
                self.inner.inner.remove(&key);
            }
        }
    }

    pub fn get(&self, k: &Row) -> Option<&Row> {
        self.inner.get(k)
    }

    /// if key row is expired then return expire error
    pub fn insert(&mut self, current: Timestamp, k: Row, v: Row) -> Result<Option<Row>, EvalError> {
        let ts = self.extract_event_ts(&k)?;
        let expire_at = self.get_expire_time(current);
        if Some(ts) < expire_at {
            return LateDataDiscardedSnafu {
                late_by: std::time::Duration::from_millis((expire_at.unwrap() - ts) as u64),
            }
            .fail();
        }

        self.time2key.entry(ts).or_default().insert(k.clone());
        /// this insert should produce delta row if not expired
        let ret = self.inner.insert(k, v);
        Ok(ret)
    }

    pub fn remove(&mut self, current: Timestamp, k: &Row) -> Result<Option<Row>, EvalError> {
        let ts = self.extract_event_ts(k)?;
        let expire_at = self.get_expire_time(current);
        if Some(ts) < expire_at {
            return LateDataDiscardedSnafu {
                late_by: std::time::Duration::from_millis((expire_at.unwrap() - ts) as u64),
            }
            .fail();
        }
        self.time2key.entry(ts).or_default().remove(k);
        Ok(self.inner.remove(k))
    }

    pub fn gen_diff(&mut self, tick: repr::Timestamp) -> Vec<((Row, Row), repr::Timestamp, Diff)> {
        self.inner.gen_diff(tick)
    }
}
