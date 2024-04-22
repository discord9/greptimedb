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

//! Source and Sink for the dataflow

use std::collections::BTreeMap;

use hydroflow::scheduled::graph_ext::GraphExt;
use itertools::Itertools;
use snafu::OptionExt;
use tokio::sync::broadcast;

use crate::adapter::error::{Error, PlanSnafu};
use crate::compute::render::Context;
use crate::compute::types::{Arranged, Collection, CollectionBundle, Toff};
use crate::expr::GlobalId;
use crate::repr::DiffRow;

#[allow(clippy::mutable_key_type)]
impl<'referred, 'df> Context<'referred, 'df> {
    /// Render a source which comes from brocast channel into the dataflow
    /// will immediately send updates not greater than `now` and buffer the rest in arrangement
    pub fn render_source(
        &mut self,
        mut src_recv: broadcast::Receiver<DiffRow>,
    ) -> Result<CollectionBundle, Error> {
        let (send_port, recv_port) = self.df.make_edge::<_, Toff>("source");
        let arrange_handler = self.compute_state.new_arrange(None);
        let arrange_handler_inner =
            arrange_handler
                .clone_future_only()
                .with_context(|| PlanSnafu {
                    reason: "No write is expected at this point",
                })?;
        let now = self.compute_state.current_time_ref();
        let err_collector = self.err_collector.clone();

        let sub = self
            .df
            .add_subgraph_source("source", send_port, move |_ctx, send| {
                let now = *now.borrow();
                let arr = arrange_handler_inner.write().get_updates_in_range(..=now);
                err_collector.run(|| arrange_handler_inner.write().compaction_to(now));

                let prev_avail = arr.into_iter().map(|((k, _), t, d)| (k, t, d));
                let mut new_arrive = Vec::new();
                // TODO(discord9): handling tokio broadcast error
                while let Ok(update) = src_recv.try_recv() {
                    new_arrive.push(update);
                }
                let all = prev_avail.chain(new_arrive);
                send.give(all.collect_vec());
            });
        let arranged = Arranged::new(arrange_handler);
        arranged.writer.borrow_mut().replace(sub);
        let arranged = BTreeMap::from([(vec![], arranged)]);
        Ok(CollectionBundle {
            collection: Collection::from_port(recv_port),
            arranged,
        })
    }

    /// Render a sink which send updates to broadcast channel
    pub fn render_sink(&mut self, bundle: CollectionBundle, sender: broadcast::Sender<DiffRow>) {
        let CollectionBundle {
            collection,
            arranged: _,
        } = bundle;
        self.df
            .add_subgraph_sink("Sink", collection.into_inner(), move |_ctx, recv| {
                let data = recv.take_inner();
                for row in data.into_iter().flat_map(|i| i.into_iter()) {
                    // TODO(discord9): handling tokio broadcast error
                    let _ = sender.send(row);
                }
            });
    }
}
