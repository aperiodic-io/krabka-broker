//! The in-memory message bus: delivery, and the application of every [`Action`]
//! the state machine returns.
//!
//! This module holds the half of the harness that moves data. It hands one
//! [`Event`] to a node, turns each returned action into queued messages, log
//! appends, truncations, and timer arms, and it holds the two replay faults
//! that deliver the queue back-to-front or deliver one message twice.

use super::{
    Sim,
    node::Message,
    trace::{TraceAction, event_label},
};
use crate::{
    event::Event,
    role::Role,
    types::{LogView, NodeId},
};

impl Sim {
    pub(super) fn deliver(&mut self, msg: &Message) {
        if self.partitioned.contains(&msg.src) || self.partitioned.contains(&msg.dst) {
            return;
        }
        if !self.nodes.contains_key(&msg.dst) {
            return;
        }
        let label = event_label(&msg.event);
        let (src, dst) = (msg.src, msg.dst);
        self.step(dst, msg.event);
        self.record(
            TraceAction::Deliver {
                src: src.0,
                dst: dst.0,
                event: label.clone(),
            },
            format!("N{src} → N{dst}: {label}"),
        );
        self.record_new_leaders();
    }

    krabka_macros::simulation_step!(crate);

    krabka_macros::simulation_actions!(crate);

    fn advance_simulated_watermark(&mut self, id: NodeId, hwm: i64) {
        let node = self.nodes.get_mut(&id).unwrap();
        node.high_watermark = hwm;
        for peer in self.all_node_ids() {
            if peer != id && !self.partitioned.contains(&peer) {
                let p = self.nodes.get_mut(&peer).unwrap();
                p.high_watermark = hwm.min(p.log.end_offset());
            }
        }
    }

    fn replicate_from_leader(&mut self, follower: NodeId, leader: NodeId) {
        if !self.can_replicate(follower, leader) {
            return;
        }
        let leader_hwm = match self.nodes[&leader].machine.role() {
            Role::Leader { high_watermark, .. } => *high_watermark,
            _ => self.nodes[&leader].high_watermark,
        };
        let mut follower_node = self.nodes.remove(&follower).expect("follower exists");
        follower_node.log.replicate_from(&self.nodes[&leader].log);
        follower_node.high_watermark = leader_hwm.min(follower_node.log.end_offset());
        self.nodes.insert(follower, follower_node);
    }

    krabka_macros::simulation_transport!(crate);

    /// Drain and deliver the queued messages back-to-front, a deliberately
    /// non-FIFO but deterministic order.
    ///
    /// This method returns the number of messages delivered.
    /// `out_of_order_delivery` calls it to show that the log stays consistent
    /// under reordered delivery.
    pub(super) fn deliver_queue_reversed(&mut self) -> usize {
        let mut drained: Vec<Message> = self.queue.drain(..).collect();
        drained.reverse();
        let n = drained.len();
        for msg in drained {
            self.deliver(&msg);
        }
        n
    }

    /// Deliver the front-of-queue message twice.
    ///
    /// The duplicate is a no-op on the recipient because `KRaft` messages carry
    /// monotonic epochs and offsets. This method returns `true` if a message
    /// was available to duplicate.
    pub(super) fn deliver_front_twice(&mut self) -> bool {
        let Some(msg) = self.queue.front().cloned() else {
            return false;
        };
        // Deliver the genuine copy.
        let first = self.queue.pop_front().expect("front exists");
        self.deliver(&first);
        // Deliver the duplicate of the same message.
        self.deliver(&msg);
        true
    }
}
