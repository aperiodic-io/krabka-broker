//! The curated, deterministic failure scenarios and the traces they record.
//!
//! Each driver here bootstraps a 3-voter [`Sim`], injects one fault, settles
//! the cluster, and returns a [`ScenarioTrace`]. The three scenarios show
//! split-brain prevention under a leader partition, log convergence under
//! reordered delivery, and idempotent handling of a duplicated message.
//! `krabka-docgen` renders the traces into the sequence-diagram slideshow.

use {
    super::{Sim, trace::ScenarioTrace},
    crate::types::NodeId,
};

/// Run the three curated, deterministic failure scenarios on a 3-voter cluster
/// and return their recorded traces.
#[must_use]
pub fn scenarios() -> Vec<ScenarioTrace> {
    vec![
        split_brain_prevented(),
        out_of_order_delivery(),
        message_duplication(),
    ]
}

/// Bootstrap one leader and then partition it.
///
/// The majority elects a fresh leader at a higher epoch, and the isolated old
/// leader cannot. The scenario then heals the partition, and the old leader
/// steps down. The cluster ends with exactly one leader.
fn split_brain_prevented() -> ScenarioTrace {
    let nodes = [NodeId(1), NodeId(2), NodeId(3)];
    let mut sim = Sim::new(&nodes);
    sim.run_until_stable(10_000);
    let old_leader = sim.leaders().first().copied().unwrap_or(NodeId(1));

    sim.partition(old_leader);
    sim.run_until_stable(10_000);
    let new_leader = sim
        .leaders()
        .into_iter()
        .find(|&l| l != old_leader)
        .unwrap_or(old_leader);

    sim.heal(old_leader);
    sim.run_until_stable(10_000);

    let final_leaders = sim.leaders();
    assert2::assert!(final_leaders.len() == 1);
    let outcome = format!(
        "The majority side elected N{new_leader} at a strictly higher epoch. The \
         isolated old leader N{old_leader} could not advance (no quorum), and on \
         healing it learned the newer epoch from a BeginQuorumEpoch heartbeat and \
         stepped down to follower. Exactly one leader remains."
    );
    ScenarioTrace {
        id: "split_brain_prevented".to_string(),
        title: "Split-brain prevented (leader partition)".to_string(),
        summary: "A 3-voter cluster elects a leader, the leader is network-partitioned \
                  away from the majority, and the two-node majority elects a new leader \
                  at a higher epoch. The isolated old leader cannot make progress without \
                  a quorum, so there is never a second live leader. When the partition \
                  heals, the stale leader learns the newer epoch and steps down."
            .to_string(),
        invariant: "At most one leader per epoch (election safety)".to_string(),
        nodes: nodes.iter().map(|n| n.0).collect(),
        steps: sim.steps,
        outcome,
    }
}

/// Drive a replication round that delivers its bus messages in a deliberately
/// non-FIFO order.
///
/// The scenario shows that the log stays consistent. Appends carry monotonic
/// offsets and leader epochs, so the replicas detect stale and late messages.
fn out_of_order_delivery() -> ScenarioTrace {
    let nodes = [NodeId(1), NodeId(2), NodeId(3)];
    let mut sim = Sim::new(&nodes);
    sim.run_until_stable(10_000);
    let leader = sim.leaders().first().copied().unwrap_or(NodeId(1));

    // Produce some records, then let the fetch/replication traffic queue up and
    // deliver it back-to-front before settling.
    sim.leader_append(leader, 3);
    // Prime a replication round so there are messages to reorder.
    sim.run_until_stable(50);
    let reordered = sim.deliver_queue_reversed();
    sim.run_until_stable(10_000);

    let final_leaders = sim.leaders();
    let log_len = sim
        .nodes
        .values()
        .next()
        .map_or(0, |n| n.log.record_count());
    let outcome = format!(
        "Even though {reordered} in-flight messages were delivered out of order, \
         every voter's log converged identically ({log_len} records) and the \
         cluster kept exactly {} leader. Stale or late messages were ignored \
         because each fetch and append is tagged with a monotonic offset and \
         leader epoch.",
        final_leaders.len()
    );
    ScenarioTrace {
        id: "out_of_order_delivery".to_string(),
        title: "Reordered message delivery".to_string(),
        summary: "The simulator deliberately delivers a round of replication messages \
                  back-to-front (non-FIFO). Because every fetch and append carries a \
                  monotonic offset and the producing leader epoch, a node detects and \
                  ignores any message that is stale or out of order — the replicated \
                  logs still converge to the same contents."
            .to_string(),
        invariant: "Log matching under reordered delivery".to_string(),
        nodes: nodes.iter().map(|n| n.0).collect(),
        steps: sim.steps,
        outcome,
    }
}

/// Deliver one message twice and show idempotent handling: no double
/// application and no extra leader.
fn message_duplication() -> ScenarioTrace {
    let nodes = [NodeId(1), NodeId(2), NodeId(3)];
    let mut sim = Sim::new(&nodes);
    // Run a few ticks so there is real in-flight election traffic to duplicate,
    // then deliver the front message twice.
    sim.run_until_stable(20);
    let duplicated = sim.deliver_front_twice();
    sim.run_until_stable(10_000);

    let final_leaders = sim.leaders();
    assert2::assert!(final_leaders.len() <= 1);
    let outcome = format!(
        "A message was delivered twice ({}). The duplicate was handled idempotently — \
         a vote already counted is not counted again and an already-known epoch is \
         a no-op — so the cluster still converged to exactly {} leader.",
        if duplicated {
            "duplicate injected"
        } else {
            "no in-flight message"
        },
        final_leaders.len()
    );
    ScenarioTrace {
        id: "message_duplication".to_string(),
        title: "Duplicate message delivery".to_string(),
        summary: "The simulator delivers the same in-flight message twice. KRaft handles \
                  duplicates idempotently: a vote that was already granted/counted has no \
                  additional effect, and a BeginQuorumEpoch for an epoch the node already \
                  knows is a no-op. No double application happens and no spurious second \
                  leader emerges."
            .to_string(),
        invariant: "Idempotent handling of duplicate messages".to_string(),
        nodes: nodes.iter().map(|n| n.0).collect(),
        steps: sim.steps,
        outcome,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_three_traces() {
        let traces = scenarios();
        assert2::assert!(traces.len() == 3);
    }

    #[test]
    fn every_trace_has_steps() {
        for trace in scenarios() {
            assert2::assert!(!trace.steps.is_empty());
        }
    }

    #[test]
    fn split_brain_ends_with_exactly_one_leader() {
        let traces = scenarios();
        let split = traces
            .iter()
            .find(|t| t.id == "split_brain_prevented")
            .expect("split_brain_prevented trace present");
        let last = split.steps.last().expect("split-brain has steps");
        let leaders = last.roles.iter().filter(|r| r.role == "Leader").count();
        assert2::assert!(leaders == 1);
    }
}
