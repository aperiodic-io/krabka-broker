//! The interactive control surface the in-browser playground drives.
//!
//! `krabka-playground` steps the same scheduler one microstep at a time,
//! injects the operator faults that drop, reorder, or duplicate a message on
//! the bus, and reads back a serializable [`SimSnapshot`] after every action.
//! The accessors here are the whole of what that UI needs.

use {
    super::{
        Sim,
        trace::{InFlight, SimSnapshot, TraceAction, TraceStep, event_label},
    },
    crate::types::NodeId,
};

impl Sim {
    /// Advance the simulation by one scheduler microstep.
    ///
    /// This method delivers the front-of-bus message if one is queued.
    /// Otherwise it fires the next-due timer. It returns `true` if there was a
    /// message or a timer.
    pub fn step_once(&mut self) -> bool {
        if let Some(msg) = self.queue.pop_front() {
            self.deliver(&msg);
            return true;
        }
        self.fire_next_timer()
    }

    /// Append `n` records on whichever node is currently the leader.
    ///
    /// This method backs the playground "produce" button. It returns `false` if
    /// there is no leader to append to.
    pub fn append(&mut self, n: usize) -> bool {
        let Some(leader) = self.leaders().first().copied() else {
            return false;
        };
        self.leader_append(leader, n);
        true
    }

    /// Discard the front-of-bus message instead of delivering it: the "drop"
    /// fault.
    ///
    /// This method returns `false` if the bus is empty. It records a
    /// [`TraceAction::Drop`], so the fault appears on the event timeline.
    pub fn drop_next(&mut self) -> bool {
        let Some(msg) = self.queue.pop_front() else {
            return false;
        };
        let label = event_label(&msg.event);
        self.record(
            TraceAction::Drop {
                src: msg.src.0,
                dst: msg.dst.0,
                event: label.clone(),
            },
            format!("N{} → N{}: {label} dropped in flight", msg.src, msg.dst),
        );
        true
    }

    /// Deliver every queued message back-to-front, that is, non-FIFO: the
    /// "reorder" fault.
    ///
    /// This method returns the number of messages delivered.
    pub fn reorder(&mut self) -> usize { self.deliver_queue_reversed() }

    /// Deliver the front-of-bus message twice: the "duplicate" fault.
    ///
    /// This method returns `false` if the bus is empty.
    pub fn duplicate_next(&mut self) -> bool { self.deliver_front_twice() }

    /// The current logical clock, in milliseconds.
    #[must_use]
    pub fn clock_ms(&self) -> u64 { self.now.0 }

    /// The voter ids of this cluster, ascending.
    #[must_use]
    pub fn voter_ids(&self) -> Vec<NodeId> { self.voter_ids.clone() }

    /// The recorded event timeline: every delivery, fault, timeout, and
    /// election the simulation has taken so far.
    #[must_use]
    pub fn steps(&self) -> &[TraceStep] { &self.steps }

    /// The messages currently in flight on the bus, front (next to deliver) first.
    #[must_use]
    pub fn in_flight(&self) -> Vec<InFlight> {
        self.queue
            .iter()
            .map(|m| InFlight {
                src: m.src.0,
                dst: m.dst.0,
                event: event_label(&m.event),
            })
            .collect()
    }

    /// A full, serializable snapshot of the cluster.
    ///
    /// The snapshot holds the clock, every node's role, the in-flight bus, the
    /// current leaders, and the number of steps that elapsed. The browser UI
    /// renders this after each control action.
    #[must_use]
    pub fn snapshot(&self) -> SimSnapshot {
        SimSnapshot {
            clock_ms: self.now.0,
            nodes: self.snapshot_roles(),
            in_flight: self.in_flight(),
            leaders: self.leaders().iter().map(|id| id.0).collect(),
            step_count: self.steps.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// Step the bus and the timers one microstep at a time until a leader
    /// appears, with a bounded number of steps.
    ///
    /// This is how the UI's "step" button works.
    fn step_until<F: Fn(&Sim) -> bool>(sim: &mut Sim, max: usize, done: F) {
        for _ in 0..max {
            if done(sim) {
                return;
            }
            if !sim.step_once() {
                return;
            }
        }
    }

    #[test]
    fn interactive_bootstrap_elects_one_leader() {
        let mut sim = Sim::new(&[NodeId(1), NodeId(2), NodeId(3)]);
        // Fresh cluster: no leader, election timers armed, bus empty.
        assert2::assert!((sim.leaders().is_empty(), sim.snapshot().nodes.len()) == (true, 3));

        step_until(&mut sim, 10_000, |s| !s.leaders().is_empty());
        sim.run_until_stable(10_000);
        assert2::assert!(sim.leaders().len() == 1);

        let snap = sim.snapshot();
        check!((snap.leaders.len(), snap.clock_ms > 0, snap.step_count > 0) == (1, true, true));
    }

    #[test]
    fn interactive_partition_then_heal_keeps_one_leader() {
        let mut sim = Sim::new(&[NodeId(1), NodeId(2), NodeId(3)]);
        step_until(&mut sim, 10_000, |s| !s.leaders().is_empty());
        sim.run_until_stable(10_000);
        let old = sim.leaders()[0];

        sim.partition(old);
        step_until(&mut sim, 10_000, |s| s.leaders().iter().any(|&l| l != old));
        sim.run_until_stable(10_000);

        sim.heal(old);
        sim.run_until_stable(10_000);
        assert2::assert!(sim.leaders().len() == 1);
    }

    #[test]
    fn drop_next_removes_a_message_and_records_it() {
        let mut sim = Sim::new(&[NodeId(1), NodeId(2), NodeId(3)]);
        // Fire the first timer so there is election traffic on the bus.
        while sim.in_flight().is_empty() && sim.step_once() {}
        let before = sim.in_flight().len();
        assert2::assert!(before > 0);

        let steps_before = sim.steps().len();
        check!(sim.drop_next());
        check!(sim.in_flight().len() == before - 1);
        // The drop is recorded on the timeline.
        check!(sim.steps().len() == steps_before + 1);
        let last = sim.steps().last().unwrap();
        assert2::assert!(matches!(last.action, TraceAction::Drop { .. }));
    }

    #[test]
    fn accessors_and_bus_faults_report_consistently() {
        let mut sim = Sim::new(&[NodeId(1), NodeId(2), NodeId(3)]);
        assert2::assert!(
            (sim.voter_ids(), sim.clock_ms()) == (vec![NodeId(1), NodeId(2), NodeId(3)], 0)
        );

        // Pump until there is election traffic, then exercise the bus-replay faults.
        while sim.in_flight().is_empty() && sim.step_once() {}
        assert2::assert!(!sim.in_flight().is_empty());
        assert2::assert!(sim.reorder() >= 1);

        // The logical clock advances as timers fire.
        sim.run_until_stable(10_000);
        assert2::assert!(sim.clock_ms() > 0);

        // duplicate_next is a no-op-safe replay when the bus has a message.
        while sim.in_flight().is_empty() && sim.step_once() {}
        if !sim.in_flight().is_empty() {
            assert2::assert!(sim.duplicate_next());
        }
    }

    #[test]
    fn append_targets_the_current_leader() {
        let mut sim = Sim::new(&[NodeId(1), NodeId(2), NodeId(3)]);
        // No leader yet -> append is a no-op.
        assert2::assert!(!sim.append(2));

        step_until(&mut sim, 10_000, |s| !s.leaders().is_empty());
        sim.run_until_stable(10_000);
        let leader = sim.leaders()[0];
        let before = sim
            .snapshot()
            .nodes
            .iter()
            .find(|n| n.id == leader.0)
            .map_or(0, |n| n.log_len);

        assert2::assert!(sim.append(2));
        let after = sim
            .snapshot()
            .nodes
            .iter()
            .find(|n| n.id == leader.0)
            .map_or(0, |n| n.log_len);
        assert2::assert!(after == before + 2);
    }
}
