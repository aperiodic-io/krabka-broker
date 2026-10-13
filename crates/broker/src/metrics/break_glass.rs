//! KFC-9 freeze and break-glass accounting: the per-topic produce-refusal
//! counter, the freeze-registry gauge, and the proposal, refusal and bypass
//! families that report on the two-person rule.

use {
    super::{
        BreakGlassAction, BreakGlassActionLabel, BreakGlassState, BreakGlassStateLabel,
        BrokerMetrics,
    },
    std::sync::Arc,
};

impl BrokerMetrics {
    /// KFC-9: account one Produce partition row the broker refused because a
    /// freeze covers `topic`.
    ///
    /// The produce gate calls it once for each refused row, so one request
    /// that names three partitions of a frozen topic makes three calls.
    /// `topic` comes from a name that resolved in the metadata image, and the
    /// series count is bounded by the number of topics a freeze covers.
    pub fn record_topic_freeze_rejection(&self, topic: &str) {
        self.count_topic(&self.topic_freeze_rejections, Arc::from(topic), 1);
    }

    /// KFC-9: publish the number of live entries in the freeze registry.
    ///
    /// The metadata-image watcher calls it after an apply changes the
    /// registry, so the gauge falls when a thaw removes an entry.
    pub fn record_topic_freezes_active(&self, entries: i64) {
        self.topic_freezes_active.set(entries);
    }

    /// KFC-9: publish the number of break-glass proposals in `state`.
    ///
    /// The caller publishes one value for each [`BreakGlassState`], so a
    /// proposal that moves from `Pending` to `Consumed` lowers one series and
    /// raises another.
    pub fn record_break_glass_proposals(&self, state: BreakGlassState, count: i64) {
        let lbl = BreakGlassStateLabel { state };
        self.break_glass_proposals.get_or_create(&lbl).set(count);
    }

    /// KFC-9: account one privileged transition the broker refused because no
    /// approved break-glass proposal covers `action`.
    pub fn record_break_glass_refusal(&self, action: BreakGlassAction) {
        let lbl = BreakGlassActionLabel { action };
        self.break_glass_refusals.get_or_create(&lbl).inc();
    }

    /// KFC-9: account one privileged transition that ran **without** an
    /// approved break-glass proposal.
    ///
    /// This is the series an operator alerts on. It counts a data-losing
    /// transition that no second person approved: the background
    /// unclean-recovery path has no caller to refuse, so the `audit-only`
    /// policy lets recovery run and calls this method instead of failing
    /// closed. A gated transition that an operator ran with an approval never
    /// reaches here.
    pub fn record_break_glass_bypass(&self, action: BreakGlassAction) {
        let lbl = BreakGlassActionLabel { action };
        self.break_glass_bypassed.get_or_create(&lbl).inc();
    }
}

#[cfg(test)]
mod tests {
    use {super::*, crate::metrics::TopicLabel, assert2::assert};

    #[test]
    fn topic_freeze_rejections_accumulate_per_topic() {
        // KFC-9: the produce gate bumps once per refused partition row, and a
        // topic no freeze covers keeps a flat series.
        let m = BrokerMetrics::new();
        m.record_topic_freeze_rejection("orders");
        m.record_topic_freeze_rejection("orders");
        m.record_topic_freeze_rejection("payments");

        let cases = [("orders", 2), ("payments", 1), ("unfrozen", 0)];
        for (topic, want) in cases {
            let lbl = TopicLabel {
                topic: Arc::from(topic),
            };
            // One `get_or_create` guard per statement (first
            // materialization takes the family write lock).
            let got = m.topic_freeze_rejections.get_or_create(&lbl).get();
            assert!(got == want, "freeze rejections for {topic}");
        }
    }

    #[test]
    fn kfc9_gauges_fall_as_well_as_rise() {
        // A thaw removes a registry entry and consumes the proposal that
        // authorized it, so both gauges have to come back down.
        let m = BrokerMetrics::new();
        let pending = BreakGlassStateLabel {
            state: BreakGlassState::Pending,
        };
        let consumed = BreakGlassStateLabel {
            state: BreakGlassState::Consumed,
        };

        m.record_topic_freezes_active(3);
        assert!(m.topic_freezes_active.get() == 3);
        m.record_topic_freezes_active(1);
        assert!(m.topic_freezes_active.get() == 1);
        m.record_topic_freezes_active(0);
        assert!(m.topic_freezes_active.get() == 0);

        m.record_break_glass_proposals(BreakGlassState::Pending, 2);
        m.record_break_glass_proposals(BreakGlassState::Consumed, 5);
        // One `get_or_create` guard per statement (first materialization
        // takes the family write lock).
        let up = m.break_glass_proposals.get_or_create(&pending).get();
        assert!(up == 2);

        m.record_break_glass_proposals(BreakGlassState::Pending, 0);
        m.record_break_glass_proposals(BreakGlassState::Consumed, 6);
        let down = m.break_glass_proposals.get_or_create(&pending).get();
        assert!(down == 0);
        let rose = m.break_glass_proposals.get_or_create(&consumed).get();
        assert!(rose == 6);
    }
}
