//! The heartbeat session of a broker: how it opens, how it is refreshed, and
//! how it expires.
//!
//! Recording a heartbeat, expiring a stale entry on the liveness tick, and
//! seeding or discovering an entry are the three ways a session's clock moves,
//! so they sit together and the edge transitions they emit are compared in one
//! place.

use super::{
    BrokerLivenessState, ControllerLivenessState, LivenessTransition, registry::BrokerEntry,
};

impl ControllerLivenessState {
    /// Record a heartbeat from `broker_id`. A broker that has no existing
    /// session starts fenced until the handler confirms metadata catch-up.
    pub(crate) async fn record_fenced_heartbeat(
        &self,
        broker_id: u64,
    ) -> Option<LivenessTransition> {
        self.record_heartbeat_inner(broker_id, true).await
    }

    #[cfg(test)]
    pub(crate) async fn record_heartbeat(&self, broker_id: u64) -> Option<LivenessTransition> {
        self.record_heartbeat_inner(broker_id, false).await
    }

    async fn record_heartbeat_inner(
        &self,
        broker_id: u64,
        initially_fenced: bool,
    ) -> Option<LivenessTransition> {
        let mut map = self.brokers.lock().await;
        let now = self.clock.now();
        let entry = map
            .entry(broker_id)
            .or_insert(BrokerEntry::new(now, initially_fenced, true));
        let prev = entry.state;
        entry.last_heartbeat = now;
        entry.state = BrokerLivenessState::Alive;
        entry.contact = true;
        if prev == BrokerLivenessState::Dead {
            tracing::info!(
                broker_id,
                "broker liveness: DEAD -> ALIVE (heartbeat resumed)"
            );
            Some(LivenessTransition::DeadToAlive(broker_id))
        } else {
            None
        }
    }

    /// Scan all registered brokers and mark those that have not sent a
    /// heartbeat within `timeout` as `Dead`. Returns the list of
    /// transitions that occurred this tick.
    pub(crate) async fn tick(&self) -> Vec<LivenessTransition> {
        let mut map = self.brokers.lock().await;
        let now = self.clock.now();
        let mut transitions = Vec::new();
        for (&id, entry) in map.iter_mut() {
            if entry.state == BrokerLivenessState::Alive
                && now.duration_since(entry.last_heartbeat) > self.timeout
            {
                entry.state = BrokerLivenessState::Dead;
                tracing::warn!(
                    broker_id = id,
                    since_last_heartbeat_ms =
                        u64::try_from(now.duration_since(entry.last_heartbeat).as_millis())
                            .unwrap_or(u64::MAX),
                    timeout_ms = u64::try_from(self.timeout.as_millis()).unwrap_or(u64::MAX),
                    "broker liveness: ALIVE -> DEAD (heartbeat session timeout) — triggers partition-leader failover"
                );
                transitions.push(LivenessTransition::AliveToDead(id));
            }
        }
        transitions
    }

    /// Start a session for every broker in `broker_ids` that the registry does
    /// not know yet. Each new entry starts `Alive` with `last_heartbeat =
    /// now`, so the broker gets one full timeout window to send its first
    /// heartbeat. It also starts fenced, as a first heartbeat would leave it:
    /// a broker that has not yet proved metadata catch-up must not be elected
    /// or receive replicas, and only a caught-up heartbeat
    /// ([`touch`](Self::touch)) lifts the fence. Known entries keep their state,
    /// their fence, and their death clock.
    ///
    /// The controller leader calls this on every liveness tick with the
    /// brokers registered in the metadata image. Without it the registry
    /// only knows brokers that heartbeated this controller or that a
    /// leadership change seeded. A broker that registers and dies before its
    /// first heartbeat reaches this controller would then never expire, and
    /// the partitions it leads would never fail over.
    pub(crate) async fn track_registered(&self, broker_ids: impl IntoIterator<Item = u64>) {
        let mut map = self.brokers.lock().await;
        let now = self.clock.now();
        for id in broker_ids {
            map.entry(id).or_insert(BrokerEntry::new(now, true, false));
        }
    }

    /// Seed the liveness registry when this node becomes the controller
    /// leader. `brokers` holds every registered broker with the fence and
    /// controlled shutdown its registration replays to. Every entry becomes
    /// `Alive` with `last_heartbeat = now`, so live peers get a full timeout
    /// window to redirect their heartbeat loop at the new controller, and
    /// [`tick`](Self::tick) still detects dead peers after `timeout`.
    ///
    /// This is `ClusterControlManager.activate`: a broker keeps the fence the
    /// cluster replicated for it, and only an unfenced one starts with a
    /// heartbeat session. A broker the previous controller had fenced has no
    /// session until it heartbeats, so a new incarnation of it may register.
    ///
    /// An unfenced registration in controlled shutdown starts in controlled
    /// shutdown, so nothing elects it (Kafka's `ClusterControlManager.isActive`
    /// reads the registration). Its shutdown offset is unknown to this term, so
    /// it is -1: its next heartbeat drains any leadership it still holds and
    /// moves the offset to the end of that drain, or, if it leads nothing, lets
    /// it shut down, as Kafka's first heartbeat after a failover does. A fenced
    /// registration leaves controlled shutdown, as Kafka's
    /// `BrokerHeartbeatManager.touch` does.
    pub(crate) async fn seed_brokers(
        &self,
        brokers: impl IntoIterator<Item = super::ReplicatedRegistration>,
    ) {
        let mut map = self.brokers.lock().await;
        let now = self.clock.now();
        for replicated in brokers {
            let entry = map.entry(replicated.node_id).or_insert(BrokerEntry::new(
                now,
                replicated.fenced,
                !replicated.fenced,
            ));
            entry.last_heartbeat = now;
            entry.state = BrokerLivenessState::Alive;
            entry.fenced = entry.fenced || replicated.fenced;
            entry.contact = !entry.fenced;
            if entry.fenced {
                entry.controlled_shutdown_offset = None;
            } else if replicated.in_controlled_shutdown {
                entry.controlled_shutdown_offset.get_or_insert(-1);
            }
        }
    }

    /// [`seed_brokers`](Self::seed_brokers) once per controller term.
    ///
    /// The leadership watcher, the first liveness tick of a term, and a
    /// controller request that reads the registry all call it, and the first
    /// one seeds. A request served right after this node became the leader
    /// then never reads a registry left over from an earlier term, whichever
    /// task runs first. With no known term every call seeds.
    pub(crate) async fn seed_term(
        &self,
        term: Option<u64>,
        brokers: impl IntoIterator<Item = super::ReplicatedRegistration>,
    ) {
        use std::sync::atomic::Ordering;
        // One registration turn at a time decides whether to seed, so two
        // callers in the same new term cannot both seed.
        let _turn = self.registrations.lock().await;
        if term.is_some_and(|term| self.seeded_term.load(Ordering::Acquire) == term) {
            return;
        }
        self.seed_brokers(brokers).await;
        if let Some(term) = term {
            self.seeded_term.store(term, Ordering::Release);
        }
    }

    /// Whether `broker_id` still holds a heartbeat session: the broker was in
    /// contact with this controller within the timeout. Kafka's
    /// `BrokerHeartbeatManager.hasValidSession`, which
    /// `ClusterControlManager.registerBroker` asks before it refuses a new
    /// incarnation with `DUPLICATE_BROKER_REGISTRATION`.
    ///
    /// Fencing does not matter here. A fenced broker that still heartbeats is
    /// still running, and a second process must not take its id.
    pub(crate) async fn has_valid_session(&self, broker_id: u64) -> bool {
        let map = self.brokers.lock().await;
        let now = self.clock.now();
        map.get(&broker_id).is_some_and(|entry| {
            entry.contact
                && entry.state == BrokerLivenessState::Alive
                && now.saturating_duration_since(entry.last_heartbeat) <= self.timeout
        })
    }

    /// End the session of `broker_id` without touching its fence or its
    /// death clock, as if the broker stopped heartbeating long enough ago.
    #[cfg(test)]
    pub(crate) async fn end_session(&self, broker_id: u64) {
        if let Some(entry) = self.brokers.lock().await.get_mut(&broker_id) {
            entry.contact = false;
        }
    }

    /// Wait for the turn to decide one broker registration. Kafka's controller
    /// decides registrations one at a time on its event thread; the holder
    /// checks the session, commits the registration and replaces the session
    /// before the next registration may look at any of them.
    ///
    /// Registrations are rare, and the guard is a `tokio` mutex, so a waiter
    /// yields its worker thread and no heartbeat waits on it.
    pub(crate) async fn registration_turn(&self) -> tokio::sync::MutexGuard<'_, ()> {
        self.registrations.lock().await
    }

    /// Replace the session of the previous incarnation of `broker_id` with a
    /// fresh entry for the new one. Kafka removes the old session and
    /// registers the new incarnation with `Fenced = true`.
    ///
    /// The new entry is alive and fenced, with no session: the new process
    /// gets one full timeout window for its first heartbeat, nothing treats
    /// it as the dead process it replaced, and nothing elects it before it
    /// catches up to its new registration record.
    pub(crate) async fn replace_incarnation(&self, broker_id: u64) {
        let now = self.clock.now();
        self.brokers
            .lock()
            .await
            .insert(broker_id, BrokerEntry::new(now, true, false));
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use assert2::assert;

    use super::*;
    use crate::heartbeat::controller_state::{
        BrokerControlState, ReplicatedRegistration, TestClock,
    };

    /// krabka-io/krabka-broker#825: a controller that takes over, a restarted
    /// one included, starts every broker from the fence and controlled
    /// shutdown its registration replays to, as
    /// `ClusterControlManager.activate` registers each replayed
    /// `BrokerRegistration` with the heartbeat manager.
    #[tokio::test]
    async fn a_new_term_starts_from_the_replayed_registration() {
        let cases = [
            (
                "unfenced",
                ReplicatedRegistration::unfenced(1),
                (BrokerControlState::Unfenced, true, true, None),
            ),
            (
                "fenced",
                ReplicatedRegistration::fenced(1),
                (BrokerControlState::Fenced, false, false, None),
            ),
            (
                "in controlled shutdown",
                ReplicatedRegistration {
                    in_controlled_shutdown: true,
                    ..ReplicatedRegistration::unfenced(1)
                },
                (
                    BrokerControlState::ControlledShutdown,
                    false,
                    true,
                    Some(-1),
                ),
            ),
            (
                "fenced in controlled shutdown",
                ReplicatedRegistration {
                    in_controlled_shutdown: true,
                    ..ReplicatedRegistration::fenced(1)
                },
                (BrokerControlState::Fenced, false, false, None),
            ),
        ];
        for (what, replicated, want) in cases {
            let liveness = ControllerLivenessState::new(krabka_units::secs(10));

            liveness.seed_term(Some(1), [replicated]).await;

            assert2::check!(
                (
                    liveness.control_state(1).await,
                    liveness.is_alive(1).await,
                    liveness.has_valid_session(1).await,
                    liveness.controlled_shutdown_offset(1).await,
                ) == want,
                "{what}"
            );
        }
    }

    #[tokio::test]
    async fn new_broker_starts_alive_after_first_heartbeat() {
        let liveness = ControllerLivenessState::new(krabka_units::secs(10));
        let transition = liveness.record_heartbeat(1).await;
        assert!(transition == None); // first heartbeat: not a revival
        assert!(liveness.state(1).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn tick_marks_expired_broker_dead() {
        let (_clock, liveness, transitions) = crate::test_support::expired_broker_fixture(2).await;
        assert!(transitions == vec![LivenessTransition::AliveToDead(2)]);
        assert!(liveness.state(2).await == Some(BrokerLivenessState::Dead));
    }

    #[tokio::test]
    async fn heartbeat_after_dead_emits_revival() {
        let clock = TestClock::new();
        let liveness =
            ControllerLivenessState::with_clock(Duration::from_millis(10), clock.clock());
        liveness.record_heartbeat(3).await;
        clock.advance(Duration::from_millis(11));
        let _ = liveness.tick().await; // broker 3 → Dead
        let transition = liveness.record_heartbeat(3).await;
        assert!(transition == Some(LivenessTransition::DeadToAlive(3)));
        assert!(liveness.state(3).await == Some(BrokerLivenessState::Alive));
    }

    #[tokio::test]
    async fn track_registered_adds_unknown_brokers_and_keeps_known_state() {
        let clock = TestClock::new();
        let liveness = ControllerLivenessState::with_test_clock(Duration::from_millis(10), &clock);
        // Broker 1 is known and expires.
        liveness.record_heartbeat(1).await;
        clock.advance(Duration::from_millis(11));
        assert!(liveness.tick().await == vec![LivenessTransition::AliveToDead(1)]);

        // Broker 1 keeps its dead state. Broker 2 starts a fresh session that
        // is fenced until it proves catch-up: not dead, not electable, and
        // unavailable for new replicas.
        liveness.track_registered([1, 2]).await;
        assert!(liveness.dead_snapshot().await == [1].into_iter().collect());
        assert!(!liveness.is_alive(2).await);
        assert!(liveness.unavailable_snapshot().await.contains(&2));

        // A caught-up heartbeat lifts the fence and only then makes it alive.
        liveness.record_fenced_heartbeat(2).await;
        assert!(!liveness.apply_fencing(2, false, true).await);
        assert!(liveness.is_alive(2).await);

        // Broker 3 is discovered and never heartbeats. It expires one full
        // window later, while broker 2 keeps heartbeating.
        liveness.track_registered([3]).await;
        clock.advance(Duration::from_millis(11));
        liveness.record_fenced_heartbeat(2).await;
        assert!(liveness.tick().await == vec![LivenessTransition::AliveToDead(3)]);
        assert!(liveness.dead_snapshot().await == [1, 3].into_iter().collect());
    }

    #[tokio::test]
    async fn track_registered_does_not_refresh_a_stale_session() {
        let clock = TestClock::new();
        let liveness = ControllerLivenessState::with_test_clock(Duration::from_millis(10), &clock);
        liveness.record_heartbeat(5).await;
        clock.advance(Duration::from_millis(9));

        // Unlike `seed_brokers`, a track call must not extend the window.
        liveness.track_registered([5]).await;
        clock.advance(Duration::from_millis(2));

        assert!(liveness.tick().await == vec![LivenessTransition::AliveToDead(5)]);
    }

    #[tokio::test]
    async fn seed_moves_dead_broker_out_of_dead_snapshot() {
        let clock = TestClock::new();
        let liveness = ControllerLivenessState::with_test_clock(Duration::from_millis(10), &clock);
        liveness.record_heartbeat(4).await;
        clock.advance(Duration::from_millis(11));
        let _ = liveness.tick().await;
        assert!(liveness.dead_snapshot().await.contains(&4));

        liveness
            .seed_brokers([ReplicatedRegistration::unfenced(4)])
            .await;

        assert!(liveness.dead_snapshot().await.is_empty());
    }

    #[tokio::test]
    async fn normal_seed_gives_new_and_existing_brokers_a_full_timeout_window() {
        for (timeout_ms, stale_heartbeat_ms) in [(50, None), (10, Some(20))] {
            let clock = TestClock::new();
            let liveness = ControllerLivenessState::with_clock(
                Duration::from_millis(timeout_ms),
                clock.clock(),
            );
            if let Some(age_ms) = stale_heartbeat_ms {
                liveness.record_heartbeat(7).await;
                clock.advance(Duration::from_millis(age_ms));
            }
            // A re-seed refreshes even a stale entry to a full timeout window.
            liveness
                .seed_brokers([ReplicatedRegistration::unfenced(7)])
                .await;
            clock.advance(Duration::from_millis(1));
            let transitions = liveness.tick().await;
            assert!(transitions.is_empty());
            assert!(liveness.state(7).await == Some(BrokerLivenessState::Alive));
        }
    }

    /// krabka-io/krabka-broker#822: a broker id is held against a new
    /// incarnation only while the broker is in contact, as Kafka's
    /// `BrokerHeartbeatTracker.hasValidSession` decides.
    #[tokio::test]
    async fn a_session_is_valid_only_while_the_broker_is_in_contact() {
        const TIMEOUT: Duration = Duration::from_millis(10);
        const BROKER: u64 = 7;

        /// What the registry saw of the broker, and when.
        #[derive(Debug, Clone, Copy)]
        enum Step {
            Heartbeat,
            FencedHeartbeat,
            Fence,
            Discover,
            Seed,
            SeedFenced,
            ReplaceIncarnation,
            Advance(u64),
            Tick,
        }
        use Step::{
            Advance, Discover, Fence, FencedHeartbeat, Heartbeat, ReplaceIncarnation, Seed,
            SeedFenced, Tick,
        };

        let cases: &[(&str, &[Step], bool)] = &[
            ("never seen", &[], false),
            ("a heartbeat", &[Heartbeat], true),
            (
                "a heartbeat at the edge of the timeout",
                &[Heartbeat, Advance(10)],
                true,
            ),
            (
                "a heartbeat past the timeout, before the tick",
                &[Heartbeat, Advance(11)],
                false,
            ),
            (
                "a heartbeat past the timeout, after the tick",
                &[Heartbeat, Advance(11), Tick],
                false,
            ),
            (
                "a fenced broker that still heartbeats",
                &[FencedHeartbeat],
                true,
            ),
            (
                "a broker fenced on request that still heartbeats",
                &[Heartbeat, Fence],
                true,
            ),
            ("a broker only discovered in the image", &[Discover], false),
            (
                "an unfenced broker seeded by a new controller",
                &[Seed],
                true,
            ),
            (
                "a fenced broker seeded by a new controller",
                &[FencedHeartbeat, Seed],
                false,
            ),
            (
                "a broker the image replicates as fenced",
                &[SeedFenced],
                false,
            ),
            (
                "a heartbeating broker the image replicates as fenced",
                &[Heartbeat, SeedFenced],
                false,
            ),
            (
                "a new incarnation replaced the session",
                &[Heartbeat, ReplaceIncarnation],
                false,
            ),
            (
                "the new incarnation heartbeats",
                &[Heartbeat, ReplaceIncarnation, FencedHeartbeat],
                true,
            ),
            (
                "a dead broker revived by a heartbeat",
                &[Heartbeat, Advance(11), Tick, Heartbeat],
                true,
            ),
        ];
        for (what, steps, valid) in cases {
            let clock = TestClock::new();
            let liveness = ControllerLivenessState::with_test_clock(TIMEOUT, &clock);
            for step in *steps {
                match step {
                    Heartbeat => {
                        liveness.record_heartbeat(BROKER).await;
                    }
                    FencedHeartbeat => {
                        liveness.record_fenced_heartbeat(BROKER).await;
                    }
                    Fence => {
                        liveness.apply_fencing(BROKER, true, true).await;
                    }
                    Discover => liveness.track_registered([BROKER]).await,
                    Seed => {
                        liveness
                            .seed_brokers([ReplicatedRegistration::unfenced(BROKER)])
                            .await;
                    }
                    SeedFenced => {
                        liveness
                            .seed_brokers([ReplicatedRegistration::fenced(BROKER)])
                            .await;
                    }
                    ReplaceIncarnation => liveness.replace_incarnation(BROKER).await,
                    Advance(millis) => clock.advance(Duration::from_millis(*millis)),
                    Tick => {
                        liveness.tick().await;
                    }
                }
            }
            assert2::check!(liveness.has_valid_session(BROKER).await == *valid, "{what}");
        }
    }

    /// A new incarnation starts alive, fenced and without a session, whatever
    /// its previous incarnation had reached: nothing elects it before it
    /// catches up, and nothing treats it as the dead process it replaced.
    #[tokio::test]
    async fn a_new_incarnation_starts_alive_fenced_and_without_a_session() {
        const TIMEOUT: Duration = Duration::from_millis(10);

        // (what the previous incarnation reached, is it past the timeout)
        let cases = [("alive and unfenced", false), ("dead", true)];
        for (what, expired) in cases {
            let clock = TestClock::new();
            let liveness = ControllerLivenessState::with_test_clock(TIMEOUT, &clock);
            liveness.record_heartbeat(7).await;
            if expired {
                clock.advance(Duration::from_millis(11));
                liveness.tick().await;
            }

            liveness.replace_incarnation(7).await;

            assert2::check!(
                (
                    liveness.state(7).await,
                    liveness.is_alive(7).await,
                    liveness.unavailable_snapshot().await.contains(&7),
                    liveness.has_valid_session(7).await,
                ) == (Some(BrokerLivenessState::Alive), false, true, false),
                "{what}"
            );
            // It gets a whole window for its first heartbeat.
            clock.advance(Duration::from_millis(10));
            assert2::check!(liveness.tick().await == vec![], "{what}");
        }
    }

    /// A term is seeded once, whichever caller comes first, and a new term is
    /// seeded again.
    #[tokio::test]
    async fn a_term_is_seeded_once() {
        let clock = TestClock::new();
        let liveness = ControllerLivenessState::with_test_clock(Duration::from_millis(10), &clock);

        liveness
            .seed_term(Some(3), [ReplicatedRegistration::unfenced(1)])
            .await;
        clock.advance(Duration::from_millis(11));
        // A second caller in term 3 does not refresh the window.
        liveness
            .seed_term(
                Some(3),
                [
                    ReplicatedRegistration::unfenced(1),
                    ReplicatedRegistration::unfenced(2),
                ],
            )
            .await;
        assert!(liveness.tick().await == vec![LivenessTransition::AliveToDead(1)]);
        assert!(liveness.state(2).await == None);

        // Term 4 seeds again.
        liveness
            .seed_term(
                Some(4),
                [
                    ReplicatedRegistration::unfenced(1),
                    ReplicatedRegistration::unfenced(2),
                ],
            )
            .await;
        assert!(liveness.alive_snapshot().await == [1, 2].into_iter().collect());
    }

    #[tokio::test]
    async fn tick_does_not_expire_recently_heartbeated_broker() {
        let liveness = ControllerLivenessState::new(krabka_units::minutes(1));
        liveness.record_heartbeat(4).await;
        let transitions = liveness.tick().await;
        assert!(
            transitions.is_empty(),
            "broker 4 should not expire with 60s timeout"
        );
        assert!(liveness.state(4).await == Some(BrokerLivenessState::Alive));
    }
}
