//! Tests for one liveness tick: the discovery step that starts a session for
//! a registered broker that never heartbeated, the follower that must track
//! nothing, the first tick of a new term that seeds before it sweeps, and the
//! fencing state each tick publishes for the nodes that do not hold the
//! registry.

use assert2::assert;

use super::*;
use crate::{
    heartbeat::controller_state::TestClock,
    leader_election::test_support::{
        ElectionSetup, fake_source, fencing_updates, img_with_partition, one_partition_change,
        partition_batches, recovery_handle_for_tests, register_brokers,
    },
};

struct TickFixture {
    source: Arc<crate::test_support::FakeMetadataSource>,
    clock: TestClock,
    liveness: Arc<ControllerLivenessState>,
    metrics: crate::metrics::BrokerMetrics,
    recovery: crate::unclean_recovery::UncleanRecoveryHandle,
    state: LivenessTickState,
}

impl TickFixture {
    fn new(image: MetadataImage, leader: u64, was_leader: bool) -> Self {
        let clock = TestClock::new();
        Self {
            source: fake_source(image, Some(NodeId(leader))),
            liveness: Arc::new(ControllerLivenessState::with_test_clock(
                std::time::Duration::from_millis(10),
                &clock,
            )),
            clock,
            metrics: crate::metrics::BrokerMetrics::new(),
            recovery: recovery_handle_for_tests(),
            state: LivenessTickState {
                was_leader,
                ..Default::default()
            },
        }
    }

    fn registered_partition(leader: u64, was_leader: bool) -> Self {
        let mut image = img_with_partition(ElectionSetup::default());
        register_brokers(&mut image, &[1, 2, 3]);
        Self::new(image, leader, was_leader)
    }

    async fn tick(&mut self) {
        let controller: Arc<dyn crate::metadata_source::MetadataSource> = self.source.clone();
        run_liveness_tick(
            &controller,
            NodeId(2),
            &self.liveness,
            &self.metrics,
            &self.recovery,
            &mut self.state,
        )
        .await;
    }
}

#[tokio::test]
async fn tick_discovers_registered_broker_that_never_heartbeated_and_fails_it_over() {
    // Broker 1 leads t-0 and dies before its first heartbeat reaches this
    // controller. Brokers 2 and 3 heartbeat as usual.
    let mut fixture = TickFixture::registered_partition(2, true);
    fixture.liveness.record_heartbeat(2).await;
    fixture.liveness.record_heartbeat(3).await;

    // First tick: discovery starts broker 1's session, fenced until it
    // proves catch-up. Nothing expires and nothing is submitted.
    fixture.tick().await;
    assert!(!fixture.liveness.is_alive(1).await);
    assert!(fixture.liveness.unavailable_snapshot().await.contains(&1));
    assert!(fixture.liveness.dead_snapshot().await.is_empty());
    let batches = fixture.source.submitted();
    assert!(partition_batches(&batches).is_empty());
    // Broker 1 is fenced until it proves catch-up, and the tick publishes
    // that so a follower-served response can see it.
    assert!(fencing_updates(&batches) == vec![(1, krabka_metadata::FencingChange::Fence)]);

    // One full window later brokers 2 and 3 heartbeated again. Broker 1
    // did not. The tick expires it and fails t-0 over to broker 2.
    fixture.clock.advance(std::time::Duration::from_millis(11));
    fixture.liveness.record_heartbeat(2).await;
    fixture.liveness.record_heartbeat(3).await;
    fixture.tick().await;

    let batches = partition_batches(&fixture.source.submitted());
    assert!(batches.len() == 1, "the edge submits once, got {batches:?}");
    let expected = crate::leader_election::test_support::expected_clean_election(
        krabka_raft::NodeId(2),
        &[krabka_raft::NodeId(2), krabka_raft::NodeId(3)],
        vec![],
    );
    assert!(*one_partition_change(&batches[0]) == expected);

    // The test source never applies the change, so the image still shows
    // broker 1 as leader. That models a lost commit. The next tick's
    // sweep re-drives the same failover.
    fixture.tick().await;
    let batches = partition_batches(&fixture.source.submitted());
    assert!(batches.len() == 2, "the sweep retries, got {batches:?}");
    assert!(*one_partition_change(&batches[1]) == expected);
}

#[tokio::test]
async fn tick_on_a_follower_tracks_nothing_and_submits_nothing() {
    let mut fixture = TickFixture::registered_partition(9, false);

    fixture.tick().await;
    fixture.clock.advance(std::time::Duration::from_millis(11));
    fixture.tick().await;

    // A follower does not receive heartbeats, so it must not start
    // sessions from the image. Otherwise every broker would look dead.
    assert!(fixture.liveness.dead_snapshot().await.is_empty());
    assert!(!fixture.liveness.is_alive(1).await);
    assert!(fixture.source.submitted().is_empty());
}

#[tokio::test]
async fn first_tick_of_a_new_term_seeds_before_it_sweeps() {
    // While this node was a follower it received no heartbeats, so its
    // registry expired every session. When it takes the lead, the first
    // tick must seed those brokers alive before any sweep can read the
    // stale dead set and fail over partitions whose leaders are healthy.
    let mut fixture = TickFixture::registered_partition(9, false);

    // Sessions from the previous term expire while node 2 follows.
    for broker in [1, 2, 3] {
        fixture.liveness.record_heartbeat(broker).await;
    }
    fixture.clock.advance(std::time::Duration::from_millis(11));
    fixture.tick().await;
    assert!(fixture.liveness.dead_snapshot().await == [1, 2, 3].into_iter().collect());

    // Node 2 takes the lead. The first tick of the term seeds every
    // registered broker alive and submits nothing.
    // `send_replace` does not need a live receiver: the tick subscribes
    // on demand and drops its receiver at once.
    fixture.source.set_leader(Some(NodeId(2)));
    fixture.tick().await;
    assert!(fixture.liveness.dead_snapshot().await.is_empty());
    assert!(fixture.liveness.is_alive(1).await);
    assert!(fixture.source.submitted().is_empty());

    // The seeded window is a real one: a broker that stays silent for a
    // full window afterwards still expires and fails over.
    fixture.clock.advance(std::time::Duration::from_millis(11));
    fixture.liveness.record_heartbeat(2).await;
    fixture.liveness.record_heartbeat(3).await;
    fixture.tick().await;
    assert!(fixture.liveness.dead_snapshot().await == [1].into_iter().collect());
    let batches = fixture.source.submitted();
    assert!(partition_batches(&batches).len() == 1);
    // The death is published in the same tick that detects it, so a
    // follower-served `Metadata` sees broker 1's replicas offline.
    assert!(fencing_updates(&batches) == vec![(1, krabka_metadata::FencingChange::Fence)]);
}

#[tokio::test]
async fn tick_leaves_the_unfence_of_a_returning_broker_to_its_heartbeat() {
    // Broker 3 is fenced in the image and alive again. Kafka unfences a
    // broker only in `processBrokerHeartbeat`, once the broker has caught up
    // to its registration, so the tick writes nothing for it.
    let mut img = img_with_partition(ElectionSetup {
        ..Default::default()
    });
    register_brokers(&mut img, &[1, 2, 3]);
    let fence = crate::heartbeat::fencing::registration_change(
        &img,
        NodeId(3),
        crate::heartbeat::fencing::RegistrationChange::FENCE,
    )
    .expect("broker 3 is registered and unfenced");
    img.apply(&fence);
    let mut fixture = TickFixture::new(img, 2, true);
    for broker in [1, 2, 3] {
        fixture.liveness.record_heartbeat(broker).await;
    }

    fixture.tick().await;

    let batches = fixture.source.submitted();
    assert!(partition_batches(&batches).is_empty());
    assert!(fencing_updates(&batches) == vec![]);
}
