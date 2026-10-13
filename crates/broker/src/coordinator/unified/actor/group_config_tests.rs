//! Unit tests for the per-group `consumer.*` overrides of a consumer group's
//! session timeout, heartbeat interval and assignment interval: Kafka's
//! `GroupMetadataManager.consumerGroupSessionTimeoutMs`,
//! `consumerGroupHeartbeatIntervalMs` and `consumerGroupAssignmentIntervalMs`.

use {
    super::{GroupActorHandle, test_support::StaticMetadata},
    crate::{
        codes,
        coordinator::unified::{
            GroupCoordinator,
            actor::test_support::rpc::consumer_request_as_client,
            config::NextGenConfig,
            offsets_log::fake::InMemoryOffsetsLog,
            reconciler::ReconcileInput,
            share::config::ShareGroupConfig,
            streams::config::StreamsGroupConfig,
            test_support::{
                check_group_assignment_timing, check_group_session_timeout, fixed_source,
                group_config_image,
            },
        },
    },
    assert2::{assert, check},
    krabka_protocol::{
        owned::{
            consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest,
            consumer_group_heartbeat_response::ConsumerGroupHeartbeatResponse,
        },
        primitives::uuid::Uuid,
    },
    std::{sync::Arc, time::Duration},
};

/// A coordinator over topic `t` with two partitions, whose metadata image
/// holds each group config in `overrides`: a group id and its `consumer.*`
/// entries.
fn coordinator(
    config: NextGenConfig,
    overrides: &[(&str, &[(&str, &str)])],
) -> Arc<GroupCoordinator> {
    let topic_id = Uuid([7; 16]);
    let metadata = Arc::new(StaticMetadata {
        input: ReconcileInput {
            topic_id_by_name: [("t".to_owned(), topic_id)].into(),
            partitions_per_topic: [(topic_id, 2)].into(),
            ..Default::default()
        },
    });
    let coordinator = Arc::new(GroupCoordinator::new(
        config,
        ShareGroupConfig::assigning_at_once(),
        metadata,
        Arc::new(InMemoryOffsetsLog::default()),
        StreamsGroupConfig::default(),
    ));
    let image = group_config_image(overrides);
    coordinator.set_metadata_source(fixed_source(image));
    coordinator
}

async fn heartbeat(
    handle: &GroupActorHandle,
    member_id: &str,
    member_epoch: i32,
) -> ConsumerGroupHeartbeatResponse {
    consumer_request_as_client(handle, ConsumerGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        subscribed_topic_names: (member_epoch == 0).then(|| vec!["t".into()]),
        rebalance_timeout_ms: 60_000,
        topic_partitions: (member_epoch == 0).then(Vec::new),
        ..Default::default()
    })
    .await
}

/// `heartbeat_interval_ms` of the join response of each group: the group's
/// `consumer.heartbeat.interval.ms`, or the broker's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_reports_its_own_heartbeat_interval() {
    let coordinator = coordinator(NextGenConfig::assigning_at_once(), &[("tuned", &[(
        "consumer.heartbeat.interval.ms",
        "7000",
    )])]);
    let mut reported = Vec::new();
    for group_id in ["tuned", "plain"] {
        let handle = coordinator.get_or_create_consumer(group_id);
        reported.push((
            group_id,
            heartbeat(&handle, "m1", 0).await.heartbeat_interval_ms,
        ));
    }
    assert!(reported.as_slice() == [("tuned", 7000), ("plain", 5000)]);
}

/// A member of a group with a `consumer.session.timeout.ms` of 1 ms is
/// expired by the next session tick, while a group without one keeps its
/// member for the broker's 45 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_expires_a_member_by_its_own_session_timeout() {
    // The broker's minimum admits the 1 ms that the group asks for, which its
    // own override is capped to.
    let coordinator = coordinator(
        NextGenConfig {
            session_expiry_tick: Duration::from_millis(10),
            min_session_timeout: Duration::from_millis(1),
            ..NextGenConfig::assigning_at_once()
        },
        &[("brief", &[("consumer.session.timeout.ms", "1")])],
    );
    check_group_session_timeout(|id| coordinator.get_or_create_consumer(id), heartbeat).await;
}

/// Kafka's `canComputeNextTargetAssignment`: a second member that joins
/// within the assignment interval of the first assignment joins at the epoch
/// of that assignment, and the group assigns for it when the interval has
/// elapsed. A group whose `consumer.assignment.interval.ms` is 0 assigns at
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_assigns_no_sooner_than_its_own_assignment_interval() {
    let coordinator = coordinator(
        NextGenConfig {
            assignment_interval: Duration::from_mins(1),
            ..NextGenConfig::default()
        },
        &[
            ("fast", &[("consumer.assignment.interval.ms", "0")]),
            ("paced", &[("consumer.assignment.interval.ms", "300")]),
        ],
    );
    check_group_assignment_timing(
        async |id| coordinator.get_or_create_consumer(id),
        heartbeat,
        |response| {
            check!(response.error_code == codes::NONE);
        },
    )
    .await;
}
