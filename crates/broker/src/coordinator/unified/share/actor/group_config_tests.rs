//! Unit tests for the per-group `share.*` overrides of a share group's
//! session timeout, heartbeat interval and assignment interval: Kafka's
//! `GroupMetadataManager.shareGroupSessionTimeoutMs`,
//! `shareGroupHeartbeatIntervalMs` and `shareGroupAssignmentIntervalMs`.

// A metadata image that holds each group config in `overrides`: a group id
// and its `share.*` entries.
use {
    super::{
        ShareGroupActorHandle,
        test_support::{heartbeat, seed_initialized},
    },
    crate::{
        coordinator::unified::{
            GroupCoordinator,
            share::{actor::test_support::TopicMetadataSetup, config::ShareGroupConfig},
            test_support::{
                check_group_assignment_timing, check_group_session_timeout, fixed_source,
                group_config_image as group_image,
            },
        },
        test_support::FakeMetadataSource,
    },
    assert2::{assert, check},
    krabka_protocol::owned::{
        share_group_heartbeat_request::ShareGroupHeartbeatRequest,
        share_group_heartbeat_response::ShareGroupHeartbeatResponse,
    },
    std::{sync::Arc, time::Duration},
};

/// A coordinator, and the id of the topic `t` it runs over.
struct TopicCoordinator {
    coordinator: Arc<GroupCoordinator>,
    topic_id: krabka_protocol::primitives::uuid::Uuid,
}

/// A coordinator over topic `t` with four partitions, whose metadata image
/// holds each group config in `overrides`: a group id and its `share.*`
/// entries. It returns the id of `t`.
fn coordinator(
    config: ShareGroupConfig,
    overrides: &[(&str, &[(&str, &str)])],
) -> TopicCoordinator {
    let (coordinator, topic_id) = super::test_support::topic_coordinator()
        .name(TopicMetadataSetup::four_partitions().name)
        .partitions(TopicMetadataSetup::four_partitions().partitions)
        .share(config)
        .call();
    coordinator.set_metadata_source(fixed_source(group_image(overrides)));
    TopicCoordinator {
        coordinator,
        topic_id,
    }
}

/// A heartbeat is handled with the settings of the group config when it
/// arrives, not with those the actor read before it began to wait for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_heartbeat_runs_with_the_group_config_of_the_moment_it_arrives() {
    let (coordinator, _) = super::test_support::topic_coordinator()
        .name(TopicMetadataSetup::four_partitions().name)
        .partitions(TopicMetadataSetup::four_partitions().partitions)
        .share(ShareGroupConfig::assigning_at_once())
        .call();
    let source = Arc::new(FakeMetadataSource::builder().build());
    coordinator.set_metadata_source(source.clone());
    let handle = coordinator.get_or_create_share("g");
    let joined = join(&handle, "m1", 0).await;
    check!(joined.heartbeat_interval_ms == 5000);

    // The override lands while the actor waits for its next message.
    tokio::time::sleep(Duration::from_millis(100)).await;
    source.set_image(group_image(&[("g", &[(
        "share.heartbeat.interval.ms",
        "7000",
    )])]));

    assert!(
        join(&handle, "m1", joined.member_epoch)
            .await
            .heartbeat_interval_ms
            == 7000
    );
}

async fn join(
    handle: &ShareGroupActorHandle,
    member_id: &str,
    member_epoch: i32,
) -> ShareGroupHeartbeatResponse {
    heartbeat(handle, ShareGroupHeartbeatRequest {
        group_id: "g".into(),
        member_id: member_id.into(),
        member_epoch,
        subscribed_topic_names: Some(vec!["t".into()]),
        ..Default::default()
    })
    .await
}

/// `heartbeat_interval_ms` of the join response of each group: the group's
/// `share.heartbeat.interval.ms`, or the broker's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_reports_its_own_heartbeat_interval() {
    let TopicCoordinator { coordinator, .. } = coordinator(
        ShareGroupConfig::assigning_at_once(),
        &[("tuned", &[("share.heartbeat.interval.ms", "7000")])],
    );
    let mut reported = Vec::new();
    for group_id in ["tuned", "plain"] {
        let handle = coordinator.get_or_create_share(group_id);
        reported.push((group_id, join(&handle, "m1", 0).await.heartbeat_interval_ms));
    }
    assert!(reported == [("tuned", 7000), ("plain", 5000)]);
}

/// A member of a group with a `share.session.timeout.ms` of 1 ms is expired
/// by the group's next session tick, which follows the group's own heartbeat
/// interval, while a group without an override keeps its member for the
/// broker's 45 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_expires_a_member_by_its_own_session_timeout() {
    // The broker's bounds admit the 1 ms and the 10 ms that the group asks for,
    // which the group's own overrides are capped to.
    let TopicCoordinator { coordinator, .. } = coordinator(
        ShareGroupConfig {
            min_session_timeout: Duration::from_millis(1),
            min_heartbeat_interval: Duration::from_millis(10),
            ..ShareGroupConfig::assigning_at_once()
        },
        &[("brief", &[
            ("share.session.timeout.ms", "1"),
            ("share.heartbeat.interval.ms", "10"),
        ])],
    );
    check_group_session_timeout(|id| coordinator.get_or_create_share(id), join).await;
}

/// Kafka's `canComputeNextTargetAssignment`: a second member that joins
/// within the assignment interval of the first assignment joins at the epoch
/// of that assignment, and the group assigns for it when the interval has
/// elapsed. A group whose `share.assignment.interval.ms` is 0 assigns at
/// once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_group_assigns_no_sooner_than_its_own_assignment_interval() {
    let TopicCoordinator {
        coordinator,
        topic_id,
    } = coordinator(
        ShareGroupConfig {
            assignment_interval: Duration::from_mins(1),
            ..ShareGroupConfig::default()
        },
        &[
            ("fast", &[("share.assignment.interval.ms", "0")]),
            ("paced", &[("share.assignment.interval.ms", "300")]),
        ],
    );
    check_group_assignment_timing(
        async |id| {
            let handle = coordinator.get_or_create_share(id);
            seed_initialized()
                .handle(&handle)
                .topic_id(topic_id)
                .partitions(vec![
                    krabka_ids::PartitionIndex(0),
                    krabka_ids::PartitionIndex(1),
                    krabka_ids::PartitionIndex(2),
                    krabka_ids::PartitionIndex(3),
                ])
                .call()
                .await;
            handle
        },
        join,
        |response| {
            check!(response.member_epoch == 3);
        },
    )
    .await;
}
