//! KIP-966 end to end: an ISR that crosses `min.insync.replicas` moves the
//! ELR the controller keeps, and `DescribeTopicPartitions` reports the move.
//!
//! The write side and the read side meet only in the metadata log, so a test
//! that stops at either one proves nothing about the pair. These drive the
//! real `AlterPartition` handler on a live controller and read the answer back
//! through the real `DescribeTopicPartitions` handler, which is the path
//! `kafka-topics --describe` takes.

use std::sync::Arc;

use assert2::assert;
use krabka_metadata::{
    BrokerRegistrationRecord, LeaderEpoch, MetadataImage, MetadataRecord, NodeId, PartitionRecord,
};
use krabka_protocol::owned::{
    broker_registration_request::{BrokerRegistrationRequest, Listener},
    describe_topic_partitions_response::DescribeTopicPartitionsResponsePartition,
};

use super::{ElrPublisher, TopicElr, state::PartitionElr};
use crate::{
    broker::Broker,
    codes,
    test_support::{peer, principal, request_context},
};

const TOPIC: &str = "orders";
const TOPIC_ID_BYTES: [u8; 16] = [9; 16];
const LEADER_EPOCH: i32 = 7;
const DESCRIBE_VERSION: i16 =
    krabka_protocol::owned::describe_topic_partitions_response::MAX_VERSION;
const REGISTER_VERSION: i16 = krabka_protocol::owned::broker_registration_request::MAX_VERSION;

fn partition_record(isr: &[NodeId]) -> PartitionRecord {
    PartitionRecord {
        topic: TOPIC.into(),
        partition: 0,
        leader: NodeId(1),
        replicas: vec![NodeId(1), NodeId(2), NodeId(3)],
        isr: isr.to_vec(),
        leader_epoch: LeaderEpoch(LEADER_EPOCH),
        adding_replicas: vec![],
        removing_replicas: vec![],
        directories: vec![uuid::Uuid::nil(); 3],
        partition_epoch: 4,
    }
}

/// The records that put topic `orders` in the image: one RF=3 partition whose
/// ISR is full, and a `min.insync.replicas` of 2, which is what gives the
/// partition an ELR to fall below.
fn seed_records() -> Vec<MetadataRecord> {
    seed_records_with_min_isr("2")
}

/// [`seed_records`] with `min.insync.replicas` chosen by the caller. A value
/// equal to the replication factor is what makes the ISR shrink of a single
/// replica cross the threshold on its own.
fn seed_records_with_min_isr(min_isr: &str) -> Vec<MetadataRecord> {
    crate::test_support::elr_topic_records(
        partition_record(&[NodeId(1), NodeId(2), NodeId(3)]),
        uuid::Uuid::from_bytes(TOPIC_ID_BYTES),
        min_isr,
    )
}

#[derive(Clone, Copy)]
struct EndpointPort(u16);

/// The endpoint list of a remote broker at `port`: the `PLAINTEXT` listener that
/// the requests of these tests arrive on. A broker with no endpoint on that
/// listener is offline in `Metadata` and `DescribeTopicPartitions`, as it is in
/// Kafka's `KRaftMetadataCache`.
fn plaintext_endpoints(port: EndpointPort) -> Vec<krabka_metadata::BrokerEndpoint> {
    vec![krabka_metadata::BrokerEndpoint {
        name: "PLAINTEXT".into(),
        host: "127.0.0.1".into(),
        port: port.0,
        protocol: krabka_security::ListenerProtocol::Plaintext,
    }]
}

/// The registration record that puts broker 3 in the image under
/// `incarnation`. Without one, the registration handler reads the broker as
/// new rather than as one that has come back.
fn registration_record(incarnation: u128) -> MetadataRecord {
    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
        broker_epoch: -1,
        incarnation_id: uuid::Uuid::from_u128(incarnation),
        port: 9094,
        endpoints: plaintext_endpoints(EndpointPort(9094)),
        ..crate::test_support::broker_registration(krabka_raft::NodeId(3))
    })
}

/// Broker 3 is fenced, and its process has stopped: its heartbeat session is
/// over, so a new incarnation may take the id.
async fn mark_broker_3_unavailable(broker: &Broker) {
    broker.liveness.record_heartbeat(3).await;
    broker.liveness.apply_fencing(3, true, true).await;
    crate::test_support::end_heartbeat_session(broker, 3).await;
}

/// Register brokers 2 and 3 and make them active on the controller, so that
/// `AlterPartition` accepts them in an ISR as Kafka's
/// `ineligibleReplicasForIsr` does for a registered, unfenced broker.
async fn activate_followers(broker: &Broker) {
    let followers = [2_u64, 3];
    broker
        .controller
        .submit_change(
            followers
                .iter()
                .map(|&node| {
                    MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                        broker_epoch: -1,
                        incarnation_id: uuid::Uuid::from_u128(u128::from(node)),
                        port: 9092 + u16::try_from(node).expect("a small node id"),
                        endpoints: plaintext_endpoints(EndpointPort(
                            9092 + u16::try_from(node).expect("a small node id"),
                        )),
                        ..crate::test_support::broker_registration(krabka_raft::NodeId(node))
                    })
                })
                .collect(),
        )
        .await
        .expect("register the followers");
    for node in [1, 2, 3] {
        broker.liveness.record_fenced_heartbeat(node).await;
        broker
            .liveness
            .touch(
                node,
                crate::heartbeat::controller_state::BrokerControlState::Unfenced,
                0,
            )
            .await;
    }
}

/// The partition row `DescribeTopicPartitions` answers with for partition 0.
async fn describe_partition(broker: &Arc<Broker>) -> DescribeTopicPartitionsResponsePartition {
    let principal = principal("replica");
    let peer = peer();
    let ctx = request_context(&principal, &peer, "admin-client");
    let request = crate::test_support::topic_partitions_request(TOPIC);
    let response =
        crate::handlers::describe_topic_partitions::handle(broker, request, DESCRIBE_VERSION, &ctx)
            .await
            .expect("DescribeTopicPartitions");

    response
        .topics
        .into_iter()
        .find(|topic| topic.name.as_deref() == Some(TOPIC))
        .expect("orders topic row")
        .partitions
        .into_iter()
        .next()
        .expect("partition 0 row")
}

/// The expected partition row for an ISR of `isr` and the ELR of `eligible`.
///
/// Every node is registered and active ([`activate_followers`]), so none is
/// reported offline. Comparing the whole struct keeps the ELR assertion
/// honest: the ELR columns cannot be read as having moved because some
/// neighbouring field moved instead.
/// Read the complete wire row and compare it with the caller's independent expectation.
async fn check_partition(broker: &Arc<Broker>, expected: DescribeTopicPartitionsResponsePartition) {
    let actual = describe_partition(broker).await;
    assert!(actual == expected);
}

/// Expected row while both followers remain unavailable to the client listener.
fn offline_followers_row(setup: ExpectedElrSetup<'_>) -> DescribeTopicPartitionsResponsePartition {
    row(ExpectedElrSetup {
        offline: &[ReportedBrokerId(2), ReportedBrokerId(3)],
        ..setup
    })
}

#[derive(Clone, Copy)]
struct ReportedBrokerId(i32);

/// The expected wire state, including the last-known ELR and offline replicas.
/// The registration tests need the offline set: they register broker 3, which
/// takes it out of it.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ExpectedElrSetup<'a> {
    #[default(&[ReportedBrokerId(1), ReportedBrokerId(2), ReportedBrokerId(3)])]
    isr: &'a [ReportedBrokerId],
    eligible: &'a [ReportedBrokerId],
    last_known: &'a [ReportedBrokerId],
    offline: &'a [ReportedBrokerId],
}

impl ExpectedElrSetup<'_> {
    /// Brokers one and two in the ISR, with no eligible or last-known replicas.
    fn two_member_isr() -> Self {
        Self {
            isr: &[ReportedBrokerId(1), ReportedBrokerId(2)],
            ..Default::default()
        }
    }
}

fn row(setup: ExpectedElrSetup<'_>) -> DescribeTopicPartitionsResponsePartition {
    let ExpectedElrSetup {
        isr,
        eligible,
        last_known,
        offline,
    } = setup;
    DescribeTopicPartitionsResponsePartition {
        error_code: codes::NONE,
        partition_index: 0,
        leader_id: 1,
        leader_epoch: LEADER_EPOCH,
        replica_nodes: vec![1, 2, 3],
        isr_nodes: isr.iter().map(|id| id.0).collect(),
        eligible_leader_replicas: Some(eligible.iter().map(|id| id.0).collect()),
        last_known_elr: Some(last_known.iter().map(|id| id.0).collect()),
        offline_replicas: offline.iter().map(|id| id.0).collect(),
        ..Default::default()
    }
}

/// Register broker 3 under `incarnation` through the real
/// `BrokerRegistration` handler, and check that it accepts the incarnation.
///
/// The features are read back off the image so the request satisfies whatever
/// the cluster has finalized, which is what a real broker's
/// `SupportedFeatures` does.
async fn register_broker_3(broker: &Arc<Broker>, incarnation: u128) {
    let image = broker.controller.current_image();
    let features = crate::test_support::finalized_broker_features(&image);
    let request = BrokerRegistrationRequest {
        broker_id: 3,
        cluster_id: image.cluster_id().to_string(),
        incarnation_id: krabka_protocol::primitives::uuid::Uuid(
            *uuid::Uuid::from_u128(incarnation).as_bytes(),
        ),
        listeners: vec![Listener {
            name: "PLAINTEXT".into(),
            host: "127.0.0.1".into(),
            port: 9094,
            security_protocol: 0,
            ..Default::default()
        }],
        features,
        log_dirs: vec![krabka_protocol::primitives::uuid::Uuid(
            *uuid::Uuid::from_u128(3000).as_bytes(),
        )],
        ..Default::default()
    };
    let principal = principal("replica");
    let peer = peer();
    let ctx = request_context(&principal, &peer, "broker-client");
    let response =
        crate::handlers::broker_registration::handle(broker, request, REGISTER_VERSION, &ctx)
            .await
            .expect("BrokerRegistration");
    assert!(response.error_code == codes::NONE, "{response:?}");
}

async fn start_orders() -> (crate::BrokerHandle, tempfile::TempDir, Arc<Broker>) {
    let (handle, dir, broker) = crate::test_support::started_controller_broker().await;
    broker
        .controller
        .submit_change(seed_records())
        .await
        .expect("seed orders");
    activate_followers(&broker).await;
    (handle, dir, broker)
}

/// The issue's acceptance path: shrink the ISR below `min.insync.replicas`
/// and the replicas it dropped are reported eligible; expand it back to
/// `min.insync.replicas` and the set clears.
#[tokio::test]
async fn an_isr_that_crosses_min_insync_replicas_moves_the_reported_elr() {
    let (handle, _dir, broker) = start_orders().await;

    check_partition(&broker, row(ExpectedElrSetup::default())).await;

    crate::test_support::accepted_isr_proposal(
        &broker,
        crate::test_support::LiveIsrSetup::default(),
        crate::test_support::IsrResponseCheck::EnvelopeAndPartition,
    )
    .await;
    check_partition(
        &broker,
        row(ExpectedElrSetup {
            isr: &[ReportedBrokerId(1)],
            eligible: &[ReportedBrokerId(2), ReportedBrokerId(3)],
            ..Default::default()
        }),
    )
    .await;

    rejoin_follower_and_check_isr(&broker).await;

    handle.shutdown().await;
}

/// Restore broker 2 to the ISR and check the published two-member partition row.
async fn rejoin_follower_and_check_isr(broker: &Arc<Broker>) {
    crate::test_support::accepted_isr_proposal(
        broker,
        crate::test_support::LiveIsrSetup {
            new_isr: &[
                crate::test_support::WireBrokerId(1),
                crate::test_support::WireBrokerId(2),
            ],
            ..Default::default()
        },
        crate::test_support::IsrResponseCheck::EnvelopeAndPartition,
    )
    .await;
    check_partition(broker, row(ExpectedElrSetup::two_member_isr())).await;
}

/// A shrink that stops at `min.insync.replicas` leaves nothing eligible, so
/// the controller publishes nothing and the columns stay empty. Without this
/// row the test above would pass on a publisher that made every ISR change
/// eligible.
#[tokio::test]
async fn an_isr_that_stays_at_min_insync_replicas_reports_no_elr() {
    let (handle, _dir, broker) = start_orders().await;

    rejoin_follower_and_check_isr(&broker).await;

    handle.shutdown().await;
}

async fn seed_returning_broker(
    min_isr: &str,
) -> (crate::BrokerHandle, Arc<Broker>, tempfile::TempDir) {
    let (handle, dir, broker) = crate::test_support::started_controller_broker().await;
    let mut seed = seed_records_with_min_isr(min_isr);
    seed.push(registration_record(1));
    broker
        .controller
        .submit_change(seed)
        .await
        .expect("seed orders");
    mark_broker_3_unavailable(&broker).await;
    (handle, broker, dir)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum InitialIsrCheck {
    Required,
    Skipped,
}

async fn assert_returning_broker_withdrawal(min_isr: &str, initial_check: InitialIsrCheck) {
    let (handle, broker, _dir) = seed_returning_broker(min_isr).await;
    if initial_check == InitialIsrCheck::Required {
        // Broker 3 is explicitly fenced so its returning incarnation is
        // deterministic even when coverage instrumentation delays the test.
        check_partition(&broker, offline_followers_row(ExpectedElrSetup::default())).await;
    }
    register_broker_3(&broker, 2).await;
    check_partition(
        &broker,
        offline_followers_row(ExpectedElrSetup::two_member_isr()),
    )
    .await;
    // Every later derivation still excludes broker 3 while allowing broker 2,
    // which left an ISR whose log was never called into question.
    crate::test_support::accepted_isr_proposal(
        &broker,
        crate::test_support::LiveIsrSetup::default(),
        crate::test_support::IsrResponseCheck::EnvelopeAndPartition,
    )
    .await;
    check_partition(
        &broker,
        offline_followers_row(ExpectedElrSetup {
            isr: &[ReportedBrokerId(1)],
            eligible: &[ReportedBrokerId(2)],
            ..Default::default()
        }),
    )
    .await;
    handle.shutdown().await;
}

/// krabka-io/krabka-broker#314: a broker that comes back under a new
/// incarnation must not be re-derived into the ELR from the ISR the image
/// still holds it in.
///
/// This is the case the published-state withdrawal alone could not see. The
/// ISR is healthy when the broker re-registers, so there is no ELR to
/// withdraw at all -- and yet `old_isr ∪ eligible_before` would hand the
/// returning broker straight back the first time the ISR fell below
/// `min.insync.replicas`. The registration batch has to take it out of the
/// ISR for that not to happen.
///
/// The last assertion is also the guard against a fix that just turns ELR
/// off: broker 2 left the same ISR in the same shrink and is eligible, which
/// is the whole point of KIP-966.
#[tokio::test]
async fn a_returning_broker_is_not_re_derived_into_the_elr_from_a_stale_isr() {
    assert_returning_broker_withdrawal("2", InitialIsrCheck::Required).await;
}

/// The same defect one step earlier: with `min.insync.replicas` at the
/// replication factor, dropping the returning broker from the ISR is itself a
/// change that falls below min ISR, so the registration batch recomputes the
/// ELR from the ISR it is in the middle of shrinking.
///
/// Kafka stops that with `uncleanShutdownReplicas`, which
/// `PartitionChangeBuilder.maybePopulateTargetElr` subtracts from `targetElr`
/// and from nothing else -- so the returning broker is in neither column,
/// which is what the assertions below read. It does not land in the last-known
/// set: that holds the last leader of a partition that has none, and this one
/// has a leader. Without the exclusion the batch publishes broker 3 as an
/// eligible leader replica: a membership backed by whatever disk the new
/// process actually has.
#[tokio::test]
async fn the_registration_batch_cannot_publish_the_broker_it_is_withdrawing() {
    assert_returning_broker_withdrawal("3", InitialIsrCheck::Skipped).await;
}

/// The state the controller publishes is an ordinary `V1TopicConfig`, so a
/// snapshot carries it and a replay of that snapshot projects the same two
/// lists. Nothing about the ELR needs a record type of its own to survive a
/// restart, and this is the test that says so.
#[test]
fn the_published_state_round_trips_through_a_snapshot() {
    let mut image = MetadataImage::new(uuid::Uuid::nil());
    for record in seed_records() {
        image.apply(&record);
    }

    // Shrink to one replica, the way the controller does it, and apply what
    // the publisher decided.
    let mut changes = vec![MetadataRecord::V1Partition(partition_record(&[NodeId(1)]))];
    ElrPublisher::new(&image).extend(&mut changes);
    for record in &changes {
        image.apply(record);
    }
    let before = TopicElr::of_topic(&image, TOPIC).partition(0);
    assert!(
        before
            == PartitionElr {
                eligible_leader_replicas: vec![2, 3],
                last_known_elr: vec![],
            }
    );

    // Snapshot, restore, and read the same partition back.
    let mut restored = MetadataImage::new(uuid::Uuid::nil());
    for record in image.to_records() {
        restored.apply(&record);
    }

    assert!(TopicElr::of_topic(&restored, TOPIC).partition(0) == before);
}
