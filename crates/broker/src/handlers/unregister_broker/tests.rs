//! Unit tests for the `UnregisterBroker` handler.
//!
//! Two kinds sit here. The wire tests drive `handle` against a live in-process
//! broker and compare the whole decoded response, because the shape of a
//! refusal is as much the contract as its error code. The gate tests call
//! `unregister_records` directly, where the break-glass decision is a pure
//! function of the metadata image, the approver set, and the clock.

use std::{net::SocketAddr, sync::Arc};

use assert2::{assert, check};
use bytes::Bytes;
use krabka_metadata::{BreakGlassProposalRecord, MetadataRecord, UnregisterBrokerRecord};
use krabka_protocol::owned::unregister_broker_response::{self, UnregisterBrokerResponse};
use krabka_security::Principal;
use uuid::Uuid;

use super::*;
use crate::{
    break_glass::gate::tests::{approved_proposal, image_of},
    broker::Broker,
    config::BreakGlassConfig,
    test_support::{DenyAll, peer, principal},
};

fn encode_request(req: &UnregisterBrokerRequest, version: i16) -> Bytes {
    crate::test_support::encode_request(req, version)
}

fn decode_response(bytes: &Bytes) -> UnregisterBrokerResponse {
    crate::test_support::decode_response(bytes, unregister_broker_response::MAX_VERSION)
}

fn context<'a>(
    principal: &'a Principal,
    peer: &'a SocketAddr,
) -> crate::handlers::RequestContext<'a> {
    crate::test_support::request_context(principal, peer, "unregister-client")
}

#[test]
fn response_preserves_error_fields_and_throttle() {
    let resp =
        UnregisterBrokerResponse::error(codes::UNKNOWN_SERVER_ERROR, Some("submit failed".into()));

    let expected = unthrottled_wire!(UnregisterBrokerResponse {
        error_code: codes::UNKNOWN_SERVER_ERROR,
        error_message: Some("submit failed".into()),
    });
    assert!(resp == expected);
}

#[tokio::test]
async fn handle_denies_cluster_alter_with_message_and_throttle() {
    let version = unregister_broker_response::MAX_VERSION;
    broker_fixture!(
        (broker_handle, _dir, broker),
        crate::test_support::start_broker_with_authorizer_no_audit(Arc::new(DenyAll))
    );
    let resp = answer(&broker, version, 1).await;

    let expected = unthrottled_wire!(UnregisterBrokerResponse {
        error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
        error_message: Some("unregister-broker denied".into()),
    });
    assert!(resp == expected, "{resp:?}");
    broker_handle.shutdown().await;
}

/// An id with no registration, a negative one included, answers
/// `BROKER_ID_NOT_REGISTERED` with the message of Kafka's
/// `ReplicationControlManager.unregisterBroker`.
#[tokio::test]
async fn handle_answers_broker_id_not_registered_for_unknown_ids() {
    let version = unregister_broker_response::MAX_VERSION;
    broker_fixture!(
        (broker_handle, _dir, broker),
        crate::test_support::start_broker_with_authorizer_no_audit(Arc::new(
            crate::authorizer::AllowAllAuthorizer
        ),)
    );
    request_identity!((principal, peer, ctx), principal("admin"), context);

    for broker_id in [-1, 0, 999] {
        let req = UnregisterBrokerRequest {
            broker_id,
            ..Default::default()
        };
        let resp = handle(&broker, version, &encode_request(&req, version), &ctx)
            .await
            .expect("handle");
        let resp = decode_response(&resp);

        let expected = unthrottled_wire!(UnregisterBrokerResponse {
            error_code: codes::BROKER_ID_NOT_REGISTERED,
            error_message: Some(format!("Broker ID {broker_id} is not currently registered")),
        });
        check!(resp == expected, "broker_id {broker_id}");
    }
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_unregisters_registered_broker_with_success_shape() {
    let version = unregister_broker_response::MAX_VERSION;
    broker_fixture!(
        (broker_handle, _dir, broker),
        crate::test_support::start_broker_with_authorizer_no_audit(Arc::new(
            crate::authorizer::AllowAllAuthorizer
        ),)
    );
    let resp = answer(&broker, version, 1).await;

    let expected = unthrottled_wire!(UnregisterBrokerResponse {
        error_code: codes::NONE,
        error_message: None,
    });
    assert!(resp == expected, "{resp:?}");
    broker_handle.shutdown().await;
}

const DOOMED: NodeId = NodeId(7);
/// The epoch of the registration an unregistration of [`DOOMED`] removes.
const DOOMED_EPOCH: i64 = 42;

fn gated_config() -> BreakGlassConfig {
    BreakGlassConfig {
        approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
        ..BreakGlassConfig::default()
    }
}

const NOW_MS: i64 = 60_000;

#[test]
fn an_unregistration_with_no_proposal_appends_nothing() {
    let image = image_of(&[]);

    let denial = unregister_records(&image, &gated_config(), DOOMED, DOOMED_EPOCH, NOW_MS)
        .expect_err("no proposal covers broker 7");

    check!(denial.action == BreakGlassAction::UnregisterBroker);
    check!(denial.target == "7");
    check!(
        denial.to_string()
            == "break-glass refused unregister_broker on 7: no approved proposal covers the request"
    );
}

#[test]
fn an_approved_unregistration_appends_the_consume_beside_the_unregister() {
    let proposal = approved_proposal(BreakGlassAction::UnregisterBroker, "7");
    let image = image_of(std::slice::from_ref(&proposal));

    let records = unregister_records(&image, &gated_config(), DOOMED, DOOMED_EPOCH, NOW_MS)
        .expect("the proposal authorizes the unregistration");

    let expected = vec![
        MetadataRecord::V1BreakGlassProposal(BreakGlassProposalRecord {
            consumed_at_ms: NOW_MS,
            ..proposal
        }),
        MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
            node_id: DOOMED,
            broker_epoch: DOOMED_EPOCH,
        }),
    ];
    assert!(records == expected);
}

#[test]
fn a_proposal_for_another_broker_does_not_cover_this_one() {
    let image = image_of(&[approved_proposal(BreakGlassAction::UnregisterBroker, "8")]);

    let denial = unregister_records(&image, &gated_config(), DOOMED, DOOMED_EPOCH, NOW_MS)
        .expect_err("a proposal for broker 8 authorizes nothing about broker 7");

    check!(denial.proposal_id() == None);
}

#[test]
fn a_broker_with_no_approver_set_gates_nothing() {
    let records = unregister_records(
        &image_of(&[]),
        &BreakGlassConfig::default(),
        DOOMED,
        DOOMED_EPOCH,
        NOW_MS,
    )
    .expect("an ungated broker unregisters with no proposal");

    assert!(
        records
            == vec![MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
                node_id: DOOMED,
                broker_epoch: DOOMED_EPOCH,
            })]
    );
}

/// The `break_glass_refusals` count for this action.
fn refusals(metrics: &crate::metrics::BrokerMetrics) -> u64 {
    metrics
        .break_glass_refusals
        .get_or_create(&crate::metrics::BreakGlassActionLabel {
            action: crate::metrics::BreakGlassAction(BreakGlassAction::UnregisterBroker),
        })
        .get()
}

#[tokio::test]
async fn the_wire_handler_refuses_an_unregistration_that_no_proposal_covers() {
    let version = unregister_broker_response::MAX_VERSION;
    broker_fixture!((broker_handle, _dir, broker), break_glass(gated_config()));
    let resp = answer(&broker, version, 1).await;

    check!(resp.error_code == codes::POLICY_VIOLATION);
    check!(
        resp.error_message
            == Some(
                "break-glass refused unregister_broker on 1: no approved proposal covers the request"
                    .to_owned()
            )
    );
    // The refusal refused: broker 1 is still registered.
    check!(
        broker
            .controller
            .current_image()
            .broker(NodeId(1))
            .is_some()
    );
    // The refusal reached the series an operator reads.
    check!(refusals(&broker.metrics) == 1);
    broker_handle.shutdown().await;
}

#[derive(Clone, Copy, Default, PartialEq, Eq)]
enum BrokerFencing {
    #[default]
    Active,
    Fenced,
}

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct RegistrationSetup {
    #[default(NodeId(2))]
    node_id: NodeId,
    fencing: BrokerFencing,
}

/// A new registration of broker `node_id`. It carries no epoch (-1), and the
/// controller stamps it with the offset that it commits at.
fn registration(setup: RegistrationSetup) -> MetadataRecord {
    let RegistrationSetup { node_id, fencing } = setup;
    MetadataRecord::V1BrokerRegistration(krabka_metadata::BrokerRegistrationRecord {
        fenced: fencing == BrokerFencing::Fenced,
        broker_epoch: -1,
        incarnation_id: Uuid::from_u128(u128::from(node_id.0)),
        host: format!("broker-{}", node_id.0),
        ..crate::test_support::broker_registration(node_id)
    })
}

/// Partition `index` of topic `t`, replicated on brokers 1 and 2.
#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct UnregisterPartitionSetup<'a> {
    index: krabka_ids::PartitionIndex,
    #[default(NodeId(1))]
    leader: NodeId,
    #[default(&[NodeId(1), NodeId(2)])]
    isr: &'a [NodeId],
    #[default(krabka_metadata::LeaderEpoch(5))]
    leader_epoch: krabka_metadata::LeaderEpoch,
    partition_epoch: crate::test_support::PartitionEpoch,
}

fn replicated_partition(setup: UnregisterPartitionSetup<'_>) -> krabka_metadata::PartitionRecord {
    let UnregisterPartitionSetup {
        index,
        leader,
        isr,
        leader_epoch,
        partition_epoch,
    } = setup;
    krabka_metadata::PartitionRecord {
        isr: isr.to_vec(),
        leader_epoch,
        partition_epoch: partition_epoch.0,
        ..crate::handlers::test_support::replicated_partition(
            crate::handlers::test_support::ReplicatedPartitionSetup {
                topic: "t",
                partition: index,
                leader,
                replicas: &[NodeId(1), NodeId(2)],
            },
        )
    }
}

/// Topic `t`, with `partitions` partitions and a replication factor of 2.
fn topic(partitions: crate::test_support::PartitionCount) -> MetadataRecord {
    MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: "t".into(),
        topic_id: Uuid::from_u128(0x7),
        partitions: partitions.0,
        replication_factor: 2,
    })
}

/// Kafka's `ReplicationControlManager.unregisterBroker` writes
/// `handleBrokerUnregistered`'s partition changes ahead of the
/// `UnregisterBrokerRecord`: the broker leaves every ISR, and a partition it led
/// moves to another replica. The change is committed by the time the handler
/// answers, so no partition names the broker as leader or ISR member while the
/// liveness ticker waits for a heartbeat timeout.
#[tokio::test]
async fn handle_removes_the_broker_from_every_isr_in_the_unregistering_append() {
    let version = unregister_broker_response::MAX_VERSION;
    broker_fixture!(
        (broker_handle, _dir, broker),
        crate::test_support::start_broker_with_authorizer_no_audit(Arc::new(
            crate::authorizer::AllowAllAuthorizer
        ),),
        controller_leader
    );
    broker
        .controller
        .submit_change(vec![
            registration(RegistrationSetup::default()),
            topic(crate::test_support::PartitionCount(3)),
            MetadataRecord::V1Partition(replicated_partition(UnregisterPartitionSetup::default())),
            MetadataRecord::V1Partition(replicated_partition(UnregisterPartitionSetup {
                index: krabka_ids::PartitionIndex(1),
                leader: NodeId(2),
                isr: &[NodeId(2), NodeId(1)],
                ..Default::default()
            })),
            MetadataRecord::V1Partition(replicated_partition(UnregisterPartitionSetup {
                index: krabka_ids::PartitionIndex(2),
                leader: NodeId(2),
                isr: &[NodeId(2)],
                ..Default::default()
            })),
        ])
        .await
        .expect("seed the partitions");
    // Broker 2 is heartbeating, so it may take over.
    broker.liveness.record_heartbeat(2).await;
    let resp = answer(&broker, version, 1).await;

    check!(resp.error_code == codes::NONE, "{resp:?}");
    let image = broker.controller.current_image();
    check!(image.broker(NodeId(1)).is_none());
    // Broker 1 led partition 0, so broker 2 takes it at the next leader epoch.
    // It only followed partition 1, so the leader stays and the epoch does not
    // move. It was in neither of partition 2's lists, so that one is untouched.
    let expected = [
        replicated_partition(UnregisterPartitionSetup {
            leader: NodeId(2),
            isr: &[NodeId(2)],
            leader_epoch: krabka_metadata::LeaderEpoch(6),
            partition_epoch: crate::test_support::PartitionEpoch(1),
            ..Default::default()
        }),
        replicated_partition(UnregisterPartitionSetup {
            index: krabka_ids::PartitionIndex(1),
            leader: NodeId(2),
            isr: &[NodeId(2)],
            partition_epoch: crate::test_support::PartitionEpoch(1),
            ..Default::default()
        }),
        replicated_partition(UnregisterPartitionSetup {
            index: krabka_ids::PartitionIndex(2),
            leader: NodeId(2),
            isr: &[NodeId(2)],
            ..Default::default()
        }),
    ];
    for partition in expected {
        check!(
            image.partition("t", partition.partition) == Some(&partition),
            "partition {}",
            partition.partition
        );
    }
    broker_handle.shutdown().await;
}

/// The partition records of an unregistration are built from the image of the
/// node that runs it, and only the active controller's image is current. Any
/// other node answers `NOT_CONTROLLER` with Kafka's wrong-controller message
/// and appends nothing, so a follower or an observer whose image trails cannot
/// roll back the leader, epoch and ISR that the controller committed since.
#[test]
fn a_node_that_is_not_the_active_controller_refuses_with_kafkas_message() {
    let node = NodeId(1);
    let not_controller = |message: &str| {
        Some(UnregisterBrokerResponse {
            error_code: codes::NOT_CONTROLLER,
            error_message: Some(message.to_owned()),
            ..Default::default()
        })
    };
    for (what, leader, expected) in [
        ("this node leads", Some(NodeId(1)), None),
        (
            "another node leads",
            Some(NodeId(3)),
            not_controller("The active controller appears to be node 3."),
        ),
        (
            "no leader is known",
            None,
            not_controller("No controller appears to be active."),
        ),
    ] {
        check!(
            wire::not_controller_refusal(leader, node) == expected,
            "{what}"
        );
    }
}

/// A request that arrives on the controller listener, or inside an `Envelope`,
/// is already at the active controller and is never forwarded again.
#[tokio::test]
async fn a_request_on_the_controller_listener_is_answered_in_place() {
    let version = unregister_broker_response::MAX_VERSION;
    broker_fixture!(
        (broker_handle, _dir, broker),
        crate::test_support::start_broker_with_authorizer_no_audit(Arc::new(
            crate::authorizer::AllowAllAuthorizer
        ),),
        controller_leader
    );
    let principal = principal("admin");
    let peer: SocketAddr = "127.0.0.1:9093".parse().unwrap();
    let ctx = crate::handlers::RequestContext::new(
        &principal,
        &peer,
        "unregister-client",
        CONTROLLER_ADMIN_CONNECTION_ID,
        false,
        "CONTROLLER",
    );
    let req = UnregisterBrokerRequest {
        broker_id: 999,
        ..Default::default()
    };

    let resp = handle(&broker, version, &encode_request(&req, version), &ctx)
        .await
        .expect("handle");

    check!(
        decode_response(&resp)
            == UnregisterBrokerResponse {
                error_code: codes::BROKER_ID_NOT_REGISTERED,
                error_message: Some("Broker ID 999 is not currently registered".to_owned()),
                ..Default::default()
            }
    );
    broker_handle.shutdown().await;
}

/// The partition changes go between the consumed proposal and the unregister
/// record: the approval commits first, and the registration goes last.
#[test]
fn the_isr_departures_sit_between_the_consume_and_the_unregister_record() {
    let consumed = MetadataRecord::V1BreakGlassProposal(approved_proposal(
        BreakGlassAction::UnregisterBroker,
        "7",
    ));
    let unregister = MetadataRecord::V1UnregisterBroker(UnregisterBrokerRecord {
        node_id: DOOMED,
        broker_epoch: DOOMED_EPOCH,
    });
    let leave = MetadataRecord::V1Partition(replicated_partition(UnregisterPartitionSetup {
        leader: NodeId(2),
        isr: &[NodeId(2)],
        leader_epoch: krabka_metadata::LeaderEpoch(6),
        partition_epoch: crate::test_support::PartitionEpoch(1),
        ..Default::default()
    }));

    for (case, records, expected) in [
        (
            "gated",
            vec![consumed.clone(), unregister.clone()],
            vec![consumed, leave.clone(), unregister.clone()],
        ),
        (
            "ungated",
            vec![unregister.clone()],
            vec![leave.clone(), unregister],
        ),
    ] {
        check!(
            gate::with_leaves(records, vec![leave.clone()]) == expected,
            "{case}"
        );
    }
}

async fn answer(broker: &Broker, version: i16, broker_id: i32) -> UnregisterBrokerResponse {
    request_identity!((principal, peer, ctx), principal("admin"), context);
    let req = UnregisterBrokerRequest {
        broker_id,
        ..Default::default()
    };
    let response = handle(broker, version, &encode_request(&req, version), &ctx)
        .await
        .expect("handle");
    decode_response(&response)
}
