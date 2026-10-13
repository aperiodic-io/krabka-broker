//! Unit tests for the `ElectLeaders` handler, centred on the KFC-9 break-glass
//! gate.
//!
//! The fixtures build a one-partition topic whose only in-sync replica is dead,
//! so an unclean election always has an out-of-ISR replica to elect and the
//! tests differ only in the approvals the metadata image holds.

use {
    super::{
        WIRE_ELECTION_PREFERRED, WIRE_ELECTION_UNCLEAN, batch::ElectionBatch, env::ElectionEnv,
        handle, partition::elect_one,
    },
    crate::{
        authorizer::AllowAllAuthorizer,
        break_glass::gate::tests::{APPROVED_PROPOSAL_ID, approved_proposal},
        broker::{Broker, BrokerHandle},
        codes,
        config::BreakGlassConfig,
        handlers::{
            RequestContext, encode_response,
            test_support::{replicated_partition, single_replica_partition},
        },
        leader_election::{ElectionType, test_support::one_partition_change},
        test_support::{decode_response, start_broker_no_audit_with, test_ctx},
    },
    assert2::{assert, check},
    krabka_metadata::{
        BreakGlassAction, BreakGlassProposalRecord, LeaderEpoch, MetadataImage, MetadataRecord,
        NodeId, PartitionRecord, TopicRecord,
    },
    krabka_protocol::owned::{
        elect_leaders_request::{ElectLeadersRequest, TopicPartitions},
        elect_leaders_response::{
            self, ElectLeadersResponse, PartitionResult, ReplicaElectionResult,
        },
    },
    std::collections::HashSet,
    uuid::Uuid,
};

const TOPIC: &str = "orders";
const VERSION: i16 = elect_leaders_response::MAX_VERSION;

mod commit_fencing;

crate::test_support::context_helper!(client_id = "kafka-leader-election");

fn gated_config() -> BreakGlassConfig {
    BreakGlassConfig {
        approvers: ["User:alice", "User:bob"].map(str::to_owned).to_vec(),
        ..BreakGlassConfig::default()
    }
}

/// One two-replica partition of [`TOPIC`], led by broker 1, beside the
/// proposals the registry holds.
fn image_with(proposals: &[BreakGlassProposalRecord]) -> MetadataImage {
    let mut image = MetadataImage::new(Uuid::nil());
    image.apply(&MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.to_owned(),
        topic_id: Uuid::nil(),
        partitions: 1,
        replication_factor: 2,
    }));
    image.apply(&MetadataRecord::V1Partition(PartitionRecord {
        isr: vec![NodeId(1)],
        leader_epoch: LeaderEpoch(5),
        ..replicated_partition()
            .topic(TOPIC)
            .leader(1)
            .replicas(&[NodeId(1), NodeId(2)])
            .call()
    }));
    for proposal in proposals {
        image.apply(&MetadataRecord::V1BreakGlassProposal(proposal.clone()));
    }
    image
}

/// The leader change that an unclean election of `orders-0` makes: broker 1
/// is in the ISR and dead, broker 2 is alive and out of it.
fn elected() -> PartitionRecord {
    PartitionRecord {
        isr: vec![NodeId(2)],
        leader_epoch: LeaderEpoch(6),
        partition_epoch: 1,
        ..replicated_partition()
            .topic(TOPIC)
            .leader(2)
            .replicas(&[NodeId(1), NodeId(2)])
            .call()
    }
}

/// Broker 2 alive, broker 1 dead, so an unclean election has an out-of-ISR
/// replica to elect.
fn alive() -> HashSet<u64> { HashSet::from([2]) }

async fn broker_with(config: BreakGlassConfig) -> (BrokerHandle, tempfile::TempDir) {
    start_broker_no_audit_with(move |cfg| {
        cfg.authorizer = std::sync::Arc::new(AllowAllAuthorizer);
        cfg.break_glass = config;
    })
    .await
}

/// Drive one partition through the election, and answer its row beside the
/// records the request would append.
async fn elect(
    broker: &Broker,
    image: &MetadataImage,
    election: ElectionType,
) -> (PartitionResult, Vec<MetadataRecord>) {
    let alive = alive();
    let witnesses = HashSet::new();
    test_ctx!(ctx, "admin");
    let env = ElectionEnv {
        broker,
        image,
        ctx: &ctx,
        alive: &alive,
        witnesses: &witnesses,
        election,
    };
    let mut batch = ElectionBatch::default();
    let row = elect_one(&env, &mut batch, TOPIC, 0).await;
    (row, std::mem::take(&mut batch.records))
}

#[tokio::test]
async fn an_unclean_election_with_no_proposal_is_refused_and_appends_nothing() {
    let (handle, _dir, row, records) = election_fixture().call().await;

    check!(row.error_code == codes::POLICY_VIOLATION);
    check!(
        row.error_message
            == Some("break-glass refused unclean_elect_leaders on orders-0: no approved proposal covers the request".to_owned())
    );
    assert!(records == vec![], "a refused election appends nothing");
    handle.shutdown().await;
}

#[tokio::test]
async fn an_approved_unclean_election_appends_the_consume_beside_the_leader_change() {
    let (handle, _dir) = broker_with(gated_config()).await;
    let broker = handle.broker_arc_for_test();
    let proposal = approved_proposal(BreakGlassAction::UncleanElectLeaders, "orders-0");
    let image = image_with(std::slice::from_ref(&proposal));

    let (row, records) = elect(&broker, &image, ElectionType::Unclean).await;

    check!(row.error_code == codes::NONE);
    // The consume and the transition it authorized are one raft append.
    assert!(records.len() == 2, "{records:?}");
    assert!(let MetadataRecord::V1BreakGlassProposal(consumed) = &records[0]);
    check!(consumed.proposal_id == APPROVED_PROPOSAL_ID);
    check!(consumed.consumed_at_ms != 0, "the approval is spent");
    check!(
        *consumed
            == BreakGlassProposalRecord {
                consumed_at_ms: consumed.consumed_at_ms,
                ..proposal
            }
    );
    check!(*one_partition_change(&records[1..]) == elected());
    assert!(let MetadataRecord::V1PartitionUpdate(update) = &records[1]);
    check!(update.eligible_leader_replicas == Some(Vec::new()));
    check!(update.last_known_elr == Some(Vec::new()));
    check!(update.recovery_state == Some(krabka_metadata::LeaderRecoveryState::Recovering));
    handle.shutdown().await;
}

#[tokio::test]
async fn a_topic_wide_proposal_is_spent_once_for_every_partition_it_covers() {
    let (handle, _dir) = broker_with(gated_config()).await;
    let broker = handle.broker_arc_for_test();
    let image = image_with(&[approved_proposal(
        BreakGlassAction::UncleanElectLeaders,
        TOPIC,
    )]);
    let alive = alive();
    let witnesses = HashSet::new();
    test_ctx!(ctx, "admin");
    let env = ElectionEnv {
        broker: &broker,
        image: &image,
        ctx: &ctx,
        alive: &alive,
        witnesses: &witnesses,
        election: ElectionType::Unclean,
    };
    let mut batch = ElectionBatch::default();

    let first = elect_one(&env, &mut batch, TOPIC, 0).await;
    let second = elect_one(&env, &mut batch, TOPIC, 0).await;

    check!(first.error_code == codes::NONE);
    check!(second.error_code == codes::NONE);
    let consumes = batch
        .records
        .iter()
        .filter(|record| matches!(record, MetadataRecord::V1BreakGlassProposal(_)))
        .count();
    check!(
        consumes == 1,
        "one approval is spent once: {:?}",
        batch.records
    );
    handle.shutdown().await;
}

#[tokio::test]
async fn a_preferred_election_is_never_gated() {
    let (handle, _dir, row, records) = election_fixture()
        .election(ElectionType::Preferred)
        .call()
        .await;

    // Broker 1 leads, but it is dead, so Kafka would have left the partition
    // leaderless and the preferred replica is not available. It is never
    // `POLICY_VIOLATION`, which is the point.
    check!(row.error_code == codes::PREFERRED_LEADER_NOT_AVAILABLE);
    assert!(records == vec![]);
    handle.shutdown().await;
}

#[tokio::test]
async fn a_broker_with_no_approver_set_gates_nothing() {
    let (handle, _dir, row, records) = election_fixture()
        .config(BreakGlassConfig::default())
        .call()
        .await;

    check!(row.error_code == codes::NONE);
    assert!(*one_partition_change(&records) == elected());
    handle.shutdown().await;
}

#[tokio::test]
async fn the_wire_handler_refuses_an_unclean_election_that_no_proposal_covers() {
    let (handle, _dir) = broker_with(gated_config()).await;
    let broker = handle.broker_arc_for_test();
    test_ctx!(ctx, "admin");
    let request = |election_type| ElectLeadersRequest {
        election_type,
        topic_partitions: Some(vec![TopicPartitions {
            topic: TOPIC.to_owned(),
            partitions: vec![0],
            ..Default::default()
        }]),
        timeout_ms: 5_000,
        ..Default::default()
    };

    let unclean = handle_request(&broker, request(WIRE_ELECTION_UNCLEAN), &ctx).await;
    let preferred = handle_request(&broker, request(WIRE_ELECTION_PREFERRED), &ctx).await;

    // The gate is an authority gate, so it answers before the broker looks
    // the partition up. A preferred election never reaches it and reports
    // the missing partition instead.
    check!(unclean == codes::POLICY_VIOLATION);
    check!(preferred == codes::UNKNOWN_TOPIC_OR_PARTITION);
    handle.shutdown().await;
}

fn named(topic: &str, partitions: &[i32]) -> TopicPartitions {
    TopicPartitions {
        topic: topic.to_owned(),
        partitions: partitions.to_vec(),
        ..Default::default()
    }
}

fn row(partition_id: i32, error_code: i16, message: &str) -> PartitionResult {
    PartitionResult {
        partition_id,
        error_code,
        error_message: Some(message.to_owned()),
        ..Default::default()
    }
}

fn topic_rows(topic: &str, partition_result: Vec<PartitionResult>) -> ReplicaElectionResult {
    ReplicaElectionResult {
        topic: topic.to_owned(),
        partition_result,
        ..Default::default()
    }
}

/// A refusal of the whole request answers the way Kafka's
/// `ElectLeadersRequest.getErrorResponse` does: the refusal code at the top
/// level (v1+), and the same code and message on every row the request named.
#[test]
fn a_whole_request_refusal_sets_the_top_level_code_and_every_named_row() {
    const DENIED: &str = "Request ElectLeaders needs ALTER permission.";
    const UNKNOWN_TYPE: &str = "Unknown election type 7";
    // (api version, topic_partitions, election_type, denied, expected)
    let cases = [
        (
            2,
            None,
            WIRE_ELECTION_PREFERRED,
            true,
            ElectLeadersResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                ..Default::default()
            },
        ),
        (
            2,
            Some(vec![named("t", &[0, 1])]),
            WIRE_ELECTION_PREFERRED,
            true,
            ElectLeadersResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                replica_election_results: vec![topic_rows("t", vec![
                    row(0, codes::CLUSTER_AUTHORIZATION_FAILED, DENIED),
                    row(1, codes::CLUSTER_AUTHORIZATION_FAILED, DENIED),
                ])],
                ..Default::default()
            },
        ),
        // v0 has no top-level field, so the rows alone carry the refusal.
        (
            0,
            Some(vec![named("t", &[0])]),
            WIRE_ELECTION_PREFERRED,
            true,
            ElectLeadersResponse {
                replica_election_results: vec![topic_rows("t", vec![row(
                    0,
                    codes::CLUSTER_AUTHORIZATION_FAILED,
                    DENIED,
                )])],
                ..Default::default()
            },
        ),
        (
            2,
            Some(vec![named("t", &[0])]),
            7,
            false,
            ElectLeadersResponse {
                error_code: codes::INVALID_REQUEST,
                replica_election_results: vec![topic_rows("t", vec![row(
                    0,
                    codes::INVALID_REQUEST,
                    UNKNOWN_TYPE,
                )])],
                ..Default::default()
            },
        ),
    ];
    for (version, topic_partitions, election_type, denied, expected) in cases {
        let request = ElectLeadersRequest {
            election_type,
            topic_partitions,
            timeout_ms: 5_000,
            ..Default::default()
        };
        let refusal = super::admit(&request, denied).expect_err("the request is refused");
        let bytes = encode_response(&refusal, version).expect("encode");
        let decoded: ElectLeadersResponse = decode_response(&bytes, version);
        check!(decoded == expected, "v{version}, denied={denied}");
    }
}

/// Three partitions of [`TOPIC`], each led by its preferred replica, so a
/// preferred election of any of them is not needed.
async fn seed_preferred_topic(broker: &Broker) {
    let mut records = vec![MetadataRecord::V1Topic(TopicRecord {
        name: TOPIC.to_owned(),
        topic_id: Uuid::from_u128(7),
        partitions: 3,
        replication_factor: 1,
    })];
    records.extend((0..3).map(|partition| {
        MetadataRecord::V1Partition(single_replica_partition(
            TOPIC,
            krabka_ids::PartitionIndex(partition),
            NodeId(1),
        ))
    }));
    broker
        .controller
        .submit_change(records)
        .await
        .expect("seed topic");
}

/// KIP-460 as Kafka's `ReplicationControlManager.electLeaders` implements it:
/// a named topic elects only the partitions it lists, so an empty list elects
/// none and answers an empty topic row; a null topic list answers a row for
/// every topic even when every partition in it was dropped as
/// `ELECTION_NOT_NEEDED`; and the row messages are Kafka's.
#[tokio::test]
async fn the_handler_elects_exactly_the_partitions_the_request_names() {
    const NOT_NEEDED: &str = "Leader election not needed for topic partition.";
    let (broker_handle, _dir) = broker_with(BreakGlassConfig::default()).await;
    let broker = broker_handle.broker_arc_for_test();
    seed_preferred_topic(&broker).await;
    test_ctx!(ctx, "admin");
    let cases: Vec<(Option<Vec<TopicPartitions>>, Vec<ReplicaElectionResult>)> = vec![
        (Some(vec![named(TOPIC, &[])]), vec![topic_rows(
            TOPIC,
            vec![],
        )]),
        (Some(vec![named(TOPIC, &[0, 9])]), vec![topic_rows(
            TOPIC,
            vec![
                row(0, codes::ELECTION_NOT_NEEDED, NOT_NEEDED),
                row(
                    9,
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    "No such partition as orders-9",
                ),
            ],
        )]),
        (Some(vec![named("missing", &[0]), named(TOPIC, &[])]), vec![
            topic_rows("missing", vec![row(
                0,
                codes::UNKNOWN_TOPIC_OR_PARTITION,
                "No such topic as missing",
            )]),
            topic_rows(TOPIC, vec![]),
        ]),
    ];
    for (topic_partitions, expected) in cases {
        let request = ElectLeadersRequest {
            election_type: WIRE_ELECTION_PREFERRED,
            topic_partitions: topic_partitions.clone(),
            timeout_ms: 5_000,
            ..Default::default()
        };
        let response = handle(&broker, request, VERSION, &ctx)
            .await
            .expect("handle");
        check!(
            response
                == ElectLeadersResponse {
                    replica_election_results: expected,
                    ..Default::default()
                },
            "{topic_partitions:?}"
        );
    }

    // A null topic list answers every topic, and drops the partitions that
    // need no election rather than the topic row.
    let response = handle(
        &broker,
        ElectLeadersRequest {
            election_type: WIRE_ELECTION_PREFERRED,
            topic_partitions: None,
            timeout_ms: 5_000,
            ..Default::default()
        },
        VERSION,
        &ctx,
    )
    .await
    .expect("handle");
    check!(response.error_code == codes::NONE);
    check!(
        response
            .replica_election_results
            .iter()
            .find(|topic| topic.topic == TOPIC)
            == Some(&topic_rows(TOPIC, vec![]))
    );
    broker_handle.shutdown().await;
}

/// Run the wire handler and answer the one partition row's error code.
async fn handle_request(
    broker: &Broker,
    req: ElectLeadersRequest,
    ctx: &RequestContext<'_>,
) -> i16 {
    let response = handle(broker, req, VERSION, ctx).await.expect("handle");
    response.replica_election_results[0].partition_result[0].error_code
}

#[derive(krabka_macros::FieldDefaults)]
struct ElectionCaseSetup<'a> {
    #[default(gated_config())]
    config: BreakGlassConfig,
    #[default(ElectionType::Unclean)]
    election: ElectionType,
    proposals: &'a [BreakGlassProposalRecord],
}

#[bon::builder]
async fn election_fixture(
    #[builder(default = gated_config ())] config: BreakGlassConfig,
    #[builder(default = ElectionType :: Unclean)] election: ElectionType,
    #[builder(default)] proposals: &'_ [BreakGlassProposalRecord],
) -> (
    BrokerHandle,
    tempfile::TempDir,
    PartitionResult,
    Vec<MetadataRecord>,
) {
    let setup = ElectionCaseSetup {
        config,
        election,
        proposals,
    };
    let (handle, dir) = Box::pin(broker_with(setup.config)).await;
    let broker = handle.broker_arc_for_test();
    let (row, records) = elect(&broker, &image_with(setup.proposals), setup.election).await;
    (handle, dir, row, records)
}
