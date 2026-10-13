//! End-to-end tests for the `AlterPartitionReassignments` wire handler.
//!
//! They drive a live broker, so they cover the cluster authorization
//! preamble, the response shape for a row the metadata image does not know,
//! and the metadata a successful alter leaves behind.

use {
    super::*,
    crate::{
        broker::Broker,
        codes::{POLICY_VIOLATION, UNKNOWN_TOPIC_OR_PARTITION},
        config_keys::MIN_INSYNC_REPLICAS,
        elr::state::test_records,
        handlers::{
            alter_partition_reassignments::test_support::{
                ReassignmentTarget, ReplicationFactorPolicy, request, test_context,
            },
            test_support::replicated_partition,
        },
        test_support::{
            DenyAll, broker_registration, finalize_elr_version_on,
            start_broker_with_authorizer as start_broker, test_ctx,
        },
    },
    assert2::{assert, check},
    krabka_metadata::{
        BrokerRegistrationRecord, LeaderEpoch, MetadataRecord, PartitionRecord, TopicFreezeRecord,
        TopicRecord,
    },
    krabka_raft::NodeId,
    std::sync::Arc,
    uuid::Uuid,
};

#[derive(Clone, Copy, krabka_macros::FieldDefaults)]
struct ReassignableSeedSetup<'a> {
    #[default(&[NodeId(1)])]
    replicas: &'a [NodeId],
    isr: Option<&'a [NodeId]>,
    #[default(&[])]
    adding: &'a [NodeId],
}

#[bon::builder]
fn reassignable_seed_record(
    #[builder(default = & [NodeId (1)])] replicas: &'_ [NodeId],
    isr: Option<&'_ [NodeId]>,
    #[builder(default = & [])] adding: &'_ [NodeId],
) -> PartitionRecord {
    let setup = ReassignableSeedSetup {
        replicas,
        isr,
        adding,
    };
    let mut record = PartitionRecord {
        leader_epoch: LeaderEpoch(3),
        partition_epoch: 11,
        ..replicated_partition()
            .partition(7)
            .replicas(setup.replicas)
            .call()
    };
    if let Some(isr) = setup.isr {
        record.isr = isr.to_vec();
    }
    record.adding_replicas = setup.adding.to_vec();
    record
}

async fn seed_reassignable_partition(broker: &Broker) {
    broker
        .controller
        .submit_change(vec![
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                broker_epoch: -1,
                host: "localhost".into(),
                ..crate::test_support::broker_registration(krabka_raft::NodeId(1))
            }),
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                broker_epoch: -1,
                host: "localhost".into(),
                port: 9093,
                ..crate::test_support::broker_registration(krabka_raft::NodeId(2))
            }),
            MetadataRecord::V1Topic(TopicRecord {
                name: "orders".into(),
                topic_id: Uuid::nil(),
                partitions: 1,
                replication_factor: 1,
            }),
            MetadataRecord::V1Partition(reassignable_seed_record().call()),
        ])
        .await
        .expect("seed reassignment metadata");
}

/// A partition mid-reassignment that is adding a replica which has not caught
/// up, so the reassignment task leaves it alone. Its ISR is already below
/// `min.insync.replicas`, so it carries KIP-966 state naming both replicas
/// that left the ISR -- including the one a cancel drops from the replica set.
async fn seed_cancellable_partition(broker: &Broker) {
    let mut records: Vec<MetadataRecord> = (1..=3u64)
        .map(|node| {
            MetadataRecord::V1BrokerRegistration(BrokerRegistrationRecord {
                broker_epoch: -1,
                host: "localhost".into(),
                port: 9092 + u16::try_from(node).expect("node id fits u16"),
                ..broker_registration(krabka_raft::NodeId(node))
            })
        })
        .collect();
    records.push(MetadataRecord::V1Topic(TopicRecord {
        name: "orders".into(),
        topic_id: Uuid::nil(),
        partitions: 1,
        replication_factor: 2,
    }));
    records.push(MetadataRecord::V1Partition(
        reassignable_seed_record()
            .replicas(&[NodeId(1), NodeId(2), NodeId(3)])
            .maybe_isr(Some(&[NodeId(1)]))
            .adding(&[NodeId(3)])
            .call(),
    ));
    records.push(MetadataRecord::V1TopicConfig(
        krabka_metadata::TopicConfigRecord {
            topic: "orders".into(),
            overrides: [(MIN_INSYNC_REPLICAS.to_string(), "3".to_string())]
                .into_iter()
                .collect(),
        },
    ));
    records.extend(test_records("orders", "7:2,3:"));
    broker
        .controller
        .submit_change(records)
        .await
        .expect("seed cancellable reassignment metadata");
}

/// KIP-966: a cancel reverts the replica set, so a replica the published ELR
/// calls eligible can stop being a replica at all. The partition can no longer
/// elect it, so it leaves the ELR -- and the batch that reverts the partition
/// is the batch that says so. It does not land in the last-known set: that
/// holds the last leader of a partition without one, and this partition has a
/// leader.
#[tokio::test]
async fn a_cancel_publishes_the_eligible_leader_state_the_revert_implies() {
    let version = 1;
    broker_fixture!((broker_handle, _dir, broker), allow_all, controller_leader);
    finalize_elr_version_on(&broker).await;
    seed_cancellable_partition(&broker).await;
    test_ctx!(ctx, "admin");

    let resp = handle(
        &broker,
        request()
            .replication_factor_policy(ReplicationFactorPolicy::PermitChange)
            .target(ReassignmentTarget::Cancel)
            .call(),
        version,
        &ctx,
    )
    .await
    .expect("handle");
    assert!(resp.responses[0].partitions[0].error_code == 0, "{resp:?}");

    let image = broker.controller.current_image();
    let partition = image.partition("orders", 7).expect("partition committed");
    assert!(partition.replicas == vec![NodeId(1), NodeId(2)]);
    assert!(partition.isr == vec![NodeId(1)]);
    assert!(
        crate::elr::TopicElr::of_topic(&image, "orders").partition(7)
            == crate::elr::state::PartitionElr {
                eligible_leader_replicas: vec![2],
                last_known_elr: vec![],
            }
    );
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_preserves_unknown_partition_response_shape() {
    let version = 1;
    broker_fixture!(
        (broker_handle, _dir, broker),
        allow_all,
        context(ctx, "admin")
    );

    let resp = handle(
        &broker,
        request().topic("payments").partition_index(8).call(),
        version,
        &ctx,
    )
    .await
    .expect("handle");

    let expected = krabka_macros::unthrottled_wire!(AlterPartitionReassignmentsResponse {
        allow_replication_factor_change: false,
        error_code: 0,
        error_message: None,
        responses: vec![krabka_macros::tagged_wire!(ReassignableTopicResponse {
            name: "payments".into(),
            partitions: vec![krabka_macros::tagged_wire!(ReassignablePartitionResponse {
                partition_index: 8,
                error_code: UNKNOWN_TOPIC_OR_PARTITION,
                error_message: Some("Unable to find a topic named payments.".into()),
            })],
        })],
    });
    assert!(resp == expected);
    broker_handle.shutdown().await;
}

/// KIP-455's cluster-`Alter` preamble is the only authorization
/// `AlterPartitionReassignments` applies; Kafka's
/// `AlterPartitionReassignmentsRequest.getErrorResponse` sets the top-level
/// `error_code`/`error_message` to the denial and leaves
/// `allow_replication_factor_change` at the schema default (`true`), not the
/// request's value. This holds across both wire versions and both settings of
/// the request field.
#[tokio::test]
async fn handle_denies_cluster_alter_with_top_level_cluster_authorization_failed() {
    for version in 0..=1 {
        for allow_rf_change in [
            ReplicationFactorPolicy::Maintain,
            ReplicationFactorPolicy::PermitChange,
        ] {
            broker_fixture!(
                (broker_handle, _dir, broker),
                deny_all,
                context(ctx, "admin")
            );

            let resp = handle(
                &broker,
                request()
                    .replication_factor_policy(allow_rf_change)
                    .topic("payments")
                    .partition_index(8)
                    .call(),
                version,
                &ctx,
            )
            .await
            .expect("handle");

            let expected = krabka_macros::unthrottled_wire!(AlterPartitionReassignmentsResponse {
                allow_replication_factor_change: true,
                error_code: CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("alter-reassignment denied".into()),
                responses: vec![krabka_macros::tagged_wire!(ReassignableTopicResponse {
                    name: "payments".into(),
                    partitions: vec![krabka_macros::tagged_wire!(ReassignablePartitionResponse {
                        partition_index: 8,
                        error_code: CLUSTER_AUTHORIZATION_FAILED,
                        error_message: Some("alter-reassignment denied".into()),
                    })],
                })],
            });
            assert!(
                resp == expected,
                "version={version} allow_rf_change={allow_rf_change:?}"
            );
            broker_handle.shutdown().await;
        }
    }
}

#[tokio::test]
async fn handle_submits_successful_reassignment_records() {
    let version = 1;
    broker_fixture!((broker_handle, _dir, broker), allow_all, controller_leader);
    seed_reassignable_partition(&broker).await;
    test_ctx!(ctx, "admin");

    let resp = handle(
        &broker,
        request()
            .replication_factor_policy(ReplicationFactorPolicy::PermitChange)
            .call(),
        version,
        &ctx,
    )
    .await
    .expect("handle");

    let expected = krabka_macros::unthrottled_wire!(AlterPartitionReassignmentsResponse {
        allow_replication_factor_change: true,
        error_code: 0,
        error_message: None,
        responses: vec![krabka_macros::tagged_wire!(ReassignableTopicResponse {
            name: "orders".into(),
            partitions: vec![krabka_macros::tagged_wire!(ReassignablePartitionResponse {
                partition_index: 7,
                error_code: 0,
                error_message: None,
            })],
        })],
    });
    assert!(resp == expected);

    let image = broker.controller.current_image();
    let partition = image.partition("orders", 7).expect("partition committed");
    assert!(partition.adding_replicas == vec![NodeId(2)]);
    assert!(partition.partition_epoch == 12);
    broker_handle.shutdown().await;
}

#[tokio::test]
async fn handle_refuses_a_frozen_reassignment_without_mutating_the_partition() {
    let version = 1;
    broker_fixture!((broker_handle, _dir, broker), allow_all, controller_leader);
    seed_reassignable_partition(&broker).await;
    broker
        .controller
        .submit_change(vec![MetadataRecord::V1TopicFreeze(TopicFreezeRecord {
            set_at_ms: 10,
            ..crate::test_support::topic_freeze_record().call()
        })])
        .await
        .expect("seed topic freeze");
    let before = broker
        .controller
        .current_image()
        .partition("orders", 7)
        .expect("seeded partition")
        .clone();
    test_ctx!(ctx, "admin");

    let response = handle(
        &broker,
        request()
            .replication_factor_policy(ReplicationFactorPolicy::PermitChange)
            .call(),
        version,
        &ctx,
    )
    .await
    .expect("handle");
    let row = &response.responses[0].partitions[0];
    check!(row.error_code == POLICY_VIOLATION);
    check!(
        row.error_message.as_deref()
            == Some(
                "a write freeze on the literal scope \"orders\" refuses this reassignment: DR cutover"
            )
    );

    let after = broker
        .controller
        .current_image()
        .partition("orders", 7)
        .expect("partition remains")
        .clone();
    check!(after == before, "the refused row must append no metadata");
    broker_handle.shutdown().await;
}
