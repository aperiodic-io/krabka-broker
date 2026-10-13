//! `AssignReplicasToDirs` (`api_key=73`, KIP-858).
//!
//! A broker reports, for each of its replicas, which log-directory UUID hosts
//! it. The controller records that in
//! `PartitionRecord.directories[broker_slot]`, so it can later map an
//! `offline_log_dirs` heartbeat back to exactly the affected partitions for
//! failover.
//!
//! Only the leader serves this RPC, and any other broker returns
//! `NOT_CONTROLLER`. This mirrors `alter_partition`.
//!
//! This file holds the ACL preamble, the broker-epoch fence, the leader
//! check, and the request-to-controller flow. The pure halves live beside it:
//! `validation` checks the reporting broker's registration and epoch,
//! `changes` maps a reported directory onto a metadata delta and a
//! per-partition error code, and `response` builds the fixed responses and
//! encodes what goes back on the wire.

use {
    crate::{
        broker::Broker,
        codes,
        error::BrokerError,
        handlers::{
            ApiVersion, RequestContext, cluster_action_denied,
            forward_to_controller::is_active_controller,
        },
    },
    krabka_protocol::owned::{
        assign_replicas_to_dirs_request::AssignReplicasToDirsRequest,
        assign_replicas_to_dirs_response::AssignReplicasToDirsResponse,
    },
};

mod changes;
mod response;
#[cfg(test)]
mod test_support;
mod validation;

use self::{
    changes::{AssignmentPlan, plan_assignments},
    response::not_controller_response,
    validation::check_broker_epoch,
};

pub(crate) async fn handle(
    broker: &Broker,
    req: AssignReplicasToDirsRequest,
    _version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> Result<AssignReplicasToDirsResponse, BrokerError> {
    let controller = broker.controller.clone();
    let image = controller.current_image();

    // ── ACL preamble ────────────────────────────────────────────
    // Inter-broker control-plane RPC: `ClusterAction` on
    // `Cluster("kafka-cluster")`. On Deny → whole-response
    // `error_code = CLUSTER_AUTHORIZATION_FAILED (31)`, as Kafka's
    // `ControllerApis.handleAssignReplicasToDirs` requires before it forwards
    // to the controller.
    if cluster_action_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return Ok(AssignReplicasToDirsResponse {
            error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
            ..Default::default()
        });
    }

    if !is_active_controller(broker) {
        return Ok(not_controller_response());
    }

    // Kafka's `ClusterControlManager.checkBrokerEpoch`, which
    // `ReplicationControlManager.handleAssignReplicasToDirs` runs before it
    // looks at the reported rows. Fences out a stale or restarted broker: an
    // unregistered id (a negative wire value included) gets
    // `BROKER_ID_NOT_REGISTERED`, and a registered id reporting the wrong
    // epoch gets `STALE_BROKER_EPOCH`.
    let broker_slot_id = match check_broker_epoch(&image, req.broker_id, req.broker_epoch) {
        Ok(broker_slot_id) => broker_slot_id,
        Err(error_code) => {
            return Ok(AssignReplicasToDirsResponse {
                error_code,
                ..Default::default()
            });
        }
    };
    if image.finalized_metadata_version().is_some_and(|level| {
        level < krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL
    }) {
        return Ok(AssignReplicasToDirsResponse {
            error_code: codes::UNSUPPORTED_VERSION,
            ..Default::default()
        });
    }
    let AssignmentPlan { changes, response } = plan_assignments(&image, broker_slot_id, &req);

    if !changes.is_empty()
        && let Err(e) = controller.submit_change(changes).await
    {
        return Err(BrokerError::Replication(format!("submit_change: {e}")));
    }

    Ok(response)
}

#[cfg(test)]
mod tests {
    use {
        super::{
            test_support::{VERSION, handle_allowed, own_broker_epoch, request},
            *,
        },
        crate::{
            broker::Broker,
            handlers::assign_replicas_to_dirs::test_support::DirectoryAssignmentSetup,
            test_support::{peer, principal, request_context, wait_for_controller_leader},
        },
        assert2::assert,
        krabka_metadata::{FeatureLevelRecord, MetadataRecord, PartitionRecord, TopicRecord},
        krabka_protocol::{
            owned::assign_replicas_to_dirs_response::{
                DirectoryData as RespDirData, PartitionData as RespPartData,
                TopicData as RespTopicData,
            },
            primitives::uuid::Uuid as ProtocolUuid,
        },
    };

    macro_rules! seeded_topic_fixture {
        (
            (
                $handle:ident,
                $directory:ident,
                $broker:ident,
                $epoch:ident,
                $dir_id:ident,
                $topic_id:ident
            )
        ) => {
            broker_fixture!(
                ($handle, $directory, $broker),
                crate::test_support::start_broker_with(|_| {}),
                controller_leader
            );
            let $epoch = own_broker_epoch(&$broker);
            let $dir_id = uuid::Uuid::from_u128(0xAA);
            let $topic_id = uuid::Uuid::from_u128(0xBB);
            seed_topic(&$broker, $topic_id).await;
        };
    }

    fn check_partition_directory(broker: &Broker, directory: uuid::Uuid) {
        let image = broker.controller.current_image();
        let partition = image.partition("t", 0).expect("partition");
        assert!(partition.directories == vec![directory]);
    }

    /// The topic reference and the partition that one request row reports.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum Report {
        /// Partition 0 of the seeded topic, which broker 1 replicates.
        KnownPartition,
        /// Partition 7 of the seeded topic, which does not exist.
        UnknownPartition,
        /// A non-zero topic id that names no topic.
        UnknownTopicId,
        /// The zero topic id.
        ZeroTopicId,
    }

    /// Kafka's `ReplicationControlManager.handleAssignReplicasToDirs` answers
    /// `UNKNOWN_TOPIC_ID` for a topic id that names no topic, the zero id
    /// included, and `UNKNOWN_TOPIC_OR_PARTITION` for a partition that the
    /// topic does not have.
    #[tokio::test]
    async fn handle_answers_each_partition_with_kafkas_error_code() {
        seeded_topic_fixture!((
            broker_handle,
            _dir,
            broker,
            broker_epoch,
            dir_uuid,
            topic_uuid
        ));

        let cases = [
            (Report::KnownPartition, codes::NONE),
            (Report::UnknownPartition, codes::UNKNOWN_TOPIC_OR_PARTITION),
            (Report::UnknownTopicId, codes::UNKNOWN_TOPIC_ID),
            (Report::ZeroTopicId, codes::UNKNOWN_TOPIC_ID),
        ];
        let mut actual = Vec::with_capacity(cases.len());
        let mut expected = Vec::with_capacity(cases.len());
        for (report, error_code) in cases {
            let (topic_id, partition_index) = match report {
                Report::KnownPartition => (topic_uuid, 0),
                Report::UnknownPartition => (topic_uuid, 7),
                Report::UnknownTopicId => (uuid::Uuid::from_u128(0xCC), 0),
                Report::ZeroTopicId => (uuid::Uuid::nil(), 0),
            };
            let resp = handle_allowed(
                &broker,
                request(DirectoryAssignmentSetup {
                    broker_epoch,
                    dir: dir_uuid,
                    topic: topic_id,
                    partitions: &[partition_index],
                    ..Default::default()
                }),
            )
            .await
            .expect("AssignReplicasToDirs handler");
            actual.push((report, resp));
            expected.push((
                report,
                krabka_macros::unthrottled_wire!(AssignReplicasToDirsResponse {
                    error_code: codes::NONE,
                    directories: vec![krabka_macros::tagged_wire!(RespDirData {
                        id: ProtocolUuid(dir_uuid.into_bytes()),
                        topics: vec![krabka_macros::tagged_wire!(RespTopicData {
                            topic_id: ProtocolUuid(topic_id.into_bytes()),
                            partitions: vec![krabka_macros::tagged_wire!(RespPartData {
                                partition_index,
                                error_code,
                            })],
                        })],
                    })],
                }),
            ));
        }
        assert!(actual == expected);
        check_partition_directory(&broker, dir_uuid);

        broker_handle.shutdown().await;
    }

    /// Seed topic "t" with id `topic_uuid` and partition 0 on broker 1.
    async fn seed_topic(broker: &Broker, topic_uuid: uuid::Uuid) {
        broker
            .controller
            .submit_change(vec![
                MetadataRecord::V1Topic(TopicRecord {
                    name: "t".into(),
                    topic_id: topic_uuid,
                    partitions: 1,
                    replication_factor: 1,
                }),
                MetadataRecord::V1Partition(PartitionRecord {
                    directories: vec![uuid::Uuid::nil()],
                    ..crate::handlers::test_support::replicated_partition()
                        .topic("t")
                        .leader(1)
                        .replicas(&[krabka_audit::NodeId(1)])
                        .call()
                }),
            ])
            .await
            .expect("seed partition");
    }

    #[tokio::test]
    async fn handle_leader_commits_known_directory_assignment() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            crate::test_support::start_broker_with(|_| {}),
            controller_leader
        );
        let dir_uuid = uuid::Uuid::from_u128(0xAA);
        let topic_uuid = uuid::Uuid::from_u128(0xBB);
        seed_topic(&broker, topic_uuid).await;
        let req = request(DirectoryAssignmentSetup {
            broker_epoch: own_broker_epoch(&broker),
            dir: dir_uuid,
            topic: topic_uuid,
            ..Default::default()
        });

        let resp = handle_allowed(&broker, req)
            .await
            .expect("AssignReplicasToDirs handler");

        assert!(resp.error_code == codes::NONE, "{resp:?}");
        assert!(resp.directories[0].topics[0].partitions[0].error_code == codes::NONE);
        check_partition_directory(&broker, dir_uuid);
        broker_handle.shutdown().await;
    }

    #[tokio::test]
    async fn handle_rejects_directory_assignment_below_kip_858_metadata_version() {
        broker_fixture!(
            (broker_handle, _dir, broker),
            crate::test_support::start_broker_with(|_| {}),
            controller_leader
        );
        let topic_uuid = uuid::Uuid::from_u128(0xBB);
        broker
            .controller
            .submit_change(vec![MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                name: krabka_metadata::metadata_version::METADATA_VERSION_FEATURE.into(),
                level: krabka_metadata::metadata_version::DIRECTORY_ASSIGNMENT_MIN_LEVEL - 1,
            })])
            .await
            .expect("seed downgraded metadata version");
        seed_topic(&broker, topic_uuid).await;

        let resp = handle_allowed(
            &broker,
            request(DirectoryAssignmentSetup {
                broker_epoch: own_broker_epoch(&broker),
                dir: uuid::Uuid::from_u128(0xAA),
                topic: topic_uuid,
                ..Default::default()
            }),
        )
        .await
        .expect("AssignReplicasToDirs handler");

        assert!(resp.error_code == codes::UNSUPPORTED_VERSION, "{resp:?}");
        // PartitionRecord v0 has no directories field at this metadata
        // version, so replay projects the seeded nil slot to an empty vector.
        assert!(
            broker
                .controller
                .current_image()
                .partition("t", 0)
                .expect("partition")
                .directories
                .is_empty()
        );
        broker_handle.shutdown().await;
    }

    /// Kafka's `ControllerApis.handleAssignReplicasToDirs` checks
    /// `ClusterAction` on the cluster before the controller applies any
    /// assignment (#636). A denial answers `CLUSTER_AUTHORIZATION_FAILED` and
    /// commits nothing.
    #[tokio::test]
    async fn handle_needs_cluster_action() {
        broker_fixture!((broker_handle, _dir, broker), principal_grants);
        wait_for_controller_leader(&broker).await;
        let broker_epoch = own_broker_epoch(&broker);
        let dir_uuid = uuid::Uuid::from_u128(0xAA);
        let topic_uuid = uuid::Uuid::from_u128(0xBB);
        seed_topic(&broker, topic_uuid).await;

        let cases = [
            ("none", codes::CLUSTER_AUTHORIZATION_FAILED),
            (
                "Cluster:Alter+Cluster:Describe",
                codes::CLUSTER_AUTHORIZATION_FAILED,
            ),
            ("Cluster:ClusterAction", codes::NONE),
        ];
        let address = peer();
        for (grants, expected_error) in cases {
            let user = principal(grants);
            let ctx = request_context(&user, &address, "assign-test");
            let resp = handle(
                &broker,
                request(DirectoryAssignmentSetup {
                    broker_epoch,
                    dir: dir_uuid,
                    topic: topic_uuid,
                    ..Default::default()
                }),
                VERSION,
                &ctx,
            )
            .await
            .expect("AssignReplicasToDirs handler");
            assert!(resp.error_code == expected_error, "{grants}: {resp:?}");
        }
        broker_handle.shutdown().await;
    }

    /// Kafka's `ClusterControlManager.checkBrokerEpoch`, which
    /// `ReplicationControlManager.handleAssignReplicasToDirs` runs before it
    /// looks at the reported rows (#636). A stale epoch or an unregistered id
    /// is refused and commits nothing; only the current epoch commits the
    /// assignment.
    #[tokio::test]
    async fn handle_fences_stale_and_unregistered_brokers() {
        seeded_topic_fixture!((
            broker_handle,
            _dir,
            broker,
            broker_epoch,
            dir_uuid,
            topic_uuid
        ));

        // A stale epoch is refused, and the seeded directory slot is
        // untouched. `+ 1` rather than `- 1`: a registered epoch of 0 would
        // otherwise turn "stale" into `-1`, which means "not provided" and
        // matches any registration (see `validation::check_broker_epoch`).
        let stale = handle_allowed(
            &broker,
            request(DirectoryAssignmentSetup {
                broker_epoch: broker_epoch + 1,
                dir: dir_uuid,
                topic: topic_uuid,
                ..Default::default()
            }),
        )
        .await
        .expect("AssignReplicasToDirs handler");
        assert!(stale.error_code == codes::STALE_BROKER_EPOCH, "{stale:?}");
        assert!(
            broker
                .controller
                .current_image()
                .partition("t", 0)
                .expect("partition")
                .directories
                == vec![uuid::Uuid::nil()]
        );

        // The current epoch succeeds and commits the assignment.
        let current = handle_allowed(
            &broker,
            request(DirectoryAssignmentSetup {
                broker_epoch,
                dir: dir_uuid,
                topic: topic_uuid,
                ..Default::default()
            }),
        )
        .await
        .expect("AssignReplicasToDirs handler");
        assert!(current.error_code == codes::NONE, "{current:?}");
        assert!(
            broker
                .controller
                .current_image()
                .partition("t", 0)
                .expect("partition")
                .directories
                == vec![dir_uuid]
        );

        // `-1` ("not provided") also succeeds: it is the epoch
        // `crate::assign_dirs::build_request` sends for this broker's own
        // self-reported directory assignments.
        let unspecified = handle_allowed(
            &broker,
            request(DirectoryAssignmentSetup {
                broker_epoch: -1,
                dir: dir_uuid,
                topic: topic_uuid,
                ..Default::default()
            }),
        )
        .await
        .expect("AssignReplicasToDirs handler");
        assert!(unspecified.error_code == codes::NONE, "{unspecified:?}");

        broker_handle.shutdown().await;
    }

    /// An id that names no registration is refused with
    /// `BROKER_ID_NOT_REGISTERED`, the negative wire sentinel included.
    #[tokio::test]
    async fn handle_refuses_an_unregistered_broker_id() {
        use krabka_protocol::owned::assign_replicas_to_dirs_request::{
            AssignReplicasToDirsRequest, DirectoryData as ReqDirData,
        };

        broker_fixture!(
            (broker_handle, _dir, broker),
            crate::test_support::start_broker_with(|_| {}),
            controller_leader
        );

        for broker_id in [99i32, -1] {
            let req = AssignReplicasToDirsRequest {
                broker_id,
                broker_epoch: -1,
                directories: Vec::<ReqDirData>::new(),
                ..Default::default()
            };
            let resp = handle_allowed(&broker, req)
                .await
                .expect("AssignReplicasToDirs handler");
            assert!(
                resp.error_code == codes::BROKER_ID_NOT_REGISTERED,
                "broker_id {broker_id}: {resp:?}"
            );
        }
        broker_handle.shutdown().await;
    }
}
