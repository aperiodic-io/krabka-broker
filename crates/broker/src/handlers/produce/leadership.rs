//! The leadership gate: the checks that decide whether this broker may accept
//! a `Produce` for one partition at all, and the metadata-image comparisons
//! that back them.

use std::sync::Arc;

use krabka_protocol::owned::produce_response::LeaderIdAndEpoch;

use super::{ACKS_ALL, NO_LEADER_ID, topic_settings::topic_min_insync_replicas};
use crate::{codes, partition_registry::PartitionRegistry};

pub(super) struct PartitionGateError {
    pub(super) code: i16,
    pub(super) current_leader: Option<LeaderIdAndEpoch>,
}

/// The broker-wide policy that the per-partition Produce gate applies.
///
/// The gate reads one partition record out of the metadata image and compares
/// it against this node. It needs the node id to decide whether this node
/// leads the partition, the broker default for `min.insync.replicas` for the
/// `acks=all` check, and the witness role, which refuses every client write.
#[derive(Clone, Copy)]
pub(super) struct BrokerProducePolicy {
    /// This node's id, compared against the image's partition leader.
    pub(super) node_id: krabka_metadata::NodeId,
    /// Broker default for `min.insync.replicas`. A topic override wins over
    /// it.
    pub(super) default_min_insync_replicas: i32,
    /// `true` when this node is a data-bearing witness.
    pub(super) is_witness: bool,
}

pub(super) fn validate_partition_gate(
    topic_name: &str,
    partition_index: i32,
    acks: i16,
    partitions: &PartitionRegistry,
    log_dir_status: &crate::log_dir_status::LogDirRegistry,
    image: &krabka_metadata::MetadataImage,
    broker_policy: BrokerProducePolicy,
) -> Result<Arc<crate::partition::Partition>, PartitionGateError> {
    let BrokerProducePolicy {
        node_id: this_node_id,
        default_min_insync_replicas,
        is_witness,
    } = broker_policy;
    let Some(record) = image
        .partition(topic_name, partition_index)
        .filter(|_| !topic_name.is_empty())
    else {
        return Err(PartitionGateError {
            code: codes::UNKNOWN_TOPIC_OR_PARTITION,
            current_leader: None,
        });
    };
    let leader = current_leader_hint(record);
    let Some(partition) = partitions.get(topic_name, krabka_ids::PartitionIndex(partition_index))
    else {
        return Err(PartitionGateError {
            code: codes::NOT_LEADER_OR_FOLLOWER,
            current_leader: Some(leader),
        });
    };
    // A witness replicates the partition and counts toward
    // `min.insync.replicas`, but it serves no client traffic, so it accepts no
    // Produce. The guard is explicit because the leader check below reads
    // `record.leader != this_node_id && !partition.diskless`: a diskless
    // partition skips the leader check outright, so without this guard a
    // diskless Produce could land on a witness. NOT_LEADER_OR_FOLLOWER is the
    // code that makes a Kafka client refresh its metadata and produce
    // somewhere else.
    if is_witness || (record.leader != this_node_id && !partition.diskless) {
        return Err(PartitionGateError {
            code: codes::NOT_LEADER_OR_FOLLOWER,
            current_leader: Some(leader),
        });
    }
    if log_dir_status.is_offline(&partition.log_dir.load()) {
        return Err(PartitionGateError {
            code: codes::KAFKA_STORAGE_ERROR,
            current_leader: None,
        });
    }
    let effective_min_isr = effective_min_isr_of(
        image,
        (this_node_id, default_min_insync_replicas),
        topic_name,
        record,
    );
    if acks == ACKS_ALL && record.isr.len() < effective_min_isr {
        return Err(PartitionGateError {
            code: codes::NOT_ENOUGH_REPLICAS,
            current_leader: None,
        });
    }
    Ok(partition)
}

/// Kafka's `Partition.effectiveMinIsr`: the `min.insync.replicas` that
/// `image` resolves for `topic` on `node`, capped at the size of `record`'s
/// replica assignment. `node` is this broker's id and `default` its
/// command-line `default_min_insync_replicas`, the last layer.
fn effective_min_isr_of(
    image: &krabka_metadata::MetadataImage,
    (node, default): (krabka_metadata::NodeId, i32),
    topic: &str,
    record: &krabka_metadata::PartitionRecord,
) -> usize {
    let configured_min_isr = topic_min_insync_replicas(image, node, topic, default);
    usize::try_from(configured_min_isr)
        .unwrap_or(0)
        .min(record.replicas.len())
}

/// The effective `min.insync.replicas` of the partition an `acks=all`
/// append went to, as `image` holds it now, or the error code of a partition
/// the image no longer holds.
///
/// An `acks=all` completion reads this when its high-watermark wait ends, as
/// Kafka's `Partition.checkEnoughReplicasReachOffset` reads `effectiveMinIsr`
/// then, so a `min.insync.replicas` change during the wait applies to it.
///
/// The partition is the one the append was admitted to only while the topic
/// still has `admitted_topic_id`: a topic deleted and recreated under the
/// same name is another partition. Kafka's `DelayedProduce` finds its
/// partition through `ReplicaManager.getPartitionOrError`, and a deleted
/// replica answers `NOT_LEADER_OR_FOLLOWER` when the metadata still names the
/// partition, here the recreated topic's, and `UNKNOWN_TOPIC_OR_PARTITION`
/// when it does not. A leader or partition epoch change is not a new
/// partition: Kafka keeps the same `Partition` across both, and the partition
/// epoch moves on every ISR change, which is what this check reads.
pub(super) fn current_effective_min_isr(
    image: &krabka_metadata::MetadataImage,
    (topic, partition_index): (&str, i32),
    admitted_topic_id: Option<uuid::Uuid>,
    min_isr_layers: (krabka_metadata::NodeId, i32),
) -> Result<usize, i16> {
    let record = image
        .partition(topic, partition_index)
        .ok_or(codes::UNKNOWN_TOPIC_OR_PARTITION)?;
    if image.topic(topic).map(|topic| topic.topic_id) != admitted_topic_id {
        return Err(codes::NOT_LEADER_OR_FOLLOWER);
    }
    Ok(effective_min_isr_of(image, min_isr_layers, topic, record))
}

pub(super) fn diskless_role_ready(
    partition: &crate::partition::Partition,
    record: &krabka_metadata::PartitionRecord,
) -> bool {
    krabka_metadata::NodeId(
        partition
            .current_leader
            .load(std::sync::atomic::Ordering::Acquire),
    ) == record.leader
        && partition
            .current_leader_epoch
            .load(std::sync::atomic::Ordering::Acquire)
            == record.leader_epoch.0
}

pub(super) fn replication_target_matches_image(
    target: &crate::partition::ReplicationTarget,
    topic_id: Option<uuid::Uuid>,
    record: &krabka_metadata::PartitionRecord,
) -> bool {
    target.topic_id == topic_id
        && target.leader_node_id == record.leader
        && target.leader_epoch == record.leader_epoch
}

pub(super) fn replica_state_matches_image(
    state: &crate::replica_state::ReplicaState,
    record: &krabka_metadata::PartitionRecord,
) -> bool {
    state.current_leader_epoch == krabka_ids::LeaderEpoch(record.leader_epoch.0)
        && state.isr.len() == record.isr.len()
        && record.isr.iter().all(|node| state.isr.contains(node))
}

pub(super) fn current_leader_hint(record: &krabka_metadata::PartitionRecord) -> LeaderIdAndEpoch {
    LeaderIdAndEpoch {
        leader_id: i32::try_from(record.leader.0).unwrap_or(NO_LEADER_ID),
        leader_epoch: record.leader_epoch.0,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::{BrokerConfigRecord, MetadataImage, MetadataRecord};
    use uuid::Uuid;

    use super::*;
    use crate::handlers::produce::test_support::image_with_topic;

    #[test]
    fn replication_target_must_match_the_complete_image_identity() {
        let image = image_with_topic("orders", &[1, 2]);
        let record = image.partition("orders", 0).expect("partition");
        let current = crate::partition::ReplicationTarget {
            topic_id: Some(Uuid::nil()),
            leader_node_id: record.leader,
            leader_epoch: record.leader_epoch,
        };
        assert!(replication_target_matches_image(
            &current,
            Some(Uuid::nil()),
            record
        ));

        assert!(!replication_target_matches_image(
            &crate::partition::ReplicationTarget {
                leader_epoch: krabka_metadata::LeaderEpoch(record.leader_epoch.0 + 1),
                ..current
            },
            Some(Uuid::nil()),
            record
        ));
        assert!(!replication_target_matches_image(
            &current,
            Some(Uuid::new_v4()),
            record
        ));
    }

    /// An `acks=all` completion reads the effective minimum only for the
    /// partition it was admitted to: a topic deleted and recreated under the
    /// same name answers `NOT_LEADER_OR_FOLLOWER`, and a deleted one
    /// `UNKNOWN_TOPIC_OR_PARTITION`, as Kafka's `getPartitionOrError` does.
    /// Epoch changes of the same partition keep it.
    #[test]
    fn the_completion_minimum_belongs_to_the_admitted_partition() {
        use krabka_metadata::{DeleteTopicRecord, PartitionRecord, TopicRecord};
        let admitted = Uuid::from_u128(1);
        let image_of = |topic_id: Option<Uuid>, leader_epoch: i32, partition_epoch: i32| {
            let mut image = image_with_topic("orders", &[1, 2, 3]);
            image.apply(&MetadataRecord::V1DeleteTopic(DeleteTopicRecord {
                name: "orders".into(),
            }));
            if let Some(topic_id) = topic_id {
                image.apply(&MetadataRecord::V1Topic(TopicRecord {
                    name: "orders".into(),
                    topic_id,
                    partitions: 1,
                    replication_factor: 3,
                }));
                image.apply(&MetadataRecord::V1Partition(PartitionRecord {
                    topic: "orders".into(),
                    partition: 0,
                    leader: krabka_audit::NodeId(1),
                    replicas: [1, 2, 3].map(krabka_audit::NodeId).to_vec(),
                    isr: [1, 2].map(krabka_audit::NodeId).to_vec(),
                    leader_epoch: krabka_metadata::LeaderEpoch(leader_epoch),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch,
                }));
            }
            image
        };
        let cases = [
            (
                "the admitted partition",
                image_of(Some(admitted), 0, 0),
                Ok(2),
            ),
            (
                "the admitted partition at later epochs",
                image_of(Some(admitted), 4, 9),
                Ok(2),
            ),
            (
                "a topic recreated under the same name",
                image_of(Some(Uuid::from_u128(2)), 0, 0),
                Err(codes::NOT_LEADER_OR_FOLLOWER),
            ),
            (
                "a deleted topic",
                image_of(None, 0, 0),
                Err(codes::UNKNOWN_TOPIC_OR_PARTITION),
            ),
        ];
        let mut actual = Vec::new();
        let mut expected = Vec::new();
        for (name, image, want) in cases {
            actual.push((
                name,
                current_effective_min_isr(
                    &image,
                    ("orders", 0),
                    Some(admitted),
                    (krabka_audit::NodeId(1), 2),
                ),
            ));
            expected.push((name, want));
        }
        assert!(actual == expected);
    }

    #[test]
    fn replica_state_must_install_the_image_epoch_and_exact_isr() {
        let image = image_with_topic("orders", &[1, 2]);
        let record = image.partition("orders", 0).expect("partition");
        let mut state = crate::replica_state::ReplicaState::new();
        assert!(!replica_state_matches_image(&state, record));

        state.install_isr(
            &record.isr,
            &record.replicas,
            record.leader,
            std::time::Instant::now(),
        );
        assert!(replica_state_matches_image(&state, record));

        state.current_leader_epoch = krabka_ids::LeaderEpoch(record.leader_epoch.0 + 1);
        assert!(!replica_state_matches_image(&state, record));
    }

    #[tokio::test]
    async fn diskless_produce_waits_for_installed_role_and_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let image = image_with_topic("orders", &[1]);
        let record = image.partition("orders", 0).expect("partition");
        let log = krabka_log::Log::open(
            crate::log_dir::partition_dir(dir.path(), "orders", 0),
            krabka_log::LogConfig::default(),
        )
        .unwrap();
        let partition = crate::test_support::spawn_standalone_partition(
            dir.path(),
            log,
            crate::test_support::StandalonePartitionSetup {
                storage: crate::test_support::StorageMode::Diskless,
                ..Default::default()
            },
        );

        assert!(!diskless_role_ready(&partition, record));
        partition
            .install_leader_change(record.leader.0, record.leader_epoch.0)
            .await;
        assert!(diskless_role_ready(&partition, record));
        partition
            .install_leader_change(record.leader.0, record.leader_epoch.0 + 1)
            .await;
        assert!(!diskless_role_ready(&partition, record));
    }

    /// Spawn one local partition and run the Produce gate over it.
    ///
    /// Returns `None` when the gate admits the write, or the complete gate
    /// error when it refuses. The gate itself is synchronous, but
    /// `spawn_partition` starts the writer-actor task, so the callers still
    /// need a Tokio runtime.
    fn produce_gate(
        image: &MetadataImage,
        node_id: krabka_audit::NodeId,
        is_witness: bool,
        diskless: bool,
    ) -> Option<(i16, Option<LeaderIdAndEpoch>)> {
        gate_with_acks(image, node_id, is_witness, diskless, 1, 1)
    }

    /// [`produce_gate`], with the two inputs the `min.insync.replicas` rule
    /// reads: the request's `acks`, and this broker's command-line
    /// `default_min_insync_replicas`.
    fn gate_with_acks(
        image: &MetadataImage,
        node_id: krabka_audit::NodeId,
        is_witness: bool,
        diskless: bool,
        acks: i16,
        default_min_insync_replicas: i32,
    ) -> Option<(i16, Option<LeaderIdAndEpoch>)> {
        let dir = tempfile::tempdir().expect("tempdir");
        let log = krabka_log::Log::open(
            crate::log_dir::partition_dir(dir.path(), "orders", 0),
            krabka_log::LogConfig::default(),
        )
        .expect("open log");
        let partitions = crate::partition_registry::PartitionRegistry::new();
        partitions.insert(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            crate::test_support::spawn_standalone_partition(
                dir.path(),
                log,
                crate::test_support::StandalonePartitionSetup {
                    storage: crate::test_support::StorageMode::from_wire(diskless),
                    ..Default::default()
                },
            ),
        );
        validate_partition_gate(
            "orders",
            0,
            acks,
            &partitions,
            &crate::log_dir_status::LogDirRegistry::default(),
            image,
            BrokerProducePolicy {
                node_id,
                default_min_insync_replicas,
                is_witness,
            },
        )
        .err()
        .map(|error| (error.code, error.current_leader))
    }

    /// The cluster-wide dynamic `min.insync.replicas` default gates
    /// `acks=all` here, exactly as it gates the ELR the controller keeps.
    ///
    /// Node 1 leads `orders` with an ISR of one and a replica set of three,
    /// and the cluster default asks for two. Reading only the topic override
    /// left this gate on the broker's own default of 1, so the write was
    /// accepted at an ISR the controller still called below-min -- and the
    /// replicas the controller held in the KIP-966 eligible set fell behind
    /// a committed record while it went on offering them as leaders.
    #[tokio::test]
    async fn acks_all_honours_the_cluster_wide_min_isr_default_the_elr_is_kept_against() {
        let mut image = image_with_topic("orders", &[1, 2, 3]);
        image.apply(&MetadataRecord::V1Partition(
            krabka_metadata::PartitionRecord {
                isr: vec![krabka_audit::NodeId(1)],
                partition_epoch: 1,
                ..crate::handlers::test_support::replicated_partition(
                    crate::handlers::test_support::ReplicatedPartitionSetup {
                        leader: krabka_audit::NodeId(1),
                        replicas: &[
                            krabka_audit::NodeId(1),
                            krabka_audit::NodeId(2),
                            krabka_audit::NodeId(3),
                        ],
                        ..Default::default()
                    },
                )
            },
        ));
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
            config_name: crate::config_keys::MIN_INSYNC_REPLICAS.into(),
            config_value: Some("2".into()),
        }));

        let refused = gate_with_acks(&image, krabka_audit::NodeId(1), false, false, -1, 1);

        assert!(
            refused == Some((crate::codes::NOT_ENOUGH_REPLICAS, None)),
            "got {refused:?}"
        );
        // The same partition still takes an acks=1 write: the rule is the
        // `acks=all` durability gate, not a partition-wide refusal.
        let admitted = gate_with_acks(&image, krabka_audit::NodeId(1), false, false, 1, 1);
        assert!(admitted.is_none(), "got {admitted:?}");
    }

    /// A per-node dynamic `min.insync.replicas` (`kafka-configs --entity-type
    /// brokers --entity-name 1`) gates `acks=all` on that node and only on it,
    /// and it outranks the cluster-wide default. Kafka pushes each node's
    /// dynamic value into its own partitions' `LogConfig`, from which
    /// `Partition.effectiveMinIsr` reads it.
    #[tokio::test]
    async fn acks_all_honours_this_nodes_own_min_isr_over_the_cluster_default() {
        for (label, cluster_default, node_one, node_two, want_refused) in [
            ("this node's value refuses", None, Some("2"), None, true),
            (
                "this node's value outranks a lower cluster default",
                Some("1"),
                Some("2"),
                None,
                true,
            ),
            (
                "this node's value outranks a higher cluster default",
                Some("2"),
                Some("1"),
                None,
                false,
            ),
            (
                "another node's value does not gate this one",
                None,
                None,
                Some("2"),
                false,
            ),
        ] {
            let mut image = image_with_topic("orders", &[1, 2, 3]);
            image.apply(&MetadataRecord::V1Partition(
                krabka_metadata::PartitionRecord {
                    topic: "orders".into(),
                    partition: 0,
                    leader: krabka_audit::NodeId(1),
                    replicas: [1, 2, 3].map(krabka_audit::NodeId).to_vec(),
                    isr: vec![krabka_audit::NodeId(1)],
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 1,
                },
            ));
            for (node, value) in [
                (
                    krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                    cluster_default,
                ),
                (krabka_audit::NodeId(1), node_one),
                (krabka_audit::NodeId(2), node_two),
            ] {
                if let Some(value) = value {
                    image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                        node_id: node,
                        config_name: crate::config_keys::MIN_INSYNC_REPLICAS.into(),
                        config_value: Some(value.into()),
                    }));
                }
            }

            assert_min_isr_gate(&image, label, want_refused);
        }
    }

    fn assert_min_isr_gate(image: &MetadataImage, label: &str, want_refused: bool) {
        let got = gate_with_acks(image, krabka_audit::NodeId(1), false, false, -1, 1);
        let refused = got
            .as_ref()
            .is_some_and(|(code, _)| *code == crate::codes::NOT_ENOUGH_REPLICAS);
        assert!(refused == want_refused, "{label}: got {got:?}");
    }

    /// Kafka's `Partition.effectiveMinIsr` clamps `min.insync.replicas` to
    /// the replica count before comparing it against the ISR. A topic with
    /// replication factor 1 on a cluster whose default asks for 2 must still
    /// accept `acks=all`, because the clamp brings the effective threshold
    /// down to 1.
    #[tokio::test]
    async fn acks_all_clamps_min_isr_to_the_replica_count() {
        for (name, replicas, isr_len, cluster_min_isr, want_refused) in [
            ("rf1 clamps below the cluster default", 1, 1, 2, false),
            ("rf1 with an empty isr still refuses", 1, 0, 2, true),
            ("rf3 below the cluster default refuses", 3, 1, 2, true),
            ("rf3 at the cluster default admits", 3, 2, 2, false),
        ] {
            let replica_ids: Vec<krabka_audit::NodeId> =
                (1..=replicas).map(krabka_audit::NodeId).collect();
            let mut image = image_with_topic(
                "orders",
                &replica_ids.iter().map(|n| n.0).collect::<Vec<_>>(),
            );
            image.apply(&MetadataRecord::V1Partition(
                krabka_metadata::PartitionRecord {
                    topic: "orders".into(),
                    partition: 0,
                    leader: krabka_audit::NodeId(1),
                    replicas: replica_ids.clone(),
                    isr: replica_ids.into_iter().take(isr_len).collect(),
                    leader_epoch: krabka_metadata::LeaderEpoch(0),
                    adding_replicas: vec![],
                    removing_replicas: vec![],
                    directories: vec![],
                    partition_epoch: 1,
                },
            ));
            image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
                node_id: krabka_metadata::DEFAULT_BROKER_CONFIG_NODE_ID,
                config_name: crate::config_keys::MIN_INSYNC_REPLICAS.into(),
                config_value: Some(cluster_min_isr.to_string()),
            }));

            assert_min_isr_gate(&image, name, want_refused);
        }
    }

    #[tokio::test]
    async fn witness_refuses_every_produce_including_a_diskless_partition() {
        // Node 1 leads `orders` at epoch 0 and node 2 follows. A refused row
        // carries the real leader, so a Kafka client re-targets without a full
        // Metadata round-trip.
        let image = image_with_topic("orders", &[1, 2]);
        let refused = Some((
            crate::codes::NOT_LEADER_OR_FOLLOWER,
            Some(LeaderIdAndEpoch {
                leader_id: 1,
                leader_epoch: 0,
                ..Default::default()
            }),
        ));
        for (name, node_id, is_witness, diskless, want) in [
            (
                "witness leads a classic partition",
                1,
                true,
                false,
                refused.clone(),
            ),
            // The leader check reads `leader != this_node && !diskless`, so a
            // diskless partition skips it. The witness guard is the only thing
            // that refuses these two rows.
            (
                "witness leads a diskless partition",
                1,
                true,
                true,
                refused.clone(),
            ),
            (
                "witness follows a diskless partition",
                2,
                true,
                true,
                refused.clone(),
            ),
            (
                "plain broker leads a classic partition",
                1,
                false,
                false,
                None,
            ),
            (
                "plain broker follows a diskless partition",
                2,
                false,
                true,
                None,
            ),
            (
                "plain broker follows a classic partition",
                2,
                false,
                false,
                refused.clone(),
            ),
        ] {
            let got = produce_gate(&image, krabka_audit::NodeId(node_id), is_witness, diskless);
            assert!(got == want, "{name}: got {got:?}, want {want:?}");
        }
    }

    #[tokio::test]
    async fn witness_on_another_node_leaves_this_brokers_produce_gate_alone() {
        // Node 2 carries `broker.witness=true` in the image. This node is 1
        // and it leads the partition, so the gate must still admit the write.
        let mut image = image_with_topic("orders", &[1, 2]);
        image.apply(&MetadataRecord::V1BrokerConfig(BrokerConfigRecord {
            node_id: krabka_audit::NodeId(2),
            config_name: crate::config_keys::BROKER_WITNESS.into(),
            config_value: Some(crate::config_keys::WITNESS_TRUE.into()),
        }));
        let got = produce_gate(&image, krabka_audit::NodeId(1), false, false);
        assert!(got.is_none(), "got {got:?}");
    }
}
