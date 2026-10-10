//! Lifecycle of the diskless WAL follower tasks: which shards this broker
//! follows for a given placement, and the start and stop of one follower task
//! per shard.

use std::collections::HashMap;

use krabka_ids::PartitionIndex;
use krabka_metadata::MetadataImage;
#[cfg(test)]
use krabka_raft::NodeId;
use tracing::warn;

use super::{ReplicatorSupervisor, WalFollowerSpec, WalFollowerTask, resolve_leader_endpoint};

impl ReplicatorSupervisor {
    pub(super) fn desired_wal_followers(
        &self,
        image: &MetadataImage,
        placements: &HashMap<
            crate::wal::quorum::registry::ShardId,
            crate::wal::quorum::registry::WalPlacement,
        >,
    ) -> HashMap<crate::wal::quorum::registry::ShardId, WalFollowerSpec> {
        image
            .all_partitions()
            .filter(|partition| {
                crate::config_keys::resolve_diskless(image.topic_config(&partition.topic))
            })
            .filter_map(|partition| {
                let topic = image.topic(&partition.topic)?;
                let shard = crate::wal::quorum::registry::ShardId {
                    topic_id: topic.topic_id,
                    partition: PartitionIndex(partition.partition),
                };
                let placement = placements.get(&shard)?;
                (placement.voters.len() == self.diskless_wal_local_replica_count
                    && placement.voters.first() == Some(&partition.leader)
                    && partition.leader != self.node_id
                    && placement.voters.contains(&self.node_id))
                .then(|| {
                    (
                        shard,
                        WalFollowerSpec {
                            topic: partition.topic.clone(),
                            leader: partition.leader,
                            leader_epoch: partition.leader_epoch,
                        },
                    )
                })
            })
            .collect()
    }

    pub(super) async fn stop_obsolete_wal_followers(
        &self,
        desired: &HashMap<crate::wal::quorum::registry::ShardId, WalFollowerSpec>,
    ) {
        let current = self
            .wal_tasks
            .iter()
            .map(|entry| *entry.key())
            .collect::<Vec<_>>();
        for shard in current {
            let target_matches = match (desired.get(&shard), self.wal_task_targets.get(&shard)) {
                (Some(desired), Some(current)) => {
                    desired == current.value()
                        && self
                            .wal_tasks
                            .get(&shard)
                            .is_some_and(|task| !task.handle.is_finished())
                }
                _ => false,
            };
            if target_matches {
                continue;
            }
            let Some((_, task)) = self.wal_tasks.remove(&shard) else {
                continue;
            };
            let target = self
                .wal_task_targets
                .remove(&shard)
                .map(|(_, target)| target);
            task.shutdown.cancel();
            let _ = task.handle.await;
            if !self.wal_shards.local_is_voter(shard)
                && let Some(target) = target
            {
                for log_dir in &self.log_dirs {
                    if let Err(error) = crate::wal::quorum::remove_shard(
                        self.wal_shards.as_ref(),
                        log_dir,
                        &target.topic,
                        shard.topic_id,
                        shard.partition,
                        crate::wal::quorum::ShardRemoval::All,
                    ) {
                        warn!(
                            topic = %target.topic,
                            partition = shard.partition.0,
                            path = %log_dir.display(),
                            error = %error,
                            "failed to remove obsolete follower WAL shard"
                        );
                    }
                }
            }
        }
    }

    pub(super) fn spawn_wal_followers(
        &self,
        image: &MetadataImage,
        desired: HashMap<crate::wal::quorum::registry::ShardId, WalFollowerSpec>,
    ) {
        for (shard, spec) in desired {
            if self.wal_tasks.contains_key(&shard) {
                continue;
            }
            let Some(broker) = image.broker(spec.leader) else {
                warn!(
                    topic = %spec.topic,
                    partition = shard.partition.0,
                    leader = spec.leader.0,
                    "diskless WAL leader broker is not registered; deferring follower"
                );
                continue;
            };
            let (leader_host, leader_port) =
                resolve_leader_endpoint(broker, &self.inter_broker_listener_name);
            let token = self.shutdown.child_token();
            self.wal_task_targets.insert(shard, spec.clone());
            let handle = tokio::spawn(crate::wal::quorum::follower::run(
                crate::wal::quorum::follower::Config {
                    node_id: self.node_id,
                    topic: spec.topic,
                    shard,
                    leader_node_id: spec.leader,
                    leader_epoch: spec.leader_epoch.0,
                    leader_host,
                    leader_port,
                    log_dirs: self.log_dirs.clone(),
                    storage: self.log_config.clone(),
                    client_id: self.client_id.clone(),
                    shutdown: token.clone(),
                    connection: self.connection_config(),
                },
            ));
            self.wal_tasks.insert(
                shard,
                WalFollowerTask {
                    shutdown: token,
                    handle,
                },
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::MetadataRecord;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::*;
    use crate::{
        replicator_supervisor::test_support::{
            broker_record, image_with, partition_record, supervisor_fixture,
        },
        wal::quorum::registry::{ShardId, WalPlacement},
    };

    /// A three-voter epoch-seven placement, retaining the original discarded fixture guards.
    fn wal_follower_fixture(
        image: &MetadataImage,
        topic_id: Uuid,
    ) -> (
        ReplicatorSupervisor,
        ShardId,
        HashMap<ShardId, WalPlacement>,
    ) {
        let (supervisor, _, _, _) = supervisor_fixture(image.clone());
        let shard = ShardId {
            topic_id,
            partition: PartitionIndex(0),
        };
        let placements = maplit::hashmap! {shard => WalPlacement { voters: vec![NodeId(1), NodeId(2), NodeId(3)], leader_epoch: 7 }};
        (supervisor, shard, placements)
    }

    #[test]
    fn desired_wal_followers_include_only_complete_nonleader_placements() {
        use std::collections::BTreeMap;

        let topic_id = Uuid::from_u128(18);
        let mut overrides = BTreeMap::new();
        overrides.insert("krabka.diskless".into(), "true".into());
        let image = image_with(&[
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(1))),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(2))),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(3))),
            MetadataRecord::V1Topic(crate::test_support::single_partition_topic(
                "diskless", topic_id,
            )),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    topic: "diskless",
                    replicas: vec![NodeId(1)],
                    epoch: krabka_metadata::LeaderEpoch(7),
                    ..Default::default()
                },
            ),
            MetadataRecord::V1TopicConfig(krabka_metadata::TopicConfigRecord {
                topic: "diskless".into(),
                overrides,
            }),
        ]);
        let (supervisor, shard, complete) = wal_follower_fixture(&image, topic_id);

        let desired = supervisor.desired_wal_followers(&image, &complete);

        assert!(
            desired.get(&shard)
                == Some(&WalFollowerSpec {
                    topic: "diskless".into(),
                    leader: NodeId(1),
                    leader_epoch: krabka_metadata::LeaderEpoch(7),
                })
        );
        let short = maplit::hashmap! {shard => WalPlacement {
            voters: vec![NodeId(1), NodeId(2)],
            leader_epoch: 7,
        }};
        assert!(supervisor.desired_wal_followers(&image, &short).is_empty());
    }

    #[tokio::test]
    async fn reconcile_wal_followers_retains_only_the_current_target() {
        use std::collections::BTreeMap;

        let topic_id = Uuid::from_u128(19);
        let image_at_epoch = |leader_epoch| {
            let mut overrides = BTreeMap::new();
            overrides.insert("krabka.diskless".into(), "true".into());
            image_with(&[
                MetadataRecord::V1Topic(crate::test_support::single_partition_topic(
                    "diskless", topic_id,
                )),
                partition_record(
                    crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                        topic: "diskless",
                        replicas: vec![NodeId(1)],
                        epoch: krabka_metadata::LeaderEpoch(leader_epoch),
                        ..Default::default()
                    },
                ),
                MetadataRecord::V1TopicConfig(krabka_metadata::TopicConfigRecord {
                    topic: "diskless".into(),
                    overrides,
                }),
            ])
        };
        let image = image_at_epoch(7);
        let (supervisor, shard, placements) = wal_follower_fixture(&image, topic_id);
        let target = WalFollowerSpec {
            topic: "diskless".into(),
            leader: NodeId(1),
            leader_epoch: krabka_metadata::LeaderEpoch(7),
        };
        let current = CancellationToken::new();
        let current_task = current.clone();
        supervisor.wal_tasks.insert(
            shard,
            WalFollowerTask {
                shutdown: current.clone(),
                handle: tokio::spawn(async move { current_task.cancelled().await }),
            },
        );
        supervisor.wal_task_targets.insert(shard, target);

        let desired = supervisor.desired_wal_followers(&image, &placements);
        supervisor.stop_obsolete_wal_followers(&desired).await;
        supervisor.spawn_wal_followers(&image, desired);

        assert!(!current.is_cancelled());
        assert!(supervisor.wal_tasks.contains_key(&shard));
        assert!(supervisor.wal_task_targets.contains_key(&shard));

        let next = image_at_epoch(8);
        let desired = supervisor.desired_wal_followers(&next, &placements);
        supervisor.stop_obsolete_wal_followers(&desired).await;
        supervisor.spawn_wal_followers(&next, desired);

        assert!(current.is_cancelled());
        assert!(!supervisor.wal_tasks.contains_key(&shard));
        assert!(!supervisor.wal_task_targets.contains_key(&shard));

        let removed = CancellationToken::new();
        let removed_task = removed.clone();
        supervisor.wal_tasks.insert(
            shard,
            WalFollowerTask {
                shutdown: removed.clone(),
                handle: tokio::spawn(async move { removed_task.cancelled().await }),
            },
        );
        supervisor.wal_task_targets.insert(
            shard,
            WalFollowerSpec {
                topic: "diskless".into(),
                leader: NodeId(1),
                leader_epoch: krabka_metadata::LeaderEpoch(8),
            },
        );
        let follower_shard = crate::wal::quorum::shard_dir(
            &supervisor.log_dirs[0],
            "diskless",
            Some(topic_id),
            PartitionIndex(0),
        );
        std::fs::create_dir_all(follower_shard.join("voter-2")).unwrap();
        std::fs::write(follower_shard.join("voter-2/checkpoint"), b"durable").unwrap();

        let empty = MetadataImage::new(Uuid::nil());
        let desired = supervisor.desired_wal_followers(&empty, &HashMap::new());
        supervisor.stop_obsolete_wal_followers(&desired).await;
        supervisor.spawn_wal_followers(&empty, desired);

        assert!(removed.is_cancelled());
        assert!(!supervisor.wal_tasks.contains_key(&shard));
        assert!(!supervisor.wal_task_targets.contains_key(&shard));
        assert!(!follower_shard.exists());
    }
}
