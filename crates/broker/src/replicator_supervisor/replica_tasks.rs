//! The follower fetchers: which leader each partition is followed from, the
//! per-partition configuration a fetcher applies its responses with, and the
//! reconcile pass that keeps the two in step with the metadata image.
//!
//! One fetcher owns one connection to one leader and every partition this
//! broker follows from that leader through it. Adding or removing a partition
//! is a write to the fetcher's shared map, not a task lifecycle event, so a
//! reassignment or a leadership change on one partition does not disturb the
//! thousands sharing its connection.

use std::{collections::BTreeMap, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_metadata::MetadataImage;
use tracing::warn;

use super::{
    FetcherKey, FetcherTask, ReplicatorSupervisor, TopicPartition, desired_follower_set,
    resolve_leader_endpoint,
};
use crate::replicator::{self, FollowedKey};

/// One leader's worth of desired follower state.
struct DesiredFetcher {
    endpoint: (String, u16),
    partitions: BTreeMap<FollowedKey, Arc<replicator::Config>>,
}

impl ReplicatorSupervisor {
    /// Retargets the follower fetchers against `image`.
    ///
    /// Cancels a fetcher whose leader is gone, whose endpoint moved, or whose
    /// task exited; hands every surviving fetcher its new partition set; and
    /// spawns one for a leader this broker did not follow before.
    ///
    /// A partition whose leader or epoch changed is simply a different
    /// `Config` under the same key, or a key on a different fetcher when the
    /// leader itself changed. Either way the fetcher picks it up on its next
    /// round, and the in-flight round it may be applying is fenced by
    /// `replication_target_changed` against the image, not by this pass.
    pub(super) fn reconcile_fetchers(&self, image: &MetadataImage) {
        let desired = self.desired_fetchers(image);

        // 1. Retire fetchers with nothing left to follow, a moved endpoint, or
        //    a task that exited.
        let current: Vec<FetcherKey> = self.tasks.iter().map(|entry| *entry.key()).collect();
        for key in current {
            let keep = self.tasks.get(&key).is_some_and(|task| {
                !task.handle.is_finished()
                    && desired
                        .get(&key)
                        .is_some_and(|wanted| wanted.endpoint == task.endpoint)
            });
            if !keep && let Some((_, task)) = self.tasks.remove(&key) {
                task.shutdown.cancel();
            }
        }

        // 2. Hand each surviving fetcher its partitions, and start one for
        //    every leader this broker now follows and did not before.
        for (key, wanted) in desired {
            if let Some(task) = self.tasks.get(&key) {
                task.followed.replace(wanted.partitions);
                continue;
            }
            let token = self.shutdown.child_token();
            let followed = Arc::new(replicator::FollowedPartitionsState::new(wanted.partitions));
            let handle = tokio::spawn(replicator::run_fetcher(replicator::FetcherConfig {
                node_id: self.node_id,
                leader_node_id: key.0,
                leader_host: wanted.endpoint.0.clone(),
                leader_port: wanted.endpoint.1,
                client_id: self.client_id.clone(),
                shutdown: token.clone(),
                connection: self.connection_config(),
                followed: Arc::clone(&followed),
            }));
            self.tasks.insert(
                key,
                FetcherTask {
                    shutdown: token,
                    handle,
                    followed,
                    endpoint: wanted.endpoint,
                },
            );
        }
    }

    /// The fetchers `image` calls for, and what each should follow.
    ///
    /// A partition whose leader has not registered yet, or whose topic record
    /// has not arrived, is deferred rather than followed: the fetcher would
    /// have nowhere to dial and no `topic_id` to name it by.
    ///
    /// A local replica on an offline log directory is not followed. This is
    /// Kafka's `ReplicaManager.handleLogDirFailure`, which removes the fetcher
    /// of every partition in a failed directory and keeps the partition
    /// offline until the broker restarts.
    fn desired_fetchers(&self, image: &MetadataImage) -> BTreeMap<FetcherKey, DesiredFetcher> {
        let mut desired: BTreeMap<FetcherKey, DesiredFetcher> = BTreeMap::new();
        for key in desired_follower_set(self.node_id, image) {
            if self.replica_dir_offline(&key) {
                continue;
            }
            let Some(partition) = image.partition(&key.0, key.1) else {
                continue;
            };
            let leader = partition.leader;
            let Some(broker) = image.broker(leader) else {
                warn!(
                    topic = %key.0, partition = key.1, leader = leader.0,
                    "leader broker not yet registered in MetadataImage; deferring"
                );
                continue;
            };
            // Resolve the topic's `topic_id` from the same image we're
            // reconciling against. The fetcher needs it for the v13+ Fetch
            // wire format; without it the leader's handler can't resolve the
            // topic name and returns UNKNOWN_TOPIC_OR_PARTITION.
            let Some(topic) = image.topic(&key.0) else {
                warn!(
                    topic = %key.0, partition = key.1,
                    "topic record missing from MetadataImage; deferring"
                );
                continue;
            };
            let endpoint = resolve_leader_endpoint(broker, &self.inter_broker_listener_name);
            let fetcher_key = (
                leader,
                replicator::fetcher_id_for(&key.0, key.1, self.replication.fetchers),
            );
            let followed_key = (
                self.partitions.shared_topic_name(&key.0),
                PartitionIndex(key.1),
            );
            // Reuse the configuration the fetcher already holds when nothing
            // about the partition's target moved. A reconcile runs on every
            // metadata image change, and building a `Config` clones the log
            // directory list, the log settings and four strings; doing that
            // for every followed partition on every image made an ordinary
            // ISR update cost more than the replication it was reporting on.
            let config = self
                .followed_config(fetcher_key, &followed_key, topic, partition, &endpoint)
                .unwrap_or_else(|| {
                    Arc::new(self.replicator_config(&key, topic, partition, endpoint.clone()))
                });
            let entry = desired
                .entry(fetcher_key)
                .or_insert_with(|| DesiredFetcher {
                    endpoint,
                    partitions: BTreeMap::new(),
                });
            entry.partitions.insert(followed_key, config);
        }
        desired
    }

    /// True when this broker hosts `key` on a log directory that is offline.
    fn replica_dir_offline(&self, key: &TopicPartition) -> bool {
        self.partitions
            .get(&key.0, PartitionIndex(key.1))
            .is_some_and(|part| self.log_dir_status.is_offline(&part.log_dir.load()))
    }

    /// The configuration a running fetcher already holds for `key`, when it
    /// still describes the partition the image names.
    ///
    /// Everything a `Config` carries is either fixed for the broker's life or
    /// part of the target the three compared fields name, so a match means the
    /// held configuration and a freshly built one would be equal.
    fn followed_config(
        &self,
        fetcher_key: FetcherKey,
        followed_key: &FollowedKey,
        topic: &krabka_metadata::TopicRecord,
        partition: &krabka_metadata::PartitionRecord,
        endpoint: &(String, u16),
    ) -> Option<Arc<replicator::Config>> {
        let task = self.tasks.get(&fetcher_key)?;
        if task.endpoint != *endpoint {
            return None;
        }
        let held = task
            .followed
            .lock()
            .expect("followed-partitions mutex poisoned")
            .get(followed_key)
            .cloned()?;
        (held.topic_id.0 == topic.topic_id.into_bytes()
            && held.leader_node_id == partition.leader
            && held.leader_epoch == partition.leader_epoch)
            .then_some(held)
    }

    /// The per-partition configuration a fetcher applies one response row
    /// with. Everything the fetcher itself needs -- the endpoint, the dialer,
    /// the pacing -- lives on the fetcher instead.
    pub(super) fn replicator_config(
        &self,
        key: &TopicPartition,
        topic: &krabka_metadata::TopicRecord,
        partition: &krabka_metadata::PartitionRecord,
        (leader_host, leader_port): (String, u16),
    ) -> replicator::Config {
        replicator::Config {
            node_id: self.node_id,
            // The registry already owns one copy of the name, and the fetcher
            // records a per-batch metric under it; take that copy rather than
            // the one this key carries.
            topic: self.partitions.shared_topic_name(&key.0),
            topic_id: krabka_protocol::primitives::uuid::Uuid(topic.topic_id.into_bytes()),
            partition: PartitionIndex(key.1),
            leader_node_id: partition.leader,
            leader_epoch: partition.leader_epoch,
            leader_host,
            leader_port,
            partitions: self.partitions.clone(),
            log_dirs: self.log_dirs.clone(),
            log_settings: self.log_config.clone(),
            client_id: self.client_id.clone(),
            connection: self.connection_config(),
            throttle_state: self.throttle_state.clone(),
            controller: self.controller.clone(),
            log_dir_status: self.log_dir_status.clone(),
            producer_state: self.producer_state.clone(),
            metrics: self.metrics.clone(),
            lag: replicator::ReplicaLag::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_metadata::{MetadataImage, MetadataRecord, TopicRecord};
    use krabka_raft::NodeId;
    use krabka_units::{bytes, millis};
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    use super::*;
    use crate::replicator_supervisor::test_support::{
        await_until, broker_record, image_with, partition_record, supervisor_fixture, topic_record,
    };

    /// The `(topic, partition)` keys one fetcher follows, in key order.
    fn followed_keys(supervisor: &ReplicatorSupervisor, key: FetcherKey) -> Vec<(String, i32)> {
        supervisor
            .tasks
            .get(&key)
            .expect("fetcher")
            .followed
            .lock()
            .expect("followed-partitions mutex poisoned")
            .keys()
            .map(|(topic, partition)| (topic.to_string(), partition.get()))
            .collect()
    }

    /// A fetcher whose task parks until its token is cancelled, so a test can
    /// observe whether reconcile retired it.
    fn running_fetcher(shutdown: CancellationToken, endpoint: (String, u16)) -> FetcherTask {
        let child_shutdown = shutdown.clone();
        FetcherTask {
            shutdown,
            handle: tokio::spawn(async move { child_shutdown.cancelled().await }),
            followed: Arc::new(replicator::FollowedPartitionsState::default()),
            endpoint,
        }
    }

    fn leader_endpoint(image: &MetadataImage, leader: NodeId) -> (String, u16) {
        resolve_leader_endpoint(image.broker(leader).expect("leader broker"), "INTERNAL")
    }

    #[test]
    fn replicator_config_receives_runtime_policy_and_tls_server_name() {
        let image = followed_image(1, &[0], 7);
        let (mut supervisor, _partitions, _reporter, _dir) = supervisor_fixture(image.clone());
        supervisor.replication.fetch_max = bytes(2_345_678);
        supervisor.replication.send_error_backoff = millis(37);
        supervisor.inter_broker_server_name = "broker.internal".into();
        let topic = image.topic("t").expect("topic");

        let config = supervisor.replicator_config(
            &("t".into(), 0),
            topic,
            image.partition("t", 0).expect("partition"),
            ("leader.internal".into(), 9094),
        );

        check!(config.connection.replication.fetch_max == bytes(2_345_678));
        check!(config.connection.replication.send_error_backoff == millis(37));
        check!(config.connection.inter_broker_server_name == "broker.internal");
        check!(config.leader_host == "leader.internal");
        check!(config.leader_port == 9094);
    }

    fn followed_image(partitions: i32, followed: &[i32], epoch: i32) -> MetadataImage {
        let mut records = vec![topic_record(
            crate::replicator_supervisor::test_support::SupervisorTopicSetup {
                partitions: crate::test_support::PartitionCount(partitions),
                ..Default::default()
            },
        )];
        records.extend(followed.iter().map(|&partition| {
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    partition: krabka_ids::PartitionIndex(partition),
                    replicas: vec![NodeId(1), NodeId(2)],
                    epoch: krabka_metadata::LeaderEpoch(epoch),
                    ..Default::default()
                },
            )
        }));
        records.push(MetadataRecord::V1BrokerRegistration(broker_record(NodeId(
            1,
        ))));
        image_with(&records)
    }

    /// The point of the batching: every partition this broker follows from one
    /// leader lands on that leader's one fetcher, rather than on a task each.
    #[tokio::test]
    async fn every_partition_of_one_leader_lands_on_one_fetcher() {
        let img = followed_image(3, &[0, 1, 2], 8);
        let (supervisor, _partitions, _reporter, _dir) =
            crate::replicator_supervisor::test_support::reconciled_supervisor(&img).await;

        check!(supervisor.tasks.len() == 1);
        check!(
            followed_keys(&supervisor, (NodeId(1), 0))
                == vec![
                    ("t".to_string(), 0),
                    ("t".to_string(), 1),
                    ("t".to_string(), 2)
                ]
        );
        for task in &supervisor.tasks {
            task.shutdown.cancel();
        }
    }

    /// Kafka's `ReplicaManager.handleLogDirFailure`: a replica on a log
    /// directory that went offline is no longer followed, and a fetcher with
    /// nothing left to follow is retired.
    #[tokio::test]
    async fn a_replica_on_an_offline_log_directory_is_not_followed() {
        let img = followed_image(1, &[0], 8);
        let (supervisor, _partitions, _reporter, dir) =
            crate::replicator_supervisor::test_support::reconciled_supervisor(&img).await;
        let followed_while_online = followed_keys(&supervisor, (NodeId(1), 0));

        supervisor
            .log_dir_status
            .mark_offline(dir.path(), "test: EIO");
        supervisor.reconcile(&img).await;

        assert!((followed_while_online, supervisor.tasks.len()) == (vec![("t".to_string(), 0)], 0));
    }

    /// Two leaders are two connections, which is the shape Kafka has and the
    /// reason a fetcher is keyed by leader in the first place.
    #[tokio::test]
    async fn partitions_led_by_different_brokers_get_a_fetcher_each() {
        // The fixture broker is node 2, so it follows the two partitions that
        // nodes 1 and 3 lead.
        let img = image_with(&[
            topic_record(
                crate::replicator_supervisor::test_support::SupervisorTopicSetup {
                    partitions: crate::test_support::PartitionCount(2),
                    ..Default::default()
                },
            ),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    replicas: vec![NodeId(1), NodeId(2)],
                    epoch: krabka_metadata::LeaderEpoch(8),
                    ..Default::default()
                },
            ),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    partition: krabka_ids::PartitionIndex(1),
                    leader: NodeId(3),
                    replicas: vec![NodeId(3), NodeId(2)],
                    epoch: krabka_metadata::LeaderEpoch(8),
                    ..Default::default()
                },
            ),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(1))),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(3))),
        ]);
        let (supervisor, _partitions, _reporter, _dir) =
            crate::replicator_supervisor::test_support::reconciled_supervisor(&img).await;

        check!(supervisor.tasks.len() == 2);
        check!(followed_keys(&supervisor, (NodeId(1), 0)) == vec![("t".to_string(), 0)]);
        check!(followed_keys(&supervisor, (NodeId(3), 0)) == vec![("t".to_string(), 1)]);
        for task in &supervisor.tasks {
            task.shutdown.cancel();
        }
    }

    /// `num.replica.fetchers` above one spreads a leader's partitions over
    /// several connections without changing which leader owns them.
    #[tokio::test]
    async fn more_fetchers_spread_one_leader_s_partitions_without_losing_any() {
        let mut records = vec![
            topic_record(
                crate::replicator_supervisor::test_support::SupervisorTopicSetup {
                    partitions: crate::test_support::PartitionCount(16),
                    ..Default::default()
                },
            ),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(1))),
        ];
        for partition in 0..16 {
            records.push(partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    partition: krabka_ids::PartitionIndex(partition),
                    replicas: vec![NodeId(1), NodeId(2)],
                    epoch: krabka_metadata::LeaderEpoch(8),
                    ..Default::default()
                },
            ));
        }
        let img = image_with(&records);
        let (mut supervisor, _partitions, _reporter, _dir) = supervisor_fixture(img.clone());
        supervisor.replication.fetchers = 4;

        supervisor.reconcile(&img).await;

        check!(
            supervisor.tasks.len() > 1,
            "four fetchers must not collapse to one"
        );
        let mut followed: Vec<i32> = (&supervisor.tasks)
            .into_iter()
            .flat_map(|task| {
                task.followed
                    .lock()
                    .expect("followed-partitions mutex poisoned")
                    .keys()
                    .map(|(_, partition)| partition.get())
                    .collect::<Vec<_>>()
            })
            .collect();
        followed.sort_unstable();
        check!(followed == (0..16).collect::<Vec<_>>());
        for task in &supervisor.tasks {
            task.shutdown.cancel();
        }
    }

    /// A partition that leaves the image leaves its fetcher's map. The fetcher
    /// itself survives while it still has work, so the connection its other
    /// partitions ride on is not torn down.
    #[tokio::test]
    async fn a_removed_partition_leaves_the_map_and_the_fetcher_keeps_running() {
        let two = followed_image(2, &[0, 1], 8);
        let (supervisor, _partitions, _reporter, _dir) =
            crate::replicator_supervisor::test_support::reconciled_supervisor(&two).await;
        let token = supervisor
            .tasks
            .get(&(NodeId(1), 0))
            .expect("fetcher")
            .shutdown
            .clone();

        let one = followed_image(2, &[0], 8);
        supervisor.reconcile(&one).await;

        check!(followed_keys(&supervisor, (NodeId(1), 0)) == vec![("t".to_string(), 0)]);
        check!(!token.is_cancelled());
        token.cancel();
    }

    #[tokio::test]
    async fn a_fetcher_with_nothing_left_to_follow_is_cancelled() {
        let img = MetadataImage::new(Uuid::nil());
        let (supervisor, _partitions, _reporter, _dir) = supervisor_fixture(img.clone());
        let token = CancellationToken::new();
        supervisor.tasks.insert(
            (NodeId(1), 0),
            running_fetcher(token.clone(), ("127.0.0.1".into(), 9092)),
        );

        supervisor.reconcile(&img).await;

        check!(token.is_cancelled());
        check!(supervisor.tasks.is_empty());
    }

    /// A leader that re-registers on a different address needs a new
    /// connection, and a connection is a fetcher.
    #[tokio::test]
    async fn a_fetcher_whose_leader_endpoint_moved_is_replaced() {
        let img = followed_image(1, &[0], 8);
        let (supervisor, _partitions, _reporter, _dir) = supervisor_fixture(img.clone());
        let stale = CancellationToken::new();
        supervisor.tasks.insert(
            (NodeId(1), 0),
            running_fetcher(stale.clone(), ("moved.example".into(), 1234)),
        );

        supervisor.reconcile(&img).await;

        assert!(stale.is_cancelled());
        let replacement = supervisor.tasks.get(&(NodeId(1), 0)).expect("respawned");
        check!(replacement.endpoint == leader_endpoint(&img, NodeId(1)));
        check!(!replacement.handle.is_finished());
        replacement.shutdown.cancel();
    }

    #[tokio::test]
    async fn reconcile_respawns_an_exited_fetcher() {
        let img = followed_image(1, &[0], 8);
        let (supervisor, _partitions, _reporter, _dir) = supervisor_fixture(img.clone());
        let stale_shutdown = CancellationToken::new();
        supervisor.tasks.insert(
            (NodeId(1), 0),
            FetcherTask {
                shutdown: stale_shutdown.clone(),
                handle: tokio::spawn(async {}),
                followed: Arc::new(replicator::FollowedPartitionsState::default()),
                endpoint: leader_endpoint(&img, NodeId(1)),
            },
        );
        await_until("old fetcher exits", || {
            supervisor
                .tasks
                .get(&(NodeId(1), 0))
                .is_some_and(|task| task.handle.is_finished())
        })
        .await;

        supervisor.reconcile(&img).await;

        assert!(stale_shutdown.is_cancelled());
        let replacement = supervisor.tasks.get(&(NodeId(1), 0)).expect("respawned");
        assert!(!replacement.handle.is_finished());
        replacement.shutdown.cancel();
    }

    /// A delete-and-recreate under the same name, and a leadership change,
    /// both reach the fetcher as a replaced `Config` under the same key: the
    /// fetcher reads the new one on its next round, and every response it
    /// applies is fenced against the image by `replication_target_changed`.
    #[tokio::test]
    async fn a_recreated_topic_replaces_the_config_the_fetcher_follows() {
        let first_id = Uuid::new_v4();
        let before = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: first_id,
                partitions: 1,
                replication_factor: 2,
            }),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    replicas: vec![NodeId(1), NodeId(2)],
                    epoch: krabka_metadata::LeaderEpoch(8),
                    ..Default::default()
                },
            ),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(1))),
        ]);
        let (supervisor, _partitions, _reporter, _dir) =
            crate::replicator_supervisor::test_support::reconciled_supervisor(&before).await;

        let recreated_id = Uuid::new_v4();
        let after = image_with(&[
            MetadataRecord::V1Topic(TopicRecord {
                name: "t".into(),
                topic_id: recreated_id,
                partitions: 1,
                replication_factor: 2,
            }),
            partition_record(
                crate::replicator_supervisor::test_support::SupervisorPartitionSetup {
                    replicas: vec![NodeId(1), NodeId(2)],
                    epoch: krabka_metadata::LeaderEpoch(9),
                    ..Default::default()
                },
            ),
            MetadataRecord::V1BrokerRegistration(broker_record(NodeId(1))),
        ]);
        supervisor.reconcile(&after).await;

        let task = supervisor.tasks.get(&(NodeId(1), 0)).expect("fetcher");
        let followed = task
            .followed
            .lock()
            .expect("followed-partitions mutex poisoned");
        let config = followed
            .get(&(
                supervisor.partitions.shared_topic_name("t"),
                PartitionIndex(0),
            ))
            .expect("the recreated partition");
        check!(config.topic_id.0 == recreated_id.into_bytes());
        check!(config.leader_epoch == krabka_metadata::LeaderEpoch(9));
        drop(followed);
        task.shutdown.cancel();
    }
}
