//! The leadership gate `ListOffsets` applies before resolving any offset:
//! whether this node may answer for the partition from what it holds
//! locally, or has to send the client elsewhere.
//!
//! Kafka's `ReplicaManager.fetchOffsetForTimestamp` computes
//! `fetchOnlyFromLeader = replicaId != DEBUGGING_REPLICA_ID`
//! (`ReplicaManager.scala` ~1481) and resolves through
//! `getPartitionOrException`, whose `Partition.localLogWithEpochOrThrow`
//! (`Partition.scala` ~1435-1441) checks `checkCurrentLeaderEpoch` -- KIP-320's
//! epoch fence -- before the leader-only gate, so a stale or future asserted
//! epoch is reported even against a partition this node does not lead. Past
//! the fence, it raises `NotLeaderOrFollowerException` when the metadata
//! cache knows the partition but this node does not lead it -- unless the
//! request is `DEBUGGING_REPLICA_ID` (-2), which is allowed to read a
//! follower's own (possibly lagging) local log -- and
//! `UnknownTopicOrPartitionException` when the cache does not know the
//! partition at all (`ReplicaManager.scala` ~1533-1545). Once past that
//! check, an offline log directory raises `KafkaStorageException`.
//!
//! Without this gate a broker holding the partition only as a follower
//! answered a consumer with its own lagging log end offset instead of
//! sending it to the leader, and a broker that did not hold the partition at
//! all answered `UNKNOWN_TOPIC_OR_PARTITION` even when the topic existed and
//! was led elsewhere.
//!
//! Leadership itself is decided from two states, the way
//! [`crate::handlers::fetch::plan::leader_refusal`] decides it for Fetch: the
//! metadata image, which the controller commits, and the partition's
//! installed local role, which the replicator supervisor reconciles onto it
//! afterwards. The image can name a new leader before the supervisor installs
//! the new role, and the installed role can lag the other way while a
//! promotion prepares the log, so both must agree before this node answers as
//! leader.

use std::sync::{Arc, atomic::Ordering};

use crate::{
    broker::Broker, codes, log_dir_status::LogDirRegistry, partition::Partition,
    partition_registry::PartitionRegistry,
};

/// Kafka's `ListOffsetsRequest.DEBUGGING_REPLICA_ID`: the one `replica_id`
/// allowed to read a follower's own local log instead of being redirected to
/// the leader. No follower sends it; it is reserved for offline debugging
/// tools.
pub(super) const DEBUGGING_REPLICA_ID: i32 = -2;

/// The broker-wide state `resolve_leadership` reads. Grouped so the function
/// takes one handle to it rather than a parameter per field.
#[derive(Clone, Copy)]
pub(super) struct LeadershipContext<'a> {
    pub(super) partitions: &'a PartitionRegistry,
    pub(super) log_dir_status: &'a LogDirRegistry,
    pub(super) image: &'a krabka_metadata::MetadataImage,
    pub(super) node_id: krabka_metadata::NodeId,
}

/// Decide whether this node may answer `ListOffsets` for one partition, and
/// hand back the local [`Partition`] to resolve it against when it may.
///
/// # Errors
///
/// Returns the Kafka error code the caller should answer that partition row
/// with: `FENCED_LEADER_EPOCH` or `UNKNOWN_LEADER_EPOCH` when `request_epoch`
/// asserts an epoch this node's locally held partition does not have live
/// (checked ahead of leadership, matching `checkCurrentLeaderEpoch`'s place in
/// `Partition.getLocalLog`), `UNKNOWN_TOPIC_OR_PARTITION` when the metadata
/// image does not know the partition at all, `NOT_LEADER_OR_FOLLOWER` when
/// the image and the installed local role do not both name this node leader
/// and the request is not the debugging sentinel, and `KAFKA_STORAGE_ERROR`
/// when the partition's log directory is offline.
pub(super) fn resolve_leadership(
    topic_name: &str,
    partition_index: i32,
    replica_id: i32,
    request_epoch: i32,
    ctx: LeadershipContext<'_>,
) -> Result<Arc<Partition>, i16> {
    let LeadershipContext {
        partitions,
        log_dir_status,
        image,
        node_id,
    } = ctx;
    let Some(record) = image.partition(topic_name, partition_index) else {
        return Err(codes::UNKNOWN_TOPIC_OR_PARTITION);
    };
    let local = partitions.get(topic_name, krabka_ids::PartitionIndex(partition_index));

    if let Some(partition) = &local
        && let Some((error_code, _)) = partition.list_offsets_leader_epoch_fence(request_epoch)
    {
        return Err(error_code);
    }

    let installed_leader = local
        .as_ref()
        .is_some_and(|partition| partition.current_leader.load(Ordering::Acquire) == node_id.0);
    let leads = installed_leader && record.leader == node_id;
    if !leads && replica_id != DEBUGGING_REPLICA_ID {
        return Err(codes::NOT_LEADER_OR_FOLLOWER);
    }
    let Some(partition) = local else {
        return Err(codes::UNKNOWN_TOPIC_OR_PARTITION);
    };
    if log_dir_status.is_offline(&partition.log_dir.load()) {
        return Err(codes::KAFKA_STORAGE_ERROR);
    }
    Ok(partition)
}

pub(super) fn resolve_for_broker(
    broker: &Broker,
    topic: &str,
    partition: i32,
    replica_id: i32,
    current_leader_epoch: i32,
) -> Result<Arc<Partition>, i16> {
    resolve_leadership(
        topic,
        partition,
        replica_id,
        current_leader_epoch,
        LeadershipContext {
            partitions: &broker.partitions,
            log_dir_status: &broker.log_dir_status,
            image: &broker.controller.current_image(),
            node_id: broker.config.node_id,
        },
    )
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::MetadataImage;
    use uuid::Uuid;

    use super::*;
    use crate::{
        handlers::test_support::{ReplicatedPartitionSetup, replicated_partition},
        log_dir::partition_dir,
        log_dir_status::LogDirRegistry,
        test_support::{
            StandalonePartitionSetup, spawn_standalone_partition, topic_partition_image,
        },
    };

    fn image_with_topic(topic: &str, leader: u64) -> MetadataImage {
        topic_partition_image(topic, 2, || {
            replicated_partition(ReplicatedPartitionSetup {
                topic,
                leader: krabka_audit::NodeId(leader),
                replicas: &[krabka_audit::NodeId(1), krabka_audit::NodeId(2)],
                ..Default::default()
            })
        })
    }

    fn local_partition(dir: &std::path::Path) -> Arc<Partition> {
        let log = krabka_log::Log::open(
            partition_dir(dir, "orders", 0),
            krabka_log::LogConfig::default(),
        )
        .expect("open log");
        spawn_standalone_partition(dir, log, StandalonePartitionSetup::default())
    }

    #[derive(Clone, Copy, Default)]
    enum InstalledRole {
        #[default]
        Uninstalled,
        Leader,
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct HostedPartitionSetup {
        #[default(krabka_audit::NodeId(1))]
        image_leader: krabka_audit::NodeId,
        role: InstalledRole,
    }

    async fn hosted_fixture(
        setup: HostedPartitionSetup,
    ) -> (
        tempfile::TempDir,
        MetadataImage,
        PartitionRegistry,
        Arc<Partition>,
    ) {
        let dir = tempfile::tempdir().expect("tempdir");
        let image = image_with_topic("orders", setup.image_leader.0);
        let partitions = PartitionRegistry::new();
        let partition = local_partition(dir.path());
        if matches!(setup.role, InstalledRole::Leader) {
            partition.install_leader_change(1, 0).await;
        }
        partitions.insert(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            partition.clone(),
        );
        (dir, image, partitions, partition)
    }

    /// Keep the borrowed context and its log-directory registry in each caller's scope.
    macro_rules! partition_context {
        (($status:ident, $context:ident), $partitions:expr, $image:expr) => {
            let $status = LogDirRegistry::default();
            let $context = LeadershipContext {
                partitions: $partitions,
                log_dir_status: &$status,
                image: $image,
                node_id: krabka_audit::NodeId(1),
            };
        };
    }

    #[test]
    fn image_without_the_partition_is_always_unknown_topic_or_partition() {
        let empty = MetadataImage::new(Uuid::nil());
        let partitions = PartitionRegistry::new();
        partition_context!((log_dir_status, ctx), &partitions, &empty);
        for replica_id in [-1, -2, 3] {
            let got = resolve_leadership("orders", 0, replica_id, -1, ctx);
            assert!(
                got.err() == Some(codes::UNKNOWN_TOPIC_OR_PARTITION),
                "{replica_id}"
            );
        }
    }

    #[tokio::test]
    async fn known_partition_not_held_locally_reflects_the_image_leader_unless_debugging() {
        let image = image_with_topic("orders", 2);
        let partitions = PartitionRegistry::new();
        partition_context!((log_dir_status, ctx), &partitions, &image);

        for (replica_id, want) in [
            (-1, Err(codes::NOT_LEADER_OR_FOLLOWER)),
            (3, Err(codes::NOT_LEADER_OR_FOLLOWER)),
            // The debugging sentinel still needs a local replica to read; this
            // node holds none, so it is UNKNOWN_TOPIC_OR_PARTITION rather than
            // NOT_LEADER_OR_FOLLOWER.
            (-2, Err(codes::UNKNOWN_TOPIC_OR_PARTITION)),
        ] {
            let got = resolve_leadership("orders", 0, replica_id, -1, ctx).map(|_| ());
            assert!(got == want, "replica_id={replica_id}");
        }
    }

    #[tokio::test]
    async fn following_locally_is_not_leader_or_follower_unless_debugging() {
        let (_dir, image, partitions, _partition) = hosted_fixture(HostedPartitionSetup {
            image_leader: krabka_audit::NodeId(2),
            ..Default::default()
        })
        .await;
        partition_context!((log_dir_status, ctx), &partitions, &image);

        let refused = resolve_leadership("orders", 0, -1, -1, ctx).map(|_| ());
        assert!(refused == Err(codes::NOT_LEADER_OR_FOLLOWER));

        let debugging = resolve_leadership("orders", 0, DEBUGGING_REPLICA_ID, -1, ctx);
        assert!(debugging.is_ok());
    }

    #[tokio::test]
    async fn leading_locally_resolves_and_an_offline_log_dir_is_reported() {
        let (_dir, image, partitions, partition) = hosted_fixture(HostedPartitionSetup {
            role: InstalledRole::Leader,
            ..Default::default()
        })
        .await;
        partition_context!((log_dir_status, ctx), &partitions, &image);
        let resolved = resolve_leadership("orders", 0, -1, -1, ctx);
        assert!(resolved.is_ok());

        log_dir_status.mark_offline(&partition.log_dir.load(), "test");
        let offline = resolve_leadership("orders", 0, -1, -1, ctx).map(|_| ());
        assert!(offline == Err(codes::KAFKA_STORAGE_ERROR));
    }

    #[tokio::test]
    async fn the_image_naming_this_node_leader_is_not_enough_without_the_installed_role() {
        // The controller can commit a promotion before the replicator
        // supervisor installs it locally. Until it does, the partition's
        // `current_leader` atomic still names the old leader, and a request
        // arriving in that window must not be served from the not-yet-caught-up
        // local log.
        let (_dir, image, partitions, partition) =
            hosted_fixture(HostedPartitionSetup::default()).await;
        partition_context!((log_dir_status, ctx), &partitions, &image);

        let got = resolve_leadership("orders", 0, -1, -1, ctx).map(|_| ());
        assert!(got == Err(codes::NOT_LEADER_OR_FOLLOWER));

        partition.install_leader_change(1, 0).await;
        let resolved = resolve_leadership("orders", 0, -1, -1, ctx);
        assert!(resolved.is_ok());
    }

    #[tokio::test]
    async fn the_epoch_fence_runs_before_leadership_is_refused() {
        // Kafka's `Partition.getLocalLog` runs `checkCurrentLeaderEpoch`
        // before the leader-only gate, so a stale or future asserted epoch
        // against a partition this node holds only as a follower is reported
        // as an epoch mismatch rather than `NOT_LEADER_OR_FOLLOWER`.
        let dir = tempfile::tempdir().expect("tempdir");
        let image = image_with_topic("orders", 2);
        let partitions = PartitionRegistry::new();
        let partition = local_partition(dir.path());
        partition.test_set_leader_epoch(3);
        partitions.insert(
            "orders".into(),
            krabka_ids::PartitionIndex(0),
            partition.clone(),
        );
        partition_context!((log_dir_status, ctx), &partitions, &image);

        let stale = resolve_leadership("orders", 0, -1, 2, ctx).map(|_| ());
        assert!(stale == Err(codes::FENCED_LEADER_EPOCH));

        let future = resolve_leadership("orders", 0, -1, 4, ctx).map(|_| ());
        assert!(future == Err(codes::UNKNOWN_LEADER_EPOCH));

        // No epoch asserted: falls through to the ordinary leadership
        // refusal, since this node neither has the image leader nor the
        // installed role.
        let unasserted = resolve_leadership("orders", 0, -1, -1, ctx).map(|_| ());
        assert!(unasserted == Err(codes::NOT_LEADER_OR_FOLLOWER));
    }
}
