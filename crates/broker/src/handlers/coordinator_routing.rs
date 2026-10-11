//! The lookups that tell a client where a coordinator is, and the protocol
//! gates a group RPC passes before it reaches one.
//!
//! A group RPC first asks whether this broker leads the group's offsets
//! partition, and `FindCoordinator` then reports the host and port of the
//! broker that does.

use crate::{
    broker::Broker,
    codes::{
        COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR,
        UNKNOWN_SERVER_ERROR,
    },
    coordinator::{GroupCoordinator, bootstrap::OFFSETS_TOPIC},
    features::{STREAMS_VERSION, feature_enabled},
    task_util::AskError,
};

/// Return the Kafka routing error for a group RPC sent to the wrong broker,
/// `COORDINATOR_LOAD_IN_PROGRESS` while this broker still replays the group's
/// offsets partition, or `None` when this broker serves the group.
pub(crate) fn group_coordinator_error(broker: &Broker, group_id: &str) -> Option<i16> {
    use crate::coordinator::partitioner::{GroupRoutingError, local_partition_for_group};

    match local_partition_for_group(
        &broker.controller.current_image(),
        broker.config.node_id,
        group_id,
    ) {
        Ok(partition) => group_partition_loading(broker, partition),
        Err(GroupRoutingError::Unavailable) => Some(COORDINATOR_NOT_AVAILABLE),
        Err(GroupRoutingError::NotCoordinator) => Some(NOT_COORDINATOR),
    }
}

/// `COORDINATOR_LOAD_IN_PROGRESS` while this broker still replays the offsets
/// `partition` it leads, or has not yet taken up the leadership term the
/// current image names, as Kafka's `CoordinatorRuntime` answers for a shard
/// that is not `ACTIVE`.
pub(crate) fn group_partition_loading(broker: &Broker, partition: i32) -> Option<i16> {
    let epoch = broker
        .controller
        .current_image()
        .partition(OFFSETS_TOPIC, partition)
        .map(|record| record.leader_epoch);
    epoch
        .is_none_or(|epoch| broker.group_coordinator.is_loading(partition, epoch))
        .then_some(COORDINATOR_LOAD_IN_PROGRESS)
}

/// `true` while any `__consumer_offsets` partition this broker leads is not
/// served yet: it is replaying, or the image watcher has not taken up its
/// leadership term. Kafka's `ListGroups` reads every local shard, and
/// `CoordinatorRuntime.withActiveContextOrThrow` throws
/// `COORDINATOR_LOAD_IN_PROGRESS` for one that is still `LOADING`, so the
/// whole answer is that error rather than the groups of the shards that are
/// ready.
pub(crate) fn any_group_partition_loading(broker: &Broker) -> bool {
    let coordinator = &broker.group_coordinator;
    coordinator.is_any_loading()
        || broker
            .controller
            .current_image()
            .partitions_of(OFFSETS_TOPIC)
            .filter(|record| record.leader == broker.config.node_id)
            .any(|record| coordinator.is_loading(record.partition, record.leader_epoch))
}

/// Minimum finalized `streams.version` feature level at which the broker
/// serves the KIP-1071 streams RPCs.
const STREAMS_VERSION_MIN_LEVEL: i16 = 1;

/// `true` when the KIP-1071 streams protocol is on: the `streams_group.enable`
/// config kill-switch allows it and `image` finalizes `streams.version >= 1`
/// (early access, default-disabled).
pub(crate) fn streams_protocol_enabled(
    broker: &Broker,
    image: &krabka_metadata::MetadataImage,
) -> bool {
    broker.config.streams_group.enable
        && feature_enabled(image, STREAMS_VERSION, STREAMS_VERSION_MIN_LEVEL)
}

/// Minimum finalized `group.version` at which the broker serves the KIP-848
/// consumer group RPCs.
const NEXT_GEN_MIN_GROUP_VERSION: i16 = 1;

/// `true` while `image` does not finalize `group.version >= 1`, the KIP-848
/// gate of `ConsumerGroupHeartbeat` and `ConsumerGroupDescribe`.
pub(crate) fn group_version_disabled(image: &krabka_metadata::MetadataImage) -> bool {
    !feature_enabled(
        image,
        krabka_metadata::group_version::GROUP_VERSION_FEATURE,
        NEXT_GEN_MIN_GROUP_VERSION,
    )
}

/// The `GROUP_ID_NOT_FOUND` message of Kafka's `GroupMetadataManager.shareGroup`
/// lookup: a group of another type is not a share group, and anything else is
/// not found. A classic or consumer group lives in the `groups` registry and a
/// streams group keeps its offset home there too.
pub(crate) fn share_group_not_found_message(
    coordinator: &GroupCoordinator,
    group_id: &str,
) -> String {
    let other_type = coordinator.group_type(group_id).is_some()
        || coordinator.find(group_id).is_some()
        || coordinator.find_streams(group_id).is_some();
    if other_type {
        format!("Group {group_id} is not a share group.")
    } else {
        format!("Group {group_id} not found.")
    }
}

pub(crate) fn group_actor_error_code(error: AskError) -> i16 {
    match error {
        AskError::Closed => COORDINATOR_LOAD_IN_PROGRESS,
        AskError::Dropped => UNKNOWN_SERVER_ERROR,
    }
}
