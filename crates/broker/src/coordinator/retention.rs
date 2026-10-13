//! KIP-211 offset retention: the periodic sweep.
//!
//! Apache Kafka drops a group's committed offsets `offsets.retention.minutes`
//! after the group loses its last member, and drops any memberless group that
//! holds no offsets — whether the sweep just took its last one, an
//! `OffsetDelete` did, or it never committed at all. Without that, anything
//! that mints group ids — a
//! per-deployment group id, a Connect task rebalance, an ad-hoc
//! `kafka-console-consumer`, a CI job — leaks one `__consumer_offsets` entry
//! per partition per group forever: the topic never compacts to a steady
//! state, coordinator replay time grows without bound, and
//! `kafka-consumer-groups --list` fills with the dead.
//!
//! This module owns the cadence and the ownership question. The decision about
//! any one group, and the append that follows it, live inside that group's
//! actor, in [`crate::coordinator::unified::actor`]'s `retention` module, so a
//! concurrent join or commit cannot race the tombstone.
//!
//! # Only the coordinator sweeps its own groups
//!
//! Each pass reads the metadata image once and keeps the groups whose
//! `__consumer_offsets` partition this broker leads. A broker that has just
//! lost a partition and not yet dropped its actors therefore writes nothing
//! for those groups, and the new leader — which replays the partition on the
//! way in — does the work instead.
//!
//! Share groups (KIP-932) and streams groups (KIP-1071) are skipped outright.
//! A streams group keeps its committed offsets on the same `groups` entry a
//! classic group would use, but its members live in a streams actor, so the
//! offset home looks memberless and would otherwise be reaped out from under a
//! live application.
//!
//! # A pass that fails changes nothing
//!
//! A failed append leaves the group exactly as it was and the next pass tries
//! again, so the sweep is safe to run on every broker and safe to run twice: a
//! tombstone written for an offset that is already gone is a no-op.

use {
    super::{
        GroupCoordinator,
        partitioner::local_partition_for_group,
        unified::{
            GroupType,
            actor::{GroupActorMessage, ReapOutcome},
        },
    },
    crate::{
        coordinator::unified::actor::GroupActorHandle,
        metadata_source::MetadataSource,
        task_util::{ask, run_every},
        time_util::now_ms,
    },
    krabka_metadata::NodeId,
    krabka_units::{Time, convert::TimeExt as _},
    std::sync::Arc,
    tokio_util::sync::CancellationToken,
};

#[cfg(test)]
mod tests;

/// Runs the sweep. It returns when `shutdown` is cancelled.
pub(crate) async fn run(
    node_id: NodeId,
    metadata: Arc<dyn MetadataSource>,
    coordinator: Arc<GroupCoordinator>,
    interval: Time,
    retention: Time,
    shutdown: CancellationToken,
) {
    let retention_ms = retention.millis_i64();
    let tick = tokio::time::interval(interval.to_std());
    let (metadata, coordinator) = (&metadata, &coordinator);
    run_every(tick, &shutdown, move || async move {
        let image = metadata.current_image();
        let owned = |group_id: &str| local_partition_for_group(&image, node_id, group_id).is_ok();
        sweep(
            coordinator,
            owned,
            now_ms(),
            retention_ms,
            interval.millis_i64(),
        )
        .await;
    })
    .await;
    tracing::info!("offset-retention sweep shutting down");
}

/// A group that a sweep changed, and what the sweep did to it.
#[derive(Debug, PartialEq)]
pub(crate) struct ReapedGroup {
    pub group_id: String,
    pub outcome: ReapOutcome,
}

/// One pass over the coordinator's groups.
///
/// `owned` answers whether this broker leads the `__consumer_offsets`
/// partition that hosts a group id; production reads it from the metadata
/// image, and a test answers it directly. `empty_grace_ms` is how long a group
/// must have been memberless before the pass may delete it for holding no
/// offsets at all. The returned rows name every group the pass changed, which
/// is what a test asserts on and what the caller logs.
pub(crate) async fn sweep(
    coordinator: &GroupCoordinator,
    owned: impl Fn(&str) -> bool,
    now_ms: i64,
    retention_ms: i64,
    empty_grace_ms: i64,
) -> Vec<ReapedGroup> {
    let group_ids = coordinator.group_ids();
    let mut changed = Vec::new();
    for group_id in group_ids {
        if !sweepable(coordinator, &group_id) || !owned(&group_id) {
            continue;
        }
        let Some(handle) = coordinator.find(&group_id) else {
            continue;
        };
        let Ok(outcome) = ask(&handle.tx, |reply| GroupActorMessage::ReapExpiredOffsets {
            now_ms,
            retention_ms,
            empty_grace_ms,
            reply,
        })
        .await
        else {
            continue;
        };
        if outcome.group_deleted {
            forget_group(coordinator, &group_id, &handle);
        }
        if outcome.group_deleted || !outcome.reaped.is_empty() {
            changed.push(ReapedGroup { group_id, outcome });
        }
    }
    changed
}

/// `true` when the classic/next-gen actor for `group_id` owns that group's
/// membership, and so can be trusted to say the group is empty.
fn sweepable(coordinator: &GroupCoordinator, group_id: &str) -> bool {
    !matches!(
        coordinator.group_type(group_id),
        Some(GroupType::Share | GroupType::Streams)
    ) && !coordinator.share_groups.contains_key(group_id)
        && !coordinator.streams_groups.contains_key(group_id)
}

/// Drop every trace of a group whose actor tombstoned itself. The actor has
/// already stopped, so a later request for the id spawns a fresh, empty one.
///
/// The registry entry goes only while it still holds `handle`. A request that
/// arrived between the actor's exit and this call has already replaced the
/// dead entry with a fresh actor, and that one is serving somebody — so the
/// auxiliary state goes only when the conditional removal actually took the
/// reaped handle out. Clearing it unconditionally would strip the
/// replacement's protocol-type lock, which routing and KIP-848 migration read,
/// and its durable seed, which a later respawn replays from.
fn forget_group(coordinator: &GroupCoordinator, group_id: &str, handle: &Arc<GroupActorHandle>) {
    if coordinator
        .groups
        .remove_if(group_id, |_, live| Arc::ptr_eq(live, handle))
        .is_none()
    {
        return;
    }
    coordinator.group_types.remove(group_id);
    coordinator.seeds.remove(group_id);
    coordinator.seeds_cache.remove(group_id);
}
