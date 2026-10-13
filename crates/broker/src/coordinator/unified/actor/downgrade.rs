//! The KIP-848 downgrade trigger.
//!
//! A consumer group whose last native member is fenced, while it still hosts
//! classic members, flips back to the classic protocol in place, as Kafka's
//! `consumerGroupFenceMembers` does: the flip replaces the fence's records
//! with one atomic batch of the conversion's.

use {
    super::{MetadataProvider, chrono_now_ms},
    crate::{
        coordinator::unified::{
            GroupCoordinator,
            config::NextGenConfig,
            consumer_state::GroupState,
            group::{CoordinatorGroup, GroupKind},
            migration,
            offsets_log::OffsetsLog,
        },
        error::BrokerError,
    },
    std::time::Instant,
};

#[cfg(test)]
mod tests;

/// Kafka's `validateOnlineDowngradeWithFencedMembers`: a consumer group whose
/// members are about to be fenced downgrades to a classic group instead when
/// every other member uses the classic protocol, at least one remains, the
/// migration policy allows a downgrade and the remaining members fit in a
/// classic group.
pub(super) fn downgrades_without(
    state: &GroupState,
    config: &NextGenConfig,
    fenced: &[String],
) -> bool {
    let mut remaining = state
        .members
        .values()
        .filter(|member| !fenced.contains(&member.member_id))
        .peekable();
    let nonempty = remaining.peek().is_some();
    let all_classic = remaining.all(|member| member.classic.is_some());
    let remaining_count = state.members.len()
        - fenced
            .iter()
            .filter(|member_id| state.members.contains_key(*member_id))
            .count();
    all_classic
        && nonempty
        && config.migration_policy.allows_downgrade()
        && remaining_count <= config.classic_max_size
}

/// Kafka's `convertToClassicGroup` for a fence: the consumer group, fenced
/// members included, is tombstoned and the classic group of the remaining
/// members is written, in one batch. The classic group starts at the consumer
/// group's epoch and, as Kafka's fence asks with `rebalance = true`, prepares
/// a rebalance at once.
///
/// It returns `Err` on a log-write failure, and the actor then exits.
pub(super) async fn downgrade_fencing(
    group: &mut CoordinatorGroup,
    fenced: &[String],
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
) -> Result<(), BrokerError> {
    let Some(state) = group.as_consumer() else {
        return Ok(());
    };
    let image = metadata.snapshot();
    let now_ms = chrono_now_ms();
    let mut classic = migration::convert_consumer_to_classic(state, fenced, &image);
    let pending = migration::downgrade_pending_records(state, &classic, now_ms);
    let group_id = group.group_id.clone();
    let batch = pending.to_batch(&group_id, now_ms)?;
    offsets_log.append(&group_id, batch).await?;
    coordinator.mark_classic_after_downgrade(&group_id);
    classic.prepare_rebalance(config.classic_initial_rebalance_delay, Instant::now());
    *group.kind_mut() = GroupKind::Classic(classic);
    Ok(())
}
