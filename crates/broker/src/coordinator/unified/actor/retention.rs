//! KIP-211 offset retention: the actor half of the sweep.
//!
//! `coordinator::retention` drives the cadence and decides which
//! groups this broker owns. This module answers the one question that needs
//! the group's own state, and it answers it inside the actor so a concurrent
//! join, commit, or leave cannot slip between the decision and the append.
//!
//! # What expires
//!
//! This follows Kafka's `OffsetMetadataManager.cleanupExpiredOffsets`. The
//! group's `offsetExpirationCondition` ([`expiration_condition`]) decides
//! whether any offset may expire and from which base. Only the offsets of a
//! topic the group does not subscribe to (`isSubscribedToTopic`) are
//! candidates, and an offset that an open transaction has written or is
//! about to write stays. So a live group loses the offsets of a topic it
//! stopped consuming, and keeps the ones it still consumes:
//!
//! - A classic group without a protocol type (a simple group that only
//!   commits) subscribes to nothing and measures from each commit.
//! - A memberless classic group subscribes to nothing and measures from the
//!   moment it went empty.
//! - A `Stable` classic group on the `consumer` protocol subscribes to the
//!   topics in its members' subscriptions and measures from each commit. In
//!   any other state, on any other protocol, or with a subscription Kafka
//!   cannot read, nothing expires.
//! - A KIP-848 group subscribes to its members' topic names and resolved
//!   regular expressions, and measures from each commit.
//!
//! Each candidate expires on its own clock, per
//! [`OffsetEntry::is_expired`](crate::coordinator::unified::classic_state::OffsetEntry::is_expired):
//! the per-commit `retention_time_ms` a v2-v4 `OffsetCommit` asked for, or
//! `offsets.retention.minutes` from the base the condition chose.
//!
//! # What the batch carries
//!
//! One tombstone for each expired offset, and — when the group is memberless,
//! has no open transaction and keeps no offset after the pass, including the
//! group that never held one and has been memberless for a whole sweep
//! interval — the group's own tombstone in the same batch, as Kafka's
//! `maybeDeleteGroup` appends it only for an empty group,
//! so a reader of `__consumer_offsets` never sees a group record with no
//! offsets and no members hanging behind a partial write. The group record is
//! the classic k2 `GroupMetadata` for a classic group, and for a KIP-848 group
//! the k6 `TargetAssignmentMetadata`, the deprecated k4
//! `ConsumerGroupPartitionMetadata` and the k3 `GroupMetadata`, in Kafka's
//! order.
//! Deleting the group stops the actor; the coordinator drops the registry
//! entry when it reads [`ReapOutcome::group_deleted`].

use std::collections::HashSet;

use tokio::sync::oneshot;

use super::{
    GroupTombstone, group_tombstone_keys,
    offset_delete::{SubscribedTopics, offset_delete_guard},
};
use crate::{
    coordinator::unified::{
        GroupCoordinator, OffsetRecordBatchBuilder,
        classic_state::GroupState as ClassicGroupState,
        group::{CoordinatorGroup, GroupKind},
        offsets_log::OffsetsLog,
        persistence::{Key, OffsetCommitValue, encode_key},
    },
    error::BrokerError,
};

/// What one reap pass did to one group.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ReapOutcome {
    /// The `(topic, partition)` offsets this pass tombstoned, sorted.
    pub reaped: Vec<(String, i32)>,
    /// `true` when the pass also tombstoned the group itself, which stops the
    /// actor and empties its registry entry.
    pub group_deleted: bool,
}

/// The `ReapExpiredOffsets` mailbox arm. Returns the actor's keep-running
/// flag: a group that tombstoned itself has no state left to serve.
pub(super) async fn handle_reap_message(
    group: &mut CoordinatorGroup,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
    retention_ms: i64,
    empty_grace_ms: i64,
    reply: oneshot::Sender<ReapOutcome>,
) -> bool {
    let outcome = reap_expired_offsets(
        group,
        offsets_log,
        coordinator,
        now_ms,
        retention_ms,
        empty_grace_ms,
    )
    .await;
    let keep_running = !outcome.group_deleted;
    let _ = reply.send(outcome);
    keep_running
}

/// Tombstone every offset of this group that has fallen out of retention, and
/// the group with them when none is left.
///
/// A failed append leaves the in-memory offsets untouched and reports an empty
/// outcome, so the next sweep tries the same group again. That is the same
/// idempotent every-broker-sweeps shape the break-glass reaper uses: a
/// tombstone written twice is a no-op.
async fn reap_expired_offsets(
    group: &mut CoordinatorGroup,
    offsets_log: &dyn OffsetsLog,
    coordinator: &GroupCoordinator,
    now_ms: i64,
    retention_ms: i64,
    empty_grace_ms: i64,
) -> ReapOutcome {
    let Some(condition) = expiration_condition(group) else {
        return ReapOutcome::default();
    };
    let (expired, all_expired) = expired_offsets(group, &condition, now_ms, retention_ms);
    // Once the sweep has taken the expired offsets the group keeps none, and
    // an empty group that keeps no offsets and no open transaction is a dead
    // group. Kafka's `GroupMetadataManager.cleanupGroupMetadata` transitions
    // exactly that group to `Dead` and appends its tombstone whether or not
    // this pass expired anything, which is why a group that never committed,
    // or whose last offset an `OffsetDelete` already removed, still goes.
    // Verified on `apache/kafka:4.3.1`: a group left memberless by
    // `kafka-consumer-groups --delete-offsets` disappears from `--list` at the
    // next `offsets.retention.check.interval.ms`. A group with members stays
    // (`maybeDeleteGroup` needs `isEmpty`).
    let delete_group = all_expired && !group.has_members();
    if expired.is_empty() && !(delete_group && settled_empty(group, now_ms, empty_grace_ms)) {
        return ReapOutcome::default();
    }
    if let Err(error) = append_tombstones(
        offsets_log,
        &group.group_id,
        &expired,
        delete_group.then_some(&group.kind),
        now_ms,
    )
    .await
    {
        tracing::warn!(
            group_id = %group.group_id,
            %error,
            "offset-retention tombstone write failed; retrying on the next sweep",
        );
        return ReapOutcome::default();
    }
    for key in &expired {
        group.committed_offsets.remove(key);
    }
    if delete_group {
        coordinator.remove_cached_seed(&group.group_id);
    }
    tracing::info!(
        group_id = %group.group_id,
        offsets = expired.len(),
        group_deleted = delete_group,
        "reaped expired committed offsets",
    );
    ReapOutcome {
        reaped: expired,
        group_deleted: delete_group,
    }
}

/// `true` when this group has been memberless for a whole sweep interval.
///
/// The zero-offset deletion path needs it. Everything that puts a group in the
/// registry spawns the actor first and populates it a message later — an
/// `OffsetCommit` for an unknown id, the `WriteTxnMarkers` materialisation, a
/// `JoinGroup` — so a group that holds nothing right now may simply be one
/// whose first message has not landed yet. Kafka has no such window: its
/// coordinator creates and fills a group under one lock. Waiting one
/// `offsets.retention.check.interval.ms` gives every such caller its own
/// mailbox turn and still deletes the group on the pass after it truly went
/// idle, which is the timing `apache/kafka:4.3.1` shows.
///
/// A group whose offsets all aged out does not consult this: those offsets are
/// themselves proof that the group is older than its retention.
fn settled_empty(group: &CoordinatorGroup, now_ms: i64, empty_grace_ms: i64) -> bool {
    group
        .empty_since_ms
        .is_some_and(|since| now_ms >= since.saturating_add(empty_grace_ms))
}

/// Kafka's `Group.offsetExpirationCondition` together with the topics
/// `isSubscribedToTopic` holds for.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ExpirationCondition {
    /// The moment the group went empty, when that is the base Kafka measures
    /// retention from, and `None` when it measures from each commit.
    empty_since_ms: Option<i64>,
    /// The topics whose offsets never expire while the condition holds.
    subscribed: HashSet<String>,
}

/// Kafka's `offsetExpirationCondition`, or `None` when no offset of the group
/// may expire.
///
/// `ClassicGroup.offsetExpirationCondition` splits three ways. A classic group
/// that carries a protocol type — one some consumer has joined — measures from
/// `currentStateTimestamp` once it is empty, and from each commit while it is
/// a `Stable` `consumer` group whose subscribed topics are known. A simple
/// group, which only ever committed offsets and so never took a protocol type,
/// measures from the commit; so does a KIP-848 group, per
/// `ConsumerGroup.offsetExpirationCondition`. A memberless classic group
/// subscribes to nothing.
///
/// Only the memberless classic group with a protocol type has a group-empty
/// moment that survives a restart. It writes the memberless k2 snapshot when
/// its last member leaves, and replay reads `current_state_timestamp_ms` back
/// out of it. The others have nothing to read, so their
/// [`empty_since_ms`](CoordinatorGroup::empty_since_ms) is only the moment
/// this process first ran their actor — and measuring from that would hand
/// every dead group another full `offsets.retention.minutes` on every broker
/// restart, which is the leak this module exists to close.
fn expiration_condition(group: &CoordinatorGroup) -> Option<ExpirationCondition> {
    if let Some(state) = group.as_classic() {
        if state.protocol_type.is_none() {
            return Some(ExpirationCondition {
                empty_since_ms: None,
                subscribed: HashSet::new(),
            });
        }
        if state.members.is_empty() {
            return Some(ExpirationCondition {
                empty_since_ms: group.empty_since_ms,
                subscribed: HashSet::new(),
            });
        }
        if state.state != ClassicGroupState::Stable {
            return None;
        }
    }
    // `offset_delete_guard` refuses a live classic group off the `consumer`
    // protocol, and answers `All` when its subscriptions cannot be read.
    // Kafka's classic group has no expiration condition in either case.
    match offset_delete_guard(group) {
        Ok(SubscribedTopics::Named(subscribed)) => Some(ExpirationCondition {
            empty_since_ms: None,
            subscribed,
        }),
        Ok(SubscribedTopics::All) | Err(_) => None,
    }
}

/// The offsets `condition` expires, sorted, and whether the group is left with
/// no offset and no open transaction afterwards.
///
/// An offset of a subscribed topic stays, as does one that an open
/// transaction has written or reserved (`hasPendingTransactionalOffsets`).
fn expired_offsets(
    group: &CoordinatorGroup,
    condition: &ExpirationCondition,
    now_ms: i64,
    retention_ms: i64,
) -> (Vec<(String, i32)>, bool) {
    let open_transactions = group.unresolved_txn_keys();
    let mut expired: Vec<(String, i32)> = group
        .committed_offsets
        .iter()
        .filter(|((topic, _), entry)| {
            !condition.subscribed.contains(topic)
                && entry.is_expired(now_ms, condition.empty_since_ms, retention_ms)
        })
        .filter(|(key, _)| !open_transactions.contains(*key))
        .map(|(key, _)| key.clone())
        .collect();
    expired.sort_unstable();
    let all_expired =
        expired.len() == group.committed_offsets.len() && open_transactions.is_empty();
    (expired, all_expired)
}

/// Appends [`tombstone_batch`] to the partition of `group_id`.
///
/// # Errors
///
/// Returns the error of the append, or [`BrokerError::Protocol`] when a key is
/// not encodable because a string of it is longer than 32767 bytes.
pub(super) async fn append_tombstones(
    offsets_log: &dyn OffsetsLog,
    group_id: &str,
    expired: &[(String, i32)],
    delete_group_of_kind: Option<&GroupKind>,
    now_ms: i64,
) -> Result<(), BrokerError> {
    let batch = tombstone_batch(group_id, expired, delete_group_of_kind, now_ms)?;
    offsets_log.append(group_id, batch).await
}

/// One `__consumer_offsets` batch: an offset tombstone for each expired key,
/// then the group's own tombstones when `delete_group_of_kind` is set.
fn tombstone_batch(
    group_id: &str,
    expired: &[(String, i32)],
    delete_group_of_kind: Option<&GroupKind>,
    now_ms: i64,
) -> Result<krabka_protocol::records::RecordBatch, BrokerError> {
    let mut builder = OffsetRecordBatchBuilder::default();
    for (topic, partition) in expired {
        builder.push(
            OffsetCommitValue::encode_key(group_id, topic, *partition)?,
            None,
        );
    }
    match delete_group_of_kind {
        None => {}
        Some(GroupKind::Classic(_)) => builder.push(
            encode_key(&Key::GroupMetadata {
                group_id: group_id.into(),
            })?,
            None,
        ),
        // Kafka's `ConsumerGroup.createGroupTombstoneRecords`. A group that is
        // deleted has no members, so it tombstones the target-assignment
        // metadata, every regular expression resolution it still holds, the
        // deprecated k4 subscription metadata whether or not the log holds
        // one, and the group epoch record.
        Some(GroupKind::Consumer(state)) => {
            let tombstone = GroupTombstone {
                members: state.members.keys().cloned().collect(),
                regexes: state.resolved_regex_names(),
            };
            for key in group_tombstone_keys(group_id, &tombstone)? {
                builder.push(key, None);
            }
        }
    }
    Ok(builder.finish(now_ms))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use assert2::check;
    use bytes::Bytes;
    use krabka_log::Offset;

    use super::*;
    use crate::coordinator::unified::{
        actor::test_support::subscription_blob,
        classic_state::{ClassicGroup, Member, OffsetEntry},
        consumer_state::{
            GroupState as ConsumerState, ResolvedRegularExpression, test_support::member,
        },
        persistence_next_gen::{NextGenKey, encode_key as encode_next_gen_key},
    };

    const RETENTION_MS: i64 = 1_000;
    const NOW_MS: i64 = 10_000;
    /// A commit old enough to expire at `NOW_MS`.
    const OLD_MS: i64 = NOW_MS - RETENTION_MS;
    /// A commit too recent to expire at `NOW_MS`.
    const FRESH_MS: i64 = NOW_MS - 1;

    fn classic(
        state: ClassicGroupState,
        protocol_type: Option<&str>,
        members: &[&[&str]],
    ) -> GroupKind {
        let mut group = ClassicGroup::new("g");
        group.state = state;
        group.protocol_type = protocol_type.map(String::from);
        group.protocol_name = (!members.is_empty()).then(|| "range".to_string());
        for (index, topics) in members.iter().enumerate() {
            let id = format!("m{index}");
            group.members.insert(
                id.clone(),
                Member::new(
                    id,
                    "client",
                    "host",
                    std::time::Duration::from_secs(30),
                    std::time::Duration::from_mins(1),
                    vec![("range".into(), subscription_blob(topics))],
                ),
            );
        }
        GroupKind::Classic(group)
    }

    fn consumer(subscriptions: &[&[&str]]) -> GroupKind {
        let mut group = ConsumerState::new("g");
        for (index, topics) in subscriptions.iter().enumerate() {
            let mut m = member(&format!("m{index}"));
            m.subscribed_topic_names = topics.iter().map(|topic| (*topic).to_string()).collect();
            group.add_or_update_member(m);
        }
        GroupKind::Consumer(group)
    }

    fn offsets(commits: &[(&str, i64)]) -> HashMap<(String, i32), OffsetEntry> {
        commits
            .iter()
            .map(|(topic, commit_timestamp_ms)| {
                (
                    ((*topic).to_string(), 0),
                    OffsetEntry {
                        offset: Offset(7),
                        leader_epoch: -1,
                        metadata: String::new(),
                        commit_timestamp_ms: *commit_timestamp_ms,
                        expire_timestamp_ms: None,
                        topic_id: None,
                    },
                )
            })
            .collect()
    }

    fn keys(topics: &[&str]) -> Vec<(String, i32)> {
        topics
            .iter()
            .map(|topic| ((*topic).to_string(), 0))
            .collect()
    }

    /// Kafka's `cleanupExpiredOffsets` over one group: (label, group kind,
    /// commits, transactional keys still open, empty-since stamp) to the
    /// offsets reaped and whether the group has no offset and no open
    /// transaction left, or `None` when the group has no expiration condition.
    #[test]
    fn cleanup_expires_only_unsubscribed_offsets_per_group_kind() {
        let stable = ClassicGroupState::Stable;
        let rows = vec![
            (
                "stable consumer group keeps its subscribed topic",
                classic(stable, Some("consumer"), &[&["a"], &["a"]]),
                vec![("a", OLD_MS), ("b", OLD_MS), ("c", FRESH_MS)],
                vec![],
                None,
                Some((vec!["b"], false)),
            ),
            (
                "rebalancing classic group expires nothing",
                classic(
                    ClassicGroupState::PreparingRebalance,
                    Some("consumer"),
                    &[&["a"]],
                ),
                vec![("b", OLD_MS)],
                vec![],
                None,
                None,
            ),
            (
                "stable group off the consumer protocol expires nothing",
                classic(stable, Some("connect"), &[&["a"]]),
                vec![("b", OLD_MS)],
                vec![],
                None,
                None,
            ),
            (
                "simple group measures from the commit",
                classic(ClassicGroupState::Empty, None, &[]),
                vec![("a", OLD_MS), ("b", FRESH_MS)],
                vec![],
                None,
                Some((vec!["a"], false)),
            ),
            (
                "empty joined group measures from the moment it emptied",
                classic(ClassicGroupState::Empty, Some("consumer"), &[]),
                vec![("a", OLD_MS - RETENTION_MS)],
                vec![],
                Some(FRESH_MS),
                Some((vec![], false)),
            ),
            (
                "empty joined group loses every offset",
                classic(ClassicGroupState::Empty, Some("consumer"), &[]),
                vec![("a", OLD_MS), ("b", OLD_MS)],
                vec![],
                Some(OLD_MS),
                Some((vec!["a", "b"], true)),
            ),
            (
                "offset committed after the group emptied still expires from the emptying",
                classic(ClassicGroupState::Empty, Some("consumer"), &[]),
                vec![("a", FRESH_MS)],
                vec![],
                Some(OLD_MS),
                Some((vec!["a"], true)),
            ),
            (
                "open transaction keeps its offset and the group",
                classic(ClassicGroupState::Empty, Some("consumer"), &[]),
                vec![("a", OLD_MS), ("b", OLD_MS)],
                vec!["b"],
                Some(OLD_MS),
                Some((vec!["a"], false)),
            ),
            (
                "consumer group keeps its subscribed topic",
                consumer(&[&["a"]]),
                vec![("a", OLD_MS), ("b", OLD_MS)],
                vec![],
                None,
                Some((vec!["b"], false)),
            ),
            (
                "live consumer group whose offsets all expired",
                consumer(&[&["a"]]),
                vec![("b", OLD_MS)],
                vec![],
                None,
                Some((vec!["b"], true)),
            ),
        ];
        for (label, kind, commits, open, empty_since_ms, want) in rows {
            let mut group = CoordinatorGroup::seeded("g", kind, offsets(&commits));
            group.empty_since_ms = empty_since_ms;
            group.add_pending_txn_offsets(1, 1, keys(&open));
            let got = expiration_condition(&group)
                .map(|condition| expired_offsets(&group, &condition, NOW_MS, RETENTION_MS));
            let want = want.map(|(topics, all_expired)| (keys(&topics), all_expired));
            check!(got == want, "{label}");
        }
    }

    /// A live group whose every offset expired keeps its group record: Kafka's
    /// `maybeDeleteGroup` removes only an empty group.
    #[tokio::test]
    async fn a_live_group_loses_unsubscribed_offsets_but_keeps_its_record() {
        use crate::coordinator::unified::offsets_log::fake::InMemoryOffsetsLog;

        let (coordinator, log) = super::super::test_support::make_coordinator();
        let mut group = CoordinatorGroup::seeded(
            "g",
            classic(ClassicGroupState::Stable, Some("consumer"), &[&["a"]]),
            offsets(&[("a", OLD_MS), ("b", OLD_MS)]),
        );
        let log: &InMemoryOffsetsLog = &log;

        let outcome =
            reap_expired_offsets(&mut group, log, &coordinator, NOW_MS, RETENTION_MS, 0).await;

        check!(
            outcome
                == ReapOutcome {
                    reaped: keys(&["b"]),
                    group_deleted: false,
                }
        );
        check!(group.committed_offsets.keys().cloned().collect::<Vec<_>>() == keys(&["a"]));
    }

    /// The group's own tombstones follow Kafka 4.3.1: k2 for a classic group,
    /// and for a consumer group with no members the k6 tombstone, a k16
    /// tombstone for each regular expression resolution the group still
    /// holds, and the k4 and k3 tombstones of
    /// `ConsumerGroup.createGroupTombstoneRecords`, in that order.
    #[test]
    fn group_tombstones_follow_kafka() {
        let tombstone = |key: Bytes| (Some(key), None::<Bytes>);
        let ng = |key: NextGenKey| tombstone(encode_next_gen_key(&key).unwrap());
        let gid = || "g".to_string();
        let expired = tombstone(OffsetCommitValue::encode_key("g", "t", 0).unwrap());
        let rows = [
            ("no group tombstone", None, vec![expired.clone()]),
            (
                "classic group",
                Some(classic(ClassicGroupState::Empty, None, &[])),
                vec![
                    expired.clone(),
                    tombstone(encode_key(&Key::GroupMetadata { group_id: gid() }).unwrap()),
                ],
            ),
            (
                "consumer group",
                Some(consumer(&[])),
                vec![
                    expired.clone(),
                    ng(NextGenKey::TargetAssignmentMetadata { group_id: gid() }),
                    ng(NextGenKey::PartitionMetadata { group_id: gid() }),
                    ng(NextGenKey::GroupMetadata { group_id: gid() }),
                ],
            ),
            (
                "consumer group with resolved regular expressions",
                Some({
                    let mut group = ConsumerState::new("g");
                    for regex in ["b.*", "a.*"] {
                        group.set_resolved_regex(
                            regex.into(),
                            ResolvedRegularExpression {
                                topics: std::collections::BTreeSet::new(),
                                version: 1,
                                timestamp_ms: 2,
                            },
                        );
                    }
                    GroupKind::Consumer(group)
                }),
                vec![
                    expired.clone(),
                    ng(NextGenKey::TargetAssignmentMetadata { group_id: gid() }),
                    ng(NextGenKey::RegularExpression {
                        group_id: gid(),
                        regex: "a.*".into(),
                    }),
                    ng(NextGenKey::RegularExpression {
                        group_id: gid(),
                        regex: "b.*".into(),
                    }),
                    ng(NextGenKey::PartitionMetadata { group_id: gid() }),
                    ng(NextGenKey::GroupMetadata { group_id: gid() }),
                ],
            ),
        ];
        for (name, kind, expected) in rows {
            let batch = tombstone_batch("g", &keys(&["t"]), kind.as_ref(), NOW_MS).unwrap();

            let records: Vec<_> = batch
                .records
                .into_iter()
                .map(|record| (record.key, record.value))
                .collect();
            check!(records == expected, "{name}");
        }
    }
}
