//! The controller half of KIP-966: recompute a partition's ELR whenever a
//! change to it is about to be submitted, and publish the result.
//!
//! ## The rules, and where they come from
//!
//! Apache Kafka keeps the rules in `PartitionChangeBuilder`, which assembles
//! one `PartitionChangeRecord` per partition change. Read out of tag 4.3.1,
//! the two sets it publishes are
//!
//! ```text
//! // the eligible set: maybePopulateTargetElr, then maybeUpdateRecordElr
//! if (targetIsr.size() >= minISR) { targetElr = []; }
//! else { targetElr = (elr ∪ isr) − targetIsr − uncleanShutdownReplicas; }
//! if (the change installs an ISR of its own) { targetElr = []; }
//!
//! // the last-known set: maybeUpdateLastKnownLeader
//! if (the change leaves no leader && lastKnownElr is empty) { lastKnownElr = [previous leader]; }
//! else if (the change gives a leader && lastKnownElr is not empty) { lastKnownElr = []; }
//! ```
//!
//! with `elr`, `isr` and `lastKnownElr` read from the partition as it stands
//! before the change and `targetIsr` the ISR the change installs. A change
//! installs an ISR of its own, in Kafka, only on an unclean election: the
//! replica it elects need not hold every committed record, so nothing that
//! came before is still known to be complete.
//!
//! `maybePopulateTargetElr` also works out a multi-member last-known set,
//! `(candidates ∪ lastKnownElr) − targetIsr − targetElr`, but it is never
//! published: `useLastKnownLeaderInBalancedRecovery` defaults to `true`,
//! nothing in `ReplicationControlManager` turns it off, and with it on
//! `maybeUpdateRecordElr` returns before it would write that set. The last-known
//! ELR that Kafka publishes is therefore one replica, the last leader, and only
//! while the partition has no leader. [`crate::leader_election`] elects that
//! replica when it returns (`canElectLastKnownLeader`).
//!
//! [`next_partition_elr`] is those rules, `uncleanShutdownReplicas` included:
//! [`ElrPublisher::after_unclean_shutdown`] is how a batch names one, and it
//! subtracts the replica from the eligible set and from nothing else. krabka
//! subtracts a second set Kafka does not, the replicas that are no longer in
//! the replica set, because the partition can no longer elect them. One
//! difference is krabka's: the unclean-election test is "the elected leader
//! was in neither the previous ISR nor the ELR" rather than "the change
//! carries an ISR", because krabka's election paths always carry one.
//!
//! ## A partition without a leader
//!
//! Kafka writes `leader = -1` when no rung of the election ladder can elect,
//! together with the ELR the shortened ISR implies and the last leader as the
//! last-known ELR. A krabka `PartitionRecord` always names a leader, so the
//! controller scans leave the record alone -- a dead leader dropped from its
//! own ISR would be a leader outside it -- and publish the rest through
//! [`ElrPublisher::leaderless`]. The record keeps the last leader and its ISR,
//! and the one-member last-known ELR that names it is what says the partition
//! has no leader (see [`is_leaderless`](super::state::is_leaderless)). Read
//! against that marker, the last leader is not in the ISR any more, which is
//! where Kafka's ISR went empty; [`next_partition_elr`] takes it out of the
//! ISRs it reads for that reason, so every later change derives from the state
//! Kafka would hold.
//!
//! Any change that gives the partition a leader, that is to say a different
//! one or the same one under a higher leader epoch, publishes the last-known
//! ELR empty again. A change that keeps both, an ISR shrink for another
//! replica, keeps the marker.
//!
//! Kafka reaches `uncleanShutdownReplicas` from
//! `ReplicationControlManager.handleBrokerShutdown`, whose unclean branch is
//! a broker rejoining without a clean-shutdown proof. krabka answers that
//! event in two places, one per Kafka call:
//! [`withdraw_elr_membership`](crate::elr::withdraw_elr_membership) withdraws
//! the published membership, and
//! [`compute_unclean_restart_changes`](crate::leader_election::compute_unclean_restart_changes)
//! drops the broker from the ISRs that still name it and runs this publisher
//! over the result with the broker excluded. A restart that does prove itself
//! clean reaches neither.
//!
//! Kafka gates the whole thing on the `eligible.leader.replicas.version`
//! feature: `ReplicationControlManager` builds every `PartitionChangeBuilder`
//! with `setEligibleLeaderReplicasEnabled(isElrEnabled())`, so at level 0 no
//! partition ever gains an eligible set or a last-known leader. krabka
//! registers the same feature, and [`ElrPublisher::extend`] publishes nothing
//! while it is below level 1. The read side needs no gate of its own: a
//! cluster that never published an ELR has none to read, and a downgrade to 0
//! clears what an earlier level 1 left behind.
//!
//! ## What gets published
//!
//! [`ElrPublisher::extend`] reads the partition changes a controller path has
//! already built, works out the ELR each of their partitions ends up with,
//! and appends one `V1PartitionElr` per partition whose rendered value moved.
//! The partitions the controller reports as left without a leader are treated
//! the same way. Legacy topic-config state is migrated first; removing its
//! private key keeps every other topic override from the batch or image
//! intact.
//!
//! Nothing is appended when nothing moved, which is the common case: while
//! every partition keeps a leader, a cluster that leaves `min.insync.replicas`
//! at Kafka's default of 1 can never have a non-empty ELR, because an ISR that
//! reached zero members has no partition record to reach it with.

use {
    super::state::{
        PartitionElr, TopicElr, legacy_elr, metadata_node_ids, wire_node_ids, without_legacy_elr,
    },
    crate::{
        config_keys::effective_min_insync_replicas,
        features::{ELR_VERSION, feature_enabled},
    },
    krabka_metadata::{MetadataImage, MetadataRecord, PartitionElrRecord, PartitionRecord},
    std::collections::{BTreeMap, BTreeSet},
};

/// Appends the ELR state implied by a batch of controller changes to that
/// batch.
///
/// It borrows the image the batch was computed against, so the "before" side
/// of every rule is the partition as the controller saw it.
pub(crate) struct ElrPublisher<'a> {
    image: &'a MetadataImage,
    /// Kafka's `uncleanShutdownReplicas`: ids the batch may not derive back
    /// into an eligible set, whatever the ISR it is leaving says.
    unclean_shutdown: BTreeSet<i32>,
    /// The partitions the batch leaves without a leader, each with the ISR
    /// Kafka would write for it. No partition record carries them.
    leaderless: BTreeMap<(String, i32), Vec<krabka_metadata::NodeId>>,
}

impl<'a> ElrPublisher<'a> {
    /// Read ELR state against `image`, the metadata as it stands before the
    /// batch applies.
    pub(crate) fn new(image: &'a MetadataImage) -> Self {
        Self {
            image,
            unclean_shutdown: BTreeSet::new(),
            leaderless: BTreeMap::new(),
        }
    }

    /// Report that the batch leaves `partition` without a leader, so its ISR
    /// becomes `isr`: what Kafka's `PartitionChangeBuilder` writes as
    /// `leader = -1` next to that ISR.
    ///
    /// The scans call it for every partition they answer with no election.
    /// Its record stays as it is, because a krabka partition record cannot
    /// name no leader; [`extend`](Self::extend) publishes the ELR the
    /// shortened ISR implies, and the last leader as the last-known ELR, which
    /// is what marks the partition as leaderless (see the module docs). A
    /// partition the marker already names publishes nothing more.
    pub(crate) fn leaderless(
        &mut self,
        partition: &PartitionRecord,
        isr: Vec<krabka_metadata::NodeId>,
    ) {
        self.leaderless
            .insert((partition.topic.clone(), partition.partition), isr);
    }

    /// Read ELR state against `image` for a batch that is reacting to `node`
    /// coming back from an unclean stop, so that no partition in the batch
    /// derives `node` back into its eligible set.
    ///
    /// This is Kafka's
    /// `PartitionChangeBuilder.setUncleanShutdownReplicas(List.of(brokerId))`,
    /// which `ReplicationControlManager.handleBrokerShutdown` sets on both of
    /// the `generateLeaderAndIsrUpdates` calls it makes for an unclean
    /// shutdown. Read out of tag 4.3.1, `maybePopulateTargetElr` subtracts the
    /// list from `targetElr` and from nothing else. In particular the excluded
    /// id does not move into the last-known ELR: that set holds the last
    /// leader of a leaderless partition and nothing more.
    ///
    /// Without it, [`next_partition_elr`] would re-derive `node` from the
    /// `old_isr` half of its candidate set -- the very ISR the batch is
    /// removing it from -- and the withdrawal would not survive its own
    /// batch.
    pub(crate) fn after_unclean_shutdown(
        image: &'a MetadataImage,
        node: krabka_metadata::NodeId,
    ) -> Self {
        Self {
            image,
            // An id too wide for the wire can never be named in a published
            // value, so there is nothing to exclude.
            unclean_shutdown: i32::try_from(node.0).ok().into_iter().collect(),
            leaderless: BTreeMap::new(),
        }
    }

    /// Append to `changes` the `V1PartitionElr` records that carry the ELR
    /// state its partition changes imply.
    ///
    /// Call it once, after a controller path has built its whole batch and
    /// before it submits: the records are appended after the partition
    /// changes they describe, so a replay that stops between the two sees a
    /// stale ELR rather than one that names a partition state no record ever
    /// established.
    pub(crate) fn extend(&self, changes: &mut Vec<MetadataRecord>) {
        if feature_enabled(self.image, ELR_VERSION, 1) {
            let mut published = self.legacy_migration_records(changes);
            published.extend(self.partition_records(changes));
            published.extend(self.leaderless_records());
            changes.extend(published);
        }
        coalesce_partition_state(self.image, changes);
    }

    fn legacy_migration_records(&self, changes: &[MetadataRecord]) -> Vec<MetadataRecord> {
        let batch = Batch::of(changes);
        let mut records = Vec::new();
        for topic in batch.partitions.keys() {
            let Some(legacy) = legacy_elr(self.image, topic) else {
                continue;
            };
            records.extend(legacy.records(topic));
            let replacement = changes.iter().rev().find_map(|record| match record {
                MetadataRecord::V1TopicConfig(config) if config.topic == *topic => {
                    Some(&config.overrides)
                }
                _ => None,
            });
            if let Some(record) = without_legacy_elr(self.image, topic, replacement) {
                records.push(record);
            }
        }
        records
    }

    /// The `V1PartitionElr` records `extend` appends. Split out so the
    /// decision can be tested without the append.
    fn partition_records(&self, changes: &[MetadataRecord]) -> Vec<MetadataRecord> {
        let batch = Batch::of(changes);
        batch
            .partitions
            .iter()
            .filter(|(topic, _)| !batch.deleted.contains(*topic))
            .flat_map(|(topic, partitions)| {
                partitions.iter().filter_map(|(partition, record)| {
                    self.partition_record(topic, *partition, record)
                })
            })
            .collect()
    }

    /// The partition's `V1PartitionElr`, or `None` when its rendered ELR value
    /// is the one the topic already carries.
    fn partition_record(
        &self,
        topic: &str,
        partition: i32,
        record: &PartitionRecord,
    ) -> Option<MetadataRecord> {
        let before = TopicElr::of_topic(self.image, topic).partition(partition);
        let after = next_partition_elr(
            self.image,
            self.image.partition(topic, partition),
            record,
            &before,
            &self.unclean_shutdown,
        );
        changed_record(topic, partition, &before, &after)
    }

    /// The `V1PartitionElr` records of the partitions [`Self::leaderless`]
    /// named.
    fn leaderless_records(&self) -> Vec<MetadataRecord> {
        self.leaderless
            .iter()
            .filter_map(|((topic, partition), isr)| {
                let previous = self.image.partition(topic, *partition)?;
                let before = TopicElr::of_topic(self.image, topic).partition(*partition);
                let after = leaderless_partition_elr(
                    self.image,
                    previous,
                    isr,
                    &before,
                    &self.unclean_shutdown,
                );
                changed_record(topic, *partition, &before, &after)
            })
            .collect()
    }
}

/// The partition's `V1PartitionElr`, or `None` when `after` is the value the
/// topic already carries as `before`.
fn changed_record(
    topic: &str,
    partition: i32,
    before: &PartitionElr,
    after: &PartitionElr,
) -> Option<MetadataRecord> {
    if after == before {
        return None;
    }
    Some(MetadataRecord::V1PartitionElr(PartitionElrRecord {
        topic: topic.to_string(),
        partition,
        eligible_leader_replicas: metadata_node_ids(&after.eligible_leader_replicas),
        last_known_elr: metadata_node_ids(&after.last_known_elr),
    }))
}

#[derive(Default)]
struct PendingState {
    eligible: Option<Vec<krabka_metadata::NodeId>>,
    last_known: Option<Vec<krabka_metadata::NodeId>>,
    recovery: Option<krabka_metadata::LeaderRecoveryState>,
}

/// Kafka carries structural, recovery, and ELR changes for one partition in
/// one `PartitionChangeRecord`. Keep the internal batch equally atomic when a
/// caller built those pieces independently.
///
/// One partition record stays whole, and its ELR and recovery records follow it
/// on their own: an election of the leader the image already names, under a
/// higher leader epoch. A `V1PartitionUpdate` reaches the log as a
/// `PartitionChangeRecord`, which sets the leader by diffing it against the
/// image and bumps the leader epoch only when the leader changes, so the bump
/// would be lost. That is the election of the last known leader, or of the
/// last leader from the ELR, when a partition that lost its leader gets it back.
fn coalesce_partition_state(image: &MetadataImage, changes: &mut Vec<MetadataRecord>) {
    let mut pending = BTreeMap::<(String, i32), PendingState>::new();
    changes.retain(|record| match record {
        MetadataRecord::V1PartitionElr(record) => {
            let state = pending
                .entry((record.topic.clone(), record.partition))
                .or_default();
            state.eligible = Some(record.eligible_leader_replicas.clone());
            state.last_known = Some(record.last_known_elr.clone());
            false
        }
        MetadataRecord::V1PartitionRecovery(record) => {
            pending
                .entry((record.topic.clone(), record.partition))
                .or_default()
                .recovery = Some(record.state);
            false
        }
        _ => true,
    });

    for record in changes.iter_mut() {
        let (topic, partition) = match record {
            MetadataRecord::V1Partition(record) if !re_elects_the_leader(image, record) => {
                (&record.topic, record.partition)
            }
            MetadataRecord::V1PartitionUpdate(record) => {
                (&record.partition.topic, record.partition.partition)
            }
            _ => continue,
        };
        let Some(state) = pending.remove(&(topic.clone(), partition)) else {
            continue;
        };
        match record {
            MetadataRecord::V1Partition(partition) => {
                *record =
                    MetadataRecord::V1PartitionUpdate(krabka_metadata::PartitionUpdateRecord {
                        partition: partition.clone(),
                        eligible_leader_replicas: state.eligible,
                        last_known_elr: state.last_known,
                        recovery_state: state.recovery,
                    });
            }
            MetadataRecord::V1PartitionUpdate(update) => {
                update.eligible_leader_replicas =
                    state.eligible.or(update.eligible_leader_replicas.take());
                update.last_known_elr = state.last_known.or(update.last_known_elr.take());
                update.recovery_state = state.recovery.or(update.recovery_state);
            }
            _ => unreachable!(),
        }
    }

    for ((topic, partition), state) in pending {
        if let (Some(eligible), Some(last_known)) = (state.eligible, state.last_known) {
            changes.push(MetadataRecord::V1PartitionElr(PartitionElrRecord {
                topic: topic.clone(),
                partition,
                eligible_leader_replicas: eligible,
                last_known_elr: last_known,
            }));
        }
        if let Some(recovery) = state.recovery {
            changes.push(MetadataRecord::V1PartitionRecovery(
                krabka_metadata::PartitionRecoveryRecord {
                    topic,
                    partition,
                    state: recovery,
                },
            ));
        }
    }
}

/// Whether `next` elects the leader `image` already names, under a higher
/// leader epoch.
fn re_elects_the_leader(image: &MetadataImage, next: &PartitionRecord) -> bool {
    image
        .partition(&next.topic, next.partition)
        .is_some_and(|current| {
            current.leader == next.leader && current.leader_epoch != next.leader_epoch
        })
}

/// The parts of a change batch the publisher reads: the last partition record
/// per partition, the last topic-config record per topic, and the topics the
/// batch deletes.
struct Batch<'a> {
    partitions: BTreeMap<&'a str, BTreeMap<i32, &'a PartitionRecord>>,
    deleted: BTreeSet<&'a str>,
}

impl<'a> Batch<'a> {
    /// Index `changes`. Later records win, which is the order the image
    /// applies them in.
    fn of(changes: &'a [MetadataRecord]) -> Self {
        let mut batch = Self {
            partitions: BTreeMap::new(),
            deleted: BTreeSet::new(),
        };
        for change in changes {
            match change {
                MetadataRecord::V1Partition(record) => {
                    batch
                        .partitions
                        .entry(record.topic.as_str())
                        .or_default()
                        .insert(record.partition, record);
                }
                MetadataRecord::V1DeleteTopic(record) => {
                    batch.deleted.insert(record.name.as_str());
                }
                _ => {}
            }
        }
        batch
    }
}

/// The ELR one partition ends up with when `next` applies.
///
/// `previous` is the partition as the image holds it, `None` for a partition
/// the batch creates. `published` is the ELR the metadata image currently
/// carries for it. `unclean_shutdown` is Kafka's `uncleanShutdownReplicas`:
/// replicas the batch has just stopped trusting, which no rule here may make
/// eligible again.
///
/// The published last-known ELR follows `maybeUpdateLastKnownLeader`. `next`
/// always names a leader, so the partition ends up without one only when the
/// image already records it that way and `next` keeps that leader under the
/// same leader epoch, which is a change that elects nobody (an ISR shrink for
/// another replica, say). Every other change gives the partition a leader, and
/// the last-known ELR ends up empty. A change that elects nobody because none
/// can be elected has no record, and [`leaderless_partition_elr`] answers for
/// it.
///
/// This is the whole KIP-966 maintenance rule, and the claim an ELR election
/// rests on is a claim about it, so the `data_path_model` stateright search
/// drives it directly: it is the one seam where the model can run the real
/// rule over a partition whose per-replica logs and committed prefix it also
/// holds.
pub(crate) fn next_partition_elr(
    image: &MetadataImage,
    previous: Option<&PartitionRecord>,
    next: &PartitionRecord,
    published: &PartitionElr,
    unclean_shutdown: &BTreeSet<i32>,
) -> PartitionElr {
    next_elr(image, previous, next, published, unclean_shutdown, false)
}

/// The ELR a partition ends up with when a change takes `isr` as its ISR and
/// elects no leader, which Kafka writes as `leader = -1`.
///
/// `previous` is the partition as the image holds it. Its record is not
/// rewritten, so the partition keeps its last leader in it, and the last-known
/// ELR that names that leader is published: the same value Kafka writes the
/// first time a partition becomes leaderless, and the one it keeps while the
/// partition stays that way.
pub(crate) fn leaderless_partition_elr(
    image: &MetadataImage,
    previous: &PartitionRecord,
    isr: &[krabka_metadata::NodeId],
    published: &PartitionElr,
    unclean_shutdown: &BTreeSet<i32>,
) -> PartitionElr {
    let next = PartitionRecord {
        isr: isr.to_vec(),
        ..previous.clone()
    };
    next_elr(
        image,
        Some(previous),
        &next,
        published,
        unclean_shutdown,
        true,
    )
}

/// Both rules of [`next_partition_elr`] and [`leaderless_partition_elr`].
/// `leaderless` says the change elects nobody whatever `next` names.
fn next_elr(
    image: &MetadataImage,
    previous: Option<&PartitionRecord>,
    next: &PartitionRecord,
    published: &PartitionElr,
    unclean_shutdown: &BTreeSet<i32>,
    leaderless: bool,
) -> PartitionElr {
    // A partition the batch creates has no history, so no replica of it is
    // known to hold records the ISR does not.
    let Some(previous) = previous else {
        return PartitionElr::default();
    };

    let mut new_isr = wire_id_set(&next.isr);
    let mut old_isr = wire_id_set(&previous.isr);
    let replicas = wire_id_set(&next.replicas);
    let eligible_before: BTreeSet<i32> =
        published.eligible_leader_replicas.iter().copied().collect();
    let last_leader = i32::try_from(previous.leader.0).ok();

    // Kafka takes the last leader out of the ISR when the partition loses its
    // leader. The record here still lists it, so read the ISR the way Kafka
    // holds it, or the leader would be derived into the eligible set from a
    // list it has already left.
    let was_leaderless = published.is_leaderless(previous.leader);
    if let Some(id) = last_leader.filter(|_| was_leaderless) {
        old_isr.remove(&id);
    }

    // `maybeUpdateLastKnownLeader`: `[previous leader]` while the change
    // leaves the partition without a leader, and empty once it has one. A
    // record that keeps the leader and its epoch elects nobody, and it keeps
    // the last leader in its ISR because a record cannot say otherwise, so that
    // ISR is read as Kafka would hold it too. An ISR the caller gives for a
    // change that elects nobody is Kafka's already.
    let keeps_no_leader = was_leaderless
        && next.leader == previous.leader
        && next.leader_epoch == previous.leader_epoch;
    if let Some(id) = last_leader.filter(|_| keeps_no_leader) {
        new_isr.remove(&id);
    }
    let leaderless = leaderless || keeps_no_leader;
    let last_known: Vec<i32> = last_leader.filter(|_| leaderless).into_iter().collect();

    // An election that installs a leader from neither the ISR nor the ELR may
    // have dropped committed records, so no earlier replica is still known to
    // be complete. Kafka reaches the same state through
    // `maybeUpdateRecordElr`, which clears both sets when a change carries an
    // ISR of its own -- in Kafka only an unclean election does.
    let unclean = !leaderless
        && i32::try_from(next.leader.0)
            .is_ok_and(|id| !old_isr.contains(&id) && !eligible_before.contains(&id));

    // KIP-966's healthy state: an ISR that meets min ISR is on its own enough
    // to hold every committed record, so nothing outside it needs remembering.
    let min_isr = effective_min_insync_replicas(image, &next.topic, next.replicas.len());
    if unclean || new_isr.len() >= min_isr {
        return PartitionElr {
            eligible_leader_replicas: Vec::new(),
            last_known_elr: last_known,
        };
    }

    // Everything that held every committed record before the change: the ISR
    // it is leaving plus whatever was already eligible.
    let complete: BTreeSet<i32> = old_isr.union(&eligible_before).copied().collect();
    // Kafka drops the replicas its caller named as unclean-shutdown ones, and
    // so does this: `unclean_shutdown` is that list. krabka drops replicas
    // that left the replica set as well, for a related reason: the partition
    // can no longer elect them, so calling them eligible would offer an
    // election that cannot happen.
    let eligible: BTreeSet<i32> = complete
        .difference(&new_isr)
        .copied()
        .filter(|id| replicas.contains(id) && !unclean_shutdown.contains(id))
        .collect();

    PartitionElr {
        eligible_leader_replicas: eligible.into_iter().collect(),
        last_known_elr: last_known,
    }
}

/// The wire ids of a node list, as a set. Node ids too wide for the wire drop,
/// the same way [`wire_node_ids`] drops them from a published value.
fn wire_id_set(nodes: &[krabka_metadata::NodeId]) -> BTreeSet<i32> {
    wire_node_ids(nodes.iter().copied()).into_iter().collect()
}

#[cfg(test)]
mod tests;
