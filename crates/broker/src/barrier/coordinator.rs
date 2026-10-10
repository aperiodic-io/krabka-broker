//! Per-broker `BarrierCoordinator`.
//!
//! The coordinator owns the barrier groups whose `__barrier_state` partition
//! this broker leads. It keeps one entry per group behind a mutex, and it
//! holds that mutex for a whole injection. A scheduled tick, a triggered
//! injection, and a group edit serialise against each other for that reason.
//! The mutex is the only concurrency device here, and a caller that cannot
//! take it gets [`BarrierError::InjectionInProgress`].
//!
//! This coordinator mirrors [`crate::txn::coordinator::TxnCoordinator`] and
//! the share coordinator.
//!
//! # The injection protocol
//!
//! 1. Refuse when this broker does not coordinate the group, and refuse when
//!    the group entry is busy.
//! 2. Allocate the next epoch, and write the injection-start record before the
//!    first marker append. That record is what makes an epoch impossible to
//!    reuse across a coordinator crash.
//! 3. Freeze the target set in that record. A topic-set edit and a
//!    partition-count change both apply from the next epoch.
//! 4. Fan the markers out over the frozen set, grouped by current leader.
//! 5. Retry the partitions that carry no marker, up to the deadline.
//! 6. Write the cut record. A deadline that runs out gives a partial cut that
//!    names the partitions it missed. The epoch is consumed either way.
//! 7. Tombstone the epoch that leaves the retention window. The coordinator
//!    never trims the log, because the group definitions share the prefix.

use std::{collections::HashSet, sync::Arc};

use bytes::Bytes;
use dashmap::DashMap;
use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_metadata::{MetadataImage, NodeId};
use krabka_protocol::records::{Record, RecordBatch};
use krabka_units::{Time, convert::TimeExt as _};
use tokio::sync::{Mutex, RwLock};

mod groups;
mod injection;
mod recovery;
#[cfg(test)]
pub(crate) mod test_support;

pub(crate) use self::{
    groups::{GroupDescription, RetainedCut},
    injection::InjectionOutcome,
};
use crate::{
    auto_topic_creation::AutoTopicCreation,
    barrier::{
        STATE_TOPIC,
        config::BarrierConfig,
        error::BarrierError,
        injection::RemoteMarkerWriter,
        metrics::BrokerBarrierMetrics,
        partitioner::partition_for_group,
        persistence::{RecordKey, encode_key},
        state::{GroupEntry, GroupSpec},
    },
    coordinator::unified::persistence::MAX_STRING_BYTES,
    metadata_source::MetadataSource,
    partition_registry::PartitionRegistry,
};

/// The fan-out deadline a request gets.
///
/// `ceiling` is the operator's configured bound. A request that names none
/// takes it, and one that asks for longer is held to it, so a caller cannot
/// hold a group's lock past what the operator allows.
fn clamp_timeout(requested: Option<Time>, ceiling: Time) -> Time {
    requested.map_or(ceiling, |asked| asked.min(ceiling))
}

/// Check the name of a group that a caller asks to create.
///
/// The name is a string in every `__barrier_state` key and in every marker,
/// and both carry it with an `i16` length.
///
/// # Errors
/// Returns [`BarrierError::InvalidDefinition`] when the name is longer than
/// 32767 bytes.
pub(crate) fn validate_group_name(group: &str) -> Result<(), BarrierError> {
    if group.len() > MAX_STRING_BYTES {
        return Err(BarrierError::InvalidDefinition(format!(
            "a barrier group name is {} bytes, and the limit is {MAX_STRING_BYTES}",
            group.len()
        )));
    }
    Ok(())
}

/// Check a group definition that a caller supplied.
///
/// # Errors
/// Returns [`BarrierError::InvalidDefinition`] when the topic list is empty,
/// when it holds an empty name, a name longer than 32767 bytes or a duplicate
/// name, when `retained_cuts` is below one, or when the interval is not
/// positive.
pub(crate) fn validate_spec(spec: &GroupSpec) -> Result<(), BarrierError> {
    if spec.topics.is_empty() {
        return Err(BarrierError::InvalidDefinition(
            "a barrier group needs at least one topic".to_owned(),
        ));
    }
    if spec.topics.iter().any(String::is_empty) {
        return Err(BarrierError::InvalidDefinition(
            "a barrier group topic name is empty".to_owned(),
        ));
    }
    if let Some(topic) = spec.topics.iter().find(|t| t.len() > MAX_STRING_BYTES) {
        return Err(BarrierError::InvalidDefinition(format!(
            "a barrier group topic name is {} bytes, and the limit is {MAX_STRING_BYTES}",
            topic.len()
        )));
    }
    let unique: HashSet<&String> = spec.topics.iter().collect();
    if unique.len() != spec.topics.len() {
        return Err(BarrierError::InvalidDefinition(
            "a barrier group names one topic twice".to_owned(),
        ));
    }
    if spec.retained_cuts < 1 {
        return Err(BarrierError::InvalidDefinition(format!(
            "retained_cuts is {}, and it must be one or more",
            spec.retained_cuts
        )));
    }
    if spec.interval.is_some_and(|i| i.millis_i64() <= 0) {
        return Err(BarrierError::InvalidDefinition(
            "the injection interval must be one millisecond or more".to_owned(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_spec_limits(
    spec: &GroupSpec,
    max_topics: usize,
    max_retained_cuts: i32,
    min_interval: Time,
) -> Result<(), BarrierError> {
    validate_spec(spec)?;
    if spec.topics.len() > max_topics {
        return Err(BarrierError::InvalidDefinition(format!(
            "a barrier group may have at most {max_topics} topics, got {}",
            spec.topics.len()
        )));
    }
    if spec.retained_cuts > max_retained_cuts {
        return Err(BarrierError::InvalidDefinition(format!(
            "retained_cuts is {}, exceeding maximum {max_retained_cuts}",
            spec.retained_cuts
        )));
    }
    if let Some(interval) = spec.interval
        && interval.millis_i64() < min_interval.millis_i64()
    {
        return Err(BarrierError::InvalidDefinition(format!(
            "injection interval {interval:?} is below minimum {min_interval:?}"
        )));
    }
    Ok(())
}

/// Per-broker barrier coordinator.
///
/// `Broker::start` builds it and shares it with the barrier wire handlers and
/// the scheduler through an `Arc`.
pub(crate) struct BarrierCoordinator {
    pub(crate) node_id: NodeId,
    partitions: Arc<PartitionRegistry>,
    controller: Arc<dyn MetadataSource>,
    /// Creates `__barrier_state` when the first group needs it.
    auto_topic_creation: Arc<AutoTopicCreation>,
    config: BarrierConfig,
    metrics: Arc<BrokerBarrierMetrics>,
    remote: Option<Arc<dyn RemoteMarkerWriter>>,
    /// Live groups: name to locked entry.
    groups: DashMap<String, Arc<Mutex<GroupEntry>>>,
    /// The `__barrier_state` partition indices this broker leads.
    leader_partitions: RwLock<HashSet<PartitionIndex>>,
}

impl BarrierCoordinator {
    pub(crate) fn new(
        node_id: NodeId,
        partitions: Arc<PartitionRegistry>,
        controller: Arc<dyn MetadataSource>,
        auto_topic_creation: Arc<AutoTopicCreation>,
        config: BarrierConfig,
        metrics: Arc<BrokerBarrierMetrics>,
    ) -> Self {
        Self {
            node_id,
            partitions,
            controller,
            auto_topic_creation,
            config,
            metrics,
            remote: None,
            groups: DashMap::new(),
            leader_partitions: RwLock::new(HashSet::new()),
        }
    }

    /// Bind the leg of the fan-out that leaves this broker.
    ///
    /// A coordinator with no transport marks only the partitions it leads, and
    /// every remote partition lands in the `missing` list of the cut.
    pub(crate) fn configure_marker_transport(&mut self, remote: Arc<dyn RemoteMarkerWriter>) {
        self.remote = Some(remote);
    }

    /// How often the scheduler should call [`Self::run_due_injections`].
    #[must_use]
    pub(crate) fn scheduler_tick(&self) -> Time {
        self.config.scheduler_tick
    }

    /// The `__barrier_state` partition that carries `group`.
    #[must_use]
    pub(crate) fn state_partition_for(&self, group: &str) -> PartitionIndex {
        PartitionIndex(partition_for_group(
            group,
            self.config.state_topic_num_partitions,
        ))
    }

    /// Recompute which `__barrier_state` partitions this broker leads.
    ///
    /// The replicator supervisor calls this on every metadata change, beside
    /// the same call for the transaction and share coordinators.
    pub(crate) async fn refresh_leader_partitions(&self, image: &MetadataImage) {
        let mut set = HashSet::new();
        for p in image.partitions_of(STATE_TOPIC) {
            if p.leader == self.node_id {
                set.insert(PartitionIndex(p.partition));
            }
        }
        *self.leader_partitions.write().await = set;
    }

    /// Whether this broker coordinates `group` now.
    pub(crate) async fn is_coordinator_for(&self, group: &str) -> bool {
        let partition = self.state_partition_for(group);
        self.leader_partitions.read().await.contains(&partition)
    }

    /// The coordinator epoch of `group`, which is the leader epoch of its
    /// state partition.
    ///
    /// It fences a coordinator that lost and regained the partition, the same
    /// way the transaction coordinator fences a marker dispatch.
    #[must_use]
    pub(crate) fn coordinator_epoch(&self, group: &str, image: &MetadataImage) -> Option<i32> {
        image
            .partition(STATE_TOPIC, self.state_partition_for(group).get())
            .map(|p| p.leader_epoch.get())
    }

    /// Append one batch of `__barrier_state` records for `group`.
    ///
    /// Every record of a group lands on one partition, so the group's epochs
    /// hold a total order. A `None` value writes a tombstone.
    ///
    /// # Errors
    /// Returns [`BarrierError::StateNotLocal`] when the state partition is not
    /// open here, and [`BarrierError::Persist`] when a key does not encode or
    /// the append fails.
    async fn append_records(
        &self,
        group: &str,
        records: Vec<(RecordKey, Option<Bytes>)>,
    ) -> Result<Offset, BarrierError> {
        let index = self.state_partition_for(group);
        let partition = self
            .partitions
            .get(STATE_TOPIC, index)
            .ok_or(BarrierError::StateNotLocal { partition: index })?;

        let last = i32::try_from(records.len())
            .map_err(|_| {
                BarrierError::InvalidDefinition("too many records for one append".to_owned())
            })?
            .saturating_sub(1);
        let mut batch = RecordBatch {
            last_offset_delta: last,
            records: Vec::with_capacity(records.len()),
            ..RecordBatch::default()
        };
        for (delta, (key, value)) in records.into_iter().enumerate() {
            batch.records.push(Record {
                offset_delta: i32::try_from(delta).expect("the record count fits in an i32"),
                key: Some(encode_key(&key)?.into()),
                value,
                ..Record::default()
            });
        }
        Ok(partition.produce_batch(batch).await?)
    }

    /// The entry of `group`, or a fresh one.
    fn entry_handle(&self, group: &str) -> Arc<Mutex<GroupEntry>> {
        self.groups
            .entry(group.to_owned())
            .or_default()
            .value()
            .clone()
    }

    /// The entry of a group that this broker holds.
    ///
    /// # Errors
    /// Returns [`BarrierError::UnknownGroup`] when the name is not live here.
    fn live_entry(&self, group: &str) -> Result<Arc<Mutex<GroupEntry>>, BarrierError> {
        self.groups
            .get(group)
            .map(|e| e.value().clone())
            .ok_or_else(|| BarrierError::UnknownGroup {
                group: group.to_owned(),
            })
    }

    /// # Errors
    /// Returns [`BarrierError::NotCoordinator`] when another broker leads the
    /// state partition of `group`.
    async fn require_coordinator(&self, group: &str) -> Result<(), BarrierError> {
        if self.is_coordinator_for(group).await {
            Ok(())
        } else {
            Err(BarrierError::NotCoordinator {
                group: group.to_owned(),
            })
        }
    }

    fn report_group_count(&self) {
        self.metrics.groups_coordinated(self.groups.len());
    }
}

#[cfg(test)]
mod tests {
    use assert2::{assert, check};
    use krabka_units::millis;

    use super::*;
    use crate::barrier::{
        coordinator::test_support::{Fixture, GROUP, GroupSpecSetup, RetainedCutCount, spec},
        persistence::{RecordKind, decode_key},
    };

    /// The configured timeout is a ceiling, not a default a caller can raise.
    /// A request that asks for longer would otherwise hold the group's lock
    /// past what the operator allows.
    #[test]
    fn a_requested_timeout_is_clamped_to_the_configured_ceiling() {
        let ceiling = krabka_units::secs(30);
        let cases = [
            ("no opinion takes the ceiling", None, ceiling),
            (
                "under the ceiling is honoured",
                Some(krabka_units::secs(5)),
                krabka_units::secs(5),
            ),
            (
                "over the ceiling is clamped",
                Some(krabka_units::secs(600)),
                ceiling,
            ),
            ("exactly the ceiling", Some(ceiling), ceiling),
        ];
        for (case, requested, expected) in cases {
            assert!(clamp_timeout(requested, ceiling) == expected, "{case}");
        }
    }

    #[test]
    fn a_definition_must_name_at_least_one_usable_topic() {
        let cases: &[(&str, GroupSpec, bool)] = &[
            (
                "good",
                spec(GroupSpecSetup {
                    interval: Some(millis(1_000)),
                    ..Default::default()
                }),
                true,
            ),
            (
                "no interval",
                spec(GroupSpecSetup {
                    retained_cuts: RetainedCutCount(1),
                    ..Default::default()
                }),
                true,
            ),
            (
                "no topic",
                spec(GroupSpecSetup {
                    topics: &[],
                    ..Default::default()
                }),
                false,
            ),
            (
                "empty topic name",
                spec(GroupSpecSetup {
                    topics: &["orders", ""],
                    ..Default::default()
                }),
                false,
            ),
            (
                "duplicate topic",
                spec(GroupSpecSetup {
                    topics: &["orders", "orders"],
                    ..Default::default()
                }),
                false,
            ),
            (
                "no retention",
                spec(GroupSpecSetup {
                    retained_cuts: RetainedCutCount(0),
                    ..Default::default()
                }),
                false,
            ),
            (
                "negative retention",
                spec(GroupSpecSetup {
                    retained_cuts: RetainedCutCount(-1),
                    ..Default::default()
                }),
                false,
            ),
            (
                "zero interval",
                spec(GroupSpecSetup {
                    interval: Some(Time::ZERO),
                    ..Default::default()
                }),
                false,
            ),
        ];
        for (case, spec, ok) in cases {
            check!(validate_spec(spec).is_ok() == *ok, "{case}");
        }
    }

    /// A group name and a topic name go into `i16`-length strings of the
    /// state records, so 32767 bytes is the longest that a definition can use.
    #[test]
    fn a_group_or_topic_name_is_usable_up_to_32767_bytes() {
        for (length, ok) in [(MAX_STRING_BYTES, true), (MAX_STRING_BYTES + 1, false)] {
            let long = "n".repeat(length);
            check!(
                validate_group_name(&long).is_ok() == ok,
                "group of {length} bytes"
            );
            check!(
                validate_spec(&spec(GroupSpecSetup {
                    topics: &["orders", long.as_str()],
                    ..Default::default()
                }))
                .is_ok()
                    == ok,
                "topic of {length} bytes"
            );
        }
    }

    #[test]
    fn validate_spec_limits_enforces_bounds() {
        let max_topics = 3;
        let max_retained_cuts = 10;
        let min_interval = krabka_units::secs(1);

        // Good spec
        let good = spec(GroupSpecSetup {
            topics: &["t1", "t2"],
            interval: Some(krabka_units::secs(2)),
            retained_cuts: RetainedCutCount(5),
        });
        assert!(validate_spec_limits(&good, max_topics, max_retained_cuts, min_interval).is_ok());

        // Too many topics
        let too_many_topics = spec(GroupSpecSetup {
            topics: &["t1", "t2", "t3", "t4"],
            interval: Some(krabka_units::secs(2)),
            retained_cuts: RetainedCutCount(5),
        });
        assert!(
            validate_spec_limits(
                &too_many_topics,
                max_topics,
                max_retained_cuts,
                min_interval
            )
            .is_err()
        );

        // Too many cuts
        let too_many_cuts = spec(GroupSpecSetup {
            topics: &["t1"],
            interval: Some(krabka_units::secs(2)),
            retained_cuts: RetainedCutCount(15),
        });
        assert!(
            validate_spec_limits(&too_many_cuts, max_topics, max_retained_cuts, min_interval)
                .is_err()
        );

        // Interval below minimum
        let interval_too_short = spec(GroupSpecSetup {
            topics: &["t1"],
            interval: Some(krabka_units::millis(500)),
            retained_cuts: RetainedCutCount(5),
        });
        assert!(
            validate_spec_limits(
                &interval_too_short,
                max_topics,
                max_retained_cuts,
                min_interval
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn the_state_partition_of_a_group_carries_every_record_of_that_group() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        coordinator
            .create_group(GROUP, spec(GroupSpecSetup::default()))
            .await
            .expect("the group is created");
        coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");

        let index = coordinator.state_partition_for(GROUP);
        let partition = fixture
            .registry
            .get(STATE_TOPIC, index)
            .expect("the state partition is open");
        let read = partition
            .read_log(Offset(0), krabka_units::mebibytes(1))
            .expect("read the log back");
        let mut kinds = Vec::new();
        for batch in &read.batches {
            for record in &batch.records {
                let key = decode_key(record.key.as_ref().expect("every record has a key"))
                    .expect("the key decodes");
                check!(key.group == GROUP);
                kinds.push(key.kind);
            }
        }
        // Group, injection start, cut, group again, then the retirement of the
        // injection-start record.
        assert!(
            kinds
                == vec![
                    RecordKind::Group,
                    RecordKind::InjectionStart,
                    RecordKind::Cut,
                    RecordKind::Group,
                    RecordKind::InjectionStart,
                ]
        );
    }
}
