//! The group lifecycle of the barrier coordinator.
//!
//! The module holds the edits that change what a group is, the two reads that
//! report one, and the lazy creation of `__barrier_state` behind them. Each of
//! them turns on the group record, and none of them runs the injection
//! protocol, so they sit in their own file.

use std::{sync::Arc, time::Instant};

use krabka_units::{Time, convert::TimeExt as _};
use tokio::sync::Mutex;

use super::{BarrierCoordinator, validate_group_name, validate_spec_limits};
use crate::{
    barrier::{
        STATE_TOPIC,
        error::BarrierError,
        persistence::{CutValue, GroupValue, RecordKey, encode_group},
        state::{GroupEntry, GroupSpec, schedule_next},
    },
    time_util::now_ms,
};

/// How long `create_group` waits for the state topic's partition to take a
/// leader before it gives up. Creation and leader assignment are two separate
/// metadata rounds, and the caller cannot act until the second one lands.
const STATE_TOPIC_READY_TIMEOUT: Time = krabka_units::secs(10);

/// How often that wait re-reads the metadata image.
const STATE_TOPIC_READY_POLL: Time = krabka_units::millis(20);

/// What one published cut says about its group.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RetainedCut {
    pub(crate) epoch: i64,
    pub(crate) cut: CutValue,
}

/// What `DescribeBarrierGroups` reports for one group.
///
/// The type is [`PartialEq`] but not [`Eq`], because [`GroupValue`] carries a
/// [`Time`].
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct GroupDescription {
    pub(crate) group: String,
    pub(crate) definition: GroupValue,
    /// The epochs of the cuts the group retains, in ascending order.
    pub(crate) cut_epochs: Vec<i64>,
    /// The epoch of an injection that published no cut yet.
    pub(crate) pending_epoch: Option<i64>,
}

impl BarrierCoordinator {
    /// Ask for `__barrier_state` if it does not exist, and wait until the
    /// partition `group` hashes to has this broker as its leader.
    ///
    /// The broker creates the topic in the background, with its configured
    /// partition count and replication factor
    /// ([`crate::auto_topic_creation::AutoTopicCreation`]). The call returns
    /// at once when the topic is already there. The wait is what makes lazy
    /// creation safe: leadership is assigned after the topic record lands,
    /// and `is_coordinator_for` reads the leader set, so a caller that raced
    /// the assignment would be told it is not the coordinator for a group it
    /// just asked to create.
    async fn ensure_state_topic(&self, group: &str) -> Result<(), BarrierError> {
        if self.controller.current_image().topic(STATE_TOPIC).is_none() {
            self.auto_topic_creation.request(STATE_TOPIC);
        }

        let partition = self.state_partition_for(group);
        let deadline = Instant::now() + STATE_TOPIC_READY_TIMEOUT.to_std();
        loop {
            let image = self.controller.current_image();
            let led = image
                .partition(STATE_TOPIC, partition.get())
                .is_some_and(|p| p.leader == self.node_id);
            // Metadata leadership lands one round before the log is open here,
            // and the coordinator writes through the partition, so both have
            // to be true before the caller can be told it is the coordinator.
            let open = self.partitions.get(STATE_TOPIC, partition).is_some();
            if led && open {
                self.refresh_leader_partitions(&image).await;
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(BarrierError::NotCoordinator {
                    group: group.to_owned(),
                });
            }
            tokio::time::sleep(STATE_TOPIC_READY_POLL.to_std()).await;
        }
    }

    /// Create a group.
    ///
    /// # Errors
    /// Returns [`BarrierError::NotCoordinator`] when another broker owns the
    /// group, [`BarrierError::GroupExists`] when the name is live,
    /// [`BarrierError::InvalidDefinition`] when the name or the definition is
    /// not usable, and [`BarrierError::Persist`] when the append fails.
    pub(crate) async fn create_group(
        &self,
        group: &str,
        spec: GroupSpec,
    ) -> Result<GroupValue, BarrierError> {
        // Both checks run before the group has an entry, so a refusal leaves
        // nothing behind.
        validate_group_name(group)?;
        validate_spec_limits(
            &spec,
            self.config.max_topics_per_group,
            self.config.max_retained_cuts,
            self.config.min_injection_interval,
        )?;
        // The first group is what brings __barrier_state into being. The topic
        // has to exist before require_coordinator can decide anything, because
        // that decision is the leadership of the partition the group hashes
        // to. A broker that loses the race finds the topic already there.
        self.ensure_state_topic(group).await?;
        self.require_coordinator(group).await?;
        let handle = self.entry_handle(group);
        let mut entry = handle.lock().await;
        if entry.is_defined() {
            return Err(BarrierError::GroupExists {
                group: group.to_owned(),
            });
        }
        if self.groups.len() > self.config.max_groups {
            return Err(BarrierError::InvalidDefinition(format!(
                "barrier groups limit reached ({})",
                self.config.max_groups
            )));
        }

        let definition = self
            .persist_group_definition(group, &mut entry, spec)
            .await?;
        drop(entry);
        self.report_group_count();
        Ok(definition)
    }

    /// Replace the definition of a live group.
    ///
    /// The new topic set and the new partition counts apply from the next
    /// epoch, because an injection freezes its target set before it appends
    /// any marker.
    ///
    /// # Errors
    /// Returns [`BarrierError::NotCoordinator`] when another broker owns the
    /// group, [`BarrierError::UnknownGroup`] when no group of that name is
    /// live, [`BarrierError::InvalidDefinition`] when the definition is not
    /// usable, and [`BarrierError::Persist`] when the append fails.
    pub(crate) async fn update_group(
        &self,
        group: &str,
        spec: GroupSpec,
    ) -> Result<GroupValue, BarrierError> {
        validate_spec_limits(
            &spec,
            self.config.max_topics_per_group,
            self.config.max_retained_cuts,
            self.config.min_injection_interval,
        )?;
        let mut entry = self.lock_defined_group(group).await?;

        self.persist_group_definition(group, &mut entry, spec).await
    }

    /// Keep the definition check under the same group lock as the ensuing mutation.
    async fn lock_defined_group(
        &self,
        group: &str,
    ) -> Result<tokio::sync::OwnedMutexGuard<GroupEntry>, BarrierError> {
        self.require_coordinator(group).await?;
        let entry = self.live_entry(group)?.lock_owned().await;
        if !entry.is_defined() {
            return Err(BarrierError::UnknownGroup {
                group: group.to_owned(),
            });
        }
        Ok(entry)
    }

    /// Persist the replacement before publishing it to the locked group entry.
    async fn persist_group_definition(
        &self,
        group: &str,
        entry: &mut GroupEntry,
        spec: GroupSpec,
    ) -> Result<GroupValue, BarrierError> {
        let definition = GroupValue {
            topics: spec.topics,
            interval: spec.interval,
            retained_cuts: spec.retained_cuts,
            last_epoch: entry.last_epoch(),
        };
        self.append_records(
            group,
            vec![(
                RecordKey::group(group),
                Some(encode_group(&definition)?.into()),
            )],
        )
        .await?;

        entry.definition = definition.clone();
        schedule_next(entry, now_ms());
        Ok(definition)
    }

    /// Delete a group and every record it owns.
    ///
    /// The coordinator tombstones the group record, the cut record of every
    /// retained epoch, and the injection-start record of an injection that
    /// published no cut. No record of the group then survives compaction.
    ///
    /// # Errors
    /// Returns [`BarrierError::NotCoordinator`] when another broker owns the
    /// group, [`BarrierError::UnknownGroup`] when no group of that name is
    /// live, and [`BarrierError::Persist`] when the append fails.
    pub(crate) async fn delete_group(&self, group: &str) -> Result<(), BarrierError> {
        let mut entry = self.lock_defined_group(group).await?;

        let mut records = vec![(RecordKey::group(group), None)];
        for epoch in entry.cuts.keys().copied() {
            records.push((RecordKey::cut(group, epoch), None));
        }
        if let Some(pending) = &entry.pending {
            records.push((RecordKey::injection_start(group, pending.epoch), None));
        }
        self.append_records(group, records).await?;

        entry.cuts.clear();
        entry.pending = None;
        drop(entry);
        self.groups.remove(group);
        self.report_group_count();
        Ok(())
    }

    /// Describe the named groups, or every group this broker holds when
    /// `names` is empty.
    pub(crate) async fn describe_groups(&self, names: &[String]) -> Vec<GroupDescription> {
        let selected: Vec<(String, Arc<Mutex<GroupEntry>>)> = if names.is_empty() {
            self.groups
                .iter()
                .map(|e| (e.key().clone(), e.value().clone()))
                .collect()
        } else {
            names
                .iter()
                .filter_map(|name| {
                    self.groups
                        .get(name)
                        .map(|e| (name.clone(), e.value().clone()))
                })
                .collect()
        };

        let mut out = Vec::with_capacity(selected.len());
        for (group, handle) in selected {
            let entry = handle.lock().await;
            if !entry.is_defined() {
                continue;
            }
            out.push(GroupDescription {
                group,
                definition: entry.definition.clone(),
                cut_epochs: entry.cuts.keys().copied().collect(),
                pending_epoch: entry.pending.as_ref().map(|p| p.epoch),
            });
        }
        out.sort_by(|a, b| a.group.cmp(&b.group));
        out
    }

    /// The cuts that `group` retains, newest last.
    ///
    /// # Errors
    /// Returns [`BarrierError::UnknownGroup`] when no group of that name is
    /// live on this broker.
    pub(crate) async fn list_cuts(&self, group: &str) -> Result<Vec<RetainedCut>, BarrierError> {
        let handle = self.live_entry(group)?;
        let entry = handle.lock().await;
        if !entry.is_defined() {
            return Err(BarrierError::UnknownGroup {
                group: group.to_owned(),
            });
        }
        Ok(entry
            .cuts
            .iter()
            .map(|(epoch, cut)| RetainedCut {
                epoch: *epoch,
                cut: cut.clone(),
            })
            .collect())
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;
    use krabka_metadata::NodeId;

    use super::*;
    use crate::{
        barrier::{
            coordinator::test_support::{Fixture, GROUP, GroupSpecSetup, spec},
            test_support::topic_records,
        },
        coordinator::unified::persistence::MAX_STRING_BYTES,
        metadata_source::MetadataSource,
    };

    #[tokio::test]
    async fn a_created_group_reads_back_through_describe() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        let definition = coordinator
            .create_group(
                GROUP,
                spec(GroupSpecSetup {
                    topics: &["orders", "payments"],
                    ..Default::default()
                }),
            )
            .await
            .expect("the group is created");

        let expected = GroupValue {
            topics: vec!["orders".to_owned(), "payments".to_owned()],
            interval: None,
            retained_cuts: 4,
            last_epoch: 0,
        };
        assert!(definition == expected);
        assert!(
            coordinator.describe_groups(&[]).await
                == vec![GroupDescription {
                    group: GROUP.to_owned(),
                    definition: expected,
                    cut_epochs: Vec::new(),
                    pending_epoch: None,
                }]
        );
    }

    /// The longest name that a state record can carry is 32767 bytes. A group
    /// of that name and a topic of that length are written, replayed and cut,
    /// and each string goes through a key, a value and a marker.
    #[tokio::test]
    async fn a_group_and_a_topic_of_32767_bytes_are_created_cut_and_recovered() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        let group = "g".repeat(MAX_STRING_BYTES);
        let topic = "t".repeat(MAX_STRING_BYTES);
        let definition = coordinator
            .create_group(
                &group,
                spec(GroupSpecSetup {
                    topics: &["orders", &topic],
                    ..Default::default()
                }),
            )
            .await
            .expect("the group is created");
        let outcome = coordinator
            .trigger_injection(&group, None)
            .await
            .expect("the injection runs");

        let replayed = fixture.recovered().await;
        assert!(
            replayed.describe_groups(&[]).await
                == vec![GroupDescription {
                    group,
                    definition: GroupValue {
                        last_epoch: outcome.epoch,
                        ..definition
                    },
                    cut_epochs: vec![outcome.epoch],
                    pending_epoch: None,
                }]
        );
    }

    /// A name over 32767 bytes is refused before the group has an entry, and
    /// nothing reaches a state partition. Each of the two strings would
    /// otherwise panic the encoder of the record that names it.
    #[tokio::test]
    async fn a_group_or_topic_of_32768_bytes_is_refused_and_leaves_nothing_behind() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        let long = "n".repeat(MAX_STRING_BYTES + 1);
        coordinator
            .create_group(GROUP, spec(GroupSpecSetup::default()))
            .await
            .expect("the group is created");
        let before = coordinator.groups.len();

        let refused = [
            (
                "group name on create",
                coordinator
                    .create_group(&long, spec(GroupSpecSetup::default()))
                    .await,
            ),
            (
                "topic name on create",
                coordinator
                    .create_group(
                        "other",
                        spec(GroupSpecSetup {
                            topics: &["orders", &long],
                            ..Default::default()
                        }),
                    )
                    .await,
            ),
            (
                "topic name on update",
                coordinator
                    .update_group(
                        GROUP,
                        spec(GroupSpecSetup {
                            topics: &[&long],
                            ..Default::default()
                        }),
                    )
                    .await,
            ),
        ];
        for (case, result) in refused {
            assert!(let Err(BarrierError::InvalidDefinition(_)) = result, "{case}");
        }

        assert!(coordinator.groups.len() == before);
        assert!(
            coordinator
                .describe_groups(&[])
                .await
                .iter()
                .map(|d| (d.group.as_str(), d.definition.topics.clone()))
                .collect::<Vec<_>>()
                == vec![(GROUP, vec!["orders".to_owned()])]
        );
    }

    #[tokio::test]
    async fn a_second_create_of_the_same_name_is_refused() {
        let fixture = Fixture::new();
        let coordinator = fixture
            .coordinator_with_group(GROUP, spec(GroupSpecSetup::default()))
            .await;
        let again = coordinator
            .create_group(
                GROUP,
                spec(GroupSpecSetup {
                    topics: &["payments"],
                    ..Default::default()
                }),
            )
            .await;
        assert!(let Err(BarrierError::GroupExists { .. }) = again);
    }

    #[tokio::test]
    async fn a_group_that_this_broker_does_not_coordinate_is_refused() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        // Node 2 takes every state partition.
        fixture.source.set_records(
            &[
                topic_records(STATE_TOPIC, 4, NodeId(2)),
                topic_records("orders", 2, NodeId(1)),
            ]
            .concat(),
        );
        coordinator
            .refresh_leader_partitions(&fixture.source.current_image())
            .await;

        let created = coordinator
            .create_group(GROUP, spec(GroupSpecSetup::default()))
            .await;
        assert!(let Err(BarrierError::NotCoordinator { .. }) = created);
    }

    #[tokio::test]
    async fn a_deleted_group_leaves_no_state_behind() {
        let fixture = Fixture::new();
        let coordinator = fixture
            .coordinator_with_group(GROUP, spec(GroupSpecSetup::default()))
            .await;
        coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");

        coordinator
            .delete_group(GROUP)
            .await
            .expect("the group is deleted");
        assert!(coordinator.describe_groups(&[]).await.is_empty());
        assert!(let Err(BarrierError::UnknownGroup { .. }) = coordinator.list_cuts(GROUP).await);

        let replayed = fixture.recovered().await;
        assert!(replayed.describe_groups(&[]).await.is_empty());
    }

    #[tokio::test]
    async fn deleting_a_group_that_is_not_there_is_refused() {
        let fixture = Fixture::new();
        let coordinator = fixture.coordinator().await;
        assert!(let Err(BarrierError::UnknownGroup { .. }) = coordinator.delete_group(GROUP).await);
        assert!(
            let Err(BarrierError::UnknownGroup { .. }) =
                coordinator.trigger_injection(GROUP, None).await
        );
    }

    #[tokio::test]
    async fn creating_beyond_max_groups_is_refused() {
        let fixture = Fixture::new();
        let mut coordinator = fixture.coordinator().await;
        // Limit max_groups to 1
        coordinator.config.max_groups = 1;
        coordinator
            .create_group("g1", spec(GroupSpecSetup::default()))
            .await
            .expect("first group created");
        let second = coordinator
            .create_group("g2", spec(GroupSpecSetup::default()))
            .await;
        assert!(let Err(BarrierError::InvalidDefinition(msg)) = second);
        assert!(msg.contains("barrier groups limit reached"));
    }
}
