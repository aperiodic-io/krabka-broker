//! Recovery of the barrier coordinator from `__barrier_state`.
//!
//! The module replays every locally-led state partition into group entries,
//! decodes the records it reads, and closes an injection that a crash left
//! open. It runs once at startup, and nothing on the request path calls it,
//! so it is separate from the group edits and the injection protocol.

use std::{collections::BTreeMap, sync::Arc};

use krabka_ids::PartitionIndex;
use krabka_log::Offset;
use krabka_metadata::MetadataImage;
use krabka_protocol::records::Record;
use krabka_verified::{
    BarrierRecoveryFinalizeDecision, ReplayCursorDecision, barrier_recovery_finalize_decision,
    replay_batch_cursor_decision,
};
use tokio::sync::Mutex;
use tracing::{info, warn};

use super::BarrierCoordinator;
use crate::{
    barrier::{
        STATE_TOPIC,
        error::BarrierError,
        persistence::{
            CutStatus, RecordKind, decode_cut, decode_group, decode_injection_start, decode_key,
        },
        state::{GroupEntry, StateRecord, apply_record, build_cut, schedule_next},
    },
    time_util::now_ms,
};

impl BarrierCoordinator {
    /// Replay every locally-led `__barrier_state` partition, and finalise an
    /// injection that a crash left open.
    ///
    /// # Errors
    /// Returns [`BarrierError::Persist`] when the append of a recovery cut
    /// fails. A per-partition read error skips that partition, as if it holds
    /// nothing to replay.
    pub(crate) async fn recover(&self, image: &MetadataImage) -> Result<(), BarrierError> {
        self.refresh_leader_partitions(image).await;
        let replayed = self.replay_led_partitions().await;

        self.groups.clear();
        let now = now_ms();
        for (group, mut entry) in replayed {
            if !entry.is_defined() {
                warn!(
                    group,
                    "no group record defines this barrier state; dropping it"
                );
                continue;
            }
            schedule_next(&mut entry, now);
            self.groups.insert(group, Arc::new(Mutex::new(entry)));
        }

        self.finalize_open_injections(image).await?;
        self.report_group_count();
        info!(
            groups = self.groups.len(),
            "BarrierCoordinator recovery complete"
        );
        Ok(())
    }

    /// Publish a partial cut for every injection that started and published no
    /// cut.
    ///
    /// A coordinator that crashed between the injection-start record and the
    /// cut record left markers that nothing can withdraw. The partial cut is
    /// what accounts for them, and it consumes the epoch for good. The
    /// coordinator did not observe the offsets, so the cut names every frozen
    /// target as missing.
    async fn finalize_open_injections(&self, image: &MetadataImage) -> Result<(), BarrierError> {
        let open: Vec<(String, Arc<Mutex<GroupEntry>>)> = self
            .groups
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect();

        for (group, handle) in open {
            let mut entry = handle.lock().await;
            let pending = entry.pending.clone();
            let frozen_coordinator_epoch = pending
                .as_ref()
                .map_or(-1, |pending| pending.start.coordinator_epoch);
            let targets_valid = pending.as_ref().is_some_and(|pending| {
                !pending.start.targets.is_empty()
                    && pending
                        .start
                        .targets
                        .iter()
                        .all(|target| !target.topic.is_empty() && target.partition_count > 0)
            });
            let decision = barrier_recovery_finalize_decision(
                pending.is_some(),
                self.coordinator_epoch(&group, image),
                frozen_coordinator_epoch,
                targets_valid,
            );
            let pending = match decision {
                BarrierRecoveryFinalizeDecision::NoPending
                | BarrierRecoveryFinalizeDecision::UnknownCoordinator => continue,
                BarrierRecoveryFinalizeDecision::MalformedPending => {
                    warn!(
                        group,
                        epoch = pending.as_ref().map(|pending| pending.epoch),
                        "malformed interrupted barrier injection remains fenced open"
                    );
                    continue;
                }
                BarrierRecoveryFinalizeDecision::FencedCoordinator => {
                    warn!(
                        group,
                        epoch = pending.as_ref().map(|pending| pending.epoch),
                        frozen_at = frozen_coordinator_epoch,
                        "the current coordinator epoch is stale; leaving the injection open"
                    );
                    continue;
                }
                BarrierRecoveryFinalizeDecision::FinalizePartial => pending
                    .expect("the verified finalization decision requires a pending injection"),
            };

            let completed_at = now_ms();
            let cut = build_cut(
                pending.start.triggered_at,
                completed_at,
                &pending.start.targets,
                &BTreeMap::new(),
            );
            if cut.status != CutStatus::Partial {
                warn!(
                    group,
                    epoch = pending.epoch,
                    "recovery refused to publish an interrupted injection as complete"
                );
                continue;
            }
            warn!(
                group,
                epoch = pending.epoch,
                missing = cut.missing.len(),
                "finalising an interrupted barrier injection as partial"
            );
            self.publish_cut(&group, &mut entry, pending.epoch, cut)
                .await?;
        }
        Ok(())
    }

    /// Replay every `__barrier_state` partition this broker leads.
    async fn replay_led_partitions(&self) -> BTreeMap<String, GroupEntry> {
        let led: Vec<PartitionIndex> = self
            .leader_partitions
            .read()
            .await
            .iter()
            .copied()
            .collect();

        let mut state = BTreeMap::new();
        for index in led {
            let Some(partition) = self.partitions.get(STATE_TOPIC, index) else {
                continue;
            };
            let mut offset = partition.log_start_offset();
            let end = partition.log_end_offset();
            'partition_replay: while offset < end {
                let read = match partition.read_log(offset, self.config.recovery_read_max) {
                    Ok(read) => read,
                    Err(error) => {
                        warn!(
                            partition = index.get(),
                            %error,
                            "read error during __barrier_state recovery; skipping partition"
                        );
                        break;
                    }
                };
                if read.batches.is_empty() {
                    break;
                }
                for batch in &read.batches {
                    let ReplayCursorDecision::Advance(batch_end) = replay_batch_cursor_decision(
                        offset.0,
                        end.0,
                        Some((batch.base_offset, batch.last_offset_delta)),
                    ) else {
                        warn!(
                            partition = index.get(),
                            base_offset = batch.base_offset,
                            "malformed __barrier_state replay batch; stopping partition replay"
                        );
                        break 'partition_replay;
                    };
                    for record in &batch.records {
                        if let Some(decoded) = decode_state_record(index, record) {
                            apply_record(&mut state, decoded);
                        }
                    }
                    offset = Offset(batch_end);
                }
            }
        }
        state
    }
}

/// Decode one replayed `__barrier_state` record.
///
/// The function returns `None` for a record with no key, and for a record that
/// carries a key or a value this broker cannot decode.
fn decode_state_record(partition: PartitionIndex, record: &Record) -> Option<StateRecord> {
    let key_bytes = record.key.as_ref()?;
    let key = match decode_key(key_bytes) {
        Ok(key) => key,
        Err(error) => {
            warn!(
                partition = partition.get(),
                %error,
                "invalid __barrier_state key; skipping record"
            );
            return None;
        }
    };
    let value = record.value.as_deref();

    match key.kind {
        RecordKind::Group => Some(StateRecord::Group {
            group: key.group,
            value: match value {
                None => None,
                Some(bytes) => Some(keep_decoded(partition, decode_group(bytes))?),
            },
        }),
        RecordKind::InjectionStart => Some(StateRecord::InjectionStart {
            group: key.group,
            epoch: key.epoch,
            value: match value {
                None => None,
                Some(bytes) => Some(keep_decoded(partition, decode_injection_start(bytes))?),
            },
        }),
        RecordKind::Cut => Some(StateRecord::Cut {
            group: key.group,
            epoch: key.epoch,
            value: match value {
                None => None,
                Some(bytes) => Some(keep_decoded(partition, decode_cut(bytes))?),
            },
        }),
    }
}

/// Keep a decoded value, and drop one that does not decode.
///
/// A record whose value is present but malformed is not a tombstone, so the
/// caller skips it rather than deleting what the key names.
fn keep_decoded<T>(
    partition: PartitionIndex,
    decoded: Result<T, krabka_protocol::ProtocolError>,
) -> Option<T> {
    match decoded {
        Ok(value) => Some(value),
        Err(error) => {
            warn!(
                partition = partition.get(),
                %error,
                "invalid __barrier_state value; skipping record"
            );
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use assert2::assert;
    use krabka_ids::PartitionIndex;
    use tokio::sync::Mutex;

    use crate::{
        barrier::{
            STATE_TOPIC,
            coordinator::{
                GroupDescription, RetainedCut,
                test_support::{Fixture, GROUP, GroupSpecSetup, RetainedCutCount, spec},
            },
            error::BarrierError,
            persistence::{
                CutStatus, GroupValue, InjectionStartValue, MissingPartition, RecordKey,
                encode_injection_start,
            },
        },
        metadata_source::MetadataSource,
    };

    #[tokio::test]
    async fn recovery_rebuilds_the_group_and_its_cuts() {
        let fixture = Fixture::new();
        let coordinator = fixture
            .coordinator_with_group(
                GROUP,
                spec(GroupSpecSetup {
                    topics: &["orders", "payments"],
                    retained_cuts: RetainedCutCount(8),
                    ..Default::default()
                }),
            )
            .await;
        let first = coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
        let second = coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");

        let replayed = fixture.recovered().await;
        assert!(
            replayed.describe_groups(&[]).await
                == vec![GroupDescription {
                    group: GROUP.to_owned(),
                    definition: GroupValue {
                        topics: vec!["orders".to_owned(), "payments".to_owned()],
                        interval: None,
                        retained_cuts: 8,
                        last_epoch: 2,
                    },
                    cut_epochs: vec![1, 2],
                    pending_epoch: None,
                }]
        );
        assert!(
            replayed.list_cuts(GROUP).await.expect("the group is live")
                == vec![
                    RetainedCut {
                        epoch: 1,
                        cut: first.cut,
                    },
                    RetainedCut {
                        epoch: 2,
                        cut: second.cut,
                    },
                ]
        );

        // The recovered coordinator allocates the next epoch, never a used one.
        let third = replayed
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
        assert!(third.epoch == 3);
    }

    async fn recover_one_cut(
        fixture: &Fixture,
        epoch: InjectionEpoch,
    ) -> (crate::barrier::coordinator::BarrierCoordinator, RetainedCut) {
        let replayed = fixture.recovered().await;
        let mut cuts = replayed.list_cuts(GROUP).await.expect("the group is live");
        assert!(cuts.len() == 1);
        let cut = cuts.remove(0);
        assert!(cut.epoch == epoch.0);
        (replayed, cut)
    }

    #[derive(Clone, Copy)]
    struct InjectionEpoch(i64);

    #[derive(Clone, Copy)]
    struct BarrierPartitionCount(i32);

    async fn append_interrupted_start(
        coordinator: &crate::barrier::coordinator::BarrierCoordinator,
        epoch: InjectionEpoch,
        start: &InjectionStartValue,
        context: &str,
    ) {
        coordinator
            .append_records(
                GROUP,
                vec![(
                    RecordKey::injection_start(GROUP, epoch.0),
                    Some(encode_injection_start(start).expect("encodes").into()),
                )],
            )
            .await
            .expect(context);
    }

    fn interrupted_injection(partition_count: BarrierPartitionCount) -> InjectionStartValue {
        InjectionStartValue {
            coordinator_epoch: 3,
            triggered_at: 1_000,
            targets: vec![crate::barrier::persistence::TopicTarget {
                topic: "orders".to_owned(),
                partition_count: partition_count.0,
            }],
        }
    }

    #[tokio::test]
    async fn recovery_finalises_an_interrupted_injection_as_partial() {
        let fixture = Fixture::new();
        let coordinator = fixture
            .coordinator_with_group(GROUP, spec(GroupSpecSetup::default()))
            .await;

        // A coordinator that crashed after the injection-start record leaves
        // exactly this behind.
        let start = interrupted_injection(BarrierPartitionCount(2));
        append_interrupted_start(
            &coordinator,
            InjectionEpoch(1),
            &start,
            "the injection-start record lands",
        )
        .await;

        let (replayed, cut) = recover_one_cut(&fixture, InjectionEpoch(1)).await;
        assert!(cut.cut.status == CutStatus::Partial);
        assert!(cut.cut.triggered_at == 1_000);
        assert!(
            cut.cut.missing
                == vec![
                    MissingPartition {
                        topic: "orders".to_owned(),
                        partition: PartitionIndex(0),
                    },
                    MissingPartition {
                        topic: "orders".to_owned(),
                        partition: PartitionIndex(1),
                    },
                ]
        );
        assert!(
            replayed.describe_groups(&[]).await[0]
                .pending_epoch
                .is_none()
        );

        // The epoch is consumed and it is never reused.
        let next = replayed
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
        assert!(next.epoch == 2);
    }

    #[tokio::test]
    async fn recovery_leaves_malformed_and_stale_injections_fenced_open() {
        for (group, start) in [
            (
                "malformed-cut",
                InjectionStartValue {
                    coordinator_epoch: 3,
                    triggered_at: 1_000,
                    targets: Vec::new(),
                },
            ),
            (
                "stale-cut",
                InjectionStartValue {
                    coordinator_epoch: 4,
                    triggered_at: 1_000,
                    targets: vec![crate::barrier::persistence::TopicTarget {
                        topic: "orders".to_owned(),
                        partition_count: 2,
                    }],
                },
            ),
        ] {
            let fixture = Fixture::new();
            let coordinator = fixture
                .coordinator_with_group(group, spec(GroupSpecSetup::default()))
                .await;
            coordinator
                .append_records(
                    group,
                    vec![(
                        RecordKey::injection_start(group, 1),
                        Some(encode_injection_start(&start).expect("encodes").into()),
                    )],
                )
                .await
                .expect("the injection-start record lands");

            let replayed = fixture.recovered().await;
            let description = replayed.describe_groups(&[group.to_owned()]).await;
            assert!(description[0].pending_epoch == Some(1), "{group}");
            assert!(
                replayed
                    .list_cuts(group)
                    .await
                    .expect("the group is live")
                    .is_empty(),
                "{group}"
            );
        }
    }

    #[tokio::test]
    async fn recovery_consumes_the_maximum_epoch_without_wrapping() {
        let fixture = Fixture::new();
        let coordinator = fixture
            .coordinator_with_group(GROUP, spec(GroupSpecSetup::default()))
            .await;
        let start = interrupted_injection(BarrierPartitionCount(1));
        append_interrupted_start(
            &coordinator,
            InjectionEpoch(i64::MAX),
            &start,
            "the maximum-epoch start lands",
        )
        .await;

        let (replayed, cut) = recover_one_cut(&fixture, InjectionEpoch(i64::MAX)).await;
        assert!(cut.cut.status == CutStatus::Partial);
        assert!(matches!(
            replayed.trigger_injection(GROUP, None).await,
            Err(BarrierError::EpochExhausted { .. })
        ));
    }

    #[tokio::test]
    async fn a_failed_recovery_append_keeps_the_injection_pending() {
        let fixture = Fixture::new();
        let writer = fixture.coordinator().await;
        writer
            .create_group(GROUP, spec(GroupSpecSetup::default()))
            .await
            .expect("the group is created");
        let start = interrupted_injection(BarrierPartitionCount(1));
        append_interrupted_start(
            &writer,
            InjectionEpoch(1),
            &start,
            "the injection-start record lands",
        )
        .await;

        let recovering = fixture.coordinator().await;
        for (group, entry) in recovering.replay_led_partitions().await {
            recovering.groups.insert(group, Arc::new(Mutex::new(entry)));
        }
        let partition = recovering.state_partition_for(GROUP);
        fixture.registry.remove(STATE_TOPIC, partition);

        let result = recovering
            .finalize_open_injections(&fixture.source.current_image())
            .await;
        assert!(matches!(result, Err(BarrierError::StateNotLocal { .. })));
        let handle = recovering
            .groups
            .get(GROUP)
            .expect("the replayed group remains")
            .value()
            .clone();
        let entry = handle.lock().await;
        assert!(entry.pending.as_ref().map(|pending| pending.epoch) == Some(1));
        assert!(entry.cuts.is_empty());
    }
}
