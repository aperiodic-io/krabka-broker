//! Unit tests for the injection protocol: the epoch a run takes, the markers
//! it places, the cut it publishes, and the group lock that holds a second
//! caller off.

use assert2::{assert, check};
use krabka_ids::PartitionIndex;
use krabka_units::millis;

use super::*;
use crate::{
    barrier::{
        coordinator::test_support::{Fixture, GROUP, GroupSpecSetup, RetainedCutCount, spec},
        marker::parse_barrier_marker,
        persistence::{MissingPartition, PartitionOffset, TopicOffsets},
    },
    partition_registry::PartitionRegistry,
};

async fn check_retained_epochs(coordinator: &BarrierCoordinator, expected: &[i64]) {
    let epochs: Vec<i64> = coordinator
        .list_cuts(GROUP)
        .await
        .expect("the group is live")
        .iter()
        .map(|cut| cut.epoch)
        .collect();
    assert!(epochs == expected);
}

fn marker_at(
    registry: &PartitionRegistry,
    topic: &str,
    partition: PartitionIndex,
    offset: Offset,
) -> Option<BarrierMarker> {
    let part = registry.get(topic, partition)?;
    let read = part
        .read_log(offset, krabka_units::mebibytes(1))
        .expect("read the log back");
    let batch = read.batches.first()?;
    parse_barrier_marker(&batch.records[0]).ok()
}

#[tokio::test]
async fn an_injection_marks_every_partition_and_publishes_a_complete_cut() {
    let fixture = Fixture::new();
    let coordinator = fixture
        .coordinator_with_group(
            GROUP,
            spec(GroupSpecSetup {
                topics: &["orders", "payments"],
                ..Default::default()
            }),
        )
        .await;

    let outcome = coordinator
        .trigger_injection(GROUP, None)
        .await
        .expect("the injection runs");
    assert!(outcome.epoch == 1);
    assert!(outcome.cut.status == CutStatus::Complete);
    assert!(outcome.cut.missing.is_empty());
    assert!(
        outcome.cut.topics
            == vec![
                TopicOffsets {
                    topic: "orders".to_owned(),
                    partitions: vec![
                        PartitionOffset {
                            partition: PartitionIndex(0),
                            offset: Offset(0),
                        },
                        PartitionOffset {
                            partition: PartitionIndex(1),
                            offset: Offset(0),
                        },
                    ],
                },
                TopicOffsets {
                    topic: "payments".to_owned(),
                    partitions: vec![PartitionOffset {
                        partition: PartitionIndex(0),
                        offset: Offset(0),
                    }],
                },
            ]
    );

    // The record at every named offset is this epoch's marker.
    for topic in &outcome.cut.topics {
        for entry in &topic.partitions {
            let marker = marker_at(
                &fixture.registry,
                &topic.topic,
                entry.partition,
                entry.offset,
            );
            check!(marker.map(|m| (m.group, m.epoch)) == Some((GROUP.to_owned(), 1)));
        }
    }
}

#[tokio::test]
async fn every_injection_takes_the_next_epoch() {
    let fixture = Fixture::new();
    let coordinator = fixture
        .coordinator_with_group(GROUP, spec(GroupSpecSetup::default()))
        .await;

    let mut epochs = Vec::new();
    for _ in 0..3 {
        epochs.push(
            coordinator
                .trigger_injection(GROUP, None)
                .await
                .expect("the injection runs")
                .epoch,
        );
    }
    assert!(epochs == vec![1, 2, 3]);
    assert!(
        coordinator.describe_groups(&[]).await[0]
            .definition
            .last_epoch
            == 3
    );
}

#[tokio::test]
async fn an_exhausted_group_epoch_rejects_injection() {
    let fixture = Fixture::new();
    let coordinator = fixture
        .coordinator_with_group(GROUP, spec(GroupSpecSetup::default()))
        .await;
    let entry = coordinator
        .groups
        .get(GROUP)
        .expect("the group is live")
        .clone();
    entry.lock().await.definition.last_epoch = i64::MAX;

    let result = coordinator.trigger_injection(GROUP, None).await;

    assert!(let Err(BarrierError::EpochExhausted { group }) = result);
    assert!(group == GROUP);
}

#[tokio::test]
async fn a_partition_that_carries_no_marker_makes_the_cut_partial() {
    // Only partition 0 of `orders` is open here, so partition 1 stays
    // unmarked until the deadline runs out.
    let fixture = Fixture::with_data_partitions(&[("orders", 1)]);
    let coordinator = fixture
        .coordinator_with_group(GROUP, spec(GroupSpecSetup::default()))
        .await;

    let outcome = coordinator
        .trigger_injection(GROUP, None)
        .await
        .expect("the injection runs");
    assert!(outcome.epoch == 1);
    assert!(outcome.cut.status == CutStatus::Partial);
    assert!(
        outcome.cut.missing
            == vec![MissingPartition {
                topic: "orders".to_owned(),
                partition: PartitionIndex(1),
            }]
    );

    // The epoch is consumed. The next injection takes epoch 2.
    let next = coordinator
        .trigger_injection(GROUP, None)
        .await
        .expect("the injection runs");
    assert!(next.epoch == 2);
}

#[tokio::test]
async fn a_topic_set_edit_applies_from_the_next_epoch() {
    let fixture = Fixture::new();
    let coordinator = fixture
        .coordinator_with_group(GROUP, spec(GroupSpecSetup::default()))
        .await;
    let first = coordinator
        .trigger_injection(GROUP, None)
        .await
        .expect("the injection runs");
    assert!(first.cut.topics.len() == 1);

    coordinator
        .update_group(
            GROUP,
            spec(GroupSpecSetup {
                topics: &["orders", "payments"],
                ..Default::default()
            }),
        )
        .await
        .expect("the group is updated");
    let second = coordinator
        .trigger_injection(GROUP, None)
        .await
        .expect("the injection runs");
    let topics: Vec<&str> = second.cut.topics.iter().map(|t| t.topic.as_str()).collect();
    assert!(topics == vec!["orders", "payments"]);
    assert!(second.epoch == 2);
}

#[tokio::test]
async fn the_group_keeps_only_its_retained_cuts() {
    let fixture = Fixture::new();
    let coordinator = fixture
        .coordinator_with_group(
            GROUP,
            spec(GroupSpecSetup {
                retained_cuts: RetainedCutCount(2),
                ..Default::default()
            }),
        )
        .await;
    for _ in 0..4 {
        coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
    }

    check_retained_epochs(&coordinator, &[3, 4]).await;

    // The tombstones are durable, so a replay agrees.
    let replayed = fixture.recovered().await;
    check_retained_epochs(&replayed, &[3, 4]).await;
}

#[tokio::test]
async fn a_smaller_retention_drops_every_cut_below_the_new_window() {
    let fixture = Fixture::new();
    let coordinator = fixture
        .coordinator_with_group(
            GROUP,
            spec(GroupSpecSetup {
                retained_cuts: RetainedCutCount(8),
                ..Default::default()
            }),
        )
        .await;
    for _ in 0..4 {
        coordinator
            .trigger_injection(GROUP, None)
            .await
            .expect("the injection runs");
    }
    coordinator
        .update_group(
            GROUP,
            spec(GroupSpecSetup {
                retained_cuts: RetainedCutCount(1),
                ..Default::default()
            }),
        )
        .await
        .expect("the group is updated");
    coordinator
        .trigger_injection(GROUP, None)
        .await
        .expect("the injection runs");

    check_retained_epochs(&coordinator, &[5]).await;

    let replayed = fixture.recovered().await;
    check_retained_epochs(&replayed, &[5]).await;
}

#[tokio::test]
async fn the_scheduler_injects_only_a_group_whose_interval_elapsed() {
    let fixture = Fixture::new();
    let coordinator = fixture
        .coordinator_with_group(
            GROUP,
            spec(GroupSpecSetup {
                interval: Some(millis(1_000)),
                ..Default::default()
            }),
        )
        .await;
    coordinator
        .create_group(
            "on-demand",
            spec(GroupSpecSetup {
                topics: &["payments"],
                ..Default::default()
            }),
        )
        .await
        .expect("the group is created");

    assert!(coordinator.run_due_injections(0).await.is_empty());

    let due = coordinator.run_due_injections(now_ms() + 2_000).await;
    assert!(due == vec![GROUP.to_owned()]);
    assert!(
        coordinator
            .list_cuts("on-demand")
            .await
            .expect("the group is live")
            .is_empty()
    );
}

#[tokio::test]
async fn an_injection_holds_the_group_against_a_second_caller() {
    let fixture = Fixture::new();
    let coordinator = fixture
        .coordinator_with_group(GROUP, spec(GroupSpecSetup::default()))
        .await;

    let handle = coordinator
        .live_entry(GROUP)
        .expect("the group entry is there");
    let held = handle.lock().await;
    let refused = coordinator.trigger_injection(GROUP, None).await;
    assert!(let Err(BarrierError::InjectionInProgress { .. }) = refused);
    drop(held);

    assert!(coordinator.trigger_injection(GROUP, None).await.is_ok());
}
