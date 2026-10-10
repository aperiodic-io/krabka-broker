//! Unit tests for the share-state machine: Kafka's validation and fencing
//! order, and the records each operation appends.

use assert2::{assert, check};
use tempfile::tempdir;

use super::*;
use crate::share_coordinator::{
    config::ShareCoordinatorConfig,
    coordinator::test_support::{
        Logged, NOW_MS, batch, configured_coordinator, coordinator, image_with_topic,
        initialize_led_group, lead_all, logged_records, share_write,
    },
};

const TOPIC: uuid::Uuid = uuid::Uuid::from_bytes([5; 16]);

type WriteOutcome = Result<(), ShareStateError>;

/// What a write leaves behind: its answer and the stored summary.
type WriteResult = (WriteOutcome, Option<ShareStateSummary>);

/// One write row: `(partition, (state_epoch, leader_epoch), (start_offset,
/// dcc), drop the state partition log)`, and what Kafka 4.3.1 and Kafka trunk
/// leave behind.
struct WriteRow {
    request: (i32, (i32, i32), (i64, i32), bool),
    kafka_4_3_1: WriteResult,
    trunk: WriteResult,
}

impl WriteRow {
    /// A row that both rule sets answer alike.
    fn same(request: (i32, (i32, i32), (i64, i32), bool), result: WriteResult) -> Self {
        Self {
            request,
            kafka_4_3_1: result,
            trunk: result,
        }
    }
}

/// What a read leaves behind: `(expected read as (state_epoch, start),
/// expected write with leader epoch 3, stored leader epoch after reload)`.
type ReadResult = (
    Result<(i32, i64), ShareStateError>,
    Option<WriteOutcome>,
    i32,
);

/// One read row: `(partition, leader_epoch)`, and what Kafka 4.3.1 and Kafka
/// trunk leave behind.
struct ReadRow {
    request: (i32, i32),
    kafka_4_3_1: ReadResult,
    trunk: ReadResult,
}

impl ReadRow {
    fn same(request: (i32, i32), result: ReadResult) -> Self {
        Self {
            request,
            kafka_4_3_1: result,
            trunk: result,
        }
    }
}

/// A coordinator with Kafka trunk's rules on or off.
fn rules(trunk: bool) -> ShareCoordinatorConfig {
    ShareCoordinatorConfig {
        trunk_rules: trunk,
        ..ShareCoordinatorConfig::default()
    }
}

fn refused(code: ShareErrorCode, message: &'static str) -> ShareStateError {
    ShareStateError::Refused { code, message }
}

#[tokio::test]
async fn initialize_then_summary() {
    let dir = tempdir().unwrap();
    let (coord, _reg) = coordinator(dir.path());
    lead_all(&coord).await;

    coord
        .initialize(&image_with_topic(TOPIC, 1), "g", TOPIC, 0, 5, Offset(100))
        .await
        .unwrap();

    let summary = coord.read_summary("g", TOPIC, 0).await;
    assert!(summary == Ok(Some((5, 0, Offset(100), 0))));
}

/// `InitializeShareGroupState` creates the state key, which writes its group id
/// with an `INT16` length. A group id of 32767 bytes is initialized and deleted.
/// A longer one is refused with the internal error, as Kafka answers a record
/// that its writer cannot serialize, and the coordinator keeps no state for it.
#[tokio::test]
async fn a_group_id_over_32767_bytes_is_not_initialized() {
    let dir = tempdir().unwrap();
    let (coord, _reg) = coordinator(dir.path());
    lead_all(&coord).await;
    let image = image_with_topic(TOPIC, 1);
    let limit = crate::coordinator::unified::persistence::MAX_STRING_BYTES;
    let longest = "g".repeat(limit);
    let too_long = "g".repeat(limit + 1);

    check!(
        coord
            .initialize(&image, &longest, TOPIC, 0, 5, Offset(100))
            .await
            == Ok(())
    );
    check!(coord.read_summary(&longest, TOPIC, 0).await == Ok(Some((5, 0, Offset(100), 0))));
    check!(coord.delete(&image, &longest, TOPIC, 0).await == Ok(()));

    let refused = coord
        .initialize(&image, &too_long, TOPIC, 0, 5, Offset(100))
        .await;
    check!(
        refused
            == Err(ShareStateError::Operation {
                code: codes::UNKNOWN_SERVER_ERROR,
                message: message::UNKNOWN_SERVER_ERROR,
            })
    );
    check!(coord.read_summary(&too_long, TOPIC, 0).await == Ok(None));
}

/// What a second initialize leaves behind, for one Kafka release: its
/// answer, the snapshots logged, and the stored summary.
type InitializeResult = (WriteOutcome, usize, Option<ShareStateSummary>);

/// `InitializeShareGroupState` as Kafka's `ShareCoordinatorShard.initializeState`
/// answers a second request on a key that state epoch 5 and start offset 100
/// initialized. Kafka 4.3.1 skips the fence for a state epoch of `-1` and
/// always writes a new snapshot; trunk refuses a negative state epoch and
/// takes a repeat of the stored epoch and start offset as a no-op. The rows
/// are the request's `(state epoch, start offset)`, then the result under
/// Kafka 4.3.1 and under trunk.
#[tokio::test]
async fn initialize_follows_the_rules_of_each_kafka_release() {
    let fenced = || refused(codes::FENCED_STATE_EPOCH, message::FENCED_STATE_EPOCH);
    let negative = || refused(codes::INVALID_REQUEST, message::NEGATIVE_STATE_EPOCH);
    let first = Some((5, 0, Offset(100), 0));
    let rows: Vec<(&str, (i32, i64), InitializeResult, InitializeResult)> = vec![
        (
            "a repeat of the stored epoch and start offset",
            (5, 100),
            (Ok(()), 2, first),
            (Ok(()), 1, first),
        ),
        (
            "a state epoch of -1",
            (-1, 100),
            (Ok(()), 2, Some((-1, 0, Offset(100), 0))),
            (Err(negative()), 1, first),
        ),
        (
            "a lower state epoch",
            (4, 100),
            (Err(fenced()), 1, first),
            (Err(fenced()), 1, first),
        ),
        (
            "a higher state epoch and a new start offset",
            (6, 50),
            (Ok(()), 2, Some((6, 0, Offset(50), 0))),
            (Ok(()), 2, Some((6, 0, Offset(50), 0))),
        ),
    ];
    for trunk in [false, true] {
        for (name, (state_epoch, start), kafka_4_3_1, trunk_rules) in &rows {
            let (expected, snapshots, summary) = if trunk { *trunk_rules } else { *kafka_4_3_1 };
            let dir = tempdir().unwrap();
            let (coord, _reg, _clock) = configured_coordinator(dir.path(), rules(trunk));
            lead_all(&coord).await;
            let image = image_with_topic(TOPIC, 1);
            coord
                .initialize(&image, "g", TOPIC, 0, 5, Offset(100))
                .await
                .unwrap();

            let second = coord
                .initialize(&image, "g", TOPIC, 0, *state_epoch, Offset(*start))
                .await;

            let state_partition = coord.state_partition_for("g", &TOPIC, 0);
            let logged = logged_records(&coord, state_partition)
                .into_iter()
                .filter(|(_, logged)| matches!(logged, Logged::Snapshot(_)))
                .count();
            check!(second == expected, "{name}, trunk {trunk}");
            check!(logged == snapshots, "{name}, trunk {trunk}");
            check!(
                coord.read_summary("g", TOPIC, 0).await == Ok(summary),
                "{name}, trunk {trunk}"
            );
        }
    }
}

/// Stored state for the read and write tables, on topic `TOPIC` with two
/// partitions: partition 0 at state epoch 2, leader epoch 3, start offset
/// 10 and delivery complete count 4, and partition 2 (not in the image of
/// two partitions) at state epoch 2.
async fn seeded(dir: &std::path::Path, trunk: bool) -> (ShareCoordinator, MetadataImage) {
    let (coord, _reg, _clock) = configured_coordinator(dir, rules(trunk));
    lead_all(&coord).await;
    let image = image_with_topic(TOPIC, 2);
    let wide = image_with_topic(TOPIC, 3);
    coord
        .initialize(&wide, "g", TOPIC, 0, 2, Offset(10))
        .await
        .unwrap();
    coord
        .initialize(&wide, "g", TOPIC, 2, 2, Offset(10))
        .await
        .unwrap();
    coord.read(&image, "g", TOPIC, 0, 3).await.unwrap();
    coord
        .write(&image, "g", TOPIC, 0, share_write((2, 3), (10, 4), vec![]))
        .await
        .unwrap();
    assert!(coord.read_summary("g", TOPIC, 0).await == Ok(Some((2, 3, Offset(10), 4))));
    (coord, image)
}

fn write_rows() -> Vec<WriteRow> {
    let unchanged = Some((2, 3, Offset(10), 4));
    let negative_leader_epoch = || {
        Err(refused(
            codes::INVALID_REQUEST,
            message::NEGATIVE_LEADER_EPOCH,
        ))
    };
    let negative_state_epoch = || {
        Err(refused(
            codes::INVALID_REQUEST,
            message::NEGATIVE_STATE_EPOCH,
        ))
    };
    vec![
        // The same start offset with a lower count: 4.3.1 stores the count as
        // sent, trunk keeps the greater one.
        WriteRow {
            request: (0, (2, 3), (10, 2), false),
            kafka_4_3_1: (Ok(()), Some((2, 3, Offset(10), 2))),
            trunk: (Ok(()), unchanged),
        },
        // A lower start offset: 4.3.1 lowers the stored one and stores the
        // count as sent, trunk never lets the start offset go back.
        WriteRow {
            request: (0, (2, 3), (5, 9), false),
            kafka_4_3_1: (Ok(()), Some((2, 3, Offset(5), 9))),
            trunk: (Ok(()), unchanged),
        },
        WriteRow::same(
            (0, (2, 3), (20, 1), false),
            (Ok(()), Some((2, 3, Offset(20), 1))),
        ),
        WriteRow::same((0, (7, 3), (10, 4), false), (Ok(()), unchanged)),
        WriteRow::same((0, (2, 9), (10, 4), false), (Ok(()), unchanged)),
        // A version 0 write has no count: `-1`.
        WriteRow {
            request: (0, (2, 3), (10, -1), false),
            kafka_4_3_1: (Ok(()), Some((2, 3, Offset(10), -1))),
            trunk: (Ok(()), unchanged),
        },
        WriteRow::same(
            (0, (1, 3), (10, 4), false),
            (
                Err(refused(
                    codes::FENCED_STATE_EPOCH,
                    message::FENCED_STATE_EPOCH,
                )),
                unchanged,
            ),
        ),
        WriteRow::same(
            (0, (2, 2), (10, 4), false),
            (
                Err(refused(
                    codes::FENCED_LEADER_EPOCH,
                    message::FENCED_LEADER_EPOCH,
                )),
                unchanged,
            ),
        ),
        WriteRow::same(
            (1, (0, 0), (0, 0), false),
            (
                Err(refused(
                    codes::INVALID_REQUEST,
                    message::WRITE_UNINITIALIZED_SHARE_PARTITION,
                )),
                None,
            ),
        ),
        WriteRow::same(
            (-1, (2, 3), (10, 4), false),
            (
                Err(refused(
                    codes::INVALID_REQUEST,
                    message::NEGATIVE_PARTITION_ID,
                )),
                None,
            ),
        ),
        // `-1` is "not supplied" in 4.3.1, and skips the fence. Trunk refuses
        // a negative epoch.
        WriteRow {
            request: (0, (2, -1), (10, 4), false),
            kafka_4_3_1: (Ok(()), unchanged),
            trunk: (negative_leader_epoch(), unchanged),
        },
        WriteRow {
            request: (0, (-1, 3), (10, 4), false),
            kafka_4_3_1: (Ok(()), unchanged),
            trunk: (negative_state_epoch(), unchanged),
        },
        WriteRow::same(
            (2, (2, 3), (10, 4), false),
            (
                Err(refused(
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    message::UNKNOWN_TOPIC_OR_PARTITION,
                )),
                Some((2, 0, Offset(10), 0)),
            ),
        ),
        WriteRow::same(
            (0, (2, 3), (30, 0), true),
            (
                Err(ShareStateError::Operation {
                    code: codes::COORDINATOR_NOT_AVAILABLE,
                    message: message::UNKNOWN_TOPIC_OR_PARTITION,
                }),
                unchanged,
            ),
        ),
    ]
}

/// `WriteShareGroupState` as Kafka's `ShareCoordinatorShard.writeState`
/// answers it, under the rules of Kafka 4.3.1 and of Kafka trunk, and the
/// stored summary after each write.
#[tokio::test]
async fn write_matches_kafka_checks_and_record_rules() {
    for trunk in [false, true] {
        for (index, row) in write_rows().into_iter().enumerate() {
            let (partition, epochs, progress, drop_log) = row.request;
            let (expected, summary) = if trunk { row.trunk } else { row.kafka_4_3_1 };
            let dir = tempdir().unwrap();
            let (coord, image) = seeded(dir.path(), trunk).await;
            if drop_log {
                let state_partition = coord.state_partition_for("g", &TOPIC, partition);
                coord
                    .partitions
                    .remove(crate::share_coordinator::bootstrap::TOPIC, state_partition);
            }
            let result = coord
                .write(
                    &image,
                    "g",
                    TOPIC,
                    partition,
                    share_write(
                        epochs,
                        progress,
                        vec![batch(Offset(progress.0)..=Offset(progress.0 + 9))],
                    ),
                )
                .await;
            check!(result == expected, "row {index}, trunk {trunk}");
            check!(
                coord.read_summary("g", TOPIC, partition).await == Ok(summary),
                "row {index}, trunk {trunk}"
            );
        }
    }
}

/// `ReadShareGroupState` as Kafka's `readStateAndMaybeUpdateLeaderEpoch`
/// answers it, then a write with leader epoch 3, and the stored leader epoch
/// after a reload of the log.
#[tokio::test]
async fn read_fences_and_persists_the_leader_epoch() {
    let fenced = || refused(codes::FENCED_LEADER_EPOCH, message::FENCED_LEADER_EPOCH);
    let rows = [
        ReadRow::same((0, 3), (Ok((2, 10)), Some(Ok(())), 3)),
        ReadRow::same((0, 4), (Ok((2, 10)), Some(Err(fenced())), 4)),
        ReadRow::same((0, 2), (Err(fenced()), Some(Ok(())), 3)),
        ReadRow::same(
            (1, 0),
            (
                Err(refused(
                    codes::INVALID_REQUEST,
                    message::READ_UNINITIALIZED_SHARE_PARTITION,
                )),
                None,
                3,
            ),
        ),
        ReadRow::same(
            (-1, 0),
            (
                Err(refused(
                    codes::INVALID_REQUEST,
                    message::NEGATIVE_PARTITION_ID,
                )),
                None,
                3,
            ),
        ),
        // `-1` is "not supplied" in 4.3.1: a plain read that skips the fence
        // and updates no leader epoch. Trunk refuses a negative epoch.
        ReadRow {
            request: (0, -1),
            kafka_4_3_1: (Ok((2, 10)), Some(Ok(())), 3),
            trunk: (
                Err(refused(
                    codes::INVALID_REQUEST,
                    message::NEGATIVE_LEADER_EPOCH,
                )),
                None,
                3,
            ),
        },
        ReadRow::same(
            (2, 0),
            (
                Err(refused(
                    codes::UNKNOWN_TOPIC_OR_PARTITION,
                    message::UNKNOWN_TOPIC_OR_PARTITION,
                )),
                None,
                3,
            ),
        ),
    ];

    for trunk in [false, true] {
        for (index, row) in rows.iter().enumerate() {
            let (partition, leader_epoch) = row.request;
            let (expected, write, reloaded_leader_epoch) =
                if trunk { row.trunk } else { row.kafka_4_3_1 };
            let dir = tempdir().unwrap();
            let (coord, image) = seeded(dir.path(), trunk).await;
            let read = coord
                .read(&image, "g", TOPIC, partition, leader_epoch)
                .await
                .map(|st| (st.state_epoch, st.start_offset.0));
            check!(read == expected, "row {index}, trunk {trunk}");
            if let Some(expected_write) = write {
                let written = coord
                    .write(
                        &image,
                        "g",
                        TOPIC,
                        partition,
                        share_write((2, 3), (10, 4), vec![]),
                    )
                    .await;
                check!(written == expected_write, "row {index}, trunk {trunk}");
            }
            coord.reload_all_partitions_for_test().await;
            let summary = coord.read_summary("g", TOPIC, 0).await;
            check!(
                summary.map(|s| s.map(|(_, leader, ..)| leader)) == Ok(Some(reloaded_leader_epoch)),
                "row {index}, trunk {trunk}"
            );
        }
    }
}

/// A failed append leaves the in-memory state as it was.
#[tokio::test]
async fn failed_read_append_changes_no_state() {
    let dir = tempdir().unwrap();
    let (coord, image) = seeded(dir.path(), false).await;
    let state_partition = coord.state_partition_for("g", &TOPIC, 0);
    coord
        .partitions
        .remove(crate::share_coordinator::bootstrap::TOPIC, state_partition);

    let read = coord.read(&image, "g", TOPIC, 0, 8).await;
    assert!(
        read == Err(ShareStateError::Operation {
            code: codes::COORDINATOR_NOT_AVAILABLE,
            message: message::UNKNOWN_TOPIC_OR_PARTITION,
        })
    );
    assert!(coord.read_summary("g", TOPIC, 0).await == Ok(Some((2, 3, Offset(10), 4))));
}

/// A re-initialize writes leader epoch 0, but the recorded leader epoch
/// stays, as Kafka's `leaderEpochMap` only grows: a share-partition leader
/// with an older epoch is still fenced.
#[tokio::test]
async fn reinitialize_keeps_the_recorded_leader_epoch() {
    let dir = tempdir().unwrap();
    let (coord, image) = seeded(dir.path(), false).await;
    coord
        .initialize(&image, "g", TOPIC, 0, 3, Offset(40))
        .await
        .unwrap();
    check!(coord.read_summary("g", TOPIC, 0).await == Ok(Some((3, 0, Offset(40), 0))));
    let rows = [
        (
            2,
            Err(refused(
                codes::FENCED_LEADER_EPOCH,
                message::FENCED_LEADER_EPOCH,
            )),
        ),
        (3, Ok(())),
    ];
    for (leader_epoch, expected) in rows {
        let written = coord
            .write(
                &image,
                "g",
                TOPIC,
                0,
                share_write((3, leader_epoch), (40, 0), vec![]),
            )
            .await;
        check!(written == expected, "leader epoch {leader_epoch}");
    }
}

fn update(start_offset: i64, delivery_complete_count: i32, batches: Vec<StateBatch>) -> Logged {
    Logged::Update(ShareUpdateValue {
        snapshot_epoch: 0,
        leader_epoch: 0,
        start_offset: Offset(start_offset),
        delivery_complete_count,
        state_batches: batches,
    })
}

/// With a threshold of two updates, the third write is one `ShareSnapshot`
/// in place of the update, as Kafka's `generateShareStateRecord` picks it.
/// An update holds only the written batches, combined among themselves and
/// clipped at the start offset. The snapshot folds the stored batches in.
#[tokio::test]
async fn writes_append_updates_then_one_snapshot_at_the_threshold() {
    let dir = tempdir().unwrap();
    let config = ShareCoordinatorConfig {
        snapshot_update_records_per_snapshot: 2,
        ..ShareCoordinatorConfig::default()
    };
    let (coord, _reg, _clock) = configured_coordinator(dir.path(), config);
    let image = initialize_led_group(
        &coord,
        crate::share_coordinator::coordinator::test_support::StateSeedSetup::default(),
    )
    .await;
    let writes = [
        (
            0,
            1,
            vec![batch(Offset(0)..=Offset(9)), batch(Offset(5)..=Offset(14))],
        ),
        (10, 2, vec![batch(Offset(0)..=Offset(19))]),
        (12, 3, vec![batch(Offset(20)..=Offset(29))]),
        (12, 3, vec![batch(Offset(30)..=Offset(39))]),
    ];
    for (start, dcc, batches) in writes {
        coord
            .write(
                &image,
                "g",
                TOPIC,
                0,
                share_write((1, 0), (start, dcc), batches),
            )
            .await
            .unwrap();
    }

    let state_partition = coord.state_partition_for("g", &TOPIC, 0);
    let logged: Vec<Logged> = logged_records(&coord, state_partition)
        .into_iter()
        .map(|(_, logged)| logged)
        .collect();
    let expected = vec![
        Logged::Snapshot(ShareSnapshotValue {
            snapshot_epoch: 0,
            state_epoch: 1,
            leader_epoch: 0,
            start_offset: Offset(0),
            delivery_complete_count: 0,
            create_timestamp: NOW_MS,
            write_timestamp: NOW_MS,
            state_batches: vec![],
        }),
        update(0, 1, vec![batch(Offset(0)..=Offset(14))]),
        update(10, 2, vec![batch(Offset(10)..=Offset(19))]),
        Logged::Snapshot(ShareSnapshotValue {
            snapshot_epoch: 1,
            state_epoch: 1,
            leader_epoch: 0,
            start_offset: Offset(12),
            delivery_complete_count: 3,
            create_timestamp: NOW_MS,
            write_timestamp: NOW_MS,
            state_batches: vec![batch(Offset(12)..=Offset(29))],
        }),
        Logged::Update(ShareUpdateValue {
            snapshot_epoch: 1,
            leader_epoch: 0,
            start_offset: Offset(12),
            delivery_complete_count: 3,
            state_batches: vec![batch(Offset(30)..=Offset(39))],
        }),
    ];
    assert!(logged == expected);
    let state = coord.state_for_test("g", TOPIC, 0).await.unwrap();
    check!(state.state_batches == vec![batch(Offset(12)..=Offset(39))]);
    check!(state.updates_since_snapshot == 1);
}

/// One share-state operation on the key `("g", TOPIC, 0)`.
#[derive(Debug, Clone, Copy)]
enum Op {
    Initialize,
    Write,
    /// A read with a newer leader epoch, which writes that epoch.
    Read,
    Delete,
}

/// What happens to the state partition of the key around an operation.
#[derive(Debug, Clone, Copy)]
enum Fault {
    /// A follower in the ISR does not fetch, so no record commits.
    NoCommit,
    /// The partition moves to broker 2 while the operation waits.
    MovesAway,
    /// The partition leads on this broker at a newer epoch before the
    /// operation starts: the term of the coordinator has ended.
    NewerEpoch,
}

/// An operation answers only when its record commits under the term of the
/// coordinator, as Kafka's `CoordinatorRuntime` completes a write. A record
/// that does not commit in `share.coordinator.write.timeout.ms` answers
/// `COORDINATOR_NOT_AVAILABLE`. A term that ends first answers
/// `NOT_COORDINATOR`, and a coordinator whose term has ended appends nothing.
#[tokio::test]
async fn an_operation_answers_only_when_its_record_commits_in_the_term() {
    let timed_out = ShareStateError::Operation {
        code: codes::COORDINATOR_NOT_AVAILABLE,
        message: message::REQUEST_TIMED_OUT,
    };
    let not_coordinator = ShareStateError::Operation {
        code: codes::NOT_COORDINATOR,
        message: message::NOT_COORDINATOR,
    };
    // (fault, the answer of every operation, records in the log afterwards)
    let faults = [
        (Fault::NoCommit, timed_out, 2),
        (Fault::MovesAway, not_coordinator, 2),
        (Fault::NewerEpoch, not_coordinator, 1),
    ];
    for (fault, expected, logged) in faults {
        for op in [Op::Initialize, Op::Write, Op::Read, Op::Delete] {
            let dir = tempdir().unwrap();
            // The setup `initialize` below commits under the same timeout,
            // so the no-commit case keeps enough of it for a loaded machine.
            let timeout = match fault {
                Fault::NoCommit => std::time::Duration::from_secs(2),
                Fault::MovesAway | Fault::NewerEpoch => std::time::Duration::from_secs(30),
            };
            let config = ShareCoordinatorConfig {
                write_timeout: timeout,
                ..ShareCoordinatorConfig::default()
            };
            let (coord, reg, _clock) = configured_coordinator(dir.path(), config);
            let image = initialize_led_group(
                &coord,
                crate::share_coordinator::coordinator::test_support::StateSeedSetup::default(),
            )
            .await;
            let state_partition = coord.state_partition_for("g", &TOPIC, 0);
            let part = reg
                .get(crate::share_coordinator::bootstrap::TOPIC, state_partition)
                .expect("the state partition is open");
            let both = [krabka_raft::NodeId(1), krabka_raft::NodeId(2)];
            match fault {
                Fault::NoCommit | Fault::MovesAway => {
                    part.replica_state.lock().await.install_isr(
                        &both,
                        &both,
                        krabka_raft::NodeId(1),
                        std::time::Instant::now(),
                    );
                }
                Fault::NewerEpoch => part.install_leader_change(1, 1).await,
            }
            let mover = matches!(fault, Fault::MovesAway).then(|| {
                let part = std::sync::Arc::clone(&part);
                tokio::spawn(async move {
                    // intentional: the operation has to wait before the move.
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    part.install_leader_change(2, 1).await;
                })
            });

            let answer = match op {
                Op::Initialize => coord.initialize(&image, "g", TOPIC, 0, 2, Offset(5)).await,
                Op::Write => {
                    coord
                        .write(
                            &image,
                            "g",
                            TOPIC,
                            0,
                            share_write((1, 0), (0, 0), vec![batch(Offset(0)..=Offset(9))]),
                        )
                        .await
                }
                Op::Read => coord.read(&image, "g", TOPIC, 0, 1).await.map(|_| ()),
                Op::Delete => coord.delete(&image, "g", TOPIC, 0).await,
            };
            if let Some(mover) = mover {
                mover.await.expect("the partition moves");
            }

            check!(answer == Err(expected), "{op:?} under {fault:?}");
            check!(
                logged_records(&coord, state_partition).len() == logged,
                "{op:?} under {fault:?}"
            );
        }
    }
}
