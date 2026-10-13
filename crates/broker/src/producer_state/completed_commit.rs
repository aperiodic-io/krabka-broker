use {
    super::{Decision, ProducerState, RetainedBatch, SequenceContext},
    crate::{broker::spawn_partition, log_dir_status::LogDirRegistry},
    assert2::assert,
    krabka_ids::PartitionIndex,
};

async fn commit_physical_batch(state: &ProducerState, partition: PartitionIndex, sequence: i32) {
    let offset = i64::from(sequence) * 10;
    state
        .commit(
            "t",
            partition,
            (42, 3),
            (sequence, 0),
            (offset, i64::from(sequence), false),
        )
        .await;
}

#[tokio::test]
async fn late_completions_merge_retries_without_regressing_the_latest_batch() {
    let state = ProducerState::new();
    let partition = PartitionIndex(0);
    // The newer completion arrives first; the older batch still belongs in
    // the retry window when its deferred acknowledgement finishes.
    state
        .commit("t", partition, (42, 3), (2, 1), (20, 200, false))
        .await;
    state
        .commit("t", partition, (42, 3), (0, 1), (10, 100, false))
        .await;
    let latest = state.snapshot("t", partition).await[0].1;
    assert!(latest.last_offset == 21 && latest.last_sequence == 3);
    for (sequence, base, timestamp) in [(0, 10, 100), (2, 20, 200)] {
        let checked = state
            .check_batch(
                "t",
                partition,
                SequenceContext::RELEASED,
                (42, 3),
                (sequence, 1),
            )
            .await;
        assert!(checked.decision == Decision::Duplicate { base_offset: base });
        assert!(
            checked.duplicate
                == Some(RetainedBatch {
                    base_sequence: sequence,
                    last_sequence: sequence + 1,
                    base_offset: base,
                    last_offset: base + 1,
                    timestamp,
                })
        );
    }
    assert!(state.check("t", partition, 42, 3, 4, 0).await == Decision::Append);
    let before = state.snapshot("t", partition).await;
    state
        .commit("t", partition, (42, 3), (0, 1), (10, 100, false))
        .await;
    assert!(state.snapshot("t", partition).await == before);
}

#[tokio::test]
async fn all_completion_orders_keep_the_same_five_physical_batches() {
    let partition = PartitionIndex(0);
    for rank in 0..720 {
        let state = ProducerState::new();
        let mut choices: Vec<i32> = (0..6).collect();
        let mut remaining = rank;
        while !choices.is_empty() {
            let index = remaining % choices.len();
            remaining /= choices.len();
            let sequence = choices.remove(index);
            commit_physical_batch(&state, partition, sequence).await;
        }
        let before = state.snapshot("t", partition).await;
        assert!(before[0].1.last_sequence == 5 && before[0].1.last_offset == 50);
        // Completing an evicted batch again must not bring it back or evict
        // a newer batch. Completing retained batches again is idempotent.
        for sequence in 0..6 {
            commit_physical_batch(&state, partition, sequence).await;
            assert!(state.snapshot("t", partition).await == before);
            let checked = state
                .check_batch(
                    "t",
                    partition,
                    SequenceContext::RELEASED,
                    (42, 3),
                    (sequence, 0),
                )
                .await;
            if sequence == 0 {
                assert!(checked.decision == Decision::OutOfOrder && checked.duplicate == None);
            } else {
                assert!(
                    checked.decision
                        == Decision::Duplicate {
                            base_offset: i64::from(sequence) * 10
                        }
                );
                assert!(
                    checked.duplicate
                        == Some(RetainedBatch {
                            base_sequence: sequence,
                            last_sequence: sequence,
                            base_offset: i64::from(sequence) * 10,
                            last_offset: i64::from(sequence) * 10,
                            timestamp: i64::from(sequence),
                        })
                );
            }
        }
        assert!(state.check("t", partition, 42, 3, 6, 0).await == Decision::Append);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn completions_after_real_markers_preserve_closed_state_and_restart_decisions() {
    use {
        crate::txn::marker::{MarkerType, build_marker_batch},
        krabka_log::{Log, LogConfig, ProducerId},
        krabka_protocol::records::{Attributes, Record, RecordBatch},
        std::sync::Arc,
    };

    for epoch in [3, 4] {
        for marker in [MarkerType::Commit, MarkerType::Abort] {
            let directory = tempfile::tempdir().unwrap();
            let state = Arc::new(ProducerState::new());
            let log = Log::open(directory.path(), LogConfig::default()).unwrap();
            let partition = spawn_partition(
                "t".into(),
                PartitionIndex(0),
                directory.path().to_path_buf(),
                log,
                LogDirRegistry::default(),
                Arc::clone(&state),
                false,
            );
            for sequence in [0, 2] {
                partition
                    .produce_batch(RecordBatch {
                        producer_id: 42,
                        producer_epoch: 3,
                        base_sequence: sequence,
                        last_offset_delta: 1,
                        base_timestamp: 100 + i64::from(sequence),
                        max_timestamp: 100 + i64::from(sequence),
                        attributes: Attributes::default().with_transactional(true),
                        records: (0..2)
                            .map(|offset_delta| Record {
                                offset_delta,
                                ..Record::default()
                            })
                            .collect(),
                        ..RecordBatch::default()
                    })
                    .await
                    .unwrap();
            }
            // Only the newer data completion finishes before the marker.
            state
                .commit("t", PartitionIndex(0), (42, 3), (2, 1), (2, 102, true))
                .await;
            let mut batch =
                build_marker_batch(ProducerId(42), epoch, partition.log_end_offset(), marker, 0);
            batch.base_timestamp = 900;
            batch.max_timestamp = 900;
            partition.produce_control_batch(batch).await.unwrap();
            let before = state.snapshot("t", PartitionIndex(0)).await;
            let mut expected = before.clone();
            if epoch == 3 {
                expected[0].1.earlier[0] = Some(RetainedBatch {
                    base_sequence: 0,
                    last_sequence: 1,
                    base_offset: 0,
                    last_offset: 1,
                    timestamp: 100,
                });
            }
            state
                .commit("t", PartitionIndex(0), (42, 3), (0, 1), (0, 100, true))
                .await;
            state
                .commit("t", PartitionIndex(0), (42, 3), (2, 1), (2, 102, true))
                .await;
            assert!(state.snapshot("t", PartitionIndex(0)).await == expected);
            assert!(
                expected[0].1.current_txn_first_offset == None
                    && expected[0].1.entry_timestamp == 900
            );
            let probes = [(3, 0, 1), (3, 2, 1), (3, 4, 0), (4, 0, 0), (4, 1, 0)];
            let mut live = Vec::new();
            for (epoch, sequence, delta) in probes {
                live.push(
                    state
                        .check("t", PartitionIndex(0), 42, epoch, sequence, delta)
                        .await,
                );
            }
            partition.log.lock().unwrap().sync().unwrap();
            let writer = partition.take_writer_handle().unwrap();
            drop(partition);
            writer.await.unwrap();
            let reopened = Log::open(directory.path(), LogConfig::default()).unwrap();
            let recovered = ProducerState::new();
            recovered
                .rebuild_from_log("t", PartitionIndex(0), &reopened)
                .await
                .unwrap();
            let mut restored = Vec::new();
            for (epoch, sequence, delta) in probes {
                restored.push(
                    recovered
                        .check("t", PartitionIndex(0), 42, epoch, sequence, delta)
                        .await,
                );
            }
            assert!(live == restored);
            assert!(
                live == if epoch == 3 {
                    vec![
                        Decision::Duplicate { base_offset: 0 },
                        Decision::Duplicate { base_offset: 2 },
                        Decision::Append,
                        Decision::Append,
                        Decision::OutOfOrder,
                    ]
                } else {
                    vec![
                        Decision::Fenced,
                        Decision::Fenced,
                        Decision::Fenced,
                        Decision::Append,
                        Decision::OutOfOrder,
                    ]
                }
            );
        }
    }
}
