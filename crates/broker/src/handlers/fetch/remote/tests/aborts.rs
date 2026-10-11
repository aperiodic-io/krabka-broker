use assert2::assert;
use krabka_ids::{LeaderEpoch, Offset, PartitionIndex};
use krabka_log::{Log, LogConfig};
use krabka_protocol::{
    owned::fetch_response::AbortedTransaction,
    records::{Attributes, Record, RecordBatch},
};

use super::{TIERED_TOPIC, out_of_range_at, tiered_broker, tiered_topic_id};
use crate::{
    broker::spawn_partition,
    remote_log_manager::{ArchiveMode, copy_eligible, test_support::tier},
};

fn data_batch(transactional: bool) -> RecordBatch {
    RecordBatch {
        producer_id: if transactional { 7777 } else { -1 },
        producer_epoch: 0,
        attributes: Attributes::default().with_transactional(transactional),
        records: vec![Record {
            value: Some(bytes::Bytes::from(vec![b'x'; 64])),
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn abort_marker() -> RecordBatch {
    RecordBatch {
        producer_id: 7777,
        producer_epoch: 0,
        attributes: Attributes::default()
            .with_transactional(true)
            .with_control(true),
        records: vec![Record {
            key: Some(bytes::Bytes::from_static(&[0, 0, 0, 0])),
            value: Some(bytes::Bytes::from_static(&[0, 0, 0, 0, 0, 0])),
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test]
async fn remote_fetch_reports_an_abort_in_a_later_segment_or_the_local_tail() {
    for (archive_marker, retain_marker) in [(true, false), (false, true), (true, true)] {
        let (handle, dir, _remote) = tiered_broker().await;
        let broker = handle.broker_arc_for_test();
        let part_dir = dir.path().join(format!("{TIERED_TOPIC}-0"));
        let mut log = Log::open(
            &part_dir,
            LogConfig {
                segment_size: krabka_units::bytes(1),
                remote_storage_enable: true,
                ..Default::default()
            },
        )
        .unwrap();
        log.append(&mut data_batch(true)).unwrap();
        log.append(&mut data_batch(false)).unwrap();
        log.append(&mut abort_marker()).unwrap();
        if archive_marker {
            log.append(&mut data_batch(false)).unwrap();
        }
        let end = log.log_end_offset();
        log.release_replicated_transactions(end);
        assert!(log.last_stable_offset(end) == end);
        assert!(log.aborted_in_range(Offset(0), Offset(1)).len() == 1);
        log.sync().unwrap();
        let exports = log.tierable_segments();
        assert!(exports[0].transaction_index_path.is_none());
        assert!(
            exports
                .iter()
                .any(|segment| segment.transaction_index_path.is_some())
                == archive_marker
        );
        let copied_end = exports.last().unwrap().last_offset + 1;
        let reader = broker.remote_reader.clone().unwrap();
        let tp = krabka_remote_storage::TopicIdPartition::new(tiered_topic_id(), TIERED_TOPIC, 0);
        assert!(
            copy_eligible(
                &tier(ArchiveMode::Mutable, &reader.rsm, &reader.rlmm),
                &tp,
                1,
                LeaderEpoch(0),
                exports
            )
            .await
                > 0
        );
        log.delete_local_segments_through(if retain_marker { Offset(1) } else { copied_end })
            .unwrap();
        assert!(log.local_log_start_offset() > Offset(0));
        let part = spawn_partition(
            TIERED_TOPIC.into(),
            PartitionIndex(0),
            dir.path().to_path_buf(),
            log,
            broker.log_dir_status.clone(),
            broker.producer_state.clone(),
            false,
        );
        part.replica_state.lock().await.hw = end;
        let mut pending = out_of_range_at(&part, 0);
        pending.read_committed = true;
        assert!(
            super::super::try_remote_read(&broker, &mut pending, &part)
                .await
                .is_some_and(|n| n > 0)
        );
        assert!(
            pending.out.aborted_transactions
                == Some(vec![AbortedTransaction {
                    producer_id: 7777,
                    first_offset: 0,
                    ..Default::default()
                }]),
            "archived marker {archive_marker}, retained marker {retain_marker}: {:?}",
            pending.out
        );
        handle.shutdown().await;
    }
}
