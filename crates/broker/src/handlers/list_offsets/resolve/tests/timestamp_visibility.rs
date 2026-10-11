use assert2::assert;

use super::*;
use crate::txn::marker::{MarkerType, build_marker_batch};

#[tokio::test]
async fn timestamp_lookup_matches_the_first_record_in_the_isolation_prefix() {
    const TOPIC: &str = "list-offsets-timestamp-visibility";
    const TIMESTAMPS: [i64; 5] = [100, 300, 200, 400, 300];
    list_offsets_topic_fixture!((broker, _dir, client), TOPIC);
    let broker_arc = broker.broker_arc_for_test();
    let partition = broker_arc
        .partitions
        .get(TOPIC, krabka_ids::PartitionIndex(0))
        .expect("partition");
    for (index, timestamp) in TIMESTAMPS.into_iter().enumerate() {
        let offset =
            produce_one_at_timestamp(&partition, 0, timestamp, (index == 2).then_some(77)).await;
        assert!(offset == i64::try_from(index).expect("record index"));
    }
    // Offset 2 opens a transaction; later nontransactional records are still
    // behind its unstable frontier. Ties and regressions must not skip offset 1.
    for hw in 0..=5 {
        partition.replica_state.lock().await.hw = krabka_log::Offset(hw);
        for isolation in [0, 1] {
            let boundary = if isolation == 1 { hw.min(2) } else { hw };
            for target in [0, 100, 101, 200, 250, 300, 301, 350, 400, 401] {
                let expected = TIMESTAMPS.iter().enumerate().find(|(i, timestamp)| {
                    i64::try_from(*i).expect("record index") < boundary && **timestamp >= target
                });
                let response = list_one_at_version(&broker, TOPIC, (target, isolation), 11).await;
                assert!(response.error_code == codes::NONE, "{response:?}");
                let (offset, timestamp, epoch) = expected.map_or((-1, -1, -1), |(i, timestamp)| {
                    (i64::try_from(i).expect("record index"), *timestamp, 0)
                });
                assert!(
                    response.offset == offset
                        && response.timestamp == timestamp
                        && response.leader_epoch == epoch,
                    "hw={hw}, isolation={isolation}, target={target}: {response:?}"
                );
            }
        }
    }
    let marker = build_marker_batch(
        krabka_log::ProducerId(77),
        0,
        partition.log_end_offset(),
        MarkerType::Commit,
        0,
    );
    assert!(
        partition
            .produce_control_batch(marker)
            .await
            .expect("commit marker")
            .0
            == 5
    );
    // Closing the transaction is insufficient: HWM must pass its marker.
    for (hw, expected) in [(5, (-1, -1, -1)), (6, (3, 400, 0))] {
        partition.replica_state.lock().await.hw = krabka_log::Offset(hw);
        let response = list_one_at_version(&broker, TOPIC, (350, 1), 11).await;
        assert!(response.error_code == codes::NONE);
        assert!((response.offset, response.timestamp, response.leader_epoch) == expected);
    }
    drop(client);
    broker.shutdown().await;
}
