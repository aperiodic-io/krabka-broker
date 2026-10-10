//! `DeleteRecords` on a tiered topic, over the wire.
//!
//! KIP-405 gives a partition two floors. `local.retention.*` moves the local
//! one and leaves the records reachable through the remote tier;
//! `DeleteRecords` moves the global one and takes them out of every tier. The
//! test drives both in order on one topic, so the second observation is
//! measured against a remote tier that has just been shown to answer:
//!
//! * offset 0 reads back after the copy pass evicted its local segment, and
//! * the same read is `OFFSET_OUT_OF_RANGE` once `DeleteRecords` moves the
//!   global floor past it, and
//! * the remote-retention pass then frees the segment bytes the floor
//!   breached, rather than leaving them listed, fetchable and billed until
//!   time or size retention happens to reach them.

use std::time::{Duration, Instant};

use assert2::{assert, check};
use krabka_client_admin::{AdminClient, DeleteRecordsOp};
use krabka_client_core::Client;
use krabka_protocol::primitives::uuid::Uuid as WireUuid;

use crate::{
    rlmm_cluster::{
        await_activation, await_tiered_config, build_client, start_broker_with_topic_rlmm,
    },
    rlmm_round_trip::remote_log_files,
    run_broker_test,
    support::{
        fetch::{fetch_partition, single_partition_fetch},
        topic_id_for,
    },
};

const TOPIC: &str = "tiered-delete-records-itest";
/// Well inside the 80 records the test produces, and above the first sealed
/// segment, so the floor lands between two remote segments.
const DELETE_THROUGH: i64 = 40;

/// A fetch below the offset an operator passed to `DeleteRecords` is
/// `OFFSET_OUT_OF_RANGE` on a tiered topic, and the remote copies below the
/// new floor are freed.
#[test]
fn delete_records_puts_the_tiered_prefix_out_of_range_and_frees_it() {
    run_broker_test(delete_records_puts_the_tiered_prefix_out_of_range_and_frees_it_case());
}

async fn delete_records_puts_the_tiered_prefix_out_of_range_and_frees_it_case() {
    let (broker, _log_dir, remote_dir) = start_broker_with_topic_rlmm().await;
    await_activation(&broker).await;

    let client = build_client(&broker).await;
    create_tiered_topic(&client).await;
    await_tiered_config(&broker, TOPIC).await;

    broker
        .produce_records_for_test(TOPIC, 0, 80)
        .await
        .expect("produce records");
    await_copied_segments(remote_dir.path(), 2).await;

    let topic_id = topic_id_for(&client, TOPIC).await;
    // The local segments are evicted by `local.retention.bytes=1`, so this
    // read proves the remote tier answers for offset 0 before anything asks
    // it to stop.
    let served = await_fetch_records(&client, topic_id, 0).await;
    assert!(
        served == Some(b"test-record-0".to_vec()),
        "offset 0 should read back from the remote tier, got {served:?}"
    );

    // Name the objects the archive holds before the floor moves, so the wait
    // below is not confused by the copy task adding more.
    let tiered_before = remote_log_files(remote_dir.path());

    let mut admin = AdminClient::connect(&[broker.listen_addr().to_string()])
        .await
        .expect("admin connect");
    let outcomes = admin
        .delete_records(
            &[DeleteRecordsOp {
                topic: TOPIC.to_string(),
                partition: 0,
                offset: DELETE_THROUGH,
            }],
            krabka_units::secs(5),
        )
        .await
        .expect("DeleteRecords");
    assert!(
        outcomes.len() == 1 && outcomes[0].error_code == 0,
        "DeleteRecords failed: {outcomes:?}"
    );
    check!(outcomes[0].low_watermark == DELETE_THROUGH);

    // The RLMM still lists the segment that holds offset 0 and the archive
    // still has its bytes, and the fetch must refuse it all the same.
    let refused = fetch_once(&client, topic_id, 0).await;
    check!(
        refused.error_code == 1,
        "offset 0 should be OFFSET_OUT_OF_RANGE after DeleteRecords, got {}",
        refused.error_code
    );
    // Kafka's non-tiered OFFSET_OUT_OF_RANGE row carries -1 offsets.
    check!(refused.log_start_offset == -1);
    // The floor is not a wall across the whole partition: what is left above
    // it still reads.
    let above = await_fetch_records(&client, topic_id, DELETE_THROUGH).await;
    check!(above.is_some(), "the records above the floor still read");

    // The expiration pass frees what the floor breached. `retention.ms` and
    // `retention.bytes` are both off on this topic, so the log-start breach
    // is the only axis that can delete anything here.
    await_remote_segments_freed(&tiered_before).await;

    drop(client);
    broker.shutdown().await;
}

async fn create_tiered_topic(client: &Client) {
    crate::topic_fixture::create_configured_topic(
        client,
        TOPIC,
        crate::support::topics::topic_configs([
            ("remote.storage.enable", "true"),
            ("internal.segment.bytes", "1024"),
            // Evict every copied segment from local disk, so the reads
            // below go to the remote tier.
            ("local.retention.bytes", "1"),
            // No total retention at all: the only thing that may
            // delete a remote segment in this test is the log-start
            // breach. `produce_records_for_test` stamps no record
            // timestamp, so the default 7-day `retention.ms` would
            // otherwise expire every copied segment on the first tick.
            ("retention.bytes", "-1"),
            ("retention.ms", "-1"),
        ]),
    )
    .await;
}

/// Wait for the copy task to tier at least `want` segments.
// intentional: remote-tier object presence is filesystem state on the
// `LocalTieredStorage` backend — it is not in the metadata image and has no
// broker metric, so poll the remote dir directly (bounded loop).
async fn await_copied_segments(remote_dir: &std::path::Path, want: usize) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if remote_log_files(remote_dir).len() >= want {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "fewer than {want} segments tiered within 30s"
        );
        tokio::task::yield_now().await;
    }
}

/// Wait for the remote-retention pass to free one of the segment objects that
/// existed before the floor moved.
///
/// The named objects, rather than a count: the copy task is still adding
/// segments while this runs, so a total that stays level says nothing.
// intentional: same filesystem-state observation as `await_copied_segments`,
// in the other direction.
async fn await_remote_segments_freed(before: &[std::path::PathBuf]) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if before.iter().any(|path| !path.exists()) {
            return;
        }
        assert!(
            Instant::now() <= deadline,
            "the log-start breach freed none of the {} segment objects within 30s",
            before.len()
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn fetch_once(
    client: &Client,
    topic_id: WireUuid,
    fetch_offset: i64,
) -> krabka_protocol::owned::fetch_response::PartitionData {
    let resp = client
        .send(single_partition_fetch(
            crate::support::fetch::SinglePartitionFetchSetup {
                topic: TOPIC.into(),
                topic_id,
                partition: fetch_partition(crate::support::fetch::FetchPartitionSetup {
                    offset: krabka_ids::Offset(fetch_offset),
                    ..Default::default()
                }),
                limits: crate::support::fetch::FetchLimits::wait_for_data(
                    crate::support::fetch::RequestWaitMillis(500),
                ),
            },
        ))
        .await
        .expect("Fetch");
    resp.responses
        .into_iter()
        .next()
        .and_then(|topic| topic.partitions.into_iter().next())
        .expect("the response carries the requested partition")
}

/// Fetch `fetch_offset` until it returns records, and give back the first
/// record's value. Absorbs the local-retention eviction race the same way the
/// round-trip body does.
// intentional: this drives the wire Fetch API and inspects the returned
// records — a wire-response poll with no backing metric/image signal, so keep
// the bounded retry loop.
async fn await_fetch_records(
    client: &Client,
    topic_id: WireUuid,
    fetch_offset: i64,
) -> Option<Vec<u8>> {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let partition = fetch_once(client, topic_id, fetch_offset).await;
        if let Some(value) = partition
            .records
            .as_ref()
            .and_then(krabka_protocol::records::RecordsPayload::as_v2)
            .and_then(|batches| batches.first())
            .and_then(|batch| batch.records.first())
        {
            return Some(value.value.clone().unwrap_or_default().to_vec());
        }
        if Instant::now() > deadline {
            return None;
        }
        tokio::task::yield_now().await;
    }
}
