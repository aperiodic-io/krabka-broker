//! `DeleteRecords` (`api_key` 21): trimming a partition from a given offset,
//! the agreement between the response's `low_watermark` and the partition's
//! `log_start_offset`, and the durability of a trim that lands where no segment
//! name can record it.

use {
    crate::{
        admin_harness::create_topic_helper,
        support::{
            self,
            fetch::fetch_topic_row,
            offsets::{
                delete_records_partition, delete_records_request, delete_records_topic,
                list_offset_partition, single_partition_list_offsets,
            },
            records::record_count,
            start_n_node_client,
        },
    },
    assert2::{assert, check},
    krabka_broker::{NodeId, codes},
    krabka_protocol::{
        owned::{
            delete_records_response::DeleteRecordsPartitionResult,
            fetch_request::{FetchPartition, FetchRequest},
            fetch_response::PartitionData,
            list_offsets_request::ListOffsetsRequest,
            list_offsets_response::ListOffsetsPartitionResponse,
        },
        primitives::uuid::Uuid,
    },
};

/// Request timestamp sentinel (-2) asking where the log starts. Kafka's
/// `ListOffsetsRequest.EARLIEST_TIMESTAMP`.
const EARLIEST_TIMESTAMP: i64 = -2;

/// `DeleteRecords`: the test produces 100 records and then trims from offset
/// 50. The response carries a valid `low_watermark`, and the broker's
/// `log_start_offset` moves forward.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delete_records_trims_log_start() {
    let (cluster, client) = start_n_node_client(1, "admin-handlers-test").await;
    let broker = &cluster[0].0;

    create_topic_helper(&client, "t-dr", 1).await;

    // Produce 100 single-record batches through the broker's test helper.
    broker
        .produce_records_for_test("t-dr", 0, 100)
        .await
        .expect("produce_records_for_test");

    let part_result = trim(&client, "t-dr", 50).await;
    check!(
        part_result.error_code == 0,
        "delete_records error: {:?}",
        part_result.error_code
    );
    // low_watermark must be the resulting log_start_offset after trim.
    check!(
        part_result.low_watermark >= 0,
        "low_watermark should be non-negative, got {}",
        part_result.low_watermark
    );
    check!(
        part_result.low_watermark <= 50,
        "low_watermark {} should be <= requested offset 50",
        part_result.low_watermark
    );

    let log_start = broker
        .partition_log_start_for_test("t-dr", 0)
        .expect("partition exists");
    assert!(
        log_start == part_result.low_watermark,
        "partition log_start_offset should equal low_watermark"
    );
}

/// The topic the restart case trims, kept clear of the one above so the two
/// cases never share a partition directory.
const RESTART_TOPIC: &str = "t-dr-restart";

/// Records produced before the restart case trims. `segment.bytes` defaults to
/// 1 GiB, so every one of them lands in the first, still-active segment.
const RESTART_RECORDS: i64 = 100;

/// Where the restart case moves the log start. It is strictly inside the one
/// segment, so no segment name witnesses the trim: the records below it are
/// still in the same file, and only the partition's own checkpoint says they
/// are gone.
const RESTART_TRIM_TO: i64 = 50;

/// What a client can see of a trimmed partition: where `ListOffsets` says the
/// log starts, and what a `Fetch` aimed below that point is answered with.
///
/// The two readings are one value so a case asserts against a whole expected
/// struct rather than against four fields in sequence. A broker that forgot the
/// trim answers offset 0, no error and 50 served records here, so the whole
/// difference shows up in one failure.
#[derive(Debug, PartialEq, Eq)]
struct TrimmedView {
    earliest_error_code: i16,
    earliest_offset: i64,
    fetch_error_code: i16,
    fetch_log_start_offset: i64,
    fetched_records: usize,
}

/// A `DeleteRecords` that lands inside the active segment survives a restart.
///
/// A trim onto a segment boundary needs nothing durable of its own: the
/// segments below it are deleted, and the first surviving base offset *is* the
/// log start. A trim inside a segment has no such witness, so the reopened log
/// has to read the start back from its own checkpoint. Without that, the broker
/// serves records an operator already deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_trim_inside_the_active_segment_survives_a_restart() {
    // The directory outlives both brokers, which is what makes the second boot
    // a restart rather than a fresh cluster.
    let dir = tempfile::tempdir().expect("tempdir");
    let records = usize::try_from(RESTART_RECORDS).expect("record count fits usize");

    {
        let (broker, client) = support::start_with_dir(dir.path()).await;
        create_topic_helper(&client, RESTART_TOPIC, 1).await;
        broker
            .wait_until_local_partition_leader(RESTART_TOPIC, 0, NodeId(1))
            .await;
        broker
            .produce_records_for_test(RESTART_TOPIC, 0, records)
            .await
            .expect("produce_records_for_test");
        // A trim is bounded by the high watermark, so the whole log has to be
        // acknowledged before the requested offset is reachable.
        broker
            .wait_until_high_watermark(RESTART_TOPIC, 0, RESTART_RECORDS)
            .await;

        check!(
            segment_base_offsets(dir.path(), RESTART_TOPIC) == vec![0],
            "the case needs every record in one segment, or a deleted segment \
             name would carry the trim on its own"
        );

        let trimmed = trim(&client, RESTART_TOPIC, RESTART_TRIM_TO).await;
        check!(
            trimmed.error_code == codes::NONE,
            "delete_records: {trimmed:?}"
        );
        check!(trimmed.low_watermark == RESTART_TRIM_TO);
        check!(
            broker.partition_log_start_for_test(RESTART_TOPIC, 0) == Some(RESTART_TRIM_TO),
            "the trim moved the live partition's log start"
        );

        broker.shutdown().await;
    }

    // Nothing about the trim is in the segment names: the file that holds the
    // deleted records is still there, and it still starts at 0.
    check!(segment_base_offsets(dir.path(), RESTART_TOPIC) == vec![0]);

    let (broker, client) = support::start_with_dir(dir.path()).await;
    broker
        .wait_until_local_partition_leader(RESTART_TOPIC, 0, NodeId(1))
        .await;

    assert!(
        trimmed_view(&client, RESTART_TOPIC, 0).await
            == TrimmedView {
                earliest_error_code: codes::NONE,
                earliest_offset: RESTART_TRIM_TO,
                fetch_error_code: codes::OFFSET_OUT_OF_RANGE,
                fetch_log_start_offset: -1,
                fetched_records: 0,
            }
    );

    broker.shutdown().await;
}

/// Trim partition 0 of `topic` from `offset` and return the partition row.
async fn trim(
    client: &krabka_client_core::Client,
    topic: &str,
    offset: i64,
) -> DeleteRecordsPartitionResult {
    let mut resp = client
        .send(delete_records_request(
            vec![delete_records_topic(topic, vec![delete_records_partition(
                0, offset,
            )])],
            5_000,
        ))
        .await
        .expect("delete_records");
    resp.topics.remove(0).partitions.remove(0)
}

/// Base offsets of the `.log` segment files in partition 0 of `topic`, sorted.
///
/// A case reads this to say out loud which segments exist, so "the trim landed
/// inside the active segment" is a checked precondition rather than an
/// assumption about the default `segment.bytes`.
fn segment_base_offsets(log_dir: &std::path::Path, topic: &str) -> Vec<i64> {
    let dir = krabka_log::name::partition_dir(log_dir, topic, 0);
    let mut bases: Vec<i64> = std::fs::read_dir(&dir)
        .expect("read partition dir")
        .map(|entry| entry.expect("partition dir entry").file_name())
        .filter_map(|name| krabka_log::name::parse_log_filename(&name.to_string_lossy()).ok())
        .collect();
    bases.sort_unstable();
    bases
}

/// Read partition 0 of `topic` the way a client would: `ListOffsets` at the
/// `EARLIEST` sentinel, and a `Fetch` from `fetch_offset`.
async fn trimmed_view(
    client: &krabka_client_core::Client,
    topic: &str,
    fetch_offset: i64,
) -> TrimmedView {
    let topic_id = support::topic_id_for(client, topic).await;
    let earliest = list_earliest(client, topic).await;
    let fetched = fetch_from(client, topic, topic_id, fetch_offset).await;
    TrimmedView {
        earliest_error_code: earliest.error_code,
        earliest_offset: earliest.offset,
        fetch_error_code: fetched.error_code,
        fetch_log_start_offset: fetched.log_start_offset,
        fetched_records: record_count(fetched.records.as_ref()),
    }
}

/// `ListOffsets` for partition 0 of `topic` at Kafka's `EARLIEST_TIMESTAMP`,
/// which is where the log starts.
async fn list_earliest(
    client: &krabka_client_core::Client,
    topic: &str,
) -> ListOffsetsPartitionResponse {
    let mut resp = client
        .send(ListOffsetsRequest {
            replica_id: -1,
            timeout_ms: 5_000,
            ..single_partition_list_offsets(topic, list_offset_partition(0, EARLIEST_TIMESTAMP))
        })
        .await
        .expect("ListOffsets");
    resp.topics.remove(0).partitions.remove(0)
}

/// One non-blocking `Fetch` of partition 0 of `topic` from `fetch_offset`.
async fn fetch_from(
    client: &krabka_client_core::Client,
    topic: &str,
    topic_id: Uuid,
    fetch_offset: i64,
) -> PartitionData {
    let mut resp = client
        .send(FetchRequest {
            // A snapshot read: an empty answer comes back at once rather than
            // parking in the long poll.
            max_wait_ms: 0,
            min_bytes: 1,
            max_bytes: 1 << 20,
            topics: vec![fetch_topic_row(topic, topic_id, vec![FetchPartition {
                partition: 0,
                fetch_offset,
                partition_max_bytes: 1 << 20,
                ..Default::default()
            }])],
            ..Default::default()
        })
        .await
        .expect("Fetch");
    resp.responses.remove(0).partitions.remove(0)
}
