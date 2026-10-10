//! Multi-RPC sequences against an in-process broker, driven through
//! `krabka-client-core`. These run on every push (no Docker required).

use assert2::{assert, check};

use crate::support::{
    client::connect_client,
    discovery::{api_versions_request_for, topic_metadata_request},
    fetch::single_partition_fetch,
    offsets::{list_offset_partition, single_partition_list_offsets},
    records::value_record,
    topics::{creatable_topic, create_topic_request},
};
mod support;

use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use krabka_protocol::{
    Encode,
    owned::{
        list_offsets_request::{ListOffsetsPartition, ListOffsetsRequest},
        metadata_request::MetadataRequest,
    },
    records::{Record, RecordBatch},
};
use support::topic_id_for;
use tokio::io::AsyncReadExt;

/// Build a `RecordBatch` with one entry per provided value. Codegen's
/// `PartitionProduceData.records` is `Option<RecordsPayload>`. Callers
/// pass the batch by value and `.into()` it at the assignment site.
fn record_batch_with_values(values: &[&str]) -> RecordBatch {
    let len_i32 = i32::try_from(values.len()).expect("test fixture small enough for i32");
    let len_i64 = i64::try_from(values.len()).expect("test fixture small enough for i64");
    let mut batch = RecordBatch {
        last_offset_delta: (len_i32 - 1).max(0),
        max_timestamp: len_i64,
        ..RecordBatch::default()
    };
    for (i, v) in values.iter().enumerate() {
        batch.records.push(value_record(
            i32::try_from(i).expect("test fixture small enough for i32"),
            Some(Bytes::from(v.to_string())),
        ));
    }
    batch
}

/// One record per `(value, timestamp)` pair. `base_timestamp` is the
/// first timestamp. Each record's `timestamp_delta` reconstructs the
/// requested absolute timestamp. `max_timestamp` is the largest
/// timestamp.
fn timestamped_batch(entries: &[(&str, i64)]) -> RecordBatch {
    let base_ts = entries.first().map_or(0, |(_, ts)| *ts);
    let max_ts = entries.iter().map(|(_, ts)| *ts).max().unwrap_or(0);
    let len_i32 = i32::try_from(entries.len()).expect("small");
    let mut batch = RecordBatch {
        base_timestamp: base_ts,
        max_timestamp: max_ts,
        last_offset_delta: (len_i32 - 1).max(0),
        ..RecordBatch::default()
    };
    for (i, (v, ts)) in entries.iter().enumerate() {
        batch.records.push(Record {
            timestamp_delta: ts - base_ts,
            ..value_record(
                i32::try_from(i).expect("small"),
                Some(Bytes::from((*v).to_string())),
            )
        });
    }
    batch
}

#[tokio::test]
async fn list_offsets_by_timestamp_local() {
    let p = support::start().await;

    p.client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("by_ts").into(),
                ..Default::default()
            },
        )))
        .await
        .unwrap();
    let topic_id = topic_id_for(&p.client, "by_ts").await;

    // Offsets 0..=2 with timestamps 100, 200, 300.
    p.client
        .send(crate::support::produce::batch_request(
            timestamped_batch(&[("a", 100), ("b", 200), ("c", 300)]),
            crate::support::produce::SinglePartitionProduceSetup {
                topic: ("by_ts").into(),
                topic_id,
                ..Default::default()
            },
        ))
        .await
        .unwrap();

    let query = |ts: i64| {
        let client = p.client.clone();
        async move {
            client
                .send(ListOffsetsRequest {
                    replica_id: -1,
                    ..single_partition_list_offsets("by_ts", list_offset_partition(0, ts))
                })
                .await
                .unwrap()
        }
    };

    // Positive timestamp: first record with ts >= 150 is offset 1 (ts 200).
    let r = query(150).await;
    check!(r.topics[0].partitions[0].error_code == 0);
    check!(r.topics[0].partitions[0].offset == 1);
    check!(r.topics[0].partitions[0].timestamp == 200);

    // EARLIEST_LOCAL (-4) → local log start = 0.
    let r = query(-4).await;
    assert!(r.topics[0].partitions[0].offset == 0);

    // MAX_TIMESTAMP (-3) → offset 2 (ts 300), echoes timestamp 300.
    let r = query(-3).await;
    assert!(r.topics[0].partitions[0].offset == 2);
    assert!(r.topics[0].partitions[0].timestamp == 300);
}

#[tokio::test]
async fn end_to_end_create_produce_fetch_delete() {
    let p = support::start().await;

    // 1. ApiVersions.
    let v = p
        .client
        .send(api_versions_request_for("krabka", "0.0.0"))
        .await
        .unwrap();
    assert!(v.error_code == 0);

    // 2. CreateTopics.
    let cr = p
        .client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("e2e").into(),
                ..Default::default()
            },
        )))
        .await
        .unwrap();
    assert!(cr.topics[0].error_code == 0);

    // 3. Metadata — confirm topic is visible and grab its UUID.
    let meta = p.client.send(topic_metadata_request(None)).await.unwrap();
    assert!(meta.topics.iter().any(|t| t.name.as_deref() == Some("e2e")));
    let topic_id = topic_id_for(&p.client, "e2e").await;

    // 4. Produce 3 records.
    let pr = p
        .client
        .send(crate::support::produce::batch_request(
            record_batch_with_values(&["a", "b", "c"]),
            crate::support::produce::SinglePartitionProduceSetup {
                topic: ("e2e").into(),
                topic_id,
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    assert!(pr.responses[0].partition_responses[0].error_code == 0);

    // 5. ListOffsets — latest after producing 3 records is 3.
    let lo = p
        .client
        .send(ListOffsetsRequest {
            replica_id: -1,
            ..single_partition_list_offsets(
                "e2e",
                ListOffsetsPartition {
                    partition_index: 0,
                    timestamp: -1, // latest
                    ..Default::default()
                },
            )
        })
        .await
        .unwrap();
    assert!(lo.topics[0].partitions[0].error_code == 0);
    assert!(lo.topics[0].partitions[0].offset == 3);

    // 6. Fetch and confirm 3 records are returned.
    let fr = p
        .client
        .send(single_partition_fetch(
            crate::support::fetch::SinglePartitionFetchSetup {
                topic: "e2e".into(),
                topic_id,
                limits: crate::support::fetch::FetchLimits::one_mebibyte(
                    crate::support::fetch::RequestWaitMillis(100),
                ),
                ..Default::default()
            },
        ))
        .await
        .unwrap();
    crate::support::fetch::check_record_count(&fr.responses[0].partitions[0], 3);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn produce_acks_zero_sends_no_frame_and_keeps_connection_usable() {
    let p = support::start().await;
    let create = p
        .client
        .send(create_topic_request(creatable_topic(
            crate::support::topics::ConfiguredTopicSetup {
                name: ("one-way-produce").into(),
                ..Default::default()
            },
        )))
        .await
        .expect("create topic");
    assert!(create.topics[0].error_code == 0);

    let mut stream = tokio::net::TcpStream::connect(p.broker.listen_addr())
        .await
        .expect("connect raw client");
    let produce = crate::support::produce::batch_request(
        record_batch_with_values(&["value"]),
        crate::support::produce::SinglePartitionProduceSetup {
            topic: ("one-way-produce").into(),
            acknowledgements: crate::support::produce::ProduceAcknowledgements::NoResponse,
            ..Default::default()
        },
    );
    let mut body = BytesMut::new();
    produce.encode(&mut body, 9).expect("encode Produce v9");
    let client_id = b"acks-zero-test";
    let mut frame = BytesMut::new();
    frame.put_i16(0);
    frame.put_i16(9);
    frame.put_i32(1);
    frame.put_i16(i16::try_from(client_id.len()).unwrap());
    frame.put_slice(client_id);
    frame.put_u8(0);
    frame.put_slice(&body);
    crate::support::wire::write_frame(&mut stream, &frame, None)
        .await
        .unwrap();

    let unexpected_response =
        tokio::time::timeout(Duration::from_millis(150), stream.readable()).await;
    assert!(
        unexpected_response.is_err(),
        "acks=0 must not make a response frame readable"
    );

    let mut metadata_body = BytesMut::new();
    MetadataRequest::default()
        .encode(&mut metadata_body, 12)
        .expect("encode Metadata v12");
    let mut metadata_frame = BytesMut::new();
    metadata_frame.put_i16(3);
    metadata_frame.put_i16(12);
    metadata_frame.put_i32(2);
    metadata_frame.put_i16(i16::try_from(client_id.len()).unwrap());
    metadata_frame.put_slice(client_id);
    metadata_frame.put_u8(0);
    metadata_frame.put_slice(&metadata_body);
    crate::support::wire::write_frame(&mut stream, &metadata_frame, None)
        .await
        .unwrap();

    let response_len = tokio::time::timeout(Duration::from_secs(5), stream.read_u32())
        .await
        .expect("Metadata response arrives")
        .expect("read Metadata frame length");
    let mut response = vec![0u8; response_len as usize];
    stream.read_exact(&mut response).await.unwrap();
    assert!(i32::from_be_bytes(response[..4].try_into().unwrap()) == 2);

    let latest = p
        .client
        .send(ListOffsetsRequest {
            replica_id: -1,
            ..single_partition_list_offsets("one-way-produce", list_offset_partition(0, -1))
        })
        .await
        .expect("read log end");
    assert!(latest.topics[0].partitions[0].offset == 1);

    p.broker.shutdown().await;
}

#[tokio::test]
async fn second_open_recovers_partitions_from_disk() {
    let dir = tempfile::tempdir().unwrap();
    {
        let config = krabka_broker::BrokerConfig::for_tests(dir.path().to_path_buf());
        let handle = krabka_broker::Broker::start(config).await.unwrap();
        let bootstrap = handle.listen_addr().to_string();
        let client = connect_client(&bootstrap, Some("recovery-test")).await;
        let cr = client
            .send(create_topic_request(creatable_topic(
                crate::support::topics::ConfiguredTopicSetup {
                    name: ("persisted").into(),
                    partitions: crate::support::topics::TopicPartitionCount(2),
                    ..Default::default()
                },
            )))
            .await
            .unwrap();
        assert!(cr.topics[0].error_code == 0);
        handle.shutdown().await;
    }
    // Reopen on the same log_dir. Must use Rejoin because the raft log
    // already exists from the first run; Bootstrap would be rejected.
    let mut config = krabka_broker::BrokerConfig::for_tests(dir.path().to_path_buf());
    config.bootstrap_mode = krabka_broker::BootstrapMode::Rejoin;
    let handle = krabka_broker::Broker::start(config).await.unwrap();
    let bootstrap = handle.listen_addr().to_string();
    let client = connect_client(&bootstrap, Some("recovery-test")).await;
    let meta = client.send(topic_metadata_request(None)).await.unwrap();
    let t = meta
        .topics
        .iter()
        .find(|t| t.name.as_deref() == Some("persisted"))
        .expect("recovered topic visible in metadata");
    assert!(t.partitions.len() == 2);
    handle.shutdown().await;
}
