//! Reading the partition back after the cleaner has run, and the assertion
//! that says what compaction must have left behind.
//!
//! `fetch_all` exists because the generated `FetchResponse` codec decodes only
//! the first batch of a partition, so collecting the whole log takes a loop of
//! `Fetch` calls. Its result feeds `assert_latest_records_survive`, which is
//! the actual claim of this suite, so the two live together.

use {
    crate::{
        CLIENT_ID,
        kafka_wire::{self, ExchangeSetup as WireExchangeSetup},
        support::{
            fetch::{
                FetchByteLimit, FetchLimits, FetchPartitionSetup, RequestWaitMillis,
                SinglePartitionFetchSetup, fetch_partition, single_partition_fetch,
            },
            wire::HeaderEncoding as RequestHeaderEncoding,
        },
    },
    assert2::assert,
    krabka_ids::{ApiKey as WireApiKey, ApiVersion as WireApiVersion},
    krabka_protocol::{
        owned::{fetch_request::FetchRequest, fetch_response::FetchResponse},
        primitives::uuid::Uuid,
    },
    std::net::SocketAddr,
    tokio::net::TcpStream,
};

/// A flattened record: key and value as plain byte vecs.
#[derive(Debug)]
pub(crate) struct FlatRecord {
    key: Vec<u8>,
    value: Vec<u8>,
}

/// Fetch all records from (topic, partition 0) and start at offset 0.
///
/// The function returns a flat list of (key, value) pairs from all batches.
///
/// The generated `FetchResponse` codec decodes only the FIRST batch from each
/// partition's `records` field and silently discards the rest of the byte
/// stream. To collect every batch, the function fetches again and again. Each
/// time it moves `fetch_offset` past the last batch it saw. It stops when the
/// broker returns no batch.
pub(crate) async fn fetch_all(addr: SocketAddr, topic: &str, topic_id: Uuid) -> Vec<FlatRecord> {
    let version: i16 = 12; // flexible
    let mut out: Vec<FlatRecord> = Vec::new();
    let mut next_offset: i64 = 0;
    loop {
        let req = FetchRequest {
            replica_id: -1,
            ..single_partition_fetch(SinglePartitionFetchSetup {
                topic: topic.to_string(),
                topic_id,
                partition: fetch_partition(FetchPartitionSetup {
                    offset: krabka_ids::Offset(next_offset),
                    maximum: FetchByteLimit(1 << 22),
                    ..Default::default()
                }),
                limits: FetchLimits::wait_for_data_with_maximum(
                    RequestWaitMillis(100),
                    FetchByteLimit(1 << 22),
                ),
            })
        };
        let mut stream = TcpStream::connect(addr).await.expect("connect");
        let resp: FetchResponse = kafka_wire::exchange(&mut stream, &req, WireExchangeSetup {
            api_key: WireApiKey(1),
            version: WireApiVersion(version),
            client_id: CLIENT_ID,
            header: RequestHeaderEncoding::Flexible,
            ..Default::default()
        })
        .await
        .expect("Fetch round-trip");

        let mut got_any = false;
        for topic_resp in &resp.responses {
            for part_resp in &topic_resp.partitions {
                assert!(
                    part_resp.error_code == 0,
                    "Fetch partition error: {}",
                    part_resp.error_code
                );
                if let Some(batches) = part_resp.records.as_ref().and_then(|p| p.as_v2()) {
                    for batch in batches {
                        got_any = true;
                        let batch_last_abs = batch.base_offset + i64::from(batch.last_offset_delta);
                        for record in &batch.records {
                            let key = match &record.key {
                                Some(k) => k.to_vec(),
                                None => continue,
                            };
                            let value = match &record.value {
                                Some(v) => v.to_vec(),
                                None => Vec::new(),
                            };
                            out.push(FlatRecord { key, value });
                        }
                        next_offset = batch_last_abs + 1;
                    }
                }
            }
        }
        if !got_any {
            break;
        }
    }
    out
}

pub(crate) fn assert_latest_records_survive(records: &[FlatRecord]) {
    let distinct_keys: std::collections::BTreeSet<String> = records
        .iter()
        .map(|record| String::from_utf8(record.key.clone()).unwrap())
        .filter(|key| key != "__pad__")
        .collect();
    assert!(
        distinct_keys
            == ["k1".to_string(), "k2".to_string(), "k3".to_string()]
                .into_iter()
                .collect::<std::collections::BTreeSet<_>>(),
        "k1, k2, k3 must all survive compaction; got: {distinct_keys:?}"
    );

    for key in ["k1", "k2", "k3"] {
        let values_for_key: Vec<String> = records
            .iter()
            .filter(|record| record.key == key.as_bytes())
            .map(|record| String::from_utf8(record.value.clone()).unwrap())
            .collect();
        let expected_latest = format!("v10-{key}");
        assert!(
            values_for_key.contains(&expected_latest),
            "key {key} must have latest value {expected_latest}; got {values_for_key:?}"
        );
        for stale_round in 0..10u32 {
            let stale = format!("v{stale_round}-{key}");
            assert!(
                !values_for_key.contains(&stale),
                "key {key} must NOT retain stale value {stale}; got {values_for_key:?}"
            );
        }
    }
}
