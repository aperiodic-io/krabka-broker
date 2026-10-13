//! The rule itself under the record-key identity: the first copy wins, a
//! dropped record leaves its offset empty, and the producer's `i`-th record
//! is at `base_offset + i` whether or not it was kept.

use {
    crate::{
        harness::{Topic, appended_at, rec, recs, stored, stored_key, topic},
        support::{
            self,
            client::connect_to_broker,
            dedup::{Rec, StoredBatch, StoredRecord, keyed_batch, produce},
            fetch::{FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch},
        },
    },
    assert2::{assert, check},
    futures_util::future::join_all,
    krabka_protocol::records::RecordsPayload,
};

/// Inside one request the rule applies in record order: the first record of
/// an identity is kept with its own value, and every later copy in the same
/// batch is dropped and leaves its offset empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn within_one_request_the_first_copy_of_a_key_is_kept() {
    let p = support::start().await;
    let cases: [(&str, Vec<Rec>, Vec<StoredRecord>); 3] = [
        (
            "one key twice",
            vec![rec("a", "a-1"), rec("a", "a-2")],
            vec![stored(0, "a", "a-1")],
        ),
        (
            "keys repeating among others",
            vec![
                rec("a", "a-1"),
                rec("b", "b-1"),
                rec("a", "a-2"),
                rec("c", "c-1"),
                rec("b", "b-2"),
                rec("a", "a-3"),
            ],
            vec![
                stored(0, "a", "a-1"),
                stored(1, "b", "b-1"),
                stored(3, "c", "c-1"),
            ],
        ),
        (
            "every record the same key",
            vec![
                rec("k", "k-1"),
                rec("k", "k-2"),
                rec("k", "k-3"),
                rec("k", "k-4"),
            ],
            vec![stored(0, "k", "k-1")],
        ),
    ];
    for (index, (name, records, kept)) in cases.into_iter().enumerate() {
        let topic_name = format!("within-{index}");
        let t = topic().p(&p).name(&topic_name).call().await;
        let last = i64::try_from(records.len()).unwrap() - 1;

        let row = t.send(&records).await;
        let contents = t.contents().await;

        check!(row == appended_at(0), "{name}");
        check!(contents.records == kept, "{name}");
        check!(
            contents.batches
                == [StoredBatch {
                    base_offset: 0,
                    last_offset: last,
                    records: kept.len(),
                }],
            "{name}"
        );
        check!(contents.high_watermark == last + 1, "{name}");
    }
    p.broker.shutdown().await;
}

/// Across requests the earliest copy wins too, and a consumer reads the value
/// that was written first, never a later copy's value.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn across_requests_consumers_read_the_first_value() {
    let p = support::start().await;
    let t = topic().p(&p).name("across").call().await;

    let mut rows = Vec::new();
    for records in [
        vec![rec("a", "a-first"), rec("b", "b-first")],
        vec![
            rec("b", "b-second"),
            rec("c", "c-first"),
            rec("a", "a-second"),
        ],
        vec![
            rec("c", "c-second"),
            rec("d", "d-first"),
            rec("a", "a-third"),
        ],
    ] {
        rows.push(t.send(&records).await);
    }
    let contents = t.contents().await;

    assert!(rows == [appended_at(0), appended_at(2), appended_at(5)]);
    assert!(
        contents.records
            == [
                stored(0, "a", "a-first"),
                stored(1, "b", "b-first"),
                stored(3, "c", "c-first"),
                stored(6, "d", "d-first"),
            ]
    );
    assert!(contents.high_watermark == 8);
    p.broker.shutdown().await;
}

/// A partial drop keeps each kept record at the offset the producer was told,
/// wherever in the batch the duplicates sit, and the batch keeps its whole
/// offset span.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_partial_drop_keeps_every_kept_record_at_its_producer_offset() {
    let p = support::start().await;
    let cases: [(&str, &[&str], Vec<StoredRecord>); 4] = [
        ("the first record", &["a", "x", "y"], vec![
            stored_key(3, "x"),
            stored_key(4, "y"),
        ]),
        ("the last record", &["x", "y", "b"], vec![
            stored_key(2, "x"),
            stored_key(3, "y"),
        ]),
        ("a middle record", &["x", "b", "y"], vec![
            stored_key(2, "x"),
            stored_key(4, "y"),
        ]),
        ("every other record", &["a", "x", "b", "y"], vec![
            stored_key(3, "x"),
            stored_key(5, "y"),
        ]),
    ];
    for (index, (dropped, keys, tail)) in cases.into_iter().enumerate() {
        let topic_name = format!("partial-{index}");
        let t = topic().p(&p).name(&topic_name).call().await;
        let span = i64::try_from(keys.len()).unwrap();

        let seeded = t.send(&recs(&["a", "b"])).await;
        let row = t.send(&recs(keys)).await;
        let next = t.send(&recs(&["z"])).await;
        let contents = t.contents().await;

        let mut expected = vec![stored_key(0, "a"), stored_key(1, "b")];
        expected.extend(tail.iter().cloned());
        expected.push(stored_key(2 + span, "z"));
        check!(
            [seeded, row, next] == [appended_at(0), appended_at(2), appended_at(2 + span)],
            "dropping {dropped}"
        );
        check!(contents.records == expected, "dropping {dropped}");
        check!(
            contents.batches
                == [
                    StoredBatch {
                        base_offset: 0,
                        last_offset: 1,
                        records: 2,
                    },
                    StoredBatch {
                        base_offset: 2,
                        last_offset: 1 + span,
                        records: tail.len(),
                    },
                    StoredBatch {
                        base_offset: 2 + span,
                        last_offset: 2 + span,
                        records: 1,
                    },
                ],
            "dropping {dropped}"
        );
    }
    p.broker.shutdown().await;
}

/// A request whose records are all duplicates succeeds with its own base
/// offset. Its span is an empty batch in the log, the high watermark passes
/// it, and the next request appends after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_all_duplicate_request_reserves_its_span() {
    let p = support::start().await;
    let t = topic().p(&p).name("all-duplicate").call().await;

    let first = t.send(&recs(&["a", "b", "c"])).await;
    let duplicates = t.send(&recs(&["c", "a", "b"])).await;
    let after_duplicates = t.contents().await;
    let next = t.send(&recs(&["d"])).await;
    let contents = t.contents().await;

    assert!([first, duplicates, next] == [appended_at(0), appended_at(3), appended_at(6)]);
    assert!(after_duplicates.high_watermark == 6);
    assert!(
        contents.records
            == [
                stored_key(0, "a"),
                stored_key(1, "b"),
                stored_key(2, "c"),
                stored_key(6, "d"),
            ]
    );
    assert!(
        contents.batches
            == [
                StoredBatch {
                    base_offset: 0,
                    last_offset: 2,
                    records: 3,
                },
                StoredBatch {
                    base_offset: 3,
                    last_offset: 5,
                    records: 0,
                },
                StoredBatch {
                    base_offset: 6,
                    last_offset: 6,
                    records: 1,
                },
            ]
    );
    assert!(contents.high_watermark == 7);
    p.broker.shutdown().await;
}

/// Under the record-key rule a null key has no identity, so every keyless
/// record is appended. An empty key is an identity like any other.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn null_keys_are_never_deduplicated_and_an_empty_key_is_an_identity() {
    let p = support::start().await;
    let t = topic().p(&p).name("null-and-empty").call().await;
    let empty = |value: &str| Rec {
        key: Some(Vec::new()),
        ..Rec::keyless(value)
    };

    let first = t
        .send(&[
            Rec::keyless("n"),
            Rec::keyless("n"),
            empty("e-1"),
            empty("e-2"),
        ])
        .await;
    let second = t.send(&[Rec::keyless("n"), empty("e-3")]).await;
    let contents = t.contents().await;

    let keyless = |offset| StoredRecord {
        offset,
        key: None,
        value: Some(b"n".to_vec()),
    };
    assert!([first, second] == [appended_at(0), appended_at(4)]);
    assert!(
        contents.records
            == [
                keyless(0),
                keyless(1),
                StoredRecord {
                    offset: 2,
                    key: Some(Vec::new()),
                    value: Some(b"e-1".to_vec()),
                },
                keyless(4),
            ]
    );
    assert!(contents.high_watermark == 6);
    p.broker.shutdown().await;
}

/// Each partition has its own index: one key is kept once in each partition,
/// and a duplicate is dropped only against its own partition.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn partitions_deduplicate_independently() {
    let p = support::start().await;
    let t = topic()
        .p(&p)
        .name("two-partitions")
        .partitions(2)
        .call()
        .await;

    for partition in [0, 1] {
        let row = t.send_to(partition, &recs(&["a", "b"])).await;
        check!(row.error_code == 0, "partition {partition}");
    }
    let only_in_zero = t.send_to(0, &recs(&["c"])).await;
    let duplicate_in_one = t.send_to(1, &recs(&["a", "c"])).await;
    let duplicate_in_zero = t.send_to(0, &recs(&["c", "b"])).await;
    let zero = t.contents_of(0).await;
    let one = t.contents_of(1).await;

    assert!(only_in_zero.base_offset == 2);
    assert!(duplicate_in_one.base_offset == 2);
    assert!(duplicate_in_zero.base_offset == 3);
    assert!(zero.records == [stored_key(0, "a"), stored_key(1, "b"), stored_key(2, "c")]);
    assert!(zero.high_watermark == 5);
    assert!(one.records == [stored_key(0, "a"), stored_key(1, "b"), stored_key(3, "c")]);
    assert!(one.high_watermark == 4);
    assert!(t.metric_of("dedup_records_dropped_total", 0).await == 2);
    assert!(t.metric_of("dedup_records_dropped_total", 1).await == 1);
    assert!(t.metric_of("dedup_index_entries", 0).await == 3);
    assert!(t.metric_of("dedup_index_entries", 1).await == 3);
    p.broker.shutdown().await;
}

/// Requests that race on one key from several connections keep exactly one
/// copy. Each answers its own base offset, the kept copy sits at the base
/// offset of the request that sent it, and every other request's offset is
/// empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn racing_requests_with_one_key_keep_exactly_one_copy() {
    const CONNECTIONS: usize = 8;
    const REQUESTS_PER_CONNECTION: usize = 4;
    let p = support::start().await;
    let t = topic().p(&p).name("racing").call().await;
    let mut clients = Vec::new();
    for _ in 0..CONNECTIONS {
        clients.push(connect_to_broker(&p.broker).await);
    }

    let sends = clients.iter().enumerate().flat_map(|(connection, client)| {
        (0..REQUESTS_PER_CONNECTION).map(move |request| {
            let value = format!("from-{connection}-{request}");
            async move {
                let row = produce()
                    .client(client)
                    .topic(t.name)
                    .topic_id(t.id)
                    .batch(keyed_batch().records(&[rec("k", &value)]).call())
                    .call()
                    .await;
                (row, value)
            }
        })
    });
    let answers = join_all(sends).await;
    let contents = t.contents().await;

    let total = CONNECTIONS * REQUESTS_PER_CONNECTION;
    let mut bases: Vec<i64> = answers.iter().map(|(row, _)| row.base_offset).collect();
    bases.sort_unstable();
    assert!(answers.iter().all(|(row, _)| row.error_code == 0));
    assert!(bases == (0..i64::try_from(total).unwrap()).collect::<Vec<_>>());
    assert!(contents.records.len() == 1);
    let kept = &contents.records[0];
    let sender = answers
        .iter()
        .find(|(row, _)| row.base_offset == kept.offset)
        .map(|(_, value)| value.as_str());
    assert!(sender.map(str::as_bytes) == kept.value.as_deref());
    assert!(contents.high_watermark == i64::try_from(total).unwrap());
    assert!(t.metric("dedup_records_dropped_total").await == u64::try_from(total - 1).unwrap());
    for client in clients {
        client.close();
    }
    p.broker.shutdown().await;
}

/// A consumer that fetches from a dropped offset, or from inside an empty
/// batch, gets the next kept record and never a dropped one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_fetch_from_a_dropped_offset_returns_the_next_kept_record() {
    let p = support::start().await;
    let t = topic().p(&p).name("fetch-gap").call().await;

    for keys in [&["a", "b"][..], &["a", "c"], &["b", "c"], &["d"]] {
        assert!(t.send(&recs(keys)).await.error_code == 0);
    }

    let cases = [
        ("a dropped offset", 2, stored_key(3, "c")),
        ("the start of an empty batch", 4, stored_key(6, "d")),
        ("the end of an empty batch", 5, stored_key(6, "d")),
    ];
    for (from, offset, next) in cases {
        let first = first_record_from(t, offset).await;
        check!(first == Some(next), "fetching from {from}");
    }
    p.broker.shutdown().await;
}

/// The first record at or after `offset` that one Fetch from `offset`
/// returns, as a consumer reads it: a consumer skips the records of the
/// first batch that come before its fetch offset.
async fn first_record_from(t: Topic<'_>, offset: i64) -> Option<StoredRecord> {
    let request = single_partition_fetch()
        .topic(t.name.to_owned())
        .topic_id(t.id)
        .partition(fetch_partition().partition(0).offset(offset).call())
        .limits(FetchLimits::one_mebibyte(RequestWaitMillis(0)))
        .call();
    let response = t.p.client.send(request).await.expect("Fetch");
    let row = &response.responses[0].partitions[0];
    assert!(
        row.error_code == 0,
        "Fetch from {offset}: {}",
        row.error_code
    );
    row.records
        .as_ref()
        .and_then(RecordsPayload::as_v2)
        .unwrap_or_default()
        .iter()
        .flat_map(|batch| {
            batch.records.iter().map(|record| StoredRecord {
                offset: batch.base_offset + i64::from(record.offset_delta),
                key: record.key.as_ref().map(|key| key.to_vec()),
                value: record.value.as_ref().map(|value| value.to_vec()),
            })
        })
        .find(|record| record.offset >= offset)
}
