//! Handler tests for the share-session rules of `ShareFetch`, Kafka's
//! `SharePartitionManager.newContext` and `ShareSessionContext`.
//!
//! An initial fetch that replaces a live session keeps the member's acquired
//! records. A final fetch may name and forget partitions, fetches nothing,
//! and releases the member's records. An initial fetch ignores forgotten
//! partitions. A full session cache answers `SHARE_SESSION_LIMIT_REACHED`
//! only after `MaxWaitMs`. An incremental response leaves out the session
//! partitions that have nothing new.

#[cfg(test)]
use crate::handlers::test_support::StartedBroker as HandlerStartedBroker;
use {
    crate::{
        authorizer::AllowAllAuthorizer,
        broker::BrokerHandle,
        codes,
        handlers::test_support::{
            OffsetRun, StartedBroker, WireAckBatch, acquired_share_records,
            create_topic as create_topic_from_test_support, initialize_earliest_share,
            produce_records, share_fetch_topics, share_fetch_wire, share_record_states,
        },
        share_partition::state::RecordState::{self, Acquired, Available},
        test_support::{
            decode_response, encode_request, start_broker_no_audit_with, try_dispatch_context,
        },
    },
    assert2::assert,
    krabka_metadata::{GroupConfigRecord, MetadataRecord},
    krabka_protocol::{
        owned::{
            share_fetch_request::{
                AcknowledgementBatch, FetchPartition, FetchTopic, ForgottenTopic, ShareFetchRequest,
            },
            share_fetch_response::ShareFetchResponse,
        },
        primitives::uuid::Uuid as WireUuid,
    },
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
};

const VERSION: i16 = 2;
const ACCEPT: i8 = 1;

async fn start(session_max: usize) -> StartedBroker {
    let HandlerStartedBroker { broker, dir } = start_broker_no_audit_with(move |cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.share_session_cache_max_when_unlimited = session_max;
    })
    .await;
    StartedBroker { broker, dir }
}

/// Creates `topic` with `partitions` partitions and returns its id.
async fn create_topic(broker: &BrokerHandle, topic: &str, partitions: i32) -> WireUuid {
    create_topic_from_test_support()
        .broker(broker)
        .client_id("share-session-test")
        .name(topic)
        .partitions(partitions)
        .call()
        .await
}

/// Appends three records to `partition` of `topic`.
async fn produce(broker: &BrokerHandle, topic: &str, partition: i32) {
    produce_records()
        .broker(broker)
        .topic(topic)
        .partition_index(partition)
        .call()
        .await;
}

/// Starts `group` at the earliest offset of each partition of `topic_id`.
async fn earliest(broker: &BrokerHandle, group: &str, topic_id: WireUuid, partitions: i32) {
    initialize_earliest_share(broker, group, topic_id, 0..partitions).await;
}

/// One request row: a partition and its acknowledgement batches.
#[derive(Clone, Copy)]
struct Row {
    partition: i32,
    batches: &'static [WireAckBatch<'static>],
}

struct Fetch<'a> {
    group: &'a str,
    epoch: i32,
    topic_id: WireUuid,
    rows: &'a [Row],
    forgotten: &'a [i32],
    max_wait_ms: i32,
}

async fn share_fetch(broker: &BrokerHandle, fetch: &Fetch<'_>) -> ShareFetchResponse {
    let request = ShareFetchRequest {
        group_id: Some(fetch.group.into()),
        member_id: Some("member".into()),
        share_session_epoch: fetch.epoch,
        max_wait_ms: fetch.max_wait_ms,
        max_bytes: 1 << 20,
        max_records: 500,
        batch_size: 500,
        topics: if fetch.rows.is_empty() {
            Vec::new()
        } else {
            vec![FetchTopic {
                topic_id: fetch.topic_id,
                partitions: fetch
                    .rows
                    .iter()
                    .map(|&Row { partition, batches }| FetchPartition {
                        partition_index: partition,
                        acknowledgement_batches: batches
                            .iter()
                            .map(
                                |&WireAckBatch { first, last, types }| AcknowledgementBatch {
                                    first_offset: first,
                                    last_offset: last,
                                    acknowledge_types: types.to_vec(),
                                    ..Default::default()
                                },
                            )
                            .collect(),
                        ..Default::default()
                    })
                    .collect(),
                ..Default::default()
            }]
        },
        forgotten_topics_data: if fetch.forgotten.is_empty() {
            Vec::new()
        } else {
            vec![ForgottenTopic {
                topic_id: fetch.topic_id,
                partitions: fetch.forgotten.to_vec(),
                ..Default::default()
            }]
        },
        ..Default::default()
    };
    share_fetch_wire()
        .broker(broker)
        .request(&request)
        .version(VERSION)
        .call()
        .await
}

async fn states(broker: &BrokerHandle, group: &str, topic_id: WireUuid) -> Vec<RecordState> {
    share_record_states(broker, group, topic_id).await
}

/// The partition, error codes and acquired ranges of one response row.
#[derive(Debug, PartialEq, Eq)]
struct RowOutcome {
    partition: i32,
    error_code: i16,
    acknowledge_error_code: i16,
    acquired: Vec<OffsetRun>,
}

/// The [`RowOutcome`] of every row of `response`.
fn rows(response: &ShareFetchResponse) -> Vec<RowOutcome> {
    response
        .responses
        .iter()
        .flat_map(|topic| &topic.partitions)
        .map(|row| RowOutcome {
            partition: row.partition_index,
            error_code: row.error_code,
            acknowledge_error_code: row.acknowledge_error_code,
            acquired: acquired_share_records(row),
        })
        .collect()
}

const HELD: &[RecordState] = &[Acquired, Acquired, Acquired];

/// Re-opening at epoch 0 keeps the records the member holds, and the member
/// can still acknowledge them. A final fetch that names a partition fetches
/// nothing and releases what the member holds.
#[tokio::test]
async fn reopening_keeps_records_and_a_final_fetch_fetches_nothing() {
    let StartedBroker { broker, dir: _dir } = start(10_000).await;
    let topic_id = create_topic(&broker, "reopen", 1).await;
    produce(&broker, "reopen", 0).await;
    earliest(&broker, "g", topic_id, 1).await;
    let fetch = |epoch, rows, forgotten| Fetch {
        group: "g",
        epoch,
        topic_id,
        rows,
        forgotten,
        max_wait_ms: 0,
    };

    let opened = share_fetch(
        &broker,
        &fetch(
            0,
            &[Row {
                partition: 0,
                batches: &[],
            }],
            &[],
        ),
    )
    .await;
    // Epoch 0 again, forgetting the partition it names: Kafka ignores the
    // forgotten partitions of an initial request.
    let reopened = share_fetch(
        &broker,
        &fetch(
            0,
            &[Row {
                partition: 0,
                batches: &[],
            }],
            &[0],
        ),
    )
    .await;
    let after_reopen = states(&broker, "g", topic_id).await;
    let accepted = share_fetch(
        &broker,
        &fetch(
            1,
            &[Row {
                partition: 0,
                batches: &[WireAckBatch {
                    first: 0,
                    last: 0,
                    types: &[ACCEPT],
                }],
            }],
            &[],
        ),
    )
    .await;
    let closed = share_fetch(
        &broker,
        &fetch(
            -1,
            &[Row {
                partition: 0,
                batches: &[],
            }],
            &[0],
        ),
    )
    .await;
    let after_close = states(&broker, "g", topic_id).await;

    assert!(
        (
            (opened.error_code, rows(&opened)),
            (reopened.error_code, rows(&reopened)),
            after_reopen,
            (accepted.error_code, rows(&accepted)),
            (closed.error_code, rows(&closed)),
            after_close,
        ) == (
            (codes::NONE, vec![RowOutcome {
                partition: 0,
                error_code: codes::NONE,
                acknowledge_error_code: codes::NONE,
                acquired: vec![OffsetRun {
                    first_offset: 0,
                    last_offset: 2
                }]
            }]),
            (codes::NONE, vec![RowOutcome {
                partition: 0,
                error_code: codes::NONE,
                acknowledge_error_code: codes::NONE,
                acquired: Vec::new()
            }]),
            HELD.to_vec(),
            (codes::NONE, vec![RowOutcome {
                partition: 0,
                error_code: codes::NONE,
                acknowledge_error_code: codes::NONE,
                acquired: Vec::new()
            }]),
            (codes::NONE, Vec::new()),
            vec![Available, Available],
        )
    );
    broker.shutdown().await;
}

/// A full session cache answers `SHARE_SESSION_LIMIT_REACHED` after
/// `MaxWaitMs`, not at once.
#[tokio::test]
async fn a_full_session_cache_answers_after_max_wait() {
    let StartedBroker { broker, dir: _dir } = start(1).await;
    let topic_id = create_topic(&broker, "full", 1).await;
    let first = share_fetch(&broker, &Fetch {
        group: "first",
        epoch: 0,
        topic_id,
        rows: &[],
        forgotten: &[],
        max_wait_ms: 0,
    })
    .await;
    let started = Instant::now();
    let refused = share_fetch(&broker, &Fetch {
        group: "second",
        epoch: 0,
        topic_id,
        rows: &[],
        forgotten: &[],
        max_wait_ms: 300,
    })
    .await;
    let waited = started.elapsed();

    assert!(
        (
            first.error_code,
            refused.error_code,
            waited >= Duration::from_millis(300)
        ) == (codes::NONE, codes::SHARE_SESSION_LIMIT_REACHED, true)
    );
    broker.shutdown().await;
}

/// An incremental response carries only the partition that has records.
#[tokio::test]
async fn an_incremental_response_leaves_out_partitions_without_news() {
    let StartedBroker { broker, dir: _dir } = start(10_000).await;
    let topic_id = create_topic(&broker, "incremental", 3).await;
    earliest(&broker, "g", topic_id, 3).await;
    let fetch = |epoch, rows| Fetch {
        group: "g",
        epoch,
        topic_id,
        rows,
        forgotten: &[],
        max_wait_ms: 0,
    };

    let opened = share_fetch(
        &broker,
        &fetch(0, &[
            Row {
                partition: 0,
                batches: &[],
            },
            Row {
                partition: 1,
                batches: &[],
            },
            Row {
                partition: 2,
                batches: &[],
            },
        ]),
    )
    .await;
    produce(&broker, "incremental", 1).await;
    let continued = share_fetch(&broker, &fetch(1, &[])).await;
    let quiet = share_fetch(&broker, &fetch(2, &[])).await;

    assert!(
        (rows(&opened).len(), rows(&continued), rows(&quiet))
            == (
                3,
                vec![RowOutcome {
                    partition: 1,
                    error_code: codes::NONE,
                    acknowledge_error_code: codes::NONE,
                    acquired: vec![OffsetRun {
                        first_offset: 0,
                        last_offset: 2
                    }]
                }],
                Vec::new()
            )
    );
    broker.shutdown().await;
}

/// The per-group `share.*` overrides reach the share partition: the lock
/// duration that `AcquisitionLockTimeoutMs` reports, the record lock limit of
/// the window, and the delivery count limit at which a release archives.
#[tokio::test]
async fn group_share_settings_override_the_broker_defaults() {
    const RELEASE: i8 = 2;
    // The broker's bound admits the record lock limit of 2 that the group
    // asks for, which is below Kafka's default minimum and would be capped to
    // it.
    let HandlerStartedBroker { broker, dir: _dir } = start_broker_no_audit_with(|cfg| {
        cfg.authorizer = Arc::new(AllowAllAuthorizer);
        cfg.share_session_cache_max_when_unlimited = 10_000;
        cfg.share_group.min_partition_max_record_locks = 2;
    })
    .await;
    let topic_id = create_topic(&broker, "overrides", 1).await;
    produce(&broker, "overrides", 0).await;
    earliest(&broker, "g", topic_id, 1).await;
    broker
        .broker_arc_for_test()
        .controller
        .submit_change(vec![MetadataRecord::V1GroupConfig(GroupConfigRecord {
            group_id: "g".to_string(),
            configs: maplit::btreemap! {
                "share.auto.offset.reset".to_owned() => "earliest".to_owned(),
                "share.record.lock.duration.ms".to_owned() => "60000".to_owned(),
                "share.delivery.count.limit".to_owned() => "2".to_owned(),
                "share.partition.max.record.locks".to_owned() => "2".to_owned(),
            },
        })])
        .await
        .expect("set the group config");
    let fetch = |epoch, rows| Fetch {
        group: "g",
        epoch,
        topic_id,
        rows,
        forgotten: &[],
        max_wait_ms: 0,
    };
    let delivered = |response: &ShareFetchResponse| {
        response
            .responses
            .iter()
            .flat_map(|topic| &topic.partitions)
            .flat_map(|row| &row.acquired_records)
            .map(|range| (range.first_offset, range.last_offset, range.delivery_count))
            .collect::<Vec<_>>()
    };

    let first = share_fetch(
        &broker,
        &fetch(0, &[Row {
            partition: 0,
            batches: &[],
        }]),
    )
    .await;
    let second = share_fetch(
        &broker,
        &fetch(1, &[Row {
            partition: 0,
            batches: &[WireAckBatch {
                first: 0,
                last: 1,
                types: &[RELEASE],
            }],
        }]),
    )
    .await;
    let third = share_fetch(
        &broker,
        &fetch(2, &[Row {
            partition: 0,
            batches: &[WireAckBatch {
                first: 0,
                last: 1,
                types: &[RELEASE],
            }],
        }]),
    )
    .await;

    assert!(
        (
            (first.acquisition_lock_timeout_ms, delivered(&first)),
            delivered(&second),
            delivered(&third),
        ) == (
            (60_000, vec![(0, 1, 1)]),
            vec![(0, 1, 2)],
            // The second release reaches the limit of 2 and archives 0 and 1
            // at once, so the window moves on to offset 2.
            vec![(2, 2, 1)],
        )
    );
    broker.shutdown().await;
}

/// The version, `ShareAcquireMode`, `MaxRecords` and `BatchSize` of a request,
/// and the acquired rows expected.
struct ShapeCase {
    version: i16,
    mode: i8,
    max_records: i32,
    batch_size: i32,
    rows: Vec<OffsetRun>,
}

/// Kafka's `ShareAcquireMode` and `BatchSize` over two three-record log
/// batches at offsets 0 and 3, each case in a group of its own.
#[tokio::test]
async fn the_acquire_mode_and_batch_size_shape_the_acquired_rows() {
    let StartedBroker { broker, dir: _dir } = start(10_000).await;
    let topic_id = create_topic(&broker, "shaped", 1).await;
    produce(&broker, "shaped", 0).await;
    produce(&broker, "shaped", 0).await;
    // (version, ShareAcquireMode, MaxRecords, BatchSize, acquired rows)
    let cases: [ShapeCase; 4] = [
        // batch_optimized, the only mode at v1: a whole log batch.
        ShapeCase {
            version: 1,
            mode: 0,
            max_records: 2,
            batch_size: 500,
            rows: vec![OffsetRun {
                first_offset: 0,
                last_offset: 2,
            }],
        },
        // record_limit: exactly MaxRecords.
        ShapeCase {
            version: 2,
            mode: 1,
            max_records: 2,
            batch_size: 500,
            rows: vec![OffsetRun {
                first_offset: 0,
                last_offset: 1,
            }],
        },
        // BatchSize splits new records on log batch boundaries.
        ShapeCase {
            version: 2,
            mode: 0,
            max_records: 500,
            batch_size: 3,
            rows: vec![
                OffsetRun {
                    first_offset: 0,
                    last_offset: 2,
                },
                OffsetRun {
                    first_offset: 3,
                    last_offset: 5,
                },
            ],
        },
        ShapeCase {
            version: 2,
            mode: 1,
            max_records: 500,
            batch_size: 3,
            rows: vec![OffsetRun {
                first_offset: 0,
                last_offset: 5,
            }],
        },
    ];
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    for (
        index,
        ShapeCase {
            version,
            mode,
            max_records,
            batch_size,
            rows,
        },
    ) in cases.into_iter().enumerate()
    {
        let group = format!("shaped-{index}");
        earliest(&broker, &group, topic_id, 1).await;
        let request = ShareFetchRequest {
            group_id: Some(group),
            member_id: Some("member".into()),
            max_bytes: 1 << 20,
            max_records,
            batch_size,
            share_acquire_mode: mode,
            topics: vec![FetchTopic {
                topic_id,
                partitions: vec![FetchPartition::default()],
                ..Default::default()
            }],
            ..Default::default()
        };
        let response = share_fetch_wire()
            .broker(&broker)
            .request(&request)
            .version(version)
            .call()
            .await;
        let acquired: Vec<OffsetRun> = response
            .responses
            .iter()
            .flat_map(|topic| &topic.partitions)
            .flat_map(|row| &row.acquired_records)
            .map(|range| OffsetRun::new(range.first_offset, range.last_offset))
            .collect();
        actual.push((index, acquired));
        expected.push((index, rows));
    }
    assert!(actual == expected);
    broker.shutdown().await;
}

/// The size and wait limits of a `ShareFetch`.
#[derive(Clone, Copy)]
struct FetchLimits {
    max_bytes: i32,
    min_bytes: i32,
    max_wait_ms: i32,
}

/// The acquired ranges of one response partition.
#[derive(Debug, PartialEq, Eq)]
struct PartitionRuns {
    partition: i32,
    acquired: Vec<OffsetRun>,
}

/// The acquired ranges per partition of a response, and the time it took.
struct LimitedFetch {
    partitions: Vec<PartitionRuns>,
    took: Duration,
}

/// A `ShareFetch` with `limits` over `partitions` of `topic_id`, at session
/// epoch 0 of `group`. It returns the acquired ranges per partition, in
/// partition order, and the time the response took.
async fn fetch_with_limits(
    broker: &BrokerHandle,
    group: &str,
    topic_id: WireUuid,
    partitions: &[i32],
    limits: FetchLimits,
) -> LimitedFetch {
    let FetchLimits {
        max_bytes,
        min_bytes,
        max_wait_ms,
    } = limits;
    let request = ShareFetchRequest {
        group_id: Some(group.into()),
        member_id: Some("member".into()),
        max_bytes,
        min_bytes,
        max_wait_ms,
        max_records: 500,
        batch_size: 500,
        topics: share_fetch_topics(topic_id, partitions),
        ..Default::default()
    };
    let shared = broker.broker_arc_for_test();
    krabka_macros::request_test_identity! {
        bind(user, address, ctx);
        user ["share-consumer"];
        client ["share-client"];
    };
    let started = Instant::now();
    let response = try_dispatch_context(
        &shared,
        krabka_protocol::owned::share_fetch_request::API_KEY,
        VERSION,
        &encode_request(&request, VERSION),
        &ctx,
    )
    .await
    .expect("handle share fetch");
    let took = started.elapsed();
    let response: ShareFetchResponse = decode_response(&response, VERSION);
    let mut rows: Vec<PartitionRuns> = response
        .responses
        .iter()
        .flat_map(|topic| &topic.partitions)
        .map(|row| PartitionRuns {
            partition: row.partition_index,
            acquired: acquired_share_records(row),
        })
        .collect();
    rows.sort_unstable_by_key(|row| row.partition);
    LimitedFetch {
        partitions: rows,
        took,
    }
}

/// Kafka's `PartitionMaxBytesStrategy.UNIFORM`: `MaxBytes` is the budget of
/// the whole response, split across the partitions. A budget of one byte
/// reaches one of two partitions, which still gets its first batch whole.
#[tokio::test]
async fn max_bytes_is_split_across_the_partitions() {
    let StartedBroker { broker, dir: _dir } = start(10_000).await;
    let topic_id = create_topic(&broker, "split", 2).await;
    for partition in [0, 1] {
        produce(&broker, "split", partition).await;
        produce(&broker, "split", partition).await;
    }
    earliest(&broker, "tight", topic_id, 2).await;
    earliest(&broker, "roomy", topic_id, 2).await;

    let LimitedFetch {
        partitions: tight, ..
    } = fetch_with_limits(&broker, "tight", topic_id, &[0, 1], FetchLimits {
        max_bytes: 1,
        min_bytes: 0,
        max_wait_ms: 0,
    })
    .await;
    let LimitedFetch {
        partitions: roomy, ..
    } = fetch_with_limits(&broker, "roomy", topic_id, &[0, 1], FetchLimits {
        max_bytes: 1 << 20,
        min_bytes: 0,
        max_wait_ms: 0,
    })
    .await;

    assert!(
        (tight, roomy)
            == (
                vec![
                    PartitionRuns {
                        partition: 0,
                        acquired: vec![OffsetRun {
                            first_offset: 0,
                            last_offset: 2
                        }]
                    },
                    PartitionRuns {
                        partition: 1,
                        acquired: Vec::new()
                    }
                ],
                vec![
                    PartitionRuns {
                        partition: 0,
                        acquired: vec![OffsetRun {
                            first_offset: 0,
                            last_offset: 5
                        }]
                    },
                    PartitionRuns {
                        partition: 1,
                        acquired: vec![OffsetRun {
                            first_offset: 0,
                            last_offset: 5
                        }]
                    }
                ],
            )
    );
    broker.shutdown().await;
}

/// Kafka's `DelayedShareFetch.isMinBytesSatisfied`: a response that holds
/// fewer than `MinBytes` waits out `MaxWaitMs`, and one that holds enough
/// answers at once.
#[tokio::test]
async fn min_bytes_holds_the_response_until_max_wait() {
    let StartedBroker { broker, dir: _dir } = start(10_000).await;
    let topic_id = create_topic(&broker, "min-bytes", 1).await;
    produce(&broker, "min-bytes", 0).await;
    earliest(&broker, "short", topic_id, 1).await;
    earliest(&broker, "enough", topic_id, 1).await;

    let LimitedFetch {
        partitions: short,
        took: short_took,
    } = fetch_with_limits(&broker, "short", topic_id, &[0], FetchLimits {
        max_bytes: 1 << 20,
        min_bytes: 1 << 20,
        max_wait_ms: 300,
    })
    .await;
    let LimitedFetch {
        partitions: enough,
        took: enough_took,
    } = fetch_with_limits(&broker, "enough", topic_id, &[0], FetchLimits {
        max_bytes: 1 << 20,
        min_bytes: 1,
        max_wait_ms: 30_000,
    })
    .await;

    assert!(
        (
            short,
            short_took >= Duration::from_millis(300),
            enough,
            enough_took < Duration::from_secs(10),
        ) == (
            vec![PartitionRuns {
                partition: 0,
                acquired: vec![OffsetRun {
                    first_offset: 0,
                    last_offset: 2
                }]
            }],
            true,
            vec![PartitionRuns {
                partition: 0,
                acquired: vec![OffsetRun {
                    first_offset: 0,
                    last_offset: 2
                }]
            }],
            true
        )
    );
    broker.shutdown().await;
}
