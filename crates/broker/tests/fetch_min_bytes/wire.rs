//! The one wire exchange both halves of the `fetch.min.bytes` evidence run:
//! a topic, one small record, and a Fetch whose `min_bytes` floor is far above
//! what that record weighs.
//!
//! Both halves send it with the same client code, so the only difference
//! between them is which broker is on the other end.

use std::time::{Duration, Instant};

use assert2::assert;
use krabka_client_core::Client;
use krabka_protocol::{
    primitives::uuid::Uuid as WireUuid,
    records::{Record, RecordsPayload},
};

use crate::support::{
    client::connect_owned, fetch::single_partition_fetch, records::batch_from_records,
    topics::creatable_topic,
};

/// The floor the fetch asks for. 64 KiB is orders of magnitude above the one
/// small record the exchange produces, so nothing that is readable can satisfy
/// it and only `max_wait_ms` can end the wait.
///
/// The stock consumer default is `fetch.min.bytes=1`, which is why the default
/// path never showed this; a Streams or Connect deployment raises it, and this
/// is the shape of the request it then sends.
pub(crate) const MIN_BYTES: i32 = 64 * 1024;

/// The wait that bounds the floor.
pub(crate) const MAX_WAIT_MS: i32 = 2_000;

/// The least time the answer may take.
///
/// A broker that treats `min_bytes` as a hint answers on the first append,
/// which lands here in single-digit milliseconds. The bound is one-sided and a
/// slow machine only makes it more true, so it is not the kind of timing
/// assertion instrumentation can break.
pub(crate) const HELD_AT_LEAST: Duration = Duration::from_millis(1_800);

/// Topic-created-already, which a re-run of the ignored differential meets.
const TOPIC_ALREADY_EXISTS: i16 = 36;

/// What a fetch answered with, whole, so two brokers' answers compare as one
/// value.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct FetchFacts {
    pub(crate) error_code: i16,
    pub(crate) high_watermark: i64,
    /// The base offset of every batch the response carried.
    pub(crate) base_offsets: Vec<i64>,
}

/// The delay before the second record of the exchange is appended, well
/// inside `MAX_WAIT_MS`.
///
/// It is what makes the held time mean something: a broker that answers on the
/// first append it sees comes back at roughly this mark, and one that holds
/// out for `min_bytes` comes back at `MAX_WAIT_MS`.
const APPEND_AFTER: Duration = Duration::from_millis(300);

/// A client on `bootstrap`.
async fn connect(bootstrap: &str) -> Client {
    connect_owned(bootstrap, "krabka-fetch-min-bytes", "client build").await
}

/// Append one small record -- small enough that `MIN_BYTES` stays out of reach
/// whatever the batch overhead of the broker on the other end is.
async fn produce_one(client: &Client, topic: &str, topic_id: WireUuid, value: &'static [u8]) {
    let produced = crate::support::produce::send_batch(
        &client,
        batch_from_records(vec![Record {
            value: Some(bytes::Bytes::from_static(value)),
            ..Default::default()
        }]),
        crate::support::produce::SinglePartitionProduceSetup {
            topic: (topic).into(),
            topic_id,
            timeout: crate::support::produce::ProduceTimeoutMillis(10_000),
            ..Default::default()
        },
    )
    .await;
    assert!(
        produced.responses[0].partition_responses[0].error_code == 0,
        "Produce"
    );
}

/// Create `topic` with one partition, tolerating a topic a previous run left
/// behind.
async fn create_topic(client: &Client, topic: &str) {
    let response = client
        .send(crate::support::topics::create_topic_request_with_setup(
            creatable_topic(crate::support::topics::ConfiguredTopicSetup {
                name: (topic).into(),
                ..Default::default()
            }),
            crate::support::topics::CreateTopicRequestSetup {
                timeout: crate::support::topics::CreateTopicsTimeoutMillis(10_000),
            },
        ))
        .await
        .expect("CreateTopics");
    let code = response.topics[0].error_code;
    assert!(code == 0 || code == TOPIC_ALREADY_EXISTS, "CreateTopics");
}

/// Poll `Metadata` until the topic has a leader, and answer with its id.
///
/// A real Kafka broker publishes the topic through its own metadata log, so
/// the id is not there the instant `CreateTopics` returns.
async fn topic_id(client: &Client, topic: &str) -> WireUuid {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let response = client
            .send(crate::support::discovery::named_topic_metadata(topic))
            .await
            .expect("Metadata");
        if let Some(found) = response
            .topics
            .iter()
            .find(|t| t.name.as_deref() == Some(topic) && t.error_code == 0)
            && found.partitions.iter().all(|p| p.leader_id >= 0)
        {
            return found.topic_id;
        }
        assert!(Instant::now() < deadline, "topic never got a leader");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// The base offset of every batch in a fetched partition's records field.
fn base_offsets(records: Option<&RecordsPayload>) -> Vec<i64> {
    let Some(payload) = records else {
        return Vec::new();
    };
    let batches = match payload {
        RecordsPayload::V2(batches) => batches.clone(),
        RecordsPayload::Raw(raw) => {
            match RecordsPayload::from_bytes(raw.clone()).expect("decode the fetched records") {
                RecordsPayload::V2(batches) => batches,
                other => panic!("fetched records are not v2 batches: {other:?}"),
            }
        }
        other => panic!("unexpected records payload: {other:?}"),
    };
    batches.iter().map(|batch| batch.base_offset).collect()
}

/// Create the topic, put one record in it, then fetch under a `min_bytes`
/// floor neither that record nor the second one appended mid-flight can reach.
///
/// The second append is the point: it wakes the broker's long poll partway
/// through the wait, and a broker that treats `min_bytes` as a hint answers
/// there and then. Both records must come back, and not before `MAX_WAIT_MS`.
///
/// Returns how long the fetch was held and what it answered with.
pub(crate) async fn min_bytes_exchange(bootstrap: &str, topic: &str) -> (Duration, FetchFacts) {
    let client = connect(bootstrap).await;
    create_topic(&client, topic).await;
    let topic_id = topic_id(&client, topic).await;
    produce_one(&client, topic, topic_id, b"first").await;

    // Its own connection: the fetch below occupies this one for seconds, and
    // the append has to land while it does.
    let producer = connect(bootstrap).await;
    let appended = {
        let topic = topic.to_owned();
        tokio::spawn(async move {
            tokio::time::sleep(APPEND_AFTER).await;
            produce_one(&producer, &topic, topic_id, b"second").await;
        })
    };

    let started = Instant::now();
    let fetched = client
        .send(single_partition_fetch(
            crate::support::fetch::SinglePartitionFetchSetup {
                topic: topic.into(),
                topic_id,
                limits: crate::support::fetch::FetchLimits {
                    wait: crate::support::fetch::RequestWaitMillis(MAX_WAIT_MS),
                    minimum: crate::support::fetch::FetchByteLimit(MIN_BYTES),
                    ..Default::default()
                },
                ..Default::default()
            },
        ))
        .await
        .expect("Fetch");
    let held = started.elapsed();
    appended.await.expect("the mid-fetch append");

    let partition = &fetched.responses[0].partitions[0];
    (
        held,
        FetchFacts {
            error_code: partition.error_code,
            high_watermark: partition.high_watermark,
            base_offsets: base_offsets(partition.records.as_ref()),
        },
    )
}
