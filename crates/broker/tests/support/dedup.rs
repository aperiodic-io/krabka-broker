//! KFC-10 produce-time deduplication fixtures shared by the `dedup_*` suites.
//!
//! The suites drive a broker through the Kafka protocol only: they create a
//! topic with `dedup.window.ms` and `dedup.key.header`, produce hand-built
//! batches so the keys, headers, timestamps and producer fields are exact, and
//! read every stored batch back with Fetch so an assertion can name the offset
//! of each record and the offset span of each batch.

use {
    crate::support::{
        client::{create_topic_with, metric_value},
        fetch::{FetchLimits, RequestWaitMillis, fetch_partition, single_partition_fetch},
        produce::{
            ProduceAcknowledgements, ProduceTimeoutMillis, SinglePartitionProduceSetup,
            batch_request,
        },
        records::now_ms,
        topic_id_for,
        topics::{CreateTopicSetup, TopicPartitionCount},
    },
    bytes::Bytes,
    krabka_broker::BrokerHandle,
    krabka_client_core::Client,
    krabka_compression::CompressionType,
    krabka_protocol::{
        owned::produce_response::PartitionProduceResponse,
        primitives::uuid::Uuid,
        records::{Attributes, Record, RecordBatch, RecordHeader, RecordsPayload},
    },
};

/// The header the header-mode suites name in `dedup.key.header`.
pub const DEDUP_HEADER: &str = "dedup-id";

/// Create a topic whose records are deduplicated within `window_ms`, by key or
/// by `key_header`, and wait until this broker leads every partition. Returns
/// the topic id that Produce v13 and Fetch v13 address it by.
#[bon::builder]
pub async fn create_dedup_topic(
    broker: &BrokerHandle,
    client: &Client,
    topic: &str,
    #[builder(default = 60_000)] window_ms: i64,
    key_header: Option<&str>,
    #[builder(default = 1)] partitions: i32,
    #[builder(default = &[])] extra_configs: &[(&str, &str)],
) -> Uuid {
    let window = window_ms.to_string();
    let mut configs: Vec<(&str, &str)> = vec![("dedup.window.ms", window.as_str())];
    if let Some(header) = key_header {
        configs.push(("dedup.key.header", header));
    }
    configs.extend_from_slice(extra_configs);
    create_topic_with(client, CreateTopicSetup {
        topic,
        configs: &configs,
        num_partitions: TopicPartitionCount(partitions),
        ..Default::default()
    })
    .await;
    for partition in 0..partitions {
        broker.wait_until_partition_present(topic, partition).await;
    }
    topic_id_for(client, topic).await
}

/// One record of a [`keyed_batch`]: its key (`None` is a null key), its value,
/// its headers, and how many milliseconds before now it was created.
#[derive(Clone, Debug, Default)]
pub struct Rec {
    pub key: Option<Vec<u8>>,
    pub value: Vec<u8>,
    pub headers: Vec<(String, Option<Vec<u8>>)>,
    pub age_ms: i64,
}

impl Rec {
    /// A record whose key and value are both `key`.
    pub fn key(key: &str) -> Self {
        Self {
            key: Some(key.as_bytes().to_vec()),
            value: format!("value-of-{key}").into_bytes(),
            ..Self::default()
        }
    }

    /// A record with a null key and the given value.
    pub fn keyless(value: &str) -> Self {
        Self {
            value: value.as_bytes().to_vec(),
            ..Self::default()
        }
    }

    /// Add a header; `None` is a null header value.
    #[must_use]
    pub fn header(mut self, name: &str, value: Option<&str>) -> Self {
        self.headers
            .push((name.to_owned(), value.map(|v| v.as_bytes().to_vec())));
        self
    }

    /// Add the [`DEDUP_HEADER`] header with `value`.
    #[must_use]
    pub fn dedup_id(self, value: &str) -> Self { self.header(DEDUP_HEADER, Some(value)) }

    /// Replace the value.
    #[must_use]
    pub fn value(mut self, value: &str) -> Self {
        self.value = value.as_bytes().to_vec();
        self
    }

    /// Date the record `age_ms` before now; negative is in the future.
    #[must_use]
    pub fn aged(mut self, age_ms: i64) -> Self {
        self.age_ms = age_ms;
        self
    }
}

/// The producer fields of a batch: `None` for a plain producer, or the
/// idempotent producer id, epoch and base sequence.
#[derive(Clone, Copy, Debug)]
pub struct Idempotence {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub base_sequence: i32,
}

/// One client v2 batch holding `records` in order, `CreateTime`, with
/// contiguous offset deltas as a producer writes them.
#[bon::builder]
pub fn keyed_batch(
    records: &[Rec],
    #[builder(default = CompressionType::None)] compression: CompressionType,
    idempotence: Option<Idempotence>,
    #[builder(default)] transactional: bool,
) -> RecordBatch {
    let now = now_ms();
    let timestamps: Vec<i64> = records.iter().map(|r| now - r.age_ms).collect();
    let base_timestamp = timestamps.iter().copied().min().unwrap_or(now);
    let max_timestamp = timestamps.iter().copied().max().unwrap_or(now);
    let records: Vec<Record> = records
        .iter()
        .zip(&timestamps)
        .zip(0_i32..)
        .map(|((rec, ts), offset_delta)| Record {
            offset_delta,
            timestamp_delta: ts - base_timestamp,
            key: rec.key.clone().map(Bytes::from),
            value: Some(Bytes::from(rec.value.clone())),
            headers: rec
                .headers
                .iter()
                .map(|(name, value)| RecordHeader {
                    key: name.clone(),
                    value: value.clone().map(Bytes::from),
                })
                .collect(),
            ..Record::default()
        })
        .collect();
    let mut attributes = Attributes::default().with_compression(compression);
    if transactional {
        attributes = attributes.with_transactional(true);
    }
    let (producer_id, producer_epoch, base_sequence) = idempotence.map_or((-1, -1, -1), |i| {
        (i.producer_id, i.producer_epoch, i.base_sequence)
    });
    RecordBatch {
        attributes,
        last_offset_delta: i32::try_from(records.len()).expect("batch size") - 1,
        base_timestamp,
        max_timestamp,
        producer_id,
        producer_epoch,
        base_sequence,
        records,
        ..RecordBatch::default()
    }
}

/// Produce one batch to `partition` and return its partition row.
#[bon::builder]
pub async fn produce(
    client: &Client,
    topic: &str,
    topic_id: Uuid,
    batch: RecordBatch,
    #[builder(default)] partition: i32,
    #[builder(default = ProduceAcknowledgements::AllReplicas)] acks: ProduceAcknowledgements,
) -> PartitionProduceResponse {
    let request = batch_request(batch, SinglePartitionProduceSetup {
        topic: topic.to_owned(),
        topic_id,
        partition: krabka_ids::PartitionIndex(partition),
        acknowledgements: acks,
        timeout: ProduceTimeoutMillis(30_000),
        ..SinglePartitionProduceSetup::default()
    });
    let response = client.send(request).await.expect("Produce");
    response.responses[0].partition_responses[0].clone()
}

/// One record as a consumer reads it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredRecord {
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub value: Option<Vec<u8>>,
}

/// One stored batch: its offset span and whether it holds any record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct StoredBatch {
    pub base_offset: i64,
    pub last_offset: i64,
    pub records: usize,
}

/// Everything a partition holds, read with Fetch from offset 0 to the high
/// watermark.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionContents {
    pub records: Vec<StoredRecord>,
    pub batches: Vec<StoredBatch>,
    pub high_watermark: i64,
}

impl PartitionContents {
    /// Every record key as UTF-8, in offset order, `"<null>"` for a null key.
    pub fn keys(&self) -> Vec<String> {
        self.records
            .iter()
            .map(|r| {
                r.key.as_deref().map_or_else(
                    || "<null>".to_owned(),
                    |k| String::from_utf8_lossy(k).into(),
                )
            })
            .collect()
    }

    /// Every `(offset, key)` pair in offset order.
    pub fn offsets_and_keys(&self) -> Vec<(i64, String)> {
        self.records
            .iter()
            .zip(self.keys())
            .map(|(r, k)| (r.offset, k))
            .collect()
    }
}

/// Read every record and batch of a partition with Fetch.
pub async fn read_partition(
    client: &Client,
    topic: &str,
    topic_id: Uuid,
    partition: i32,
) -> PartitionContents {
    let mut contents = PartitionContents::default();
    let mut offset = 0_i64;
    loop {
        let request = single_partition_fetch()
            .topic(topic.to_owned())
            .topic_id(topic_id)
            .partition(fetch_partition().partition(partition).offset(offset).call())
            .limits(FetchLimits::one_mebibyte(RequestWaitMillis(0)))
            .call();
        let response = client.send(request).await.expect("Fetch");
        let row = &response.responses[0].partitions[0];
        assert2::assert!(row.error_code == 0, "fetch error {}", row.error_code);
        contents.high_watermark = row.high_watermark;
        let batches: Vec<RecordBatch> = row
            .records
            .as_ref()
            .and_then(RecordsPayload::as_v2)
            .map(<[RecordBatch]>::to_vec)
            .unwrap_or_default();
        if batches.is_empty() || offset >= row.high_watermark {
            break;
        }
        for batch in batches {
            let last = batch.base_offset + i64::from(batch.last_offset_delta);
            if last < offset {
                continue;
            }
            contents.batches.push(StoredBatch {
                base_offset: batch.base_offset,
                last_offset: last,
                records: batch.records.len(),
            });
            contents
                .records
                .extend(batch.records.iter().map(|record| StoredRecord {
                    offset: batch.base_offset + i64::from(record.offset_delta),
                    key: record.key.as_ref().map(|k| k.to_vec()),
                    value: record.value.as_ref().map(|v| v.to_vec()),
                }));
            offset = last + 1;
        }
        if offset >= contents.high_watermark {
            break;
        }
    }
    contents
}

/// The value of one per-partition KFC-10 series, such as
/// `"dedup_records_dropped_total"` or `"dedup_index_entries"`.
pub async fn partition_metric(
    handle: &BrokerHandle,
    name: &str,
    topic: &str,
    partition: i32,
) -> f64 {
    let series = format!("krabka_broker_{name}{{topic=\"{topic}\",partition=\"{partition}\"}}");
    metric_value(handle, &series).await
}

/// The value of one broker-wide KFC-10 gauge, such as
/// `"dedup_broker_index_entries"`.
pub async fn broker_metric(handle: &BrokerHandle, name: &str) -> f64 {
    metric_value(handle, &format!("krabka_broker_{name} ")).await
}
