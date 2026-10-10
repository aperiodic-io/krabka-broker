//! Tests for the internal-topic gate.
//!
//! Kafka's `ReplicaManager.appendToLocalLog` refuses every partition of a
//! `Topic.isInternal` topic with `InvalidTopicException` (17) unless the
//! request's `client_id` is `"__admin_client"`, so the produce path resolves
//! that once per topic beside the freeze and refuses each partition row
//! before it parses the batch.

use std::{path::PathBuf, sync::Arc};

use assert2::check;
use bytes::Bytes;
use krabka_protocol::{
    owned::produce_response::PartitionProduceResponse,
    records::{Record, RecordBatch},
};

use super::super::{PartitionInput, process_partition};
use crate::{
    config::BrokerConfig,
    handlers::produce::{
        framing::PartitionPayload,
        test_support::{encode_batch, image_with_topic},
    },
    internal_topics::{is_internal_topic, produce_internal_topics_allowed},
};

// The same resolve the produce handler runs once per topic, spelled out for
// the table below rather than copied by hand into every case.
fn internal_topic_denied(config: &BrokerConfig, topic: &str, client_id: &str) -> bool {
    is_internal_topic(config, topic) && !produce_internal_topics_allowed(client_id)
}

// Kafka's three coordinator topics plus krabka's own broker-owned topics are
// denied for every `client_id` but the admin-tooling and diskless-index-writer
// exceptions; an ordinary topic is never denied, whatever the `client_id`.
#[test]
fn only_the_admin_client_or_the_diskless_index_writer_may_produce_to_an_internal_topic() {
    let config = BrokerConfig::for_tests(PathBuf::from("/nonexistent"));
    let cases = [
        (
            "the offsets topic, an ordinary application",
            "__consumer_offsets",
            "my-app",
            true,
        ),
        (
            "the offsets topic, the admin client",
            "__consumer_offsets",
            "__admin_client",
            false,
        ),
        (
            "the transaction state topic, an ordinary application",
            "__transaction_state",
            "my-app",
            true,
        ),
        (
            "the share group state topic, an ordinary application",
            "__share_group_state",
            "my-app",
            true,
        ),
        (
            "an ordinary topic, an ordinary application",
            "orders",
            "my-app",
            false,
        ),
        (
            "an ordinary topic, even the admin client id",
            "orders",
            "__admin_client",
            false,
        ),
        (
            "the offsets topic, the empty client id null decodes to",
            "__consumer_offsets",
            "",
            true,
        ),
        (
            "the diskless WAL index topic, an ordinary application",
            "__diskless_wal_index",
            "my-app",
            true,
        ),
        (
            "the diskless WAL index topic, the broker's own index writer",
            "__diskless_wal_index",
            "krabka-diskless-index-broker-1-producer",
            false,
        ),
    ];
    for (label, topic, client_id, expected) in cases {
        check!(
            internal_topic_denied(&config, topic, client_id) == expected,
            "case: {label}"
        );
    }
}

// A client Produce to `__consumer_offsets` is refused with
// `INVALID_TOPIC_EXCEPTION` (17) and appends nothing; the admin client's
// Produce to the same topic, and an ordinary topic in the same request,
// both append normally.
//
// The log-end-offset assertions are the load-bearing ones, the same way
// they are for the freeze gate: the gate sits ahead of `prepare_batch`, so a
// refused row must leave the partition exactly as it found it.
#[tokio::test]
async fn a_denied_internal_topic_is_refused_and_its_log_end_offset_does_not_move() {
    let dir = tempfile::tempdir().expect("log root");
    let config = BrokerConfig::for_tests(PathBuf::from("/nonexistent"));
    let image = Arc::new(image_with_topic("__consumer_offsets", &[1]));

    let fixture =
        crate::handlers::produce::test_support::PipelineFixture::new(krabka_ids::NodeId(1));

    let part = fixture
        .partition(dir.path(), "__consumer_offsets", &image)
        .await;
    part.log
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .append(&mut seed_batch())
        .expect("seed the partition");
    fixture.partitions.insert(
        "__consumer_offsets".into(),
        krabka_ids::PartitionIndex(0),
        part,
    );

    let cases = [
        (
            "an ordinary application is refused before any append",
            "my-app",
            PartitionProduceResponse {
                index: 0,
                error_code: crate::codes::INVALID_TOPIC_EXCEPTION,
                base_offset: -1,
                ..Default::default()
            },
        ),
        (
            "the admin client appends normally",
            "__admin_client",
            PartitionProduceResponse {
                index: 0,
                error_code: crate::codes::NONE,
                base_offset: 1,
                log_start_offset: 0,
                ..Default::default()
            },
        ),
    ];

    for (label, client_id, want) in cases {
        let resp = process_partition(
            PartitionInput {
                internal_topic_denied: internal_topic_denied(
                    &config,
                    "__consumer_offsets",
                    client_id,
                ),
                ..crate::handlers::produce::test_support::pipeline_input(
                    "__consumer_offsets",
                    PartitionPayload::Slice(encode_batch(&seed_batch())),
                )
            },
            fixture.services(&image),
        )
        .await
        .expect("process partition")
        .expect_done();
        check!(resp == want, "case: {label}");
    }

    check!(
        fixture
            .partitions
            .get("__consumer_offsets", krabka_ids::PartitionIndex(0))
            .expect("the partition is registered")
            .log_end_offset()
            == krabka_log::Offset(2),
        "the seed batch plus the admin client's one append; the ordinary \
         application's refused batch must not have landed"
    );
}

// One record, enough to seed a partition or to be refused.
fn seed_batch() -> RecordBatch {
    RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(b"v")),
            ..Default::default()
        }],
        ..Default::default()
    }
}
