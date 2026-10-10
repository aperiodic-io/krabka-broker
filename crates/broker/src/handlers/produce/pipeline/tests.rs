//! Tests for the per-partition pipeline's leadership-gate and write-freeze
//! response rows.

use std::sync::Arc;

use assert2::assert;
use bytes::Bytes;
use krabka_metadata::{MetadataRecord, PartitionRecord};
use krabka_protocol::{
    owned::produce_response::LeaderIdAndEpoch,
    records::{Record, RecordBatch},
};
use wiremock::{Mock, MockServer, ResponseTemplate, matchers::method};

use super::*;
use crate::handlers::produce::{
    framing::PartitionPayload,
    test_support::{encode_batch, image_with_topic},
};

#[tokio::test]
async fn process_partition_non_leader_skips_schema_registry_and_preserves_hint() {
    let registry = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&registry)
        .await;
    let schema_validator = Arc::new(
        crate::schema_validation::SchemaValidator::new(
            registry.uri(),
            false,
            100,
            krabka_units::minutes(1),
            krabka_units::secs(1),
        )
        .expect("validator"),
    );
    let image = non_leader_image();
    let fixture =
        crate::handlers::produce::test_support::PipelineFixture::new(krabka_ids::NodeId(1));
    let payload = encode_batch(&RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(&[0, 0, 0, 0, 42, b'a'])),
            ..Default::default()
        }],
        ..Default::default()
    });

    let resp = process_partition(
        PartitionInput {
            schema: Some(crate::schema_validation::SchemaGate {
                key: false,
                value: true,
                mode: crate::schema_validation::ValidationMode::Full,
            }),
            ..crate::handlers::produce::test_support::pipeline_input(
                "orders",
                PartitionPayload::Slice(payload),
            )
        },
        PartitionServices {
            schema_validator: Some(&schema_validator),
            ..fixture.services(&image)
        },
    )
    .await
    .expect("process partition")
    .expect_done();

    let expected = non_leader_row();
    assert!(resp == expected);
}

#[tokio::test]
async fn process_partition_leader_without_local_replica_hints_leader() {
    // We ARE the image-designated leader (this_node_id == leader), but the
    // local writer-actor hasn't been spun up (empty registry). This takes
    // the "transient not-leader" branch, whose `current_leader` hint must
    // still carry the real leader id + epoch from the image — not the 0
    // defaults a struct-field-deletion mutant would leave.
    let image = non_leader_image();
    // Empty registry → `fixture.partitions.get(..)` returns None.
    let fixture =
        crate::handlers::produce::test_support::PipelineFixture::new(krabka_ids::NodeId(2));
    let payload = encode_batch(&RecordBatch {
        records: vec![Record {
            value: Some(Bytes::from_static(b"hello")),
            ..Default::default()
        }],
        ..Default::default()
    });

    let resp = process_partition(
        crate::handlers::produce::test_support::pipeline_input(
            "orders",
            PartitionPayload::Slice(payload),
        ),
        PartitionServices {
            broker_policy: BrokerProducePolicy {
                node_id: krabka_audit::NodeId(2),
                default_min_insync_replicas: 1,
                is_witness: false,
            },
            ..fixture.services(&image)
        },
    )
    .await
    .expect("process partition")
    .expect_done();

    let expected = non_leader_row();
    assert!(resp == expected);
}

// ── KFC-9 topic write freeze ─────────────────────────────────────
//
// The gate that refuses every partition row of a frozen topic, and the
// per-topic resolve the handler feeds it.
mod freeze;

// ── internal-topic gate ───────────────────────────────────────────
//
// The gate that refuses a client Produce to a broker-owned topic such as
// `__consumer_offsets`, unless the request's `client_id` is Kafka's own
// admin-tooling exception.
mod internal_topic;

fn non_leader_image() -> Arc<krabka_metadata::MetadataImage> {
    let mut img = image_with_topic("orders", &[2, 3]);
    img.apply(&MetadataRecord::V1Partition(PartitionRecord {
        leader_epoch: krabka_metadata::LeaderEpoch(17),
        partition_epoch: 1,
        ..crate::handlers::test_support::replicated_partition(
            crate::handlers::test_support::ReplicatedPartitionSetup {
                leader: krabka_audit::NodeId(2),
                replicas: &[krabka_audit::NodeId(2), krabka_audit::NodeId(3)],
                ..Default::default()
            },
        )
    }));
    Arc::new(img)
}

fn non_leader_row() -> PartitionProduceResponse {
    tagged_wire!(PartitionProduceResponse {
        index: 0,
        error_code: crate::codes::NOT_LEADER_OR_FOLLOWER,
        base_offset: -1,
        log_append_time_ms: -1,
        log_start_offset: -1,
        record_errors: vec![],
        error_message: None,
        current_leader: tagged_wire!(LeaderIdAndEpoch {
            leader_id: 2,
            leader_epoch: 17,
        }),
    })
}
