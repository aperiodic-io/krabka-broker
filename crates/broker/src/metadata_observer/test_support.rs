//! The fixtures more than one of this module's unit-test modules needs: the
//! per-fetch soft byte cap every test `ObserverConfig` is built with, the
//! config itself with everything a test does not care about already filled in,
//! and the `ApiVersions` handshake every `MockBroker` an observer dials has to
//! answer before it sees a request.

use {
    super::ObserverConfig,
    krabka_units::{ByteSize, mebibytes, minutes},
    std::{path::PathBuf, sync::Arc},
};

/// Per-fetch soft byte cap for every observer fixture: 1 MiB.
pub(crate) const TEST_MAX_FETCH_BYTES: ByteSize = mebibytes(1);

/// An observer config a fixture can build on: no voters, the plaintext dialer,
/// a real timer, and no self-written checkpoints. Tests override the fields
/// they are actually about with struct-update syntax, so a new field in
/// [`ObserverConfig`] does not have to be spelled out in every fixture.
///
/// `data_dir` is where the observer keeps its metadata checkpoints, so it must
/// outlive the observer — pass the path of a `TempDir` the test still holds.
pub(crate) fn observer_config(cluster_id: uuid::Uuid, data_dir: PathBuf) -> ObserverConfig {
    ObserverConfig {
        client_dispatch_queue_capacity:
            krabka_client_core::ConnectionDispatchQueueCapacity::default(),
        client_frame_max: krabka_client_core::ClientFrameMax::default(),
        voters: vec![],
        bootstrap_servers: vec![],
        dialer: Arc::new(krabka_raft::PlaintextDialer),
        client_id: "test-observer".into(),
        cluster_id,
        // Fixtures put the controller at node 1, so the observer is node 2.
        node_id: krabka_raft::NodeId(2),
        directory_id: uuid::Uuid::from_u128(2),
        data_dir,
        // Off by default: a fixture that is about resuming from disk turns it
        // on, and every other one is spared the image serialization.
        snapshot_interval_records: 0,
        snapshot_fetch_max: krabka_raft::kraft::snapshot_fetch::MetadataSnapshotFetchMax::default(),
        max_bytes: TEST_MAX_FETCH_BYTES,
        poll_interval: minutes(1),
        timer: Arc::new(qubit_clock::StdTimer::new()),
    }
}

/// The `ApiVersions` response body a [`krabka_client_core::MockBroker`] must
/// answer the dial handshake with before the observer can issue anything.
pub(crate) fn api_versions_response_v0() -> Vec<u8> {
    use krabka_protocol::{
        Encode as _,
        owned::{
            api_versions_request,
            api_versions_response::{ApiVersion, ApiVersionsResponse},
        },
    };

    let key = ApiVersion {
        api_key: api_versions_request::API_KEY,
        min_version: 0,
        max_version: 3,
        ..Default::default()
    };
    let resp = ApiVersionsResponse {
        error_code: 0,
        api_keys: vec![key],
        ..Default::default()
    };
    let mut buf = bytes::BytesMut::new();
    resp.encode(&mut buf, 0).expect("encode ApiVersions");
    buf.to_vec()
}

/// The KIP-631 value bytes of a `TopicRecord` for `name`: the frame version,
/// apiKey and apiVersion are its first three bytes, each a one-byte varint.
pub(crate) fn topic_value(name: &str, id: u128) -> Vec<u8> {
    let record = krabka_metadata::MetadataRecord::V1Topic(krabka_metadata::TopicRecord {
        name: name.into(),
        topic_id: uuid::Uuid::from_u128(id),
        partitions: 0,
        replication_factor: 1,
    });
    krabka_metadata::to_kraft_values(
        &record,
        &krabka_metadata::MetadataImage::new(uuid::Uuid::nil()),
    )
    .expect("encode a topic")
    .remove(0)
    .to_vec()
}

/// [`topic_value`] with byte `index` set to `byte`: index 0 is the frame
/// version, 1 the apiKey and 2 the apiVersion.
pub(crate) fn patched_topic_value(index: usize, byte: u8) -> Vec<u8> {
    let mut value = topic_value("patched", 99);
    value[index] = byte;
    value
}

/// A `NoOpRecord` carrying krabka-private tag 1003 whose body is not a
/// record.
pub(crate) fn undecodable_private_value() -> Vec<u8> {
    use krabka_protocol::{
        owned::no_op_record::NoOpRecord,
        records::metadata::KraftMetadataRecord,
        tagged_fields::{UnknownTaggedField, UnknownTaggedFields},
    };
    KraftMetadataRecord::NoOp(NoOpRecord {
        unknown_tagged_fields: UnknownTaggedFields(vec![UnknownTaggedField {
            tag: 1003,
            bytes: bytes::Bytes::from_static(&[0xff, 0xff, 0xff]),
        }]),
    })
    .encode_value(0)
    .expect("encode a private carrier")
    .to_vec()
}

/// A metadata batch at `base_offset` with one record per value.
pub(crate) fn values_batch(
    base_offset: i64,
    values: &[Vec<u8>],
) -> krabka_protocol::records::RecordBatch {
    let records: Vec<krabka_protocol::records::Record> = (0_i32..)
        .zip(values)
        .map(|(offset_delta, value)| krabka_protocol::records::Record {
            offset_delta,
            value: Some(bytes::Bytes::from(value.clone())),
            ..Default::default()
        })
        .collect();
    krabka_protocol::records::RecordBatch {
        base_offset,
        last_offset_delta: i32::try_from(records.len().saturating_sub(1)).expect("delta"),
        records,
        ..Default::default()
    }
}

/// A control batch at `base_offset` whose one record is a `KRaftVersionRecord`
/// with a negative `kraft.version`, which Kafka refuses on every role.
pub(crate) fn negative_kraft_version_batch(
    base_offset: i64,
) -> krabka_protocol::records::RecordBatch {
    use krabka_protocol::{
        owned::k_raft_version_record::KRaftVersionRecord,
        records::{header::Attributes, metadata::control::ControlRecord},
    };
    let (key, value) = ControlRecord::KRaftVersion(KRaftVersionRecord {
        k_raft_version: -1,
        ..Default::default()
    })
    .encode_key_value()
    .expect("encode a control record");
    krabka_protocol::records::RecordBatch {
        base_offset,
        attributes: Attributes::default().with_control(true),
        last_offset_delta: 0,
        records: vec![krabka_protocol::records::Record {
            key: Some(key),
            value: Some(value),
            ..Default::default()
        }],
        ..Default::default()
    }
}

/// The wire bytes of `batches`, as a `MetadataFetch` response carries them.
pub(crate) fn encode_batches(batches: &[krabka_protocol::records::RecordBatch]) -> bytes::Bytes {
    let mut out = Vec::new();
    for batch in batches {
        batch.encode(&mut out).expect("encode batch");
    }
    bytes::Bytes::from(out)
}
