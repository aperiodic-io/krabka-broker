//! Manifest fixtures that the unit tests of several submodules share.

use {
    super::{
        ChainHead, ChainStamp, EpochId, MANIFEST_FORMAT_VERSION, ManifestBody, ManifestSeq,
        ObjectEntry, SegmentIdentity, Sha256Digest,
    },
    uuid::Uuid,
};

pub(super) const KEY_ID: &str = "worm-key-1";

pub(super) fn located_object() -> ObjectEntry {
    ObjectEntry {
        suffix: ".log".to_string(),
        key: "archive/orders-3/00000000000000000100.log".to_string(),
        size_bytes: 4096,
        sha256: Sha256Digest::of(b"log body"),
        e_tag: Some("\"d41d8cd98f00b204e9800998ecf8427e-3\"".to_string()),
        version_id: Some("3HL4kqtJlcpXroDTDmjVBH40Nrjfkd".to_string()),
        create_precondition: false,
    }
}

pub(super) fn bare_object() -> ObjectEntry {
    ObjectEntry {
        suffix: ".index".to_string(),
        key: "archive/orders-3/00000000000000000100.index".to_string(),
        size_bytes: 64,
        sha256: Sha256Digest::of(b"index body"),
        e_tag: None,
        version_id: None,
        create_precondition: true,
    }
}

pub(super) fn sample_segment() -> SegmentIdentity {
    SegmentIdentity {
        topic: "orders".to_string(),
        topic_id: Uuid::from_u128(0x11),
        partition: 3,
        segment_id: Uuid::from_u128(0x22),
        start_offset: 100,
        end_offset: 199,
        max_timestamp_ms: 1_713_000_000_000,
        broker_id: 7,
        event_timestamp_ms: 1_713_000_001_000,
        segment_size_bytes: 4096,
        leader_epochs: maplit::btreemap! {0 => 100, 1 => 150},
        txn_index_empty: false,
    }
}

pub(super) fn sample_body() -> ManifestBody {
    ManifestBody {
        format_version: MANIFEST_FORMAT_VERSION,
        segment: sample_segment(),
        objects: vec![located_object(), bare_object()],
        chain: ChainStamp {
            epoch_id: EpochId(Uuid::from_u128(0x99)),
            seq: ManifestSeq(4),
            prev_head: ChainHead([7u8; 32]),
        },
    }
}
