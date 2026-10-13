//! The manifest document: the segment it describes, the objects it names,
//! and the envelope that carries the signature.
//!
//! These are the `serde` types of the manifest file. `ManifestBody` is the
//! signed content and `SegmentManifest` is the file as a whole.

use {
    super::{ChainStamp, ManifestSignature, Sha256Digest},
    crate::metadata::RemoteLogSegmentMetadata,
    serde::{Deserialize, Serialize},
    std::collections::BTreeMap,
    uuid::Uuid,
};

/// One object that a segment copy wrote to the archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectEntry {
    /// File suffix this object carries, such as `.log` or `.index`.
    pub suffix: String,
    /// Full object-store key of the object.
    pub key: String,
    /// Size of the object body in bytes.
    pub size_bytes: u64,
    /// `SHA-256` of the object body. The only integrity claim in this entry.
    pub sha256: Sha256Digest,
    /// Object-store `ETag`, as the store reported it at write time.
    ///
    /// A **locator, not an integrity proof**. A multipart `ETag` is a checksum
    /// of the part checksums and not of the object body, so it cannot confirm
    /// the bytes. Use it to find and pin an object; use
    /// [`Self::sha256`] to prove it.
    pub e_tag: Option<String>,
    /// Object-store version id, when the bucket has versioning on.
    ///
    /// A **locator, not an integrity proof**. It names which version of the
    /// key to read back. [`Self::sha256`] is the only integrity claim.
    pub version_id: Option<String>,
    /// Whether this write used an atomic create precondition.
    ///
    /// `false` means the object relied on the bucket's versioning and default
    /// retention policy, as multipart uploads do.
    #[serde(default)]
    pub create_precondition: bool,
}

/// The segment this manifest describes, flattened out of
/// [`RemoteLogSegmentMetadata`].
///
/// The manifest keeps its own copy rather than a reference to the metadata
/// store, because a verifier reads the archive alone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentIdentity {
    /// Topic name, for diagnostics.
    pub topic: String,
    /// Stable topic id.
    pub topic_id: Uuid,
    /// Partition index.
    pub partition: i32,
    /// Per-segment id.
    pub segment_id: Uuid,
    /// First offset (inclusive) in the segment.
    pub start_offset: i64,
    /// Last offset (inclusive) in the segment.
    pub end_offset: i64,
    /// Highest record timestamp in the segment.
    pub max_timestamp_ms: i64,
    /// Broker that copied the segment.
    pub broker_id: i32,
    /// Wall-clock time of the copy event.
    pub event_timestamp_ms: i64,
    /// Size of the `.log` data in bytes.
    pub segment_size_bytes: i64,
    /// Leader epoch to the first offset that epoch contributed.
    pub leader_epochs: BTreeMap<i32, i64>,
    /// `true` when the segment has no transaction index.
    pub txn_index_empty: bool,
}

impl SegmentIdentity {
    /// Flattens the fields a manifest records out of `md`.
    #[must_use]
    pub fn from_metadata(md: &RemoteLogSegmentMetadata) -> Self {
        let id = md.remote_log_segment_id();
        Self {
            topic: id.topic_id_partition.topic.clone(),
            topic_id: id.topic_id_partition.topic_id,
            partition: id.topic_id_partition.partition,
            segment_id: id.id,
            start_offset: md.start_offset(),
            end_offset: md.end_offset(),
            max_timestamp_ms: md.max_timestamp_ms(),
            broker_id: md.broker_id(),
            event_timestamp_ms: md.event_timestamp_ms(),
            segment_size_bytes: i64::from(md.segment_size_in_bytes()),
            leader_epochs: md
                .segment_leader_epochs()
                .iter()
                .map(|(epoch, offset)| (epoch.0, *offset))
                .collect(),
            txn_index_empty: md.txn_index_empty(),
        }
    }
}

/// Everything a manifest signature covers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManifestBody {
    /// Encoding version. See [`MANIFEST_FORMAT_VERSION`](super::MANIFEST_FORMAT_VERSION).
    pub format_version: u32,
    /// The segment this manifest describes.
    pub segment: SegmentIdentity,
    /// Every object the copy wrote, in the order the copy wrote them.
    pub objects: Vec<ObjectEntry>,
    /// Chain position of this manifest.
    pub chain: ChainStamp,
}

/// One segment manifest, as it is written to and read back from the archive.
///
/// The signature is a **sibling** of the body and never a member of it. A
/// signature can then not be included in what it signs, and the property holds
/// by the shape of the type rather than by the care of the writer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SegmentManifest {
    /// The signed content.
    pub body: ManifestBody,
    /// The signature, when the archive has a signing key configured.
    pub signature: Option<ManifestSignature>,
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::{
            metadata::{
                RemoteLogSegmentDetails, RemoteLogSegmentId, RemoteLogSegmentState,
                TopicIdPartition,
            },
            worm::manifest::{
                HexBytes,
                test_support::{KEY_ID, bare_object, located_object, sample_body, sample_segment},
            },
        },
        assert2::check,
        krabka_ids::LeaderEpoch,
    };

    #[test]
    fn manifest_json_round_trips() {
        let every_optional_populated = SegmentManifest {
            body: ManifestBody {
                objects: vec![located_object()],
                ..sample_body()
            },
            signature: Some(ManifestSignature {
                key_id: KEY_ID.to_string(),
                public_key: HexBytes(vec![0x01, 0x02, 0x03]),
                signature: HexBytes(vec![0xfe, 0xed]),
            }),
        };
        let every_optional_absent = SegmentManifest {
            body: ManifestBody {
                objects: vec![bare_object()],
                ..sample_body()
            },
            signature: None,
        };

        for (name, manifest) in [
            ("every optional populated", every_optional_populated),
            ("every optional absent", every_optional_absent),
        ] {
            let json = serde_json::to_string(&manifest).unwrap();
            let parsed: SegmentManifest = serde_json::from_str(&json).unwrap();
            check!(parsed == manifest, "case {name}");
        }

        // An unknown field is a decode failure, not a silently dropped one.
        let with_extra = serde_json::json!({
            "body": {"format_version": 1, "surprise": true},
            "signature": null,
        });
        check!(serde_json::from_value::<SegmentManifest>(with_extra).is_err());

        let mut legacy = serde_json::to_value(SegmentManifest {
            body: sample_body(),
            signature: None,
        })
        .unwrap();
        legacy["body"]["format_version"] = serde_json::Value::from(1);
        legacy["body"]["objects"][0]
            .as_object_mut()
            .unwrap()
            .remove("create_precondition");
        let legacy: SegmentManifest = serde_json::from_value(legacy).unwrap();
        check!(!legacy.body.objects[0].create_precondition);
    }

    #[test]
    fn segment_identity_flattens_metadata() {
        let md = RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(Uuid::from_u128(0x11), "orders", 3),
                Uuid::from_u128(0x22),
            ),
            100,
            199,
            1_713_000_000_000,
            7,
            1_713_000_001_000,
            RemoteLogSegmentDetails::new(
                4096,
                RemoteLogSegmentState::CopySegmentFinished,
                maplit::btreemap! {LeaderEpoch(0) => 100, LeaderEpoch(1) => 150},
            ),
        )
        .unwrap()
        .with_txn_index_empty(true);

        check!(
            SegmentIdentity::from_metadata(&md)
                == SegmentIdentity {
                    txn_index_empty: true,
                    ..sample_segment()
                }
        );
    }
}
