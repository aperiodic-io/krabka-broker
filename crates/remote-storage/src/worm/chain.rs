//! The per-partition manifest chain, and where the next manifest joins it.
//!
//! Each copy stamps a [`WormChainRecord`] onto the segment's
//! [`CustomMetadata`]. The record is the receipt: it says which chain run the
//! manifest belongs to, where in the run it sits, and what head it produced.
//! [`next_chain_stamp`] reads those receipts back and works out where the next
//! manifest goes.

use krabka_verified::{ChainStep, chain::select_chain_tip, chain_step};
use serde::{Deserialize, Serialize};

use crate::{
    metadata::{CustomMetadata, RemoteLogSegmentMetadata, RemoteLogSegmentState},
    worm::{
        error::WormError,
        manifest::{ChainHead, ChainStamp, EpochId, ManifestSeq},
    },
};

/// Version of the [`WormChainRecord`] JSON in a segment's
/// [`CustomMetadata`].
///
/// Part of the 1.x on-disk contract: a 1.x broker reads every chain record
/// that any earlier 1.x broker wrote to `__remote_log_metadata`.
pub const WORM_CHAIN_RECORD_VERSION: i16 = 0;

/// The JSON form of a [`WormChainRecord`], version first.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WormChainRecordJson {
    version: i16,
    epoch_id: EpochId,
    seq: ManifestSeq,
    prev_head: ChainHead,
    head: Option<ChainHead>,
    manifest_version_id: Option<String>,
}

/// Just the version of a chain record, read before the rest of it.
#[derive(Deserialize)]
struct WormChainRecordVersion {
    version: Option<serde_json::Value>,
}

/// The chain receipt a copy leaves on a segment's custom metadata.
///
/// The record has two forms. The **request** form, from
/// [`WormChainRecord::request`], carries no head: the broker builds it before
/// the copy, when the manifest bytes do not exist yet. The **receipt** form,
/// from [`WormChainRecord::with_head`], carries the head the manifest produced
/// and is the only form [`next_chain_stamp`] continues from.
///
/// On the wire the record is a JSON object with a required top-level
/// `"version"`, [`WORM_CHAIN_RECORD_VERSION`]. The object is closed
/// (`deny_unknown_fields`): a later field always comes with a new version, so
/// a reader never drops a part of a receipt it does not understand.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WormChainRecord {
    /// Chain run this manifest belongs to.
    pub epoch_id: EpochId,
    /// Position within the run.
    pub seq: ManifestSeq,
    /// Chain head before this manifest.
    pub prev_head: ChainHead,
    /// The head *after* this manifest. `None` in the pre-copy request form.
    pub head: Option<ChainHead>,
    /// Object-store version id of the manifest object, when the bucket has
    /// versioning on.
    pub manifest_version_id: Option<String>,
}

impl WormChainRecord {
    /// The pre-copy request form of a record at `stamp`, with no head yet.
    #[must_use]
    pub fn request(stamp: ChainStamp) -> Self {
        Self {
            epoch_id: stamp.epoch_id,
            seq: stamp.seq,
            prev_head: stamp.prev_head,
            head: None,
            manifest_version_id: None,
        }
    }

    /// Turns a request form into a receipt by recording the head the manifest
    /// produced.
    #[must_use]
    pub fn with_head(mut self, head: ChainHead) -> Self {
        self.head = Some(head);
        self
    }

    /// Records the object-store version id of the manifest object.
    #[must_use]
    pub fn with_manifest_version(mut self, version_id: Option<String>) -> Self {
        self.manifest_version_id = version_id;
        self
    }

    /// Encodes the record as the JSON bytes of a [`CustomMetadata`].
    ///
    /// # Panics
    ///
    /// Panics if `serde_json` cannot serialise the record. Every field is a
    /// `UUID`, an integer, a fixed-size byte array, or a string, so no
    /// serialisation of this type can fail.
    #[must_use]
    pub fn to_custom_metadata(&self) -> CustomMetadata {
        let json = serde_json::to_vec(&WormChainRecordJson {
            version: WORM_CHAIN_RECORD_VERSION,
            epoch_id: self.epoch_id,
            seq: self.seq,
            prev_head: self.prev_head,
            head: self.head,
            manifest_version_id: self.manifest_version_id.clone(),
        })
        .expect("WormChainRecord holds only infallibly serialisable fields");
        CustomMetadata(json)
    }

    /// Decodes a record from a segment's [`CustomMetadata`].
    ///
    /// Never panics. Arbitrary bytes produce an error.
    ///
    /// # Errors
    ///
    /// Returns [`WormError::UnsupportedChainRecordVersion`] when the object
    /// has no `"version"`, as a record written before krabka 1.0 does, or a
    /// version other than [`WORM_CHAIN_RECORD_VERSION`]. Returns
    /// [`WormError::MalformedChainRecord`] when `cm` does not hold the JSON of
    /// a chain record: bytes that are not `UTF-8`, text that is not a JSON
    /// object, an object with a missing or unknown field, or a hex string that
    /// is not 64 characters.
    pub fn from_custom_metadata(cm: &CustomMetadata) -> Result<Self, WormError> {
        let malformed = |e: serde_json::Error| WormError::MalformedChainRecord(e.to_string());
        let probe: WormChainRecordVersion = serde_json::from_slice(&cm.0).map_err(malformed)?;
        let version = probe.version.map(|version| version.to_string());
        if version.as_deref() != Some(WORM_CHAIN_RECORD_VERSION.to_string().as_str()) {
            return Err(WormError::UnsupportedChainRecordVersion { found: version });
        }
        let json: WormChainRecordJson = serde_json::from_slice(&cm.0).map_err(malformed)?;
        Ok(Self {
            epoch_id: json.epoch_id,
            seq: json.seq,
            prev_head: json.prev_head,
            head: json.head,
            manifest_version_id: json.manifest_version_id,
        })
    }

    /// The chain position the next manifest takes after this one.
    ///
    /// `None` for the request form, which has produced no head to chain onto.
    #[must_use]
    pub fn next_stamp(&self) -> Option<ChainStamp> {
        let head = self.head?;
        match chain_step(self.seq.0, self.seq.0, true) {
            ChainStep::Continue(next) => Some(ChainStamp {
                epoch_id: self.epoch_id,
                seq: ManifestSeq(next),
                prev_head: head,
            }),
            ChainStep::SequenceMismatch | ChainStep::HeadMismatch | ChainStep::Exhausted => None,
        }
    }
}

/// Next chain position for a partition, given every segment the metadata
/// manager knows about.
///
/// Picks the receipt on the segment with the greatest `start_offset`, breaking
/// a tie by the greatest `seq`, and continues from it. A segment in a delete
/// state is ignored, and so is a record in the request form, which carries no
/// head.
///
/// Returns a **fresh epoch at genesis** when no receipt survives. That is a new
/// partition, or a restart on the non-durable in-memory metadata manager, and
/// in both cases the old chain cannot be continued. A new epoch says so, rather
/// than restarting the old chain at sequence zero and looking like a rewrite.
/// Returns `None` when the selected receipt is at `u64::MAX`, because no later
/// sequence exists and restarting at genesis would hide exhaustion as a new
/// chain run. Returns `None` too when a live segment carries a chain record of
/// a version other than [`WORM_CHAIN_RECORD_VERSION`], because this build
/// cannot tell where that chain ends.
///
/// `new_epoch_id` is a parameter and not a `Uuid::new_v4()` call inside, so the
/// function stays pure and testable.
#[must_use]
pub fn next_chain_stamp(
    segments: &[RemoteLogSegmentMetadata],
    new_epoch_id: EpochId,
) -> Option<ChainStamp> {
    let mut candidates = Vec::with_capacity(segments.len());
    let mut receipts = Vec::with_capacity(segments.len());
    for md in segments {
        let receipt = if matches!(
            md.state(),
            RemoteLogSegmentState::DeleteSegmentStarted
                | RemoteLogSegmentState::DeleteSegmentFinished
        ) {
            None
        } else {
            match md
                .custom_metadata()
                .map(WormChainRecord::from_custom_metadata)
            {
                Some(Ok(record)) => Some(record).filter(|record| record.head.is_some()),
                // A chain record of a version this build does not read: the
                // chain cannot be continued, and starting a fresh epoch over
                // it would hide that. Custom metadata with no version at all
                // is no chain record, like any other backend's metadata: the
                // object may be another backend's JSON receipt, and a pre-1.0
                // record never reaches a 1.x broker, whose cluster was
                // reformatted.
                Some(Err(WormError::UnsupportedChainRecordVersion { found: Some(_) })) => {
                    return None;
                }
                Some(Err(_)) | None => None,
            }
        };
        let sequence = receipt.as_ref().map_or(0, |record| record.seq.0);
        candidates.push((md.start_offset(), sequence, receipt.is_some()));
        receipts.push(receipt);
    }
    let Some(index) = select_chain_tip(&candidates) else {
        return Some(ChainStamp {
            epoch_id: new_epoch_id,
            seq: ManifestSeq(0),
            prev_head: ChainHead::GENESIS,
        });
    };
    receipts.get(index)?.as_ref()?.next_stamp()
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use krabka_ids::LeaderEpoch;
    use proptest::{collection::vec as prop_vec, num::u8::ANY as ANY_U8, proptest};
    use uuid::Uuid;

    use super::*;
    use crate::metadata::{RemoteLogSegmentDetails, RemoteLogSegmentId, TopicIdPartition};

    fn epoch() -> EpochId {
        EpochId(Uuid::from_u128(0x1234))
    }

    fn head(byte: u8) -> ChainHead {
        ChainHead([byte; 32])
    }

    fn sample_metadata(
        start_offset: i64,
        state: RemoteLogSegmentState,
        custom: Option<CustomMetadata>,
    ) -> RemoteLogSegmentMetadata {
        let md = RemoteLogSegmentMetadata::new(
            RemoteLogSegmentId::new(
                TopicIdPartition::new(Uuid::from_u128(1), "orders", 0),
                Uuid::from_u128(u128::try_from(start_offset).unwrap() + 100),
            ),
            start_offset,
            start_offset + 99,
            123,
            1,
            456,
            RemoteLogSegmentDetails::new(
                8,
                state,
                maplit::btreemap! {LeaderEpoch(0) => start_offset},
            ),
        )
        .unwrap();
        match custom {
            Some(cm) => md.with_custom_metadata(cm),
            None => md,
        }
    }

    fn receipt(seq: u64, prev: u8, produced: u8) -> CustomMetadata {
        WormChainRecord::request(ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(seq),
            prev_head: head(prev),
        })
        .with_head(head(produced))
        .to_custom_metadata()
    }

    #[test]
    fn next_chain_stamp_starts_new_epoch_on_empty_partition() {
        let fresh = EpochId(Uuid::from_u128(0xfeed));
        check!(
            next_chain_stamp(&[], fresh)
                == Some(ChainStamp {
                    epoch_id: fresh,
                    seq: ManifestSeq(0),
                    prev_head: ChainHead::GENESIS,
                })
        );
    }

    #[test]
    fn next_chain_stamp_continues_from_highest_offset_receipt() {
        let segments = [
            sample_metadata(
                0,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(receipt(0, 0x00, 0xaa)),
            ),
            sample_metadata(
                200,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(receipt(2, 0xbb, 0xcc)),
            ),
            sample_metadata(
                100,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(receipt(1, 0xaa, 0xbb)),
            ),
        ];
        check!(
            next_chain_stamp(&segments, EpochId(Uuid::from_u128(0xfeed)))
                == Some(ChainStamp {
                    epoch_id: epoch(),
                    seq: ManifestSeq(3),
                    prev_head: head(0xcc),
                })
        );
    }

    #[test]
    fn next_chain_stamp_breaks_offset_ties_by_greatest_seq() {
        let request_at_same_offset = WormChainRecord::request(ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(9),
            prev_head: head(0xcc),
        })
        .with_head(head(0xdd))
        .to_custom_metadata();
        let segments = [
            sample_metadata(
                100,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(receipt(1, 0xaa, 0xbb)),
            ),
            sample_metadata(
                100,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(request_at_same_offset),
            ),
        ];
        check!(
            next_chain_stamp(&segments, EpochId(Uuid::from_u128(0xfeed)))
                == Some(ChainStamp {
                    epoch_id: epoch(),
                    seq: ManifestSeq(10),
                    prev_head: head(0xdd),
                })
        );
    }

    #[test]
    fn next_chain_stamp_ignores_request_form_records() {
        let request_only = WormChainRecord::request(ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(7),
            prev_head: head(0xbb),
        })
        .to_custom_metadata();
        let segments = [
            sample_metadata(
                100,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(receipt(1, 0xaa, 0xbb)),
            ),
            // Highest offset, but the copy never finished, so no head.
            sample_metadata(
                200,
                RemoteLogSegmentState::CopySegmentStarted,
                Some(request_only),
            ),
        ];
        check!(
            next_chain_stamp(&segments, EpochId(Uuid::from_u128(0xfeed)))
                == Some(ChainStamp {
                    epoch_id: epoch(),
                    seq: ManifestSeq(2),
                    prev_head: head(0xbb),
                })
        );
    }

    #[test]
    fn next_chain_stamp_ignores_deleted_segments() {
        let expected_stamp = Some(ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(2),
            prev_head: head(0xbb),
        });
        for (name, deleted_state) in [
            (
                "delete started",
                RemoteLogSegmentState::DeleteSegmentStarted,
            ),
            (
                "delete finished",
                RemoteLogSegmentState::DeleteSegmentFinished,
            ),
        ] {
            let segments = [
                sample_metadata(
                    100,
                    RemoteLogSegmentState::CopySegmentFinished,
                    Some(receipt(1, 0xaa, 0xbb)),
                ),
                sample_metadata(300, deleted_state, Some(receipt(5, 0xee, 0xff))),
            ];
            let actual = next_chain_stamp(&segments, EpochId(Uuid::from_u128(0xfeed)));
            check!(actual == expected_stamp, "case {name}");
        }
    }

    #[test]
    fn next_chain_stamp_starts_new_epoch_when_no_receipt_decodes() {
        let fresh = EpochId(Uuid::from_u128(0xfeed));
        let expected = ChainStamp {
            epoch_id: fresh,
            seq: ManifestSeq(0),
            prev_head: ChainHead::GENESIS,
        };
        for (name, custom) in [
            ("no custom metadata at all", None),
            ("empty custom metadata", Some(CustomMetadata(Vec::new()))),
            (
                "custom metadata from another backend",
                Some(CustomMetadata(b"s3://bucket/key".to_vec())),
            ),
            (
                "JSON that is not a chain record",
                Some(CustomMetadata(br#"{"key":"value"}"#.to_vec())),
            ),
            (
                "chain record with a bad head",
                Some(CustomMetadata(
                    br#"{"epoch_id":"00000000-0000-0000-0000-000000000001","seq":1,"prev_head":"zz","head":null,"manifest_version_id":null}"#
                        .to_vec(),
                )),
            ),
        ] {
            let segments = [sample_metadata(
                100,
                RemoteLogSegmentState::CopySegmentFinished,
                custom,
            )];
            check!(next_chain_stamp(&segments, fresh) == Some(expected), "case {name}");
        }
    }

    #[test]
    fn next_chain_stamp_rejects_sequence_exhaustion() {
        let exhausted = WormChainRecord::request(ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(u64::MAX),
            prev_head: head(0xaa),
        })
        .with_head(head(0xbb))
        .to_custom_metadata();
        let segments = [sample_metadata(
            100,
            RemoteLogSegmentState::CopySegmentFinished,
            Some(exhausted),
        )];

        check!(next_chain_stamp(&segments, EpochId(Uuid::from_u128(0xfeed))) == None);
    }

    #[test]
    fn chain_record_round_trips_through_custom_metadata() {
        let stamp = ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(11),
            prev_head: head(0x5a),
        };
        let request = WormChainRecord::request(stamp);
        let full = request
            .clone()
            .with_head(head(0x6b))
            .with_manifest_version(Some("3HL4kqtJlcpXroDTDmjVBH40Nrjfkd".to_string()));

        check!(
            request
                == WormChainRecord {
                    epoch_id: epoch(),
                    seq: ManifestSeq(11),
                    prev_head: head(0x5a),
                    head: None,
                    manifest_version_id: None,
                }
        );
        check!(request.next_stamp() == None);
        check!(
            full.next_stamp()
                == Some(ChainStamp {
                    epoch_id: epoch(),
                    seq: ManifestSeq(12),
                    prev_head: head(0x6b),
                })
        );

        for (name, record) in [("request form", request), ("receipt form", full)] {
            let encoded = record.to_custom_metadata();
            check!(
                WormChainRecord::from_custom_metadata(&encoded).unwrap() == record,
                "case {name}"
            );
        }
    }

    fn golden_record() -> WormChainRecord {
        WormChainRecord::request(ChainStamp {
            epoch_id: epoch(),
            seq: ManifestSeq(11),
            prev_head: head(0x5a),
        })
        .with_head(head(0x6b))
        .with_manifest_version(Some("v1".to_owned()))
    }

    /// The exact custom-metadata bytes of [`golden_record`]. A change here is
    /// a change to the 1.x `__remote_log_metadata` contract.
    const GOLDEN_RECORD: &str = concat!(
        r#"{"version":0,"epoch_id":"00000000-0000-0000-0000-000000001234","seq":11,"#,
        r#""prev_head":"5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a","#,
        r#""head":"6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b","#,
        r#""manifest_version_id":"v1"}"#,
    );

    #[test]
    fn chain_record_encodes_to_the_golden_bytes_and_decodes_back() {
        check!(golden_record().to_custom_metadata().0 == GOLDEN_RECORD.as_bytes());
        check!(
            WormChainRecord::from_custom_metadata(&CustomMetadata(
                GOLDEN_RECORD.as_bytes().to_vec()
            ))
            .unwrap()
                == golden_record()
        );
    }

    #[test]
    fn chain_record_of_a_missing_or_unknown_version_is_refused() {
        let golden: serde_json::Value = serde_json::from_str(GOLDEN_RECORD).unwrap();
        let with_version = |version: Option<serde_json::Value>| {
            let mut record = golden.clone();
            let object = record.as_object_mut().unwrap();
            object.remove("version");
            if let Some(version) = version {
                object.insert("version".to_owned(), version);
            }
            CustomMetadata(serde_json::to_vec(&record).unwrap())
        };
        for (name, custom, found) in [
            ("pre-1.0 record with no version", with_version(None), None),
            (
                "future version",
                with_version(Some(serde_json::json!(1))),
                Some("1"),
            ),
            (
                "version that is not a number",
                with_version(Some(serde_json::json!("0"))),
                Some("\"0\""),
            ),
        ] {
            let err = WormChainRecord::from_custom_metadata(&custom).unwrap_err();
            assert2::assert!(
                let WormError::UnsupportedChainRecordVersion { found: actual } = &err,
                "case {name}: {err}"
            );
            check!(actual.as_deref() == found, "case {name}");
        }
    }

    #[test]
    fn next_chain_stamp_refuses_to_continue_past_a_record_of_an_unknown_version() {
        let mut future: serde_json::Value = serde_json::from_str(GOLDEN_RECORD).unwrap();
        future["version"] = serde_json::json!(1);
        let segments = [
            sample_metadata(
                100,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(receipt(3, 0x01, 0x02)),
            ),
            sample_metadata(
                50,
                RemoteLogSegmentState::CopySegmentFinished,
                Some(CustomMetadata(serde_json::to_vec(&future).unwrap())),
            ),
        ];

        check!(next_chain_stamp(&segments, EpochId(Uuid::from_u128(0xfeed))) == None);
    }

    #[test]
    fn malformed_chain_record_is_an_error_not_a_panic() {
        let err = WormChainRecord::from_custom_metadata(&CustomMetadata(b"not json".to_vec()))
            .unwrap_err();
        check!(matches!(err, WormError::MalformedChainRecord(_)));
    }

    proptest! {
        #[test]
        fn from_custom_metadata_never_panics_on_arbitrary_bytes(
            bytes in prop_vec(ANY_U8, 0..512usize),
        ) {
            let _ = WormChainRecord::from_custom_metadata(&CustomMetadata(bytes));
        }
    }
}
