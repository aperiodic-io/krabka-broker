//! Krabka-private controller RPCs over Kafka TCP framing.
//!
//! These bodies are NOT part of `krabka-protocol`'s codegen. They are
//! controller-only and Krabka-specific, with hand-written `encode_v0` and
//! `decode_v0` methods. The KIP-595 quorum RPCs (Fetch, Vote, Begin, End) ride
//! the generated codecs instead. See [`crate::kraft::transport::wire`]. The two
//! types here back the observer metadata-fetch and the follower-to-leader
//! submit-change forward.
//!
//! Api keys: `1003` `SubmitChange` (forward), `1004` `MetadataFetch`
//! (observer), and `1005` `DelegationTokenMutation` (guarded forward). They
//! come from [`PrivateRpc::api_key`], so this module and `krabka-metadata`
//! cannot disagree.
//!
//! # Version negotiation
//!
//! The request header's `api_version` versions each body. No `ApiVersions`
//! response advertises these keys. The finalized `krabka.version` feature
//! negotiates them instead, as `metadata.version` negotiates Kafka's
//! inter-broker protocol versions. The controller finalizes a `krabka.version`
//! level only once every registered node supports it, so the level tells a
//! sender which request versions every peer serves:
//!
//! - A **sender** sends [`private_request_version`]: the version that
//!   `krabka_metadata::private_rpc_version` gives for the `krabka.version`
//!   finalized in its current metadata image. With no image, or an image that
//!   has not finalized the feature, that is v0, the krabka-broker 1.0.0
//!   baseline that every 1.x node serves.
//! - A **receiver** serves every version from 0 up to
//!   [`private_api_highest_version`], the table's version at this build's
//!   `KRABKA_VERSION_MAX`. It answers any other version with
//!   [`PRIVATE_UNSUPPORTED_VERSION`] in the v0 response shape, which every 1.x
//!   sender decodes.
//!
//! At `krabka.version` 0 and 1 every RPC is v0. The first level that raises
//! one adds a `max_request_version` arm in `krabka-metadata`, and the matching
//! encode and decode methods here.

use {
    bytes::{Buf, BufMut, Bytes},
    krabka_metadata::{
        KrabkaVersion, MetadataImage, PrivateRpc,
        krabka_version::{KRABKA_VERSION_MAX, KRABKA_VERSION_MIN},
        private_rpc_version,
    },
    krabka_protocol::ProtocolError,
};

const I32_LEN: usize = 4;
const SUBMIT_CHANGE_RESPONSE_FIXED_LEN: usize = 10;
const METADATA_FETCH_REQUEST_LEN: usize = 32;
const METADATA_FETCH_RESPONSE_FIXED_LEN: usize = 54;
/// `snapshot_id.end_offset` sentinel meaning "no snapshot": the observer's
/// fetch offset is still inside the responder's retained log.
const NO_SNAPSHOT: i64 = -1;

/// Forwards a `Controller::submit_change` from a follower to the leader.
///
/// The body is the wincode-encoded `Vec<MetadataRecord>`. The response carries
/// a single `error_code`, where 0 means applied and any non-zero value means
/// not-leader or metadata-validation.
pub const API_KEY_SUBMIT_CHANGE: i16 = PrivateRpc::SubmitChange.api_key();

/// `KrabkaSubmitChangeResponse::error_code`: the leader refused a
/// compare-and-set until its uncommitted tail commits. The forwarding node
/// turns it back into [`crate::RaftError::UncommittedTail`].
pub const SUBMIT_CHANGE_UNCOMMITTED_TAIL: i16 = 4;

/// The `error_code` of a krabka-private response when the connection
/// principal lacks `ClusterAction` on the cluster. It is Kafka's
/// `CLUSTER_AUTHORIZATION_FAILED`.
pub const PRIVATE_CLUSTER_AUTHORIZATION_FAILED: i16 = 31;

/// Observer metadata fetch.
///
/// The body carries a `fetch_offset`, which is a `KraftLog` offset, and
/// `max_bytes`. The response carries committed `__cluster_metadata` entries
/// encoded as Kafka record batches, plus `log_start_offset`, `high_watermark`,
/// `quorum_high_watermark`, a `leader_hint`, the responder's `leader_epoch`,
/// and the KIP-630 `snapshot_id` that replaces the records when the fetch
/// offset has been pruned away.
pub const API_KEY_METADATA_FETCH: i16 = PrivateRpc::MetadataFetch.api_key();

/// Generation-bound delegation-token mutation forwarded to the leader.
pub const API_KEY_DELEGATION_TOKEN_MUTATION: i16 = PrivateRpc::DelegationTokenMutation.api_key();

/// The version of every krabka-private API that a sender uses when it has no
/// metadata image to read `krabka.version` from: v0, the krabka-broker 1.0.0
/// formats, which every 1.x receiver serves. It is also what an image that has
/// not finalized `krabka.version` negotiates, since that image reads as level
/// 0.
pub const PRIVATE_BASELINE_VERSION: i16 = 0;

/// The lowest version of every krabka-private API that this build serves.
const PRIVATE_LOWEST_VERSION: i16 = PRIVATE_BASELINE_VERSION;

/// The `error_code` of a krabka-private response to a request at a version
/// the receiver does not implement. It is Kafka's `UNSUPPORTED_VERSION`.
pub const PRIVATE_UNSUPPORTED_VERSION: i16 = 35;

/// The highest version of the krabka-private `api_key` this build serves, or
/// `None` when `api_key` is not one of them.
///
/// It is the version the `krabka.version` table gives at this build's
/// `KRABKA_VERSION_MAX`. A receiver serves every version from 0 up to it,
/// because a sender may send any version that a level up to that maximum
/// gives, and a level never lowers a version.
#[must_use]
pub const fn private_api_highest_version(api_key: i16) -> Option<i16> {
    private_rpc_version(api_key, KRABKA_VERSION_MAX)
}

/// The version of `rpc` a sender puts in the request header, negotiated from
/// `image`, the sender's current metadata image.
///
/// It is `KrabkaVersion::finalized(image).max_request_version(rpc)`. The
/// fallbacks all land on a version every peer serves:
///
/// - With no image (`None`), it is [`PRIVATE_BASELINE_VERSION`], v0.
/// - An image that has not finalized `krabka.version` reads as level 0, which
///   gives v0 too.
/// - A level above this build's `KRABKA_VERSION_MAX` sends this build's
///   highest version. The controller finalized that level because every
///   registered node supports it, and a node that supports a level serves
///   every version below the one it gives.
#[must_use]
pub fn private_request_version(rpc: PrivateRpc, image: Option<&MetadataImage>) -> i16 {
    let Some(image) = image else {
        return PRIVATE_BASELINE_VERSION;
    };
    let level = KrabkaVersion::finalized(image)
        .level()
        .clamp(KRABKA_VERSION_MIN, KRABKA_VERSION_MAX);
    KrabkaVersion::new(level)
        .max_request_version(rpc)
        .unwrap_or(PRIVATE_BASELINE_VERSION)
}

/// The answer to a krabka-private request at `version` when this build does
/// not serve that version: the v0 response of `api_key` with
/// [`PRIVATE_UNSUPPORTED_VERSION`]. `None` when it serves `version`, or when
/// `api_key` is not a krabka-private API.
///
/// # Errors
/// Never in practice: the v0 error responses carry no payload.
pub fn unsupported_version_response(
    api_key: i16,
    version: i16,
) -> Result<Option<Bytes>, ProtocolError> {
    let Some(highest) = private_api_highest_version(api_key) else {
        return Ok(None);
    };
    if (PRIVATE_LOWEST_VERSION..=highest).contains(&version) {
        return Ok(None);
    }
    let mut out = Vec::new();
    if api_key == API_KEY_METADATA_FETCH {
        KrabkaMetadataFetchResponse {
            error_code: PRIVATE_UNSUPPORTED_VERSION,
            leader_hint: -1,
            leader_epoch: -1,
            log_start_offset: -1,
            high_watermark: -1,
            quorum_high_watermark: -1,
            snapshot_id: None,
            records: Bytes::new(),
        }
        .encode_v0(&mut out)?;
    } else {
        KrabkaSubmitChangeResponse {
            error_code: PRIVATE_UNSUPPORTED_VERSION,
            leader_hint: -1,
            result: Bytes::new(),
        }
        .encode_v0(&mut out)?;
    }
    Ok(Some(Bytes::from(out)))
}

fn require_remaining(buf: &[u8], required: usize) -> Result<(), ProtocolError> {
    match required.checked_sub(buf.remaining()) {
        Some(0) | None => Ok(()),
        Some(needed) => Err(ProtocolError::UnexpectedEof { needed }),
    }
}

fn put_i32_len_prefixed_bytes(
    out: &mut Vec<u8>,
    bytes: &Bytes,
    too_long: &'static str,
) -> Result<(), ProtocolError> {
    out.put_i32(i32::try_from(bytes.len()).map_err(|_| ProtocolError::InvalidValue(too_long))?);
    out.put_slice(bytes);
    Ok(())
}

fn get_i32_len_prefixed_bytes(
    buf: &mut &[u8],
    negative_len: &'static str,
) -> Result<Bytes, ProtocolError> {
    require_remaining(buf, I32_LEN)?;
    let len = buf.get_i32();
    let len = usize::try_from(len).map_err(|_| ProtocolError::InvalidValue(negative_len))?;
    require_remaining(buf, len)?;
    let bytes = Bytes::copy_from_slice(&buf[..len]);
    buf.advance(len);
    Ok(bytes)
}

/// Forward-to-leader payload.
///
/// The body is opaque wincode bytes that represent the `Vec<MetadataRecord>` to
/// apply. The controller layer owns the serde details, so the wire module stays
/// metadata-agnostic.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KrabkaSubmitChangeRequest {
    pub records: Bytes,
}

impl KrabkaSubmitChangeRequest {
    /// # Errors
    /// Returns an error if the record payload is too large for the wire format.
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        put_i32_len_prefixed_bytes(out, &self.records, "records length exceeds i32::MAX")
    }

    /// # Errors
    /// Returns an error if the payload is truncated or has an invalid length.
    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        let records = get_i32_len_prefixed_bytes(buf, "negative records length")?;
        Ok(Self { records })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KrabkaSubmitChangeResponse {
    /// 0 means success. Any other value is an opaque transport-level error
    /// code: 1 is not leader, 2 is metadata validation, 3 is other, and 4
    /// ([`SUBMIT_CHANGE_UNCOMMITTED_TAIL`]) is an uncommitted leader tail.
    pub error_code: i16,
    /// The leader id the responder believes is current, when the responder
    /// cannot apply the change itself. -1 means "unknown".
    pub leader_hint: i64,
    /// Wincode-encoded [`crate::SubmitChangeResult`] on success.
    pub result: Bytes,
}

impl KrabkaSubmitChangeResponse {
    /// Encodes this response with wire version zero.
    ///
    /// # Errors
    ///
    /// Returns an error when the response payload exceeds the protocol's
    /// signed 32-bit length field.
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i16(self.error_code);
        out.put_i64(self.leader_hint);
        put_i32_len_prefixed_bytes(out, &self.result, "result length exceeds i32::MAX")?;
        Ok(())
    }

    /// # Errors
    /// Returns an error if the response payload is truncated.
    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        require_remaining(buf, SUBMIT_CHANGE_RESPONSE_FIXED_LEN)?;
        Ok(Self {
            error_code: buf.get_i16(),
            leader_hint: buf.get_i64(),
            result: get_i32_len_prefixed_bytes(buf, "negative result length")?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KrabkaMetadataFetchRequest {
    /// Next `KraftLog` offset the observer wants.
    pub fetch_offset: i64,
    /// Soft cap on the encoded record-batch payload.
    pub max_bytes: i32,
    /// The observer's node id, as Kafka's `FetchRequest.ReplicaId`. The leader
    /// tracks the observer's progress under it for `DescribeQuorum`. A
    /// negative id is a fetcher that is not a replica and is not tracked.
    pub replica_id: i32,
    /// The observer's directory id, as `FetchRequest.ReplicaDirectoryId`. Nil
    /// when the observer has none.
    pub replica_directory_id: uuid::Uuid,
}

impl KrabkaMetadataFetchRequest {
    pub fn encode_v0(&self, out: &mut Vec<u8>) {
        out.put_i64(self.fetch_offset);
        out.put_i32(self.max_bytes);
        out.put_i32(self.replica_id);
        out.put_slice(self.replica_directory_id.as_bytes());
    }

    /// # Errors
    /// Returns an error if the request payload is truncated.
    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        require_remaining(buf, METADATA_FETCH_REQUEST_LEN)?;
        let fetch_offset = buf.get_i64();
        let max_bytes = buf.get_i32();
        let replica_id = buf.get_i32();
        let mut directory = [0u8; 16];
        buf.copy_to_slice(&mut directory);
        Ok(Self {
            fetch_offset,
            max_bytes,
            replica_id,
            replica_directory_id: uuid::Uuid::from_bytes(directory),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KrabkaMetadataFetchResponse {
    /// 0 means success. 1 means this node cannot serve, so read
    /// `leader_hint`.
    pub error_code: i16,
    /// Leader id the responder believes is current. -1 means unknown.
    pub leader_hint: i64,
    /// The responder's quorum epoch. An observer sends it as the
    /// `CurrentLeaderEpoch` of a KIP-595 `FetchSnapshot`, which the leader
    /// checks against its own epoch.
    pub leader_epoch: i32,
    /// Lowest retained log offset on the responder.
    pub log_start_offset: i64,
    /// Highest committed and applied log offset on the responder. This bounds
    /// the records it can serve, and on a follower it is clamped to that
    /// follower's own log end.
    pub high_watermark: i64,
    /// Highest offset the *quorum* has committed, as the responder last heard
    /// it from the leader.
    ///
    /// Every controller serves this API, not only the leader, so an observer
    /// cannot read `high_watermark` as the quorum's progress: a follower that
    /// is itself catching up reports its own clamped watermark, and an
    /// observer that has drawn level with that follower would measure zero lag
    /// against a quorum thousands of records ahead. This field is what the
    /// readiness probe measures against.
    pub quorum_high_watermark: i64,
    /// The KIP-630 snapshot `(end_offset, epoch)` the observer must install
    /// before it can resume fetching, set when `fetch_offset` is below
    /// `log_start_offset` because the responder has pruned those records away.
    ///
    /// This is the observer's half of what `FetchResponse.SnapshotId` does for
    /// a controller follower (KIP-595). Without it a restarted observer fetches
    /// an offset the leader no longer holds, is served an empty slice, and
    /// never makes progress.
    pub snapshot_id: Option<(i64, i32)>,
    /// Concatenated Kafka `RecordBatch`es, one for each committed log batch.
    /// Empty whenever `snapshot_id` is set: the records the observer asked for
    /// are gone, so there is nothing to serve from the log.
    pub records: Bytes,
}

impl KrabkaMetadataFetchResponse {
    /// # Errors
    /// Returns an error if the record payload is too large for the wire format.
    pub fn encode_v0(&self, out: &mut Vec<u8>) -> Result<(), ProtocolError> {
        out.put_i16(self.error_code);
        out.put_i64(self.leader_hint);
        out.put_i32(self.leader_epoch);
        out.put_i64(self.log_start_offset);
        out.put_i64(self.high_watermark);
        out.put_i64(self.quorum_high_watermark);
        let (snapshot_end_offset, snapshot_epoch) = self.snapshot_id.unwrap_or((NO_SNAPSHOT, -1));
        out.put_i64(snapshot_end_offset);
        out.put_i32(snapshot_epoch);
        put_i32_len_prefixed_bytes(out, &self.records, "records length exceeds i32::MAX")
    }

    /// # Errors
    /// Returns an error if the payload is truncated or has an invalid length.
    pub fn decode_v0(buf: &mut &[u8]) -> Result<Self, ProtocolError> {
        require_remaining(buf, METADATA_FETCH_RESPONSE_FIXED_LEN)?;
        let error_code = buf.get_i16();
        let leader_hint = buf.get_i64();
        let leader_epoch = buf.get_i32();
        let log_start_offset = buf.get_i64();
        let high_watermark = buf.get_i64();
        let quorum_high_watermark = buf.get_i64();
        let snapshot_end_offset = buf.get_i64();
        let snapshot_epoch = buf.get_i32();
        let records = get_i32_len_prefixed_bytes(buf, "negative records length")?;
        Ok(Self {
            error_code,
            leader_hint,
            leader_epoch,
            log_start_offset,
            high_watermark,
            quorum_high_watermark,
            snapshot_id: (snapshot_end_offset != NO_SNAPSHOT)
                .then_some((snapshot_end_offset, snapshot_epoch)),
            records,
        })
    }
}

#[cfg(test)]
mod tests {
    use {super::*, assert2::check};

    /// The v0 `SubmitChange` request body that [`submit_request`] encodes to.
    const SUBMIT_REQUEST_V0: &[u8] = &[0, 0, 0, 3, 1, 2, 3];

    /// The v0 `SubmitChange` response body that [`submit_response`] encodes to.
    const SUBMIT_RESPONSE_V0: &[u8] = &[0, 4, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 2, b'o', b'k'];

    /// The v0 `MetadataFetch` request body that [`fetch_request`] encodes to.
    const FETCH_REQUEST_V0: &[u8] = &[
        0, 0, 0, 0, 0, 0, 0, 42, 0, 0x10, 0, 0, 0, 0, 0, 7, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,
        13, 14, 15, 16,
    ];

    /// The v0 `MetadataFetch` response body that [`fetch_response`] encodes to.
    const FETCH_RESPONSE_V0: &[u8] = &[
        0, 0, // error_code
        0, 0, 0, 0, 0, 0, 0, 3, // leader_hint
        0, 0, 0, 4, // leader_epoch
        0, 0, 0, 0, 0, 0, 0, 1, // log_start_offset
        0, 0, 0, 0, 0, 0, 0, 99, // high_watermark
        0, 0, 0, 0, 0, 0, 2, 0, // quorum_high_watermark
        0, 0, 0, 0, 0, 0, 0, 64, // snapshot end offset
        0, 0, 0, 2, // snapshot epoch
        0, 0, 0, 1, 0xaa, // records
    ];

    fn submit_request() -> KrabkaSubmitChangeRequest {
        KrabkaSubmitChangeRequest {
            records: Bytes::from_static(b"\x01\x02\x03"),
        }
    }

    fn submit_response() -> KrabkaSubmitChangeResponse {
        KrabkaSubmitChangeResponse {
            error_code: SUBMIT_CHANGE_UNCOMMITTED_TAIL,
            leader_hint: 3,
            result: Bytes::from_static(b"ok"),
        }
    }

    fn fetch_request() -> KrabkaMetadataFetchRequest {
        KrabkaMetadataFetchRequest {
            fetch_offset: 42,
            max_bytes: 1_048_576,
            replica_id: 7,
            replica_directory_id: uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10),
        }
    }

    fn fetch_response() -> KrabkaMetadataFetchResponse {
        KrabkaMetadataFetchResponse {
            error_code: 0,
            leader_hint: 3,
            leader_epoch: 4,
            log_start_offset: 1,
            high_watermark: 99,
            quorum_high_watermark: 512,
            snapshot_id: Some((64, 2)),
            records: Bytes::from_static(b"\xaa"),
        }
    }

    /// The v0 bodies of the krabka-private RPCs, byte for byte. A change to
    /// one of them is a change to the 1.x rolling-upgrade contract.
    #[test]
    fn v0_bodies_match_their_golden_bytes() {
        let mut submit_request_bytes = Vec::new();
        submit_request()
            .encode_v0(&mut submit_request_bytes)
            .unwrap();
        let mut submit_response_bytes = Vec::new();
        submit_response()
            .encode_v0(&mut submit_response_bytes)
            .unwrap();
        let mut fetch_request_bytes = Vec::new();
        fetch_request().encode_v0(&mut fetch_request_bytes);
        let mut fetch_response_bytes = Vec::new();
        fetch_response()
            .encode_v0(&mut fetch_response_bytes)
            .unwrap();

        let cases: [(&str, Vec<u8>, &[u8]); 4] = [
            (
                "SubmitChange request",
                submit_request_bytes,
                SUBMIT_REQUEST_V0,
            ),
            (
                "SubmitChange response",
                submit_response_bytes,
                SUBMIT_RESPONSE_V0,
            ),
            (
                "MetadataFetch request",
                fetch_request_bytes,
                FETCH_REQUEST_V0,
            ),
            (
                "MetadataFetch response",
                fetch_response_bytes,
                FETCH_RESPONSE_V0,
            ),
        ];
        for (case, encoded, golden) in cases {
            check!(encoded == golden, "{case}");
        }
    }

    /// The golden v0 bodies decode back to the values they were made from.
    #[test]
    fn golden_v0_bodies_decode_back() {
        check!(
            KrabkaSubmitChangeRequest::decode_v0(&mut &*SUBMIT_REQUEST_V0).unwrap()
                == submit_request()
        );
        check!(
            KrabkaSubmitChangeResponse::decode_v0(&mut &*SUBMIT_RESPONSE_V0).unwrap()
                == submit_response()
        );
        check!(
            KrabkaMetadataFetchRequest::decode_v0(&mut &*FETCH_REQUEST_V0).unwrap()
                == fetch_request()
        );
        check!(
            KrabkaMetadataFetchResponse::decode_v0(&mut &*FETCH_RESPONSE_V0).unwrap()
                == fetch_response()
        );
    }

    /// A receiver serves every version from 0 up to the highest its
    /// `krabka.version` table gives at `KRABKA_VERSION_MAX`, and answers any
    /// other version with `UNSUPPORTED_VERSION` in that API's v0 response
    /// shape.
    #[test]
    fn unsupported_versions_are_answered_in_the_v0_shape() {
        let submit_refusal = KrabkaSubmitChangeResponse {
            error_code: PRIVATE_UNSUPPORTED_VERSION,
            leader_hint: -1,
            result: Bytes::new(),
        };
        let mut submit_refusal_bytes = Vec::new();
        submit_refusal.encode_v0(&mut submit_refusal_bytes).unwrap();
        let fetch_refusal = KrabkaMetadataFetchResponse {
            error_code: PRIVATE_UNSUPPORTED_VERSION,
            leader_hint: -1,
            leader_epoch: -1,
            log_start_offset: -1,
            high_watermark: -1,
            quorum_high_watermark: -1,
            snapshot_id: None,
            records: Bytes::new(),
        };
        let mut fetch_refusal_bytes = Vec::new();
        fetch_refusal.encode_v0(&mut fetch_refusal_bytes).unwrap();
        let submit = Some(Bytes::from(submit_refusal_bytes));
        let fetch = Some(Bytes::from(fetch_refusal_bytes));
        let max = |api_key| private_api_highest_version(api_key).expect("a private api");
        let cases = [
            (API_KEY_SUBMIT_CHANGE, 0, None),
            (API_KEY_SUBMIT_CHANGE, max(API_KEY_SUBMIT_CHANGE), None),
            (
                API_KEY_SUBMIT_CHANGE,
                max(API_KEY_SUBMIT_CHANGE) + 1,
                submit.clone(),
            ),
            (API_KEY_SUBMIT_CHANGE, -1, submit.clone()),
            (API_KEY_DELEGATION_TOKEN_MUTATION, 0, None),
            (
                API_KEY_DELEGATION_TOKEN_MUTATION,
                max(API_KEY_DELEGATION_TOKEN_MUTATION),
                None,
            ),
            (
                API_KEY_DELEGATION_TOKEN_MUTATION,
                max(API_KEY_DELEGATION_TOKEN_MUTATION) + 1,
                submit,
            ),
            (API_KEY_METADATA_FETCH, 0, None),
            (API_KEY_METADATA_FETCH, max(API_KEY_METADATA_FETCH), None),
            (
                API_KEY_METADATA_FETCH,
                max(API_KEY_METADATA_FETCH) + 1,
                fetch,
            ),
            (1, 99, None),
        ];
        for (api_key, version, want) in cases {
            check!(
                unsupported_version_response(api_key, version).unwrap() == want,
                "api {api_key} v{version}"
            );
        }
    }

    /// The api keys are `PrivateRpc`'s, and this build serves exactly v0 of
    /// each: the only bodies this module has codecs for. A `krabka.version`
    /// level that raises a version fails this row until the module gains the
    /// matching encode and decode methods.
    #[test]
    fn private_apis_come_from_the_krabka_version_table() {
        let actual: Vec<_> = PrivateRpc::ALL
            .iter()
            .map(|&rpc| {
                (
                    rpc,
                    rpc.api_key(),
                    private_api_highest_version(rpc.api_key()),
                )
            })
            .collect();
        let expected = vec![
            (PrivateRpc::SubmitChange, API_KEY_SUBMIT_CHANGE, Some(0)),
            (PrivateRpc::MetadataFetch, API_KEY_METADATA_FETCH, Some(0)),
            (
                PrivateRpc::DelegationTokenMutation,
                API_KEY_DELEGATION_TOKEN_MUTATION,
                Some(0),
            ),
        ];
        check!(actual == expected);
        check!(
            (
                API_KEY_SUBMIT_CHANGE,
                API_KEY_METADATA_FETCH,
                API_KEY_DELEGATION_TOKEN_MUTATION
            ) == (1003, 1004, 1005)
        );
        check!(private_api_highest_version(1002).is_none());
    }

    /// A sender's request version follows the `krabka.version` finalized in
    /// its image: v0 without an image, with the feature absent, and at levels
    /// 0 and 1, which are all the krabka-broker 1.0.0 formats. A level above
    /// this build's maximum sends this build's highest version.
    #[test]
    fn sender_version_follows_the_finalized_krabka_version() {
        use krabka_metadata::{
            FeatureLevelRecord, MetadataRecord, krabka_version::KRABKA_VERSION_FEATURE,
        };

        let image_at = |level: Option<i16>| {
            let mut image = MetadataImage::new(uuid::Uuid::nil());
            if let Some(level) = level {
                image.apply(&MetadataRecord::V1FeatureLevel(FeatureLevelRecord {
                    name: KRABKA_VERSION_FEATURE.into(),
                    level,
                }));
            }
            image
        };
        let rows: [(&str, Option<MetadataImage>, [i16; 3]); 5] = [
            ("no image", None, [0, 0, 0]),
            ("feature absent", Some(image_at(None)), [0, 0, 0]),
            ("level 0", Some(image_at(Some(0))), [0, 0, 0]),
            ("level 1", Some(image_at(Some(1))), [0, 0, 0]),
            (
                "level above this build's maximum",
                Some(image_at(Some(KRABKA_VERSION_MAX + 1))),
                [0, 0, 0],
            ),
        ];
        let actual: Vec<_> = rows
            .iter()
            .map(|(case, image, _)| {
                (
                    *case,
                    PrivateRpc::ALL.map(|rpc| private_request_version(rpc, image.as_ref())),
                )
            })
            .collect();
        let expected: Vec<_> = rows.iter().map(|(case, _, want)| (*case, *want)).collect();
        check!(actual == expected);
        check!(private_request_version(PrivateRpc::SubmitChange, None) == PRIVATE_BASELINE_VERSION);
    }

    fn assert_unexpected_eof<T: std::fmt::Debug>(result: Result<T, ProtocolError>, want: usize) {
        match result {
            Err(ProtocolError::UnexpectedEof { needed }) => assert2::assert!(needed == want),
            other => panic!("expected UnexpectedEof {{ needed: {want} }}, got {other:?}"),
        }
    }

    fn assert_invalid_value<T: std::fmt::Debug>(result: Result<T, ProtocolError>) {
        match result {
            Err(ProtocolError::InvalidValue(_)) => {}
            other => panic!("expected InvalidValue, got {other:?}"),
        }
    }

    #[test]
    fn submit_change_round_trips() {
        let req = KrabkaSubmitChangeRequest {
            records: Bytes::from_static(b"\x01\x02\x03"),
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert2::assert!(KrabkaSubmitChangeRequest::decode_v0(&mut cur).unwrap() == req);

        let resp = KrabkaSubmitChangeResponse {
            error_code: 1,
            leader_hint: 3,
            result: Bytes::from_static(b"result"),
        };
        let mut out = Vec::new();
        resp.encode_v0(&mut out).unwrap();
        let mut cur: &[u8] = &out;
        assert2::assert!(KrabkaSubmitChangeResponse::decode_v0(&mut cur).unwrap() == resp);
    }

    #[test]
    fn submit_change_request_decode_checks_prefix_and_payload_lengths() {
        let mut short_prefix: &[u8] = &[0, 0, 0];
        assert_unexpected_eof(KrabkaSubmitChangeRequest::decode_v0(&mut short_prefix), 1);

        let mut negative_len: &[u8] = &(-1_i32).to_be_bytes();
        assert_invalid_value(KrabkaSubmitChangeRequest::decode_v0(&mut negative_len));

        let mut exact_empty: &[u8] = &[0, 0, 0, 0];
        let decoded = KrabkaSubmitChangeRequest::decode_v0(&mut exact_empty).unwrap();
        assert2::assert!(decoded.records.is_empty());
        assert2::assert!(exact_empty.is_empty());

        let mut short_payload: &[u8] = &[0, 0, 0, 4, 0xaa];
        assert_unexpected_eof(KrabkaSubmitChangeRequest::decode_v0(&mut short_payload), 3);
    }

    #[test]
    fn submit_change_response_decode_checks_fixed_length() {
        let mut short: &[u8] = &[0, 1, 2];
        assert_unexpected_eof(KrabkaSubmitChangeResponse::decode_v0(&mut short), 7);
    }

    #[test]
    fn metadata_fetch_round_trips() {
        let req = KrabkaMetadataFetchRequest {
            fetch_offset: 42,
            max_bytes: 1_048_576,
            replica_id: 7,
            replica_directory_id: uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10),
        };
        let mut out = Vec::new();
        req.encode_v0(&mut out);
        let mut cur: &[u8] = &out;
        assert2::assert!(KrabkaMetadataFetchRequest::decode_v0(&mut cur).unwrap() == req);

        let resp = KrabkaMetadataFetchResponse {
            error_code: 0,
            leader_hint: 3,
            leader_epoch: 4,
            log_start_offset: 1,
            high_watermark: 99,
            quorum_high_watermark: 512,
            snapshot_id: None,
            records: Bytes::from_static(b"\x01\x02\x03"),
        };
        let mut out = Vec::new();
        resp.encode_v0(&mut out).unwrap();
        assert2::assert!(NO_SNAPSHOT == -1);
        let snapshot_end_offset = i64::from_be_bytes(out[38..46].try_into().unwrap());
        let snapshot_epoch = i32::from_be_bytes(out[46..50].try_into().unwrap());
        assert2::assert!(snapshot_end_offset == -1);
        assert2::assert!(snapshot_epoch == -1);
        let mut cur: &[u8] = &out;
        assert2::assert!(KrabkaMetadataFetchResponse::decode_v0(&mut cur).unwrap() == resp);
    }

    #[test]
    fn metadata_fetch_request_decode_checks_fixed_length() {
        let mut short: &[u8] = &[0, 1, 2, 3, 4];
        assert_unexpected_eof(KrabkaMetadataFetchRequest::decode_v0(&mut short), 27);

        let request = KrabkaMetadataFetchRequest {
            fetch_offset: 9,
            max_bytes: 512,
            replica_id: -1,
            replica_directory_id: uuid::Uuid::nil(),
        };
        let mut exact = Vec::new();
        request.encode_v0(&mut exact);
        assert2::assert!(exact.len() == METADATA_FETCH_REQUEST_LEN);
        let mut cur: &[u8] = &exact;
        let decoded = KrabkaMetadataFetchRequest::decode_v0(&mut cur).unwrap();
        assert2::assert!(decoded == request);
        assert2::assert!(cur.is_empty());
    }

    #[test]
    fn metadata_fetch_response_decode_checks_fixed_and_payload_lengths() {
        let mut short_fixed: &[u8] = &[0, 1, 2, 3, 4, 5, 6, 7, 8];
        assert_unexpected_eof(KrabkaMetadataFetchResponse::decode_v0(&mut short_fixed), 45);

        let resp = KrabkaMetadataFetchResponse {
            error_code: 0,
            leader_hint: -1,
            leader_epoch: 4,
            log_start_offset: 4,
            high_watermark: 4,
            quorum_high_watermark: 6,
            snapshot_id: None,
            records: Bytes::new(),
        };
        let mut exact = Vec::new();
        resp.encode_v0(&mut exact).unwrap();
        let mut cur: &[u8] = &exact;
        assert2::assert!(KrabkaMetadataFetchResponse::decode_v0(&mut cur).unwrap() == resp);
        assert2::assert!(cur.is_empty());

        let mut short_payload = Vec::new();
        short_payload.extend_from_slice(&0_i16.to_be_bytes());
        short_payload.extend_from_slice(&1_i64.to_be_bytes());
        short_payload.extend_from_slice(&5_i32.to_be_bytes());
        short_payload.extend_from_slice(&2_i64.to_be_bytes());
        short_payload.extend_from_slice(&3_i64.to_be_bytes());
        short_payload.extend_from_slice(&3_i64.to_be_bytes());
        short_payload.extend_from_slice(&(-1_i64).to_be_bytes());
        short_payload.extend_from_slice(&(-1_i32).to_be_bytes());
        short_payload.extend_from_slice(&4_i32.to_be_bytes());
        short_payload.push(0xaa);
        let mut cur: &[u8] = &short_payload;
        assert_unexpected_eof(KrabkaMetadataFetchResponse::decode_v0(&mut cur), 3);
    }

    /// A pruned observer fetch answers with the snapshot id instead of
    /// records, so the id has to survive the round trip as a `Some` and the
    /// `-1` sentinel has to decode back to `None` rather than to `(-1, -1)`.
    #[test]
    fn metadata_fetch_response_round_trips_a_snapshot_id() {
        for want in [Some((4_096_i64, 7_i32)), Some((0, 0)), None] {
            let resp = KrabkaMetadataFetchResponse {
                error_code: 0,
                leader_hint: 1,
                leader_epoch: 4,
                log_start_offset: 4_096,
                high_watermark: 5_000,
                quorum_high_watermark: 5_000,
                snapshot_id: want,
                records: Bytes::new(),
            };
            let mut out = Vec::new();
            resp.encode_v0(&mut out).unwrap();
            let mut cur: &[u8] = &out;
            let decoded = KrabkaMetadataFetchResponse::decode_v0(&mut cur).unwrap();
            assert2::assert!(decoded == resp, "snapshot id {want:?}");
            assert2::assert!(cur.is_empty());
        }
    }
}
