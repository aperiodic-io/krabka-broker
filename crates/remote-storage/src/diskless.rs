//! Shared diskless-WAL object, index, and disaster-recovery capture codecs.

use std::collections::{BTreeMap, HashMap};

use bytes::{BufMut, Bytes, BytesMut};
use krabka_audit::FileEd25519Signer;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::worm::{
    ChainHead, ChainStamp, EpochId, HexBytes, MANIFEST_FORMAT_VERSION, ManifestBody, ManifestSeq,
    ManifestSignature, ObjectEntry, SegmentIdentity, SegmentManifest, Sha256Digest,
    TrustedManifestKeys, manifest_head, manifest_signing_bytes, verify_manifest_signature,
};

const MAGIC: [u8; 4] = *b"CKWL";
const OBJECT_VERSION: u16 = 1;
const HEADER_LEN: usize = 6;
const TRAILER_LEN: usize = 8;
const OBJECT_ENTRY_LEN: usize = 48;

/// Stable keyed replay fence ignored by every diskless index projection.
pub const REPLAY_FENCE_KEY: &[u8] = b"__krabka_diskless_replay_fence";

/// Synthetic expected-head name used for a signed capture boundary.
pub const CAPTURE_HEAD_NAME: &str = "diskless-capture";

const CAPTURE_STATE_KEY: &str = "__krabka_diskless_capture_state";

/// One partition's byte range in a flushed object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalIndexEntry {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub byte_start: u64,
    pub byte_len: u32,
    pub max_timestamp_ms: i64,
}

/// Compaction key for one logical range.
///
/// Every `__diskless_wal_index` key starts with a big-endian `i16` key
/// version. As in Kafka's coordinator records, the key version names the
/// record type the key and its value belong to, so a reader dispatches on it
/// before it looks at anything else and a key type it does not know is
/// refused instead of being decoded as another type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalIndexKey {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
}

impl WalIndexKey {
    /// Key version of a range key, whose value is a [`WalFlushRecord`].
    ///
    /// Part of the 1.x on-disk contract: a 1.x broker reads every range key
    /// that any earlier 1.x broker wrote.
    pub const KEY_VERSION: i16 = 0;
    const LEN: usize = PARTITION_KEY_PREFIX_LEN + 8;

    #[must_use]
    pub fn to_bytes(self) -> Bytes {
        let mut out = encode_partition_key_prefix::<{ Self::LEN }>(
            Self::KEY_VERSION,
            self.topic_id,
            self.partition,
        );
        out.extend_from_slice(&self.first_offset.to_be_bytes());
        out.into()
    }

    /// Decode a range key, or `None` when `bytes` are not one: another key
    /// version, or the wrong length.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let (topic_id, partition, rest) =
            decode_partition_key_prefix::<{ Self::LEN }>(bytes, Self::KEY_VERSION)?;
        Some(Self {
            topic_id,
            partition,
            first_offset: i64::from_be_bytes(rest.try_into().ok()?),
        })
    }
}

/// The leading key version of a `__diskless_wal_index` key, or `None` for a
/// key shorter than the version itself.
#[must_use]
pub fn wal_index_key_version(key: &[u8]) -> Option<i16> {
    Some(i16::from_be_bytes(key.get(..2)?.try_into().ok()?))
}

/// Length of the big-endian `i16` key version, topic id and big-endian `i32`
/// partition that every partition-scoped `__diskless_wal_index` key starts
/// with.
const PARTITION_KEY_PREFIX_LEN: usize = 22;

/// Start a `KEY_LEN`-byte partition-scoped key with its version, topic id and
/// partition.
///
/// The caller appends the rest of the key, if any.
fn encode_partition_key_prefix<const KEY_LEN: usize>(
    version: i16,
    topic_id: Uuid,
    partition: i32,
) -> Vec<u8> {
    let mut out = Vec::with_capacity(KEY_LEN);
    out.extend_from_slice(&version.to_be_bytes());
    out.extend_from_slice(topic_id.as_bytes());
    out.extend_from_slice(&partition.to_be_bytes());
    out
}

/// Split a `KEY_LEN`-byte partition-scoped key into its topic id, partition and
/// the bytes after them, or `None` when `bytes` are the wrong length or carry a
/// key version other than `version`.
fn decode_partition_key_prefix<const KEY_LEN: usize>(
    bytes: &[u8],
    version: i16,
) -> Option<(Uuid, i32, &[u8])> {
    let bytes: &[u8; KEY_LEN] = bytes.try_into().ok()?;
    if wal_index_key_version(bytes) != Some(version) {
        return None;
    }
    let topic_id = Uuid::from_bytes(bytes.get(2..18)?.try_into().ok()?);
    let partition = i32::from_be_bytes(bytes.get(18..PARTITION_KEY_PREFIX_LEN)?.try_into().ok()?);
    Some((topic_id, partition, bytes.get(PARTITION_KEY_PREFIX_LEN..)?))
}

impl From<&WalIndexEntry> for WalIndexKey {
    fn from(value: &WalIndexEntry) -> Self {
        Self {
            topic_id: value.topic_id,
            partition: value.partition,
            first_offset: value.first_offset,
        }
    }
}

/// Compaction key for a partition delete floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalDeleteFloorKey {
    pub topic_id: Uuid,
    pub partition: i32,
}

impl WalDeleteFloorKey {
    /// Key version of a delete-floor key, whose value is a
    /// [`WalDeleteFloorRecord`].
    ///
    /// Part of the 1.x on-disk contract: a 1.x broker reads every delete-floor
    /// key that any earlier 1.x broker wrote.
    pub const KEY_VERSION: i16 = 1;
    const LEN: usize = PARTITION_KEY_PREFIX_LEN;

    #[must_use]
    pub fn to_bytes(self) -> Bytes {
        encode_partition_key_prefix::<{ Self::LEN }>(
            Self::KEY_VERSION,
            self.topic_id,
            self.partition,
        )
        .into()
    }

    /// Decode a delete-floor key, or `None` when `bytes` are not one: another
    /// key version, or the wrong length.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        let (topic_id, partition, _) =
            decode_partition_key_prefix::<{ Self::LEN }>(bytes, Self::KEY_VERSION)?;
        Some(Self {
            topic_id,
            partition,
        })
    }
}

/// Split the leading big-endian `i16` version off a `__diskless_wal_index`
/// value, refusing any version other than `expected`.
fn split_value_version<'a>(
    bytes: &'a [u8],
    record: &str,
    expected: i16,
) -> Result<&'a [u8], String> {
    let (version, body) = bytes
        .split_first_chunk::<2>()
        .ok_or_else(|| format!("truncated {record}: {} bytes hold no version", bytes.len()))?;
    let version = i16::from_be_bytes(*version);
    if version != expected {
        return Err(format!(
            "unsupported {record} version {version}: this build reads version {expected}. \
             A record written before krabka 1.0 has no version prefix and is not readable; \
             reformat the cluster"
        ));
    }
    Ok(body)
}

/// Durable `DeleteRecords` floor.
///
/// The value is a big-endian `i16` [`Self::VERSION`] followed by the wincode
/// body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalDeleteFloorRecord {
    pub topic_id: Uuid,
    pub partition: i32,
    pub floor: i64,
}

impl WalDeleteFloorRecord {
    /// Value version of a delete-floor record.
    ///
    /// Part of the 1.x on-disk contract: a 1.x broker reads every delete-floor
    /// record that any earlier 1.x broker wrote.
    pub const VERSION: i16 = 0;

    /// Encode with the index topic codec.
    ///
    /// # Errors
    /// Returns the codec error when the record cannot be encoded.
    pub fn to_bytes(&self) -> Result<Bytes, String> {
        let body = <serde_wincode::SerdeCompat<Self> as wincode::Serialize>::serialize(self)
            .map_err(|error| error.to_string())?;
        let mut out = Vec::with_capacity(2 + body.len());
        out.extend_from_slice(&Self::VERSION.to_be_bytes());
        out.extend_from_slice(&body);
        Ok(out.into())
    }

    /// Decode the index topic codec.
    ///
    /// # Errors
    /// Returns an error for a missing or unsupported version, or when the body
    /// is malformed.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let body = split_value_version(bytes, "diskless WAL delete-floor record", Self::VERSION)?;
        <serde_wincode::SerdeCompat<Self> as wincode::Deserialize>::deserialize(body)
            .map_err(|error| error.to_string())
    }
}

/// Durable index value for a flushed object.
///
/// The value is a big-endian `i16` `format_version` followed by the wincode
/// body of `object_key` and `entries`, so a reader checks the version before it
/// decodes anything else.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WalFlushRecord {
    pub object_key: String,
    pub format_version: u16,
    pub entries: Vec<WalIndexEntry>,
}

/// The wincode body of a [`WalFlushRecord`], as it is encoded.
#[derive(Serialize)]
struct WalFlushBodyRef<'a> {
    object_key: &'a str,
    entries: &'a [WalIndexEntry],
}

/// The wincode body of a [`WalFlushRecord`], as it is decoded.
#[derive(Deserialize)]
struct WalFlushBody {
    object_key: String,
    entries: Vec<WalIndexEntry>,
}

impl WalFlushRecord {
    /// Value version of a flush record.
    ///
    /// Part of the 1.x on-disk contract: a 1.x broker reads every flush record
    /// that any earlier 1.x broker wrote. It is encoded as a big-endian `i16`.
    pub const FORMAT_VERSION: u16 = 2;

    /// Encode one keyed WAL range in the format a flusher publishes.
    ///
    /// # Errors
    /// Returns the index codec's serialization error.
    pub fn keyed_entry(object_key: &str, entry: WalIndexEntry) -> Result<(Bytes, Bytes), String> {
        let key = WalIndexKey::from(&entry).to_bytes();
        let value = Self {
            object_key: object_key.into(),
            format_version: Self::FORMAT_VERSION,
            entries: vec![entry],
        }
        .to_bytes()?;
        Ok((key, value))
    }

    /// Encode with the index topic codec.
    ///
    /// # Errors
    /// Returns an error for an unsupported version or codec failure.
    pub fn to_bytes(&self) -> Result<Bytes, String> {
        if self.format_version != Self::FORMAT_VERSION {
            return Err(format!(
                "unsupported diskless WAL index format version {}",
                self.format_version
            ));
        }
        let version = Self::wire_version();
        let body =
            <serde_wincode::SerdeCompat<WalFlushBodyRef<'_>> as wincode::Serialize>::serialize(
                &WalFlushBodyRef {
                    object_key: &self.object_key,
                    entries: &self.entries,
                },
            )
            .map_err(|error| error.to_string())?;
        let mut out = Vec::with_capacity(2 + body.len());
        out.extend_from_slice(&version.to_be_bytes());
        out.extend_from_slice(&body);
        Ok(out.into())
    }

    /// Decode with strict format-version checking.
    ///
    /// # Errors
    /// Returns an error for a missing or unsupported version, or malformed
    /// bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        let body = split_value_version(bytes, "diskless WAL index format", Self::wire_version())?;
        let body =
            <serde_wincode::SerdeCompat<WalFlushBody> as wincode::Deserialize>::deserialize(body)
                .map_err(|error| error.to_string())?;
        Ok(Self {
            object_key: body.object_key,
            format_version: Self::FORMAT_VERSION,
            entries: body.entries,
        })
    }

    /// [`Self::FORMAT_VERSION`] as the `i16` the value leads with. The
    /// version is far below `i16::MAX`, so the conversion keeps its value.
    const fn wire_version() -> i16 {
        Self::FORMAT_VERSION.cast_signed()
    }
}

/// One live capture range and its authoritative object.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CapturedWalRange {
    pub object_key: String,
    pub entry: WalIndexEntry,
}

/// Captured committed state for one data partition.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisklessPartitionCapture {
    pub topic: String,
    pub topic_id: Uuid,
    pub partition: i32,
    pub delete_floor: i64,
    pub recovery_cutoff: i64,
    pub ranges: Vec<CapturedWalRange>,
}

/// Portable committed diskless-WAL capture.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DisklessWalCapture {
    pub format_version: u16,
    pub captured_at_ms: u64,
    pub source_cutoffs: Vec<i64>,
    pub partitions: Vec<DisklessPartitionCapture>,
    /// Digest of the controller checkpoint captured beside this boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metadata_snapshot_sha256: Option<Sha256Digest>,
    /// Digest of the RLMM cache snapshot captured beside this boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rlmm_snapshot_sha256: Option<Sha256Digest>,
    /// Digest of the consumer-group offsets captured beside this boundary.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_offsets_sha256: Option<Sha256Digest>,
    /// Optional signed WORM boundary covering this capture and its WAL objects.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub authentication: Option<SegmentManifest>,
}

/// Just the version of a capture, read before the rest of it.
#[derive(Deserialize)]
struct DisklessWalCaptureVersion {
    format_version: Option<serde_json::Value>,
}

impl DisklessWalCapture {
    /// Version of `diskless-wal-index.json`, its required top-level
    /// `"format_version"` field.
    ///
    /// Part of the 1.x on-disk contract: a 1.x restore reads every capture
    /// that any earlier 1.x build wrote.
    pub const FORMAT_VERSION: u16 = 1;

    /// Decode JSON and reject a missing or unknown capture version.
    ///
    /// The version is read on its own first, so a capture of another version
    /// is reported as that, and not as whatever field its shape lacks.
    ///
    /// # Errors
    /// Returns an error for malformed JSON, a missing or unsupported version,
    /// or invalid state.
    pub fn from_slice(bytes: &[u8]) -> Result<Self, String> {
        let probe: DisklessWalCaptureVersion =
            serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        match probe.format_version {
            None => {
                return Err(
                    "diskless WAL capture has no format_version: it predates krabka \
                     1.0, which this build does not read"
                        .to_owned(),
                );
            }
            Some(version) if version != Self::FORMAT_VERSION => {
                return Err(format!(
                    "unsupported diskless WAL capture format version {version}: this build \
                     reads version {}",
                    Self::FORMAT_VERSION
                ));
            }
            Some(_) => {}
        }
        let capture: Self = serde_json::from_slice(bytes).map_err(|error| error.to_string())?;
        capture.validate()?;
        Ok(capture)
    }

    /// Validate all state that controls restore selection and boundaries.
    ///
    /// # Errors
    /// Returns an error for invalid partitions, ranges, floors, or cutoffs.
    pub fn validate(&self) -> Result<(), String> {
        if self.source_cutoffs.iter().any(|cutoff| *cutoff < 0) {
            return Err("negative diskless WAL source cutoff".to_owned());
        }
        let mut partitions = std::collections::HashSet::new();
        for partition in &self.partitions {
            if !valid_topic_name(&partition.topic)
                || partition.partition < 0
                || partition.delete_floor < 0
                || partition.recovery_cutoff < partition.delete_floor
                || !partitions.insert((partition.topic_id, partition.partition))
            {
                return Err(format!(
                    "invalid or duplicate diskless capture partition {}-{}",
                    partition.topic, partition.partition
                ));
            }
            let mut previous_last = None;
            let mut covered_through = partition.delete_floor;
            for range in &partition.ranges {
                if range.object_key.is_empty()
                    || range.object_key == CAPTURE_STATE_KEY
                    || range.entry.topic_id != partition.topic_id
                    || range.entry.partition != partition.partition
                    || range.entry.first_offset < 0
                    || range.entry.last_offset < range.entry.first_offset
                    || range.entry.byte_len == 0
                    || previous_last.is_some_and(|last| last >= range.entry.first_offset)
                {
                    return Err(format!(
                        "invalid or unordered diskless WAL range for {}-{}",
                        partition.topic, partition.partition
                    ));
                }
                if range.entry.last_offset >= partition.delete_floor {
                    if range.entry.first_offset > covered_through {
                        return Err(format!(
                            "diskless WAL ranges for {}-{} have a gap at {covered_through}",
                            partition.topic, partition.partition
                        ));
                    }
                    covered_through = range
                        .entry
                        .last_offset
                        .checked_add(1)
                        .ok_or_else(|| "diskless WAL recovery cutoff overflow".to_owned())?;
                }
                previous_last = Some(range.entry.last_offset);
            }
            let observed_cutoff = previous_last
                .map(|last| {
                    last.checked_add(1)
                        .ok_or_else(|| "diskless WAL recovery cutoff overflow".to_owned())
                })
                .transpose()?
                .unwrap_or(partition.delete_floor)
                .max(partition.delete_floor);
            if observed_cutoff != partition.recovery_cutoff {
                return Err(format!(
                    "diskless WAL ranges for {}-{} end at {observed_cutoff}, capture declares {}",
                    partition.topic, partition.partition, partition.recovery_cutoff
                ));
            }
        }
        Ok(())
    }

    fn unsigned_bytes(&self) -> Result<Vec<u8>, String> {
        let mut unsigned = self.clone();
        unsigned.authentication = None;
        serde_json::to_vec(&unsigned).map_err(|error| error.to_string())
    }

    /// Sign the exact capture state and referenced WAL object claims.
    ///
    /// # Errors
    /// Returns an error when capture state is invalid, object claims do not exactly cover it,
    /// or its deterministic encoding fails.
    pub fn seal(
        &mut self,
        mut objects: Vec<ObjectEntry>,
        signer: &FileEd25519Signer,
    ) -> Result<ChainHead, String> {
        self.validate()?;
        let expected = self
            .partitions
            .iter()
            .flat_map(|partition| &partition.ranges)
            .map(|range| range.object_key.clone())
            .collect::<std::collections::BTreeSet<_>>();
        objects.sort_by(|a, b| a.key.cmp(&b.key));
        let actual = objects
            .iter()
            .map(|object| object.key.clone())
            .collect::<std::collections::BTreeSet<_>>();
        if expected != actual || objects.len() != actual.len() {
            return Err("signed WAL claims do not exactly cover capture references".to_owned());
        }
        let state = self.unsigned_bytes()?;
        let state_digest = Sha256Digest::of(&state);
        objects.push(ObjectEntry {
            suffix: ".capture-state".to_owned(),
            key: CAPTURE_STATE_KEY.to_owned(),
            size_bytes: state.len() as u64,
            sha256: state_digest,
            e_tag: None,
            version_id: None,
            create_precondition: false,
        });
        let mut segment_id = [0; 16];
        segment_id.copy_from_slice(&state_digest.0[..16]);
        let identity = SegmentIdentity {
            topic: CAPTURE_HEAD_NAME.to_owned(),
            topic_id: Uuid::nil(),
            partition: 0,
            segment_id: Uuid::from_bytes(segment_id),
            start_offset: 0,
            end_offset: 0,
            max_timestamp_ms: i64::try_from(self.captured_at_ms).unwrap_or(i64::MAX),
            broker_id: -1,
            event_timestamp_ms: i64::try_from(self.captured_at_ms).unwrap_or(i64::MAX),
            segment_size_bytes: objects
                .iter()
                .map(|object| object.size_bytes)
                .sum::<u64>()
                .try_into()
                .unwrap_or(i64::MAX),
            leader_epochs: BTreeMap::new(),
            txn_index_empty: true,
        };
        let chain = ChainStamp {
            epoch_id: EpochId(Uuid::nil()),
            seq: ManifestSeq(0),
            prev_head: ChainHead::GENESIS,
        };
        let body = ManifestBody {
            format_version: MANIFEST_FORMAT_VERSION,
            segment: identity,
            objects,
            chain,
        };
        let head = manifest_head(&body);
        let message = manifest_signing_bytes(signer.key_id(), chain.epoch_id, chain.seq, head);
        self.authentication = Some(SegmentManifest {
            body,
            signature: Some(ManifestSignature {
                key_id: signer.key_id().to_owned(),
                public_key: HexBytes(signer.public_key()),
                signature: HexBytes(signer.sign(&message)),
            }),
        });
        Ok(head)
    }

    /// Verify the embedded signed boundary and return trusted WAL claims.
    ///
    /// # Errors
    /// Returns an error for invalid capture state, missing or untrusted evidence, a bad
    /// signature, a head mismatch, or incomplete object claims.
    pub fn authenticate(
        &self,
        trusted: &TrustedManifestKeys,
        expected_head: Option<&str>,
    ) -> Result<BTreeMap<String, ObjectEntry>, String> {
        self.validate()?;
        let manifest = self
            .authentication
            .as_ref()
            .ok_or_else(|| "diskless capture has no signed WORM boundary".to_owned())?;
        if manifest.body.format_version != MANIFEST_FORMAT_VERSION {
            return Err("unsupported diskless capture manifest version".to_owned());
        }
        let signature = manifest
            .signature
            .as_ref()
            .ok_or_else(|| "diskless capture WORM boundary is unsigned".to_owned())?;
        let key = trusted
            .get(&signature.key_id)
            .ok_or_else(|| format!("diskless capture uses untrusted key `{}`", signature.key_id))?;
        if !verify_manifest_signature(manifest, key) {
            return Err("diskless capture WORM signature is invalid".to_owned());
        }
        let head = manifest_head(&manifest.body).to_string();
        let expected_head = expected_head.ok_or_else(|| {
            format!("trusted diskless restore requires --worm-expect-head {CAPTURE_HEAD_NAME}=HEX")
        })?;
        if !head.eq_ignore_ascii_case(expected_head) {
            return Err(format!(
                "pinned diskless capture head is {expected_head}, authenticated head is {head}"
            ));
        }
        let state = self.unsigned_bytes()?;
        let mut claims = BTreeMap::new();
        let mut state_claim = None;
        for object in &manifest.body.objects {
            if object.key == CAPTURE_STATE_KEY {
                if state_claim.replace(object).is_some() {
                    return Err("duplicate signed diskless capture-state claim".to_owned());
                }
            } else if claims.insert(object.key.clone(), object.clone()).is_some() {
                return Err(format!("duplicate signed WAL claim `{}`", object.key));
            }
        }
        let state_claim = state_claim
            .ok_or_else(|| "signed diskless capture-state claim is missing".to_owned())?;
        if state_claim.size_bytes != state.len() as u64
            || state_claim.sha256 != Sha256Digest::of(&state)
        {
            return Err("diskless capture state changed after signing".to_owned());
        }
        let expected = self
            .partitions
            .iter()
            .flat_map(|partition| &partition.ranges)
            .map(|range| range.object_key.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        if expected != claims.keys().map(String::as_str).collect() {
            return Err("signed WAL claims do not exactly cover capture references".to_owned());
        }
        Ok(claims)
    }
}

fn valid_topic_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 249
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

/// Reducer for committed keyed index events.
#[derive(Default)]
pub struct WalCaptureProjection {
    ranges: HashMap<WalIndexKey, CapturedWalRange>,
    floors: HashMap<(Uuid, i32), i64>,
}

impl WalCaptureProjection {
    /// Apply one committed keyed value or tombstone.
    ///
    /// # Errors
    /// Returns an error for malformed keys, values, or inconsistent records.
    pub fn apply(&mut self, key: Option<&[u8]>, value: Option<&[u8]>) -> Result<(), String> {
        // Every index record is keyed: the flusher publishes one keyed record
        // per range, and compaction keeps only the latest per key. An
        // unkeyed record has no range it could be the latest for.
        let key = key.ok_or_else(|| "diskless WAL index record has no key".to_owned())?;
        if let Some(floor_key) = WalDeleteFloorKey::from_bytes(key) {
            if let Some(value) = value {
                let record = WalDeleteFloorRecord::from_bytes(value)?;
                if record.topic_id != floor_key.topic_id
                    || record.partition != floor_key.partition
                    || record.floor < 0
                {
                    return Err("diskless WAL delete-floor key/value mismatch".to_owned());
                }
                let floor = self
                    .floors
                    .entry((record.topic_id, record.partition))
                    .or_default();
                *floor = (*floor).max(record.floor);
            } else {
                self.floors
                    .remove(&(floor_key.topic_id, floor_key.partition));
            }
            return Ok(());
        }
        let range_key =
            WalIndexKey::from_bytes(key).ok_or_else(|| match wal_index_key_version(key) {
                Some(version)
                    if version != WalIndexKey::KEY_VERSION
                        && version != WalDeleteFloorKey::KEY_VERSION =>
                {
                    format!("unknown diskless WAL index key version {version}")
                }
                _ => "invalid diskless WAL index key".to_owned(),
            })?;
        let Some(value) = value else {
            self.ranges.remove(&range_key);
            return Ok(());
        };
        let record = WalFlushRecord::from_bytes(value)?;
        let entry = record
            .entries
            .into_iter()
            .find(|entry| WalIndexKey::from(entry) == range_key)
            .ok_or_else(|| "diskless WAL index key/value mismatch".to_owned())?;
        Self::validate_range(&entry)?;
        self.store_range(range_key, record.object_key, entry);
        Ok(())
    }

    fn validate_range(entry: &WalIndexEntry) -> Result<(), String> {
        if entry.first_offset < 0 || entry.last_offset < entry.first_offset || entry.byte_len == 0 {
            return Err("invalid diskless WAL index range".to_owned());
        }
        Ok(())
    }

    fn store_range(&mut self, key: WalIndexKey, object_key: String, entry: WalIndexEntry) {
        self.ranges
            .insert(key, CapturedWalRange { object_key, entry });
    }

    /// Freeze the projection into a deterministic portable capture.
    ///
    /// # Errors
    /// Returns an error for unknown topic ids or inconsistent live ranges.
    pub fn capture<S: std::hash::BuildHasher>(
        &self,
        topics: &HashMap<Uuid, (String, i32), S>,
        source_cutoffs: Vec<i64>,
        captured_at_ms: u64,
    ) -> Result<DisklessWalCapture, String> {
        let mut grouped: BTreeMap<(String, Uuid, i32), Vec<CapturedWalRange>> = BTreeMap::new();
        for (topic_id, (topic, partition_count)) in topics {
            if *partition_count < 0 {
                return Err(format!(
                    "negative partition count for diskless topic {topic}"
                ));
            }
            for partition in 0..*partition_count {
                grouped
                    .entry((topic.clone(), *topic_id, partition))
                    .or_default();
            }
        }
        for range in self.ranges.values() {
            let Some((topic, _)) = topics.get(&range.entry.topic_id) else {
                continue;
            };
            grouped
                .entry((topic.clone(), range.entry.topic_id, range.entry.partition))
                .or_default()
                .push(range.clone());
        }
        for (topic_id, partition) in self.floors.keys() {
            let Some((topic, _)) = topics.get(topic_id) else {
                continue;
            };
            grouped
                .entry((topic.clone(), *topic_id, *partition))
                .or_default();
        }
        let mut partitions = Vec::with_capacity(grouped.len());
        for ((topic, topic_id, partition), mut ranges) in grouped {
            ranges.sort_by_key(|range| range.entry.first_offset);
            if ranges
                .windows(2)
                .any(|pair| pair[0].entry.last_offset >= pair[1].entry.first_offset)
            {
                return Err(format!(
                    "overlapping diskless WAL ranges for {topic}-{partition}"
                ));
            }
            let mut runs: Vec<CapturedWalRange> = Vec::with_capacity(ranges.len());
            for range in ranges {
                let joins_previous = runs.last().is_some_and(|previous| {
                    previous.object_key == range.object_key
                        && previous.entry.last_offset.checked_add(1)
                            == Some(range.entry.first_offset)
                        && previous
                            .entry
                            .byte_start
                            .checked_add(u64::from(previous.entry.byte_len))
                            == Some(range.entry.byte_start)
                });
                if joins_previous {
                    if let Some(previous) = runs.last_mut() {
                        previous.entry.last_offset = range.entry.last_offset;
                        previous.entry.byte_len = previous
                            .entry
                            .byte_len
                            .checked_add(range.entry.byte_len)
                            .ok_or_else(|| {
                                format!("diskless WAL run exceeds 4 GiB for {topic}-{partition}")
                            })?;
                        previous.entry.max_timestamp_ms = previous
                            .entry
                            .max_timestamp_ms
                            .max(range.entry.max_timestamp_ms);
                    }
                } else {
                    runs.push(range);
                }
            }
            let ranges = runs;
            let delete_floor = self
                .floors
                .get(&(topic_id, partition))
                .copied()
                .unwrap_or(0);
            let recovery_cutoff = ranges
                .last()
                .map_or(delete_floor, |range| {
                    range.entry.last_offset.saturating_add(1)
                })
                .max(delete_floor);
            partitions.push(DisklessPartitionCapture {
                topic,
                topic_id,
                partition,
                delete_floor,
                recovery_cutoff,
                ranges,
            });
        }
        Ok(DisklessWalCapture {
            format_version: DisklessWalCapture::FORMAT_VERSION,
            captured_at_ms,
            source_cutoffs,
            partitions,
            metadata_snapshot_sha256: None,
            rlmm_snapshot_sha256: None,
            group_offsets_sha256: None,
            authentication: None,
        })
    }
}

/// One run recorded by a CKWL footer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalObjectEntry {
    pub topic_id: Uuid,
    pub partition: i32,
    pub first_offset: i64,
    pub last_offset: i64,
    pub byte_start: u64,
    pub byte_len: u32,
}

/// Invalid combined diskless-WAL object framing.
#[derive(Debug, thiserror::Error)]
pub enum WalObjectError {
    /// Object does not contain the minimum header and trailer.
    #[error("wal object too short")]
    TooShort,
    /// Header or trailer magic is invalid.
    #[error("bad wal object magic")]
    BadMagic,
    /// Object uses an unsupported framing version.
    #[error("unsupported wal object version {0}")]
    BadVersion(u16),
    /// Footer length, entry encoding, or byte ranges are invalid.
    #[error("corrupt wal object manifest")]
    BadManifest,
}

/// Builder for combined CKWL objects.
#[derive(Default)]
pub struct WalObjectBuilder {
    body: BytesMut,
    entries: Vec<WalObjectEntry>,
}

impl WalObjectBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
    #[must_use]
    pub fn body_len(&self) -> usize {
        self.body.len()
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    /// Append one contiguous partition run.
    ///
    /// # Panics
    /// Panics only if an in-memory object exceeds platform or CKWL length limits.
    pub fn append_run(
        &mut self,
        topic_id: Uuid,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        run: &[u8],
    ) {
        let byte_start = u64::try_from(HEADER_LEN + self.body.len()).expect("object size fits u64");
        let byte_len = u32::try_from(run.len()).expect("run size fits u32");
        self.body.extend_from_slice(run);
        self.entries.push(WalObjectEntry {
            topic_id,
            partition,
            first_offset,
            last_offset,
            byte_start,
            byte_len,
        });
    }

    #[must_use]
    /// # Panics
    /// Panics only if the in-memory footer exceeds 4 GiB.
    pub fn finish(self) -> Bytes {
        let mut out = BytesMut::with_capacity(
            HEADER_LEN + self.body.len() + self.entries.len() * OBJECT_ENTRY_LEN + TRAILER_LEN,
        );
        out.extend_from_slice(&MAGIC);
        out.put_u16_le(OBJECT_VERSION);
        out.extend_from_slice(&self.body);
        let footer_start = out.len();
        for entry in self.entries {
            out.extend_from_slice(entry.topic_id.as_bytes());
            out.put_i32_le(entry.partition);
            out.put_i64_le(entry.first_offset);
            out.put_i64_le(entry.last_offset);
            out.put_u64_le(entry.byte_start);
            out.put_u32_le(entry.byte_len);
        }
        out.put_u32_le(u32::try_from(out.len() - footer_start).expect("footer fits u32"));
        out.extend_from_slice(&MAGIC);
        out.freeze()
    }

    /// Append one run and finish the object.
    #[must_use]
    pub fn finish_with_run(
        mut self,
        topic_id: Uuid,
        partition: i32,
        first_offset: i64,
        last_offset: i64,
        run: &[u8],
    ) -> Bytes {
        self.append_run(topic_id, partition, first_offset, last_offset, run);
        self.finish()
    }
}

/// Parse and validate a CKWL footer.
///
/// # Errors
/// Returns an error for malformed framing, entries, versions, or byte ranges.
pub fn parse_wal_object(object: &Bytes) -> Result<Vec<WalObjectEntry>, WalObjectError> {
    if object.len() < HEADER_LEN + TRAILER_LEN {
        return Err(WalObjectError::TooShort);
    }
    if object[..4] != MAGIC || object[object.len() - 4..] != MAGIC {
        return Err(WalObjectError::BadMagic);
    }
    let version = u16::from_le_bytes([object[4], object[5]]);
    if version != OBJECT_VERSION {
        return Err(WalObjectError::BadVersion(version));
    }
    let trailer = object.len() - TRAILER_LEN;
    let footer_len = usize::try_from(u32::from_le_bytes(
        object[trailer..trailer + 4]
            .try_into()
            .map_err(|_| WalObjectError::BadManifest)?,
    ))
    .map_err(|_| WalObjectError::BadManifest)?;
    if footer_len % OBJECT_ENTRY_LEN != 0 {
        return Err(WalObjectError::BadManifest);
    }
    let footer = trailer
        .checked_sub(footer_len)
        .filter(|start| *start >= HEADER_LEN)
        .ok_or(WalObjectError::BadManifest)?;
    let mut entries = Vec::with_capacity(footer_len / OBJECT_ENTRY_LEN);
    for raw in object[footer..trailer].as_chunks::<OBJECT_ENTRY_LEN>().0 {
        let entry = WalObjectEntry {
            topic_id: Uuid::from_slice(&raw[..16]).map_err(|_| WalObjectError::BadManifest)?,
            partition: i32::from_le_bytes(
                raw[16..20]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
            first_offset: i64::from_le_bytes(
                raw[20..28]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
            last_offset: i64::from_le_bytes(
                raw[28..36]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
            byte_start: u64::from_le_bytes(
                raw[36..44]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
            byte_len: u32::from_le_bytes(
                raw[44..48]
                    .try_into()
                    .map_err(|_| WalObjectError::BadManifest)?,
            ),
        };
        let start = usize::try_from(entry.byte_start).map_err(|_| WalObjectError::BadManifest)?;
        let end = start
            .checked_add(usize::try_from(entry.byte_len).map_err(|_| WalObjectError::BadManifest)?)
            .filter(|end| *end <= footer)
            .ok_or(WalObjectError::BadManifest)?;
        let _ = end;
        if start < HEADER_LEN {
            return Err(WalObjectError::BadManifest);
        }
        entries.push(entry);
    }
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use assert2::check;

    use super::*;

    const TOPIC_ID: Uuid = Uuid::from_u128(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff);

    fn floor_record() -> WalDeleteFloorRecord {
        WalDeleteFloorRecord {
            topic_id: TOPIC_ID,
            partition: 3,
            floor: 42,
        }
    }

    fn flush_record() -> WalFlushRecord {
        WalFlushRecord {
            object_key: "wal/a".to_owned(),
            format_version: WalFlushRecord::FORMAT_VERSION,
            entries: vec![WalIndexEntry {
                topic_id: TOPIC_ID,
                partition: 3,
                first_offset: 10,
                last_offset: 12,
                byte_start: 6,
                byte_len: 64,
                max_timestamp_ms: 1_700_000_000_000,
            }],
        }
    }

    /// The exact value bytes of [`floor_record`]: the `i16` version 0, then
    /// the wincode body. A change here is a change to the 1.x on-disk
    /// contract.
    const GOLDEN_FLOOR_RECORD: &[u8] = include_bytes!("../tests/fixtures/golden_floor_record.bin");

    /// The exact value bytes of [`flush_record`]: the `i16` version 2, then
    /// the wincode body of `object_key` and `entries`. A change here is a
    /// change to the 1.x on-disk contract.
    const GOLDEN_FLUSH_RECORD: &[u8] = include_bytes!("../tests/fixtures/golden_flush_record.bin");

    /// The exact key bytes of the range key of [`flush_record`]: the `i16`
    /// key version 0, then the topic id, partition and first offset.
    const GOLDEN_RANGE_KEY: &[u8] = include_bytes!("../tests/fixtures/golden_range_key.bin");

    /// The exact key bytes of the delete-floor key of [`floor_record`]: the
    /// `i16` key version 1, then the topic id and partition.
    const GOLDEN_FLOOR_KEY: &[u8] = include_bytes!("../tests/fixtures/golden_floor_key.bin");

    #[test]
    fn index_values_encode_to_the_golden_bytes_and_decode_back() {
        check!(floor_record().to_bytes().unwrap().as_ref() == GOLDEN_FLOOR_RECORD);
        check!(WalDeleteFloorRecord::from_bytes(GOLDEN_FLOOR_RECORD) == Ok(floor_record()));
        check!(flush_record().to_bytes().unwrap().as_ref() == GOLDEN_FLUSH_RECORD);
        check!(WalFlushRecord::from_bytes(GOLDEN_FLUSH_RECORD) == Ok(flush_record()));
    }

    #[test]
    fn index_keys_encode_to_the_golden_bytes_and_decode_back() {
        let range = WalIndexKey::from(&flush_record().entries[0]);
        let floor = WalDeleteFloorKey {
            topic_id: TOPIC_ID,
            partition: 3,
        };
        check!(range.to_bytes().as_ref() == GOLDEN_RANGE_KEY);
        check!(WalIndexKey::from_bytes(GOLDEN_RANGE_KEY) == Some(range));
        check!(floor.to_bytes().as_ref() == GOLDEN_FLOOR_KEY);
        check!(WalDeleteFloorKey::from_bytes(GOLDEN_FLOOR_KEY) == Some(floor));
        // Neither key type decodes as the other, nor does the replay fence.
        check!(WalIndexKey::from_bytes(GOLDEN_FLOOR_KEY) == None);
        check!(WalDeleteFloorKey::from_bytes(GOLDEN_RANGE_KEY) == None);
        check!(WalIndexKey::from_bytes(REPLAY_FENCE_KEY) == None);
        check!(WalDeleteFloorKey::from_bytes(REPLAY_FENCE_KEY) == None);
    }

    /// `golden` with its leading two-byte version replaced by `version`.
    fn with_version(golden: &[u8], version: i16) -> Vec<u8> {
        let mut bytes = version.to_be_bytes().to_vec();
        bytes.extend_from_slice(&golden[2..]);
        bytes
    }

    #[test]
    fn index_values_of_another_version_are_refused() {
        let pre_1_0 = " A record written before krabka 1.0 has no version prefix and is not \
                       readable; reformat the cluster";
        for (name, decoded, expected) in [
            (
                "flush record of version 3",
                WalFlushRecord::from_bytes(&with_version(GOLDEN_FLUSH_RECORD, 3)).map(|_| ()),
                format!(
                    "unsupported diskless WAL index format version 3: this build reads \
                     version 2.{pre_1_0}"
                ),
            ),
            (
                "flush record in the pre-1.0 layout, version inside the body",
                WalFlushRecord::from_bytes(&GOLDEN_FLUSH_RECORD[2..]).map(|_| ()),
                format!(
                    "unsupported diskless WAL index format version 1280: this build reads \
                     version 2.{pre_1_0}"
                ),
            ),
            (
                "delete floor of version 1",
                WalDeleteFloorRecord::from_bytes(&with_version(GOLDEN_FLOOR_RECORD, 1)).map(|_| ()),
                format!(
                    "unsupported diskless WAL delete-floor record version 1: this build reads \
                     version 0.{pre_1_0}"
                ),
            ),
            (
                "delete floor in the pre-1.0 layout, no version",
                WalDeleteFloorRecord::from_bytes(&GOLDEN_FLOOR_RECORD[2..]).map(|_| ()),
                format!(
                    "unsupported diskless WAL delete-floor record version 4096: this build \
                     reads version 0.{pre_1_0}"
                ),
            ),
            (
                "empty value",
                WalDeleteFloorRecord::from_bytes(&[]).map(|_| ()),
                "truncated diskless WAL delete-floor record: 0 bytes hold no version".to_owned(),
            ),
        ] {
            check!(decoded == Err(expected), "case {name}");
        }
    }

    fn capture() -> DisklessWalCapture {
        DisklessWalCapture {
            format_version: DisklessWalCapture::FORMAT_VERSION,
            captured_at_ms: 1_700_000_000_000,
            source_cutoffs: vec![5],
            partitions: vec![DisklessPartitionCapture {
                topic: "orders".to_owned(),
                topic_id: TOPIC_ID,
                partition: 3,
                delete_floor: 10,
                recovery_cutoff: 13,
                ranges: vec![CapturedWalRange {
                    object_key: "wal/a".to_owned(),
                    entry: flush_record().entries[0].clone(),
                }],
            }],
            metadata_snapshot_sha256: None,
            rlmm_snapshot_sha256: None,
            group_offsets_sha256: None,
            authentication: None,
        }
    }

    /// The exact `diskless-wal-index.json` bytes of [`capture`]. A change here
    /// is a change to the 1.x capture format.
    const GOLDEN_CAPTURE: &str = concat!(
        r#"{"format_version":1,"captured_at_ms":1700000000000,"source_cutoffs":[5],"#,
        r#""partitions":[{"topic":"orders","topic_id":"00112233-4455-6677-8899-aabbccddeeff","#,
        r#""partition":3,"delete_floor":10,"recovery_cutoff":13,"ranges":[{"object_key":"wal/a","#,
        r#""entry":{"topic_id":"00112233-4455-6677-8899-aabbccddeeff","partition":3,"#,
        r#""first_offset":10,"last_offset":12,"byte_start":6,"byte_len":64,"#,
        r#""max_timestamp_ms":1700000000000}}]}]}"#,
    );

    #[test]
    fn capture_encodes_to_the_golden_bytes_and_decodes_back() {
        check!(serde_json::to_string(&capture()).unwrap() == GOLDEN_CAPTURE);
        check!(DisklessWalCapture::from_slice(GOLDEN_CAPTURE.as_bytes()) == Ok(capture()));
    }

    #[test]
    fn capture_of_a_missing_or_unknown_version_is_refused() {
        let golden: serde_json::Value = serde_json::from_str(GOLDEN_CAPTURE).unwrap();
        let mut future = golden.clone();
        future["format_version"] = serde_json::json!(2);
        let mut pre_1_0 = golden;
        pre_1_0.as_object_mut().unwrap().remove("format_version");
        for (name, json, expected) in [
            (
                "future version",
                future,
                "unsupported diskless WAL capture format version 2: this build reads version 1",
            ),
            (
                "pre-1.0 capture with no version",
                pre_1_0,
                "diskless WAL capture has no format_version: it predates krabka 1.0, which this \
                 build does not read",
            ),
        ] {
            check!(
                DisklessWalCapture::from_slice(&serde_json::to_vec(&json).unwrap())
                    == Err(expected.to_owned()),
                "case {name}"
            );
        }
    }

    #[test]
    fn index_keys_of_another_version_are_refused() {
        for (name, key, expected) in [
            (
                "unknown key version",
                with_version(GOLDEN_RANGE_KEY, 7),
                "unknown diskless WAL index key version 7",
            ),
            (
                "pre-1.0 range key, no version",
                GOLDEN_RANGE_KEY[2..].to_vec(),
                "unknown diskless WAL index key version 17",
            ),
            (
                "pre-1.0 delete-floor key, one-byte tag",
                [&[0xf0], &GOLDEN_FLOOR_KEY[2..]].concat(),
                "unknown diskless WAL index key version -4096",
            ),
            (
                "range key of the wrong length",
                GOLDEN_RANGE_KEY[..29].to_vec(),
                "invalid diskless WAL index key",
            ),
        ] {
            let mut projection = WalCaptureProjection::default();
            check!(
                projection.apply(Some(&key), Some(GOLDEN_FLUSH_RECORD)) == Err(expected.to_owned()),
                "case {name}"
            );
        }
    }
}
