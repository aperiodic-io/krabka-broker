//! Wire-byte codecs for the `__consumer_offsets` internal topic.
//!
//! The topic carries two kinds of records, discriminated by the first
//! `i16` of the key:
//!
//! - **`OffsetCommit`**, key version `0` or `1`: one record for each
//!   `(group_id, topic, partition)` committed offset.
//! - **`GroupMetadata`**, key version `2`: one record for each group state
//!   snapshot. The broker writes it at the end of every successful rebalance.
//!
//! Field layouts mirror Apache Kafka's schemas at tag `4.3.1`. The two classic
//! families here, `OffsetCommitValue` and `GroupMetadataValue`, declare
//! `"flexibleVersions": "4+"`. The broker writes value versions 1 and 4 of the
//! first, as Kafka does, through krabka-protocol's generated codec, and version
//! 3 of the second, which stays on the legacy non-flexible encoding that this
//! module's leaf helpers implement.
//!
//! The later families do not. Every `coordinator-value` of the KIP-848,
//! KIP-932 and KIP-1071 record types declares `"flexibleVersions": "0+"`, so
//! their values are compact-encoded and carry a tagged-field trailer. Their
//! leaf helpers live in the private `flex` submodule. Every `coordinator-key` in the group
//! coordinator, of every family, declares `"flexibleVersions": "none"`, so
//! keys use the helpers here.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use krabka_log::Offset;
use krabka_protocol::primitives::{fixed, string_bytes};

use crate::error::BrokerError;

pub(crate) mod flex;

/// Encode and append one nonempty actor delta before publishing its cache update.
/// Each protocol supplies its original borrowed or consuming encoder and cache operation,
/// and the reference it takes its state by: a `&mut` state lets the cache operation also
/// apply what the appended records mean to the live actor state.
macro_rules! flush_pending_records {
    ($state:ident: $state_type:ty, $pending:ident: $pending_type:ty;
        $log:ident, $coordinator:ident, $now:ident;
        group $group:expr; encode $encode:expr; cache $cache:expr;
    ) => {
        pub(super) async fn flush_pending(
            $state: $state_type,
            $pending: $pending_type,
            $log: &dyn $crate::coordinator::unified::offsets_log::OffsetsLog,
            $coordinator: &$crate::coordinator::unified::GroupCoordinator,
            $now: i64,
        ) -> Result<(), $crate::error::BrokerError> {
            if $pending.is_empty() {
                return Ok(());
            }
            let batch = $encode?;
            $log.append($group, batch).await?;
            $cache;
            Ok(())
        }
    };
}
pub(super) use flush_pending_records;

/// The shared durable offset fields, keeping the runtime entry and wire value
/// as distinct concrete types with their original documentation and derives.
macro_rules! committed_offset_type {
    ($(#[$meta:meta])* $visibility:vis struct $name:ident {
        $(#[$expiry:meta])* expire_timestamp_ms,
        $(#[$topic:meta])* topic_id,
    }) => {
        $(#[$meta])*
        $visibility struct $name {
            pub offset: ::krabka_log::Offset,
            pub leader_epoch: i32,
            pub metadata: String,
            pub commit_timestamp_ms: i64,
            $(#[$expiry])*
            pub expire_timestamp_ms: Option<i64>,
            $(#[$topic])*
            pub topic_id: Option<::uuid::Uuid>,
        }
    };
}
pub(super) use committed_offset_type;

/// The group record key families share a group id followed by at most one
/// member id or regular expression. Keep their public variants and version
/// tables together while using the same legacy string codec.
macro_rules! group_record_keys {
    ($visibility:vis enum $name:ident {
        $($(#[$variant_meta:meta])* $variant:ident $(($extra:ident))? => $version:ident,)*
    }
        $(#[$parse_docs:meta])* fn $parse:ident;
        $(#[$encode_docs:meta])* fn $encode:ident;
        invalid $invalid:literal;
    ) => {
        #[derive(Debug, Clone, PartialEq, Eq)]
        $visibility enum $name {
            $($(#[$variant_meta])* $variant { group_id: String, $($extra: String,)? },)*
        }

        impl $name {
            /// The group named by any record family of this protocol.
            #[must_use]
            pub fn group_id(&self) -> &str {
                match self {
                    $(Self::$variant { group_id, .. } => group_id,)*
                }
            }
        }

        $(#[$parse_docs])*
        $visibility fn $parse(version: i16, mut buf: &[u8]) -> Result<$name, $crate::error::BrokerError> {
            let key = match version {
                $($version => $name::$variant {
                    group_id: $crate::coordinator::unified::persistence::get_string(&mut buf)?,
                    $($extra: $crate::coordinator::unified::persistence::get_string(&mut buf)?,)?
                },)*
                _ => return Err($crate::error::BrokerError::Protocol(
                    ::krabka_protocol::ProtocolError::InvalidValue($invalid),
                )),
            };
            Ok(key)
        }

        $(#[$encode_docs])*
        $visibility fn $encode(key: &$name) -> Result<::bytes::Bytes, $crate::error::BrokerError> {
            match key {
                $($name::$variant { group_id, $($extra,)? } =>
                    $crate::coordinator::unified::persistence::encode_string_key(
                        $version, &[group_id, $($extra,)?],
                    ),)*
            }
        }
    };
}
pub(super) use group_record_keys;

/// Named leaf key encoders used by streams record writers.
macro_rules! string_key_encoders {
    ($($(#[$docs:meta])* $visibility:vis fn $name:ident($($field:ident),+) = $version:ident;)*) => {
        $($(#[$docs])*
        $visibility fn $name($($field: &str),+) -> Result<::bytes::Bytes, $crate::error::BrokerError> {
            $crate::coordinator::unified::persistence::encode_string_key($version, &[$($field),+])
        })*
    };
}
pub(super) use string_key_encoders;

/// Discriminator that [`parse_key`] returns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key {
    /// `(group_id, topic, partition)`. This key names the committed offset.
    OffsetCommit {
        group_id: String,
        topic: String,
        partition: i32,
    },
    /// Just `group_id`. The value carries the whole `GroupMetadataValue`.
    GroupMetadata { group_id: String },
    /// KIP-848 next-gen consumer group record types, versions 3–8 and 16.
    NextGen(crate::coordinator::unified::persistence_next_gen::NextGenKey),
    /// KIP-932 share-group record types, versions 10–15.
    Share(crate::coordinator::unified::share::persistence::ShareGroupKey),
    /// KIP-1071 streams-group record types, versions 17 and 19–23. Kafka 4.3.1
    /// defines no type 18.
    Streams(crate::coordinator::unified::streams::persistence::StreamsGroupKey),
}

/// A `__consumer_offsets` record key as the coordinator loader reads it.
///
/// Kafka's `GroupCoordinatorRecordSerde.apiMessageKeyFor` throws
/// `UnknownRecordTypeException` for a record type that its
/// `CoordinatorRecordType` does not list, and `CoordinatorLoaderImpl` skips
/// that record rather than fail the load. Such a key is
/// [`RecordKey::UnknownType`] here, so the loader can tell it apart from a key
/// of a known type that does not decode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordKey {
    /// A record type this broker reads, with its decoded key.
    Known(Key),
    /// A record type this broker does not read. Nothing past the leading
    /// `i16` was decoded.
    UnknownType(i16),
}

/// Decodes a `__consumer_offsets` record key and refuses an unknown record
/// type.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when the key is shorter than its `i16`
/// record type, names a record type that [`parse_record_key`] does not know,
/// or does not decode as that type's key.
pub fn parse_key(buf: &[u8]) -> Result<Key, BrokerError> {
    match parse_record_key(buf)? {
        RecordKey::Known(key) => Ok(key),
        RecordKey::UnknownType(_) => Err(BrokerError::Protocol(
            krabka_protocol::ProtocolError::InvalidValue("unknown __consumer_offsets key version"),
        )),
    }
}

/// Decodes a `__consumer_offsets` record key, reporting an unknown record type
/// as [`RecordKey::UnknownType`] instead of an error.
///
/// The record type is read first, as Kafka's `CoordinatorRecordSerde.deserialize`
/// does, so an unknown type is reported whatever bytes follow it.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when the key is shorter than its `i16`
/// record type, or when a key of a known record type does not decode.
pub fn parse_record_key(mut buf: &[u8]) -> Result<RecordKey, BrokerError> {
    if buf.remaining() < 2 {
        return Err(BrokerError::Protocol(
            krabka_protocol::ProtocolError::InvalidValue("offsets key too short"),
        ));
    }
    let version = buf.get_i16();
    let key = match version {
        0 | 1 => {
            let group_id = get_string(&mut buf)?;
            let topic = get_string(&mut buf)?;
            let partition = get_i32(&mut buf)?;
            Key::OffsetCommit {
                group_id,
                topic,
                partition,
            }
        }
        2 => {
            let group_id = get_string(&mut buf)?;
            Key::GroupMetadata { group_id }
        }
        3..=8 | 16 => Key::NextGen(
            crate::coordinator::unified::persistence_next_gen::parse_key(version, buf)?,
        ),
        10..=15 => Key::Share(
            crate::coordinator::unified::share::persistence::parse_share_key(version, buf)?,
        ),
        17 | 19..=23 => Key::Streams(
            crate::coordinator::unified::streams::persistence::parse_streams_key(version, buf)?,
        ),
        unknown => return Ok(RecordKey::UnknownType(unknown)),
    };
    Ok(RecordKey::Known(key))
}

/// Encodes a [`Key`] back to its `__consumer_offsets` wire bytes, symmetric to
/// [`parse_key`]. This function encodes the k0/k1 `OffsetCommit` and k2
/// `GroupMetadata` variants. The next-gen, share, and streams families delegate
/// to their own `encode_*` helpers.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when a string of the key is longer than
/// 32767 bytes, which no key can carry.
pub fn encode_key(key: &Key) -> Result<Bytes, BrokerError> {
    match key {
        Key::OffsetCommit {
            group_id,
            topic,
            partition,
        } => OffsetCommitValue::encode_key(group_id, topic, *partition),
        Key::GroupMetadata { group_id } => GroupMetadataValue::encode_key(group_id),
        Key::NextGen(k) => crate::coordinator::unified::persistence_next_gen::encode_key(k),
        Key::Share(k) => crate::coordinator::unified::share::persistence::encode_share_key(k),
        Key::Streams(k) => crate::coordinator::unified::streams::persistence::encode_streams_key(k),
    }
}

committed_offset_type! {
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct OffsetCommitValue {
        /// KIP-211: the per-commit expiry a v2-v4 `OffsetCommitRequest` asked for
        /// through `retention_time_ms`, as an absolute wall-clock millisecond.
        /// `None` means the commit takes the broker's `offsets.retention.minutes`.
        expire_timestamp_ms,
        /// The id of the committed topic, Kafka's `OffsetCommitValue.topicId`
        /// (version 4, tagged field 0). `None` is Kafka's zero id: the topic was
        /// unknown at commit time, or the record predates version 4.
        topic_id,
    }
}

impl OffsetCommitValue {
    /// Encodes an `OffsetCommit` key, version 1.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Protocol`] when the group id or the topic is
    /// longer than 32767 bytes.
    pub fn encode_key(group_id: &str, topic: &str, partition: i32) -> Result<Bytes, BrokerError> {
        let mut buf = BytesMut::new();
        buf.put_i16(1); // key version
        put_string(&mut buf, group_id)?;
        put_string(&mut buf, topic)?;
        buf.put_i32(partition);
        Ok(buf.freeze())
    }

    /// The value version Kafka's `GroupCoordinatorRecordHelpers.
    /// offsetCommitValueVersion` writes: version 1, the only schema with an
    /// `expireTimestamp`, for a commit that carries a per-commit expiry, and
    /// version 4 for every other commit.
    #[must_use]
    pub fn value_version(&self) -> i16 {
        if self.expire_timestamp_ms.is_some() {
            1
        } else {
            4
        }
    }

    /// Encodes an `OffsetCommit` value at [`Self::value_version`].
    ///
    /// Version 1 has no `leader_epoch` or `topic_id`, so a per-commit expiry
    /// drops both the same way Kafka's does; a reader sees `-1` and no id.
    #[must_use]
    pub fn encode_value(&self) -> Bytes {
        use krabka_protocol::Encode as _;
        let version = self.value_version();
        let wire = krabka_protocol::owned::offset_commit_value::OffsetCommitValue {
            offset: self.offset.0,
            leader_epoch: self.leader_epoch,
            metadata: self.metadata.clone(),
            commit_timestamp: self.commit_timestamp_ms,
            expire_timestamp: self.expire_timestamp_ms.unwrap_or(-1),
            topic_id: krabka_protocol::primitives::uuid::Uuid(
                self.topic_id.unwrap_or_default().into_bytes(),
            ),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        buf.put_i16(version);
        wire.encode(&mut buf, version)
            .expect("an OffsetCommitValue encodes at versions 1 and 4");
        buf.freeze()
    }

    /// Decodes an `OffsetCommit` value of any version Kafka defines, 0 to 4,
    /// as Kafka's `OffsetAndMetadata.fromRecord` reads it: a `-1` expiry is
    /// none, and the zero topic id is none.
    pub fn decode_value(mut buf: &[u8]) -> Result<Self, BrokerError> {
        use krabka_protocol::Decode as _;
        let version = get_i16(&mut buf)?;
        if !(krabka_protocol::owned::offset_commit_value::MIN_VERSION
            ..=krabka_protocol::owned::offset_commit_value::MAX_VERSION)
            .contains(&version)
        {
            return Err(BrokerError::Protocol(
                krabka_protocol::ProtocolError::InvalidValue("unknown OffsetCommitValue version"),
            ));
        }
        let wire = krabka_protocol::owned::offset_commit_value::OffsetCommitValue::decode(
            &mut buf, version,
        )?;
        let topic_id = uuid::Uuid::from_bytes(wire.topic_id.0);
        Ok(Self {
            offset: Offset(wire.offset),
            leader_epoch: wire.leader_epoch,
            metadata: wire.metadata,
            commit_timestamp_ms: wire.commit_timestamp,
            expire_timestamp_ms: (wire.expire_timestamp != -1).then_some(wire.expire_timestamp),
            topic_id: (!topic_id.is_nil()).then_some(topic_id),
        })
    }
}

impl From<OffsetCommitValue> for crate::coordinator::unified::classic_state::OffsetEntry {
    fn from(value: OffsetCommitValue) -> Self {
        Self {
            offset: value.offset,
            leader_epoch: value.leader_epoch,
            metadata: value.metadata,
            commit_timestamp_ms: value.commit_timestamp_ms,
            expire_timestamp_ms: value.expire_timestamp_ms,
            topic_id: value.topic_id,
        }
    }
}

impl From<&crate::coordinator::unified::classic_state::OffsetEntry> for OffsetCommitValue {
    fn from(entry: &crate::coordinator::unified::classic_state::OffsetEntry) -> Self {
        Self {
            offset: entry.offset,
            leader_epoch: entry.leader_epoch,
            metadata: entry.metadata.clone(),
            commit_timestamp_ms: entry.commit_timestamp_ms,
            expire_timestamp_ms: entry.expire_timestamp_ms,
            topic_id: entry.topic_id,
        }
    }
}

// Migration downgrades and normal classic group transitions write this
// wire-faithful k2 value. Bootstrap replay decodes the latest snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMetadataValue {
    pub protocol_type: String,
    pub generation: i32,
    pub protocol_name: Option<String>,
    pub leader: Option<String>,
    pub current_state_timestamp_ms: i64,
    pub members: Vec<MemberMetadata>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberMetadata {
    pub member_id: String,
    pub group_instance_id: Option<String>,
    pub client_id: String,
    pub client_host: String,
    pub rebalance_timeout_ms: i32,
    pub session_timeout_ms: i32,
    pub subscription: Bytes,
    pub assignment: Bytes,
}

impl GroupMetadataValue {
    /// Encodes a `GroupMetadata` key, version 2.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Protocol`] when the group id is longer than
    /// 32767 bytes.
    pub fn encode_key(group_id: &str) -> Result<Bytes, BrokerError> {
        encode_string_key(2, &[group_id])
    }

    /// Encodes a `GroupMetadata` value, version 3.
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::Protocol`] when the protocol type or name, the
    /// leader, or a member's id, instance id, client id or host is longer than
    /// 32767 bytes.
    pub fn encode_value(&self) -> Result<Bytes, BrokerError> {
        let mut buf = BytesMut::new();
        buf.put_i16(3); // value version
        put_string(&mut buf, &self.protocol_type)?;
        buf.put_i32(self.generation);
        put_nullable_string(&mut buf, self.protocol_name.as_deref())?;
        put_nullable_string(&mut buf, self.leader.as_deref())?;
        buf.put_i64(self.current_state_timestamp_ms);
        let n = i32::try_from(self.members.len()).expect("members fit in i32");
        buf.put_i32(n);
        for m in &self.members {
            put_string(&mut buf, &m.member_id)?;
            put_nullable_string(&mut buf, m.group_instance_id.as_deref())?;
            put_string(&mut buf, &m.client_id)?;
            put_string(&mut buf, &m.client_host)?;
            buf.put_i32(m.rebalance_timeout_ms);
            buf.put_i32(m.session_timeout_ms);
            put_bytes(&mut buf, &m.subscription);
            put_bytes(&mut buf, &m.assignment);
        }
        Ok(buf.freeze())
    }

    pub fn decode_value(mut buf: &[u8]) -> Result<Self, BrokerError> {
        let version = get_i16(&mut buf)?;
        if !(0..=3).contains(&version) {
            return Err(BrokerError::Protocol(
                krabka_protocol::ProtocolError::InvalidValue("unknown GroupMetadataValue version"),
            ));
        }
        let protocol_type = get_string(&mut buf)?;
        let generation = get_i32(&mut buf)?;
        let protocol_name = get_nullable_string(&mut buf)?;
        let leader = get_nullable_string(&mut buf)?;
        let current_state_timestamp_ms = if version >= 2 { get_i64(&mut buf)? } else { -1 };
        let n = get_i32(&mut buf)?;
        let cap = usize::try_from(n.max(0)).expect("non-negative i32 fits in usize");
        let mut members = Vec::with_capacity(cap);
        for _ in 0..n.max(0) {
            let member_id = get_string(&mut buf)?;
            let group_instance_id = if version >= 3 {
                get_nullable_string(&mut buf)?
            } else {
                None
            };
            let client_id = get_string(&mut buf)?;
            let client_host = get_string(&mut buf)?;
            // Version 0 has no rebalance timeout, and Kafka's schema default
            // is -1: replay then takes the session timeout instead.
            let rebalance_timeout_ms = if version >= 1 { get_i32(&mut buf)? } else { -1 };
            let session_timeout_ms = get_i32(&mut buf)?;
            let subscription = get_bytes(&mut buf)?;
            let assignment = get_bytes(&mut buf)?;
            members.push(MemberMetadata {
                member_id,
                group_instance_id,
                client_id,
                client_host,
                rebalance_timeout_ms,
                session_timeout_ms,
                subscription,
                assignment,
            });
        }
        Ok(Self {
            protocol_type,
            generation,
            protocol_name,
            leader,
            current_state_timestamp_ms,
            members,
        })
    }
}

// ── primitives (non-flexible Kafka encoding) ───────────────────────────────

pub(crate) fn get_i16(buf: &mut &[u8]) -> Result<i16, BrokerError> {
    Ok(fixed::get_i16(buf)?)
}

pub(crate) fn get_i32(buf: &mut &[u8]) -> Result<i32, BrokerError> {
    Ok(fixed::get_i32(buf)?)
}

pub(crate) fn get_i64(buf: &mut &[u8]) -> Result<i64, BrokerError> {
    Ok(fixed::get_i64(buf)?)
}

pub(crate) fn get_string(buf: &mut &[u8]) -> Result<String, BrokerError> {
    Ok(string_bytes::get_string_owned(buf)?)
}

pub(crate) fn get_nullable_string(buf: &mut &[u8]) -> Result<Option<String>, BrokerError> {
    Ok(string_bytes::get_nullable_string_owned(buf)?)
}

/// Reads a `BYTES` field, a null reading as empty.
pub(crate) fn get_bytes(buf: &mut &[u8]) -> Result<Bytes, BrokerError> {
    Ok(string_bytes::get_nullable_bytes_owned(buf)?.unwrap_or_default())
}

/// The longest string, in bytes, that Kafka reads or writes: `Short.MAX_VALUE`,
/// the largest `INT16` length. Its generated request readers refuse a longer
/// string field, and its generated writers refuse to serialize one.
pub(crate) const MAX_STRING_BYTES: usize = 0x7fff;

/// Writes `s` with an `INT16` length.
///
/// Kafka's generated writer throws `'<field>' field is too long to be
/// serialized` for a string longer than `0x7fff` bytes, and the coordinator
/// runtime then fails the write, so the request that asked for it gets an
/// error. This refuses the same string, and the record builders pass the
/// refusal up to the transition that asked for the write.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when `s` is longer than `i16::MAX` bytes.
pub(crate) fn put_string<B: BufMut>(buf: &mut B, s: &str) -> Result<(), BrokerError> {
    let n = i16::try_from(s.len()).map_err(|_| {
        BrokerError::Protocol(krabka_protocol::ProtocolError::InvalidValue(
            "STRING longer than 32767 bytes",
        ))
    })?;
    buf.put_i16(n);
    buf.put_slice(s.as_bytes());
    Ok(())
}

/// Encodes a coordinator record key: `version` as an `INT16`, then each of
/// `parts` as an `INT16`-length string.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when a part is longer than `i16::MAX`
/// bytes.
pub(crate) fn encode_string_key(version: i16, parts: &[&str]) -> Result<Bytes, BrokerError> {
    let mut buf = BytesMut::new();
    buf.put_i16(version);
    for part in parts {
        put_string(&mut buf, part)?;
    }
    Ok(buf.freeze())
}

/// Writes `s` as a nullable string with an `INT16` length.
///
/// # Errors
///
/// Returns [`BrokerError::Protocol`] when `s` is longer than `i16::MAX` bytes.
pub(crate) fn put_nullable_string<B: BufMut>(
    buf: &mut B,
    s: Option<&str>,
) -> Result<(), BrokerError> {
    match s {
        None => {
            buf.put_i16(-1);
            Ok(())
        }
        Some(s) => put_string(buf, s),
    }
}

pub(crate) fn put_bytes<B: BufMut>(buf: &mut B, b: &Bytes) {
    string_bytes::put_bytes(buf, b);
}

#[cfg(test)]
macro_rules! key_string_boundaries {
    ($pending:ty, $factory:expr) => {
        #[test]
        fn a_key_string_over_32767_bytes_does_not_encode() {
            type Field = (&'static str, fn(&mut $pending, String));
            let fields: [Field; 3] = [
                ("member id of a member record", |pending, value| {
                    pending.member_metadata[0].0 = value
                }),
                ("member id of a target assignment", |pending, value| {
                    pending.target_per_member[0].0 = value
                }),
                ("member id of a current assignment", |pending, value| {
                    pending.current_per_member[0].0 = value
                }),
            ];
            let limit = $crate::coordinator::unified::persistence::MAX_STRING_BYTES;
            for (name, set) in fields {
                for (length, encodes) in [(limit, true), (limit + 1, false)] {
                    let mut pending: $pending = ($factory)();
                    set(&mut pending, "a".repeat(length));
                    let outcome = pending.into_batch("g", 0);
                    assert2::assert!(outcome.is_ok() == encodes, "{name} of {length} bytes");
                    assert2::assert!(
                        encodes || matches!(outcome, Err($crate::error::BrokerError::Protocol(_))),
                        "{name} of {length} bytes is a protocol error"
                    );
                }
            }
            for (length, encodes) in [(limit, true), (limit + 1, false)] {
                let outcome = ($factory)().into_batch(&"g".repeat(length), 0);
                assert2::assert!(outcome.is_ok() == encodes, "group id of {length} bytes");
            }
        }
    };
}
#[cfg(test)]
pub(crate) use key_string_boundaries;

/// Validate the three per-member record families of an atomic migration.
macro_rules! assert_member_record_count {
    ($pending:expr, $count:expr) => {
        assert2::debug_assert!($pending.member_metadata.len() == $count);
        assert2::debug_assert!($pending.target_per_member.len() == $count);
        assert2::debug_assert!($pending.current_per_member.len() == $count);
    };
}
pub(crate) use assert_member_record_count;

/// The topics in id order and the partitions ascending, so that two equal
/// assignments give equal records.
pub(super) fn sorted_partitions(
    partitions: &std::collections::HashMap<krabka_protocol::primitives::uuid::Uuid, Vec<i32>>,
) -> Vec<(krabka_protocol::primitives::uuid::Uuid, Vec<i32>)> {
    let mut topics: Vec<(krabka_protocol::primitives::uuid::Uuid, Vec<i32>)> = partitions
        .iter()
        .filter(|(_, partitions)| !partitions.is_empty())
        .map(|(topic_id, partitions)| {
            let mut partitions = partitions.clone();
            partitions.sort_unstable();
            (*topic_id, partitions)
        })
        .collect();
    topics.sort_by_key(|(topic_id, _)| topic_id.0);
    topics
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::test_support::wire_bytes;

    const TOPIC_ID: uuid::Uuid = uuid::Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);

    fn value(expire_timestamp_ms: Option<i64>, topic_id: Option<uuid::Uuid>) -> OffsetCommitValue {
        OffsetCommitValue {
            offset: Offset(42),
            leader_epoch: 4,
            metadata: "meta".into(),
            commit_timestamp_ms: 1_000_000,
            expire_timestamp_ms,
            topic_id,
        }
    }

    /// `GroupCoordinatorRecordHelpers.newOffsetCommitRecord`: a commit with a
    /// per-commit expiry writes version 1, which has no leader epoch and no
    /// topic id, and every other commit writes version 4, flexible, with the
    /// topic id in tagged field 0 unless it is the zero id.
    #[test]
    fn offset_commit_value_writes_kafkas_version_and_bytes() {
        let v4_with_id = wire_bytes(&[
            "0004",                             // version
            "000000000000002a",                 // offset
            "00000004",                         // leader epoch
            "056d657461",                       // compact metadata
            "00000000000f4240",                 // commit timestamp
            "010010",                           // one tagged field: tag 0, 16 bytes
            "0102030405060708090a0b0c0d0e0f10", // topic id
        ]);
        let v4_without_id = wire_bytes(&[
            "0004",             // version
            "000000000000002a", // offset
            "00000004",         // leader epoch
            "056d657461",       // compact metadata
            "00000000000f4240", // commit timestamp
            "00",               // no tagged fields
        ]);
        let v1 = wire_bytes(&[
            "0001",             // version
            "000000000000002a", // offset
            "00046d657461",     // metadata
            "00000000000f4240", // commit timestamp
            "000000000098967f", // expire timestamp
        ]);
        let cases = [
            (
                value(None, Some(TOPIC_ID)),
                v4_with_id,
                value(None, Some(TOPIC_ID)),
            ),
            (value(None, None), v4_without_id, value(None, None)),
            (
                value(Some(9_999_999), Some(TOPIC_ID)),
                v1,
                OffsetCommitValue {
                    leader_epoch: -1,
                    ..value(Some(9_999_999), None)
                },
            ),
        ];
        for (written, bytes, read) in cases {
            let encoded = written.encode_value();
            assert!(encoded[..] == bytes[..], "{written:?}");
            assert!(OffsetCommitValue::decode_value(&encoded).unwrap() == read);
        }
    }

    /// Every version Kafka defines decodes, the way `OffsetAndMetadata.
    /// fromRecord` reads it: a leader epoch only from version 3, an expiry only
    /// at version 1 and only when it is not -1, a topic id only from version 4.
    #[test]
    fn offset_commit_value_reads_versions_0_to_4() {
        use krabka_protocol::Encode as _;
        let wire = krabka_protocol::owned::offset_commit_value::OffsetCommitValue {
            offset: 42,
            leader_epoch: 4,
            metadata: "meta".into(),
            commit_timestamp: 1_000_000,
            expire_timestamp: 9_999_999,
            topic_id: krabka_protocol::primitives::uuid::Uuid(TOPIC_ID.into_bytes()),
            ..Default::default()
        };
        let cases = [
            (
                0,
                OffsetCommitValue {
                    leader_epoch: -1,
                    ..value(None, None)
                },
            ),
            (
                1,
                OffsetCommitValue {
                    leader_epoch: -1,
                    ..value(Some(9_999_999), None)
                },
            ),
            (
                2,
                OffsetCommitValue {
                    leader_epoch: -1,
                    ..value(None, None)
                },
            ),
            (3, value(None, None)),
            (4, value(None, Some(TOPIC_ID))),
        ];
        for (version, want) in cases {
            let mut buf = BytesMut::new();
            buf.put_i16(version);
            wire.encode(&mut buf, version).unwrap();
            assert!(
                OffsetCommitValue::decode_value(&buf).unwrap() == want,
                "version {version}"
            );
        }
        let mut unexpiring_v1 = BytesMut::new();
        unexpiring_v1.put_i16(1);
        krabka_protocol::owned::offset_commit_value::OffsetCommitValue {
            expire_timestamp: -1,
            ..wire.clone()
        }
        .encode(&mut unexpiring_v1, 1)
        .unwrap();
        assert!(
            OffsetCommitValue::decode_value(&unexpiring_v1).unwrap()
                == OffsetCommitValue {
                    leader_epoch: -1,
                    ..value(None, None)
                }
        );
        let mut v5 = BytesMut::new();
        v5.put_i16(5);
        assert!(OffsetCommitValue::decode_value(&v5).is_err());
    }

    #[test]
    fn group_metadata_round_trip() {
        let v = GroupMetadataValue {
            protocol_type: "consumer".into(),
            generation: 5,
            protocol_name: Some("range".into()),
            leader: Some("m1".into()),
            current_state_timestamp_ms: 12_345,
            members: vec![MemberMetadata {
                member_id: "m1".into(),
                group_instance_id: None,
                client_id: "test-client".into(),
                client_host: "127.0.0.1".into(),
                rebalance_timeout_ms: 60_000,
                session_timeout_ms: 30_000,
                subscription: Bytes::from_static(b"sub"),
                assignment: Bytes::from_static(b"asgn"),
            }],
        };
        let encoded = v.encode_value().unwrap();
        let decoded = GroupMetadataValue::decode_value(&encoded).unwrap();
        assert!(decoded == v);
    }

    // Kafka's `GroupMetadataValue` schema adds `rebalanceTimeout` in version
    // 1, `currentStateTimestamp` in version 2 and `groupInstanceId` in version
    // 3, with the defaults -1, -1 and null. An older value decodes to them.
    #[test]
    fn group_metadata_value_decodes_older_versions_to_the_schema_defaults() {
        fn encode_at(version: i16) -> Bytes {
            let mut buf = BytesMut::new();
            buf.put_i16(version);
            put_string(&mut buf, "consumer").unwrap();
            buf.put_i32(5);
            put_nullable_string(&mut buf, Some("range")).unwrap();
            put_nullable_string(&mut buf, Some("m1")).unwrap();
            if version >= 2 {
                buf.put_i64(12_345);
            }
            buf.put_i32(1);
            put_string(&mut buf, "m1").unwrap();
            if version >= 3 {
                put_nullable_string(&mut buf, Some("inst")).unwrap();
            }
            put_string(&mut buf, "c").unwrap();
            put_string(&mut buf, "h").unwrap();
            if version >= 1 {
                buf.put_i32(60_000);
            }
            buf.put_i32(30_000);
            put_bytes(&mut buf, &Bytes::from_static(b"sub"));
            put_bytes(&mut buf, &Bytes::from_static(b"asn"));
            buf.freeze()
        }

        // (version, rebalance timeout, group instance id, state timestamp)
        let cases = [
            (0, -1, None, -1),
            (1, 60_000, None, -1),
            (2, 60_000, None, 12_345),
            (3, 60_000, Some("inst"), 12_345),
        ];
        let mut decoded_rows = Vec::new();
        let mut expected_rows = Vec::new();
        for (version, rebalance_timeout_ms, group_instance_id, current_state_timestamp_ms) in cases
        {
            let decoded = GroupMetadataValue::decode_value(&encode_at(version)).unwrap();
            decoded_rows.push((version, decoded));
            expected_rows.push((
                version,
                GroupMetadataValue {
                    protocol_type: "consumer".into(),
                    generation: 5,
                    protocol_name: Some("range".into()),
                    leader: Some("m1".into()),
                    current_state_timestamp_ms,
                    members: vec![MemberMetadata {
                        member_id: "m1".into(),
                        group_instance_id: group_instance_id.map(str::to_string),
                        client_id: "c".into(),
                        client_host: "h".into(),
                        rebalance_timeout_ms,
                        session_timeout_ms: 30_000,
                        subscription: Bytes::from_static(b"sub"),
                        assignment: Bytes::from_static(b"asn"),
                    }],
                },
            ));
        }
        assert!(decoded_rows == expected_rows);
        // The hand-built version 3 is what the broker itself writes.
        let current = GroupMetadataValue::decode_value(&encode_at(3)).unwrap();
        assert!(current.encode_value().unwrap() == encode_at(3));
    }

    /// A key writes each of its strings with an `INT16` length, in every
    /// family. A string of 32767 bytes encodes, and one of 32768 bytes is an
    /// error rather than a panic.
    #[test]
    fn a_key_string_over_32767_bytes_does_not_encode() {
        use crate::coordinator::unified::{
            persistence_next_gen::NextGenKey, share::persistence::ShareGroupKey,
            streams::persistence::StreamsGroupKey,
        };

        type Make = fn(String) -> Key;
        let keys: [(&str, Make); 11] = [
            ("offset group id", |s| Key::OffsetCommit {
                group_id: s,
                topic: "t".into(),
                partition: 0,
            }),
            ("offset topic", |s| Key::OffsetCommit {
                group_id: "g".into(),
                topic: s,
                partition: 0,
            }),
            ("classic group id", |s| Key::GroupMetadata { group_id: s }),
            ("next-gen group id", |s| {
                Key::NextGen(NextGenKey::GroupMetadata { group_id: s })
            }),
            ("next-gen member id", |s| {
                Key::NextGen(NextGenKey::MemberMetadata {
                    group_id: "g".into(),
                    member_id: s,
                })
            }),
            ("next-gen regular expression", |s| {
                Key::NextGen(NextGenKey::RegularExpression {
                    group_id: "g".into(),
                    regex: s,
                })
            }),
            ("share group id", |s| {
                Key::Share(ShareGroupKey::GroupMetadata { group_id: s })
            }),
            ("share member id", |s| {
                Key::Share(ShareGroupKey::MemberMetadata {
                    group_id: "g".into(),
                    member_id: s,
                })
            }),
            ("streams group id", |s| {
                Key::Streams(StreamsGroupKey::GroupMetadata { group_id: s })
            }),
            ("streams member id", |s| {
                Key::Streams(StreamsGroupKey::MemberMetadata {
                    group_id: "g".into(),
                    member_id: s,
                })
            }),
            ("streams topology group id", |s| {
                Key::Streams(StreamsGroupKey::Topology { group_id: s })
            }),
        ];
        for (name, make) in keys {
            for (length, encodes) in [(MAX_STRING_BYTES, true), (MAX_STRING_BYTES + 1, false)] {
                let key = make("a".repeat(length));

                let encoded = encode_key(&key);

                assert!(encoded.is_ok() == encodes, "{name} of {length} bytes");
                if let Ok(bytes) = encoded {
                    assert!(parse_key(&bytes).unwrap() == key, "{name}");
                } else {
                    assert!(
                        matches!(encoded, Err(BrokerError::Protocol(_))),
                        "{name} of {length} bytes is a protocol error"
                    );
                }
            }
        }
    }

    #[test]
    fn parse_key_offset_commit_v1() {
        let key = OffsetCommitValue::encode_key("grp", "topic", 7).unwrap();
        assert!(
            parse_key(&key).unwrap()
                == Key::OffsetCommit {
                    group_id: "grp".to_string(),
                    topic: "topic".to_string(),
                    partition: 7,
                }
        );
    }

    #[test]
    fn parse_key_group_metadata_v2() {
        let key = GroupMetadataValue::encode_key("grp").unwrap();
        match parse_key(&key).unwrap() {
            Key::GroupMetadata { group_id } => assert!(group_id == "grp"),
            k @ (Key::OffsetCommit { .. } | Key::NextGen(_) | Key::Share(_) | Key::Streams(_)) => {
                panic!("expected GroupMetadata, got {k:?}")
            }
        }
    }
}
