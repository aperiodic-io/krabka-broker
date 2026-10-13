//! The canonical byte string that an operator signs over a freeze record.
//!
//! The domain separator, the field order and the `u32` big-endian length
//! prefixes are a contract that `krabka-guard` and an offline auditor rebuild
//! on their own, so this is the one place that writes the payload and no byte
//! of it moves. The parent module documents the layout the writer follows.

use {
    crate::signing_domains::{FREEZE_DOMAIN, put_len_prefixed},
    krabka_metadata::{PatternType, TopicFreezeRecord},
    krabka_protocol::krabka::freeze::{PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED},
};

/// The canonical bytes that an operator signs to author `record` in the
/// cluster named by `cluster_id`.
///
/// `cluster_id` is the string form that `Metadata` and `DescribeCluster`
/// already give a client, so the operator's command signs the identifier it
/// read from the cluster it means to act on.
pub(crate) fn freeze_signing_bytes(cluster_id: &str, record: &TopicFreezeRecord) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(signing_bytes_capacity(cluster_id, record));
    bytes.extend_from_slice(FREEZE_DOMAIN);
    put_len_prefixed(&mut bytes, cluster_id.as_bytes());
    bytes.push(pattern_type_byte(record.pattern_type));
    put_len_prefixed(&mut bytes, record.scope.as_bytes());
    bytes.push(u8::from(record.frozen));
    put_len_prefixed(&mut bytes, record.reason.as_bytes());
    put_len_prefixed(&mut bytes, record.set_by.as_bytes());
    bytes.extend_from_slice(&record.set_at_ms.to_be_bytes());
    bytes.extend_from_slice(record.proposal_id.as_bytes());
    bytes
}

/// The pattern type's byte in the canonical layout.
///
/// It is Kafka's ACL discriminant, the same byte the wire request carries, so
/// the bytes the operator signs and the bytes they send name one value.
fn pattern_type_byte(pattern_type: PatternType) -> u8 {
    let wire = match pattern_type {
        PatternType::Literal => PATTERN_TYPE_LITERAL,
        PatternType::Prefixed => PATTERN_TYPE_PREFIXED,
    };
    wire.to_be_bytes()[0]
}

/// How many bytes [`freeze_signing_bytes`] writes, for the one allocation.
fn signing_bytes_capacity(cluster_id: &str, record: &TopicFreezeRecord) -> usize {
    const LEN_PREFIX: usize = size_of::<u32>();
    const PATTERN_TYPE_AND_FROZEN: usize = 2;
    const SET_AT_MS: usize = size_of::<i64>();
    const PROPOSAL_ID: usize = 16;

    FREEZE_DOMAIN.len()
        + 4 * LEN_PREFIX
        + cluster_id.len()
        + record.scope.len()
        + record.reason.len()
        + record.set_by.len()
        + PATTERN_TYPE_AND_FROZEN
        + SET_AT_MS
        + PROPOSAL_ID
}
