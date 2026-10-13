//! The deterministic byte encoding that the manifest hash chain covers.
//!
//! `canonical_manifest_bytes` is the single definition of that layout, and
//! the length-prefix helpers beside it are what make the encoding injective.

use super::ManifestBody;

/// Domain separation for the chain preimage, so a manifest body can never
/// collide with any other chained value.
pub const MANIFEST_BODY_DOMAIN: &[u8] = b"krabka-worm-manifest-body-v1\0";

/// Appends a big-endian `u64` length prefix.
///
/// `u64` rather than `u32` so the conversion from `usize` is lossless on every
/// target Krabka builds for. A saturating `u32` prefix would make the encoding
/// non-injective in principle — two different bodies could share a preimage —
/// and "a field that large cannot occur" is a claim about callers, not a
/// property of the encoding. A canonical encoding that a chain head depends on
/// should not rest on one, and four bytes per field is not worth the argument.
fn push_len(out: &mut Vec<u8>, len: usize) { out.extend_from_slice(&(len as u64).to_be_bytes()); }

/// Appends a length-prefixed byte string.
pub(super) fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_len(out, bytes.len());
    out.extend_from_slice(bytes);
}

/// Appends an optional string as a presence byte and, when present, a
/// length-prefixed body.
fn push_optional(out: &mut Vec<u8>, value: Option<&str>) {
    match value {
        None => out.push(0),
        Some(text) => {
            out.push(1);
            push_bytes(out, text.as_bytes());
        }
    }
}

/// Deterministic byte encoding of a manifest body.
///
/// This is the preimage the hash chain covers. The writer and the verifier
/// both call this function, and a disagreement between them makes every
/// archive written under the older encoding fail verification with no way to
/// tell tampering from a format change.
///
/// Every integer is big-endian, every length prefix is a `u64`, and every
/// string is `UTF-8`. Every variable-length field carries a length, so no two
/// distinct bodies encode to the same bytes.
///
/// ```text
/// MANIFEST_BODY_DOMAIN
/// format_version:u32
/// len+topic  topic_id:16  partition:i32  segment_id:16
/// start_offset:i64  end_offset:i64  max_timestamp_ms:i64
/// broker_id:i32  event_timestamp_ms:i64  segment_size_bytes:i64
/// leader_epochs.len():u64, then per entry in BTreeMap order: epoch:i32 offset:i64
/// txn_index_empty:u8
/// objects.len():u64, then per entry in vec order:
///     len+suffix  len+key  size_bytes:u64  sha256:32
///     e_tag:      0u8 | (1u8 len+value)
///     version_id: 0u8 | (1u8 len+value)
///     create_precondition:u8 (format version >= 2 only)
/// epoch_id:16  seq:u64  prev_head:32
/// ```
#[must_use]
pub fn canonical_manifest_bytes(body: &ManifestBody) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(MANIFEST_BODY_DOMAIN);
    out.extend_from_slice(&body.format_version.to_be_bytes());

    let segment = &body.segment;
    push_bytes(&mut out, segment.topic.as_bytes());
    out.extend_from_slice(segment.topic_id.as_bytes());
    out.extend_from_slice(&segment.partition.to_be_bytes());
    out.extend_from_slice(segment.segment_id.as_bytes());
    out.extend_from_slice(&segment.start_offset.to_be_bytes());
    out.extend_from_slice(&segment.end_offset.to_be_bytes());
    out.extend_from_slice(&segment.max_timestamp_ms.to_be_bytes());
    out.extend_from_slice(&segment.broker_id.to_be_bytes());
    out.extend_from_slice(&segment.event_timestamp_ms.to_be_bytes());
    out.extend_from_slice(&segment.segment_size_bytes.to_be_bytes());
    push_len(&mut out, segment.leader_epochs.len());
    for (epoch, offset) in &segment.leader_epochs {
        out.extend_from_slice(&epoch.to_be_bytes());
        out.extend_from_slice(&offset.to_be_bytes());
    }
    out.push(u8::from(segment.txn_index_empty));

    push_len(&mut out, body.objects.len());
    for object in &body.objects {
        push_bytes(&mut out, object.suffix.as_bytes());
        push_bytes(&mut out, object.key.as_bytes());
        out.extend_from_slice(&object.size_bytes.to_be_bytes());
        out.extend_from_slice(&object.sha256.0);
        push_optional(&mut out, object.e_tag.as_deref());
        push_optional(&mut out, object.version_id.as_deref());
        if body.format_version >= 2 {
            out.push(u8::from(object.create_precondition));
        }
    }

    out.extend_from_slice(body.chain.epoch_id.0.as_bytes());
    out.extend_from_slice(&body.chain.seq.0.to_be_bytes());
    out.extend_from_slice(&body.chain.prev_head.0);
    out
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::worm::manifest::{
            ChainHead, EpochId, ManifestSeq, ObjectEntry, Sha256Digest, manifest_head,
            test_support::{bare_object, sample_body},
        },
        assert2::check,
        uuid::Uuid,
    };

    /// One labelled edit to a manifest body, for the preimage-coverage table.
    struct Mutation {
        name: &'static str,
        mutate: Box<dyn Fn(&mut ManifestBody)>,
    }

    fn mutation(name: &'static str, mutate: impl Fn(&mut ManifestBody) + 'static) -> Mutation {
        Mutation {
            name,
            mutate: Box::new(mutate),
        }
    }

    #[test]
    fn canonical_bytes_survive_json_reserialization() {
        let body = sample_body();
        let expected = canonical_manifest_bytes(&body);

        let compact = serde_json::to_string(&body).unwrap();
        let from_compact: ManifestBody = serde_json::from_str(&compact).unwrap();
        check!(
            canonical_manifest_bytes(&from_compact) == expected,
            "compact"
        );

        let pretty = serde_json::to_string_pretty(&from_compact).unwrap();
        let from_pretty: ManifestBody = serde_json::from_str(&pretty).unwrap();
        check!(canonical_manifest_bytes(&from_pretty) == expected, "pretty");

        // A second full cycle, so an encoding that is stable only on the first
        // pass does not slip through.
        let again: ManifestBody =
            serde_json::from_str(&serde_json::to_string(&from_pretty).unwrap()).unwrap();
        check!(canonical_manifest_bytes(&again) == expected, "second cycle");
        check!(again == body, "structural equality");
        check!(manifest_head(&again) == manifest_head(&body), "head");
    }

    #[test]
    fn canonical_bytes_change_with_every_field() {
        let base = sample_body();
        let base_bytes = canonical_manifest_bytes(&base);

        let mutations: Vec<Mutation> = vec![
            mutation("format_version", |b| b.format_version += 1),
            mutation("segment.topic", |b| {
                b.segment.topic = "payments".to_string();
            }),
            mutation("segment.topic_id", |b| {
                b.segment.topic_id = Uuid::from_u128(0x12);
            }),
            mutation("segment.partition", |b| b.segment.partition = 4),
            mutation("segment.segment_id", |b| {
                b.segment.segment_id = Uuid::from_u128(0x23);
            }),
            mutation("segment.start_offset", |b| b.segment.start_offset = 101),
            mutation("segment.end_offset", |b| b.segment.end_offset = 200),
            mutation("segment.max_timestamp_ms", |b| {
                b.segment.max_timestamp_ms += 1;
            }),
            mutation("segment.broker_id", |b| b.segment.broker_id = 8),
            mutation("segment.event_timestamp_ms", |b| {
                b.segment.event_timestamp_ms += 1;
            }),
            mutation("segment.segment_size_bytes", |b| {
                b.segment.segment_size_bytes += 1;
            }),
            mutation("segment.leader_epochs value", |b| {
                b.segment.leader_epochs.insert(1, 151);
            }),
            mutation("segment.leader_epochs extra entry", |b| {
                b.segment.leader_epochs.insert(2, 180);
            }),
            mutation("segment.leader_epochs removed entry", |b| {
                b.segment.leader_epochs.remove(&1);
            }),
            mutation("segment.txn_index_empty", |b| {
                b.segment.txn_index_empty = true;
            }),
            mutation("objects[0].suffix", |b| {
                b.objects[0].suffix = ".timeindex".to_string();
            }),
            mutation("objects[0].key", |b| {
                b.objects[0].key = "archive/elsewhere.log".to_string();
            }),
            mutation("objects[0].size_bytes", |b| b.objects[0].size_bytes += 1),
            mutation("objects[0].sha256", |b| {
                b.objects[0].sha256 = Sha256Digest::of(b"other body");
            }),
            mutation("objects[0].e_tag changed", |b| {
                b.objects[0].e_tag = Some("\"other\"".to_string());
            }),
            mutation("objects[0].e_tag cleared", |b| b.objects[0].e_tag = None),
            mutation("objects[1].e_tag set", |b| {
                b.objects[1].e_tag = Some("\"new\"".to_string());
            }),
            mutation("objects[0].version_id changed", |b| {
                b.objects[0].version_id = Some("other".to_string());
            }),
            mutation("objects[0].version_id cleared", |b| {
                b.objects[0].version_id = None;
            }),
            mutation("objects[1].version_id set", |b| {
                b.objects[1].version_id = Some("new".to_string());
            }),
            mutation("objects[0].create_precondition", |b| {
                b.objects[0].create_precondition = true;
            }),
            mutation("objects order", |b| b.objects.swap(0, 1)),
            mutation("objects count grows", |b| b.objects.push(bare_object())),
            mutation("objects count shrinks", |b| {
                b.objects.pop();
            }),
            mutation("chain.epoch_id", |b| {
                b.chain.epoch_id = EpochId(Uuid::from_u128(0x98));
            }),
            mutation("chain.seq", |b| b.chain.seq = ManifestSeq(5)),
            mutation("chain.prev_head", |b| {
                b.chain.prev_head = ChainHead([8u8; 32]);
            }),
        ];

        for Mutation { name, mutate } in mutations {
            let mut mutated = base.clone();
            mutate(&mut mutated);
            check!(mutated != base, "case {name} did not change the body");
            check!(
                canonical_manifest_bytes(&mutated) != base_bytes,
                "case {name} is missing from the preimage"
            );
            check!(
                manifest_head(&mutated) != manifest_head(&base),
                "case {name} does not move the chain head"
            );
        }
    }

    #[test]
    fn length_prefixes_prevent_field_boundary_ambiguity() {
        let mut left = sample_body();
        left.segment.topic = "ab".to_string();
        left.objects = vec![ObjectEntry {
            key: "c".to_string(),
            ..bare_object()
        }];

        let mut right = left.clone();
        right.segment.topic = "a".to_string();
        right.objects[0].key = "bc".to_string();

        check!(canonical_manifest_bytes(&left) != canonical_manifest_bytes(&right));
    }
}
