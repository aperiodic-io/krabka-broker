//! `DescribeTopicFreezes`, api key 1016.
//!
//! The request reads the write-freeze registry. `scope_filter` matches one
//! entry's `scope` exactly and is not itself a prefix match, and
//! `pattern_type_filter` narrows to one pattern type. A request with neither
//! filter reads every live entry.
//!
//! Authorization: `Describe` on `Cluster("kafka-cluster")`. On a deny the
//! response carries `CLUSTER_AUTHORIZATION_FAILED` (31) and no entries.
//!
//! # Why the response gives the signature back
//!
//! Each entry carries the `key_id` and the detached signature that
//! [`set_freeze`][super::set_freeze] put in the metadata log. That is
//! deliberate. An operator's own command re-verifies each entry against the
//! operator public keys on the operator's machine, so the broker's word is not
//! the only evidence of who set a freeze. An entry with an empty signature is
//! an attestation and not a proof, and this response is what tells the two
//! apart.

use {
    crate::{
        broker::Broker,
        codes,
        freeze::handlers::{FreezeFilter, cluster_describe_denied, pattern_type_filter},
        handlers::{
            ApiVersion, DispatchResult, RequestContext, acl_wire::pattern_type_to_wire,
            encode_response,
        },
    },
    krabka_metadata::{MetadataImage, TopicFreezeRecord},
    krabka_protocol::{
        krabka::freeze::{
            DescribeTopicFreezesRequest, DescribeTopicFreezesResponse, DescribedTopicFreeze,
        },
        primitives::uuid::Uuid as ProtocolUuid,
    },
};

#[krabka_macros::extension_wire(
    name = "handle_describe_topic_freezes",
    api = "DescribeTopicFreezes",
    level = "info"
)]
pub(crate) async fn handle(
    broker: &Broker,
    req: DescribeTopicFreezesRequest,
    version: ApiVersion,
    ctx: &RequestContext<'_>,
) -> DispatchResult {
    let image = broker.controller.current_image();
    if cluster_describe_denied(broker.config.authorizer.as_ref(), &image, ctx) {
        return encode_response(
            &DescribeTopicFreezesResponse {
                error_code: codes::CLUSTER_AUTHORIZATION_FAILED,
                error_message: Some("describe-topic-freezes denied".to_owned()),
                ..DescribeTopicFreezesResponse::default()
            },
            version,
        );
    }

    encode_response(
        &DescribeTopicFreezesResponse {
            freezes: matching_freezes(&image, req.scope_filter.as_deref(), req.pattern_type_filter),
            ..DescribeTopicFreezesResponse::default()
        },
        version,
    )
}

/// The live registry entries that the two filters select, in a stable order.
///
/// The image holds one hash map and one sorted map, so its own iteration order
/// is unspecified. The rows are sorted by scope and then by pattern type, which
/// gives an operator the same output for the same registry.
fn matching_freezes(
    image: &MetadataImage,
    scope_filter: Option<&str>,
    pattern_filter: i8,
) -> Vec<DescribedTopicFreeze> {
    let filter = pattern_type_filter(pattern_filter);
    if filter == FreezeFilter::Nothing {
        return Vec::new();
    }
    let mut rows: Vec<DescribedTopicFreeze> = image
        .topic_freezes()
        .filter(|entry| filter.accepts(entry.pattern_type))
        .filter(|entry| scope_filter.is_none_or(|scope| scope == entry.scope))
        .map(described)
        .collect();
    rows.sort_by(|left, right| {
        left.scope
            .cmp(&right.scope)
            .then(left.pattern_type.cmp(&right.pattern_type))
    });
    rows
}

/// One registry entry as the response carries it.
fn described(entry: &TopicFreezeRecord) -> DescribedTopicFreeze {
    DescribedTopicFreeze {
        scope: entry.scope.clone(),
        pattern_type: pattern_type_to_wire(entry.pattern_type),
        reason: entry.reason.clone(),
        set_by: entry.set_by.clone(),
        set_at_ms: entry.set_at_ms,
        proposal_id: ProtocolUuid(entry.proposal_id.into_bytes()),
        key_id: entry.key_id.clone(),
        signature: entry.signature.clone(),
        ..DescribedTopicFreeze::default()
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        assert2::check,
        krabka_metadata::{MetadataRecord, PatternType},
        krabka_protocol::krabka::freeze::{
            PATTERN_TYPE_ANY, PATTERN_TYPE_LITERAL, PATTERN_TYPE_PREFIXED,
        },
        uuid::Uuid,
    };

    const PROPOSAL: Uuid = Uuid::from_u128(0x0102_0304_0506_0708_090a_0b0c_0d0e_0f10);

    fn entry(scope: &str, pattern_type: PatternType) -> TopicFreezeRecord {
        TopicFreezeRecord {
            scope: scope.to_owned(),
            pattern_type,
            frozen: true,
            reason: "DR cutover".to_owned(),
            set_by: "User:alice".to_owned(),
            set_at_ms: 1_770_000_000_000,
            proposal_id: PROPOSAL,
            key_id: "alice-yubi".to_owned(),
            signature: vec![0xAB; 64],
        }
    }

    /// A freeze scope and its pattern type.
    struct FrozenScope<'a> {
        scope: &'a str,
        pattern_type: PatternType,
    }

    fn registry(entries: &[FrozenScope<'_>]) -> MetadataImage {
        let mut image = MetadataImage::new(Uuid::from_u128(0x5150));
        for FrozenScope {
            scope,
            pattern_type,
        } in entries
        {
            image.apply(&MetadataRecord::V1TopicFreeze(entry(scope, *pattern_type)));
        }
        image
    }

    /// A described freeze's scope and its pattern type byte.
    #[derive(Debug, PartialEq, Eq)]
    struct ScopeRow {
        scope: String,
        pattern_type: i8,
    }

    fn scopes(rows: &[DescribedTopicFreeze]) -> Vec<ScopeRow> {
        rows.iter()
            .map(|row| ScopeRow {
                scope: row.scope.clone(),
                pattern_type: row.pattern_type,
            })
            .collect()
    }

    #[yare::parameterized(
        signed = { true },
        unsigned = { false },
    )]
    fn an_entry_preserves_its_signature_fields(signed: bool) {
        let mut record = entry("orders", PatternType::Literal);
        if !signed {
            record.proposal_id = Uuid::nil();
            record.key_id.clear();
            record.signature.clear();
        }
        let mut image = MetadataImage::new(Uuid::from_u128(0x5150));
        image.apply(&MetadataRecord::V1TopicFreeze(record));
        let expected = DescribedTopicFreeze {
            scope: "orders".to_owned(),
            pattern_type: PATTERN_TYPE_LITERAL,
            reason: "DR cutover".to_owned(),
            set_by: "User:alice".to_owned(),
            set_at_ms: 1_770_000_000_000,
            proposal_id: if signed {
                ProtocolUuid(PROPOSAL.into_bytes())
            } else {
                ProtocolUuid::ZERO
            },
            key_id: if signed {
                "alice-yubi".to_owned()
            } else {
                String::new()
            },
            signature: if signed { vec![0xAB; 64] } else { Vec::new() },
            ..DescribedTopicFreeze::default()
        };
        check!(matching_freezes(&image, None, PATTERN_TYPE_ANY) == vec![expected]);
    }

    #[test]
    fn an_empty_registry_answers_with_no_entries() {
        let image = MetadataImage::new(Uuid::from_u128(0x5150));

        check!(matching_freezes(&image, None, PATTERN_TYPE_ANY).is_empty());
        check!(matching_freezes(&image, Some("orders"), PATTERN_TYPE_LITERAL).is_empty());
    }

    #[test]
    fn the_two_filters_select_the_entries_they_name() {
        let image = registry(&[
            FrozenScope {
                scope: "orders",
                pattern_type: PatternType::Literal,
            },
            FrozenScope {
                scope: "orders",
                pattern_type: PatternType::Prefixed,
            },
            FrozenScope {
                scope: "payments",
                pattern_type: PatternType::Literal,
            },
            FrozenScope {
                scope: "tenant-a.",
                pattern_type: PatternType::Prefixed,
            },
        ]);

        for (label, scope_filter, pattern_filter, expected) in [
            (
                "no filter reads the whole registry",
                None,
                PATTERN_TYPE_ANY,
                vec![
                    ("orders", PATTERN_TYPE_LITERAL),
                    ("orders", PATTERN_TYPE_PREFIXED),
                    ("payments", PATTERN_TYPE_LITERAL),
                    ("tenant-a.", PATTERN_TYPE_PREFIXED),
                ],
            ),
            (
                "the unknown byte also reads the whole registry",
                None,
                0,
                vec![
                    ("orders", PATTERN_TYPE_LITERAL),
                    ("orders", PATTERN_TYPE_PREFIXED),
                    ("payments", PATTERN_TYPE_LITERAL),
                    ("tenant-a.", PATTERN_TYPE_PREFIXED),
                ],
            ),
            ("a pattern type alone", None, PATTERN_TYPE_PREFIXED, vec![
                ("orders", PATTERN_TYPE_PREFIXED),
                ("tenant-a.", PATTERN_TYPE_PREFIXED),
            ]),
            (
                "a scope alone reads both pattern types of that scope",
                Some("orders"),
                PATTERN_TYPE_ANY,
                vec![
                    ("orders", PATTERN_TYPE_LITERAL),
                    ("orders", PATTERN_TYPE_PREFIXED),
                ],
            ),
            (
                "both filters together",
                Some("orders"),
                PATTERN_TYPE_LITERAL,
                vec![("orders", PATTERN_TYPE_LITERAL)],
            ),
            (
                "a scope filter is an exact match and never a prefix match",
                Some("tenant-a"),
                PATTERN_TYPE_ANY,
                vec![],
            ),
            (
                "a scope no entry carries",
                Some("absent"),
                PATTERN_TYPE_ANY,
                vec![],
            ),
            (
                "a pattern type byte no build knows reads nothing",
                None,
                9,
                vec![],
            ),
        ] {
            let rows = matching_freezes(&image, scope_filter, pattern_filter);
            let expected: Vec<ScopeRow> = expected
                .into_iter()
                .map(|(scope, pattern_type)| ScopeRow {
                    scope: scope.to_owned(),
                    pattern_type,
                })
                .collect();
            check!(scopes(&rows) == expected, "{label}");
        }
    }

    #[test]
    fn a_thawed_scope_is_gone_from_the_registry() {
        let mut image = registry(&[FrozenScope {
            scope: "orders",
            pattern_type: PatternType::Literal,
        }]);
        image.apply(&MetadataRecord::V1TopicFreeze(TopicFreezeRecord {
            frozen: false,
            ..entry("orders", PatternType::Literal)
        }));

        check!(matching_freezes(&image, None, PATTERN_TYPE_ANY).is_empty());
    }
}
