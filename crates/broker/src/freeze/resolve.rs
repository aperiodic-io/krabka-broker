//! The produce path's question: is this topic frozen (KFC-9)?
//!
//! [`resolve_topic_freeze`] runs once per topic per produce request, beside
//! the schema-validation resolve and for the same reason. A freeze is a
//! property of the topic and not of a partition or of a batch, so every
//! partition of the topic then pays one test of an [`Option`] and nothing
//! else.
//!
//! The image supplies the live entries. The verified precedence kernel makes a
//! literal entry beat every prefix and makes the longest matching prefix win.
//! A name that starts with `__` never matches. Every enforcement path calls
//! this resolver, and its tests compare the result with the image's indexed
//! lookup. [`FreezeVerdict`] turns the selected borrow into the `error_message`
//! the producer reads.

use {
    crate::freeze::pattern_type_name,
    krabka_metadata::{MetadataImage, PatternType, TopicFreezeRecord},
    krabka_verified::{
        FreezeMutationDecision, FreezeMutationKind, FreezeScopeDecision, FreezeScopeRank,
        freeze_mutation_decision, freeze_scope_decision,
    },
};

/// The write-freeze entry that covers `topic`, or `None` when the topic
/// accepts writes.
///
/// This is the whole of the produce-path gate's read. The registry is bounded
/// by configuration, and the fold allocates nothing.
pub(crate) fn resolve_topic_freeze<'a>(
    image: &'a MetadataImage,
    topic: &str,
) -> Option<&'a TopicFreezeRecord> {
    let mut best = None;
    for candidate in image.topic_freezes() {
        let current_rank = best.map_or(FreezeScopeRank::NoMatch, |current| {
            scope_rank(current, topic)
        });
        let candidate_rank = scope_rank(candidate, topic);
        if freeze_scope_decision(current_rank, candidate_rank) == FreezeScopeDecision::Replace {
            best = Some(candidate);
        }
    }
    best
}

/// The authorization-and-freeze result shared by mutation adapters.
///
/// The frozen variant is the only one that carries registry detail. This
/// keeps an unauthorized caller from observing a matching scope or reason.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum FreezeMutationResolution<'a> {
    AuthorizationDenied,
    Frozen(&'a TopicFreezeRecord),
    Admit,
}

/// Resolve one mutation through the verified authorization-first decision.
///
/// An unauthorized call does not inspect the freeze registry. Authorized
/// calls use the same scope resolver as every other freeze-aware path.
pub(crate) fn resolve_freeze_mutation<'a>(
    image: &'a MetadataImage,
    topic: &str,
    authorized: bool,
    kind: FreezeMutationKind,
) -> FreezeMutationResolution<'a> {
    if !authorized {
        return match freeze_mutation_decision(false, false, kind) {
            FreezeMutationDecision::AuthorizationDenied => {
                FreezeMutationResolution::AuthorizationDenied
            }
            FreezeMutationDecision::Frozen | FreezeMutationDecision::Admit => {
                unreachable!("an unauthorized freeze decision must deny authorization")
            }
        };
    }

    let freeze = resolve_topic_freeze(image, topic);
    match freeze_mutation_decision(true, freeze.is_some(), kind) {
        FreezeMutationDecision::AuthorizationDenied => {
            unreachable!("an authorized freeze decision cannot deny authorization")
        }
        FreezeMutationDecision::Frozen => match freeze {
            Some(record) => FreezeMutationResolution::Frozen(record),
            None => unreachable!("a frozen decision must have a matching freeze record"),
        },
        FreezeMutationDecision::Admit => FreezeMutationResolution::Admit,
    }
}

fn scope_rank(record: &TopicFreezeRecord, topic: &str) -> FreezeScopeRank {
    if topic.starts_with("__") {
        return FreezeScopeRank::NoMatch;
    }
    match record.pattern_type {
        PatternType::Literal if record.scope == topic => FreezeScopeRank::Literal,
        PatternType::Prefixed if topic.starts_with(&record.scope) => FreezeScopeRank::Prefix {
            length: u64::try_from(record.scope.len()).unwrap_or(u64::MAX),
        },
        _ => FreezeScopeRank::NoMatch,
    }
}

/// The refusal that one frozen topic gives every produce partition of a
/// request.
///
/// The produce path resolves it once per topic and carries it down to each
/// partition, so a partition of a frozen topic builds its message and nothing
/// more. A partition of an unfrozen topic never sees one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FreezeVerdict {
    /// The registry scope that matched the topic.
    pub scope: String,
    /// The pattern type of that scope.
    pub pattern_type: PatternType,
    /// The text the operator gave when they set the freeze. It can be empty.
    pub reason: String,
}

impl FreezeVerdict {
    /// The `error_message` that rides beside `POLICY_VIOLATION` (44).
    ///
    /// It names the freeze and the scope that matched, so the producer's
    /// on-call reads the state of the cluster rather than guessing at a
    /// misconfigured principal. The operator's reason follows when there is
    /// one.
    pub(crate) fn error_message(&self) -> String { self.refusal("this write") }

    /// The `error_message` for an operation that would take data *out* of the
    /// frozen topic, which `DeleteRecords` and `DeleteTopics` both do.
    ///
    /// The wording differs from [`Self::error_message`] because the operator is
    /// not producing. Telling someone who ran `kafka-topics --delete` that
    /// their write was refused sends them looking for a producer that does not
    /// exist.
    pub(crate) fn removal_message(&self) -> String { self.refusal("this deletion") }

    /// The refusal text for a reassignment start, cancel, or completion.
    pub(crate) fn reassignment_message(&self) -> String { self.refusal("this reassignment") }

    /// The refusal text, with `refused` naming what the freeze turned away.
    ///
    /// It names the freeze and the scope that matched, so the caller's on-call
    /// reads the state of the cluster rather than guessing at a misconfigured
    /// principal. The operator's reason follows when there is one.
    fn refusal(&self, refused: &str) -> String {
        let kind = pattern_type_name(self.pattern_type);
        let scope = &self.scope;
        let mut message = format!("a write freeze on the {kind} scope {scope:?} refuses {refused}");
        if !self.reason.is_empty() {
            message.push_str(": ");
            message.push_str(&self.reason);
        }
        message
    }
}

impl From<&TopicFreezeRecord> for FreezeVerdict {
    fn from(record: &TopicFreezeRecord) -> Self {
        Self {
            scope: record.scope.clone(),
            pattern_type: record.pattern_type,
            reason: record.reason.clone(),
        }
    }
}

/// The refusal that covers `topic`, or `None` when the topic accepts writes.
///
/// This is [`resolve_topic_freeze`] plus the message build, for a caller that
/// wants the finished verdict rather than the record.
pub(crate) fn resolve_freeze_verdict(image: &MetadataImage, topic: &str) -> Option<FreezeVerdict> {
    resolve_topic_freeze(image, topic).map(FreezeVerdict::from)
}

#[cfg(test)]
mod tests {
    use {
        super::{
            FreezeMutationResolution, FreezeVerdict, resolve_freeze_mutation,
            resolve_freeze_verdict, resolve_topic_freeze,
        },
        crate::test_support::FreezeSetup,
        assert2::check,
        krabka_metadata::{MetadataImage, MetadataRecord, PatternType},
        krabka_verified::FreezeMutationKind,
        uuid::Uuid,
    };

    fn image() -> MetadataImage { MetadataImage::new(Uuid::from_u128(0x5150)) }

    use crate::test_support::topic_freeze_record as freeze;

    fn frozen(image: &mut MetadataImage, setup: FreezeSetup<'_>) {
        image.apply(&MetadataRecord::V1TopicFreeze(
            freeze()
                .scope(setup.scope)
                .pattern_type(setup.pattern_type)
                .state(setup.state)
                .reason(setup.reason)
                .call(),
        ));
    }

    #[yare::parameterized(
        ordinary_topic = { "an ordinary topic", "orders" },
        internal_topic = { "an internal topic", "__consumer_offsets" },
        empty_name = { "an empty name", "" },
    )]
    fn a_cluster_with_no_freeze_resolves_nothing(label: &str, topic: &str) {
        let image = image();

        check!(resolve_topic_freeze(&image, topic).is_none(), "{label}");
        check!(resolve_freeze_verdict(&image, topic).is_none(), "{label}");
    }

    #[test]
    fn mutation_resolution_hides_freeze_detail_until_authorized() {
        let mut image = image();
        image.apply(&MetadataRecord::V1TopicFreeze(freeze().call()));

        check!(
            resolve_freeze_mutation(&image, "orders", false, FreezeMutationKind::Produce)
                == FreezeMutationResolution::AuthorizationDenied
        );
        check!(matches!(
            resolve_freeze_mutation(&image, "orders", true, FreezeMutationKind::Produce),
            FreezeMutationResolution::Frozen(record) if record.reason == "DR cutover"
        ));
        check!(
            resolve_freeze_mutation(&image, "orders", true, FreezeMutationKind::Replication)
                == FreezeMutationResolution::Admit
        );
    }

    #[yare::parameterized(
        literal_beats_prefix = {
            "a literal entry beats every prefix entry",
            "tenant-a.orders.eu",
            Some("tenant-a.orders.eu"),
        },
        longest_prefix_wins = {
            "the longest matching prefix wins",
            "tenant-a.orders.us",
            Some("tenant-a.orders."),
        },
        shorter_prefix_covers_namespace = {
            "a shorter prefix still covers its namespace",
            "tenant-a.billing",
            Some("tenant-a."),
        },
        no_matching_scope = { "a topic that no scope covers", "tenant-b.orders", None },
    )]
    fn a_literal_scope_beats_a_prefix_and_the_longest_prefix_wins(
        label: &str,
        topic: &str,
        expected: Option<&str>,
    ) {
        let mut image = image();
        frozen(&mut image, FreezeSetup {
            scope: "tenant-a.",
            pattern_type: PatternType::Prefixed,
            reason: "wide",
            ..Default::default()
        });
        frozen(&mut image, FreezeSetup {
            scope: "tenant-a.orders.",
            pattern_type: PatternType::Prefixed,
            reason: "narrow",
            ..Default::default()
        });
        frozen(&mut image, FreezeSetup {
            scope: "tenant-a.orders.eu",
            reason: "one topic",
            ..Default::default()
        });

        check!(
            resolve_topic_freeze(&image, topic).map(|f| f.scope.as_str()) == expected,
            "{label}"
        );
        check!(
            resolve_topic_freeze(&image, topic) == image.topic_freeze(topic),
            "{label}"
        );
        check!(
            resolve_freeze_verdict(&image, topic).map(|v| v.scope) == expected.map(str::to_owned),
            "{label}"
        );
    }

    #[yare::parameterized(
        empty_prefix = { "an empty prefix scope", "", PatternType::Prefixed, Some("orders") },
        one_underscore_prefix = {
            "a one-underscore prefix scope",
            "_",
            PatternType::Prefixed,
            Some("_metrics"),
        },
        literal_internal_name = {
            "a literal internal name",
            "__consumer_offsets",
            PatternType::Literal,
            None,
        },
    )]
    fn an_internal_topic_resolves_no_freeze_even_under_a_live_registry(
        label: &str,
        scope: &str,
        pattern_type: PatternType,
        covered: Option<&str>,
    ) {
        let mut image = image();
        frozen(&mut image, FreezeSetup {
            scope,
            pattern_type,
            reason: "incident",
            ..Default::default()
        });

        check!(
            resolve_topic_freeze(&image, "__consumer_offsets").is_none(),
            "{label}"
        );
        check!(
            resolve_topic_freeze(&image, "__transaction_state").is_none(),
            "{label}"
        );
        if let Some(topic) = covered {
            // The registry is live, so the exemption comes from the name
            // and not from an empty registry.
            check!(resolve_topic_freeze(&image, topic).is_some(), "{label}");
        }
    }

    #[test]
    fn a_verdict_carries_the_scope_the_pattern_type_and_the_reason() {
        let mut image = image();
        frozen(&mut image, FreezeSetup {
            scope: "tenant-a.",
            pattern_type: PatternType::Prefixed,
            ..Default::default()
        });

        let expected = FreezeVerdict {
            scope: "tenant-a.".to_owned(),
            pattern_type: PatternType::Prefixed,
            reason: "DR cutover".to_owned(),
        };
        check!(resolve_freeze_verdict(&image, "tenant-a.orders") == Some(expected));
    }

    #[yare::parameterized(
        literal_with_reason = {
            "a literal scope with a reason",
            "orders",
            PatternType::Literal,
            "DR cutover",
            "a write freeze on the literal scope \"orders\" refuses this write: DR cutover",
        },
        prefixed_with_reason = {
            "a prefixed scope with a reason",
            "tenant-a.",
            PatternType::Prefixed,
            "tenant offboarding",
            "a write freeze on the prefixed scope \"tenant-a.\" refuses this write: tenant offboarding",
        },
        without_reason = {
            "a scope with no reason",
            "orders",
            PatternType::Literal,
            "",
            "a write freeze on the literal scope \"orders\" refuses this write",
        },
    )]
    fn a_verdict_message_names_the_freeze_and_the_scope_that_matched(
        label: &str,
        scope: &str,
        pattern_type: PatternType,
        reason: &str,
        expected: &str,
    ) {
        let verdict = FreezeVerdict {
            scope: scope.to_owned(),
            pattern_type,
            reason: reason.to_owned(),
        };
        check!(verdict.error_message() == expected, "{label}");
    }

    #[yare::parameterized(
        with_reason = {
            "with a reason",
            "DR cutover",
            "a write freeze on the literal scope \"orders\" refuses this deletion: DR cutover",
        },
        without_reason = {
            "with no reason",
            "",
            "a write freeze on the literal scope \"orders\" refuses this deletion",
        },
    )]
    fn a_removal_message_says_deletion_rather_than_write(
        label: &str,
        reason: &str,
        expected: &str,
    ) {
        let verdict = FreezeVerdict {
            scope: "orders".to_owned(),
            pattern_type: PatternType::Literal,
            reason: reason.to_owned(),
        };
        check!(verdict.removal_message() == expected, "{label}");
        // The two messages differ only in what they name as refused. A
        // `DeleteTopics` caller told their *write* was refused goes looking
        // for a producer that is not there.
        check!(
            verdict.removal_message() != verdict.error_message(),
            "{label}"
        );
    }
}
