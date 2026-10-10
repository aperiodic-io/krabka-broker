//! The group side of `OffsetDelete` (KIP-496).
//!
//! Kafka's `OffsetMetadataManager.deleteOffsets` first runs the group's
//! `validateOffsetDelete`, then answers `GROUP_SUBSCRIBED_TO_TOPIC` for every
//! partition of a topic for which `isSubscribedToTopic` holds. This module
//! computes both from the live group: the group-level error, or the set of
//! topics that the group subscribes to.

use std::collections::HashSet;

use bytes::Buf as _;
use krabka_protocol::{
    Decode, owned::consumer_protocol_subscription::ConsumerProtocolSubscription,
};

use super::ErrorCode;
use crate::{
    codes,
    coordinator::unified::{
        classic_state::{ClassicGroup as ClassicState, GroupState as ClassicGroupState},
        consumer_state::GroupState as ConsumerState,
        group::CoordinatorGroup,
    },
};

/// The topics whose offsets `OffsetDelete` must not remove.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscribedTopics {
    /// Every topic. A classic consumer group whose subscriptions Kafka cannot
    /// read (`ClassicGroup.isSubscribedToTopic` falls back to
    /// `usesConsumerGroupProtocol()`), or a KIP-848 group with a regex
    /// subscription that replay restored and no heartbeat has resolved since.
    All,
    /// Exactly these topics.
    Named(HashSet<String>),
}

impl SubscribedTopics {
    /// Kafka's `Group.isSubscribedToTopic`.
    #[must_use]
    pub fn contains(&self, topic: &str) -> bool {
        match self {
            Self::All => true,
            Self::Named(topics) => topics.contains(topic),
        }
    }
}

/// Kafka's `validateOffsetDelete` followed by the group's subscribed topics.
pub(super) fn offset_delete_guard(group: &CoordinatorGroup) -> Result<SubscribedTopics, ErrorCode> {
    if let Some(state) = group.as_classic() {
        classic_guard(state)
    } else if let Some(state) = group.as_consumer() {
        // `ConsumerGroup.validateOffsetDelete` accepts every state.
        Ok(consumer_subscribed_topics(state))
    } else {
        Err(codes::GROUP_ID_NOT_FOUND)
    }
}

/// `ClassicGroup.validateOffsetDelete` and `ClassicGroup.isSubscribedToTopic`.
///
/// A non-empty group that does not use the consumer protocol (Connect, for
/// example) is refused with `NON_EMPTY_GROUP`. A consumer group subscribes to
/// the topics in its members' metadata for the selected protocol, read with
/// the v0 schema that prefixes every `ConsumerProtocolSubscription` version.
/// When no protocol is selected yet or a member's metadata does not decode,
/// Kafka cannot tell, and treats the group as subscribed to every topic.
fn classic_guard(state: &ClassicState) -> Result<SubscribedTopics, ErrorCode> {
    if state.state == ClassicGroupState::Empty || state.members.is_empty() {
        return Ok(SubscribedTopics::Named(HashSet::new()));
    }
    if state.protocol_type.as_deref() != Some("consumer") {
        return Err(codes::NON_EMPTY_GROUP);
    }
    if state.protocol_name.is_none() {
        return Ok(SubscribedTopics::All);
    }
    let mut topics = HashSet::new();
    for member in state.members.values() {
        let Some(member_topics) = decode_subscribed_topics(&member.protocol_metadata) else {
            return Ok(SubscribedTopics::All);
        };
        topics.extend(member_topics);
    }
    Ok(SubscribedTopics::Named(topics))
}

/// `ModernGroup.isSubscribedToTopic`: the names the members subscribe to,
/// plus the topics their regular expressions resolved to in the group.
///
/// A regex that the group has not resolved yet subscribes the group to topics
/// it cannot name. Until it is resolved the group counts as subscribed to
/// every topic, so neither `OffsetDelete` nor offset expiration removes an
/// offset the regex may still cover.
fn consumer_subscribed_topics(state: &ConsumerState) -> SubscribedTopics {
    if state.has_unresolved_regex() {
        return SubscribedTopics::All;
    }
    let mut topics = HashSet::new();
    for member in state.members.values() {
        topics.extend(member.subscribed_topic_names.iter().cloned());
        topics.extend(state.regex_topics(member).cloned());
    }
    SubscribedTopics::Named(topics)
}

/// Kafka's `ConsumerProtocol.deserializeVersion` and
/// `deserializeConsumerProtocolSubscription(buffer, 0)`: skip the `i16`
/// version, then read the v0 body. `None` when the blob does not decode.
fn decode_subscribed_topics(metadata: &[u8]) -> Option<Vec<String>> {
    if metadata.len() < 2 {
        return None;
    }
    let mut cur = metadata;
    let _version = cur.get_i16();
    ConsumerProtocolSubscription::decode(&mut cur, 0)
        .ok()
        .map(|subscription| subscription.topics)
}

#[cfg(test)]
mod tests {
    use assert2::check;
    use bytes::Bytes;

    use super::*;
    use crate::coordinator::unified::{
        actor::test_support::subscription_blob_at as subscription,
        classic_state::Member as ClassicMember,
        consumer_state::{ResolvedRegularExpression, test_support::member},
    };

    fn named(topics: &[&str]) -> SubscribedTopics {
        SubscribedTopics::Named(topics.iter().map(|s| (*s).to_string()).collect())
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct ClassicOffsetSetup<'a> {
        #[default(ClassicGroupState::Empty)]
        state: ClassicGroupState,
        #[default(Some("consumer"))]
        protocol_type: Option<&'a str>,
        protocol_name: Option<&'a str>,
        metadata: &'a [Bytes],
    }

    fn classic(setup: ClassicOffsetSetup<'_>) -> ClassicState {
        let ClassicOffsetSetup {
            state,
            protocol_type,
            protocol_name,
            metadata,
        } = setup;
        let mut g = ClassicState::new("g");
        g.state = state;
        g.protocol_type = protocol_type.map(String::from);
        g.protocol_name = protocol_name.map(String::from);
        for (i, blob) in metadata.iter().enumerate() {
            let id = format!("m{i}");
            let m = ClassicMember::new(
                id.clone(),
                "client",
                "host",
                std::time::Duration::from_secs(30),
                std::time::Duration::from_mins(1),
                vec![("range".into(), blob.clone())],
            );
            g.members.insert(id, m);
        }
        g
    }

    /// `ClassicGroup.validateOffsetDelete` and `isSubscribedToTopic`, row by
    /// row.
    #[test]
    fn classic_guard_follows_kafka() {
        let rows = [
            (
                "empty consumer group subscribes to nothing",
                classic(ClassicOffsetSetup::default()),
                Ok(named(&[])),
            ),
            (
                "empty connect group subscribes to nothing",
                classic(ClassicOffsetSetup {
                    protocol_type: Some("connect"),
                    ..Default::default()
                }),
                Ok(named(&[])),
            ),
            (
                "stable connect group is not empty",
                classic(ClassicOffsetSetup {
                    state: ClassicGroupState::Stable,
                    protocol_type: Some("connect"),
                    protocol_name: Some("default"),
                    metadata: &[Bytes::from_static(b"x")],
                }),
                Err(codes::NON_EMPTY_GROUP),
            ),
            (
                "preparing group without a protocol type is not empty",
                classic(ClassicOffsetSetup {
                    state: ClassicGroupState::PreparingRebalance,
                    protocol_type: None,
                    metadata: &[Bytes::new()],
                    ..Default::default()
                }),
                Err(codes::NON_EMPTY_GROUP),
            ),
            (
                "consumer group unions its members' topics, any version",
                classic(ClassicOffsetSetup {
                    state: ClassicGroupState::Stable,
                    protocol_name: Some("range"),
                    metadata: &[subscription(0, &["a", "b"]), subscription(3, &["c"])],
                    ..Default::default()
                }),
                Ok(named(&["a", "b", "c"])),
            ),
            (
                "undecodable metadata subscribes to every topic",
                classic(ClassicOffsetSetup {
                    state: ClassicGroupState::CompletingRebalance,
                    protocol_name: Some("range"),
                    metadata: &[subscription(1, &["a"]), Bytes::from_static(b"\x00")],
                    ..Default::default()
                }),
                Ok(SubscribedTopics::All),
            ),
            (
                "no protocol selected yet subscribes to every topic",
                classic(ClassicOffsetSetup {
                    state: ClassicGroupState::PreparingRebalance,
                    metadata: &[subscription(0, &["a"])],
                    ..Default::default()
                }),
                Ok(SubscribedTopics::All),
            ),
        ];
        for (name, group, want) in rows {
            check!(classic_guard(&group) == want, "{name}");
        }
    }

    /// `ModernGroup.isSubscribedToTopic`: the names, and the topics that the
    /// group resolved each regex to. A regex the group has not resolved makes
    /// the group subscribed to every topic.
    #[test]
    fn consumer_group_subscribes_to_names_and_resolved_regex() {
        let mut g = ConsumerState::new("g");
        let mut by_name = member("m1");
        by_name.subscribed_topic_names = HashSet::from(["orders".to_string()]);
        let mut by_regex = member("m2");
        by_regex.subscribed_topic_regex = Some("pay.*".into());
        g.add_or_update_member(by_name);
        g.add_or_update_member(by_regex);

        check!(consumer_subscribed_topics(&g) == SubscribedTopics::All);

        g.set_resolved_regex(
            "pay.*".into(),
            ResolvedRegularExpression {
                topics: ["payments".to_string()].into(),
                version: 1,
                timestamp_ms: 1,
            },
        );
        check!(consumer_subscribed_topics(&g) == named(&["orders", "payments"]));
    }
}
