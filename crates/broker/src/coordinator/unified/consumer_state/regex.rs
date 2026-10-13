//! The regular-expression subscriptions of a next-gen consumer group and the
//! topics that each one resolved to.
//!
//! A member that sends a `SubscribedTopicRegex` subscribes to the topics the
//! pattern resolves to, beside the names it lists. Kafka resolves a pattern for
//! the whole group, not for each member: `TopicRegexResolver` matches every
//! pattern the members use against the topics of the metadata image, keeps the
//! topics that the requesting principal may `Describe`, and the group stores
//! the result, `ConsumerGroup.resolvedRegularExpressions`. A
//! `ConsumerGroupRegularExpression` record persists each entry, so a
//! coordinator failover restores the topics that the patterns had resolved to.
//! The group refreshes a resolution when a heartbeat finds it stale; see
//! `actor::regex_resolution`.

use {
    super::{group::GroupState, member::MemberState},
    crate::coordinator::unified::persistence_next_gen::RegularExpressionValue,
    std::collections::{BTreeSet, HashMap},
};

/// Kafka's `ResolvedRegularExpression`: the topics that a regular expression
/// resolved to, the version of the metadata image they were resolved from, and
/// the time of the resolution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedRegularExpression {
    /// The topics that the pattern matches and the resolving principal may
    /// `Describe`. A topic that was deleted since stays until the next
    /// resolution, and is ignored where the metadata image no longer has it.
    pub topics: BTreeSet<String>,
    /// The version of the metadata image the topics were resolved from.
    pub version: i64,
    /// The wall-clock time of the resolution, in milliseconds since the epoch.
    pub timestamp_ms: i64,
}

impl From<RegularExpressionValue> for ResolvedRegularExpression {
    fn from(value: RegularExpressionValue) -> Self {
        Self {
            topics: value.topics.into_iter().collect(),
            version: value.version,
            timestamp_ms: value.timestamp_ms,
        }
    }
}

impl From<&ResolvedRegularExpression> for RegularExpressionValue {
    fn from(resolved: &ResolvedRegularExpression) -> Self {
        Self {
            topics: resolved.topics.iter().cloned().collect(),
            version: resolved.version,
            timestamp_ms: resolved.timestamp_ms,
        }
    }
}

impl GroupState {
    /// Kafka's `ConsumerGroup.subscribedRegularExpressions`: how many members
    /// subscribe to each regular expression. A member with no pattern, or the
    /// empty one that drops it, subscribes to none.
    #[must_use]
    pub(crate) fn subscribed_regexes(&self) -> HashMap<String, usize> {
        let mut counts = HashMap::new();
        for regex in self
            .members
            .values()
            .filter_map(|member| member.subscribed_topic_regex.as_deref())
        {
            *counts.entry(regex.to_owned()).or_insert(0) += 1;
        }
        counts
    }

    /// Kafka's `ConsumerGroup.numSubscribedMembers`.
    #[must_use]
    pub(crate) fn num_subscribed_members(&self, regex: &str) -> usize {
        self.members
            .values()
            .filter(|member| member.subscribed_topic_regex.as_deref() == Some(regex))
            .count()
    }

    /// Kafka's `ConsumerGroup.resolvedRegularExpression`.
    #[must_use]
    pub fn resolved_regex(&self, regex: &str) -> Option<&ResolvedRegularExpression> {
        self.resolved_regexes.get(regex)
    }

    /// Kafka's `ConsumerGroup.numResolvedRegularExpressions`.
    #[must_use]
    pub(crate) fn num_resolved_regexes(&self) -> usize { self.resolved_regexes.len() }

    /// The regular expressions that the group holds a resolution of: the keys
    /// of Kafka's `ConsumerGroup.resolvedRegularExpressions`.
    #[must_use]
    pub(crate) fn resolved_regex_names(&self) -> Vec<String> {
        self.resolved_regexes.keys().cloned().collect()
    }

    /// Kafka's `ConsumerGroup.updateResolvedRegularExpression`.
    pub(crate) fn set_resolved_regex(
        &mut self,
        regex: String,
        resolved: ResolvedRegularExpression,
    ) {
        self.resolved_regexes.insert(regex, resolved);
    }

    /// Kafka's `ConsumerGroup.removeResolvedRegularExpression`. It returns
    /// whether the group held the resolution.
    pub(crate) fn remove_resolved_regex(&mut self, regex: &str) -> bool {
        self.resolved_regexes.remove(regex).is_some()
    }

    /// Kafka's `ConsumerGroup.lastResolvedRegularExpressionRefreshTimeMs`:
    /// the time of the latest resolution, or `i64::MIN` when the group has
    /// none. Kafka reads one entry as the proxy for all of them, because a
    /// refresh resolves every pattern together.
    #[must_use]
    pub(crate) fn last_regex_resolution_ms(&self) -> i64 {
        self.resolved_regexes
            .values()
            .map(|resolved| resolved.timestamp_ms)
            .max()
            .unwrap_or(i64::MIN)
    }

    /// Kafka's `ConsumerGroup.lastResolvedRegularExpressionVersion`: the
    /// oldest metadata version a resolution was made from, or `0` when the
    /// group has none.
    #[must_use]
    pub(crate) fn last_regex_resolution_version(&self) -> i64 {
        self.resolved_regexes
            .values()
            .map(|resolved| resolved.version)
            .min()
            .unwrap_or(0)
    }

    /// The topics that `member`'s regular expression resolved to, in name
    /// order. It is empty for a member with no pattern and for a pattern the
    /// group has not resolved yet.
    pub(crate) fn regex_topics(&self, member: &MemberState) -> impl Iterator<Item = &String> {
        member
            .subscribed_topic_regex
            .as_deref()
            .and_then(|regex| self.resolved_regexes.get(regex))
            .into_iter()
            .flat_map(|resolved| resolved.topics.iter())
    }

    /// `true` when `member` subscribes to the topic `name`, by name or through
    /// the topics its regex resolved to. A pattern that is not resolved yet
    /// matches no topic, as Kafka's `CurrentAssignmentBuilder.subscribedTopicIds`
    /// treats it.
    #[must_use]
    pub(crate) fn member_subscribes_to(&self, member: &MemberState, name: &str) -> bool {
        member.subscribed_topic_names.contains(name)
            || member
                .subscribed_topic_regex
                .as_deref()
                .and_then(|regex| self.resolved_regexes.get(regex))
                .is_some_and(|resolved| resolved.topics.contains(name))
    }

    /// `true` when a member subscribes to a regular expression that the group
    /// has not resolved, so the topics that the member subscribes to are not
    /// known yet.
    #[must_use]
    pub(crate) fn has_unresolved_regex(&self) -> bool {
        self.members.values().any(|member| {
            member
                .subscribed_topic_regex
                .as_deref()
                .is_some_and(|regex| !self.resolved_regexes.contains_key(regex))
        })
    }

    /// The resolved regular expressions that no member subscribes to any more,
    /// in name order: Kafka's `maybeDeleteResolvedRegularExpressions`, which
    /// tombstones each of them when the members that used it leave.
    #[must_use]
    pub(crate) fn unsubscribed_resolved_regexes(&self) -> Vec<String> {
        let subscribed = self.subscribed_regexes();
        let mut regexes: Vec<String> = self
            .resolved_regexes
            .keys()
            .filter(|regex| !subscribed.contains_key(regex.as_str()))
            .cloned()
            .collect();
        regexes.sort_unstable();
        regexes
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::coordinator::unified::consumer_state::test_support::{
            regex_group as group_with_regexes, resolved_regex as resolved,
        },
        assert2::assert,
    };

    #[test]
    fn subscriptions_are_counted_per_regex() {
        let group = group_with_regexes(&[
            ("m1", Some("a.*")),
            ("m2", Some("a.*")),
            ("m3", Some("b.*")),
            ("m4", None),
        ]);
        assert!(
            group.subscribed_regexes()
                == HashMap::from([("a.*".to_owned(), 2), ("b.*".to_owned(), 1)])
        );
        assert!(group.num_subscribed_members("a.*") == 2);
        assert!(group.num_subscribed_members("c.*") == 0);
    }

    #[test]
    fn a_member_subscribes_to_the_topics_its_regex_resolved_to() {
        let mut group =
            group_with_regexes(&[("m1", Some("a.*")), ("m2", Some("b.*")), ("m3", None)]);
        group.set_resolved_regex("a.*".into(), resolved(&["a1", "a2"], 5, 100));
        let topics_of = |member_id: &str| -> Vec<String> {
            group
                .regex_topics(&group.members[member_id])
                .cloned()
                .collect()
        };
        assert!(topics_of("m1") == ["a1", "a2"]);
        // `b.*` is not resolved yet, and m3 has no pattern.
        assert!(topics_of("m2").is_empty());
        assert!(topics_of("m3").is_empty());
        assert!(group.has_unresolved_regex());
        assert!(group.subscribes_to_any(&["a2".to_owned()]));
        assert!(!group.subscribes_to_any(&["b1".to_owned()]));
        group.set_resolved_regex("b.*".into(), resolved(&[], 5, 100));
        assert!(!group.has_unresolved_regex());
    }

    #[test]
    fn the_last_resolution_is_the_latest_time_and_the_oldest_version() {
        let mut group = group_with_regexes(&[("m1", Some("a.*")), ("m2", Some("b.*"))]);
        assert!(group.last_regex_resolution_ms() == i64::MIN);
        assert!(group.last_regex_resolution_version() == 0);
        group.set_resolved_regex("a.*".into(), resolved(&[], 7, 100));
        group.set_resolved_regex("b.*".into(), resolved(&[], 9, 300));
        assert!(group.last_regex_resolution_ms() == 300);
        assert!(group.last_regex_resolution_version() == 7);
        assert!(group.num_resolved_regexes() == 2);
        assert!(group.remove_resolved_regex("a.*"));
        assert!(!group.remove_resolved_regex("a.*"));
    }

    #[test]
    fn a_resolution_no_member_uses_any_more_is_unsubscribed() {
        let mut group = group_with_regexes(&[("m1", Some("a.*")), ("m2", Some("b.*"))]);
        for regex in ["a.*", "b.*", "c.*"] {
            group.set_resolved_regex(regex.into(), resolved(&[], 1, 1));
        }
        assert!(group.unsubscribed_resolved_regexes() == ["c.*"]);
        group.remove_member("m1");
        assert!(group.unsubscribed_resolved_regexes() == ["a.*", "c.*"]);
    }

    #[test]
    fn the_persisted_value_and_the_resolution_convert_both_ways() {
        let value = RegularExpressionValue {
            topics: vec!["a".into(), "b".into()],
            version: 3,
            timestamp_ms: 4,
        };
        let converted = ResolvedRegularExpression::from(value.clone());
        assert!(converted == resolved(&["a", "b"], 3, 4));
        assert!(RegularExpressionValue::from(&converted) == value);
    }
}
