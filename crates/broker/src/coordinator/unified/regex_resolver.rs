//! Resolving the regular expressions of a consumer group against the metadata
//! image: Kafka's `TopicRegexResolver`.
//!
//! A `ConsumerGroupHeartbeat` reaches the group actor with a resolver bound to
//! the request: the metadata image the handler read, and the principal and
//! address of the caller. The actor decides when the group's regular
//! expressions need a resolution (see `actor::regex_resolution`) and calls the
//! resolver only then. The resolution is done with the requesting member's
//! principal, as Kafka does: `AuthorizableRequestContext` of the heartbeat that
//! triggered the refresh decides which of the matching topics the group keeps.

use std::{
    collections::{BTreeSet, HashMap, HashSet},
    fmt,
    net::SocketAddr,
    sync::Arc,
};

use krabka_metadata::{AclOperation, MetadataImage};
use krabka_security::Principal;

use super::consumer_state::ResolvedRegularExpression;
use crate::authorizer::{AuthorizationResult, Authorizer, authorize_topics_logged};

/// Resolves regular expressions to topics. See the module documentation.
pub trait TopicRegexResolver: Send + Sync + fmt::Debug {
    /// Kafka's `TopicRegexResolver.resolveRegularExpressions`: each of
    /// `regexes` mapped to the topics that it matches by the whole name and
    /// that the requesting principal may `Describe`, stamped with the version
    /// of the image the topics come from and the time. Every pattern of
    /// `regexes` has an entry. A pattern that does not compile resolves to no
    /// topic, as in Kafka, where the members' patterns were validated when
    /// they arrived.
    fn resolve(&self, regexes: &BTreeSet<String>) -> HashMap<String, ResolvedRegularExpression>;
}

/// The resolver of the running broker: it matches against the topics of one
/// metadata image and asks the authorizer whether `principal` may `Describe`
/// each match.
#[derive(derive_more::Debug)]
pub struct ImageTopicRegexResolver {
    #[debug(skip)]
    image: Arc<MetadataImage>,
    /// The version of `image`: the metadata offset that the caller read before
    /// it read the image. The offset can then only be older than the image,
    /// which makes a later refresh happen once too often at worst, never once
    /// too rarely.
    version: i64,
    #[debug(skip)]
    authorizer: Arc<dyn Authorizer>,
    #[debug("{:?}", principal.name)]
    principal: Principal,
    peer: SocketAddr,
}

impl ImageTopicRegexResolver {
    #[must_use]
    pub fn new(
        image: Arc<MetadataImage>,
        version: i64,
        authorizer: Arc<dyn Authorizer>,
        principal: Principal,
        peer: SocketAddr,
    ) -> Self {
        Self {
            image,
            version,
            authorizer,
            principal,
            peer,
        }
    }
}

impl TopicRegexResolver for ImageTopicRegexResolver {
    fn resolve(&self, regexes: &BTreeSet<String>) -> HashMap<String, ResolvedRegularExpression> {
        let timestamp_ms = crate::time_util::now_ms();
        let compiled: Vec<(&String, regex::Regex)> = regexes
            .iter()
            .filter_map(|pattern| {
                // Kafka's `TopicRegexResolver` selects a topic with
                // `Matcher.matches()`, so the pattern must match the whole
                // name.
                match crate::re2j::compile_full_match(pattern) {
                    Ok(regex) => Some((pattern, regex)),
                    Err(error) => {
                        tracing::error!(%pattern, %error,
                            "ignoring a subscribed topic regex that does not compile");
                        None
                    }
                }
            })
            .collect();
        let mut matched: HashMap<&String, BTreeSet<String>> = regexes
            .iter()
            .map(|pattern| (pattern, BTreeSet::new()))
            .collect();
        for topic in self.image.topics() {
            for (pattern, regex) in &compiled {
                if regex.is_match(&topic.name) {
                    matched
                        .entry(pattern)
                        .or_default()
                        .insert(topic.name.clone());
                }
            }
        }

        // Kafka's `filterTopicDescribeAuthorizedTopics`: every distinct match
        // is authorized once, and a topic that is denied leaves every pattern.
        let candidates: BTreeSet<&str> = matched
            .values()
            .flat_map(|topics| topics.iter().map(String::as_str))
            .collect();
        let denied: HashSet<String> = authorize_topics_logged(
            &*self.authorizer,
            &*self.image,
            &self.principal,
            &self.peer,
            AclOperation::Describe,
            candidates,
            false,
        )
        .into_iter()
        .filter(|(_, decision)| *decision != AuthorizationResult::Allow)
        .map(|(topic, _)| topic.to_owned())
        .collect();

        matched
            .into_iter()
            .map(|(pattern, mut topics)| {
                topics.retain(|topic| !denied.contains(topic));
                (
                    pattern.clone(),
                    ResolvedRegularExpression {
                        topics,
                        version: self.version,
                        timestamp_ms,
                    },
                )
            })
            .collect()
    }
}

/// A resolver that finds no topic: the tests of the actor that resolve no
/// pattern use it.
#[cfg(test)]
#[derive(Debug)]
pub(crate) struct NoTopicRegexResolver;

#[cfg(test)]
impl TopicRegexResolver for NoTopicRegexResolver {
    fn resolve(&self, regexes: &BTreeSet<String>) -> HashMap<String, ResolvedRegularExpression> {
        regexes
            .iter()
            .map(|pattern| {
                (
                    pattern.clone(),
                    ResolvedRegularExpression {
                        topics: BTreeSet::new(),
                        version: 0,
                        timestamp_ms: 0,
                    },
                )
            })
            .collect()
    }
}

/// The resolver that the tests of the actor use when a heartbeat resolves no
/// pattern.
#[cfg(test)]
#[must_use]
pub(crate) fn no_topic_regex_resolver() -> Arc<dyn TopicRegexResolver> {
    Arc::new(NoTopicRegexResolver)
}

/// The time that the tests of the actor put a heartbeat at, in milliseconds
/// since the epoch, and that a [`FixedRegexResolver`] stamps its resolutions
/// with.
#[cfg(test)]
pub(crate) const TEST_NOW_MS: i64 = 1_000_000_000_000;

/// A resolver that answers from a fixed table of what each pattern selects,
/// at version 100 and at [`TEST_NOW_MS`], and counts its calls. A pattern that
/// the table does not list selects no topic.
#[cfg(test)]
#[derive(Debug, Default)]
pub(crate) struct FixedRegexResolver {
    topics: HashMap<String, BTreeSet<String>>,
    timestamp_ms: i64,
    calls: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl FixedRegexResolver {
    /// A resolver for `entries`, each a pattern and the topics it selects.
    pub(crate) fn new(entries: &[(&str, &[&str])]) -> Self {
        Self {
            topics: entries
                .iter()
                .map(|(regex, topics)| {
                    (
                        (*regex).to_owned(),
                        topics.iter().map(|topic| (*topic).to_owned()).collect(),
                    )
                })
                .collect(),
            timestamp_ms: TEST_NOW_MS,
            calls: std::sync::atomic::AtomicUsize::default(),
        }
    }

    /// How many times the resolver was asked.
    pub(crate) fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[cfg(test)]
impl TopicRegexResolver for FixedRegexResolver {
    fn resolve(&self, regexes: &BTreeSet<String>) -> HashMap<String, ResolvedRegularExpression> {
        self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        regexes
            .iter()
            .map(|regex| {
                (
                    regex.clone(),
                    ResolvedRegularExpression {
                        topics: self.topics.get(regex).cloned().unwrap_or_default(),
                        version: 100,
                        timestamp_ms: self.timestamp_ms,
                    },
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::assert;
    use krabka_metadata::{MetadataRecord, TopicRecord};

    use super::*;
    use crate::{
        authorizer::{AllowAllAuthorizer, SimpleAclAuthorizer},
        test_support::peer,
    };

    fn image_with_topics(names: &[&str]) -> MetadataImage {
        let mut image = MetadataImage::new(uuid::Uuid::nil());
        for (index, name) in names.iter().enumerate() {
            image.apply(&MetadataRecord::V1Topic(TopicRecord {
                name: (*name).into(),
                topic_id: uuid::Uuid::from_bytes([u8::try_from(index + 1).unwrap(); 16]),
                partitions: 1,
                replication_factor: 1,
            }));
        }
        image
    }

    fn alice() -> Principal {
        crate::test_support::sasl_principal("alice")
    }

    fn describe_acl(topic: &str) -> MetadataRecord {
        MetadataRecord::V1AccessControlEntry(crate::test_support::allow_acl(
            crate::test_support::AllowAclSetup {
                resource_name: topic,
                operation: AclOperation::Describe,
                ..Default::default()
            },
        ))
    }

    fn resolver(image: MetadataImage, authorizer: Arc<dyn Authorizer>) -> ImageTopicRegexResolver {
        ImageTopicRegexResolver::new(Arc::new(image), 42, authorizer, alice(), peer())
    }

    fn resolved(
        resolver: &ImageTopicRegexResolver,
        patterns: &[&str],
    ) -> HashMap<String, Vec<String>> {
        let patterns: BTreeSet<String> = patterns.iter().map(|p| (*p).to_owned()).collect();
        resolver
            .resolve(&patterns)
            .into_iter()
            .map(|(pattern, resolution)| (pattern, resolution.topics.into_iter().collect()))
            .collect()
    }

    /// Kafka's `TopicRegexResolver` selects a topic with `Matcher.matches()`,
    /// so the pattern `orders` selects the topic `orders` and neither
    /// `orders-eu` nor `my-orders`. Every pattern has an entry, and a pattern
    /// that does not compile resolves to no topic.
    #[test]
    fn a_pattern_selects_only_topics_whose_whole_name_matches() {
        let image = image_with_topics(&["orders", "orders-eu", "my-orders", "audit"]);
        let resolver = resolver(image, Arc::new(AllowAllAuthorizer));
        // (pattern, the topics it selects)
        let rows: [(&str, &[&str]); 6] = [
            ("orders", &["orders"]),
            ("orders.*", &["orders", "orders-eu"]),
            (".*orders", &["my-orders", "orders"]),
            ("rders", &[]),
            ("a|orders", &["orders"]),
            ("*invalid", &[]),
        ];
        let patterns: Vec<&str> = rows.iter().map(|(pattern, _)| *pattern).collect();
        let expected: HashMap<String, Vec<String>> = rows
            .iter()
            .map(|(pattern, topics)| {
                (
                    (*pattern).to_owned(),
                    topics.iter().map(|topic| (*topic).to_owned()).collect(),
                )
            })
            .collect();
        assert!(resolved(&resolver, &patterns) == expected);
    }

    /// The resolution carries the version of the image, and the time of the
    /// call.
    #[test]
    fn a_resolution_carries_the_version_and_the_time() {
        let resolver = resolver(image_with_topics(&["a"]), Arc::new(AllowAllAuthorizer));
        let before = crate::time_util::now_ms();
        let resolutions = resolver.resolve(&BTreeSet::from(["a".to_owned()]));
        let after = crate::time_util::now_ms();
        let resolution = &resolutions["a"];
        assert!(resolution.version == 42);
        assert!((before..=after).contains(&resolution.timestamp_ms));
    }

    /// Kafka's `filterTopicDescribeAuthorizedTopics`: a match that the
    /// requesting principal may not `Describe` leaves every pattern that
    /// selects it, and an allowed match stays.
    #[test]
    fn a_match_the_principal_may_not_describe_is_left_out() {
        let mut image = image_with_topics(&["orders-eu", "orders-us", "shipments"]);
        image.apply(&describe_acl("orders-eu"));
        image.apply(&describe_acl("shipments"));
        let resolver = resolver(image, Arc::new(SimpleAclAuthorizer::new(HashSet::new())));
        let got = resolved(&resolver, &["orders-.*", "shipments", ".*"]);
        assert!(
            got == HashMap::from([
                ("orders-.*".to_owned(), vec!["orders-eu".to_owned()]),
                ("shipments".to_owned(), vec!["shipments".to_owned()]),
                (
                    ".*".to_owned(),
                    vec!["orders-eu".to_owned(), "shipments".to_owned()]
                ),
            ])
        );
    }

    /// Each distinct match is authorized once, however many patterns select
    /// it.
    #[test]
    fn each_distinct_match_is_authorized_once() {
        #[derive(Debug, Default)]
        struct Counting(AtomicUsize);
        krabka_macros::test_authorizer! { Counting, _req; {
            self.0.fetch_add(1, Ordering::SeqCst);
            AuthorizationResult::Allow
        }}
        let counting = Arc::new(Counting::default());
        let resolver = resolver(image_with_topics(&["a", "b"]), counting.clone());
        let got = resolved(&resolver, &["a", ".*", "[ab]"]);
        assert!(got["a"] == ["a"]);
        assert!(got[".*"] == ["a", "b"]);
        assert!(counting.0.load(Ordering::SeqCst) == 2);
    }
}
