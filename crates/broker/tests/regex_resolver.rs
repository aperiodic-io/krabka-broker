//! A custom resolver implemented through the broker's public API.

use std::collections::{BTreeSet, HashMap};

use assert2::assert;
use krabka_broker::coordinator::unified::regex_resolver::{
    ResolvedRegularExpression, TopicRegexResolver,
};

#[derive(Debug)]
struct OrdersResolver;

impl TopicRegexResolver for OrdersResolver {
    fn resolve(&self, regexes: &BTreeSet<String>) -> HashMap<String, ResolvedRegularExpression> {
        regexes
            .iter()
            .map(|pattern| {
                (
                    pattern.clone(),
                    ResolvedRegularExpression {
                        topics: BTreeSet::from(["orders".to_owned()]),
                        version: 42,
                        timestamp_ms: 1_000,
                    },
                )
            })
            .collect()
    }
}

#[test]
fn a_consumer_can_implement_and_call_a_custom_resolver() {
    let resolver: &dyn TopicRegexResolver = &OrdersResolver;
    let subscriptions = resolver.resolve(&BTreeSet::from(["orders.*".to_owned()]));
    assert!(
        subscriptions
            == HashMap::from([(
                "orders.*".to_owned(),
                ResolvedRegularExpression {
                    topics: BTreeSet::from(["orders".to_owned()]),
                    version: 42,
                    timestamp_ms: 1_000,
                },
            )])
    );
}
