//! A custom resolver implemented through the broker's public API.

use {
    assert2::assert,
    krabka_broker::coordinator::unified::regex_resolver::{
        ResolvedRegularExpression, TopicRegexResolver,
    },
    std::collections::{BTreeSet, HashMap},
};

#[derive(Debug)]
struct OrdersResolver;

impl TopicRegexResolver for OrdersResolver {
    fn resolve(&self, regexes: &BTreeSet<String>) -> HashMap<String, ResolvedRegularExpression> {
        regexes.first().map_or_default(|pattern| {
            HashMap::from([(pattern.clone(), ResolvedRegularExpression {
                topics: BTreeSet::from(["orders".to_owned()]),
                version: 42,
                timestamp_ms: 1_000,
            })])
        })
    }
}

#[test]
fn a_consumer_can_implement_and_call_a_custom_resolver() {
    let resolver: &dyn TopicRegexResolver = &OrdersResolver;
    let subscriptions = resolver.resolve(&BTreeSet::from(["orders.*".to_owned()]));
    assert!(
        subscriptions
            == HashMap::from([("orders.*".to_owned(), ResolvedRegularExpression {
                topics: BTreeSet::from(["orders".to_owned()]),
                version: 42,
                timestamp_ms: 1_000,
            },)])
    );
}
