//! The member fixture the `consumer_state` submodule tests share.

use {
    super::{group::GroupState, member::MemberState, regex::ResolvedRegularExpression},
    crate::coordinator::unified::{actor::MetadataProvider, reconciler::ReconcileInput},
    krabka_protocol::primitives::uuid::Uuid,
    std::time::{Duration, Instant},
};

pub(crate) fn member(id: &str) -> MemberState {
    MemberState {
        client_id: "c".into(),
        client_host: "/127.0.0.1".into(),
        rebalance_timeout: Duration::from_mins(1),
        ..MemberState::empty(id, Instant::now())
    }
}

/// `member(id)` subscribed to the topics `names`.
pub(crate) fn subscribed_member(id: &str, names: &[&str]) -> MemberState {
    MemberState {
        subscribed_topic_names: names.iter().map(|name| (*name).to_owned()).collect(),
        ..member(id)
    }
}

pub(crate) fn regex_group(subscriptions: &[(&str, Option<&str>)]) -> GroupState {
    let mut group = GroupState::new("g");
    for (member_id, regex) in subscriptions {
        let mut m = member(member_id);
        m.subscribed_topic_regex = regex.map(str::to_owned);
        group.add_or_update_member(m);
    }
    group
}

pub(crate) fn resolved_regex(
    topics: &[&str],
    version: i64,
    timestamp_ms: i64,
) -> ResolvedRegularExpression {
    ResolvedRegularExpression {
        topics: topics.iter().map(|topic| (*topic).to_owned()).collect(),
        version,
        timestamp_ms,
    }
}

/// A metadata provider that holds the topics it was given, each with two
/// partitions.
#[derive(Debug)]
pub(crate) struct Topics(pub(crate) Vec<(&'static str, Uuid)>);

impl MetadataProvider for Topics {
    fn snapshot(&self) -> ReconcileInput {
        ReconcileInput {
            topic_id_by_name: self
                .0
                .iter()
                .map(|(name, id)| ((*name).to_owned(), *id))
                .collect(),
            partitions_per_topic: self.0.iter().map(|(_, id)| (*id, 2)).collect(),
            ..Default::default()
        }
    }
}
