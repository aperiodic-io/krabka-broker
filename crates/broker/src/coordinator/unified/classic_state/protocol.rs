//! The classic-protocol embedded-protocol vote and the protocol support check.
//!
//! `select_protocol` decides which assignor name a rebalance round settles on,
//! from the `protocols` list every member offered in its `JoinGroup`.
//! [`ClassicGroup::supports_protocols`] is the gate a joining member passes
//! before the group admits it, so the vote always has a candidate.

use std::collections::{HashMap, HashSet};

use super::{
    group::{ClassicGroup, GroupState},
    member::Member,
};

/// The protocol names every member proposed, Kafka's
/// `ClassicGroup.candidateProtocols`.
fn candidate_protocols(members: &HashMap<String, Member>) -> HashSet<&str> {
    let mut support: HashMap<&str, usize> = HashMap::new();
    for member in members.values() {
        let names: HashSet<&str> = member.protocols.iter().map(|(n, _)| n.as_str()).collect();
        for name in names {
            *support.entry(name).or_insert(0) += 1;
        }
    }
    support
        .into_iter()
        .filter(|&(_, count)| count == members.len())
        .map(|(name, _)| name)
        .collect()
}

/// Kafka's `ClassicGroup.selectProtocol`. Each member votes for its most
/// preferred protocol among the names every member proposed
/// (`ClassicGroupMember.vote`), and the name with the most votes wins. Kafka
/// breaks a tie by hash-map order; this picks the lexicographically smallest
/// name, so the choice is deterministic. It returns `None` when the
/// intersection is empty, and when there are no members.
#[must_use]
pub fn select_protocol(members: &HashMap<String, Member>) -> Option<String> {
    let candidates = candidate_protocols(members);
    let mut votes: HashMap<&str, usize> = HashMap::new();
    for member in members.values() {
        if let Some((name, _)) = member
            .protocols
            .iter()
            .find(|(name, _)| candidates.contains(name.as_str()))
        {
            *votes.entry(name.as_str()).or_insert(0) += 1;
        }
    }
    votes
        .into_iter()
        .max_by(|(a, va), (b, vb)| va.cmp(vb).then_with(|| b.cmp(a)))
        .map(|(name, _)| name.to_string())
}

impl ClassicGroup {
    /// Kafka's `ClassicGroup.supportsProtocols`. An `Empty` group accepts any
    /// non-empty protocol type with at least one protocol. A group with
    /// members needs the same protocol type and one protocol name that every
    /// current member supports.
    #[must_use]
    pub fn supports_protocols<'a>(
        &self,
        protocol_type: &str,
        mut protocol_names: impl Iterator<Item = &'a str>,
    ) -> bool {
        if self.state == GroupState::Empty {
            return !protocol_type.is_empty() && protocol_names.next().is_some();
        }
        if self.protocol_type.as_deref() != Some(protocol_type) {
            return false;
        }
        // With no members every name has the full support count of zero,
        // exactly as Kafka's `supportedProtocols` count compares.
        let candidates = candidate_protocols(&self.members);
        protocol_names.any(|name| self.members.is_empty() || candidates.contains(name))
    }
}

#[cfg(test)]
mod tests {
    use assert2::assert;

    use super::*;
    use crate::coordinator::unified::classic_state::test_support::member_with_protocols;

    fn one_range_first_member() -> HashMap<String, super::super::member::Member> {
        HashMap::from([(
            "m1".to_string(),
            member_with_protocols("m1", vec![("range", b""), ("cooperative_sticky", b"")]),
        )])
    }

    #[test]
    fn select_protocol_single_member_picks_first() {
        let members = one_range_first_member();
        assert!(select_protocol(&members).as_deref() == Some("range"));
    }

    #[test]
    fn select_protocol_intersection_empty_returns_none() {
        let mut members = HashMap::new();
        members.insert(
            "m1".to_string(),
            member_with_protocols("m1", vec![("range", b"")]),
        );
        members.insert(
            "m2".to_string(),
            member_with_protocols("m2", vec![("cooperative_sticky", b"")]),
        );
        assert!(select_protocol(&members) == None);
    }

    #[test]
    fn select_protocol_max_votes_wins() {
        let mut members = one_range_first_member();
        members.insert(
            "m2".to_string(),
            member_with_protocols("m2", vec![("range", b""), ("cooperative_sticky", b"")]),
        );
        members.insert(
            "m3".to_string(),
            member_with_protocols("m3", vec![("cooperative_sticky", b""), ("range", b"")]),
        );
        assert!(select_protocol(&members).as_deref() == Some("range"));
    }

    #[test]
    fn select_protocol_tie_breaks_lexicographically() {
        let mut members = one_range_first_member();
        members.insert(
            "m2".to_string(),
            member_with_protocols("m2", vec![("cooperative_sticky", b""), ("range", b"")]),
        );
        assert!(select_protocol(&members).as_deref() == Some("cooperative_sticky"));
    }

    #[test]
    fn select_protocol_empty_members_returns_none() {
        let members = HashMap::new();
        assert!(select_protocol(&members) == None);
    }

    /// #788: Kafka's `ClassicGroupMember.vote`. Each member votes for its
    /// most preferred protocol among the ones every member supports, also
    /// when its first choice is not one of them.
    #[test]
    fn each_member_votes_for_its_most_preferred_candidate() {
        type Prefs = &'static [&'static str];
        let rows: [(&str, &[Prefs], Option<&str>); 4] = [
            ("one member", &[&["range", "sticky"]], Some("range")),
            (
                "first choices outside the intersection still vote",
                &[
                    &["roundrobin", "sticky", "range"],
                    &["cooperative", "sticky", "range"],
                    &["range", "sticky"],
                ],
                Some("sticky"),
            ),
            (
                "majority of first choices",
                &[
                    &["range", "sticky"],
                    &["range", "sticky"],
                    &["sticky", "range"],
                ],
                Some("range"),
            ),
            ("no shared protocol", &[&["range"], &["sticky"]], None),
        ];
        for (name, prefs, want) in rows {
            let members: HashMap<String, Member> = prefs
                .iter()
                .enumerate()
                .map(|(i, names)| {
                    let id = format!("m{i}");
                    let protocols = names.iter().map(|n| (*n, &b""[..])).collect();
                    (id.clone(), member_with_protocols(&id, protocols))
                })
                .collect();
            assert!(select_protocol(&members).as_deref() == want, "{name}");
        }
    }

    /// #788: Kafka's `ClassicGroup.supportsProtocols`.
    #[test]
    fn supports_protocols_matches_kafka() {
        let mut stable = ClassicGroup::new("g");
        stable.add_member(member_with_protocols(
            "m1",
            vec![("range", b""), ("sticky", b"")],
        ));
        stable.add_member(member_with_protocols("m2", vec![("sticky", b"")]));
        stable.protocol_type = Some("consumer".into());
        stable.state = GroupState::Stable;
        let empty = ClassicGroup::new("g");
        for (name, group, protocol_type, names, want) in [
            ("empty, empty type", &empty, "", &["range"][..], false),
            ("empty, no protocols", &empty, "consumer", &[][..], false),
            ("empty, anything else", &empty, "connect", &["x"][..], true),
            (
                "stable, other type",
                &stable,
                "connect",
                &["sticky"][..],
                false,
            ),
            (
                "stable, protocol one member lacks",
                &stable,
                "consumer",
                &["range"][..],
                false,
            ),
            (
                "stable, shared protocol",
                &stable,
                "consumer",
                &["x", "sticky"][..],
                true,
            ),
        ] {
            assert!(
                group.supports_protocols(protocol_type, names.iter().copied()) == want,
                "{name}"
            );
        }
    }
}
