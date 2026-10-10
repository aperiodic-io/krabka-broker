//! Per-member bookkeeping for the next-gen protocol: building a member from a
//! heartbeat, applying steady-state updates to one, choosing the assignor, and
//! driving the reconciler when the group is dirty.

use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use krabka_protocol::{
    owned::consumer_group_heartbeat_request::ConsumerGroupHeartbeatRequest, primitives::uuid::Uuid,
};

use super::{
    FALLBACK_REBALANCE_TIMEOUT_MS, MetadataProvider,
    regex_resolution::{
        RegexRecord, RegexResolution, RegexUpdate, Resolutions, maybe_update_regular_expressions,
    },
    views::preferred_server_assignor,
};
use crate::coordinator::unified::{
    ClientIdentity,
    assignor::Assignor,
    config::NextGenConfig,
    consumer_state::{GroupState, MemberState},
    reconciler,
};

/// The partitions a member reports that it owns in its heartbeat, or `None`
/// when the heartbeat carries no `topic_partitions`. The Java client sends the
/// list only when its assignment changed, so `None` means "unchanged", and the
/// reconciler reads it as Kafka does: the member still owns everything it
/// holds.
///
/// A request may list a topic more than once. Kafka's
/// `ownsRevokedPartitions` reads every entry, so the partitions of the entries
/// of one topic add up.
pub(super) fn reported_owned(
    req: &ConsumerGroupHeartbeatRequest,
) -> Option<HashMap<Uuid, Vec<i32>>> {
    req.topic_partitions.as_ref().map(|tp| {
        let mut owned: HashMap<Uuid, Vec<i32>> = HashMap::new();
        for topic in tp {
            owned
                .entry(topic.topic_id)
                .or_default()
                .extend(&topic.partitions);
        }
        owned
    })
}

/// Rejects a heartbeat whose `SubscribedTopicRegex` does not compile, with the
/// message Kafka builds in
/// `GroupMetadataManager.throwIfRegularExpressionIsInvalid`.
///
/// Kafka compiles the pattern with `com.google.re2j.Pattern.compile` and
/// raises `InvalidRegularExpressionException` (`INVALID_REGULAR_EXPRESSION`,
/// 128) before it writes any member record, so the heartbeat that carries the
/// bad pattern fails and the member is not admitted.
///
/// The pattern is compiled by [`crate::re2j`], which documents where its
/// dialect and RE2J's differ. The pattern is only ever matched against Kafka
/// topic names, whose legal alphabet is `[a-zA-Z0-9._-]`, so the ASCII and
/// Unicode readings of `\d`, `\w`, `\s` and `\b` cannot differ here.
pub(super) fn check_subscribed_topic_regex(pattern: &str) -> Result<(), String> {
    crate::re2j::check(pattern).map_err(|detail| {
        format!("SubscribedTopicRegex `{pattern}` is not a valid regular expression: {detail}.")
    })
}

/// What a heartbeat's member update did to the group, for the records and the
/// regex resolution that follow it.
pub(super) struct MemberUpdate {
    /// The `ConsumerGroupRegularExpression` tombstones the heartbeat wrote.
    pub(super) regex_records: Vec<RegexRecord>,
    /// What the heartbeat's regex resolution found, which the group applies
    /// in a batch of its own after the heartbeat's.
    pub(super) resolutions: Option<Resolutions>,
    /// The heartbeat ran Kafka's `updateSubscriptionMetadata` for a group
    /// whose log holds the deprecated k4 record, which it tombstones.
    pub(super) partition_metadata_tombstone: bool,
    /// The members whose target changed, when the heartbeat computed one.
    pub(super) target: Option<Vec<String>>,
}

/// Kafka's `isNotEmpty` gate on a subscribed regular expression: the empty
/// pattern is how a client drops its regex subscription.
pub(super) fn non_empty_regex(pattern: &str) -> Option<String> {
    (!pattern.is_empty()).then(|| pattern.to_owned())
}

/// Steps 1 to 3 of Kafka's `consumerGroupHeartbeat` for the member
/// `req.member_id`, which the group already holds (a new member included):
/// the member update, the regular expression update, the subscription
/// metadata update that bumps the group epoch, the target assignment of a
/// group whose epoch is ahead of its target, and the member's
/// reconciliation. `regex_before` is the member's pattern before the
/// heartbeat. It returns what the heartbeat did, and the
/// `INVALID_REGULAR_EXPRESSION` message when the heartbeat carries a
/// `SubscribedTopicRegex` that does not compile.
pub(super) fn update_member_state(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
    regexes: &RegexResolution<'_>,
) -> Result<MemberUpdate, String> {
    let old_regex = state
        .members
        .get(&req.member_id)
        .and_then(|m| m.subscribed_topic_regex.clone());
    // Kafka's `maybeUpdateSubscribedTopicRegex`: an absent pattern keeps the
    // stored one (the Java client sends the pattern only when it changed), and
    // the empty string drops it.
    let new_regex = req
        .subscribed_topic_regex
        .as_deref()
        .map_or_else(|| old_regex.clone(), non_empty_regex);
    // Kafka validates the pattern before it touches member state, and only
    // when the heartbeat brings one that differs from the member's stored
    // pattern. Do the same, so a rejected heartbeat leaves the group exactly
    // as it found it.
    if new_regex != old_regex
        && let Some(pattern) = new_regex.as_deref()
    {
        check_subscribed_topic_regex(pattern)?;
    }
    let mut names_changed = false;
    if let Some(m) = state.members.get_mut(&req.member_id) {
        m.last_seen = now;
        client.update_metadata(&mut m.client_id, &mut m.client_host);
        // Kafka's `setClassicMemberMetadata(null)`: a member that heartbeats
        // speaks the consumer protocol.
        m.classic = None;
        // Kafka's `maybeUpdateRackId` and `maybeUpdateServerAssignorName`: an
        // absent value keeps the stored one. Neither changes the group epoch.
        super::super::member_helpers::update_present(&mut m.rack_id, req.rack_id.as_ref());
        super::super::member_helpers::update_present(
            &mut m.server_assignor,
            req.server_assignor.as_ref(),
        );
        // Kafka's `maybeUpdateRebalanceTimeoutMs(ofSentinel(..))`: -1 keeps the
        // stored timeout, and any other value replaces it.
        if let Ok(millis) = u64::try_from(req.rebalance_timeout_ms) {
            m.rebalance_timeout = Duration::from_millis(millis);
        }
        if let Some(ref names) = req.subscribed_topic_names {
            let set: std::collections::HashSet<String> = names.iter().cloned().collect();
            if set != m.subscribed_topic_names {
                m.subscribed_topic_names = set;
                names_changed = true;
            }
        }
    }
    let owned = reported_owned(req);
    Ok(after_member_update(
        state,
        config,
        metadata,
        MemberChange {
            member_id: &req.member_id,
            old_regex,
            new_regex,
            names_changed,
            owned: owned.as_ref(),
        },
        regexes,
    ))
}

/// A member update that the rest of Kafka's `consumerGroupHeartbeat` follows.
pub(super) struct MemberChange<'a> {
    pub(super) member_id: &'a str,
    /// The member's pattern before the heartbeat.
    pub(super) old_regex: Option<String>,
    /// The member's pattern after the heartbeat.
    pub(super) new_regex: Option<String>,
    /// The heartbeat changed the member's subscribed topic names.
    pub(super) names_changed: bool,
    /// What the member reports owning, or `None` when it reports nothing.
    pub(super) owned: Option<&'a HashMap<Uuid, Vec<i32>>>,
}

/// The steps of Kafka's `consumerGroupHeartbeat` that follow the member
/// update: the regular expression update, the subscription metadata update
/// that bumps the group epoch, the target assignment, and the member's
/// reconciliation against what it owns.
pub(super) fn after_member_update(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
    change: MemberChange<'_>,
    regexes: &RegexResolution<'_>,
) -> MemberUpdate {
    let MemberChange {
        member_id,
        old_regex,
        new_regex,
        names_changed,
        owned,
    } = change;
    // Kafka's `maybeUpdateRegularExpressions`, before the member's new pattern
    // reaches the group's counts.
    let mut regex_records = Vec::new();
    let (regex_update, resolutions) = maybe_update_regular_expressions(
        state,
        old_regex.as_deref(),
        new_regex.as_deref(),
        regexes,
        &mut regex_records,
    );
    if let Some(m) = state.members.get_mut(member_id) {
        m.subscribed_topic_regex = new_regex;
    }
    let subscription_changed = names_changed || regex_update.regex_updated();
    // Kafka bumps the group epoch when the member changed its names, or its
    // pattern to one that the group resolved. A pattern that is not resolved
    // yet waits for its resolution, which bumps the epoch when it finds
    // topics.
    let bump = names_changed || regex_update == RegexUpdate::UpdatedAndResolved;
    let partition_metadata_tombstone = update_subscription(state, metadata, bump);
    let target = maybe_update_target(state, config, metadata);
    // Kafka's `maybeReconcile`: reconcile this member's current assignment
    // against the (possibly new) target and what it reports owning, in this
    // heartbeat only. A heartbeat without `topic_partitions` reports no change,
    // so the member still owns what it holds.
    state.reconcile_member(member_id, owned, subscription_changed, metadata);
    MemberUpdate {
        regex_records,
        resolutions,
        partition_metadata_tombstone,
        target,
    }
}

/// Kafka's `updateSubscriptionMetadata` where `consumerGroupHeartbeat` and
/// `classicGroupJoinToConsumerGroup` run it: when the heartbeat changed the
/// subscription (`bump`) or the group's metadata expired. It returns whether
/// the transition tombstones the deprecated k4 record, which Kafka does each
/// time it runs for a group whose log holds one.
pub(super) fn update_subscription(
    state: &mut GroupState,
    metadata: &dyn MetadataProvider,
    bump: bool,
) -> bool {
    if !bump && !state.metadata_refresh_requested() {
        return false;
    }
    let tombstone = state.has_subscription_metadata_record();
    if reconciler::update_subscription_metadata(state, &metadata.snapshot(), bump).is_err() {
        tracing::warn!(group_id = %state.group_id, "the group epoch is exhausted");
    }
    tombstone
}

/// Kafka's `maybeUpdateTargetAssignment`: a group whose epoch is ahead of its
/// target computes a new target, unless its assignment interval since the
/// last one has not elapsed. It returns the members whose target changed
/// when it computed one.
pub(super) fn maybe_update_target(
    state: &mut GroupState,
    config: &NextGenConfig,
    metadata: &dyn MetadataProvider,
) -> Option<Vec<String>> {
    if !state.target_is_stale()
        || state.assignment_delayed(
            config.assignment_interval,
            crate::coordinator::unified::wall_clock_ms(),
        )
    {
        return None;
    }
    let input = metadata.snapshot();
    let assignor = pick_assignor(state, config);
    let changed = reconciler::compute_target(state, &input, &*assignor);
    state.record_assignment(crate::coordinator::unified::wall_clock_ms());
    Some(changed)
}

/// Kafka's `maybeUpdateTargetAssignment`: the group runs the assignor that the
/// most members name (`ConsumerGroup.computePreferredServerAssignor`), and the
/// first of `group.consumer.assignors` when no member names one. `Describe`
/// reports the same choice.
fn pick_assignor(state: &GroupState, config: &NextGenConfig) -> Arc<dyn Assignor> {
    preferred_server_assignor(state)
        .and_then(|name| config.find_assignor(&name))
        .or_else(|| config.assignors.first().cloned())
        .expect("NextGenConfig must have at least one registered assignor")
}

/// Builds a first-join member, rejecting a heartbeat whose
/// `SubscribedTopicRegex` does not compile.
///
/// Kafka runs the same check on the join path, before any member record is
/// written, so a first heartbeat with a bad pattern fails instead of admitting
/// a member that would then never receive partitions.
pub(super) fn try_build_member(
    member_id: &str,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
) -> Result<MemberState, String> {
    if let Some(pattern) = req
        .subscribed_topic_regex
        .as_deref()
        .filter(|pattern| !pattern.is_empty())
    {
        check_subscribed_topic_regex(pattern)?;
    }
    Ok(build_member(member_id, req, client, now))
}

pub(super) fn build_member(
    member_id: &str,
    req: &ConsumerGroupHeartbeatRequest,
    client: ClientIdentity<'_>,
    now: Instant,
) -> MemberState {
    let subs: std::collections::HashSet<String> = req
        .subscribed_topic_names
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect();
    MemberState {
        instance_id: req.instance_id.clone(),
        rack_id: req.rack_id.clone(),
        client_id: client.id.into(),
        client_host: client.host.into(),
        subscribed_topic_names: subs,
        subscribed_topic_regex: req
            .subscribed_topic_regex
            .as_deref()
            .and_then(non_empty_regex),
        server_assignor: req.server_assignor.clone(),
        rebalance_timeout: Duration::from_millis(
            u64::try_from(req.rebalance_timeout_ms.max(0)).unwrap_or(FALLBACK_REBALANCE_TIMEOUT_MS),
        ),
        ..MemberState::empty(member_id, now)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use assert2::{assert, check};

    use super::*;
    use crate::coordinator::unified::{
        actor::{
            heartbeat::step_heartbeat,
            regex_resolution::apply_regex_result,
            test_support::{StaticMetadata, empty_metadata},
        },
        assignor::{Assignment, GroupSpec, TopicMetadata},
        offsets_log::fake::InMemoryOffsetsLog,
        persistence_next_gen::MemberAssignmentState,
        reconciler::ReconcileInput,
        regex_resolver::FixedRegexResolver,
    };

    /// Kafka's `consumerGroupHeartbeat` for a subscription change: the
    /// targets that changed and the target metadata, and the current
    /// assignment of the member that heartbeats only. Another member's
    /// assignment moves at its own heartbeat.
    #[test]
    fn a_subscription_change_writes_the_changed_targets_and_its_own_assignment() {
        let config = NextGenConfig::assigning_at_once();
        let first_topic = Uuid([10; 16]);
        let second_topic = Uuid([11; 16]);
        let metadata = StaticMetadata {
            input: ReconcileInput {
                topic_id_by_name: [
                    ("first".into(), first_topic),
                    ("second".into(), second_topic),
                ]
                .into(),
                partitions_per_topic: [(first_topic, 2), (second_topic, 2)].into(),
                ..Default::default()
            },
        };
        let mut state =
            super::super::test_support::subscribed_consumer_group("g", &["m1", "m2"], &["first"]);
        state.bump_epoch();
        maybe_update_target(&mut state, &config, &metadata);
        state.advance_member_epoch("m1");
        state.advance_member_epoch("m2");
        let member_epoch = state.group_epoch;

        let step = step_heartbeat(
            &mut state,
            &config,
            &metadata,
            &ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m2".into(),
                member_epoch,
                subscribed_topic_names: Some(vec!["second".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            crate::coordinator::unified::ClientIdentity {
                id: "client",
                host: "host",
            },
            Instant::now(),
            &RegexResolution::none(),
        );

        let mut target_ids: Vec<&str> = step
            .pending
            .target_per_member
            .iter()
            .map(|(member_id, _)| member_id.as_str())
            .collect();
        let mut current_ids: Vec<&str> = step
            .pending
            .current_per_member
            .iter()
            .map(|(member_id, _)| member_id.as_str())
            .collect();
        target_ids.sort_unstable();
        current_ids.sort_unstable();

        check!(step.pending.target_metadata.is_some());
        check!(target_ids == vec!["m1", "m2"]);
        assert!(current_ids == vec!["m2"]);
    }

    /// A heartbeat may list a topic twice. The partitions of its entries add
    /// up, so a member that reports a partition it must revoke in any entry
    /// still owns it, and a later entry of the same topic does not hide it.
    #[test]
    fn a_topic_listed_twice_reports_the_partitions_of_both_entries() {
        use krabka_protocol::owned::consumer_group_heartbeat_request::TopicPartitions;

        let topic = Uuid([1; 16]);
        let other = Uuid([2; 16]);
        let entry = |topic_id, partitions: &[i32]| TopicPartitions {
            topic_id,
            partitions: partitions.to_vec(),
            ..Default::default()
        };
        let report = |entries| ConsumerGroupHeartbeatRequest {
            topic_partitions: entries,
            ..Default::default()
        };
        check!(reported_owned(&report(None)).is_none());
        check!(
            reported_owned(&report(Some(vec![
                entry(topic, &[2]),
                entry(other, &[1]),
                entry(topic, &[0]),
            ]))) == Some(HashMap::from([(topic, vec![2, 0]), (other, vec![1])]))
        );

        // A member at epoch 5 that must still revoke partition 2 stays there
        // when it reports the partition in the first of two entries.
        let config = NextGenConfig::assigning_at_once();
        let metadata = StaticMetadata {
            input: ReconcileInput {
                topic_id_by_name: [("t".into(), topic)].into(),
                partitions_per_topic: [(topic, 3)].into(),
                ..Default::default()
            },
        };
        let client = ClientIdentity {
            id: "client",
            host: "host",
        };
        let mut state = GroupState::new("g");
        let mut member = build_member(
            "m1",
            &ConsumerGroupHeartbeatRequest {
                subscribed_topic_names: Some(vec!["t".into()]),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            client,
            Instant::now(),
        );
        member.member_epoch = 5;
        member.previous_member_epoch = 4;
        member.assignment_state = MemberAssignmentState::UnrevokedPartitions;
        member.assigned_partitions = [(topic, vec![0])].into();
        member.partitions_pending_revocation = [(topic, vec![2])].into();
        state.add_or_update_member(member);
        state.group_epoch = 6;
        state.install_target([("m1".to_owned(), [(topic, vec![0])].into())].into());

        let step = step_heartbeat(
            &mut state,
            &config,
            &metadata,
            &ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch: 5,
                topic_partitions: Some(vec![entry(topic, &[2]), entry(topic, &[0])]),
                ..Default::default()
            },
            client,
            Instant::now(),
            &RegexResolution::none(),
        );

        check!(step.response.error_code == 0);
        let member = &state.members["m1"];
        check!(member.member_epoch == 5);
        check!(member.assignment_state == MemberAssignmentState::UnrevokedPartitions);
        check!(member.partitions_pending_revocation == HashMap::from([(topic, vec![2])]));
    }

    /// A group that waits for its assignment interval stays dirty, because the
    /// target that a member joined for is not computed yet. Its heartbeats that
    /// change nothing write nothing, and a member that changes its
    /// subscription writes only its own member record, until the heartbeat that
    /// computes the target writes the group and target records.
    #[test]
    fn heartbeats_inside_the_assignment_interval_write_only_what_they_change() {
        let config = NextGenConfig {
            assignment_interval: Duration::from_mins(1),
            ..NextGenConfig::default()
        };
        let metadata = orders_metadata();
        let client = ClientIdentity {
            id: "client",
            host: "host",
        };
        let request =
            |member_id: &str, member_epoch: i32, names: &[&str]| ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: member_id.into(),
                member_epoch,
                subscribed_topic_names: Some(names.iter().map(|name| (*name).into()).collect()),
                rebalance_timeout_ms: 60_000,
                topic_partitions: Some(vec![]),
                ..Default::default()
            };
        let mut state = GroupState::new("g");
        let mut heartbeat = |member_id: &str, names: &[&str]| {
            let epoch = state.members.get(member_id).map_or(0, |m| m.member_epoch);
            step_heartbeat(
                &mut state,
                &config,
                &metadata,
                &request(member_id, epoch, names),
                client,
                Instant::now(),
                &RegexResolution::none(),
            )
        };

        // The first member computes the first target. The second joins inside
        // the interval, and the group waits to compute for it.
        check!(
            heartbeat("m1", &["orders-eu"])
                .pending
                .group_metadata
                .is_some()
        );
        heartbeat("m2", &["orders-eu"]);

        let unchanged = heartbeat("m1", &["orders-eu"]);
        check!(unchanged.response.error_code == 0);
        check!(unchanged.pending.is_empty());
        let resubscribed = heartbeat("m1", &["orders-eu", "other"]);
        check!(resubscribed.response.error_code == 0);
        check!(resubscribed.pending.target_metadata.is_none());
        check!(resubscribed.pending.target_per_member.is_empty());
        let written: Vec<&str> = resubscribed
            .pending
            .member_metadata
            .iter()
            .map(|(member_id, _)| member_id.as_str())
            .collect();
        check!(written == vec!["m1"]);
    }

    /// A group holding one member subscribed by regex, already reconciled and
    /// at a stable epoch.
    fn group_with_regex_member(metadata: &StaticMetadata, pattern: &str) -> GroupState {
        let config = NextGenConfig::assigning_at_once();
        let mut state = GroupState::new("g");
        state.add_or_update_member(build_member(
            "m1",
            &ConsumerGroupHeartbeatRequest {
                subscribed_topic_regex: Some(pattern.into()),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
            ClientIdentity {
                id: "client",
                host: "host",
            },
            Instant::now(),
        ));
        state.bump_epoch();
        maybe_update_target(&mut state, &config, metadata);
        state.advance_member_epoch("m1");
        state
    }

    fn orders_metadata() -> StaticMetadata {
        let orders = Uuid([12; 16]);
        StaticMetadata {
            input: ReconcileInput {
                topic_id_by_name: [("orders-eu".into(), orders)].into(),
                partitions_per_topic: [(orders, 2)].into(),
                ..Default::default()
            },
        }
    }

    #[derive(Clone, Copy, krabka_macros::FieldDefaults)]
    struct RegexJoinSetup<'a> {
        #[default("^orders-.*")]
        pattern: &'a str,
    }

    fn regex_join_request(setup: RegexJoinSetup<'_>) -> ConsumerGroupHeartbeatRequest {
        ConsumerGroupHeartbeatRequest {
            group_id: "g".into(),
            member_id: "m1".into(),
            member_epoch: 0,
            subscribed_topic_regex: Some(setup.pattern.into()),
            rebalance_timeout_ms: 60_000,
            ..Default::default()
        }
    }

    /// Kafka's `throwIfRegularExpressionIsInvalid` fails the heartbeat that
    /// carries a bad pattern, before any member record is written, so the
    /// joining member is never admitted.
    #[test]
    fn invalid_regex_on_join_rejects_the_heartbeat() {
        let config = NextGenConfig::assigning_at_once();
        let metadata = orders_metadata();
        for pattern in ["(", "[a-", "a{2,1}"] {
            let mut state = GroupState::new("g");
            let step = step_heartbeat(
                &mut state,
                &config,
                &metadata,
                &regex_join_request(RegexJoinSetup { pattern }),
                ClientIdentity {
                    id: "client",
                    host: "host",
                },
                Instant::now(),
                &RegexResolution::none(),
            );

            check!(
                step.response.error_code == crate::codes::INVALID_REGULAR_EXPRESSION,
                "{pattern}"
            );
            check!(
                step.response
                    .error_message
                    .as_deref()
                    .is_some_and(|m| m.starts_with(&format!(
                        "SubscribedTopicRegex `{pattern}` is not a valid regular expression: "
                    ))),
                "{pattern}: {:?}",
                step.response.error_message,
            );
            check!(step.pending.is_empty(), "{pattern}");
            assert!(state.members.is_empty(), "{pattern}");
        }
    }

    /// A pattern change to something that does not compile leaves the existing
    /// member exactly as it was: same pattern, same epoch, same group epoch.
    #[test]
    fn invalid_regex_on_pattern_change_leaves_member_untouched() {
        let config = NextGenConfig::assigning_at_once();
        let metadata = orders_metadata();
        for pattern in ["(", "[a-", "a{2,1}"] {
            let mut state = group_with_regex_member(&metadata, "^orders-.*");
            let member_epoch = state.members["m1"].member_epoch;
            let group_epoch = state.group_epoch;

            let result = update_member_state(
                &mut state,
                &config,
                &metadata,
                &ConsumerGroupHeartbeatRequest {
                    group_id: "g".into(),
                    member_id: "m1".into(),
                    member_epoch,
                    subscribed_topic_regex: Some(pattern.into()),
                    rebalance_timeout_ms: 60_000,
                    ..Default::default()
                },
                ClientIdentity {
                    id: "other-client",
                    host: "other-host",
                },
                Instant::now(),
                &RegexResolution::none(),
            );

            check!(result.is_err(), "{pattern}");
            let member = &state.members["m1"];
            check!(
                member.subscribed_topic_regex.as_deref() == Some("^orders-.*"),
                "{pattern}"
            );
            check!(member.client_id == "client", "{pattern}");
            check!(member.member_epoch == member_epoch, "{pattern}");
            assert!(state.group_epoch == group_epoch, "{pattern}");
        }
    }

    /// The rejection is specific to the bad pattern: a valid one still admits
    /// the member and reconciles the topics it matches.
    #[test]
    fn valid_regex_still_reconciles() {
        let config = NextGenConfig::assigning_at_once();
        let metadata = orders_metadata();
        let resolver = FixedRegexResolver::new(&[("^orders-.*", &["orders-eu"])]);
        let mut state = GroupState::new("g");

        let step = step_heartbeat(
            &mut state,
            &config,
            &metadata,
            &regex_join_request(RegexJoinSetup::default()),
            ClientIdentity {
                id: "client",
                host: "host",
            },
            Instant::now(),
            &RegexResolution::with(&resolver),
        );

        check!(step.response.error_code == 0);
        check!(step.response.error_message.is_none());
        // The resolution's batch follows the join's, and the next heartbeat
        // assigns what it found.
        apply_regex_result(
            &mut state,
            step.resolutions.expect("the join resolves its pattern"),
            &metadata.input,
        );
        let member_epoch = state.members["m1"].member_epoch;
        step_heartbeat(
            &mut state,
            &config,
            &metadata,
            &ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch,
                ..Default::default()
            },
            ClientIdentity {
                id: "client",
                host: "host",
            },
            Instant::now(),
            &RegexResolution::with(&resolver),
        );
        let assigned: Vec<i32> = state.members["m1"]
            .assigned_partitions
            .values()
            .flatten()
            .copied()
            .collect();
        assert!(assigned.len() == 2, "{:?}", state.members["m1"]);
    }

    /// Kafka's `maybeUpdateSubscribedTopicRegex`: an absent pattern leaves the
    /// stored one alone (the Java client sends the pattern only when it
    /// changed), and the empty string drops it (the client's way to remove a
    /// pattern). The dropped pattern selects no topic, where the empty regex
    /// used to match every topic the principal may describe, and its
    /// resolution, which no member uses any more, is tombstoned.
    #[test]
    fn an_absent_pattern_keeps_the_regex_and_an_empty_one_drops_it() {
        let config = NextGenConfig::assigning_at_once();
        let metadata = orders_metadata();
        let resolver = FixedRegexResolver::new(&[("orders-.*", &["orders-eu"])]);
        // (pattern sent at the steady-state heartbeat, the member's pattern,
        // the topics of its target afterwards, the resolution records)
        for (sent, pattern, target_topics, records) in [
            (None, Some("orders-.*"), 1, vec![]),
            (Some(""), None, 0, vec![("orders-.*".to_owned(), None)]),
            (Some("orders-.*"), Some("orders-.*"), 1, vec![]),
        ] {
            let mut state = GroupState::new("g");
            let request = |member_epoch, regex: Option<&str>| ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: "m1".into(),
                member_epoch,
                subscribed_topic_regex: regex.map(str::to_owned),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            };
            let client = ClientIdentity {
                id: "client",
                host: "host",
            };
            let joined = step_heartbeat(
                &mut state,
                &config,
                &metadata,
                &request(0, Some("orders-.*")),
                client,
                Instant::now(),
                &RegexResolution::with(&resolver),
            );
            apply_regex_result(
                &mut state,
                joined.resolutions.expect("the join resolves its pattern"),
                &metadata.input,
            );
            let member_epoch = state.members["m1"].member_epoch;

            let step = step_heartbeat(
                &mut state,
                &config,
                &metadata,
                &request(member_epoch, sent),
                client,
                Instant::now(),
                &RegexResolution::with(&resolver),
            );

            check!(step.response.error_code == 0, "{sent:?}");
            check!(step.pending.resolved_regexes == records, "{sent:?}");
            check!(
                state.members["m1"].subscribed_topic_regex.as_deref() == pattern,
                "{sent:?}"
            );
            check!(
                state.target.per_member.get("m1").map_or(0, HashMap::len) == target_topics,
                "{sent:?}"
            );
        }
    }

    /// Acceptance parity with RE2J, the engine Kafka validates with. Rust's
    /// `regex` runs in Unicode mode here on purpose: it accepts everything
    /// RE2J does in these cases, where `RegexBuilder::unicode(false)` would
    /// reject the Unicode classes RE2J supports.
    #[test]
    fn regex_acceptance_matches_re2j() {
        for (pattern, accepted) in [
            // ASCII in RE2J, Unicode-aware in Rust — both compile.
            (r"\d+", true),
            (r"\w+", true),
            (r"x", true),
            // Unicode classes: RE2J supports them, and `unicode(false)` would
            // not.
            (r"\pN", true),
            (r"\p{Greek}", true),
            // Named groups: RE2's `(?P<name>)` spelling and the modern
            // `(?<name>)` spelling.
            (r"(?P<name>a)", true),
            (r"(?<name>a)", true),
            // Rejected by both engines.
            ("(", false),
            ("[a-", false),
            // Inline flags. RE2J's `parsePerlFlags` takes only `i`, `m`, `s`
            // and `U`, with `-` to negate; `regex` also takes `x`, `u` and
            // `R`, so those must be rejected here to stay with Kafka.
            ("(?i)abc", true),
            ("(?im)abc", true),
            ("(?i-s)abc", true),
            ("(?U)a+", true),
            ("(?i:abc)", true),
            ("(?:abc)", true),
            ("(?x) a b c", false),
            ("(?x:abc)", false),
            ("(?iu)abc", false),
            ("(?-u)abc", false),
            ("(?R)abc", false),
            // A `(?` that is not a flag group at all: the literal `(` an
            // escape produces, and one inside a character class.
            (r"\(?abc", true),
            ("[(?x]abc", true),
            (r"a\[(?x)", false),
        ] {
            check!(
                check_subscribed_topic_regex(pattern).is_ok() == accepted,
                "{pattern}: {:?}",
                check_subscribed_topic_regex(pattern),
            );
        }
    }

    /// A flag RE2J does not have is answered with the message Kafka builds
    /// from RE2J's own `PatternSyntaxException.getDescription`, so a client
    /// that reads `error_message` sees the same text either broker produced.
    #[test]
    fn an_re2j_unsupported_flag_carries_kafkas_message() {
        check!(
            check_subscribed_topic_regex("(?x)abc")
                == Err(
                    "SubscribedTopicRegex `(?x)abc` is not a valid regular expression: \
                        invalid or unsupported Perl syntax."
                        .to_string()
                )
        );
    }

    /// The one behavioral difference the Unicode choice leaves — `\d` matching
    /// a non-ASCII digit — cannot be observed through a subscription, because
    /// Kafka topic names are drawn from `[a-zA-Z0-9._-]`.
    #[test]
    fn unicode_digit_class_cannot_change_a_topic_name_match() {
        let re = regex::Regex::new(r"^t\d+$").expect("compiles");
        check!(re.is_match("t42"));
        check!(!re.is_match("t-42"));
        // Unicode-aware in Rust, ASCII-only in RE2J; no legal topic name can
        // contain this character, so the divergence is unreachable.
        assert!(re.is_match("t\u{0663}"));
    }

    #[derive(Debug)]
    struct CountingAssignor {
        calls: Arc<AtomicUsize>,
    }
    impl Assignor for CountingAssignor {
        fn name(&self) -> &'static str {
            "counting"
        }
        fn assign(&self, _group: &GroupSpec, _topics: &TopicMetadata) -> Assignment {
            self.calls.fetch_add(1, Ordering::SeqCst);
            std::collections::HashMap::new()
        }
    }

    #[test]
    fn pick_assignor_skips_unregistered_member_preference() {
        let config = NextGenConfig::assigning_at_once();
        let mut state = crate::coordinator::unified::consumer_state::GroupState::new("g");
        let mut m = build_member(
            "m1",
            &ConsumerGroupHeartbeatRequest::default(),
            crate::coordinator::unified::ClientIdentity {
                id: "client-a",
                host: "h",
            },
            Instant::now(),
        );
        m.server_assignor = Some("ghost".into());
        state.members.insert("m1".into(), m);

        let picked = pick_assignor(&state, &config);
        assert!(picked.name() == "uniform");
    }

    /// Kafka's `computePreferredServerAssignor`: the group runs the assignor
    /// the most members name, and the first configured one when none does.
    /// `Describe` reports the same choice.
    #[test]
    fn pick_assignor_follows_the_majority_of_members() {
        let config = NextGenConfig::assigning_at_once();
        let uniform = || vec![Some("uniform"); 1];
        let range = |count| vec![Some("range"); count];
        // (assignors the members name, the assignor the group runs)
        for (named, expected) in [
            (vec![], "uniform"),
            (vec![None, None], "uniform"),
            (range(1), "range"),
            (uniform().into_iter().chain(range(20)).collect(), "range"),
            (range(20).into_iter().chain(uniform()).collect(), "range"),
            ([uniform(), vec![None; 3], range(2)].concat(), "range"),
            (
                uniform()
                    .into_iter()
                    .chain(uniform())
                    .chain(range(1))
                    .collect(),
                "uniform",
            ),
        ] {
            let mut state = GroupState::new("g");
            for (index, assignor) in named.iter().enumerate() {
                let mut member = build_member(
                    &format!("m{index}"),
                    &ConsumerGroupHeartbeatRequest::default(),
                    ClientIdentity {
                        id: "client",
                        host: "host",
                    },
                    Instant::now(),
                );
                member.server_assignor = assignor.map(str::to_owned);
                state.members.insert(member.member_id.clone(), member);
            }

            check!(
                pick_assignor(&state, &config).name() == expected,
                "{named:?}"
            );
            check!(
                preferred_server_assignor(&state).unwrap_or_else(|| "uniform".to_string())
                    == expected,
                "{named:?}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn custom_assignor_invoked_when_requested() {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut config = NextGenConfig::assigning_at_once();
        config
            .register_assignor(Arc::new(CountingAssignor {
                calls: calls.clone(),
            }))
            .unwrap();

        let log = Arc::new(InMemoryOffsetsLog::default());
        let coord = crate::coordinator::unified::actor::test_support::coordinator_with_log(
            config,
            empty_metadata(),
            log,
        );
        let handle = coord.get_or_create_consumer("g");

        let resp = crate::coordinator::unified::actor::test_support::rpc::consumer_request(
            &handle,
            ConsumerGroupHeartbeatRequest {
                group_id: "g".into(),
                member_id: String::new(),
                member_epoch: 0,
                subscribed_topic_names: Some(vec!["t".into()]),
                server_assignor: Some("counting".into()),
                rebalance_timeout_ms: 60_000,
                ..Default::default()
            },
        )
        .await;
        assert!(resp.error_code == 0);
        assert!(
            calls.load(Ordering::SeqCst) >= 1,
            "custom assignor must be invoked at least once",
        );
    }
}
